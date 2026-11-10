package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
)

func TestBootstrapRecoversTemporaryAuthenticationEOF(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	var attempts atomic.Int32
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if attempts.Add(1) <= 2 {
			return nil, fmt.Errorf("ceph messenger authentication: %w", io.EOF)
		}
		return dial(ctx, network, endpoint)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal("temporary setup loss prevented bootstrap", err)
	}
	defer c.Close()
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 3 {
		t.Fatal("unexpected setup attempts", attempts.Load())
	}
}

// dialTrace records every synthetic dial boundary of one bootstrap: the
// attempt number, the time since Dial started, the endpoint context's
// remaining budget and state, and the caller context's state. A bootstrap
// failure that only says "context deadline exceeded" cannot tell an endpoint
// timeout, an exhausted retry window and a caller timeout apart; the trace can.
type dialTrace struct {
	caller  context.Context
	started time.Time

	mu       sync.Mutex
	attempts int
	events   []string
}

func newDialTrace(caller context.Context) *dialTrace {
	return &dialTrace{caller: caller, started: time.Now()}
}

func (d *dialTrace) record(attempt int, boundary string, endpoint context.Context, err error) {
	remaining := "none"
	if deadline, ok := endpoint.Deadline(); ok {
		remaining = time.Until(deadline).String()
	}
	d.events = append(d.events, fmt.Sprintf("attempt=%d %s elapsed=%s endpoint_remaining=%s endpoint_ctx=%v caller_ctx=%v dial_err=%v",
		attempt, boundary, time.Since(d.started), remaining, endpoint.Err(), d.caller.Err(), err))
}

// enter records the start of a dial and returns its one-based attempt number.
func (d *dialTrace) enter(endpoint context.Context) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.attempts++
	d.record(d.attempts, "dial-enter", endpoint, nil)
	return d.attempts
}

func (d *dialTrace) exit(attempt int, endpoint context.Context, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.record(attempt, "dial-exit", endpoint, err)
}

func (d *dialTrace) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.attempts
}

// report renders the outcome of Dial and every recorded boundary.
func (d *dialTrace) report(err error) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return fmt.Sprintf("Dial returned: attempts=%d elapsed=%s caller_ctx=%v err=%q\n%s",
		d.attempts, time.Since(d.started), d.caller.Err(), fmt.Sprint(err), strings.Join(d.events, "\n"))
}

// blockUntilEndpointDeadline is a synthetic unreachable endpoint: it holds
// the dial until the context bootstrap gave it ends, then reports that cause.
func blockUntilEndpointDeadline(ctx context.Context) (net.Conn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// The bootstrap deadline tests run in a synctest bubble. Time advances only
// when every goroutine is blocked, so the mock handshake costs no time and
// each deadline fires at an exact instant regardless of scheduler load.

func TestBootstrapRetryHonorsContextAndSetupLimit(t *testing.T) {
	const connectTimeout = 30 * time.Millisecond
	for _, test := range []struct {
		name   string
		caller time.Duration
		bound  time.Duration
	}{
		// The caller's deadline ends the retry window early.
		{name: "caller deadline", caller: 10 * time.Millisecond, bound: 10 * time.Millisecond},
		// A caller without a deadline is still bounded by ConnectTimeout.
		{name: "setup limit", bound: connectTimeout},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				options := mockOptions(t, 20, [16]byte{1})
				options.ConnectTimeout = connectTimeout
				ctx := context.Background()
				if test.caller != 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, test.caller)
					defer cancel()
				}
				trace := newDialTrace(ctx)
				// Fake time never runs out, so an unbounded retry loop would
				// spin until the test binary's own timeout. End it instead
				// with a cause bootstrap does not retry.
				runaway := errors.New("synthetic dialer: runaway bootstrap retries")
				options.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
					attempt, err := trace.enter(ctx), io.EOF
					if attempt > 16 {
						err = runaway
					}
					trace.exit(attempt, ctx, err)
					return nil, err
				}
				_, err := Dial(ctx, options)
				elapsed := time.Since(trace.started)
				if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, io.EOF) {
					t.Fatal("setup loss or timeout cause missing\n" + trace.report(err))
				}
				if elapsed != test.bound || trace.count() < 2 {
					t.Fatalf("bootstrap retry ended after %s, want exactly %s with at least one retry\n%s", elapsed, test.bound, trace.report(err))
				}
			})
		})
	}
}

// An endpoint that exhausts its own ConnectTimeout while the caller stays
// live is a retryable setup failure: bootstrap dials again and recovers.
func TestBootstrapRecoversEndpointDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const connectTimeout = 100 * time.Millisecond
		options := mockOptions(t, 20, [16]byte{1})
		options.ConnectTimeout = connectTimeout
		dial := options.DialContext
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		trace := newDialTrace(ctx)
		var firstExit, secondBudget time.Duration
		var firstEndpoint, firstCaller error
		options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
			attempt := trace.enter(ctx)
			if attempt == 1 {
				conn, err := blockUntilEndpointDeadline(ctx)
				firstExit, firstEndpoint, firstCaller = time.Since(trace.started), ctx.Err(), trace.caller.Err()
				trace.exit(attempt, ctx, err)
				return conn, err
			}
			if attempt == 2 {
				deadline, _ := ctx.Deadline()
				secondBudget = time.Until(deadline)
			}
			conn, err := dial(ctx, network, endpoint)
			trace.exit(attempt, ctx, err)
			return conn, err
		}
		c, err := Dial(ctx, options)
		if err != nil {
			t.Fatal("endpoint deadline prevented bounded bootstrap recovery\n" + trace.report(err))
		}
		defer c.Close()
		if firstExit != connectTimeout || !errors.Is(firstEndpoint, context.DeadlineExceeded) || firstCaller != nil {
			t.Fatal("first endpoint did not end at its own deadline under a live caller\n" + trace.report(err))
		}
		// The retry window is one more ConnectTimeout shared by the backoff
		// and every further attempt, never the caller's remaining second.
		if trace.count() != 2 || secondBudget <= 0 || secondBudget >= connectTimeout || ctx.Err() != nil {
			t.Fatal("unexpected recovery attempt or budget\n" + trace.report(err))
		}
		if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
			t.Fatal("recovered client refused a command\n" + trace.report(err))
		}
	})
}

// Retries after an endpoint deadline are themselves bounded by one more
// ConnectTimeout. A second endpoint that also never answers ends bootstrap at
// exactly that bound with the caller still live, and keeps the timeout cause.
func TestBootstrapEndpointDeadlineRetryWindowIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const connectTimeout = 100 * time.Millisecond
		options := mockOptions(t, 20, [16]byte{1})
		options.ConnectTimeout = connectTimeout
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		trace := newDialTrace(ctx)
		options.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			attempt := trace.enter(ctx)
			conn, err := blockUntilEndpointDeadline(ctx)
			trace.exit(attempt, ctx, err)
			return conn, err
		}
		_, err := Dial(ctx, options)
		elapsed := time.Since(trace.started)
		if !errors.Is(err, context.DeadlineExceeded) || trace.count() != 2 {
			t.Fatal("retry window did not end bootstrap after one retry\n" + trace.report(err))
		}
		if elapsed != 2*connectTimeout || ctx.Err() != nil {
			t.Fatalf("bootstrap ended after %s, want exactly %s with a live caller\n%s", elapsed, 2*connectTimeout, trace.report(err))
		}
	})
}

// The caller's own deadline is terminal: the endpoint context ends because
// the caller did, long before ConnectTimeout, and bootstrap must not redial.
func TestBootstrapDoesNotRetryCallerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const callerTimeout = 20 * time.Millisecond
		options := mockOptions(t, 20, [16]byte{1})
		ctx, cancel := context.WithTimeout(context.Background(), callerTimeout)
		defer cancel()
		trace := newDialTrace(ctx)
		var callerAtExit error
		options.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			attempt := trace.enter(ctx)
			conn, err := blockUntilEndpointDeadline(ctx)
			callerAtExit = trace.caller.Err()
			trace.exit(attempt, ctx, err)
			return conn, err
		}
		_, err := Dial(ctx, options)
		elapsed := time.Since(trace.started)
		if !errors.Is(err, context.DeadlineExceeded) || trace.count() != 1 {
			t.Fatal("caller deadline was retried or lost\n" + trace.report(err))
		}
		if elapsed != callerTimeout || !errors.Is(callerAtExit, context.DeadlineExceeded) {
			t.Fatalf("bootstrap ended after %s, want exactly the caller's %s\n%s", elapsed, callerTimeout, trace.report(err))
		}
	})
}

func TestBootstrapDoesNotRetryExplicitAuthenticationRejection(t *testing.T) {
	for _, transient := range []error{io.EOF, context.DeadlineExceeded} {
		t.Run(transient.Error(), func(t *testing.T) {
			options := mockOptions(t, 20, [16]byte{1})
			var attempts atomic.Int32
			rejection := &cephx.AuthenticationError{Method: 2, Code: -13}
			options.DialContext = func(context.Context, string, string) (net.Conn, error) {
				attempts.Add(1)
				// One transient leaf must not make a joined explicit rejection retryable.
				return nil, errors.Join(transient, rejection)
			}
			_, err := Dial(context.Background(), options)
			if !errors.Is(err, rejection) || attempts.Load() != 1 {
				t.Fatal("explicit rejection was retried or lost", attempts.Load(), err)
			}
		})
	}
}
