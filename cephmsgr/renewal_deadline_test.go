package cephmsgr

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

type renewalAttempt struct {
	number int32
	at     time.Time
}

// Tickets travel through the ordinary encrypted AUTH_DONE exchange. No caller
// command or readiness wait wakes the coordinator while a renewal is due.
func renewalDeadlineOptions(t *testing.T, configure func(int32, *peerConfig), failure func(int32) error) (Options, *atomic.Int32, <-chan renewalAttempt) {
	t.Helper()
	options := mockOptions(t, 20, [16]byte{1})
	var count atomic.Int32
	attempts := make(chan renewalAttempt, 16)
	var peers sync.WaitGroup
	options.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n := count.Add(1)
		select {
		case attempts <- renewalAttempt{n, time.Now()}:
		default:
		}
		if failure != nil {
			if err := failure(n); err != nil {
				return nil, err
			}
		}
		cfg := peerConfig{fsid: [16]byte{1}, release: 20, role: 1}
		if configure != nil {
			configure(n, &cfg)
		}
		client, peer := net.Pipe()
		peers.Add(1)
		go func() {
			defer peers.Done()
			mockDaemon(peer, cfg)
		}()
		return client, nil
	}
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { peers.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("ticket renewal peers did not stop")
		}
	})
	return options, &count, attempts
}

func TestFractionalTicketRenewalBeforeExpiry(t *testing.T) {
	for _, service := range []struct {
		name string
		id   uint32
	}{{"AUTH", cephx.ServiceAuth}, {"MGR", cephx.ServiceMgr}} {
		t.Run(service.name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			options, _, attempts := renewalDeadlineOptions(t, func(n int32, cfg *peerConfig) {
				if n != 1 {
					return
				}
				if service.id == cephx.ServiceAuth {
					cfg.authTTL = 500 * time.Millisecond
				} else {
					cfg.mgrTTL = 500 * time.Millisecond
				}
				cfg.command = func(m msgr.MessageData) {
					if strings.Contains(string(m.Front), "hold mutation") {
						close(started)
						<-release
					}
				}
			}, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := Dial(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { unblock(); c.Close() }()
			<-attempts // Bootstrap.
			initial := c.snapshotAuth()
			ticket := initial.Tickets[service.id]
			if ticket.Expires.Sub(ticket.RenewAfter) != 125*time.Millisecond {
				t.Fatal("fractional ticket was not decoded with its renewal margin")
			}
			c.mu.Lock()
			old := c.mon
			c.mu.Unlock()
			raw := []byte{0, 255, 1}
			done := make(chan struct {
				result Result
				err    error
			}, 1)
			go func() {
				result, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"hold mutation"}`), Input: raw})
				done <- struct {
					result Result
					err    error
				}{result, err}
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			expiry := time.NewTimer(time.Until(ticket.Expires))
			defer expiry.Stop()
			select {
			case attempt := <-attempts:
				if attempt.number != 2 || attempt.at.Before(ticket.RenewAfter) || !attempt.at.Before(ticket.Expires) {
					t.Fatal("renewal attempt did not honor ticket validity", attempt, ticket.RenewAfter, ticket.Expires)
				}
			case <-expiry.C:
				t.Fatal("fractional ticket expired before the coordinator began renewal")
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			for {
				c.mu.Lock()
				ready := c.mon != old && c.monReady && c.auth.Tickets[service.id].Expires.After(ticket.Expires)
				changed := c.changed
				globalID := c.auth.GlobalID
				c.mu.Unlock()
				if ready {
					if globalID != initial.GlobalID || old.Err() != nil {
						t.Fatal("deadline renewal discarded identity or interrupted an admitted mutation", globalID, old.Err())
					}
					break
				}
				select {
				case <-changed:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			select {
			case <-done:
				t.Fatal("renewal replayed or released a mutation before its original reply")
			default:
			}
			unblock()
			select {
			case reply := <-done:
				if reply.err != nil || reply.result.Code != 0 || reply.result.Message != "status text" || !bytes.Equal(reply.result.Data, raw) {
					t.Fatal("renewal lost the original mutation result", reply.result, reply.err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestTicketRenewalDeadlineWakeAndClose(t *testing.T) {
	options, count, attempts := renewalDeadlineOptions(t, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	<-attempts
	c.mu.Lock()
	old := c.mon
	c.auth = invalidateAuthTickets(c.auth)
	c.mu.Unlock()
	c.wakeMonitor()
	select {
	case <-attempts:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ticket invalidation did not interrupt the future renewal timer")
	}
	for {
		c.mu.Lock()
		ready := c.mon != old && c.monReady && c.auth.Tickets[cephx.ServiceAuth].RenewAfter.After(time.Now())
		changed := c.changed
		c.mu.Unlock()
		if ready {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case err := <-closed:
		if err != nil || count.Load() != 2 {
			t.Fatal("Close retried or failed while waiting for a future renewal", count.Load(), err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close did not interrupt the future renewal timer")
	}
}

func TestTicketRenewalDeadlineBackoff(t *testing.T) {
	options, _, attempts := renewalDeadlineOptions(t, func(n int32, cfg *peerConfig) {
		cfg.authTTL = 200 * time.Millisecond
	}, func(n int32) error {
		if n > 1 {
			return io.EOF
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	<-attempts
	var previous renewalAttempt
	select {
	case previous = <-attempts:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for _, delay := range []time.Duration{250 * time.Millisecond, 500 * time.Millisecond} {
		// Repeated hints must not turn a failed renewal into a tight retry loop.
		func() {
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			deadline := time.NewTimer(delay + 300*time.Millisecond)
			defer deadline.Stop()
			for {
				select {
				case next := <-attempts:
					if next.at.Sub(previous.at) < delay {
						t.Fatal("wake bypassed renewal retry backoff", next.at.Sub(previous.at), delay)
					}
					previous = next
					return
				case <-ticker.C:
					c.wakeMonitor()
				case <-deadline.C:
					t.Fatal("renewal retry missed its backoff deadline")
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		}()
	}
}

func TestAlreadyDueTicketRenewalDoesNotSpin(t *testing.T) {
	options, count, attempts := renewalDeadlineOptions(t, func(_ int32, cfg *peerConfig) {
		cfg.authTTL, cfg.mgrTTL = time.Nanosecond, time.Nanosecond
	}, func(n int32) error {
		if n > 6 {
			return io.EOF // Bound peers even if the coordinator regresses to spinning.
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	<-attempts
	select {
	case <-attempts: // Exercise a successful exchange with already-due tickets.
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-time.After(400 * time.Millisecond):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if n := count.Load(); n < 2 || n > 3 {
		t.Fatal("already-due successful tickets caused a renewal spin", n)
	}
}
