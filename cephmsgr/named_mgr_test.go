package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/maps"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

var namedMgrVersion = Command{JSON: []byte(`{"prefix":"version"}`), Input: []byte{0, 255, 7}}

func namedMgrMap(t *testing.T, c *Client, mutate func(*maps.Mgr)) {
	t.Helper()
	c.mu.Lock()
	m, source := c.mgrMap, c.mon
	m.Addresses = append([]msgr.Address(nil), m.Addresses...)
	c.mu.Unlock()
	mutate(&m)
	if _, err := c.handleMap(source, mockMgrMapWithStandbys(m, m.Epoch+1, []StandbyManager{{Name: "standby", GlobalID: 101}})); err != nil {
		t.Fatal(err)
	}
}

func TestNamedMgrExactActiveUsesSharedSessionAndRawResults(t *testing.T) {
	f := newTellFixture(t, [16]byte{1}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.WaitMgrMap(ctx, 0); err != nil {
		t.Fatal(err)
	}
	namedMgrMap(t, c, func(*maps.Mgr) {})
	for _, tc := range []struct {
		name string
		want error
	}{
		{"standby", ErrManagerNotActive}, {"missing", ErrManagerNotFound},
		{"mgr.a", ErrManagerNotFound}, {"*", ErrManagerNotFound}, {"0", ErrManagerNotFound}, {" a", ErrManagerNotFound},
	} {
		_, err := c.MgrTellTo(ctx, tc.name, namedMgrVersion)
		var unknown *OutcomeUnknownError
		if !errors.Is(err, tc.want) || errors.As(err, &unknown) || f.mgrDial.Load() != 0 || f.calls.Load() != 0 {
			t.Fatal("non-active exact name was sent or rerouted", tc.name, err)
		}
	}
	if _, err := c.MgrTellTo(ctx, "", namedMgrVersion); err == nil {
		t.Fatal("empty name selected current active implicitly")
	}
	var wg sync.WaitGroup
	for _, call := range []func(context.Context, Command) (Result, error){c.MgrCommand, c.MgrTell, func(ctx context.Context, cmd Command) (Result, error) { return c.MgrTellTo(ctx, "a", cmd) }} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := call(ctx, namedMgrVersion)
			if err != nil || got.Code != 0 || got.Message != "status text" || !bytes.Equal(got.Data, namedMgrVersion.Input) {
				t.Error("shared raw command result", got, err)
			}
		}()
	}
	wg.Wait()
	seen := make(map[uint64]bool)
	for range 3 {
		request := nextTellRequest(t, ctx, f)
		if request.role != 16 || request.data.Transaction == 0 || seen[request.data.Transaction] {
			t.Fatal("shared MGR TID collision or route", request)
		}
		seen[request.data.Transaction] = true
	}
	if f.mgrDial.Load() != 1 {
		t.Fatal("named tell opened a private MGR connection", f.mgrDial.Load())
	}
	denial, err := c.MgrTellTo(ctx, "a", Command{JSON: []byte(`{"prefix":"deny"}`), Input: namedMgrVersion.Input})
	var server *CommandError
	var auth *AuthenticationError
	if denial.Code != -13 || !errors.As(err, &server) || errors.As(err, &auth) || !bytes.Equal(denial.Data, namedMgrVersion.Input) {
		t.Fatal("signed command denial lost raw result", denial, err)
	}
	// A higher epoch or changed standby metadata leaves the selected daemon
	// and the existing session intact. Ticket renewal with the same client ID
	// also retains this binding (covered by the manager renewal regressions).
	c.mu.Lock()
	before := c.mgr
	c.mu.Unlock()
	namedMgrMap(t, c, func(*maps.Mgr) {})
	if _, err := c.MgrTellTo(ctx, "a", namedMgrVersion); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	unchanged := c.mgr == before
	c.mu.Unlock()
	if !unchanged || f.mgrDial.Load() != 1 {
		t.Fatal("metadata-only map update retired the active session")
	}
}

func TestNamedMgrSelectedBindingSurvivesNoChangesAndRejectsEveryIdentityChange(t *testing.T) {
	for _, field := range []string{"name", "gid", "nonce", "client", "unavailable", "mon-admission", "auth-rejection", "stale-source", "epoch"} {
		t.Run(field, func(t *testing.T) {
			f := newTellFixture(t, [16]byte{1}, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := Dial(ctx, f.options)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if err := c.WaitMgrReady(ctx); err != nil {
				t.Fatal(err)
			}
			c.mu.Lock()
			s, b := c.mgr, bindManager(c.mgrMap, c.auth.GlobalID)
			original := append([]msgr.Address(nil), b.addresses...)
			switch field {
			case "name":
				c.mgrMap.Name = "renamed"
			case "gid":
				c.mgrMap.GlobalID++
			case "nonce":
				c.mgrMap.Addresses[0].Nonce++
			case "client":
				// Copy the immutable published auth bundle before changing its ID.
				a := *c.auth
				a.GlobalID++
				c.auth = &a
			case "unavailable":
				c.mgrMap.Available = false
			case "mon-admission":
				c.monReady = false
			case "auth-rejection":
				c.authErr = &AuthenticationError{Method: 2, Code: -13}
			case "epoch":
				c.mgrMap.Epoch++
			}
			c.mu.Unlock()
			if field == "stale-source" {
				m := mockMgrMapWithStandbys(maps.Mgr{Name: "wrong", GlobalID: 101, Addresses: original}, 100, nil)
				if _, err := c.handleMap(new(session.Session), m); err != nil {
					t.Fatal(err)
				}
			}
			err = c.managerBeforeWrite(s, b)
			if field == "stale-source" || field == "epoch" {
				if err != nil {
					t.Fatal("unchanged binding refused", err)
				}
			} else if err == nil {
				t.Fatal("writer accepted changed binding", field)
			}
			if !reflect.DeepEqual(b.addresses, original) {
				t.Fatal("binding aliased mutable map addresses")
			}
		})
	}
}

func TestNamedMgrNameOnlyMapSwitchRetiresStartedCallsWithoutReplay(t *testing.T) {
	f := newTellFixture(t, [16]byte{1}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	old, selected := c.mgr, bindManager(c.mgrMap, c.auth.GlobalID)
	c.mu.Unlock()
	started := make(chan error, 1)
	go func() { _, err := c.MgrTellTo(ctx, "a", Command{JSON: []byte(`{"prefix":"block"}`)}); started <- err }()
	nextTellRequest(t, ctx, f) // The server has observed the command; its reply is withheld.
	namedMgrMap(t, c, func(m *maps.Mgr) { m.Name = "renamed" })
	var unknown *OutcomeUnknownError
	if err := admissionResult(t, ctx, started); !errors.Is(err, ErrManagerTargetChanged) || !errors.As(err, &unknown) {
		t.Fatal("started name-bound tell lost its unknown result", err)
	}
	if err := old.Err(); !errors.Is(err, ErrManagerChanged) {
		t.Fatal("name-only map change did not fail old session", err)
	}
	if _, err := c.managerForBinding(ctx, &selected); !errors.Is(err, ErrManagerTargetChanged) {
		t.Fatal("frozen setup selected new active", err)
	}
	if _, err := c.MgrTellTo(ctx, "a", namedMgrVersion); !errors.Is(err, ErrManagerNotFound) {
		t.Fatal("old name substituted new active", err)
	}
	if f.calls.Load() != 1 || f.mgrDial.Load() != 1 {
		t.Fatal("uncertain command replayed", f.calls.Load(), f.mgrDial.Load())
	}
	// A new explicit call may choose the new active name, on a fresh session.
	if _, err := c.MgrTellTo(ctx, "renamed", namedMgrVersion); err != nil {
		t.Fatal(err)
	}
}

type namedMgrBlockedConn struct {
	net.Conn
	armed                    atomic.Bool
	started, release, closed chan struct{}
	once                     sync.Once
}

func (c *namedMgrBlockedConn) Write(p []byte) (int, error) {
	if c.armed.Swap(false) {
		close(c.started)
		select {
		case <-c.release:
		case <-c.closed:
			return 0, net.ErrClosed
		}
	}
	return c.Conn.Write(p)
}

func (c *namedMgrBlockedConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// The operation checks Done while entering the global slot, the manager gate,
// the session writer queue and then the reply wait. Observe that last wait so
// a blocked writer deterministically leaves this command queued and unstarted.
type namedMgrQueuedContext struct {
	context.Context
	calls  atomic.Int32
	queued chan struct{}
}

func (c *namedMgrQueuedContext) Done() <-chan struct{} {
	if c.calls.Add(1) == 4 {
		close(c.queued)
	}
	return c.Context.Done()
}

func TestNamedMgrWriterQueueMapSwitchSeparatesUnstartedAndUnknown(t *testing.T) {
	f := newTellFixture(t, [16]byte{1}, nil)
	dial := f.options.DialContext
	var conn *namedMgrBlockedConn
	f.options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		c, err := dial(ctx, network, endpoint)
		if err == nil && strings.HasSuffix(endpoint, ":6800") {
			conn = &namedMgrBlockedConn{Conn: c, started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
			return conn, nil
		}
		return c, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal(err)
	}
	conn.armed.Store(true)
	first := make(chan error, 1)
	go func() { _, err := c.MgrTellTo(ctx, "a", namedMgrVersion); first <- err }()
	select {
	case <-conn.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	queued := &namedMgrQueuedContext{Context: ctx, queued: make(chan struct{})}
	second := make(chan error, 1)
	go func() { _, err := c.MgrTellTo(queued, "a", namedMgrVersion); second <- err }()
	select {
	case <-queued.queued:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	namedMgrMap(t, c, func(m *maps.Mgr) { m.Name = "replacement" })
	var unknown *OutcomeUnknownError
	if err := admissionResult(t, ctx, second); !errors.Is(err, ErrManagerTargetChanged) || errors.As(err, &unknown) {
		t.Fatal("queued command became uncertain or rerouted", err)
	}
	if err := admissionResult(t, ctx, first); !errors.Is(err, ErrManagerTargetChanged) || !errors.As(err, &unknown) {
		t.Fatal("started write lost uncertainty", err)
	}
	if f.calls.Load() != 0 || f.mgrDial.Load() != 1 {
		t.Fatal("queued or uncertain command was replayed", f.calls.Load(), f.mgrDial.Load())
	}
}

func TestNamedMgrBeforeWriteRefusesQueuedCallAfterMONAdmissionLoss(t *testing.T) {
	f := newTellFixture(t, [16]byte{1}, nil)
	dial := f.options.DialContext
	var conn *namedMgrBlockedConn
	f.options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		c, err := dial(ctx, network, endpoint)
		if err == nil && strings.HasSuffix(endpoint, ":6800") {
			conn = &namedMgrBlockedConn{Conn: c, started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
			return conn, nil
		}
		return c, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal(err)
	}
	conn.armed.Store(true)
	first := make(chan error, 1)
	go func() { _, err := c.MgrTellTo(ctx, "a", namedMgrVersion); first <- err }()
	select {
	case <-conn.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	queued := &namedMgrQueuedContext{Context: ctx, queued: make(chan struct{})}
	second := make(chan error, 1)
	go func() { _, err := c.MgrTellTo(queued, "a", namedMgrVersion); second <- err }()
	select {
	case <-queued.queued:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// This does not Fail the MGR session. Only its installed BeforeWrite guard
	// can prevent the queued message from reaching the synthetic daemon.
	c.mu.Lock()
	c.monReady = false
	c.signal()
	c.mu.Unlock()
	close(conn.release)
	if err := admissionResult(t, ctx, first); err != nil {
		t.Fatal("already started call lost valid reply", err)
	}
	var unknown *OutcomeUnknownError
	if err := admissionResult(t, ctx, second); !errors.Is(err, session.ErrRetired) || errors.As(err, &unknown) {
		t.Fatal("unadmitted MON allowed queued named tell", err)
	}
	if f.calls.Load() != 1 {
		t.Fatal("writer guard did not run", f.calls.Load())
	}
	c.mu.Lock()
	c.monReady = true
	c.signal()
	c.mu.Unlock()
	if _, err := c.MgrTellTo(ctx, "a", namedMgrVersion); err != nil {
		t.Fatal("re-admission changed shared session", err)
	}
	if f.calls.Load() != 2 || f.mgrDial.Load() != 1 {
		t.Fatal("known-unsent named call replayed", f.calls.Load(), f.mgrDial.Load())
	}
}

func TestNamedMgrSetupSwitchNeverDialsReplacement(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed-handshake", true: "failed-handshake"}[failed], func(t *testing.T) {
			f := newTellFixture(t, [16]byte{1}, nil)
			dial := f.options.DialContext
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			var attempts atomic.Int32
			f.options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
				if strings.HasSuffix(endpoint, ":6800") {
					attempts.Add(1)
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					if failed {
						return nil, io.EOF
					}
				}
				return dial(ctx, network, endpoint)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := Dial(ctx, f.options)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			finished := make(chan error, 1)
			go func() { _, err := c.MgrTellTo(ctx, "a", namedMgrVersion); finished <- err }()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			namedMgrMap(t, c, func(m *maps.Mgr) { m.Name = "replacement" })
			once.Do(func() { close(release) })
			err = admissionResult(t, ctx, finished)
			var unknown *OutcomeUnknownError
			if !errors.Is(err, ErrManagerTargetChanged) || errors.As(err, &unknown) || attempts.Load() != 1 || f.calls.Load() != 0 {
				t.Fatal("obsolete setup sent, retried or became uncertain", err, attempts.Load(), f.calls.Load())
			}
		})
	}
}

func TestNamedMgrCancellationCloseAndUnavailableAreKnownBeforeSend(t *testing.T) {
	f := newTellFixture(t, [16]byte{1}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	before, stop := context.WithCancel(ctx)
	stop()
	if _, err := c.MgrTellTo(before, "", Command{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := c.WaitMgrMap(ctx, 0); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.mgrMap.Available = false
	c.mu.Unlock()
	if _, err := c.MgrTellTo(ctx, "a", namedMgrVersion); !errors.Is(err, ErrManagerNotActive) {
		t.Fatal(err)
	}
	if f.calls.Load() != 0 || f.mgrDial.Load() != 0 {
		t.Fatal("unavailable target was dialed")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MgrTellTo(ctx, "", Command{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestNamedMgrStartedCancellationAndCloseDoNotReplay(t *testing.T) {
	f := newTellFixture(t, [16]byte{1}, nil)
	f.options.MaxInFlight = 1
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	active, stop := context.WithCancel(ctx)
	first := make(chan error, 1)
	go func() { _, err := c.MgrTellTo(active, "a", Command{JSON: []byte(`{"prefix":"block"}`)}); first <- err }()
	nextTellRequest(t, ctx, f)
	queued, stopQueued := context.WithCancel(ctx)
	stopQueued()
	_, err = c.MgrTellTo(queued, "a", namedMgrVersion)
	var unknown *OutcomeUnknownError
	if !errors.Is(err, context.Canceled) || errors.As(err, &unknown) || f.calls.Load() != 1 {
		t.Fatal("queued canceled tell was sent", err)
	}
	stop()
	if err := admissionResult(t, ctx, first); !errors.Is(err, context.Canceled) || !errors.As(err, &unknown) {
		t.Fatal("started cancel lost uncertainty", err)
	}
	if _, err := c.MgrCommand(ctx, namedMgrVersion); err != nil {
		t.Fatal("cancellation failed shared session", err)
	}
	nextTellRequest(t, ctx, f)
	closing := make(chan error, 1)
	go func() { _, err := c.MgrTellTo(ctx, "a", Command{JSON: []byte(`{"prefix":"block"}`)}); closing <- err }()
	nextTellRequest(t, ctx, f)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := admissionResult(t, ctx, closing); !errors.Is(err, ErrClosed) || !errors.As(err, &unknown) || f.calls.Load() != 3 {
		t.Fatal("started Close replayed or lost uncertainty", err, f.calls.Load())
	}
}
