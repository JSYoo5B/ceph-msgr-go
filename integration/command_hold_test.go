package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// commandHold observes the connections one test client dials to one daemon
// and can withhold what the client would receive on them. It is test-only and
// decodes nothing, so it works the same in CRC and secure mode and identifies
// no frame: not the message a write carried, not a transaction and not a
// reply.
//
// Two ways start the hold. armOnWrite: the first write of at least threshold
// bytes binds its connection before the bytes leave; the caller makes its
// subject command the only write of that size. holdNow: every selected
// connection is held from that call on. A held connection parks the result of
// each Read, received bytes and errors alike, so its session learns neither of
// a reply nor of the end of the peer. release returns the parked result
// unchanged; closing the connection ends the parked Read with net.ErrClosed.
type commandHold struct {
	selects   func(endpoint string) bool
	threshold int

	mu       sync.Mutex
	armed    bool
	all      bool
	holder   *commandHoldConn
	dials    int
	large    int
	returned int
	complete int
	released bool
	lost     []error

	// bound is closed when a write binds the hold; withheld when a held Read
	// returned from the peer and was parked, bytes or the end of the peer
	// alike; received only when such a parked Read carried at least one
	// byte, so that an empty end of the peer is never taken for bytes that
	// arrived from a live peer; wrote when a write of at least
	// threshold bytes on a held connection returned, with or without an
	// error; closed when a selected connection was closed.
	bound, withheld, received, wrote, closed, release chan struct{}
	once                                              struct{ bound, withheld, received, wrote, closed, release sync.Once }
}

func newCommandHold(threshold int, selects func(endpoint string) bool) *commandHold {
	return &commandHold{selects: selects, threshold: threshold, bound: make(chan struct{}), withheld: make(chan struct{}),
		received: make(chan struct{}), wrote: make(chan struct{}), closed: make(chan struct{}), release: make(chan struct{})}
}

// daemonEndpoint reports whether a dialed "<ip>:<port>" is the endpoint of a
// "v2:<ip>:<port>/<nonce>" daemon address.
func daemonEndpoint(address string) func(string) bool {
	target, _, _ := strings.Cut(strings.TrimPrefix(address, "v2:"), "/")
	want, wantErr := netip.ParseAddrPort(target)
	return func(endpoint string) bool {
		got, err := netip.ParseAddrPort(endpoint)
		return err == nil && wantErr == nil && got.Port() == want.Port() && got.Addr().Unmap() == want.Addr().Unmap()
	}
}

// dialer wraps every selected connection of a client.
func (h *commandHold) dialer(dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil || !h.selects(endpoint) {
			return conn, err
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		h.dials++
		return &commandHoldConn{Conn: conn, hold: h, index: h.dials, done: make(chan struct{})}, nil
	}
}

func (h *commandHold) armOnWrite() { h.mu.Lock(); h.armed = true; h.mu.Unlock() }
func (h *commandHold) holdNow()    { h.mu.Lock(); h.all = true; h.mu.Unlock() }

// releaseHeld lets every parked and later Read return what the peer gave.
func (h *commandHold) releaseHeld() {
	h.mu.Lock()
	h.released = true
	h.mu.Unlock()
	h.once.release.Do(func() { close(h.release) })
}

// counts returns the selected dials, the writes of at least threshold bytes
// on them and the dial index of the connection a write bound (0: none).
func (h *commandHold) counts() (dials, large, boundConnection int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.holder != nil {
		boundConnection = h.holder.index
	}
	return h.dials, h.large, boundConnection
}

// completeWrites returns how many of the returned writes of at least threshold
// bytes accepted every byte without an error. A short or failed write is
// counted as issued and returned but not as complete, so only this count
// shows that a padded command left the client in full.
func (h *commandHold) completeWrites() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.complete
}

// awaitReturned waits until want writes of at least threshold bytes on
// selected connections have returned, held or not, and reports the count.
func (h *commandHold) awaitReturned(ctx context.Context, want int) int {
	for {
		h.mu.Lock()
		returned := h.returned
		h.mu.Unlock()
		if returned >= want {
			return returned
		}
		select {
		case <-ctx.Done():
			return returned
		case <-time.After(time.Millisecond):
		}
	}
}

func (h *commandHold) holds(c *commandHoldConn) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.released && (h.all || h.holder == c)
}

type commandHoldConn struct {
	net.Conn
	hold      *commandHold
	index     int
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func (c *commandHoldConn) Write(p []byte) (int, error) {
	h := c.hold
	h.mu.Lock()
	large := len(p) >= h.threshold
	if large {
		h.large++
		if h.armed && h.holder == nil {
			// Bind before the bytes leave, so a reply cannot pass first.
			h.holder = c
			h.once.bound.Do(func() { close(h.bound) })
		}
	}
	h.mu.Unlock()
	n, err := c.Conn.Write(p)
	c.observe(err)
	if large {
		h.mu.Lock()
		h.returned++
		if n == len(p) && err == nil {
			h.complete++
		}
		h.mu.Unlock()
		if h.holds(c) {
			h.once.wrote.Do(func() { close(h.wrote) })
		}
	}
	return n, err
}

func (c *commandHoldConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.observe(err)
	if (n > 0 || err != nil) && c.hold.holds(c) {
		c.hold.once.withheld.Do(func() { close(c.hold.withheld) })
		if n > 0 {
			c.hold.once.received.Do(func() { close(c.hold.received) })
		}
		select {
		case <-c.hold.release:
		case <-c.done:
			return 0, net.ErrClosed
		}
	}
	return n, err
}

// Close closes the connection once and gives every caller that result. A
// session closes its connection only after it recorded its terminal cause,
// so the closed signal means that the session of a selected connection has
// ended, held or not.
func (c *commandHoldConn) Close() error {
	c.closeOnce.Do(func() {
		// done first: an I/O error that the close itself causes is not the
		// loss of the transport.
		close(c.done)
		c.closeErr = c.Conn.Close()
		c.hold.once.closed.Do(func() { close(c.hold.closed) })
	})
	return c.closeErr
}

// observe records every error that a Read or Write of a selected connection
// returned before the client closed that connection, other than an expired
// deadline: the errors with which the transport was lost, as the session met
// them at the connection. A reader and a writer may each meet one, and the
// session records whichever it handled first.
func (c *commandHoldConn) observe(err error) {
	if err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		return
	}
	select {
	case <-c.done:
		return
	default:
	}
	c.hold.mu.Lock()
	c.hold.lost = append(c.hold.lost, err)
	c.hold.mu.Unlock()
}

// transportLoss returns the errors recorded by observe.
func (h *commandHold) transportLoss() []error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]error(nil), h.lost...)
}

// endedWithSessionCause reports whether a known-unsent command failure
// carries the terminal cause of its ended session: one of the errors the
// hold saw the transport end with, or, where the transport gave none before
// the client closed it, the keepalive timeout that ended the session. Any other
// known-unsent error, such as a target or argument refusal, is not that
// cause.
func endedWithSessionCause(err error, transportLoss []error) bool {
	if err == nil {
		return false
	}
	if len(transportLoss) == 0 {
		return errors.Is(err, cephmsgr.ErrKeepaliveTimeout)
	}
	for _, loss := range transportLoss {
		if errors.Is(err, loss) {
			return true
		}
	}
	return false
}

// knownUnsent reports a command failure that is neither an unknown outcome
// nor a completed daemon reply.
func knownUnsent(result cephmsgr.Result, err error) bool {
	return err != nil && !errors.As(err, new(*cephmsgr.OutcomeUnknownError)) && !errors.As(err, new(*cephmsgr.CommandError)) &&
		result.Code == 0 && result.Message == "" && len(result.Data) == 0
}

// unknownOutcome reports an unknown outcome without any server result.
func unknownOutcome(result cephmsgr.Result, err error) bool {
	return errors.As(err, new(*cephmsgr.OutcomeUnknownError)) && result.Code == 0 && result.Message == "" && len(result.Data) == 0
}

// The hold parks received bytes and the end of the peer alike, binds only to
// a selected connection's first large write after arming, and returns the
// parked result unchanged when released.
func TestCommandHoldParksBytesAndErrors(t *testing.T) {
	type side struct {
		client net.Conn
		server net.Conn
	}
	pair := func(t *testing.T) side {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		accepted := make(chan net.Conn, 1)
		go func() {
			conn, _ := listener.Accept()
			accepted <- conn
		}()
		client, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		server := <-accepted
		t.Cleanup(func() { client.Close(); server.Close() })
		return side{client, server}
	}
	wrap := func(t *testing.T, h *commandHold, endpoint string) (net.Conn, net.Conn) {
		t.Helper()
		s := pair(t)
		conn, err := h.dialer(func(context.Context, string, string) (net.Conn, error) { return s.client, nil })(context.Background(), "tcp", endpoint)
		if err != nil {
			t.Fatal(err)
		}
		return conn, s.server
	}
	read := func(conn net.Conn) <-chan error {
		done := make(chan error, 1)
		go func() {
			buffer := make([]byte, 16)
			n, err := conn.Read(buffer)
			if err == nil && string(buffer[:n]) != "reply" {
				err = errors.New("another payload: " + string(buffer[:n]))
			}
			done <- err
		}()
		return done
	}
	pending := func(t *testing.T, done <-chan error) {
		t.Helper()
		select {
		case err := <-done:
			t.Fatal("a held read returned", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	selected := daemonEndpoint("v2:192.0.2.1:6800/7")
	if !selected("192.0.2.1:6800") || selected("192.0.2.1:6801") || selected("192.0.2.2:6800") || !daemonEndpoint("v2:[2001:db8::1]:6800/7")("[2001:db8::1]:6800") {
		t.Fatal("daemon endpoint selection")
	}
	t.Run("an unselected connection is never wrapped", func(t *testing.T) {
		h := newCommandHold(8, selected)
		h.holdNow()
		conn, server := wrap(t, h, "192.0.2.9:6800")
		if _, held := conn.(*commandHoldConn); held {
			t.Fatal("an unselected endpoint was wrapped")
		}
		server.Write([]byte("reply"))
		if err := <-read(conn); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("only the first large write after arming binds", func(t *testing.T) {
		h := newCommandHold(8, selected)
		conn, server := wrap(t, h, "192.0.2.1:6800")
		other, _ := wrap(t, h, "192.0.2.1:6800")
		conn.Write([]byte("a large write before arming"))
		server.Write([]byte("reply"))
		if err := <-read(conn); err != nil {
			t.Fatal("an unarmed hold withheld bytes", err)
		}
		h.armOnWrite()
		conn.Write([]byte("small"))
		select {
		case <-h.bound:
			t.Fatal("a small write bound the hold")
		default:
		}
		other.Write([]byte("the subject command"))
		conn.Write([]byte("a later large write"))
		<-h.bound
		<-h.wrote
		if dials, large, bound := h.counts(); dials != 2 || large != 3 || bound != 2 {
			t.Fatal("counts", dials, large, bound)
		}
		if returned := h.awaitReturned(context.Background(), 3); returned != 3 || h.completeWrites() != 3 {
			t.Fatal("returned padded writes", returned, h.completeWrites())
		}
		expired, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer stop()
		if returned := h.awaitReturned(expired, 4); returned != 3 {
			t.Fatal("a padded write that was never issued was reported as returned", returned)
		}
		server.Write([]byte("reply"))
		if err := <-read(conn); err != nil {
			t.Fatal("a connection that did not carry the binding write was held", err)
		}
	})
	t.Run("a short or failed large write returns but is not complete", func(t *testing.T) {
		for name, script := range map[string]shortWriteConn{
			"short with an error":      {accept: 3, err: io.ErrShortWrite},
			"short without an error":   {accept: 3},
			"refused":                  {accept: 0, err: net.ErrClosed},
			"every byte with an error": {accept: -1, err: net.ErrClosed},
		} {
			h := newCommandHold(8, selected)
			conn, err := h.dialer(func(context.Context, string, string) (net.Conn, error) { return script, nil })(context.Background(), "tcp", "192.0.2.1:6800")
			if err != nil {
				t.Fatal(err)
			}
			h.holdNow()
			conn.Write([]byte("a padded command"))
			<-h.wrote
			if _, large, _ := h.counts(); large != 1 || h.awaitReturned(context.Background(), 1) != 1 || h.completeWrites() != 0 {
				t.Fatal(name, "was counted as a complete write", large, h.completeWrites())
			}
		}
	})
	t.Run("bytes are parked until released", func(t *testing.T) {
		h := newCommandHold(8, selected)
		conn, server := wrap(t, h, "192.0.2.1:6800")
		h.holdNow()
		conn.Write([]byte("small"))
		select {
		case <-h.wrote:
			t.Fatal("a small write on a held connection was reported as the command")
		default:
		}
		conn.Write([]byte("a padded command"))
		<-h.wrote
		server.Write([]byte("reply"))
		done := read(conn)
		<-h.withheld
		<-h.received
		pending(t, done)
		h.releaseHeld()
		if err := <-done; err != nil {
			t.Fatal("the released read lost its bytes", err)
		}
		if len(h.transportLoss()) != 0 {
			t.Fatal("received bytes were recorded as a transport loss", h.transportLoss())
		}
	})
	t.Run("the end of the peer is parked until released", func(t *testing.T) {
		h := newCommandHold(8, selected)
		conn, server := wrap(t, h, "192.0.2.1:6800")
		h.holdNow()
		server.Close()
		done := read(conn)
		<-h.withheld
		pending(t, done)
		// An empty end of the peer is parked but is not received bytes.
		select {
		case <-h.received:
			t.Fatal("the end of the peer without a byte was reported as received bytes")
		default:
		}
		if loss := h.transportLoss(); len(loss) != 1 || !errors.Is(loss[0], io.EOF) {
			t.Fatal("the end of the peer was not recorded as the transport loss", h.transportLoss())
		}
		select {
		case <-h.closed:
			t.Fatal("the hold reported a close of the client side")
		default:
		}
		h.releaseHeld()
		if err := <-done; !errors.Is(err, io.EOF) {
			t.Fatal("the released read lost the peer's end", err)
		}
	})
	t.Run("only the session's own cause passes for an ended session", func(t *testing.T) {
		reset := &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}
		for name, tc := range map[string]struct {
			err  error
			loss []error
			want bool
		}{
			"the observed end of the peer":           {io.EOF, []error{io.EOF}, true},
			"the observed error wrapped":             {fmt.Errorf("session: %w", reset), []error{reset}, true},
			"keepalive without a transport error":    {fmt.Errorf("ended: %w", cephmsgr.ErrKeepaliveTimeout), nil, true},
			"another transport error":                {io.ErrUnexpectedEOF, []error{io.EOF}, false},
			"an equal message from another error":    {errors.New("EOF"), []error{io.EOF}, false},
			"a target refusal":                       {cephmsgr.ErrOSDCommandTarget, []error{io.EOF}, false},
			"an argument refusal":                    {errors.New("invalid command"), []error{io.EOF}, false},
			"keepalive although a loss was observed": {cephmsgr.ErrKeepaliveTimeout, []error{io.EOF}, false},
			"a target refusal without a loss":        {cephmsgr.ErrOSDCommandTarget, nil, false},
			"a closed handle without a loss":         {cephmsgr.ErrClosed, nil, false},
			"no error":                               {nil, []error{io.EOF}, false},
			"the second of two observed errors":      {reset, []error{io.EOF, reset}, true},
			"neither of two observed errors":         {io.ErrUnexpectedEOF, []error{io.EOF, reset}, false},
		} {
			if got := endedWithSessionCause(tc.err, tc.loss); got != tc.want {
				t.Error(name, got)
			}
		}
	})
	t.Run("an error caused by the client's own close is not a transport loss", func(t *testing.T) {
		h := newCommandHold(8, selected)
		conn, _ := wrap(t, h, "192.0.2.1:6800")
		done := read(conn)
		conn.Close()
		if err := <-done; err == nil {
			t.Fatal("a read on a closed connection returned no error")
		}
		conn.Write([]byte("after the close"))
		if len(h.transportLoss()) != 0 {
			t.Fatal("the close was recorded as a transport loss", h.transportLoss())
		}
	})
	t.Run("a close is reported without a hold", func(t *testing.T) {
		h := newCommandHold(8, selected)
		conn, _ := wrap(t, h, "192.0.2.1:6800")
		select {
		case <-h.closed:
			t.Fatal("an open connection was reported closed")
		default:
		}
		conn.Close()
		<-h.closed
	})
	t.Run("closing a held connection ends its parked read", func(t *testing.T) {
		h := newCommandHold(8, selected)
		conn, server := wrap(t, h, "192.0.2.1:6800")
		h.holdNow()
		server.Write([]byte("reply"))
		done := read(conn)
		<-h.withheld
		conn.Close()
		<-h.closed
		if err := <-done; !errors.Is(err, net.ErrClosed) {
			t.Fatal("the parked read did not end with the close", err)
		}
	})
	t.Run("classification helpers", func(t *testing.T) {
		unknown := &cephmsgr.OutcomeUnknownError{Cause: io.EOF}
		refused := &cephmsgr.CommandError{Code: -13}
		if !knownUnsent(cephmsgr.Result{}, io.EOF) || knownUnsent(cephmsgr.Result{}, nil) || knownUnsent(cephmsgr.Result{}, unknown) ||
			knownUnsent(cephmsgr.Result{Code: -13}, refused) || knownUnsent(cephmsgr.Result{Data: []byte("x")}, io.EOF) {
			t.Fatal("known-unsent classification")
		}
		if !unknownOutcome(cephmsgr.Result{}, unknown) || unknownOutcome(cephmsgr.Result{}, io.EOF) || unknownOutcome(cephmsgr.Result{}, nil) ||
			unknownOutcome(cephmsgr.Result{Message: "x"}, unknown) {
			t.Fatal("unknown-outcome classification")
		}
	})
}

// shortWriteConn accepts at most accept bytes of a write (all of them when
// accept is negative) and returns err with the count.
type shortWriteConn struct {
	net.Conn
	accept int
	err    error
}

func (c shortWriteConn) Write(p []byte) (int, error) {
	if c.accept >= 0 && c.accept < len(p) {
		return c.accept, c.err
	}
	return len(p), c.err
}
