package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func gateTellReply(t *testing.T, kind uint16, transaction, sequence uint64) []byte {
	t.Helper()
	return tellFrame(t, msgr.MessageData{Type: kind, Version: 1, Transaction: transaction, Sequence: sequence, Front: []byte{0, 0, 0, 0, 0, 0, 0, 0}})
}

// gatedPipe returns a gated connection whose peer end the test writes to.
func gatedPipe(t *testing.T, gate *tellReplyGate, connection uint64) (*tellReplyGateConn, net.Conn) {
	t.Helper()
	local, peer := net.Pipe()
	conn := newTellReplyGateConn(local, gate, connection)
	t.Cleanup(func() { conn.Close(); peer.Close() })
	return conn, peer
}

// bindGate writes the selected command through conn with the given identity.
func bindGate(t *testing.T, conn *tellReplyGateConn, peer net.Conn, command msgr.MessageData, transaction, sequence uint64) {
	t.Helper()
	command.Transaction, command.Sequence = transaction, sequence
	frame := tellFrame(t, command)
	received := make(chan []byte, 1)
	go func() {
		data := make([]byte, len(frame))
		_, _ = io.ReadFull(peer, data)
		received <- data
	}()
	if n, err := conn.Write(frame); err != nil || n != len(frame) {
		t.Fatal("gated write", n, err)
	}
	if got := <-received; !bytes.Equal(got, frame) {
		t.Fatal("the gate changed outgoing bytes")
	}
}

func readExactly(t *testing.T, conn net.Conn, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	result := make(chan error, 1)
	go func() { _, err := io.ReadFull(conn, data); result <- err }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal("gated read", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the gate did not release bytes that it must pass")
	}
	return data
}

func TestTellReplyGateWithholdsOnlyTheBoundTransactionReply(t *testing.T) {
	gate := newTellReplyGate()
	conn, peer := gatedPipe(t, gate, 2)
	command := msgr.TellCommand([16]byte{1}, []string{`{"prefix":"config set","var":"log_max_recent","val":["10001"]}`}, nil)
	gate.arm(command.Front, command.Data)
	// A different command on the same connection never binds the armed gate.
	bindGate(t, conn, peer, msgr.TellCommand([16]byte{1}, []string{`{"prefix":"config get","var":"log_max_recent"}`}, nil), 1, 1)
	if _, _, _, bound := gate.binding(); bound {
		t.Fatal("a different command bound the gate")
	}
	bindGate(t, conn, peer, command, 2, 2)
	connection, transaction, sequence, bound := gate.binding()
	if !bound || connection != 2 || transaction != 2 || sequence != 2 {
		t.Fatal("the gate is not bound to the selected command's own identity", connection, transaction, sequence, bound)
	}
	// A later identical write never moves the binding.
	bindGate(t, conn, peer, command, 9, 9)
	if _, transaction, _, _ := gate.binding(); transaction != 2 {
		t.Fatal("a replayed write rebound the gate", transaction)
	}

	banner := msgr.Banner{Supported: msgr.Revision1, Required: msgr.Revision1}.Encode()
	hello := tellFrame(t, msgr.MessageData{Type: msgr.MgrMapMessage, Version: 1, Sequence: 1, Front: []byte("map")})
	// Near misses on the bound connection: the same type with another
	// transaction, and the bound transaction with another reply type.
	otherTransaction := gateTellReply(t, msgr.TellCommandReplyMessage, 1, 2)
	otherType := gateTellReply(t, msgr.MgrCommandReplyMessage, 2, 3)
	passed := bytes.Join([][]byte{banner, hello, otherTransaction, otherType}, nil)
	withheld := gateTellReply(t, msgr.TellCommandReplyMessage, 2, 4)
	trailing := gateTellReply(t, msgr.TellCommandReplyMessage, 3, 5)
	go func() {
		// Three-byte fragments: frame boundaries never align with reads.
		stream := bytes.Join([][]byte{passed, withheld, trailing}, nil)
		for len(stream) > 0 {
			n := min(3, len(stream))
			if _, err := peer.Write(stream[:n]); err != nil {
				return
			}
			stream = stream[n:]
		}
	}()
	if got := readExactly(t, conn, len(passed)); !bytes.Equal(got, passed) {
		t.Fatal("the gate changed or reordered passed bytes")
	}
	blocked := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := conn.Read(make([]byte, 1))
		blocked <- struct {
			n   int
			err error
		}{n, err}
	}()
	select {
	case <-gate.held:
	case result := <-blocked:
		t.Fatal("the bound reply was not withheld", result.n, result.err)
	case <-time.After(5 * time.Second):
		t.Fatal("the gate never decoded the bound reply")
	}
	held, err := gate.heldReply()
	if err != nil || held != (tellHeldReply{Connection: 2, Type: msgr.TellCommandReplyMessage, Transaction: 2, Sequence: 4, FrameBytes: len(withheld)}) {
		t.Fatal("withheld reply identity", held, err)
	}
	select {
	case result := <-blocked:
		t.Fatal("a byte of the withheld frame reached the reader", result.n, result.err)
	default:
	}
	if err := gate.closeHeld(); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-blocked:
		if result.n != 0 || !errors.Is(result.err, net.ErrClosed) {
			t.Fatal("closing the held connection released frame bytes or lost its cause", result.n, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not release the withheld read")
	}
}

func TestTellReplyGatePassesTheBoundTransactionOnAnotherConnection(t *testing.T) {
	gate := newTellReplyGate()
	bound, boundPeer := gatedPipe(t, gate, 2)
	other, otherPeer := gatedPipe(t, gate, 3)
	command := msgr.TellCommand([16]byte{1}, []string{`{"prefix":"config set"}`}, nil)
	gate.arm(command.Front, command.Data)
	bindGate(t, bound, boundPeer, command, 2, 2)
	// The same command written later on another connection is not a binding,
	// and that connection's reply with the same transaction ID passes.
	bindGate(t, other, otherPeer, command, 2, 2)
	if connection, _, _, _ := gate.binding(); connection != 2 {
		t.Fatal("a later connection took over the binding", connection)
	}
	stream := append(msgr.Banner{Supported: msgr.Revision1, Required: msgr.Revision1}.Encode(), gateTellReply(t, msgr.TellCommandReplyMessage, 2, 1)...)
	go func() { _, _ = otherPeer.Write(stream) }()
	if got := readExactly(t, other, len(stream)); !bytes.Equal(got, stream) {
		t.Fatal("another connection's reply was changed")
	}
	select {
	case <-gate.held:
		t.Fatal("a reply on another connection was withheld")
	default:
	}
}

func TestTellReplyGateRecordsAnUndecodableStreamAndForwardsIt(t *testing.T) {
	gate := newTellReplyGate()
	conn, peer := gatedPipe(t, gate, 1)
	command := msgr.TellCommand([16]byte{1}, []string{`{"prefix":"config set"}`}, nil)
	gate.arm(command.Front, command.Data)
	bindGate(t, conn, peer, command, 2, 2)
	// Not a CRC frame after the banner, as a secure stream would be.
	stream := append(msgr.Banner{Supported: msgr.Revision1, Required: msgr.Revision1}.Encode(), bytes.Repeat([]byte{0xee}, 96)...)
	go func() { _, _ = peer.Write(stream) }()
	if got := readExactly(t, conn, len(stream)); !bytes.Equal(got, stream) {
		t.Fatal("an undecodable stream was changed")
	}
	if _, err := gate.heldReply(); err == nil {
		t.Fatal("an undecodable subject stream left no recorded error")
	}
	// A truncated final frame is released unchanged with the original error.
	gate = newTellReplyGate()
	conn, peer = gatedPipe(t, gate, 1)
	partial := append(msgr.Banner{Supported: msgr.Revision1, Required: msgr.Revision1}.Encode(), gateTellReply(t, msgr.TellCommandReplyMessage, 7, 1)[:20]...)
	go func() { _, _ = peer.Write(partial); peer.Close() }()
	got, err := io.ReadAll(conn)
	if err != nil || !bytes.Equal(got, partial) {
		t.Fatal("a stream that ended inside a frame lost bytes", len(got), err)
	}
}

// strictCloseConn fails a second Close, as a TCP connection does.
type strictCloseConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *strictCloseConn) Close() error {
	if c.closed.Swap(true) {
		return net.ErrClosed
	}
	return c.Conn.Close()
}

func paddedPipe(t *testing.T, gate *paddedReplyGate, connection uint64, role string) (*paddedReplyGateConn, net.Conn) {
	t.Helper()
	local, peer := net.Pipe()
	conn := gate.wrap(&strictCloseConn{Conn: local}, connection, role)
	t.Cleanup(func() { conn.Close(); peer.Close() })
	return conn, peer
}

func paddedWrite(t *testing.T, conn net.Conn, peer net.Conn, size int) {
	t.Helper()
	done := make(chan error, 1)
	go func() { _, err := io.ReadFull(peer, make([]byte, size)); done <- err }()
	if n, err := conn.Write(make([]byte, size)); err != nil || n != size {
		t.Fatal("padded gate write", n, err)
	}
	if err := <-done; err != nil {
		t.Fatal("the padded gate did not forward a write", err)
	}
}

func TestPaddedReplyGateWithholdsEveryByteAfterTheFirstPaddedWrite(t *testing.T) {
	gate := newPaddedReplyGate(4096, func(role string) bool { return role == "mgr.a" || role == "mgr.b" })
	other, otherPeer := paddedPipe(t, gate, 1, "mon.a")
	subject, subjectPeer := paddedPipe(t, gate, 2, "mgr.a")
	later, laterPeer := paddedPipe(t, gate, 3, "mgr.a")
	// A padded write to an unselected role and a small write to a selected
	// role never bind, and both connections keep delivering received bytes.
	paddedWrite(t, other, otherPeer, 8192)
	paddedWrite(t, subject, subjectPeer, 4095)
	if _, _, bound := gate.binding(); bound {
		t.Fatal("an unselected role or a small write bound the gate")
	}
	go func() { _, _ = subjectPeer.Write([]byte("before")) }()
	if got := readExactly(t, subject, 6); string(got) != "before" {
		t.Fatal("bytes received before the padded write were changed", got)
	}
	paddedWrite(t, subject, subjectPeer, 4096)
	if connection, role, bound := gate.binding(); !bound || connection != 2 || role != "mgr.a" {
		t.Fatal("the gate is not bound to the padded write's connection", connection, role, bound)
	}
	// A later padded write, also on another selected connection, never rebinds.
	paddedWrite(t, subject, subjectPeer, 8192)
	paddedWrite(t, later, laterPeer, 8192)
	if connection, _, _ := gate.binding(); connection != 2 {
		t.Fatal("a later padded write rebound the gate", connection)
	}
	blocked := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := subject.Read(make([]byte, 16))
		blocked <- struct {
			n   int
			err error
		}{n, err}
	}()
	go func() { _, _ = subjectPeer.Write([]byte("reply")) }()
	select {
	case <-gate.received:
	case result := <-blocked:
		t.Fatal("bytes received after the padded write reached the reader", result.n, result.err)
	case <-time.After(5 * time.Second):
		t.Fatal("the gate never saw the bytes it must withhold")
	}
	// Another selected connection still delivers what it receives.
	go func() { _, _ = laterPeer.Write([]byte("other")) }()
	if got := readExactly(t, later, 5); string(got) != "other" {
		t.Fatal("another connection's bytes were withheld or changed", got)
	}
	select {
	case result := <-blocked:
		t.Fatal("a withheld byte reached the reader", result.n, result.err)
	default:
	}
	if err := gate.closeHeld(); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-blocked:
		if result.n != 0 || !errors.Is(result.err, net.ErrClosed) {
			t.Fatal("closing the held connection released bytes or lost its cause", result.n, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not release the withheld read")
	}
	if err := newPaddedReplyGate(4096, func(string) bool { return true }).closeHeld(); err == nil {
		t.Fatal("an unbound gate closed a connection")
	}
	// The session closes its connection once its withheld read is released.
	// That second close reports the first close's result, not a new error.
	if err := subject.Close(); err != nil {
		t.Fatal("a second close of the held connection failed", err)
	}
}

// The reader that a hold blocks belongs to a session, which closes its
// connection as soon as that read is released. Whichever close comes first,
// both must succeed, so closing the held connection never fails a test.
func TestReplyGateConnectionsCloseOnceForEveryCaller(t *testing.T) {
	for range 200 {
		gate := newTellReplyGate()
		local, peer := net.Pipe()
		conn := newTellReplyGateConn(&strictCloseConn{Conn: local}, gate, 1)
		command := msgr.TellCommand([16]byte{1}, []string{`{"prefix":"config set"}`}, nil)
		gate.arm(command.Front, command.Data)
		bindGate(t, conn, peer, command, 2, 2)
		stream := append(msgr.Banner{Supported: msgr.Revision1, Required: msgr.Revision1}.Encode(), gateTellReply(t, msgr.TellCommandReplyMessage, 2, 1)...)
		// The banner arrives in the same read as the reply and still passes.
		go func() { _, _ = peer.Write(stream) }()
		banner := len(stream) - len(gateTellReply(t, msgr.TellCommandReplyMessage, 2, 1))
		if got := readExactly(t, conn, banner); !bytes.Equal(got, stream[:banner]) {
			t.Fatal("bytes received together with the withheld reply were changed")
		}
		session := make(chan error, 1)
		go func() {
			// A session reader: when its read ends it closes its connection.
			_, _ = conn.Read(make([]byte, 1))
			session <- conn.Close()
		}()
		<-gate.held
		if err := gate.closeHeld(); err != nil {
			t.Fatal("closing the held connection failed", err)
		}
		if err := <-session; err != nil {
			t.Fatal("the session's own close failed", err)
		}
		peer.Close()
	}
}

// Every complete frame that precedes the bound reply is delivered before the
// gate parks on the reply, whether the peer's bytes arrive in three-byte
// fragments or in one write that also carries the reply and a later frame, and
// whether the caller reads one byte at a time or with a large buffer. The reply
// and everything after it on that connection stay withheld.
func TestTellReplyGateDeliversTheWholePrefixBeforeParking(t *testing.T) {
	for _, fragment := range []int{3, 1 << 20} {
		for _, readSize := range []int{1, 7, 64 << 10} {
			gate := newTellReplyGate()
			local, peer := net.Pipe()
			conn := newTellReplyGateConn(&strictCloseConn{Conn: local}, gate, 2)
			command := msgr.TellCommand([16]byte{1}, []string{`{"prefix":"config set"}`}, nil)
			gate.arm(command.Front, command.Data)
			bindGate(t, conn, peer, command, 2, 2)
			passed := bytes.Join([][]byte{
				msgr.Banner{Supported: msgr.Revision1, Required: msgr.Revision1}.Encode(),
				tellFrame(t, msgr.MessageData{Type: msgr.MgrMapMessage, Version: 1, Sequence: 1, Front: bytes.Repeat([]byte("m"), 300)}),
				gateTellReply(t, msgr.TellCommandReplyMessage, 1, 2),
				gateTellReply(t, msgr.MgrCommandReplyMessage, 2, 3),
			}, nil)
			withheld := gateTellReply(t, msgr.TellCommandReplyMessage, 2, 4)
			later := gateTellReply(t, msgr.TellCommandReplyMessage, 3, 5)
			stream := bytes.Join([][]byte{passed, withheld, later}, nil)
			go func() {
				for len(stream) > 0 {
					n := min(fragment, len(stream))
					if _, err := peer.Write(stream[:n]); err != nil {
						return
					}
					stream = stream[n:]
				}
			}()
			type result struct {
				data []byte
				err  error
			}
			reads := make(chan result, 1)
			go func() {
				// A session-like reader with a fixed buffer: it keeps reading
				// until the connection ends.
				var got []byte
				for {
					buffer := make([]byte, readSize)
					n, err := conn.Read(buffer)
					got = append(got, buffer[:n]...)
					if err != nil {
						reads <- result{got, err}
						return
					}
				}
			}()
			select {
			case <-gate.held:
			case got := <-reads:
				t.Fatal("the reader ended before the bound reply was held", fragment, readSize, len(got.data), got.err)
			case <-time.After(5 * time.Second):
				t.Fatal("the gate never held the bound reply", fragment, readSize)
			}
			if err := gate.closeHeld(); err != nil {
				t.Fatal(err)
			}
			got := <-reads
			if !bytes.Equal(got.data, passed) || !errors.Is(got.err, net.ErrClosed) {
				t.Fatalf("fragment=%d read=%d: delivered %d bytes, want exactly the %d bytes before the bound reply; error %v", fragment, readSize, len(got.data), len(passed), got.err)
			}
			if held, err := gate.heldReply(); err != nil || held.Transaction != 2 || held.Sequence != 4 || held.FrameBytes != len(withheld) {
				t.Fatal("withheld reply identity", held, err)
			}
			peer.Close()
		}
	}
}

// lateReturnConn completes a write on the wire and returns from it only when
// released.
type lateReturnConn struct {
	net.Conn
	release chan struct{}
}

func (c *lateReturnConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	<-c.release
	return n, err
}

// The peer has read the whole command and its reply is held before the local
// Write returns. The receipt and the padded-write count are published only
// then, and the cells wait for them instead of reading them at the barrier.
func TestHeldReplyDoesNotOrderTheLocalWriteReceipt(t *testing.T) {
	gate := newTellReplyGate()
	local, peer := net.Pipe()
	defer peer.Close()
	gated := newTellReplyGateConn(local, gate, 7)
	defer gated.Close()
	probe := &monOutcomeWrites{}
	probe.enabled.Store(true)
	var writes atomic.Int32
	late := &lateReturnConn{Conn: gated, release: make(chan struct{})}
	conn := &commandWriteProbe{Conn: &monOutcomeConn{Conn: late, record: probe, connection: 7, peer: "mgr.x"}, writes: &writes}
	command := msgr.TellCommand([16]byte{1}, []string{`{"prefix":"config set","val":"` + string(bytes.Repeat([]byte("a"), 8<<10)) + `"}`}, nil)
	gate.arm(command.Front, command.Data)
	command.Transaction, command.Sequence = 5, 3
	frame := tellFrame(t, command)
	written := make(chan error, 1)
	go func() {
		n, err := conn.Write(frame)
		if err == nil && n != len(frame) {
			err = io.ErrShortWrite
		}
		written <- err
	}()
	// The peer reads all of A and replies to its transaction.
	if _, err := io.ReadFull(peer, make([]byte, len(frame))); err != nil {
		t.Fatal(err)
	}
	reply := append(msgr.Banner{Supported: msgr.Revision1, Required: msgr.Revision1}.Encode(), gateTellReply(t, msgr.TellCommandReplyMessage, 5, 1)...)
	go func() { _, _ = peer.Write(reply) }()
	go func() {
		// A session-like reader: it keeps reading until the connection ends.
		for buffer := make([]byte, 64<<10); ; {
			if _, err := gated.Read(buffer); err != nil {
				return
			}
		}
	}()
	select {
	case <-gate.held:
	case <-time.After(5 * time.Second):
		t.Fatal("the reply was never held")
	}
	// The barrier is reached and nothing is published yet.
	if matching, err := probe.matching(command.Front, command.Data); err != nil || len(matching) != 0 || writes.Load() != 0 {
		t.Fatal("the local write was published before it returned", matching, writes.Load(), err)
	}
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if err := probe.await(expired, command.Front, command.Data); !errors.Is(err, context.Canceled) {
		t.Fatal("waiting for an unpublished receipt outlived its context", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	awaited := make(chan error, 1)
	go func() { awaited <- probe.await(ctx, command.Front, command.Data) }()
	select {
	case err := <-awaited:
		t.Fatal("the wait returned before the local write completed", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(late.release)
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if err := <-awaited; err != nil {
		t.Fatal("the completed write was never published", err)
	}
	submitted := awaitMonOutcomeWrite(t, ctx, probe, command.Front, command.Data)
	if !submitted.Complete || submitted.Connection != 7 || submitted.Peer != "mgr.x" || submitted.Transaction != 5 || submitted.Sequence != 3 ||
		submitted.Written != len(frame) || awaitPaddedWrites(t, ctx, &writes, 1) != 1 {
		t.Fatal("receipt of the completed write", submitted, writes.Load())
	}
	if held, err := gate.heldReply(); err != nil || held.Connection != 7 || held.Transaction != 5 {
		t.Fatal("held reply", held, err)
	}
}
