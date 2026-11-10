package cephmsgr

import (
	"bytes"
	"errors"
	"io"
	"net"
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
