package testcluster

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func gateFrame(t *testing.T, kind uint16, transaction, sequence uint64, data []byte) []byte {
	t.Helper()
	var encoded bytes.Buffer
	message := msgr.MessageData{Type: kind, Version: 1, Transaction: transaction, Sequence: sequence, Front: []byte{0, 0, 0, 0, 0, 0, 0, 0}, Data: data}
	if err := msgr.NewWriter(&encoded, 128<<10).Write(message.Frame()); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func gatedOSDPipe(t *testing.T, gate *OSDReplyGate) (net.Conn, net.Conn) {
	t.Helper()
	local, peer := net.Pipe()
	conn := gate.Wrap(local)
	t.Cleanup(func() { conn.Close(); peer.Close() })
	return conn, peer
}

// gateWrite writes p through the gated connection and requires the peer to
// receive it unchanged.
func gateWrite(t *testing.T, conn, peer net.Conn, p []byte) {
	t.Helper()
	received := make(chan []byte, 1)
	go func() {
		data := make([]byte, len(p))
		_, _ = io.ReadFull(peer, data)
		received <- data
	}()
	if n, err := conn.Write(p); err != nil || n != len(p) {
		t.Fatal("gated write", n, err)
	}
	if got := <-received; !bytes.Equal(got, p) {
		t.Fatal("the gate changed outgoing bytes")
	}
}

func gateRead(t *testing.T, conn net.Conn, size, buffer int) []byte {
	t.Helper()
	var data []byte
	result := make(chan error, 1)
	go func() {
		p := make([]byte, buffer)
		for len(data) < size {
			n, err := conn.Read(p[:min(buffer, size-len(data))])
			data = append(data, p[:n]...)
			if err != nil {
				result <- err
				return
			}
		}
		result <- nil
	}()
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

// gateParked requires the next read to stay blocked until the connection is
// closed, without releasing a byte.
func gateParked(t *testing.T, conn net.Conn, closeHeld func() error) {
	t.Helper()
	type read struct {
		n   int
		err error
	}
	blocked := make(chan read, 1)
	go func() {
		n, err := conn.Read(make([]byte, 64<<10))
		blocked <- read{n, err}
	}()
	select {
	case got := <-blocked:
		t.Fatal("the gate released the withheld stream", got.n, got.err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := closeHeld(); err != nil {
		t.Fatal("close the held connection", err)
	}
	select {
	case got := <-blocked:
		if got.n != 0 || !errors.Is(got.err, net.ErrClosed) {
			t.Fatal("the withheld read did not end with the close error", got.n, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("closing did not release the parked read")
	}
}

func TestOSDReplyGateHoldsOnlyTheMarkedTransactionReply(t *testing.T) {
	marker := bytes.Repeat([]byte{0xa5, 0x5a}, 16)
	for _, buffer := range []int{1, 7, 64 << 10} {
		gate := NewOSDReplyGate(true, 4096)
		conn, peer := gatedOSDPipe(t, gate)
		other, otherPeer := gatedOSDPipe(t, gate)
		// Nothing binds or is counted before Arm, also not the marker.
		gateWrite(t, conn, peer, gateFrame(t, msgr.OSDOpMessage, 1, 1, append([]byte("early"), marker...)))
		if s := gate.State(); s.Bound || *s.MarkedRequests != 0 {
			t.Fatal("an unarmed gate bound or counted", s)
		}
		if gate.Arm(marker[:8]) == nil {
			t.Fatal("a short marker was accepted")
		}
		if err := gate.Arm(marker); err != nil || gate.Arm(marker) == nil {
			t.Fatal("arming once differs", err)
		}
		// Near misses: another request without the marker, and another
		// message type that carries it.
		gateWrite(t, conn, peer, gateFrame(t, msgr.OSDOpMessage, 2, 2, bytes.Repeat([]byte{1}, 5000)))
		gateWrite(t, conn, peer, gateFrame(t, msgr.OSDOpReplyMessage, 3, 3, marker))
		if s := gate.State(); s.Bound || *s.MarkedRequests != 0 || s.LargeWrites != 1 || s.Held != nil || s.Transaction != nil {
			t.Fatal("a near miss bound the gate", s)
		}
		request := gateFrame(t, msgr.OSDOpMessage, 7, 4, append(append([]byte("journalappend"), marker...), 9))
		gateWrite(t, conn, peer, request)
		select {
		case <-gate.Bound():
		default:
			t.Fatal("the marked request did not bind")
		}
		s := gate.State()
		if !s.Bound || s.BoundConnection != 1 || s.BoundWriteBytes != len(request) || *s.Transaction != 7 || *s.Sequence != 4 || *s.MarkedRequests != 1 || s.WithheldBytesSeen != nil {
			t.Fatal("the binding differs from the marked request", s)
		}

		banner := msgr.Banner{Supported: msgr.Revision1, Required: msgr.Revision1}.Encode()
		passed := bytes.Join([][]byte{banner,
			gateFrame(t, msgr.OSDOpReplyMessage, 2, 1, []byte("another transaction")),
			gateFrame(t, msgr.OSDOpMessage, 7, 2, []byte("bound transaction, another type"))}, nil)
		held := gateFrame(t, msgr.OSDOpReplyMessage, 7, 3, nil)
		trailing := gateFrame(t, msgr.OSDOpReplyMessage, 8, 4, nil)
		go func() {
			// Three-byte fragments: frame boundaries never align with reads.
			stream := bytes.Join([][]byte{passed, held, trailing}, nil)
			for len(stream) > 0 {
				n := min(3, len(stream))
				if _, err := peer.Write(stream[:n]); err != nil {
					return
				}
				stream = stream[n:]
			}
		}()
		if got := gateRead(t, conn, len(passed), buffer); !bytes.Equal(got, passed) {
			t.Fatal("the gate changed or reordered passed bytes")
		}
		// The bound transaction on another connection of the client passes.
		foreign := append(bytes.Clone(banner), gateFrame(t, msgr.OSDOpReplyMessage, 7, 1, nil)...)
		go func() { _, _ = otherPeer.Write(foreign) }()
		if got := gateRead(t, other, len(foreign), buffer); !bytes.Equal(got, foreign) {
			t.Fatal("the gate withheld the bound transaction on another connection")
		}
		select {
		case <-gate.Held():
			t.Fatal("the hold was reported before the reader reached the reply")
		default:
		}
		gateParked(t, conn, gate.CloseHeld)
		<-gate.Held()
		s = gate.State()
		if s.Held == nil || *s.Held != (OSDHeldReply{1, msgr.OSDOpReplyMessage, 7, 3, len(held)}) || s.Error != "" {
			t.Fatal("the held frame differs", s.Held, s.Error)
		}
		// A repeated marked request is counted with its connection and never
		// moves the binding.
		gateWrite(t, other, otherPeer, gateFrame(t, msgr.OSDOpMessage, 9, 5, marker))
		s = gate.State()
		if *s.MarkedRequests != 2 || len(s.MarkedConnections) != 2 || s.MarkedConnections[1] != 2 || *s.Transaction != 7 || s.BoundConnection != 1 || s.Connections != 2 {
			t.Fatal("a repeated request was not counted or rebound the gate", s)
		}
	}
}

func TestOSDReplyGateForwardsAnUndecodableStream(t *testing.T) {
	gate := NewOSDReplyGate(true, 4096)
	conn, peer := gatedOSDPipe(t, gate)
	marker := bytes.Repeat([]byte{7}, 16)
	if err := gate.Arm(marker); err != nil {
		t.Fatal(err)
	}
	gateWrite(t, conn, peer, gateFrame(t, msgr.OSDOpMessage, 5, 1, marker))
	garbage := []byte("not a msgr2 stream, forwarded unchanged")
	go func() { _, _ = peer.Write(garbage) }()
	if got := gateRead(t, conn, len(garbage), 5); !bytes.Equal(got, garbage) {
		t.Fatal("an undecodable stream was changed")
	}
	if s := gate.State(); s.Error == "" || s.Held != nil {
		t.Fatal("an undecodable stream was not reported", s)
	}
	if NewOSDReplyGate(true, 0).Arm(marker) == nil || NewOSDReplyGate(false, 4096).CloseHeld() == nil {
		t.Fatal("an unusable gate was accepted")
	}
}

func TestOSDReplyGateUndecodedBindsTheFirstLargeWrite(t *testing.T) {
	gate := NewOSDReplyGate(false, 4096)
	conn, peer := gatedOSDPipe(t, gate)
	other, otherPeer := gatedOSDPipe(t, gate)
	large := bytes.Repeat([]byte{3}, 4096)
	// A large write before Arm is counted and binds nothing.
	gateWrite(t, other, otherPeer, large)
	if s := gate.State(); s.Bound || s.LargeWrites != 1 || s.MarkedRequests != nil || s.Transaction != nil || s.Held != nil || *s.WithheldBytesSeen {
		t.Fatal("an unarmed undecoded gate bound, or reports what it cannot observe", s)
	}
	if err := gate.Arm(nil); err != nil {
		t.Fatal(err)
	}
	gateWrite(t, conn, peer, large[:4095])
	early := []byte("received before the padded write")
	go func() { _, _ = peer.Write(early) }()
	if got := gateRead(t, conn, len(early), 64); !bytes.Equal(got, early) {
		t.Fatal("bytes before the binding were changed")
	}
	if s := gate.State(); s.Bound || s.LargeWrites != 1 {
		t.Fatal("a write below the threshold bound the gate", s)
	}
	gateWrite(t, conn, peer, large)
	// A later large write on another connection is counted, never rebinds.
	gateWrite(t, other, otherPeer, large)
	s := gate.State()
	if !s.Bound || s.BoundConnection != 1 || s.BoundWriteBytes != 4096 || s.LargeWrites != 3 || *s.WithheldBytesSeen {
		t.Fatal("the undecoded binding differs", s)
	}
	passing := []byte("another connection keeps delivering")
	go func() { _, _ = otherPeer.Write(passing) }()
	if got := gateRead(t, other, len(passing), 64); !bytes.Equal(got, passing) {
		t.Fatal("the gate withheld another connection")
	}
	go func() { _, _ = peer.Write([]byte("anything after the binding")) }()
	gateParked(t, conn, gate.CloseHeld)
	<-gate.Held()
	if s := gate.State(); !*s.WithheldBytesSeen || s.Held != nil {
		t.Fatal("withheld bytes were not reported, or were attributed to a frame", s)
	}
}
