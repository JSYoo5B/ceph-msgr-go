package integration_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// helloSegment is a HELLO payload as ProtocolV2 writes it: the connector's
// entity type and the target address.
func helloSegment(entity uint8, target msgr.Address) []byte {
	e := wire.Encoder{}
	e.U8(entity)
	target.Encode(&e)
	return e.Data
}

// connectHello plays a connector against the observer: banners, then the
// given frame through the Go frame writer (nil sends no frame and closes),
// and waits until the observer closes the connection, by which time its
// observation is queued.
func connectHello(t *testing.T, endpoint string, frame *msgr.Frame) {
	t.Helper()
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	local := msgr.Banner{Supported: msgr.Revision1 | msgr.Compression, Required: msgr.Revision1}
	if err := msgr.WriteFull(conn, local.Encode()); err != nil {
		t.Fatal(err)
	}
	if _, err := msgr.ReadBanner(conn, local); err != nil {
		t.Fatal(err)
	}
	if frame == nil {
		return
	}
	if err := msgr.NewWriter(conn, 4096).Write(*frame); err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, conn)
}

func helloFrame(entity uint8, target msgr.Address) *msgr.Frame {
	return &msgr.Frame{Tag: msgr.Hello, Segments: [][]byte{helloSegment(entity, target)}}
}

// startHelloObserver runs observeHellos on a loopback listener.
func startHelloObserver(t *testing.T) (string, chan connectorHello) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hellos, done := make(chan connectorHello, 64), make(chan struct{})
	go observeHellos(listener, hellos, done)
	t.Cleanup(func() { listener.Close(); close(done) })
	return listener.Addr().String(), hellos
}

var helloTarget = msgr.Address{Type: 2, Nonce: 1234567, Endpoint: netip.MustParseAddrPort("192.0.2.40:6800")}

// TestAwaitOSDHelloPassesOtherConnectorsFirst: a MON HELLO to the right
// target, then a connector that closes before its HELLO, then the OSD's;
// the wait returns the OSD and passes the other two.
func TestAwaitOSDHelloPassesOtherConnectorsFirst(t *testing.T) {
	endpoint, hellos := startHelloObserver(t)
	connectHello(t, endpoint, helloFrame(1, helloTarget))
	connectHello(t, endpoint, nil)
	connectHello(t, endpoint, helloFrame(4, helloTarget))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	osd, passed, err := awaitOSDHello(ctx, hellos, helloTarget)
	if err != nil || osd.Entity != 4 || osd.Target != helloTarget {
		t.Fatal("OSD HELLO", osd, err)
	}
	if len(passed) != 2 || passed[0].Entity != 1 || passed[0].Target != helloTarget || passed[0].Err != nil ||
		passed[1].HasEntity || !errors.Is(passed[1].Err, io.EOF) && !errors.Is(passed[1].Err, io.ErrUnexpectedEOF) {
		t.Fatal("passed connectors", passed)
	}
}

// TestAwaitOSDHelloWithoutAnOSD: only MON connectors arrive; the wait ends
// at its bound and names what it passed.
func TestAwaitOSDHelloWithoutAnOSD(t *testing.T) {
	endpoint, hellos := startHelloObserver(t)
	connectHello(t, endpoint, helloFrame(1, helloTarget))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, passed, err := awaitOSDHello(ctx, hellos, helloTarget)
	if !errors.Is(err, context.DeadlineExceeded) || len(passed) != 1 || passed[0].Entity != 1 || !strings.Contains(err.Error(), passed[0].Remote) {
		t.Fatal("wait without an OSD", passed, err)
	}
}

// TestAwaitOSDHelloRefusesOSDAndMalformedConnectors: an OSD HELLO with
// another target or nonce, an OSD HELLO whose address does not decode, a
// first frame that is not a HELLO and an unknown entity type each end the
// wait with an error naming that connector's remote endpoint, even after a
// MON was passed.
func TestAwaitOSDHelloRefusesOSDAndMalformedConnectors(t *testing.T) {
	otherNonce := helloTarget
	otherNonce.Nonce++
	otherPort := helloTarget
	otherPort.Endpoint = netip.MustParseAddrPort("192.0.2.40:6801")
	for name, frame := range map[string]*msgr.Frame{
		"wrong nonce":     helloFrame(4, otherNonce),
		"wrong endpoint":  helloFrame(4, otherPort),
		"cut OSD address": {Tag: msgr.Hello, Segments: [][]byte{helloSegment(4, helloTarget)[:9]}},
		"not a HELLO":     {Tag: msgr.AuthRequest, Segments: [][]byte{{1, 2, 3}}},
		"unknown entity":  helloFrame(3, helloTarget),
	} {
		t.Run(name, func(t *testing.T) {
			endpoint, hellos := startHelloObserver(t)
			connectHello(t, endpoint, helloFrame(1, helloTarget))
			connectHello(t, endpoint, frame)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, passed, err := awaitOSDHello(ctx, hellos, helloTarget)
			if err == nil || errors.Is(err, context.DeadlineExceeded) || got.Remote == "" || !strings.Contains(err.Error(), got.Remote) ||
				len(passed) != 1 || passed[0].Entity != 1 {
				t.Fatal("refusal", got, passed, err)
			}
		})
	}
}

// TestObservedHellosKeepEntityAndTargetTogether: many connectors at once,
// each with its own entity type and nonce; every observation carries the
// entity and target of one and the same HELLO.
func TestObservedHellosKeepEntityAndTargetTogether(t *testing.T) {
	endpoint, hellos := startHelloObserver(t)
	const n = 32
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			target := helloTarget
			target.Nonce = uint32(1000 + i)
			connectHello(t, endpoint, helloFrame([]uint8{1, 2, 8, 16}[i%4], target))
		}()
	}
	wg.Wait()
	for range n {
		h := <-hellos
		i := int(h.Target.Nonce) - 1000
		if h.Err != nil || i < 0 || i >= n || h.Entity != []uint8{1, 2, 8, 16}[i%4] {
			t.Fatal("observation mixes HELLOs", h)
		}
	}
}
