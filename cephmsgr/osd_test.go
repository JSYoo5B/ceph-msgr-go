package cephmsgr

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

type osdTestPeer struct {
	conn     net.Conn
	writer   *msgr.Writer
	requests chan msgr.MessageData
	done     chan struct{}
	seq      uint64
}

func (p *osdTestPeer) serve() {
	defer close(p.done)
	defer p.conn.Close()
	r, w, err := mockAuthenticate(p.conn, peerConfig{role: 4})
	if err != nil {
		return
	}
	p.writer = w
	for {
		f, err := r.Read()
		if err != nil {
			return
		}
		if f.Tag != msgr.Message {
			continue
		}
		m, err := msgr.DecodeMessage(f)
		if err != nil {
			return
		}
		select {
		case p.requests <- m:
		case <-p.done:
			return
		}
	}
}

func (p *osdTestPeer) next(t *testing.T) msgr.MessageData {
	t.Helper()
	select {
	case m := <-p.requests:
		return m
	case <-time.After(3 * time.Second):
		t.Fatal("OSD request did not arrive")
	}
	return msgr.MessageData{}
}

func (p *osdTestPeer) reply(t *testing.T, request msgr.MessageData) {
	t.Helper()
	// Synthetic reply for lifetime/correlation tests, not an interoperability
	// oracle. Independent native object results are checked by integration tests.
	e := wire.Encoder{}
	e.String("object")
	e.U8(1)
	e.U64(1)
	e.U32(0)
	e.U32(^uint32(0))
	e.U64(4)
	e.U32(0)
	e.U64(0)
	e.U32(0)
	e.U32(5)
	e.U32(1)
	e.U16(uint16(OSDRead))
	e.U32(0)
	e.U64(0)
	e.U64(1)
	e.U64(0)
	e.U32(0)
	e.U32(1)
	e.U32(0)
	e.U32(0)
	e.U64(0)
	e.U32(0)
	e.U64(0)
	locator := wire.Encoder{}
	locator.U64(^uint64(0))
	locator.U32(^uint32(0))
	locator.String("")
	locator.String("")
	locator.U64(^uint64(0))
	redirect := wire.Encoder{}
	redirect.Struct(6, 3, locator.Data)
	redirect.String("")
	redirect.Bytes(nil)
	e.Struct(1, 1, redirect.Data)
	e.U64(0)
	e.U64(0)
	e.U64(0)
	p.seq++
	m := msgr.MessageData{Type: msgr.OSDOpReplyMessage, Version: 6, CompatVersion: 2,
		Transaction: request.Transaction, Sequence: p.seq, Front: e.Data, Data: []byte{42}}
	if err := p.writer.Write(m.Frame()); err != nil {
		t.Fatal(err)
	}
}

func osdTestClient(t *testing.T) (*Client, <-chan *osdTestPeer, *atomic.Uint32) {
	t.Helper()
	options := mockOptions(t, 20, [16]byte{1})
	options.EnableOSD, options.MaxOSDConnections = true, 1
	peers := make(chan *osdTestPeer, 4)
	var dials atomic.Uint32
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		client, server := net.Pipe()
		if endpoint == "192.0.2.1:6804" {
			dials.Add(1)
			peer := &osdTestPeer{conn: server, requests: make(chan msgr.MessageData, 8), done: make(chan struct{})}
			peers <- peer
			go peer.serve()
		} else {
			go mockDaemon(server, peerConfig{role: 1, release: 20, fsid: [16]byte{1}, services: cephx.ServiceAuth | cephx.ServiceMgr | cephx.ServiceOSD})
		}
		return client, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, peers, &dials
}

func osdTestRequest() OSDRequest {
	return OSDRequest{MapEpoch: 5, Object: OSDObject{Pool: 1, Name: "object"}, Operations: []OSDOperation{{Code: OSDRead, Length: 1}}}
}

func TestOSDSharedHandleTargetLimitsAndSetupContext(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	cancel() // The setup context does not own the established connection.
	other, err := c.OpenOSD(context.Background(), OSDTarget{Address: "v2:192.0.2.1:6804/0"})
	if err != nil || other != o || dials.Load() != 1 {
		t.Fatal("OSD sharing", err, dials.Load())
	}
	if _, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6805"}); !errors.Is(err, ErrOSDTargetChanged) {
		t.Fatal(err)
	}
	if _, err := c.OpenOSD(context.Background(), OSDTarget{ID: 1, Address: "192.0.2.1:6804"}); !errors.Is(err, ErrOSDConnectionLimit) {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		reply, err := o.Request(context.Background(), osdTestRequest())
		if err == nil && (reply.Transaction != 1 || reply.GlobalID != 42 || reply.RawData[0] != 42) {
			err = errors.New("OSD reply metadata lost")
		}
		result <- err
	}()
	first := peer.next(t)
	peer.reply(t, first)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	<-peer.done
	o, err = c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer = <-peers
	go func() { _, err := o.Request(context.Background(), osdTestRequest()); result <- err }()
	second := peer.next(t)
	if second.Transaction <= first.Transaction {
		t.Fatal("OSD request ID restarted across connection", first.Transaction, second.Transaction)
	}
	peer.reply(t, second)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	c.Close()
	if _, err := o.Request(context.Background(), osdTestRequest()); !errors.Is(err, ErrClosed) {
		t.Fatal("parent close", err)
	}
}

func TestOSDCancelSentRequestAndConsumeLateReply(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := o.Request(ctx, osdTestRequest()); result <- err }()
	first := peer.next(t)
	cancel()
	err = <-result
	var unknown *OutcomeUnknownError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &unknown) {
		t.Fatal("sent cancellation lost uncertainty", err)
	}
	peer.reply(t, first)
	go func() { _, err := o.Request(context.Background(), osdTestRequest()); result <- err }()
	second := peer.next(t)
	peer.reply(t, second)
	if err := <-result; err != nil {
		t.Fatal("late reply damaged other call", err)
	}
}

func TestOSDDisabledAndInvalidTargetsDoNotDial(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	c.options.EnableOSD = false
	if _, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"}); !errors.Is(err, ErrOSDDisabled) {
		t.Fatal(err)
	}
	c.options.EnableOSD = true
	for _, target := range []OSDTarget{{ID: -1, Address: "192.0.2.1:6804"}, {Address: "v1:192.0.2.1:6804"}, {Address: "osd.example:6804"}} {
		if _, err := c.OpenOSD(context.Background(), target); err == nil {
			t.Fatal("invalid OSD target", target)
		}
	}
	if dials.Load() != 0 || len(peers) != 0 {
		t.Fatal("preflight contacted OSD")
	}
}
