package cephmsgr

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// These are deterministic synthetic wire captures, not native lock tests.
// Client.cc:2131 and 2277 exclude only SETFILELOCK from oldest_tid. The MDS
// likewise excludes it from journaled completed requests (Server.cc:2365).
func filelockOldestSubmit(t *testing.T, ctx context.Context, s *MDSSession, p *mdsTestPeer, request MDSRequest) (*MDSRequestCall, mds.ClientRequest) {
	t.Helper()
	call, err := s.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	return call, filelockOldestCapture(t, p, call)
}

func filelockOldestCapture(t *testing.T, p *mdsTestPeer, call *MDSRequestCall) mds.ClientRequest {
	t.Helper()
	message := p.next(t)
	request, err := mds.DecodeClientRequest(message, 0)
	if err != nil || message.Transaction != call.Transaction() {
		t.Fatal("captured request has wrong wire body or TID", message.Transaction, err)
	}
	// Check the independently located packed-head field as well as decoding.
	if len(message.Front) < 10 || binary.LittleEndian.Uint64(message.Front[2:10]) != request.OldestClientTID {
		t.Fatal("decoded oldest TID differs from its packed wire field")
	}
	return request
}

func filelockOldestComplete(t *testing.T, ctx context.Context, p *mdsTestPeer, call *MDSRequestCall) {
	t.Helper()
	p.send(t, cephfsReply(call.op, call.Transaction(), 0, true, nil))
	if _, err := call.WaitSafe(ctx); err != nil {
		t.Fatal(err)
	}
}

func filelockOldestWant(t *testing.T, label string, got mds.ClientRequest, want uint64) {
	t.Helper()
	if got.OldestClientTID != want {
		t.Errorf("%s: wire oldest_client_tid=%d, want %d (op %#x)", label, got.OldestClientTID, want, got.Op)
	}
}

func TestMDSFilelockOldestSkipsOnlySetFilelock(t *testing.T) {
	_, _, s, p, ctx := cephfsOpen(t)
	lock1, request := filelockOldestSubmit(t, ctx, s, p, MDSRequest{Op: MDSOpSetFileLock})
	filelockOldestWant(t, "lock alone", request, 0)
	ordinary1, request := filelockOldestSubmit(t, ctx, s, p, MDSRequest{Op: MDSOpSetxattr})
	filelockOldestWant(t, "ordinary after older lock", request, ordinary1.Transaction())
	lock2, request := filelockOldestSubmit(t, ctx, s, p, MDSRequest{Op: MDSOpSetFileLock})
	filelockOldestWant(t, "lock while ordinary pending", request, ordinary1.Transaction())
	ordinary2, request := filelockOldestSubmit(t, ctx, s, p, MDSRequest{Op: MDSOpGetattr})
	filelockOldestWant(t, "two ordinary pending", request, ordinary1.Transaction())
	filelockOldestComplete(t, ctx, p, ordinary1)
	ordinary3, request := filelockOldestSubmit(t, ctx, s, p, MDSRequest{Op: MDSOpGetFileLock})
	filelockOldestWant(t, "GETFILELOCK remains ordinary", request, ordinary2.Transaction())
	filelockOldestComplete(t, ctx, p, ordinary2)
	filelockOldestComplete(t, ctx, p, ordinary3)
	// A read-only operation still participates, including WANT_DENTRY. The
	// product's existing refusal of caller ASYNC is outside this correction.
	ordinary4, request := filelockOldestSubmit(t, ctx, s, p, MDSRequest{Op: MDSOpGetattr, Flags: MDSRequestFlagWantDentry})
	filelockOldestWant(t, "ordinary after completed requests", request, ordinary4.Transaction())
	filelockOldestComplete(t, ctx, p, ordinary4)
	filelockOldestComplete(t, ctx, p, lock1)
	lock3, request := filelockOldestSubmit(t, ctx, s, p, MDSRequest{Op: MDSOpSetFileLock})
	filelockOldestWant(t, "only locks remain", request, 0)
	filelockOldestComplete(t, ctx, p, lock2)
	filelockOldestComplete(t, ctx, p, lock3)
}

func TestMDSFilelockOldestForwardedOrdinarySkipsDestinationLock(t *testing.T) {
	c, peers, ctx := forwardBindingClient(t)
	source, p0 := openForwardBindingSession(t, c, peers, ctx, 7, 0, "17")
	destination, p1 := openForwardBindingSession(t, c, peers, ctx, 7, 1, "0")
	lock, _ := filelockOldestSubmit(t, ctx, destination, p1, MDSRequest{Op: MDSOpSetFileLock})
	call, original := filelockOldestSubmit(t, ctx, source, p0, MDSRequest{Op: MDSOpGetattr, Stamp: MDSTime{Seconds: 7, Nanoseconds: 11}})
	f := wire.Encoder{}
	f.U32(1)
	f.U32(1)
	f.U8(1)
	p0.send(t, msgr.MessageData{Type: mds.ClientRequestForwardMessage, Version: 1, CompatVersion: 1, Transaction: call.Transaction(), Front: f.Data})
	if _, err := call.Wait(ctx); err == nil {
		t.Fatal("source did not forward the request")
	}
	resent, err := destination.ResubmitForwarded(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	request := filelockOldestCapture(t, p1, resent)
	filelockOldestWant(t, "forwarded ordinary after destination lock", request, call.Transaction())
	if request.Stamp != original.Stamp || request.Op != original.Op || resent.Transaction() != call.Transaction() || request.Forward != 1 || request.Retry != 0 {
		t.Fatal("oldest calculation changed forwarded request identity or attempts", request)
	}
	filelockOldestComplete(t, ctx, p1, resent)
	filelockOldestComplete(t, ctx, p1, lock)
}

func TestMDSFilelockOldestExplicitReconnectResubmission(t *testing.T) {
	c, peers, ctx := cephfsTestClient(t)
	previous, p0 := openForwardBindingSession(t, c, peers, ctx, 7, 0, "17")
	lock, _ := filelockOldestSubmit(t, ctx, previous, p0, MDSRequest{Op: MDSOpSetFileLock})
	ordinary, original := filelockOldestSubmit(t, ctx, previous, p0, MDSRequest{Op: MDSOpSetxattr, Stamp: MDSTime{Seconds: 17, Nanoseconds: 23}})
	p0.send(t, cephfsReply(ordinary.op, ordinary.Transaction(), 0, false, nil))
	if reply, err := ordinary.Wait(ctx); err != nil || reply.Safe {
		t.Fatal("could not establish the unsafe ordinary request", reply, err)
	}
	p0.conn.Close()
	select {
	case <-previous.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-peers: // the explicit RESET answer to Messenger resume
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := previous.conn.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/0"})
	if err != nil {
		t.Fatal(err)
	}
	var peer *mdsTestPeer
	select {
	case peer = <-peers:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	type result struct {
		s     *MDSSession
		calls []*MDSRequestCall
		err   error
	}
	done := make(chan result, 1)
	go func() {
		s, calls, err := m.ReconnectSession(ctx, previous, MDSReconnectState{Resubmit: []*MDSRequestCall{lock, ordinary}}, MDSSessionOptions{RenewInterval: time.Hour})
		done <- result{s, calls, err}
	}()
	lockFrame := peer.next(t)
	lockWire, err := mds.DecodeClientRequest(lockFrame, 0)
	if err != nil || lockFrame.Transaction != lock.Transaction() {
		t.Fatal("reconnected lock changed its TID or wire format", lockFrame, err)
	}
	filelockOldestWant(t, "first reconnect lock", lockWire, 0)
	ordinaryFrame := peer.next(t)
	ordinaryWire, err := mds.DecodeClientRequest(ordinaryFrame, 0)
	if err != nil || ordinaryFrame.Transaction != ordinary.Transaction() {
		t.Fatal("replayed ordinary changed its TID or wire format", ordinaryFrame, err)
	}
	filelockOldestWant(t, "replayed ordinary after reconnect lock", ordinaryWire, ordinary.Transaction())
	if lockWire.Flags&mds.RequestFlagReplay != 0 || lockWire.Retry != 1 || ordinaryWire.Flags&mds.RequestFlagReplay == 0 || ordinaryWire.Retry != 1 || ordinaryWire.Stamp != original.Stamp {
		t.Fatal("oldest exception changed explicit replay policy, attempts or stamp", lockWire, ordinaryWire)
	}
	if reconnect := peer.next(t); reconnect.Type != mds.ClientReconnectMessage {
		t.Fatal("explicit resubmissions did not precede reconnect", reconnect.Type)
	}
	peer.send(t, mdsSessionV7(mds.SessionOpen, 0, nil, 0, false))
	var got result
	select {
	case got = <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if got.err != nil || got.s == nil || len(got.calls) != 2 {
		t.Fatal("explicit reconnect did not return both calls", got.err)
	}
	mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
	for _, call := range got.calls {
		filelockOldestComplete(t, ctx, peer, call)
	}
}
