package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/mds"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Synthetic CephFS peers check request correlation, push ordering and
// lifetime policy with the Go codecs. They are not native interoperability.

var allMDSCephFSFeatures = []byte{0xff, 0xff, 0xff, 0, 0, 0, 0, 0}

// resetResume answers every SESSION_RECONNECT with a full SESSION_RESET.
func resetResume(msgr.Frame) (msgr.Tag, []byte) { return msgr.SessionReset, []byte{1} }

func cephfsTestClient(t *testing.T) (*Client, <-chan *mdsTestPeer, context.Context) {
	t.Helper()
	return cephfsTestClientWith(t, func(peer *mdsTestPeer, dial int) {
		if dial > 1 {
			peer.reconnect = resetResume
		}
	})
}

// cephfsTestClientWith lets a test configure each dialed MDS peer (dial
// counts from 1); later dials are Messenger resume attempts.
func cephfsTestClientWith(t *testing.T, configure func(*mdsTestPeer, int)) (*Client, <-chan *mdsTestPeer, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	options := mockOptions(t, 20, [16]byte{1})
	options.EnableMDS, options.EnableCephFS, options.MaxMDSConnections, options.MaxInFlight = true, true, 2, 4
	peers := make(chan *mdsTestPeer, 16)
	var dials atomic.Int32
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		clientPipe, server := net.Pipe()
		client := &mdsTestClientConn{Conn: clientPipe}
		if endpoint == "192.0.2.1:6805" {
			peer := &mdsTestPeer{conn: server, client: client, requests: make(chan msgr.MessageData, 64), done: make(chan struct{}), errors: make(chan error, 1), cephfs: true}
			peer.sessions.Store(true)
			configure(peer, int(dials.Add(1)))
			peers <- peer
			go peer.serve()
		} else {
			go mdsMockMonitor(server)
		}
		return client, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, peers, ctx
}

func cephfsOpen(t *testing.T) (*Client, *MDSConnection, *MDSSession, *mdsTestPeer, context.Context) {
	t.Helper()
	c, peers, ctx := cephfsTestClient(t)
	m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/17"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	pending := mdsStartSession(m, ctx, MDSSessionOptions{RenewInterval: time.Hour})
	open := peer.next(t)
	got, err := mds.DecodeClientSession(msgr.MessageData{Type: open.Type, Version: 1, CompatVersion: 1, Front: open.Front[:mds.SessionHeadBytes]}, 0)
	_ = got
	if err != nil || !bytes.Contains(open.Front, append([]byte{8, 0, 0, 0}, mds.CephFSClientFeatures...)) {
		t.Fatal("CephFS REQUEST_OPEN did not declare the CephFS feature set", err)
	}
	reply := mdsSessionV7(mds.SessionOpen, 0, nil, 0, false)
	peer.send(t, reply)
	result := <-pending
	if result.err != nil {
		t.Fatal(result.err)
	}
	mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
	return c, m, result.session, peer, ctx
}

func cephfsReply(op uint32, tid uint64, result int32, safe bool, trace []byte) msgr.MessageData {
	e := wire.Encoder{}
	e.U32(op)
	e.U32(uint32(result))
	e.U32(9)
	b := uint8(0)
	if safe {
		b = 1
	}
	e.U8(b)
	e.U8(0)
	if trace != nil {
		e.U8(1)
	} else {
		e.U8(0)
	}
	e.Bytes(trace)
	e.Bytes(nil)
	e.Bytes(nil)
	return msgr.MessageData{Type: mds.ClientReplyMessage, Version: 1, CompatVersion: 1, Transaction: tid, Front: e.Data}
}

func TestCephFSRequestReplyForwardAndSafety(t *testing.T) {
	c, _, s, peer, ctx := cephfsOpen(t)
	request := MDSRequest{Op: MDSOpLookup, Flags: MDSRequestFlagWantDentry, CallerUID: 1000, CallerGID: 1000, OwnerUID: 1000, OwnerGID: 1000,
		Path: MDSFilePath{Ino: 1, Path: "file"}, GIDs: []uint64{1000}, MDSMapEpoch: 9}
	call, err := s.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	sent := peer.next(t)
	wireRequest, err := mds.DecodeClientRequest(sent, 0)
	if err != nil || sent.Transaction != call.Transaction() || wireRequest.Op != MDSOpLookup || wireRequest.OldestClientTID != call.Transaction() || wireRequest.Path.Path != "file" || wireRequest.Stamp == (MDSTime{}) {
		t.Fatal("request wire", wireRequest, err)
	}
	peer.send(t, cephfsReply(MDSOpLookup, call.Transaction(), -2, true, nil))
	reply, err := call.Wait(ctx)
	if err != nil || reply.Result != -2 || !reply.Safe || reply.Transaction != call.Transaction() {
		t.Fatal(reply, err)
	}
	if safe, err := call.WaitSafe(ctx); err != nil || safe.Result != -2 {
		t.Fatal(safe, err)
	}
	// Unsafe then safe on a write; the second request reports the older TID.
	mkdir, _ := s.Submit(ctx, MDSRequest{Op: MDSOpMkdir, Path: MDSFilePath{Ino: 1, Path: "d"}})
	sentMkdir, _ := mds.DecodeClientRequest(peer.next(t), 0)
	rename, _ := s.Submit(ctx, MDSRequest{Op: MDSOpRename, Path: MDSFilePath{Ino: 1, Path: "a"}, Path2: MDSFilePath{Ino: 1, Path: "b"}})
	sentRename, _ := mds.DecodeClientRequest(peer.next(t), 0)
	if sentMkdir.OldestClientTID != mkdir.Transaction() || sentRename.OldestClientTID != mkdir.Transaction() {
		t.Fatal("oldest client TID", sentMkdir.OldestClientTID, sentRename.OldestClientTID)
	}
	peer.send(t, cephfsReply(MDSOpMkdir, mkdir.Transaction(), 0, false, nil))
	if first, err := mkdir.Wait(ctx); err != nil || first.Safe {
		t.Fatal("unsafe first reply", first, err)
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	if _, err := mkdir.WaitSafe(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("safe reply before journal", err)
	}
	cancel()
	peer.send(t, cephfsReply(MDSOpMkdir, mkdir.Transaction(), 0, true, nil))
	if safe, err := mkdir.WaitSafe(ctx); err != nil || !safe.Safe {
		t.Fatal(safe, err)
	}
	f := wire.Encoder{}
	f.U32(1)
	f.U32(1)
	f.U8(1)
	peer.send(t, msgr.MessageData{Type: mds.ClientRequestForwardMessage, Version: 1, CompatVersion: 1, Transaction: rename.Transaction(), Front: f.Data})
	var forwarded *MDSRequestForwardedError
	if _, err := rename.Wait(ctx); !errors.As(err, &forwarded) || forwarded.DestinationMDS != 1 || !forwarded.MustResend {
		t.Fatal("forward", err)
	}
	if _, err := s.Submit(ctx, MDSRequest{Op: MDSOpGetattr, Flags: 1}); err == nil {
		t.Fatal("replay flag admitted")
	}
	// Session loss: a pending unsafe write keeps its first reply but the safe
	// outcome becomes unknown; the request is never replayed.
	pending, _ := s.Submit(ctx, MDSRequest{Op: MDSOpUnlink, Path: MDSFilePath{Ino: 1, Path: "x"}})
	peer.next(t)
	peer.send(t, cephfsReply(MDSOpUnlink, pending.Transaction(), 0, false, nil))
	if _, err := pending.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	other, _ := s.Submit(ctx, MDSRequest{Op: MDSOpGetattr, Path: MDSFilePath{Ino: 1}})
	peer.next(t)
	peer.conn.Close()
	var unknown *OutcomeUnknownError
	if _, err := pending.WaitSafe(ctx); !errors.As(err, &unknown) {
		t.Fatal("safe outcome after loss", err)
	}
	if _, err := other.Wait(ctx); !errors.As(err, &unknown) {
		t.Fatal("pending outcome after loss", err)
	}
	if len(c.calls) != 0 {
		t.Fatal("request slots leaked", len(c.calls))
	}
}

func TestCephFSPushesCountedAndClientAnswers(t *testing.T) {
	_, _, s, peer, ctx := cephfsOpen(t)
	grant := mds.EncodeCaps(mds.Caps{Op: mds.CapOpGrant, Ino: 0x10000000001, CapID: 3, Seq: 1, Caps: 0x55, Xattrs: []byte{0, 0, 0, 0}, Files: -1, Subdirs: -1}, 0)
	peer.send(t, grant)
	peer.send(t, mds.EncodeLease(mds.Lease{Action: mds.LeaseRevoke, Mask: 1, Ino: 1, First: 2, Last: mds.NoSnap, Seq: 4, DentryName: "f"}))
	q := wire.Encoder{}
	q.U64(1)
	q.U64(0)
	q.U64(1)
	q.U64(1)
	q.U64(0)
	quota := wire.Encoder{}
	quota.U64(10)
	quota.U64(0)
	q.Struct(1, 1, quota.Data)
	peer.send(t, msgr.MessageData{Type: mds.ClientQuotaMessage, Version: 1, CompatVersion: 1, Front: q.Data})
	ack := mds.EncodeCaps(mds.Caps{Op: mds.CapOpFlushAck, Ino: 0x10000000001, CapID: 3, Dirty: 1, Files: -1, Subdirs: -1}, 77)
	peer.send(t, ack)
	for i, check := range []func(MDSSessionEvent) bool{
		func(e MDSSessionEvent) bool {
			return e.Caps != nil && e.Caps.Op == MDSCapOpGrant && len(e.Caps.Xattrs) == 4
		},
		func(e MDSSessionEvent) bool { return e.Lease != nil && e.Lease.DentryName == "f" },
		func(e MDSSessionEvent) bool { return e.Quota != nil && e.Quota.MaxBytes == 10 },
		func(e MDSSessionEvent) bool {
			return e.Caps != nil && e.Caps.Op == MDSCapOpFlushAck && e.Transaction == 77
		},
	} {
		event, err := s.Next(ctx)
		if err != nil || !check(event) {
			t.Fatal("push event", i, event, err)
		}
	}
	if s.Status().PushSequence != 4 {
		t.Fatal("push sequence", s.Status().PushSequence)
	}
	if err := s.SendCaps(ctx, MDSCaps{Op: MDSCapOpUpdate, Ino: 0x10000000001, CapID: 3, Caps: 0x15, Files: -1, Subdirs: -1}, 0); err != nil {
		t.Fatal(err)
	}
	if got, err := mds.DecodeCaps(peer.next(t), 0); err != nil || got.Op != MDSCapOpUpdate || got.Caps != 0x15 {
		t.Fatal(got, err)
	}
	if err := s.SendCaps(ctx, MDSCaps{Op: MDSCapOpGrant}, 0); err == nil {
		t.Fatal("server capability operation sent by client")
	}
	if err := s.ReleaseCaps(ctx, []MDSCapItem{{Ino: 1, CapID: 3, MigrateSeq: 0, IssueSeq: 1}}, 12); err != nil {
		t.Fatal(err)
	}
	if items, barrier, err := mds.DecodeCapRelease(peer.next(t), 0); err != nil || barrier != 12 || len(items) != 1 {
		t.Fatal(items, barrier, err)
	}
	if err := s.SendLease(ctx, MDSLease{Action: MDSLeaseRelease, Ino: 1, Seq: 4, DentryName: "f"}); err != nil {
		t.Fatal(err)
	}
	if lease, err := mds.DecodeLease(peer.next(t), 0); err != nil || lease.Action != MDSLeaseRelease {
		t.Fatal(lease, err)
	}
	// A push while closing raises the sequence; REQUEST_CLOSE is resent.
	closed := make(chan error, 1)
	go func() { closed <- s.Close(ctx) }()
	if request := mdsSessionNext(t, peer, mds.SessionRequestClose); request.Sequence != 4 {
		t.Fatal("close sequence", request)
	}
	mdsWaitState(t, s, MDSSessionClosing)
	peer.send(t, grant)
	if request := mdsSessionNext(t, peer, mds.SessionRequestClose); request.Sequence != 5 {
		t.Fatal("close resend sequence", request)
	}
	peer.send(t, mdsSessionReply(mds.SessionClose, 0))
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestCephFSProtocolViolationsAndFeatureGates(t *testing.T) {
	for name, messages := range map[string]func(uint64) []msgr.MessageData{
		"op mismatch": func(tid uint64) []msgr.MessageData {
			return []msgr.MessageData{cephfsReply(MDSOpMkdir, tid, 0, true, nil)}
		},
		"unknown future": func(tid uint64) []msgr.MessageData {
			return []msgr.MessageData{cephfsReply(MDSOpGetattr, tid+1000, 0, true, nil)}
		},
		"duplicate unsafe": func(tid uint64) []msgr.MessageData {
			return []msgr.MessageData{cephfsReply(MDSOpGetattr, tid, 0, false, nil), cephfsReply(MDSOpGetattr, tid, 0, false, nil)}
		},
		"forward after reply": func(tid uint64) []msgr.MessageData {
			f := wire.Encoder{}
			f.U32(1)
			f.U32(1)
			f.U8(1)
			return []msgr.MessageData{cephfsReply(MDSOpGetattr, tid, 0, false, nil), {Type: mds.ClientRequestForwardMessage, Version: 1, CompatVersion: 1, Transaction: tid, Front: f.Data}}
		},
		"client cap op": func(uint64) []msgr.MessageData {
			return []msgr.MessageData{mds.EncodeCaps(mds.Caps{Op: mds.CapOpUpdate, Files: -1, Subdirs: -1}, 0)}
		},
		"cap release from MDS": func(uint64) []msgr.MessageData { m, _ := mds.EncodeCapRelease(nil, 0); return []msgr.MessageData{m} },
		"client lease action": func(uint64) []msgr.MessageData {
			return []msgr.MessageData{mds.EncodeLease(mds.Lease{Action: mds.LeaseRelease})}
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, m, s, peer, ctx := cephfsOpen(t)
			call, err := s.Submit(ctx, MDSRequest{Op: MDSOpGetattr, Path: MDSFilePath{Ino: 1}})
			if err != nil {
				t.Fatal(err)
			}
			peer.next(t)
			for _, message := range messages(call.Transaction()) {
				peer.send(t, message)
			}
			<-m.Done()
			var unknown *OutcomeUnknownError
			if _, err := call.WaitSafe(ctx); !errors.As(err, &unknown) {
				t.Fatal(err)
			}
		})
	}
}

func TestCephFSFeatureGatesAndDataSegment(t *testing.T) {
	// A server lacking a CephFS Messenger feature is refused at handshake.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	options := mockOptions(t, 20, [16]byte{1})
	options.EnableMDS, options.EnableCephFS = true, true
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		clientPipe, server := net.Pipe()
		client := &mdsTestClientConn{Conn: clientPipe}
		if endpoint == "192.0.2.1:6805" {
			peer := &mdsTestPeer{conn: server, client: client, requests: make(chan msgr.MessageData, 64), done: make(chan struct{}), errors: make(chan error, 1), cephfs: true, serverFeatures: session.Features}
			go peer.serve()
		} else {
			go mdsMockMonitor(server)
		}
		return client, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/17"}); !errors.Is(err, msgr.ErrFeatures) {
		t.Fatal("MDS without CephFS Messenger features admitted", err)
	}
	options.EnableMDS = false
	if _, err := Dial(ctx, options); err == nil {
		t.Fatal("EnableCephFS without EnableMDS admitted")
	}

	// The session-only profile refuses CephFS operations locally.
	_, m, peer, ctx2, _ := mdsTestOpen(t)
	s := mdsOpenTestSession(t, ctx2, m, peer, time.Hour)
	if _, err := s.Submit(ctx2, MDSRequest{Op: MDSOpGetattr}); !errors.Is(err, ErrCephFSDisabled) {
		t.Fatal(err)
	}
	if err := s.SendCaps(ctx2, MDSCaps{Op: MDSCapOpUpdate}, 0); !errors.Is(err, ErrCephFSDisabled) {
		t.Fatal(err)
	}

	// An OPEN without the request codec features refuses requests; a request
	// data segment travels unchanged.
	c3, peers, ctx3 := cephfsTestClient(t)
	m3, err := c3.OpenMDS(ctx3, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/17"})
	if err != nil {
		t.Fatal(err)
	}
	peer3 := <-peers
	pending := mdsStartSession(m3, ctx3, MDSSessionOptions{RenewInterval: time.Hour})
	peer3.next(t)
	e := mdsSessionHead(mds.SessionOpen, 0)
	e.U32(0)
	e.Bytes([]byte{0xff, 0x01, 0, 0, 0, 0, 0, 0})
	metric := wire.Encoder{}
	metric.Bytes(nil)
	e.Struct(1, 1, metric.Data)
	e.U32(0)
	e.U32(0)
	e.U64(^uint64(0))
	peer3.send(t, msgr.MessageData{Type: mds.ClientSessionMessage, Version: 7, CompatVersion: 1, Front: e.Data})
	old := <-pending
	if old.err != nil {
		t.Fatal(old.err)
	}
	if _, err := old.session.Submit(ctx3, MDSRequest{Op: MDSOpGetattr}); !errors.Is(err, ErrMDSRequestFeatures) {
		t.Fatal("MDS without REPLY_ENCODING admitted a request", err)
	}
	_, _, s4, peer4, ctx4 := cephfsOpen(t)
	call, err := s4.Submit(ctx4, MDSRequest{Op: MDSOpSetxattr, Path: MDSFilePath{Ino: 1}, Path2: MDSFilePath{Path: "user.k"}, Data: []byte{0, 1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	sent := peer4.next(t)
	got, err := mds.DecodeClientRequest(sent, 0)
	if err != nil || !bytes.Equal(sent.Data, []byte{0, 1, 2}) || !bytes.Equal(got.Data, []byte{0, 1, 2}) || got.Path2.Path != "user.k" {
		t.Fatal("request data segment", got, err)
	}
	peer4.send(t, cephfsReply(MDSOpSetxattr, call.Transaction(), 0, true, nil))
	if _, err := call.WaitSafe(ctx4); err != nil {
		t.Fatal(err)
	}
}

func TestCephFSMessengerResumeKeepsSessionAndRequests(t *testing.T) {
	var reconnects []msgr.Frame
	c, peers, ctx := cephfsTestClientWith(t, func(peer *mdsTestPeer, dial int) {
		if dial == 2 {
			peer.reconnect = func(f msgr.Frame) (msgr.Tag, []byte) {
				reconnects = append(reconnects, f)
				// The MDS received REQUEST_OPEN and the first renewal (seq 1,2)
				// but not the request: it must be retransmitted.
				e := wire.Encoder{}
				e.U64(2)
				return msgr.SessionReconnectOK, e.Data
			}
		}
	})
	m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/17"})
	if err != nil {
		t.Fatal(err)
	}
	first := <-peers
	pending := mdsStartSession(m, ctx, MDSSessionOptions{RenewInterval: time.Hour})
	first.next(t)
	first.send(t, mdsSessionV7(mds.SessionOpen, 0, nil, 0, false))
	opened := <-pending
	if opened.err != nil {
		t.Fatal(opened.err)
	}
	s := opened.session
	mdsSessionNext(t, first, mds.SessionRequestRenewCaps)
	call, err := s.Submit(ctx, MDSRequest{Op: MDSOpGetattr, Path: MDSFilePath{Ino: 1}})
	if err != nil {
		t.Fatal(err)
	}
	sent := first.next(t)
	first.conn.Close()
	second := <-peers
	resent := second.next(t)
	if resent.Transaction != call.Transaction() || resent.Sequence != sent.Sequence || resent.Type != mds.ClientRequestMessage {
		t.Fatal("request not retransmitted with its original sequence", resent.Sequence, sent.Sequence)
	}
	if len(reconnects) != 1 {
		t.Fatal("reconnect attempts", len(reconnects))
	}
	d := wire.NewDecoder(reconnects[0].Segments[0])
	msgr.DecodeAddresses(d)
	clientCookie, serverCookie, _, connectSeq, inSeq := d.U64(), d.U64(), d.U64(), d.U64(), d.U64()
	if clientCookie == 0 || serverCookie != 0x1020304050607080 || connectSeq != 1 || inSeq != first.seq {
		t.Fatal("reconnect identity", clientCookie, serverCookie, connectSeq, inSeq, first.seq)
	}
	// The MDS continues its own gapless stream on the resumed transport.
	second.seq = first.seq
	second.send(t, cephfsReply(MDSOpGetattr, call.Transaction(), 0, true, nil))
	if reply, err := call.WaitSafe(ctx); err != nil || reply.Result != 0 {
		t.Fatal("request did not complete across the resume", reply, err)
	}
	if m.Resumes() != 1 || s.Status().State != MDSSessionOpen || m.session.Err() != nil {
		t.Fatal("resume did not preserve the session", m.Resumes(), s.Status().State, m.session.Err())
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close(ctx) }()
	mdsSessionNext(t, second, mds.SessionRequestClose)
	second.send(t, mdsSessionReply(mds.SessionClose, 0))
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestCephFSClientSharesOneAddressNonceForMDSAndOSD(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	options := mockOptions(t, 20, [16]byte{1})
	options.EnableMDS, options.EnableCephFS, options.EnableOSD, options.MaxMDSConnections = true, true, true, 4
	nonces := make(chan uint32, 8)
	capture := func(addresses []msgr.Address) {
		if len(addresses) == 1 {
			nonces <- addresses[0].Nonce
		} else {
			nonces <- 0
		}
	}
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		clientPipe, server := net.Pipe()
		switch endpoint {
		case "192.0.2.1:6805":
			peer := &mdsTestPeer{conn: server, client: &mdsTestClientConn{Conn: clientPipe}, requests: make(chan msgr.MessageData, 8), done: make(chan struct{}), errors: make(chan error, 1), cephfs: true}
			go func() {
				defer server.Close()
				r, _, err := mdsMockAuthenticate(server, peerConfig{role: 2, cephfs: true, clientAddresses: capture})
				if err == nil {
					r.Read()
				}
			}()
			return peer.client, nil
		case "192.0.2.1:6804":
			go func() {
				defer server.Close()
				r, _, err := mockAuthenticate(server, peerConfig{role: 4, clientAddresses: capture})
				if err == nil {
					r.Read()
				}
			}()
		default:
			go mdsMockMonitorServices(server, 42, cephx.ServiceAuth|cephx.ServiceMgr|cephx.ServiceOSD|cephx.ServiceMDS)
		}
		return clientPipe, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	shared := c.Snapshot().CephFSNonce
	if shared == 0 {
		t.Fatal("CephFS client has no shared nonce")
	}
	if _, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/17"}); err != nil {
		t.Fatal(err)
	}
	if got := <-nonces; got != shared {
		t.Fatal("MDS ClientIdent nonce", got, shared)
	}
	if _, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"}); err != nil {
		t.Fatal(err)
	}
	if got := <-nonces; got != shared {
		t.Fatal("OSD ClientIdent nonce", got, shared)
	}
	// The MDS registers stateful connections by this address instance: a
	// second handle (another filesystem label) at that address is refused.
	if _, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 8, Rank: 0, Address: "v2:192.0.2.1:6805/17"}); !errors.Is(err, ErrMDSAddressInUse) {
		t.Fatal("second CephFS handle at one MDS address", err)
	}
}

func TestCephFSFlushJournalAndArrivalOrder(t *testing.T) {
	c, _, s, peer, ctx := cephfsOpen(t)
	if got := c.MDSClientFeatures(); !bytes.Equal(got, mds.CephFSClientFeatures) || !bytes.Equal(MDSSessionClientFeatures(), mds.ClientFeatures) {
		t.Fatal("declared feature accessors", got)
	}
	if err := s.FlushJournal(ctx); err != nil {
		t.Fatal(err)
	}
	if flush := mdsSessionNext(t, peer, mds.SessionRequestFlushMDLog); flush.Sequence != 0 || flush.Version != 1 {
		t.Fatal("REQUEST_FLUSH_MDLOG", flush)
	}
	call, err := s.Submit(ctx, MDSRequest{Op: MDSOpGetattr, Path: MDSFilePath{Ino: 1}})
	if err != nil {
		t.Fatal(err)
	}
	peer.next(t)
	peer.send(t, mds.EncodeLease(mds.Lease{Action: mds.LeaseRevoke, Ino: 1, Seq: 1, DentryName: "a"}))
	peer.send(t, cephfsReply(MDSOpGetattr, call.Transaction(), 0, true, nil))
	peer.send(t, mdsSessionReply(mds.SessionRecallState, 0))
	lease, err := s.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := call.WaitSafe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	recall, err := s.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Lease == nil || !(lease.Arrival < reply.Arrival && reply.Arrival < recall.Arrival) || recall.Arrival != reply.Arrival+1 {
		t.Fatal("arrival order across pushes, replies and session events", lease.Arrival, reply.Arrival, recall.Arrival)
	}
}

func TestCephFSResubmitForwardedKeepsTIDOnDestinationRank(t *testing.T) {
	c, peers, ctx := cephfsTestClientWith(t, func(peer *mdsTestPeer, dial int) {
		if dial == 2 {
			peer.rank = 1
		}
	})
	open := func(target MDSTarget) (*MDSSession, *mdsTestPeer) {
		m, err := c.OpenMDS(ctx, target)
		if err != nil {
			t.Fatal(err)
		}
		peer := <-peers
		pending := mdsStartSession(m, ctx, MDSSessionOptions{RenewInterval: time.Hour})
		peer.next(t)
		peer.send(t, mdsSessionV7(mds.SessionOpen, 0, nil, 0, false))
		got := <-pending
		if got.err != nil {
			t.Fatal(got.err)
		}
		mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
		return got.session, peer
	}
	s0, p0 := open(MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/17"})
	s1, p1 := open(MDSTarget{FilesystemID: 7, Rank: 1, Address: "v2:192.0.2.1:6805/0"})
	call, err := s0.Submit(ctx, MDSRequest{Op: MDSOpLookup, Path: MDSFilePath{Ino: 1, Path: "pinned"}, Stamp: MDSTime{Seconds: 7}})
	if err != nil {
		t.Fatal(err)
	}
	p0.next(t)
	f := wire.Encoder{}
	f.U32(1)
	f.U32(1)
	f.U8(1)
	p0.send(t, msgr.MessageData{Type: mds.ClientRequestForwardMessage, Version: 1, CompatVersion: 1, Transaction: call.Transaction(), Front: f.Data})
	var forwarded *MDSRequestForwardedError
	if _, err := call.Wait(ctx); !errors.As(err, &forwarded) || forwarded.Arrival == 0 {
		t.Fatal("forward", err)
	}
	if _, err := s0.ResubmitForwarded(ctx, call); err == nil {
		t.Fatal("resubmitted on the forwarding session")
	}
	resent, err := s1.ResubmitForwarded(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	wireRequest, err := mds.DecodeClientRequest(p1.next(t), 0)
	if err != nil || resent.Transaction() != call.Transaction() || wireRequest.Forward != 1 || wireRequest.Stamp.Seconds != 7 || wireRequest.Path.Path != "pinned" {
		t.Fatal("forward resubmission", wireRequest, err)
	}
	if _, err := s1.ResubmitForwarded(ctx, call); err == nil {
		t.Fatal("forwarded call resubmitted twice")
	}
	p1.send(t, cephfsReply(MDSOpLookup, call.Transaction(), 0, true, nil))
	if reply, err := resent.WaitSafe(ctx); err != nil || reply.Transaction != call.Transaction() {
		t.Fatal(reply, err)
	}
}

func TestCephFSCompletedCreateConversion(t *testing.T) {
	for _, tc := range []struct {
		request MDSRequest
		op      uint32
		want    bool
	}{
		{MDSRequest{Op: MDSOpMkdir, Retry: 1, Flags: MDSRequestFlagWantDentry}, MDSOpLookup, true},
		{MDSRequest{Op: mds.OpMknod, Retry: 2}, MDSOpGetattr, true},
		{MDSRequest{Op: mds.OpSymlink, Retry: 1, Flags: MDSRequestFlagWantDentry}, MDSOpLookup, true},
		{MDSRequest{Op: MDSOpMkdir, Retry: 1, Flags: MDSRequestFlagWantDentry}, MDSOpGetattr, false},
		{MDSRequest{Op: MDSOpMkdir, Retry: 1}, MDSOpLookup, false},
		{MDSRequest{Op: MDSOpMkdir, Flags: MDSRequestFlagWantDentry}, MDSOpLookup, false},
		{MDSRequest{Op: MDSOpMkdir, Retry: 1, Flags: MDSRequestFlagWantDentry | mds.RequestFlagReplay}, MDSOpLookup, false},
		{MDSRequest{Op: MDSOpCreate, Retry: 1, Flags: MDSRequestFlagWantDentry}, MDSOpLookup, false},
		{MDSRequest{Op: MDSOpRename, Retry: 1, Flags: MDSRequestFlagWantDentry}, MDSOpLookup, false},
	} {
		if got := convertedCompletedReply(tc.request, tc.op); got != tc.want {
			t.Fatalf("op %#x retry %d flags %#x answered as %#x: %v", tc.request.Op, tc.request.Retry, tc.request.Flags, tc.op, got)
		}
	}
	_, _, s, peer, ctx := cephfsOpen(t)
	mkdir, err := s.Submit(ctx, MDSRequest{Op: MDSOpMkdir, Retry: 1, Flags: MDSRequestFlagWantDentry, Path: MDSFilePath{Ino: 1, Path: "d"}})
	if err != nil {
		t.Fatal(err)
	}
	peer.next(t)
	peer.send(t, cephfsReply(MDSOpLookup, mkdir.Transaction(), 0, true, nil))
	if reply, err := mkdir.WaitSafe(ctx); err != nil || reply.Op != MDSOpLookup || !reply.Safe {
		t.Fatal("converted completed MKDIR", reply, err)
	}
	fresh, err := s.Submit(ctx, MDSRequest{Op: MDSOpMkdir, Flags: MDSRequestFlagWantDentry, Path: MDSFilePath{Ino: 1, Path: "e"}})
	if err != nil {
		t.Fatal(err)
	}
	peer.next(t)
	peer.send(t, cephfsReply(MDSOpLookup, fresh.Transaction(), 0, true, nil))
	var unknown *OutcomeUnknownError
	if _, err := fresh.WaitSafe(ctx); !errors.As(err, &unknown) {
		t.Fatal("first-attempt MKDIR answered as LOOKUP accepted", err)
	}
}
