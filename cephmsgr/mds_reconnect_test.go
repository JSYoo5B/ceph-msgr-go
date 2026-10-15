package cephmsgr

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/mds"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// lostCephFSSession opens a CephFS session, then simulates an MDS restart:
// the transport dies and the resume attempt is answered with RESET.
func lostCephFSSession(t *testing.T, maxFrame uint32) (*Client, *MDSSession, <-chan *mdsTestPeer, context.Context) {
	t.Helper()
	c, peers, ctx := cephfsTestClient(t)
	if maxFrame != 0 {
		c.options.MaxFrameSize = maxFrame
	}
	m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/17"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	pending := mdsStartSession(m, ctx, MDSSessionOptions{RenewInterval: time.Hour})
	peer.next(t)
	peer.send(t, mdsSessionV7(mds.SessionOpen, 0, nil, 0, false))
	opened := <-pending
	if opened.err != nil {
		t.Fatal(opened.err)
	}
	mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
	peer.conn.Close()
	<-opened.session.Done()
	if opened.session.Status().State != MDSSessionLost || !errors.Is(opened.session.Err(), mds.ErrSession) && opened.session.Err() == nil {
		t.Fatal("restart did not lose the session", opened.session.Status())
	}
	<-peers // the RESET resume attempt
	// The failed handle stays cached until closed; the caller closes it
	// before opening the new incarnation of the same filesystem rank.
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	return c, opened.session, peers, ctx
}

func TestCephFSReconnectSessionAfterMDSRestart(t *testing.T) {
	c, lost, peers, ctx := lostCephFSSession(t, 0)
	newTarget := MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/0"}
	m, err := c.OpenMDS(ctx, newTarget)
	if err != nil {
		t.Fatal("new MDS incarnation refused", err)
	}
	peer := <-peers
	state := MDSReconnectState{
		Caps:   []MDSCapReconnect{{Ino: 0x10000000001, CapID: 5, Wanted: 0x55, Issued: 0x55, SnapRealm: 1, PathBase: 1, Path: "f", Flock: []byte{0, 0, 0, 0}}, {Ino: 1, CapID: 4, Wanted: 1, Issued: 1, SnapRealm: 1}},
		Realms: []MDSRealmReconnect{{Ino: 1, Seq: 1}},
	}
	for _, bad := range []func() error{
		func() error {
			_, _, err := m.ReconnectSession(ctx, nil, state, MDSSessionOptions{RenewInterval: time.Hour})
			return err
		},
		func() error {
			_, _, err := m.ReconnectSession(ctx, lost, state, MDSSessionOptions{})
			return err
		},
		func() error {
			_, _, err := lost.conn.ReconnectSession(ctx, lost, state, MDSSessionOptions{RenewInterval: time.Hour})
			return err
		},
	} {
		if err := bad(); err == nil {
			t.Fatal("invalid reconnect admitted")
		}
	}
	pending := make(chan mdsSessionOpenResult, 1)
	go func() {
		s, _, err := m.ReconnectSession(ctx, lost, state, MDSSessionOptions{RenewInterval: time.Hour})
		pending <- mdsSessionOpenResult{s, err}
	}()
	sent := peer.next(t)
	caps, realms, more, err := mds.DecodeReconnect(sent, 0)
	if err != nil || more || len(caps) != 2 || caps[0].Ino != 1 || !reflect.DeepEqual(realms, state.Realms) || sent.Version != 5 || sent.CompatVersion != 4 {
		t.Fatal("reconnect wire", caps, realms, more, err)
	}
	peer.send(t, mdsSessionV7(mds.SessionOpen, 0, nil, 0, false))
	got := <-pending
	if got.err != nil {
		t.Fatal(got.err)
	}
	status := got.session.Status()
	if status.State != MDSSessionOpen || status.ReconnectedFrom != lost.Status().Generation || status.Generation <= lost.Status().Generation || status.PushSequence != 0 || status.Target != newTarget {
		t.Fatal("reconnected status", status)
	}
	if renew := mdsSessionNext(t, peer, mds.SessionRequestRenewCaps); renew.Sequence != 1 {
		t.Fatal("reconnected session renewal", renew)
	}
}

func TestCephFSReconnectDeniedAndSplit(t *testing.T) {
	c, lost, peers, ctx := lostCephFSSession(t, 1024)
	m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/0"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	var state MDSReconnectState
	for i := range 6 {
		state.Caps = append(state.Caps, MDSCapReconnect{Ino: uint64(0x10000000000 + i), CapID: uint64(i + 1), Path: strings.Repeat("p", 300)})
	}
	state.Realms = []MDSRealmReconnect{{Ino: 1, Seq: 1}}
	pending := make(chan mdsSessionOpenResult, 1)
	go func() {
		s, _, err := m.ReconnectSession(ctx, lost, state, MDSSessionOptions{RenewInterval: time.Hour})
		pending <- mdsSessionOpenResult{s, err}
	}()
	var total int
	for {
		sent := peer.next(t)
		if uint64(len(sent.Data))+41 > 1024 {
			t.Fatal("reconnect message exceeds the frame limit", len(sent.Data))
		}
		caps, realms, more, err := mds.DecodeReconnect(sent, 0)
		if err != nil {
			t.Fatal(err)
		}
		total += len(caps)
		if more != (len(realms) == 0) {
			t.Fatal("realms must close the sequence", more, realms)
		}
		if !more {
			break
		}
	}
	if total != 6 {
		t.Fatal("split lost capabilities", total)
	}
	peer.send(t, mdsSessionReply(mds.SessionClose, 0))
	got := <-pending
	if got.session != nil || !errors.Is(got.err, ErrMDSSessionReconnectDenied) || !errors.Is(got.err, ErrMDSSessionClosed) {
		t.Fatal("denied reconnect", got.err)
	}
	var huge MDSReconnectState
	huge.Caps = []MDSCapReconnect{{Ino: 1, Path: strings.Repeat("x", 2048)}}
	if _, _, err := m.ReconnectSession(ctx, lost, huge, MDSSessionOptions{RenewInterval: time.Hour}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatal("oversized capability admitted", err)
	}
	_ = msgr.ErrFrame
}

// cephfsTestInode encodes a minimal REPLY_ENCODING InodeStat v8 for ino.
func cephfsTestInode(e *wire.Encoder, ino uint64) {
	b := wire.Encoder{}
	b.U64(ino)
	b.U64(mds.NoSnap)
	b.U32(0)
	b.U64(1)
	b.U64(1)
	b.Raw(make([]byte, 33)) // reply cap
	b.Raw(make([]byte, 28)) // legacy layout
	b.Raw(make([]byte, 24)) // ctime, mtime, atime
	b.U32(0)
	b.Raw(make([]byte, 24)) // size, max size, truncate size
	b.U32(0)
	b.U32(0o40755)
	b.Raw(make([]byte, 12)) // uid, gid, nlink
	b.Raw(make([]byte, 40)) // dirstat and rstat counters
	b.Raw(make([]byte, 8))  // rctime
	b.U32(0)                // fragtree
	b.String("")
	b.Raw(make([]byte, 8)) // dir layout
	b.Bytes(nil)           // xattrs
	b.U64(0)
	b.Bytes(nil)
	quota := wire.Encoder{}
	quota.U64(0)
	quota.U64(0)
	b.Struct(1, 1, quota.Data)
	b.String("")
	b.Raw(make([]byte, 16)) // btime, change attr
	b.U32(^uint32(0))
	b.Raw(make([]byte, 16)) // snap btime, rsnaps
	b.U32(0)
	b.U8(0)
	b.Bytes(nil)
	b.Bytes(nil)
	opt := wire.Encoder{}
	opt.U32(0)
	b.Struct(1, 1, opt.Data)
	e.Struct(8, 1, b.Data)
}

func TestCephFSReconnectResubmitsChosenCallsFirst(t *testing.T) {
	c, peers, ctx := cephfsTestClient(t)
	m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/17"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	pending := mdsStartSession(m, ctx, MDSSessionOptions{RenewInterval: time.Hour})
	peer.next(t)
	peer.send(t, mdsSessionV7(mds.SessionOpen, 0, nil, 0, false))
	opened := <-pending
	if opened.err != nil {
		t.Fatal(opened.err)
	}
	lost := opened.session
	mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
	release := []MDSRequestRelease{{Ino: 1, CapID: 1, DentryName: "d"}}
	mkdir, _ := lost.Submit(ctx, MDSRequest{Op: MDSOpMkdir, Path: MDSFilePath{Ino: 1, Path: "d"}, Releases: release, Retry: 2})
	peer.next(t)
	getattr, _ := lost.Submit(ctx, MDSRequest{Op: MDSOpGetattr, Path: MDSFilePath{Ino: 1}})
	peer.next(t)
	lookup, _ := lost.Submit(ctx, MDSRequest{Op: MDSOpLookup, Path: MDSFilePath{Ino: 1, Path: "x"}})
	peer.next(t)
	trace := wire.Encoder{}
	cephfsTestInode(&trace, 0x10000000009)
	unsafe := cephfsReply(MDSOpMkdir, mkdir.Transaction(), 0, false, trace.Data)
	peer.send(t, unsafe)
	if first, err := mkdir.Wait(ctx); err != nil || first.Safe {
		t.Fatal(first, err)
	}
	peer.send(t, cephfsReply(MDSOpLookup, lookup.Transaction(), -2, true, nil))
	if _, err := lookup.WaitSafe(ctx); err != nil {
		t.Fatal(err)
	}
	peer.conn.Close()
	<-lost.Done()
	<-peers // RESET resume attempt
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m2, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/0"})
	if err != nil {
		t.Fatal(err)
	}
	peer2 := <-peers
	if _, _, err := m2.ReconnectSession(ctx, lost, MDSReconnectState{Resubmit: []*MDSRequestCall{lookup}}, MDSSessionOptions{RenewInterval: time.Hour}); err == nil {
		t.Fatal("safe-replied call resubmitted")
	}
	if _, _, err := m2.ReconnectSession(ctx, lost, MDSReconnectState{Resubmit: []*MDSRequestCall{getattr, getattr}}, MDSSessionOptions{RenewInterval: time.Hour}); err == nil {
		t.Fatal("duplicate resubmission admitted")
	}
	type result struct {
		session *MDSSession
		calls   []*MDSRequestCall
		err     error
	}
	done := make(chan result, 1)
	go func() {
		s, calls, err := m2.ReconnectSession(ctx, lost, MDSReconnectState{Resubmit: []*MDSRequestCall{mkdir, getattr}}, MDSSessionOptions{RenewInterval: time.Hour})
		done <- result{s, calls, err}
	}()
	replay, err := mds.DecodeClientRequest(peer2.next(t), 0)
	if err != nil || replay.Flags&mds.RequestFlagReplay == 0 || replay.Ino != 0x10000000009 || replay.Retry != 3 || len(replay.Releases) != 0 || replay.Op != MDSOpMkdir {
		t.Fatal("replayed unsafe request", replay, err)
	}
	resend, err := mds.DecodeClientRequest(peer2.next(t), 0)
	if err != nil || resend.Flags&mds.RequestFlagReplay != 0 || resend.Retry != 1 || resend.Op != MDSOpGetattr {
		t.Fatal("resent unanswered request", resend, err)
	}
	if msg := peer2.next(t); msg.Type != mds.ClientReconnectMessage {
		t.Fatal("reconnect must follow the resubmissions", msg.Type)
	}
	peer2.send(t, mdsSessionV7(mds.SessionOpen, 0, nil, 0, false))
	got := <-done
	if got.err != nil || len(got.calls) != 2 || got.calls[0].Transaction() != mkdir.Transaction() || got.calls[1].Transaction() != getattr.Transaction() {
		t.Fatal("reconnect result", got.err, got.calls)
	}
	mdsSessionNext(t, peer2, mds.SessionRequestRenewCaps)
	peer2.send(t, cephfsReply(MDSOpMkdir, mkdir.Transaction(), 0, true, nil))
	peer2.send(t, cephfsReply(MDSOpGetattr, getattr.Transaction(), 0, true, nil))
	for _, call := range got.calls {
		if reply, err := call.WaitSafe(ctx); err != nil || !reply.Safe {
			t.Fatal("resubmitted call", reply, err)
		}
	}
	if len(c.calls) != 0 {
		t.Fatal("slots leaked", len(c.calls))
	}
}
