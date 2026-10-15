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
	for _, bad := range []func() (*MDSSession, error){
		func() (*MDSSession, error) {
			return m.ReconnectSession(ctx, nil, state, MDSSessionOptions{RenewInterval: time.Hour})
		},
		func() (*MDSSession, error) { return m.ReconnectSession(ctx, lost, state, MDSSessionOptions{}) },
		func() (*MDSSession, error) {
			return lost.conn.ReconnectSession(ctx, lost, state, MDSSessionOptions{RenewInterval: time.Hour})
		},
	} {
		if _, err := bad(); err == nil {
			t.Fatal("invalid reconnect admitted")
		}
	}
	pending := make(chan mdsSessionOpenResult, 1)
	go func() {
		s, err := m.ReconnectSession(ctx, lost, state, MDSSessionOptions{RenewInterval: time.Hour})
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
		s, err := m.ReconnectSession(ctx, lost, state, MDSSessionOptions{RenewInterval: time.Hour})
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
	if _, err := m.ReconnectSession(ctx, lost, huge, MDSSessionOptions{RenewInterval: time.Hour}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatal("oversized capability admitted", err)
	}
	_ = msgr.ErrFrame
}
