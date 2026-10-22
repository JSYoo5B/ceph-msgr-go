package cephmsgr

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
)

// Synthetic peers pin the SendCaps outcome classes: an error is returned only
// before writer admission, so the refused frame is never transmitted and the
// caller may send its original flush TID later. These are not native results.

func capOutcomeUpdate() MDSCaps {
	return MDSCaps{Op: MDSCapOpUpdate, Ino: 0x10000000005, CapID: 3, Caps: 0x55, Dirty: 0x10, Size: 4096, SnapFollows: 1, OldestFlushTID: 9, Files: -1, Subdirs: -1}
}

func TestCephFSSendCapsCancelledBeforeAdmissionIsNeverWritten(t *testing.T) {
	_, _, s, peer, ctx := cephfsOpen(t)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.SendCaps(cancelled, capOutcomeUpdate(), 9); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled cap send was not refused before admission", err)
	}
	next := capOutcomeUpdate()
	next.Size = 8192
	if err := s.SendCaps(ctx, next, 10); err != nil {
		t.Fatal(err)
	}
	frame := peer.next(t)
	got, err := mds.DecodeCaps(frame, 0)
	if err != nil || frame.Transaction != 10 || got.Size != 8192 {
		t.Fatal("refused cap frame reached the writer before the admitted one", frame.Transaction, got, err)
	}
	select {
	case extra := <-peer.requests:
		t.Fatal("unexpected frame after the admitted cap", extra.Type, extra.Transaction)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestCephFSSendCapsOnLostSessionIsKnownUnsentAndExplicitlySendable(t *testing.T) {
	c, lost, peers, ctx := lostCephFSSession(t, 0)
	update := capOutcomeUpdate()
	err := lost.SendCaps(ctx, update, 9)
	if err == nil || lost.Err() == nil || !errors.Is(err, lost.Err()) || lost.Status().State != MDSSessionLost {
		t.Fatal("cap send on a Lost session did not return its terminal cause before admission", err, lost.Err())
	}
	if err := lost.ReleaseCaps(ctx, []MDSCapItem{{Ino: update.Ino, CapID: update.CapID}}, 0); err == nil {
		t.Fatal("cap release admitted on a Lost session")
	}
	if err := lost.SendLease(ctx, MDSLease{Action: MDSLeaseRelease, Ino: 1, DentryName: "f"}); err == nil {
		t.Fatal("lease release admitted on a Lost session")
	}
	m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/0"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	pending := make(chan mdsSessionOpenResult, 1)
	go func() {
		s, _, err := m.ReconnectSession(ctx, lost, MDSReconnectState{Caps: []MDSCapReconnect{{Ino: update.Ino, CapID: update.CapID, Wanted: 0x55, Issued: 0x55, SnapRealm: 1}},
			Realms: []MDSRealmReconnect{{Ino: 1, Seq: 1}}}, MDSSessionOptions{RenewInterval: time.Hour})
		pending <- mdsSessionOpenResult{s, err}
	}()
	// The refused UPDATE was neither retained nor replayed ahead of reconnect.
	if first := peer.next(t); first.Type != mds.ClientReconnectMessage {
		t.Fatal("a refused cap frame preceded reconnect", first.Type, first.Transaction)
	}
	peer.send(t, mdsSessionV7(mds.SessionOpen, 0, nil, 0, false))
	opened := <-pending
	if opened.err != nil {
		t.Fatal(opened.err)
	}
	s := opened.session
	mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
	select {
	case extra := <-peer.requests:
		t.Fatal("the library resent a refused cap frame by itself", extra.Type, extra.Transaction)
	case <-time.After(50 * time.Millisecond):
	}
	// The caller's explicit decision sends the original identity once.
	if err := s.SendCaps(ctx, update, 9); err != nil {
		t.Fatal(err)
	}
	frame := peer.next(t)
	got, err := mds.DecodeCaps(frame, 0)
	if err != nil || frame.Type != mds.ClientCapsMessage || frame.Transaction != 9 || got.Op != MDSCapOpUpdate || got.Ino != update.Ino || got.Dirty != update.Dirty ||
		got.SnapFollows != update.SnapFollows || got.OldestFlushTID != update.OldestFlushTID || got.Size != update.Size {
		t.Fatal("explicit first send changed the original flush identity", frame.Transaction, got, err)
	}
	peer.send(t, mds.EncodeCaps(MDSCaps{Op: MDSCapOpFlushAck, Ino: update.Ino, CapID: update.CapID, Dirty: update.Dirty}, 9))
	event, err := s.Next(ctx)
	if err != nil || event.Caps == nil || event.Caps.Op != MDSCapOpFlushAck || event.Transaction != 9 || event.Caps.Ino != update.Ino || event.Caps.Dirty != update.Dirty {
		t.Fatal("FLUSH_ACK did not correlate with the original TID", event, err)
	}
	select {
	case extra := <-peer.requests:
		if extra.Type == mds.ClientCapsMessage {
			t.Fatal("the cap frame was transmitted twice", extra.Transaction)
		}
	case <-time.After(50 * time.Millisecond):
	}
}
