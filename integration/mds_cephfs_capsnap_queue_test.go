package integration_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephFSCapSnapGenerations(t *testing.T) {
	a := newCephFSCapAgent(context.Background(), nil)
	a.enableCapSnaps(map[uint64]cephmsgr.MDSSnapRealm{1: {Ino: 1, Seq: 1}})
	c := &cephfsCap{Ino: 17, CapID: 4, Realm: 1, Issued: cephCapFileWR | cephCapFileBuffer, Size: 10, MTime: cephmsgr.MDSTime{Seconds: 100}, ChangeAttr: 5}
	d := &cephfsDirtyFile{Ino: 17, Dirty: cephCapFileWR, Buffered: true, WriteBack: func() error { return nil }}
	a.caps[17], a.dirty[17] = c, d
	old := cephmsgr.OSDSnapContext{Sequence: 2, Snapshots: []uint64{2}}
	if err := a.queueCapSnapLocked(c, d, old); err != nil {
		t.Fatal(err)
	}
	old.Snapshots[0] = 99
	if d.Dirty != 0 || d.Buffered || d.WriteBack != nil || !a.snapBufferUsedLocked(17) {
		t.Fatal("captured generation did not take ownership of dirty buffers")
	}
	c.Size, c.MTime, c.ChangeAttr = 20, cephmsgr.MDSTime{Seconds: 200}, 6
	d.Dirty, d.Buffered, d.WriteBack = cephCapFileWR, true, func() error { return nil }
	if err := a.queueCapSnapLocked(c, d, cephmsgr.OSDSnapContext{Sequence: 3, Snapshots: []uint64{3, 2}}); err != nil {
		t.Fatal(err)
	}
	first, second := a.capSnaps[cephfsFlushSnapKey{17, 2}], a.capSnaps[cephfsFlushSnapKey{17, 3}]
	if first.Caps.Size != 10 || first.Caps.MTime.Seconds != 100 || first.Caps.ChangeAttr != 5 || !reflect.DeepEqual(first.Context.Snapshots, []uint64{2}) || second.Caps.Size != 20 {
		t.Fatal("later head write overwrote the earlier cap snap", first, second)
	}
	saved := cloneCephFSCapSnap(first)
	saved.Context.Snapshots[0] = 55
	if first.Context.Snapshots[0] != 2 {
		t.Fatal("evidence mutated a live generation")
	}
	// A duplicate push must not overwrite either pending generation.
	d.Dirty = cephCapFileWR
	if err := a.queueCapSnapLocked(c, d, cephmsgr.OSDSnapContext{Sequence: 2}); err == nil || first != a.capSnaps[cephfsFlushSnapKey{17, 2}] {
		t.Fatal("duplicate generation was replaced")
	}
}

func TestCephFSCapSnapACKIdentityAndOldest(t *testing.T) {
	a := newCephFSCapAgent(context.Background(), nil)
	if err := a.expectFlushSnap(17, 2, 4, cephCapFileWR); err != nil {
		t.Fatal(err)
	}
	if err := a.expectFlushSnap(17, 3, 5, cephCapFileWR); err != nil {
		t.Fatal(err)
	}
	if err := a.expectFlushSnap(18, 4, 5, cephCapFileWR); err == nil {
		t.Fatal("session flush TID was reused")
	}
	a.flushing[3] = 18
	if got := a.oldestFlushTIDLocked(); got != 3 {
		t.Fatal("head and cap-snap flush TIDs were not combined", got)
	}
	delete(a.flushing, 3)
	if got := a.oldestFlushTIDLocked(); got != 4 {
		t.Fatal(got)
	}
	for _, m := range []cephmsgr.MDSCaps{
		{Ino: 18, SnapFollows: 3, Dirty: cephCapFileWR},
		{Ino: 17, SnapFollows: 2, Dirty: cephCapFileWR},
		{Ino: 17, SnapFollows: 3, Dirty: 0},
	} {
		if a.consumeFlushSnapLocked(&m, 5, time.Now()) || len(a.flushSnaps) != 2 {
			t.Fatal("mismatching ACK removed a generation", m)
		}
	}
	// A later generation may be acknowledged first; its predecessor remains.
	m := cephmsgr.MDSCaps{Ino: 17, SnapFollows: 3, Dirty: cephCapFileWR}
	if !a.consumeFlushSnapLocked(&m, 5, time.Now()) || len(a.flushSnaps) != 1 || a.oldestFlushTIDLocked() != 4 {
		t.Fatal("out-of-order ACK consumed the predecessor")
	}
	if a.consumeFlushSnapLocked(&m, 5, time.Now()) {
		t.Fatal("duplicate ACK consumed twice")
	}
	m.SnapFollows = 2
	if !a.consumeFlushSnapLocked(&m, 4, time.Now()) || a.oldestFlushTIDLocked() != 0 {
		t.Fatal("matching predecessor ACK did not complete")
	}
}

func TestCephFSCapSnapFailedWritebackStopsFlushes(t *testing.T) {
	a := newCephFSCapAgent(context.Background(), nil)
	a.enableCapSnaps(nil)
	a.caps[17] = &cephfsCap{Ino: 17}
	failure := errors.New("independent OSD writeback failure")
	calls := 0
	a.capSnaps[cephfsFlushSnapKey{17, 1}] = &cephfsCapSnap{Ino: 17, Buffered: true, writeBack: func() error { calls++; return failure }}
	a.capSnaps[cephfsFlushSnapKey{17, 2}] = &cephfsCapSnap{Ino: 17, Buffered: true, writeBack: func() error { calls += 100; return nil }}
	if err := a.drainCapSnaps(17); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	first := a.capSnaps[cephfsFlushSnapKey{17, 1}]
	if calls != 1 || !first.Buffered || !first.Sent.IsZero() || first.Transaction != 0 || len(a.flushSnaps) != 0 || !first.WritebackCompleted.IsZero() {
		t.Fatal("failed data admitted a cap flush or a newer write", first, calls)
	}
	if err := a.drainCapSnaps(17); err == nil || calls != 1 {
		t.Fatal("failed writeback automatically replayed", err, calls)
	}
}

func TestCephFSCapSnapCapturesOldRealmContext(t *testing.T) {
	a := newCephFSCapAgent(context.Background(), nil)
	a.enableCapSnaps(map[uint64]cephmsgr.MDSSnapRealm{1: {Ino: 1, Seq: 1}})
	c := &cephfsCap{Ino: 17, CapID: 4, Realm: 1, Issued: cephCapFileWR | cephCapFileBuffer, Size: 10}
	a.caps[17] = c
	a.dirty[17] = &cephfsDirtyFile{Ino: 17, Dirty: cephCapFileWR, Buffered: true, WriteBack: func() error { return nil }}
	old := a.oldDirtySnapContextsLocked()
	// SPLIT moves the cap and then updates the realm trace. Queueing must
	// keep the pre-move context, not the new realm's snapshot sequence.
	c.Realm = 33
	a.realms[33] = cephmsgr.MDSSnapRealm{Ino: 33, Parent: 1, Seq: 2, Snaps: []uint64{2}}
	a.queueChangedCapSnapsLocked(old)
	s := a.capSnaps[cephfsFlushSnapKey{17, 1}]
	if s == nil || s.Context.Sequence != 1 || len(s.Context.Snapshots) != 0 || s.Caps.Realm != 33 || len(a.errs) != 0 {
		t.Fatal("SPLIT replaced the old snap context", s, a.errs)
	}
	// Repeating an unchanged trace cannot manufacture another generation.
	a.dirty[17].Dirty = cephCapFileWR
	unchanged := a.oldDirtySnapContextsLocked()
	a.queueChangedCapSnapsLocked(unchanged)
	if len(a.capSnaps) != 1 || a.dirty[17].Dirty == 0 {
		t.Fatal("unchanged realm captured fresh head metadata")
	}
}

func TestCephFSCapSnapUncertainSendBlocksNewerGenerations(t *testing.T) {
	a := newCephFSCapAgent(context.Background(), nil)
	a.enableCapSnaps(nil)
	calls := 0
	failed := &cephfsCapSnap{Ino: 17, Transaction: 7, Error: "uncertain cap send", WritebackStarted: time.Now(), WritebackCompleted: time.Now()}
	a.capSnaps[cephfsFlushSnapKey{17, 1}] = failed
	a.capSnaps[cephfsFlushSnapKey{17, 2}] = &cephfsCapSnap{Ino: 17, Buffered: true, writeBack: func() error { calls++; return nil }}
	for range 2 {
		if err := a.drainCapSnaps(17); err == nil || calls != 0 || failed.Transaction != 7 || !failed.Sent.IsZero() {
			t.Fatal("uncertain older cap send did not retain its barrier", err, calls, failed)
		}
	}
}
