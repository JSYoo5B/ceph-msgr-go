package integration_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// These test-only generations restate Client::queue_cap_snap, flush_snaps
// and handle_cap_flushsnap_ack at the pinned Tentacle source. No native code
// is copied. A generation owns the old snap context and immutable metadata;
// no FLUSHSNAP is admitted before its data writeback completes.
type cephfsFlushSnapKey struct{ Ino, Follows uint64 }
type cephfsFlushSnapExpected struct {
	TID   uint64
	Dirty uint32
}
type cephfsCapSnap struct {
	Ino                                               uint64
	Context                                           cephmsgr.OSDSnapContext
	Caps                                              cephmsgr.MDSCaps
	Buffered                                          bool
	WritebackStarted, WritebackCompleted, Sent, Acked time.Time
	Transaction                                       uint64
	Error                                             string `json:",omitempty"`
	writeBack                                         func() error
}

func cloneCephFSCapSnap(s *cephfsCapSnap) cephfsCapSnap {
	out := *s
	out.Context.Snapshots = slices.Clone(s.Context.Snapshots)
	out.writeBack = nil
	return out
}

// enableCapSnaps is used only by the data acceptance. Other test agents
// retain their existing clean-cap behavior.
func (a *cephfsCapAgent) enableCapSnaps(realms map[uint64]cephmsgr.MDSSnapRealm) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.capSnaps = map[cephfsFlushSnapKey]*cephfsCapSnap{}
	for ino, r := range realms {
		if previous, ok := a.realms[ino]; !ok || r.Seq >= previous.Seq {
			a.realms[ino] = r
		}
	}
}

func (a *cephfsCapAgent) oldDirtySnapContextsLocked() map[uint64]cephmsgr.OSDSnapContext {
	if a.capSnaps == nil {
		return nil
	}
	old := map[uint64]cephmsgr.OSDSnapContext{}
	for ino, d := range a.dirty {
		if d.Dirty == 0 {
			continue
		}
		c := a.caps[ino]
		if c == nil || c.Released {
			continue
		}
		context, err := cephfsSnapContext(a.realms, c.Realm)
		if err != nil {
			a.failLocked("old snap context of %d: %v", ino, err)
			continue
		}
		old[ino] = context
	}
	return old
}

func (a *cephfsCapAgent) queueChangedCapSnapsLocked(old map[uint64]cephmsgr.OSDSnapContext) {
	for ino, before := range old {
		c, d := a.caps[ino], a.dirty[ino]
		after, err := cephfsSnapContext(a.realms, c.Realm)
		if err != nil {
			a.failLocked("new snap context of %d: %v", ino, err)
			continue
		}
		if !slices.ContainsFunc(after.Snapshots, func(id uint64) bool { return !slices.Contains(before.Snapshots, id) }) {
			continue
		}
		if err := a.queueCapSnapLocked(c, d, before); err != nil {
			a.failLocked("queue cap snap of %d: %v", ino, err)
		}
	}
}

func (a *cephfsCapAgent) queueCapSnapLocked(c *cephfsCap, d *cephfsDirtyFile, old cephmsgr.OSDSnapContext) error {
	key := cephfsFlushSnapKey{c.Ino, old.Sequence}
	if _, exists := a.capSnaps[key]; exists || d.writing || d.Dirty == 0 || d.Buffered && d.WriteBack == nil {
		return fmt.Errorf("invalid or duplicate dirty cap snap %d/%d", c.Ino, old.Sequence)
	}
	old.Snapshots = slices.Clone(old.Snapshots)
	a.capSnaps[key] = &cephfsCapSnap{Ino: c.Ino, Context: old, Buffered: d.Buffered, writeBack: d.WriteBack,
		Caps: cephmsgr.MDSCaps{Op: cephmsgr.MDSCapOpFlushSnap, Ino: c.Ino, Realm: c.Realm, MigrateSeq: c.MSeq,
			Caps: c.Issued, Dirty: d.Dirty, SnapFollows: old.Sequence, UID: c.UID, GID: c.GID, Mode: c.Mode,
			Size: c.Size, MTime: c.MTime, ATime: c.ATime, CTime: c.CTime, TimeWarpSeq: c.TimeWarpSeq,
			InlineVersion: c.InlineVersion, BTime: c.BTime, ChangeAttr: c.ChangeAttr, CallerUID: ^uint32(0), CallerGID: ^uint32(0), Files: -1, Subdirs: -1}}
	// The captured generation now owns these bytes. Subsequent writes dirty
	// the head independently rather than overwriting this snapshot's state.
	d.Dirty, d.Buffered, d.WriteBack = 0, false, nil
	return nil
}

func (a *cephfsCapAgent) snapBufferUsedLocked(ino uint64) bool {
	for _, s := range a.capSnaps {
		if s.Ino == ino && s.Buffered {
			return true
		}
	}
	return false
}

func (a *cephfsCapAgent) oldestFlushTIDLocked() uint64 {
	var oldest uint64
	for tid := range a.flushing {
		if oldest == 0 || tid < oldest {
			oldest = tid
		}
	}
	for _, expected := range a.flushSnaps {
		if oldest == 0 || expected.TID < oldest {
			oldest = expected.TID
		}
	}
	return oldest
}

func (a *cephfsCapAgent) expectFlushSnapLocked(ino, follows, tid uint64, dirty uint32) error {
	if tid == 0 || dirty == 0 {
		return fmt.Errorf("cap snap must have a flush TID and dirty metadata")
	}
	if a.flushSnaps == nil {
		a.flushSnaps = map[cephfsFlushSnapKey]cephfsFlushSnapExpected{}
	}
	key := cephfsFlushSnapKey{ino, follows}
	if _, exists := a.flushSnaps[key]; exists {
		return fmt.Errorf("cap snap %d/%d already pending", ino, follows)
	}
	for _, pending := range a.flushSnaps {
		if pending.TID == tid {
			return fmt.Errorf("flush TID %d already pending", tid)
		}
	}
	if _, exists := a.flushing[tid]; exists {
		return fmt.Errorf("flush TID %d already flushing head metadata", tid)
	}
	a.flushSnaps[key] = cephfsFlushSnapExpected{tid, dirty}
	a.flushTID = max(a.flushTID, tid)
	return nil
}

func (a *cephfsCapAgent) consumeFlushSnapLocked(m *cephmsgr.MDSCaps, tid uint64, at time.Time) bool {
	key := cephfsFlushSnapKey{m.Ino, m.SnapFollows}
	expected, ok := a.flushSnaps[key]
	if !ok || expected.TID != tid || expected.Dirty != m.Dirty {
		return false
	}
	delete(a.flushSnaps, key)
	if s := a.capSnaps[key]; s != nil {
		s.Acked = at
	}
	return true
}

// drainCapSnaps is an explicit test consumer action. It does not retry failed
// writes or uncertain cap sends. A blocked or failed older generation stops
// newer generations, matching flush_snaps' ordered traversal.
func (a *cephfsCapAgent) drainCapSnaps(ino uint64) error {
	a.mu.Lock()
	keys := slices.Collect(maps.Keys(a.capSnaps))
	slices.SortFunc(keys, func(x, y cephfsFlushSnapKey) int { return compareUint64(x.Follows, y.Follows) })
	a.mu.Unlock()
	for _, key := range keys {
		if key.Ino != ino {
			continue
		}
		a.mu.Lock()
		s := a.capSnaps[key]
		if s.Transaction != 0 {
			a.mu.Unlock()
			continue
		}
		if !s.WritebackStarted.IsZero() || s.Error != "" {
			a.mu.Unlock()
			return fmt.Errorf("cap snap %d/%d already started or failed", ino, key.Follows)
		}
		s.WritebackStarted = time.Now()
		writeBack := s.writeBack
		buffered := s.Buffered
		a.mu.Unlock()
		var err error
		if buffered {
			err = writeBack()
		}
		a.mu.Lock()
		if err != nil {
			s.Error = err.Error()
			a.mu.Unlock()
			return err
		}
		s.Buffered, s.writeBack, s.WritebackCompleted = false, nil, time.Now()
		c := a.caps[ino]
		s.Caps.Realm, s.Caps.MigrateSeq, s.Caps.OSDEpochBarrier = c.Realm, c.MSeq, a.barrier
		a.flushTID++
		tid := a.flushTID
		if err = a.expectFlushSnapLocked(ino, key.Follows, tid, s.Caps.Dirty); err != nil {
			a.mu.Unlock()
			return err
		}
		s.Transaction, s.Caps.OldestFlushTID = tid, a.oldestFlushTIDLocked()
		msg := s.Caps
		err = a.send(func(ctx context.Context) error { return a.s.SendCaps(ctx, msg, tid) })
		if err != nil {
			s.Error = err.Error()
			a.mu.Unlock()
			return err
		}
		s.Sent = time.Now()
		a.mu.Unlock()
	}
	return nil
}

// capSnapContext reads the latest decoded realm graph held by the agent.
func (a *cephfsCapAgent) capSnapContext(ino uint64) (cephmsgr.OSDSnapContext, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c := a.caps[ino]
	if c == nil {
		return cephmsgr.OSDSnapContext{}, fmt.Errorf("unknown inode %d", ino)
	}
	return cephfsSnapContext(a.realms, c.Realm)
}
