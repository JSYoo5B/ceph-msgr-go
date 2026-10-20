package integration_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
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

type cephfsCapSnapResubmission struct {
	Generation, ReconnectedFrom uint64
	Selected, Started, Sent     time.Time
	Error                       string `json:",omitempty"`
}

type cephfsCapSnap struct {
	Ino                                               uint64
	Context                                           cephmsgr.OSDSnapContext
	Caps                                              cephmsgr.MDSCaps
	Buffered                                          bool
	WritebackStarted, WritebackCompleted, Sent, Acked time.Time
	Transaction                                       uint64
	SentGeneration                                    uint64
	Resubmissions                                     []cephfsCapSnapResubmission `json:",omitempty"`
	Error                                             string                      `json:",omitempty"`
	writeBack                                         func() error
}

func cloneCephFSCapSnap(s *cephfsCapSnap) cephfsCapSnap {
	out := *s
	out.Context.Snapshots = slices.Clone(s.Context.Snapshots)
	out.Caps = cloneCephFSCaps(s.Caps)
	out.Resubmissions = slices.Clone(s.Resubmissions)
	out.writeBack = nil
	return out
}

func cloneCephFSCaps(c cephmsgr.MDSCaps) cephmsgr.MDSCaps {
	c.SnapTrace, c.Xattrs, c.Flock = slices.Clone(c.SnapTrace), slices.Clone(c.Xattrs), slices.Clone(c.Flock)
	c.InlineData = slices.Clone(c.InlineData)
	c.FscryptAuth, c.FscryptFile = slices.Clone(c.FscryptAuth), slices.Clone(c.FscryptFile)
	return c
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

func (a *cephfsCapAgent) capSnapKeys(ino uint64) []cephfsFlushSnapKey {
	a.mu.Lock()
	defer a.mu.Unlock()
	keys := slices.Collect(maps.Keys(a.capSnaps))
	keys = slices.DeleteFunc(keys, func(k cephfsFlushSnapKey) bool { return k.Ino != ino })
	slices.SortFunc(keys, func(x, y cephfsFlushSnapKey) int { return compareUint64(x.Follows, y.Follows) })
	return keys
}

func capSnapFailure(s *cephfsCapSnap) string {
	if s.Error != "" {
		return s.Error
	}
	for _, attempt := range s.Resubmissions {
		if attempt.Error != "" {
			return attempt.Error
		}
		if attempt.Sent.IsZero() {
			return "explicit resubmission selected but not completed"
		}
	}
	return ""
}

// prepareCapSnap completes old-context data writeback and registers one
// original flush identity without sending metadata. A repeat does not redo
// completed data I/O or allocate another TID.
func (a *cephfsCapAgent) prepareCapSnap(key cephfsFlushSnapKey) error {
	a.mu.Lock()
	s := a.capSnaps[key]
	if failure := capSnapFailure(s); failure != "" {
		a.mu.Unlock()
		return fmt.Errorf("cap snap %d/%d failed: %s", key.Ino, key.Follows, failure)
	}
	if s.Transaction != 0 || !s.Acked.IsZero() {
		a.mu.Unlock()
		return nil
	}
	if !s.WritebackStarted.IsZero() {
		a.mu.Unlock()
		return fmt.Errorf("cap snap %d/%d already started or failed", key.Ino, key.Follows)
	}
	if a.caps[key.Ino] == nil || s.Buffered && s.writeBack == nil {
		a.mu.Unlock()
		return fmt.Errorf("cap snap %d/%d lacks cap or data writeback", key.Ino, key.Follows)
	}
	s.WritebackStarted = time.Now()
	writeBack, buffered := s.writeBack, s.Buffered
	a.mu.Unlock()
	var err error
	if buffered {
		err = writeBack()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		s.Error = err.Error()
		return err
	}
	s.Buffered, s.writeBack, s.WritebackCompleted = false, nil, time.Now()
	c := a.caps[key.Ino]
	s.Caps.Realm, s.Caps.MigrateSeq, s.Caps.OSDEpochBarrier = c.Realm, c.MSeq, a.barrier
	tid := a.flushTID + 1
	if err = a.expectFlushSnapLocked(key.Ino, key.Follows, tid, s.Caps.Dirty); err != nil {
		s.Error = err.Error()
		return err
	}
	s.Transaction, s.Caps.OldestFlushTID = tid, a.oldestFlushTIDLocked()
	return nil
}

// prepareCapSnaps stages ordered data writeback and ACK identities only.
// The caller may retain this state across a Lost session before first send.
func (a *cephfsCapAgent) prepareCapSnaps(ino uint64) error {
	for _, key := range a.capSnapKeys(ino) {
		if err := a.prepareCapSnap(key); err != nil {
			return err
		}
	}
	return nil
}

func (a *cephfsCapAgent) sendPreparedCapSnap(key cephfsFlushSnapKey, send func(context.Context, cephmsgr.MDSCaps, uint64) error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.capSnaps[key]
	// Check failures before Sent: uncertain admission is never replayed.
	if failure := capSnapFailure(s); failure != "" {
		return fmt.Errorf("cap snap %d/%d failed: %s", key.Ino, key.Follows, failure)
	}
	if !s.Sent.IsZero() || !s.Acked.IsZero() {
		return nil
	}
	expected, ok := a.flushSnaps[key]
	if s.Transaction == 0 || s.Buffered || s.WritebackCompleted.IsZero() || !ok || expected.TID != s.Transaction || expected.Dirty != s.Caps.Dirty {
		return fmt.Errorf("cap snap %d/%d is not prepared", key.Ino, key.Follows)
	}
	c := a.caps[key.Ino]
	if c == nil {
		return fmt.Errorf("cap snap %d/%d lacks its current cap", key.Ino, key.Follows)
	}
	// send_flush_snap uses the current realm, auth-cap migrate sequence and
	// barrier, while snapshot metadata and the registered identity stay fixed.
	s.Caps.Realm, s.Caps.MigrateSeq, s.Caps.OSDEpochBarrier = c.Realm, c.MSeq, a.barrier
	msg := cloneCephFSCaps(s.Caps)
	if err := a.send(func(ctx context.Context) error { return send(ctx, msg, s.Transaction) }); err != nil {
		s.Error = err.Error()
		return err
	}
	s.Sent = time.Now()
	if a.s != nil {
		s.SentGeneration = a.s.Status().Generation
	}
	return nil
}

// resubmitCapSnaps explicitly selects already-admitted, unacknowledged
// generations after reconnect. It never reruns data I/O or clears original
// send evidence. Each TID can be selected once in this new session generation.
func (a *cephfsCapAgent) resubmitCapSnaps(tids []uint64) error {
	return a.resubmitCapSnapsWith(tids, a.s.Status(), a.s.SendCaps)
}

func (a *cephfsCapAgent) resubmitCapSnapsWith(tids []uint64, status cephmsgr.MDSSessionStatus, send func(context.Context, cephmsgr.MDSCaps, uint64) error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(tids) == 0 || status.State != cephmsgr.MDSSessionOpen || status.ReconnectedFrom == 0 || status.Generation <= status.ReconnectedFrom {
		return fmt.Errorf("cap-snap resubmission requires a nonempty selection and a new reconnected open session")
	}
	selected := make([]*cephfsCapSnap, 0, len(tids))
	for i, tid := range tids {
		if tid == 0 || i > 0 && tid <= tids[i-1] {
			return fmt.Errorf("cap-snap resubmission TIDs must be distinct and in original order")
		}
		var found *cephfsCapSnap
		for _, s := range a.capSnaps {
			if s.Transaction == tid {
				if found != nil {
					return fmt.Errorf("cap-snap flush TID %d is ambiguous", tid)
				}
				found = s
			}
		}
		if found == nil {
			return fmt.Errorf("cap-snap flush TID %d is unknown", tid)
		}
		s := found
		key := cephfsFlushSnapKey{s.Ino, s.Context.Sequence}
		expected, ok := a.flushSnaps[key]
		if s.Sent.IsZero() || s.SentGeneration != status.ReconnectedFrom || !s.Acked.IsZero() || s.Error != "" || s.Buffered || s.writeBack != nil || s.WritebackCompleted.IsZero() ||
			s.Caps.Op != cephmsgr.MDSCapOpFlushSnap || s.Caps.Ino != s.Ino || s.Caps.Dirty == 0 ||
			!ok || expected.TID != tid || expected.Dirty != s.Caps.Dirty || s.Caps.SnapFollows != key.Follows || s.Caps.OldestFlushTID == 0 || s.Caps.OldestFlushTID > tid || a.caps[s.Ino] == nil {
			return fmt.Errorf("cap-snap flush TID %d lacks a successful original admission pending from the lost generation", tid)
		}
		for _, attempt := range s.Resubmissions {
			if attempt.Generation == status.Generation {
				return fmt.Errorf("cap-snap flush TID %d was already selected in generation %d", tid, status.Generation)
			}
		}
		selected = append(selected, s)
	}
	// Reserve the entire validated choice before its first send. A failed
	// prefix cannot make a later selected TID eligible for an implicit retry.
	for _, s := range selected {
		s.Resubmissions = append(s.Resubmissions, cephfsCapSnapResubmission{Generation: status.Generation, ReconnectedFrom: status.ReconnectedFrom, Selected: time.Now()})
	}
	for _, s := range selected {
		attempt := &s.Resubmissions[len(s.Resubmissions)-1]
		attempt.Started = time.Now()
		c := a.caps[s.Ino]
		msg := cloneCephFSCaps(s.Caps)
		msg.Realm, msg.MigrateSeq, msg.OSDEpochBarrier = c.Realm, c.MSeq, a.barrier
		if err := a.send(func(ctx context.Context) error { return send(ctx, msg, s.Transaction) }); err != nil {
			attempt.Error = err.Error()
			return err
		}
		attempt.Sent = time.Now()
	}
	return nil
}

func (a *cephfsCapAgent) sendPreparedCapSnapsWith(ino uint64, send func(context.Context, cephmsgr.MDSCaps, uint64) error) error {
	for _, key := range a.capSnapKeys(ino) {
		if err := a.sendPreparedCapSnap(key, send); err != nil {
			return err
		}
	}
	return nil
}

// sendPreparedCapSnaps performs explicit first sends only. It never reruns
// data writeback, resends a submitted generation or retries an uncertain send.
func (a *cephfsCapAgent) sendPreparedCapSnaps(ino uint64) error {
	return a.sendPreparedCapSnapsWith(ino, a.s.SendCaps)
}

// drainCapSnaps preserves the original interleaved writeback/flush order:
// an older failed send prevents even the next generation's data writeback.
func (a *cephfsCapAgent) drainCapSnaps(ino uint64) error {
	return a.drainCapSnapsWith(ino, a.s.SendCaps)
}

func (a *cephfsCapAgent) drainCapSnapsWith(ino uint64, send func(context.Context, cephmsgr.MDSCaps, uint64) error) error {
	for _, key := range a.capSnapKeys(ino) {
		if err := a.prepareCapSnap(key); err != nil {
			return err
		}
		if err := a.sendPreparedCapSnap(key, send); err != nil {
			return err
		}
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
