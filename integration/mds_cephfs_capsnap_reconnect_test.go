package integration_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

func preparedCapSnapAgent(t *testing.T) (*cephfsCapAgent, *[]uint64) {
	t.Helper()
	a := newCephFSCapAgent(context.Background(), nil)
	a.enableCapSnaps(map[uint64]cephmsgr.MDSSnapRealm{1: {Ino: 1, Seq: 4, Snaps: []uint64{4, 3, 2}, PriorParentSnaps: []uint64{1}}})
	a.barrier = 8
	a.caps[17] = &cephfsCap{Ino: 17, CapID: 9, Realm: 1, MSeq: 5, Seq: 6, IssueSeq: 7,
		Issued: cephCapFileWR, Implemented: cephCapFileWR | cephCapFileBuffer, Open: map[int]int{cephFileModeRDWR: 1}, FscryptAuth: []byte{11}, FscryptFile: []byte{12}}
	a.flushing[4], a.flushTID = 18, 4
	var writes []uint64
	for _, follows := range []uint64{2, 3} {
		key := cephfsFlushSnapKey{17, follows}
		a.capSnaps[key] = &cephfsCapSnap{Ino: 17, Context: cephmsgr.OSDSnapContext{Sequence: follows, Snapshots: []uint64{follows}}, Buffered: true,
			Caps: cephmsgr.MDSCaps{Op: cephmsgr.MDSCapOpFlushSnap, Ino: 17, Realm: 1, MigrateSeq: 5, Dirty: cephCapFileWR, SnapFollows: follows,
				Size: follows * 10, MTime: cephmsgr.MDSTime{Seconds: uint32(follows * 100)}, SnapTrace: []byte{1}, Xattrs: []byte{2}, Flock: []byte{3}, InlineData: []byte{4}, FscryptAuth: []byte{5}, FscryptFile: []byte{6}},
			writeBack: func() error { writes = append(writes, follows); return nil }}
	}
	return a, &writes
}

func TestCephFSCapSnapPrepareRetainsOriginalIdentity(t *testing.T) {
	a, writes := preparedCapSnapAgent(t)
	for range 2 {
		if err := a.prepareCapSnaps(17); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(*writes, []uint64{2, 3}) || a.flushTID != 6 || len(a.flushSnaps) != 2 {
		t.Fatal("preparation repeated data I/O or allocated new identities", writes, a.flushTID, a.flushSnaps)
	}
	for i, follows := range []uint64{2, 3} {
		s := a.capSnaps[cephfsFlushSnapKey{17, follows}]
		if s.Buffered || s.writeBack != nil || s.WritebackCompleted.IsZero() || s.WritebackCompleted.Before(s.WritebackStarted) || !s.Sent.IsZero() || !s.Acked.IsZero() ||
			s.Transaction != uint64(5+i) || s.Caps.OldestFlushTID != 4 || s.Caps.OSDEpochBarrier != 8 || s.Caps.MigrateSeq != 5 || s.Caps.Size != follows*10 {
			t.Fatalf("preparation sent or changed snapshot metadata: %+v", s)
		}
	}
}

func TestCephFSCapSnapReconnectCarriesStateBeforeACK(t *testing.T) {
	a, writes := preparedCapSnapAgent(t)
	if err := a.prepareCapSnaps(17); err != nil {
		t.Fatal(err)
	}
	// Carry unsent head work too, without executing its callback at handoff.
	headWrites := 0
	a.dirty[17] = &cephfsDirtyFile{Ino: 17, Dirty: cephCapFileWR, Flushing: cephCapFileWR, Buffered: true, Follows: 4,
		WriteBack: func() error { headWrites++; return nil }, Events: []cephfsDirtyEvent{{Kind: "buffered"}}}
	a.caps[18] = &cephfsCap{Ino: 18, CapID: 99, Released: true}
	b := newCephFSCapAgentFrom(context.Background(), nil, a)
	key := cephfsFlushSnapKey{17, 2}
	s := b.capSnaps[key]
	if s.Transaction != 5 || s.Caps.OldestFlushTID != 4 || s.Caps.SnapFollows != 2 || s.Caps.MigrateSeq != 0 || s.Caps.Seq != 0 || s.Caps.IssueSeq != 0 ||
		s.Caps.Size != 20 || s.Caps.MTime.Seconds != 200 || s.writeBack != nil || b.flushTID != 6 || b.flushing[4] != 18 || b.barrier != 8 || b.caps[18] != nil {
		t.Fatal("reconnect changed a retained generation or omitted outstanding state", s, b.flushing)
	}
	if c := b.caps[17]; c.Seq != 0 || c.IssueSeq != 0 || c.MSeq != 0 || c.Issued != c.Implemented || c.Open[cephFileModeRDWR] != 1 {
		t.Fatal("reconnect cap state differs", c)
	}
	// Every mutable payload belongs to the new consumer, including state
	// retained for later writes rather than only evidence snapshots.
	s.Context.Snapshots[0] = 88
	for _, blob := range [][]byte{s.Caps.SnapTrace, s.Caps.Xattrs, s.Caps.Flock, s.Caps.InlineData, s.Caps.FscryptAuth, s.Caps.FscryptFile} {
		blob[0] = 99
	}
	r := b.realms[1]
	r.Snaps[0], r.PriorParentSnaps[0] = 66, 77
	b.dirty[17].Events[0].Kind = "changed"
	b.caps[17].Open[cephFileModeRDWR] = 9
	b.caps[17].FscryptAuth[0], b.caps[17].FscryptFile[0] = 99, 99
	if a.capSnaps[key].Context.Snapshots[0] != 2 || a.capSnaps[key].Caps.Xattrs[0] != 2 || a.capSnaps[key].Caps.FscryptFile[0] != 6 ||
		a.realms[1].Snaps[0] != 4 || a.realms[1].PriorParentSnaps[0] != 1 || a.dirty[17].Events[0].Kind != "buffered" || a.caps[17].Open[cephFileModeRDWR] != 1 || a.caps[17].FscryptAuth[0] != 11 {
		t.Fatal("continuing agent aliases old mutable state")
	}
	// The exact expected ACKs are installed even before a consumer starts.
	ack := cephmsgr.MDSCaps{Ino: 17, SnapFollows: 3, Dirty: cephCapFileWR}
	if b.consumeFlushSnapLocked(&ack, 5, time.Now()) || !b.consumeFlushSnapLocked(&ack, 6, time.Now()) || len(b.flushSnaps) != 1 || len(a.flushSnaps) != 2 {
		t.Fatal("new agent lost or aliased exact ACK expectations")
	}
	if err := b.dirty[17].WriteBack(); err != nil || headWrites != 1 || !reflect.DeepEqual(*writes, []uint64{2, 3}) {
		t.Fatal("handoff lost the head callback or repeated completed snapshot writes", err, writes)
	}
}

func TestCephFSCapSnapReconnectUsesLatestRealmAndOldestFollows(t *testing.T) {
	a, _ := preparedCapSnapAgent(t)
	a.capSnaps[cephfsFlushSnapKey{17, 1}] = &cephfsCapSnap{Ino: 17, Acked: time.Now()}
	stale := map[uint64]cephmsgr.MDSSnapRealm{1: {Ino: 1, Seq: 1, Parent: 99}}
	caps, realms, err := a.reconnectState(stale)
	if err != nil || len(caps) != 1 || caps[0].SnapFollows != 2 || len(realms) != 1 || realms[0].Seq != 4 || realms[0].Parent != 0 {
		t.Fatal("reconnect omitted pending follows or used stale realm", caps, realms, err)
	}
	a.capSnaps[cephfsFlushSnapKey{17, 2}].Acked = time.Now()
	caps, _, err = a.reconnectState(nil)
	if err != nil || caps[0].SnapFollows != 3 {
		t.Fatal("acknowledged predecessor remained in reconnect", caps, err)
	}
}

func TestCephFSCapSnapPreparedFirstSendAndFailureBarrier(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "uncertain"}[fail], func(t *testing.T) {
			a, writes := preparedCapSnapAgent(t)
			calls := 0
			failure := errors.New("uncertain metadata admission")
			send := func(_ context.Context, m cephmsgr.MDSCaps, tid uint64) error {
				calls++
				if tid != uint64(4+calls) || m.SnapFollows != uint64(1+calls) || m.OldestFlushTID != 4 {
					t.Fatal("send changed identity or order", m, tid)
				}
				if fail {
					return failure
				}
				return nil
			}
			if err := a.sendPreparedCapSnapsWith(17, send); err == nil || calls != 0 || len(*writes) != 0 {
				t.Fatal("unprepared state admitted metadata", err, calls)
			}
			if err := a.prepareCapSnaps(17); err != nil {
				t.Fatal(err)
			}
			for attempt := range 2 {
				err := a.sendPreparedCapSnapsWith(17, send)
				if fail && (err == nil || attempt == 0 && !errors.Is(err, failure)) || !fail && err != nil {
					t.Fatal("first send result differs", err)
				}
			}
			if want := map[bool]int{false: 2, true: 1}[fail]; calls != want || len(*writes) != 2 {
				t.Fatal("repeat send replayed metadata/data or crossed the failure barrier", calls, writes)
			}
		})
	}
}

func TestCephFSCapSnapDrainKeepsInterleavedFailureBarrier(t *testing.T) {
	a, writes := preparedCapSnapAgent(t)
	failure := errors.New("uncertain older cap send")
	sends := 0
	send := func(context.Context, cephmsgr.MDSCaps, uint64) error { sends++; return failure }
	for attempt := range 2 {
		err := a.drainCapSnapsWith(17, send)
		if err == nil || attempt == 0 && !errors.Is(err, failure) || sends != 1 || !reflect.DeepEqual(*writes, []uint64{2}) || a.capSnaps[cephfsFlushSnapKey{17, 3}].Transaction != 0 {
			t.Fatal("drain wrote a newer generation or retried an uncertain send", err, sends, writes)
		}
	}
}

func TestCephFSCapSnapFirstSendUsesCurrentEnvelope(t *testing.T) {
	a, writes := preparedCapSnapAgent(t)
	if err := a.prepareCapSnaps(17); err != nil {
		t.Fatal(err)
	}
	b := newCephFSCapAgentFrom(context.Background(), nil, a)
	b.caps[17].Realm, b.caps[17].MSeq, b.barrier = 33, 9, 10
	calls := 0
	if err := b.sendPreparedCapSnapsWith(17, func(_ context.Context, m cephmsgr.MDSCaps, tid uint64) error {
		calls++
		if m.Realm != 33 || m.MigrateSeq != 9 || m.OSDEpochBarrier != 10 || m.Seq != 0 || m.IssueSeq != 0 || m.CapID != 0 ||
			tid != uint64(4+calls) || m.OldestFlushTID != 4 || m.SnapFollows != uint64(1+calls) || m.Size != uint64((1+calls)*10) {
			t.Fatal("new envelope changed frozen snapshot metadata or flush identity", m, tid)
		}
		// Even a test sender owns its argument, not the retained generation.
		m.Xattrs[0] = 99
		return nil
	}); err != nil || calls != 2 || len(*writes) != 2 || b.capSnaps[cephfsFlushSnapKey{17, 2}].Caps.Xattrs[0] != 2 {
		t.Fatal("first send repeated data writes or aliased prepared bytes", err, calls, writes)
	}
}

func TestCephFSCapSnapUnpreparedWritebackSurvivesHandoff(t *testing.T) {
	a, writes := preparedCapSnapAgent(t)
	b := newCephFSCapAgentFrom(context.Background(), nil, a)
	if err := b.prepareCapSnaps(17); err != nil || !reflect.DeepEqual(*writes, []uint64{2, 3}) {
		t.Fatal("handoff lost unstarted writeback", err, writes)
	}
	if !a.capSnaps[cephfsFlushSnapKey{17, 2}].Buffered || a.capSnaps[cephfsFlushSnapKey{17, 2}].Transaction != 0 {
		t.Fatal("new preparation mutated the stopped agent")
	}
}

func capSnapReplayStatus() cephmsgr.MDSSessionStatus {
	return cephmsgr.MDSSessionStatus{State: cephmsgr.MDSSessionOpen, Generation: 12, ReconnectedFrom: 11}
}

func admitCapSnapsForReplay(t *testing.T) (*cephfsCapAgent, *[]uint64) {
	t.Helper()
	a, writes := preparedCapSnapAgent(t)
	if err := a.drainCapSnapsWith(17, func(context.Context, cephmsgr.MDSCaps, uint64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	for _, s := range a.capSnaps {
		s.SentGeneration = 11 // Synthetic original-session admission seam.
	}
	return newCephFSCapAgentFrom(context.Background(), nil, a), writes
}

func TestCephFSCapSnapMixedResubmissionAndFirstSend(t *testing.T) {
	a, writes := preparedCapSnapAgent(t)
	if err := a.prepareCapSnaps(17); err != nil {
		t.Fatal(err)
	}
	firstKey := cephfsFlushSnapKey{17, 2}
	if err := a.sendPreparedCapSnap(firstKey, func(context.Context, cephmsgr.MDSCaps, uint64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	a.capSnaps[firstKey].SentGeneration = 11
	old := cloneCephFSCapSnap(a.capSnaps[firstKey])
	b := newCephFSCapAgentFrom(context.Background(), nil, a)
	b.caps[17].Realm, b.caps[17].MSeq, b.barrier = 33, 8, 19
	replays := 0
	send := func(_ context.Context, m cephmsgr.MDSCaps, tid uint64) error {
		replays++
		want := cloneCephFSCaps(old.Caps)
		want.Realm, want.MigrateSeq, want.OSDEpochBarrier = 33, 8, 19
		if tid != old.Transaction || !reflect.DeepEqual(m, want) {
			t.Fatal("explicit replay changed original metadata/identity", m, want, tid)
		}
		m.Xattrs[0] = 99
		return nil
	}
	if err := b.resubmitCapSnapsWith([]uint64{5}, capSnapReplayStatus(), send); err != nil || replays != 1 {
		t.Fatal("explicit replay failed", err, replays)
	}
	if err := b.resubmitCapSnapsWith([]uint64{5}, capSnapReplayStatus(), send); err == nil || replays != 1 {
		t.Fatal("same new generation selected a flush twice", err, replays)
	}
	s := b.capSnaps[firstKey]
	if s.Sent != old.Sent || s.SentGeneration != old.SentGeneration || s.Error != old.Error || s.Transaction != old.Transaction || s.Caps.OldestFlushTID != old.Caps.OldestFlushTID || s.Caps.Xattrs[0] != 2 || len(s.Resubmissions) != 1 {
		t.Fatal("explicit replay erased original admission evidence", s, old)
	}
	attempt := s.Resubmissions[0]
	if attempt.Generation != 12 || attempt.ReconnectedFrom != 11 || attempt.Selected.IsZero() || attempt.Started.Before(attempt.Selected) || attempt.Sent.Before(attempt.Started) || attempt.Error != "" {
		t.Fatal("explicit replay attempt evidence differs", attempt)
	}
	firstSends := 0
	firstSend := func(_ context.Context, m cephmsgr.MDSCaps, tid uint64) error {
		firstSends++
		if tid != 6 || m.SnapFollows != 3 {
			t.Fatal("normal sender replayed A instead of first sending B", m, tid)
		}
		return nil
	}
	if err := b.sendPreparedCapSnapsWith(17, firstSend); err != nil || firstSends != 1 || !reflect.DeepEqual(*writes, []uint64{2, 3}) {
		t.Fatal("mixed flow repeated old data or omitted B", err, firstSends, writes)
	}
	if err := b.drainCapSnapsWith(17, firstSend); err != nil || firstSends != 1 || replays != 1 {
		t.Fatal("ordinary drain automatically replayed admitted generations", err, firstSends, replays)
	}
	// Evidence and another agent continuation must own attempt slices.
	evidence := cloneCephFSCapSnap(s)
	evidence.Resubmissions[0].Error = "mutated evidence"
	c := newCephFSCapAgentFrom(context.Background(), nil, b)
	c.capSnaps[firstKey].Resubmissions[0].Error = "mutated continuation"
	if s.Resubmissions[0].Error != "" || len(a.capSnaps[firstKey].Resubmissions) != 0 {
		t.Fatal("resubmission evidence aliases live or old state")
	}
}

func TestCephFSCapSnapResubmissionPrevalidatesWholeChoice(t *testing.T) {
	for _, test := range []struct {
		name string
		tids []uint64
		edit func(*cephfsCapAgent, *cephmsgr.MDSSessionStatus)
	}{
		{name: "empty"},
		{name: "duplicate", tids: []uint64{5, 5}},
		{name: "reversed", tids: []uint64{6, 5}},
		{name: "unknown-second", tids: []uint64{5, 99}},
		{name: "unadmitted-second", tids: []uint64{5, 6}, edit: func(a *cephfsCapAgent, _ *cephmsgr.MDSSessionStatus) {
			a.capSnaps[cephfsFlushSnapKey{17, 3}].Sent = time.Time{}
		}},
		{name: "acked-second", tids: []uint64{5, 6}, edit: func(a *cephfsCapAgent, _ *cephmsgr.MDSSessionStatus) {
			a.capSnaps[cephfsFlushSnapKey{17, 3}].Acked = time.Now()
		}},
		{name: "old-error", tids: []uint64{5, 6}, edit: func(a *cephfsCapAgent, _ *cephmsgr.MDSSessionStatus) {
			a.capSnaps[cephfsFlushSnapKey{17, 3}].Error = "uncertain original admission"
		}},
		{name: "missing-expected", tids: []uint64{5, 6}, edit: func(a *cephfsCapAgent, _ *cephmsgr.MDSSessionStatus) { delete(a.flushSnaps, cephfsFlushSnapKey{17, 3}) }},
		{name: "pending-data", tids: []uint64{5, 6}, edit: func(a *cephfsCapAgent, _ *cephmsgr.MDSSessionStatus) {
			a.capSnaps[cephfsFlushSnapKey{17, 3}].Buffered = true
		}},
		{name: "not-open", tids: []uint64{5}, edit: func(_ *cephfsCapAgent, s *cephmsgr.MDSSessionStatus) { s.State = cephmsgr.MDSSessionLost }},
		{name: "not-reconnected", tids: []uint64{5}, edit: func(_ *cephfsCapAgent, s *cephmsgr.MDSSessionStatus) { s.ReconnectedFrom = 0 }},
		{name: "same-generation", tids: []uint64{5}, edit: func(_ *cephfsCapAgent, s *cephmsgr.MDSSessionStatus) { s.Generation = 11 }},
		{name: "wrong-predecessor", tids: []uint64{5}, edit: func(_ *cephfsCapAgent, s *cephmsgr.MDSSessionStatus) { s.ReconnectedFrom = 10 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, writes := admitCapSnapsForReplay(t)
			status := capSnapReplayStatus()
			if test.edit != nil {
				test.edit(a, &status)
			}
			calls := 0
			err := a.resubmitCapSnapsWith(test.tids, status, func(context.Context, cephmsgr.MDSCaps, uint64) error { calls++; return nil })
			if err == nil || calls != 0 || len(*writes) != 2 {
				t.Fatal("invalid choice admitted a prefix or data write", err, calls, writes)
			}
			for _, s := range a.capSnaps {
				if len(s.Resubmissions) != 0 {
					t.Fatal("invalid choice reserved part of the selection", s)
				}
			}
		})
	}
}

func TestCephFSCapSnapResubmissionFailureBlocksAutomaticReplay(t *testing.T) {
	a, writes := admitCapSnapsForReplay(t)
	failure := errors.New("uncertain explicit resubmission")
	calls := 0
	send := func(context.Context, cephmsgr.MDSCaps, uint64) error { calls++; return failure }
	if err := a.resubmitCapSnapsWith([]uint64{5, 6}, capSnapReplayStatus(), send); !errors.Is(err, failure) || calls != 1 {
		t.Fatal("failed prefix did not stop selected replay", err, calls)
	}
	first, second := a.capSnaps[cephfsFlushSnapKey{17, 2}], a.capSnaps[cephfsFlushSnapKey{17, 3}]
	if first.Error != "" || second.Error != "" || first.Sent.IsZero() || second.Sent.IsZero() ||
		first.Resubmissions[0].Error != failure.Error() || first.Resubmissions[0].Started.IsZero() || !first.Resubmissions[0].Sent.IsZero() || !second.Resubmissions[0].Started.IsZero() {
		t.Fatal("failed replay overwrote original evidence or sent the later choice", first, second)
	}
	if err := a.resubmitCapSnapsWith([]uint64{6}, capSnapReplayStatus(), send); err == nil || calls != 1 {
		t.Fatal("unattempted selected suffix became implicitly retryable", err, calls)
	}
	if err := a.sendPreparedCapSnapsWith(17, send); err == nil || calls != 1 {
		t.Fatal("normal sender crossed explicit replay failure", err, calls)
	}
	if err := a.drainCapSnapsWith(17, send); err == nil || calls != 1 || !reflect.DeepEqual(*writes, []uint64{2, 3}) {
		t.Fatal("normal drain retried an uncertain replay or data write", err, calls, writes)
	}
}
