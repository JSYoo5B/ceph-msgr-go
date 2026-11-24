package integration_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// rbdJournalHeldSample is a consistent record of one held-request run in
// the given mode with synthetic values: P is osd.0 and osd.2 the paused
// replica; the setup's tag is 1, the subject's 2 and native librbd's replay
// allocates 3. In the erasure-hold mode the journal object is in the
// erasure-coded pool 4, P leads its PG and osd.2 is the paused shard, the
// header's primary is osd.1 and the append travels on the subject's second
// connection.
func rbdJournalHeldSample(mode string) *rbdJournalLostAppendRecord {
	n := func(v int) *int { return &v }
	str := func(v string) *string { return &v }
	u64 := func(v uint64) *uint64 { return &v }
	hash := func(c string) string { return strings.Repeat(c, 64) }
	const subject, cookie, tag, tid = 4866, 77, 2, 10
	token := strings.Repeat("a1", 16)
	pool := "ceph-msgr-rbd-" + token
	erasure := mode == rbdJournalVariantErasureHold
	replica, change, dead := mode == rbdJournalVariantReplicaHold || erasure, mode == rbdJournalVariantHeldPrimaryChange, mode == rbdJournalVariantDeadPrimary
	e := rbdJournalLostAppendEntry{Tag: tag, EntryTID: 0, Object: 0, ObjectName: "journal_data.3.ab.0", Offset: 2110, Length: 4198, SHA256: hash("e"),
		ImageOffset: 40 << 10, ImageLength: 4 << 10, Timestamp: cephmsgr.OSDTime{Seconds: 1700000000, Nanoseconds: 5000}}
	setupSizes := []uint64{2110, 2110, 1086, 93, 0, 0, 0, 0}
	setupSHA := []string{hash("0"), hash("1"), hash("2"), hash("3"), hash("4"), hash("4"), hash("4"), hash("4")}
	setupCommit := []rbdJournalPosition{{3, 1, 3}, {2, 1, 2}, {1, 1, 1}, {0, 1, 0}}
	other := []rbdJournalInspectEntry{{Tag: 1, Type: "AioWrite", Length: 2048}, {Tag: 1, Type: "AioDiscard", Offset: 196608, Length: 65536}}
	view := func(action string) *rbdJournalLostAppendView {
		return &rbdJournalLostAppendView{Action: action, ObjectSizes: slices.Clone(setupSizes), ObjectSHA256: slices.Clone(setupSHA), ObjectExists: []bool{true, true, true, true, false, false, false, false},
			ImageHeadSHA256: hash("a"), ImageCommit: slices.Clone(setupCommit), NextTag: tag + 1, LastTag: rbdJournalLostAppendTag{TID: tag}, ObservedUTC: "2026-11-23T13:33:50Z"}
	}
	locked := func(v *rbdJournalLostAppendView, watch bool) *rbdJournalLostAppendView {
		v.LockOwner, v.LockCookie, v.SubjectWatcher, v.Watchers = fmt.Sprintf("client.%d", subject), "auto 77", watch, 1
		return v
	}
	withEntry := func(v *rbdJournalLostAppendView) *rbdJournalLostAppendView {
		v.ObjectSizes[0], v.ObjectSHA256[0] = uint64(e.Offset+e.Length), hash("x")
		v.PrefixSHA256, v.EntrySHA256, v.TrailingBytes = str(setupSHA[0]), str(e.SHA256), u64(0)
		return v
	}
	without := func(v *rbdJournalLostAppendView) *rbdJournalLostAppendView {
		v.PrefixSHA256 = str(setupSHA[0])
		return v
	}
	setup := view("setup")
	setup.LockFree, setup.NextTag, setup.LastTag = true, tag, rbdJournalLostAppendTag{TID: 1}
	setup.InspectTotal, setup.InspectErrors, setup.InspectOther = n(2), n(0), slices.Clone(other)
	r := &rbdJournalLostAppendRecord{Token: token, Pool: pool, PoolID: 3, SubjectGlobalID: subject, ObserverGlobalID: 4800, ConnectionMode: "crc", Image: "ceph-msgr-journal-" + token,
		ImageID: "ab", JournalHeader: "journal.ab", WatchCookie: cookie, LockCookie: "auto 77", Entry: e, ExpectedHeadBefore: hash("a"), ExpectedHeadAfter: hash("b"),
		ExpectedCommitAfter: []rbdJournalPosition{{0, tag, 0}, {3, 1, 3}, {2, 1, 2}, {1, 1, 1}}, Timings: map[string]string{},
		Views: map[string]*rbdJournalLostAppendView{"setup": setup, "observe-locked": locked(without(view("observe-locked")), true)}}
	_, order := rbdJournalHeldPlan(mode)
	r.Order = slices.Clone(order)
	objects := []string{"rbd_header.ab", e.ObjectName, "journal.ab"}
	placedPool, placedID, pgid := pool, int64(3), "3.0"
	if erasure {
		objects, placedPool, placedID, pgid = []string{e.ObjectName}, "ceph-msgr-rbd2-"+token, 4, "4.0"
	}
	placements := func(epoch uint32, acting []int32) []nativeRBDJournalPlacement {
		var set []nativeRBDJournalPlacement
		for _, o := range objects {
			up := slices.Clone(acting)
			if len(acting) == 3 && acting[2] == 0 {
				up = acting[:2]
			}
			set = append(set, nativeRBDJournalPlacement{Epoch: epoch, Pool: placedPool, PoolID: placedID, Object: o, RawPGID: "3.e3", PGID: pgid, Up: up, UpPrimary: up[0], Acting: slices.Clone(acting),
				ActingPrimary: acting[0]})
		}
		return set
	}
	query := func(state string, epoch uint32, acting []int32, last string, interval uint32, peers ...int32) *nativeRBDJournalPGQuery {
		q := &nativeRBDJournalPGQuery{State: state, Epoch: epoch, Up: slices.Clone(acting), Acting: slices.Clone(acting), LastUpdate: last, SameIntervalSince: interval, SameUpSince: interval,
			SamePrimarySince: interval, LastEpochStarted: interval + 1}
		for _, p := range peers {
			q.Peers = append(q.Peers, nativeRBDJournalPGPeer{Peer: p, LastUpdate: last})
		}
		if erasure {
			// Each member's shard is its acting index.
			index := func(osd int32) *int32 { i := int32(slices.Index(acting, osd)); return &i }
			q.Shard = index(acting[0])
			for i := range q.Peers {
				q.Peers[i].Shard = index(q.Peers[i].Peer)
			}
		}
		return q
	}
	clean := []int32{0, 1, 2}
	named := uint32(30)
	if change {
		named += rbdJournalForcedPendingEpochs
	}
	description := fmt.Sprintf("osd_op(client.%d.0:%d 3.e3 3:c7:::%s:head [call journal.append in=4198b] snapc 0=[] ondisk+write+returnvec e%d)", subject, tid, e.ObjectName, named)
	op := nativeRBDJournalHeldOp{OSD: 0, Description: description, Client: fmt.Sprintf("client.%d", subject), Transaction: tid, FlagPoint: "waiting for sub ops",
		Events: []string{"initiated", "header_read", "throttled", "all_read", "dispatched", "queued_for_pg", "reached_pg", "started", "waiting for subops from 1,2", "op_commit", "sub_op_commit_rec"}}
	last := "30'52"
	if erasure {
		op.FlagPoint, op.Events = "started", []string{"initiated", "header_read", "throttled", "all_read", "dispatched", "queued_for_pg", "reached_pg", "started", "sub_op_started",
			"sub_op_committed"}
	}
	if change {
		op.FlagPoint, op.Events, last = "no flag points reached", []string{"initiated", "header_read", "throttled", "all_read", "dispatched"}, "29'51"
	}
	h := &rbdJournalHeld{Mode: mode, Primary: 0, Replica: -1, GoActing: []int32{0, 1, 2}, SentAtEpoch: 30, NamedEpoch: named, SubmittedUTC: "2026-11-23T13:33:49.500000000Z", PendingAfterHeld: true, AppendConnection: 1,
		PingBefore: &rbdLockOwnerLossAttempt{Label: "watch_ping", Invocations: 1, Class: "applied"},
		Placed:     &nativeRBDJournalHeldRead{Objects: placements(30, clean), PGID: pgid, Query: query("active+clean", 30, clean, "29'51", 21, 1, 2), OSD: -1, Epoch: 30, Down: []int32{}, Settled: true, Polls: 1},
		Held: &nativeRBDJournalHeldRead{Objects: placements(30, clean), PGID: pgid, Query: query("active+clean", 30, clean, last, 21, 1, 2), OSD: 0, Ops: []nativeRBDJournalHeldOp{op},
			Epoch: 30, Down: []int32{}, Settled: true, Polls: 1}}
	r.Held = h
	submitted, _ := time.Parse(time.RFC3339Nano, h.SubmittedUTC)
	r.Gate = &testcluster.OSDReplyGateState{Decoded: true, Threshold: rbdJournalLostAppendThreshold, Connections: 1, LargeWrites: 1}
	outcome := &rbdJournalLostAppendOutcome{Label: "journal_append", CallerInvocations: 1, Submission: cephmsgr.OSDSubmission{Transaction: tid, GlobalID: subject, MapEpoch: named},
		RecordedUTC: "2026-11-23T13:33:51Z"}
	r.Outcome = outcome
	replay := withEntry(view("replay"))
	replay.LockFree, replay.LastTag, replay.NextTag, replay.ImageHeadSHA256, replay.ImageCommit = true, rbdJournalLostAppendTag{tag + 1, true, tag, 0}, tag+2, hash("b"), slices.Clone(r.ExpectedCommitAfter)
	replay.InspectTotal, replay.InspectErrors, replay.InspectOther = n(3), n(0), slices.Clone(other)
	replay.InspectOwned = []rbdJournalLostAppendEvent{rbdJournalEntryEvent(e)}
	r.Views["replay"] = replay
	handoff := func() {
		r.DialsBeforeClose, r.DialsTotal, r.Gate.Connections = n(1), n(2), 2
		h.Reconnect = &rbdLockOwnerLossAttempt{Label: "image_watch_reconnect", Invocations: 1, Class: "applied"}
		h.HandoffPrimary, h.LockerAddress, h.WatcherAddress = 1, "10.0.0.11:0/1690346225", "10.0.0.11:0/1690346225"
	}
	if replica || dead {
		h.Replica = 2
		h.Lease = &rbdJournalHeldLease{Daemon: "osd.2", Deadline: submitted.Add(13 * time.Second).UnixNano(), PausedAt: submitted.Add(-time.Second).UnixNano(),
			EndedAt: submitted.Add(2 * time.Second).UnixNano(), Reason: "resumed", LeftMS: 11000}
	}
	switch {
	case replica:
		outcome.Class, outcome.ReplyTransaction = "applied", tid
		r.DialsTotal = n(1)
		if erasure {
			h.JournalPool, h.JournalPoolID, h.HeaderPrimary, h.AppendConnection = placedPool, placedID, 1, 2
			r.DialsTotal, r.Gate.Connections = n(2), 2
		}
		h.Settled = &nativeRBDJournalHeldRead{Objects: placements(30, clean), PGID: pgid, Query: query("active+clean", 30, clean, "30'52", 21, 1, 2), OSD: -1, Epoch: 30, Down: []int32{},
			Settled: true, Polls: 1}
		r.Views["observe-answered"] = locked(withEntry(view("observe-answered")), true)
	case dead:
		outcome.Class, outcome.UnknownOutcome, outcome.Error, outcome.RecordedUTC = "uncertain", true, "ceph: request outcome unknown: EOF", "2026-11-23T13:33:51Z"
		h.Lease.EndedAt = submitted.Add(3 * time.Second).UnixNano()
		h.Kill = &rbdJournalHeldKill{Daemon: "osd.0", ExitCode: rbdJournalHeldKilledExit, RequestedUTC: "2026-11-23T13:33:50.500000000Z"}
		h.Lost = &nativeRBDJournalHeldRead{Objects: placements(32, []int32{1, 2}), PGID: "3.0", Query: query("active+undersized+degraded", 32, []int32{1, 2}, "30'52", 31, 2), OSD: -1,
			Epoch: 32, Down: []int32{0}, Settled: true, Polls: 2}
		r.Views["observe-recorded"] = locked(withEntry(view("observe-recorded")), false)
		handoff()
	case change:
		outcome.Class, outcome.UnknownOutcome, outcome.Canceled, outcome.Error = "uncertain", true, true, "ceph: request outcome unknown: context canceled"
		pc := &nativeRBDJournalPrimaryChange{Out: 0, Commands: []string{"osd out 0"}, Polls: 3, ElapsedMS: 3000}
		before, after := placements(30, clean), placements(32, []int32{1, 2, 0})
		for i, o := range objects {
			pc.Objects = append(pc.Objects, struct {
				Object string                    `json:"object"`
				Moved  bool                      `json:"moved"`
				Before nativeRBDJournalPlacement `json:"before"`
				After  nativeRBDJournalPlacement `json:"after"`
			}{o, true, before[i], after[i]})
		}
		h.PrimaryChange, h.PendingAfterChange = pc, true
		h.Advance = &rbdLockOwnerLossAdvance{Pool: pool, Flag: "nodeep-scrub", EpochBefore: 32, EpochTarget: 40, EpochAfter: 40,
			OpsBefore: []rbdLockOwnerLossOp{{OSD: 0, Description: description, Client: op.Client, Transaction: tid}}, OpsAfter: []rbdLockOwnerLossOp{}}
		for i := range 8 {
			h.Advance.Epochs = append(h.Advance.Epochs, uint32(33+i))
			h.Advance.Commands = append(h.Advance.Commands, fmt.Sprintf("osd pool set %s nodeep-scrub %t", pool, i%2 == 0))
		}
		moved := []int32{1, 2, 0}
		h.Settled = &nativeRBDJournalHeldRead{Objects: placements(40, moved), PGID: "3.0", Query: query("active+clean+remapped", 40, moved, "29'51", 31, 2, 0), OSD: -1, Epoch: 40,
			Down: []int32{}, Settled: true, Polls: 1}
		h.Settled.Query.Up = []int32{1, 2}
		r.Views["change-primary"] = locked(without(view("change-primary")), false)
		r.Views["observe-recorded"] = locked(without(view("observe-recorded")), false)
		replay = without(view("replay"))
		replay.LockFree, replay.LastTag, replay.NextTag = true, rbdJournalLostAppendTag{tag + 1, true, 1, 3}, tag+2
		replay.InspectTotal, replay.InspectErrors, replay.InspectOther, replay.InspectOwned = n(2), n(0), slices.Clone(other), []rbdJournalLostAppendEvent{}
		r.Views["replay"] = replay
		handoff()
	}
	return r
}

// TestRBDJournalHeldQueryProblem judges PG queries field by field. The first
// case is the held read of a secure dead-primary run: the OSDMap reads around
// it were at epoch 29 and the PG's own query still at 28, with the PG clean in
// the interval the lock was taken in; the claims once refused it.
func TestRBDJournalHeldQueryProblem(t *testing.T) {
	clean := func() *nativeRBDJournalPGQuery {
		return &nativeRBDJournalPGQuery{State: "active+clean", Epoch: 28, Up: []int32{0, 1, 2}, Acting: []int32{0, 1, 2}, LastUpdate: "28'52", SameIntervalSince: 20, SameUpSince: 20,
			SamePrimarySince: 20, LastEpochStarted: 21}
	}
	if problem := rbdJournalHeldQueryProblem(clean(), "active+clean", []int32{0, 1, 2}); problem != "" {
		t.Fatal("a clean query refused:", problem)
	}
	for want, change := range map[string]func(q *nativeRBDJournalPGQuery){
		"state is":                 func(q *nativeRBDJournalPGQuery) { q.State = "active+degraded" },
		"acting set is":            func(q *nativeRBDJournalPGQuery) { q.Acting = []int32{1, 2, 0} },
		"up set is":                func(q *nativeRBDJournalPGQuery) { q.Up = []int32{1, 2} },
		"same_interval_since 29":   func(q *nativeRBDJournalPGQuery) { q.SameIntervalSince = 29 },
		"same_interval_since 0":    func(q *nativeRBDJournalPGQuery) { q.SameIntervalSince = 0 },
		"same_primary_since 21 is": func(q *nativeRBDJournalPGQuery) { q.SamePrimarySince = 21 },
		"names no last update":     func(q *nativeRBDJournalPGQuery) { q.LastUpdate = "" },
	} {
		q := clean()
		change(q)
		if problem := rbdJournalHeldQueryProblem(q, "active+clean", []int32{0, 1, 2}); !strings.HasPrefix(problem, want) {
			t.Errorf("query problem %q, want one starting %q", problem, want)
		}
	}
}

// TestRBDJournalHeldClaims feeds the held-request claims contradictory
// records: each consistent sample passes at every stage, and each change
// fails at the stage named.
func TestRBDJournalHeldClaims(t *testing.T) {
	// judge judges r at stage with the steps completed up to it, taken from
	// full (the record's own steps, possibly changed).
	judge := func(r *rbdJournalLostAppendRecord, full []string, stage string) []string {
		_, order := rbdJournalHeldPlan(r.Held.Mode)
		r.Order = slices.Clone(full[:min(len(full), slices.Index(order, stage)+1)])
		return rbdJournalLostAppendClaims(r, stage)
	}
	for _, mode := range []string{rbdJournalVariantReplicaHold, rbdJournalVariantHeldPrimaryChange, rbdJournalVariantDeadPrimary, rbdJournalVariantErasureHold} {
		stages, _ := rbdJournalHeldPlan(mode)
		for _, stage := range stages {
			r := rbdJournalHeldSample(mode)
			if problems := judge(r, r.Order, stage); len(problems) != 0 {
				t.Fatalf("consistent %s record refused at %s: %v", mode, stage, problems)
			}
		}
	}
	type mutation struct {
		mode, stage string
		change      func(r *rbdJournalLostAppendRecord)
	}
	op := func(r *rbdJournalLostAppendRecord) *nativeRBDJournalHeldOp { return &r.Held.Held.Ops[0] }
	all := []string{rbdJournalVariantReplicaHold, rbdJournalVariantHeldPrimaryChange, rbdJournalVariantDeadPrimary}
	cases := map[string]mutation{
		// Held, in every mode.
		"placed on another primary":   {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Primary = 1 }},
		"subject placed another set":  {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.GoActing = []int32{0, 2, 1} }},
		"PG not clean before":         {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Placed.Query.State = "active+recovering" }},
		"an OSD down before":          {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Placed.Down = []int32{2} }},
		"a peer behind before":        {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Placed.Query.Peers[0].LastUpdate = "20'1" }},
		"object in another PG":        {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Placed.Objects[2].PGID = "3.1" }},
		"interval changed while held": {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Held.Query.SameIntervalSince = 29 }},
		"no operation listed":         {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Held.Ops = nil }},
		"two operations listed": {"*", "observe-held", func(r *rbdJournalLostAppendRecord) {
			r.Held.Held.Ops = append(r.Held.Held.Ops, r.Held.Held.Ops[0])
		}},
		"operations of another OSD": {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Held.OSD = 1 }},
		"held read not settled":     {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Held.Settled = false }},
		"operation on another object": {"*", "observe-held", func(r *rbdJournalLostAppendRecord) {
			op(r).Description = strings.Replace(op(r).Description, "ab.0", "ab.1", 1)
		}},
		"operation of another epoch": {"*", "observe-held", func(r *rbdJournalLostAppendRecord) {
			op(r).Description = strings.Replace(op(r).Description, " e3", " e2", 1)
		}},
		"operation not an append": {"*", "observe-held", func(r *rbdJournalLostAppendRecord) {
			op(r).Description = strings.Replace(op(r).Description, "append", "guard_append", 1)
		}},
		"caller answered before":        {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.PendingAfterHeld = false }},
		"a second large write":          {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Gate.LargeWrites = 2 }},
		"a reply withheld":              {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Gate.Bound = true }},
		"steps out of order":            {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Order[4], r.Order[5] = r.Order[5], r.Order[4] }},
		"watch not renewed":             {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.PingBefore = nil }},
		"append placed at a later map":  {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.SentAtEpoch = 31 }},
		"append placed at an older map": {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.SentAtEpoch = 29 }},
		"lock not taken":                {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Views["observe-locked"].LockOwner = "client.1" }},
		"outcome of another transaction": {"*", "replay", func(r *rbdJournalLostAppendRecord) {
			r.Outcome.Submission.Transaction, r.Outcome.ReplyTransaction = 11, map[bool]uint64{true: 11}[r.Outcome.ReplyTransaction != 0]
		}},
		"two invocations": {"*", "replay", func(r *rbdJournalLostAppendRecord) { r.Outcome.CallerInvocations = 2 }},
		"replay lists one journal object fewer": {"*", "replay", func(r *rbdJournalLostAppendRecord) {
			v := r.Views["replay"]
			v.ObjectSizes, v.ObjectSHA256, v.ObjectExists = v.ObjectSizes[:len(v.ObjectSizes)-1], v.ObjectSHA256[:len(v.ObjectSHA256)-1], v.ObjectExists[:len(v.ObjectExists)-1]
		}},
		"replay lists one journal object more": {"*", "replay", func(r *rbdJournalLostAppendRecord) {
			v := r.Views["replay"]
			v.ObjectSizes, v.ObjectSHA256, v.ObjectExists = append(v.ObjectSizes, 0), append(v.ObjectSHA256, strings.Repeat("4", 64)), append(v.ObjectExists, false)
		}},
		"locked image lists one object fewer": {"*", "observe-held", func(r *rbdJournalLostAppendRecord) {
			v := r.Views["observe-locked"]
			v.ObjectSizes, v.ObjectSHA256, v.ObjectExists = v.ObjectSizes[:7], v.ObjectSHA256[:7], v.ObjectExists[:7]
		}},
		"replay lists one digest fewer": {"*", "replay", func(r *rbdJournalLostAppendRecord) {
			v := r.Views["replay"]
			v.ObjectSHA256 = v.ObjectSHA256[:len(v.ObjectSHA256)-1]
		}},
		"replay lists one existence fewer": {"*", "replay", func(r *rbdJournalLostAppendRecord) {
			v := r.Views["replay"]
			v.ObjectExists = v.ObjectExists[:len(v.ObjectExists)-1]
		}},
		"replay twice":           {"*", "replay", func(r *rbdJournalLostAppendRecord) { r.Views["replay"].LockFree = false }},
		"replay of another head": {"*", "replay", func(r *rbdJournalLostAppendRecord) { r.Views["replay"].ImageHeadSHA256 = strings.Repeat("f", 64) }},
		// Replica hold.
		"replica not waited for": {rbdJournalVariantReplicaHold, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).Events = op(r).Events[:8] }},
		"replica commit not yet": {rbdJournalVariantReplicaHold, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).Events = op(r).Events[:10] }},
		"local commit missing": {rbdJournalVariantReplicaHold, "observe-held", func(r *rbdJournalLostAppendRecord) {
			op(r).Events = slices.DeleteFunc(op(r).Events, func(e string) bool { return e == "op_commit" })
		}},
		"PG query behind the map reads": {rbdJournalVariantDeadPrimary, "", func(r *rbdJournalLostAppendRecord) {
			// The secure dead-primary run's held read: the OSDMap at 29 around
			// a PG query still at 28.
			for i := range r.Held.Held.Objects {
				r.Held.Held.Objects[i].Epoch = 31
			}
			r.Held.Held.Epoch, r.Held.Lost.Query.Epoch = 31, 32
			r.Held.Held.Query.Epoch = 30
		}},
		"PG caught up with the op's map": {rbdJournalVariantDeadPrimary, "", func(r *rbdJournalLostAppendRecord) {
			// The secure primary-restart run's entry 0: the PG reached the
			// op's epoch after the op reached it.
			op(r).Events = slices.Insert(op(r).Events, 7, "op must wait for map", "reached_pg")
		}},
		"a map delay without its return": {rbdJournalVariantReplicaHold, "observe-held", func(r *rbdJournalLostAppendRecord) {
			op(r).Events = slices.Insert(op(r).Events, 7, "op must wait for map")
		}},
		"a map delay before the PG": {rbdJournalVariantReplicaHold, "observe-held", func(r *rbdJournalLostAppendRecord) {
			op(r).Events = slices.Insert(op(r).Events, 5, "op must wait for map", "reached_pg")
		}},
		"too many map delays": {rbdJournalVariantDeadPrimary, "observe-held", func(r *rbdJournalLostAppendRecord) {
			for range rbdJournalHeldMapDelayLimit + 1 {
				op(r).Events = slices.Insert(op(r).Events, 7, "waiting_for_map not empty", "reached_pg")
			}
		}},
		"commits in either order": {rbdJournalVariantDeadPrimary, "", func(r *rbdJournalLostAppendRecord) {
			n := len(op(r).Events)
			op(r).Events[n-2], op(r).Events[n-1] = op(r).Events[n-1], op(r).Events[n-2]
		}},
		"replica wait flag missing": {rbdJournalVariantReplicaHold, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).FlagPoint = "commit sent; apply or cleanup" }},
		"held op skipped a step":    {rbdJournalVariantDeadPrimary, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).Events = slices.Delete(op(r).Events, 5, 6) }},
		"an unknown event":          {rbdJournalVariantDeadPrimary, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).Events = append(op(r).Events, "done") }},
		"held op reached its PG": {rbdJournalVariantHeldPrimaryChange, "observe-held", func(r *rbdJournalLostAppendRecord) {
			op(r).Events = append(op(r).Events, "queued_for_pg", "reached_pg")
		}},
		"held op started":           {rbdJournalVariantHeldPrimaryChange, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).FlagPoint = "started" }},
		"held op committed locally": {rbdJournalVariantHeldPrimaryChange, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).Events = append(op(r).Events, "op_commit") }},
		"held op before dispatch":   {rbdJournalVariantHeldPrimaryChange, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).Events = op(r).Events[:3] }},
		"another op sampled": {rbdJournalVariantHeldPrimaryChange, "observe-recorded", func(r *rbdJournalLostAppendRecord) {
			r.Held.Advance.OpsBefore[0].Description = "osd_op(x)"
		}},
		"both replicas committed":  {rbdJournalVariantReplicaHold, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).Events = append(op(r).Events, "sub_op_commit_rec") }},
		"answered while held":      {rbdJournalVariantReplicaHold, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).Events = append(op(r).Events, "commit_sent") }},
		"another replica paused":   {rbdJournalVariantReplicaHold, "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Replica, r.Held.Lease.Daemon = 1, "osd.1" }},
		"answer refused":           {rbdJournalVariantReplicaHold, "observe-answered", func(r *rbdJournalLostAppendRecord) { r.Outcome.Class, r.Outcome.ReplyCode = "refused", -5 }},
		"answer of another reply":  {rbdJournalVariantReplicaHold, "observe-answered", func(r *rbdJournalLostAppendRecord) { r.Outcome.ReplyTransaction = 9 }},
		"answer unknown":           {rbdJournalVariantReplicaHold, "observe-answered", func(r *rbdJournalLostAppendRecord) { r.Outcome.Class, r.Outcome.UnknownOutcome = "uncertain", true }},
		"interval changed by then": {rbdJournalVariantReplicaHold, "observe-answered", func(r *rbdJournalLostAppendRecord) { r.Held.Settled.Query.SameIntervalSince = 31 }},
		"last update unmoved":      {rbdJournalVariantReplicaHold, "observe-answered", func(r *rbdJournalLostAppendRecord) { r.Held.Settled.Query.LastUpdate = "29'51" }},
		"replica marked down":      {rbdJournalVariantReplicaHold, "observe-answered", func(r *rbdJournalLostAppendRecord) { r.Held.Settled.Down = []int32{2} }},
		"lease expired":            {rbdJournalVariantReplicaHold, "observe-answered", func(r *rbdJournalLostAppendRecord) { r.Held.Lease.Reason = "expired" }},
		"paused after the append":  {rbdJournalVariantReplicaHold, "observe-answered", func(r *rbdJournalLostAppendRecord) { r.Held.Lease.PausedAt = r.Held.Lease.EndedAt - 1 }},
		"entry missing":            {rbdJournalVariantReplicaHold, "observe-answered", func(r *rbdJournalLostAppendRecord) { r.Views["observe-answered"].EntrySHA256 = nil }},
		"entry twice": {rbdJournalVariantReplicaHold, "observe-answered", func(r *rbdJournalLostAppendRecord) {
			r.Views["observe-answered"].TrailingBytes = new(uint64)
			*r.Views["observe-answered"].TrailingBytes = 4198
		}},
		"image moved": {rbdJournalVariantReplicaHold, "observe-answered", func(r *rbdJournalLostAppendRecord) {
			r.Views["observe-answered"].ImageHeadSHA256 = strings.Repeat("b", 64)
		}},
		"two connections": {rbdJournalVariantReplicaHold, "replay", func(r *rbdJournalLostAppendRecord) { r.Gate.Connections = 2 }},
		// Erasure hold: the journal object alone in the erasure-coded pool's
		// PG, a started op without a shard commit, and the append on the
		// connection to that PG's primary.
		"erasure: journal pool missing": {rbdJournalVariantErasureHold, "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.JournalPool = "" }},
		"erasure: journal pool is the owned pool": {rbdJournalVariantErasureHold, "observe-held", func(r *rbdJournalLostAppendRecord) {
			r.Held.JournalPool, r.Held.JournalPoolID = r.Pool, r.PoolID
		}},
		"erasure: object placed in the owned pool": {rbdJournalVariantErasureHold, "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Placed.Objects[0].PoolID = 3 }},
		"erasure: the headers followed too": {rbdJournalVariantErasureHold, "observe-held", func(r *rbdJournalLostAppendRecord) {
			r.Held.Held.Objects = append(r.Held.Held.Objects, r.Held.Held.Objects[0])
		}},
		"erasure: another shard paused":    {rbdJournalVariantErasureHold, "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Replica, r.Held.Lease.Daemon = 1, "osd.1" }},
		"erasure: a shard commit recorded": {rbdJournalVariantErasureHold, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).Events = append(op(r).Events, "sub_op_commit_rec") }},
		"erasure: replica wait flag": {rbdJournalVariantErasureHold, "observe-held", func(r *rbdJournalLostAppendRecord) {
			op(r).FlagPoint = rbdJournalHeldSubOpsFlag
		}},
		"erasure: committed before the answer": {rbdJournalVariantErasureHold, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).Events = append(op(r).Events, "op_commit") }},
		"erasure: not started":                 {rbdJournalVariantErasureHold, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).Events = op(r).Events[:7] }},
		"erasure: own shard not committed":     {rbdJournalVariantErasureHold, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).Events = op(r).Events[:9] }},
		"erasure: own shard not started": {rbdJournalVariantErasureHold, "observe-held", func(r *rbdJournalLostAppendRecord) {
			op(r).Events = slices.DeleteFunc(op(r).Events, func(e string) bool { return e == "sub_op_started" })
		}},
		"erasure: own shard committed twice": {rbdJournalVariantErasureHold, "observe-held", func(r *rbdJournalLostAppendRecord) {
			op(r).Events = append(op(r).Events, "sub_op_committed")
		}},
		// The decoder's shard binding, restated by the claims: valid
		// numbering of another member's shard is refused at every read.
		"erasure: primary on a peer's shard": {rbdJournalVariantErasureHold, "observe-held", func(r *rbdJournalLostAppendRecord) {
			*r.Held.Placed.Query.Shard = 1
		}},
		"erasure: peers' shards swapped": {rbdJournalVariantErasureHold, "observe-held", func(r *rbdJournalLostAppendRecord) {
			p := r.Held.Held.Query.Peers
			p[0].Shard, p[1].Shard = p[1].Shard, p[0].Shard
		}},
		"erasure: settled peer without its shard": {rbdJournalVariantErasureHold, "observe-answered", func(r *rbdJournalLostAppendRecord) {
			r.Held.Settled.Query.Peers[0].Shard = nil
		}},
		"replica: a shard in a replicated PG": {rbdJournalVariantReplicaHold, "observe-held", func(r *rbdJournalLostAppendRecord) {
			s := int32(1)
			r.Held.Placed.Query.Peers[0].Shard = &s
		}},
		"erasure: answer unknown": {rbdJournalVariantErasureHold, "observe-answered", func(r *rbdJournalLostAppendRecord) {
			r.Outcome.Class, r.Outcome.UnknownOutcome = "uncertain", true
		}},
		"erasure: entry missing":                     {rbdJournalVariantErasureHold, "observe-answered", func(r *rbdJournalLostAppendRecord) { r.Views["observe-answered"].EntrySHA256 = nil }},
		"erasure: append on the header's connection": {rbdJournalVariantErasureHold, "replay", func(r *rbdJournalLostAppendRecord) { r.Held.AppendConnection = 1 }},
		"erasure: one connection to two primaries": {rbdJournalVariantErasureHold, "replay", func(r *rbdJournalLostAppendRecord) {
			*r.DialsTotal, r.Gate.Connections = 1, 1
		}},
		"erasure: one primary, one connection": {rbdJournalVariantErasureHold, "", func(r *rbdJournalLostAppendRecord) {
			r.Held.HeaderPrimary, r.Held.AppendConnection, *r.DialsTotal, r.Gate.Connections = 0, 1, 1, 1
		}},
		"erasure: replay without it": {rbdJournalVariantErasureHold, "replay", func(r *rbdJournalLostAppendRecord) { r.Views["replay"].InspectOwned = nil }},
		"replay without it":          {rbdJournalVariantReplicaHold, "replay", func(r *rbdJournalLostAppendRecord) { r.Views["replay"].InspectOwned = nil }},
		// Dead primary.
		"kill exited cleanly":      {rbdJournalVariantDeadPrimary, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Held.Kill.ExitCode = 0 }},
		"another daemon killed":    {rbdJournalVariantDeadPrimary, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Held.Kill.Daemon = "osd.1" }},
		"outcome known":            {rbdJournalVariantDeadPrimary, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Outcome.Class, r.Outcome.UnknownOutcome = "applied", false }},
		"outcome canceled":         {rbdJournalVariantDeadPrimary, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Outcome.Canceled = true }},
		"resumed before recording": {rbdJournalVariantDeadPrimary, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Outcome.RecordedUTC = "2026-11-23T13:33:59Z" }},
		"primary not down":         {rbdJournalVariantDeadPrimary, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Held.Lost.Down = []int32{} }},
		"PG still led by it":       {rbdJournalVariantDeadPrimary, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Held.Lost.Objects = r.Held.Placed.Objects }},
		"PG not active again":      {rbdJournalVariantDeadPrimary, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Held.Lost.Query.State = "peering" }},
		"interval unchanged":       {rbdJournalVariantDeadPrimary, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Held.Lost.Query.SameIntervalSince = 21 }},
		"entry lost":               {rbdJournalVariantDeadPrimary, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Views["observe-recorded"].EntrySHA256 = nil }},
		"lock record lost":         {rbdJournalVariantDeadPrimary, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Views["observe-recorded"].LockFree = true }},
		"watch not reattached":     {rbdJournalVariantDeadPrimary, "replay", func(r *rbdJournalLostAppendRecord) { r.Held.Reconnect.Class = "refused" }},
		"handoff on the old one":   {rbdJournalVariantDeadPrimary, "replay", func(r *rbdJournalLostAppendRecord) { r.Held.HandoffPrimary = 0 }},
		"a third connection":       {rbdJournalVariantDeadPrimary, "replay", func(r *rbdJournalLostAppendRecord) { *r.DialsTotal, r.Gate.Connections = 3, 3 }},
		// Held primary change.
		"held append executed":      {rbdJournalVariantHeldPrimaryChange, "observe-held", func(r *rbdJournalLostAppendRecord) { op(r).Events = append(op(r).Events, "sub_op_commit_rec") }},
		"named the next epoch only": {rbdJournalVariantHeldPrimaryChange, "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.NamedEpoch = 31 }},
		"a replica paused":          {rbdJournalVariantHeldPrimaryChange, "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Replica = 2 }},
		"another OSD marked out":    {rbdJournalVariantHeldPrimaryChange, "change-primary", func(r *rbdJournalLostAppendRecord) { r.Held.PrimaryChange.Out = 1 }},
		"primary did not move":      {rbdJournalVariantHeldPrimaryChange, "change-primary", func(r *rbdJournalLostAppendRecord) { r.Held.PrimaryChange.Objects[1].Moved = false }},
		"moved at the named epoch":  {rbdJournalVariantHeldPrimaryChange, "change-primary", func(r *rbdJournalLostAppendRecord) { r.Held.PrimaryChange.Objects[0].After.Epoch = 38 }},
		"answered at the change":    {rbdJournalVariantHeldPrimaryChange, "change-primary", func(r *rbdJournalLostAppendRecord) { r.Held.PendingAfterChange = false }},
		"journal changed":           {rbdJournalVariantHeldPrimaryChange, "change-primary", func(r *rbdJournalLostAppendRecord) { r.Views["change-primary"].ObjectSizes[0]++ }},
		"still listed after": {rbdJournalVariantHeldPrimaryChange, "observe-recorded", func(r *rbdJournalLostAppendRecord) {
			r.Held.Advance.OpsAfter = slices.Clone(r.Held.Advance.OpsBefore)
		}},
		"not listed before":       {rbdJournalVariantHeldPrimaryChange, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Held.Advance.OpsBefore = nil }},
		"advance short":           {rbdJournalVariantHeldPrimaryChange, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Held.Advance.EpochAfter = 37 }},
		"flag left set":           {rbdJournalVariantHeldPrimaryChange, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Held.Advance.FlagFinally = true }},
		"advance of another pool": {rbdJournalVariantHeldPrimaryChange, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Held.Advance.Commands[0] = "osd pool set x nodeep-scrub true" }},
		"answered after":          {rbdJournalVariantHeldPrimaryChange, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Outcome.Class, r.Outcome.ReplyCode = "refused", -6 }},
		"wait not canceled":       {rbdJournalVariantHeldPrimaryChange, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Outcome.Canceled = false }},
		"same interval after":     {rbdJournalVariantHeldPrimaryChange, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Held.Settled.Query.SameIntervalSince = 21 }},
		"entry appeared":          {rbdJournalVariantHeldPrimaryChange, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Views["observe-recorded"].ObjectSHA256[0] = "x" }},
		"replay applied an entry": {rbdJournalVariantHeldPrimaryChange, "replay", func(r *rbdJournalLostAppendRecord) { r.Views["replay"].ImageHeadSHA256 = strings.Repeat("b", 64) }},
		"replay of an owned entry": {rbdJournalVariantHeldPrimaryChange, "replay", func(r *rbdJournalLostAppendRecord) {
			r.Views["replay"].InspectOwned = []rbdJournalLostAppendEvent{rbdJournalEntryEvent(r.Entry)}
		}},
	}
	for name, c := range cases {
		modes := []string{c.mode}
		if c.mode == "*" {
			modes = all
		}
		for _, mode := range modes {
			stages, _ := rbdJournalHeldPlan(mode)
			r := rbdJournalHeldSample(mode)
			c.change(r)
			full := slices.Clone(r.Order)
			// The stage named, and every later one, refuses the record; the
			// earlier ones do not see the change.
			// An empty stage marks a change the claims must accept.
			at := slices.Index(stages, c.stage)
			if at < 0 && c.stage != "" {
				t.Fatalf("%s (%s): unknown stage %s", name, mode, c.stage)
			}
			for i, stage := range stages {
				problems := judge(r, full, stage)
				if c.stage == "" && len(problems) != 0 {
					t.Errorf("%s (%s): refused at %s: %v", name, mode, stage, problems)
				}
				if c.stage != "" && i >= at && len(problems) == 0 {
					t.Errorf("%s (%s): accepted at %s", name, mode, stage)
				}
			}
		}
	}
}
