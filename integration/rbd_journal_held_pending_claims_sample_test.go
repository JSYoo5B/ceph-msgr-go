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

// rbdJournalHeldPendingSample is a consistent record of one pending-append
// run in the given mode with synthetic values: P is osd.0 and osd.2 the
// paused replica; the setup's tag is 1, the subject's 2 and native librbd's
// replay allocates 3; entry i has transaction 10+i.
func rbdJournalHeldPendingSample(mode string) *rbdJournalLostAppendRecord {
	n := func(v int) *int { return &v }
	hash := func(c string) string { return strings.Repeat(c, 64) }
	const subject, cookie, tag = 4866, 77, 2
	restart := mode == rbdJournalVariantPrimaryRestart
	token := strings.Repeat("a1", 16)
	pool := "ceph-msgr-rbd-" + token
	holds := rbdJournalHeldPendingHolds[mode]
	setupSizes := []uint64{2110, 2110, 1086, 93, 0, 0, 0, 0}
	setupSHA := []string{hash("0"), hash("1"), hash("2"), hash("3"), hash("4"), hash("4"), hash("4"), hash("4")}
	setupCommit := []rbdJournalPosition{{3, 1, 3}, {2, 1, 2}, {1, 1, 1}, {0, 1, 0}}
	other := []rbdJournalInspectEntry{{Tag: 1, Type: "AioWrite", Length: 2048}, {Tag: 1, Type: "AioDiscard", Offset: 196608, Length: 65536}}
	p := &rbdJournalHeldPending{}
	for i, hold := range holds {
		e := rbdJournalLostAppendEntry{Tag: tag, EntryTID: uint64(i), Object: uint64(i), ObjectName: fmt.Sprintf("journal_data.3.ab.%d", i), Offset: int(setupSizes[i]), Length: 4198,
			SHA256: hash(string(rune('e' + i))), ImageOffset: []uint64{40 << 10, 120 << 10, 160 << 10}[i], ImageLength: 4 << 10,
			Timestamp: cephmsgr.OSDTime{Seconds: 1700000000 + uint32(i), Nanoseconds: 5000}}
		named := uint32(30)
		if hold == rbdJournalHoldMap {
			named += rbdJournalForcedPendingEpochs
		}
		p.Calls = append(p.Calls, rbdJournalHeldCall{Label: fmt.Sprintf("journal_append_%d", i), Phase: "go-held-pending", Entry: e, Hold: hold, NamedEpoch: named,
			BeforeSize: setupSizes[i], BeforeSHA256: setupSHA[i], AppliedSHA256: hash(string(rune('x' + i))),
			Outcome: &rbdJournalLostAppendOutcome{Label: fmt.Sprintf("journal_append_%d", i), CallerInvocations: 1,
				Submission: cephmsgr.OSDSubmission{Transaction: uint64(10 + i), GlobalID: subject, MapEpoch: named}, RecordedUTC: "2026-11-23T13:33:51Z"}})
	}
	p.Calls[0].Phase = "go-lost-append"
	e := p.Calls[0].Entry
	view := func(action string, applied ...bool) *rbdJournalLostAppendView {
		v := &rbdJournalLostAppendView{Action: action, ObjectSizes: slices.Clone(setupSizes), ObjectSHA256: slices.Clone(setupSHA), ObjectExists: []bool{true, true, true, true, false, false, false, false},
			ImageHeadSHA256: hash("a"), ImageCommit: slices.Clone(setupCommit), NextTag: tag + 1, LastTag: rbdJournalLostAppendTag{TID: tag}, ObservedUTC: "2026-11-23T13:33:50Z"}
		for i, a := range applied {
			if a {
				v.ObjectSizes[i], v.ObjectSHA256[i] = setupSizes[i]+4198, p.Calls[i].AppliedSHA256
			}
		}
		return v
	}
	locked := func(v *rbdJournalLostAppendView, watch bool) *rbdJournalLostAppendView {
		v.LockOwner, v.LockCookie, v.SubjectWatcher, v.Watchers = fmt.Sprintf("client.%d", subject), "auto 77", watch, 1
		return v
	}
	setup := view("setup")
	setup.LockFree, setup.NextTag, setup.LastTag = true, tag, rbdJournalLostAppendTag{TID: 1}
	setup.InspectTotal, setup.InspectErrors, setup.InspectOther = n(2), n(0), slices.Clone(other)
	r := &rbdJournalLostAppendRecord{Token: token, Pool: pool, PoolID: 3, SubjectGlobalID: subject, ObserverGlobalID: 4800, ConnectionMode: "crc", Image: "ceph-msgr-journal-" + token,
		ImageID: "ab", JournalHeader: "journal.ab", WatchCookie: cookie, LockCookie: "auto 77", Entry: e, ExpectedHeadBefore: hash("a"), ExpectedHeadAfter: hash("b"),
		Timings: map[string]string{}, Views: map[string]*rbdJournalLostAppendView{"setup": setup, "observe-locked": locked(view("observe-locked"), true)}}
	_, order := rbdJournalHeldPlan(mode)
	r.Order = slices.Clone(order)
	objects := []string{"rbd_header.ab", e.ObjectName, "journal.ab"}
	placements := func(epoch uint32, acting []int32) []nativeRBDJournalPlacement {
		var set []nativeRBDJournalPlacement
		for _, o := range objects {
			set = append(set, nativeRBDJournalPlacement{Epoch: epoch, Pool: pool, PoolID: 3, Object: o, RawPGID: "3.e3", PGID: "3.0", Up: slices.Clone(acting), UpPrimary: acting[0],
				Acting: slices.Clone(acting), ActingPrimary: acting[0]})
		}
		return set
	}
	query := func(state string, epoch uint32, acting []int32, last string, interval uint32, peers ...int32) *nativeRBDJournalPGQuery {
		q := &nativeRBDJournalPGQuery{State: state, Epoch: epoch, Up: slices.Clone(acting), Acting: slices.Clone(acting), LastUpdate: last, SameIntervalSince: interval, SameUpSince: interval,
			SamePrimarySince: interval, LastEpochStarted: interval + 1}
		for _, p := range peers {
			q.Peers = append(q.Peers, struct {
				Peer       int32  `json:"peer"`
				LastUpdate string `json:"last_update"`
			}{p, last})
		}
		return q
	}
	clean := []int32{0, 1, 2}
	var ops []nativeRBDJournalHeldOp
	for i, c := range p.Calls {
		op := nativeRBDJournalHeldOp{OSD: 0, Client: fmt.Sprintf("client.%d", subject), Transaction: uint64(10 + i),
			Description: fmt.Sprintf("osd_op(client.%d.0:%d 3.e3 3:c7:::%s:head [call journal.append in=4210b] snapc 0=[] ondisk+write+returnvec e%d)", subject, 10+i, c.Entry.ObjectName, c.NamedEpoch),
			FlagPoint:   "waiting for sub ops",
			Events:      []string{"initiated", "header_read", "throttled", "all_read", "dispatched", "queued_for_pg", "reached_pg", "started", "waiting for subops from 1,2", "sub_op_commit_rec", "op_commit"}}
		if c.Hold == rbdJournalHoldMap {
			op.FlagPoint, op.Events = "no flag points reached", []string{"initiated", "header_read", "throttled", "all_read", "dispatched"}
		}
		ops = append(ops, op)
	}
	// The primary lists the ops in another order than they were sent.
	slices.Reverse(ops)
	h := &rbdJournalHeld{Mode: mode, Primary: 0, Replica: 2, GoActing: []int32{0, 1, 2}, SentAtEpoch: 30, NamedEpoch: 30, SubmittedUTC: "2026-11-23T13:33:49.500000000Z", PendingAfterHeld: true,
		AppendConnection: 1, PingBefore: &rbdLockOwnerLossAttempt{Label: "watch_ping", Invocations: 1, Class: "applied"}, Pending: p,
		Placed: &nativeRBDJournalHeldRead{Objects: placements(30, clean), PGID: "3.0", Query: query("active+clean", 30, clean, "29'51", 21, 1, 2), OSD: -1, Epoch: 30, Down: []int32{}, Settled: true, Polls: 1},
		Held: &nativeRBDJournalHeldRead{Objects: placements(30, clean), PGID: "3.0", Query: query("active+clean", 30, clean, "30'54", 21, 1, 2), OSD: 0, Ops: ops,
			Epoch: 30, Down: []int32{}, Settled: true, Polls: 1}}
	r.Held = h
	submitted, _ := time.Parse(time.RFC3339Nano, h.SubmittedUTC)
	h.Lease = &rbdJournalHeldLease{Daemon: "osd.2", Deadline: submitted.Add(13 * time.Second).UnixNano(), PausedAt: submitted.Add(-time.Second).UnixNano(),
		EndedAt: submitted.Add(3 * time.Second).UnixNano(), Reason: "resumed", LeftMS: 10000}
	r.Gate = &testcluster.OSDReplyGateState{Decoded: true, Threshold: rbdJournalLostAppendThreshold, Connections: 1, LargeWrites: len(p.Calls)}
	replay := view("replay")
	replay.LockFree, replay.NextTag, replay.ImageHeadSHA256 = true, tag+2, hash("b")
	replay.InspectErrors, replay.InspectOther = n(0), slices.Clone(other)
	if !restart {
		for i, c := range p.Calls {
			o := c.Outcome
			if i == rbdJournalHeldPendingCanceled {
				o.Class, o.UnknownOutcome, o.Canceled, o.Error, o.RecordedUTC = "uncertain", true, true, "ceph: request outcome unknown: context canceled", "2026-11-23T13:33:51Z"
			} else {
				o.Class, o.ReplyTransaction, o.RecordedUTC = "applied", o.Submission.Transaction, "2026-11-23T13:33:53Z"
			}
		}
		p.CanceledUTC = "2026-11-23T13:33:51.100000000Z"
		r.DialsTotal = n(1)
		h.Settled = &nativeRBDJournalHeldRead{Objects: placements(30, clean), PGID: "3.0", Query: query("active+clean", 30, clean, "30'54", 21, 1, 2), OSD: -1, Epoch: 30, Down: []int32{},
			Settled: true, Polls: 1}
		r.Views["observe-answered"] = locked(view("observe-answered", true, true, true), true)
		replay.ObjectSizes, replay.ObjectSHA256 = slices.Clone(r.Views["observe-answered"].ObjectSizes), slices.Clone(r.Views["observe-answered"].ObjectSHA256)
		r.ExpectedCommitAfter = []rbdJournalPosition{{2, tag, 2}, {1, tag, 1}, {0, tag, 0}, {3, 1, 3}}
		replay.LastTag = rbdJournalLostAppendTag{tag + 1, true, tag, 2}
		replay.InspectTotal = n(5)
		for _, c := range p.Calls {
			replay.InspectOwned = append(replay.InspectOwned, rbdJournalEntryEvent(c.Entry))
		}
	} else {
		for _, c := range p.Calls {
			c.Outcome.Class, c.Outcome.UnknownOutcome, c.Outcome.Error = "uncertain", true, "ceph: request outcome unknown: EOF"
		}
		h.Kill = &rbdJournalHeldKill{Daemon: "osd.0", ExitCode: rbdJournalHeldKilledExit, RequestedUTC: "2026-11-23T13:33:50.500000000Z"}
		h.Lost = &nativeRBDJournalHeldRead{Objects: placements(32, []int32{1, 2}), PGID: "3.0", Query: query("active+undersized+degraded", 32, []int32{1, 2}, "30'52", 31, 2), OSD: -1,
			Epoch: 32, Down: []int32{0}, Settled: true, Polls: 2}
		r.Views["observe-recorded"] = locked(view("observe-recorded", true, false), false)
		p.StartedUTC = "2026-11-23T13:34:10Z"
		p.AddressBefore, p.AddressAfter = "10.0.0.10:6800/1200", "10.0.0.10:6800/3400"
		p.Back = &nativeRBDJournalHeldRead{Objects: placements(36, clean), PGID: "3.0", Query: query("active+clean", 36, clean, "34'60", 34, 1, 2), OSD: -1, Epoch: 36, Down: []int32{},
			Settled: true, Polls: 40}
		r.Views["observe-restarted"] = locked(view("observe-restarted", true, false), false)
		r.DialsBeforeClose, r.DialsTotal, r.Gate.Connections = n(1), n(2), 2
		h.Reconnect = &rbdLockOwnerLossAttempt{Label: "image_watch_reconnect", Invocations: 1, Class: "applied"}
		h.HandoffPrimary, h.LockerAddress, h.WatcherAddress = 0, "10.0.0.11:0/1690346225", "10.0.0.11:0/1690346225"
		r.ExpectedCommitAfter = []rbdJournalPosition{{0, tag, 0}, {3, 1, 3}, {2, 1, 2}, {1, 1, 1}}
		replay = view("replay", true, false)
		replay.LockFree, replay.NextTag, replay.ImageHeadSHA256 = true, tag+2, hash("b")
		replay.InspectErrors, replay.InspectOther = n(0), slices.Clone(other)
		replay.LastTag = rbdJournalLostAppendTag{tag + 1, true, tag, 0}
		replay.InspectTotal, replay.InspectOwned = n(3), []rbdJournalLostAppendEvent{rbdJournalEntryEvent(e)}
	}
	replay.ImageCommit = slices.Clone(r.ExpectedCommitAfter)
	r.Views["replay"] = replay
	r.Outcome = p.Calls[0].Outcome
	return r
}

// TestRBDJournalHeldPendingClaims feeds the pending-append claims
// contradictory records: each consistent sample passes at every stage, and
// each change fails at the stage named.
func TestRBDJournalHeldPendingClaims(t *testing.T) {
	judge := func(r *rbdJournalLostAppendRecord, full []string, stage string) []string {
		_, order := rbdJournalHeldPlan(r.Held.Mode)
		r.Order = slices.Clone(full[:min(len(full), slices.Index(order, stage)+1)])
		return rbdJournalLostAppendClaims(r, stage)
	}
	modes := []string{rbdJournalVariantPendingHold, rbdJournalVariantPrimaryRestart}
	for _, mode := range modes {
		stages, _ := rbdJournalHeldPlan(mode)
		for _, stage := range stages {
			r := rbdJournalHeldPendingSample(mode)
			if problems := judge(r, r.Order, stage); len(problems) != 0 {
				t.Fatalf("consistent %s record refused at %s: %v", mode, stage, problems)
			}
		}
	}
	type mutation struct {
		mode, stage string
		change      func(r *rbdJournalLostAppendRecord)
	}
	// opOf is the primary's listed op of entry i.
	opOf := func(r *rbdJournalLostAppendRecord, i int) *nativeRBDJournalHeldOp {
		for k := range r.Held.Held.Ops {
			if strings.Contains(r.Held.Held.Ops[k].Description, fmt.Sprintf(":::journal_data.3.ab.%d:head", i)) {
				return &r.Held.Held.Ops[k]
			}
		}
		return nil
	}
	calls := func(r *rbdJournalLostAppendRecord) []rbdJournalHeldCall { return r.Held.Pending.Calls }
	cases := map[string]mutation{
		// Held, in both modes.
		"one append not listed": {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Held.Ops = r.Held.Held.Ops[1:] }},
		"an append listed twice": {"*", "observe-held", func(r *rbdJournalLostAppendRecord) {
			r.Held.Held.Ops = append(r.Held.Held.Ops, r.Held.Held.Ops[0])
		}},
		"two entries in one object": {"*", "observe-held", func(r *rbdJournalLostAppendRecord) {
			opOf(r, 1).Description = strings.Replace(opOf(r, 1).Description, "ab.1", "ab.0", 1)
		}},
		"one transaction for two appends": {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { opOf(r, 1).Transaction = 10 }},
		"entry 0 not held for the replica": {"*", "observe-held", func(r *rbdJournalLostAppendRecord) {
			opOf(r, 0).Events = slices.DeleteFunc(opOf(r, 0).Events, func(e string) bool { return e == "sub_op_commit_rec" })
		}},
		"one write per entry missing":   {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Gate.LargeWrites = 1 }},
		"an append sent again":          {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Gate.LargeWrites++ }},
		"a caller answered before":      {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.PendingAfterHeld = false }},
		"interval changed while held":   {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Held.Query.SameIntervalSince = 29 }},
		"entry offset not object size":  {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { calls(r)[1].Entry.Offset++ }},
		"entries overlap in the image":  {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { calls(r)[1].Entry.ImageOffset = 42 << 10 }},
		"entry in another object":       {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { calls(r)[1].Entry.Object = 2 }},
		"entry 0 not the record's":      {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Entry.Length++ }},
		"replica not paused before":     {"*", "@1", func(r *rbdJournalLostAppendRecord) { r.Held.Lease.PausedAt = time.Now().UnixNano() }},
		"lease not resumed by the test": {"*", "@1", func(r *rbdJournalLostAppendRecord) { r.Held.Lease.Reason = "deadline" }},
		"outcomes swapped": {"*", "@1", func(r *rbdJournalLostAppendRecord) {
			o := calls(r)
			o[0].Outcome, o[1].Outcome = o[1].Outcome, o[0].Outcome
		}},
		"two invocations of one entry": {"*", "@1", func(r *rbdJournalLostAppendRecord) { calls(r)[1].Outcome.CallerInvocations = 2 }},
		"an outcome missing":           {"*", "@1", func(r *rbdJournalLostAppendRecord) { calls(r)[1].Outcome = nil }},
		"outcome of another epoch":     {"*", "@1", func(r *rbdJournalLostAppendRecord) { calls(r)[1].Outcome.Submission.MapEpoch = 31 }},
		"record outcome not entry 0's": {"*", "@1", func(r *rbdJournalLostAppendRecord) { r.Outcome = calls(r)[1].Outcome }},
		"replay of another head":       {"*", "replay", func(r *rbdJournalLostAppendRecord) { r.Views["replay"].ImageHeadSHA256 = strings.Repeat("f", 64) }},
		"replay commit before last":    {"*", "replay", func(r *rbdJournalLostAppendRecord) { r.ExpectedCommitAfter = r.ExpectedCommitAfter[1:] }},
		"replay with the lock held":    {"*", "replay", func(r *rbdJournalLostAppendRecord) { r.Views["replay"].LockFree = false }},
		"replay of an entry twice": {"*", "replay", func(r *rbdJournalLostAppendRecord) {
			v := r.Views["replay"]
			v.InspectOwned = append(v.InspectOwned, v.InspectOwned[0])
		}},
		"replay journal object grew":      {"*", "replay", func(r *rbdJournalLostAppendRecord) { r.Views["replay"].ObjectSizes[0] += 4198 }},
		"tag predecessor another entry":   {"*", "replay", func(r *rbdJournalLostAppendRecord) { r.Views["replay"].LastTag.PredecessorEnt++ }},
		"steps out of order":              {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Order[6], r.Order[7] = r.Order[7], r.Order[6] }},
		"one entry fewer":                 {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Pending.Calls = calls(r)[:1] }},
		"a held op of another OSD":        {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Held.OSD = 1 }},
		"an object grew before":           {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Views["observe-locked"].ObjectSizes[1]++ }},
		"appends placed at a later map":   {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.SentAtEpoch = 31 }},
		"entry before-bytes not native":   {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { calls(r)[1].BeforeSHA256 = strings.Repeat("9", 64) }},
		"an applied digest equals before": {"*", "observe-held", func(r *rbdJournalLostAppendRecord) { calls(r)[1].AppliedSHA256 = calls(r)[1].BeforeSHA256 }},
		// Pending hold.
		"canceled entry answered": {rbdJournalVariantPendingHold, "observe-answered", func(r *rbdJournalLostAppendRecord) {
			o := calls(r)[1].Outcome
			o.Class, o.UnknownOutcome, o.Canceled, o.ReplyTransaction, o.Error = "applied", false, false, 11, ""
		}},
		"an answered entry canceled": {rbdJournalVariantPendingHold, "observe-answered", func(r *rbdJournalLostAppendRecord) {
			o := calls(r)[2].Outcome
			o.Class, o.UnknownOutcome, o.Canceled, o.ReplyTransaction = "uncertain", true, true, 0
		}},
		"answered with an error code": {rbdJournalVariantPendingHold, "observe-answered", func(r *rbdJournalLostAppendRecord) { calls(r)[0].Outcome.ReplyCode = -5 }},
		"canceled after the resume": {rbdJournalVariantPendingHold, "observe-answered", func(r *rbdJournalLostAppendRecord) {
			calls(r)[1].Outcome.RecordedUTC = "2026-11-23T13:33:59Z"
		}},
		"canceled entry not applied": {rbdJournalVariantPendingHold, "observe-answered", func(r *rbdJournalLostAppendRecord) {
			v := r.Views["observe-answered"]
			v.ObjectSizes[1], v.ObjectSHA256[1] = 2110, strings.Repeat("1", 64)
		}},
		"an entry applied twice": {rbdJournalVariantPendingHold, "observe-answered", func(r *rbdJournalLostAppendRecord) { r.Views["observe-answered"].ObjectSizes[2] += 4198 }},
		"another object changed": {rbdJournalVariantPendingHold, "observe-answered", func(r *rbdJournalLostAppendRecord) {
			r.Views["observe-answered"].ObjectSHA256[3] = strings.Repeat("8", 64)
		}},
		"interval changed after": {rbdJournalVariantPendingHold, "observe-answered", func(r *rbdJournalLostAppendRecord) { r.Held.Settled.Query.SameIntervalSince = 29 }},
		"a held append not applied at P": {rbdJournalVariantPendingHold, "observe-held", func(r *rbdJournalLostAppendRecord) {
			opOf(r, 2).FlagPoint, opOf(r, 2).Events = "no flag points reached", []string{"initiated", "header_read", "throttled", "all_read", "dispatched"}
		}},
		"replay without the canceled entry": {rbdJournalVariantPendingHold, "replay", func(r *rbdJournalLostAppendRecord) {
			v := r.Views["replay"]
			v.InspectOwned = slices.Delete(slices.Clone(v.InspectOwned), 1, 2)
			*v.InspectTotal--
		}},
		"a second connection": {rbdJournalVariantPendingHold, "replay", func(r *rbdJournalLostAppendRecord) { r.Gate.Connections = 2 }},
		// Primary restart.
		"map-held entry progressed": {rbdJournalVariantPrimaryRestart, "observe-held", func(r *rbdJournalLostAppendRecord) {
			opOf(r, 1).Events = append(opOf(r, 1).Events, "queued_for_pg", "reached_pg")
		}},
		"map-held entry names its epoch": {rbdJournalVariantPrimaryRestart, "observe-held", func(r *rbdJournalLostAppendRecord) {
			opOf(r, 1).Description = strings.Replace(opOf(r, 1).Description, " e38)", " e30)", 1)
		}},
		"an outcome canceled": {rbdJournalVariantPrimaryRestart, "observe-recorded", func(r *rbdJournalLostAppendRecord) { calls(r)[1].Outcome.Canceled = true }},
		"an outcome answered": {rbdJournalVariantPrimaryRestart, "observe-recorded", func(r *rbdJournalLostAppendRecord) {
			o := calls(r)[0].Outcome
			o.Class, o.UnknownOutcome, o.ReplyTransaction = "applied", false, 10
		}},
		"resumed before the outcomes": {rbdJournalVariantPrimaryRestart, "observe-recorded", func(r *rbdJournalLostAppendRecord) {
			calls(r)[1].Outcome.RecordedUTC = "2026-11-23T13:33:59Z"
		}},
		"not killed":            {rbdJournalVariantPrimaryRestart, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Held.Kill.ExitCode = 0 }},
		"primary never down":    {rbdJournalVariantPrimaryRestart, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Held.Lost.Down = []int32{} }},
		"entry 0 lost":          {rbdJournalVariantPrimaryRestart, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Views["observe-recorded"].ObjectSizes[0] = 2110 }},
		"map-held entry exists": {rbdJournalVariantPrimaryRestart, "observe-recorded", func(r *rbdJournalLostAppendRecord) { r.Views["observe-recorded"].ObjectSizes[1] += 4198 }},
		"primary not back":      {rbdJournalVariantPrimaryRestart, "observe-restarted", func(r *rbdJournalLostAppendRecord) { r.Held.Pending.Back.Settled = false }},
		"back on other members": {rbdJournalVariantPrimaryRestart, "observe-restarted", func(r *rbdJournalLostAppendRecord) {
			b := r.Held.Pending.Back
			for i := range b.Objects {
				b.Objects[i].Acting, b.Objects[i].ActingPrimary = []int32{1, 2, 0}, 1
			}
		}},
		"back not clean":           {rbdJournalVariantPrimaryRestart, "observe-restarted", func(r *rbdJournalLostAppendRecord) { r.Held.Pending.Back.Query.State = "active+recovering" }},
		"back in the old interval": {rbdJournalVariantPrimaryRestart, "observe-restarted", func(r *rbdJournalLostAppendRecord) { r.Held.Pending.Back.Query.SameIntervalSince = 31 }},
		"back with a peer behind":  {rbdJournalVariantPrimaryRestart, "observe-restarted", func(r *rbdJournalLostAppendRecord) { r.Held.Pending.Back.Query.Peers[0].LastUpdate = "30'52" }},
		"back with an OSD down":    {rbdJournalVariantPrimaryRestart, "observe-restarted", func(r *rbdJournalLostAppendRecord) { r.Held.Pending.Back.Down = []int32{2} }},
		"restart grew entry 0 again": {rbdJournalVariantPrimaryRestart, "observe-restarted", func(r *rbdJournalLostAppendRecord) {
			r.Views["observe-restarted"].ObjectSizes[0] += 4198
		}},
		"restart executed the map-held entry": {rbdJournalVariantPrimaryRestart, "observe-restarted", func(r *rbdJournalLostAppendRecord) {
			v := r.Views["observe-restarted"]
			v.ObjectSizes[1], v.ObjectSHA256[1] = 2110+4198, calls(r)[1].AppliedSHA256
		}},
		"restart kept the address": {rbdJournalVariantPrimaryRestart, "observe-restarted", func(r *rbdJournalLostAppendRecord) {
			r.Held.Pending.AddressAfter = r.Held.Pending.AddressBefore
		}},
		"restart lost the lock":      {rbdJournalVariantPrimaryRestart, "observe-restarted", func(r *rbdJournalLostAppendRecord) { r.Views["observe-restarted"].LockFree = true }},
		"handoff on another primary": {rbdJournalVariantPrimaryRestart, "replay", func(r *rbdJournalLostAppendRecord) { r.Held.HandoffPrimary = 1 }},
		"watch not reattached":       {rbdJournalVariantPrimaryRestart, "replay", func(r *rbdJournalLostAppendRecord) { r.Held.Reconnect = nil }},
		"a third connection":         {rbdJournalVariantPrimaryRestart, "replay", func(r *rbdJournalLostAppendRecord) { r.DialsTotal = func(v int) *int { return &v }(3) }},
		"replay of the map-held entry": {rbdJournalVariantPrimaryRestart, "replay", func(r *rbdJournalLostAppendRecord) {
			v := r.Views["replay"]
			v.InspectOwned = append(v.InspectOwned, rbdJournalEntryEvent(calls(r)[1].Entry))
			*v.InspectTotal++
		}},
	}
	for name, m := range cases {
		for _, mode := range modes {
			if m.mode != "*" && m.mode != mode {
				continue
			}
			stages, _ := rbdJournalHeldPlan(mode)
			// "@1" is each mode's first stage after the held one, where the
			// outcomes are judged.
			at := slices.Index(stages, m.stage)
			if m.stage == "@1" {
				at = 1
			}
			if at < 0 {
				t.Fatalf("%s: unknown stage %s", name, m.stage)
			}
			for _, stage := range stages {
				r := rbdJournalHeldPendingSample(mode)
				m.change(r)
				problems := judge(r, r.Order, stage)
				if before := slices.Index(stages, stage) < at; before && len(problems) != 0 {
					t.Errorf("%s (%s): refused at %s, before %s: %v", name, mode, stage, stages[at], problems)
				} else if !before && len(problems) == 0 {
					t.Errorf("%s (%s): admitted at %s", name, mode, stage)
				}
			}
		}
	}
}
