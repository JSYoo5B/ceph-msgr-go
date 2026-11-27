package integration_test

import (
	"slices"
	"strings"
	"testing"
)

// rbdJournalPendingErasure moves a pending-append sample's journal objects
// into the erasure-coded pool 4 on acting set acting, with P (osd.0) leading
// it: every fixture read then follows journal object 0 alone, each PG query
// names every member with its acting index, and every op held for the paused
// member shows P's own shard sub-write started and committed instead of a
// replica wait.
func rbdJournalPendingErasure(r *rbdJournalLostAppendRecord, acting []int32) {
	h := r.Held
	h.Mode = "erasure-" + h.Mode
	h.JournalPool, h.JournalPoolID = "ceph-msgr-rbd2-"+r.Token, 4
	h.GoActing = slices.Clone(acting)
	index := func(osd int32) *int32 { i := int32(slices.Index(acting, osd)); return &i }
	place := func(o nativeRBDJournalPlacement) nativeRBDJournalPlacement {
		o.Pool, o.PoolID, o.PGID = h.JournalPool, h.JournalPoolID, "4.0"
		if len(o.Acting) == 3 {
			o.Up, o.Acting, o.UpPrimary = slices.Clone(acting), slices.Clone(acting), acting[0]
		}
		return o
	}
	reads := []*nativeRBDJournalHeldRead{h.Placed, h.Held, h.Settled}
	if h.Pending != nil {
		reads = append(reads, h.Pending.Back)
	}
	for _, read := range reads {
		if read == nil {
			continue
		}
		read.Objects, read.PGID = []nativeRBDJournalPlacement{place(read.Objects[1])}, "4.0"
		q := *read.Query
		q.Up, q.Acting, q.Shard, q.Peers = slices.Clone(acting), slices.Clone(acting), index(h.Primary), slices.Clone(q.Peers)
		for i := range q.Peers {
			q.Peers[i].Shard = index(q.Peers[i].Peer)
		}
		read.Query = &q
	}
	if d := h.Pending.Dead; d != nil {
		for i := range d.Samples {
			d.Samples[i].Objects = []nativeRBDJournalPlacement{place(d.Samples[i].Objects[1])}
		}
	}
	for i := range h.Held.Ops {
		if o := &h.Held.Ops[i]; o.FlagPoint == "waiting for sub ops" {
			o.FlagPoint, o.Events = "started", []string{"initiated", "header_read", "throttled", "all_read", "dispatched", "queued_for_pg", "reached_pg", "started", "sub_op_started",
				"sub_op_committed"}
		}
	}
}

// rbdJournalFencedPendingSample is a consistent record of one fenced pending
// run: the pending-hold sample's three appends (P is osd.0, osd.2 the paused
// member, transactions 10, 11 and 12), fenced while held and each answered
// 0 after the resume, the stat refused as transaction 13. In the erasure mode
// the journal objects are in the erasure-coded pool 4 and the header's
// primary is P; override moves the erasure-coded PG's CRUSH primary to
// osd.1, so that the alignment overrides it with P (acting set [1, 0, 2], P
// at shard 1).
func rbdJournalFencedPendingSample(erasure, override bool) *rbdJournalLostAppendRecord {
	n := func(v int) *int { return &v }
	r := rbdJournalHeldPendingSample(rbdJournalVariantPendingHold)
	h, p := r.Held, r.Held.Pending
	h.Mode = rbdJournalVariantFencedPendingHold
	for i := range p.Calls {
		o := p.Calls[i].Outcome
		o.Class, o.UnknownOutcome, o.Canceled, o.Error, o.ReplyTransaction, o.RecordedUTC = "applied", false, false, "", o.Submission.Transaction, "2026-11-23T13:33:53Z"
	}
	p.CanceledUTC = ""
	answered := r.Views["observe-answered"]
	delete(r.Views, "observe-answered")
	answered.Action, answered.SubjectWatcher = "observe-fenced", false
	answered.Blocklist = []rbdLockOwnerLossBlock{{rbdJournalFencedSampleAddress, "2026-11-23T14:33:51Z"}}
	r.Views["observe-fenced"] = answered
	acting := []int32{0, 1, 2}
	if override {
		acting = []int32{1, 0, 2}
	}
	if erasure {
		rbdJournalPendingErasure(r, acting)
		zero := int32(0)
		for i := range p.Calls {
			p.Calls[i].Outcome.OperationCode = &zero
		}
	}
	_, order := rbdJournalHeldPlan(h.Mode)
	r.Order = slices.Clone(order)
	block := []nativeRBDBlock{{Address: rbdJournalFencedSampleAddress, Until: "2026-11-23T14:33:51Z"}}
	h.Fenced = &rbdJournalFenced{LockerAddress: rbdJournalFencedSampleAddress, PendingAfterFence: true, PendingAfterRefusal: true,
		Fence: &nativeRBDJournalFence{Address: rbdJournalFencedSampleAddress, OSD: 0, PGID: h.Placed.PGID, Commands: []string{"osd blocklist add " + rbdJournalFencedSampleAddress},
			BlocklistBefore: []nativeRBDBlock{}, Blocklist: block, Epoch: 31, PGEpoch: 31, PGState: "active+clean", Ops: slices.Clone(h.Held.Ops), Down: []int32{}, Polls: 2},
		Refusal: &rbdLockOwnerLossAttempt{Label: "stat_fenced", Invocations: 1, Class: "refused", Code: rbdOSDBlocklisted, ErrorCode: rbdOSDBlocklisted,
			Error: "ceph: OSD result code -108", ReplyTransaction: 13, ReplyMapEpoch: 31}}
	h.Fenced.Refusal.Submission.Transaction, h.Fenced.Refusal.Submission.GlobalID, h.Fenced.Refusal.Submission.MapEpoch = 13, r.SubjectGlobalID, 30
	r.DialsTotal, r.Gate.Connections, h.AppendConnection = n(1), 1, 1
	if erasure {
		header := nativeRBDJournalPlacement{Epoch: 30, Pool: r.Pool, PoolID: r.PoolID, Object: "rbd_header.ab", RawPGID: "3.e3", PGID: "3.0", Up: []int32{0, 1, 2}, UpPrimary: 0,
			Acting: []int32{0, 1, 2}, ActingPrimary: 0}
		a := &nativeRBDJournalAlign{Header: header, Before: h.Placed.Objects[0], After: h.Placed.Objects[0], Commands: []string{}, Query: h.Placed.Query, Polls: 1}
		h.HeaderPrimary, h.Fenced.Align = 0, a
		if override {
			a.Before.Epoch, a.Before.ActingPrimary = 28, 1
			a.Commands = []string{"osd primary-temp 4.0 0"}
			// The read before the alignment went to the CRUSH primary.
			r.DialsTotal, r.Gate.Connections = n(2), 2
		}
	}
	return r
}

// TestRBDJournalFencedPendingClaims feeds the fenced pending claims
// contradictory records: each consistent sample passes at every stage, and
// each change fails at the stage named and not before it.
func TestRBDJournalFencedPendingClaims(t *testing.T) {
	type sample struct{ erasure, override bool }
	samples := []sample{{false, false}, {true, false}, {true, true}}
	judge := func(r *rbdJournalLostAppendRecord, full []string, stage string) []string {
		_, order := rbdJournalHeldPlan(r.Held.Mode)
		r.Order = slices.Clone(full[:min(len(full), slices.Index(order, stage)+1)])
		return rbdJournalLostAppendClaims(r, stage)
	}
	for _, s := range samples {
		for _, stage := range rbdJournalFencedPendingHoldStages {
			r := rbdJournalFencedPendingSample(s.erasure, s.override)
			if problems := judge(r, r.Order, stage); len(problems) != 0 {
				t.Fatalf("consistent fenced pending record %+v refused at %s: %v", s, stage, problems)
			}
		}
	}
	type rec = rbdJournalLostAppendRecord
	f := func(r *rec) *rbdJournalFenced { return r.Held.Fenced }
	calls := func(r *rec) []rbdJournalHeldCall { return r.Held.Pending.Calls }
	fenced := func(r *rec) *rbdJournalLostAppendView { return r.Views["observe-fenced"] }
	cases := map[string]struct {
		erasure bool
		stage   string
		change  func(*rec)
	}{
		// The record and what was held.
		"no fence part":            {false, "observe-held", func(r *rec) { r.Held.Fenced = nil }},
		"no lock record address":   {false, "observe-held", func(r *rec) { f(r).LockerAddress = "" }},
		"two appends listed":       {false, "observe-held", func(r *rec) { r.Held.Held.Ops = r.Held.Held.Ops[1:] }},
		"an append not applied":    {false, "observe-held", func(r *rec) { r.Held.Held.Ops[0].Events = r.Held.Held.Ops[0].Events[:8] }},
		"an append sent again":     {false, "observe-held", func(r *rec) { r.Gate.LargeWrites++ }},
		"a caller answered early":  {false, "observe-held", func(r *rec) { r.Held.PendingAfterHeld = false }},
		"primary paused":           {false, "observe-held", func(r *rec) { r.Held.Replica, r.Held.Lease.Daemon = 0, "osd.0" }},
		"replicated aligned":       {false, "observe-held", func(r *rec) { f(r).Align = &nativeRBDJournalAlign{} }},
		"erasure not aligned":      {true, "observe-held", func(r *rec) { f(r).Align = nil }},
		"erasure header elsewhere": {true, "observe-held", func(r *rec) { r.Held.HeaderPrimary = 1 }},
		"erasure shards moved":     {true, "observe-held", func(r *rec) { f(r).Align.After.Acting = []int32{0, 2, 1} }},
		// The fence and its probe.
		"fence of another address":     {false, "refusal_recorded", func(r *rec) { f(r).Fence.Address = "10.0.0.11:0/1" }},
		"not blocklisted after":        {false, "refusal_recorded", func(r *rec) { f(r).Fence.Blocklist = []nativeRBDBlock{} }},
		"PG behind the fence":          {false, "refusal_recorded", func(r *rec) { f(r).Fence.PGEpoch = 30 }},
		"an append gone at the fence":  {false, "refusal_recorded", func(r *rec) { f(r).Fence.Ops = f(r).Fence.Ops[1:] }},
		"an append changed at fence":   {false, "refusal_recorded", func(r *rec) { f(r).Fence.Ops[2].FlagPoint = "commit sent; apply or cleanup" }},
		"another op at the fence":      {false, "refusal_recorded", func(r *rec) { f(r).Fence.Ops[1].Transaction = 14 }},
		"answered during the fence":    {false, "refusal_recorded", func(r *rec) { f(r).PendingAfterFence = false }},
		"answered during the probe":    {false, "refusal_recorded", func(r *rec) { f(r).PendingAfterRefusal = false }},
		"probe answered":               {false, "refusal_recorded", func(r *rec) { f(r).Refusal.Class, f(r).Refusal.Code, f(r).Refusal.ErrorCode = "applied", 0, 0 }},
		"probe is an append":           {false, "refusal_recorded", func(r *rec) { f(r).Refusal.Submission.Transaction, f(r).Refusal.ReplyTransaction = 11, 11 }},
		"probe answered from before":   {false, "refusal_recorded", func(r *rec) { f(r).Refusal.ReplyMapEpoch = 30 }},
		"probe missing":                {true, "refusal_recorded", func(r *rec) { f(r).Refusal = nil }},
		"erasure fence on another OSD": {true, "refusal_recorded", func(r *rec) { f(r).Fence.OSD = 1 }},
		// What each caller was told, and the native observation after it.
		"an append unknown": {false, "observe-fenced", func(r *rec) {
			o := calls(r)[1].Outcome
			o.Class, o.UnknownOutcome, o.ReplyTransaction = "uncertain", true, 0
		}},
		"an append's wait canceled": {false, "observe-fenced", func(r *rec) {
			o := calls(r)[1].Outcome
			o.Class, o.UnknownOutcome, o.Canceled, o.ReplyTransaction = "uncertain", true, true, 0
		}},
		"an append refused": {false, "observe-fenced", func(r *rec) {
			o := calls(r)[2].Outcome
			o.Class, o.ReplyCode = "refused", rbdOSDBlocklisted
		}},
		"answers swapped": {false, "observe-fenced", func(r *rec) {
			c := calls(r)
			c[0].Outcome.ReplyTransaction, c[1].Outcome.ReplyTransaction = c[1].Outcome.ReplyTransaction, c[0].Outcome.ReplyTransaction
		}},
		"an append invoked twice":       {false, "observe-fenced", func(r *rec) { calls(r)[2].Outcome.CallerInvocations = 2 }},
		"answered before the resume":    {false, "observe-fenced", func(r *rec) { calls(r)[1].Outcome.RecordedUTC = "2026-11-23T13:33:51Z" }},
		"interval changed after":        {false, "observe-fenced", func(r *rec) { r.Held.Settled.Query.SameIntervalSince = 29 }},
		"an entry absent after":         {false, "observe-fenced", func(r *rec) { fenced(r).ObjectSizes[1], fenced(r).ObjectSHA256[1] = 2110, strings.Repeat("1", 64) }},
		"an entry twice after":          {false, "observe-fenced", func(r *rec) { fenced(r).ObjectSizes[2] += 4198 }},
		"lock free after the fence":     {false, "observe-fenced", func(r *rec) { fenced(r).LockFree = true }},
		"watch kept after the fence":    {false, "observe-fenced", func(r *rec) { fenced(r).SubjectWatcher = true }},
		"address free after the fence":  {false, "observe-fenced", func(r *rec) { fenced(r).Blocklist = nil }},
		"image applied after the fence": {false, "observe-fenced", func(r *rec) { fenced(r).ImageHeadSHA256 = r.ExpectedHeadAfter }},
		"erasure answer without result": {true, "observe-fenced", func(r *rec) { calls(r)[0].Outcome.OperationCode = nil }},
		"erasure answer with error": {true, "observe-fenced", func(r *rec) {
			code := int32(-5)
			calls(r)[1].Outcome.OperationCode = &code
		}},
		// Transmission and the replay.
		"a second connection":                  {false, "replay", func(r *rec) { n := 2; r.DialsTotal, r.Gate.Connections = &n, 2 }},
		"erasure append on another connection": {true, "replay", func(r *rec) { r.Held.AppendConnection = 2 }},
		"replay without an entry": {false, "replay", func(r *rec) {
			v := r.Views["replay"]
			v.InspectOwned = slices.Delete(slices.Clone(v.InspectOwned), 1, 2)
			*v.InspectTotal--
		}},
		"replay out of order": {false, "replay", func(r *rec) {
			v := r.Views["replay"]
			v.InspectOwned[0], v.InspectOwned[1] = v.InspectOwned[1], v.InspectOwned[0]
		}},
		"replay of an entry twice": {false, "replay", func(r *rec) {
			v := r.Views["replay"]
			v.InspectOwned = append(v.InspectOwned, v.InspectOwned[2])
			*v.InspectTotal++
		}},
		"replay commit before the last": {false, "replay", func(r *rec) { r.ExpectedCommitAfter = r.ExpectedCommitAfter[1:] }},
		"lock held after the replay":    {false, "replay", func(r *rec) { r.Views["replay"].LockFree = false }},
		"order: probe before the fence": {false, "refusal_recorded", func(r *rec) {
			i, j := slices.Index(r.Order, "fence"), slices.Index(r.Order, "refusal_recorded")
			r.Order[i], r.Order[j] = r.Order[j], r.Order[i]
		}},
		"order: the fenced owner unlocked": {false, "replay", func(r *rec) {
			r.Order = slices.Insert(r.Order, slices.Index(r.Order, "replay"), "unlock")
		}},
	}
	for name, c := range cases {
		variants := []sample{{c.erasure, false}}
		if c.erasure {
			variants = append(variants, sample{true, true})
		}
		for _, s := range variants {
			stages := rbdJournalFencedPendingHoldStages
			at := slices.Index(stages, c.stage)
			if at < 0 {
				t.Fatalf("%s: unknown stage %s", name, c.stage)
			}
			for _, stage := range stages {
				r := rbdJournalFencedPendingSample(s.erasure, s.override)
				c.change(r)
				problems := judge(r, slices.Clone(r.Order), stage)
				if before := slices.Index(stages, stage) < at; before && len(problems) != 0 {
					t.Errorf("%s (%+v): refused at %s, before %s: %v", name, s, stage, c.stage, problems)
				} else if !before && len(problems) == 0 {
					t.Errorf("%s (%+v): admitted at %s", name, s, stage)
				}
			}
		}
	}
}
