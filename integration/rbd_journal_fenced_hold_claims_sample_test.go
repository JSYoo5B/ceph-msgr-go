package integration_test

import (
	"slices"
	"testing"
)

// rbdJournalFencedSampleAddress is the sample's lock record address.
const rbdJournalFencedSampleAddress = "10.0.0.11:0/1690346225"

// rbdJournalFencedSample is a consistent record of one fenced-hold run with
// the synthetic values of rbdJournalHeldSample's replica hold (P is osd.0,
// osd.2 the paused member). In the erasure mode the journal object is in
// the erasure-coded pool 4 and the header's primary is P too; override
// moves the erasure-coded PG's CRUSH primary to osd.1, so that the
// alignment overrides it with P (acting set [1, 0, 2], P at shard 1).
func rbdJournalFencedSample(erasure, override bool) *rbdJournalLostAppendRecord {
	n := func(v int) *int { return &v }
	mode := rbdJournalVariantReplicaHold
	if erasure {
		mode = rbdJournalVariantErasureHold
	}
	r := rbdJournalHeldSample(mode)
	h := r.Held
	h.Mode = rbdJournalVariantFencedHold
	if erasure {
		h.Mode = "erasure-" + rbdJournalVariantFencedHold
		h.HeaderPrimary, h.AppendConnection, r.DialsTotal, r.Gate.Connections = 0, 1, n(1), 1
	}
	_, order := rbdJournalFencedPlan(h.Mode)
	r.Order = slices.Clone(order)
	// The answer after the fence and the resume.
	r.Outcome.RecordedUTC = "2026-11-23T13:33:52Z"
	answered := r.Views["observe-answered"]
	delete(r.Views, "observe-answered")
	answered.Action, answered.SubjectWatcher = "observe-fenced", false
	answered.Blocklist = []rbdLockOwnerLossBlock{{rbdJournalFencedSampleAddress, "2026-11-23T14:33:51Z"}}
	r.Views["observe-fenced"] = answered
	op := h.Held.Ops[0]
	pgid := h.Placed.PGID
	block := []nativeRBDBlock{{Address: rbdJournalFencedSampleAddress, Until: "2026-11-23T14:33:51Z"}}
	h.Fenced = &rbdJournalFenced{LockerAddress: rbdJournalFencedSampleAddress, PendingAfterFence: true, PendingAfterRefusal: true,
		Fence: &nativeRBDJournalFence{Address: rbdJournalFencedSampleAddress, OSD: 0, PGID: pgid, Commands: []string{"osd blocklist add " + rbdJournalFencedSampleAddress},
			BlocklistBefore: []nativeRBDBlock{}, Blocklist: block, Epoch: 31, PGEpoch: 31, PGState: "active+clean", Ops: []nativeRBDJournalHeldOp{op}, Down: []int32{}, Polls: 2},
		Refusal: &rbdLockOwnerLossAttempt{Label: "stat_fenced", Invocations: 1, Class: "refused", Code: rbdOSDBlocklisted, ErrorCode: rbdOSDBlocklisted,
			Error: "ceph: OSD result code -108", ReplyTransaction: 11, ReplyMapEpoch: 31}}
	h.Fenced.Refusal.Submission.Transaction, h.Fenced.Refusal.Submission.GlobalID, h.Fenced.Refusal.Submission.MapEpoch = 11, r.SubjectGlobalID, 30
	if erasure {
		header := h.Placed.Objects[0]
		header.Object, header.Pool, header.PoolID, header.PGID = "rbd_header.ab", r.Pool, 3, "3.0"
		h.Fenced.Align = &nativeRBDJournalAlign{Header: header, Before: h.Placed.Objects[0], After: h.Placed.Objects[0], Commands: []string{},
			Query: h.Placed.Query, Polls: 1}
	}
	if override {
		acting := []int32{1, 0, 2}
		index := func(osd int32) *int32 { i := int32(slices.Index(acting, osd)); return &i }
		for _, read := range []*nativeRBDJournalHeldRead{h.Placed, h.Held, h.Settled} {
			for i := range read.Objects {
				read.Objects[i].Up, read.Objects[i].Acting, read.Objects[i].UpPrimary = slices.Clone(acting), slices.Clone(acting), 1
			}
			q := *read.Query
			q.Up, q.Acting, q.Shard, q.Peers = slices.Clone(acting), slices.Clone(acting), index(0), slices.Clone(q.Peers)
			for i := range q.Peers {
				q.Peers[i].Shard = index(q.Peers[i].Peer)
			}
			read.Query = &q
		}
		h.GoActing = slices.Clone(acting)
		a := h.Fenced.Align
		a.Before, a.After = h.Placed.Objects[0], h.Placed.Objects[0]
		a.Before.Epoch, a.Before.ActingPrimary, a.Before.UpPrimary = 28, 1, 1
		a.Commands = []string{"osd primary-temp 4.0 0"}
		a.Query = h.Placed.Query
	}
	return r
}

// TestRBDJournalFencedHoldClaims feeds the fenced-hold claims contradictory
// records: each consistent sample passes at every stage, and each change
// fails at the stage named.
func TestRBDJournalFencedHoldClaims(t *testing.T) {
	type sample struct{ erasure, override bool }
	samples := []sample{{false, false}, {true, false}, {true, true}}
	judge := func(r *rbdJournalLostAppendRecord, full []string, stage string) []string {
		_, order := rbdJournalFencedPlan(r.Held.Mode)
		r.Order = slices.Clone(full[:min(len(full), slices.Index(order, stage)+1)])
		return rbdJournalLostAppendClaims(r, stage)
	}
	for _, s := range samples {
		for _, stage := range rbdJournalFencedHoldStages {
			r := rbdJournalFencedSample(s.erasure, s.override)
			if problems := judge(r, r.Order, stage); len(problems) != 0 {
				t.Fatalf("consistent fenced-hold record %+v refused at %s: %v", s, stage, problems)
			}
		}
	}
	if len(rbdJournalLostAppendClaims(rbdJournalFencedSample(false, false), "observe-answered")) == 0 {
		t.Fatal("unknown stage accepted")
	}
	type mutation struct {
		erasure bool
		stage   string
		change  func(r *rbdJournalLostAppendRecord)
	}
	f := func(r *rbdJournalLostAppendRecord) *rbdJournalFenced { return r.Held.Fenced }
	cases := map[string]mutation{
		// Held.
		"no fence part":            {false, "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Fenced = nil }},
		"no lock record address":   {false, "observe-held", func(r *rbdJournalLostAppendRecord) { f(r).LockerAddress = "" }},
		"pending appends":          {false, "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Pending = &rbdJournalHeldPending{} }},
		"primary paused":           {false, "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Replica, r.Held.Lease.Daemon = 0, "osd.0" }},
		"append not held":          {false, "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Held.Ops = nil }},
		"append answered early":    {false, "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.PendingAfterHeld = false }},
		"interval changed":         {false, "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.Held.Query.SameIntervalSince = 29 }},
		"replicated aligned":       {false, "observe-held", func(r *rbdJournalLostAppendRecord) { f(r).Align = &nativeRBDJournalAlign{} }},
		"erasure not aligned":      {true, "observe-held", func(r *rbdJournalLostAppendRecord) { f(r).Align = nil }},
		"erasure header elsewhere": {true, "observe-held", func(r *rbdJournalLostAppendRecord) { r.Held.HeaderPrimary = 1 }},
		"erasure alignment to another OSD": {true, "observe-held", func(r *rbdJournalLostAppendRecord) {
			f(r).Align.Commands = []string{"osd primary-temp 4.0 2"}
		}},
		"erasure shards moved": {true, "observe-held", func(r *rbdJournalLostAppendRecord) { f(r).Align.After.Acting = []int32{0, 2, 1} }},
		// The fence.
		"fence of another address": {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).Fence.Address = "10.0.0.11:0/1" }},
		"fence on another OSD":     {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).Fence.OSD = 1 }},
		"two fence commands": {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) {
			f(r).Fence.Commands = append(f(r).Fence.Commands, f(r).Fence.Commands[0])
		}},
		"blocklisted before":        {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).Fence.BlocklistBefore = f(r).Fence.Blocklist }},
		"not blocklisted after":     {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).Fence.Blocklist = []nativeRBDBlock{} }},
		"only a range blocklisted":  {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).Fence.Blocklist[0].Range = true }},
		"fence epoch not newer":     {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).Fence.Epoch = 30 }},
		"PG behind the fence":       {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).Fence.PGEpoch = 30 }},
		"PG not clean at the fence": {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).Fence.PGState = "active" }},
		"append gone at the fence":  {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).Fence.Ops = nil }},
		"another op at the fence":   {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).Fence.Ops[0].Transaction = 12 }},
		"answered during the fence": {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).PendingAfterFence = false }},
		"answered during the probe": {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).PendingAfterRefusal = false }},
		"probe answered": {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) {
			f(r).Refusal.Class, f(r).Refusal.Code, f(r).Refusal.ErrorCode = "applied", 0, 0
		}},
		"probe unknown": {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) {
			f(r).Refusal.Class, f(r).Refusal.UnknownOutcome = "uncertain", true
		}},
		"probe sent twice": {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).Refusal.Invocations = 2 }},
		"probe is the append": {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) {
			f(r).Refusal.Submission.Transaction, f(r).Refusal.ReplyTransaction = 10, 10
		}},
		"probe answered from before": {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).Refusal.ReplyMapEpoch = 30 }},
		"probe missing":              {true, "refusal_recorded", func(r *rbdJournalLostAppendRecord) { f(r).Refusal = nil }},
		// What the caller was told, and the native observation after it.
		"outcome unknown": {false, "observe-fenced", func(r *rbdJournalLostAppendRecord) {
			r.Outcome.Class, r.Outcome.UnknownOutcome, r.Outcome.ReplyTransaction = "uncertain", true, 0
		}},
		"outcome refused": {false, "observe-fenced", func(r *rbdJournalLostAppendRecord) {
			r.Outcome.Class, r.Outcome.ReplyCode = "refused", rbdOSDBlocklisted
		}},
		"append sent twice":        {false, "observe-fenced", func(r *rbdJournalLostAppendRecord) { r.Outcome.CallerInvocations = 2 }},
		"answered before resume":   {false, "observe-fenced", func(r *rbdJournalLostAppendRecord) { r.Outcome.RecordedUTC = "2026-11-23T13:33:50Z" }},
		"interval changed after":   {false, "observe-fenced", func(r *rbdJournalLostAppendRecord) { r.Held.Settled.Query.SameIntervalSince = 29 }},
		"entry absent after fence": {false, "observe-fenced", func(r *rbdJournalLostAppendRecord) { r.Views["observe-fenced"].EntrySHA256 = nil }},
		"entry twice after fence": {false, "observe-fenced", func(r *rbdJournalLostAppendRecord) {
			v := r.Views["observe-fenced"]
			v.ObjectSizes[0] += 4198
			*v.TrailingBytes = 4198
		}},
		"lock free after fence":     {false, "observe-fenced", func(r *rbdJournalLostAppendRecord) { r.Views["observe-fenced"].LockFree = true }},
		"watch kept after fence":    {false, "observe-fenced", func(r *rbdJournalLostAppendRecord) { r.Views["observe-fenced"].SubjectWatcher = true }},
		"address free after fence":  {false, "observe-fenced", func(r *rbdJournalLostAppendRecord) { r.Views["observe-fenced"].Blocklist = nil }},
		"image applied after fence": {false, "observe-fenced", func(r *rbdJournalLostAppendRecord) { r.Views["observe-fenced"].ImageHeadSHA256 = r.ExpectedHeadAfter }},
		"erasure answer with error": {true, "observe-fenced", func(r *rbdJournalLostAppendRecord) { code := int32(-5); r.Outcome.OperationCode = &code }},
		// Transmission and the replay.
		"second connection":  {false, "replay", func(r *rbdJournalLostAppendRecord) { n := 2; r.DialsTotal, r.Gate.Connections = &n, 2 }},
		"second large write": {false, "replay", func(r *rbdJournalLostAppendRecord) { r.Gate.LargeWrites = 2 }},
		"erasure append on another connection": {true, "replay", func(r *rbdJournalLostAppendRecord) {
			n := 2
			r.DialsTotal, r.Gate.Connections, r.Held.AppendConnection = &n, 2, 2
		}},
		"replayed twice": {false, "replay", func(r *rbdJournalLostAppendRecord) {
			v := r.Views["replay"]
			v.InspectOwned = append(v.InspectOwned, v.InspectOwned[0])
			*v.InspectTotal++
		}},
		"lock held after replay": {false, "replay", func(r *rbdJournalLostAppendRecord) { r.Views["replay"].LockFree = false }},
		"image not replayed":     {false, "replay", func(r *rbdJournalLostAppendRecord) { r.Views["replay"].ImageHeadSHA256 = r.ExpectedHeadBefore }},
		"order: probe before the fence": {false, "refusal_recorded", func(r *rbdJournalLostAppendRecord) {
			i, j := slices.Index(r.Order, "fence"), slices.Index(r.Order, "refusal_recorded")
			r.Order[i], r.Order[j] = r.Order[j], r.Order[i]
		}},
	}
	for name, m := range cases {
		modes := []sample{{m.erasure, false}}
		if m.erasure {
			modes = append(modes, sample{true, true})
		}
		for _, s := range modes {
			r := rbdJournalFencedSample(s.erasure, s.override)
			full := slices.Clone(r.Order)
			m.change(r)
			if slices.Equal(full, rbdJournalFencedHoldOrder) || slices.Equal(full, rbdJournalErasureFencedHoldOrder) {
				full = slices.Clone(r.Order)
			}
			if problems := judge(r, full, m.stage); len(problems) == 0 {
				t.Errorf("contradictory fenced-hold record %+v accepted at %s: %s", s, m.stage, name)
			}
		}
	}
	// The override sample fails without its command.
	r := rbdJournalFencedSample(true, true)
	r.Held.Fenced.Align.Commands = []string{}
	if len(rbdJournalLostAppendClaims(r, "observe-held")) == 0 {
		t.Error("an overridden primary without the override command accepted")
	}
}
