package integration_test

import (
	"slices"
	"testing"
)

// rbdJournalBeforeDownSample is a consistent record of one pre-down restart
// run: the restart sample of rbdJournalHeldPendingSample (P is osd.0, killed
// at 13:33:50.5 and started at 13:34:10) with P flagged nodown at epoch 30,
// listed up at its old address in three samples of a window from 13:33:51 to
// 13:34:00 at epoch 30, then marked down at 33 and up from 34 at a new
// address, with the PG clean again in the interval from 34.
func rbdJournalBeforeDownSample() *rbdJournalLostAppendRecord {
	r := rbdJournalHeldPendingSample(rbdJournalVariantPrimaryRestart)
	h, p := r.Held, r.Held.Pending
	h.Mode = rbdJournalVariantPrimaryRestartBeforeDown
	_, order := rbdJournalHeldPlan(h.Mode)
	r.Order = slices.Clone(order)
	h.Lost = nil
	delete(r.Views, "observe-recorded")
	info := func(epoch uint32, address string, flagged bool) nativeRBDJournalOSDInfo {
		state := []string{"exists", "up"}
		if flagged {
			state = append(state, "nodown")
		}
		return nativeRBDJournalOSDInfo{Epoch: epoch, OSD: 0, Up: true, State: state, UpFrom: 12, Address: address, Down: []int32{}}
	}
	p.KeepUp = &nativeRBDJournalKeepUp{OSD: 0, Commands: []string{"osd add-nodown 0"}, Before: info(29, p.AddressBefore, false), After: info(30, p.AddressBefore, true)}
	p.Dead = &nativeRBDJournalPrimaryDead{OSD: 0, Address: p.AddressBefore}
	for _, at := range []string{"2026-11-23T13:33:51Z", "2026-11-23T13:33:55.5Z", "2026-11-23T13:34:00Z"} {
		objects := []nativeRBDJournalPlacement{}
		for _, o := range h.Placed.Objects {
			o.Up, o.Acting = slices.Clone(o.Up), slices.Clone(o.Acting)
			objects = append(objects, o)
		}
		p.Dead.Samples = append(p.Dead.Samples, nativeRBDJournalPrimarySample{UTC: at, Info: info(30, p.AddressBefore, true), Objects: objects})
	}
	p.AddressDead = p.AddressBefore
	back := info(36, p.AddressAfter, true)
	back.DownAt, back.UpFrom = 33, 34
	p.Back.OSDInfo = &back
	return r
}

// TestRBDJournalBeforeDownClaims feeds the pre-down restart claims
// contradictory records: the consistent sample passes at every stage, and
// each change fails at the stage named.
func TestRBDJournalBeforeDownClaims(t *testing.T) {
	judge := func(r *rbdJournalLostAppendRecord, stage string) []string {
		r.Order = r.Order[:min(len(r.Order), slices.Index(rbdJournalPrimaryRestartBeforeDownOrder, stage)+1)]
		return rbdJournalLostAppendClaims(r, stage)
	}
	for _, stage := range rbdJournalPrimaryRestartBeforeDownStages {
		if problems := judge(rbdJournalBeforeDownSample(), stage); len(problems) != 0 {
			t.Fatalf("consistent pre-down restart record refused at %s: %v", stage, problems)
		}
	}
	// The plain restart keeps none of the pre-down part.
	r := rbdJournalHeldPendingSample(rbdJournalVariantPrimaryRestart)
	r.Held.Pending.KeepUp = rbdJournalBeforeDownSample().Held.Pending.KeepUp
	if len(rbdJournalLostAppendClaims(r, "replay")) == 0 {
		t.Fatal("a plain restart record with a nodown flag accepted")
	}
	type rec = rbdJournalLostAppendRecord
	p := func(r *rec) *rbdJournalHeldPending { return r.Held.Pending }
	s := func(r *rec, i int) *nativeRBDJournalPrimarySample { return &r.Held.Pending.Dead.Samples[i] }
	cases := map[string]struct {
		stage  string
		change func(*rec)
	}{
		// The flag.
		"no flag receipt":         {"observe-primary-dead", func(r *rec) { p(r).KeepUp = nil }},
		"flag not set":            {"observe-primary-dead", func(r *rec) { p(r).KeepUp.After.State = []string{"exists", "up"} }},
		"flagged before":          {"observe-primary-dead", func(r *rec) { p(r).KeepUp.Before.State = p(r).KeepUp.After.State }},
		"another OSD flagged":     {"observe-primary-dead", func(r *rec) { p(r).KeepUp.OSD = 1 }},
		"two flag commands":       {"observe-primary-dead", func(r *rec) { p(r).KeepUp.Commands = append(p(r).KeepUp.Commands, "osd add-nodown 1") }},
		"flag at another address": {"observe-primary-dead", func(r *rec) { p(r).KeepUp.After.Address = p(r).AddressAfter }},
		"appends before the flag": {"observe-primary-dead", func(r *rec) { p(r).KeepUp.After.Epoch = 31 }},
		// The window.
		"no window read":             {"observe-primary-dead", func(r *rec) { p(r).Dead = nil }},
		"marked down at the end":     {"observe-primary-dead", func(r *rec) { s(r, 2).Info.Up = false }},
		"a read saw another address": {"observe-primary-dead", func(r *rec) { s(r, 0).Info.Address = p(r).AddressAfter }},
		"window too short":           {"observe-primary-dead", func(r *rec) { s(r, 2).UTC = "2026-11-23T13:33:55.6Z" }},
		"window before the kill":     {"observe-primary-dead", func(r *rec) { s(r, 0).UTC = "2026-11-23T13:33:50Z" }},
		"samples out of order":       {"observe-primary-dead", func(r *rec) { s(r, 1).UTC = "2026-11-23T13:33:50.9Z" }},
		"one read only":              {"observe-primary-dead", func(r *rec) { p(r).Dead.Samples = p(r).Dead.Samples[2:] }},
		"an OSD down at the end":     {"observe-primary-dead", func(r *rec) { s(r, 2).Info.Down = []int32{2} }},
		"PG led by another member":   {"observe-primary-dead", func(r *rec) { s(r, 2).Objects[1].ActingPrimary = 1 }},
		"epoch went back":            {"observe-primary-dead", func(r *rec) { s(r, 0).Info.Epoch, s(r, 1).Info.Epoch = 31, 30 }},
		"another object followed":    {"observe-primary-dead", func(r *rec) { s(r, 1).Objects[2].Object = "journal.cd" }},
		// Violations in the middle sample only; the last sample has recovered.
		"nodown lost in the middle":      {"observe-primary-dead", func(r *rec) { s(r, 1).Info.State = []string{"exists", "up"} }},
		"marked down in the middle":      {"observe-primary-dead", func(r *rec) { s(r, 1).Info.Up = false }},
		"another OSD down in the middle": {"observe-primary-dead", func(r *rec) { s(r, 1).Info.Down = []int32{2} }},
		"acting set changed in the middle": {"observe-primary-dead", func(r *rec) {
			for i := range s(r, 1).Objects {
				s(r, 1).Objects[i].Acting = []int32{1, 2}
			}
		}},
		"up set changed in the middle": {"observe-primary-dead", func(r *rec) { s(r, 1).Objects[0].Up = []int32{1, 2, 0} }},
		"subject rerouted":             {"observe-primary-dead", func(r *rec) { p(r).AddressDead = p(r).AddressAfter }},
		"a lost read kept":             {"observe-primary-dead", func(r *rec) { r.Held.Lost = &nativeRBDJournalHeldRead{} }},
		// The return.
		"started within the window": {"observe-restarted", func(r *rec) { p(r).StartedUTC = "2026-11-23T13:33:59Z" }},
		"down before the window":    {"observe-restarted", func(r *rec) { p(r).Back.OSDInfo.DownAt = 30 }},
		"down before the window's highest epoch": {"observe-restarted", func(r *rec) {
			s(r, 1).Info.Epoch, s(r, 2).Info.Epoch, p(r).Back.OSDInfo.DownAt = 33, 33, 33
		}},
		"up before down":           {"observe-restarted", func(r *rec) { p(r).Back.OSDInfo.UpFrom = 33 }},
		"no OSD entry after":       {"observe-restarted", func(r *rec) { p(r).Back.OSDInfo = nil }},
		"address kept":             {"observe-restarted", func(r *rec) { p(r).AddressAfter, p(r).Back.OSDInfo.Address = p(r).AddressBefore, p(r).AddressBefore }},
		"interval before the boot": {"observe-restarted", func(r *rec) { p(r).Back.Query.SameIntervalSince = 33; p(r).Back.Query.SamePrimarySince = 33 }},
		"PG not clean after":       {"observe-restarted", func(r *rec) { p(r).Back.Query.State = "active+recovering" }},
		"entry 1 executed": {"observe-restarted", func(r *rec) {
			v := r.Views["observe-restarted"]
			v.ObjectSizes[1], v.ObjectSHA256[1] = p(r).Calls[1].BeforeSize+4198, p(r).Calls[1].AppliedSHA256
		}},
		"entry 0 lost": {"observe-restarted", func(r *rec) {
			v := r.Views["observe-restarted"]
			v.ObjectSizes[0], v.ObjectSHA256[0] = p(r).Calls[0].BeforeSize, p(r).Calls[0].BeforeSHA256
		}},
		"outcome known": {"observe-primary-dead", func(r *rec) {
			o := p(r).Calls[0].Outcome
			o.Class, o.UnknownOutcome, o.ReplyTransaction = "applied", false, o.Submission.Transaction
		}},
		"append resent": {"replay", func(r *rec) { r.Gate.LargeWrites = 3 }},
		"replayed twice": {"replay", func(r *rec) {
			v := r.Views["replay"]
			v.InspectOwned = append(v.InspectOwned, v.InspectOwned[0])
			*v.InspectTotal++
		}},
		"order: flag after the pause": {"observe-primary-dead", func(r *rec) {
			i, j := slices.Index(r.Order, "keep-up"), slices.Index(r.Order, "replica_paused")
			r.Order[i], r.Order[j] = r.Order[j], r.Order[i]
		}},
	}
	for name, c := range cases {
		r := rbdJournalBeforeDownSample()
		c.change(r)
		if len(judge(r, c.stage)) == 0 {
			t.Errorf("contradictory pre-down restart record accepted at %s: %s", c.stage, name)
		}
	}
}
