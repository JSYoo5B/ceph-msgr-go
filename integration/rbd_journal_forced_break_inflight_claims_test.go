package integration_test

import (
	"slices"
	"strings"
	"testing"
)

// rbdJournalForcedInflightSample is a consistent record of one in-flight
// forced-break run in the given connection mode: the lost-append sample up to
// its checkpoint, then the break, the recorded outcome, the fence and the
// takeover.
func rbdJournalForcedInflightSample(mode string) *rbdJournalLostAppendRecord {
	n := func(v int) *int { return &v }
	r := rbdJournalLostAppendSample(mode)
	block := []rbdLockOwnerLossBlock{{rbdJournalOwnerLossSampleAddress, "2023-11-14T23:13:20Z"}}
	delete(r.Views, "replay")
	delete(r.Views, "observe-recorded")
	r.Order = slices.Clone(rbdJournalForcedInflightOrder)
	r.Gate.Connections, r.DialsBeforeClose, r.DialsTotal = 1, n(1), n(1)
	fenced := func(action string) *rbdJournalLostAppendView {
		v := *r.Views["checkpoint"]
		v.Action, v.ObjectSizes, v.ObjectSHA256, v.ImageCommit = action, slices.Clone(v.ObjectSizes), slices.Clone(v.ObjectSHA256), slices.Clone(v.ImageCommit)
		v.InspectTotal, v.InspectErrors, v.InspectOwned = nil, nil, nil
		v.LockFree, v.LockOwner, v.LockCookie, v.SubjectWatcher, v.Watchers, v.Blocklist = true, "", "", false, 0, slices.Clone(block)
		v.ObservedUTC = "2023-11-14T22:13:20.9Z"
		return &v
	}
	r.Views["force-break"], r.Views["observe-fenced"] = fenced("force-break"), fenced("observe-fenced")
	r.Views["observe-fenced"].ObservedUTC = "2023-11-14T22:13:22Z"
	r.OwnerLoss = &rbdJournalOwnerLoss{Forced: true, LockerAddress: rbdJournalOwnerLossSampleAddress,
		Break: &rbdJournalBreak{Owners: []string{rbdJournalOwnerLossSampleAddress}, OwnersAfter: []string{}, BreakNS: 3000, LockerBefore: "client.4321", LockerCookieBefore: r.LockCookie,
			WatchersBefore:      []rbdJournalWatch{{r.SubjectGlobalID, r.WatchCookie, rbdJournalOwnerLossSampleAddress}},
			LockerAddressBefore: rbdJournalOwnerLossSampleAddress, BlocklistBefore: []rbdLockOwnerLossBlock{}, Blocklist: slices.Clone(block),
			LockPath: rbdJournalLockPathSample(rbdJournalForcedBreakLockPath)},
		PingBefore: &rbdLockOwnerLossAttempt{Label: "watch_ping", Invocations: 1, Class: "applied"}}
	r.Inflight = &rbdJournalForcedInflight{PingedUTC: "2023-11-14T22:13:19Z", PendingAfterBreak: true}
	rbdJournalTakeoverSample(r, true)
	return r
}

// TestRBDJournalForcedInflightClaims feeds the in-flight forced-break claims
// contradictory records in both connection modes.
func TestRBDJournalForcedInflightClaims(t *testing.T) {
	n := func(v int) *int { return &v }
	flag := func(v bool) *bool { return &v }
	for _, mode := range []string{"crc", "secure"} {
		if problems := rbdJournalLostAppendClaims(rbdJournalForcedInflightSample(mode), "takeover"); len(problems) != 0 {
			t.Fatal("consistent in-flight forced-break record refused", mode, problems)
		}
		for _, stage := range rbdJournalForcedInflightStages[:2] {
			r := rbdJournalForcedInflightSample(mode)
			if len(rbdJournalLostAppendClaims(r, stage)) == 0 {
				t.Fatal("steps beyond the stage accepted", mode, stage)
			}
			r.Order = r.Order[:slices.Index(r.Order, stage)+1]
			delete(r.Views, "takeover")
			r.OwnerLoss.Takeover = nil
			if stage == "checkpoint" {
				delete(r.Views, "force-break")
				delete(r.Views, "observe-fenced")
				r.OwnerLoss.Break, r.Outcome, r.Inflight.PendingAfterBreak = nil, nil, false
			}
			if problems := rbdJournalLostAppendClaims(r, stage); len(problems) != 0 {
				t.Fatal("consistent partial in-flight forced-break record refused", mode, stage, problems)
			}
		}
	}
	if len(rbdJournalLostAppendClaims(rbdJournalForcedInflightSample("crc"), "replay")) == 0 {
		t.Fatal("unknown stage accepted")
	}
	type change = func(*rbdJournalLostAppendRecord)
	cases := map[string]change{
		"setup entry duplicated at the checkpoint": func(r *rbdJournalLostAppendRecord) {
			v := r.Views["checkpoint"]
			v.InspectOther = append([]rbdJournalInspectEntry{v.InspectOther[0]}, v.InspectOther[:len(v.InspectOther)-1]...)
		},
		"setup entry duplicated at the takeover": func(r *rbdJournalLostAppendRecord) {
			v := r.Views["takeover"]
			v.InspectOther = append([]rbdJournalInspectEntry{v.InspectOther[0]}, v.InspectOther[:len(v.InspectOther)-1]...)
		},
		// The owner was live and the caller pending at the checkpoint.
		"no ping before the append":       func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.PingBefore = nil },
		"ping refused":                    func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.PingBefore.Class = "refused" },
		"entry absent at the checkpoint":  func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].EntrySHA256 = nil },
		"entry twice at the checkpoint":   func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].InspectTotal = n(7) },
		"lock free at the checkpoint":     func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].LockFree = true },
		"watch gone at the checkpoint":    func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].SubjectWatcher = false },
		"caller done at the checkpoint":   func(r *rbdJournalLostAppendRecord) { r.PendingAfterCheckpoint = flag(false) },
		"image applied at the checkpoint": func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].ImageHeadSHA256 = r.ExpectedHeadAfter },
		// The break with the reply in flight.
		"caller answered during the break":  func(r *rbdJournalLostAppendRecord) { r.Inflight.PendingAfterBreak = false },
		"subject watch not listed at break": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Break.WatchersBefore = nil },
		"blocklisted before the break":      func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Break.BlocklistBefore = r.OwnerLoss.Break.Blocklist },
		"break without blocklist":           func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Break.Blocklist = []rbdLockOwnerLossBlock{} },
		"break did not see the live owner": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Break.LockPath.Steps = slices.Delete(r.OwnerLoss.Break.LockPath.Steps, 1, 2)
		},
		"lock held after the break":    func(r *rbdJournalLostAppendRecord) { r.Views["force-break"].LockFree = false },
		"journal changed by the break": func(r *rbdJournalLostAppendRecord) { r.Views["force-break"].ObjectSizes[0] = 0 },
		// What the caller was told.
		"outcome known": func(r *rbdJournalLostAppendRecord) {
			r.Outcome.Class, r.Outcome.UnknownOutcome, r.Outcome.ReplyTransaction = "applied", false, 9
		},
		"outcome refused":               func(r *rbdJournalLostAppendRecord) { r.Outcome.Class, r.Outcome.ReplyCode = "refused", -108 },
		"append sent twice":             func(r *rbdJournalLostAppendRecord) { r.Outcome.CallerInvocations = 2 },
		"outcome recorded before break": func(r *rbdJournalLostAppendRecord) { r.Outcome.RecordedUTC = "2023-11-14T22:13:20Z" },
		// The fence.
		"entry appended again": func(r *rbdJournalLostAppendRecord) {
			v := r.Views["observe-fenced"]
			v.ObjectSizes[0] += 4173
			v.TrailingBytes = r.Views["takeover"].TrailingBytes
		},
		"entry removed by the break": func(r *rbdJournalLostAppendRecord) { r.Views["observe-fenced"].EntrySHA256 = nil },
		"fenced lock held":           func(r *rbdJournalLostAppendRecord) { r.Views["observe-fenced"].LockFree = false },
		"fenced watch listed":        func(r *rbdJournalLostAppendRecord) { r.Views["observe-fenced"].SubjectWatcher = true },
		"fenced image applied":       func(r *rbdJournalLostAppendRecord) { r.Views["observe-fenced"].ImageHeadSHA256 = r.ExpectedHeadAfter },
		"fenced address free":        func(r *rbdJournalLostAppendRecord) { r.Views["observe-fenced"].Blocklist = []rbdLockOwnerLossBlock{} },
		// Transmission and the takeover.
		"second subject connection": func(r *rbdJournalLostAppendRecord) { r.Gate.Connections, r.DialsTotal = 2, n(2) },
		"gate not bound":            func(r *rbdJournalLostAppendRecord) { r.Gate.Bound = false },
		"second large write":        func(r *rbdJournalLostAppendRecord) { r.Gate.LargeWrites = 2 },
		"takeover replayed twice": func(r *rbdJournalLostAppendRecord) {
			v := r.Views["takeover"]
			v.InspectOwned = append(v.InspectOwned, v.InspectOwned[0])
			v.InspectTotal = n(8)
		},
		"takeover without replay": func(r *rbdJournalLostAppendRecord) { r.Views["takeover"].ImageHeadSHA256 = strings.Repeat("d", 64) },
		"takeover found a locker": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.LockPath.Steps[0] = "acquire.handle_get_locker r=0"
		},
		"order: outcome before the break": func(r *rbdJournalLostAppendRecord) {
			i, j := slices.Index(r.Order, "force-break"), slices.Index(r.Order, "outcome_recorded")
			r.Order[i], r.Order[j] = r.Order[j], r.Order[i]
		},
	}
	for name, mutate := range cases {
		for _, mode := range []string{"crc", "secure"} {
			r := rbdJournalForcedInflightSample(mode)
			mutate(r)
			if len(rbdJournalLostAppendClaims(r, "takeover")) == 0 {
				t.Errorf("contradictory in-flight forced-break record accepted (%s): %s", mode, name)
			}
		}
	}
	for name, mutate := range map[string]change{
		"decoded gate held another transaction": func(r *rbdJournalLostAppendRecord) { r.Gate.Held.Transaction = 10 },
		"decoded gate held nothing":             func(r *rbdJournalLostAppendRecord) { r.Gate.Held = nil },
	} {
		r := rbdJournalForcedInflightSample("crc")
		mutate(r)
		if len(rbdJournalLostAppendClaims(r, "takeover")) == 0 {
			t.Errorf("contradictory in-flight forced-break record accepted: %s", name)
		}
	}
	r := rbdJournalForcedInflightSample("secure")
	r.WithheldBeforeCancel = flag(false)
	if len(rbdJournalLostAppendClaims(r, "takeover")) == 0 {
		t.Error("undecoded gate without withheld bytes accepted")
	}
}
