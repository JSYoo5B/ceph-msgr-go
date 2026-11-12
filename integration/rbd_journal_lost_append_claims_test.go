package integration_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// rbdJournalLostAppendSample is a consistent record of one run in the given
// connection mode, with synthetic values.
func rbdJournalLostAppendSample(mode string) *rbdJournalLostAppendRecord {
	p := func(s string) *string { return &s }
	n := func(v int) *int { return &v }
	u := func(v uint64) *uint64 { return &v }
	flag := func(v bool) *bool { return &v }
	hash := func(c string) string { return strings.Repeat(c, 64) }
	const subject, cookie = 4321, 77
	commit := []rbdJournalPosition{{0, 1, 4}, {3, 1, 3}, {2, 1, 2}, {1, 1, 1}}
	entry := rbdJournalLostAppendEntry{Tag: 2, EntryTID: 0, Object: 0, ObjectName: "journal_data.3.ab.0", Offset: 8347, Length: 4173, SHA256: hash("e"),
		ImageOffset: 40 << 10, ImageLength: 4 << 10, Timestamp: cephmsgr.OSDTime{Seconds: 1700000000, Nanoseconds: 123456789}}
	event := rbdJournalLostAppendEvent{"AioWrite", 40 << 10, 4 << 10, "2023-11-14T22:13:20.123456Z"}
	before := func(action string) *rbdJournalLostAppendView {
		return &rbdJournalLostAppendView{Action: action, ObjectSizes: []uint64{8347, 8269, 4173, 8269}, ObjectSHA256: []string{hash("0"), hash("1"), hash("2"), hash("3")},
			ImageHeadSHA256: hash("a"), ImageCommit: slices.Clone(commit), NextTag: 2, LastTag: rbdJournalLostAppendTag{TID: 1}, LockFree: true}
	}
	held := func(v *rbdJournalLostAppendView) *rbdJournalLostAppendView {
		v.NextTag, v.LastTag = 3, rbdJournalLostAppendTag{2, true, 1, 4}
		v.LockFree, v.LockOwner, v.LockCookie, v.SubjectWatcher, v.Watchers = false, "client.4321", "auto 77", true, 2
		return v
	}
	appended := func(v *rbdJournalLostAppendView) *rbdJournalLostAppendView {
		v.ObjectSizes[0], v.ObjectSHA256[0] = 8347+4173, hash("9")
		v.PrefixSHA256, v.EntrySHA256, v.TrailingBytes = p(hash("0")), p(hash("e")), u(0)
		return v
	}
	inspect := func(v *rbdJournalLostAppendView, total int, owned ...rbdJournalLostAppendEvent) *rbdJournalLostAppendView {
		v.InspectTotal, v.InspectErrors, v.InspectOwned = n(total), n(0), owned
		return v
	}
	locked := held(before("observe-locked"))
	locked.PrefixSHA256 = p(hash("0"))
	replay := inspect(appended(before("replay")), 6, event)
	replay.ImageHeadSHA256, replay.ImageCommit = hash("b"), []rbdJournalPosition{{0, 2, 0}, {3, 1, 3}, {2, 1, 2}, {1, 1, 1}}
	replay.NextTag, replay.LastTag = 4, rbdJournalLostAppendTag{3, true, 2, 0}
	r := &rbdJournalLostAppendRecord{SubjectGlobalID: subject, ObserverGlobalID: 4300, ConnectionMode: mode, WatchCookie: cookie, LockCookie: "auto 77", Entry: entry,
		Outcome: &rbdJournalLostAppendOutcome{Label: "journal_append", CallerInvocations: 1, Class: "uncertain", UnknownOutcome: true, Canceled: true, Error: "unknown",
			Submission: cephmsgr.OSDSubmission{Transaction: 9, GlobalID: subject}, RecordedUTC: "2023-11-14T22:13:21Z"},
		Gate:             &testcluster.OSDReplyGateState{Threshold: rbdJournalLostAppendThreshold, Connections: 2, Bound: true, BoundConnection: 1, BoundWriteBytes: 4400, LargeWrites: 1},
		DialsBeforeClose: n(1), DialsTotal: n(2), PendingAfterCheckpoint: flag(true), Order: slices.Clone(rbdJournalLostAppendOrder),
		Views: map[string]*rbdJournalLostAppendView{"setup": inspect(before("setup"), 5), "observe-locked": locked, "checkpoint": inspect(held(appended(before("checkpoint"))), 6, event),
			"observe-recorded": held(appended(before("observe-recorded"))), "replay": replay},
		ExpectedHeadBefore: hash("a"), ExpectedHeadAfter: hash("b"), ExpectedCommitAfter: slices.Clone(replay.ImageCommit)}
	if mode == "crc" {
		r.Gate.Decoded, r.Gate.Transaction, r.Gate.Sequence, r.Gate.MarkedRequests, r.Gate.MarkedConnections = true, u(9), u(12), n(1), []int{1}
		r.Gate.Held = &testcluster.OSDHeldReply{Connection: 1, Type: msgr.OSDOpReplyMessage, Transaction: 9, Sequence: 11, FrameBytes: 180}
		r.Barrier = "held reply"
	} else {
		r.Gate.WithheldBytesSeen, r.WithheldBeforeCancel, r.Barrier, r.BarrierPolls = flag(true), flag(true), "polled object size", n(2)
	}
	return r
}

// TestRBDJournalLostAppendClaims feeds the claims contradictory records. Each
// one keeps every other part of the record intact, so a claim that another
// part of the evidence would satisfy is still refused.
func TestRBDJournalLostAppendClaims(t *testing.T) {
	n := func(v int) *int { return &v }
	u := func(v uint64) *uint64 { return &v }
	p := func(s string) *string { return &s }
	flag := func(v bool) *bool { return &v }
	for _, mode := range []string{"crc", "secure"} {
		if problems := rbdJournalLostAppendClaims(rbdJournalLostAppendSample(mode), "replay"); len(problems) != 0 {
			t.Fatal("consistent record refused", mode, problems)
		}
		// Earlier stages judge only what exists by then: later observations,
		// the outcome and the gate's final counts are not required, and a
		// step beyond the stage is an order contradiction.
		for _, stage := range rbdJournalLostAppendStages[:2] {
			r := rbdJournalLostAppendSample(mode)
			if len(rbdJournalLostAppendClaims(r, stage)) == 0 {
				t.Fatal("steps beyond the stage accepted", mode, stage)
			}
			r.Order = r.Order[:slices.Index(r.Order, stage)+1]
			delete(r.Views, "replay")
			r.Gate, r.DialsBeforeClose, r.DialsTotal = nil, nil, nil
			if stage == "checkpoint" {
				delete(r.Views, "observe-recorded")
				r.Outcome = nil
			}
			if problems := rbdJournalLostAppendClaims(r, stage); len(problems) != 0 {
				t.Fatal("consistent partial record refused", mode, stage, problems)
			}
			r.Views["checkpoint"].LockFree = true
			if len(rbdJournalLostAppendClaims(r, stage)) == 0 {
				t.Fatal("early handoff accepted at", stage)
			}
		}
		if len(rbdJournalLostAppendClaims(rbdJournalLostAppendSample(mode), "final")) == 0 {
			t.Fatal("unknown stage accepted")
		}
	}
	type change = func(*rbdJournalLostAppendRecord)
	both := map[string]change{
		// Application.
		"entry absent at the checkpoint": func(r *rbdJournalLostAppendRecord) {
			r.Views["checkpoint"].EntrySHA256, r.Views["checkpoint"].TrailingBytes = nil, nil
		},
		"other bytes at the entry offset": func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].EntrySHA256 = p(strings.Repeat("f", 64)) },
		"bytes after the entry":           func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].TrailingBytes = u(4173) },
		"earlier journal bytes rewritten": func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].PrefixSHA256 = p(strings.Repeat("f", 64)) },
		"another journal object changed":  func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].ObjectSHA256[2] = strings.Repeat("f", 64) },
		"entry present before the append": func(r *rbdJournalLostAppendRecord) {
			r.Views["observe-locked"].EntrySHA256 = p(strings.Repeat("e", 64))
		},
		"inspect did not run":       func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].InspectTotal = nil },
		"inspect reported an error": func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].InspectErrors = n(1) },
		"inspect shows the entry twice": func(r *rbdJournalLostAppendRecord) {
			v := r.Views["checkpoint"]
			v.InspectOwned = append(v.InspectOwned, v.InspectOwned[0])
			v.InspectTotal = n(7)
		},
		"inspect shows another event":  func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].InspectOwned[0].Offset = 0 },
		"image changed while pending":  func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].ImageHeadSHA256 = r.ExpectedHeadAfter },
		"commit moved while pending":   func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].ImageCommit = r.ExpectedCommitAfter },
		"lock free at the checkpoint":  func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].LockFree = true },
		"another lock owner":           func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].LockOwner = "client.4300" },
		"subject watch gone":           func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"].SubjectWatcher = false },
		"caller not pending":           func(r *rbdJournalLostAppendRecord) { r.PendingAfterCheckpoint = flag(false) },
		"pending state not observed":   func(r *rbdJournalLostAppendRecord) { r.PendingAfterCheckpoint = nil },
		"checkpoint missing":           func(r *rbdJournalLostAppendRecord) { delete(r.Views, "checkpoint") },
		"checkpoint is another action": func(r *rbdJournalLostAppendRecord) { r.Views["checkpoint"] = r.Views["observe-recorded"] },
		// Outcome.
		"no outcome": func(r *rbdJournalLostAppendRecord) { r.Outcome = nil },
		"reply delivered": func(r *rbdJournalLostAppendRecord) {
			r.Outcome.Class, r.Outcome.UnknownOutcome, r.Outcome.ReplyTransaction = "applied", false, 9
		},
		"plain cancellation before sending": func(r *rbdJournalLostAppendRecord) { r.Outcome.UnknownOutcome = false },
		"keepalive failure, not a cancel":   func(r *rbdJournalLostAppendRecord) { r.Outcome.Canceled = false },
		"submission lost":                   func(r *rbdJournalLostAppendRecord) { r.Outcome.Submission.Transaction = 0 },
		"second caller invocation":          func(r *rbdJournalLostAppendRecord) { r.Outcome.CallerInvocations = 2 },
		"outcome not recorded":              func(r *rbdJournalLostAppendRecord) { r.Outcome.RecordedUTC = "" },
		// Order.
		"unlock before the record": func(r *rbdJournalLostAppendRecord) {
			i, j := slices.Index(r.Order, "outcome_recorded"), slices.Index(r.Order, "unlock")
			r.Order[i], r.Order[j] = r.Order[j], r.Order[i]
		},
		"extra step":                        func(r *rbdJournalLostAppendRecord) { r.Order = append(r.Order, "client_commit") },
		"lock handed off before the record": func(r *rbdJournalLostAppendRecord) { r.Views["observe-recorded"].LockFree = true },
		"commit advanced before the record": func(r *rbdJournalLostAppendRecord) { r.Views["observe-recorded"].ImageCommit = r.ExpectedCommitAfter },
		"new tag before the record":         func(r *rbdJournalLostAppendRecord) { r.Views["observe-recorded"].NextTag = 4 },
		"journal grew before the record":    func(r *rbdJournalLostAppendRecord) { r.Views["observe-recorded"].ObjectSizes[1] += 4173 },
		// Transmission.
		"gate state missing":              func(r *rbdJournalLostAppendRecord) { r.Gate = nil },
		"gate never bound":                func(r *rbdJournalLostAppendRecord) { r.Gate.Bound = false },
		"second write of the append size": func(r *rbdJournalLostAppendRecord) { r.Gate.LargeWrites = 2 },
		"bound on the later connection":   func(r *rbdJournalLostAppendRecord) { r.Gate.BoundConnection = 2 },
		"redial while silent":             func(r *rbdJournalLostAppendRecord) { r.DialsBeforeClose = n(2) },
		"third connection":                func(r *rbdJournalLostAppendRecord) { r.DialsTotal, r.Gate.Connections = n(3), 3 },
		"dials not counted":               func(r *rbdJournalLostAppendRecord) { r.DialsTotal = nil },
		"undecodable stream":              func(r *rbdJournalLostAppendRecord) { r.Gate.Error = "frame" },
		"gate mode differs from the row":  func(r *rbdJournalLostAppendRecord) { r.Gate.Decoded = !r.Gate.Decoded },
		// Replay.
		"replay appended to the journal": func(r *rbdJournalLostAppendRecord) {
			r.Views["replay"].ObjectSizes[1] += 78
			r.Views["replay"].ObjectSHA256[1] = strings.Repeat("f", 64)
		},
		"entry duplicated in the journal": func(r *rbdJournalLostAppendRecord) {
			r.Views["replay"].TrailingBytes = u(4173)
			r.Views["replay"].ObjectSizes[0] += 4173
		},
		"replay saw the entry twice": func(r *rbdJournalLostAppendRecord) {
			v := r.Views["replay"]
			v.InspectOwned = append(v.InspectOwned, v.InspectOwned[0])
			v.InspectTotal = n(7)
		},
		"entry not applied by the replay": func(r *rbdJournalLostAppendRecord) { r.Views["replay"].ImageHeadSHA256 = r.ExpectedHeadBefore },
		"replay did not commit the entry": func(r *rbdJournalLostAppendRecord) { r.Views["replay"].ImageCommit = r.Views["setup"].ImageCommit },
		"model expects no change": func(r *rbdJournalLostAppendRecord) {
			r.ExpectedHeadAfter = r.ExpectedHeadBefore
			r.Views["replay"].ImageHeadSHA256 = r.ExpectedHeadBefore
		},
		"native tag has another predecessor": func(r *rbdJournalLostAppendRecord) { r.Views["replay"].LastTag.PredecessorTag = 1 },
		"lock still held after the replay":   func(r *rbdJournalLostAppendRecord) { r.Views["replay"].LockFree = false },
		"subject watch left behind":          func(r *rbdJournalLostAppendRecord) { r.Views["replay"].SubjectWatcher = true },
	}
	decoded := map[string]change{
		"marked request written twice": func(r *rbdJournalLostAppendRecord) {
			r.Gate.MarkedRequests, r.Gate.MarkedConnections = n(2), []int{1, 2}
		},
		"marked request not counted":        func(r *rbdJournalLostAppendRecord) { r.Gate.MarkedRequests = nil },
		"marked request never written":      func(r *rbdJournalLostAppendRecord) { r.Gate.MarkedRequests, r.Gate.MarkedConnections = n(0), nil },
		"no reply held":                     func(r *rbdJournalLostAppendRecord) { r.Gate.Held = nil },
		"held reply of another transaction": func(r *rbdJournalLostAppendRecord) { r.Gate.Held.Transaction = 8 },
		"held frame is not an OSD reply":    func(r *rbdJournalLostAppendRecord) { r.Gate.Held.Type = msgr.OSDOpMessage },
		"bound to another transaction":      func(r *rbdJournalLostAppendRecord) { r.Gate.Transaction = u(8) },
		"barrier was a poll":                func(r *rbdJournalLostAppendRecord) { r.Barrier, r.BarrierPolls = "polled object size", n(1) },
		"claims an undecoded withheld flag": func(r *rbdJournalLostAppendRecord) { r.WithheldBeforeCancel = flag(true) },
	}
	undecoded := map[string]change{
		"claims a transaction": func(r *rbdJournalLostAppendRecord) { r.Gate.Transaction = u(9) },
		"claims a held frame": func(r *rbdJournalLostAppendRecord) {
			r.Gate.Held = &testcluster.OSDHeldReply{Connection: 1, Type: msgr.OSDOpReplyMessage, Transaction: 9, Sequence: 1, FrameBytes: 1}
		},
		"claims a marked-request count": func(r *rbdJournalLostAppendRecord) { r.Gate.MarkedRequests = n(1) },
		"no barrier poll":               func(r *rbdJournalLostAppendRecord) { r.BarrierPolls = nil },
		"claims the held-reply barrier": func(r *rbdJournalLostAppendRecord) { r.Barrier = "held reply" },
		// The gate bound to the write but intercepted no received byte: the
		// flag is present and false, before the cancellation and finally.
		"nothing withheld": func(r *rbdJournalLostAppendRecord) {
			r.Gate.WithheldBytesSeen, r.WithheldBeforeCancel = flag(false), flag(false)
		},
		// Bytes were first intercepted after the wait was canceled.
		"withheld only after the cancellation":           func(r *rbdJournalLostAppendRecord) { r.WithheldBeforeCancel = flag(false) },
		"withheld flag not read before the cancellation": func(r *rbdJournalLostAppendRecord) { r.WithheldBeforeCancel = nil },
		"final gate state lost the withheld flag":        func(r *rbdJournalLostAppendRecord) { r.Gate.WithheldBytesSeen = flag(false) },
		"final gate state has no withheld flag":          func(r *rbdJournalLostAppendRecord) { r.Gate.WithheldBytesSeen = nil },
	}
	for mode, specific := range map[string]map[string]change{"crc": decoded, "secure": undecoded} {
		for _, set := range []map[string]change{both, specific} {
			for name, mutate := range set {
				r := rbdJournalLostAppendSample(mode)
				mutate(r)
				if len(rbdJournalLostAppendClaims(r, "replay")) == 0 {
					t.Errorf("%s: contradictory record accepted: %s", mode, name)
				}
			}
		}
	}
}
