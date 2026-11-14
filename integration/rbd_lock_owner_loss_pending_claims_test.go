package integration_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// rbdLockOwnerLossPendingSample is a consistent record of one run of the
// pending variant up to the given stage, with synthetic values. It is the
// ordinary sample with the two pending requests, the fenced attempts and the
// read on a new connection moved behind their answers, and the advance-map
// receipt.
func rbdLockOwnerLossPendingSample(stage string) *rbdLockOwnerLossRecord {
	base := stage
	if stage == "answered" {
		base = "go-break"
	}
	r := rbdLockOwnerLossSample(base)
	z := r.Zombie
	p := &rbdLockOwnerLossPending{SentAtEpoch: 24, NamedEpoch: 24 + rbdLockOwnerLossPendingEpochs, PendingAfterGoOwner: true,
		Requests: []rbdLockOwnerLossAttempt{{Label: rbdLockOwnerLossCanceled, Invocations: 1}, {Label: rbdLockOwnerLossAwaited, Invocations: 1}}}
	r.Pending = p
	r.Order = slices.Clone(rbdLockOwnerLossPendingOrder[:slices.Index(rbdLockOwnerLossPendingOrder, rbdLockOwnerLossPendingStageEnd[stage])+1])
	if stage == "go-owner" {
		return r
	}
	// The zombie is fenced: the first wait is canceled, nothing else is sent.
	p.Requests[0] = rbdLockOwnerLossAttempt{Label: rbdLockOwnerLossCanceled, Invocations: 1, Class: "uncertain", UnknownOutcome: true, Canceled: true, Error: "ceph: request outcome unknown: context canceled",
		Submission: cephmsgr.OSDSubmission{Transaction: 10, GlobalID: z.GlobalID, MapEpoch: p.NamedEpoch}}
	p.CanceledUTC, p.PendingAfterTakeover = "2026-11-13T01:00:31Z", true
	r.Fenced, r.NewConnection, r.NewConnectionLockers = nil, nil, nil
	if stage == "takeover" {
		return r
	}
	p.PendingAfterGoBreak = true
	if stage == "go-break" {
		return r
	}
	// The epoch is raised; the second request and the attempts are answered.
	p.Requests[1] = rbdLockOwnerLossAttempt{Label: rbdLockOwnerLossAwaited, Invocations: 1, Class: "refused", Code: rbdOSDBlocklisted, ErrorCode: rbdOSDBlocklisted, Error: "refused",
		Submission: cephmsgr.OSDSubmission{Transaction: 11, GlobalID: z.GlobalID, MapEpoch: p.NamedEpoch}, ReplyTransaction: 11, ReplyMapEpoch: 33, OperationResults: []int32{0, 0}}
	p.AnsweredUTC = "2026-11-13T01:01:40Z"
	held := func(tid uint64) rbdLockOwnerLossOp {
		return rbdLockOwnerLossOp{OSD: 0, Description: "osd_op(client.4791.0:" + string(rune('0'+tid-10)) + ")", Client: "client.4791", Transaction: tid, InitiatedAt: "2026-11-13T00:59:59.500000+0000", Age: 95, FlagPoint: "no flag points reached"}
	}
	p.Advance = &rbdLockOwnerLossAdvance{Pool: "ceph-msgr-rbd", Flag: "nodeep-scrub", EpochBefore: 27, EpochTarget: 35, EpochAfter: 35, Epochs: []uint32{28, 29, 30, 31, 32, 33, 34, 35},
		Commands:  slices.Repeat([]string{"osd pool set ceph-msgr-rbd nodeep-scrub true", "osd pool set ceph-msgr-rbd nodeep-scrub false"}, 4),
		OpsBefore: []rbdLockOwnerLossOp{held(10), held(11)}, OpsAfter: []rbdLockOwnerLossOp{}}
	for i, label := range rbdLockOwnerLossFenced {
		tid := uint64(12 + i)
		r.Fenced = append(r.Fenced, rbdLockOwnerLossAttempt{Label: label, Invocations: 1, Class: "refused", Code: rbdOSDBlocklisted, ErrorCode: rbdOSDBlocklisted, Error: "refused",
			Submission: cephmsgr.OSDSubmission{Transaction: tid, GlobalID: z.GlobalID, MapEpoch: 35}, ReplyTransaction: tid, ReplyMapEpoch: 35})
	}
	r.NewConnection = &rbdLockOwnerLossAttempt{Label: "new_connection_lock_get_info", Invocations: 1, Class: "applied", Submission: cephmsgr.OSDSubmission{Transaction: 15, GlobalID: z.GlobalID}, ReplyTransaction: 15}
	broke := r.Views["observe-go-broke"]
	r.NewConnectionLockers = slices.Clone(broke.HeaderLockers)
	answered := *broke
	answered.Label, answered.Objects, answered.Watchers, answered.Blocklist = "observe-answered", slices.Clone(broke.Objects), slices.Clone(broke.Watchers), slices.Clone(broke.Blocklist)
	answered.HeaderLockers, answered.MapLockers, answered.MapStates = slices.Clone(broke.HeaderLockers), slices.Clone(broke.MapLockers), slices.Clone(broke.MapStates)
	r.Views["observe-answered"] = &answered
	return r
}

// TestRBDLockOwnerLossPendingClaims feeds the claims of the pending variant
// contradictory records. Each one keeps every other part of the record
// intact. The claims the variant shares with the ordinary sequence are
// covered by TestRBDLockOwnerLossClaims.
func TestRBDLockOwnerLossPendingClaims(t *testing.T) {
	for i, stage := range rbdLockOwnerLossPendingStages {
		if problems := rbdLockOwnerLossClaims(rbdLockOwnerLossPendingSample(stage), stage); len(problems) != 0 {
			t.Fatal("consistent record refused at", stage, problems)
		}
		for j, other := range rbdLockOwnerLossPendingStages {
			if i != j && len(rbdLockOwnerLossClaims(rbdLockOwnerLossPendingSample(stage), other)) == 0 {
				t.Fatal("record of stage", stage, "accepted at", other)
			}
		}
	}
	// The two variants do not pass for each other: an ordinary record has no
	// "answered" stage, and a record that only gained or lost its pending
	// part keeps the other variant's step order.
	if len(rbdLockOwnerLossClaims(rbdLockOwnerLossSample("final"), "answered")) == 0 {
		t.Fatal("ordinary record accepted at the pending variant's stage")
	}
	for _, stage := range rbdLockOwnerLossStages {
		r := rbdLockOwnerLossPendingSample(stage)
		r.Pending = nil
		if len(rbdLockOwnerLossClaims(r, stage)) == 0 {
			t.Fatal("pending run without its pending record accepted at", stage)
		}
		r = rbdLockOwnerLossSample(stage)
		r.Pending = rbdLockOwnerLossPendingSample(stage).Pending
		if len(rbdLockOwnerLossClaims(r, stage)) == 0 {
			t.Fatal("ordinary run with a pending record accepted at", stage)
		}
	}
	type record = rbdLockOwnerLossRecord
	applied := func(a *rbdLockOwnerLossAttempt) {
		a.Class, a.Code, a.ErrorCode, a.Error, a.UnknownOutcome, a.Canceled = "applied", 0, 0, "", false, false
		a.ReplyTransaction = a.Submission.Transaction
	}
	cases := map[string]struct {
		stage  string
		change func(*record)
	}{
		// go-owner: both requests are held because of the epoch they name.
		"requests name the current epoch":    {"go-owner", func(r *record) { r.Pending.NamedEpoch = r.Pending.SentAtEpoch }},
		"requests name another distance":     {"go-owner", func(r *record) { r.Pending.NamedEpoch++ }},
		"epoch before sending unknown":       {"go-owner", func(r *record) { r.Pending.SentAtEpoch, r.Pending.NamedEpoch = 0, rbdLockOwnerLossPendingEpochs }},
		"cluster already at the named epoch": {"go-owner", func(r *record) { r.Views["go-owner"].Epoch = r.Pending.NamedEpoch }},
		"calls not seen blocked":             {"go-owner", func(r *record) { r.Pending.PendingAfterGoOwner = false }},
		"one pending request":                {"go-owner", func(r *record) { r.Pending.Requests = r.Pending.Requests[:1] }},
		"third pending request":              {"go-owner", func(r *record) { r.Pending.Requests = append(r.Pending.Requests, r.Pending.Requests[1]) }},
		"requests in the other roles": {"go-owner", func(r *record) {
			r.Pending.Requests[0].Label, r.Pending.Requests[1].Label = r.Pending.Requests[1].Label, r.Pending.Requests[0].Label
		}},
		"pending step missing": {"go-owner", func(r *record) {
			r.Order = slices.DeleteFunc(r.Order, func(s string) bool { return s == "pending_submitted" })
		}},

		// takeover: the fence comes first, then the cancellation; the other
		// request stays unanswered and nothing else is sent.
		"canceled request was answered": {"takeover", func(r *record) {
			c := &r.Pending.Requests[0]
			c.Class, c.Code, c.ErrorCode, c.UnknownOutcome, c.Canceled, c.ReplyTransaction = "refused", rbdOSDBlocklisted, rbdOSDBlocklisted, false, false, 10
		}},
		"canceled request was applied":        {"takeover", func(r *record) { applied(&r.Pending.Requests[0]) }},
		"wait ended by something else":        {"takeover", func(r *record) { r.Pending.Requests[0].Canceled = false }},
		"plain cancellation before sending":   {"takeover", func(r *record) { r.Pending.Requests[0].UnknownOutcome = false }},
		"canceled request never submitted":    {"takeover", func(r *record) { r.Pending.Requests[0].Submission.Transaction = 0 }},
		"canceled request sent later":         {"takeover", func(r *record) { r.Pending.Requests[0].Submission.Transaction = 12 }},
		"canceled request of another client":  {"takeover", func(r *record) { r.Pending.Requests[0].Submission.GlobalID = r.Observer.GlobalID }},
		"canceled request at another epoch":   {"takeover", func(r *record) { r.Pending.Requests[0].Submission.MapEpoch = 26 }},
		"canceled request invoked twice":      {"takeover", func(r *record) { r.Pending.Requests[0].Invocations = 2 }},
		"canceled before the fence":           {"takeover", func(r *record) { r.Pending.CanceledUTC = "2026-11-13T00:59:59Z" }},
		"cancellation time missing":           {"takeover", func(r *record) { r.Pending.CanceledUTC = "" }},
		"fence observation time missing":      {"takeover", func(r *record) { r.Views["native-owner"].ObservedUTC = "" }},
		"second request answered at the kill": {"takeover", func(r *record) { r.Pending.PendingAfterTakeover = false }},
		"named epoch reached at the request":  {"takeover", func(r *record) { r.Views["request-while-listed"].Epoch = r.Pending.NamedEpoch }},
		"named epoch reached by the fence":    {"takeover", func(r *record) { r.Views["native-owner"].Epoch = r.Pending.NamedEpoch + 1 }},
		"epoch went backwards":                {"takeover", func(r *record) { r.Views["native-owner"].Epoch = r.Pending.SentAtEpoch - 1 }},
		"cancel step missing": {"takeover", func(r *record) {
			r.Order = slices.DeleteFunc(r.Order, func(s string) bool { return s == "pending_canceled" })
		}},
		"pending data object seen natively": {"takeover", func(r *record) { r.Views["takeover"].Objects[4] = strings.Repeat("f", 64) }},
		"pending map update seen natively":  {"takeover", func(r *record) { r.Views["takeover"].MapStates = []int{1, 1, 0, 0, 1} }},

		// go-break: still unanswered while the breaker took the lock.
		"second request answered in the break": {"go-break", func(r *record) { r.Pending.PendingAfterGoBreak = false }},
		"named epoch reached in the break":     {"go-break", func(r *record) { r.Views["blocklisted"].Epoch = r.Pending.NamedEpoch }},
		"named epoch reached before the watch": {"go-break", func(r *record) { r.Views["dead-owner-listed"].Epoch = r.Pending.NamedEpoch }},
		"pending object in the breaker's view": {"go-break", func(r *record) { r.Views["observe-go-broke"].Objects[4] = strings.Repeat("f", 64) }},

		// answered: the epoch was raised from below the named one, the OSD
		// held both requests since before the handoff, and answered the
		// second with the blocklisted code.
		"advance receipt missing":         {"answered", func(r *record) { r.Pending.Advance = nil }},
		"epoch was already there":         {"answered", func(r *record) { r.Pending.Advance.EpochBefore = r.Pending.NamedEpoch }},
		"advance began before the break":  {"answered", func(r *record) { r.Pending.Advance.EpochBefore = 26 }},
		"epoch stopped below the named":   {"answered", func(r *record) { r.Pending.Advance.EpochAfter = r.Pending.NamedEpoch - 1 }},
		"epoch stopped below its target":  {"answered", func(r *record) { r.Pending.Advance.EpochAfter = 34 }},
		"pool flag left set":              {"answered", func(r *record) { r.Pending.Advance.FlagFinally = true }},
		"no command issued":               {"answered", func(r *record) { r.Pending.Advance.Commands = nil }},
		"OSD did not hold the canceled":   {"answered", func(r *record) { r.Pending.Advance.OpsBefore = r.Pending.Advance.OpsBefore[1:] }},
		"OSD did not hold the awaited":    {"answered", func(r *record) { r.Pending.Advance.OpsBefore = r.Pending.Advance.OpsBefore[:1] }},
		"OSD held nothing":                {"answered", func(r *record) { r.Pending.Advance.OpsBefore = nil }},
		"held request of another client":  {"answered", func(r *record) { r.Pending.Advance.OpsBefore[1].Client = "client.4788" }},
		"held request with another tid":   {"answered", func(r *record) { r.Pending.Advance.OpsBefore[0].Transaction = 9 }},
		"OSD received it after the start": {"answered", func(r *record) { r.Pending.Advance.OpsBefore[1].InitiatedAt = "2026-11-13T01:00:00.500000+0000" }},
		"receipt time unreadable":         {"answered", func(r *record) { r.Pending.Advance.OpsBefore[0].InitiatedAt = "yesterday" }},
		"OSD still holds a request": {"answered", func(r *record) {
			r.Pending.Advance.OpsAfter = r.Pending.Advance.OpsBefore[:1]
		}},
		"OSD holds a new zombie request": {"answered", func(r *record) {
			r.Pending.Advance.OpsAfter = []rbdLockOwnerLossOp{{Client: "client.4791", Transaction: 14}}
		}},
		"awaited request applied": {"answered", func(r *record) { applied(&r.Pending.Requests[1]) }},
		"awaited request applied and seen": {"answered", func(r *record) {
			applied(&r.Pending.Requests[1])
			r.Views["observe-answered"].Objects[4] = strings.Repeat("f", 64)
		}},
		"awaited request refused but seen": {"answered", func(r *record) { r.Views["observe-answered"].Objects[4] = strings.Repeat("f", 64) }},
		"awaited request unknown": {"answered", func(r *record) {
			w := &r.Pending.Requests[1]
			w.Class, w.UnknownOutcome, w.Code, w.ErrorCode, w.ReplyTransaction = "uncertain", true, 0, 0, 0
		}},
		"awaited request canceled too":      {"answered", func(r *record) { r.Pending.Requests[1].Canceled = true }},
		"awaited request with another code": {"answered", func(r *record) { r.Pending.Requests[1].Code, r.Pending.Requests[1].ErrorCode = -16, -16 }},
		"answered below the named epoch":    {"answered", func(r *record) { r.Pending.Requests[1].ReplyMapEpoch = r.Pending.NamedEpoch - 1 }},
		"awaited request sent twice":        {"answered", func(r *record) { r.Pending.Requests[1].Invocations = 2 }},
		"awaited request sent again later": {"answered", func(r *record) {
			r.Pending.Requests[1].Submission.Transaction, r.Pending.Requests[1].ReplyTransaction = 16, 16
		}},
		"both requests are one transaction": {"answered", func(r *record) {
			r.Pending.Requests[1].Submission.Transaction, r.Pending.Requests[1].ReplyTransaction = 10, 10
		}},
		"answer of another transaction":    {"answered", func(r *record) { r.Pending.Requests[1].ReplyTransaction = 10 }},
		"awaited request at another epoch": {"answered", func(r *record) { r.Pending.Requests[1].Submission.MapEpoch = 27 }},
		"attempts missing":                 {"answered", func(r *record) { r.Fenced = nil }},
		"attempt applied":                  {"answered", func(r *record) { applied(&r.Fenced[1]) }},
		"attempts before the answers": {"answered", func(r *record) {
			for i := range r.Fenced {
				r.Fenced[i].Submission.Transaction, r.Fenced[i].ReplyTransaction = uint64(9+i), uint64(9+i)
			}
		}},
		"request between the attempts": {"answered", func(r *record) {
			r.Fenced[2].Submission.Transaction, r.Fenced[2].ReplyTransaction, r.NewConnection.Submission.Transaction = 15, 15, 16
		}},
		"new connection not read":            {"answered", func(r *record) { r.NewConnection = nil }},
		"new connection refused":             {"answered", func(r *record) { r.NewConnection.Class, r.NewConnection.Code = "refused", rbdOSDBlocklisted }},
		"new connection read the dead owner": {"answered", func(r *record) { r.NewConnectionLockers = r.Views["takeover"].HeaderLockers }},
		"new connection before the attempts": {"answered", func(r *record) { r.NewConnection.Submission.Transaction = 12 }},
		"object map changed by the answers":  {"answered", func(r *record) { r.Views["observe-answered"].MapRawHex = "map [1 1 1 0 1]" }},
		"object map marked by the answers":   {"answered", func(r *record) { r.Views["observe-answered"].MapStates = []int{1, 1, 1, 0, 1} }},
		"lock changed by the answers":        {"answered", func(r *record) { r.Views["observe-answered"].HeaderLockers = nil }},
		"lock bytes changed by the answers":  {"answered", func(r *record) { r.Views["observe-answered"].MapLockHex = "other" }},
		"data object changed by the answers": {"answered", func(r *record) { r.Views["observe-answered"].Objects[2] = strings.Repeat("f", 64) }},
		"image changed by the answers":       {"answered", func(r *record) { r.Views["observe-answered"].ImageHeadSHA256 = strings.Repeat("f", 64) }},
		"image flagged after the answers":    {"answered", func(r *record) { r.Views["observe-answered"].Flags = 1 }},
		"observation after the answers gone": {"answered", func(r *record) { delete(r.Views, "observe-answered") }},
		"observation is a Go view":           {"answered", func(r *record) { r.Views["observe-answered"].Source = "go" }},
		"breaker blocklisted by the answers": {"answered", func(r *record) { r.Views["observe-answered"].Blocklist[0].Address = r.Breaker.Address }},
		"answers before the epoch was raised": {"answered", func(r *record) {
			i, j := slices.Index(r.Order, "advance-map"), slices.Index(r.Order, "pending_answered")
			r.Order[i], r.Order[j] = r.Order[j], r.Order[i]
		}},
	}
	for name, c := range cases {
		r := rbdLockOwnerLossPendingSample(c.stage)
		c.change(r)
		if len(rbdLockOwnerLossClaims(r, c.stage)) == 0 {
			t.Error("contradictory record accepted:", name)
		}
		// The same contradiction is refused in a complete record.
		if c.stage != "final" {
			r = rbdLockOwnerLossPendingSample("final")
			c.change(r)
			if len(rbdLockOwnerLossClaims(r, "final")) == 0 {
				t.Error("contradictory record accepted in a complete run:", name)
			}
		}
	}
	t.Logf("%d contradictory records refused", len(cases))
}
