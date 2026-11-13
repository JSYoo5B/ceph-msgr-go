package integration_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// rbdJournalPrimaryChangeSample is a consistent record of one run of the
// primary-change variant up to the given stage, with synthetic values: the
// lost-append sample on three OSDs. The one PG is led by osd.1, which is
// marked out; osd.0 leads it afterwards and osd.1 is the last member of its
// acting set.
func rbdJournalPrimaryChangeSample(mode, stage string) *rbdJournalLostAppendRecord {
	n := func(v int) *int { return &v }
	r := rbdJournalLostAppendSample(mode)
	r.Pool, r.PoolID, r.ImageID, r.JournalHeader = "ceph-msgr-rbd", 3, "ab", "journal.ab"
	objects := []string{"rbd_header.ab", "journal_data.3.ab.0", "journal.ab"}
	place := func(object string, epoch uint32, up []int32, acting ...int32) nativeRBDJournalPlacement {
		return nativeRBDJournalPlacement{Epoch: epoch, Pool: r.Pool, PoolID: r.PoolID, Object: object, RawPGID: "3.abcd", PGID: "3.0", Up: slices.Clone(up), UpPrimary: up[0],
			Acting: slices.Clone(acting), ActingPrimary: acting[0]}
	}
	native := &nativeRBDJournalPrimaryChange{Out: 1, Commands: []string{"osd out 1"}, Polls: 1, ElapsedMS: 900}
	pc := &rbdJournalPrimaryChange{Objects: objects, LockPrimary: 1, AppendPrimary: 1, AppendConnection: 1, ResubmitConnection: 2, Native: native,
		LockerAddress: "10.0.0.9:0/11", WatcherAddress: "10.0.0.9:0/11", VersionBefore: 7, VersionAfter: 7,
		Resubmission: &rbdJournalResubmission{CallerInvocations: 1, Class: "applied", Primary: 0, RetryAttempt: 1, Submission: cephmsgr.OSDSubmission{Transaction: 9, GlobalID: r.SubjectGlobalID, MapEpoch: 42},
			ReplyTransaction: 9, ReplyRetryAttempt: 1, ReplyMapEpoch: 42, OnDisk: true, UserVersion: 7, RecordedUTC: "2023-11-14T22:13:25Z"}}
	for _, object := range objects {
		before, after := place(object, 40, []int32{1, 0, 2}, 1, 0, 2), place(object, 42, []int32{0, 2}, 0, 2, 1)
		native.Objects = append(native.Objects, struct {
			Object string                    `json:"object"`
			Moved  bool                      `json:"moved"`
			Before nativeRBDJournalPlacement `json:"before"`
			After  nativeRBDJournalPlacement `json:"after"`
		}{object, true, before, after})
		pc.GoBefore = append(pc.GoBefore, rbdJournalPlacementView{object, 40, before.PGID, before.ActingPrimary, slices.Clone(before.Acting)})
		pc.GoAfter = append(pc.GoAfter, rbdJournalPlacementView{object, 42, after.PGID, after.ActingPrimary, slices.Clone(after.Acting)})
	}
	r.PrimaryChange = pc
	for _, name := range []string{"change-primary", "observe-resubmitted"} {
		v := *r.Views["observe-recorded"]
		v.Action, v.ObjectSizes, v.ObjectSHA256, v.ImageCommit = name, slices.Clone(v.ObjectSizes), slices.Clone(v.ObjectSHA256), slices.Clone(v.ImageCommit)
		prefix, entry, trailing := *v.PrefixSHA256, *v.EntrySHA256, *v.TrailingBytes
		v.PrefixSHA256, v.EntrySHA256, v.TrailingBytes = &prefix, &entry, &trailing
		r.Views[name] = &v
	}
	// One connection to the old primary, which carried the append, and one
	// to the new primary afterwards, as on the single-OSD fixture; the entry
	// leaves the client on both.
	r.Gate.LargeWrites = 2
	if mode == "crc" {
		r.Gate.MarkedRequests, r.Gate.MarkedConnections = n(2), []int{1, 2}
	}
	// A record judged at an earlier stage holds only what exists by then.
	at := slices.Index(rbdJournalPrimaryChangeStages, stage)
	r.Order = slices.Clone(rbdJournalPrimaryChangeOrder[:slices.Index(rbdJournalPrimaryChangeOrder, stage)+1])
	if at < 4 {
		delete(r.Views, "replay")
		r.Gate, r.DialsTotal = nil, nil
	}
	if at < 3 {
		delete(r.Views, "observe-resubmitted")
		pc.Resubmission, pc.VersionBefore, pc.VersionAfter, pc.LockerAddress, pc.WatcherAddress, pc.ResubmitConnection, r.DialsBeforeClose = nil, 0, 0, "", "", 0, nil
	}
	if at < 2 {
		delete(r.Views, "change-primary")
		pc.Native, pc.GoBefore, pc.GoAfter = nil, nil, nil
	}
	if at < 1 {
		delete(r.Views, "observe-recorded")
		r.Outcome = nil
	}
	return r
}

// TestRBDJournalPrimaryChangeClaims feeds the claims of the primary-change
// variant contradictory records. Each one keeps every other part of the
// record intact. The claims the variant shares with the single-OSD sequence
// are covered by TestRBDJournalLostAppendClaims.
func TestRBDJournalPrimaryChangeClaims(t *testing.T) {
	n := func(v int) *int { return &v }
	u := func(v uint64) *uint64 { return &v }
	type record = rbdJournalLostAppendRecord
	type change struct {
		stage string
		// partial says that the contradiction exists only in a record that
		// ended at its stage.
		partial bool
		apply   func(*record)
	}
	object := func(r *record, i int) *struct {
		Object string                    `json:"object"`
		Moved  bool                      `json:"moved"`
		Before nativeRBDJournalPlacement `json:"before"`
		After  nativeRBDJournalPlacement `json:"after"`
	} {
		return &r.PrimaryChange.Native.Objects[i]
	}
	// lead sets an up set and the acting set that goes with it.
	lead := func(p *nativeRBDJournalPlacement, up []int32, acting ...int32) {
		p.Up, p.Acting, p.UpPrimary, p.ActingPrimary = up, acting, up[0], acting[0]
	}
	both := map[string]change{
		// change-primary: the one primary was marked out and another member
		// leads every followed object.
		"primary did not move": {"change-primary", false, func(r *record) {
			o := object(r, 0)
			o.After, o.Moved = o.Before, false
			o.After.Epoch = 42
			r.PrimaryChange.GoAfter[0].Primary, r.PrimaryChange.GoAfter[0].Acting = 1, []int32{1, 0, 2}
		}},
		"journal object not reported moved": {"change-primary", false, func(r *record) { object(r, 1).Moved = false }},
		"old primary still in the up set": {"change-primary", false, func(r *record) {
			lead(&object(r, 1).After, []int32{0, 1, 2}, 0, 1, 2)
			r.PrimaryChange.GoAfter[1].Acting = []int32{0, 1, 2}
		}},
		"old primary still leads": {"change-primary", false, func(r *record) {
			lead(&object(r, 0).After, []int32{0, 2}, 1, 0, 2)
			r.PrimaryChange.GoAfter[0].Primary, r.PrimaryChange.GoAfter[0].Acting = 1, []int32{1, 0, 2}
		}},
		"old primary left the acting set": {"change-primary", false, func(r *record) {
			lead(&object(r, 0).After, []int32{0, 2}, 0, 2)
			r.PrimaryChange.GoAfter[0].Acting = []int32{0, 2}
		}},
		"map epoch did not advance": {"change-primary", false, func(r *record) {
			object(r, 1).After.Epoch = 40
			r.PrimaryChange.GoAfter[1].Epoch = 40
		}},
		"up set shrank to one": {"change-primary", false, func(r *record) {
			lead(&object(r, 0).After, []int32{0}, 0, 1)
			r.PrimaryChange.GoAfter[0].Acting = []int32{0, 1}
		}},
		"acting set has another OSD": {"change-primary", false, func(r *record) {
			lead(&object(r, 1).After, []int32{0, 3}, 0, 3, 1)
			r.PrimaryChange.GoAfter[1].Acting = []int32{0, 3, 1}
		}},
		"object in another PG afterwards": {"change-primary", false, func(r *record) {
			object(r, 0).After.PGID = "3.7"
			r.PrimaryChange.GoAfter[0].PGID = "3.7"
		}},
		"acting primary is not the up primary": {"change-primary", false, func(r *record) {
			lead(&object(r, 1).After, []int32{0, 2}, 2, 0, 1)
			r.PrimaryChange.GoAfter[1].Primary, r.PrimaryChange.GoAfter[1].Acting = 2, []int32{2, 0, 1}
		}},
		"placement starts degraded": {"change-primary", false, func(r *record) {
			lead(&object(r, 0).Before, []int32{1, 0}, 1, 0)
			r.PrimaryChange.GoBefore[0].Acting = []int32{1, 0}
		}},
		"placement starts on a pg_temp": {"change-primary", false, func(r *record) { object(r, 0).Before.Up = []int32{1, 0} }},
		"journal header led by another OSD": {"change-primary", false, func(r *record) {
			lead(&object(r, 2).Before, []int32{2, 0, 1}, 2, 0, 1)
			r.PrimaryChange.GoBefore[2].Primary, r.PrimaryChange.GoBefore[2].Acting = 2, []int32{2, 0, 1}
		}},
		"placement of another pool":        {"change-primary", false, func(r *record) { object(r, 0).Before.Pool = "other" }},
		"placement of another object":      {"change-primary", false, func(r *record) { object(r, 1).After.Object = "journal_data.3.ab.1" }},
		"receipt names another object":     {"change-primary", false, func(r *record) { object(r, 1).Object = "journal_data.3.ab.1" }},
		"journal header placement missing": {"change-primary", false, func(r *record) { r.PrimaryChange.Native.Objects = r.PrimaryChange.Native.Objects[:2] }},
		"no OSD marked out":                {"change-primary", false, func(r *record) { r.PrimaryChange.Native.Out, r.PrimaryChange.Native.Commands = -1, nil }},
		"another OSD marked out": {"change-primary", false, func(r *record) {
			r.PrimaryChange.Native.Out, r.PrimaryChange.Native.Commands = 2, []string{"osd out 2"}
		}},
		"command for another OSD":           {"change-primary", false, func(r *record) { r.PrimaryChange.Native.Commands = []string{"osd out 2"} }},
		"two OSDs marked out":               {"change-primary", false, func(r *record) { r.PrimaryChange.Native.Commands = []string{"osd out 1", "osd out 2"} }},
		"lock was taken on another OSD":     {"change-primary", false, func(r *record) { r.PrimaryChange.LockPrimary = 2 }},
		"append was sent to another OSD":    {"change-primary", false, func(r *record) { r.PrimaryChange.AppendPrimary = 2 }},
		"fixture receipt missing":           {"change-primary", false, func(r *record) { r.PrimaryChange.Native = nil }},
		"followed objects differ":           {"change-primary", false, func(r *record) { r.PrimaryChange.Objects[1] = "journal_data.3.ab.1" }},
		"test placement missing":            {"change-primary", false, func(r *record) { r.PrimaryChange.GoAfter = r.PrimaryChange.GoAfter[:2] }},
		"test saw another primary before":   {"change-primary", false, func(r *record) { r.PrimaryChange.GoBefore[1].Primary = 2 }},
		"test saw another PG before":        {"change-primary", false, func(r *record) { r.PrimaryChange.GoBefore[0].PGID = "3.9" }},
		"test placement before is later":    {"change-primary", false, func(r *record) { r.PrimaryChange.GoBefore[0].Epoch = 41 }},
		"test saw another acting set after": {"change-primary", false, func(r *record) { r.PrimaryChange.GoAfter[1].Acting = []int32{0, 2} }},
		"test placement before has no map":  {"change-primary", false, func(r *record) { r.PrimaryChange.GoBefore[2].Epoch = 0 }},
		"test kept the old primary":         {"change-primary", false, func(r *record) { r.PrimaryChange.GoAfter[1].Primary = 1 }},
		"test placement after is stale":     {"change-primary", false, func(r *record) { r.PrimaryChange.GoAfter[0].Epoch = 41 }},
		"test placement of another object":  {"change-primary", false, func(r *record) { r.PrimaryChange.GoAfter[2].Object = "journal.cd" }},
		"journal changed across the move":   {"change-primary", false, func(r *record) { r.Views["change-primary"].ObjectSHA256[2] = strings.Repeat("f", 64) }},
		"entry twice after the move": {"change-primary", false, func(r *record) {
			r.Views["change-primary"].TrailingBytes = u(4173)
			r.Views["change-primary"].ObjectSizes[0] += 4173
		}},
		"entry gone after the move":          {"change-primary", false, func(r *record) { r.Views["change-primary"].EntrySHA256 = nil }},
		"lock lost across the move":          {"change-primary", false, func(r *record) { r.Views["change-primary"].LockFree = true }},
		"lock has another owner after":       {"change-primary", false, func(r *record) { r.Views["change-primary"].LockOwner = "client.4300" }},
		"watch not listed after the move":    {"change-primary", false, func(r *record) { r.Views["change-primary"].SubjectWatcher = false }},
		"commit moved across the move":       {"change-primary", false, func(r *record) { r.Views["change-primary"].ImageCommit = r.ExpectedCommitAfter }},
		"image changed across the move":      {"change-primary", false, func(r *record) { r.Views["change-primary"].ImageHeadSHA256 = r.ExpectedHeadAfter }},
		"observation after the move missing": {"change-primary", false, func(r *record) { delete(r.Views, "change-primary") }},
		"observation is of another action":   {"change-primary", false, func(r *record) { r.Views["change-primary"].Action = "observe-recorded" }},
		"moved before the outcome was known": {"change-primary", false, func(r *record) { r.Outcome = nil }},
		"resubmitted before the move was judged": {"change-primary", true, func(r *record) {
			r.PrimaryChange.Resubmission = &rbdJournalResubmission{CallerInvocations: 1, Class: "applied"}
		}},
		"move before the outcome record": {"change-primary", false, func(r *record) {
			i, j := slices.Index(r.Order, "outcome_recorded"), slices.Index(r.Order, "change-primary")
			r.Order[i], r.Order[j] = r.Order[j], r.Order[i]
		}},

		// observe-resubmitted: one known answer to the original transaction
		// from the new primary, and nothing appended.
		"resubmission missing": {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.Resubmission = nil }},
		"resubmission outcome unknown": {"observe-resubmitted", false, func(r *record) {
			x := r.PrimaryChange.Resubmission
			x.Class, x.UnknownOutcome, x.Error, x.ReplyTransaction, x.UserVersion, x.OnDisk = "uncertain", true, "unknown", 0, 0, false
		}},
		"resubmission refused": {"observe-resubmitted", false, func(r *record) {
			r.PrimaryChange.Resubmission.Class, r.PrimaryChange.Resubmission.ReplyCode, r.PrimaryChange.Resubmission.Error = "refused", -2, "refused"
		}},
		"sent as a new request": {"observe-resubmitted", false, func(r *record) {
			x := r.PrimaryChange.Resubmission
			x.Submission.Transaction, x.ReplyTransaction, x.RetryAttempt, x.ReplyRetryAttempt = 10, 10, 0, 0
		}},
		"new transaction with a retry attempt": {"observe-resubmitted", false, func(r *record) {
			r.PrimaryChange.Resubmission.Submission.Transaction, r.PrimaryChange.Resubmission.ReplyTransaction = 10, 10
		}},
		"retry attempt not raised": {"observe-resubmitted", false, func(r *record) {
			r.PrimaryChange.Resubmission.RetryAttempt, r.PrimaryChange.Resubmission.ReplyRetryAttempt = 0, 0
		}},
		"second resubmission": {"observe-resubmitted", false, func(r *record) {
			r.PrimaryChange.Resubmission.RetryAttempt, r.PrimaryChange.Resubmission.ReplyRetryAttempt = 2, 2
		}},
		"answer echoes another attempt":   {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.Resubmission.ReplyRetryAttempt = 0 }},
		"answer of another transaction":   {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.Resubmission.ReplyTransaction = 8 }},
		"answer without the durable flag": {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.Resubmission.OnDisk = false }},
		"resubmitted to the old primary":  {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.Resubmission.Primary = 1 }},
		"resubmitted to the other member": {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.Resubmission.Primary = 2 }},
		"answered under an older map":     {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.Resubmission.ReplyMapEpoch = 41 }},
		"caller resubmitted twice":        {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.Resubmission.CallerInvocations = 2 }},
		"resubmission of another client":  {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.Resubmission.Submission.GlobalID = r.ObserverGlobalID }},
		"original submission unknown": {"observe-resubmitted", false, func(r *record) {
			r.Outcome.Submission.Transaction, r.PrimaryChange.Resubmission.ReplyTransaction, r.PrimaryChange.Resubmission.Submission.Transaction = 0, 0, 0
		}},
		"answer carries a newer version": {"observe-resubmitted", false, func(r *record) {
			r.PrimaryChange.Resubmission.UserVersion, r.PrimaryChange.VersionAfter = 8, 8
		}},
		"object version raised afterwards":  {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.VersionAfter = 8 }},
		"object version not read before":    {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.VersionBefore = 0 }},
		"answer without a version":          {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.Resubmission.UserVersion = 0 }},
		"recorded before the outcome":       {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.Resubmission.RecordedUTC = "2023-11-14T22:13:20Z" }},
		"resubmission time missing":         {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.Resubmission.RecordedUTC = "" }},
		"lock record address not read":      {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.LockerAddress = "" }},
		"reattached watch address not read": {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.WatcherAddress = "" }},
		"watch listed at another address":   {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.WatcherAddress = "10.0.0.9:0/22" }},
		"neither address read":              {"observe-resubmitted", false, func(r *record) { r.PrimaryChange.LockerAddress, r.PrimaryChange.WatcherAddress = "", "" }},
		"journal grew by the entry again": {"observe-resubmitted", false, func(r *record) {
			v := r.Views["observe-resubmitted"]
			v.ObjectSizes[0], v.ObjectSHA256[0], v.TrailingBytes = v.ObjectSizes[0]+4173, strings.Repeat("d", 64), u(4173)
		}},
		"bytes follow the entry":            {"observe-resubmitted", false, func(r *record) { r.Views["observe-resubmitted"].TrailingBytes = u(1) }},
		"another journal object grew":       {"observe-resubmitted", false, func(r *record) { r.Views["observe-resubmitted"].ObjectSizes[1] += 4173 }},
		"another journal object rewritten":  {"observe-resubmitted", false, func(r *record) { r.Views["observe-resubmitted"].ObjectSHA256[3] = strings.Repeat("f", 64) }},
		"lock released before the answer":   {"observe-resubmitted", false, func(r *record) { r.Views["observe-resubmitted"].LockFree = true }},
		"watch gone after the resubmission": {"observe-resubmitted", false, func(r *record) { r.Views["observe-resubmitted"].SubjectWatcher = false }},
		"commit moved by the resubmission":  {"observe-resubmitted", false, func(r *record) { r.Views["observe-resubmitted"].ImageCommit = r.ExpectedCommitAfter }},
		"new tag after the resubmission":    {"observe-resubmitted", false, func(r *record) { r.Views["observe-resubmitted"].NextTag = 4 }},
		"observation after it missing":      {"observe-resubmitted", false, func(r *record) { delete(r.Views, "observe-resubmitted") }},
		"released before the resubmission": {"observe-resubmitted", false, func(r *record) {
			i, j := slices.Index(r.Order, "resubmitted"), slices.Index(r.Order, "watch_reconnected")
			r.Order[i], r.Order[j] = r.Order[j], r.Order[i]
		}},

		// replay: the entry left the client twice, once per connection.
		"entry written a third time":             {"replay", false, func(r *record) { r.Gate.LargeWrites = 3 }},
		"entry written once only":                {"replay", false, func(r *record) { r.Gate.LargeWrites = 1 }},
		"resubmitted on the silent connection":   {"replay", false, func(r *record) { r.PrimaryChange.ResubmitConnection = 1 }},
		"resubmission connection unknown":        {"replay", false, func(r *record) { r.PrimaryChange.ResubmitConnection = 0 }},
		"resubmission on an undialed connection": {"replay", false, func(r *record) { r.PrimaryChange.ResubmitConnection = 3 }},
		"append connection unknown":              {"replay", false, func(r *record) { r.PrimaryChange.AppendConnection = 0 }},
		"append on the later connection":         {"replay", false, func(r *record) { r.PrimaryChange.AppendConnection = 2 }},
		"gate bound on the later connection":     {"replay", false, func(r *record) { r.Gate.BoundConnection = 2 }},
		"a connection the test did not dial":     {"replay", false, func(r *record) { r.Gate.Connections = 3 }},
		"dials before the close not counted":     {"replay", false, func(r *record) { r.DialsBeforeClose = nil }},
		"replay appended the entry again": {"replay", false, func(r *record) {
			r.Views["replay"].TrailingBytes = u(4173)
			r.Views["replay"].ObjectSizes[0] += 4173
		}},
		"replay saw the entry twice": {"replay", false, func(r *record) {
			v := r.Views["replay"]
			v.InspectOwned = append(v.InspectOwned, v.InspectOwned[0])
			v.InspectTotal = n(7)
		}},
		"entry not applied by the replay": {"replay", false, func(r *record) { r.Views["replay"].ImageHeadSHA256 = r.ExpectedHeadBefore }},
	}
	decoded := map[string]change{
		"entry frame seen once":            {"replay", false, func(r *record) { r.Gate.MarkedRequests, r.Gate.MarkedConnections = n(1), []int{1} }},
		"entry frame seen three times":     {"replay", false, func(r *record) { r.Gate.MarkedRequests, r.Gate.MarkedConnections = n(3), []int{1, 2, 2} }},
		"both frames on one connection":    {"replay", false, func(r *record) { r.Gate.MarkedConnections = []int{1, 1} }},
		"reply held on the new connection": {"replay", false, func(r *record) { r.Gate.Held.Connection = 2 }},
		"held reply of another request":    {"replay", false, func(r *record) { r.Gate.Held.Transaction = 10 }},
	}
	undecoded := map[string]change{
		"secure record names a frame":    {"replay", false, func(r *record) { r.Gate.MarkedRequests, r.Gate.MarkedConnections = n(2), []int{1, 2} }},
		"nothing withheld before cancel": {"replay", false, func(r *record) { *r.WithheldBeforeCancel = false }},
	}
	total := 0
	for _, mode := range []string{"crc", "secure"} {
		for i, stage := range rbdJournalPrimaryChangeStages {
			if problems := rbdJournalLostAppendClaims(rbdJournalPrimaryChangeSample(mode, stage), stage); len(problems) != 0 {
				t.Fatal("consistent record refused", mode, stage, problems)
			}
			for j, other := range rbdJournalPrimaryChangeStages {
				if i != j && len(rbdJournalLostAppendClaims(rbdJournalPrimaryChangeSample(mode, stage), other)) == 0 {
					t.Fatal("record of stage", stage, "accepted at", other, mode)
				}
			}
		}
		// The two variants do not pass for each other.
		r := rbdJournalPrimaryChangeSample(mode, "replay")
		r.PrimaryChange = nil
		if len(rbdJournalLostAppendClaims(r, "replay")) == 0 {
			t.Fatal("primary-change run without its record accepted", mode)
		}
		r = rbdJournalLostAppendSample(mode)
		r.PrimaryChange = rbdJournalPrimaryChangeSample(mode, "replay").PrimaryChange
		if len(rbdJournalLostAppendClaims(r, "replay")) == 0 {
			t.Fatal("single-OSD run with a primary-change record accepted", mode)
		}
		if len(rbdJournalLostAppendClaims(rbdJournalLostAppendSample(mode), "change-primary")) == 0 {
			t.Fatal("single-OSD record accepted at a primary-change stage", mode)
		}
		cases := map[string]change{}
		for name, c := range both {
			cases[name] = c
		}
		extra := decoded
		if mode != "crc" {
			extra = undecoded
		}
		for name, c := range extra {
			cases[name] = c
		}
		for name, c := range cases {
			r := rbdJournalPrimaryChangeSample(mode, c.stage)
			c.apply(r)
			if len(rbdJournalLostAppendClaims(r, c.stage)) == 0 {
				t.Error("contradictory record accepted:", mode, name)
			}
			// The same contradiction is refused in a complete record.
			if c.stage != "replay" && !c.partial {
				r = rbdJournalPrimaryChangeSample(mode, "replay")
				c.apply(r)
				if len(rbdJournalLostAppendClaims(r, "replay")) == 0 {
					t.Error("contradictory record accepted in a complete run:", mode, name)
				}
			}
		}
		total += len(cases)
		t.Logf("%s: %d contradictory records refused", mode, len(cases))
	}
	if total == 0 {
		t.Fatal("no contradictory record was judged")
	}
}
