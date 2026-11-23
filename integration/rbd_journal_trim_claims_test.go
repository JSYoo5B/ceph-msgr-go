package integration_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// rbdJournalTrimSample is a consistent record of one trimming run with
// synthetic values: tag 2 for the subject, tag 1 for the setup, 3 for the
// takeover.
func rbdJournalTrimSample() *rbdJournalLostAppendRecord {
	n := func(v int) *int { return &v }
	hash := func(c string) string { return strings.Repeat(c, 64) }
	const subject, cookie, tag = 4321, 77, 2
	token := strings.Repeat("a1", 16)
	oracle := "ceph-msgr-oracle-a1a1a1a1"
	prefix := "journal_data.3.ab."
	block := []rbdLockOwnerLossBlock{{rbdJournalOwnerLossSampleAddress, "2023-11-14T23:13:20Z"}}
	setupCommit := []rbdJournalPosition{{3, 1, 3}, {2, 1, 2}, {1, 1, 1}, {0, 1, 0}}
	writes := []nativeRBDExtent{{4 << 10, 3 << 10}, {12 << 10, 3 << 10}, {20 << 10, 3 << 10}, {28 << 10, 3 << 10}, {80 << 10, 3 << 10}, {88 << 10, 3 << 10}, {96 << 10, 3 << 10},
		{104 << 10, 3 << 10}}
	setupSizes := []uint64{2110, 2110, 1086, 93, 0, 0, 0, 0}
	x := &rbdJournalTrim{SoftMax: 4096, Reads: map[string][]rbdJournalTrimRead{}, Notifies: []string{"journal_notify_active_set", "journal_notify_commit"}, Applied: []uint64{0, 1, 2, 3, 4, 5, 6},
		ExpectedHeadCommitted: hash("c"), ExpectedHeadTakeover: hash("d"), DisconnectOutput: []string{"client " + oracle + " disconnected"},
		TrimWaitPolls: 3, TrimWaitNS: 400e6, TrimWaitMinimum: 1, TrimWaitMoved: true,
		TrimLog:        append([]string{"handle_metadata_updated", "trim_objects: min_set=1"}, append(rbdJournalTrimMarks(prefix, 4)[1:], "handle_metadata_updated")...),
		TakeoverEvents: []rbdJournalLostAppendEvent{{Type: "AioWrite", Offset: 104 << 10, Length: 1 << 10}}}
	sizes := slices.Clone(setupSizes)
	submit := uint64(20)
	attempt := func(label string, code int32) rbdLockOwnerLossAttempt {
		submit++
		a := rbdLockOwnerLossAttempt{Label: label, Invocations: 1, Class: "applied", Submission: cephmsgr.OSDSubmission{Transaction: submit, GlobalID: subject}, ReplyTransaction: submit}
		if code != 0 {
			a.Class, a.ErrorCode, a.ReplyTransaction, a.Error = "refused", code, 0, "ceph: OSD result code -75"
		}
		return a
	}
	for i, w := range writes {
		object := uint64(i)
		if i == 4 {
			x.Appends = append(x.Appends, rbdJournalTrimAppend{Entry: 4, Object: 0, Offset: sizes[0], Outcome: attempt("journal_append_4_object_0", rbdJournalOverflow)})
			a := attempt("journal_set_active_set_1", 0)
			x.ActiveSet = &a
		}
		x.Appends = append(x.Appends, rbdJournalTrimAppend{Entry: uint64(i), Object: object, Offset: sizes[object], Outcome: attempt(fmt.Sprintf("journal_append_%d_object_%d", i, object), 0)})
		x.Entries = append(x.Entries, rbdJournalLostAppendEntry{Tag: tag, EntryTID: uint64(i), Object: object, ObjectName: prefix + fmt.Sprint(object), Offset: int(sizes[object]),
			Length: 3149, SHA256: hash(fmt.Sprint(i)), ImageOffset: w.Offset, ImageLength: w.Length, Timestamp: cephmsgr.OSDTime{Seconds: 1700000000 + uint32(i), Nanoseconds: 5000}})
		sizes[object] += 3149
	}
	commit := attempt("client_commit_image", 0)
	x.Commit, x.CommitPositions = &commit, []rbdJournalPosition{{6, tag, 6}, {5, tag, 5}, {4, tag, 4}, {3, tag, 3}}
	x.SizesAppended = sizes
	for i := range sizes {
		x.SHA256Appended = append(x.SHA256Appended, hash(string(rune('a'+i))))
	}
	x.ExpectedCommitTakeover = []rbdJournalPosition{{4, tag + 1, 0}, {7, tag, 7}, {6, tag, 6}, {5, tag, 5}}
	x.Object4Tail = []rbdJournalEntry{{Object: 4, ObjectOffset: 0, Tag: tag + 1, EntryID: 0, Length: 1100}}
	x.Object4TailEvents = slices.Clone(x.TakeoverEvents)
	for _, stage := range []string{"observe-committed", "takeover"} {
		for _, h := range rbdJournalTrimHeaderReads(oracle) {
			x.Reads[stage] = append(x.Reads[stage], rbdJournalTrimRead{Label: stage + "_" + h.Method, Object: "journal.ab", Method: h.Method, InputHex: h.InputHex, Number: -1,
				NativeExists: true, Equal: true})
		}
		for n := range 8 {
			rd := rbdJournalTrimRead{Label: fmt.Sprintf("%s_read_journal_object_%d", stage, n), Object: prefix + fmt.Sprint(n), Number: n, NativeExists: true, Equal: true}
			if stage == "takeover" && n < 4 {
				rd.NativeExists, rd.Code = false, -2
			}
			x.Reads[stage] = append(x.Reads[stage], rd)
		}
	}
	client := func(id, state string, positions []rbdJournalPosition) rbdJournalClientView {
		return rbdJournalClientView{ID: id, State: state, Positions: slices.Clone(positions)}
	}
	view := func(action string, objectSizes []uint64, objectSHA256 []string, exists []bool, minimum, active uint64, image []rbdJournalPosition, state, head string) *rbdJournalLostAppendView {
		return &rbdJournalLostAppendView{Action: action, ObjectSizes: slices.Clone(objectSizes), ObjectSHA256: slices.Clone(objectSHA256), ObjectExists: slices.Clone(exists),
			ImageHeadSHA256: head, ImageCommit: slices.Clone(image), NextTag: tag + 1, LastTag: rbdJournalLostAppendTag{TID: tag}, MinimumSet: minimum, ActiveSet: active,
			Clients: []rbdJournalClientView{client("", "connected", image), client(oracle, state, nil)}, ObservedUTC: "2023-11-14T22:13:21Z"}
	}
	held := func(v *rbdJournalLostAppendView) *rbdJournalLostAppendView {
		v.LockOwner, v.LockCookie, v.SubjectWatcher, v.Watchers = fmt.Sprintf("client.%d", subject), "auto 77", true, 1
		return v
	}
	free := func(v *rbdJournalLostAppendView) *rbdJournalLostAppendView {
		v.LockFree, v.Blocklist = true, slices.Clone(block)
		return v
	}
	setupSHA := []string{hash("0"), hash("1"), hash("2"), hash("3"), hash("4"), hash("4"), hash("4"), hash("4")}
	set0 := []bool{true, true, true, true, false, false, false, false}
	all := []bool{true, true, true, true, true, true, true, true}
	setup := view("setup", setupSizes, setupSHA, set0, 0, 0, setupCommit, "connected", hash("a"))
	setup.LockFree, setup.NextTag, setup.LastTag = true, tag, rbdJournalLostAppendTag{TID: 1}
	setup.InspectTotal, setup.InspectErrors = n(4), n(0)
	locked := held(view("observe-locked", setupSizes, setupSHA, set0, 0, 0, setupCommit, "connected", hash("a")))
	committed := held(view("observe-committed", sizes, x.SHA256Appended, all, 0, 1, x.CommitPositions, "connected", hash("c")))
	disconnected := held(view("disconnect-oracle", sizes, x.SHA256Appended, all, 0, 1, x.CommitPositions, "disconnected", hash("c")))
	disconnected.InspectTotal, disconnected.InspectErrors = n(1), n(0)
	disconnected.InspectOwned = []rbdJournalLostAppendEvent{rbdJournalEntryEvent(x.Entries[7])}
	broke := free(view("force-break", sizes, x.SHA256Appended, all, 0, 1, x.CommitPositions, "disconnected", hash("c")))
	takenSizes, takenSHA := slices.Clone(sizes), slices.Clone(x.SHA256Appended)
	for i := range 4 {
		takenSizes[i], takenSHA[i] = 0, hash("4")
	}
	takenSizes[4], takenSHA[4] = sizes[4]+1100, hash("t")
	takeover := free(view("takeover", takenSizes, takenSHA, []bool{false, false, false, false, true, true, true, true}, 1, 1, x.ExpectedCommitTakeover, "disconnected", hash("d")))
	takeover.NextTag, takeover.LastTag = tag+2, rbdJournalLostAppendTag{tag + 1, true, tag, 7}
	takeover.InspectTotal, takeover.InspectErrors, takeover.InspectOwned = n(0), n(0), []rbdJournalLostAppendEvent{}
	r := &rbdJournalLostAppendRecord{Token: token, JournalHeader: "journal.ab", SubjectGlobalID: subject, ObserverGlobalID: 4300, ConnectionMode: "crc", WatchCookie: cookie, LockCookie: "auto 77",
		Entry: x.Entries[0], Order: slices.Clone(rbdJournalTrimOrder), ExpectedHeadBefore: hash("a"), Trim: x,
		Views: map[string]*rbdJournalLostAppendView{"setup": setup, "observe-locked": locked, "observe-committed": committed, "disconnect-oracle": disconnected,
			"force-break": broke, "takeover": takeover},
		Requests: []rbdWriteRequestEvidence{{Label: "journal_set_active_set_1", Inputs: []rbdOperationInputEvidence{{Code: uint16(cephmsgr.OSDCall), Class: "journal", Method: "set_active_set"}}}}}
	r.OwnerLoss = &rbdJournalOwnerLoss{Forced: true, LockerAddress: rbdJournalOwnerLossSampleAddress,
		Break: &rbdJournalBreak{Owners: []string{rbdJournalOwnerLossSampleAddress}, OwnersAfter: []string{}, BreakNS: 3000, LockerBefore: "client.4321", LockerCookieBefore: r.LockCookie,
			WatchersBefore:      []rbdJournalWatch{{subject, cookie, rbdJournalOwnerLossSampleAddress}},
			LockerAddressBefore: rbdJournalOwnerLossSampleAddress, BlocklistBefore: []rbdLockOwnerLossBlock{}, Blocklist: slices.Clone(block),
			LockPath: rbdJournalLockPathSample(rbdJournalForcedBreakLockPath)},
		PingBefore: &rbdLockOwnerLossAttempt{Label: "watch_ping", Invocations: 1, Class: "applied"},
		Takeover: &rbdJournalTakeover{Owner: true, WriteNS: 9000, Events: slices.Clone(x.TakeoverEvents), BlocklistBefore: slices.Clone(block), Blocklist: slices.Clone(block),
			NativeGlobalID: rbdJournalTakeoverSampleGID, BoundaryLockers: []rbdJournalLockRecord{},
			BoundaryWatchers: []rbdJournalWatch{{rbdJournalTakeoverSampleGID, rbdJournalTakeoverSampleHandle, "10.0.0.5:0/5"}}, LockPath: rbdJournalLockPathSample(rbdJournalFreeLockPath)}}
	return r
}

// TestRBDJournalTrimClaims feeds the trimming claims contradictory records.
func TestRBDJournalTrimClaims(t *testing.T) {
	if problems := rbdJournalLostAppendClaims(rbdJournalTrimSample(), "takeover"); len(problems) != 0 {
		t.Fatal("consistent trimming record refused", problems)
	}
	for i, stage := range rbdJournalTrimStages[:3] {
		r := rbdJournalTrimSample()
		if len(rbdJournalLostAppendClaims(r, stage)) == 0 {
			t.Fatal("steps beyond the stage accepted", stage)
		}
		r.Order = r.Order[:slices.Index(r.Order, stage)+1]
		for _, later := range rbdJournalTrimStages[i+1:] {
			delete(r.Views, later)
		}
		r.OwnerLoss.Takeover = nil
		if i < 2 {
			r.OwnerLoss.Break, r.OwnerLoss.PingBefore = nil, nil
		}
		if problems := rbdJournalLostAppendClaims(r, stage); len(problems) != 0 {
			t.Fatal("consistent partial trimming record refused", stage, problems)
		}
	}
	if len(rbdJournalLostAppendClaims(rbdJournalTrimSample(), "replay")) == 0 {
		t.Fatal("unknown stage accepted")
	}
	type change = func(*rbdJournalLostAppendRecord)
	for name, mutate := range map[string]change{
		// The setup and the appends.
		"setup object already full": func(r *rbdJournalLostAppendRecord) { r.Views["setup"].ObjectSizes[2] = 4096 },
		"setup entry in set 1":      func(r *rbdJournalLostAppendRecord) { r.Views["setup"].ObjectExists[4] = true },
		"no overflow": func(r *rbdJournalLostAppendRecord) {
			r.Trim.Appends = slices.Delete(r.Trim.Appends, 4, 5)
		},
		"overflow resent to the full object": func(r *rbdJournalLostAppendRecord) {
			r.Trim.Appends = slices.Insert(r.Trim.Appends, 5, r.Trim.Appends[4])
		},
		"overflow unknown": func(r *rbdJournalLostAppendRecord) {
			a := &r.Trim.Appends[4].Outcome
			a.Class, a.UnknownOutcome, a.ErrorCode = "uncertain", true, 0
		},
		"overflow of a writable object": func(r *rbdJournalLostAppendRecord) { r.Trim.Appends[4].Offset = 4000 },
		"refused for another reason":    func(r *rbdJournalLostAppendRecord) { r.Trim.Appends[4].Outcome.ErrorCode = -22 },
		"append sent twice":             func(r *rbdJournalLostAppendRecord) { r.Trim.Appends[6].Outcome.Invocations = 2 },
		"append past the soft limit":    func(r *rbdJournalLostAppendRecord) { r.Trim.Appends[0].Offset = 4096 },
		"active set not raised":         func(r *rbdJournalLostAppendRecord) { r.Trim.ActiveSet = nil },
		"header not notified":           func(r *rbdJournalLostAppendRecord) { r.Trim.Notifies = r.Trim.Notifies[:1] },
		"entry 7 applied":               func(r *rbdJournalLostAppendRecord) { r.Trim.Applied = append(r.Trim.Applied, 7) },
		"entry 7 applied in place of 6": func(r *rbdJournalLostAppendRecord) { r.Trim.Applied[6] = 7 },
		"entry 2 applied twice":         func(r *rbdJournalLostAppendRecord) { r.Trim.Applied[3] = 2 },
		"commit after entry 7": func(r *rbdJournalLostAppendRecord) {
			r.Trim.CommitPositions = []rbdJournalPosition{{7, 2, 7}, {6, 2, 6}, {5, 2, 5}, {4, 2, 4}}
		},
		"commit refused": func(r *rbdJournalLostAppendRecord) { r.Trim.Commit.Class = "refused" },
		// What native clients saw before the trim.
		"journal bytes differ": func(r *rbdJournalLostAppendRecord) {
			r.Views["observe-committed"].ObjectSHA256[5] = strings.Repeat("z", 64)
		},
		"active set not seen":       func(r *rbdJournalLostAppendRecord) { r.Views["observe-committed"].ActiveSet = 0 },
		"trimmed before disconnect": func(r *rbdJournalLostAppendRecord) { r.Views["observe-committed"].MinimumSet = 1 },
		"oracle moved": func(r *rbdJournalLostAppendRecord) {
			r.Views["observe-committed"].Clients[1].Positions = []rbdJournalPosition{{0, 1, 0}}
		},
		"third client registered": func(r *rbdJournalLostAppendRecord) {
			v := r.Views["observe-committed"]
			v.Clients = append(v.Clients, v.Clients[1])
		},
		"image not applied": func(r *rbdJournalLostAppendRecord) {
			r.Views["observe-committed"].ImageHeadSHA256 = r.ExpectedHeadBefore
		},
		"go header read differs": func(r *rbdJournalLostAppendRecord) { r.Trim.Reads["observe-committed"][2].Equal = false },
		"go object read missing": func(r *rbdJournalLostAppendRecord) {
			r.Trim.Reads["observe-committed"] = r.Trim.Reads["observe-committed"][:12]
		},
		// The reads are judged by identity, not by count.
		"every read a copy of the first": func(r *rbdJournalLostAppendRecord) {
			for _, stage := range []string{"observe-committed", "takeover"} {
				reads := r.Trim.Reads[stage]
				for i := range reads {
					reads[i] = reads[0]
				}
			}
		},
		"header read duplicated in place of another": func(r *rbdJournalLostAppendRecord) {
			reads := r.Trim.Reads["takeover"]
			reads[1] = reads[0]
		},
		"object read duplicated in place of another": func(r *rbdJournalLostAppendRecord) {
			reads := r.Trim.Reads["takeover"]
			reads[5+7] = reads[5+6]
		},
		"oracle client read missing": func(r *rbdJournalLostAppendRecord) {
			r.Trim.Reads["observe-committed"] = slices.Delete(r.Trim.Reads["observe-committed"], 4, 5)
		},
		"header read of another input": func(r *rbdJournalLostAppendRecord) { r.Trim.Reads["observe-committed"][4].InputHex = "00" },
		"header read refused":          func(r *rbdJournalLostAppendRecord) { r.Trim.Reads["observe-committed"][0].Code = -2 },
		"header read on another object": func(r *rbdJournalLostAppendRecord) {
			r.Trim.Reads["observe-committed"][0].Object = "journal_data.3.ab.0"
		},
		"trimmed object answered": func(r *rbdJournalLostAppendRecord) {
			rd := &r.Trim.Reads["takeover"][5+1]
			rd.Code = 0
		},
		"trimmed object found natively": func(r *rbdJournalLostAppendRecord) { r.Trim.Reads["takeover"][5+2].NativeExists = true },
		"kept object answered ENOENT":   func(r *rbdJournalLostAppendRecord) { r.Trim.Reads["takeover"][5+5].Code = -2 },
		"object absent before the trim": func(r *rbdJournalLostAppendRecord) {
			rd := &r.Trim.Reads["observe-committed"][5+0]
			rd.NativeExists, rd.Code = false, -2
		},
		"object read under another name": func(r *rbdJournalLostAppendRecord) {
			r.Trim.Reads["observe-committed"][5+3].Object = "journal_data.3.ab.4"
		},
		"extra object read": func(r *rbdJournalLostAppendRecord) {
			rd := r.Trim.Reads["takeover"][5+4]
			rd.Number = 8
			r.Trim.Reads["takeover"] = append(r.Trim.Reads["takeover"], rd)
		},
		"oracle still connected":       func(r *rbdJournalLostAppendRecord) { r.Views["disconnect-oracle"].Clients[1].State = "connected" },
		"disconnect named nothing":     func(r *rbdJournalLostAppendRecord) { r.Trim.DisconnectOutput = nil },
		"disconnect failed":            func(r *rbdJournalLostAppendRecord) { r.Trim.DisconnectCode = 2 },
		"trimmed without a journal":    func(r *rbdJournalLostAppendRecord) { r.Views["disconnect-oracle"].ObjectExists[0] = false },
		"lock lost before the break":   func(r *rbdJournalLostAppendRecord) { r.Views["disconnect-oracle"].LockFree = true },
		"break without the live owner": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Break.WatchersBefore = nil },
		"no ping before the break":     func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.PingBefore = nil },
		"break observation unblocked":  func(r *rbdJournalLostAppendRecord) { r.Views["force-break"].Blocklist = []rbdLockOwnerLossBlock{} },
		"trimmed by the break":         func(r *rbdJournalLostAppendRecord) { r.Views["force-break"].MinimumSet = 1 },
		// The takeover and its trim.
		"minimum set not raised": func(r *rbdJournalLostAppendRecord) { r.Views["takeover"].MinimumSet = 0 },
		"trim wait timed out":    func(r *rbdJournalLostAppendRecord) { r.Trim.TrimWaitMoved, r.Trim.TrimWaitMinimum = false, 0 },
		"object 2 not removed":   func(r *rbdJournalLostAppendRecord) { r.Views["takeover"].ObjectExists[2] = true },
		"set 1 removed":          func(r *rbdJournalLostAppendRecord) { r.Views["takeover"].ObjectExists[6] = false },
		"object 5 changed":       func(r *rbdJournalLostAppendRecord) { r.Views["takeover"].ObjectSHA256[5] = strings.Repeat("z", 64) },
		"trim log without object 3": func(r *rbdJournalLostAppendRecord) {
			r.Trim.TrimLog = slices.DeleteFunc(r.Trim.TrimLog, func(l string) bool { return strings.HasSuffix(l, ".3") })
		},
		"trim log removed set 1": func(r *rbdJournalLostAppendRecord) {
			r.Trim.TrimLog = append(r.Trim.TrimLog, "remove_set: removing object set 1")
		},
		"trim log truncated":                 func(r *rbdJournalLostAppendRecord) { r.Trim.TrimLogTruncated = true },
		"inspect replays after the takeover": func(r *rbdJournalLostAppendRecord) { *r.Views["takeover"].InspectTotal = 5 },
		"inspect before the trim saw nothing": func(r *rbdJournalLostAppendRecord) {
			v := r.Views["disconnect-oracle"]
			v.InspectOwned, *v.InspectTotal = []rbdJournalLostAppendEvent{}, 0
		},
		"inspect before the trim saw entry 6": func(r *rbdJournalLostAppendRecord) {
			r.Views["disconnect-oracle"].InspectOwned = []rbdJournalLostAppendEvent{rbdJournalEntryEvent(r.Trim.Entries[6])}
		},
		"inspect errors before the trim": func(r *rbdJournalLostAppendRecord) { *r.Views["disconnect-oracle"].InspectErrors = 1 },
		"entry 7 not replayed": func(r *rbdJournalLostAppendRecord) {
			r.Views["takeover"].ImageHeadSHA256 = r.Trim.ExpectedHeadCommitted
		},
		"commit still in set 0":         func(r *rbdJournalLostAppendRecord) { r.Views["takeover"].ImageCommit[3] = rbdJournalPosition{3, 2, 3} },
		"takeover entry in another tag": func(r *rbdJournalLostAppendRecord) { r.Trim.Object4Tail[0].Tag = 2 },
		"takeover entry elsewhere":      func(r *rbdJournalLostAppendRecord) { r.Views["takeover"].ObjectSizes[4] = r.Trim.SizesAppended[4] },
		"tag predecessor not entry 7":   func(r *rbdJournalLostAppendRecord) { r.Views["takeover"].LastTag.PredecessorEnt = 6 },
		"takeover found a locker": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.LockPath.Steps[0] = "acquire.handle_get_locker r=0"
		},
		"oracle back at the takeover":  func(r *rbdJournalLostAppendRecord) { r.Views["takeover"].Clients[1].State = "connected" },
		"observer object read differs": func(r *rbdJournalLostAppendRecord) { r.Trim.Reads["takeover"][6].Equal = false },
		"subject moved the minimum set": func(r *rbdJournalLostAppendRecord) {
			r.Requests = append(r.Requests, rbdWriteRequestEvidence{Label: "x", Inputs: []rbdOperationInputEvidence{{Code: uint16(cephmsgr.OSDCall), Method: "set_minimum_set"}}})
		},
		"subject removed an object": func(r *rbdJournalLostAppendRecord) {
			r.Requests = append(r.Requests, rbdWriteRequestEvidence{Label: "x", Inputs: []rbdOperationInputEvidence{{Code: uint16(cephmsgr.OSDDelete)}}})
		},
		"order: break before the disconnect": func(r *rbdJournalLostAppendRecord) {
			i, j := slices.Index(r.Order, "force-break"), slices.Index(r.Order, "disconnect-oracle")
			r.Order[i], r.Order[j] = r.Order[j], r.Order[i]
		},
	} {
		r := rbdJournalTrimSample()
		mutate(r)
		if len(rbdJournalLostAppendClaims(r, "takeover")) == 0 {
			t.Errorf("contradictory trimming record accepted: %s", name)
		}
	}
	// The trimmer's marks name every object of set 0 in order.
	if marks := rbdJournalTrimMarks("p.", 4); len(marks) != 7 || marks[2] != "removing journal object p.0" || marks[5] != "removing journal object p.3" ||
		!rbdJournalSubsequence([]string{"x", marks[0], "y", marks[1], marks[2], marks[3], marks[4], marks[5], marks[6]}, marks) ||
		rbdJournalSubsequence([]string{marks[1], marks[0], marks[2], marks[3], marks[4], marks[5], marks[6]}, marks) {
		t.Fatal("trim marks or their order check differ", marks)
	}
}
