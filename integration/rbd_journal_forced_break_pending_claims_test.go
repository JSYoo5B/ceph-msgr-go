package integration_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// rbdJournalForcedPendingSample is a consistent record of one pending
// forced-break run with synthetic values: the forced-break sample's lock,
// answered entry 0 (tag 2 in journal object 0) and break, two held appends
// (entries 1 and 2 in journal objects 1 and 2), the new-connection entry 1
// and the takeover's one entry of tag 3 in journal object 0.
func rbdJournalForcedPendingSample() *rbdJournalLostAppendRecord {
	hash := func(c string) string { return strings.Repeat(c, 64) }
	n := func(v int) *int { return &v }
	r := rbdJournalForcedBreakSample()
	r.Token, r.Pool = strings.Repeat("ab", 16), "ceph-msgr-rbd-"+strings.Repeat("ab", 16)
	r.Order = slices.Clone(rbdJournalForcedPendingOrder)
	ol := r.OwnerLoss
	clone := func(name, action, observed string) *rbdJournalLostAppendView {
		v := *r.Views[name]
		v.Action, v.ObservedUTC, v.ObjectSizes, v.ObjectSHA256 = action, observed, slices.Clone(v.ObjectSizes), slices.Clone(v.ObjectSHA256)
		v.Blocklist = slices.Clone(v.Blocklist)
		return &v
	}
	r.Views["observe-pending"] = clone("observe-appended", "observe-pending", "2023-11-14T22:13:30Z")
	r.Views["force-break"].ObservedUTC = "2023-11-14T22:13:31Z"
	opened := clone("observe-fenced", "observe-new-connection", "2023-11-14T22:13:35Z")
	opened.ObjectSizes[1], opened.ObjectSHA256[1] = 8269+4173, hash("7")
	r.Views["observe-new-connection"] = opened
	// The takeover replayed entry 0 and the new-connection entry 1 of tag
	// 2, then wrote its own entry 0 of tag 3 into journal object 0.
	tk := r.Views["takeover"]
	tk.ObjectSizes, tk.ObjectSHA256 = slices.Clone(tk.ObjectSizes), slices.Clone(tk.ObjectSHA256)
	tk.ObjectSizes[1], tk.ObjectSHA256[1] = 8269+4173, hash("7")
	tk.InspectTotal = n(8)
	tk.InspectOwned = append(slices.Clone(tk.InspectOwned), rbdJournalLostAppendEvent{"AioWrite", 100 << 10, 4 << 10, "2023-11-14T22:13:30.000005Z"})
	tk.ImageCommit = []rbdJournalPosition{{0, 3, 0}, {1, 2, 1}, {3, 1, 3}, {2, 1, 2}}
	tk.LastTag = rbdJournalLostAppendTag{3, true, 2, 1}
	// rbd journal status: the image client and the oracle client, which
	// never moves.
	oracle := "ceph-msgr-oracle-" + r.Token[:8]
	for _, v := range r.Views {
		v.Clients = []rbdJournalClientView{{"", "connected", slices.Clone(v.ImageCommit)}, {oracle, "connected", []rbdJournalPosition{}}}
	}
	submission := func(tid uint64, epoch uint32) cephmsgr.OSDSubmission {
		return cephmsgr.OSDSubmission{Transaction: tid, GlobalID: r.SubjectGlobalID, MapEpoch: epoch}
	}
	applied := func(label string, tid uint64) *rbdLockOwnerLossAttempt {
		return &rbdLockOwnerLossAttempt{Label: label, Invocations: 1, Class: "applied", Submission: submission(tid, 52), ReplyTransaction: tid, ReplyMapEpoch: 52}
	}
	pending := func(tid, object uint64, offset int, sum string, image uint64) rbdJournalLostAppendEntry {
		return rbdJournalLostAppendEntry{Tag: 2, EntryTID: tid, Object: object, ObjectName: "journal_data.3.ab." + string(rune('0'+object)), Offset: offset, Length: 4173,
			SHA256: hash(sum), ImageOffset: image, ImageLength: 4 << 10}
	}
	takeover := ol.Takeover
	ol.Takeover, ol.Fenced, ol.Probe, ol.FencedEpoch = nil, nil, nil, 0
	r.ForcedPending = &rbdJournalForcedPending{SentAtEpoch: 42, NamedEpoch: 50,
		Entries: []rbdJournalLostAppendEntry{pending(1, 1, 8269, "5", 120<<10), pending(2, 2, 4173, "6", 160<<10)},
		Requests: []rbdLockOwnerLossAttempt{
			{Label: rbdJournalPendingAwaited, Invocations: 1, Class: "refused", Code: rbdOSDBlocklisted, ErrorCode: rbdOSDBlocklisted, Error: "ceph: OSD result code -108",
				Submission: submission(13, 50), ReplyTransaction: 13, ReplyMapEpoch: 51},
			{Label: rbdJournalPendingCanceled, Invocations: 1, Class: "uncertain", UnknownOutcome: true, Canceled: true, Error: "unknown", Submission: submission(14, 50)}},
		PendingAfterObserve: true, PendingAfterBreak: true, CanceledUTC: "2023-11-14T22:13:32Z", AnsweredUTC: "2023-11-14T22:13:33Z",
		Advance: &rbdLockOwnerLossAdvance{Pool: r.Pool, Flag: "nodeep-scrub", EpochBefore: 43, EpochTarget: 51, EpochAfter: 51, Epochs: []uint32{44, 45, 46, 47, 48, 49, 50, 51},
			Commands: []string{"osd pool set " + r.Pool + " nodeep-scrub true"},
			OpsBefore: []rbdLockOwnerLossOp{{Client: "client.4321", Transaction: 13, InitiatedAt: "2023-11-14T22:13:29.000000+0000"},
				{Client: "client.4321", Transaction: 14, InitiatedAt: "2023-11-14T22:13:29.000100+0000"}},
			OpsAfter: []rbdLockOwnerLossOp{}},
		DialsBefore: 1, DialsAfter: 2, NewEpoch: 52, NewWatchObject: r.Entry.ObjectName, NewWatchCookie: 91, NewAddress: "10.0.0.9:0/12",
		NewWatch: applied("new_watch_register", 1), NewUnwatch: applied("new_watch_unwatch", 3),
		Probe: &rbdJournalLostAppendEntry{Tag: 2, EntryTID: 1, Object: 1, ObjectName: "journal_data.3.ab.1", Offset: 8269, Length: 4173, SHA256: hash("d"), ImageOffset: 100 << 10,
			ImageLength: 4 << 10, Timestamp: cephmsgr.OSDTime{Seconds: 1700000010, Nanoseconds: 5000}},
		ProbeObjectSHA256: hash("7"), ProbeOutcome: applied("journal_append_new_connection", 2),
		TakeoverEvents:       []rbdJournalLostAppendEvent{{Type: "AioWrite", Offset: 40 << 10, Length: 2 << 10}},
		ExpectedHeadTakeover: tk.ImageHeadSHA256, ExpectedCommitTakeover: slices.Clone(tk.ImageCommit), Takeover: takeover}
	r.Gate.Connections = 2
	return r
}

// rbdJournalForcedPendingCut drops what the record would not have yet at
// stage.
func rbdJournalForcedPendingCut(r *rbdJournalLostAppendRecord, stage string) {
	at := slices.Index(rbdJournalForcedPendingStages, stage)
	r.Order = r.Order[:slices.Index(r.Order, stage)+1]
	later := map[int][]string{0: {"observe-pending"}, 1: {"force-break", "observe-fenced"}, 2: {"observe-new-connection"}, 3: {"takeover"}}
	for from, names := range later {
		if at < from+1 {
			for _, name := range names {
				delete(r.Views, name)
			}
		}
	}
	fp, ol := r.ForcedPending, r.OwnerLoss
	if at < 4 {
		fp.Takeover = nil
	}
	if at < 3 {
		r.Gate.Connections = 1
		fp.DialsBefore, fp.DialsAfter, fp.NewEpoch, fp.NewWatchCookie, fp.NewAddress, fp.NewWatch, fp.NewUnwatch, fp.Probe, fp.ProbeOutcome = 0, 0, 0, 0, "", nil, nil, nil, nil
	}
	if at < 2 {
		ol.Break, fp.PendingAfterBreak, fp.CanceledUTC, fp.AnsweredUTC, fp.Advance = nil, false, "", "", nil
		for i := range fp.Requests {
			fp.Requests[i] = rbdLockOwnerLossAttempt{Label: fp.Requests[i].Label, Invocations: 1}
		}
	}
	if at < 1 {
		r.ForcedPending = &rbdJournalForcedPending{}
		ol.PingBefore = nil
	}
}

// TestRBDJournalForcedPendingClaims feeds the pending forced-break claims
// contradictory records.
func TestRBDJournalForcedPendingClaims(t *testing.T) {
	n := func(v int) *int { return &v }
	if problems := rbdJournalLostAppendClaims(rbdJournalForcedPendingSample(), "takeover"); len(problems) != 0 {
		t.Fatal("consistent pending forced-break record refused", problems)
	}
	for _, stage := range rbdJournalForcedPendingStages[:4] {
		if len(rbdJournalLostAppendClaims(rbdJournalForcedPendingSample(), stage)) == 0 {
			t.Fatal("steps beyond the stage accepted", stage)
		}
		r := rbdJournalForcedPendingSample()
		rbdJournalForcedPendingCut(r, stage)
		if problems := rbdJournalLostAppendClaims(r, stage); len(problems) != 0 {
			t.Fatal("consistent partial pending forced-break record refused", stage, problems)
		}
	}
	if len(rbdJournalLostAppendClaims(rbdJournalForcedPendingSample(), "replay")) == 0 {
		t.Fatal("unknown stage accepted")
	}
	r := rbdJournalForcedPendingSample()
	rbdJournalForcedPendingCut(r, "observe-pending")
	r.ForcedPending.Requests[0].Class = "refused"
	if len(rbdJournalLostAppendClaims(r, "observe-pending")) == 0 {
		t.Fatal("a held append answered at the observe-pending stage accepted")
	}
	type change = func(*rbdJournalLostAppendRecord)
	fp := func(r *rbdJournalLostAppendRecord) *rbdJournalForcedPending { return r.ForcedPending }
	view := func(r *rbdJournalLostAppendRecord, name string) *rbdJournalLostAppendView { return r.Views[name] }
	for name, mutate := range map[string]change{
		// The held appends before the break.
		"held append applied before the break": func(r *rbdJournalLostAppendRecord) { view(r, "observe-pending").ObjectSizes[1] += 4173 },
		"blocklisted while held": func(r *rbdJournalLostAppendRecord) {
			view(r, "observe-pending").Blocklist = view(r, "observe-fenced").Blocklist
		},
		"lock lost while held":        func(r *rbdJournalLostAppendRecord) { view(r, "observe-pending").LockFree = true },
		"not held at the observation": func(r *rbdJournalLostAppendRecord) { fp(r).PendingAfterObserve = false },
		"named epoch not above":       func(r *rbdJournalLostAppendRecord) { fp(r).NamedEpoch = fp(r).SentAtEpoch },
		"one held append only": func(r *rbdJournalLostAppendRecord) {
			fp(r).Entries, fp(r).Requests = fp(r).Entries[:1], fp(r).Requests[:1]
		},
		"held entry repeats entry 0":      func(r *rbdJournalLostAppendRecord) { fp(r).Entries[0].EntryTID = 0 },
		"held entry in the owned object":  func(r *rbdJournalLostAppendRecord) { fp(r).Entries[1].Object = 0 },
		"held entry at another offset":    func(r *rbdJournalLostAppendRecord) { fp(r).Entries[0].Offset = 0 },
		"no ping before the held appends": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.PingBefore = nil },
		// The break and the answer.
		"answered before the break":    func(r *rbdJournalLostAppendRecord) { fp(r).PendingAfterBreak = false },
		"break lock path missing":      func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Break.LockPath = nil },
		"break without its blocklist":  func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Break.Blocklist = []rbdLockOwnerLossBlock{} },
		"break of another owner":       func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Break.Owners = []string{"10.0.0.2:0/2"} },
		"journal changed by the break": func(r *rbdJournalLostAppendRecord) { view(r, "force-break").ObjectSizes[2] += 4173 },
		"canceled wait answered": func(r *rbdJournalLostAppendRecord) {
			c := &fp(r).Requests[1]
			c.Class, c.UnknownOutcome, c.Canceled, c.Code, c.ErrorCode, c.ReplyTransaction = "refused", false, false, -108, -108, 14
		},
		"canceled append resent":       func(r *rbdJournalLostAppendRecord) { fp(r).Requests[1].Invocations = 2 },
		"canceled before the break":    func(r *rbdJournalLostAppendRecord) { fp(r).CanceledUTC = "2023-11-14T22:13:30.5Z" },
		"canceled without transaction": func(r *rbdJournalLostAppendRecord) { fp(r).Requests[1].Submission.Transaction = 0 },
		"awaited append applied": func(r *rbdJournalLostAppendRecord) {
			a := &fp(r).Requests[0]
			a.Class, a.Code, a.ErrorCode, a.Error = "applied", 0, 0, ""
		},
		"awaited append refused otherwise": func(r *rbdJournalLostAppendRecord) { fp(r).Requests[0].ErrorCode, fp(r).Requests[0].Code = -2, -2 },
		"awaited answer at an older epoch": func(r *rbdJournalLostAppendRecord) { fp(r).Requests[0].ReplyMapEpoch = 49 },
		"awaited append resent":            func(r *rbdJournalLostAppendRecord) { fp(r).Requests[0].Invocations = 2 },
		"answered before the cancel":       func(r *rbdJournalLostAppendRecord) { fp(r).AnsweredUTC = "2023-11-14T22:13:31.5Z" },
		"advance missing":                  func(r *rbdJournalLostAppendRecord) { fp(r).Advance = nil },
		"advance started above the named":  func(r *rbdJournalLostAppendRecord) { fp(r).Advance.EpochBefore = 50 },
		"advance started before the break": func(r *rbdJournalLostAppendRecord) { fp(r).Advance.EpochBefore = 42 },
		"advance short of the named":       func(r *rbdJournalLostAppendRecord) { fp(r).Advance.EpochAfter = 49 },
		"advance left the flag set":        func(r *rbdJournalLostAppendRecord) { fp(r).Advance.FlagFinally = true },
		"advance on another pool":          func(r *rbdJournalLostAppendRecord) { fp(r).Advance.Pool = "rbd" },
		"canceled append not in flight":    func(r *rbdJournalLostAppendRecord) { fp(r).Advance.OpsBefore = fp(r).Advance.OpsBefore[:1] },
		"canceled append held twice": func(r *rbdJournalLostAppendRecord) {
			fp(r).Advance.OpsBefore = append(fp(r).Advance.OpsBefore, fp(r).Advance.OpsBefore[1])
		},
		"awaited append held twice": func(r *rbdJournalLostAppendRecord) {
			fp(r).Advance.OpsBefore = append(fp(r).Advance.OpsBefore, fp(r).Advance.OpsBefore[0])
		},
		"another subject operation held": func(r *rbdJournalLostAppendRecord) {
			fp(r).Advance.OpsBefore = append(fp(r).Advance.OpsBefore, rbdLockOwnerLossOp{Client: "client.4321", Transaction: 15, InitiatedAt: "2023-11-14T22:13:29.000200+0000"})
		},
		"held by another client": func(r *rbdJournalLostAppendRecord) {
			fp(r).Advance.OpsBefore[0].Client = "client.4300"
		},
		"in flight only after the observation": func(r *rbdJournalLostAppendRecord) {
			fp(r).Advance.OpsBefore[1].InitiatedAt = "2023-11-14T22:13:30.500000+0000"
		},
		"still in flight after the advance": func(r *rbdJournalLostAppendRecord) { fp(r).Advance.OpsAfter = fp(r).Advance.OpsBefore[:1] },
		"held append in the fenced journal": func(r *rbdJournalLostAppendRecord) {
			view(r, "observe-fenced").ObjectSHA256[2] = strings.Repeat("f", 64)
		},
		"fenced watch still listed": func(r *rbdJournalLostAppendRecord) { view(r, "observe-fenced").SubjectWatcher = true },
		// The new connection.
		"new connection with the lock record's nonce": func(r *rbdJournalLostAppendRecord) { fp(r).NewAddress = rbdJournalOwnerLossSampleAddress },
		"new connection from another host":            func(r *rbdJournalLostAppendRecord) { fp(r).NewAddress = "10.0.0.8:0/12" },
		"new address unparsed":                        func(r *rbdJournalLostAppendRecord) { fp(r).NewAddress = "" },
		"new address blocklisted": func(r *rbdJournalLostAppendRecord) {
			v := view(r, "observe-new-connection")
			v.Blocklist = append(v.Blocklist, rbdLockOwnerLossBlock{"10.0.0.9:0/12", "2023-11-14T23:13:20Z"})
		},
		"no new dial":                    func(r *rbdJournalLostAppendRecord) { fp(r).DialsAfter = 1 },
		"new requests at an older epoch": func(r *rbdJournalLostAppendRecord) { fp(r).NewEpoch = 50 },
		"new append before the advance":  func(r *rbdJournalLostAppendRecord) { fp(r).ProbeOutcome.Submission.MapEpoch = 50 },
		"new watch refused": func(r *rbdJournalLostAppendRecord) {
			fp(r).NewWatch.Class, fp(r).NewWatch.ErrorCode = "refused", rbdOSDBlocklisted
		},
		"new watch reuses the lock cookie": func(r *rbdJournalLostAppendRecord) { fp(r).NewWatchCookie = r.WatchCookie },
		"new-connection append refused": func(r *rbdJournalLostAppendRecord) {
			fp(r).ProbeOutcome.Class, fp(r).ProbeOutcome.Code, fp(r).ProbeOutcome.ErrorCode = "refused", rbdOSDBlocklisted, rbdOSDBlocklisted
		},
		"new-connection append uncertain": func(r *rbdJournalLostAppendRecord) {
			fp(r).ProbeOutcome.Class, fp(r).ProbeOutcome.UnknownOutcome = "uncertain", true
		},
		"refused entry sent again":          func(r *rbdJournalLostAppendRecord) { fp(r).Probe.SHA256 = fp(r).Entries[0].SHA256 },
		"new-connection entry tid 2":        func(r *rbdJournalLostAppendRecord) { fp(r).Probe.EntryTID = 2 },
		"new-connection entry elsewhere":    func(r *rbdJournalLostAppendRecord) { fp(r).Probe.Offset = 0 },
		"new-connection entry not appended": func(r *rbdJournalLostAppendRecord) { view(r, "observe-new-connection").ObjectSizes[1] = 8269 },
		"new-connection entry other bytes": func(r *rbdJournalLostAppendRecord) {
			view(r, "observe-new-connection").ObjectSHA256[1] = strings.Repeat("f", 64)
		},
		"new watch not removed": func(r *rbdJournalLostAppendRecord) { fp(r).NewUnwatch = nil },
		"one connection only":   func(r *rbdJournalLostAppendRecord) { r.Gate.Connections = 1 },
		// The takeover and the journal header.
		"takeover replayed only entry 0": func(r *rbdJournalLostAppendRecord) {
			v := view(r, "takeover")
			v.InspectOwned, v.InspectTotal = v.InspectOwned[:1], n(7)
		},
		"takeover tag after entry 0":     func(r *rbdJournalLostAppendRecord) { view(r, "takeover").LastTag.PredecessorEnt = 0 },
		"takeover image without entry 1": func(r *rbdJournalLostAppendRecord) { view(r, "takeover").ImageHeadSHA256 = strings.Repeat("d", 64) },
		"takeover commit before entry 1": func(r *rbdJournalLostAppendRecord) {
			view(r, "takeover").ImageCommit = []rbdJournalPosition{{0, 3, 0}, {3, 1, 3}, {2, 1, 2}, {1, 1, 1}}
		},
		"takeover changed journal object 1": func(r *rbdJournalLostAppendRecord) { view(r, "takeover").ObjectSizes[1] += 10 },
		"takeover found a locker": func(r *rbdJournalLostAppendRecord) {
			fp(r).Takeover.LockPath.Steps[0] = "acquire.handle_get_locker r=0"
		},
		"takeover lock log missing": func(r *rbdJournalLostAppendRecord) { fp(r).Takeover.LockPath = nil },
		"takeover lock record not free": func(r *rbdJournalLostAppendRecord) {
			fp(r).Takeover.BoundaryLockers = []rbdJournalLockRecord{{"client.4321", r.LockCookie, rbdJournalOwnerLossSampleAddress}}
		},
		"takeover never owned":     func(r *rbdJournalLostAppendRecord) { fp(r).Takeover.Owner = false },
		"takeover output missing":  func(r *rbdJournalLostAppendRecord) { fp(r).Takeover = nil },
		"takeover wrote elsewhere": func(r *rbdJournalLostAppendRecord) { fp(r).Takeover.Events[0].Offset = 0 },
		"active set advanced":      func(r *rbdJournalLostAppendRecord) { view(r, "takeover").ActiveSet = 1 },
		"minimum set advanced":     func(r *rbdJournalLostAppendRecord) { view(r, "observe-fenced").MinimumSet = 1 },
		"oracle client committed": func(r *rbdJournalLostAppendRecord) {
			view(r, "takeover").Clients[1].Positions = []rbdJournalPosition{{0, 3, 0}}
		},
		"oracle client unregistered": func(r *rbdJournalLostAppendRecord) {
			view(r, "observe-pending").Clients = view(r, "observe-pending").Clients[:1]
		},
		"subject registered a client": func(r *rbdJournalLostAppendRecord) {
			v := view(r, "takeover")
			v.Clients = append(v.Clients, rbdJournalClientView{ID: "go"})
		},
		"image client status differs": func(r *rbdJournalLostAppendRecord) { view(r, "observe-fenced").Clients[0].Positions = nil },
		"image client disconnected":   func(r *rbdJournalLostAppendRecord) { view(r, "takeover").Clients[0].State = "disconnected" },
		"order: new connection too soon": func(r *rbdJournalLostAppendRecord) {
			i := slices.Index(r.Order, "new_connection")
			r.Order[i], r.Order[i-1] = r.Order[i-1], r.Order[i]
		},
		"order: answered before advance": func(r *rbdJournalLostAppendRecord) {
			i, j := slices.Index(r.Order, "advance-map"), slices.Index(r.Order, "pending_answered")
			r.Order[i], r.Order[j] = r.Order[j], r.Order[i]
		},
	} {
		r := rbdJournalForcedPendingSample()
		mutate(r)
		if len(rbdJournalLostAppendClaims(r, "takeover")) == 0 {
			t.Errorf("contradictory pending forced-break record accepted: %s", name)
		}
	}
}

// TestRBDJournalLockPathOf projects librbd debug log lines in the formats
// of the pinned sources (dout_prefix with the object pointer and the
// function, then the message) into lock steps.
func TestRBDJournalLockPathOf(t *testing.T) {
	line := func(component, text string) struct {
		Level     int    `json:"level"`
		Component string `json:"component"`
		Text      string `json:"text"`
	} {
		return struct {
			Level     int    `json:"level"`
			Component string `json:"component"`
			Text      string `json:"text"`
		}{10, component, text}
	}
	const acquire, breaker, exclusive, watcher, managed = "librbd::managed_lock::AcquireRequest", "librbd::managed_lock::BreakRequest", "librbd::ExclusiveLock",
		"librbd::ImageWatcher", "librbd::ManagedLock"
	l := &nativeRBDJournalLockLog{Present: true, Matched: 14}
	for _, x := range [][2]string{
		{managed, "@0 send_acquire_lock: "}, {acquire, "@1 send_get_locker: "}, {acquire, "@1 handle_get_locker: r=0"}, {acquire, "@1 handle_lock: r=-16"},
		{breaker, "@2 handle_get_watchers: r=0"}, {breaker, "@2 handle_get_watchers: lock owner is still alive"}, {breaker, "@2 finish: r=-11"},
		{acquire, "@1 handle_break_lock: r=-11"}, {acquire, "@1 handle_break_lock: lock owner is still alive"}, {exclusive, "@3 post_acquire_lock_handler: r=-11"},
		{watcher, "@4 notify request lock"}, {watcher, "@4 timed out requesting lock: retrying"},
		{breaker, "@5 send_blocklist: local entity=client.4500, locker entity=client.4321"}, {"librbd::managed_lock::GetLockerRequest", "@6 handle_get_lockers: r=0"},
	} {
		l.Lines = append(l.Lines, line(x[0], x[1]))
	}
	want := []string{"acquire.handle_get_locker r=0", "acquire.handle_lock r=-16", "break.handle_get_watchers r=0", "break.handle_get_watchers lock owner is still alive",
		"break.finish r=-11", "acquire.handle_break_lock r=-11", "exclusive.post_acquire_lock_handler r=-11", "watcher.notify_request_lock"}
	p := rbdJournalLockPathOf(l)
	if p == nil || !p.Present || p.Truncated || p.Matched != 14 || !slices.Equal(p.Steps, want) {
		t.Fatalf("lock log projection differs: %+v", p)
	}
	if !slices.Equal(want, rbdJournalOwnerLossLockPath(1)[:len(want)]) {
		t.Fatal("the projected blocked attempt is not the owner-loss path's")
	}
	if rbdJournalLockPathOf(nil) != nil {
		t.Fatal("an absent lock log projected")
	}
}
