package integration_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

const rbdJournalOwnerLossSampleAddress = "10.0.0.9:0/11"

// rbdJournalTakeoverSample is a consistent takeover observation for the
// lost-append sample's entry (tag 2, entry 0 in journal object 0): native
// librbd's one entry of tag 3 follows the owned entry in object 0.
func rbdJournalTakeoverSample(r *rbdJournalLostAppendRecord, forced bool) {
	p := func(s string) *string { return &s }
	n := func(v int) *int { return &v }
	u := func(v uint64) *uint64 { return &v }
	hash := func(c string) string { return strings.Repeat(c, 64) }
	block := []rbdLockOwnerLossBlock{{rbdJournalOwnerLossSampleAddress, "2023-11-14T23:13:20Z"}}
	v := *r.Views["checkpoint"]
	v.Action, v.ObjectSizes, v.ObjectSHA256 = "takeover", slices.Clone(v.ObjectSizes), slices.Clone(v.ObjectSHA256)
	v.ObjectSizes[0], v.ObjectSHA256[0] = 8347+4173+2190, hash("8")
	v.PrefixSHA256, v.EntrySHA256, v.TrailingBytes = p(hash("0")), p(hash("e")), u(2190)
	v.InspectTotal, v.InspectErrors = n(7), n(0)
	v.InspectOwned = slices.Clone(r.Views["checkpoint"].InspectOwned)
	v.InspectNext = []rbdJournalLostAppendEvent{{"AioWrite", 40 << 10, 2 << 10, "2023-11-14T22:14:00.5Z"}}
	v.ImageHeadSHA256, v.ImageCommit = hash("c"), []rbdJournalPosition{{0, 3, 0}, {3, 1, 3}, {2, 1, 2}, {1, 1, 1}}
	v.NextTag, v.LastTag = 4, rbdJournalLostAppendTag{3, true, 2, 0}
	v.LockFree, v.LockOwner, v.LockCookie, v.SubjectWatcher, v.Watchers = true, "", "", false, 0
	v.Blocklist = slices.Clone(block)
	r.Views["takeover"] = &v
	before := []rbdLockOwnerLossBlock{}
	if forced {
		before = slices.Clone(block)
	}
	r.OwnerLoss.TakeoverEvents = []rbdJournalLostAppendEvent{{Type: "AioWrite", Offset: 40 << 10, Length: 2 << 10}}
	r.OwnerLoss.ExpectedHeadTakeover, r.OwnerLoss.ExpectedCommitTakeover = hash("c"), slices.Clone(v.ImageCommit)
	r.OwnerLoss.Takeover = &rbdJournalTakeover{Owner: true, WriteNS: 9000, Events: []rbdJournalLostAppendEvent{{Type: "AioWrite", Offset: 40 << 10, Length: 2 << 10}},
		BlocklistBefore: before, Blocklist: slices.Clone(block), NativeGlobalID: rbdJournalTakeoverSampleGID,
		BoundaryLockers: []rbdJournalLockRecord{}, BoundaryWatchers: []rbdJournalWatch{{rbdJournalTakeoverSampleGID, rbdJournalTakeoverSampleHandle, "10.0.0.5:0/5"}},
		LockPath: rbdJournalLockPathSample(rbdJournalFreeLockPath)}
	if forced {
		return
	}
	// Owner loss: the subject's lock record and watch at the boundary, the
	// observer's watch beside it, and the takeover's requests, acquisition
	// and release as the observer received them.
	tk := r.OwnerLoss.Takeover
	tk.BoundaryLockers = []rbdJournalLockRecord{{fmt.Sprintf("client.%d", r.SubjectGlobalID), r.LockCookie, rbdJournalOwnerLossSampleAddress}}
	tk.BoundaryTag, tk.BoundaryExclusive = rbdLockTag, true
	tk.BoundaryWatchers = append([]rbdJournalWatch{{r.SubjectGlobalID, r.WatchCookie, rbdJournalOwnerLossSampleAddress}, {r.ObserverGlobalID, 66, "10.0.0.6:0/6"}}, tk.BoundaryWatchers...)
	r.OwnerLoss.ObserverWatchCookie, r.OwnerLoss.WatchAgeAtTakeover = 66, "14.2s"
	native := rbdClientID{GID: rbdJournalTakeoverSampleGID, Handle: rbdJournalTakeoverSampleHandle}
	for i, op := range []uint32{rbdNotifyRequestLock, rbdNotifyRequestLock, rbdNotifyAcquiredLock, rbdNotifyReleasedLock} {
		tk.Notes = append(tk.Notes, rbdLockNotification{Index: i, NotifyID: uint64(700 + i), NotifierGlobalID: native.GID, Cookie: 66, Op: op, Client: native, Role: "non-owner",
			AckDisposition: rbdAckSubmittedEmpty})
	}
	tk.LockPath = rbdJournalLockPathSample(rbdJournalOwnerLossLockPath(2))
}

// rbdJournalLockPathSample is a complete lock log projected to steps.
func rbdJournalLockPathSample(steps []string) *rbdJournalLockPath {
	return &rbdJournalLockPath{Present: true, Matched: 3 * len(steps), Steps: slices.Clone(steps)}
}

// The takeover instance of the samples.
const (
	rbdJournalTakeoverSampleGID    = 4500
	rbdJournalTakeoverSampleHandle = 99
)

// rbdJournalOwnerLossSample is a consistent record of one owner-loss run in
// the given connection mode: the lost-append sample up to the observation
// after the recorded outcome, then the closed connection and the takeover.
func rbdJournalOwnerLossSample(mode string) *rbdJournalLostAppendRecord {
	n := func(v int) *int { return &v }
	r := rbdJournalLostAppendSample(mode)
	delete(r.Views, "replay")
	r.Order = slices.Clone(rbdJournalOwnerLossOrder)
	r.OwnerLoss = &rbdJournalOwnerLoss{LockerAddress: rbdJournalOwnerLossSampleAddress}
	r.Gate.Connections, r.DialsTotal = 1, n(1)
	rbdJournalTakeoverSample(r, false)
	return r
}

// rbdJournalForcedBreakSample is a consistent record of one forced-break
// run with synthetic values.
func rbdJournalForcedBreakSample() *rbdJournalLostAppendRecord {
	hash := func(c string) string { return strings.Repeat(c, 64) }
	base := rbdJournalLostAppendSample("crc")
	block := []rbdLockOwnerLossBlock{{rbdJournalOwnerLossSampleAddress, "2023-11-14T23:13:20Z"}}
	clone := func(name, action string) *rbdJournalLostAppendView {
		v := *base.Views[name]
		v.Action, v.ObjectSizes, v.ObjectSHA256, v.ImageCommit = action, slices.Clone(v.ObjectSizes), slices.Clone(v.ObjectSHA256), slices.Clone(v.ImageCommit)
		v.InspectTotal, v.InspectErrors, v.InspectOwned = nil, nil, nil
		return &v
	}
	appended := clone("observe-recorded", "observe-appended")
	appended.Blocklist = []rbdLockOwnerLossBlock{}
	free := func(v *rbdJournalLostAppendView) *rbdJournalLostAppendView {
		v.LockFree, v.LockOwner, v.LockCookie, v.SubjectWatcher, v.Watchers, v.Blocklist = true, "", "", false, 0, slices.Clone(block)
		return v
	}
	r := &rbdJournalLostAppendRecord{SubjectGlobalID: base.SubjectGlobalID, ObserverGlobalID: base.ObserverGlobalID, ConnectionMode: "crc", WatchCookie: base.WatchCookie,
		LockCookie: base.LockCookie, Entry: base.Entry, Order: slices.Clone(rbdJournalForcedBreakOrder),
		Outcome: &rbdJournalLostAppendOutcome{Label: "journal_append", CallerInvocations: 1, Class: "applied", Submission: cephmsgr.OSDSubmission{Transaction: 9, GlobalID: base.SubjectGlobalID},
			ReplyTransaction: 9, RecordedUTC: "2023-11-14T22:13:21Z"},
		Views: map[string]*rbdJournalLostAppendView{"setup": base.Views["setup"], "observe-locked": base.Views["observe-locked"], "observe-appended": appended,
			"force-break": free(clone("observe-recorded", "force-break")), "observe-fenced": free(clone("observe-recorded", "observe-fenced")),
			"checkpoint": base.Views["checkpoint"]},
		ExpectedHeadBefore: hash("a"), ExpectedHeadAfter: hash("b"), ExpectedCommitAfter: slices.Clone(base.ExpectedCommitAfter)}
	r.Gate = base.Gate
	r.Gate.Bound, r.Gate.Connections, r.Gate.LargeWrites = false, 1, 0
	fenced := func(label string) rbdLockOwnerLossAttempt {
		return rbdLockOwnerLossAttempt{Label: label, Invocations: 1, Class: "refused", ErrorCode: -108, Error: "ceph: OSD result code -108", Submission: cephmsgr.OSDSubmission{Transaction: 12}}
	}
	r.OwnerLoss = &rbdJournalOwnerLoss{Forced: true, LockerAddress: rbdJournalOwnerLossSampleAddress,
		Break: &rbdJournalBreak{Owners: []string{rbdJournalOwnerLossSampleAddress}, OwnersAfter: []string{}, BreakNS: 3000, LockerBefore: "client.4321", LockerCookieBefore: base.LockCookie,
			WatchersBefore:      []rbdJournalWatch{{base.SubjectGlobalID, base.WatchCookie, rbdJournalOwnerLossSampleAddress}, {base.ObserverGlobalID, 66, "10.0.0.6:0/6"}},
			LockerAddressBefore: rbdJournalOwnerLossSampleAddress, BlocklistBefore: []rbdLockOwnerLossBlock{}, Blocklist: slices.Clone(block),
			LockPath: rbdJournalLockPathSample(rbdJournalForcedBreakLockPath)},
		PingBefore:  &rbdLockOwnerLossAttempt{Label: "watch_ping", Invocations: 1, Class: "applied"},
		Fenced:      []rbdLockOwnerLossAttempt{fenced("watch_ping_fenced"), fenced("journal_append_fenced"), fenced("lock_unlock_fenced")},
		FencedEpoch: 44,
		Probe:       &rbdJournalLostAppendEntry{Tag: 2, EntryTID: 1, Object: 1, ObjectName: "journal_data.3.ab.1", Offset: 8269, Length: 4173, SHA256: hash("f"), ImageOffset: 100 << 10, ImageLength: 4 << 10}}
	rbdJournalTakeoverSample(r, true)
	// The takeover changes no journal object but the owned entry's, since
	// the fenced observation.
	delete(r.Views, "checkpoint")
	return r
}

// TestRBDJournalOwnerLossClaims feeds the owner-loss claims contradictory
// records. The claims the variant shares with the lost-append sequence up to
// the recorded outcome are covered by TestRBDJournalLostAppendClaims.
func TestRBDJournalOwnerLossClaims(t *testing.T) {
	n := func(v int) *int { return &v }
	u := func(v uint64) *uint64 { return &v }
	for _, mode := range []string{"crc", "secure"} {
		if problems := rbdJournalLostAppendClaims(rbdJournalOwnerLossSample(mode), "takeover"); len(problems) != 0 {
			t.Fatal("consistent owner-loss record refused", mode, problems)
		}
		for _, stage := range rbdJournalOwnerLossStages[:2] {
			r := rbdJournalOwnerLossSample(mode)
			r.Order = r.Order[:slices.Index(r.Order, stage)+1]
			delete(r.Views, "takeover")
			r.Gate, r.DialsBeforeClose, r.DialsTotal, r.OwnerLoss.Takeover = nil, nil, nil, nil
			if stage == "checkpoint" {
				delete(r.Views, "observe-recorded")
				r.Outcome = nil
			}
			if problems := rbdJournalLostAppendClaims(r, stage); len(problems) != 0 {
				t.Fatal("consistent partial owner-loss record refused", mode, stage, problems)
			}
		}
		if len(rbdJournalLostAppendClaims(rbdJournalOwnerLossSample(mode), "replay")) == 0 {
			t.Fatal("the replay stage accepted in the owner-loss variant")
		}
	}
	type change = func(*rbdJournalLostAppendRecord)
	tk := func(r *rbdJournalLostAppendRecord) *rbdJournalLostAppendView { return r.Views["takeover"] }
	for name, mutate := range map[string]change{
		"takeover missing":               func(r *rbdJournalLostAppendRecord) { delete(r.Views, "takeover") },
		"takeover output missing":        func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover = nil },
		"reconnected after the loss":     func(r *rbdJournalLostAppendRecord) { r.DialsTotal, r.Gate.Connections = n(2), 2 },
		"unlocked after the record":      func(r *rbdJournalLostAppendRecord) { r.Order = slices.Insert(r.Order, len(r.Order)-1, "unlock") },
		"replay instead of the takeover": func(r *rbdJournalLostAppendRecord) { r.Order[len(r.Order)-1] = "replay" },
		"append resent":                  func(r *rbdJournalLostAppendRecord) { r.Gate.LargeWrites = 2 },
		"native librbd never owned":      func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.Owner = false },
		"lock left held":                 func(r *rbdJournalLostAppendRecord) { tk(r).LockFree = false },
		"subject watch left":             func(r *rbdJournalLostAppendRecord) { tk(r).SubjectWatcher = true },
		"subject not blocklisted":        func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.Blocklist = []rbdLockOwnerLossBlock{} },
		"blocklisted before the takeover": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.BlocklistBefore = slices.Clone(r.OwnerLoss.Takeover.Blocklist)
		},
		"another address blocklisted": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.Blocklist = []rbdLockOwnerLossBlock{{"10.0.0.9:0/12", "2023-11-14T23:13:20Z"}}
		},
		"lock record address unknown": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.LockerAddress = "" },
		"takeover wrote elsewhere":    func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.Events[0].Offset = 0 },
		"owned entry gone":            func(r *rbdJournalLostAppendRecord) { tk(r).EntrySHA256 = nil },
		"no native entry after it":    func(r *rbdJournalLostAppendRecord) { tk(r).TrailingBytes = u(0) },
		"another journal object grew": func(r *rbdJournalLostAppendRecord) { tk(r).ObjectSizes[1] += 4173 },
		"owned entry replayed twice": func(r *rbdJournalLostAppendRecord) {
			tk(r).InspectOwned = append(tk(r).InspectOwned, tk(r).InspectOwned[0])
			tk(r).InspectTotal = n(8)
		},
		"native entry missing":          func(r *rbdJournalLostAppendRecord) { tk(r).InspectNext = nil; tk(r).InspectTotal = n(6) },
		"native entry of another write": func(r *rbdJournalLostAppendRecord) { tk(r).InspectNext[0].Length = 4 << 10 },
		"inspect error":                 func(r *rbdJournalLostAppendRecord) { tk(r).InspectErrors = n(1) },
		"owned entry not replayed":      func(r *rbdJournalLostAppendRecord) { tk(r).ImageHeadSHA256 = strings.Repeat("d", 64) },
		"model without the takeover write": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.ExpectedHeadTakeover, tk(r).ImageHeadSHA256 = r.ExpectedHeadAfter, r.ExpectedHeadAfter
		},
		"commit without the takeover entry": func(r *rbdJournalLostAppendRecord) {
			tk(r).ImageCommit, r.OwnerLoss.ExpectedCommitTakeover = slices.Clone(r.ExpectedCommitAfter), slices.Clone(r.ExpectedCommitAfter)
		},
		"commit differs from the model":   func(r *rbdJournalLostAppendRecord) { tk(r).ImageCommit = slices.Clone(r.Views["setup"].ImageCommit) },
		"tag without the owned entry":     func(r *rbdJournalLostAppendRecord) { tk(r).LastTag.PredecessorTag = 1 },
		"second native tag":               func(r *rbdJournalLostAppendRecord) { tk(r).NextTag = 5 },
		"lock handed off before the loss": func(r *rbdJournalLostAppendRecord) { r.Views["observe-recorded"].LockFree = true },
		"observer watch missing": func(r *rbdJournalLostAppendRecord) {
			r.Order = slices.DeleteFunc(r.Order, func(s string) bool { return s == "observer_watch" })
		},
		// The takeover boundary: the subject's lock record and watch.
		"boundary without the takeover's identity": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.NativeGlobalID = 0 },
		"boundary without its own watch": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.BoundaryWatchers = r.OwnerLoss.Takeover.BoundaryWatchers[:2]
		},
		"boundary lock already free": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.BoundaryLockers = []rbdJournalLockRecord{} },
		"boundary lock of another cookie": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.BoundaryLockers[0].Cookie = "auto 1"
		},
		"boundary lock of another client": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.BoundaryLockers[0].Client = "client.4300"
		},
		"boundary lock at another address": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.BoundaryLockers[0].Address = "10.0.0.9:0/12"
		},
		"boundary lock not managed": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.BoundaryTag = "" },
		"boundary lock shared":      func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.BoundaryExclusive = false },
		"subject watch gone at the boundary": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.BoundaryWatchers = r.OwnerLoss.Takeover.BoundaryWatchers[1:]
		},
		"subject watch of another cookie at the boundary": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.BoundaryWatchers[0].Cookie++
		},
		"subject watch at another address at the boundary": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.BoundaryWatchers[0].Address = "10.0.0.9:0/12"
		},
		// The observer's notifications of the takeover.
		"no RequestLock before the acquisition": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.Notes = r.OwnerLoss.Takeover.Notes[2:]
		},
		"no notifications":  func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.Notes = nil },
		"no observer watch": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.ObserverWatchCookie = 0 },
		"responder failed":  func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.NoteFailure = "header watch ended" },
		"RequestLock from another client": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.Notes[0].Client.GID, r.OwnerLoss.Takeover.Notes[0].NotifierGlobalID = 4300, 4300
		},
		"RequestLock from another handle": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.Notes[1].Client.Handle = 98 },
		"RequestLock notifier differs": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.Notes[0].NotifierGlobalID = r.SubjectGlobalID
		},
		"forced RequestLock": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.Notes[0].Force = true },
		"RequestLock after the acquisition": func(r *rbdJournalLostAppendRecord) {
			n := r.OwnerLoss.Takeover.Notes
			n[1].Op, n[2].Op = n[2].Op, n[1].Op
		},
		"acquisition not announced": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.Notes = slices.Delete(r.OwnerLoss.Takeover.Notes, 2, 3)
		},
		"release not announced": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.Notes = r.OwnerLoss.Takeover.Notes[:3] },
		"extra notification": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.Notes = append(r.OwnerLoss.Takeover.Notes, r.OwnerLoss.Takeover.Notes[3])
		},
		"acknowledged as owner": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.Notes[0].AckDisposition = rbdAckSubmittedResponse
		},
		"acknowledgement failed": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.Notes[2].AckError = "ceph: OSD result code -2"
		},
		"unexpected notification":       func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.Notes[3].Unexpected = true },
		"notification on another watch": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.Notes[0].Cookie = 67 },
		// librbd's own log of the acquisition.
		"lock log missing":   func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.LockPath = nil },
		"lock log absent":    func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.LockPath.Present = false },
		"lock log truncated": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.LockPath.Truncated = true },
		"locker changed on the recheck": func(r *rbdJournalLostAppendRecord) {
			steps := r.OwnerLoss.Takeover.LockPath.Steps
			steps[3] = "break.handle_get_locker r=0"
		},
		"incompatible lock instead of a live watcher": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.LockPath.Steps = slices.Concat([]string{"acquire.handle_get_locker r=-16", "exclusive.post_acquire_lock_handler r=-16", "watcher.notify_request_lock"},
				r.OwnerLoss.Takeover.LockPath.Steps[8:])
		},
		"one RequestLock fewer in the log": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.LockPath.Steps = r.OwnerLoss.Takeover.LockPath.Steps[8:]
		},
		"broken without blocklisting": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.LockPath.Steps = slices.DeleteFunc(r.OwnerLoss.Takeover.LockPath.Steps, func(s string) bool { return s == "break.handle_blocklist r=0" })
		},
		"broken before blocklisting": func(r *rbdJournalLostAppendRecord) {
			steps := r.OwnerLoss.Takeover.LockPath.Steps
			i, j := slices.Index(steps, "break.handle_blocklist r=0"), slices.Index(steps, "break.handle_break_lock r=0")
			steps[i], steps[j] = steps[j], steps[i]
		},
		"lock not taken after the break": func(r *rbdJournalLostAppendRecord) {
			steps := r.OwnerLoss.Takeover.LockPath.Steps
			steps[len(steps)-2] = "acquire.handle_lock r=-16"
		},
	} {
		r := rbdJournalOwnerLossSample("secure")
		mutate(r)
		if len(rbdJournalLostAppendClaims(r, "takeover")) == 0 {
			t.Errorf("contradictory owner-loss record accepted: %s", name)
		}
	}
}

// TestRBDJournalForcedBreakClaims feeds the forced-break claims
// contradictory records.
func TestRBDJournalForcedBreakClaims(t *testing.T) {
	n := func(v int) *int { return &v }
	p := func(s string) *string { return &s }
	if problems := rbdJournalLostAppendClaims(rbdJournalForcedBreakSample(), "takeover"); len(problems) != 0 {
		t.Fatal("consistent forced-break record refused", problems)
	}
	for _, stage := range rbdJournalForcedBreakStages[:2] {
		r := rbdJournalForcedBreakSample()
		if len(rbdJournalLostAppendClaims(r, stage)) == 0 {
			t.Fatal("steps beyond the stage accepted", stage)
		}
		r.Order = r.Order[:slices.Index(r.Order, stage)+1]
		delete(r.Views, "takeover")
		r.OwnerLoss.Takeover = nil
		if stage == "observe-appended" {
			delete(r.Views, "force-break")
			delete(r.Views, "observe-fenced")
			r.OwnerLoss.Break, r.OwnerLoss.PingBefore, r.OwnerLoss.Fenced, r.OwnerLoss.Probe, r.OwnerLoss.FencedEpoch = nil, nil, nil, nil, 0
		}
		if problems := rbdJournalLostAppendClaims(r, stage); len(problems) != 0 {
			t.Fatal("consistent partial forced-break record refused", stage, problems)
		}
	}
	if len(rbdJournalLostAppendClaims(rbdJournalForcedBreakSample(), "replay")) == 0 {
		t.Fatal("unknown stage accepted")
	}
	type change = func(*rbdJournalLostAppendRecord)
	for name, mutate := range map[string]change{
		// Before the break.
		"append unanswered": func(r *rbdJournalLostAppendRecord) {
			r.Outcome.Class, r.Outcome.UnknownOutcome, r.Outcome.ReplyTransaction = "uncertain", true, 0
		},
		"append refused":            func(r *rbdJournalLostAppendRecord) { r.Outcome.Class, r.Outcome.ReplyCode = "refused", -2 },
		"append sent twice":         func(r *rbdJournalLostAppendRecord) { r.Outcome.CallerInvocations = 2 },
		"entry absent after append": func(r *rbdJournalLostAppendRecord) { r.Views["observe-appended"].EntrySHA256 = nil },
		"blocklisted before break": func(r *rbdJournalLostAppendRecord) {
			r.Views["observe-appended"].Blocklist = r.Views["observe-fenced"].Blocklist
		},
		"appended without the lock":  func(r *rbdJournalLostAppendRecord) { r.Views["observe-appended"].LockFree = true },
		"subject watch not listed":   func(r *rbdJournalLostAppendRecord) { r.Views["observe-appended"].SubjectWatcher = false },
		"withheld connection":        func(r *rbdJournalLostAppendRecord) { r.Gate.Bound = true },
		"second subject connection":  func(r *rbdJournalLostAppendRecord) { r.Gate.Connections = 2 },
		"image changed before break": func(r *rbdJournalLostAppendRecord) { r.Views["observe-appended"].ImageHeadSHA256 = r.ExpectedHeadAfter },
		// The break of a live owner.
		"no ping before the break":  func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.PingBefore = nil },
		"ping refused before break": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.PingBefore.Class = "refused" },
		"subject watch not listed at the break": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Break.WatchersBefore = r.OwnerLoss.Break.WatchersBefore[1:]
		},
		"owner watch of another cookie at the break": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Break.WatchersBefore[0].Cookie++ },
		"owner watch at another address at the break": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Break.WatchersBefore[0].Address = "10.0.0.9:0/12"
		},
		"broke another owner": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Break.Owners, r.OwnerLoss.Break.LockerAddressBefore = []string{"10.0.0.2:0/2"}, "10.0.0.2:0/2"
		},
		"lock record of another client": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Break.LockerBefore = "client.4300" },
		"lock record of another cookie": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Break.LockerCookieBefore = "auto 1" },
		"break without blocklist": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Break.Blocklist = []rbdLockOwnerLossBlock{}
		},
		"breaker took the lock":       func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Break.IsOwner = true },
		"owner left after the break":  func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Break.OwnersAfter = r.OwnerLoss.Break.Owners },
		"lock held after the break":   func(r *rbdJournalLostAppendRecord) { r.Views["force-break"].LockFree = false },
		"break observation unblocked": func(r *rbdJournalLostAppendRecord) { r.Views["force-break"].Blocklist = []rbdLockOwnerLossBlock{} },
		"journal changed by the break": func(r *rbdJournalLostAppendRecord) {
			r.Views["force-break"].EntrySHA256 = p(strings.Repeat("f", 64))
		},
		// The fenced owner.
		"fenced append applied": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Fenced[1].Class, r.OwnerLoss.Fenced[1].ErrorCode = "applied", 0
		},
		"fenced append uncertain": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Fenced[1].Class, r.OwnerLoss.Fenced[1].UnknownOutcome = "uncertain", true
		},
		"fenced append resent":     func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Fenced[1].Invocations = 2 },
		"unlock refused otherwise": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Fenced[2].ErrorCode = -2 },
		"ping answered after break": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Fenced[0].Class, r.OwnerLoss.Fenced[0].ErrorCode = "applied", 0
		},
		"fenced unlock missing": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Fenced = r.OwnerLoss.Fenced[:2] },
		"extra fenced request": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Fenced = append(r.OwnerLoss.Fenced, r.OwnerLoss.Fenced[1])
		},
		"fenced epoch unknown":        func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.FencedEpoch = 0 },
		"probe in the owned object":   func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Probe.Object = 0 },
		"probe repeats the entry tid": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Probe.EntryTID = 0 },
		"probe at another offset":     func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Probe.Offset = 0 },
		"fenced append in the journal": func(r *rbdJournalLostAppendRecord) {
			v := r.Views["observe-fenced"]
			v.ObjectSizes[1] += 4173
			v.ObjectSHA256[1] = strings.Repeat("f", 64)
		},
		"fenced unlock released":   func(r *rbdJournalLostAppendRecord) { r.Views["observe-fenced"].ImageCommit = r.ExpectedCommitAfter },
		"fenced watch still there": func(r *rbdJournalLostAppendRecord) { r.Views["observe-fenced"].SubjectWatcher = true },
		"fenced lock held":         func(r *rbdJournalLostAppendRecord) { r.Views["observe-fenced"].LockFree = false },
		// The takeover.
		"not blocklisted before the takeover": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.BlocklistBefore = []rbdLockOwnerLossBlock{}
		},
		"fenced entry replayed": func(r *rbdJournalLostAppendRecord) {
			v := r.Views["takeover"]
			v.InspectOwned = append(v.InspectOwned, rbdJournalLostAppendEvent{"AioWrite", 100 << 10, 4 << 10, ""})
			v.InspectTotal = n(8)
		},
		"takeover changed the probe object": func(r *rbdJournalLostAppendRecord) { r.Views["takeover"].ObjectSizes[1] += 10 },
		"takeover without replay":           func(r *rbdJournalLostAppendRecord) { r.Views["takeover"].ImageHeadSHA256 = strings.Repeat("d", 64) },
		"takeover boundary still locked": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.BoundaryLockers = []rbdJournalLockRecord{{"client.4321", r.LockCookie, rbdJournalOwnerLossSampleAddress}}
		},
		"subject watch listed at the takeover boundary": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.BoundaryWatchers = append(r.OwnerLoss.Takeover.BoundaryWatchers, rbdJournalWatch{r.SubjectGlobalID, r.WatchCookie, rbdJournalOwnerLossSampleAddress})
		},
		"takeover boundary without its own watch": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.BoundaryWatchers = []rbdJournalWatch{} },
		"forced-break notifications judged": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.Notes = rbdJournalOwnerLossSample("crc").OwnerLoss.Takeover.Notes
		},
		"order: fenced before the break": func(r *rbdJournalLostAppendRecord) {
			i, j := slices.Index(r.Order, "force-break"), slices.Index(r.Order, "fenced")
			r.Order[i], r.Order[j] = r.Order[j], r.Order[i]
		},
		// librbd's own logs of the break and of the takeover.
		"break lock log missing": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Break.LockPath = nil },
		"break did not see the live owner": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Break.LockPath.Steps = slices.Delete(r.OwnerLoss.Break.LockPath.Steps, 1, 2)
		},
		"break without its blocklist step": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Break.LockPath.Steps = slices.Delete(r.OwnerLoss.Break.LockPath.Steps, 3, 4)
		},
		"break ended with -EAGAIN": func(r *rbdJournalLostAppendRecord) {
			s := r.OwnerLoss.Break.LockPath.Steps
			s[len(s)-1] = "break.finish r=-11"
		},
		"takeover lock log truncated": func(r *rbdJournalLostAppendRecord) { r.OwnerLoss.Takeover.LockPath.Truncated = true },
		"takeover broke a lock": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.LockPath.Steps = rbdJournalOwnerLossLockPath(0)
		},
		"takeover found a locker": func(r *rbdJournalLostAppendRecord) {
			r.OwnerLoss.Takeover.LockPath.Steps[0] = "acquire.handle_get_locker r=0"
		},
	} {
		r := rbdJournalForcedBreakSample()
		mutate(r)
		if len(rbdJournalLostAppendClaims(r, "takeover")) == 0 {
			t.Errorf("contradictory forced-break record accepted: %s", name)
		}
	}
}
