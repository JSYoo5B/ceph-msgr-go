package integration_test

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// rbdLockOwnerLossSample is a consistent record of one run up to the given
// stage, with synthetic values.
func rbdLockOwnerLossSample(stage string) *rbdLockOwnerLossRecord {
	reached := slices.Index(rbdLockOwnerLossStages, stage)
	hash := func(c string) string { return strings.Repeat(c, 64) }
	flag := func(v bool) *bool { return &v }
	z := rbdLockOwnerLossParty{4791, 1, "auto 1", "10.0.0.9:0/11"}
	o := rbdLockOwnerLossParty{4788, 1, "auto 1", "10.0.0.9:0/22"}
	n := rbdLockOwnerLossParty{4809, 7000, "auto 7000", "10.0.0.9:0/33"}
	b := rbdLockOwnerLossParty{4812, 8, "auto 8", "10.0.0.9:0/44"}
	const observed, until = "2026-11-13T01:00:00Z", "2026-11-13T02:00:00Z"
	view := func(label, source string, epoch uint32, owner *rbdLockOwnerLossParty, watchers []rbdLockOwnerLossParty, blocked []string, states ...int) *rbdLockOwnerLossView {
		v := &rbdLockOwnerLossView{Label: label, Source: source, ObservedUTC: observed, HeaderLockers: []rbdLockOwnerLossLocker{}, MapLockers: []rbdLockOwnerLossLocker{},
			Watchers: []rbdLockOwnerLossWatcher{}, Blocklist: []rbdLockOwnerLossBlock{}, MapStates: states, HeaderLockHex: "free", MapLockHex: "free", MapRawHex: fmt.Sprint("map", states)}
		if source == "go" {
			v.Epoch = epoch
		}
		if owner != nil {
			entity := fmt.Sprintf("client.%d", owner.GlobalID)
			v.HeaderLockers = []rbdLockOwnerLossLocker{{entity, owner.LockCookie, owner.Address}}
			v.MapLockers = []rbdLockOwnerLossLocker{{entity, "", owner.Address}}
			v.HeaderLockHex, v.MapLockHex = "header "+entity, "map "+entity
		}
		for _, w := range watchers {
			v.Watchers = append(v.Watchers, rbdLockOwnerLossWatcher{w.GlobalID, w.WatchCookie, w.Address})
		}
		for _, address := range blocked {
			v.Blocklist = append(v.Blocklist, rbdLockOwnerLossBlock{address, until})
		}
		return v
	}
	image := func(v *rbdLockOwnerLossView, head string, objects int) *rbdLockOwnerLossView {
		v.ImageHeadSHA256, v.Objects = head, make([]string, 5)
		for i := 0; i < objects; i++ {
			v.Objects[i] = hash(fmt.Sprint(i))
		}
		return v
	}
	parties := func(p ...rbdLockOwnerLossParty) []rbdLockOwnerLossParty { return p }
	note := func(op uint32, id rbdClientID) rbdLockNotification {
		return rbdLockNotification{Op: op, Client: id, NotifierGlobalID: id.GID, Cookie: o.WatchCookie, Version: 7, Role: "non-owner", AckDisposition: rbdAckSubmittedEmpty}
	}
	r := &rbdLockOwnerLossRecord{WatchTimeoutSeconds: rbdLockOwnerLossWatchTimeout, Zombie: z, Observer: o, Views: map[string]*rbdLockOwnerLossView{},
		ExpectedHead: map[string]string{"setup": hash("a")}, Order: slices.Clone(rbdLockOwnerLossOrder[:slices.Index(rbdLockOwnerLossOrder, rbdLockOwnerLossStageEnd[stage])+1])}
	add := func(views ...*rbdLockOwnerLossView) {
		for _, v := range views {
			r.Views[v.Label] = v
		}
	}
	add(image(view("setup", "native", 0, nil, nil, nil, 1, 0, 0, 0, 0), hash("a"), 1),
		image(view("observe-go-owner", "native", 0, &z, parties(o, z), nil, 1, 0, 0, 0, 0), hash("a"), 1),
		view("go-owner", "go", 24, &z, parties(o, z), nil, 1, 0, 0, 0, 0))
	r.Notifications = []rbdLockNotification{note(rbdNotifyAcquiredLock, z.id())}
	if reached < 1 {
		return r
	}

	r.NativeOwner, r.Breaker.GlobalID = n, b.GlobalID
	r.ExpectedHead["takeover"] = hash("b")
	acquired := view("acquired", "native", 0, &n, parties(o, n), []string{z.Address}, 1, 1, 0, 0, 0)
	acquired.IsOwner, acquired.Owners, acquired.BlocklistBefore = flag(true), []string{n.Address}, []rbdLockOwnerLossBlock{}
	add(view("request-while-listed", "go", 25, &z, parties(o, z, n), nil, 1, 0, 0, 0, 0), acquired,
		view("native-owner", "go", 26, &n, parties(o, n), []string{z.Address}, 1, 1, 0, 0, 0),
		image(view("takeover", "native", 0, &n, parties(o, n), []string{z.Address}, 1, 1, 0, 0, 0), hash("b"), 2))
	for i := uint64(0); i < 3; i++ {
		request := note(rbdNotifyRequestLock, n.id())
		request.NotifyID = 100 + i
		r.Notifications = append(r.Notifications, request)
		r.ZombieEvents = append(r.ZombieEvents, rbdLockOwnerLossEvent{Opcode: cephmsgr.OSDWatchNotification, NotifyID: 100 + i, NotifierGlobalID: n.GlobalID, Op: rbdNotifyRequestLock, Client: n.id()})
	}
	r.Notifications = append(r.Notifications, note(rbdNotifyAcquiredLock, n.id()))
	r.ZombieEvents, r.ZombieWatchEnd = append(r.ZombieEvents, rbdLockOwnerLossEvent{Opcode: cephmsgr.OSDWatchDisconnect}), "disconnected"
	r.ZombieLastTransaction = 9
	for i, label := range rbdLockOwnerLossFenced {
		tid := uint64(10 + i)
		r.Fenced = append(r.Fenced, rbdLockOwnerLossAttempt{Label: label, Invocations: 1, Class: "refused", Code: rbdOSDBlocklisted, ErrorCode: rbdOSDBlocklisted, Error: "refused",
			Submission: cephmsgr.OSDSubmission{Transaction: tid, GlobalID: z.GlobalID, MapEpoch: 26}, ReplyTransaction: tid, ReplyMapEpoch: 26})
	}
	r.NewConnection = &rbdLockOwnerLossAttempt{Label: "new_connection_lock_get_info", Invocations: 1, Class: "applied", Submission: cephmsgr.OSDSubmission{Transaction: 13, GlobalID: z.GlobalID}, ReplyTransaction: 13}
	r.NewConnectionLockers = slices.Clone(r.Views["takeover"].HeaderLockers)
	r.Fresh = &rbdLockOwnerLossFresh{HeaderLockHex: r.Views["takeover"].HeaderLockHex, MapRawHex: r.Views["takeover"].MapRawHex, Objects: slices.Clone(r.Views["takeover"].Objects)}
	exit := -9
	r.TakeoverExit = &exit
	if reached < 2 {
		return r
	}

	r.Breaker = b
	r.ExpectedHead["go-break"] = hash("c")
	dead := rbdLockOwnerLossLocker{"client.4809", n.LockCookie, n.Address}
	r.Break = &rbdLockOwnerLossBreak{Locker: dead, RefreshedLocker: dead, FirstLockCode: rbdLockBusy, ListedPolls: 30, UnlistedPolls: 1, BlocklistAddress: n.Address,
		BlocklistCommand: "osd blocklist add", BlocklistMessage: "blocklisting", BreakRequestEpoch: 27, BreakReplyEpoch: 27, MapFirstLockCode: rbdLockBusy,
		MapLockers: []rbdLockOwnerLossLocker{{"client.4809", "", n.Address}}}
	both := []string{z.Address, n.Address}
	add(view("dead-owner-listed", "go", 26, &n, parties(o, b, n), []string{z.Address}, 1, 1, 0, 0, 0),
		view("dead-owner-unlisted", "go", 26, &n, parties(o, b), []string{z.Address}, 1, 1, 0, 0, 0),
		view("blocklisted", "go", 27, &n, parties(o, b), both, 1, 1, 0, 0, 0),
		view("go-broke", "go", 27, &b, parties(o, b), both, 1, 1, 1, 0, 0),
		image(view("observe-go-broke", "native", 0, &b, parties(o, b), both, 1, 1, 1, 0, 0), hash("c"), 3))
	r.Notifications = append(r.Notifications, note(rbdNotifyAcquiredLock, b.id()))
	if reached < 3 {
		return r
	}

	r.ExpectedHead["final"] = hash("d")
	add(view("released", "go", 27, nil, parties(o), both, 1, 1, 1, 0, 0),
		image(view("final", "native", 0, nil, parties(o), both, 1, 1, 1, 1, 0), hash("d"), 4),
		view("end", "go", 27, nil, nil, both, 1, 1, 1, 1, 0))
	writer, checker := rbdClientID{14696, 9000}, rbdClientID{14710, 9100}
	r.Notifications = append(r.Notifications, note(rbdNotifyReleasedLock, b.id()), note(rbdNotifyAcquiredLock, writer), note(rbdNotifyReleasedLock, writer),
		note(rbdNotifyAcquiredLock, checker), note(rbdNotifyReleasedLock, checker))
	r.Check, r.FinalGoExtentsMatch = &nativeRBDLockCheck{ElapsedNS: 1}, flag(true)
	return r
}

// TestRBDLockOwnerLossClaims feeds the claims contradictory records. Each one
// keeps every other part of the record intact, so a claim that another part
// of the evidence would satisfy is still refused.
func TestRBDLockOwnerLossClaims(t *testing.T) {
	for i, stage := range rbdLockOwnerLossStages {
		if problems := rbdLockOwnerLossClaims(rbdLockOwnerLossSample(stage), stage); len(problems) != 0 {
			t.Fatal("consistent record refused at", stage, problems)
		}
		// A record is judged at the stage it reached: steps beyond the stage
		// and a stage that was not reached are both contradictions.
		for j, other := range rbdLockOwnerLossStages {
			if i != j && len(rbdLockOwnerLossClaims(rbdLockOwnerLossSample(stage), other)) == 0 {
				t.Fatal("record of stage", stage, "accepted at", other)
			}
		}
	}
	if len(rbdLockOwnerLossClaims(rbdLockOwnerLossSample("final"), "released")) == 0 {
		t.Fatal("unknown stage accepted")
	}
	type record = rbdLockOwnerLossRecord
	gone := func(v *rbdLockOwnerLossView, gid uint64) {
		v.Watchers = slices.DeleteFunc(v.Watchers, func(w rbdLockOwnerLossWatcher) bool { return w.GlobalID == gid })
	}
	unblock := func(v *rbdLockOwnerLossView, address string) {
		v.Blocklist = slices.DeleteFunc(v.Blocklist, func(b rbdLockOwnerLossBlock) bool { return b.Address == address })
	}
	cases := map[string]struct {
		stage  string
		change func(*record)
	}{
		// go-owner: the starting point is a live, unfenced Go owner.
		"zombie without the object-map lock": {"go-owner", func(r *record) { r.Views["observe-go-owner"].MapLockers = nil }},
		"zombie lock with another cookie":    {"go-owner", func(r *record) { r.Views["go-owner"].HeaderLockers[0].Cookie = "auto 2" }},
		"zombie watch not listed natively":   {"go-owner", func(r *record) { gone(r.Views["observe-go-owner"], r.Zombie.GlobalID) }},
		"zombie watch at another address": {"go-owner", func(r *record) {
			r.Views["go-owner"].Watchers[1].Address = "10.0.0.9:0/99"
		}},
		"zombie already blocklisted": {"go-owner", func(r *record) {
			r.Views["go-owner"].Blocklist = []rbdLockOwnerLossBlock{{r.Zombie.Address, "2026-11-13T02:00:00Z"}}
			r.Views["observe-go-owner"].Blocklist = nil
		}},
		"Go and native lock bytes differ":       {"go-owner", func(r *record) { r.Views["go-owner"].HeaderLockHex = "other" }},
		"object map already marked":             {"go-owner", func(r *record) { r.Views["observe-go-owner"].MapStates = []int{1, 1, 0, 0, 0} }},
		"image locked at setup":                 {"go-owner", func(r *record) { r.Views["setup"].HeaderLockers = r.Views["go-owner"].HeaderLockers }},
		"zombie and observer share an address":  {"go-owner", func(r *record) { r.Observer.Address = r.Zombie.Address }},
		"lock cookie not from the watch handle": {"go-owner", func(r *record) { r.Zombie.LockCookie = "auto 9" }},
		"observation missing":                   {"go-owner", func(r *record) { delete(r.Views, "go-owner") }},
		"observation under another label":       {"go-owner", func(r *record) { r.Views["go-owner"].Label = "end" }},
		"step out of order":                     {"go-owner", func(r *record) { r.Order[0], r.Order[1] = r.Order[1], r.Order[0] }},
		"no AcquiredLock from the zombie":       {"go-owner", func(r *record) { r.Notifications = nil }},
		"observer answered as an owner": {"go-owner", func(r *record) {
			r.Notifications[0].AckDisposition, r.Notifications[0].AckHex = rbdAckSubmittedResponse, "01"
		}},
		"observer omitted an acknowledgement": {"go-owner", func(r *record) { r.Notifications[0].AckDisposition = rbdAckOmitted }},

		// takeover: liveness rule, break, blocklist and fencing.
		"lock broken while the watch was listed": {"takeover", func(r *record) {
			r.Views["request-while-listed"].HeaderLockers = r.Views["takeover"].HeaderLockers
		}},
		"no request while the watch was listed": {"takeover", func(r *record) { gone(r.Views["request-while-listed"], r.Zombie.GlobalID) }},
		"blocklisted while the watch was listed": {"takeover", func(r *record) {
			r.Views["request-while-listed"].Blocklist = r.Views["takeover"].Blocklist
		}},
		"native never asked": {"takeover", func(r *record) {
			r.Notifications = append(r.Notifications[:1], r.Notifications[4:]...)
			r.ZombieEvents = r.ZombieEvents[3:]
		}},
		"native forced its request":          {"takeover", func(r *record) { r.Notifications[1].Force = true }},
		"native AcquiredLock missing":        {"takeover", func(r *record) { r.Notifications = r.Notifications[:4] }},
		"AcquiredLock of another handle":     {"takeover", func(r *record) { r.Notifications[4].Client.Handle++ }},
		"extra notification":                 {"takeover", func(r *record) { r.Notifications = append(r.Notifications, r.Notifications[4]) }},
		"zombie not blocklisted natively":    {"takeover", func(r *record) { unblock(r.Views["acquired"], r.Zombie.Address) }},
		"zombie not blocklisted in the MON":  {"takeover", func(r *record) { unblock(r.Views["native-owner"], r.Zombie.Address) }},
		"blocklisted before the native open": {"takeover", func(r *record) { r.Views["acquired"].BlocklistBefore = r.Views["acquired"].Blocklist }},
		"another address blocklisted": {"takeover", func(r *record) {
			r.Views["native-owner"].Blocklist[0].Address = "10.0.0.9:0/12"
		}},
		"observer blocklisted too": {"takeover", func(r *record) {
			r.Views["takeover"].Blocklist = append(r.Views["takeover"].Blocklist, rbdLockOwnerLossBlock{r.Observer.Address, "2026-11-13T02:00:00Z"})
		}},
		"blocklist entry already expired":    {"takeover", func(r *record) { r.Views["acquired"].Blocklist[0].Until = "2026-11-13T00:59:59Z" }},
		"blocklist entry without an expiry":  {"takeover", func(r *record) { r.Views["takeover"].Blocklist[0].Until = "" }},
		"zombie watch still listed":          {"takeover", func(r *record) { r.Views["acquired"].Watchers = r.Views["request-while-listed"].Watchers }},
		"zombie keeps the header lock":       {"takeover", func(r *record) { r.Views["native-owner"].HeaderLockers = r.Views["go-owner"].HeaderLockers }},
		"zombie keeps the object-map lock":   {"takeover", func(r *record) { r.Views["acquired"].MapLockers = r.Views["go-owner"].MapLockers }},
		"native does not report ownership":   {"takeover", func(r *record) { *r.Views["acquired"].IsOwner = false }},
		"native reports another owner":       {"takeover", func(r *record) { r.Views["acquired"].Owners = []string{r.Zombie.Address} }},
		"native write not in the object map": {"takeover", func(r *record) { r.Views["acquired"].MapStates = []int{1, 0, 0, 0, 0} }},
		"zombie sent a request while silent": {"takeover", func(r *record) { r.ZombieLastTransaction = 8 }},
		"zombie sent a request between its attempts": {"takeover", func(r *record) {
			r.Fenced[2].Submission.Transaction, r.Fenced[2].ReplyTransaction, r.NewConnection.Submission.Transaction = 13, 13, 14
		}},
		"zombie's last request unknown":        {"takeover", func(r *record) { r.ZombieLastTransaction = 0 }},
		"new connection after another request": {"takeover", func(r *record) { r.NewConnection.Submission.Transaction = 14 }},
		"zombie watch not ended by the OSD":    {"takeover", func(r *record) { r.ZombieWatchEnd = "context deadline exceeded" }},
		"zombie watch without a DISCONNECT":    {"takeover", func(r *record) { r.ZombieEvents = r.ZombieEvents[:3] }},
		"zombie received no request":           {"takeover", func(r *record) { r.ZombieEvents = r.ZombieEvents[3:] }},
		"zombie received a request nobody saw": {"takeover", func(r *record) {
			r.ZombieEvents[0].NotifyID = 999
		}},
		"zombie received more than the observer": {"takeover", func(r *record) {
			r.ZombieEvents = append([]rbdLockOwnerLossEvent{r.ZombieEvents[0]}, r.ZombieEvents...)
		}},
		"fenced update applied": {"takeover", func(r *record) {
			r.Fenced[0].Class, r.Fenced[0].Code, r.Fenced[0].ErrorCode = "applied", 0, 0
		}},
		"fenced write applied and seen natively": {"takeover", func(r *record) {
			r.Fenced[1].Class, r.Fenced[1].Code, r.Fenced[1].ErrorCode = "applied", 0, 0
			r.Views["takeover"].Objects[4] = strings.Repeat("f", 64)
		}},
		"fenced write refused but seen natively": {"takeover", func(r *record) { r.Views["takeover"].Objects[4] = strings.Repeat("f", 64) }},
		"object map changed after the attempts":  {"takeover", func(r *record) { r.Views["takeover"].MapRawHex = "map [1 1 0 0 1]" }},
		"image changed after the attempts":       {"takeover", func(r *record) { r.Views["takeover"].ImageHeadSHA256 = strings.Repeat("f", 64) }},
		"image flagged after the takeover":       {"takeover", func(r *record) { r.Views["takeover"].Flags = 1 }},
		"fenced with another code": {"takeover", func(r *record) {
			r.Fenced[2].Code, r.Fenced[2].ErrorCode = -2, -2
		}},
		"fenced outcome unknown": {"takeover", func(r *record) {
			r.Fenced[0].Class, r.Fenced[0].UnknownOutcome, r.Fenced[0].Code, r.Fenced[0].ErrorCode, r.Fenced[0].ReplyTransaction = "uncertain", true, 0, 0, 0
		}},
		"fenced attempt repeated":           {"takeover", func(r *record) { r.Fenced[1].Invocations = 2 }},
		"fenced attempt listed twice":       {"takeover", func(r *record) { r.Fenced = append(r.Fenced, r.Fenced[1]) }},
		"fenced attempt missing":            {"takeover", func(r *record) { r.Fenced = r.Fenced[:2] }},
		"fenced attempt never sent":         {"takeover", func(r *record) { r.Fenced[0].Submission.Transaction, r.Fenced[0].ReplyTransaction = 0, 0 }},
		"fenced attempt of another client":  {"takeover", func(r *record) { r.Fenced[0].Submission.GlobalID = r.Observer.GlobalID }},
		"new connection refused":            {"takeover", func(r *record) { r.NewConnection.Class, r.NewConnection.Code = "refused", rbdOSDBlocklisted }},
		"new connection not recorded":       {"takeover", func(r *record) { r.NewConnection = nil }},
		"new connection read another owner": {"takeover", func(r *record) { r.NewConnectionLockers = r.Views["go-owner"].HeaderLockers }},
		"fresh client is an old client":     {"takeover", func(r *record) { r.Breaker.GlobalID = r.Zombie.GlobalID }},
		"fresh client read another lock":    {"takeover", func(r *record) { r.Fresh.HeaderLockHex = r.Views["go-owner"].HeaderLockHex }},
		"fresh client read another object":  {"takeover", func(r *record) { r.Fresh.Objects[1] = "" }},
		"fresh read missing":                {"takeover", func(r *record) { r.Fresh = nil }},
		"native owner exited by itself":     {"takeover", func(r *record) { *r.TakeoverExit = 0 }},
		"native owner exit unknown":         {"takeover", func(r *record) { r.TakeoverExit = nil }},
		"native owner is the zombie":        {"takeover", func(r *record) { r.NativeOwner.GlobalID = r.Zombie.GlobalID }},

		// go-break: librbd's order on the Go side.
		"lock taken at the first attempt":       {"go-break", func(r *record) { r.Break.FirstLockCode = 0 }},
		"dead owner's watch never seen listed":  {"go-break", func(r *record) { r.Break.ListedPolls = 0 }},
		"dead owner's watch not listed at kill": {"go-break", func(r *record) { gone(r.Views["dead-owner-listed"], r.NativeOwner.GlobalID) }},
		"broken while the watch was listed": {"go-break", func(r *record) {
			r.Views["dead-owner-unlisted"].Watchers = r.Views["dead-owner-listed"].Watchers
		}},
		"watch never seen unlisted":           {"go-break", func(r *record) { r.Break.UnlistedPolls = 0 }},
		"locker changed before the break":     {"go-break", func(r *record) { r.Break.RefreshedLocker.Cookie = "auto 7001" }},
		"locker is not the killed owner":      {"go-break", func(r *record) { r.Break.Locker.Entity, r.Break.RefreshedLocker.Entity = "client.1", "client.1" }},
		"lock gone before the break":          {"go-break", func(r *record) { r.Views["dead-owner-unlisted"].HeaderLockers = nil }},
		"blocklisted before the watch lapsed": {"go-break", func(r *record) { r.Views["dead-owner-unlisted"].Blocklist = r.Views["blocklisted"].Blocklist }},
		"break without a blocklist entry":     {"go-break", func(r *record) { unblock(r.Views["blocklisted"], r.NativeOwner.Address) }},
		"another address blocklisted by Go":   {"go-break", func(r *record) { r.Break.BlocklistAddress = r.Zombie.Address }},
		"blocklist command refused":           {"go-break", func(r *record) { r.Break.BlocklistCode = -13 }},
		"blocklist without a new map epoch":   {"go-break", func(r *record) { r.Views["blocklisted"].Epoch = 26 }},
		"lock broken before the blocklist":    {"go-break", func(r *record) { r.Views["blocklisted"].HeaderLockers = r.Views["go-broke"].HeaderLockers }},
		"break_lock at an older map epoch":    {"go-break", func(r *record) { r.Break.BreakRequestEpoch = 26 }},
		"break_lock answered at an older map": {"go-break", func(r *record) { r.Break.BreakReplyEpoch = 26 }},
		"break_lock refused":                  {"go-break", func(r *record) { r.Break.BreakCode = -2 }},
		"lock refused after the break":        {"go-break", func(r *record) { r.Break.LockCode = rbdLockBusy }},
		"object-map lock was free":            {"go-break", func(r *record) { r.Break.MapFirstLockCode = 0 }},
		"object-map locker is not the owner":  {"go-break", func(r *record) { r.Break.MapLockers[0].Entity = "client.1" }},
		"object-map break refused":            {"go-break", func(r *record) { r.Break.MapBreakCode = -2 }},
		"object-map lock refused":             {"go-break", func(r *record) { r.Break.MapLockCode = rbdLockBusy }},
		"break record missing":                {"go-break", func(r *record) { r.Break = nil }},
		"break steps out of order": {"go-break", func(r *record) {
			i, j := slices.Index(r.Order, "blocklist_add"), slices.Index(r.Order, "break_lock")
			r.Order[i], r.Order[j] = r.Order[j], r.Order[i]
		}},
		"breaker does not hold the lock natively":   {"go-break", func(r *record) { r.Views["observe-go-broke"].HeaderLockers = r.Views["takeover"].HeaderLockers }},
		"killed owner keeps the object-map lock":    {"go-break", func(r *record) { r.Views["go-broke"].MapLockers = r.Views["takeover"].MapLockers }},
		"killed owner's watch listed after":         {"go-break", func(r *record) { r.Views["observe-go-broke"].Watchers = r.Views["dead-owner-listed"].Watchers }},
		"Go write not in the object map":            {"go-break", func(r *record) { r.Views["observe-go-broke"].MapStates = []int{1, 1, 0, 0, 0} }},
		"Go write not in the native image":          {"go-break", func(r *record) { r.Views["observe-go-broke"].ImageHeadSHA256 = r.ExpectedHead["takeover"] }},
		"Go data object absent natively":            {"go-break", func(r *record) { r.Views["observe-go-broke"].Objects[2] = "" }},
		"native and Go object maps differ":          {"go-break", func(r *record) { r.Views["go-broke"].MapRawHex = "other" }},
		"zombie entry gone from the blocklist":      {"go-break", func(r *record) { unblock(r.Views["observe-go-broke"], r.Zombie.Address) }},
		"killed owner not blocklisted natively":     {"go-break", func(r *record) { unblock(r.Views["observe-go-broke"], r.NativeOwner.Address) }},
		"breaker blocklisted":                       {"go-break", func(r *record) { r.Views["go-broke"].Blocklist[0].Address = r.Breaker.Address }},
		"breaker's AcquiredLock missing":            {"go-break", func(r *record) { r.Notifications = r.Notifications[:5] }},
		"breaker took the lock at another address":  {"go-break", func(r *record) { r.Breaker.Address = r.NativeOwner.Address }},
		"breaker's cookie not from its watch":       {"go-break", func(r *record) { r.Breaker.LockCookie = "auto 1" }},
		"expected image of the Go write is missing": {"go-break", func(r *record) { delete(r.ExpectedHead, "go-break") }},

		// final: release, native writer, check and the whole history.
		"lock left after the release":        {"final", func(r *record) { r.Views["released"].HeaderLockers = r.Views["go-broke"].HeaderLockers }},
		"breaker's watch left":               {"final", func(r *record) { r.Views["released"].Watchers = r.Views["go-broke"].Watchers }},
		"lock left at the end":               {"final", func(r *record) { r.Views["end"].MapLockers = r.Views["go-broke"].MapLockers }},
		"observer's watch left at the end":   {"final", func(r *record) { r.Views["end"].Watchers = r.Views["final"].Watchers }},
		"native writer left its lock":        {"final", func(r *record) { r.Views["final"].HeaderLockers = r.Views["takeover"].HeaderLockers }},
		"final write not in the object map":  {"final", func(r *record) { r.Views["final"].MapStates = []int{1, 1, 1, 0, 0} }},
		"fenced object exists at the end":    {"final", func(r *record) { r.Views["final"].Objects[4] = strings.Repeat("f", 64) }},
		"fenced object marked at the end":    {"final", func(r *record) { r.Views["end"].MapStates = []int{1, 1, 1, 1, 1} }},
		"object map changed after the check": {"final", func(r *record) { r.Views["end"].MapRawHex = "other" }},
		"final image differs":                {"final", func(r *record) { r.Views["final"].ImageHeadSHA256 = r.ExpectedHead["go-break"] }},
		"object map invalidated":             {"final", func(r *record) { r.Views["final"].Flags = 1 }},
		"check found an error":               {"final", func(r *record) { r.Check.ReturnCode = 1 }},
		"check printed an error":             {"final", func(r *record) { r.Check.Stderr = "object map error" }},
		"check did not run":                  {"final", func(r *record) { r.Check = nil }},
		"owner read the Go extent as zeros":  {"final", func(r *record) { *r.FinalGoExtentsMatch = false }},
		"blocklist entry removed too early":  {"final", func(r *record) { unblock(r.Views["final"], r.Zombie.Address) }},
		"breaker's ReleasedLock missing":     {"final", func(r *record) { r.Notifications = slices.Delete(r.Notifications, 6, 7) }},
		"check ran as the writer": {"final", func(r *record) {
			r.Notifications[9].Client, r.Notifications[10].Client = r.Notifications[7].Client, r.Notifications[7].Client
		}},
		"writer never released": {"final", func(r *record) { r.Notifications[8].Op = rbdNotifyAcquiredLock }},
		"killed owner announced a lock": {"final", func(r *record) {
			r.Notifications[7].Client, r.Notifications[8].Client = r.NativeOwner.id(), r.NativeOwner.id()
		}},
		"a RequestLock after the break":       {"final", func(r *record) { r.Notifications = append(r.Notifications, r.Notifications[1]) }},
		"unexpected notification":             {"final", func(r *record) { r.Notifications[9].Unexpected = true }},
		"acknowledgement failed":              {"final", func(r *record) { r.Notifications[10].AckError = "refused" }},
		"observer watch failed":               {"final", func(r *record) { r.ObserverFailure = "header watch ended" }},
		"observer ping failed":                {"final", func(r *record) { r.ObserverPingErrors = []string{"ceph: OSD result code -107"} }},
		"zombie attempted again later":        {"final", func(r *record) { r.Fenced = append(r.Fenced, r.Fenced[0]) }},
		"takeover claim broken by the end":    {"final", func(r *record) { r.Fenced[1].Class, r.Fenced[1].Code, r.Fenced[1].ErrorCode = "applied", 0, 0 }},
		"go-owner claim broken by the end":    {"final", func(r *record) { r.Views["go-owner"].MapLockers = nil }},
		"go-break claim broken by the end":    {"final", func(r *record) { r.Break.BreakRequestEpoch = 26 }},
		"native view is a Go view":            {"final", func(r *record) { r.Views["final"].Source = "go" }},
		"view without a source":               {"final", func(r *record) { r.Views["released"].Source = "" }},
		"watch timeout not recorded":          {"final", func(r *record) { r.WatchTimeoutSeconds = 0 }},
		"expected final image equals nothing": {"final", func(r *record) { r.ExpectedHead["final"] = "" }},
	}
	for name, c := range cases {
		r := rbdLockOwnerLossSample(c.stage)
		c.change(r)
		if len(rbdLockOwnerLossClaims(r, c.stage)) == 0 {
			t.Error("contradictory record accepted:", name)
		}
		// A contradiction of an earlier stage is still one when the run went
		// on: the same change is refused in a complete record.
		if c.stage != "final" {
			r = rbdLockOwnerLossSample("final")
			c.change(r)
			if len(rbdLockOwnerLossClaims(r, "final")) == 0 {
				t.Error("contradictory record accepted in a complete run:", name)
			}
		}
	}
	t.Logf("%d contradictory records refused", len(cases))
}

// TestRBDLockOwnerLossTestOnlyEncodings checks the test-only break_lock input
// and the projections of native and Go observations against fixed bytes. The
// lock.get_info vector is the output a live OSD returned for librbd's managed
// lock held by client.4809.
// The breaker checks its target before the MON blocklist command and before
// each break_lock. A lock that changed to another single owner during the
// polling keeps every length check satisfied, so these guards are what stops
// the sequence before either mutation.
func TestRBDLockOwnerLossBreakTargets(t *testing.T) {
	owner := rbdLockOwnerLossParty{GlobalID: 4809, WatchCookie: 7000, LockCookie: "auto 7000", Address: "192.0.2.1:0/11"}
	dead := rbdLockOwnerLossLocker{"client.4809", "auto 7000", "192.0.2.1:0/11"}
	replacement := rbdLockOwnerLossLocker{"client.4900", "auto 7100", "192.0.2.1:0/12"}
	if err := rbdLockOwnerLossBreakTarget(owner, dead, dead); err != nil {
		t.Fatal("the killed owner's unchanged record is refused:", err)
	}
	for name, c := range map[string]struct {
		owner            rbdLockOwnerLossParty
		first, refreshed rbdLockOwnerLossLocker
	}{
		"owner changed during the polling":   {owner, dead, replacement},
		"another owner from the start":       {owner, replacement, replacement},
		"first read is another owner":        {owner, replacement, dead},
		"same client, new cookie":            {owner, dead, rbdLockOwnerLossLocker{dead.Entity, "auto 7001", dead.Address}},
		"same client, new connection":        {owner, dead, rbdLockOwnerLossLocker{dead.Entity, dead.Cookie, "192.0.2.1:0/13"}},
		"same address, another client":       {owner, dead, rbdLockOwnerLossLocker{"client.4900", dead.Cookie, dead.Address}},
		"lock record gone":                   {owner, dead, rbdLockOwnerLossLocker{}},
		"killed owner unknown":               {rbdLockOwnerLossParty{}, rbdLockOwnerLossLocker{Entity: "client.0"}, rbdLockOwnerLossLocker{Entity: "client.0"}},
		"killed owner without an address":    {rbdLockOwnerLossParty{GlobalID: 4809, LockCookie: "auto 7000"}, rbdLockOwnerLossLocker{"client.4809", "auto 7000", ""}, rbdLockOwnerLossLocker{"client.4809", "auto 7000", ""}},
		"killed owner without a lock cookie": {rbdLockOwnerLossParty{GlobalID: 4809, Address: dead.Address}, rbdLockOwnerLossLocker{"client.4809", "", dead.Address}, rbdLockOwnerLossLocker{"client.4809", "", dead.Address}},
	} {
		if err := rbdLockOwnerLossBreakTarget(c.owner, c.first, c.refreshed); err == nil {
			t.Errorf("%s: the breaker would blocklist and break", name)
		}
	}
	deadMap := rbdLockOwnerLossLocker{"client.4809", "", "192.0.2.1:0/11"}
	if err := rbdLockOwnerLossMapBreakTarget(owner, []rbdLockOwnerLossLocker{deadMap}); err != nil {
		t.Fatal("the killed owner's object-map record is refused:", err)
	}
	for name, lockers := range map[string][]rbdLockOwnerLossLocker{
		"replacement owner":            {{"client.4900", "", "192.0.2.1:0/12"}},
		"same client, new connection":  {{"client.4809", "", "192.0.2.1:0/13"}},
		"same address, another client": {{"client.4900", "", deadMap.Address}},
		"header cookie on the map":     {{"client.4809", "auto 7000", deadMap.Address}},
		"no locker":                    {},
		"dead owner and a replacement": {deadMap, {"client.4900", "", "192.0.2.1:0/12"}},
		"dead owner twice":             {deadMap, deadMap},
	} {
		if err := rbdLockOwnerLossMapBreakTarget(owner, lockers); err == nil {
			t.Errorf("%s: the breaker would break the object-map lock", name)
		}
	}
	if err := rbdLockOwnerLossMapBreakTarget(rbdLockOwnerLossParty{}, []rbdLockOwnerLossLocker{{Entity: "client.0"}}); err == nil {
		t.Error("an unknown killed owner: the breaker would break the object-map lock")
	}
}

func TestRBDLockOwnerLossTestOnlyEncodings(t *testing.T) {
	input, err := rbdLockBreakInput(rbdLockOwnerLossLocker{Entity: "client.4809", Cookie: "auto 7000"})
	want := "0101" + "22000000" + "08000000" + hex.EncodeToString([]byte("rbd_lock")) + "08" + "c912000000000000" + "09000000" + hex.EncodeToString([]byte("auto 7000"))
	if err != nil || hex.EncodeToString(input) != want {
		t.Fatal("cls_lock_break_op encoding differs", hex.EncodeToString(input), err)
	}
	// librbd's object-map lock has an empty cookie.
	if input, err = rbdLockBreakInput(rbdLockOwnerLossLocker{Entity: "client.1"}); err != nil ||
		hex.EncodeToString(input) != "0101"+"19000000"+"08000000"+hex.EncodeToString([]byte("rbd_lock"))+"08"+"0100000000000000"+"00000000" {
		t.Fatal("cls_lock_break_op encoding with an empty cookie differs", hex.EncodeToString(input), err)
	}
	for _, entity := range []string{"", "client.", "client.x", "osd.3", "client.07", "client.5 ", "mds.client.5"} {
		if _, err := rbdLockBreakInput(rbdLockOwnerLossLocker{Entity: entity}); err == nil {
			t.Fatal("locker entity admitted", entity)
		}
	}
	for value, want := range map[string]string{
		"2026-11-13T02:21:50.084618+0000": "2026-11-13T02:21:50.084618Z",
		"2026-11-13T11:21:50.084618+0900": "2026-11-13T02:21:50.084618Z",
		"2026-11-13T02:21:50Z":            "2026-11-13T02:21:50Z",
	} {
		if got, err := rbdLockOwnerLossUntil(value); err != nil || got != want {
			t.Fatal("blocklist expiry normalization differs", value, got, err)
		}
	}
	for _, value := range []string{"", "never", "2026-11-13 02:21:50"} {
		if _, err := rbdLockOwnerLossUntil(value); err == nil {
			t.Fatal("blocklist expiry admitted", value)
		}
	}
	// cls_lock_get_info_reply with one exclusive locker tagged "internal".
	info := "01016d000000" + "01000000" + "010121000000" + "08" + "c912000000000000" + "14000000" + hex.EncodeToString([]byte("auto 128328656781904")) +
		"01012f000000" + "0000000000000000" + "01" + "01011c000000" + "01000000" + "74f8ff85" + "10000000" + "02000000ac1200090000000000000000" + "00000000" +
		"01" + "08000000" + hex.EncodeToString([]byte("internal"))
	raw, _ := hex.DecodeString(info)
	lockers, err := rbdLockOwnerLossLockers(raw, rbdLockTag)
	if err != nil || !slices.Equal(lockers, []rbdLockOwnerLossLocker{{"client.4809", "auto 128328656781904", "172.18.0.9:0/2248145012"}}) {
		t.Fatal("locker projection differs", lockers, err)
	}
	// The object-map lock has no tag; the same bytes are not one.
	if _, err := rbdLockOwnerLossLockers(raw, ""); err == nil {
		t.Fatal("locker with another tag admitted")
	}
	free, _ := hex.DecodeString("01011100000000000000" + "01" + "08000000696e7465726e616c")
	if lockers, err := rbdLockOwnerLossLockers(free, rbdLockTag); err != nil || len(lockers) != 0 {
		t.Fatal("free lock projection differs", lockers, err)
	}
	if _, err := rbdLockOwnerLossLockers(raw[:len(raw)-3], rbdLockTag); err == nil {
		t.Fatal("truncated lock info admitted")
	}
	// A native view is refused when librbd's locker list and the class
	// output name different lockers, or when a part is missing.
	call := func(output string) nativeRBDWriteClassCall {
		return nativeRBDWriteClassCall{Class: "lock", Method: "get_info", OutputHex: output}
	}
	objectMap := nativeRBDLockRaw{Exists: true, Hex: "0e000000" + "0101" + "08000000" + "0500000000000000" + "5000" + "0c000000" + "18459682" + "01000000" + "8537d2c0"}
	listed := nativeRBDWriteLockers{Tag: rbdLockTag, Exclusive: true}
	listed.Lockers = append(listed.Lockers, struct {
		Client  string `json:"client"`
		Cookie  string `json:"cookie"`
		Address string `json:"addr"`
	}{"client.4809", "auto 128328656781904", "172.18.0.9:0/2248145012"})
	blocks := []nativeRBDLockBlock{{"172.19.0.9:0/7", "2026-11-13T02:21:50.084618+0000"}}
	watchers := []nativeRBDWatcher{{Address: "172.18.0.9:0/2248145012", ID: 4809, Cookie: 128328656781904}}
	mapFree := "01010900000000000000" + "01" + "00000000"
	v, err := rbdLockOwnerLossNativeView("x", "2026-11-13T01:00:00Z", call(info), call(mapFree), objectMap, watchers, listed, &blocks, 0)
	if err != nil || !slices.Equal(v.HeaderLockers, lockers) || len(v.MapLockers) != 0 || !slices.Equal(v.MapStates, []int{1, 1, 0, 0, 0}) ||
		!slices.Equal(v.Watchers, []rbdLockOwnerLossWatcher{{4809, 128328656781904, "172.18.0.9:0/2248145012"}}) ||
		!slices.Equal(v.Blocklist, []rbdLockOwnerLossBlock{{"172.19.0.9:0/7", "2026-11-13T02:21:50.084618Z"}}) || v.Source != "native" {
		t.Fatalf("native view projection differs: %+v %v", v, err)
	}
	other := listed
	other.Lockers = slices.Clone(listed.Lockers)
	other.Lockers[0].Cookie = "auto 1"
	failed := call(info)
	failed.Code = -2
	for name, build := range map[string]func() (*rbdLockOwnerLossView, error){
		"librbd lists another cookie": func() (*rbdLockOwnerLossView, error) {
			return rbdLockOwnerLossNativeView("x", "", call(info), call(mapFree), objectMap, watchers, other, &blocks, 0)
		},
		"librbd lists no locker": func() (*rbdLockOwnerLossView, error) {
			return rbdLockOwnerLossNativeView("x", "", call(info), call(mapFree), objectMap, watchers, nativeRBDWriteLockers{Empty: true}, &blocks, 0)
		},
		"exec failed": func() (*rbdLockOwnerLossView, error) {
			return rbdLockOwnerLossNativeView("x", "", failed, call(mapFree), objectMap, watchers, listed, &blocks, 0)
		},
		"blocklist missing": func() (*rbdLockOwnerLossView, error) {
			return rbdLockOwnerLossNativeView("x", "", call(info), call(mapFree), objectMap, watchers, listed, nil, 0)
		},
		"object map absent": func() (*rbdLockOwnerLossView, error) {
			return rbdLockOwnerLossNativeView("x", "", call(info), call(mapFree), nativeRBDLockRaw{}, watchers, listed, &blocks, 0)
		},
		"blocklist expiry unreadable": func() (*rbdLockOwnerLossView, error) {
			return rbdLockOwnerLossNativeView("x", "", call(info), call(mapFree), objectMap, watchers, listed, &[]nativeRBDLockBlock{{"a", "soon"}}, 0)
		},
	} {
		if _, err := build(); err == nil {
			t.Fatal("native view admitted:", name)
		}
	}
}
