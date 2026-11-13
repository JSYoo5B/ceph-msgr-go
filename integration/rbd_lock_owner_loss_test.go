package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephRBDLockOwnerLossIntegration hands an image's exclusive lock over
// twice without a release, once in each direction, on a fresh image of the RBD
// fixture with exclusive-lock, object-map and fast-diff.
//
// First a Go client (the zombie) takes the managed lock as librbd does and
// then stops renewing its header watch: it sends nothing, but keeps its
// connection and releases nothing. Native librbd, asked to write, requests
// the lock from that owner while its watch is still listed, and once the OSD
// has expired the watch it blocklists the owner's address through the MON,
// breaks the header and object-map locks and writes. The zombie then tries
// three mutations on its lock-bearing connection; each must come back as the
// OSD's raw blocklisted error and change nothing native clients read.
//
// Then that native owner is killed while it holds the lock. A Go client
// dialed after the first handoff (the breaker) repeats librbd's break
// sequence with existing primitives: it reads the locker, finds the lock
// busy, waits until the dead owner's watch is no longer listed, blocklists
// the locker's address with a MON command, breaks the header and object-map
// locks, takes them, updates the object map and writes. A fresh native open
// finally writes, closes and runs `rbd object-map check`.
//
// A third Go client (the observer) keeps a header watch through the whole
// sequence, acknowledges every notification as a non-owner and supplies the
// Go-side reads. Native librbd, librados, `osd blocklist ls` and `rbd
// object-map check` are the independent oracles. Every lock, notify and
// object-map encoding is the test-only consumer code of rbd_lock_test.go. The
// product is unchanged. See
// integration/fixture/RBD_LOCK_OWNER_LOSS_VALIDATION.md.

const (
	// rbdLockOwnerLossWatchTimeout is the header watch timeout of every Go
	// watch here, in seconds: the OSD's default, which librbd's watch uses.
	rbdLockOwnerLossWatchTimeout = 30
	// rbdOSDBlocklisted is -EBLOCKLISTED (ESHUTDOWN), the OSD's answer to an
	// operation from a blocklisted address.
	rbdOSDBlocklisted = -108
	rbdLockBusy       = -16
)

// rbdLockOwnerLossStages are the points at which the record is judged. Each
// stage includes the earlier ones.
var rbdLockOwnerLossStages = []string{"go-owner", "takeover", "go-break", "final"}

// rbdLockOwnerLossOrder is the order in which the steps must complete, and
// rbdLockOwnerLossStageEnd the last step of each stage.
var (
	rbdLockOwnerLossOrder = []string{
		"setup", "zombie_lock", "observe-go-owner",
		"request_while_listed", "native_acquired", "fenced_attempts", "zombie_watch_end", "new_connection", "fresh_read", "takeover",
		"dead_owner_listed", "dead_owner_unlisted", "blocklist_add", "break_lock", "go_lock", "map_break_lock", "go_write", "observe-go-broke",
		"go_release", "final", "observer_unwatch"}
	rbdLockOwnerLossStageEnd = map[string]string{"go-owner": "observe-go-owner", "takeover": "takeover", "go-break": "observe-go-broke", "final": "observer_unwatch"}
)

// rbdLockOwnerLossFenced are the mutations the zombie attempts after the
// first handoff, each once.
var rbdLockOwnerLossFenced = []string{"object_map_update", "data_write", "lock_unlock"}

// rbdLockOwnerLossParty is one client of the sequence: its GlobalID, its
// header watch handle, the lock cookie librbd derives from that handle and
// the legacy address (ip:port/nonce) of the connection that carries both.
type rbdLockOwnerLossParty struct {
	GlobalID    uint64
	WatchCookie uint64
	LockCookie  string
	Address     string
}

func (p rbdLockOwnerLossParty) id() rbdClientID { return rbdClientID{p.GlobalID, p.WatchCookie} }

type rbdLockOwnerLossLocker struct{ Entity, Cookie, Address string }

type rbdLockOwnerLossWatcher struct {
	GlobalID, Cookie uint64
	Address          string
}

type rbdLockOwnerLossBlock struct{ Address, Until string }

// rbdLockOwnerLossView is one observation of the image's lock state. A
// native view comes from the driver's output (librados exec and reads,
// librbd's watcher list, `osd blocklist ls`); a Go view from the observer
// client's requests and its MON `osd dump`. Lockers are decoded from the raw
// `lock.get_info` output in both. Until and ObservedUTC are RFC 3339 UTC.
type rbdLockOwnerLossView struct {
	Label, Source string
	ObservedUTC   string
	// Epoch is the OSDMap epoch of the blocklist read (Go views only).
	Epoch                     uint32
	HeaderLockers, MapLockers []rbdLockOwnerLossLocker
	HeaderLockHex, MapLockHex string
	Watchers                  []rbdLockOwnerLossWatcher
	Blocklist                 []rbdLockOwnerLossBlock
	MapRawHex                 string
	MapStates                 []int
	// Native views only. Objects holds the SHA-256 of each data object,
	// empty for an absent one; IsOwner, Owners and BlocklistBefore belong to
	// the read-write instance of the takeover.
	Objects         []string
	ImageHeadSHA256 string
	Flags           uint64
	IsOwner         *bool
	Owners          []string
	BlocklistBefore []rbdLockOwnerLossBlock
}

// rbdLockOwnerLossEvent is one event the zombie's watch had queued when it
// was read after the handoff. The zombie sent nothing while they arrived.
type rbdLockOwnerLossEvent struct {
	Opcode                     uint8
	NotifyID, NotifierGlobalID uint64
	Op                         uint32
	Client                     rbdClientID
	Force                      bool
	PayloadHex                 string
}

// rbdLockOwnerLossAttempt is one request whose raw outcome is kept instead of
// being required: what the caller sent and what it was told.
type rbdLockOwnerLossAttempt struct {
	Label            string
	Invocations      int
	Class            string
	Code             int32
	ErrorCode        int32
	UnknownOutcome   bool
	Error            string
	Submission       cephmsgr.OSDSubmission
	ReplyTransaction uint64
	ReplyMapEpoch    uint32
	OperationResults []int32
}

// rbdLockOwnerLossFresh is what the breaker read right after it was dialed.
type rbdLockOwnerLossFresh struct {
	HeaderLockHex, MapRawHex string
	Objects                  []string
}

// rbdLockOwnerLossBreak is the breaker's raw record of librbd's break
// sequence against the killed native owner.
type rbdLockOwnerLossBreak struct {
	Locker, RefreshedLocker rbdLockOwnerLossLocker
	FirstLockCode           int32
	ListedPolls             int
	UnlistedPolls           int
	BlocklistCommand        string
	BlocklistAddress        string
	BlocklistCode           int32
	BlocklistMessage        string
	BreakRequestEpoch       uint32
	BreakReplyEpoch         uint32
	BreakCode               int32
	LockCode                int32
	MapFirstLockCode        int32
	MapLockers              []rbdLockOwnerLossLocker
	MapBreakCode            int32
	MapLockCode             int32
}

// rbdLockOwnerLossBreakTarget says why the breaker must not go on to the MON
// blocklist command and break_lock: the locker it read first, or the one it
// read again once the watch was gone, is not the killed owner's record. It is
// checked before either mutation, so a lock that changed hands during the
// polling stops the sequence instead of fencing or breaking the new owner.
func rbdLockOwnerLossBreakTarget(owner rbdLockOwnerLossParty, first, refreshed rbdLockOwnerLossLocker) error {
	dead := rbdLockOwnerLossLocker{fmt.Sprintf("client.%d", owner.GlobalID), owner.LockCookie, owner.Address}
	if owner.GlobalID == 0 || owner.LockCookie == "" || owner.Address == "" {
		return fmt.Errorf("the killed owner's identity is incomplete: %+v", owner)
	}
	if first != dead {
		return fmt.Errorf("the locker read first is not the killed owner: %+v, want %+v", first, dead)
	}
	if refreshed != dead {
		return fmt.Errorf("the locker read again is not the killed owner: %+v, want %+v", refreshed, dead)
	}
	return nil
}

// rbdLockOwnerLossMapBreakTarget is the same check for the object map, whose
// lock librbd takes with an empty cookie: break_lock is built only from the
// killed owner's sole record.
func rbdLockOwnerLossMapBreakTarget(owner rbdLockOwnerLossParty, lockers []rbdLockOwnerLossLocker) error {
	dead := rbdLockOwnerLossLocker{fmt.Sprintf("client.%d", owner.GlobalID), "", owner.Address}
	if owner.GlobalID == 0 || owner.Address == "" {
		return fmt.Errorf("the killed owner's identity is incomplete: %+v", owner)
	}
	if len(lockers) != 1 || lockers[0] != dead {
		return fmt.Errorf("the object map is not locked by exactly the killed owner: %+v, want %+v", lockers, dead)
	}
	return nil
}

type rbdLockOwnerLossRecord struct {
	Token, Pool                            string
	PoolID                                 int64
	GoIdentity, ConnectionMode             string
	Image, ImageID                         string
	WatchTimeoutSeconds                    uint32
	Zombie, Observer, Breaker, NativeOwner rbdLockOwnerLossParty
	Order                                  []string
	Views                                  map[string]*rbdLockOwnerLossView
	Notifications                          []rbdLockNotification
	ObserverFailure                        string
	ObserverPings                          int
	ObserverPingErrors                     []string
	BreakerNotifications                   []rbdLockNotification
	ZombieEvents                           []rbdLockOwnerLossEvent
	ZombieWatchEnd                         string
	// ZombieLastTransaction is the transaction of the last request the
	// zombie sent before it went silent. Transactions are allocated per
	// client, so the next ones show whether it sent anything meanwhile.
	ZombieLastTransaction                     uint64
	Fenced                                    []rbdLockOwnerLossAttempt
	NewConnection                             *rbdLockOwnerLossAttempt
	NewConnectionLockers                      []rbdLockOwnerLossLocker
	Fresh                                     *rbdLockOwnerLossFresh
	TakeoverExit                              *int
	Break                                     *rbdLockOwnerLossBreak
	ExpectedHead                              map[string]string
	Check                                     *nativeRBDLockCheck
	FinalGoExtentsMatch                       *bool
	Requests                                  []rbdWriteRequestEvidence
	GoNotifies                                []*rbdLockGoNotify
	NativeActions                             []rbdNativeActionEvidence
	NativeStderr                              map[string]string
	TakeoverWriteNS                           int64
	WatchRegisteredToAcquired, KillToUnlisted string
	// How long after their observation the two blocklist entries expire.
	ZombieBlockSeconds, NativeOwnerBlockSeconds float64
	ClaimsStage                                 string
	Problems                                    []string
	Timings                                     map[string]string
}

// rbdLockOwnerLossClaims checks the record up to one stage and returns one
// line per contradiction. It reads nothing but the record.
//
//   - go-owner: the zombie holds the header and object-map locks with a
//     listed watch, natively and from the observer, and is not blocklisted.
//   - takeover: native librbd asked for the lock while the watch was listed
//     and nothing was broken then; afterwards the zombie's watch is gone,
//     its address is the one new blocklist entry and native librbd owns both
//     locks; every mutation the zombie then attempted was refused with the
//     blocklisted code and changed nothing in the next native observation;
//     the driver ended by SIGKILL.
//   - go-break: the breaker saw the lock busy and the dead owner's watch
//     listed, broke nothing until it was unlisted, blocklisted the locker's
//     address before break_lock and sent break_lock at a map epoch that
//     holds the entry; then it owns both locks natively and from Go.
//   - final: the breaker released, a fresh native open wrote and closed,
//     `rbd object-map check` found nothing, and the observer saw exactly the
//     notifications of that history.
func rbdLockOwnerLossClaims(r *rbdLockOwnerLossRecord, stage string) []string {
	var problems []string
	fail := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	text := func(v any) string { data, _ := json.Marshal(v); return string(data) }
	reached := slices.Index(rbdLockOwnerLossStages, stage)
	if reached < 0 {
		return []string{"unknown stage " + stage}
	}
	if want := rbdLockOwnerLossOrder[:slices.Index(rbdLockOwnerLossOrder, rbdLockOwnerLossStageEnd[stage])+1]; !slices.Equal(r.Order, want) {
		fail("order: steps completed as %v, want %v", r.Order, want)
	}
	needed := [][]string{
		{"setup", "observe-go-owner", "go-owner"},
		{"request-while-listed", "acquired", "native-owner", "takeover"},
		{"dead-owner-listed", "dead-owner-unlisted", "blocklisted", "go-broke", "observe-go-broke"},
		{"released", "final", "end"}}
	for _, names := range needed[:reached+1] {
		for _, name := range names {
			if v := r.Views[name]; v == nil || v.Label != name || (v.Source != "native" && v.Source != "go") {
				fail("%s: observation missing", name)
			}
		}
	}
	if len(problems) != 0 {
		return problems
	}
	view := func(name string) *rbdLockOwnerLossView { return r.Views[name] }
	native := func(name string) *rbdLockOwnerLossView {
		v := view(name)
		if v.Source != "native" {
			fail("%s: not a native observation", name)
		}
		return v
	}
	entity := func(p rbdLockOwnerLossParty) string { return fmt.Sprintf("client.%d", p.GlobalID) }
	heldBy := func(name string, p rbdLockOwnerLossParty) {
		v := view(name)
		if !slices.Equal(v.HeaderLockers, []rbdLockOwnerLossLocker{{entity(p), p.LockCookie, p.Address}}) ||
			!slices.Equal(v.MapLockers, []rbdLockOwnerLossLocker{{entity(p), "", p.Address}}) {
			fail("%s: the header and object-map locks are not held by client.%d (%s) at %s: header %s, object map %s", name, p.GlobalID, p.LockCookie, p.Address, text(v.HeaderLockers), text(v.MapLockers))
		}
	}
	headerHeldBy := func(name string, p rbdLockOwnerLossParty) {
		if v := view(name); !slices.Equal(v.HeaderLockers, []rbdLockOwnerLossLocker{{entity(p), p.LockCookie, p.Address}}) {
			fail("%s: the header lock is not held by client.%d (%s) at %s: %s", name, p.GlobalID, p.LockCookie, p.Address, text(v.HeaderLockers))
		}
	}
	unlocked := func(name string) {
		if v := view(name); len(v.HeaderLockers) != 0 || len(v.MapLockers) != 0 {
			fail("%s: a lock is left: header %s, object map %s", name, text(v.HeaderLockers), text(v.MapLockers))
		}
	}
	watching := func(v *rbdLockOwnerLossView, p rbdLockOwnerLossParty) bool {
		return slices.Contains(v.Watchers, rbdLockOwnerLossWatcher{p.GlobalID, p.WatchCookie, p.Address})
	}
	// A watcher of the same client and cookie counts as that party's watch
	// whatever address it shows.
	listed := func(v *rbdLockOwnerLossView, p rbdLockOwnerLossParty) bool {
		return slices.ContainsFunc(v.Watchers, func(w rbdLockOwnerLossWatcher) bool { return w.GlobalID == p.GlobalID && w.Cookie == p.WatchCookie })
	}
	watchers := func(name string, present []rbdLockOwnerLossParty, absent ...rbdLockOwnerLossParty) {
		v := view(name)
		for _, p := range present {
			if !watching(v, p) {
				fail("%s: the watch of client.%d (cookie %d, %s) is not listed: %s", name, p.GlobalID, p.WatchCookie, p.Address, text(v.Watchers))
			}
		}
		for _, p := range absent {
			if listed(v, p) {
				fail("%s: the watch of client.%d (cookie %d) is still listed: %s", name, p.GlobalID, p.WatchCookie, text(v.Watchers))
			}
		}
		if len(v.Watchers) != len(present) {
			fail("%s: %d watchers listed, want %d: %s", name, len(v.Watchers), len(present), text(v.Watchers))
		}
	}
	// The blocklist must hold exactly the given addresses beyond what it
	// held before the sequence, each with an expiry after the observation.
	baseline := map[string]bool{}
	for _, name := range []string{"observe-go-owner", "go-owner"} {
		for _, b := range view(name).Blocklist {
			baseline[b.Address] = true
		}
	}
	blocked := func(name string, entries []rbdLockOwnerLossBlock, observed string, want ...string) {
		var added []string
		at, atErr := time.Parse(time.RFC3339Nano, observed)
		for _, b := range entries {
			if baseline[b.Address] {
				continue
			}
			added = append(added, b.Address)
			until, err := time.Parse(time.RFC3339Nano, b.Until)
			if err != nil || atErr != nil || !until.After(at) {
				fail("%s: blocklist entry %s does not expire after its observation (until %q, observed %q)", name, b.Address, b.Until, observed)
			}
		}
		slices.Sort(added)
		want = slices.Clone(want)
		slices.Sort(want)
		if !slices.Equal(added, want) {
			fail("%s: new blocklist entries are %v, want %v", name, added, want)
		}
	}
	states := func(name string, want ...int) {
		if v := view(name); !slices.Equal(v.MapStates, want) {
			fail("%s: object map states %v, want %v", name, v.MapStates, want)
		}
	}
	sameBytes := func(nativeName, goName string) {
		n, g := native(nativeName), view(goName)
		if n.HeaderLockHex == "" || n.HeaderLockHex != g.HeaderLockHex || n.MapLockHex != g.MapLockHex || n.MapRawHex == "" || n.MapRawHex != g.MapRawHex {
			fail("%s and %s: lock.get_info output or object-map bytes differ between native librados and Go", nativeName, goName)
		}
	}
	image := func(name, expected string, exists ...bool) {
		v := native(name)
		got := make([]bool, len(v.Objects))
		for i, sum := range v.Objects {
			got[i] = sum != ""
		}
		if want := r.ExpectedHead[expected]; want == "" || v.ImageHeadSHA256 != want || !slices.Equal(got, exists) || v.Flags != 0 {
			fail("%s: native image differs from the %s model (head %s, objects present %v, flags %d)", name, expected, v.ImageHeadSHA256, got, v.Flags)
		}
	}

	z, o := r.Zombie, r.Observer
	if z.GlobalID == 0 || o.GlobalID == 0 || z.GlobalID == o.GlobalID || z.WatchCookie == 0 || o.WatchCookie == 0 || z.LockCookie != rbdLockCookie(z.WatchCookie) ||
		z.Address == "" || o.Address == "" || z.Address == o.Address || r.WatchTimeoutSeconds == 0 {
		fail("identity: zombie or observer identity incomplete or shared: %s %s", text(z), text(o))
	}

	// go-owner.
	unlocked("setup")
	watchers("setup", nil)
	states("setup", 1, 0, 0, 0, 0)
	for _, name := range []string{"observe-go-owner", "go-owner"} {
		heldBy(name, z)
		watchers(name, []rbdLockOwnerLossParty{z, o})
		states(name, 1, 0, 0, 0, 0)
		blocked(name, view(name).Blocklist, view(name).ObservedUTC)
	}
	if baseline[z.Address] || baseline[o.Address] {
		fail("go-owner: the zombie or the observer is blocklisted before the sequence: %v", baseline)
	}
	sameBytes("observe-go-owner", "go-owner")
	notes := r.Notifications
	note := func(i int, op uint32, id rbdClientID) bool {
		return i < len(notes) && notes[i].Op == op && notes[i].Client == id && notes[i].NotifierGlobalID == id.GID && !notes[i].Force
	}
	for i, n := range notes {
		if n.Unexpected || n.AckError != "" || n.AckDisposition != rbdAckSubmittedEmpty || n.AckHex != "" || n.Role != "non-owner" || n.Cookie != o.WatchCookie || n.DecodeError != "" {
			fail("notifications: the observer did not acknowledge notification %d empty as a non-owner: %s", i, text(n))
		}
	}
	if !note(0, rbdNotifyAcquiredLock, z.id()) {
		fail("notifications: the first notification is not the zombie's AcquiredLock: %s", text(notes))
	}
	if reached < 1 {
		if len(notes) != 1 {
			fail("notifications: %d notifications before the takeover, want 1", len(notes))
		}
		return problems
	}

	// takeover.
	n := r.NativeOwner
	if n.GlobalID == 0 || n.GlobalID == z.GlobalID || n.GlobalID == o.GlobalID || n.WatchCookie == 0 || n.LockCookie != rbdLockCookie(n.WatchCookie) || n.Address == "" || n.Address == z.Address {
		fail("identity: native owner identity incomplete or shared: %s", text(n))
	}
	headerHeldBy("request-while-listed", z)
	if v := view("request-while-listed"); !watching(v, z) {
		fail("request-while-listed: the zombie's watch was not listed when native librbd asked for the lock: %s", text(v.Watchers))
	}
	blocked("request-while-listed", view("request-while-listed").Blocklist, view("request-while-listed").ObservedUTC)
	requests := 0
	for note(1+requests, rbdNotifyRequestLock, n.id()) {
		requests++
	}
	if requests < 1 || !note(1+requests, rbdNotifyAcquiredLock, n.id()) {
		fail("notifications: the zombie's AcquiredLock is not followed by at least one native RequestLock and then the native AcquiredLock (%d requests): %s", requests, text(notes))
	}
	acquired := native("acquired")
	if acquired.IsOwner == nil || !*acquired.IsOwner || !slices.Equal(acquired.Owners, []string{n.Address}) {
		fail("acquired: native librbd does not report itself as the lock owner at %s: owner %s, owners %v", n.Address, text(acquired.IsOwner), acquired.Owners)
	}
	blocked("acquired (before the open)", acquired.BlocklistBefore, view("observe-go-owner").ObservedUTC)
	for _, name := range []string{"acquired", "native-owner", "takeover"} {
		heldBy(name, n)
		watchers(name, []rbdLockOwnerLossParty{n, o}, z)
		states(name, 1, 1, 0, 0, 0)
		blocked(name, view(name).Blocklist, view(name).ObservedUTC, z.Address)
	}
	sameBytes("acquired", "native-owner")
	// The zombie's watch: every queued event is a native RequestLock it never
	// acknowledged, and the OSD ended the watch itself.
	events := r.ZombieEvents
	asked := map[uint64]bool{}
	for _, e := range notes[min(len(notes), 1):min(len(notes), 1+requests)] {
		asked[e.NotifyID] = true
	}
	if len(events) < 2 || events[len(events)-1].Opcode != cephmsgr.OSDWatchDisconnect || r.ZombieWatchEnd != "disconnected" || len(events)-1 > requests {
		fail("zombie watch: not at least one native RequestLock followed by the OSD's DISCONNECT (%d events, ended %q, observer saw %d requests)", len(events), r.ZombieWatchEnd, requests)
	}
	for i, e := range events[:max(len(events)-1, 0)] {
		if e.Opcode != cephmsgr.OSDWatchNotification || e.Op != rbdNotifyRequestLock || e.Client != n.id() || e.NotifierGlobalID != n.GlobalID || e.Force || !asked[e.NotifyID] {
			fail("zombie watch: event %d is not one of the native RequestLocks the observer saw: %s", i, text(e))
		}
	}
	// Fencing: each attempt once, each refused with the blocklisted code.
	// Their transactions follow the zombie's last request before its silence
	// without a gap, so it sent no ping, acknowledgement or release meanwhile
	// and nothing between the attempts.
	labels := []string{}
	for i, a := range r.Fenced {
		labels = append(labels, a.Label)
		if a.Invocations != 1 || a.Class != "refused" || a.Code != rbdOSDBlocklisted || a.ErrorCode != rbdOSDBlocklisted || a.UnknownOutcome || a.Submission.Transaction == 0 ||
			a.Submission.GlobalID != z.GlobalID || a.ReplyTransaction != a.Submission.Transaction {
			fail("fencing: the zombie's %s was not sent once and refused with %d: %s", a.Label, rbdOSDBlocklisted, text(a))
		}
		if r.ZombieLastTransaction == 0 || a.Submission.Transaction != r.ZombieLastTransaction+1+uint64(i) {
			fail("fencing: the zombie's %s has transaction %d, want %d directly after its last request before the silence (%d)", a.Label, a.Submission.Transaction, r.ZombieLastTransaction+1+uint64(i), r.ZombieLastTransaction)
		}
	}
	if !slices.Equal(labels, rbdLockOwnerLossFenced) {
		fail("fencing: attempts are %v, want %v", labels, rbdLockOwnerLossFenced)
	}
	if view("takeover").MapRawHex != acquired.MapRawHex || view("takeover").HeaderLockHex != acquired.HeaderLockHex || view("takeover").MapLockHex != acquired.MapLockHex {
		fail("takeover: the object map or a lock changed between the native acquisition and the observation after the zombie's attempts")
	}
	image("takeover", "takeover", true, true, false, false, false)
	// The blocklist entry names the address of the connection that took the
	// lock. Another connection of the same client has another address.
	if a := r.NewConnection; a == nil || a.Invocations != 1 || a.Class != "applied" || a.Code != 0 || a.Submission.GlobalID != z.GlobalID ||
		a.Submission.Transaction != r.ZombieLastTransaction+1+uint64(len(r.Fenced)) ||
		!slices.Equal(r.NewConnectionLockers, []rbdLockOwnerLossLocker{{entity(n), n.LockCookie, n.Address}}) {
		fail("new connection: the zombie client's read on a new connection did not return the native owner: %s %s", text(a), text(r.NewConnectionLockers))
	}
	b := r.Breaker
	if b.GlobalID == 0 || b.GlobalID == z.GlobalID || b.GlobalID == o.GlobalID || b.GlobalID == n.GlobalID {
		fail("identity: the breaker is not a new client: %s", text(b))
	}
	if f, v := r.Fresh, view("takeover"); f == nil || f.HeaderLockHex != v.HeaderLockHex || f.MapRawHex != v.MapRawHex || !slices.Equal(f.Objects, v.Objects) {
		fail("fresh read: a new Go client does not read the native lock, object map and data objects: %s", text(f))
	}
	if r.TakeoverExit == nil || *r.TakeoverExit != -9 {
		fail("takeover: the native owner did not end by SIGKILL: exit %s", text(r.TakeoverExit))
	}
	if reached < 2 {
		if len(notes) != 2+requests {
			fail("notifications: %d notifications after the takeover, want %d", len(notes), 2+requests)
		}
		return problems
	}

	// go-break.
	if b.WatchCookie == 0 || b.LockCookie != rbdLockCookie(b.WatchCookie) || b.Address == "" || b.Address == z.Address || b.Address == n.Address {
		fail("identity: breaker identity incomplete or shared: %s", text(b))
	}
	k := r.Break
	if k == nil {
		fail("break: record missing")
		k = &rbdLockOwnerLossBreak{}
	}
	dead := rbdLockOwnerLossLocker{entity(n), n.LockCookie, n.Address}
	heldBy("dead-owner-listed", n)
	if v := view("dead-owner-listed"); !watching(v, n) {
		fail("dead-owner-listed: the killed owner's watch was not listed right after the kill: %s", text(v.Watchers))
	}
	if k.Locker != dead || k.FirstLockCode != rbdLockBusy || k.ListedPolls < 1 {
		fail("break: the breaker did not read the killed owner as the locker, find the lock busy and see its watch listed: %s", text(k))
	}
	heldBy("dead-owner-unlisted", n)
	watchers("dead-owner-unlisted", []rbdLockOwnerLossParty{o, b}, n, z)
	blocked("dead-owner-unlisted", view("dead-owner-unlisted").Blocklist, view("dead-owner-unlisted").ObservedUTC, z.Address)
	if k.RefreshedLocker != dead || k.UnlistedPolls < 1 {
		fail("break: the locker was not read again unchanged once its watch was gone: %s", text(k))
	}
	// Blocklist first, then break_lock at an epoch that holds the entry.
	heldBy("blocklisted", n)
	blocked("blocklisted", view("blocklisted").Blocklist, view("blocklisted").ObservedUTC, z.Address, n.Address)
	if k.BlocklistAddress != n.Address || k.BlocklistCode != 0 || view("blocklisted").Epoch <= view("dead-owner-unlisted").Epoch {
		fail("break: the locker's address was not blocklisted through the MON in a new OSDMap epoch: %s (epochs %d, %d)", text(k), view("dead-owner-unlisted").Epoch, view("blocklisted").Epoch)
	}
	if k.BreakCode != 0 || k.BreakRequestEpoch < view("blocklisted").Epoch || k.BreakReplyEpoch < view("blocklisted").Epoch || k.LockCode != 0 {
		fail("break: break_lock and lock were not answered 0 at an epoch that holds the blocklist entry (%d): %s", view("blocklisted").Epoch, text(k))
	}
	if k.MapFirstLockCode != rbdLockBusy || !slices.Equal(k.MapLockers, []rbdLockOwnerLossLocker{{entity(n), "", n.Address}}) || k.MapBreakCode != 0 || k.MapLockCode != 0 {
		fail("break: the object-map lock of the killed owner was not found busy, broken and taken: %s", text(k))
	}
	for _, name := range []string{"go-broke", "observe-go-broke"} {
		heldBy(name, b)
		watchers(name, []rbdLockOwnerLossParty{o, b}, n, z)
		states(name, 1, 1, 1, 0, 0)
		blocked(name, view(name).Blocklist, view(name).ObservedUTC, z.Address, n.Address)
	}
	sameBytes("observe-go-broke", "go-broke")
	image("observe-go-broke", "go-break", true, true, true, false, false)
	if !note(2+requests, rbdNotifyAcquiredLock, b.id()) {
		fail("notifications: the native AcquiredLock is not followed by the breaker's: %s", text(notes))
	}
	if reached < 3 {
		if len(notes) != 3+requests {
			fail("notifications: %d notifications after the Go break, want %d", len(notes), 3+requests)
		}
		return problems
	}

	// final.
	unlocked("released")
	watchers("released", []rbdLockOwnerLossParty{o}, b, n, z)
	unlocked("final")
	watchers("final", []rbdLockOwnerLossParty{o}, b, n, z)
	states("final", 1, 1, 1, 1, 0)
	blocked("final", view("final").Blocklist, view("final").ObservedUTC, z.Address, n.Address)
	image("final", "final", true, true, true, true, false)
	unlocked("end")
	watchers("end", nil, o, b, n, z)
	states("end", 1, 1, 1, 1, 0)
	if view("end").MapRawHex != view("final").MapRawHex {
		fail("end: the object map changed after the final native observation")
	}
	if c := r.Check; c == nil || c.ReturnCode != 0 || c.Stdout != "" || c.Stderr != "" {
		fail("final: rbd object-map check did not end with 0 and no output: %s", text(c))
	}
	if r.FinalGoExtentsMatch == nil || !*r.FinalGoExtentsMatch {
		fail("final: the native owner did not read the breaker's extent through its object map")
	}
	// The observer's whole history: the breaker's release, then the final
	// native writer and the object-map check, each acquiring and releasing
	// as a client of its own.
	at := 3 + requests
	tail := notes[min(at, len(notes)):]
	if len(tail) != 5 || !note(at, rbdNotifyReleasedLock, b.id()) ||
		tail[1].Op != rbdNotifyAcquiredLock || tail[2].Op != rbdNotifyReleasedLock || tail[1].Client != tail[2].Client || tail[1].NotifierGlobalID != tail[1].Client.GID ||
		tail[3].Op != rbdNotifyAcquiredLock || tail[4].Op != rbdNotifyReleasedLock || tail[3].Client != tail[4].Client || tail[3].NotifierGlobalID != tail[3].Client.GID ||
		tail[1].Client == tail[3].Client || slices.Contains([]uint64{z.GlobalID, o.GlobalID, b.GlobalID, n.GlobalID}, tail[1].Client.GID) ||
		slices.Contains([]uint64{z.GlobalID, o.GlobalID, b.GlobalID, n.GlobalID}, tail[3].Client.GID) {
		fail("notifications: the Go break is not followed by the breaker's ReleasedLock and two native acquire and release pairs: %s", text(tail))
	}
	if r.ObserverFailure != "" || len(r.ObserverPingErrors) != 0 {
		fail("observer: its watch failed: %q, ping errors %v", r.ObserverFailure, r.ObserverPingErrors)
	}
	if len(r.Fenced) != len(rbdLockOwnerLossFenced) {
		fail("fencing: %d zombie attempts by the end, want %d", len(r.Fenced), len(rbdLockOwnerLossFenced))
	}
	return problems
}

// rbdLockBreakInput is cls_lock_break_op: ENCODE_START(1, 1), the lock name,
// the locker's entity_name_t (u8 type and s64 number) and its cookie.
func rbdLockBreakInput(locker rbdLockOwnerLossLocker) ([]byte, error) {
	var number int64
	if n, err := fmt.Sscanf(locker.Entity, "client.%d", &number); n != 1 || err != nil || locker.Entity != fmt.Sprintf("client.%d", number) {
		return nil, fmt.Errorf("locker entity %q is not a client", locker.Entity)
	}
	p := append(rbdString(rbdLockName), rbdEntityClient)
	return rbdEnvelope(append(append(p, rbdLE64(uint64(number))...), rbdString(locker.Cookie)...)), nil
}

// rbdLockOwnerLossUntil normalizes a blocklist expiry as the MON prints it.
func rbdLockOwnerLossUntil(value string) (string, error) {
	for _, layout := range []string{"2006-01-02T15:04:05.000000-0700", time.RFC3339Nano} {
		if t, err := time.Parse(layout, value); err == nil {
			return t.UTC().Format(time.RFC3339Nano), nil
		}
	}
	return "", fmt.Errorf("blocklist expiry %q does not parse", value)
}

// rbdLockOwnerLossLockers projects a raw lock.get_info output.
func rbdLockOwnerLossLockers(output []byte, tag string) ([]rbdLockOwnerLossLocker, error) {
	info, err := decodeRBDLockInfo(output)
	if err != nil {
		return nil, err
	}
	lockers := []rbdLockOwnerLossLocker{}
	for _, l := range info.Lockers {
		if l.NameType != rbdEntityClient || info.Type != rbdLockExclusive || info.Tag != tag || l.Address.Type != rbdAddrTypeLegacy || l.ExpirationSeconds != 0 || l.ExpirationNanos != 0 || l.Description != "" {
			return nil, fmt.Errorf("locker is not a managed exclusive lock with tag %q: %+v", tag, l)
		}
		lockers = append(lockers, rbdLockOwnerLossLocker{fmt.Sprintf("client.%d", l.NameNumber), l.Cookie, fmt.Sprintf("%s/%d", l.Address.Endpoint, l.Address.Nonce)})
	}
	return lockers, nil
}

// rbdLockOwnerLossNativeView projects the lock state one native observation
// or takeover phase showed.
func rbdLockOwnerLossNativeView(label, observed string, headerLock, mapLock nativeRBDWriteClassCall, mapRaw nativeRBDLockRaw, native []nativeRBDWatcher, listed nativeRBDWriteLockers,
	blocklist *[]nativeRBDLockBlock, flags uint64) (*rbdLockOwnerLossView, error) {
	v := &rbdLockOwnerLossView{Label: label, Source: "native", ObservedUTC: observed, HeaderLockHex: headerLock.OutputHex, MapLockHex: mapLock.OutputHex, MapRawHex: mapRaw.Hex,
		Watchers: []rbdLockOwnerLossWatcher{}, Blocklist: []rbdLockOwnerLossBlock{}, Flags: flags}
	for i, call := range []nativeRBDWriteClassCall{headerLock, mapLock} {
		output, err := hex.DecodeString(call.OutputHex)
		if err != nil || call.Code != 0 || call.Class != "lock" || call.Method != "get_info" {
			return nil, fmt.Errorf("%s: native lock.get_info failed: %+v", label, call)
		}
		lockers, err := rbdLockOwnerLossLockers(output, []string{rbdLockTag, ""}[i])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", label, err)
		}
		if i == 0 {
			v.HeaderLockers = lockers
		} else {
			v.MapLockers = lockers
		}
	}
	// librbd's own locker list must name the same header lockers.
	if listed.Empty != (len(v.HeaderLockers) == 0) || len(listed.Lockers) != len(v.HeaderLockers) {
		return nil, fmt.Errorf("%s: librbd list_lockers differs from lock.get_info", label)
	}
	for i, l := range listed.Lockers {
		if (rbdLockOwnerLossLocker{l.Client, l.Cookie, l.Address}) != v.HeaderLockers[i] {
			return nil, fmt.Errorf("%s: librbd list_lockers differs from lock.get_info: %+v", label, l)
		}
	}
	raw, err := hex.DecodeString(mapRaw.Hex)
	if err != nil || !mapRaw.Exists {
		return nil, fmt.Errorf("%s: native object-map read missing", label)
	}
	m, err := decodeRBDObjectMap(raw)
	if err != nil || !m.CRC {
		return nil, fmt.Errorf("%s: native object map does not decode: %v", label, err)
	}
	for _, s := range m.States {
		v.MapStates = append(v.MapStates, int(s))
	}
	for _, w := range native {
		v.Watchers = append(v.Watchers, rbdLockOwnerLossWatcher{w.ID, w.Cookie, w.Address})
	}
	if blocklist == nil {
		return nil, fmt.Errorf("%s: native blocklist missing", label)
	}
	if v.Blocklist, err = rbdLockOwnerLossBlocks(*blocklist); err != nil {
		return nil, fmt.Errorf("%s: %w", label, err)
	}
	return v, nil
}

func rbdLockOwnerLossBlocks(entries []nativeRBDLockBlock) ([]rbdLockOwnerLossBlock, error) {
	blocks := []rbdLockOwnerLossBlock{}
	for _, e := range entries {
		until, err := rbdLockOwnerLossUntil(e.Until)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, rbdLockOwnerLossBlock{e.Address, until})
	}
	return blocks, nil
}

// runRBDLockResponder answers every notification on w through r until ctx
// ends, and pings the watch every rbdLockPingInterval it holds.
func runRBDLockResponder(ctx context.Context, w *cephmsgr.OSDWatch, epoch uint32, self rbdClientID, r *rbdLockResponder, started time.Time) {
	lastPing := time.Now()
	for {
		if time.Since(lastPing) >= rbdLockPingInterval {
			_, err := w.Ping(ctx, epoch)
			if ctx.Err() != nil {
				return
			}
			lastPing = time.Now()
			r.mu.Lock()
			r.pings++
			if err != nil {
				r.pingErrors = append(r.pingErrors, err.Error())
			}
			r.mu.Unlock()
		}
		wait, stop := context.WithDeadline(ctx, lastPing.Add(rbdLockPingInterval))
		event, err := w.Next(wait)
		stop()
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			r.fail(fmt.Errorf("header watch ended: %w", err))
			return
		}
		r.handle(ctx, w, epoch, self, started, event)
	}
}

// waitRBDLockNotification waits until r recorded a notification from index
// from on that matches, a responder failure, or the bound.
func waitRBDLockNotification(ctx context.Context, r *rbdLockResponder, from int, bound time.Duration, match func(rbdLockNotification) bool) (rbdLockNotification, error) {
	deadline := time.After(bound)
	for {
		r.mu.Lock()
		for _, n := range r.events[min(from, len(r.events)):] {
			if match(n) {
				r.mu.Unlock()
				return n, nil
			}
		}
		changed, failure := r.changed, r.failure
		r.mu.Unlock()
		if failure != nil {
			return rbdLockNotification{}, failure
		}
		select {
		case <-changed:
		case <-deadline:
			return rbdLockNotification{}, fmt.Errorf("not received within %v", bound)
		case <-ctx.Done():
			return rbdLockNotification{}, ctx.Err()
		}
	}
}

func TestCephRBDLockOwnerLossIntegration(t *testing.T) {
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if os.Getenv("CEPH_MSGR_TEST_RBD") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || dir == "" {
		t.Skip("requires the opt-in disposable RBD fixture (CEPH_MSGR_TEST_RBD=1) under its exact selector")
	}
	var oracle nativeRBDOracle
	if err := readMDSFreshJSON(filepath.Join(dir, "rbd-base-oracle.json"), &oracle); err != nil {
		t.Fatal(err)
	}
	token, identity := oracle.Token, os.Getenv("CEPH_MSGR_RBD_BASE_IDENTITY")
	if oracle.Source != mdsFreshSource || len(token) != 32 || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.Plan.Pool != "ceph-msgr-rbd-"+token || oracle.Pool.Name != oracle.Plan.Pool ||
		oracle.Pool.Flags&1 != 0 || identity != "client.rbd-base-"+token || oracle.ClientIdentity != identity || oracle.Caps["mon"] != "profile rbd" || oracle.Caps["osd"] != "profile rbd pool="+oracle.Plan.Pool {
		t.Fatal("native RBD oracle scope differs", oracle.Source, oracle.Pool, identity)
	}
	options := integrationOptions(t)
	encoded, err := os.ReadFile(os.Getenv("CEPH_MSGR_RBD_BASE_KEY_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	private, err := cephx.ParseKey(strings.TrimSpace(string(encoded)))
	wantType, profileErr := mdsFreshExpectedKeyType(os.Getenv("CEPH_MSGR_TEST_KEY_TYPE"))
	if err != nil || profileErr != nil || private.Type() != wantType {
		t.Fatal("RBD credential does not match the selected key profile")
	}
	if options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(encoded))); err != nil {
		t.Fatal(err)
	}
	clear(encoded)
	options.Identity, options.EnableOSD = identity, true
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	started := time.Now()
	mode := "secure"
	if options.ConnectionMode == cephmsgr.CRCMode {
		mode = "crc"
	}

	record := rbdLockOwnerLossRecord{Token: token, Pool: oracle.Plan.Pool, PoolID: oracle.Pool.ID, GoIdentity: identity, ConnectionMode: mode, WatchTimeoutSeconds: rbdLockOwnerLossWatchTimeout,
		Views: map[string]*rbdLockOwnerLossView{}, ExpectedHead: map[string]string{}, NativeStderr: map[string]string{}, Timings: map[string]string{}}
	var observerResponder, breakerResponder *rbdLockResponder
	snapshot := func() {
		if observerResponder != nil {
			var failure error
			record.Notifications, failure = observerResponder.snapshot()
			record.ObserverFailure = ""
			if failure != nil {
				record.ObserverFailure = failure.Error()
			}
			observerResponder.mu.Lock()
			record.ObserverPings, record.ObserverPingErrors = observerResponder.pings, slices.Clone(observerResponder.pingErrors)
			observerResponder.mu.Unlock()
		}
		if breakerResponder != nil {
			record.BreakerNotifications, _ = breakerResponder.snapshot()
		}
	}
	defer func() {
		// A failed run keeps whatever it observed, with the last judged stage.
		snapshot()
		record.Timings["total"] = time.Since(started).String()
		data, err := json.Marshal(record)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "rbd-lock-owner-loss-results.json"), data)
		}
		if err != nil {
			t.Error("RBD lock-owner-loss evidence", err)
		}
	}()
	done := func(step string) { record.Order = append(record.Order, step) }
	// judge stops the run at the first stage whose claims do not hold, so a
	// contradiction is never followed by the next step of a handoff.
	judge := func(t *testing.T, stage string) {
		t.Helper()
		snapshot()
		record.ClaimsStage, record.Problems = stage, rbdLockOwnerLossClaims(&record, stage)
		if len(record.Problems) != 0 {
			t.Fatalf("claims at %s:\n%s", stage, strings.Join(record.Problems, "\n"))
		}
	}
	digest := func(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

	// Three Go clients of the same identity. Each has its own GlobalID and,
	// per OSD connection, its own address nonce.
	type party struct {
		role   string
		client *cephmsgr.Client
		gid    uint64
		conn   *cephmsgr.OSDConnection
	}
	dial := func(t *testing.T, role string) *party {
		t.Helper()
		c, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal("authenticate the", role, "Go client", err)
		}
		t.Cleanup(func() { c.Close() })
		p := &party{role: role, client: c, gid: c.Snapshot().GlobalID}
		if c.Snapshot().OSDTicket.Expires.IsZero() || p.gid == 0 {
			t.Fatal(role, "client identity or OSD ticket missing")
		}
		return p
	}
	observer, zombie := dial(t, "observer"), dial(t, "zombie")
	record.Observer.GlobalID, record.Zombie.GlobalID = observer.gid, zombie.gid

	// Native control: one closed action at a time, each journaled by the
	// fixture and answered with its own receipt.
	var plan nativeRBDLockPlan
	readNative := func(t *testing.T, action string, exit int) (nativeRBDLockResult, *nativeRBDLock) {
		t.Helper()
		var result nativeRBDLockResult
		if err := readMDSFreshJSON(filepath.Join(dir, "rbd-lock-"+action+".json"), &result); err != nil {
			t.Fatal(err)
		}
		n := result.Native
		if result.Source != mdsFreshSource || result.Token != token || result.Action != action || !result.Complete || result.Error != "" || result.Mutation == nil ||
			result.Mutation.Kind != "native-lock-"+action || result.Mutation.State != "applied" || result.Mutation.ReadOnly != strings.HasPrefix(action, "observe-") ||
			result.Mutation.RequestID != result.RequestID || result.Process == nil || result.Process.ExitCode != exit || result.Process.TimedOut || n == nil ||
			n.Action != action || n.RequestID != result.RequestID || n.NativeGlobalID == 0 || n.PoolID != oracle.Pool.ID || (plan.Image != "" && !reflect.DeepEqual(result.Plan, plan)) {
			t.Fatal("native RBD lock receipt differs", action, result.Error)
		}
		record.NativeActions = append(record.NativeActions, rbdNativeActionEvidence{Action: action, RequestID: result.RequestID, NativeGlobalID: n.NativeGlobalID, ElapsedMS: result.ElapsedMS, StderrBytes: len(result.Process.Stderr)})
		// librbd logs failed lock, object-map and notify steps at error level.
		if len(result.Process.Stderr) != 0 {
			record.NativeStderr[action] = string(result.Process.Stderr)
			t.Fatalf("native RBD lock %s logged errors: %q", action, result.Process.Stderr)
		}
		return result, n
	}
	native := func(t *testing.T, action string) (nativeRBDLockResult, *nativeRBDLock) {
		t.Helper()
		if err := os.Remove(filepath.Join(dir, "rbd-lock-"+action+".json")); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		begun := time.Now()
		if err := testcluster.RequestControl(ctx, dir, "rbd-lock", "rbd-lock-completed", token, action); err != nil {
			t.Fatal("native RBD lock", action, err)
		}
		record.Timings["native_"+action] = time.Since(begun).String()
		return readNative(t, action, 0)
	}
	// nativeView projects one whole-action observation, with its image read
	// and its data objects.
	nativeView := func(t *testing.T, label string, n *nativeRBDLock) *rbdLockOwnerLossView {
		t.Helper()
		blocklist := n.Blocklist
		if label == "setup" {
			blocklist = &[]nativeRBDLockBlock{}
		}
		v, err := rbdLockOwnerLossNativeView(label, "", n.ClassCalls[5], n.ClassCalls[6], n.MapRaw, n.Watchers, n.Lockers, blocklist, n.Flags)
		if err != nil {
			t.Fatal(err)
		}
		var receipt struct {
			ObservedUTC string `json:"observed_utc"`
		}
		if err := readMDSFreshJSON(filepath.Join(dir, "rbd-lock-"+n.Action+".json"), &receipt); err != nil || receipt.ObservedUTC == "" {
			t.Fatal("native RBD lock receipt time missing", n.Action, err)
		}
		v.ObservedUTC = receipt.ObservedUTC
		head := readRBDBlob(t, dir, n.Blobs["image_head"])
		if uint64(len(head)) != plan.Size || len(n.Objects) != int(plan.Size>>plan.Order) {
			t.Fatal("native RBD lock binary view differs", n.Action)
		}
		v.ImageHeadSHA256 = digest(head)
		for _, o := range n.Objects {
			v.Objects = append(v.Objects, o.Head.SHA256)
		}
		record.Views[label] = v
		return v
	}

	setupResult, setup := native(t, "setup")
	plan = setupResult.Plan
	k := uint64(1) << 10
	if plan.Pool != oracle.Plan.Pool || plan.Image != "ceph-msgr-lock-"+token || plan.Order != 16 || plan.Size != 320*k || plan.Features != rbdLockImageFeatures ||
		!reflect.DeepEqual(plan.SetupWrites, []nativeRBDExtent{{0, 64 * k}}) || !reflect.DeepEqual(plan.OwnerWrites, []nativeRBDExtent{{64 * k, 16 * k}}) ||
		!reflect.DeepEqual(plan.GoWrites, []nativeRBDExtent{{136 * k, 8 * k}}) || !reflect.DeepEqual(plan.ReacquireWrites, []nativeRBDExtent{{200 * k, 8 * k}}) {
		t.Fatal("RBD lock plan differs", plan)
	}
	order, objectSize, objectCount := plan.Order, uint64(1)<<plan.Order, plan.Size>>plan.Order
	imageID := setup.ImageID
	header, mapObject, prefix := "rbd_header."+imageID, "rbd_object_map."+imageID, "rbd_data."+imageID
	if setup.HeaderObject != header || setup.MapObject != mapObject || setup.Stat.Prefix != prefix || setup.Stat.Objects != objectCount || setup.Features != plan.Features || len(setup.Snapshots) != 0 {
		t.Fatal("native RBD lock image setup differs", setup.Stat, setup.Features)
	}
	record.Image, record.ImageID = plan.Image, imageID
	// The expected head image grows with every writer.
	expectedHead := make([]byte, plan.Size)
	apply := func(stage, phase string, extents []nativeRBDExtent) {
		for _, e := range extents {
			copy(expectedHead[e.Offset:], rbdLockPattern(token, phase, e.Offset, e.Length))
		}
		record.ExpectedHead[stage] = digest(expectedHead)
	}
	checkWrites := func(t *testing.T, writes []nativeRBDLockWrite, phase string, extents []nativeRBDExtent) {
		t.Helper()
		if len(writes) != len(extents) {
			t.Fatal("native writes differ from the plan", phase)
		}
		for i, w := range writes {
			if w.Phase != phase || w.Offset != extents[i].Offset || w.Length != extents[i].Length || w.Written != w.Length || w.SHA256 != digest(rbdLockPattern(token, phase, w.Offset, w.Length)) {
				t.Fatal("test-only RBD lock pattern differs from the native driver", phase, w)
			}
		}
	}
	checkWrites(t, setup.Writes, "setup", plan.SetupWrites)
	apply("setup", "setup", plan.SetupWrites)
	if v := nativeView(t, "setup", setup); v.ImageHeadSHA256 != record.ExpectedHead["setup"] {
		t.Fatal("native librbd setup read differs from the test-only pattern")
	}
	done("setup")

	// Requests: caller-side placement from the observer's MON commands, every
	// request recorded with its raw reply before it is judged. epochFloor
	// raises the map epoch a request names; it is set once the breaker has
	// seen its blocklist entry in the OSDMap.
	addresses := osdDumpAddresses(t, ctx, observer.client)
	var epochFloor uint32
	route := func(t *testing.T, object string) rbdRoute {
		t.Helper()
		p, err := recoveryObjectPlacement(ctx, observer.client, plan.Pool, object)
		if err != nil {
			t.Fatal(err)
		}
		r := rbdRoute{Object: object, Pool: p.PoolID, Hash: p.Hash, Epoch: max(p.Epoch, epochFloor), RawPGID: p.RawPGID, PGID: p.PGID, Primary: p.Primary, Address: addresses[p.Primary]}
		if r.Pool != oracle.Pool.ID || r.Primary < 0 || r.Address == "" || p.Acting[0] != p.Primary {
			t.Fatal("caller-side RBD object placement differs", r)
		}
		return r
	}
	headerRoute := route(t, header)
	target := cephmsgr.OSDTarget{ID: headerRoute.Primary, Address: headerRoute.Address}
	open := func(t *testing.T, p *party) {
		t.Helper()
		o, err := p.client.OpenOSD(ctx, target)
		if err != nil {
			t.Fatal(p.role, "explicit OSD connection", err)
		}
		p.conn = o
	}
	now := func() cephmsgr.OSDTime {
		t := time.Now()
		return cephmsgr.OSDTime{Seconds: uint32(t.Unix()), Nanoseconds: uint32(t.Nanosecond())}
	}
	var recordMu sync.Mutex
	keep := func(label string, r rbdRoute, req cephmsgr.OSDRequest, reply cephmsgr.OSDReply, err error) {
		e := rbdWriteRequestEvidence{Label: label, Route: r, Submission: reply.Submission, Transaction: reply.Transaction, GlobalID: reply.GlobalID,
			MapEpoch: reply.MapEpoch, PG: reply.PG, Code: reply.Code, Flags: reply.Flags, UserVersion: reply.UserVersion, LegacyVersion: reply.LegacyVersion, ReplayVersion: reply.ReplayVersion}
		if req.ModificationTime != (cephmsgr.OSDTime{}) {
			mtime := req.ModificationTime
			e.ModificationTime = &mtime
		}
		for _, op := range req.Operations {
			in := rbdOperationInputEvidence{Code: uint16(op.Code), Flags: uint32(op.Flags), Offset: op.Offset, Length: op.Length, DataLength: len(op.Data), DataSHA256: digest(op.Data),
				Class: op.Class.Class, Method: op.Class.Method, Mode: uint8(op.Class.Mode)}
			if len(op.Data) <= 512 {
				in.DataHex = hex.EncodeToString(op.Data)
			}
			e.Inputs = append(e.Inputs, in)
		}
		for _, op := range reply.Operations {
			out := rbdOperationEvidence{Code: uint16(op.Code), Result: op.Result, Length: op.Length, DataLength: len(op.Data), DataSHA256: digest(op.Data), Watchers: op.Watchers}
			if len(op.Data) <= 512 {
				out.DataHex = hex.EncodeToString(op.Data)
			}
			e.Operations = append(e.Operations, out)
		}
		if err != nil {
			e.Error = err.Error()
		}
		recordMu.Lock()
		record.Requests = append(record.Requests, e)
		recordMu.Unlock()
	}
	mutating := func(ops []cephmsgr.OSDOperation) bool {
		return slices.ContainsFunc(ops, func(op cephmsgr.OSDOperation) bool {
			return op.Code == cephmsgr.OSDCall && op.Class.Mode == cephmsgr.OSDCallWrite || op.Code == cephmsgr.OSDWrite || op.Code == cephmsgr.OSDSetAllocHint
		})
	}
	// submit sends one request once and returns whatever came back.
	submit := func(t *testing.T, p *party, label, object string, ops ...cephmsgr.OSDOperation) (rbdRoute, cephmsgr.OSDReply, error) {
		t.Helper()
		r := route(t, object)
		req := cephmsgr.OSDRequest{MapEpoch: r.Epoch, Object: cephmsgr.OSDObject{Pool: r.Pool, Name: r.Object, Hash: r.Hash}, Operations: ops}
		if mutating(ops) {
			req.ModificationTime = now()
		}
		reply, err := p.conn.Request(ctx, req)
		keep(p.role+":"+label, r, req, reply, err)
		return r, reply, err
	}
	// send requires the given result code and the party's reply identity.
	// known requires the one expected outcome of a request: the given code as
	// a received OSD answer with the reply identity of this submission. An
	// unknown outcome carries a zero reply code and is never a success.
	known := func(t *testing.T, p *party, label string, r rbdRoute, reply cephmsgr.OSDReply, err error, code int32, ops []cephmsgr.OSDOperation) {
		t.Helper()
		var server *cephmsgr.OSDError
		if code == 0 && err != nil || code != 0 && (!errors.As(err, &server) || server.Code != code) {
			t.Fatalf("Go %s %s: result differs from %d: %v", p.role, label, code, err)
		}
		if reply.Code != code || reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.GlobalID != p.gid || reply.Submission.MapEpoch != r.Epoch ||
			reply.Object != r.Object || reply.PG.Pool != uint64(r.Pool) || reply.PG.Seed != r.Hash || reply.MapEpoch < r.Epoch || reply.RetryAttempt != 0 || len(reply.Operations) != len(ops) ||
			mutating(ops) && code == 0 && (reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.UserVersion == 0) {
			t.Fatalf("Go %s %s: OSD reply identity differs: code=%d tid=%d gid=%d epoch=%d flags=%#x", p.role, label, reply.Code, reply.Transaction, reply.GlobalID, reply.MapEpoch, reply.Flags)
		}
	}
	sendOn := func(t *testing.T, p *party, label, object string, code int32, ops ...cephmsgr.OSDOperation) (rbdRoute, cephmsgr.OSDReply) {
		t.Helper()
		r, reply, err := submit(t, p, label, object, ops...)
		known(t, p, label, r, reply, err, code, ops)
		return r, reply
	}
	send := func(t *testing.T, p *party, label, object string, code int32, ops ...cephmsgr.OSDOperation) cephmsgr.OSDReply {
		t.Helper()
		_, reply := sendOn(t, p, label, object, code, ops...)
		return reply
	}
	// attempt keeps the raw outcome of one request without requiring it.
	attempt := func(t *testing.T, p *party, label, object string, ops ...cephmsgr.OSDOperation) rbdLockOwnerLossAttempt {
		t.Helper()
		_, reply, err := submit(t, p, label, object, ops...)
		var server *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		a := rbdLockOwnerLossAttempt{Label: label, Invocations: 1, Code: reply.Code, UnknownOutcome: errors.As(err, &unknown), Submission: reply.Submission,
			ReplyTransaction: reply.Transaction, ReplyMapEpoch: reply.MapEpoch}
		a.Class, a.Error = rbdMutationOutcome(reply, err)
		if errors.As(err, &server) {
			a.ErrorCode = server.Code
		}
		for _, op := range reply.Operations {
			a.OperationResults = append(a.OperationResults, op.Result)
		}
		return a
	}
	classOp := func(class, method string, mode cephmsgr.OSDCallMode, input []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: mode, MaxOutputBytes: 4096}}
	}
	lockInfo := func(t *testing.T, p *party, label, object, tag string) ([]byte, []rbdLockOwnerLossLocker) {
		t.Helper()
		out := send(t, p, label, object, 0, classOp("lock", "get_info", cephmsgr.OSDCallRead, rbdLockGetInfoInput())).Operations[0].Data
		lockers, err := rbdLockOwnerLossLockers(out, tag)
		if err != nil {
			t.Fatal(p.role, label, err)
		}
		return out, lockers
	}
	// readRaw returns an object's bytes, or nil when the OSD says it is
	// absent.
	readRaw := func(t *testing.T, p *party, label, object string) []byte {
		t.Helper()
		r, reply, err := submit(t, p, label, object, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: objectSize})
		var server *cephmsgr.OSDError
		if errors.As(err, &server) && server.Code == -2 && reply.Code == -2 {
			return nil
		}
		if err != nil || reply.Code != 0 || reply.GlobalID != p.gid || reply.Object != r.Object || len(reply.Operations) != 2 ||
			binary.LittleEndian.Uint64(reply.Operations[0].Data) != uint64(len(reply.Operations[1].Data)) {
			t.Fatal(p.role, label, "object read", reply.Code, err)
		}
		return reply.Operations[1].Data
	}
	listWatchers := func(t *testing.T, p *party, label string) []rbdLockOwnerLossWatcher {
		t.Helper()
		reply := send(t, p, label, header, 0, cephmsgr.OSDOperation{Code: cephmsgr.OSDListWatchers, ListWatchers: cephmsgr.OSDListWatchersInput{MaxOutputBytes: 4096}})
		watchers := []rbdLockOwnerLossWatcher{}
		for _, w := range reply.Operations[0].Watchers {
			if w.NameType != rbdEntityClient || w.TimeoutSeconds != rbdLockOwnerLossWatchTimeout {
				t.Fatal(p.role, label, "watcher is not a client with the default timeout", w)
			}
			watchers = append(watchers, rbdLockOwnerLossWatcher{uint64(w.NameNumber), w.Cookie, fmt.Sprintf("%s/%d", w.Address.Endpoint, w.Address.Nonce)})
		}
		return watchers
	}
	// blocklist reads the OSDMap blocklist through p's MON session.
	blocklist := func(t *testing.T, p *party, label string) (uint32, []rbdLockOwnerLossBlock) {
		t.Helper()
		result, err := p.client.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"osd dump","format":"json"}`)})
		if err != nil {
			t.Fatal("MON osd dump", label, err)
		}
		var dump struct {
			Epoch     uint32            `json:"epoch"`
			Blocklist map[string]string `json:"blocklist"`
			Range     map[string]string `json:"range_blocklist"`
		}
		if err := json.Unmarshal(result.Data, &dump); err != nil || dump.Epoch == 0 {
			t.Fatal("MON osd dump decoding", label, err)
		}
		blocks := []rbdLockOwnerLossBlock{}
		for i, entries := range []map[string]string{dump.Blocklist, dump.Range} {
			for address, until := range entries {
				normalized, err := rbdLockOwnerLossUntil(until)
				if err != nil {
					t.Fatal(label, err)
				}
				blocks = append(blocks, rbdLockOwnerLossBlock{[]string{"", "range:"}[i] + address, normalized})
			}
		}
		slices.SortFunc(blocks, func(a, b rbdLockOwnerLossBlock) int { return strings.Compare(a.Address, b.Address) })
		return dump.Epoch, blocks
	}
	// goView is one observation through the observer's connection and MON
	// session.
	goView := func(t *testing.T, label string) *rbdLockOwnerLossView {
		t.Helper()
		v := &rbdLockOwnerLossView{Label: label, Source: "go"}
		headerOut, headerLockers := lockInfo(t, observer, label+"_header_lock", header, rbdLockTag)
		mapOut, mapLockers := lockInfo(t, observer, label+"_map_lock", mapObject, "")
		v.HeaderLockHex, v.HeaderLockers, v.MapLockHex, v.MapLockers = hex.EncodeToString(headerOut), headerLockers, hex.EncodeToString(mapOut), mapLockers
		v.Watchers = listWatchers(t, observer, label+"_watchers")
		raw := readRaw(t, observer, label+"_map_raw", mapObject)
		m, err := decodeRBDObjectMap(raw)
		if err != nil || !m.CRC || m.Size != objectCount {
			t.Fatal(label, "object map does not decode", err)
		}
		v.MapRawHex = hex.EncodeToString(raw)
		for _, s := range m.States {
			v.MapStates = append(v.MapStates, int(s))
		}
		v.Epoch, v.Blocklist = blocklist(t, observer, label)
		v.ObservedUTC = time.Now().UTC().Format(time.RFC3339Nano)
		record.Views[label] = v
		return v
	}
	notify := func(t *testing.T, p *party, label string, payload []byte) *cephmsgr.OSDNotify {
		t.Helper()
		r := route(t, header)
		req := cephmsgr.OSDRequest{MapEpoch: r.Epoch, Object: cephmsgr.OSDObject{Pool: r.Pool, Name: r.Object, Hash: r.Hash}}
		n, reply, err := p.conn.NotifyObject(ctx, req, payload, cephmsgr.OSDWatchOptions{TimeoutSeconds: 5})
		keep(p.role+":"+label, r, req, reply, err)
		if err != nil {
			t.Fatal("Go notify", p.role, label, err)
		}
		return n
	}
	// completed waits for a notify's completion and requires an empty
	// acknowledgement from each of the given watches and nobody missed.
	completed := func(t *testing.T, p *party, label string, n *cephmsgr.OSDNotify, payload []byte, begun time.Time, from ...rbdLockOwnerLossParty) {
		t.Helper()
		defer n.Close()
		wait, stop := context.WithTimeout(ctx, 20*time.Second)
		defer stop()
		completion, err := n.Next(wait)
		if err != nil {
			t.Fatal("Go notify completion", p.role, label, err)
		}
		e := &rbdLockGoNotify{Label: p.role + ":" + label, PayloadHex: hex.EncodeToString(payload), Cookie: n.Cookie(), NotifyID: completion.NotifyID, Opcode: completion.Opcode,
			CompletionID: completion.NotifyID, ReturnCode: completion.ReturnCode, Missed: completion.Missed, RawFrontHex: hex.EncodeToString(completion.RawFront),
			RawDataHex: hex.EncodeToString(completion.RawData), CompletedAfterMS: float64(time.Since(begun).Microseconds()) / 1000}
		for _, reply := range completion.Replies {
			e.Replies = append(e.Replies, rbdNotifyReplyEvidence{GlobalID: reply.GlobalID, Cookie: reply.Cookie, DataHex: hex.EncodeToString(reply.Data)})
		}
		record.GoNotifies = append(record.GoNotifies, e)
		if completion.Opcode != cephmsgr.OSDWatchCompletion || completion.Cookie != n.Cookie() || completion.ReturnCode != 0 || len(completion.Missed) != 0 || len(completion.Replies) != len(from) {
			t.Fatalf("%s %s completion differs: %+v", p.role, label, e)
		}
		for _, w := range from {
			if !slices.ContainsFunc(e.Replies, func(r rbdNotifyReplyEvidence) bool {
				return r.GlobalID == w.GlobalID && r.Cookie == w.WatchCookie && r.DataHex == ""
			}) {
				t.Fatalf("%s %s: no empty acknowledgement from client.%d: %+v", p.role, label, w.GlobalID, e.Replies)
			}
		}
	}
	watchRequest := cephmsgr.OSDRequest{MapEpoch: headerRoute.Epoch, Object: cephmsgr.OSDObject{Pool: headerRoute.Pool, Name: headerRoute.Object, Hash: headerRoute.Hash}}
	watch := func(t *testing.T, p *party) *cephmsgr.OSDWatch {
		t.Helper()
		w, reply, err := p.conn.WatchObject(ctx, watchRequest, cephmsgr.OSDWatchOptions{TimeoutSeconds: rbdLockOwnerLossWatchTimeout})
		keep(p.role+":watch_register", headerRoute, watchRequest, reply, err)
		if err != nil {
			t.Fatal(p.role, "header watch", err)
		}
		return w
	}
	// address is the legacy address the OSD lists for p's watch.
	address := func(t *testing.T, p *party, cookie uint64) string {
		t.Helper()
		for _, w := range listWatchers(t, p, "own_watcher") {
			if w.GlobalID == p.gid && w.Cookie == cookie {
				return w.Address
			}
		}
		t.Fatal(p.role, "own header watch is not listed")
		return ""
	}

	// The observer: a header watch for the whole sequence, pinged and
	// answered as a non-owner.
	open(t, observer)
	ow := watch(t, observer)
	record.Observer.WatchCookie, record.Observer.LockCookie = ow.Cookie(), rbdLockCookie(ow.Cookie())
	record.Observer.Address = address(t, observer, ow.Cookie())
	observerResponder = &rbdLockResponder{changed: make(chan struct{})}
	observerCtx, stopObserver := context.WithCancel(ctx)
	observerDone := make(chan struct{})
	go func() {
		defer close(observerDone)
		runRBDLockResponder(observerCtx, ow, headerRoute.Epoch, record.Observer.id(), observerResponder, started)
	}()
	observerWatching := true
	seen := func(t *testing.T, from int, label string, bound time.Duration, match func(rbdLockNotification) bool) rbdLockNotification {
		t.Helper()
		n, err := waitRBDLockNotification(ctx, observerResponder, from, bound, match)
		if err != nil {
			t.Fatal("observer:", label, err)
		}
		return n
	}

	// Compensation after a failure only: whoever may still hold a lock gives
	// it back once (never a repeated lock, never a repeated mutation), the
	// watches go, and the native action is not left behind.
	var zombieLocked, zombieGone, breakerLocked, breakerReleased, breakerWatching bool
	var breaker *party
	var bw *cephmsgr.OSDWatch
	var breakerStop context.CancelFunc
	var breakerDone chan struct{}
	var controlDone chan struct{}
	unlockBoth := func(p *party, cookie string) {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		for _, u := range [][2]string{{mapObject, ""}, {header, cookie}} {
			r := headerRoute
			r.Object = u[0]
			req := cephmsgr.OSDRequest{MapEpoch: max(r.Epoch, epochFloor), Object: cephmsgr.OSDObject{Pool: r.Pool, Name: u[0], Hash: r.Hash}, ModificationTime: now(),
				Operations: []cephmsgr.OSDOperation{classOp("lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(u[1]))}}
			if p, err := recoveryObjectPlacement(cleanup, observer.client, plan.Pool, u[0]); err == nil {
				req.Object.Hash, req.MapEpoch = p.Hash, max(p.Epoch, epochFloor)
			}
			reply, err := p.conn.Request(cleanup, req)
			keep(p.role+":compensating_unlock", r, req, reply, err)
			t.Log("compensating unlock after a failure:", p.role, u[0], err)
		}
	}
	defer func() {
		if !t.Failed() {
			return
		}
		if zombieLocked && !zombieGone && zombie.conn != nil {
			unlockBoth(zombie, record.Zombie.LockCookie)
		}
		if breakerLocked && !breakerReleased {
			unlockBoth(breaker, record.Breaker.LockCookie)
		}
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if breakerStop != nil {
			breakerStop()
			<-breakerDone
		}
		if breakerWatching {
			_, _ = bw.Unwatch(cleanup, headerRoute.Epoch)
		}
		stopObserver()
		<-observerDone
		if observerWatching {
			_, _ = ow.Unwatch(cleanup, headerRoute.Epoch)
		}
		if controlDone != nil {
			// The driver gives up at its own 30 s proceed deadline.
			select {
			case <-controlDone:
			case <-time.After(100 * time.Second):
			}
		}
	}()

	// The zombie takes the lock as librbd's AcquireRequest and
	// PostAcquireRequest do: its header watch handle names the lock cookie,
	// the object map is locked with an empty cookie and tag, and AcquiredLock
	// is announced. It answers its own notification while it is still alive.
	open(t, zombie)
	zw := watch(t, zombie)
	watchRegistered := time.Now()
	record.Zombie.WatchCookie, record.Zombie.LockCookie = zw.Cookie(), rbdLockCookie(zw.Cookie())
	zombieLocked = true
	send(t, zombie, "lock_lock", header, 0, classOp("lock", "lock", cephmsgr.OSDCallWrite, rbdLockLockInput(record.Zombie.LockCookie)))
	send(t, zombie, "map_lock_lock", mapObject, 0, classOp("lock", "lock", cephmsgr.OSDCallWrite, rbdLockLockInputFor("", "")))
	_, lockers := lockInfo(t, zombie, "lock_get_info", header, rbdLockTag)
	if len(lockers) != 1 || lockers[0].Entity != fmt.Sprintf("client.%d", zombie.gid) || lockers[0].Cookie != record.Zombie.LockCookie {
		t.Fatal("the zombie does not hold the image's exclusive lock", lockers)
	}
	record.Zombie.Address = lockers[0].Address
	if got := address(t, zombie, zw.Cookie()); got != record.Zombie.Address {
		t.Fatal("the zombie's locker address differs from its watcher address", got, record.Zombie.Address)
	}
	payload := rbdLockNotify(rbdNotifyAcquiredLock, record.Zombie.id())
	begun := time.Now()
	zn := notify(t, zombie, "notify_acquired_lock", payload)
	own, stopOwn := context.WithTimeout(ctx, 20*time.Second)
	event, err := zw.Next(own)
	stopOwn()
	if err != nil || event.Opcode != cephmsgr.OSDWatchNotification || !bytes.Equal(event.Payload, payload) || event.NotifierGlobalID != zombie.gid {
		t.Fatal("the zombie did not receive its own AcquiredLock", event.Opcode, err)
	}
	acknowledged, err := zw.Acknowledge(ctx, headerRoute.Epoch, event, nil)
	if err != nil || acknowledged.Submission.Transaction == 0 {
		t.Fatal("the zombie's acknowledgement of its own AcquiredLock", err)
	}
	record.ZombieLastTransaction = acknowledged.Submission.Transaction
	completed(t, zombie, "notify_acquired_lock", zn, payload, begun, record.Zombie, record.Observer)
	seen(t, 0, "the zombie's AcquiredLock", 20*time.Second, func(n rbdLockNotification) bool {
		return n.Op == rbdNotifyAcquiredLock && n.NotifierGlobalID == zombie.gid
	})
	done("zombie_lock")

	// From here on the zombie sends nothing: no ping, no acknowledgement, no
	// release. Its connection stays open.
	_, owner := native(t, "observe-go-owner")
	nativeView(t, "observe-go-owner", owner)
	done("observe-go-owner")
	goView(t, "go-owner")
	judge(t, "go-owner")

	// The native takeover runs in the background: its write returns only
	// after librbd broke the zombie's lock.
	statePath := filepath.Join(dir, "rbd-lock-state.json")
	for _, name := range []string{statePath, filepath.Join(dir, "rbd-lock-proceed-1"), filepath.Join(dir, "rbd-lock-takeover.json")} {
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	var controlErr error
	controlDone = make(chan struct{})
	takeoverBegun := time.Now()
	go func() {
		controlErr = testcluster.RequestControl(ctx, dir, "rbd-lock", "rbd-lock-completed", token, "takeover")
		close(controlDone)
	}()
	// librbd finds the lock held, the owner's watch listed, and asks.
	goGIDs := []uint64{zombie.gid, observer.gid}
	first := seen(t, 1, "native RequestLock", 60*time.Second, func(n rbdLockNotification) bool {
		return n.Op == rbdNotifyRequestLock && !slices.Contains(goGIDs, n.NotifierGlobalID)
	})
	goView(t, "request-while-listed")
	done("request_while_listed")
	// The OSD expires the zombie's watch a timeout after its registration;
	// librbd then blocklists, breaks and announces its lock.
	acquiredNote := seen(t, first.Index+1, "native AcquiredLock", time.Duration(rbdLockOwnerLossWatchTimeout+60)*time.Second, func(n rbdLockNotification) bool {
		return n.Op == rbdNotifyAcquiredLock && n.NotifierGlobalID == first.NotifierGlobalID
	})
	record.WatchRegisteredToAcquired = time.Since(watchRegistered).String()
	state, phase, err := waitRBDLockState(ctx, statePath, "acquired", controlDone, &controlErr)
	if err != nil {
		t.Fatal(err)
	}
	zombieGone = true
	nativeGID := state.NativeGlobalID
	i := slices.IndexFunc(phase.Watchers, func(w nativeRBDWatcher) bool { return w.ID == nativeGID })
	if state.Token != token || nativeGID != first.NotifierGlobalID || i < 0 || state.RequestID <= 0 {
		t.Fatal("native takeover scope differs", state.Token, nativeGID, phase.Watchers)
	}
	record.NativeOwner = rbdLockOwnerLossParty{GlobalID: nativeGID, WatchCookie: phase.Watchers[i].Cookie, LockCookie: rbdLockCookie(phase.Watchers[i].Cookie), Address: phase.Watchers[i].Address}
	checkWrites(t, phase.Writes, "owner", plan.OwnerWrites)
	apply("takeover", "owner", plan.OwnerWrites)
	record.TakeoverWriteNS = phase.WriteNS
	acquired, err := rbdLockOwnerLossNativeView("acquired", time.Now().UTC().Format(time.RFC3339Nano), phase.HeaderLock, phase.MapLock, phase.MapRaw, phase.Watchers, phase.Lockers, phase.Blocklist, phase.Flags)
	if err != nil || phase.BlocklistBefore == nil {
		t.Fatal("native takeover phase", err)
	}
	isOwner := phase.IsOwner
	acquired.IsOwner = &isOwner
	for _, o := range phase.Owners {
		if o.Mode != rbdLockModeExclusive {
			t.Fatal("native lock owner mode differs", o)
		}
		acquired.Owners = append(acquired.Owners, o.Owner)
	}
	if acquired.BlocklistBefore, err = rbdLockOwnerLossBlocks(*phase.BlocklistBefore); err != nil {
		t.Fatal(err)
	}
	record.Views["acquired"] = acquired
	goView(t, "native-owner")
	done("native_acquired")
	t.Logf("native client.%d owns the lock %s after its takeover was requested; AcquiredLock %s, write %v", nativeGID, time.Since(takeoverBegun).Round(time.Millisecond), acquiredNote.PayloadHex, time.Duration(phase.WriteNS))

	// The zombie comes back on the connection that took the lock and tries
	// what an owner does: librbd's guarded object-map update for an object
	// nobody wrote, the data write that would follow, and a release. Each is
	// sent once and its raw outcome is kept; none is ever sent again.
	fencedObject := objectCount - 1
	fencedOffset, fencedData := uint64(8*k), rbdLockPattern(token, "fenced", (objectCount-1)*objectSize+8*k, 4*k)
	record.Fenced = append(record.Fenced, attempt(t, zombie, "object_map_update", mapObject,
		classOp("lock", "assert_locked", cephmsgr.OSDCallRead, rbdLockAssertInput("", "")),
		classOp("rbd", "object_map_update", cephmsgr.OSDCallWrite, rbdObjectMapUpdateInput(fencedObject, fencedObject+1, rbdObjectExists, nil))))
	record.Fenced = append(record.Fenced, attempt(t, zombie, "data_write", fmt.Sprintf("%s.%016x", prefix, fencedObject),
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetAllocHint, Flags: cephmsgr.OSDFailOK, AllocHint: cephmsgr.OSDAllocHint{ExpectedObjectSize: objectSize, ExpectedWriteSize: objectSize}},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: fencedOffset, Length: uint64(len(fencedData)), Data: fencedData}))
	record.Fenced = append(record.Fenced, attempt(t, zombie, "lock_unlock", header, classOp("lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(record.Zombie.LockCookie))))
	done("fenced_attempts")
	// What its watch had queued while it was silent. It acknowledges nothing.
	for {
		wait, stop := context.WithTimeout(ctx, 20*time.Second)
		event, err := zw.Next(wait)
		stop()
		if err != nil {
			record.ZombieWatchEnd = err.Error()
			if errors.Is(err, cephmsgr.ErrOSDWatchDisconnected) {
				record.ZombieWatchEnd = "disconnected"
			}
			break
		}
		e := rbdLockOwnerLossEvent{Opcode: event.Opcode, NotifyID: event.NotifyID, NotifierGlobalID: event.NotifierGlobalID, PayloadHex: hex.EncodeToString(event.Payload)}
		if event.Opcode == cephmsgr.OSDWatchNotification {
			m, err := decodeRBDNotifyMessage(event.Payload)
			if err != nil {
				t.Fatal("the zombie's queued notification does not decode", err)
			}
			e.Op, e.Client, e.Force = m.Op, m.Client, m.Force
		}
		record.ZombieEvents = append(record.ZombieEvents, e)
	}
	done("zombie_watch_end")
	// The same client on a new OSD connection: a read only.
	if err := zombie.conn.Close(); err != nil {
		t.Fatal("close the zombie's fenced connection", err)
	}
	open(t, zombie)
	_, reply, err := submit(t, zombie, "new_connection_lock_get_info", header, classOp("lock", "get_info", cephmsgr.OSDCallRead, rbdLockGetInfoInput()))
	again := rbdLockOwnerLossAttempt{Label: "new_connection_lock_get_info", Invocations: 1, Code: reply.Code, Submission: reply.Submission, ReplyTransaction: reply.Transaction, ReplyMapEpoch: reply.MapEpoch}
	again.Class, again.Error = rbdMutationOutcome(reply, err)
	var server *cephmsgr.OSDError
	if errors.As(err, &server) {
		again.ErrorCode = server.Code
	}
	record.NewConnection = &again
	if err == nil && len(reply.Operations) == 1 {
		if record.NewConnectionLockers, err = rbdLockOwnerLossLockers(reply.Operations[0].Data, rbdLockTag); err != nil {
			t.Fatal("the zombie client's read on a new connection does not decode", err)
		}
	}
	done("new_connection")

	// A Go client dialed only now reads what native librbd left.
	breaker = dial(t, "breaker")
	record.Breaker.GlobalID = breaker.gid
	open(t, breaker)
	fresh := &rbdLockOwnerLossFresh{}
	out, _ := lockInfo(t, breaker, "fresh_header_lock", header, rbdLockTag)
	fresh.HeaderLockHex = hex.EncodeToString(out)
	fresh.MapRawHex = hex.EncodeToString(readRaw(t, breaker, "fresh_map_raw", mapObject))
	freshObjects := make([][]byte, objectCount)
	for i := range freshObjects {
		freshObjects[i] = readRaw(t, breaker, fmt.Sprintf("fresh_read_%d", i), fmt.Sprintf("%s.%016x", prefix, i))
		sum := ""
		if freshObjects[i] != nil {
			sum = digest(freshObjects[i])
		}
		fresh.Objects = append(fresh.Objects, sum)
	}
	record.Fresh = fresh
	done("fresh_read")

	// The native owner observes once more and is killed while it owns the
	// lock.
	if err := testcluster.WritePublicEvidence(filepath.Join(dir, "rbd-lock-proceed-1"), fmt.Appendf(nil, "%s %d 1", token, state.RequestID)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-controlDone:
	case <-time.After(90 * time.Second):
		t.Fatal("native takeover action did not end")
	}
	killed := time.Now()
	if controlErr != nil {
		t.Fatal("native takeover action", controlErr)
	}
	takeoverResult, takeover := readNative(t, "takeover", -9)
	exit := takeoverResult.Process.ExitCode
	record.TakeoverExit = &exit
	if takeover.NativeGlobalID != nativeGID || len(takeover.Phases) != 1 || takeover.Phases[0].Phase != "acquired" {
		t.Fatal("native takeover receipt differs", takeover.NativeGlobalID, len(takeover.Phases))
	}
	tv := nativeView(t, "takeover", takeover)
	// The breaker's reads are the same bytes librados returned.
	objectsHead := readRBDBlob(t, dir, takeover.Blobs["objects_head"])
	for i, o := range takeover.Objects {
		if o.Head.Exists != (freshObjects[i] != nil) || o.Head.Exists && !bytes.Equal(objectsHead[o.Head.Offset:o.Head.Offset+o.Head.Size], freshObjects[i]) {
			t.Fatalf("native librados object %d differs from the breaker's read", i)
		}
	}
	for _, e := range tv.Blocklist {
		if e.Address == record.Zombie.Address {
			if until, err := time.Parse(time.RFC3339Nano, e.Until); err == nil {
				record.ZombieBlockSeconds = until.Sub(killed).Seconds()
			}
		}
	}
	done("takeover")
	judge(t, "takeover")

	// The breaker registers its header watch, as a librbd open does, and
	// repeats librbd's AcquireRequest and BreakRequest against the dead
	// owner.
	bw = watch(t, breaker)
	breakerWatching = true
	record.Breaker.WatchCookie, record.Breaker.LockCookie = bw.Cookie(), rbdLockCookie(bw.Cookie())
	record.Breaker.Address = address(t, breaker, bw.Cookie())
	breakerResponder = &rbdLockResponder{changed: make(chan struct{})}
	var breakerCtx context.Context
	breakerCtx, breakerStop = context.WithCancel(ctx)
	breakerDone = make(chan struct{})
	go func() {
		defer close(breakerDone)
		runRBDLockResponder(breakerCtx, bw, headerRoute.Epoch, record.Breaker.id(), breakerResponder, started)
	}()
	brk := &rbdLockOwnerLossBreak{}
	record.Break = brk
	_, lockers = lockInfo(t, breaker, "break_get_locker", header, rbdLockTag)
	if len(lockers) != 1 {
		t.Fatal("the killed owner's lock record is gone", lockers)
	}
	brk.Locker = lockers[0]
	if err := rbdLockOwnerLossBreakTarget(record.NativeOwner, brk.Locker, brk.Locker); err != nil {
		t.Fatal("before the first lock attempt:", err)
	}
	deadHandle, ok := rbdLockHandle(brk.Locker.Cookie)
	if !ok {
		t.Fatal("the locker's cookie is not a managed-lock cookie", brk.Locker.Cookie)
	}
	// Every break and acquire call below must be answered with its one
	// expected code as a known OSD answer; anything else, an unknown
	// outcome included, stops the sequence before the next mutation.
	firstOps := []cephmsgr.OSDOperation{classOp("lock", "lock", cephmsgr.OSDCallWrite, rbdLockLockInput(record.Breaker.LockCookie))}
	firstRoute, firstReply, firstErr := submit(t, breaker, "break_first_lock", header, firstOps...)
	brk.FirstLockCode = firstReply.Code
	if firstErr == nil && firstReply.Code == 0 {
		breakerLocked = true
		t.Fatal("the breaker took a lock that the killed owner still held")
	}
	known(t, breaker, "break_first_lock", firstRoute, firstReply, firstErr, rbdLockBusy, firstOps)
	// BreakRequest: an owner whose watch is listed with the locker's address
	// and handle is alive, and its lock is not broken.
	alive := func(watchers []rbdLockOwnerLossWatcher) bool {
		return slices.ContainsFunc(watchers, func(w rbdLockOwnerLossWatcher) bool { return w.Address == brk.Locker.Address && w.Cookie == deadHandle })
	}
	goView(t, "dead-owner-listed")
	for bound := time.After(time.Duration(rbdLockOwnerLossWatchTimeout+60) * time.Second); ; {
		if !alive(listWatchers(t, breaker, "break_get_watchers")) {
			brk.UnlistedPolls++
			break
		}
		brk.ListedPolls++
		select {
		case <-bound:
			t.Fatal("the killed owner's watch is still listed")
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	record.KillToUnlisted = time.Since(killed).String()
	done("dead_owner_listed")
	_, lockers = lockInfo(t, breaker, "break_refresh_locker", header, rbdLockTag)
	if len(lockers) != 1 {
		t.Fatal("the killed owner's lock record changed while its watch expired", lockers)
	}
	brk.RefreshedLocker = lockers[0]
	goView(t, "dead-owner-unlisted")
	done("dead_owner_unlisted")
	// The blocklist command and break_lock name the locker read at the start.
	// Both are sent only while the refreshed record is still that locker and
	// the killed native owner; a lock that changed hands stops here.
	if err := rbdLockOwnerLossBreakTarget(record.NativeOwner, brk.Locker, brk.RefreshedLocker); err != nil {
		t.Fatal("before the MON blocklist command:", err)
	}
	// librados blocklist_add: the same MON command, without an expiry. The
	// identity's `profile rbd` grants exactly this form.
	command, _ := json.Marshal(map[string]string{"prefix": "osd blocklist", "blocklistop": "add", "addr": brk.Locker.Address})
	brk.BlocklistCommand, brk.BlocklistAddress = string(command), brk.Locker.Address
	result, err := breaker.client.MonCommand(ctx, cephmsgr.Command{JSON: command})
	brk.BlocklistCode, brk.BlocklistMessage = result.Code, result.Message
	if err != nil {
		t.Fatal("the breaker's MON osd blocklist add", err)
	}
	// librados then waits for the latest OSDMap. The breaker reads the epoch
	// that holds its entry and names it in every later request, so the OSD
	// does not run them under an older map.
	epoch, blocks := blocklist(t, breaker, "blocklisted")
	if !slices.ContainsFunc(blocks, func(b rbdLockOwnerLossBlock) bool { return b.Address == brk.Locker.Address }) {
		t.Fatal("the MON's OSDMap lacks the breaker's blocklist entry", epoch, blocks)
	}
	epochFloor = epoch
	blocklisted := goView(t, "blocklisted")
	for _, e := range blocklisted.Blocklist {
		if e.Address == brk.Locker.Address {
			if until, err := time.Parse(time.RFC3339Nano, e.Until); err == nil {
				record.NativeOwnerBlockSeconds = time.Until(until).Seconds()
			}
		}
	}
	done("blocklist_add")
	// cls_lock_break_op: the lock name, the locker's entity and its cookie.
	breakInput := func(l rbdLockOwnerLossLocker) []byte {
		input, err := rbdLockBreakInput(l)
		if err != nil {
			t.Fatal(err)
		}
		return input
	}
	r, reply := sendOn(t, breaker, "break_lock", header, 0, classOp("lock", "break_lock", cephmsgr.OSDCallWrite, breakInput(brk.Locker)))
	brk.BreakRequestEpoch, brk.BreakReplyEpoch, brk.BreakCode = r.Epoch, reply.MapEpoch, reply.Code
	done("break_lock")
	// The header lock may be held from the moment this request is sent, so
	// the cleanup releases it whatever the outcome; ownership is announced
	// only after the known answer.
	breakerLocked = true
	reply = send(t, breaker, "lock_lock", header, 0, classOp("lock", "lock", cephmsgr.OSDCallWrite, rbdLockLockInput(record.Breaker.LockCookie)))
	brk.LockCode = reply.Code
	breakerResponder.setOwning(true)
	done("go_lock")
	// object_map::LockRequest: lock, and on -EBUSY read the lockers, break
	// each of them in one write and lock again.
	reply = send(t, breaker, "map_first_lock", mapObject, rbdLockBusy, classOp("lock", "lock", cephmsgr.OSDCallWrite, rbdLockLockInputFor("", "")))
	brk.MapFirstLockCode = reply.Code
	_, brk.MapLockers = lockInfo(t, breaker, "map_get_lockers", mapObject, "")
	// break_lock is built only from the killed owner's sole record.
	if err := rbdLockOwnerLossMapBreakTarget(record.NativeOwner, brk.MapLockers); err != nil {
		t.Fatal("before the object-map break_lock:", err)
	}
	reply = send(t, breaker, "map_break_lock", mapObject, 0, classOp("lock", "break_lock", cephmsgr.OSDCallWrite, breakInput(brk.MapLockers[0])))
	brk.MapBreakCode = reply.Code
	reply = send(t, breaker, "map_lock_lock", mapObject, 0, classOp("lock", "lock", cephmsgr.OSDCallWrite, rbdLockLockInputFor("", "")))
	brk.MapLockCode = reply.Code
	done("map_break_lock")
	// The owner's work: load the object map, announce the lock, mark the
	// object and write it, as in the exclusive-lock acceptance.
	if m, err := decodeRBDObjectMap(send(t, breaker, "object_map_load", mapObject, 0, classOp("rbd", "object_map_load", cephmsgr.OSDCallRead, nil)).Operations[0].Data); err != nil || !slices.Equal(m.States, []uint8{1, 1, 0, 0, 0}) {
		t.Fatal("the breaker loaded another object map than the killed owner left", m.States, err)
	}
	payload, begun = rbdLockNotify(rbdNotifyAcquiredLock, record.Breaker.id()), time.Now()
	completed(t, breaker, "notify_acquired_lock", notify(t, breaker, "notify_acquired_lock", payload), payload, begun, record.Breaker, record.Observer)
	seen(t, acquiredNote.Index+1, "the breaker's AcquiredLock", 20*time.Second, func(n rbdLockNotification) bool {
		return n.Op == rbdNotifyAcquiredLock && n.NotifierGlobalID == breaker.gid
	})
	goData := rbdLockPattern(token, "go", plan.GoWrites[0].Offset, plan.GoWrites[0].Length)
	pieces := rbdObjectExtents(plan.GoWrites[0].Offset, plan.GoWrites[0].Length, order)
	if len(plan.GoWrites) != 1 || len(pieces) != 1 {
		t.Fatal("the Go extent of the plan is not inside one object", plan.GoWrites)
	}
	number, offset, length := pieces[0][0], pieces[0][1], pieces[0][2]
	send(t, breaker, "object_map_update", mapObject, 0, classOp("lock", "assert_locked", cephmsgr.OSDCallRead, rbdLockAssertInput("", "")),
		classOp("rbd", "object_map_update", cephmsgr.OSDCallWrite, rbdObjectMapUpdateInput(number, number+1, rbdObjectExists, nil)))
	goObject := fmt.Sprintf("%s.%016x", prefix, number)
	send(t, breaker, "data_write", goObject, 0,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetAllocHint, Flags: cephmsgr.OSDFailOK, AllocHint: cephmsgr.OSDAllocHint{ExpectedObjectSize: objectSize, ExpectedWriteSize: objectSize}},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: offset, Length: length, Data: goData})
	apply("go-break", "go", plan.GoWrites)
	done("go_write")
	goView(t, "go-broke")
	_, broke := native(t, "observe-go-broke")
	nativeView(t, "observe-go-broke", broke)
	if o := broke.Objects[number].Head; !o.Exists || !bytes.Equal(readRBDBlob(t, dir, broke.Blobs["objects_head"])[o.Offset:o.Offset+o.Size], append(make([]byte, offset), goData...)) {
		t.Fatal("native librados reads the breaker's object differently from its write")
	}
	done("observe-go-broke")
	judge(t, "go-break")

	// The breaker releases as librbd's PreReleaseRequest and ReleaseRequest
	// do and removes its watch.
	breakerResponder.setOwning(false)
	send(t, breaker, "map_lock_unlock", mapObject, 0, classOp("lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput("")))
	send(t, breaker, "lock_unlock", header, 0, classOp("lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(record.Breaker.LockCookie)))
	breakerReleased = true
	payload, begun = rbdLockNotify(rbdNotifyReleasedLock, record.Breaker.id()), time.Now()
	completed(t, breaker, "notify_released_lock", notify(t, breaker, "notify_released_lock", payload), payload, begun, record.Breaker, record.Observer)
	// The notify completes when the OSD has the acknowledgements; the
	// breaker's own responder records its event only once its
	// acknowledgement was answered. Wait for that record before stopping it,
	// so that no acknowledgement is abandoned halfway.
	if _, err := waitRBDLockNotification(ctx, breakerResponder, 0, 20*time.Second, func(n rbdLockNotification) bool { return n.Op == rbdNotifyReleasedLock && n.Own }); err != nil {
		t.Fatal("breaker: its own ReleasedLock", err)
	}
	breakerStop()
	<-breakerDone
	if _, failure := breakerResponder.snapshot(); failure != nil {
		t.Fatal("breaker header watch responder", failure)
	}
	unwatch, err := bw.Unwatch(ctx, headerRoute.Epoch)
	keep("breaker:watch_unwatch", headerRoute, watchRequest, unwatch, err)
	if err != nil {
		t.Fatal("breaker header unwatch", err)
	}
	breakerWatching = false
	goView(t, "released")
	done("go_release")

	// A fresh native open writes, closes and checks the object map.
	before, _ := observerResponder.snapshot()
	mark := len(before)
	_, final := native(t, "final")
	checkWrites(t, final.Writes, "reacquire", plan.ReacquireWrites)
	apply("final", "reacquire", plan.ReacquireWrites)
	nativeView(t, "final", final)
	record.Check = final.Check
	match := len(final.GoExtents) == 1 && final.GoExtents[0].SHA256 == digest(goData)
	record.FinalGoExtentsMatch = &match
	if !bytes.Equal(readRBDBlob(t, dir, final.Blobs["image_head"]), expectedHead) {
		t.Fatal("native librbd head read differs from the setup, takeover, Go and final writes")
	}
	done("final")
	// The final writer's and the check's notifications: four in all.
	seen(t, mark+3, "the object-map check's ReleasedLock", 30*time.Second, func(n rbdLockNotification) bool { return n.Op == rbdNotifyReleasedLock })
	stopObserver()
	<-observerDone
	drain, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	if event, err := ow.Next(drain); err == nil {
		t.Fatal("unacknowledged observer notification left", event.NotifyID, hex.EncodeToString(event.Payload))
	}
	stop()
	unwatch, err = ow.Unwatch(ctx, headerRoute.Epoch)
	keep("observer:watch_unwatch", headerRoute, watchRequest, unwatch, err)
	if err != nil {
		t.Fatal("observer header unwatch", err)
	}
	observerWatching = false
	goView(t, "end")
	done("observer_unwatch")
	judge(t, "final")
	t.Logf("lock-owner loss mode=%s zombie=client.%d@%s native=client.%d@%s breaker=client.%d@%s native_requests=%d zombie_events=%d watch_to_acquired=%s kill_to_unlisted=%s blocklist_seconds=%.0f/%.0f",
		mode, zombie.gid, record.Zombie.Address, nativeGID, record.NativeOwner.Address, breaker.gid, record.Breaker.Address, len(record.Notifications)-8, len(record.ZombieEvents),
		record.WatchRegisteredToAcquired, record.KillToUnlisted, record.ZombieBlockSeconds, record.NativeOwnerBlockSeconds)
}
