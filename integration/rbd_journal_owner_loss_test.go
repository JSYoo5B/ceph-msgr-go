package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// TestCephRBDJournalOwnerLossIntegration loses the lock owner of a
// journaling image while the outcome of its journal append is unknown. It
// runs the lost-append sequence of TestCephRBDJournalLostAppendReplyIntegration
// up to the observation after the recorded unknown outcome: the subject holds
// the image's exclusive lock in librbd's managed-lock format, one AioWrite
// entry it appended under its own tag is in the journal, the reply to that
// append was withheld and the subject's wait was canceled. A second Go
// client (the observer) then watches the image header and answers every
// notification as a non-owner. The subject dies as a lock owner: it closes
// its silent connection and never sends anything again, so it releases
// nothing, does not unwatch and does not renew its watch, which the OSD
// keeps for its timeout after the registration. A native librbd write (the
// takeover) then takes the lock. Right before its first write the takeover
// lists the subject's lock record (client, cookie and address) and the
// subject's watch with that cookie at that address. The observer receives
// from the takeover's instance and watch handle at least one RequestLock,
// then only its AcquiredLock and its ReleasedLock. A RequestLock says who
// asked and when, not why: at v20.2.4 ExclusiveLock.cc
// post_acquire_lock_handler sends it when the acquisition ends with -EBUSY
// (AcquireRequest.cc handle_get_locker) or -EAGAIN, and BreakRequest.cc
// returns -EAGAIN when a watcher matches the locker's address and handle
// (handle_get_watchers) or when the locker changed on its recheck
// (handle_get_locker), all in ceph/ceph/blob/v20.2.4/src/librbd/. librbd's
// own debug log of the takeover (level 10, the lines of those files and of
// ImageWatcher.cc) names the branch: for each RequestLock the observer
// received, one attempt found the lock held (handle_lock -EBUSY),
// BreakRequest found a watcher with the locker's address and handle ("lock
// owner is still alive") and ended with -EAGAIN, and
// post_acquire_lock_handler requested the lock. In the last attempt
// BreakRequest found no such watcher, reread the locker, blocklisted its
// address, waited for the latest OSDMap and broke the lock, which the
// attempt then took. The log is librbd's report of its own steps, not an
// observation of the wire; the observation after the takeover shows the
// address of the subject's lock record blocklisted, which it was not before.
// The takeover opens the journal, replays the owned entry and
// journals and applies its own write, which covers the first half of the
// owned extent. The takeover's native
// observation shows the owned entry exactly once in the journal, followed
// only by native librbd's own entry under the next tag, whose predecessor is
// the owned entry; the image is the setup with the owned extent and then the
// takeover write over it; the image client's commit position names the
// takeover's entry; the lock is free; the subject's address is blocklisted,
// and was not before the takeover. The subject's outcome stays unknown: it
// never sends the append again, never applies or commits the entry, and the
// gate counts one frame of the append's size on its one connection. A lost
// lock owner's uncertain append is therefore resolved by the next owner's
// replay of what the OSD holds, not by the caller. Not covered: an owner
// that keeps its connection open after the loss (the forced-break variant
// covers requests of a fenced owner), journal appends pending at the OSD
// during the break, several pending appends, and journal trimming.
//
// Everything journal-specific is the test-only consumer code of
// rbd_journal_test.go. The product is unchanged.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournalownerloss key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-owner-loss
//scenario:row rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournalownerloss key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-owner-loss
//scenario:covers RBD underlying I/O
//fixture:isolated blocklists the Go lock owner's address (removed only at fixture teardown) and leaves a journaling image
func TestCephRBDJournalOwnerLossIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, rbdJournalVariantOwnerLoss)
}

// TestCephRBDJournalForcedBreakIntegration breaks the lock of a live Go
// owner of a journaling image and fences its journal and lock requests. The
// subject takes the image's exclusive lock in librbd's managed-lock format,
// allocates its tag and appends one AioWrite entry with a known answer,
// which it neither applies nor commits. A native observation shows that
// entry at its offset and the subject holding the lock with its watch
// listed. The subject pings its watch; then a native librbd instance that
// does not own the lock breaks it with lock_break, naming the one owner it
// lists. Right before lock_break that instance lists the subject's lock
// record (client, cookie and address) and the subject's watch with that
// cookie at that address. librbd does not ask whether that owner is alive:
// with force_break_lock it skips the liveness check, blocklists the
// owner's address and breaks the lock (internal.cc lock_break,
// ManagedLock.cc break_lock and managed_lock/BreakRequest.cc at
// ceph/ceph/blob/v20.2.4/src/librbd/). After lock_break the instance lists
// no owner and the address blocklisted. librbd's debug log of lock_break
// (level 10) shows the order: BreakRequest reports the owner's watch ("lock
// owner is still alive") and goes on, rereads the locker, blocklists its
// address, waits for the latest OSDMap and breaks the lock. The subject then sends, once each on
// the connection that sent lock.lock and naming the OSDMap epoch it read
// after the break, a watch ping, the append of its second entry and the
// unlock. The OSD answers each with -108 (EBLOCKLISTED), a known refusal,
// and none is sent again. A native observation shows every journal object
// unchanged since the append, the lock free and the subject's watch gone.
// Finally a native librbd write, which lists no lock record and not the
// subject's watch right before its first write and whose debug log shows the
// locker missing and the lock taken at once, takes the free lock,
// replays the subject's entry and journals and applies its own write over the first half of that
// extent: the same postconditions as in the owner-loss variant, except that
// the subject's address was already blocklisted before the takeover. Not
// covered: a fenced owner's requests on a connection whose address the lock
// record does not name (each OSD connection of a client without
// EnableCephFS has its own address nonce), requests in flight at the break,
// and a break by a client that is not librbd.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournalforcedbreak key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-forced-break
//scenario:row rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournalforcedbreak key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-forced-break
//scenario:covers RBD underlying I/O
//fixture:isolated blocklists the Go lock owner's address (removed only at fixture teardown) and leaves a journaling image
func TestCephRBDJournalForcedBreakIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, rbdJournalVariantForcedBreak)
}

// The variants of runRBDJournalLostAppend.
const (
	rbdJournalVariantLostAppend    = "lost-append"
	rbdJournalVariantPrimaryChange = "primary-change"
	rbdJournalVariantOwnerLoss     = "owner-loss"
	rbdJournalVariantForcedBreak   = "forced-break"
)

// The owner-loss variant: the lost-append order up to the observation after
// the recorded outcome, then the closed silent connection and the takeover.
var (
	rbdJournalOwnerLossOrder = []string{"setup", "lock", "tag_create", "observe-locked", "append_submitted", "barrier", "checkpoint", "outcome_recorded",
		"observe-recorded", "observer_watch", "blocked_connection_closed", "takeover"}
	rbdJournalOwnerLossStages = []string{"checkpoint", "observe-recorded", "takeover"}
)

// rbdJournalOwnerLossTakeoverStart bounds the age of the subject's watch,
// registered without a later ping, when the owner-loss variant requests the
// takeover. It leaves the takeover's start-up two thirds of the remaining
// watch lifetime; the claims do not rely on it, they judge what the takeover
// listed and what the observer received.
const rbdJournalOwnerLossTakeoverStart = 20 * time.Second

// The forced-break variant's order and the stages at which it is judged.
var (
	rbdJournalForcedBreakOrder = []string{"setup", "lock", "tag_create", "observe-locked", "append", "observe-appended", "ping", "force-break", "fenced",
		"observe-fenced", "takeover"}
	rbdJournalForcedBreakStages = []string{"observe-appended", "observe-fenced", "takeover"}
)

// rbdJournalForcedBreakFenced are the labels of the requests the fenced
// subject sends once each, in this order, after the forced break.
var rbdJournalForcedBreakFenced = []string{"watch_ping_fenced", "journal_append_fenced", "lock_unlock_fenced"}

// rbdJournalOwnerLoss is the owner-loss and forced-break variants' own
// record.
type rbdJournalOwnerLoss struct {
	// Forced is set in the forced-break variant.
	Forced bool
	// LockerAddress is the address of the subject's lock record as native
	// librbd listed it while the subject held the lock (observe-locked).
	LockerAddress string
	// TakeoverEvents are the planned takeover writes as the AioWrite events
	// rbd journal inspect shows for them.
	TakeoverEvents []rbdJournalLostAppendEvent
	// ExpectedHeadTakeover is the image after the setup, the owned entry
	// and then the takeover writes.
	ExpectedHeadTakeover string
	// ExpectedCommitTakeover is the image client's commit position once the
	// takeover committed the owned entry and its own entries.
	ExpectedCommitTakeover []rbdJournalPosition
	Takeover               *rbdJournalTakeover
	// ObserverWatchCookie is the observer's image header watch, registered
	// before the subject's loss and removed after the takeover (owner-loss
	// variant only).
	ObserverWatchCookie uint64
	// WatchAgeAtTakeover is the time from the subject's watch registration
	// to the takeover request (owner-loss variant only).
	WatchAgeAtTakeover string
	// The forced-break variant only.
	Break       *rbdJournalBreak
	PingBefore  *rbdLockOwnerLossAttempt
	Fenced      []rbdLockOwnerLossAttempt
	FencedEpoch uint32
	// Probe is the second entry the fenced append carried.
	Probe *rbdJournalLostAppendEntry
}

// rbdJournalTakeover is the takeover action's own output, and in the
// owner-loss variant the header notifications the observer received while
// it ran.
type rbdJournalTakeover struct {
	Owner                      bool
	WriteNS                    int64
	Events                     []rbdJournalLostAppendEvent
	BlocklistBefore, Blocklist []rbdLockOwnerLossBlock
	// NativeGlobalID is the takeover instance's global ID.
	NativeGlobalID uint64
	// The boundary: the lock record and the header watchers the takeover
	// listed after its open and right before its first write, that is
	// before its lock acquisition started.
	BoundaryLockers   []rbdJournalLockRecord
	BoundaryTag       string
	BoundaryExclusive bool
	BoundaryWatchers  []rbdJournalWatch
	// Notes are the observer's header notifications received from the
	// takeover's request to its return, in receipt order; NoteFailure is the
	// observer responder's failure, if any.
	Notes       []rbdLockNotification
	NoteFailure string
	// LockPath is the takeover's lock acquisition as librbd's debug log
	// shows it.
	LockPath *rbdJournalLockPath
}

// rbdJournalLockPath is one native action's lock steps as librbd's debug log
// at level 10 shows them: each kept line of managed_lock/AcquireRequest.cc,
// managed_lock/BreakRequest.cc and ExclusiveLock.cc post_acquire_lock_handler
// as "<request>.<function> <result>", and ImageWatcher.cc notify_request_lock
// as "watcher.notify_request_lock", in the order logged (all at
// ceph/ceph/blob/v20.2.4/src/librbd/). Present, Matched and Truncated are
// the driver's: a missing log file, or more lines than it keeps, leaves the
// path unjudgeable.
type rbdJournalLockPath struct {
	Present, Truncated bool
	Matched            int
	Steps              []string
}

// rbdJournalLockStep is one kept line: an optional object ordinal, the
// function and its result, or BreakRequest's verdict on a matching watcher.
var rbdJournalLockStep = regexp.MustCompile(`^(?:@[0-9]+ )?([a-z_]+): (r=-?[0-9]+|lock owner is still alive)$`)

// rbdJournalLockSteps are the functions a path keeps. Every one logs "r=" on
// entry at level 10; BreakRequest's handle_get_watchers also logs "lock
// owner is still alive" for a watcher whose address and handle match the
// locker's.
var rbdJournalLockSteps = map[string]bool{
	"acquire.handle_get_locker": true, "acquire.handle_lock": true, "acquire.handle_break_lock": true,
	"break.handle_get_watchers": true, "break.handle_get_locker": true, "break.handle_blocklist": true, "break.handle_wait_for_osd_map": true,
	"break.handle_break_lock": true, "break.finish": true, "exclusive.post_acquire_lock_handler": true,
}

// rbdJournalLockPathOf projects the driver's lock log, nil without one.
func rbdJournalLockPathOf(l *nativeRBDJournalLockLog) *rbdJournalLockPath {
	if l == nil {
		return nil
	}
	p := &rbdJournalLockPath{Present: l.Present, Truncated: l.Truncated, Matched: l.Matched, Steps: []string{}}
	short := map[string]string{"librbd::managed_lock::AcquireRequest": "acquire", "librbd::managed_lock::BreakRequest": "break", "librbd::ExclusiveLock": "exclusive"}
	for _, line := range l.Lines {
		if line.Component == "librbd::ImageWatcher" {
			if strings.HasSuffix(line.Text, " notify request lock") {
				p.Steps = append(p.Steps, "watcher.notify_request_lock")
			}
			continue
		}
		m := rbdJournalLockStep.FindStringSubmatch(line.Text)
		if short[line.Component] == "" || m == nil {
			continue
		}
		step := short[line.Component] + "." + m[1]
		if rbdJournalLockSteps[step] && (strings.HasPrefix(m[2], "r=") || step == "break.handle_get_watchers") {
			p.Steps = append(p.Steps, step+" "+m[2])
		}
	}
	return p
}

// The lock paths the claims expect. A free lock is locked at once
// (AcquireRequest.cc handle_get_locker -ENOENT, send_lock).
var rbdJournalFreeLockPath = []string{"acquire.handle_get_locker r=-2", "acquire.handle_lock r=0", "exclusive.post_acquire_lock_handler r=0"}

// rbdJournalForcedBreakLockPath is lock_break's BreakRequest with
// force_break_lock: it reports the live owner's watch and goes on, rereads
// the locker, blocklists its address, waits for the latest OSDMap and then
// breaks the lock.
var rbdJournalForcedBreakLockPath = []string{"break.handle_get_watchers r=0", "break.handle_get_watchers lock owner is still alive", "break.handle_get_locker r=0",
	"break.handle_blocklist r=0", "break.handle_wait_for_osd_map r=0", "break.handle_break_lock r=0", "break.finish r=0"}

// rbdJournalOwnerLossLockPath is the takeover of a lock whose owner's watch
// is listed for the first requests attempts. Each such attempt finds the
// lock held (-EBUSY), and BreakRequest finds the locker's address and handle
// among the watchers and ends with -EAGAIN, so post_acquire_lock_handler
// sends a RequestLock. The last attempt finds no such watcher, rereads the
// locker, blocklists its address, waits for the latest OSDMap, breaks the
// lock and locks it.
func rbdJournalOwnerLossLockPath(requests int) []string {
	var path []string
	for range requests {
		path = append(path, "acquire.handle_get_locker r=0", "acquire.handle_lock r=-16", "break.handle_get_watchers r=0", "break.handle_get_watchers lock owner is still alive",
			"break.finish r=-11", "acquire.handle_break_lock r=-11", "exclusive.post_acquire_lock_handler r=-11", "watcher.notify_request_lock")
	}
	return append(path, "acquire.handle_get_locker r=0", "acquire.handle_lock r=-16", "break.handle_get_watchers r=0", "break.handle_get_locker r=0",
		"break.handle_blocklist r=0", "break.handle_wait_for_osd_map r=0", "break.handle_break_lock r=0", "break.finish r=0", "acquire.handle_break_lock r=0",
		"acquire.handle_lock r=0", "exclusive.post_acquire_lock_handler r=0")
}

// rbdJournalLockPathClaims checks one action's lock path against want.
func rbdJournalLockPathClaims(name string, p *rbdJournalLockPath, want []string, fail func(string, ...any)) {
	if p == nil || !p.Present || p.Truncated || p.Matched == 0 || !slices.Equal(p.Steps, want) {
		text := func(v any) string { data, _ := json.Marshal(v); return string(data) }
		fail("%s: librbd's lock log does not show the expected lock path %v: %s", name, want, text(p))
	}
}

// rbdJournalLockRecord is one locker of the image header's lock record.
type rbdJournalLockRecord struct{ Client, Cookie, Address string }

// rbdJournalWatch is one header watcher as native librbd lists it.
type rbdJournalWatch struct {
	GlobalID, Cookie uint64
	Address          string
}

// rbdJournalBreak is the force-break action's own output.
type rbdJournalBreak struct {
	Owners, OwnersAfter []string
	IsOwner             bool
	BreakNS             int64
	// The lock record (client, cookie and address) and the header watchers
	// the breaking instance read right before lock_break.
	LockerBefore, LockerCookieBefore, LockerAddressBefore string
	WatchersBefore                                        []rbdJournalWatch
	BlocklistBefore, Blocklist                            []rbdLockOwnerLossBlock
	// LockPath is lock_break as librbd's debug log shows it.
	LockPath *rbdJournalLockPath
}

// rbdJournalBlocked says whether address has an exact blocklist entry.
func rbdJournalBlocked(blocks []rbdLockOwnerLossBlock, address string) bool {
	return slices.ContainsFunc(blocks, func(b rbdLockOwnerLossBlock) bool { return b.Address == address })
}

// rbdJournalTakeoverClaims checks the takeover of both variants: native
// librbd owned the lock for its writes and released it, the subject's lock
// record address is blocklisted (before the takeover only in the
// forced-break variant), the journal holds the owned entry once followed
// only by native librbd's own entries, the image and the commit position are
// those of the replay of the owned entry followed by the takeover writes,
// and the new tag's predecessor is the owned entry. before is the
// observation whose other journal objects the takeover must not change.
func rbdJournalTakeoverClaims(r *rbdJournalLostAppendRecord, v, before, locked, setup *rbdJournalLostAppendView, fail func(string, ...any)) {
	text := func(v any) string { data, _ := json.Marshal(v); return string(data) }
	ol, e := r.OwnerLoss, r.Entry
	tk := ol.Takeover
	if tk == nil {
		fail("takeover: native output missing")
		return
	}
	if !tk.Owner || tk.WriteNS <= 0 || !v.LockFree || v.SubjectWatcher {
		fail("takeover: native librbd did not own the lock for its writes and release it, or the subject's watch is still listed (owner=%v free=%v subject watcher=%v)", tk.Owner, v.LockFree, v.SubjectWatcher)
	}
	rbdJournalTakeoverBoundaryClaims(r, fail)
	if ol.LockerAddress == "" || !rbdJournalBlocked(tk.Blocklist, ol.LockerAddress) || !rbdJournalBlocked(v.Blocklist, ol.LockerAddress) ||
		rbdJournalBlocked(tk.BlocklistBefore, ol.LockerAddress) != ol.Forced {
		fail("takeover: the subject's lock record address %q is not blocklisted after the takeover, or its blocklisting before it does not match the variant (forced=%v): before %s, after %s",
			ol.LockerAddress, ol.Forced, text(tk.BlocklistBefore), text(tk.Blocklist))
	}
	writes := []rbdJournalLostAppendEvent{}
	for _, w := range tk.Events {
		writes = append(writes, rbdJournalLostAppendEvent{Type: w.Type, Offset: w.Offset, Length: w.Length})
	}
	planned := []rbdJournalLostAppendEvent{}
	for _, w := range ol.TakeoverEvents {
		planned = append(planned, rbdJournalLostAppendEvent{Type: w.Type, Offset: w.Offset, Length: w.Length})
	}
	if len(planned) == 0 || !reflect.DeepEqual(writes, planned) {
		fail("takeover: the native writes differ from the plan: %s, want %s", text(tk.Events), text(ol.TakeoverEvents))
	}
	// Journal: the owned entry where it was, then native entries only.
	if v.PrefixSHA256 == nil || *v.PrefixSHA256 != locked.ObjectSHA256[e.Object] || v.EntrySHA256 == nil || *v.EntrySHA256 != e.SHA256 || v.TrailingBytes == nil || *v.TrailingBytes == 0 {
		fail("takeover: journal object %d is not its earlier bytes, the owned entry and native librbd's entries (entry sha256 %s, trailing bytes %s)", e.Object, text(v.EntrySHA256), text(v.TrailingBytes))
	}
	for i := range v.ObjectSizes {
		if uint64(i) != e.Object && (i >= len(before.ObjectSHA256) || v.ObjectSHA256[i] != before.ObjectSHA256[i] || v.ObjectSizes[i] != before.ObjectSizes[i]) {
			fail("takeover: journal object %d changed", i)
		}
	}
	want := rbdJournalLostAppendEvent{Type: "AioWrite", Offset: e.ImageOffset, Length: e.ImageLength,
		Timestamp: time.Unix(int64(e.Timestamp.Seconds), int64(e.Timestamp.Nanoseconds)/1000*1000).UTC().Format(time.RFC3339Nano)}
	next := []rbdJournalLostAppendEvent{}
	for _, n := range v.InspectNext {
		next = append(next, rbdJournalLostAppendEvent{Type: n.Type, Offset: n.Offset, Length: n.Length})
	}
	if v.InspectTotal == nil || v.InspectErrors == nil || setup.InspectTotal == nil || *v.InspectTotal != *setup.InspectTotal+1+len(planned) || *v.InspectErrors != 0 ||
		len(v.InspectOwned) != 1 || v.InspectOwned[0] != want || !reflect.DeepEqual(next, planned) {
		fail("takeover: rbd journal inspect does not show the setup entries, the owned entry once and then only the takeover's entries (owned %s, next %s)", text(v.InspectOwned), text(v.InspectNext))
	}
	// Replay before the writes.
	if ol.ExpectedHeadTakeover == "" || ol.ExpectedHeadTakeover == r.ExpectedHeadAfter || ol.ExpectedHeadTakeover == r.ExpectedHeadBefore || v.ImageHeadSHA256 != ol.ExpectedHeadTakeover {
		fail("takeover: image differs from the replay of the owned entry followed by the takeover writes (head=%s)", v.ImageHeadSHA256)
	}
	if len(ol.ExpectedCommitTakeover) == 0 || ol.ExpectedCommitTakeover[0] != (rbdJournalPosition{e.Object, e.Tag + 1, 0}) ||
		!reflect.DeepEqual(v.ImageCommit, ol.ExpectedCommitTakeover) {
		fail("takeover: image client commit position differs from the takeover's entries after the owned entry (commit=%v)", v.ImageCommit)
	}
	if v.LastTag != (rbdJournalLostAppendTag{e.Tag + 1, true, e.Tag, e.EntryTID}) || v.NextTag != e.Tag+2 {
		fail("takeover: native tag differs (tag=%+v next=%d)", v.LastTag, v.NextTag)
	}
}

// rbdJournalTakeoverBoundaryClaims checks what the takeover listed when it
// started and, in the owner-loss variant, the notifications it sent.
// In both variants the boundary is what the takeover instance listed after
// its open and right before its first write, before its lock acquisition
// started: the lock record and the header watchers, its own watch among them.
//
// Owner-loss variant: the boundary lists the subject's lock record (client,
// cookie and address) and the subject's watch with that cookie at that
// address, the pair librbd's liveness check compares. The observer's header
// watch then received, from the takeover's instance and watch handle, at
// least one RequestLock, followed only by its AcquiredLock and its
// ReleasedLock, each acknowledged empty. These are the claims: the subject
// was listed when the takeover started, and the takeover asked for the lock
// before it announced it. The RequestLock does not name the branch that
// sent it. At v20.2.4 ExclusiveLock.cc post_acquire_lock_handler sends it
// for -EBUSY (managed_lock/AcquireRequest.cc handle_get_locker, an
// incompatible lock) and for -EAGAIN, which managed_lock/BreakRequest.cc
// returns from handle_get_watchers (a watcher matches the locker's address
// and handle) and from handle_get_locker (the locker changed on the
// recheck). librbd's debug log of the same acquisition names the branch:
// one blocked attempt per RequestLock the observer received, each ended by
// the live-watcher branch, then an attempt that finds no matching watcher,
// blocklists the address, waits for the latest OSDMap, breaks the lock and
// takes it (rbdJournalOwnerLossLockPath). The observation after the
// takeover shows the address blocklisted, the lock free and the watch gone.
//
// Forced-break variant: the boundary lists no lock record and not the
// subject's watch, and librbd's debug log shows the takeover taking a free
// lock (AcquireRequest.cc handle_get_locker -ENOENT, then send_lock). The
// variant keeps no observer watch and judges no notification.
func rbdJournalTakeoverBoundaryClaims(r *rbdJournalLostAppendRecord, fail func(string, ...any)) {
	text := func(v any) string { data, _ := json.Marshal(v); return string(data) }
	ol := r.OwnerLoss
	tk := ol.Takeover
	own := slices.IndexFunc(tk.BoundaryWatchers, func(w rbdJournalWatch) bool { return w.GlobalID == tk.NativeGlobalID })
	if tk.NativeGlobalID == 0 || tk.NativeGlobalID == r.SubjectGlobalID || tk.NativeGlobalID == r.ObserverGlobalID || tk.BoundaryLockers == nil || own < 0 {
		fail("takeover boundary: the takeover's identity, its lock record or its own header watch is missing (client.%d, lockers %s, watchers %s)",
			tk.NativeGlobalID, text(tk.BoundaryLockers), text(tk.BoundaryWatchers))
		return
	}
	listed := slices.Contains(tk.BoundaryWatchers, rbdJournalWatch{r.SubjectGlobalID, r.WatchCookie, ol.LockerAddress})
	if ol.Forced {
		if len(tk.BoundaryLockers) != 0 || slices.ContainsFunc(tk.BoundaryWatchers, func(w rbdJournalWatch) bool { return w.GlobalID == r.SubjectGlobalID }) {
			fail("takeover boundary: the lock is not free or the subject's watch is listed when the takeover starts (lockers %s, watchers %s)", text(tk.BoundaryLockers), text(tk.BoundaryWatchers))
		}
		if tk.Notes != nil || tk.NoteFailure != "" || ol.ObserverWatchCookie != 0 {
			fail("takeover notifications: the forced-break variant keeps no observer watch")
		}
		rbdJournalLockPathClaims("takeover lock path", tk.LockPath, rbdJournalFreeLockPath, fail)
		return
	}
	locker := rbdJournalLockRecord{fmt.Sprintf("client.%d", r.SubjectGlobalID), r.LockCookie, ol.LockerAddress}
	if !slices.Equal(tk.BoundaryLockers, []rbdJournalLockRecord{locker}) || tk.BoundaryTag != rbdLockTag || !tk.BoundaryExclusive || !listed {
		fail("takeover boundary: the takeover did not start with the subject's lock record %s and its matching watch listed (lockers %s, tag %q, exclusive %v, watchers %s)",
			text(locker), text(tk.BoundaryLockers), tk.BoundaryTag, tk.BoundaryExclusive, text(tk.BoundaryWatchers))
	}
	native := rbdClientID{GID: tk.NativeGlobalID, Handle: tk.BoundaryWatchers[own].Cookie}
	notes, requests := tk.Notes, rbdJournalRequestLocks(tk)
	ok := tk.NoteFailure == "" && ol.ObserverWatchCookie != 0 && requests > 0 && len(notes) == requests+2 &&
		notes[requests].Op == rbdNotifyAcquiredLock && notes[requests+1].Op == rbdNotifyReleasedLock
	for _, n := range notes {
		ok = ok && n.Client == native && n.NotifierGlobalID == native.GID && n.Cookie == ol.ObserverWatchCookie && !n.Force && !n.Own && n.Role == "non-owner" &&
			n.AckDisposition == rbdAckSubmittedEmpty && n.AckError == "" && !n.Unexpected
	}
	if !ok {
		fail("takeover notifications: the observer did not receive at least one RequestLock from the takeover %s, then only its AcquiredLock and its ReleasedLock, each acknowledged empty (failure %q): %s",
			text(native), tk.NoteFailure, text(notes))
	}
	// librbd's own log of the same acquisition: one blocked attempt per
	// RequestLock the observer received, each ended by the live-watcher
	// branch, then the break and the lock.
	rbdJournalLockPathClaims("takeover lock path", tk.LockPath, rbdJournalOwnerLossLockPath(requests), fail)
}

// rbdJournalRequestLocks counts the RequestLock notifications that open the
// observer's notifications of the takeover.
func rbdJournalRequestLocks(tk *rbdJournalTakeover) int {
	n := 0
	for n < len(tk.Notes) && tk.Notes[n].Op == rbdNotifyRequestLock {
		n++
	}
	return n
}

// rbdJournalForcedBreakClaims checks the forced-break record up to one
// stage. It keeps apart what native clients saw before the break (the
// append, from "observe-appended" on), the break of a live owner and what
// the fenced owner was told (from "observe-fenced" on), and the native
// takeover ("takeover").
func rbdJournalForcedBreakClaims(r *rbdJournalLostAppendRecord, stage string) []string {
	var problems []string
	fail := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	text := func(v any) string { data, _ := json.Marshal(v); return string(data) }
	ol, e := r.OwnerLoss, r.Entry
	reached := slices.Index(rbdJournalForcedBreakStages, stage)
	if reached < 0 {
		return []string{"unknown stage " + stage}
	}
	view := func(name string, from int) *rbdJournalLostAppendView {
		if reached < from {
			return nil
		}
		v := r.Views[name]
		if v == nil || v.Action != name || int(e.Object)+1 >= len(v.ObjectSizes) || len(v.ObjectSizes) != len(v.ObjectSHA256) {
			fail("%s: native observation missing or incomplete", name)
			return nil
		}
		return v
	}
	setup, locked, appended := view("setup", 0), view("observe-locked", 0), view("observe-appended", 0)
	broke, fenced, takeover := view("force-break", 1), view("observe-fenced", 1), view("takeover", 2)
	if len(problems) != 0 {
		return problems
	}
	if want := rbdJournalForcedBreakOrder[:slices.Index(rbdJournalForcedBreakOrder, stage)+1]; !reflect.DeepEqual(r.Order, want) {
		fail("order: steps completed as %v, want %v", r.Order, want)
	}
	if e.Length <= 0 || len(e.SHA256) != 64 || e.Tag == 0 || r.SubjectGlobalID == 0 || r.SubjectGlobalID == r.ObserverGlobalID || r.WatchCookie == 0 {
		fail("identity: entry or client identity incomplete")
	}
	owner := fmt.Sprintf("client.%d", r.SubjectGlobalID)
	held := func(name string, v *rbdJournalLostAppendView) {
		if v.LockFree || v.LockOwner != owner || v.LockCookie != r.LockCookie || !v.SubjectWatcher {
			fail("%s: the subject does not hold the lock with its watch (free=%v owner=%q cookie=%q watcher=%v)", name, v.LockFree, v.LockOwner, v.LockCookie, v.SubjectWatcher)
		}
	}
	unmoved := func(name string, v *rbdJournalLostAppendView) {
		if v.ImageHeadSHA256 != r.ExpectedHeadBefore || !reflect.DeepEqual(v.ImageCommit, setup.ImageCommit) || v.NextTag != e.Tag+1 || v.LastTag.TID != e.Tag {
			fail("%s: image, image client commit position or tags moved (head=%s commit=%v next tag=%d)", name, v.ImageHeadSHA256, v.ImageCommit, v.NextTag)
		}
	}
	entryAt := func(name string, v *rbdJournalLostAppendView) {
		if v.PrefixSHA256 == nil || *v.PrefixSHA256 != locked.ObjectSHA256[e.Object] || v.EntrySHA256 == nil || *v.EntrySHA256 != e.SHA256 ||
			v.TrailingBytes == nil || *v.TrailingBytes != 0 || v.ObjectSizes[e.Object] != uint64(e.Offset+e.Length) {
			fail("%s: journal object %d is not its earlier bytes followed by exactly the owned entry (size %d, entry sha256 %s, trailing bytes %s)",
				name, e.Object, v.ObjectSizes[e.Object], text(v.EntrySHA256), text(v.TrailingBytes))
		}
		for i := range v.ObjectSizes {
			if uint64(i) != e.Object && (i >= len(locked.ObjectSHA256) || v.ObjectSHA256[i] != locked.ObjectSHA256[i] || v.ObjectSizes[i] != locked.ObjectSizes[i]) {
				fail("%s: journal object %d changed", name, i)
			}
		}
	}
	blocked := func(name string, blocks []rbdLockOwnerLossBlock, want bool) {
		if blocks == nil || rbdJournalBlocked(blocks, ol.LockerAddress) != want {
			fail("%s: the subject's lock record address %q blocklisted is not %v: %s", name, ol.LockerAddress, want, text(blocks))
		}
	}

	// Before the break: one append with a known answer, natively seen.
	if locked.EntrySHA256 != nil || locked.ObjectSizes[e.Object] != uint64(e.Offset) || !reflect.DeepEqual(locked.ObjectSHA256, setup.ObjectSHA256) {
		fail("observe-locked: journal objects changed before the append, or the entry offset is not the object size")
	}
	held("observe-locked", locked)
	unmoved("observe-locked", locked)
	if ol.LockerAddress == "" {
		fail("identity: the subject's lock record address is missing")
	}
	o := r.Outcome
	if o == nil || o.Class != "applied" || o.UnknownOutcome || o.Canceled || o.CallerInvocations != 1 || o.Submission.Transaction == 0 || o.Submission.GlobalID != r.SubjectGlobalID ||
		o.ReplyTransaction != o.Submission.Transaction || o.ReplyCode != 0 || o.RecordedUTC == "" {
		fail("append: not one known answer 0 to its one submission: %s", text(o))
	}
	entryAt("observe-appended", appended)
	held("observe-appended", appended)
	unmoved("observe-appended", appended)
	blocked("observe-appended", appended.Blocklist, false)
	if g := r.Gate; g != nil && (g.Bound || g.Connections != 1) {
		fail("transmission: the subject's requests did not stay on one unwithheld connection: gate %s", text(g))
	}
	if reached < 1 {
		return problems
	}

	// The break of a live owner, and its fence.
	b := ol.Break
	if p := ol.PingBefore; p == nil || p.Class != "applied" || p.Code != 0 || p.Invocations != 1 {
		fail("ping: the subject's watch was not renewed before the break: %s", text(p))
	}
	if b == nil || !slices.Equal(b.Owners, []string{ol.LockerAddress}) || b.LockerBefore != owner || b.LockerCookieBefore != r.LockCookie || b.LockerAddressBefore != ol.LockerAddress ||
		// The subject's watch as librbd's liveness check matches an owner:
		// its cookie at the lock record's address.
		!slices.Contains(b.WatchersBefore, rbdJournalWatch{r.SubjectGlobalID, r.WatchCookie, ol.LockerAddress}) ||
		len(b.OwnersAfter) != 0 || b.IsOwner || b.BreakNS <= 0 {
		fail("force-break: native librbd did not break the lock of the one live owner, the subject: %s", text(b))
	} else {
		blocked("force-break (before)", b.BlocklistBefore, false)
		blocked("force-break (after)", b.Blocklist, true)
		rbdJournalLockPathClaims("force-break lock path", b.LockPath, rbdJournalForcedBreakLockPath, fail)
	}
	entryAt("force-break", broke)
	unmoved("force-break", broke)
	if !broke.LockFree {
		fail("force-break: the lock is still held after the break")
	}
	blocked("force-break", broke.Blocklist, true)
	labels := []string{}
	for _, a := range ol.Fenced {
		labels = append(labels, a.Label)
		if a.Invocations != 1 || a.Class != "refused" || a.ErrorCode != -108 || a.UnknownOutcome || a.Canceled {
			fail("fenced: %s is not one known -108 refusal: %s", a.Label, text(a))
		}
	}
	if !slices.Equal(labels, rbdJournalForcedBreakFenced) || ol.FencedEpoch == 0 {
		fail("fenced: not the ping, the append and the unlock once each at a known epoch: %v, epoch %d", labels, ol.FencedEpoch)
	}
	if p := ol.Probe; p == nil || p.Tag != e.Tag || p.EntryTID != e.EntryTID+1 || p.Object != e.Object+1 || p.Offset != int(locked.ObjectSizes[e.Object+1]) || p.Length <= 0 {
		fail("fenced: the fenced append is not the subject's next entry at its splay offset: %s", text(p))
	}
	// Nothing the fenced owner sent changed the journal.
	if !reflect.DeepEqual(fenced.ObjectSHA256, appended.ObjectSHA256) || !reflect.DeepEqual(fenced.ObjectSizes, appended.ObjectSizes) {
		fail("observe-fenced: journal objects changed after the append")
	}
	unmoved("observe-fenced", fenced)
	if !fenced.LockFree || fenced.SubjectWatcher {
		fail("observe-fenced: the lock is held or the subject's watch is still listed (free=%v watcher=%v)", fenced.LockFree, fenced.SubjectWatcher)
	}
	blocked("observe-fenced", fenced.Blocklist, true)
	if reached < 2 {
		return problems
	}
	rbdJournalTakeoverClaims(r, takeover, fenced, locked, setup, fail)
	return problems
}

// rbdJournalTakeoverOf projects the takeover action's own output.
func rbdJournalTakeoverOf(n *nativeRBDJournal) *rbdJournalTakeover {
	tk := &rbdJournalTakeover{Owner: n.Owner != nil && *n.Owner, WriteNS: n.WriteNS, Events: []rbdJournalLostAppendEvent{}, NativeGlobalID: n.NativeGlobalID,
		LockPath: rbdJournalLockPathOf(n.LockLog)}
	if b := n.Boundary; b != nil {
		tk.BoundaryLockers, tk.BoundaryWatchers = []rbdJournalLockRecord{}, []rbdJournalWatch{}
		tk.BoundaryTag, tk.BoundaryExclusive = b.Lockers.Tag, b.Lockers.Exclusive
		for _, l := range b.Lockers.Lockers {
			tk.BoundaryLockers = append(tk.BoundaryLockers, rbdJournalLockRecord{l.Client, l.Cookie, l.Address})
		}
		for _, w := range b.Watchers {
			tk.BoundaryWatchers = append(tk.BoundaryWatchers, rbdJournalWatch{w.ID, w.Cookie, w.Address})
		}
	}
	for _, e := range n.Events {
		if e.Kind == "write" {
			tk.Events = append(tk.Events, rbdJournalLostAppendEvent{Type: "AioWrite", Offset: e.Offset, Length: e.Length})
		} else {
			tk.Events = append(tk.Events, rbdJournalLostAppendEvent{Type: e.Kind, Offset: e.Offset, Length: e.Length})
		}
	}
	if n.BlocklistBefore != nil {
		tk.BlocklistBefore, _ = rbdLockOwnerLossBlocks(*n.BlocklistBefore)
	}
	if n.Blocklist != nil {
		tk.Blocklist, _ = rbdLockOwnerLossBlocks(*n.Blocklist)
	}
	return tk
}

// rbdJournalForcedBreakSteps hands the forced-break variant the subject's
// state and the closures of runRBDJournalLostAppend once its lock, tag and
// owned entry exist and the observation of the locked image is recorded.
type rbdJournalForcedBreakSteps struct {
	ctx         context.Context
	record      *rbdJournalLostAppendRecord
	entry       *rbdJournalLostAppendEntry
	header      string
	plan        nativeRBDJournalPlan
	token       string
	gid         uint64
	w           *cephmsgr.OSDWatch
	conn        *cephmsgr.OSDConnection
	appendInput []byte
	splay       uint64
	locked      *nativeRBDJournal
	route       func(*testing.T, string) rbdRoute
	request     func(rbdRoute, ...cephmsgr.OSDOperation) cephmsgr.OSDRequest
	keep        func(string, rbdRoute, cephmsgr.OSDRequest, cephmsgr.OSDReply, error)
	now         func() cephmsgr.OSDTime
	send        func(*testing.T, *cephmsgr.OSDConnection, uint64, string, string, ...cephmsgr.OSDOperation) cephmsgr.OSDReply
	native      func(*testing.T, string) *nativeRBDJournal
	judge       func(*testing.T, string)
	done        func(string)
	released    func()
}

// runRBDJournalForcedBreak is the forced-break variant after the
// observation of the locked image: the append with a known answer, its
// observation, the forced break by native librbd, the fenced requests, their
// observation and the native takeover.
func runRBDJournalForcedBreak(t *testing.T, s rbdJournalForcedBreakSteps) {
	r, e, ol := s.record, s.entry, s.record.OwnerLoss
	classOp := func(class, method string, mode cephmsgr.OSDCallMode, input []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: mode, MaxOutputBytes: 65536}}
	}
	ctx := s.ctx

	// The append, answered: the subject neither applies nor commits it.
	reply := s.send(t, s.conn, s.gid, "journal_append", e.ObjectName, classOp("journal", "append", cephmsgr.OSDCallWrite, s.appendInput))
	r.Outcome = &rbdJournalLostAppendOutcome{Label: "journal_append", CallerInvocations: 1, Class: "applied", Submission: reply.Submission, ReplyTransaction: reply.Transaction,
		ReplyCode: reply.Code, RecordedUTC: time.Now().UTC().Format(time.RFC3339Nano)}
	s.done("append")
	s.native(t, "observe-appended")
	s.judge(t, "observe-appended")

	// The subject's watch is live: it renews it right before the break.
	headerRoute := s.route(t, s.header)
	watchRequest := s.request(headerRoute)
	watchRequest.Operations = nil
	ping, err := s.w.Ping(ctx, headerRoute.Epoch)
	s.keep("watch_ping", headerRoute, watchRequest, ping, err)
	attempt := rbdLockOwnerLossOutcome("watch_ping", ping, err)
	ol.PingBefore = &attempt
	if err != nil {
		t.Fatal("subject watch ping before the break", err)
	}
	s.done("ping")

	// Native librbd breaks the lock of the live owner; the subject's
	// compensating unlock of a failed run would no longer name its lock.
	s.released()
	b, err := rbdJournalBreakOf(s.native(t, "force-break"), r.Views["force-break"])
	if err != nil {
		t.Fatal(err)
	}
	ol.Break = b

	// The fenced owner: a ping, the append of its next entry and the
	// unlock, each sent once on the connection that sent lock.lock and
	// naming the OSDMap epoch read after the break, so that the OSD judges
	// them with the blocklist entry. Each answer is recorded before it is
	// judged, and nothing is sent again.
	headerRoute = s.route(t, s.header)
	ol.FencedEpoch = headerRoute.Epoch
	extent := s.plan.GoWrites[1]
	at := s.now()
	probeBytes := rbdJournalEntryEncode(e.Tag, e.EntryTID+1, rbdJournalWriteEvent(extent.Offset, rbdJournalPattern(s.token, "go-fenced-append", extent.Offset, extent.Length), at))
	probeObject := e.Object + 1
	prefix := e.ObjectName[:len(e.ObjectName)-len(strconv.FormatUint(e.Object, 10))]
	ol.Probe = &rbdJournalLostAppendEntry{Tag: e.Tag, EntryTID: e.EntryTID + 1, Object: probeObject, ObjectName: prefix + strconv.FormatUint(probeObject, 10),
		Offset: int(s.locked.JournalObjects[probeObject].Size), Length: len(probeBytes), SHA256: rbdJournalDigest(probeBytes), ImageOffset: extent.Offset, ImageLength: extent.Length, Timestamp: at}
	if probeObject%s.splay == 0 {
		t.Fatal("the fenced entry's splay offset is in another object set")
	}
	fence := func(label string, route rbdRoute, req cephmsgr.OSDRequest, call func() (cephmsgr.OSDReply, error)) {
		reply, err := call()
		s.keep(label, route, req, reply, err)
		ol.Fenced = append(ol.Fenced, rbdLockOwnerLossOutcome(label, reply, err))
	}
	fence("watch_ping_fenced", headerRoute, watchRequest, func() (cephmsgr.OSDReply, error) { return s.w.Ping(ctx, headerRoute.Epoch) })
	probeRoute := s.route(t, ol.Probe.ObjectName)
	// journal.append input: the journal object's soft limit, as for the
	// owned entry, then the entry bytes.
	probe := s.request(probeRoute, classOp("journal", "append", cephmsgr.OSDCallWrite, append(slices.Clone(s.appendInput[:8]), rbdJournalBL(probeBytes)...)))
	probe.ModificationTime = s.now()
	fence("journal_append_fenced", probeRoute, probe, func() (cephmsgr.OSDReply, error) { return s.conn.Request(ctx, probe) })
	unlock := s.request(headerRoute, classOp("lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(r.LockCookie)))
	unlock.ModificationTime = s.now()
	fence("lock_unlock_fenced", headerRoute, unlock, func() (cephmsgr.OSDReply, error) { return s.conn.Request(ctx, unlock) })
	s.done("fenced")
	s.native(t, "observe-fenced")
	s.judge(t, "observe-fenced")

	taken := s.native(t, "takeover")
	ol.Takeover = rbdJournalTakeoverOf(taken)
	s.judge(t, "takeover")
	t.Logf("go journal forced break tag=%d entry=0 object=%s locker=%s break=%v fenced=%v epoch=%d takeover_write=%v",
		e.Tag, e.ObjectName, ol.LockerAddress, time.Duration(b.BreakNS), ol.Fenced, ol.FencedEpoch, time.Duration(taken.WriteNS))
}

// rbdJournalBreakOf projects the force-break action's own output and the
// blocklist its observation read.
func rbdJournalBreakOf(broke *nativeRBDJournal, view *rbdJournalLostAppendView) (*rbdJournalBreak, error) {
	fb := broke.ForceBreak
	if fb == nil {
		return nil, errors.New("native force-break output missing")
	}
	b := &rbdJournalBreak{IsOwner: fb.IsOwner, BreakNS: fb.BreakNS, Owners: []string{}, OwnersAfter: []string{}, WatchersBefore: []rbdJournalWatch{}, Blocklist: view.Blocklist,
		LockPath: rbdJournalLockPathOf(broke.LockLog)}
	for _, o := range fb.Owners {
		b.Owners = append(b.Owners, o.Owner)
	}
	for _, o := range fb.OwnersAfter {
		b.OwnersAfter = append(b.OwnersAfter, o.Owner)
	}
	if len(fb.LockersBefore.Lockers) == 1 {
		l := fb.LockersBefore.Lockers[0]
		b.LockerBefore, b.LockerCookieBefore, b.LockerAddressBefore = l.Client, l.Cookie, l.Address
	}
	for _, w := range fb.WatchersBefore {
		b.WatchersBefore = append(b.WatchersBefore, rbdJournalWatch{w.ID, w.Cookie, w.Address})
	}
	var err error
	b.BlocklistBefore, err = rbdLockOwnerLossBlocks(fb.BlocklistBefore)
	return b, err
}

// rbdJournalDigest is the hex SHA-256 of data.
func rbdJournalDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
