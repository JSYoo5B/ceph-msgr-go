package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
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
// append was withheld and the subject's wait was canceled. The subject then
// dies as a lock owner: it closes its silent connection and never sends
// anything again, so it releases nothing, does not unwatch and does not
// renew its watch. A native librbd write (the takeover) asks that owner for
// the lock while its watch is still listed, and once the OSD dropped the
// watch it blocklists the address of the subject's lock record and breaks
// the lock (managed_lock/AcquireRequest.cc and BreakRequest.cc at
// ceph/ceph/blob/v20.2.4/src/librbd/), acquires it, opens the journal,
// replays the owned entry and journals and applies its own write, which
// covers the first half of the owned extent. The takeover's native
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
// lists. librbd does not ask whether that owner is alive: it blocklists the
// owner's address and breaks the lock while the subject's watch is still
// listed (internal.cc lock_break, ManagedLock.cc break_lock and
// managed_lock/BreakRequest.cc with force_break_lock at
// ceph/ceph/blob/v20.2.4/src/librbd/). The subject then sends, once each on
// the connection that sent lock.lock and naming the OSDMap epoch it read
// after the break, a watch ping, the append of its second entry and the
// unlock. The OSD answers each with -108 (EBLOCKLISTED), a known refusal,
// and none is sent again. A native observation shows every journal object
// unchanged since the append, the lock free and the subject's watch gone.
// Finally a native librbd write takes the free lock, replays the subject's
// entry and journals and applies its own write over the first half of that
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
		"observe-recorded", "blocked_connection_closed", "takeover"}
	rbdJournalOwnerLossStages = []string{"checkpoint", "observe-recorded", "takeover"}
)

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
	// The forced-break variant only.
	Break       *rbdJournalBreak
	PingBefore  *rbdLockOwnerLossAttempt
	Fenced      []rbdLockOwnerLossAttempt
	FencedEpoch uint32
	// Probe is the second entry the fenced append carried.
	Probe *rbdJournalLostAppendEntry
}

// rbdJournalTakeover is the takeover action's own output.
type rbdJournalTakeover struct {
	Owner                      bool
	WriteNS                    int64
	Events                     []rbdJournalLostAppendEvent
	BlocklistBefore, Blocklist []rbdLockOwnerLossBlock
}

// rbdJournalBreak is the force-break action's own output.
type rbdJournalBreak struct {
	Owners, OwnersAfter []string
	IsOwner             bool
	BreakNS             int64
	// The lock record and whether the subject's watch was listed, before
	// lock_break.
	LockerBefore, LockerAddressBefore string
	SubjectWatcherBefore              bool
	BlocklistBefore, Blocklist        []rbdLockOwnerLossBlock
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
	if b == nil || !slices.Equal(b.Owners, []string{ol.LockerAddress}) || b.LockerBefore != owner || b.LockerAddressBefore != ol.LockerAddress || !b.SubjectWatcherBefore ||
		len(b.OwnersAfter) != 0 || b.IsOwner || b.BreakNS <= 0 {
		fail("force-break: native librbd did not break the lock of the one live owner, the subject: %s", text(b))
	} else {
		blocked("force-break (before)", b.BlocklistBefore, false)
		blocked("force-break (after)", b.Blocklist, true)
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
	tk := &rbdJournalTakeover{Owner: n.Owner != nil && *n.Owner, WriteNS: n.WriteNS, Events: []rbdJournalLostAppendEvent{}}
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
	broke := s.native(t, "force-break")
	fb := broke.ForceBreak
	if fb == nil {
		t.Fatal("native force-break output missing")
	}
	b := &rbdJournalBreak{IsOwner: fb.IsOwner, BreakNS: fb.BreakNS, Owners: []string{}, OwnersAfter: []string{}}
	for _, o := range fb.Owners {
		b.Owners = append(b.Owners, o.Owner)
	}
	for _, o := range fb.OwnersAfter {
		b.OwnersAfter = append(b.OwnersAfter, o.Owner)
	}
	if len(fb.LockersBefore.Lockers) == 1 {
		b.LockerBefore, b.LockerAddressBefore = fb.LockersBefore.Lockers[0].Client, fb.LockersBefore.Lockers[0].Address
	}
	b.SubjectWatcherBefore = slices.ContainsFunc(fb.WatchersBefore, func(w nativeRBDWatcher) bool { return w.ID == s.gid && w.Cookie == s.w.Cookie() })
	if b.BlocklistBefore, err = rbdLockOwnerLossBlocks(fb.BlocklistBefore); err != nil {
		t.Fatal(err)
	}
	b.Blocklist = r.Views["force-break"].Blocklist
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

// rbdJournalDigest is the hex SHA-256 of data.
func rbdJournalDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
