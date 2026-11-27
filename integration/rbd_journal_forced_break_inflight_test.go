package integration_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephRBDJournalForcedBreakInflightIntegration breaks the lock of a live
// Go owner of a journaling image while the outcome of its journal append is
// in flight: the OSD has applied the append and nothing has reached the Go
// caller. It runs the lost-append sequence of
// TestCephRBDJournalLostAppendReplyIntegration with a live owner: the subject
// takes the image's exclusive lock, allocates its tag, renews its watch and
// invokes one AioWrite entry with `journal.append`, once, on the connection
// that sent lock.lock. A test-only gate on that connection withholds what
// comes back, and what it observes differs by row. On the CRC row it decodes
// the stream: it counts the MOSDOp frames that carry the entry (one), binds
// to that frame's transaction and holds the MOSDOpReply frame of the same
// transaction, so the OSD's answer is observed and withheld. On the secure
// row it cannot read frames: it binds to the first write of at least its
// threshold (the one such write of the run, which the test checks) and,
// armed from before that write leaves, intercepts every byte received on
// that connection afterwards; it identifies no request or reply frame and
// no transaction. There the object size that the observer client polls
// until it shows the entry establishes the application, independently of
// the interception, and the gate reports that received bytes were
// intercepted while the call was pending, not that they were the reply. A
// native checkpoint, with `rbd journal
// inspect`, shows the entry applied once while the subject still holds the
// lock with its watch listed and its call still pending. Native librbd then
// breaks the lock with lock_break, as in
// TestCephRBDJournalForcedBreakIntegration: right before it, the breaking
// instance lists the subject's lock record and its watch at the lock
// record's address; its debug log shows the forced BreakRequest blocklisting
// that address, waiting for the latest OSDMap and breaking the lock. The
// subject's call is still pending after the break. Only then does the
// subject cancel its local wait; the outcome stays unknown, is recorded, and
// the append is never sent again. A native observation shows the journal
// object unchanged since the checkpoint (the entry once), the lock free, the
// subject's watch gone and its address blocklisted. The subject closes its
// silent connection and dials nothing more. Finally a native librbd write
// takes the free lock, replays the entry once and journals and applies its
// own write over the first half of that extent: `rbd journal inspect` shows
// the entry once followed only by the takeover's entry, and the image is the
// setup, the replayed entry and then the takeover write.
//
// The disposition is "applied once" for native clients and "unknown" for
// the caller, kept apart: the break neither undid nor refused an append the
// OSD had already executed, and the caller does not learn that from the OSD.
// The frame-level transmission count (one marked request, its reply held)
// is a CRC-row claim; on the secure row the claim is one caller invocation,
// one write of the threshold size and bytes intercepted after it.
//
// What this does not hold: an append that is still executing inside the OSD
// when the blocklist entry arrives. At ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc
// do_op refuses a blocklisted source address (`is_blocklisted(m->get_source_addr())`,
// -EBLOCKLISTED) before any of the waits that park a request
// (wait_for_degraded_object, waiting_for_blocked_object, the scrub and
// read/write-lock waits in do_op and get_rw_locks); each of them requeues
// the request, which then enters do_op again and meets the check with the
// newer map, which is the held-before-execution case of
// TestCephRBDJournalForcedBreakPendingIntegration. Once execute_ctx has
// run, a single-OSD replicated PG waits only for its local object store
// commit, and no stock option of that release delays that commit
// (common/options/global.yaml.in and osd.yaml.in offer dispatch and
// messenger delays, which act before do_op). So the bounded stock state
// after the check is the one here: executed and answered, the answer in
// flight to the caller. An append held for a replica's sub-write on several
// OSDs when the fence arrives is the fenced-hold cell of
// rbd_journal_fenced_hold_test.go.
//
// Everything journal-specific is the test-only consumer code of
// rbd_journal_test.go. The product is unchanged.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournalforcedbreakinflight key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-forced-break-inflight
//scenario:row rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournalforcedbreakinflight key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-forced-break-inflight
//scenario:covers RBD underlying I/O
//fixture:isolated blocklists the Go lock owner's address (removed only at fixture teardown) and leaves a journaling image
func TestCephRBDJournalForcedBreakInflightIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, rbdJournalVariantForcedBreakInflight)
}

// rbdJournalVariantForcedBreakInflight is the variant of
// runRBDJournalLostAppend this file adds.
const rbdJournalVariantForcedBreakInflight = "forced-break-inflight"

// The in-flight variant's order and the stages at which it is judged.
var (
	rbdJournalForcedInflightOrder = []string{"setup", "lock", "tag_create", "observe-locked", "ping", "append_submitted", "barrier", "checkpoint", "force-break",
		"outcome_recorded", "observe-fenced", "blocked_connection_closed", "takeover"}
	rbdJournalForcedInflightStages = []string{"checkpoint", "observe-fenced", "takeover"}
)

// rbdJournalForcedInflight is the in-flight variant's own record. The ping,
// the lock record address and the break are in the record's OwnerLoss.
type rbdJournalForcedInflight struct {
	// PingedUTC is when the subject's watch was renewed before the append.
	PingedUTC string
	// PendingAfterBreak says that the subject's append call had not returned
	// when the force-break action had.
	PendingAfterBreak bool
}

// runRBDJournalForcedBreakInflightEnd is the in-flight variant after the
// unknown outcome was recorded: the observation of the fence, the closed
// silent connection and the native takeover.
func runRBDJournalForcedBreakInflightEnd(t *testing.T, r *rbdJournalLostAppendRecord, native func(*testing.T, string) *nativeRBDJournal, judge func(*testing.T, string),
	done func(string), closeAll func() error, dials func() int) {
	native(t, "observe-fenced")
	judge(t, "observe-fenced")
	before := dials()
	r.DialsBeforeClose = &before
	if err := closeAll(); err != nil {
		t.Fatal("close the silent OSD connection", err)
	}
	done("blocked_connection_closed")
	taken := native(t, "takeover")
	r.OwnerLoss.Takeover = rbdJournalTakeoverOf(taken)
	total := dials()
	r.DialsTotal = &total
	judge(t, "takeover")
	ol := r.OwnerLoss
	t.Logf("go journal forced break with an append in flight mode=%s tag=%d object=%s submission_tid=%d locker=%s break=%v unknown=%v takeover_write=%v",
		r.ConnectionMode, r.Entry.Tag, r.Entry.ObjectName, r.Outcome.Submission.Transaction, ol.LockerAddress, time.Duration(ol.Break.BreakNS), r.Outcome.UnknownOutcome,
		time.Duration(taken.WriteNS))
}

// rbdJournalForcedInflightClaims checks the in-flight variant's record up to
// one stage. It keeps apart what native clients saw while the owner held the
// lock and the caller was pending ("checkpoint"), the break with the reply
// still withheld, what the caller recorded and the fence ("observe-fenced"),
// and the gate's record of the append with the native takeover ("takeover").
func rbdJournalForcedInflightClaims(r *rbdJournalLostAppendRecord, stage string) []string {
	var problems []string
	fail := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	text := func(v any) string { data, _ := json.Marshal(v); return string(data) }
	ol, in, e := r.OwnerLoss, r.Inflight, r.Entry
	reached := slices.Index(rbdJournalForcedInflightStages, stage)
	if reached < 0 {
		return []string{"unknown stage " + stage}
	}
	if ol == nil || !ol.Forced || in == nil {
		return []string{"record: not an in-flight forced-break record"}
	}
	view := func(name string, from int) *rbdJournalLostAppendView {
		if reached < from {
			return nil
		}
		v := r.Views[name]
		if v == nil || v.Action != name || int(e.Object) >= len(v.ObjectSizes) || len(v.ObjectSizes) != len(v.ObjectSHA256) {
			fail("%s: native observation missing or incomplete", name)
			return nil
		}
		return v
	}
	setup, locked, checkpoint := view("setup", 0), view("observe-locked", 0), view("checkpoint", 0)
	broke, fenced, takeover := view("force-break", 1), view("observe-fenced", 1), view("takeover", 2)
	if len(problems) != 0 {
		return problems
	}
	if want := rbdJournalForcedInflightOrder[:slices.Index(rbdJournalForcedInflightOrder, stage)+1]; !reflect.DeepEqual(r.Order, want) {
		fail("order: steps completed as %v, want %v", r.Order, want)
	}
	if e.Length <= 0 || len(e.SHA256) != 64 || e.Tag == 0 || r.SubjectGlobalID == 0 || r.SubjectGlobalID == r.ObserverGlobalID || r.WatchCookie == 0 || ol.LockerAddress == "" {
		fail("identity: entry, client identity or lock record address incomplete")
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

	// Application while the owner was live and the caller pending.
	if locked.EntrySHA256 != nil || locked.ObjectSizes[e.Object] != uint64(e.Offset) || !reflect.DeepEqual(locked.ObjectSHA256, setup.ObjectSHA256) {
		fail("observe-locked: journal objects changed before the append, or the entry offset is not the object size")
	}
	held("observe-locked", locked)
	unmoved("observe-locked", locked)
	if p := ol.PingBefore; p == nil || p.Class != "applied" || p.Code != 0 || p.Invocations != 1 || in.PingedUTC == "" {
		fail("ping: the subject's watch was not renewed before the append: %s", text(p))
	}
	entryAt("checkpoint", checkpoint)
	held("checkpoint", checkpoint)
	unmoved("checkpoint", checkpoint)
	want := rbdJournalLostAppendEvent{Type: "AioWrite", Offset: e.ImageOffset, Length: e.ImageLength,
		Timestamp: time.Unix(int64(e.Timestamp.Seconds), int64(e.Timestamp.Nanoseconds)/1000*1000).UTC().Format(time.RFC3339Nano)}
	if checkpoint.InspectTotal == nil || checkpoint.InspectErrors == nil || setup.InspectTotal == nil || *checkpoint.InspectTotal != *setup.InspectTotal+1 ||
		*checkpoint.InspectErrors != 0 || len(checkpoint.InspectOwned) != 1 || checkpoint.InspectOwned[0] != want || !slices.Equal(checkpoint.InspectOther, setup.InspectOther) {
		fail("checkpoint: rbd journal inspect does not replay the setup entries and the owned entry once (%v)", checkpoint.InspectOwned)
	}
	if r.PendingAfterCheckpoint == nil || !*r.PendingAfterCheckpoint {
		fail("application: the caller was not observed pending after the checkpoint")
	}
	if reached < 1 {
		return problems
	}

	// The break of the live owner while its reply was withheld.
	b := ol.Break
	if b == nil || !slices.Equal(b.Owners, []string{ol.LockerAddress}) || b.LockerBefore != owner || b.LockerCookieBefore != r.LockCookie || b.LockerAddressBefore != ol.LockerAddress ||
		!slices.Contains(b.WatchersBefore, rbdJournalWatch{r.SubjectGlobalID, r.WatchCookie, ol.LockerAddress}) || len(b.OwnersAfter) != 0 || b.IsOwner || b.BreakNS <= 0 {
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
	if !in.PendingAfterBreak {
		fail("in flight: the caller's append was no longer pending when the break had returned")
	}
	// What the caller was told: an unknown outcome, recorded after the break.
	o := r.Outcome
	if o == nil || o.Class != "uncertain" || !o.UnknownOutcome || !o.Canceled || o.CallerInvocations != 1 || o.Submission.Transaction == 0 ||
		o.Submission.GlobalID != r.SubjectGlobalID || o.ReplyTransaction != 0 || o.ReplyCode != 0 || o.RecordedUTC == "" {
		fail("outcome: not one unknown outcome of a canceled wait with its submission: %s", text(o))
		o = &rbdJournalLostAppendOutcome{}
	}
	recordedAt, err1 := time.Parse(time.RFC3339Nano, o.RecordedUTC)
	brokeAt, err2 := time.Parse(time.RFC3339Nano, broke.ObservedUTC)
	if err1 != nil || err2 != nil || !recordedAt.After(brokeAt) {
		fail("outcome: not recorded after the break was observed (%q, %q)", o.RecordedUTC, broke.ObservedUTC)
	}
	// The fence: nothing moved since the checkpoint, the entry is there once.
	if !reflect.DeepEqual(fenced.ObjectSHA256, checkpoint.ObjectSHA256) || !reflect.DeepEqual(fenced.ObjectSizes, checkpoint.ObjectSizes) {
		fail("observe-fenced: journal objects changed after the checkpoint")
	}
	entryAt("observe-fenced", fenced)
	unmoved("observe-fenced", fenced)
	if !fenced.LockFree || fenced.SubjectWatcher {
		fail("observe-fenced: the lock is held or the subject's watch is still listed (free=%v watcher=%v)", fenced.LockFree, fenced.SubjectWatcher)
	}
	blocked("observe-fenced", fenced.Blocklist, true)
	if reached < 2 {
		return problems
	}

	// Transmission: one caller invocation on one connection and no other
	// dial; what the gate saw of it is judged by mode (rbdJournalGateClaims:
	// the marked frame and its held reply in CRC mode, the one write of the
	// threshold size and the intercepted bytes in secure mode).
	g := r.Gate
	if g == nil {
		fail("transmission: gate state missing")
		g = &testcluster.OSDReplyGateState{}
	}
	if r.DialsBeforeClose == nil || *r.DialsBeforeClose != 1 || r.DialsTotal == nil || *r.DialsTotal != 1 || g.Connections != 1 {
		fail("transmission: not one subject connection: gate %s, dials before close %s, in total %s", text(g), text(r.DialsBeforeClose), text(r.DialsTotal))
	}
	rbdJournalGateClaims(r, g, o, 1, 1, []int{1}, fail)
	rbdJournalTakeoverClaims(r, takeover, fenced, locked, setup, fail)
	return problems
}
