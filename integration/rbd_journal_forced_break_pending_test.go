package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephRBDJournalForcedBreakPendingIntegration breaks the lock of a live
// Go owner of a journaling image while two of its journal appends are held
// at the OSD, and then lets the fenced owner append on a new OSD connection.
// It runs the forced-break sequence of TestCephRBDJournalForcedBreakIntegration
// up to the observation of the subject's one answered entry (tag t, entry
// 0). The subject renews its watch and then sends, once each on the
// connection that sent lock.lock, the appends of entries 1 and 2 of its tag
// (journal objects 1 and 2), naming an OSDMap epoch eight above the one it
// read: the OSD holds them until it has that map (osd/OSD.cc
// dispatch_session_waiting at ceph/ceph/blob/v20.2.4/src/). A native
// observation shows every journal object unchanged and the subject holding
// the lock with its watch. Native librbd then breaks the lock with
// lock_break, as in the forced-break variant; its debug log shows
// BreakRequest reporting the live owner's watch, rereading the locker,
// blocklisting the lock record's address, waiting for the latest OSDMap and
// then breaking the lock. Both appends are still unanswered. The subject
// cancels its wait for entry 2, which ends as an unknown outcome and is
// never sent again, and the fixture raises the OSDMap epoch past the named
// one. The OSD listed both requests in flight before (dump_ops_in_flight)
// and none of the subject's afterwards, and answers entry 1 with -108
// (EBLOCKLISTED), a known refusal: PrimaryLogPG::do_op checks the request's
// source address against the map it executes with
// (ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc). A native observation
// shows every journal object unchanged since entry 0, the lock free and the
// subject's watch gone.
//
// The subject then closes that connection and dials a new one, which has
// its own address nonce (the client has no EnableCephFS). On it the subject
// registers a watch on journal object 0, and the OSD lists that watch at
// the lock record's IP address with another nonce, an address the blocklist
// does not hold. On the same connection the subject appends a new entry 1
// of its tag (other bytes than the refused one, at the same offset of
// journal object 1) and the OSD answers 0: OSDMap::is_blocklisted matches
// the exact address, then the address with port and nonce cleared, then
// the ranges (ceph/ceph/blob/v20.2.4/src/osd/OSDMap.cc), and librbd
// blocklists only the exact address of the lock record
// (librbd/managed_lock/BreakRequest.cc send_blocklist). A native
// observation shows journal object 1 grown by exactly that entry. The
// subject removes its new watch. Finally a native librbd write takes the
// free lock (its log shows the locker missing and the lock taken at once),
// replays the subject's two entries of tag t, entry 0 and the
// new-connection entry 1, and journals and applies its own write: the image
// is the setup with both extents and then the takeover write, the image
// client's commit position names the takeover's entry after both, and the
// new tag's predecessor is the new-connection entry. So the blocklist
// fences the fenced owner's connection, not the client: a journal append
// sent on another connection is applied and the next lock owner replays
// it. The test records this and changes no product behaviour (the address
// nonce of OSD connections is the consumer's open choice, see
// Options.EnableCephFS).
//
// Every observation also reads the journal header through `rbd journal
// status`: the minimum and the active object set stay 0, the registered
// clients are the image client and the native oracle client, the oracle
// client's commit position never moves and the image client's moves only
// with the takeover's commit. No object set completes on this plan, and the
// oracle client keeps the minimum set at 0, so journal trimming is not
// exercised. Not covered: an append held for another reason than its map
// epoch or executing at the OSD while the break happens, a held request on
// another OSD or with a primary change, an EnableCephFS client (one shared
// nonce), trimming, and a break by a client that is not librbd.
//
// Everything journal-specific is the test-only consumer code of
// rbd_journal_test.go. The product is unchanged.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournalforcedbreakpending key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-forced-break-pending
//scenario:row rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournalforcedbreakpending key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-forced-break-pending
//scenario:covers RBD underlying I/O
//fixture:isolated blocklists the Go lock owner (removed at teardown), raises the OSDMap epoch and leaves a journaling image
func TestCephRBDJournalForcedBreakPendingIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, rbdJournalVariantForcedBreakPending)
}

// rbdJournalVariantForcedBreakPending is the variant of
// runRBDJournalLostAppend this file adds.
const rbdJournalVariantForcedBreakPending = "forced-break-pending"

// The pending variant's order and the stages at which it is judged.
var (
	rbdJournalForcedPendingOrder = []string{"setup", "lock", "tag_create", "observe-locked", "append", "observe-appended", "ping", "pending_submitted", "observe-pending",
		"force-break", "pending_canceled", "advance-map", "pending_answered", "observe-fenced", "new_connection", "observe-new-connection", "takeover"}
	rbdJournalForcedPendingStages = []string{"observe-appended", "observe-pending", "observe-fenced", "observe-new-connection", "takeover"}
)

const (
	// rbdJournalForcedPendingEpochs is how far above the epoch read before
	// sending the held appends name theirs. The fixture's advance-map raises
	// the epoch by as much from where it starts (rbdLockPendingEpochs), and
	// the break has raised it at least once by then.
	rbdJournalForcedPendingEpochs = 8
	// The labels of the two held appends: entry 1, awaited, and entry 2,
	// whose wait is canceled.
	rbdJournalPendingAwaited  = "journal_append_pending_awaited"
	rbdJournalPendingCanceled = "journal_append_pending_canceled"
)

// rbdJournalPendingExtents are the image extents of the two held entries.
// Neither overlaps the owned entry, the new-connection entry or the
// takeover write; neither is ever applied.
var rbdJournalPendingExtents = []nativeRBDExtent{{Offset: 120 << 10, Length: 4 << 10}, {Offset: 160 << 10, Length: 4 << 10}}

// rbdJournalForcedPending is the pending variant's own record. The fields it
// shares with the forced-break variant (the lock record address, the break,
// the ping) are in the record's OwnerLoss.
type rbdJournalForcedPending struct {
	// SentAtEpoch is the OSDMap epoch read before the held appends were
	// sent, NamedEpoch the one they name.
	SentAtEpoch, NamedEpoch uint32
	// Entries and Requests are the awaited entry 1 and then the canceled
	// entry 2. A request has no Class until its call returned.
	Entries  []rbdJournalLostAppendEntry
	Requests []rbdLockOwnerLossAttempt
	// Whether both calls were still blocked when the named observation had
	// returned.
	PendingAfterObserve, PendingAfterBreak bool
	CanceledUTC, AnsweredUTC               string
	Advance                                *rbdLockOwnerLossAdvance
	// The new connection: the subject's OSD dials before and after it, the
	// epoch its requests name, its watch on journal object 0 and the
	// address the OSD lists that watch at.
	DialsBefore, DialsAfter int
	NewEpoch                uint32
	NewWatchObject          string
	NewWatchCookie          uint64
	NewAddress              string
	NewWatch, NewUnwatch    *rbdLockOwnerLossAttempt
	// Probe is the entry appended on the new connection, ProbeObjectSHA256
	// journal object 1 as it must be after it: its bytes at observe-locked
	// followed by the entry.
	Probe             *rbdJournalLostAppendEntry
	ProbeObjectSHA256 string
	ProbeOutcome      *rbdLockOwnerLossAttempt
	// The takeover: the planned writes, the image after the replay of both
	// entries and the writes, the image client's commit position after
	// them, and the takeover's own output.
	TakeoverEvents         []rbdJournalLostAppendEvent
	ExpectedHeadTakeover   string
	ExpectedCommitTakeover []rbdJournalPosition
	Takeover               *rbdJournalTakeover
}

// rbdJournalAddressParts splits a legacy address ip:port/nonce.
func rbdJournalAddressParts(address string) (netip.AddrPort, uint64, bool) {
	i := strings.LastIndex(address, "/")
	if i < 0 {
		return netip.AddrPort{}, 0, false
	}
	endpoint, err1 := netip.ParseAddrPort(address[:i])
	nonce, err2 := strconv.ParseUint(address[i+1:], 10, 32)
	return endpoint, nonce, err1 == nil && err2 == nil
}

// rbdJournalForcedPendingClaims checks the pending variant's record up to
// one stage. It keeps apart the answered entry (from "observe-appended" on),
// the held appends before the break ("observe-pending"), the break, the
// answer to the held appends and the fence ("observe-fenced"), the requests
// on the new connection ("observe-new-connection") and the native takeover
// ("takeover"). Every native observation also judges the journal header's
// object sets and registered clients.
func rbdJournalForcedPendingClaims(r *rbdJournalLostAppendRecord, stage string) []string {
	var problems []string
	fail := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	text := func(v any) string { data, _ := json.Marshal(v); return string(data) }
	ol, fp, e := r.OwnerLoss, r.ForcedPending, r.Entry
	reached := slices.Index(rbdJournalForcedPendingStages, stage)
	if reached < 0 {
		return []string{"unknown stage " + stage}
	}
	if ol == nil || !ol.Forced || fp == nil {
		return []string{"record: not a pending forced-break record"}
	}
	view := func(name string, from int) *rbdJournalLostAppendView {
		if reached < from {
			return nil
		}
		v := r.Views[name]
		if v == nil || v.Action != name || int(e.Object)+2 >= len(v.ObjectSizes) || len(v.ObjectSizes) != len(v.ObjectSHA256) {
			fail("%s: native observation missing or incomplete", name)
			return nil
		}
		return v
	}
	setup, locked, appended := view("setup", 0), view("observe-locked", 0), view("observe-appended", 0)
	pending, broke, fenced := view("observe-pending", 1), view("force-break", 2), view("observe-fenced", 2)
	opened, takeover := view("observe-new-connection", 3), view("takeover", 4)
	if len(problems) != 0 {
		return problems
	}
	if want := rbdJournalForcedPendingOrder[:slices.Index(rbdJournalForcedPendingOrder, stage)+1]; !reflect.DeepEqual(r.Order, want) {
		fail("order: steps completed as %v, want %v", r.Order, want)
	}
	if e.Length <= 0 || len(e.SHA256) != 64 || e.Tag == 0 || e.EntryTID != 0 || r.SubjectGlobalID == 0 || r.SubjectGlobalID == r.ObserverGlobalID || r.WatchCookie == 0 || len(r.Token) != 32 {
		fail("identity: entry or client identity incomplete")
	}
	owner := fmt.Sprintf("client.%d", r.SubjectGlobalID)
	oracle := "ceph-msgr-oracle-" + r.Token[:min(8, len(r.Token))]
	held := func(name string, v *rbdJournalLostAppendView) {
		if v.LockFree || v.LockOwner != owner || v.LockCookie != r.LockCookie || !v.SubjectWatcher {
			fail("%s: the subject does not hold the lock with its watch (free=%v owner=%q cookie=%q watcher=%v)", name, v.LockFree, v.LockOwner, v.LockCookie, v.SubjectWatcher)
		}
	}
	free := func(name string, v *rbdJournalLostAppendView) {
		if !v.LockFree || v.SubjectWatcher {
			fail("%s: the lock is held or the subject's watch is still listed (free=%v watcher=%v)", name, v.LockFree, v.SubjectWatcher)
		}
	}
	unmoved := func(name string, v *rbdJournalLostAppendView) {
		if v.ImageHeadSHA256 != r.ExpectedHeadBefore || !reflect.DeepEqual(v.ImageCommit, setup.ImageCommit) || v.NextTag != e.Tag+1 || v.LastTag.TID != e.Tag {
			fail("%s: image, image client commit position or tags moved (head=%s commit=%v next tag=%d)", name, v.ImageHeadSHA256, v.ImageCommit, v.NextTag)
		}
	}
	same := func(name string, v, than *rbdJournalLostAppendView, thanName string) {
		if !reflect.DeepEqual(v.ObjectSHA256, than.ObjectSHA256) || !reflect.DeepEqual(v.ObjectSizes, than.ObjectSizes) {
			fail("%s: journal objects changed since %s", name, thanName)
		}
	}
	// The journal header: no object set moves, the two registered clients
	// stay, the oracle's commit position never moves and the image client's
	// is the one rbd journal status reports for it.
	header := func(name string, v *rbdJournalLostAppendView) {
		ok := v.MinimumSet == 0 && v.ActiveSet == 0 && len(v.Clients) == 2 && len(setup.Clients) == 2 && v.Clients[0].ID == "" && v.Clients[1].ID == oracle &&
			reflect.DeepEqual(v.Clients[0].Positions, v.ImageCommit) && reflect.DeepEqual(v.Clients[1], setup.Clients[1]) && v.Clients[0].State == setup.Clients[0].State
		if !ok {
			fail("%s: the journal header's object sets or registered clients moved (minimum %d, active %d, clients %s, at setup %s)", name, v.MinimumSet, v.ActiveSet, text(v.Clients), text(setup.Clients))
		}
	}
	blocked := func(name string, blocks []rbdLockOwnerLossBlock, want bool) {
		if blocks == nil || rbdJournalBlocked(blocks, ol.LockerAddress) != want {
			fail("%s: the subject's lock record address %q blocklisted is not %v: %s", name, ol.LockerAddress, want, text(blocks))
		}
	}
	for _, v := range []*rbdJournalLostAppendView{setup, locked, appended, pending, broke, fenced, opened, takeover} {
		if v != nil {
			header(v.Action, v)
		}
	}

	// The answered entry 0, as in the forced-break variant.
	if locked.EntrySHA256 != nil || locked.ObjectSizes[e.Object] != uint64(e.Offset) || !reflect.DeepEqual(locked.ObjectSHA256, setup.ObjectSHA256) {
		fail("observe-locked: journal objects changed before the append, or the entry offset is not the object size")
	}
	held("observe-locked", locked)
	unmoved("observe-locked", locked)
	if ol.LockerAddress == "" {
		fail("identity: the subject's lock record address is missing")
	}
	if o := r.Outcome; o == nil || o.Class != "applied" || o.UnknownOutcome || o.Canceled || o.CallerInvocations != 1 || o.Submission.Transaction == 0 ||
		o.Submission.GlobalID != r.SubjectGlobalID || o.ReplyTransaction != o.Submission.Transaction || o.ReplyCode != 0 || o.RecordedUTC == "" {
		fail("append: not one known answer 0 to its one submission: %s", text(o))
	}
	if appended.PrefixSHA256 == nil || *appended.PrefixSHA256 != locked.ObjectSHA256[e.Object] || appended.EntrySHA256 == nil || *appended.EntrySHA256 != e.SHA256 ||
		appended.TrailingBytes == nil || *appended.TrailingBytes != 0 || appended.ObjectSizes[e.Object] != uint64(e.Offset+e.Length) {
		fail("observe-appended: journal object %d is not its earlier bytes followed by exactly the owned entry", e.Object)
	}
	for i := range appended.ObjectSizes {
		if uint64(i) != e.Object && (appended.ObjectSHA256[i] != locked.ObjectSHA256[i] || appended.ObjectSizes[i] != locked.ObjectSizes[i]) {
			fail("observe-appended: journal object %d changed", i)
		}
	}
	held("observe-appended", appended)
	unmoved("observe-appended", appended)
	blocked("observe-appended", appended.Blocklist, false)
	g := r.Gate
	connections := 1
	if reached >= 3 {
		connections = 2
	}
	if g == nil || g.Bound || g.Connections != connections {
		fail("transmission: the subject's requests are not on %d unwithheld connection(s): gate %s", connections, text(g))
	}
	if reached < 1 {
		return problems
	}

	// The held appends, before the break.
	if p := ol.PingBefore; p == nil || p.Class != "applied" || p.Code != 0 || p.Invocations != 1 {
		fail("ping: the subject's watch was not renewed before the held appends: %s", text(p))
	}
	if fp.SentAtEpoch == 0 || fp.NamedEpoch != fp.SentAtEpoch+rbdJournalForcedPendingEpochs || len(fp.Entries) != 2 || len(fp.Requests) != 2 ||
		fp.Requests[0].Label != rbdJournalPendingAwaited || fp.Requests[1].Label != rbdJournalPendingCanceled {
		return append(problems, fmt.Sprintf("pending: not the two appends naming an epoch %d above the one read before they were sent: %s", rbdJournalForcedPendingEpochs, text(fp)))
	}
	for i, p := range fp.Entries {
		if p.Tag != e.Tag || p.EntryTID != uint64(i+1) || p.Object != e.Object+uint64(i+1) || p.Offset != int(locked.ObjectSizes[p.Object]) || p.Length <= 0 || len(p.SHA256) != 64 ||
			p.ImageOffset != rbdJournalPendingExtents[i].Offset || p.ImageLength != rbdJournalPendingExtents[i].Length {
			fail("pending: held entry %d is not entry %d of the subject's tag at its splay offset: %s", i, i+1, text(p))
		}
	}
	unanswered := func(at string, a rbdLockOwnerLossAttempt) {
		if a.Class != "" || a.Invocations != 1 {
			fail("pending: the subject's %s is no longer unanswered at %s: %s", a.Label, at, text(a))
		}
	}
	if !fp.PendingAfterObserve {
		fail("pending: the two appends were not both unanswered after the observe-pending observation")
	}
	same("observe-pending", pending, appended, "observe-appended")
	held("observe-pending", pending)
	unmoved("observe-pending", pending)
	blocked("observe-pending", pending.Blocklist, false)
	if reached < 2 {
		unanswered("observe-pending", fp.Requests[0])
		unanswered("observe-pending", fp.Requests[1])
		return problems
	}

	// The break of a live owner with appends held, and the answer to them.
	b := ol.Break
	if b == nil || !slices.Equal(b.Owners, []string{ol.LockerAddress}) || b.LockerBefore != owner || b.LockerCookieBefore != r.LockCookie || b.LockerAddressBefore != ol.LockerAddress ||
		!slices.Contains(b.WatchersBefore, rbdJournalWatch{r.SubjectGlobalID, r.WatchCookie, ol.LockerAddress}) || len(b.OwnersAfter) != 0 || b.IsOwner || b.BreakNS <= 0 {
		fail("force-break: native librbd did not break the lock of the one live owner, the subject: %s", text(b))
	} else {
		blocked("force-break (before)", b.BlocklistBefore, false)
		blocked("force-break (after)", b.Blocklist, true)
		rbdJournalLockPathClaims("force-break lock path", b.LockPath, rbdJournalForcedBreakLockPath, fail)
	}
	same("force-break", broke, appended, "observe-appended")
	unmoved("force-break", broke)
	if !broke.LockFree {
		fail("force-break: the lock is still held after the break")
	}
	blocked("force-break", broke.Blocklist, true)
	if !fp.PendingAfterBreak {
		fail("pending: the two appends were not both unanswered after the break")
	}
	awaited, canceled := fp.Requests[0], fp.Requests[1]
	if canceled.Invocations != 1 || canceled.Class != "uncertain" || !canceled.UnknownOutcome || !canceled.Canceled || canceled.Submission.Transaction == 0 ||
		canceled.Submission.GlobalID != r.SubjectGlobalID || canceled.Submission.MapEpoch != fp.NamedEpoch || canceled.ReplyTransaction != 0 {
		fail("pending: the subject's %s did not end as one unknown outcome of a canceled wait with its submission: %s", canceled.Label, text(canceled))
	}
	canceledAt, err1 := time.Parse(time.RFC3339Nano, fp.CanceledUTC)
	brokeAt, err2 := time.Parse(time.RFC3339Nano, broke.ObservedUTC)
	if err1 != nil || err2 != nil || !canceledAt.After(brokeAt) {
		fail("pending: the wait for %s was not canceled after the break was observed (%q, %q)", canceled.Label, fp.CanceledUTC, broke.ObservedUTC)
	}
	a := fp.Advance
	if a == nil {
		return append(problems, "advance: the fixture's receipt is missing")
	}
	if a.EpochBefore == 0 || a.EpochBefore >= fp.NamedEpoch || a.EpochBefore <= fp.SentAtEpoch || a.EpochAfter < fp.NamedEpoch || a.EpochAfter < a.EpochTarget || a.FlagFinally ||
		len(a.Commands) == 0 || a.Pool != r.Pool {
		fail("advance: the OSDMap epoch was not raised from between the sent and the named epoch (%d, %d) to at least the named one, with the pool flag left clear: %s",
			fp.SentAtEpoch, fp.NamedEpoch, text(a))
	}
	pendingAt, err := time.Parse(time.RFC3339Nano, pending.ObservedUTC)
	ops, opProblems := rbdLockOwnerLossHeld(a.OpsBefore, owner, awaited.Submission.Transaction, canceled.Submission.Transaction)
	for _, p := range opProblems {
		fail("advance: before the epoch was raised, %s: %s", p, text(a.OpsBefore))
	}
	for i, request := range []rbdLockOwnerLossAttempt{awaited, canceled} {
		if ops[i] == nil {
			continue
		}
		initiated, parseErr := rbdLockOwnerLossUntil(ops[i].InitiatedAt)
		at, atErr := time.Parse(time.RFC3339Nano, initiated)
		if err != nil || parseErr != nil || atErr != nil || !at.Before(pendingAt) {
			fail("advance: the OSD did not have the subject's %s before the observe-pending observation (initiated %q, observed %q)", request.Label, ops[i].InitiatedAt, pending.ObservedUTC)
		}
	}
	if slices.ContainsFunc(a.OpsAfter, func(o rbdLockOwnerLossOp) bool { return o.Client == owner }) {
		fail("advance: the OSD still reports an operation of the subject in flight after the epoch was raised: %s", text(a.OpsAfter))
	}
	if awaited.Invocations != 1 || awaited.Class != "refused" || awaited.Code != rbdOSDBlocklisted || awaited.ErrorCode != rbdOSDBlocklisted || awaited.UnknownOutcome || awaited.Canceled ||
		awaited.Submission.Transaction == 0 || awaited.Submission.Transaction == canceled.Submission.Transaction || awaited.ReplyTransaction != awaited.Submission.Transaction ||
		awaited.Submission.GlobalID != r.SubjectGlobalID || awaited.Submission.MapEpoch != fp.NamedEpoch || awaited.ReplyMapEpoch < fp.NamedEpoch {
		fail("pending: the subject's %s was not answered once with %d at the named epoch %d or later: %s", awaited.Label, rbdOSDBlocklisted, fp.NamedEpoch, text(awaited))
	}
	answeredAt, err := time.Parse(time.RFC3339Nano, fp.AnsweredUTC)
	if err != nil || !answeredAt.After(canceledAt) {
		fail("pending: %s was not answered after the other wait was canceled (%q, %q)", awaited.Label, fp.AnsweredUTC, fp.CanceledUTC)
	}
	// Neither held append changed the journal.
	same("observe-fenced", fenced, appended, "observe-appended")
	unmoved("observe-fenced", fenced)
	free("observe-fenced", fenced)
	blocked("observe-fenced", fenced.Blocklist, true)
	if reached < 3 {
		return problems
	}

	// The new connection: another address, not blocklisted, whose watch
	// registration and append the OSD applied.
	newEndpoint, newNonce, ok1 := rbdJournalAddressParts(fp.NewAddress)
	lockEndpoint, lockNonce, ok2 := rbdJournalAddressParts(ol.LockerAddress)
	if fp.DialsBefore != 1 || fp.DialsAfter != 2 || !ok1 || !ok2 || newEndpoint.Addr() != lockEndpoint.Addr() || newNonce == lockNonce || fp.NewAddress == ol.LockerAddress ||
		fp.NewWatchCookie == 0 || fp.NewWatchCookie == r.WatchCookie || fp.NewEpoch < a.EpochAfter {
		fail("new connection: not a second subject connection whose watch the OSD lists at the lock record's IP with another nonce (lock %q, new %q, dials %d then %d, epoch %d)",
			ol.LockerAddress, fp.NewAddress, fp.DialsBefore, fp.DialsAfter, fp.NewEpoch)
	}
	for name, blocks := range map[string][]rbdLockOwnerLossBlock{"observe-fenced": fenced.Blocklist, "observe-new-connection": opened.Blocklist} {
		if rbdJournalBlocked(blocks, fp.NewAddress) {
			fail("%s: the new connection's address %q is blocklisted: %s", name, fp.NewAddress, text(blocks))
		}
	}
	applied := func(label string, a *rbdLockOwnerLossAttempt) {
		if a == nil || a.Label != label || a.Invocations != 1 || a.Class != "applied" || a.Code != 0 || a.UnknownOutcome || a.Canceled || a.Submission.Transaction == 0 ||
			a.ReplyTransaction != a.Submission.Transaction || a.Submission.GlobalID != r.SubjectGlobalID || a.Submission.MapEpoch < fp.NewEpoch {
			fail("new connection: %s is not one known answer 0 at epoch %d or later: %s", label, fp.NewEpoch, text(a))
		}
	}
	applied("new_watch_register", fp.NewWatch)
	applied("journal_append_new_connection", fp.ProbeOutcome)
	applied("new_watch_unwatch", fp.NewUnwatch)
	pr := fp.Probe
	if pr == nil || pr.Tag != e.Tag || pr.EntryTID != 1 || pr.Object != e.Object+1 || pr.Offset != fp.Entries[0].Offset || pr.Length <= 0 || len(pr.SHA256) != 64 ||
		pr.SHA256 == fp.Entries[0].SHA256 || len(fp.ProbeObjectSHA256) != 64 {
		return append(problems, fmt.Sprintf("new connection: the appended entry is not a new entry 1 of the subject's tag where the refused one would have been: %s", text(pr)))
	}
	if opened.ObjectSizes[pr.Object] != uint64(pr.Offset+pr.Length) || opened.ObjectSHA256[pr.Object] != fp.ProbeObjectSHA256 {
		fail("observe-new-connection: journal object %d is not its earlier bytes followed by exactly the new-connection entry (size %d)", pr.Object, opened.ObjectSizes[pr.Object])
	}
	for i := range opened.ObjectSizes {
		if uint64(i) != pr.Object && (opened.ObjectSHA256[i] != fenced.ObjectSHA256[i] || opened.ObjectSizes[i] != fenced.ObjectSizes[i]) {
			fail("observe-new-connection: journal object %d changed", i)
		}
	}
	unmoved("observe-new-connection", opened)
	free("observe-new-connection", opened)
	blocked("observe-new-connection", opened.Blocklist, true)
	if reached < 4 {
		return problems
	}

	// The takeover: a free lock, both entries of tag t replayed, then the
	// takeover's own entries.
	tk := fp.Takeover
	if tk == nil {
		return append(problems, "takeover: native output missing")
	}
	if !tk.Owner || tk.WriteNS <= 0 || tk.NativeGlobalID == 0 || tk.NativeGlobalID == r.SubjectGlobalID || tk.BoundaryLockers == nil || len(tk.BoundaryLockers) != 0 ||
		slices.ContainsFunc(tk.BoundaryWatchers, func(w rbdJournalWatch) bool { return w.GlobalID == r.SubjectGlobalID }) ||
		!slices.ContainsFunc(tk.BoundaryWatchers, func(w rbdJournalWatch) bool { return w.GlobalID == tk.NativeGlobalID }) || tk.Notes != nil {
		fail("takeover: native librbd did not start on a free lock without the subject's watches and own the lock for its writes: %s", text(tk))
	}
	rbdJournalLockPathClaims("takeover lock path", tk.LockPath, rbdJournalFreeLockPath, fail)
	free("takeover", takeover)
	for name, blocks := range map[string][]rbdLockOwnerLossBlock{"takeover (before)": tk.BlocklistBefore, "takeover (after)": tk.Blocklist, "takeover": takeover.Blocklist} {
		blocked(name, blocks, true)
		if rbdJournalBlocked(blocks, fp.NewAddress) {
			fail("%s: the new connection's address %q is blocklisted", name, fp.NewAddress)
		}
	}
	if len(fp.TakeoverEvents) == 0 || !reflect.DeepEqual(tk.Events, fp.TakeoverEvents) {
		fail("takeover: the native writes differ from the plan: %s, want %s", text(tk.Events), text(fp.TakeoverEvents))
	}
	if takeover.PrefixSHA256 == nil || *takeover.PrefixSHA256 != locked.ObjectSHA256[e.Object] || takeover.EntrySHA256 == nil || *takeover.EntrySHA256 != e.SHA256 ||
		takeover.TrailingBytes == nil || *takeover.TrailingBytes == 0 {
		fail("takeover: journal object %d is not its earlier bytes, the owned entry and native librbd's entries", e.Object)
	}
	for i := range takeover.ObjectSizes {
		if uint64(i) != e.Object && (takeover.ObjectSHA256[i] != opened.ObjectSHA256[i] || takeover.ObjectSizes[i] != opened.ObjectSizes[i]) {
			fail("takeover: journal object %d changed since the new-connection entry", i)
		}
	}
	event := func(x rbdJournalLostAppendEntry) rbdJournalLostAppendEvent {
		return rbdJournalLostAppendEvent{Type: "AioWrite", Offset: x.ImageOffset, Length: x.ImageLength,
			Timestamp: time.Unix(int64(x.Timestamp.Seconds), int64(x.Timestamp.Nanoseconds)/1000*1000).UTC().Format(time.RFC3339Nano)}
	}
	next := []rbdJournalLostAppendEvent{}
	for _, n := range takeover.InspectNext {
		next = append(next, rbdJournalLostAppendEvent{Type: n.Type, Offset: n.Offset, Length: n.Length})
	}
	if takeover.InspectTotal == nil || takeover.InspectErrors == nil || setup.InspectTotal == nil || *takeover.InspectTotal != *setup.InspectTotal+2+len(fp.TakeoverEvents) ||
		*takeover.InspectErrors != 0 || !slices.Equal(takeover.InspectOwned, []rbdJournalLostAppendEvent{event(e), event(*pr)}) || !reflect.DeepEqual(next, fp.TakeoverEvents) {
		fail("takeover: rbd journal inspect does not show the setup entries, the owned entry and the new-connection entry, then only the takeover's entries (owned %s, next %s)",
			text(takeover.InspectOwned), text(takeover.InspectNext))
	}
	if fp.ExpectedHeadTakeover == "" || fp.ExpectedHeadTakeover == r.ExpectedHeadAfter || takeover.ImageHeadSHA256 != fp.ExpectedHeadTakeover {
		fail("takeover: image differs from the replay of both entries followed by the takeover writes (head=%s)", takeover.ImageHeadSHA256)
	}
	if len(fp.ExpectedCommitTakeover) == 0 || fp.ExpectedCommitTakeover[0] != (rbdJournalPosition{e.Object, e.Tag + 1, 0}) || !reflect.DeepEqual(takeover.ImageCommit, fp.ExpectedCommitTakeover) {
		fail("takeover: image client commit position differs from the takeover's entries after both replayed entries (commit=%v)", takeover.ImageCommit)
	}
	if takeover.LastTag != (rbdJournalLostAppendTag{e.Tag + 1, true, e.Tag, pr.EntryTID}) || takeover.NextTag != e.Tag+2 {
		fail("takeover: native tag differs (tag=%+v next=%d)", takeover.LastTag, takeover.NextTag)
	}
	return problems
}

// rbdJournalForcedPendingSteps is what the pending variant needs beyond the
// forced-break steps: the subject's connections, its dial count, the image
// after the owned entry and the control directory.
type rbdJournalForcedPendingSteps struct {
	rbdJournalForcedBreakSteps
	dir      string
	closeAll func() error
	connect  func(*testing.T, rbdRoute) *cephmsgr.OSDConnection
	dials    func() int
	after    []byte
	timings  map[string]string
}

// runRBDJournalForcedBreakPending is the pending variant after the
// observation of the locked image.
func runRBDJournalForcedBreakPending(t *testing.T, s rbdJournalForcedPendingSteps) {
	r, e, ol, fp := s.record, s.entry, s.record.OwnerLoss, s.record.ForcedPending
	ctx := s.ctx
	classOp := func(class, method string, mode cephmsgr.OSDCallMode, input []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: mode, MaxOutputBytes: 65536}}
	}
	digest := func(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
	prefix := e.ObjectName[:len(e.ObjectName)-len(strconv.FormatUint(e.Object, 10))]
	appendInput := func(entry []byte) []byte { return append(slices.Clone(s.appendInput[:8]), rbdJournalBL(entry)...) }
	entryAt := func(tid, object uint64, phase string, extent nativeRBDExtent) (*rbdJournalLostAppendEntry, []byte) {
		at := s.now()
		data := rbdJournalEntryEncode(e.Tag, tid, rbdJournalWriteEvent(extent.Offset, rbdJournalPattern(s.token, phase, extent.Offset, extent.Length), at))
		return &rbdJournalLostAppendEntry{Tag: e.Tag, EntryTID: tid, Object: object, ObjectName: prefix + strconv.FormatUint(object, 10), Offset: int(s.locked.JournalObjects[object].Size),
			Length: len(data), SHA256: digest(data), ImageOffset: extent.Offset, ImageLength: extent.Length, Timestamp: at}, data
	}

	// Entry 0, answered: the subject neither applies nor commits it.
	reply := s.send(t, s.conn, s.gid, "journal_append", e.ObjectName, classOp("journal", "append", cephmsgr.OSDCallWrite, s.appendInput))
	r.Outcome = &rbdJournalLostAppendOutcome{Label: "journal_append", CallerInvocations: 1, Class: "applied", Submission: reply.Submission, ReplyTransaction: reply.Transaction,
		ReplyCode: reply.Code, RecordedUTC: time.Now().UTC().Format(time.RFC3339Nano)}
	s.done("append")
	s.native(t, "observe-appended")
	s.judge(t, "observe-appended")

	// The watch is live: renewed right before the held appends, since
	// anything sent on this connection after them waits behind them.
	headerRoute := s.route(t, s.header)
	watchRequest := s.request(headerRoute)
	watchRequest.Operations = nil
	ping, err := s.w.Ping(ctx, headerRoute.Epoch)
	s.keep("watch_ping", headerRoute, watchRequest, ping, err)
	attempt := rbdLockOwnerLossOutcome("watch_ping", ping, err)
	ol.PingBefore = &attempt
	if err != nil {
		t.Fatal("subject watch ping before the held appends", err)
	}
	s.done("ping")

	// Entries 1 and 2, held by the OSD: they name an epoch the cluster has
	// not reached. The test invokes each once, from its own goroutine, and
	// never again; the OSD's in-flight list shows what reached it.
	type pendingCall struct {
		label  string
		route  rbdRoute
		req    cephmsgr.OSDRequest
		cancel context.CancelFunc
		done   chan struct{}
		reply  cephmsgr.OSDReply
		err    error
	}
	var calls []*pendingCall
	defer func() {
		for _, c := range calls {
			c.cancel()
		}
	}()
	stillPending := func() bool {
		for _, c := range calls {
			select {
			case <-c.done:
				return false
			default:
			}
		}
		return true
	}
	settle := func(t *testing.T, i int, bound time.Duration) {
		t.Helper()
		c := calls[i]
		select {
		case <-c.done:
		case <-time.After(bound):
			t.Fatalf("the subject's %s did not return within %v", c.label, bound)
		}
		s.keep(c.label, c.route, c.req, c.reply, c.err)
		fp.Requests[i] = rbdLockOwnerLossOutcome(c.label, c.reply, c.err)
	}
	fp.SentAtEpoch = headerRoute.Epoch
	fp.NamedEpoch = fp.SentAtEpoch + rbdJournalForcedPendingEpochs
	for i, label := range []string{rbdJournalPendingAwaited, rbdJournalPendingCanceled} {
		entry, data := entryAt(uint64(i+1), e.Object+uint64(i+1), "go-pending-append", rbdJournalPendingExtents[i])
		if entry.Object%s.splay == 0 {
			t.Fatal("a held entry's splay offset is in another object set")
		}
		fp.Entries = append(fp.Entries, *entry)
		route := s.route(t, entry.ObjectName)
		route.Epoch = fp.NamedEpoch
		req := s.request(route, classOp("journal", "append", cephmsgr.OSDCallWrite, appendInput(data)))
		req.ModificationTime = s.now()
		callCtx, cancelCall := context.WithCancel(ctx)
		call := &pendingCall{label: label, route: route, req: req, cancel: cancelCall, done: make(chan struct{})}
		calls = append(calls, call)
		fp.Requests = append(fp.Requests, rbdLockOwnerLossAttempt{Label: label, Invocations: 1})
		go func() {
			defer close(call.done)
			call.reply, call.err = s.conn.Request(callCtx, call.req)
		}()
	}
	s.done("pending_submitted")
	s.native(t, "observe-pending")
	fp.PendingAfterObserve = stillPending()
	s.judge(t, "observe-pending")

	// Native librbd breaks the lock of the live owner; a compensating
	// unlock would no longer name the subject's lock.
	s.released()
	b, err := rbdJournalBreakOf(s.native(t, "force-break"), r.Views["force-break"])
	if err != nil {
		t.Fatal(err)
	}
	ol.Break = b
	fp.PendingAfterBreak = stillPending()

	// The subject gives up waiting for entry 2: its outcome stays unknown.
	calls[1].cancel()
	settle(t, 1, 10*time.Second)
	fp.CanceledUTC = time.Now().UTC().Format(time.RFC3339Nano)
	s.done("pending_canceled")

	// The fixture raises the OSDMap epoch past the named one and reads the
	// OSD's operations in flight before and after.
	var advance struct {
		Source    string `json:"source_commit"`
		Token     string `json:"token"`
		RequestID int64  `json:"request_id"`
		Action    string `json:"action"`
		Mutation  *struct {
			Kind      string `json:"kind"`
			RequestID int64  `json:"request_id"`
			ReadOnly  bool   `json:"read_only"`
			State     string `json:"state"`
		} `json:"mutation"`
		Advance  *rbdLockOwnerLossAdvance `json:"advance"`
		Complete bool                     `json:"complete"`
		Error    string                   `json:"error"`
	}
	begun := time.Now()
	if err := testcluster.RequestControl(ctx, s.dir, "rbd-journal", "rbd-journal-completed", s.token, "advance-map"); err != nil {
		t.Fatal("fixture advance-map", err)
	}
	s.timings["native_advance-map"] = time.Since(begun).String()
	if err := readMDSFreshJSON(filepath.Join(s.dir, "rbd-journal-advance-map.json"), &advance); err != nil {
		t.Fatal(err)
	}
	if advance.Source != mdsFreshSource || advance.Token != s.token || advance.Action != "advance-map" || !advance.Complete || advance.Error != "" || advance.Mutation == nil ||
		advance.Mutation.Kind != "native-journal-advance-map" || advance.Mutation.State != "applied" || advance.Mutation.ReadOnly || advance.Mutation.RequestID != advance.RequestID ||
		advance.Advance == nil {
		t.Fatal("fixture advance-map receipt differs", advance.Error)
	}
	fp.Advance = advance.Advance
	s.done("advance-map")
	// The OSD now has the named map and answers entry 1. The test invokes nothing again.
	settle(t, 0, 30*time.Second)
	fp.AnsweredUTC = time.Now().UTC().Format(time.RFC3339Nano)
	s.done("pending_answered")
	s.native(t, "observe-fenced")
	s.judge(t, "observe-fenced")

	// The fenced owner on a new connection. The client keeps one connection
	// per OSD, so the lock-bearing one is closed first.
	fp.DialsBefore = s.dials()
	if err := s.closeAll(); err != nil {
		t.Fatal("close the lock-bearing connection", err)
	}
	route0 := s.route(t, e.ObjectName)
	conn := s.connect(t, route0)
	fp.DialsAfter, fp.NewEpoch = s.dials(), route0.Epoch
	fp.NewWatchObject = e.ObjectName
	newWatch := s.request(route0)
	newWatch.Operations = nil
	nw, reply, err := conn.WatchObject(ctx, newWatch, cephmsgr.OSDWatchOptions{TimeoutSeconds: rbdJournalLostAppendWatchTimeout})
	s.keep("new_watch_register", route0, newWatch, reply, err)
	registered := rbdLockOwnerLossOutcome("new_watch_register", reply, err)
	fp.NewWatch = &registered
	if err != nil {
		t.Fatal("subject watch on the new connection", err)
	}
	fp.NewWatchCookie = nw.Cookie()
	listed := s.send(t, conn, s.gid, "list_watchers_new_connection", e.ObjectName,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDListWatchers, ListWatchers: cephmsgr.OSDListWatchersInput{MaxOutputBytes: 4096}})
	for _, w := range listed.Operations[0].Watchers {
		if uint64(w.NameNumber) == s.gid && w.Cookie == fp.NewWatchCookie {
			fp.NewAddress = fmt.Sprintf("%s/%d", w.Address.Endpoint, w.Address.Nonce)
		}
	}
	// A new entry 1 of the subject's tag where the refused one would have
	// been. It is not the refused entry sent again: other image bytes.
	probe, probeBytes := entryAt(1, e.Object+1, "go-new-connection", s.plan.GoWrites[1])
	fp.Probe = probe
	objectsBlob := readRBDBlob(t, s.dir, s.locked.Blobs["journal_objects"])
	before := objectsBlob[s.locked.JournalObjects[probe.Object].Offset : s.locked.JournalObjects[probe.Object].Offset+s.locked.JournalObjects[probe.Object].Size]
	fp.ProbeObjectSHA256 = digest(append(slices.Clone(before), probeBytes...))
	probeRoute := s.route(t, probe.ObjectName)
	probeRequest := s.request(probeRoute, classOp("journal", "append", cephmsgr.OSDCallWrite, appendInput(probeBytes)))
	probeRequest.ModificationTime = s.now()
	reply, err = conn.Request(ctx, probeRequest)
	s.keep("journal_append_new_connection", probeRoute, probeRequest, reply, err)
	outcome := rbdLockOwnerLossOutcome("journal_append_new_connection", reply, err)
	fp.ProbeOutcome = &outcome
	reply, err = nw.Unwatch(ctx, route0.Epoch)
	s.keep("new_watch_unwatch", route0, newWatch, reply, err)
	unwatched := rbdLockOwnerLossOutcome("new_watch_unwatch", reply, err)
	fp.NewUnwatch = &unwatched
	s.done("new_connection")
	s.native(t, "observe-new-connection")
	s.judge(t, "observe-new-connection")

	// The takeover's expectations: both entries of tag t replayed in order,
	// then its writes as entries 0, 1, ... of the next tag.
	taken := slices.Clone(s.after)
	copy(taken[probe.ImageOffset:], rbdJournalPattern(s.token, "go-new-connection", probe.ImageOffset, probe.ImageLength))
	committed := []rbdJournalEntry{{Object: e.Object, Tag: e.Tag, EntryID: 0}, {Object: probe.Object, Tag: e.Tag, EntryID: 1}}
	for i, w := range s.plan.TakeoverEvents {
		copy(taken[w.Offset:], rbdJournalPattern(s.token, "takeover", w.Offset, w.Length))
		fp.TakeoverEvents = append(fp.TakeoverEvents, rbdJournalLostAppendEvent{Type: "AioWrite", Offset: w.Offset, Length: w.Length})
		committed = append(committed, rbdJournalEntry{Object: e.Object - e.Object%s.splay + uint64(i)%s.splay, Tag: e.Tag + 1, EntryID: uint64(i)})
	}
	fp.ExpectedHeadTakeover = digest(taken)
	fp.ExpectedCommitTakeover = rbdJournalCommitPosition(r.Views["setup"].ImageCommit, committed, s.splay)
	fp.Takeover = rbdJournalTakeoverOf(s.native(t, "takeover"))
	s.judge(t, "takeover")
	t.Logf("go journal forced break with pending appends tag=%d locker=%s new_address=%s break=%v named_epoch=%d advance=%d..%d awaited=%d canceled_unknown=%v probe=%d takeover_write=%v",
		e.Tag, ol.LockerAddress, fp.NewAddress, time.Duration(b.BreakNS), fp.NamedEpoch, fp.Advance.EpochBefore, fp.Advance.EpochAfter, fp.Requests[0].ErrorCode,
		fp.Requests[1].UnknownOutcome, fp.ProbeOutcome.Code, time.Duration(fp.Takeover.WriteNS))
}
