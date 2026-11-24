package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// The pending-append cells extend the held-request cells of
// rbd_journal_held_test.go to several appends of the one Go lock owner that
// are held in the cluster at once, each with its own recorded outcome and its
// own native postcondition, and continue the dead-primary cell with a
// primary that is started again. The appends are entries 0, 1, ... of the
// subject's tag, one per journal object 0, 1, ... of object set 0 (splay
// offsets 0, 1, ...), with image extents that overlap neither each other nor
// any setup event. Each is invoked once, from its own goroutine, on the
// connection that took the lock, and never again. What reached the primary
// is its op tracker's list (`dump_ops_in_flight`), matched to each call by
// the journal object its description names; each caller outcome must name
// the transaction the primary listed for it.
//
// The source at ceph/ceph/blob/v20.2.4/src/ gives the expected native
// behaviour, beyond the held cells' list:
//
//   - An OSD that is started again after SIGKILL reads its PG from its
//     store, peers with the members that served the PG meanwhile and takes
//     the primary role back once the PG's up set is its original one again;
//     the PG log the members agree on decides which writes exist
//     (osd/PeeringState.cc choose_acting and the log merge). Only writes
//     that some member committed can survive: a request held at the primary
//     for its map exists only in that process's session queue (osd/OSD.cc
//     dispatch_session_waiting) and is gone with it.
//   - A watch the OSD keeps in the object's info is restored on whichever
//     OSD leads the PG, and lasts its registration's timeout after the last
//     ping or reconnect (osd/PrimaryLogPG.cc populate_obc_watchers, osd/Watch.cc).
//
// The subject never resends an append, never applies or commits an entry,
// and native librbd's replay is the only consumer of what the journal holds.
// Consumer choices kept explicit here are the number of appends and which of
// them is held for its map, which local wait is canceled, the paused
// replica, and the watch timeout of the restart cell. Not covered: an
// erasure-coded pool, a PG log trimmed past the entries, appends on several
// connections or to several PGs, a fenced owner while several appends are
// held, and a primary that is started again before it was marked down.
//
// Everything journal-specific is the test-only consumer code of
// rbd_journal_test.go. The product is unchanged.

// TestCephRBDJournalPendingHoldIntegration holds three appends of the
// subject at the primary P at once. Right after a watch ping the subject
// leases a pause of one replica of the PG (the last member of the acting set)
// and sends entries 0, 1 and 2 to P. While all three callers are pending, P
// lists exactly three client ops of the subject, one `journal.append` call to
// each of journal objects 0, 1 and 2, each at the flag point "waiting for sub
// ops" with the events up to "waiting for subops from" both replicas, its own
// op_commit and one "sub_op_commit_rec"; the PG is still active+clean in the
// interval the lock was taken in. The subject then cancels only its local
// wait for entry 1, which ends as an unknown outcome of its one invocation,
// and resumes the replica within the lease. Entries 0 and 2 receive one known
// answer 0 on disk each, for the transaction P listed for them. A PG read
// after the answers shows the same interval with every peer at P's last
// update. The native observation then shows each journal object as its bytes
// before the append followed by exactly its entry, entry 1 included: the
// canceled wait did not cancel the append. The image and the image client's
// commit position are unchanged and the lock is still the subject's; the
// subject releases the lock, and native librbd replays the three entries
// once each, in order: the image is the setup with the three extents, the
// image client's commit position names entry 2 at splay offset 2, entry 1 at
// offset 1 and entry 0 at offset 0, and the next tag's predecessor is entry
// 2.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalpendinghold key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-pending-hold
//scenario:row rbd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalpendinghold key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-pending-hold
//scenario:covers RBD underlying I/O
//fixture:isolated pauses one replica OSD under a lease (resumed by the test or by the lease's deadline) and leaves a journaling image
func TestCephRBDJournalPendingHoldIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, rbdJournalVariantPendingHold)
}

// TestCephRBDJournalPrimaryRestartIntegration kills the primary P while two
// appends of the subject are held there, and starts P again. The subject
// registers its image header watch with a 180 s timeout, so that the watch
// outlasts the death, the restart and the peering in between. After a watch
// ping it leases a pause of the last acting member and sends entry 0 to
// journal object 0, which P applies and holds for the paused replica, and
// entry 1 to journal object 1, naming an OSDMap epoch eight above the one it
// read, which P holds for that map. P lists exactly the two ops: entry 0
// waiting for sub ops with one replica commit recorded, and entry 1 with no
// flag point and no event past dispatched. The subject then has the fixture
// SIGKILL P (exit 137): both calls end with the loss of the connection as
// unknown outcomes of one invocation each, before the paused replica is
// resumed. The cluster marks P down by itself and the PG is active again in a
// newer interval led by one of the two others, without P. A native
// observation shows journal object 0 as its earlier bytes followed by exactly
// entry 0 (the running replica had committed it) and journal object 1
// unchanged, the image and the image client's commit position unchanged and
// the lock record still the subject's.
//
// The subject then has the fixture start P again. The fixture waits until
// the OSDMap has P up and the PG is active+clean on its original acting set,
// led by P again, in an interval newer than the one without P, with every
// peer at P's last update. A native observation through the restarted P
// finds every journal object byte-identical to the observation without P:
// the restarted primary neither lost entry 0 nor executed entry 1, and the
// subject sent neither again. The subject closes its connections, reattaches
// its watch on P, reads its lock there, releases it and unwatches. Native
// librbd's replay applies entry 0 once and finds no entry 1: the image is the
// setup with entry 0's extent, the image client's commit position names
// entry 0, and the next tag's predecessor is entry 0.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalprimaryrestart key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-primary-restart
//scenario:row rbd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalprimaryrestart key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-primary-restart
//scenario:covers RBD underlying I/O
//fixture:isolated pauses one replica OSD under a lease, kills the PG primary OSD and starts it again, and leaves a journaling image
func TestCephRBDJournalPrimaryRestartIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, rbdJournalVariantPrimaryRestart)
}

// The variants of runRBDJournalLostAppend this file adds, and the record's
// Mode of each.
const (
	rbdJournalVariantPendingHold    = "pending-hold"
	rbdJournalVariantPrimaryRestart = "primary-restart"
)

// rbdJournalPrimaryRestartWatchTimeout is the restart cell's image header
// watch timeout in seconds: long enough for the kill, the restart and both
// peerings, which together take far longer than librbd's default of 30 s on
// the container runtime.
const rbdJournalPrimaryRestartWatchTimeout = 180

// The two kinds of hold of a pending append.
const (
	rbdJournalHoldSubOps = "subops" // applied at P, waiting for the paused replica
	rbdJournalHoldMap    = "map"    // naming an epoch the cluster has not reached
)

// rbdJournalHeldPendingHolds are the holds of the appends of each mode, in
// entry order.
var rbdJournalHeldPendingHolds = map[string][]string{
	rbdJournalVariantPendingHold:    {rbdJournalHoldSubOps, rbdJournalHoldSubOps, rbdJournalHoldSubOps},
	rbdJournalVariantPrimaryRestart: {rbdJournalHoldSubOps, rbdJournalHoldMap},
}

// rbdJournalHeldPendingCanceled is the entry whose local wait the
// pending-hold mode cancels.
const rbdJournalHeldPendingCanceled = 1

// The orders and stages of the two modes.
var (
	rbdJournalPendingHoldOrder = []string{"setup", "lock", "tag_create", "observe-locked", "ping", "observe-placed", "replica_paused", "appends_submitted", "observe-held",
		"pending_canceled", "replica_resumed", "outcomes_recorded", "observe-settled", "observe-answered", "unlock", "unwatch", "replay"}
	rbdJournalPendingHoldStages = []string{"observe-held", "observe-answered", "replay"}

	rbdJournalPrimaryRestartOrder = []string{"setup", "lock", "tag_create", "observe-locked", "ping", "observe-placed", "replica_paused", "appends_submitted", "observe-held",
		"primary_killed", "outcomes_recorded", "replica_resumed", "observe-primary-lost", "observe-recorded", "primary_started", "observe-primary-back", "observe-restarted",
		"blocked_connection_closed", "watch_reconnected", "unlock", "unwatch", "replay"}
	rbdJournalPrimaryRestartStages = []string{"observe-held", "observe-recorded", "observe-restarted", "replay"}
)

// rbdJournalHeldCall is one pending append: its entry, how it is held, the
// epoch it names, and its journal object's bytes before it (as native
// librados read them while the subject held the lock) and with it.
type rbdJournalHeldCall struct {
	Label string
	// Phase names the pattern of the entry's image extent.
	Phase         string
	Entry         rbdJournalLostAppendEntry
	Hold          string
	NamedEpoch    uint32
	BeforeSize    uint64
	BeforeSHA256  string
	AppliedSHA256 string
	// Outcome is what the caller was told about its one invocation.
	Outcome *rbdJournalLostAppendOutcome
}

// rbdJournalHeldPending is the pending-append modes' own part of the record.
type rbdJournalHeldPending struct {
	Calls []rbdJournalHeldCall
	// CanceledUTC is when the wait for the canceled entry ended
	// (pending-hold mode).
	CanceledUTC string
	// StartedUTC is when the killed primary's start was requested, Back the
	// fixture's read once it was back (primary-restart mode).
	StartedUTC string
	Back       *nativeRBDJournalHeldRead
	// AddressBefore and AddressAfter are the killed primary's address in
	// the OSDMap before its death and once it was back, as the subject's
	// routing read them.
	AddressBefore, AddressAfter string
}

// rbdJournalHeldPendingMode says whether a held mode is one of this file's.
func rbdJournalHeldPendingMode(mode string) bool {
	return mode == rbdJournalVariantPendingHold || mode == rbdJournalVariantPrimaryRestart
}

// runRBDJournalHeldPending is a pending-append mode after the observation of
// the locked image.
func runRBDJournalHeldPending(t *testing.T, s rbdJournalHeldSteps, imageCommit []rbdJournalPosition) {
	r, e, h := s.record, s.entry, s.record.Held
	p := &rbdJournalHeldPending{}
	h.Pending = p
	ctx := s.ctx
	restart := s.mode == rbdJournalVariantPrimaryRestart
	classOp := func(class, method string, mode cephmsgr.OSDCallMode, input []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: mode, MaxOutputBytes: 65536}}
	}
	digest := func(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
	headerRoute := s.route(t, s.header)
	watchRequest := s.request(headerRoute)
	watchRequest.Operations = nil

	// The owner's watch is renewed right before the fault.
	ping, err := s.w.Ping(ctx, headerRoute.Epoch)
	s.keep("watch_ping", headerRoute, watchRequest, ping, err)
	pinged := rbdLockOwnerLossOutcome("watch_ping", ping, err)
	h.PingBefore = &pinged
	if err != nil {
		t.Fatal("subject watch ping before the fault", err)
	}
	s.done("ping")

	h.Placed = rbdJournalHeldReadOf(t, ctx, s, "observe-placed")
	if len(h.Placed.Objects) != 3 || len(h.Placed.Objects[0].Acting) != 3 {
		t.Fatal("the PG is not on three acting members", h.Placed.Objects)
	}
	targetRoute := s.route(t, e.ObjectName)
	goPlacement, err := recoveryObjectPlacement(ctx, s.observer, s.plan.Pool, e.ObjectName)
	if err != nil || len(goPlacement.Acting) != 3 || goPlacement.Primary != targetRoute.Primary || headerRoute.Primary != targetRoute.Primary {
		t.Fatalf("the subject's placement of the journal object and the header is not one three-member acting set (%v, osd.%d, osd.%d): %v", goPlacement.Acting, targetRoute.Primary, headerRoute.Primary, err)
	}
	h.Primary, h.Replica, h.GoActing = targetRoute.Primary, goPlacement.Acting[2], goPlacement.Acting
	h.SentAtEpoch, h.NamedEpoch = targetRoute.Epoch, targetRoute.Epoch

	// The entries and their journal objects' bytes before them.
	objects := readRBDBlob(t, s.dir, s.locked.Blobs["journal_objects"])
	prefix := e.ObjectName[:len(e.ObjectName)-len(strconv.FormatUint(e.Object, 10))]
	extents := append([]nativeRBDExtent{{Offset: e.ImageOffset, Length: e.ImageLength}}, rbdJournalPendingExtents...)
	// Entry 0 is the record's entry, built by the common sequence.
	phases := []string{"go-lost-append", "go-held-pending", "go-held-pending"}
	var inputs [][]byte
	for i, hold := range rbdJournalHeldPendingHolds[s.mode] {
		entry, input := *e, s.appendInput
		if i > 0 {
			at := s.now()
			data := rbdJournalEntryEncode(e.Tag, uint64(i), rbdJournalWriteEvent(extents[i].Offset, rbdJournalPattern(s.token, phases[i], extents[i].Offset, extents[i].Length), at))
			object := e.Object + uint64(i)
			entry = rbdJournalLostAppendEntry{Tag: e.Tag, EntryTID: uint64(i), Object: object, ObjectName: prefix + strconv.FormatUint(object, 10),
				Offset: int(s.locked.JournalObjects[object].Size), Length: len(data), SHA256: digest(data), ImageOffset: extents[i].Offset, ImageLength: extents[i].Length, Timestamp: at}
			input = append(slices.Clone(s.appendInput[:8]), rbdJournalBL(data)...)
		}
		if entry.Object/s.splay != e.Object/s.splay {
			t.Fatal("a pending entry is in another object set")
		}
		o := s.locked.JournalObjects[entry.Object]
		before := objects[o.Offset : o.Offset+o.Size]
		if len(before) != entry.Offset {
			t.Fatal("a pending entry's offset is not its journal object's size", entry.Object)
		}
		// The entry's bytes are the input's after the soft limit and the
		// bufferlist's length.
		p.Calls = append(p.Calls, rbdJournalHeldCall{Label: fmt.Sprintf("journal_append_%d", i), Phase: phases[i], Entry: entry, Hold: hold, BeforeSize: o.Size, BeforeSHA256: digest(before),
			AppliedSHA256: digest(append(slices.Clone(before), input[12:]...))})
		inputs = append(inputs, input)
	}

	pause := startHeldPause(t, ctx, s.dir, "osd", strconv.Itoa(int(h.Replica)), time.Now().Add(rbdJournalHeldLeaseBudget), rbdJournalHeldLeaseReserve)
	h.Lease = &rbdJournalHeldLease{Daemon: pause.daemon(), Deadline: pause.lease.Deadline.UnixNano()}
	s.done("replica_paused")
	resume := func(t *testing.T) {
		t.Helper()
		left, err := pause.resume()
		h.Lease.LeftMS = left.Milliseconds()
		if ended, ok, readErr := pause.lease.Ended(); readErr == nil && ok {
			h.Lease.PausedAt, h.Lease.EndedAt, h.Lease.Reason = ended.PausedAt, ended.EndedAt, ended.Reason
		}
		if err != nil {
			t.Fatal(err)
		}
		s.done("replica_resumed")
	}

	// The appends, each submitted once and never again.
	type call struct {
		route  rbdRoute
		req    cephmsgr.OSDRequest
		cancel context.CancelFunc
		done   chan struct{}
		reply  cephmsgr.OSDReply
		err    error
	}
	appendConn := s.conn
	h.AppendConnection = s.dialOf(appendConn)
	calls := make([]*call, len(p.Calls))
	defer func() {
		for _, c := range calls {
			if c != nil {
				c.cancel()
			}
		}
	}()
	for i := range p.Calls {
		c := &p.Calls[i]
		route := s.route(t, c.Entry.ObjectName)
		if route.Primary != h.Primary {
			t.Fatal("a journal object of the pending entries has another primary", c.Entry.ObjectName, route.Primary)
		}
		req := s.request(route, classOp("journal", "append", cephmsgr.OSDCallWrite, inputs[i]))
		if c.Hold == rbdJournalHoldMap {
			req.MapEpoch = h.SentAtEpoch + rbdJournalForcedPendingEpochs
		} else {
			req.MapEpoch = h.SentAtEpoch
		}
		c.NamedEpoch = req.MapEpoch
		req.ModificationTime = s.now()
		callCtx, cancelCall := context.WithCancel(ctx)
		pending := &call{route: route, req: req, cancel: cancelCall, done: make(chan struct{})}
		calls[i] = pending
		go func() {
			defer close(pending.done)
			pending.reply, pending.err = appendConn.Request(callCtx, pending.req)
		}()
	}
	h.SubmittedUTC = time.Now().UTC().Format(time.RFC3339Nano)
	s.done("appends_submitted")
	allPending := func() bool {
		for _, c := range calls {
			select {
			case <-c.done:
				return false
			default:
			}
		}
		return true
	}
	// settle records what the caller of entry i was told, once its call
	// returned within bound.
	settle := func(t *testing.T, i int, bound time.Duration) {
		t.Helper()
		c := calls[i]
		select {
		case <-c.done:
		case <-time.After(bound):
			t.Fatalf("the subject's %s did not return within %v", p.Calls[i].Label, bound)
		}
		s.keep(p.Calls[i].Label, c.route, c.req, c.reply, c.err)
		var unknown *cephmsgr.OutcomeUnknownError
		class, detail := rbdMutationOutcome(c.reply, c.err)
		p.Calls[i].Outcome = &rbdJournalLostAppendOutcome{Label: p.Calls[i].Label, CallerInvocations: 1, Class: class, UnknownOutcome: errors.As(c.err, &unknown),
			Canceled: errors.Is(c.err, context.Canceled), Error: detail, Submission: c.reply.Submission, ReplyTransaction: c.reply.Transaction, ReplyCode: c.reply.Code,
			RecordedUTC: time.Now().UTC().Format(time.RFC3339Nano)}
	}
	recorded := func(t *testing.T) {
		t.Helper()
		r.Outcome = p.Calls[0].Outcome
		if err := s.outcome(); err != nil {
			t.Fatal("record the appends' outcomes", err)
		}
		s.done("outcomes_recorded")
	}

	h.Held = rbdJournalHeldReadOf(t, ctx, s, "observe-held")
	h.PendingAfterHeld = allPending()
	if err := pause.check("the held observation"); err != nil {
		t.Fatal(err)
	}
	s.judge(t, "observe-held")

	if !restart {
		// Only the local wait for entry 1 ends; the append stays at P.
		calls[rbdJournalHeldPendingCanceled].cancel()
		settle(t, rbdJournalHeldPendingCanceled, 10*time.Second)
		p.CanceledUTC = time.Now().UTC().Format(time.RFC3339Nano)
		s.done("pending_canceled")
		resume(t)
		for i := range calls {
			if i != rbdJournalHeldPendingCanceled {
				settle(t, i, rbdJournalHeldAnswerWait)
			}
		}
		recorded(t)
		h.Settled = rbdJournalHeldReadOf(t, ctx, s, "observe-settled")
		s.native(t, "observe-answered")
		s.judge(t, "observe-answered")
		s.send(t, nil, s.gid, "lock_unlock", s.header, classOp("lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(r.LockCookie)))
		s.unlocked()
		s.done("unlock")
		reply, err := s.w.Unwatch(ctx, headerRoute.Epoch)
		s.keep("image_watch_unwatch", headerRoute, watchRequest, reply, err)
		if err != nil {
			t.Fatal("subject image header unwatch", err)
		}
		s.done("unwatch")
		// Native librbd replays the three entries.
		expected := rbdJournalHeldPendingImage(s.plan, s.token)
		var committed []rbdJournalEntry
		for _, c := range p.Calls {
			copy(expected[c.Entry.ImageOffset:], rbdJournalPattern(s.token, c.Phase, c.Entry.ImageOffset, c.Entry.ImageLength))
			committed = append(committed, rbdJournalEntry{Object: c.Entry.Object, Tag: c.Entry.Tag, EntryID: c.Entry.EntryTID})
		}
		r.ExpectedHeadAfter, r.ExpectedCommitAfter = digest(expected), rbdJournalCommitPosition(imageCommit, committed, s.splay)
	} else {
		h.Kill = &rbdJournalHeldKill{Daemon: fmt.Sprintf("osd.%d", h.Primary), RequestedUTC: time.Now().UTC().Format(time.RFC3339Nano)}
		crash := filepath.Join(s.dir, "osd-crash-state.json")
		if err := os.Remove(crash); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if err := testcluster.ControlDaemon(ctx, s.dir, "kill", "osd", strconv.Itoa(int(h.Primary))); err != nil {
			t.Fatal("kill the primary", err)
		}
		var state struct {
			Daemon    string `json:"daemon"`
			ExitCode  int    `json:"exit_code"`
			OOMKilled bool   `json:"oom_killed"`
		}
		if err := readMDSFreshJSON(crash, &state); err != nil {
			t.Fatal("the primary's kill receipt", err)
		}
		h.Kill.ExitCode, h.Kill.OOMKilled = state.ExitCode, state.OOMKilled
		if state.Daemon != h.Kill.Daemon {
			t.Fatal("the fixture killed another daemon", state.Daemon)
		}
		s.done("primary_killed")
		for i := range calls {
			settle(t, i, rbdJournalHeldAnswerWait)
		}
		recorded(t)
		if err := pause.check("the kill of the primary"); err != nil {
			t.Fatal(err)
		}
		resume(t)
		h.Lost = rbdJournalHeldReadOf(t, ctx, s, "observe-primary-lost")
		s.native(t, "observe-recorded")
		s.judge(t, "observe-recorded")
		// The killed primary comes back; nothing of the subject is sent
		// meanwhile.
		p.StartedUTC = time.Now().UTC().Format(time.RFC3339Nano)
		if err := testcluster.ControlDaemon(ctx, s.dir, "start", "osd", strconv.Itoa(int(h.Primary))); err != nil {
			t.Fatal("start the killed primary", err)
		}
		s.done("primary_started")
		p.Back = rbdJournalHeldReadOf(t, ctx, s, "observe-primary-back")
		// The restarted OSD has a new address: its messenger binds afresh,
		// with a new nonce.
		p.AddressBefore = s.address(h.Primary)
		s.readdress(t)
		p.AddressAfter = s.address(h.Primary)
		s.native(t, "observe-restarted")
		s.judge(t, "observe-restarted")
		rbdJournalHeldHandoff(t, s, watchRequest)
	}

	replay := s.native(t, "replay")
	if replay.Owner == nil || !*replay.Owner {
		t.Fatal("native librbd did not acquire the exclusive lock")
	}
	total := int(s.dials.Load())
	r.DialsTotal = &total
	s.judge(t, "replay")
	for _, c := range p.Calls {
		t.Logf("go journal held pending append mode=%s primary=osd.%d replica=osd.%d tag=%d entry=%d object=%s hold=%s named_epoch=%d submission_tid=%d class=%s code=%d",
			s.mode, h.Primary, h.Replica, c.Entry.Tag, c.Entry.EntryTID, c.Entry.ObjectName, c.Hold, c.NamedEpoch, c.Outcome.Submission.Transaction, c.Outcome.Class, c.Outcome.ReplyCode)
	}
}

// rbdJournalHeldPendingImage is the image the native setup leaves: its
// writes in order, its discards cleared.
func rbdJournalHeldPendingImage(plan nativeRBDJournalPlan, token string) []byte {
	image := make([]byte, plan.Size)
	for _, event := range plan.SetupEvents {
		if event.Kind == "write" {
			copy(image[event.Offset:], rbdJournalPattern(token, "setup", event.Offset, event.Length))
		} else {
			clear(image[event.Offset : event.Offset+event.Length])
		}
	}
	return image
}
