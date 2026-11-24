package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// The held-request journal cells run the lost-append-reply setup on three
// replicated BlueStore OSDs whose owned pool has one PG, so the image header,
// the journal header and every journal object share one acting set and one
// primary P. The Go subject takes the image's exclusive lock in librbd's
// managed-lock form, allocates its journal tag and sends one AioWrite entry
// with journal.append to P, as the lost-append cell does. Nothing withholds
// a reply in the client: the request is held in the cluster instead, and the
// test keeps four claims apart: what the OSDs listed while the caller was
// pending (held), what the caller was told and how often it asked
// (outcome), what the PG's interval history says around the request
// (interval), and what native clients find afterwards, up to native
// librbd's replay of the journal (journal).
//
// The fixture's reads of the PG are `ceph osd map` of the three objects,
// `ceph pg <pgid> query` (state, up and acting sets, last update, the
// history's same_interval_since, same_up_since and same_primary_since, and
// each peer's last update), the OSDMap's down OSDs, and one OSD's
// `dump_ops_in_flight` with the events its op tracker recorded
// (integration/fixture/rbd_journal_held.go). The source at
// ceph/ceph/blob/v20.2.4/src/ gives the expected native behaviour:
//
//   - PrimaryLogPG::execute_ctx and issue_repop apply a write on the
//     primary and ReplicatedBackend::submit_transaction sends one
//     MOSDRepOp to every other acting member, marking the client op
//     "waiting for subops from <replicas>"; do_repop_reply marks
//     "sub_op_commit_rec" for each replica's commit, and the client is
//     answered once every member committed (osd/ReplicatedBackend.cc,
//     osd/PrimaryLogPG.cc eval_repop).
//   - PG::can_discard_op drops a client op whose epoch is older than the
//     PG's same_primary_since, and a write that reaches a PG on an OSD that
//     is not its primary goes to OSDService::handle_misdirected_op, which
//     drops it silently unless osd_debug_misdirected_ops is set (osd/PG.cc,
//     osd/OSD.cc, osd/PrimaryLogPG.cc do_op).
//   - A client op whose epoch the OSD has not reached waits on its session
//     for that map (osd/OSD.cc dispatch_session_waiting).
//
// The subject never resends the append and never applies or commits its
// entry; consumer choices kept explicit here are which replica is paused,
// the epoch the held append names and releasing the lock after the fault
// instead of handing it over. The journaling image has no object map, so no
// object-map or data companion request travels with the append. Several
// pending appends and a primary that is started again are the cells of
// rbd_journal_held_pending_test.go, an erasure-coded journal pool that of
// TestCephRBDJournalErasureHoldIntegration. Not covered: a PG log trimmed
// past the entry and a fenced owner.
//
// Everything journal-specific is the test-only consumer code of
// rbd_journal_test.go. The product is unchanged.

// TestCephRBDJournalReplicaHoldIntegration holds the subject's append in the
// replicated write pipeline. Right after a watch ping the subject leases a
// pause of one replica of the PG (the last member of the acting set) from
// the fixture, sends the append to P and, while the caller is pending, the
// fixture lists P's operations in flight: exactly one client op of the
// subject on journal object 0, a `journal.append` call, at the flag point
// "waiting for sub ops", whose events are exactly the steps up to dispatch,
// queued_for_pg, reached_pg, started, "waiting for subops from" both
// replicas, then its own op_commit and one "sub_op_commit_rec" in either
// order. The PG is still active+clean in the interval the
// lock was taken in. The subject resumes the replica within the lease, well
// inside the 20 s osd_heartbeat_grace, and the caller receives one known
// answer 0 on disk for the transaction P listed. A PG read after the answer
// shows the same interval (same_interval_since, up and acting sets) and
// every peer at P's last update, so the answer is the original execution's
// commit and not a duplicate answer of a later interval. The native
// observation then shows the owned entry once at its offset, the image and
// the image client's commit position unchanged and the lock still the
// subject's; the subject releases the lock and native librbd replays the
// entry once.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalreplicahold key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-replica-hold
//scenario:row rbd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalreplicahold key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-replica-hold
//scenario:covers RBD underlying I/O
//fixture:isolated pauses one replica OSD under a lease (resumed by the test or by the lease's deadline) and leaves a journaling image
func TestCephRBDJournalReplicaHoldIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, rbdJournalVariantReplicaHold)
}

// TestCephRBDJournalHeldPrimaryChangeIntegration changes the acting primary
// while the subject's append is held at it. The append names an epoch eight
// above the one the subject read, so P holds it waiting for that map, and
// the fixture lists it among P's operations in flight with no flag point
// reached and no event past dispatched (it has not reached its PG), with the
// caller pending. The fixture's change-primary action then marks P
// out (as in TestCephRBDJournalPrimaryChangeIntegration): in a newer epoch,
// still below the named one, another member leads the PG and P is the last
// member of the acting set. The fixture's advance-map then raises the epoch
// past the named one by toggling the owned pool's nodeep-scrub flag (as
// TestCephRBDJournalForcedBreakPendingIntegration does), sampling P's
// operations in flight before and after: P lists the held append once
// before and not after. Once P has the named epoch it no longer leads the
// PG, and a write that reaches a PG on an OSD that is not its primary is
// dropped without an answer. The caller has had no answer; it cancels its
// wait and records an unknown outcome of its one invocation. A PG read shows
// the PG active in a newer interval led by the new primary. Native
// observations show the journal objects byte-identical to the observation
// before the append, the lock still the subject's and the image unchanged.
// The subject closes its connections, reattaches its watch on the new
// primary, finds its lock, releases it and unwatches, and native librbd's
// replay finds nothing of the subject's tag to replay. An epoch only one
// above the subject's was tried first and is not used: the cluster reached
// it before the primary change on its own, and P executed the append.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalheldprimarychange key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-held-primary-change
//scenario:row rbd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalheldprimarychange key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-held-primary-change
//scenario:covers RBD underlying I/O
//fixture:isolated marks the PG primary OSD out (marked in only at fixture teardown) and leaves a journaling image
func TestCephRBDJournalHeldPrimaryChangeIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, rbdJournalVariantHeldPrimaryChange)
}

// TestCephRBDJournalDeadPrimaryIntegration kills the primary while the
// subject's append waits there for a replica. As in
// TestCephRBDJournalReplicaHoldIntegration one replica is paused and P lists
// the op with exactly one replica commit recorded, so the other replica,
// which keeps running, holds the entry. The subject then has the fixture
// SIGKILL P (exit 137) while the caller is pending: the caller's connection
// ends and it records an unknown outcome of its one invocation, before the
// paused replica is resumed within its lease. The cluster marks P down by
// itself from its peers' failure reports (the test does not mark it down), and
// the PG peers in a new interval led by one of the two others, active
// without P. Native observations through the new primary show the owned
// entry once at its offset, the image and the image client's commit position
// unchanged and the lock record still the subject's. The subject closes its
// connections, reattaches its watch on the new primary, releases its lock
// and unwatches; it never resends the append. Native librbd's replay applies
// the entry once. The killed OSD is not started again.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournaldeadprimary key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-dead-primary
//scenario:row rbd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournaldeadprimary key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-dead-primary
//scenario:covers RBD underlying I/O
//fixture:isolated pauses one replica OSD under a lease, kills the PG primary OSD (not started again) and leaves a journaling image
func TestCephRBDJournalDeadPrimaryIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, rbdJournalVariantDeadPrimary)
}

// TestCephRBDJournalErasureHoldIntegration is the replica hold with the
// journal's data objects in an erasure-coded pool. The fixture creates the
// second owned pool as an erasure-coded pool (k=2, m=1 over the three
// BlueStore OSDs, overwrites allowed, one PG, no HASHPSPOOL), and native
// librbd creates the journaling image with rbd_journal_pool naming it: the
// image header and the journal header stay in the replicated owned pool, and
// the journal's data objects, which librbd names after the header's pool,
// are in the erasure-coded one (ceph/ceph/blob/v20.2.4/src/librbd/journal/CreateRequest.cc,
// src/journal/Journaler.cc object_oid_prefix). cls_journal's get_pool_id on
// the journal header answers that pool's ID.
//
// The subject takes the lock and allocates its tag on the header's primary,
// reads journal object 0 in the erasure-coded pool, pauses the last member
// of that PG's acting set (one shard) under a lease and sends its one
// journal.append to the PG's primary P, on the subject's connection to P.
// cls_journal's append stats the object and writes at its size
// (src/cls/journal/cls_journal.cc journal_object_append), which an
// erasure-coded pool executes only with overwrites (src/osd/osd_types.h
// requires_aligned_append); P's erasure-coded write pipeline answers once
// every shard committed (src/osd/ECCommon.cc). While the caller is pending
// the fixture lists P's operations in flight: exactly one client op of the
// subject on journal object 0, a `journal.append` call, at the flag point
// "started" with exactly the steps up to started followed by
// "sub_op_started" and "sub_op_committed": P writes its own shard with the
// client op, which records that sub-write and nothing of the other shards'
// (src/osd/ECBackend.cc handle_sub_write and SubWriteCommitted), so unlike
// the replicated hold the op names no replica wait; the hold is attributed
// to the paused shard by the lease and the pending call. The PG is still
// active+clean in the interval it had before. The subject resumes the shard within the lease and the
// caller receives one known answer 0 on disk for the transaction P listed;
// the PG is in the same interval with every peer at P's new last update.
// Native observations show the owned entry once at its offset in the
// erasure-coded object, the image and the image client's commit position
// unchanged and the lock still the subject's; the subject releases the
// lock and native librbd replays the entry once.
//
// Consumer choices kept explicit: which shard is paused, the erasure-code
// profile, and keeping the journal header (omap) in the replicated pool,
// which librbd requires (an erasure-coded pool keeps no omap,
// src/osd/osd_types.h supports_omap). Not covered: an erasure-coded journal
// pool in the dead-primary, primary-change and pending-append cells, a
// missing shard (min_size), optimized erasure coding, and an image whose data
// pool is erasure-coded.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalerasurehold key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-erasure-hold
//scenario:row rbd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalerasurehold key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-erasure-hold
//scenario:covers RBD underlying I/O
//fixture:isolated adds an erasure-coded journal pool, pauses one shard OSD under a lease and leaves a journaling image
func TestCephRBDJournalErasureHoldIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, rbdJournalVariantErasureHold)
}

// The variants of runRBDJournalLostAppend this file adds, and the record's
// Mode of each.
const (
	rbdJournalVariantReplicaHold       = "replica-hold"
	rbdJournalVariantHeldPrimaryChange = "held-primary-change"
	rbdJournalVariantDeadPrimary       = "dead-primary"
	rbdJournalVariantErasureHold       = "erasure-hold"
)

// rbdJournalHeldLeaseBudget is how long the paused replica's lease lasts
// from its request, and rbdJournalHeldLeaseReserve the part of it kept for
// the resume. Both stay well below the 20 s default osd_heartbeat_grace, so
// the replica's peers never report it.
const (
	rbdJournalHeldLeaseBudget  = 14 * time.Second
	rbdJournalHeldLeaseReserve = 3 * time.Second
)

// rbdJournalHeldAnswerWait bounds the wait for the caller after the replica
// was resumed (replica hold), after P was killed (dead primary), and after
// the held op was seen dropped (held primary change, where no answer is
// expected).
const (
	rbdJournalHeldAnswerWait = 20 * time.Second
	rbdJournalHeldLateWait   = 3 * time.Second
)

// The orders and stages of the three modes.
var (
	rbdJournalReplicaHoldOrder = []string{"setup", "lock", "tag_create", "observe-locked", "ping", "observe-placed", "replica_paused", "append_submitted", "observe-held",
		"replica_resumed", "outcome_recorded", "observe-settled", "observe-answered", "unlock", "unwatch", "replay"}
	rbdJournalReplicaHoldStages = []string{"observe-held", "observe-answered", "replay"}

	rbdJournalHeldChangeOrder = []string{"setup", "lock", "tag_create", "observe-locked", "ping", "observe-placed", "append_submitted", "observe-held", "change-primary",
		"advance-map", "outcome_recorded", "observe-settled", "observe-recorded", "blocked_connection_closed", "watch_reconnected", "unlock", "unwatch", "replay"}
	rbdJournalHeldChangeStages = []string{"observe-held", "change-primary", "observe-recorded", "replay"}

	rbdJournalDeadPrimaryOrder = []string{"setup", "lock", "tag_create", "observe-locked", "ping", "observe-placed", "replica_paused", "append_submitted", "observe-held",
		"primary_killed", "outcome_recorded", "replica_resumed", "observe-primary-lost", "observe-recorded", "blocked_connection_closed", "watch_reconnected", "unlock",
		"unwatch", "replay"}
	rbdJournalDeadPrimaryStages = []string{"observe-held", "observe-recorded", "replay"}
)

// rbdJournalHeldPlan returns the stages and the order of a held mode.
func rbdJournalHeldPlan(mode string) (stages, order []string) {
	switch mode {
	case rbdJournalVariantReplicaHold, rbdJournalVariantErasureHold:
		return rbdJournalReplicaHoldStages, rbdJournalReplicaHoldOrder
	case rbdJournalVariantHeldPrimaryChange:
		return rbdJournalHeldChangeStages, rbdJournalHeldChangeOrder
	case rbdJournalVariantDeadPrimary:
		return rbdJournalDeadPrimaryStages, rbdJournalDeadPrimaryOrder
	case rbdJournalVariantPendingHold:
		return rbdJournalPendingHoldStages, rbdJournalPendingHoldOrder
	case rbdJournalVariantPrimaryRestart:
		return rbdJournalPrimaryRestartStages, rbdJournalPrimaryRestartOrder
	}
	return nil, nil
}

// nativeRBDJournalPGQuery, nativeRBDJournalHeldOp and nativeRBDJournalHeldRead
// mirror the fixture's held-request reads (rbd_journal_held.go).
type nativeRBDJournalPGQuery struct {
	State             string  `json:"state"`
	Epoch             uint32  `json:"epoch"`
	Up                []int32 `json:"up"`
	Acting            []int32 `json:"acting"`
	LastUpdate        string  `json:"last_update"`
	SameIntervalSince uint32  `json:"same_interval_since"`
	SameUpSince       uint32  `json:"same_up_since"`
	SamePrimarySince  uint32  `json:"same_primary_since"`
	LastEpochStarted  uint32  `json:"last_epoch_started"`
	Peers             []struct {
		Peer       int32  `json:"peer"`
		LastUpdate string `json:"last_update"`
	} `json:"peers"`
}

type nativeRBDJournalHeldOp struct {
	OSD         int32    `json:"osd"`
	Description string   `json:"description"`
	Client      string   `json:"client"`
	Transaction uint64   `json:"tid"`
	FlagPoint   string   `json:"flag_point"`
	Events      []string `json:"events"`
}

type nativeRBDJournalHeldRead struct {
	Objects   []nativeRBDJournalPlacement `json:"objects"`
	PGID      string                      `json:"pgid"`
	Query     *nativeRBDJournalPGQuery    `json:"pg_query"`
	OSD       int32                       `json:"osd"`
	Ops       []nativeRBDJournalHeldOp    `json:"ops"`
	Epoch     uint32                      `json:"epoch"`
	Down      []int32                     `json:"down"`
	Settled   bool                        `json:"settled"`
	Polls     int                         `json:"polls"`
	ElapsedMS int64                       `json:"elapsed_ms"`
}

// rbdJournalHeldLease is the paused replica's lease as the fixture recorded
// it: the daemon, the deadline, when it was paused and ended and why.
type rbdJournalHeldLease struct {
	Daemon             string
	Deadline, PausedAt int64
	EndedAt            int64
	Reason             string
	// LeftMS is how long before its deadline the lease was ended.
	LeftMS int64
}

// rbdJournalHeldKill is the fixture's kill of the primary.
type rbdJournalHeldKill struct {
	Daemon       string
	ExitCode     int
	OOMKilled    bool
	RequestedUTC string
}

// rbdJournalHeld is the held-request variants' own record.
type rbdJournalHeld struct {
	Mode string
	// Primary is P and GoActing the acting set, both as the subject's own
	// placement of journal object 0 named them before the append; Replica
	// is the member that was paused (-1 when none was).
	Primary, Replica int32
	GoActing         []int32
	// The fixture's reads; Settled in the replica-hold and held
	// primary-change modes and Lost in the dead-primary mode only.
	Placed, Held, Settled, Lost *nativeRBDJournalHeldRead
	PingBefore                  *rbdLockOwnerLossAttempt
	Lease                       *rbdJournalHeldLease
	// SentAtEpoch is the epoch of the subject's placement of the append,
	// NamedEpoch the one the append named.
	SentAtEpoch, NamedEpoch uint32
	SubmittedUTC            string
	// PendingAfterHeld, PendingAfterChange: whether the caller was still
	// waiting when the named read had returned.
	PendingAfterHeld, PendingAfterChange bool
	Kill                                 *rbdJournalHeldKill
	// The subject's connection that carried the append, by dial order.
	AppendConnection int
	// Pending is set in the pending-append modes
	// (rbd_journal_held_pending_test.go) only.
	Pending *rbdJournalHeldPending
	// The handoff after a change or death of the primary: the watch
	// reattached on the new primary, the lock record's address and the
	// address the new primary lists the reattached watch at.
	Reconnect                     *rbdLockOwnerLossAttempt
	LockerAddress, WatcherAddress string
	HandoffPrimary                int32
	// The change-primary and advance-map receipts (held primary-change
	// mode).
	PrimaryChange *nativeRBDJournalPrimaryChange
	Advance       *rbdLockOwnerLossAdvance
	// The erasure-hold mode's journal pool, which holds the journal data
	// objects, and the image header's primary in the owned pool, which
	// may be another OSD than the journal object's.
	JournalPool   string `json:",omitempty"`
	JournalPoolID int64  `json:",omitempty"`
	HeaderPrimary int32  `json:",omitempty"`
}

// rbdJournalHeldSteps hands a held mode what it needs beyond the
// forced-break steps.
type rbdJournalHeldSteps struct {
	rbdJournalForcedBreakSteps
	mode     string
	dir      string
	observer *cephmsgr.Client
	dials    *atomic.Int32
	dialOf   func(*cephmsgr.OSDConnection) int
	closeAll func() error
	connect  func(*testing.T, rbdRoute) *cephmsgr.OSDConnection
	// outcome writes the outcome record file.
	outcome func() error
	// unlocked marks the lock released, so the compensating unlock of a
	// failed run is not sent.
	unlocked func()
	// address is the subject's routing address of an OSD; readdress
	// rereads every OSD's address from the OSDMap.
	address   func(int32) string
	readdress func(*testing.T)
}

// rbdJournalHeldReadOf runs one fixture observation of a held mode.
func rbdJournalHeldReadOf(t *testing.T, ctx context.Context, s rbdJournalHeldSteps, action string) *nativeRBDJournalHeldRead {
	t.Helper()
	receipt := filepath.Join(s.dir, "rbd-journal-"+action+".json")
	if err := os.Remove(receipt); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	begun := time.Now()
	if err := testcluster.RequestControl(ctx, s.dir, "rbd-journal", "rbd-journal-completed", s.token, action); err != nil {
		t.Fatal("fixture", action, err)
	}
	s.record.Timings["native_"+action] = time.Since(begun).String()
	var result struct {
		Source   string                    `json:"source_commit"`
		Token    string                    `json:"token"`
		Action   string                    `json:"action"`
		Complete bool                      `json:"complete"`
		Error    string                    `json:"error"`
		Held     *nativeRBDJournalHeldRead `json:"held"`
		Mutation *struct {
			Kind      string `json:"kind"`
			RequestID int64  `json:"request_id"`
			ReadOnly  bool   `json:"read_only"`
			State     string `json:"state"`
		} `json:"mutation"`
		RequestID int64 `json:"request_id"`
	}
	if err := readMDSFreshJSON(receipt, &result); err != nil {
		t.Fatal(err)
	}
	if result.Source != mdsFreshSource || result.Token != s.token || result.Action != action || !result.Complete || result.Error != "" || result.Mutation == nil ||
		result.Mutation.Kind != "native-journal-"+action || result.Mutation.State != "applied" || !result.Mutation.ReadOnly || result.Mutation.RequestID != result.RequestID ||
		result.Held == nil {
		t.Fatal("fixture held-request receipt differs", action, result.Error)
	}
	s.done(action)
	return result.Held
}

// runRBDJournalHeld is a held mode after the observation of the locked
// image.
func runRBDJournalHeld(t *testing.T, s rbdJournalHeldSteps) {
	r, e, h := s.record, s.entry, s.record.Held
	ctx := s.ctx
	gid := s.gid
	classOp := func(class, method string, mode cephmsgr.OSDCallMode, input []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: mode, MaxOutputBytes: 65536}}
	}
	headerRoute := s.route(t, s.header)
	watchRequest := s.request(headerRoute)
	watchRequest.Operations = nil

	// The owner's watch is renewed right before the fault, on its own
	// connection while that still delivers replies.
	ping, err := s.w.Ping(ctx, headerRoute.Epoch)
	s.keep("watch_ping", headerRoute, watchRequest, ping, err)
	pinged := rbdLockOwnerLossOutcome("watch_ping", ping, err)
	h.PingBefore = &pinged
	if err != nil {
		t.Fatal("subject watch ping before the fault", err)
	}
	s.done("ping")

	erasure := s.mode == rbdJournalVariantErasureHold
	followed := 3
	if erasure {
		// The fixture follows the journal object's erasure-coded PG alone.
		followed = 1
	}
	h.Placed = rbdJournalHeldReadOf(t, ctx, s, "observe-placed")
	if len(h.Placed.Objects) != followed || len(h.Placed.Objects[0].Acting) != 3 {
		t.Fatal("the PG is not on three acting members", h.Placed.Objects)
	}
	// The OSDs the test acts on come from the subject's own placement (the
	// observer's MON command), not from the fixture's read; the claims
	// compare the two.
	targetRoute := s.route(t, e.ObjectName)
	pool := s.plan.Pool
	if erasure {
		pool = h.JournalPool
	}
	goPlacement, err := recoveryObjectPlacement(ctx, s.observer, pool, e.ObjectName)
	if err != nil || len(goPlacement.Acting) != 3 || goPlacement.Primary != targetRoute.Primary || !erasure && headerRoute.Primary != targetRoute.Primary {
		t.Fatalf("the subject's placement of the journal object and the header is not one three-member acting set (%v, osd.%d, osd.%d): %v", goPlacement.Acting, targetRoute.Primary, headerRoute.Primary, err)
	}
	h.Primary, h.Replica, h.GoActing = targetRoute.Primary, -1, goPlacement.Acting
	if erasure {
		h.HeaderPrimary = headerRoute.Primary
	}
	h.SentAtEpoch = targetRoute.Epoch

	// The replica modes pause the last acting member under a lease.
	var pause *heldPause
	if s.mode != rbdJournalVariantHeldPrimaryChange {
		h.Replica = h.GoActing[2]
		pause = startHeldPause(t, ctx, s.dir, "osd", strconv.Itoa(int(h.Replica)), time.Now().Add(rbdJournalHeldLeaseBudget), rbdJournalHeldLeaseReserve)
		h.Lease = &rbdJournalHeldLease{Daemon: pause.daemon(), Deadline: pause.lease.Deadline.UnixNano()}
		s.done("replica_paused")
	}
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

	// The one append, submitted once and never again: on the connection
	// to the journal object's primary, which in the erasure-hold mode may
	// be another OSD than the header's.
	appendConn := s.conn
	if erasure {
		appendConn = s.connect(t, targetRoute)
	}
	h.AppendConnection = s.dialOf(appendConn)
	appendRequest := s.request(targetRoute, classOp("journal", "append", cephmsgr.OSDCallWrite, s.appendInput))
	if s.mode == rbdJournalVariantHeldPrimaryChange {
		// An epoch the cluster reaches only through the fixture's `osd out`
		// and advance-map, whatever else changes the map meanwhile.
		appendRequest.MapEpoch = targetRoute.Epoch + rbdJournalForcedPendingEpochs
	}
	h.NamedEpoch = appendRequest.MapEpoch
	appendRequest.ModificationTime = s.now()
	callCtx, cancelCall := context.WithCancel(ctx)
	defer cancelCall()
	type result struct {
		reply cephmsgr.OSDReply
		err   error
	}
	finished := make(chan result, 1)
	go func() {
		reply, err := appendConn.Request(callCtx, appendRequest)
		finished <- result{reply, err}
	}()
	h.SubmittedUTC = time.Now().UTC().Format(time.RFC3339Nano)
	s.done("append_submitted")
	// outcome records what the caller was told, once its call returned.
	outcome := func(t *testing.T, got result) {
		t.Helper()
		s.keep("journal_append", targetRoute, appendRequest, got.reply, got.err)
		var unknown *cephmsgr.OutcomeUnknownError
		class, detail := rbdMutationOutcome(got.reply, got.err)
		r.Outcome = &rbdJournalLostAppendOutcome{Label: "journal_append", CallerInvocations: 1, Class: class, UnknownOutcome: errors.As(got.err, &unknown),
			Canceled: errors.Is(got.err, context.Canceled), Error: detail, Submission: got.reply.Submission, ReplyTransaction: got.reply.Transaction, ReplyCode: got.reply.Code,
			RecordedUTC: time.Now().UTC().Format(time.RFC3339Nano)}
		if err := s.outcome(); err != nil {
			t.Fatal("record the append's outcome", err)
		}
		s.done("outcome_recorded")
	}
	pending := func() bool {
		select {
		case got := <-finished:
			// Put it back for the outcome.
			finished <- got
			return false
		default:
			return true
		}
	}

	h.Held = rbdJournalHeldReadOf(t, ctx, s, "observe-held")
	h.PendingAfterHeld = pending()
	if pause != nil {
		if err := pause.check("the held observation"); err != nil {
			t.Fatal(err)
		}
	}
	s.judge(t, "observe-held")

	switch s.mode {
	case rbdJournalVariantReplicaHold, rbdJournalVariantErasureHold:
		resume(t)
		select {
		case got := <-finished:
			outcome(t, got)
		case <-time.After(rbdJournalHeldAnswerWait):
			t.Fatal("the append was not answered after the replica was resumed")
		}
		h.Settled = rbdJournalHeldReadOf(t, ctx, s, "observe-settled")
		s.native(t, "observe-answered")
		s.judge(t, "observe-answered")
		s.send(t, nil, gid, "lock_unlock", s.header, classOp("lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(r.LockCookie)))
		s.unlocked()
		s.done("unlock")
		reply, err := s.w.Unwatch(ctx, headerRoute.Epoch)
		s.keep("image_watch_unwatch", headerRoute, watchRequest, reply, err)
		if err != nil {
			t.Fatal("subject image header unwatch", err)
		}
		s.done("unwatch")
	case rbdJournalVariantDeadPrimary:
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
		select {
		case got := <-finished:
			outcome(t, got)
		case <-time.After(rbdJournalHeldAnswerWait):
			t.Fatal("the append's call did not end after its primary was killed")
		}
		if err := pause.check("the kill of the primary"); err != nil {
			t.Fatal(err)
		}
		resume(t)
		h.Lost = rbdJournalHeldReadOf(t, ctx, s, "observe-primary-lost")
		s.native(t, "observe-recorded")
		s.judge(t, "observe-recorded")
		rbdJournalHeldHandoff(t, s, watchRequest)
	case rbdJournalVariantHeldPrimaryChange:
		s.native(t, "change-primary")
		var receipt struct {
			PrimaryChange *nativeRBDJournalPrimaryChange `json:"primary_change"`
		}
		if err := readMDSFreshJSON(filepath.Join(s.dir, "rbd-journal-change-primary.json"), &receipt); err != nil || receipt.PrimaryChange == nil {
			t.Fatal("fixture primary-change receipt missing", err)
		}
		h.PrimaryChange = receipt.PrimaryChange
		h.PendingAfterChange = pending()
		s.judge(t, "change-primary")
		// The old primary gets the named epoch, in which it no longer leads
		// the PG; the fixture samples its operations before and after.
		h.Advance = rbdJournalHeldAdvance(t, ctx, s)
		// No answer is expected; whatever arrives within the bound is
		// recorded as it is. Then only the local wait is canceled.
		select {
		case got := <-finished:
			outcome(t, got)
		case <-time.After(rbdJournalHeldLateWait):
			cancelCall()
			outcome(t, <-finished)
		}
		h.Settled = rbdJournalHeldReadOf(t, ctx, s, "observe-settled")
		s.native(t, "observe-recorded")
		s.judge(t, "observe-recorded")
		rbdJournalHeldHandoff(t, s, watchRequest)
	}

	replay := s.native(t, "replay")
	if replay.Owner == nil || !*replay.Owner {
		t.Fatal("native librbd did not acquire the exclusive lock")
	}
	total := int(s.dials.Load())
	r.DialsTotal = &total
	s.judge(t, "replay")
	t.Logf("go journal held append mode=%s primary=osd.%d replica=osd.%d tag=%d object=%s submission_tid=%d class=%s code=%d named_epoch=%d placed_interval=%d",
		s.mode, h.Primary, h.Replica, e.Tag, e.ObjectName, r.Outcome.Submission.Transaction, r.Outcome.Class, r.Outcome.ReplyCode, h.NamedEpoch, h.Placed.Query.SameIntervalSince)
}

// rbdJournalHeldAdvance runs the fixture's advance-map and returns its
// receipt.
func rbdJournalHeldAdvance(t *testing.T, ctx context.Context, s rbdJournalHeldSteps) *rbdLockOwnerLossAdvance {
	t.Helper()
	receipt := filepath.Join(s.dir, "rbd-journal-advance-map.json")
	if err := os.Remove(receipt); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
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
	s.record.Timings["native_advance-map"] = time.Since(begun).String()
	if err := readMDSFreshJSON(receipt, &advance); err != nil {
		t.Fatal(err)
	}
	if advance.Source != mdsFreshSource || advance.Token != s.token || advance.Action != "advance-map" || !advance.Complete || advance.Error != "" || advance.Mutation == nil ||
		advance.Mutation.Kind != "native-journal-advance-map" || advance.Mutation.State != "applied" || advance.Mutation.ReadOnly || advance.Mutation.RequestID != advance.RequestID ||
		advance.Advance == nil {
		t.Fatal("fixture advance-map receipt differs", advance.Error)
	}
	s.done("advance-map")
	return advance.Advance
}

// rbdJournalHeldHandoff releases the subject's lock after the primary
// changed or died: it closes the subject's connections, reattaches the watch
// on the header's new primary, reads the lock record and the listed
// watchers there, unlocks and unwatches. The append is never sent again.
func rbdJournalHeldHandoff(t *testing.T, s rbdJournalHeldSteps, watchRequest cephmsgr.OSDRequest) {
	t.Helper()
	r, h := s.record, s.record.Held
	ctx := s.ctx
	classOp := func(class, method string, mode cephmsgr.OSDCallMode, input []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: mode, MaxOutputBytes: 65536}}
	}
	before := int(s.dials.Load())
	r.DialsBeforeClose = &before
	// The connection to a killed OSD may report its loss on close.
	_ = s.closeAll()
	s.done("blocked_connection_closed")
	headerRoute := s.route(t, s.header)
	h.HandoffPrimary = headerRoute.Primary
	watchRequest = s.request(headerRoute)
	watchRequest.Operations = nil
	conn := s.connect(t, headerRoute)
	w2, reply, err := conn.ReconnectWatch(ctx, s.w, watchRequest)
	s.keep("image_watch_reconnect", headerRoute, watchRequest, reply, err)
	reconnect := rbdLockOwnerLossOutcome("image_watch_reconnect", reply, err)
	h.Reconnect = &reconnect
	if err != nil || w2.Cookie() != r.WatchCookie {
		t.Fatal("subject image header watch reconnect on the new primary", err)
	}
	s.done("watch_reconnected")
	got := s.send(t, nil, s.gid, "lock_get_info_after", s.header, classOp("lock", "get_info", cephmsgr.OSDCallRead, rbdLockGetInfoInput()))
	info, err := decodeRBDLockInfo(got.Operations[0].Data)
	if err != nil || len(info.Lockers) != 1 || info.Lockers[0].NameNumber != int64(s.gid) || info.Lockers[0].Cookie != r.LockCookie {
		t.Fatal("the subject lost the image's exclusive lock", info, err)
	}
	h.LockerAddress = fmt.Sprintf("%s/%d", info.Lockers[0].Address.Endpoint, info.Lockers[0].Address.Nonce)
	listed := s.send(t, nil, s.gid, "list_watchers_after", s.header, cephmsgr.OSDOperation{Code: cephmsgr.OSDListWatchers, ListWatchers: cephmsgr.OSDListWatchersInput{MaxOutputBytes: 4096}})
	for _, watcher := range listed.Operations[0].Watchers {
		if uint64(watcher.NameNumber) == s.gid && watcher.Cookie == r.WatchCookie {
			h.WatcherAddress = fmt.Sprintf("%s/%d", watcher.Address.Endpoint, watcher.Address.Nonce)
		}
	}
	s.send(t, nil, s.gid, "lock_unlock", s.header, classOp("lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(r.LockCookie)))
	s.unlocked()
	s.done("unlock")
	reply, err = w2.Unwatch(ctx, headerRoute.Epoch)
	s.keep("image_watch_unwatch", headerRoute, watchRequest, reply, err)
	if err != nil {
		t.Fatal("subject image header unwatch", err)
	}
	s.done("unwatch")
}

// rbdJournalHeldText prints a record value as its JSON.
func rbdJournalHeldText(v any) string { data, _ := json.Marshal(v); return string(data) }
