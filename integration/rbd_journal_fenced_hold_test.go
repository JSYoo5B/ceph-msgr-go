package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// The fenced-hold cells continue the replica hold of
// rbd_journal_held_test.go: the subject's append is applied at the primary P
// and waits there for a paused replica (or, on the erasure-coded journal
// pool, a paused shard) when the subject is fenced. The fence is librbd's:
// the exact address of the subject's lock record goes into the OSDMap
// blocklist, as librbd/managed_lock/BreakRequest.cc send_blocklist does,
// and the fixture waits until P's PG has a map with the entry, as
// wait_for_osd_map does for the breaker (integration/fixture/rbd_journal_fenced.go).
// The lock record itself is not removed then: every write to the image
// header waits for the same paused member, so native librbd's later
// acquisition breaks it.
//
// The source at ceph/ceph/blob/v20.2.4/src/ gives the expected native
// behaviour:
//
//   - The OSD checks the blocklist in only two places.
//     PrimaryLogPG::do_op refuses a request whose source address is
//     blocklisted with -108 (EBLOCKLISTED) before any wait that parks a
//     request, and PeeringState::activate_map, on a map with new blocklist
//     entries, has PrimaryLogPG::check_blocklisted_watchers drop the
//     watches of blocklisted addresses (osd/PrimaryLogPG.cc,
//     osd/PeeringState.cc). A write that passed do_op is in the write
//     pipeline, which completes and answers it whatever the map says
//     (osd/PrimaryLogPG.cc eval_repop, osd/ReplicatedBackend.cc,
//     osd/ECCommon.cc).
//   - A dropped watch is removed with a write of the object's info
//     (PrimaryLogPG::handle_watch_timeout), which on the image header waits
//     for the same paused member.
//   - OSDMap::is_blocklisted matches the exact address first
//     (osd/OSDMap.cc); librbd blocklists the lock record's address only.
//     The Go client presents one address nonce per OSD connection, so the
//     fence covers a request only on the connection that took the lock.
//     The append travels there: on the replicated pool the header and the
//     journal object share P, and on the erasure-coded pool the fixture
//     first makes the journal PG's acting primary the header's with a
//     primary override (`osd primary-temp`), which leaves every shard in
//     its slot.
//
// The subject never resends the append, never applies or commits its
// entry, and does not release the lock after the fence. Consumer choices
// kept explicit here: which member is paused, the primary override of the
// erasure-coded cell, the one stat used to probe the fence, and leaving the
// lock removal to native librbd. Not covered: a fence that reaches the
// primary before the append (that is the forced-break pending cell of
// rbd_journal_forced_break_pending_test.go), several held appends of the
// fenced owner, and an owner whose OSD connections share one nonce.
//
// Everything journal-specific is the test-only consumer code of
// rbd_journal_test.go. The product is unchanged.

// TestCephRBDJournalFencedHoldIntegration fences the subject while its
// append waits at P. After a watch ping the subject leases a pause of the
// last acting member of the PG and sends its one journal.append to P on the
// connection that took the lock. P lists the op at "waiting for sub ops"
// with one replica commit, as in the replica hold. The fixture's fence then
// blocklists the lock record's address, which the MON lists afterwards and
// not before, and waits until P's PG answers its query at an epoch at least
// the OSDMap epoch read after the entry; P still lists the subject's op,
// unchanged, and the caller is still pending. On the same connection the
// subject sends one stat of journal object 0, which P refuses at once with
// -108 while the append stays pending: the fence is in force on P for new
// requests of that address. The subject resumes the replica within the
// lease and the caller receives one known answer 0 on disk for the
// transaction P listed: an admitted write of a fenced owner completes. The
// PG is in the interval the lock was taken in with every peer at P's new
// last update. A native observation shows the owned entry once at its
// offset, the image and the image client's commit position unchanged, the
// lock record still the subject's, the subject's watch gone and the
// address blocklisted. Native librbd then acquires the lock (breaking the
// fenced owner's record), replays the entry once and releases the lock.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalfencedhold key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-fenced-hold
//scenario:row rbd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalfencedhold key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-fenced-hold
//scenario:covers RBD underlying I/O
//fixture:isolated blocklists the Go lock owner's address, pauses one replica OSD under a lease and leaves a journaling image
func TestCephRBDJournalFencedHoldIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, rbdJournalVariantFencedHold)
}

// TestCephRBDJournalErasureFencedHoldIntegration is the fenced hold with the
// journal's data objects in the erasure-coded journal pool (k=2, m=1 over
// the three BlueStore OSDs, overwrites allowed, one PG), as in
// TestCephRBDJournalErasureHoldIntegration. Before the append the
// fixture's align-primary makes the journal PG's acting primary the image
// header's primary H when they differ: one `osd primary-temp`, after which
// the up and acting sets keep every shard in its slot, and the PG is
// active+clean with H answering for its own shard. The subject's append to
// journal object 0 then travels on its connection to H, the one that took
// the lock; under an override the subject's read of the journal object
// before the alignment went to the CRUSH primary on a second connection,
// which carries nothing afterwards. The member paused is the last acting member other than H. P
// lists the op at "started" with its own shard's sub-write started and
// committed, as in the erasure hold; the fence, the -108 refusal of the
// stat, the known answer 0 after the resume and the native postconditions
// are those of the replicated cell.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalerasurefencedhold key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-erasure-fenced-hold
//scenario:row rbd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalerasurefencedhold key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-erasure-fenced-hold
//scenario:covers RBD underlying I/O
//fixture:isolated adds an erasure-coded journal pool with a primary override, blocklists the Go lock owner and pauses one shard
func TestCephRBDJournalErasureFencedHoldIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, "erasure-"+rbdJournalVariantFencedHold)
}

// rbdJournalVariantFencedHold is the variant of runRBDJournalLostAppend this
// file adds.
const rbdJournalVariantFencedHold = "fenced-hold"

// rbdJournalFencedStatWait bounds the stat sent after the fence: a refusal
// is immediate, so the bound only keeps a wrong answer from blocking.
const rbdJournalFencedStatWait = 5 * time.Second

// The orders and stages of the fenced-hold cells.
var (
	rbdJournalFencedHoldOrder = []string{"setup", "lock", "tag_create", "observe-locked", "ping", "observe-placed", "replica_paused", "append_submitted", "observe-held",
		"fence", "refusal_recorded", "replica_resumed", "outcome_recorded", "observe-settled", "observe-fenced", "replay"}
	rbdJournalErasureFencedHoldOrder = []string{"setup", "lock", "tag_create", "observe-locked", "ping", "align-primary", "observe-placed", "replica_paused", "append_submitted",
		"observe-held", "fence", "refusal_recorded", "replica_resumed", "outcome_recorded", "observe-settled", "observe-fenced", "replay"}
	rbdJournalFencedHoldStages = []string{"observe-held", "refusal_recorded", "observe-fenced", "replay"}
)

// rbdJournalFencedPlan returns the stages and the order of a fenced-hold
// variant.
func rbdJournalFencedPlan(variant string) (stages, order []string) {
	if rbdJournalErasure(variant) {
		return rbdJournalFencedHoldStages, rbdJournalErasureFencedHoldOrder
	}
	return rbdJournalFencedHoldStages, rbdJournalFencedHoldOrder
}

// nativeRBDBlock is one MON blocklist entry as the fixture read it.
type nativeRBDBlock struct {
	Address string `json:"address"`
	Range   bool   `json:"range"`
	Until   string `json:"until"`
}

// nativeRBDJournalFence and nativeRBDJournalAlign mirror the fixture's
// fence and align-primary receipts (integration/fixture/rbd_journal_fenced.go).
type nativeRBDJournalFence struct {
	Address         string                   `json:"address"`
	OSD             int32                    `json:"osd"`
	PGID            string                   `json:"pgid"`
	Commands        []string                 `json:"commands"`
	BlocklistBefore []nativeRBDBlock         `json:"blocklist_before"`
	Blocklist       []nativeRBDBlock         `json:"blocklist"`
	Epoch           uint32                   `json:"epoch"`
	PGEpoch         uint32                   `json:"pg_epoch"`
	PGState         string                   `json:"pg_state"`
	Ops             []nativeRBDJournalHeldOp `json:"ops"`
	Down            []int32                  `json:"down"`
	Polls           int                      `json:"polls"`
	ElapsedMS       int64                    `json:"elapsed_ms"`
}

type nativeRBDJournalAlign struct {
	Header    nativeRBDJournalPlacement `json:"header"`
	Before    nativeRBDJournalPlacement `json:"before"`
	After     nativeRBDJournalPlacement `json:"after"`
	Commands  []string                  `json:"commands"`
	Query     *nativeRBDJournalPGQuery  `json:"pg_query"`
	Polls     int                       `json:"polls"`
	ElapsedMS int64                     `json:"elapsed_ms"`
}

// rbdJournalFenced is the fenced-hold cells' own part of the held record.
type rbdJournalFenced struct {
	// LockerAddress is the lock record's address as native librbd listed it
	// while the subject held the lock.
	LockerAddress string
	Align         *nativeRBDJournalAlign
	Fence         *nativeRBDJournalFence
	// PendingAfterFence and PendingAfterRefusal: whether the append's caller
	// was still waiting when the fence had returned and when the stat had
	// been answered.
	PendingAfterFence, PendingAfterRefusal bool
	// Refusal is what the subject was told about its one stat after the
	// fence.
	Refusal *rbdLockOwnerLossAttempt
}

// rbdJournalFencedReceipt runs one of the fixture's own fenced-hold actions
// and decodes the receipt's key into into.
func rbdJournalFencedReceipt(t *testing.T, s rbdJournalHeldSteps, action, key string, into any) {
	t.Helper()
	receipt := filepath.Join(s.dir, "rbd-journal-"+action+".json")
	if err := os.Remove(receipt); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	begun := time.Now()
	if err := testcluster.RequestControl(s.ctx, s.dir, "rbd-journal", "rbd-journal-completed", s.token, action); err != nil {
		t.Fatal("fixture", action, err)
	}
	s.record.Timings["native_"+action] = time.Since(begun).String()
	var result map[string]json.RawMessage
	if err := readMDSFreshJSON(receipt, &result); err != nil {
		t.Fatal(err)
	}
	var head struct {
		Source    string `json:"source_commit"`
		Token     string `json:"token"`
		RequestID int64  `json:"request_id"`
		Action    string `json:"action"`
		Complete  bool   `json:"complete"`
		Error     string `json:"error"`
		Mutation  *struct {
			Kind      string `json:"kind"`
			RequestID int64  `json:"request_id"`
			ReadOnly  bool   `json:"read_only"`
			State     string `json:"state"`
		} `json:"mutation"`
	}
	if err := readMDSFreshJSON(receipt, &head); err != nil {
		t.Fatal(err)
	}
	if head.Source != mdsFreshSource || head.Token != s.token || head.Action != action || !head.Complete || head.Error != "" || head.Mutation == nil ||
		head.Mutation.Kind != "native-journal-"+action || head.Mutation.State != "applied" || head.Mutation.RequestID != head.RequestID ||
		head.Mutation.ReadOnly != rbdJournalReadOnlyAction(action) || len(result[key]) == 0 || string(result[key]) == "null" {
		t.Fatal("fixture receipt differs", action, head.Error)
	}
	if err := json.Unmarshal(result[key], into); err != nil {
		t.Fatal("fixture receipt", action, err)
	}
	s.done(action)
}

// rbdJournalReadOnlyAction is the fixture's classification of an action as
// read-only (integration/fixture/rbd_journal.go rbdJournalReadOnly).
func rbdJournalReadOnlyAction(action string) bool { return strings.HasPrefix(action, "observe-") }

// rbdJournalHeldFence runs the fence of a fenced-hold cell while the append
// is held, and the one stat that probes it on the append's connection.
func rbdJournalHeldFence(t *testing.T, s rbdJournalHeldSteps, conn *cephmsgr.OSDConnection, target rbdRoute, pending func() bool) {
	t.Helper()
	f := s.record.Held.Fenced
	f.Fence = &nativeRBDJournalFence{}
	rbdJournalFencedReceipt(t, s, "fence", "fence", f.Fence)
	f.PendingAfterFence = pending()
	// The fenced owner gives up its lock: no compensating unlock is sent
	// from it, and native librbd removes the record.
	s.unlocked()
	statRequest := s.request(target, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
	statCtx, cancel := context.WithTimeout(s.ctx, rbdJournalFencedStatWait)
	defer cancel()
	reply, err := conn.Request(statCtx, statRequest)
	s.keep("stat_fenced", target, statRequest, reply, err)
	refusal := rbdLockOwnerLossOutcome("stat_fenced", reply, err)
	f.Refusal = &refusal
	f.PendingAfterRefusal = pending()
	s.done("refusal_recorded")
}

// rbdJournalFencedLocker is the lock record's address native librbd listed
// for the subject while it held the lock.
func rbdJournalFencedLocker(t *testing.T, s rbdJournalHeldSteps) string {
	t.Helper()
	l := s.locked.Lockers
	if l.Empty || len(l.Lockers) != 1 || l.Lockers[0].Address == "" || l.Lockers[0].Cookie != s.record.LockCookie || l.Lockers[0].Client != fmt.Sprintf("client.%d", s.gid) {
		t.Fatal("native librbd does not list the subject's one lock record", l)
	}
	return l.Lockers[0].Address
}
