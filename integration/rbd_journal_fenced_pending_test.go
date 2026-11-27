package integration_test

import "testing"

// The fenced pending cells join the fenced hold of
// rbd_journal_fenced_hold_test.go and the pending hold of
// rbd_journal_held_pending_test.go: three appends of the Go lock owner,
// entries 0, 1 and 2 of its tag in journal objects 0, 1 and 2, are applied
// at the primary P and wait there for one paused member when the owner is
// fenced. The fence is the fenced hold's: the lock record's exact address
// goes into the OSDMap blocklist and the fixture waits until P's PG has a
// map with the entry. Each append is invoked once, from its own goroutine,
// on the connection that took the lock, and never again; what reached P is
// its op tracker's list, matched to each call by the journal object its
// description names.
//
// The source at ceph/ceph/blob/v20.2.4/src/ gives the expected native
// behaviour, as in the fenced hold: the OSD checks the blocklist only at
// PrimaryLogPG::do_op admission and in check_blocklisted_watchers, and a
// write that passed do_op completes in the write pipeline whatever the map
// says (osd/PrimaryLogPG.cc, osd/PeeringState.cc, osd/ReplicatedBackend.cc,
// osd/ECCommon.cc). So every admitted append completes, each with its own
// answer for its own transaction, while a later request on the same
// connection is refused with -108; the PG log orders the three writes by
// their arrival at P, and native librbd's replay reads each journal object
// of the object set in turn.
//
// The subject never resends an append, never applies or commits an entry,
// and does not release the lock after the fence: native librbd's acquisition
// breaks the record of the owner whose watch the fence dropped. Consumer
// choices kept explicit here: three appends, all held for the same paused
// member, none of their local waits canceled, which member is paused, the
// primary override of the erasure-coded cell, the one stat used to probe the
// fence and leaving the lock removal to native librbd. Not covered: appends
// on several connections or to several PGs, a fence that reaches P before
// some of the appends, and an owner whose OSD connections share one nonce.
//
// Everything journal-specific is the test-only consumer code of
// rbd_journal_test.go. The product is unchanged.

// TestCephRBDJournalFencedPendingHoldIntegration fences the subject while
// three of its appends wait at P. After a watch ping the subject leases a
// pause of the last acting member of the PG and sends entries 0, 1 and 2 to
// P on the connection that took the lock. While all three callers are
// pending, P lists exactly three client ops of the subject, one
// `journal.append` call to each journal object, each at "waiting for sub ops"
// with its own op_commit and one "sub_op_commit_rec". The fixture's fence
// then blocklists the lock record's address, which the MON lists afterwards
// and not before, and waits until P's PG answers its query at an epoch at
// least the OSDMap epoch read after the entry; P still lists the three ops
// unchanged and every caller is still pending. On the same connection the
// subject sends one stat of journal object 0, which P refuses at once with
// -108 while the appends stay pending. The subject resumes the member within
// the lease and each caller receives one known answer 0 on disk for the
// transaction P listed for it. The PG is in the interval the lock was taken
// in with every peer at P's new last update. A native observation shows each
// journal object as its bytes before the appends followed by exactly its
// entry, the image and the image client's commit position unchanged, the
// lock record still the subject's, its watch gone and its address
// blocklisted. Native librbd then acquires the lock (breaking the fenced
// owner's record) and replays the three entries once each, in order: the
// image is the setup with the three extents, the image client's commit
// position names entry 2 at splay offset 2, entry 1 at offset 1 and entry 0
// at offset 0, and the next tag's predecessor is entry 2.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalfencedpendinghold key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-fenced-pending-hold
//scenario:row rbd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalfencedpendinghold key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-fenced-pending-hold
//scenario:covers RBD underlying I/O
//fixture:isolated blocklists the Go lock owner while three appends wait for a paused replica OSD, leaving a journaling image
func TestCephRBDJournalFencedPendingHoldIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, rbdJournalVariantFencedPendingHold)
}

// TestCephRBDJournalErasureFencedPendingHoldIntegration is the fenced
// pending hold with the journal's data objects in the erasure-coded journal
// pool (k=2, m=1 over the three BlueStore OSDs, overwrites allowed, one PG).
// The fixture's align-primary first makes the journal PG's acting primary
// the image header's primary H when they differ, with one `osd primary-temp`
// that keeps every shard in its slot, as in
// TestCephRBDJournalErasureFencedHoldIntegration, so the three appends
// travel on the connection to H that took the lock. The member paused is
// the last acting member other than H. P lists each op at "started" with its
// own shard's sub-write started and committed, as in the erasure pending
// hold; the fence, the -108 refusal of the stat, the three known answers 0
// after the resume and the native postconditions are those of the
// replicated cell.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalerasurefencedpendinghold key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-erasure-fenced-pending-hold
//scenario:row rbd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalerasurefencedpendinghold key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-erasure-fenced-pending-hold
//scenario:covers RBD underlying I/O
//fixture:isolated adds an erasure-coded journal pool with a primary override, blocklists the Go lock owner, pauses one shard
func TestCephRBDJournalErasureFencedPendingHoldIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, "erasure-"+rbdJournalVariantFencedPendingHold)
}

// rbdJournalVariantFencedPendingHold is the variant of
// runRBDJournalLostAppend this file adds.
const rbdJournalVariantFencedPendingHold = "fenced-pending-hold"

// The orders and stages of the fenced pending cells.
var (
	rbdJournalFencedPendingHoldOrder = []string{"setup", "lock", "tag_create", "observe-locked", "ping", "observe-placed", "replica_paused", "appends_submitted", "observe-held",
		"fence", "refusal_recorded", "replica_resumed", "outcomes_recorded", "observe-settled", "observe-fenced", "replay"}
	rbdJournalErasureFencedPendingHoldOrder = []string{"setup", "lock", "tag_create", "observe-locked", "ping", "align-primary", "observe-placed", "replica_paused",
		"appends_submitted", "observe-held", "fence", "refusal_recorded", "replica_resumed", "outcomes_recorded", "observe-settled", "observe-fenced", "replay"}
	rbdJournalFencedPendingHoldStages = []string{"observe-held", "refusal_recorded", "observe-fenced", "replay"}
)

// rbdJournalFencedPendingPlan returns the stages and the order of a fenced
// pending variant.
func rbdJournalFencedPendingPlan(variant string) (stages, order []string) {
	if rbdJournalErasure(variant) {
		return rbdJournalFencedPendingHoldStages, rbdJournalErasureFencedPendingHoldOrder
	}
	return rbdJournalFencedPendingHoldStages, rbdJournalFencedPendingHoldOrder
}
