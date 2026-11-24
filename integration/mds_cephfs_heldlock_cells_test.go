package integration_test

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Held-lock recovery cells of the unsplit recovery recipe (issue #10): a lock
// request waiting across the restart and granted or interrupted on the new
// incarnation, one owner's merged and split FCNTL ranges, locks of two client
// sessions, a dirty locked inode, and the reconnect record without its lock
// blob. Each cell runs runCephFSRecovery with its own recipe on a fresh
// metadata fixture, because the recipe performs the fixture's only owned MDS
// restart, and each is its own acceptance result. The lock requests are raw
// SETFILELOCK/GETFILELOCK arguments and the blobs the test-only builder of
// mds_cephfs_heldflock_test.go; the product implements no lock policy.

// heldLockStep is one SETFILELOCK of a recipe before the restart: an acquire
// of Lock with its own type, or with Unlock an unlock of Lock's range.
type heldLockStep struct {
	Lock   heldLockSpec
	Unlock bool `json:",omitempty"`
}

// heldLockWaiter is a blocking acquire (wait=1) that conflicts with the
// recipe's held locks. Client::_encode_filelocks encodes only held locks, so
// the reconnect blob does not carry it: the waiting request itself is
// resubmitted with ReconnectSession, as a native client resends a request
// without a reply, and waits again on the new incarnation. With Interrupt it
// is then interrupted there (Client::_interrupt_filelock: the same arguments
// with the *_INTR rule and type unlock) and answers -EINTR; otherwise it is
// granted once the held locks are unlocked.
type heldLockWaiter struct {
	Lock      heldLockSpec
	Interrupt bool `json:",omitempty"`
}

// args is the filelock_change of the waiting acquire: the lock's own type
// with wait set.
func (w heldLockWaiter) args(l heldLock) [56]byte {
	args := l.args(l.Type)
	args[34] = 1
	return args
}

// interruptArgs is the filelock_change of the interrupt of the waiting
// acquire: its arguments, including wait, with the *_INTR rule (FCNTL 1 to 3,
// FLOCK 2 to 4) and type unlock.
func (w heldLockWaiter) interruptArgs(l heldLock) [56]byte {
	args := w.args(l)
	args[0], args[1] = l.Rule+2, cephFlockUnlock
	return args
}

var (
	// waitingHeldFlock is the recipe of
	// TestCephMDSCephFSWaitingFlockRecoveryIntegration.
	waitingHeldFlock = heldFlockRecipe{Name: "waiting-then-granted", Results: "mds-cephfs-waitflock-recovery-results.json",
		Locks:  []heldLockSpec{{Rule: cephLockRuleFlock, Type: cephFlockExclusive, Owner: cephFlockGoOwner}},
		Waiter: &heldLockWaiter{Lock: heldLockSpec{Rule: cephLockRuleFlock, Type: cephFlockExclusive, Owner: cephFlockGoSecondOwner}}}
	// interruptedHeldFlock is the recipe of
	// TestCephMDSCephFSInterruptedFlockRecoveryIntegration.
	interruptedHeldFlock = heldFlockRecipe{Name: "waiting-then-interrupted", Results: "mds-cephfs-intrflock-recovery-results.json",
		Locks:  []heldLockSpec{{Rule: cephLockRuleFlock, Type: cephFlockExclusive, Owner: cephFlockGoOwner}},
		Waiter: &heldLockWaiter{Lock: heldLockSpec{Rule: cephLockRuleFlock, Type: cephFlockExclusive, Owner: cephFlockGoSecondOwner}, Interrupt: true}}
	// fcntlRangeHeldLocks is the recipe of
	// TestCephMDSCephFSFcntlMergedRangeRecoveryIntegration. One owner locks
	// [0,16) and then the adjacent [16,32) exclusively, which the MDS
	// coalesces into one lock [0,32) (ceph_lock_state_t::adjust_locks merges
	// a same-owner, same-type neighbour). Another owner locks [64,128) shared
	// and unlocks [80,96), which leaves two locks [64,80) and [96,128)
	// (ceph_lock_state_t::remove_lock). The client's own lock state does the
	// same, so Client::_encode_filelocks sends exactly these three records.
	fcntlRangeHeldLocks = heldFlockRecipe{Name: "fcntl-merged-and-split", Results: "mds-cephfs-fcntlranges-recovery-results.json", FcntlProbe: "fcntl-probe-ranges",
		Steps: []heldLockStep{
			{Lock: heldLockSpec{Rule: cephLockRuleFcntl, Type: cephFlockExclusive, Owner: cephFlockGoOwner, PID: 4101, Start: 0, Length: 16}},
			{Lock: heldLockSpec{Rule: cephLockRuleFcntl, Type: cephFlockExclusive, Owner: cephFlockGoOwner, PID: 4101, Start: 16, Length: 16}},
			{Lock: heldLockSpec{Rule: cephLockRuleFcntl, Type: cephFlockShared, Owner: cephFlockGoSecondOwner, PID: 4103, Start: 64, Length: 64}},
			{Lock: heldLockSpec{Rule: cephLockRuleFcntl, Type: cephFlockShared, Owner: cephFlockGoSecondOwner, PID: 4103, Start: 80, Length: 16}, Unlock: true},
		},
		Locks: []heldLockSpec{
			{Rule: cephLockRuleFcntl, Type: cephFlockExclusive, Owner: cephFlockGoOwner, PID: 4101, Start: 0, Length: 32},
			{Rule: cephLockRuleFcntl, Type: cephFlockShared, Owner: cephFlockGoSecondOwner, PID: 4103, Start: 64, Length: 16},
			{Rule: cephLockRuleFcntl, Type: cephFlockShared, Owner: cephFlockGoSecondOwner, PID: 4103, Start: 96, Length: 32},
		}}
	// twoClientHeldFlock is the recipe of
	// TestCephMDSCephFSTwoClientFlockRecoveryIntegration: shared whole-file
	// FLOCK locks of two owners on two Go client sessions of one inode.
	twoClientHeldFlock = heldFlockRecipe{Name: "shared-two-clients", Results: "mds-cephfs-twoclient-recovery-results.json",
		Locks: []heldLockSpec{{Rule: cephLockRuleFlock, Type: cephFlockShared, Owner: cephFlockGoOwner}, {Rule: cephLockRuleFlock, Type: cephFlockShared, Owner: cephFlockGoSecondOwner, Second: true}}}
	// dirtyHeldFlock is the recipe of
	// TestCephMDSCephFSDirtyFlockRecoveryIntegration.
	dirtyHeldFlock = heldFlockRecipe{Name: "exclusive-dirty-inode", Results: "mds-cephfs-dirtyflock-recovery-results.json",
		Locks: []heldLockSpec{{Rule: cephLockRuleFlock, Type: cephFlockExclusive, Owner: cephFlockGoOwner}}, DirtySize: 48}
	// noBlobHeldFlock is the recipe of
	// TestCephMDSCephFSFlockWithoutBlobRecoveryIntegration.
	noBlobHeldFlock = heldFlockRecipe{Name: "exclusive-without-blob", Results: "mds-cephfs-noblob-recovery-results.json",
		Locks: []heldLockSpec{{Rule: cephLockRuleFlock, Type: cephFlockExclusive, Owner: cephFlockGoOwner}}, NoBlob: true}
)

// TestCephMDSCephFSWaitingFlockRecoveryIntegration holds one exclusive FLOCK
// lock and submits a blocking exclusive acquire of a second owner behind it
// before the owned MDS restart. The native op tracker of each incarnation must
// show that request in flight and "failed to add lock, waiting", and its local
// wait must expire without a reply. The reconnect blob carries only the held
// lock; the waiting request is resubmitted once with its original TID. After
// adoption the native probe is still refused and GETFILELOCK names the first
// owner; once that owner unlocks, the resubmitted request is granted (result
// 0), GETFILELOCK names the second owner and the native probe stays refused
// until it unlocks too. Not covered: a waiter of another client and FCNTL
// waiters.
//
//scenario:row mds-cephfs-waitflock-recovery.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfswaitingflockrecovery key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs=1 capabilities=mds-held-flock-wait
//scenario:row mds-cephfs-waitflock-recovery.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfswaitingflockrecovery key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs=1 capabilities=mds-held-flock-wait
//scenario:covers CLIENT to MDS/CephFS
//fixture:isolated pauses the MDS journal, fails the owned rank-0 MDS and evicts a session (blocklist entry); no restore proof
func TestCephMDSCephFSWaitingFlockRecoveryIntegration(t *testing.T) {
	runCephFSRecovery(t, waitingHeldFlock)
}

// TestCephMDSCephFSInterruptedFlockRecoveryIntegration is the waiting cell
// with the resubmitted waiting request interrupted on the new incarnation: the
// interrupt answers 0, the waiting request -EINTR, the native op tracker no
// longer holds it, and after the first owner's unlock the native probe
// acquires, so the interrupted waiter was never granted. Not covered: the
// interrupt-versus-grant race and an interrupt sent before the restart.
//
//scenario:row mds-cephfs-intrflock-recovery.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsinterruptedflockrecovery key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs=1 capabilities=mds-held-flock-wait
//scenario:row mds-cephfs-intrflock-recovery.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsinterruptedflockrecovery key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs=1 capabilities=mds-held-flock-wait
//scenario:covers CLIENT to MDS/CephFS
//fixture:isolated pauses the MDS journal, fails the owned rank-0 MDS and evicts a session (blocklist entry); no restore proof
func TestCephMDSCephFSInterruptedFlockRecoveryIntegration(t *testing.T) {
	runCephFSRecovery(t, interruptedHeldFlock)
}

// TestCephMDSCephFSFcntlMergedRangeRecoveryIntegration holds one owner's
// merged exclusive FCNTL range and another owner's split shared range
// (fcntlRangeHeldLocks) across the owned MDS restart. Before the restart the
// native ceph_getlk results show the MDS's own merge and split (the merged
// lock reported as [0,32), the hole [80,96) free); after the restart they
// must be the same from the three-record blob, and after each unlock match
// the records still held. Not covered: a merge or split made after the
// restart and ranges of one owner on several inodes.
//
//scenario:row mds-cephfs-fcntlranges-recovery.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsfcntlmergedrangerecovery key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs=1 capabilities=mds-held-fcntl-ranges
//scenario:row mds-cephfs-fcntlranges-recovery.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsfcntlmergedrangerecovery key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs=1 capabilities=mds-held-fcntl-ranges
//scenario:covers CLIENT to MDS/CephFS
//fixture:isolated pauses the MDS journal, fails the owned rank-0 MDS and evicts a session (blocklist entry); no restore proof
func TestCephMDSCephFSFcntlMergedRangeRecoveryIntegration(t *testing.T) {
	runCephFSRecovery(t, fcntlRangeHeldLocks)
}

// TestCephMDSCephFSTwoClientFlockRecoveryIntegration holds shared FLOCK locks
// of two owners on two Go client sessions (two GlobalIDs of the recovery
// identity) on one inode across the owned MDS restart. Both sessions become
// Lost; each reconnects to the new incarnation with its own capability and
// its own one-record blob, the restart client first, so the MDS restores the
// locks in that order. GETFILELOCK from the restart client names its own lock
// first and, after its unlock, the second client's lock, with that client's
// GlobalID; the native exclusive probe stays refused until both unlock. Not
// covered: more than two clients and a client that does not reconnect.
//
//scenario:row mds-cephfs-twoclient-recovery.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfstwoclientflockrecovery key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs=1 capabilities=exclusive-mds-cephfs
//scenario:row mds-cephfs-twoclient-recovery.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfstwoclientflockrecovery key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs=1 capabilities=exclusive-mds-cephfs
//scenario:covers CLIENT to MDS/CephFS
//fixture:isolated pauses the MDS journal, fails the owned rank-0 MDS and evicts a session (blocklist entry); no restore proof
func TestCephMDSCephFSTwoClientFlockRecoveryIntegration(t *testing.T) {
	runCephFSRecovery(t, twoClientHeldFlock)
}

// TestCephMDSCephFSDirtyFlockRecoveryIntegration holds one exclusive FLOCK
// lock on an inode that is also dirty across the owned MDS restart: a caller
// UPDATE with dirty Fw and a 48-byte size precedes MClientReconnect, whose
// record for the same inode carries the lock blob. The flush must be
// acknowledged under its original TID, the native probes then see the
// 48-byte sparse file, and the lock must be restored and released as in the
// exclusive cell. The size flush writes no object data. Not covered: dirty
// buffered data and a FLUSHSNAP on a locked inode.
//
//scenario:row mds-cephfs-dirtyflock-recovery.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsdirtyflockrecovery key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs=1 capabilities=exclusive-mds-cephfs
//scenario:row mds-cephfs-dirtyflock-recovery.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsdirtyflockrecovery key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs=1 capabilities=exclusive-mds-cephfs
//scenario:covers CLIENT to MDS/CephFS
//fixture:isolated pauses the MDS journal, fails the owned rank-0 MDS and evicts a session (blocklist entry); no restore proof
func TestCephMDSCephFSDirtyFlockRecoveryIntegration(t *testing.T) {
	runCephFSRecovery(t, dirtyHeldFlock)
}

// TestCephMDSCephFSFlockWithoutBlobRecoveryIntegration is the durability
// counterfactual of the held-lock cells. The MDS keeps file locks only in
// memory: they are not journaled and not part of the stored inode
// (CInode::_encode_file_locks is used for export and lock-state replication
// only), so a new incarnation learns them only from the reconnect records
// (Server::recover_filelocks). The cell holds one exclusive FLOCK lock that
// the native probe sees, restarts the owned MDS and reconnects the same
// session and capability without the lock blob. After adoption the native
// exclusive probe acquires and GETFILELOCK reports no conflict: the lock did
// not survive the restart. The Go unlock that follows answers 0. Not
// covered: a client that does not reconnect at all.
//
//scenario:row mds-cephfs-noblob-recovery.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsflockwithoutblobrecovery key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs=1 capabilities=exclusive-mds-cephfs
//scenario:row mds-cephfs-noblob-recovery.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsflockwithoutblobrecovery key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs=1 capabilities=exclusive-mds-cephfs
//scenario:covers CLIENT to MDS/CephFS
//fixture:isolated pauses the MDS journal, fails the owned rank-0 MDS and evicts a session (blocklist entry); no restore proof
func TestCephMDSCephFSFlockWithoutBlobRecoveryIntegration(t *testing.T) {
	runCephFSRecovery(t, noBlobHeldFlock)
}

// steps returns the SETFILELOCK requests before the restart.
func (r heldFlockRecipe) steps() []heldLockStep {
	if r.Steps != nil {
		return r.Steps
	}
	out := make([]heldLockStep, 0, len(r.Locks))
	for _, l := range r.Locks {
		out = append(out, heldLockStep{Lock: l})
	}
	return out
}

// stepName labels step i in the request evidence; without explicit steps it
// is the acquire label of lock i.
func (r heldFlockRecipe) stepName(i int) string {
	if r.Steps == nil {
		return r.callName("acquire", i)
	}
	st := r.Steps[i]
	kind, rule := "exclusive", "flock"
	if st.Lock.Type == cephFlockShared {
		kind = "shared"
	}
	if st.Lock.Rule == cephLockRuleFcntl {
		rule = "fcntl"
	}
	op := "acquire"
	if st.Unlock {
		op = "unlock"
	}
	return fmt.Sprintf("step-%d-%s-%s-%s-%d-%d", i, rule, kind, op, st.Lock.Start, st.Lock.Length)
}

// hasSecond reports whether the recipe holds a lock of a second client.
func (r heldFlockRecipe) hasSecond() bool {
	return slices.ContainsFunc(r.Locks, func(l heldLockSpec) bool { return l.Second })
}

// heldLocksOfSession returns the held locks of one session: the restart client's,
// or with second set the second client's.
func heldLocksOfSession(locks []heldLock, recipe heldFlockRecipe, second bool) []heldLock {
	var out []heldLock
	for i, l := range locks {
		if recipe.Locks[i].Second == second {
			out = append(out, l)
		}
	}
	return out
}

// expectedCalls is the exact SETFILELOCK sequence of the recipe: the steps,
// the waiting acquire and its interrupt, then one unlock per held lock in
// order and, for a granted waiter, its unlock.
func (r heldFlockRecipe) expectedCalls(client, second uint64) []heldFlockCall {
	var want []heldFlockCall
	for _, st := range r.steps() {
		l := st.Lock.held(client, second)
		kind := l.Type
		if st.Unlock {
			kind = cephFlockUnlock
		}
		want = append(want, heldFlockWantCall(l, l.Rule, kind, 0))
	}
	w := r.Waiter
	var waiter heldLock
	if w != nil {
		waiter = w.Lock.held(client, second)
		result := int32(0)
		if w.Interrupt {
			result = -int32(syscall.EINTR)
		}
		want = append(want, heldFlockWantCall(waiter, waiter.Rule, waiter.Type, result))
		if w.Interrupt {
			want = append(want, heldFlockWantCall(waiter, waiter.Rule+2, cephFlockUnlock, 0))
		}
	}
	for _, l := range r.locksFor(client, second) {
		want = append(want, heldFlockWantCall(l, l.Rule, cephFlockUnlock, 0))
	}
	if w != nil && !w.Interrupt {
		want = append(want, heldFlockWantCall(waiter, waiter.Rule, cephFlockUnlock, 0))
	}
	return want
}

// heldLockResubmissions checks the reconnect resubmissions of SETFILELOCK in
// the common request ledger: exactly the waiting acquire, once, with its
// original TID, or none without a waiter.
func heldLockResubmissions(requests []*recoveryRequestEvidence, waiterTID uint64) error {
	var got []uint64
	for _, r := range requests {
		if r.Op == cephmsgr.MDSOpSetFileLock && r.ResubmittedFrom != 0 {
			if r.Transaction != r.ResubmittedFrom {
				return fmt.Errorf("resubmitted SETFILELOCK changed its TID from %d to %d", r.ResubmittedFrom, r.Transaction)
			}
			got = append(got, r.Transaction)
		}
	}
	var want []uint64
	if waiterTID != 0 {
		want = []uint64{waiterTID}
	}
	if !slices.Equal(got, want) {
		return fmt.Errorf("resubmitted SETFILELOCK TIDs %v, want %v", got, want)
	}
	return nil
}

// heldLockCellEvidence is the retained record of a cell's additions to the
// held-lock restart.
type heldLockCellEvidence struct {
	// SecondGlobalID and SecondBlobHex are the second client's identity and
	// the blob its reconnect record carried.
	SecondGlobalID uint64 `json:",omitempty"`
	SecondBlobHex  string `json:",omitempty"`
	// WaiterTID is the waiting acquire's TID, before and after the restart.
	WaiterTID uint64 `json:",omitempty"`
	// WaiterLocalWaits are the local waits that had to expire, before the
	// restart and on the new incarnation.
	WaiterLocalWaits []string `json:",omitempty"`
	// InFlight are the native op tracker reads of the waiting request.
	InFlight []heldLockInFlightRead `json:",omitempty"`
	// InterruptTID is the interrupt's TID.
	InterruptTID uint64 `json:",omitempty"`
	// DirtyFlushTID and DirtyFlushAck are the lock inode's pending flush.
	DirtyFlushTID uint64 `json:",omitempty"`
	DirtyFlushAck any    `json:",omitempty"`
	// BlobOmitted records the counterfactual reconnect without the blob.
	BlobOmitted bool `json:",omitempty"`
}

// heldLockInFlightRead is one lock-inflight receipt and when it was read.
type heldLockInFlightRead struct {
	When    string
	Receipt *nativeMDSCephFSLockInFlight
}

// nativeMDSCephFSLockInFlight is the fixture's read of the current rank-0
// daemon's op tracker for one request (mds_cephfs_heldlocks.go).
type nativeMDSCephFSLockInFlight struct {
	GlobalID uint64                     `json:"global_id"`
	TID      uint64                     `json:"tid"`
	MDSGID   uint64                     `json:"mds_gid"`
	InFlight bool                       `json:"in_flight"`
	Op       *nativeMDSCephFSHistoricOp `json:"op"`
}

// heldLockWaitingEvent is the op-tracker event of a SETFILELOCK that could
// not be granted and waits (Server::handle_client_file_setlock).
const heldLockWaitingEvent = "failed to add lock, waiting"

// checkHeldLockInFlight checks one lock-inflight receipt of request gid:tid on
// the daemon mds. With waiting set the request must be tracked as a waiting
// setfilelock with wait 1; otherwise it must not be tracked at all.
func checkHeldLockInFlight(r nativeMDSCephFSReceipt, gid, tid, mds uint64, waiting bool) error {
	f := r.LockInFlight
	if f == nil || r.Action != "lock-inflight" || f.GlobalID != gid || f.TID != tid || f.MDSGID != mds || r.MDSGID != mds || f.InFlight != (f.Op != nil) {
		return fmt.Errorf("native in-flight lock receipt scope differs: %+v", f)
	}
	if !waiting {
		if f.InFlight {
			return fmt.Errorf("the MDS still tracks request %d:%d: %+v", gid, tid, *f.Op)
		}
		return nil
	}
	if !f.InFlight {
		return fmt.Errorf("the MDS does not track the waiting request %d:%d", gid, tid)
	}
	prefix := fmt.Sprintf("client_request(client.%d:%d setfilelock ", gid, tid)
	if !strings.HasPrefix(f.Op.Description, prefix) || !strings.Contains(f.Op.Description, "wait 1") || !slices.Contains(f.Op.Events, heldLockWaitingEvent) {
		return fmt.Errorf("the tracked request %d:%d is not a waiting setfilelock: %+v", gid, tid, *f.Op)
	}
	return nil
}

// TestCephFSHeldLockCellRecipes is a cluster-free check of the cell recipes:
// their reconnect blobs, probe expectations at each step, exact SETFILELOCK
// sequences and the waiter arguments.
func TestCephFSHeldLockCellRecipes(t *testing.T) {
	const gid, second = 4711, 4712
	exclusive := cephfsFilelock{Client: gid, Owner: cephFlockGoOwner, Type: cephFlockExclusive}
	for _, r := range []heldFlockRecipe{waitingHeldFlock, interruptedHeldFlock, dirtyHeldFlock, noBlobHeldFlock} {
		locks := r.locks(gid)
		if len(locks) != 1 || locks[0].cephfsFilelock != exclusive || len(r.steps()) != 1 || r.steps()[0].Unlock || r.hasSecond() {
			t.Fatal("a one-lock cell does not hold exactly the exclusive Go lock", r.Name, locks)
		}
	}
	results := map[string]bool{}
	for _, r := range []heldFlockRecipe{exclusiveHeldFlock, sharedHeldFlock, fcntlHeldLocks, waitingHeldFlock, interruptedHeldFlock, fcntlRangeHeldLocks, twoClientHeldFlock, dirtyHeldFlock, noBlobHeldFlock} {
		if results[r.Results] || r.Results == "" {
			t.Fatal("two recipes share an evidence file", r.Results)
		}
		results[r.Results] = true
	}

	// Waiter arguments: the waiting acquire sets wait; its interrupt keeps the
	// arguments and changes only rule and type.
	w := waitingHeldFlock.Waiter
	wl := w.Lock.held(gid, second)
	args := w.args(wl)
	if args[0] != cephLockRuleFlock || args[1] != cephFlockExclusive || binary.LittleEndian.Uint64(args[2:10]) != cephFlockGoSecondOwner || args[34] != 1 {
		t.Fatalf("waiting acquire arguments differ: %x", args)
	}
	intr := w.interruptArgs(wl)
	want := args
	want[0], want[1] = 4, cephFlockUnlock
	if intr != want || interruptedHeldFlock.Waiter.interruptArgs(wl) != want {
		t.Fatalf("interrupt arguments differ: %x", intr)
	}
	if wl.Owner == cephFlockGoOwner || wl.Owner == cephFlockGoOwner+1 || wl.Owner == cephFlockNativeOwner {
		t.Fatal("the waiter owner is not distinct from the held, testing and native owners")
	}
	// The blob carries only the held lock, never the waiter.
	if fcntl, flock, err := cephfsDecodeFilelockBlob(heldLockBlob(waitingHeldFlock.locks(gid))); err != nil || len(fcntl) != 0 || len(flock) != 1 || flock[0] != exclusive {
		t.Fatal("the waiting cell's blob differs from the held lock alone", fcntl, flock, err)
	}

	// Exact call sequences.
	acquire := heldFlockWantCall(waitingHeldFlock.locks(gid)[0], cephLockRuleFlock, cephFlockExclusive, 0)
	unlock := heldFlockWantCall(waitingHeldFlock.locks(gid)[0], cephLockRuleFlock, cephFlockUnlock, 0)
	waitGranted := heldFlockWantCall(wl, cephLockRuleFlock, cephFlockExclusive, 0)
	waitUnlock := heldFlockWantCall(wl, cephLockRuleFlock, cephFlockUnlock, 0)
	waitEINTR := heldFlockWantCall(wl, cephLockRuleFlock, cephFlockExclusive, -4)
	interrupt := heldFlockWantCall(wl, 4, cephFlockUnlock, 0)
	for name, tc := range map[string]struct {
		recipe heldFlockRecipe
		want   []heldFlockCall
	}{
		"waiting":     {waitingHeldFlock, []heldFlockCall{acquire, waitGranted, unlock, waitUnlock}},
		"interrupted": {interruptedHeldFlock, []heldFlockCall{acquire, waitEINTR, interrupt, unlock}},
		"dirty":       {dirtyHeldFlock, []heldFlockCall{acquire, unlock}},
		"no blob":     {noBlobHeldFlock, []heldFlockCall{acquire, unlock}},
	} {
		if got := tc.recipe.expectedCalls(gid, second); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%s call sequence differs:\n got %+v\nwant %+v", name, got, tc.want)
		}
	}
	// The plain recipes keep their sequence of one acquire and one unlock per
	// lock.
	for _, r := range []heldFlockRecipe{exclusiveHeldFlock, sharedHeldFlock, fcntlHeldLocks} {
		var calls []heldFlockCall
		for i, c := range r.expectedCalls(gid, gid) {
			c.TID = uint64(10 + i)
			calls = append(calls, c)
		}
		if err := heldFlockCallsExact(calls, r.locks(gid)); err != nil {
			t.Fatal("a plain recipe's sequence changed", r.Name, err)
		}
	}
	record := func(r heldFlockRecipe, edit func([]heldFlockCall)) []heldFlockCall {
		calls := r.expectedCalls(gid, second)
		for i := range calls {
			calls[i].TID = uint64(20 + i)
		}
		if edit != nil {
			edit(calls)
		}
		return calls
	}
	if err := heldFlockCallsMatch(record(interruptedHeldFlock, nil), interruptedHeldFlock.expectedCalls(gid, second)); err != nil {
		t.Fatal(err)
	}
	for name, calls := range map[string][]heldFlockCall{
		"interrupted waiter granted": record(interruptedHeldFlock, func(c []heldFlockCall) { c[1].Outcome = "reply result 0" }),
		"interrupt refused":          record(interruptedHeldFlock, func(c []heldFlockCall) { c[2].Outcome = "reply result -22" }),
		"plain unlock as interrupt":  record(interruptedHeldFlock, func(c []heldFlockCall) { c[2].Rule = cephLockRuleFlock }),
		"waiter never answered":      record(interruptedHeldFlock, func(c []heldFlockCall) { c[1].Outcome = "" }),
		"interrupt without TID":      record(interruptedHeldFlock, func(c []heldFlockCall) { c[2].TID = 0 }),
	} {
		if heldFlockCallsMatch(calls, interruptedHeldFlock.expectedCalls(gid, second)) == nil {
			t.Fatal("wrong interrupted sequence accepted:", name)
		}
	}
	if heldFlockCallsMatch(record(waitingHeldFlock, func(c []heldFlockCall) { c[1].Outcome = "reply result -11" }), waitingHeldFlock.expectedCalls(gid, second)) == nil {
		t.Fatal("a refused waiter was accepted as granted")
	}

	// Probes of the waiting cell: the waiter alone after the first unlock
	// refuses the native exclusive probe like the held lock did.
	exNo, exYes := heldFlockProbeWant{Action: "flock-ex-nb", Errno: 11}, heldFlockProbeWant{Action: "flock-ex-nb"}
	if got := waitingHeldFlock.probesFor([]heldLock{wl}, 0); !reflect.DeepEqual(got, []heldFlockProbeWant{exNo}) {
		t.Fatal("probes with the granted waiter differ", got)
	}
	if got := waitingHeldFlock.probesFor(nil, 0); !reflect.DeepEqual(got, []heldFlockProbeWant{exYes}) {
		t.Fatal("probes with nothing held differ", got)
	}
	// The dirty cell probes the flushed size; the file must be zeros.
	if got := dirtyHeldFlock.probesFor(dirtyHeldFlock.locks(gid), 48); len(got) != 1 || got[0].Size != 48 || got[0].Errno != 11 {
		t.Fatal("dirty probes differ", got)
	}
	zeros := nativeMDSCephFSFlockState{Ino: 2, Size: 48, DataHex: strings.Repeat("00", 48)}
	if !heldLockFileIs(zeros, 48) || heldLockFileIs(zeros, 0) || heldLockFileIs(nativeMDSCephFSFlockState{Size: 48, DataHex: strings.Repeat("00", 47) + "01"}, 48) ||
		!heldLockFileIs(nativeMDSCephFSFlockState{}, 0) || heldLockFileIs(nativeMDSCephFSFlockState{Size: 0, DataHex: "00"}, 0) {
		t.Fatal("owned file state check accepts nonzero bytes or another size")
	}
	goGIDs := map[uint64]bool{gid: true}
	receipt := func(errno int, state nativeMDSCephFSFlockState) nativeMDSCephFSReceipt {
		var r nativeMDSCephFSReceipt
		if err := json.Unmarshal([]byte(`{"request_id":9,"action":"flock-ex-nb","mutation":{"kind":"native-flock-ex-nb","request_id":9,"state":"applied"}}`), &r); err != nil {
			t.Fatal(err)
		}
		r.Native = &nativeMDSCephFSOutput{Action: "flock-ex-nb", NativeGlobalID: 77,
			Flock: &nativeMDSCephFSFlockProbe{Owner: cephFlockNativeOwner, Errno: errno, Acquired: errno == 0, Unlocked: errno == 0, ElapsedNS: 1, Before: state, After: state}}
		return r
	}
	var baseline *nativeMDSCephFSFlockState
	if err := checkHeldFlockProbe(receipt(11, zeros), heldFlockProbeWant{Action: "flock-ex-nb", Errno: 11, Size: 48}, 2, goGIDs, &baseline); err != nil {
		t.Fatal("dirty probe refused", err)
	}
	baseline = nil
	if checkHeldFlockProbe(receipt(11, zeros), exNo, 2, goGIDs, &baseline) == nil {
		t.Fatal("a 48-byte file was accepted where the empty file was expected")
	}
	baseline = nil
	if checkHeldFlockProbe(receipt(11, nativeMDSCephFSFlockState{Ino: 2}), heldFlockProbeWant{Action: "flock-ex-nb", Errno: 11, Size: 48}, 2, goGIDs, &baseline) == nil {
		t.Fatal("an unflushed file was accepted after the dirty restart")
	}

	// Resubmissions: only the waiter, with its own TID.
	ledger := []*recoveryRequestEvidence{{Op: cephmsgr.MDSOpCreate, Transaction: 3, ResubmittedFrom: 3}, {Op: cephmsgr.MDSOpSetFileLock, Transaction: 7, ResubmittedFrom: 7}}
	if err := heldLockResubmissions(ledger, 7); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		ledger []*recoveryRequestEvidence
		tid    uint64
	}{
		"waiter not resubmitted": {ledger[:1], 7},
		"another lock request":   {ledger, 8},
		"resubmitted twice":      {append(slices.Clone(ledger), ledger[1]), 7},
		"lock without a waiter":  {ledger, 0},
		"TID changed":            {[]*recoveryRequestEvidence{{Op: cephmsgr.MDSOpSetFileLock, Transaction: 8, ResubmittedFrom: 7}}, 7},
	} {
		if heldLockResubmissions(tc.ledger, tc.tid) == nil {
			t.Fatal("wrong SETFILELOCK resubmissions accepted:", name)
		}
	}
	// A resubmission is not a new submission of the hooked calls.
	calls := []heldFlockCall{{TID: 5}, {TID: 7}}
	if err := heldFlockLedgerMatches([]*recoveryRequestEvidence{{Op: cephmsgr.MDSOpSetFileLock, Transaction: 5}, {Op: cephmsgr.MDSOpSetFileLock, Transaction: 7},
		{Op: cephmsgr.MDSOpSetFileLock, Transaction: 7, ResubmittedFrom: 7}}, calls); err != nil {
		t.Fatal("a waiter resubmission was counted as a new submission", err)
	}
}

// TestCephFSHeldLockFcntlRanges is a cluster-free check of the merged and
// split range recipe: its steps, its three held records, the native result
// of every probe request and the GETFILELOCK answers.
func TestCephFSHeldLockFcntlRanges(t *testing.T) {
	const gid = 4711
	r := fcntlRangeHeldLocks
	locks := r.locks(gid)
	if len(locks) != 3 || len(r.steps()) != 4 || r.fcntlProbe() != "fcntl-probe-ranges" || !slices.Equal(r.rules(), []byte{cephLockRuleFcntl}) {
		t.Fatal("the range recipe changed", locks)
	}
	// The held records are what the steps leave: the two adjacent exclusive
	// ranges of the first owner coalesced, the second owner's shared range
	// without the unlocked hole.
	merged, low, high := locks[0], locks[1], locks[2]
	st := r.steps()
	if merged.Start != st[0].Lock.Start || merged.Length != st[0].Lock.Length+st[1].Lock.Length || st[0].Lock.Start+st[0].Lock.Length != st[1].Lock.Start ||
		merged.Owner != st[0].Lock.Owner || merged.PID != st[1].Lock.PID || merged.Type != cephFlockExclusive {
		t.Fatal("the held exclusive record is not the merge of the two adjacent acquires", merged)
	}
	hole := st[3].Lock
	if !st[3].Unlock || low.Start != st[2].Lock.Start || low.Start+low.Length != hole.Start || high.Start != hole.Start+hole.Length ||
		high.Start+high.Length != st[2].Lock.Start+st[2].Lock.Length || low.Owner != hole.Owner || high.Owner != hole.Owner || low.Type != cephFlockShared || high.Type != cephFlockShared {
		t.Fatal("the held shared records are not the split of the shared acquire", low, high)
	}
	// Each probe overlaps at most one held record.
	for _, p := range heldFcntlRangeProbeSet {
		n := 0
		for _, l := range locks {
			if heldFcntlOverlap(p, l.cephfsFilelock) {
				n++
			}
		}
		if n > 1 {
			t.Fatal("range probe overlaps several held records", p)
		}
	}
	busy := func(e []heldFcntlExpect, i int) heldFcntlExpect { return e[i] }
	all := heldFcntlExpectedFor(heldLocksOfRule(locks, cephLockRuleFcntl), heldFcntlRangeProbeSet)
	want := []struct {
		errno int
		getlk nativeMDSCephFSFcntlGetlk
	}{
		{11, nativeMDSCephFSFcntlGetlk{Type: "w", Start: 0, Length: 32, PID: 4101}},
		{11, nativeMDSCephFSFcntlGetlk{Type: "w", Start: 0, Length: 32, PID: 4101}},
		{11, nativeMDSCephFSFcntlGetlk{Type: "w", Start: 0, Length: 32, PID: 4101}},
		{0, nativeMDSCephFSFcntlGetlk{Type: "u", Start: 32, Length: 32}},
		{11, nativeMDSCephFSFcntlGetlk{Type: "r", Start: 64, Length: 16, PID: 4103}},
		{0, nativeMDSCephFSFcntlGetlk{Type: "u", Start: 85, Length: 1}},
		{11, nativeMDSCephFSFcntlGetlk{Type: "r", Start: 96, Length: 32, PID: 4103}},
		{0, nativeMDSCephFSFcntlGetlk{Type: "u", Start: 128}},
	}
	for i, w := range want {
		if e := busy(all, i); e.Errno != w.errno || e.Getlk != w.getlk {
			t.Fatalf("range probe %d expectation differs: %+v, want %+v", i, e, w)
		}
	}
	// Unmerged or unsplit blobs would restore other native results.
	unmerged := []cephfsFilelock{{Start: 0, Length: 16, Client: gid, Owner: cephFlockGoOwner, PID: 4101, Type: cephFlockExclusive}, {Start: 16, Length: 16, Client: gid, Owner: cephFlockGoOwner, PID: 4101, Type: cephFlockExclusive}, low.cephfsFilelock, high.cephfsFilelock}
	if reflect.DeepEqual(heldFcntlExpectedFor(unmerged, heldFcntlRangeProbeSet), all) {
		t.Fatal("the probe set does not tell a merged range from two adjacent ones")
	}
	unsplit := []cephfsFilelock{merged.cephfsFilelock, {Start: 64, Length: 64, Client: gid, Owner: cephFlockGoSecondOwner, PID: 4103, Type: cephFlockShared}}
	if reflect.DeepEqual(heldFcntlExpectedFor(unsplit, heldFcntlRangeProbeSet), all) {
		t.Fatal("the probe set does not tell a split range from the whole one")
	}
	// The blob: three FCNTL records in start order and an empty FLOCK section.
	blob := heldLockBlob(locks)
	fcntl, flock, err := cephfsDecodeFilelockBlob(blob)
	if err != nil || len(blob) != 4+3*41+4 || len(flock) != 0 || !slices.Equal(fcntl, heldLocksOfRule(locks, cephLockRuleFcntl)) {
		t.Fatal("range blob differs", fcntl, flock, err)
	}
	// Probes and GETFILELOCK after each unlock.
	if got := r.probesFor(locks, 0); len(got) != 2 || !reflect.DeepEqual(got[0], heldFlockProbeWant{Action: "flock-ex-nb"}) || got[1].Action != "fcntl-probe-ranges" || !reflect.DeepEqual(got[1].Fcntl, all) {
		t.Fatal("range probes differ", got)
	}
	if heldLockGetCheck(cephLockRuleFcntl, merged.cephfsFilelock, locks, gid) != nil || heldLockGetCheck(cephLockRuleFcntl, low.cephfsFilelock, locks[1:], gid) != nil ||
		heldLockGetCheck(cephLockRuleFcntl, cephfsFilelock{Start: 0, Length: 16, Client: gid, Owner: cephFlockGoOwner, PID: 4101, Type: cephFlockExclusive}, locks, gid) == nil {
		t.Fatal("range GETFILELOCK checks differ")
	}
	names := []string{r.stepName(0), r.stepName(3)}
	if !slices.Equal(names, []string{"step-0-fcntl-exclusive-acquire-0-16", "step-3-fcntl-shared-unlock-80-16"}) {
		t.Fatal("step labels differ", names)
	}
	calls := r.expectedCalls(gid, gid)
	if len(calls) != 7 || calls[3].Type != cephFlockUnlock || calls[3].Start != 80 || calls[4].Start != 0 || calls[4].Length != 32 || calls[6].Start != 96 {
		t.Fatal("range call sequence differs", calls)
	}
}

// TestCephFSHeldLockTwoClients is a cluster-free check of the two-client
// recipe: each session's blob carries only its own lock, GETFILELOCK names the
// restart client's lock first and then the second client's lock with its own
// GlobalID.
func TestCephFSHeldLockTwoClients(t *testing.T) {
	const gid, second = 4711, 4712
	r := twoClientHeldFlock
	locks := r.locksFor(gid, second)
	own, other := heldLocksOfSession(locks, r, false), heldLocksOfSession(locks, r, true)
	if !r.hasSecond() || len(own) != 1 || len(other) != 1 || own[0].Client != gid || other[0].Client != second || own[0].Type != cephFlockShared || other[0].Type != cephFlockShared || own[0].Owner == other[0].Owner {
		t.Fatal("the two-client recipe does not hold one shared lock per client", locks)
	}
	for _, l := range [][]heldLock{own, other} {
		if _, flock, err := cephfsDecodeFilelockBlob(heldLockBlob(l)); err != nil || len(flock) != 1 || flock[0] != l[0].cephfsFilelock {
			t.Fatal("a client's blob does not carry exactly its own lock", flock, err)
		}
	}
	if heldLockGetCheck(cephLockRuleFlock, own[0].cephfsFilelock, locks, gid) != nil || heldLockGetCheck(cephLockRuleFlock, other[0].cephfsFilelock, locks[1:], gid) != nil {
		t.Fatal("correct two-client GETFILELOCK answers refused")
	}
	// The second client's lock with the restart client's GlobalID (a blob on
	// the wrong session) or the second lock named first is refused.
	wrongClient := other[0].cephfsFilelock
	wrongClient.Client = gid
	if heldLockGetCheck(cephLockRuleFlock, wrongClient, locks[1:], gid) == nil || heldLockGetCheck(cephLockRuleFlock, other[0].cephfsFilelock, locks, gid) == nil {
		t.Fatal("a wrong two-client GETFILELOCK answer was accepted")
	}
	exNo := heldFlockProbeWant{Action: "flock-ex-nb", Errno: 11}
	if got := r.probesFor(locks, 0); !reflect.DeepEqual(got, []heldFlockProbeWant{{Action: "flock-sh-nb"}, exNo}) {
		t.Fatal("two-client probes differ", got)
	}
	if got := r.probesFor(locks[1:], 0); !reflect.DeepEqual(got, []heldFlockProbeWant{exNo}) {
		t.Fatal("two-client probes after the first unlock differ", got)
	}
}

// TestCephFSHeldLockInFlightChecks is a cluster-free check of the waiting
// request receipt refusals.
func TestCephFSHeldLockInFlightChecks(t *testing.T) {
	const gid, tid, mds = 4711, 9, 4100
	op := &nativeMDSCephFSHistoricOp{Description: fmt.Sprintf("client_request(client.%d:%d setfilelock rule 2, type 2, owner 9223372038109642755, pid 0, start 0, length 0, wait 1 #0x10000000002 2026-11-24T00:00:00.000000+0000 caller_uid=0, caller_gid=0{})", gid, tid),
		Events: []string{"initiated", "throttled", "header_read", "all_read", "dispatched", "acquired locks", heldLockWaitingEvent}}
	receipt := func(edit func(*nativeMDSCephFSReceipt)) nativeMDSCephFSReceipt {
		r := nativeMDSCephFSReceipt{Action: "lock-inflight", MDSGID: mds, LockInFlight: &nativeMDSCephFSLockInFlight{GlobalID: gid, TID: tid, MDSGID: mds, InFlight: true, Op: op}}
		if edit != nil {
			edit(&r)
		}
		return r
	}
	gone := func(r *nativeMDSCephFSReceipt) { r.LockInFlight.InFlight, r.LockInFlight.Op = false, nil }
	if err := checkHeldLockInFlight(receipt(nil), gid, tid, mds, true); err != nil {
		t.Fatal("waiting request refused", err)
	}
	if err := checkHeldLockInFlight(receipt(gone), gid, tid, mds, false); err != nil {
		t.Fatal("finished request refused", err)
	}
	clone := func(edit func(*nativeMDSCephFSHistoricOp)) func(*nativeMDSCephFSReceipt) {
		return func(r *nativeMDSCephFSReceipt) {
			o := *op
			o.Events = slices.Clone(op.Events)
			edit(&o)
			r.LockInFlight.Op = &o
		}
	}
	for name, tc := range map[string]struct {
		edit    func(*nativeMDSCephFSReceipt)
		waiting bool
	}{
		"still waiting when it must be gone": {nil, false},
		"gone when it must wait":             {gone, true},
		"granted, not waiting": {clone(func(o *nativeMDSCephFSHistoricOp) {
			o.Events = slices.DeleteFunc(o.Events, func(e string) bool { return e == heldLockWaitingEvent })
		}), true},
		"nonblocking request": {clone(func(o *nativeMDSCephFSHistoricOp) {
			o.Description = strings.Replace(o.Description, "wait 1", "wait 0", 1)
		}), true},
		"another request": {clone(func(o *nativeMDSCephFSHistoricOp) { o.Description = strings.Replace(o.Description, ":9 ", ":10 ", 1) }), true},
		"another operation": {clone(func(o *nativeMDSCephFSHistoricOp) {
			o.Description = strings.Replace(o.Description, "setfilelock", "getfilelock", 1)
		}), true},
		"another daemon":      {func(r *nativeMDSCephFSReceipt) { r.LockInFlight.MDSGID++ }, true},
		"another session":     {func(r *nativeMDSCephFSReceipt) { r.LockInFlight.GlobalID++ }, true},
		"inconsistent record": {func(r *nativeMDSCephFSReceipt) { r.LockInFlight.InFlight = false }, true},
		"no receipt":          {func(r *nativeMDSCephFSReceipt) { r.LockInFlight = nil }, true},
		"another action":      {func(r *nativeMDSCephFSReceipt) { r.Action = "recovery-ops" }, true},
	} {
		if checkHeldLockInFlight(receipt(tc.edit), gid, tid, mds, tc.waiting) == nil {
			t.Fatal("wrong in-flight lock receipt accepted:", name)
		}
	}
}
