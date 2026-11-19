package integration_test

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Held FCNTL byte-range locks across an owned MDS restart (issue #10). The
// recipe and the expected native results below are test-only consumer code:
// the product carries MDSCapReconnect.Flock as opaque bytes, and the lock
// requests are raw SETFILELOCK/GETFILELOCK arguments.

// cephFlockGoThirdOwner is the owner of the whole-file FLOCK lock of the FCNTL
// recipe. It differs from the two FCNTL owners, from the GETFILELOCK test owner
// (cephFlockGoOwner+1) and from the native owner.
const cephFlockGoThirdOwner uint64 = 0x80000000474f0005

// fcntlHeldLocks is the held lock set of
// TestCephMDSCephFSFcntlRangeRecoveryIntegration: an exclusive FCNTL range
// [0,16) and a shared FCNTL range [32,48) of two owners with their own pids,
// then one exclusive whole-file FLOCK lock of a third owner. The reconnect
// blob therefore has a nonzero count in both of its sections. The FCNTL
// ranges ascend by start, which is the held_locks order of both the MDS and
// Client::_encode_filelocks.
var fcntlHeldLocks = heldFlockRecipe{Name: "fcntl-ranges-and-flock", Results: "mds-cephfs-fcntl-recovery-results.json", Locks: []heldLockSpec{
	{Rule: cephLockRuleFcntl, Type: cephFlockExclusive, Owner: cephFlockGoOwner, PID: 4101, Start: 0, Length: 16},
	{Rule: cephLockRuleFcntl, Type: cephFlockShared, Owner: cephFlockGoSecondOwner, PID: 4103, Start: 32, Length: 16},
	{Rule: cephLockRuleFlock, Type: cephFlockExclusive, Owner: cephFlockGoThirdOwner},
}}

// TestCephMDSCephFSFcntlRangeRecoveryIntegration runs the recovery recipe of
// TestCephMDSCephFSRecoveryIntegration with two FCNTL byte-range locks and one
// FLOCK lock (fcntlHeldLocks) in place of the one exclusive FLOCK lock. It
// needs a fresh mds-cephfs fixture of its own: the recipe performs the
// fixture's only owned MDS restart. See
// integration/fixture/MDS_FILELOCK_FCNTL_RESTART_VALIDATION.md.
//
//scenario:row mds-cephfs-fcntl-recovery.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsfcntlrangerecovery key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs=1 capabilities=mds-held-fcntl
//scenario:row mds-cephfs-fcntl-recovery.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsfcntlrangerecovery key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs=1 capabilities=mds-held-fcntl
//fixture:isolated pauses the MDS journal, fails the owned rank-0 MDS and evicts a session (blocklist entry); no restore proof
func TestCephMDSCephFSFcntlRangeRecoveryIntegration(t *testing.T) {
	runCephFSRecovery(t, fcntlHeldLocks)
}

// nativeMDSCephFSFcntlGetlk is the lock ceph_getlk reported for one FCNTL
// probe request: the conflicting lock ("w" or "r", its range and pid) or "u"
// with the request's own range and pid 0.
type nativeMDSCephFSFcntlGetlk struct {
	Type   string `json:"type"`
	Start  int64  `json:"start"`
	Length int64  `json:"len"`
	PID    int32  `json:"pid"`
}

type nativeMDSCephFSFcntlResult struct {
	Type     string                    `json:"type"`
	Start    int64                     `json:"start"`
	Length   int64                     `json:"len"`
	Getlk    nativeMDSCephFSFcntlGetlk `json:"getlk"`
	Errno    int                       `json:"errno"`
	Acquired bool                      `json:"acquired"`
	Unlocked bool                      `json:"unlocked"`
}

// nativeMDSCephFSFcntlState is one FCNTL probe receipt: the second native
// mount (GlobalID) that made the requests and the owned file state the first
// mount read before and after them.
type nativeMDSCephFSFcntlState struct {
	Owner     uint64                       `json:"owner"`
	GlobalID  uint64                       `json:"global_id"`
	Probes    []nativeMDSCephFSFcntlResult `json:"probes"`
	ElapsedNS int64                        `json:"elapsed_ns"`
	Before    nativeMDSCephFSFlockState    `json:"before"`
	After     nativeMDSCephFSFlockState    `json:"after"`
}

// heldFcntlRequest is one request of the fixture's fixed FCNTL probe set
// (mdsCephFSFcntlProbes in integration/fixture/mds_cephfs.go): Type "w"
// F_WRLCK or "r" F_RDLCK over Length bytes from Start, Length 0 to the end of
// the file. The receipt check requires the fixture to report exactly these.
type heldFcntlRequest struct {
	Type          string
	Start, Length int64
}

var heldFcntlProbeSet = []heldFcntlRequest{{"w", 0, 1}, {"w", 15, 1}, {"w", 16, 16}, {"r", 32, 16}, {"w", 47, 1}, {"w", 48, 0}, {"r", 8, 1}}

// heldFcntlExpect is the result one probe request must have: EAGAIN and the
// conflicting lock from ceph_getlk, or an acquire and release with ceph_getlk
// echoing the request as unlocked.
type heldFcntlExpect struct {
	Request heldFcntlRequest
	Errno   int
	Getlk   nativeMDSCephFSFcntlGetlk
}

// heldFcntlOverlap reports whether a request and a held range share a byte.
// A length of 0 is to the end of the file (ceph_lock_state_t::share_space in
// mds/flock.cc).
func heldFcntlOverlap(r heldFcntlRequest, l cephfsFilelock) bool {
	rStart, lStart := uint64(r.Start), l.Start
	return (r.Length == 0 || lStart < rStart+uint64(r.Length)) && (l.Length == 0 || rStart < lStart+l.Length)
}

// heldFcntlExpected computes the native result of every probe request while
// the given FCNTL locks of another owner are held. A request conflicts with
// a held lock that overlaps it unless both are shared. ceph_getlk reports the
// first conflicting lock in start order (look_for_lock and
// get_overlapping_locks in mds/flock.cc; Client::_do_filelock maps it to
// F_WRLCK or F_RDLCK, start, length and pid), or F_UNLCK with the request's
// own range and pid 0 when nothing conflicts.
func heldFcntlExpected(held []cephfsFilelock) []heldFcntlExpect {
	ordered := slices.Clone(held)
	slices.SortStableFunc(ordered, func(a, b cephfsFilelock) int { return cmp.Compare(a.Start, b.Start) })
	out := make([]heldFcntlExpect, 0, len(heldFcntlProbeSet))
	for _, r := range heldFcntlProbeSet {
		e := heldFcntlExpect{Request: r, Getlk: nativeMDSCephFSFcntlGetlk{Type: "u", Start: r.Start, Length: r.Length}}
		for _, l := range ordered {
			if heldFcntlOverlap(r, l) && (r.Type == "w" || l.Type == cephFlockExclusive) {
				kind := "r"
				if l.Type == cephFlockExclusive {
					kind = "w"
				}
				e.Errno, e.Getlk = 11, nativeMDSCephFSFcntlGetlk{Type: kind, Start: int64(l.Start), Length: int64(l.Length), PID: int32(l.PID)}
				break
			}
		}
		out = append(out, e)
	}
	return out
}

// checkHeldFcntlProbe checks one FCNTL probe receipt: the closed request and
// its mutation record, two native mounts that are not Go clients, the fixed
// probe set with exactly the expected results, and the unchanged owned file.
func checkHeldFcntlProbe(r nativeMDSCephFSReceipt, want heldFlockProbeWant, ino uint64, goGIDs map[uint64]bool, baseline **nativeMDSCephFSFlockState) error {
	n := r.Native
	if n == nil || n.Fcntl == nil || n.Flock != nil {
		return errors.New("native fcntl receipt lacks its probe or carries a flock probe")
	}
	p := *n.Fcntl
	if r.Action != want.Action || n.Action != want.Action || want.Action != "fcntl-probe" || n.NativeGlobalID == 0 || goGIDs[n.NativeGlobalID] ||
		p.GlobalID == 0 || goGIDs[p.GlobalID] || p.GlobalID == n.NativeGlobalID {
		return fmt.Errorf("native fcntl receipt is for %q/%q or from a Go client: mounts %d and %d", r.Action, n.Action, n.NativeGlobalID, p.GlobalID)
	}
	if r.Mutation == nil || r.Mutation.Kind != "native-fcntl-probe" || r.Mutation.State != "applied" || r.Mutation.RequestID != r.RequestID ||
		p.Owner != cephFlockNativeOwner || p.ElapsedNS <= 0 || p.Before != p.After || p.Before.Ino != ino || p.Before.Size != 0 || p.Before.DataHex != "" {
		return fmt.Errorf("native fcntl probe record or owned file state differs: probe owner %#x mutation %+v before %+v after %+v", p.Owner, r.Mutation, p.Before, p.After)
	}
	if len(p.Probes) != len(want.Fcntl) {
		return fmt.Errorf("native fcntl probe returned %d results, want %d", len(p.Probes), len(want.Fcntl))
	}
	for i, got := range p.Probes {
		e := want.Fcntl[i]
		if got.Type != e.Request.Type || got.Start != e.Request.Start || got.Length != e.Request.Length || got.Errno != e.Errno ||
			got.Acquired != (e.Errno == 0) || got.Unlocked != got.Acquired || got.Getlk != e.Getlk {
			return fmt.Errorf("native fcntl probe %d differs: got %+v, want %+v", i, got, e)
		}
	}
	if *baseline == nil {
		first := p.Before
		*baseline = &first
	} else if **baseline != p.Before {
		return fmt.Errorf("lock probes changed native bytes, size, version or second-resolution mtime: %+v then %+v", **baseline, p.Before)
	}
	return nil
}

// TestCephFSFcntlRecipeBlobAndChecks is a cluster-free check of the FCNTL
// recipe: its SETFILELOCK and GETFILELOCK arguments, its two-section
// reconnect blob, the expected native results at each step, and the
// GETFILELOCK and probe receipt refusals.
func TestCephFSFcntlRecipeBlobAndChecks(t *testing.T) {
	const gid = 4711
	locks := fcntlHeldLocks.locks(gid)
	owners := map[uint64]bool{}
	for _, l := range locks {
		owners[l.Owner] = true
		if l.Owner>>63 != 1 || l.Owner == cephFlockGoOwner+1 || l.Owner == cephFlockNativeOwner || l.Client != gid {
			t.Fatal("a held lock owner is not a distinct high-bit Go owner", l)
		}
	}
	if len(locks) != 3 || len(owners) != 3 || !slices.Equal(fcntlHeldLocks.rules(), []byte{cephLockRuleFcntl, cephLockRuleFlock}) {
		t.Fatal("the FCNTL recipe does not hold two FCNTL locks and one FLOCK lock of three owners", locks)
	}
	fcntlLocks, flockLocks := heldLocksOfRule(locks, cephLockRuleFcntl), heldLocksOfRule(locks, cephLockRuleFlock)
	if len(fcntlLocks) != 2 || len(flockLocks) != 1 || fcntlLocks[0].Start >= fcntlLocks[1].Start || fcntlLocks[0].PID == fcntlLocks[1].PID ||
		fcntlLocks[0].Type != cephFlockExclusive || fcntlLocks[1].Type != cephFlockShared || flockLocks[0].Start != 0 || flockLocks[0].Length != 0 || flockLocks[0].PID != 0 {
		t.Fatal("the FCNTL recipe's ranges, pids or types changed", locks)
	}
	// The packed filelock_change: rule, type, owner, pid, start, length, wait.
	args := locks[0].args(cephFlockExclusive)
	want := [56]byte{cephLockRuleFcntl, cephFlockExclusive, 0x01, 0x00, 0x4f, 0x47, 0, 0, 0, 0x80, 0x05, 0x10}
	want[26] = 16
	if args != want {
		t.Fatalf("FCNTL SETFILELOCK arguments differ:\n got %x\nwant %x", args, want)
	}
	if locks[2].args(cephFlockUnlock) != cephfsFlockArgs(cephFlockUnlock, cephFlockGoThirdOwner) || exclusiveHeldFlock.locks(gid)[0].args(cephFlockExclusive) != cephfsFlockArgs(cephFlockExclusive, cephFlockGoOwner) {
		t.Fatal("a FLOCK lock's arguments differ from the whole-file FLOCK request")
	}
	if heldLockGetTest(cephLockRuleFlock) != cephfsFlockArgs(cephFlockExclusive, cephFlockGoOwner+1) || heldLockGetTest(cephLockRuleFcntl)[0] != cephLockRuleFcntl {
		t.Fatal("GETFILELOCK test locks differ")
	}

	// Client::_encode_filelocks: the FCNTL count and records, then the FLOCK
	// count and records; each section is the single-record layout compared
	// with the native encoder in TestCephFSHeldFlockBlobMatchesNativeLayout.
	blob := heldLockBlob(locks)
	if len(blob) != 4+2*41+4+41 || !slices.Equal(blob, cephfsFilelockBlob(fcntlLocks, flockLocks)) {
		t.Fatalf("two-section blob differs: %x", blob)
	}
	gotFcntl, gotFlock, err := cephfsDecodeFilelockBlob(blob)
	if err != nil || !slices.Equal(gotFcntl, fcntlLocks) || !slices.Equal(gotFlock, flockLocks) {
		t.Fatal("two-section blob does not decode to the held locks", gotFcntl, gotFlock, err)
	}
	if f, l, err := cephfsDecodeFilelockBlob(heldLockBlob(exclusiveHeldFlock.locks(gid))); err != nil || len(f) != 0 || len(l) != 1 {
		t.Fatal("the FLOCK-only blob gained an FCNTL record", f, l, err)
	}

	// Each probe request overlaps at most one held range, so the expected
	// conflict does not depend on the order of several overlapping locks.
	for _, r := range heldFcntlProbeSet {
		n := 0
		for _, l := range fcntlLocks {
			if heldFcntlOverlap(r, l) {
				n++
			}
		}
		if n > 1 {
			t.Fatal("probe request overlaps both held ranges", r)
		}
	}
	excl := nativeMDSCephFSFcntlGetlk{Type: "w", Start: 0, Length: 16, PID: 4101}
	shared := nativeMDSCephFSFcntlGetlk{Type: "r", Start: 32, Length: 16, PID: 4103}
	free := func(r heldFcntlRequest) heldFcntlExpect {
		return heldFcntlExpect{Request: r, Getlk: nativeMDSCephFSFcntlGetlk{Type: "u", Start: r.Start, Length: r.Length}}
	}
	busy := func(r heldFcntlRequest, g nativeMDSCephFSFcntlGetlk) heldFcntlExpect {
		return heldFcntlExpect{Request: r, Errno: 11, Getlk: g}
	}
	p := heldFcntlProbeSet
	both := []heldFcntlExpect{busy(p[0], excl), busy(p[1], excl), free(p[2]), free(p[3]), busy(p[4], shared), free(p[5]), busy(p[6], excl)}
	second := []heldFcntlExpect{free(p[0]), free(p[1]), free(p[2]), free(p[3]), busy(p[4], shared), free(p[5]), free(p[6])}
	none := []heldFcntlExpect{free(p[0]), free(p[1]), free(p[2]), free(p[3]), free(p[4]), free(p[5]), free(p[6])}
	fc := func(e []heldFcntlExpect) heldFlockProbeWant {
		return heldFlockProbeWant{Action: "fcntl-probe", Fcntl: e}
	}
	exNo, exYes := heldFlockProbeWant{Action: "flock-ex-nb", Errno: 11}, heldFlockProbeWant{Action: "flock-ex-nb"}
	for held, want := range map[int][]heldFlockProbeWant{
		3: {exNo, fc(both)},
		2: {exNo, fc(second)},
		1: {exNo, fc(none)},
		0: {exYes, fc(none)},
	} {
		if got := fcntlHeldLocks.probes(held); !reflect.DeepEqual(got, want) {
			t.Fatalf("probe sequence with %d locks held differs:\n got %+v\nwant %+v", held, got, want)
		}
	}
	// The FLOCK recipes keep their probe sequences.
	for name, tc := range map[string]struct {
		recipe heldFlockRecipe
		held   int
		want   []heldFlockProbeWant
	}{
		"exclusive held":       {exclusiveHeldFlock, 1, []heldFlockProbeWant{exNo}},
		"exclusive released":   {exclusiveHeldFlock, 0, []heldFlockProbeWant{exYes}},
		"shared both held":     {sharedHeldFlock, 2, []heldFlockProbeWant{{Action: "flock-sh-nb"}, exNo}},
		"shared one released":  {sharedHeldFlock, 1, []heldFlockProbeWant{exNo}},
		"shared both released": {sharedHeldFlock, 0, []heldFlockProbeWant{exYes}},
	} {
		if got := tc.recipe.probes(tc.held); !reflect.DeepEqual(got, tc.want) {
			t.Fatal("FLOCK probe sequence changed:", name, got)
		}
	}
	names := []string{fcntlHeldLocks.callName("acquire", 0), fcntlHeldLocks.callName("acquire", 1), fcntlHeldLocks.callName("unlock", 2)}
	if !slices.Equal(names, []string{"fcntl-exclusive-acquire-owner-0", "fcntl-shared-acquire-owner-1", "flock-exclusive-unlock-owner-2"}) {
		t.Fatal("SETFILELOCK call labels differ", names)
	}

	// GETFILELOCK of each rule while all, two, one or none are held.
	testLock := cephfsFilelock{Client: gid, Owner: cephFlockGoOwner + 1, Type: cephFlockUnlock}
	for _, tc := range []struct {
		held        []heldLock
		fcntl, flck cephfsFilelock
	}{
		{locks, locks[0].cephfsFilelock, locks[2].cephfsFilelock},
		{locks[1:], locks[1].cephfsFilelock, locks[2].cephfsFilelock},
		{locks[2:], testLock, locks[2].cephfsFilelock},
	} {
		if heldLockGetCheck(cephLockRuleFcntl, tc.fcntl, tc.held, gid) != nil || heldLockGetCheck(cephLockRuleFlock, tc.flck, tc.held, gid) != nil {
			t.Fatal("correct GETFILELOCK answers refused", tc.held)
		}
	}
	for name, tc := range map[string]struct {
		rule byte
		got  cephfsFilelock
		held []heldLock
	}{
		"second range while both held":   {cephLockRuleFcntl, locks[1].cephfsFilelock, locks},
		"released range":                 {cephLockRuleFcntl, locks[0].cephfsFilelock, locks[1:]},
		"FLOCK lock for FCNTL":           {cephLockRuleFcntl, locks[2].cephfsFilelock, locks},
		"FCNTL lock for FLOCK":           {cephLockRuleFlock, locks[0].cephfsFilelock, locks},
		"no conflict while range held":   {cephLockRuleFcntl, testLock, locks[1:]},
		"conflict with no range held":    {cephLockRuleFcntl, locks[1].cephfsFilelock, locks[2:]},
		"other unlocked echo":            {cephLockRuleFcntl, cephfsFilelock{Client: gid, Owner: cephFlockGoOwner + 1, Type: cephFlockUnlock, Length: 1}, locks[2:]},
		"range without its pid restored": {cephLockRuleFcntl, cephfsFilelock{Start: 32, Length: 16, Client: gid, Owner: cephFlockGoSecondOwner, Type: cephFlockShared}, locks[1:]},
		"range of another length":        {cephLockRuleFcntl, cephfsFilelock{Start: 0, Length: 0, Client: gid, Owner: cephFlockGoOwner, PID: 4101, Type: cephFlockExclusive}, locks},
	} {
		if heldLockGetCheck(tc.rule, tc.got, tc.held, gid) == nil {
			t.Fatal("wrong GETFILELOCK answer accepted:", name)
		}
	}

	// The submission record: one acquire and one unlock per lock, the unlock
	// over the same rule, pid and range.
	var h heldFlockEvidence
	for i, l := range slices.Concat(locks, locks) {
		kind := l.Type
		if i >= len(locks) {
			kind = cephFlockUnlock
		}
		h.acceptFlock(fmt.Sprint(i), l, kind)(uint64(10 + i))
		h.settleFlock(cephmsgr.MDSReply{}, nil)
	}
	if err := heldFlockCallsExact(h.Calls, locks); err != nil {
		t.Fatal("the FCNTL recipe's SETFILELOCK calls were refused", err, h.Calls)
	}
	for name, edit := range map[string]func(c []heldFlockCall){
		"whole-file unlock of a range": func(c []heldFlockCall) { c[3].Length = 0 },
		"unlock of another range":      func(c []heldFlockCall) { c[4].Start = 0 },
		"unlock with another pid":      func(c []heldFlockCall) { c[3].PID = 0 },
		"FLOCK unlock of a range":      func(c []heldFlockCall) { c[3].Rule = cephLockRuleFlock },
		"FCNTL acquire of the FLOCK":   func(c []heldFlockCall) { c[2].Rule = cephLockRuleFcntl },
	} {
		calls := slices.Clone(h.Calls)
		edit(calls)
		if heldFlockCallsExact(calls, locks) == nil {
			t.Fatal("wrong SETFILELOCK submissions accepted:", name)
		}
	}
}

// TestCephFSFcntlProbeReceiptChecks is a cluster-free check of the FCNTL
// probe receipt refusals.
func TestCephFSFcntlProbeReceiptChecks(t *testing.T) {
	const ino = 0x10000000002
	state := nativeMDSCephFSFlockState{Ino: ino, Version: 4, MTime: 1}
	goGIDs := map[uint64]bool{42: true}
	want := fcntlHeldLocks.probes(3)[1]
	receipt := func(edit func(*nativeMDSCephFSReceipt)) nativeMDSCephFSReceipt {
		var r nativeMDSCephFSReceipt
		if err := json.Unmarshal([]byte(`{"request_id":9,"action":"fcntl-probe","mutation":{"kind":"native-fcntl-probe","request_id":9,"state":"applied"}}`), &r); err != nil {
			t.Fatal(err)
		}
		var probes []nativeMDSCephFSFcntlResult
		for _, e := range want.Fcntl {
			probes = append(probes, nativeMDSCephFSFcntlResult{Type: e.Request.Type, Start: e.Request.Start, Length: e.Request.Length, Getlk: e.Getlk, Errno: e.Errno,
				Acquired: e.Errno == 0, Unlocked: e.Errno == 0})
		}
		r.Native = &nativeMDSCephFSOutput{Action: "fcntl-probe", Path: "/d/go-file", NativeGlobalID: 77,
			Fcntl: &nativeMDSCephFSFcntlState{Owner: cephFlockNativeOwner, GlobalID: 78, Probes: probes, ElapsedNS: 1, Before: state, After: state}}
		if edit != nil {
			edit(&r)
		}
		return r
	}
	var baseline *nativeMDSCephFSFlockState
	if err := checkHeldFcntlProbe(receipt(nil), want, ino, goGIDs, &baseline); err != nil || baseline == nil || *baseline != state {
		t.Fatal("correct FCNTL probe refused", err)
	}
	released := fcntlHeldLocks.probes(1)[1]
	for name, tc := range map[string]struct {
		want heldFlockProbeWant
		got  nativeMDSCephFSReceipt
	}{
		"held results while released expected": {released, receipt(nil)},
		"range acquired while held": {want, receipt(func(r *nativeMDSCephFSReceipt) {
			p := &r.Native.Fcntl.Probes[0]
			p.Errno, p.Acquired, p.Unlocked = 0, true, true
		})},
		"conflict of another pid":     {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Native.Fcntl.Probes[4].Getlk.PID = 4101 })},
		"conflict of another type":    {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Native.Fcntl.Probes[4].Getlk.Type = "w" })},
		"conflict of another range":   {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Native.Fcntl.Probes[0].Getlk.Length = 0 })},
		"probe set changed":           {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Native.Fcntl.Probes[2].Start = 17 })},
		"probe missing":               {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Native.Fcntl.Probes = r.Native.Fcntl.Probes[1:] })},
		"acquired but not released":   {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Native.Fcntl.Probes[2].Unlocked = false })},
		"lock mount is a Go client":   {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Native.Fcntl.GlobalID = 42 })},
		"state mount is a Go client":  {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Native.NativeGlobalID = 42 })},
		"one mount for both":          {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Native.Fcntl.GlobalID = 77 })},
		"another native owner":        {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Native.Fcntl.Owner = cephFlockGoOwner })},
		"uncertain native process":    {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Mutation.State = "uncertain" })},
		"mutation of the flock probe": {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Mutation.Kind = "native-flock-ex-nb" })},
		"no mutation record":          {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Mutation = nil })},
		"another inode": {want, receipt(func(r *nativeMDSCephFSReceipt) {
			r.Native.Fcntl.Before.Ino++
			r.Native.Fcntl.After.Ino++
		})},
		"file changed during the probe": {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Native.Fcntl.After.MTime++ })},
		"file changed between probes": {want, receipt(func(r *nativeMDSCephFSReceipt) {
			r.Native.Fcntl.Before.Version++
			r.Native.Fcntl.After.Version++
		})},
		"flock probe beside it": {want, receipt(func(r *nativeMDSCephFSReceipt) { r.Native.Flock = &nativeMDSCephFSFlockProbe{} })},
	} {
		if err := checkHeldFcntlProbe(tc.got, tc.want, ino, goGIDs, &baseline); err == nil {
			t.Fatal("wrong FCNTL probe receipt accepted:", name)
		}
	}
	var flockBaseline *nativeMDSCephFSFlockState
	r := receipt(nil)
	r.Native.Flock = &nativeMDSCephFSFlockProbe{Owner: cephFlockNativeOwner, Errno: 11, ElapsedNS: 1, Before: state, After: state}
	r.Action, r.Native.Action, r.Mutation.Kind = "flock-ex-nb", "flock-ex-nb", "native-flock-ex-nb"
	if checkHeldFlockProbe(r, heldFlockProbeWant{Action: "flock-ex-nb", Errno: 11}, ino, goGIDs, &flockBaseline) == nil {
		t.Fatal("a flock receipt that also carries an FCNTL probe was accepted")
	}
}
