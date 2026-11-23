package integration_test

import (
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Held-lock cells of the split reconnect recipe (issue #10): FCNTL ranges in
// a split reconnect, several locks per fragment over three or more fragments
// with a multi-record blob, and a connection loss between two fragments.
// Each runs runCephFSSplitReconnect on a fresh completion fixture and is its
// own acceptance result.

// More high-bit Go lock owners of the dense split cell, distinct from every
// other owner of the held-lock recipes and from the GETFILELOCK test owner.
const (
	cephFlockGoFourthOwner uint64 = 0x80000000474f0007
	cephFlockGoFifthOwner  uint64 = 0x80000000474f0009
)

// splitFcntlHeldLocks is the recipe of
// TestCephMDSCephFSHeldFcntlSplitReconnectIntegration: on the first split
// file, an exclusive FCNTL range [0,16) and a shared FCNTL range [32,48) of
// two owners with their own pids and an exclusive whole-file FLOCK lock of a
// third owner, the lock set of the unsplit FCNTL cell.
func splitFcntlHeldLocks() *splitHeldFlock {
	probes := func(l *splitHeldLock) *splitHeldLock {
		l.Probe, l.FcntlProbe = "split-flock-ex-nb", "split-fcntl-probe"
		return l
	}
	return &splitHeldFlock{Results: "mds-cephfs-heldfcntl-split-results.json", Locks: []*splitHeldLock{
		probes(&splitHeldLock{Name: "fcntl-exclusive", Owner: cephFlockGoOwner, Rule: cephLockRuleFcntl, Type: cephFlockExclusive, PID: 4101, Start: 0, Length: 16}),
		probes(&splitHeldLock{Name: "fcntl-shared", Owner: cephFlockGoSecondOwner, Rule: cephLockRuleFcntl, Type: cephFlockShared, PID: 4103, Start: 32, Length: 16}),
		probes(&splitHeldLock{Name: "flock", Owner: cephFlockGoThirdOwner}),
	}}
}

// splitDenseHeldLocks is the recipe of
// TestCephMDSCephFSHeldFlockSplitDenseIntegration: with a 3 KiB message bound
// the 34 capabilities need four fragments. The first split file holds two
// shared FLOCK locks of two owners (a two-record blob) and the second split
// file an exclusive one, both in the first fragment; the middle split file's
// exclusive lock is in a fragment that is neither the first nor the last,
// and the last split file's in the final one.
func splitDenseHeldLocks() *splitHeldFlock {
	return &splitHeldFlock{Results: "mds-cephfs-heldflock-split-dense-results.json", FrameLimit: 3 << 10, MinFragments: 3, SharedFragment: true, Locks: []*splitHeldLock{
		{Name: "first-shared-0", Probe: "split-flock-ex-nb", SharedProbe: "split-flock-sh-nb", Owner: cephFlockGoOwner, Type: cephFlockShared},
		{Name: "first-shared-1", Probe: "split-flock-ex-nb", SharedProbe: "split-flock-sh-nb", Owner: cephFlockGoSecondOwner, Type: cephFlockShared},
		{Name: "second", Probe: "split-second-flock-ex-nb", Owner: cephFlockGoThirdOwner, Member: 1},
		{Name: "middle", Probe: "split-middle-flock-ex-nb", Owner: cephFlockGoFourthOwner, Member: 16, Middle: true},
		{Name: "final", Probe: "split-final-flock-ex-nb", Owner: cephFlockGoFifthOwner, Member: 31, Final: true},
	}}
}

// splitCutHeldLocks is the recipe of
// TestCephMDSCephFSHeldFlockSplitCutIntegration: the one-lock split cell with
// the connection to the replacement cut between its first and second
// fragment.
func splitCutHeldLocks() *splitHeldFlock {
	h := splitHeldFlockRecipe(false)
	h.Results, h.Cut = "mds-cephfs-heldflock-split-cut-results.json", &splitCutEvidence{}
	return h
}

// TestCephMDSCephFSHeldFcntlSplitReconnectIntegration holds two FCNTL ranges
// and one FLOCK lock on the first split file across the owned MDS restart.
// Its capability record carries the three-record, two-section blob in a
// nonfinal fragment of the split reconnect. Before the restart and after
// adoption the native FCNTL probe of that file (ceph_getlk and nonblocking
// ceph_setlk from a second native mount) and its exclusive FLOCK probe must
// match the records still held, and so after each explicit unlock;
// GETFILELOCK of each rule names the first record of that rule still held.
// Not covered: FCNTL ranges in the final fragment or on several inodes of a
// split.
//
//scenario:row mds-cephfs-heldfcntl-split.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsheldfcntlsplitreconnect key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs_completion=1 capabilities=mds-held-fcntl-split,mds-held-flock-split
//scenario:row mds-cephfs-heldfcntl-split.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsheldfcntlsplitreconnect key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs_completion=1 capabilities=mds-held-fcntl-split,mds-held-flock-split
//scenario:covers CLIENT to MDS/CephFS
//fixture:isolated restarts the owned MDS via mds fail and sets debug overrides; leaves files in the fixture filesystem
func TestCephMDSCephFSHeldFcntlSplitReconnectIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION=1 for owned held-FCNTL split reconnect acceptance")
	}
	runCephFSSplitReconnect(t, splitFcntlHeldLocks())
}

// TestCephMDSCephFSHeldFlockSplitDenseIntegration holds five locks on four
// split files (splitDenseHeldLocks) across a split reconnect of at least
// three fragments. The replacement's per-capability log must place both
// first-fragment lock files in one fragment, the middle file in a middle
// fragment and the last file in the final one; the first file's blob carries
// two shared records. The native probes, including a shared FLOCK probe that
// acquires beside the two shared locks, and GETFILELOCK must match the locks
// still held before the restart, after adoption and after each of the five
// unlocks. Not covered: connection loss between fragments (its own cell) and
// more than four fragments.
//
//scenario:row mds-cephfs-heldflock-split-dense.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsheldflocksplitdense key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs_completion=1 capabilities=mds-held-flock-split,mds-held-flock-split-dense,mds-held-flock-split-fragments
//scenario:row mds-cephfs-heldflock-split-dense.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsheldflocksplitdense key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs_completion=1 capabilities=mds-held-flock-split,mds-held-flock-split-dense,mds-held-flock-split-fragments
//scenario:covers CLIENT to MDS/CephFS
//fixture:isolated restarts the owned MDS via mds fail and sets debug overrides; leaves files in the fixture filesystem
func TestCephMDSCephFSHeldFlockSplitDenseIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION=1 for owned dense held-lock split reconnect acceptance")
	}
	runCephFSSplitReconnect(t, splitDenseHeldLocks())
}

// TestCephMDSCephFSHeldFlockSplitCutIntegration is the one-lock split cell
// with the reconnect's connection cut between fragments. The test's own dial
// of the replacement passes the first reconnect fragment (the first frame of
// at least 4 KiB) and closes the connection instead of writing the next
// frame. The Messenger session resumes on a new connection (stateful
// reconnect, unacknowledged messages sent again), and the MDS must handle
// every fragment exactly once: its handler and per-capability log show the
// "(more)"/final sequence with every inode once, the session opens, and the
// lock is restored and released as in the one-lock cell. Not covered: a cut
// before the first fragment completes and a cut that forces a new session.
//
//scenario:row mds-cephfs-heldflock-split-cut.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsheldflocksplitcut key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs_completion=1 capabilities=mds-held-flock-split
//scenario:row mds-cephfs-heldflock-split-cut.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsheldflocksplitcut key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs_completion=1 capabilities=mds-held-flock-split
//scenario:covers Messenger/CephX
//scenario:covers CLIENT to MDS/CephFS
//fixture:isolated restarts the owned MDS via mds fail and sets debug overrides; leaves files in the fixture filesystem
func TestCephMDSCephFSHeldFlockSplitCutIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION=1 for owned held-lock split reconnect with a cut connection")
	}
	runCephFSSplitReconnect(t, splitCutHeldLocks())
}

// splitHeldLocksShareFragment requires one fragment that reconnected the
// capabilities of two lock files.
func splitHeldLocksShareFragment(h *splitHeldFlock) error {
	files := map[int]map[int]bool{}
	for _, l := range h.Locks {
		if l.Fragment == nil {
			return fmt.Errorf("the %s lock has no attributed fragment", l.Name)
		}
		if files[*l.Fragment] == nil {
			files[*l.Fragment] = map[int]bool{}
		}
		files[*l.Fragment][l.Member] = true
	}
	for _, members := range files {
		if len(members) >= 2 {
			return nil
		}
	}
	return fmt.Errorf("no fragment reconnected the capabilities of two lock files: %v", files)
}

// TestCephFSHeldLockSplitCellRecipes is a cluster-free check of the split
// cells: their blobs, attachment, probe expectations at each unlock and the
// fragment requirements.
func TestCephFSHeldLockSplitCellRecipes(t *testing.T) {
	const gid, dir, first, last = 42, 0x10000000000, 0x10000000001, 0x10000000020
	place := func(h *splitHeldFlock) *splitHeldFlock {
		h.Directory = "ceph-msgr-completion-x"
		for _, l := range h.Locks {
			l.File, l.Ino = fmt.Sprintf("split-%03d", l.Member), first+uint64(l.Member)
			l.Lock = l.filelock(gid)
		}
		return h
	}
	caps := func() []cephmsgr.MDSCapReconnect {
		c := []cephmsgr.MDSCapReconnect{{Ino: 1}, {Ino: dir}}
		for ino := uint64(first); ino <= last; ino++ {
			c = append(c, cephmsgr.MDSCapReconnect{Ino: ino})
		}
		return c
	}
	owners := map[uint64]bool{}
	for _, h := range []*splitHeldFlock{splitFcntlHeldLocks(), splitDenseHeldLocks(), splitCutHeldLocks()} {
		results := map[string]bool{}
		for _, l := range h.Locks {
			if l.Owner>>63 != 1 || l.Owner == cephFlockGoOwner+1 || l.Owner == cephFlockNativeOwner || results[l.Name] {
				t.Fatal("a split cell lock has a test, native or repeated owner or name", l.Name)
			}
			results[l.Name] = true
		}
		if h.Results == "" {
			t.Fatal("a split cell lacks its evidence file")
		}
	}
	for _, l := range splitDenseHeldLocks().Locks {
		if owners[l.Owner] {
			t.Fatal("two dense locks share an owner", l.Name)
		}
		owners[l.Owner] = true
	}

	// FCNTL split: one three-record blob on the first file.
	h := place(splitFcntlHeldLocks())
	state := caps()
	if _, err := splitHeldFlockAttach(state, h); err != nil {
		t.Fatal(err)
	}
	fcntl, flock, err := cephfsDecodeFilelockBlob(state[2].Flock)
	if err != nil || len(fcntl) != 2 || len(flock) != 1 || fcntl[0] != h.Locks[0].Lock || fcntl[1] != h.Locks[1].Lock || flock[0] != h.Locks[2].Lock ||
		h.Locks[0].ReconnectBlobHex != hex.EncodeToString(state[2].Flock) || h.Locks[2].ReconnectBlobHex != h.Locks[0].ReconnectBlobHex {
		t.Fatal("the FCNTL split blob differs", fcntl, flock, err)
	}
	if slices.ContainsFunc(state, func(c cephmsgr.MDSCapReconnect) bool { return c.Ino != first && len(c.Flock) != 0 }) {
		t.Fatal("another capability carries a lock blob")
	}
	want := func(h *splitHeldFlock) []string {
		var out []string
		for _, p := range h.probes() {
			out = append(out, fmt.Sprintf("%s:%d:%d", p.want.Action, p.want.Errno, len(p.want.Fcntl)))
		}
		return out
	}
	if got := want(h); !slices.Equal(got, []string{"split-flock-ex-nb:11:0", "split-fcntl-probe:0:7"}) {
		t.Fatal("FCNTL split probes differ", got)
	}
	if got := h.probes()[1].want.Fcntl; !reflect.DeepEqual(got, heldFcntlExpected([]cephfsFilelock{h.Locks[0].Lock, h.Locks[1].Lock})) {
		t.Fatal("FCNTL split expectation differs from the unsplit cell's", got)
	}
	h.Locks[0].released = true
	if got := h.probes()[1].want.Fcntl; !reflect.DeepEqual(got, heldFcntlExpected([]cephfsFilelock{h.Locks[1].Lock})) {
		t.Fatal("FCNTL split expectation after the first unlock differs", got)
	}
	h.Locks[1].released, h.Locks[2].released = true, true
	if got := want(h); !slices.Equal(got, []string{"split-flock-ex-nb:0:0", "split-fcntl-probe:0:7"}) || !reflect.DeepEqual(h.probes()[1].want.Fcntl, heldFcntlExpected(nil)) {
		t.Fatal("FCNTL split probes with nothing held differ", got)
	}
	if args := h.Locks[0].args(cephFlockExclusive); args != cephfsFilelockArgs(cephLockRuleFcntl, cephFlockExclusive, cephFlockGoOwner, 4101, 0, 16) {
		t.Fatal("FCNTL split acquire arguments differ")
	}
	if args := h.Locks[2].args(cephFlockUnlock); args != cephfsFlockArgs(cephFlockUnlock, cephFlockGoThirdOwner) {
		t.Fatal("FLOCK split unlock arguments differ")
	}

	// Dense split: blobs per file and the probe sequence across the unlocks.
	d := place(splitDenseHeldLocks())
	state = caps()
	if _, err := splitHeldFlockAttach(state, d); err != nil {
		t.Fatal(err)
	}
	carried := 0
	for _, c := range state {
		if len(c.Flock) == 0 {
			continue
		}
		carried++
		_, flock, err := cephfsDecodeFilelockBlob(c.Flock)
		n := 1
		if c.Ino == first {
			n = 2
		}
		if err != nil || len(flock) != n {
			t.Fatalf("dense blob of inode %#x has %d records, want %d", c.Ino, len(flock), n)
		}
	}
	if carried != 4 {
		t.Fatal("dense cell blobs carried on", carried, "capabilities")
	}
	steps := [][]string{
		{"split-flock-ex-nb:11:0", "split-flock-sh-nb:0:0", "split-second-flock-ex-nb:11:0", "split-middle-flock-ex-nb:11:0", "split-final-flock-ex-nb:11:0"},
		{"split-flock-ex-nb:11:0", "split-second-flock-ex-nb:11:0", "split-middle-flock-ex-nb:11:0", "split-final-flock-ex-nb:11:0"},
		{"split-flock-ex-nb:0:0", "split-second-flock-ex-nb:11:0", "split-middle-flock-ex-nb:11:0", "split-final-flock-ex-nb:11:0"},
		{"split-flock-ex-nb:0:0", "split-second-flock-ex-nb:0:0", "split-middle-flock-ex-nb:11:0", "split-final-flock-ex-nb:11:0"},
		{"split-flock-ex-nb:0:0", "split-second-flock-ex-nb:0:0", "split-middle-flock-ex-nb:0:0", "split-final-flock-ex-nb:11:0"},
		{"split-flock-ex-nb:0:0", "split-second-flock-ex-nb:0:0", "split-middle-flock-ex-nb:0:0", "split-final-flock-ex-nb:0:0"},
	}
	for i, step := range steps {
		if got := want(d); !slices.Equal(got, step) {
			t.Fatalf("dense probes with %d locks released differ:\n got %v\nwant %v", i, got, step)
		}
		if i < len(d.Locks) {
			d.Locks[i].released = true
		}
	}
	// A blob of a shared lock attached to another file, or two files on one
	// inode, is refused.
	bad := place(splitDenseHeldLocks())
	bad.Locks[2].Ino = first
	if _, err := splitHeldFlockAttach(caps(), bad); err == nil {
		t.Fatal("two lock files on one inode were attached")
	}

	// Fragments: both first-fragment files share fragment 0, the middle file
	// is in a middle fragment.
	inodes := func(from, to uint64) []uint64 {
		var out []uint64
		for ino := from; ino <= to; ino++ {
			out = append(out, ino)
		}
		return out
	}
	d = place(splitDenseHeldLocks())
	d.SentInodes = append([]uint64{1, dir}, inodes(first, last)...)
	d.Fragments = []splitReconnectFragment{{More: true, Inodes: append([]uint64{1, dir}, inodes(first, first+8)...)}, {More: true, Inodes: inodes(first+9, first+18)},
		{More: true, Inodes: inodes(first+19, first+28)}, {Inodes: inodes(first+29, last)}}
	if err := splitHeldFlockFragments(d, 4); err != nil || splitHeldLocksShareFragment(d) != nil || *d.Locks[3].Fragment != 1 || *d.Locks[4].Fragment != 3 || *d.Locks[1].Fragment != 0 {
		t.Fatal("dense fragment attribution differs", err)
	}
	moved := place(splitDenseHeldLocks())
	moved.SentInodes = d.SentInodes
	moved.Fragments = []splitReconnectFragment{{More: true, Inodes: append([]uint64{1, dir}, inodes(first, first+16)...)}, {More: true, Inodes: inodes(first+17, first+28)}, {Inodes: inodes(first+29, last)}}
	if err := splitHeldFlockFragments(moved, 3); err == nil {
		t.Fatal("a middle lock file in the first fragment was accepted")
	}
	apart := place(splitDenseHeldLocks())
	apart.SentInodes = d.SentInodes
	apart.Fragments = []splitReconnectFragment{{More: true, Inodes: append([]uint64{1, dir}, inodes(first, first)...)}, {More: true, Inodes: inodes(first+1, first+10)},
		{More: true, Inodes: inodes(first+11, first+20)}, {Inodes: inodes(first+21, last)}}
	if err := splitHeldFlockFragments(apart, 4); err != nil {
		t.Fatal(err)
	}
	if splitHeldLocksShareFragment(apart) == nil {
		t.Fatal("lock files in separate fragments were accepted as sharing one")
	}
}
