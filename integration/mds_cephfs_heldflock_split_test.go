package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// Held whole-file FLOCK on one inode of a split client reconnect (issue #10).
// The recipe is the split reconnect of mds_cephfs_multi_reconnect_test.go with
// one exclusive lock held on the plan's predetermined split file. The blob
// builder is the test-only one of mds_cephfs_heldflock_test.go; the product
// carries MDSCapReconnect.Flock as opaque bytes and implements no lock policy.

// TestCephMDSCephFSHeldFlockSplitReconnectIntegration holds the lock before
// the owned MDS restart, attaches the native-layout lock blob to exactly that
// inode's reconnect capability and requires native contention, the original
// owner and one explicit release on the replacement incarnation.
func TestCephMDSCephFSHeldFlockSplitReconnectIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION=1 for owned held-lock split reconnect acceptance")
	}
	runCephFSSplitReconnect(t, &splitHeldFlock{})
}

// splitHeldFlock is the retained record of one held lock in a split reconnect.
type splitHeldFlock struct {
	Directory, File string
	Ino             uint64
	Lock            cephfsFilelock
	AcquireTID      uint64
	UnlockTID       uint64
	// SetFileLockSubmissions counts application Submit calls of SETFILELOCK.
	SetFileLockSubmissions int
	ReconnectBlobHex       string
	// SentInodes are the inodes of every capability in the reconnect state.
	SentInodes []uint64
	// CapLines are the replacement MDS's handler and per-capability lines;
	// Fragments is their parsed form and LockFragment the index of the
	// fragment whose handler reconnected the locked inode's capability.
	CapLines             []string
	Fragments            []splitReconnectFragment
	LockFragment         int
	ProbeBefore          []heldFlockProbe
	ProbeAfter           []heldFlockProbe
	GetBefore, GetAfter  *cephfsFilelock
	MDSBefore, MDSAfter  nativeMDSCephFSDaemon
	AcquireReplayedByMDS *bool
}

// splitFlockProbeNative runs the closed nonblocking exclusive FLOCK probe of
// the completion control, which admits only the plan's predetermined split
// file, and checks the receipt. The file is empty and must not change: the
// baseline is fixed by the first probe.
func splitFlockProbeNative(ctx context.Context, control *mdsCephFSCompletionControl, held *splitHeldFlock, wantErrno int, baseline **nativeMDSCephFSFlockState) (heldFlockProbe, error) {
	r, err := control.request(ctx, "split-flock-ex-nb")
	if err != nil {
		return heldFlockProbe{}, fmt.Errorf("native split flock probe: %w", err)
	}
	if r.Native == nil || r.Native.Flock == nil {
		return heldFlockProbe{}, errors.New("native split flock receipt lacks its probe")
	}
	p := *r.Native.Flock
	out := heldFlockProbe{Action: r.Action, NativeGlobalID: r.Native.NativeGlobalID, RequestID: r.RequestID, MDSGID: r.MDSGID, MDSNonce: r.MDSNonce, MDSIncarnation: r.MDSIncarnation, Probe: p}
	return out, checkSplitFlockProbe(r, held, wantErrno, baseline)
}

func checkSplitFlockProbe(r nativeMDSCephFSCompletionReceipt, held *splitHeldFlock, wantErrno int, baseline **nativeMDSCephFSFlockState) error {
	p := *r.Native.Flock
	if r.Target != held.File || r.Native.Path != "/"+held.Directory+"/"+held.File || r.Native.NativeGlobalID == held.Lock.Client {
		return fmt.Errorf("native split flock probe names another file or the Go client: target %q path %q", r.Target, r.Native.Path)
	}
	if r.Mutation == nil || r.Mutation.Kind != "native-split-flock-ex-nb" || r.Mutation.State != "applied" || r.Mutation.RequestID != r.RequestID ||
		p.Owner != cephFlockNativeOwner || p.Errno != wantErrno || p.Acquired != (wantErrno == 0) || p.Unlocked != p.Acquired || p.ElapsedNS <= 0 ||
		p.Before != p.After || p.Before.Ino != held.Ino || p.Before.Size != 0 || p.Before.DataHex != "" {
		return fmt.Errorf("native split flock probe result or owned file state differs: want errno %d on inode %d, probe %+v mutation %+v", wantErrno, held.Ino, p, r.Mutation)
	}
	if *baseline == nil {
		first := p.Before
		*baseline = &first
	} else if **baseline != p.Before {
		return fmt.Errorf("split flock probes changed native bytes, size, version or second-resolution mtime: %+v then %+v", **baseline, p.Before)
	}
	return nil
}

// splitReconnectFragment is one MClientReconnect as the MDS handled it: its
// "more" marker and the inodes of the capabilities that handler reconnected.
type splitReconnectFragment struct {
	More   bool
	Inodes []uint64
}

// splitReconnectCapFragments parses the reconnect-cap-log lines of one client.
// Server::handle_client_reconnect (mds/Server.cc at the pinned tag) logs its
// handler line, then at level 15 one "open cap realm <realm> on [inode <ino>
// ..." line per capability on a cached auth inode, immediately before it
// recovers that capability's file locks. A capability line therefore belongs
// to the fragment of the handler line before it.
func splitReconnectCapFragments(lines []string, gid uint64) ([]splitReconnectFragment, error) {
	handler := "handle_client_reconnect client." + strconv.FormatUint(gid, 10)
	const capLine, inode = " open cap realm ", " on [inode "
	var fragments []splitReconnectFragment
	for _, line := range lines {
		if at := strings.Index(line, handler); at >= 0 {
			tail := strings.TrimSpace(line[at+len(handler):])
			if tail != "" && tail != "(more)" {
				return nil, fmt.Errorf("reconnect handler line of another client: %q", line)
			}
			fragments = append(fragments, splitReconnectFragment{More: tail == "(more)"})
			continue
		}
		at := strings.Index(line, capLine)
		if at < 0 {
			return nil, fmt.Errorf("unexpected line in the per-capability reconnect log: %q", line)
		}
		if len(fragments) == 0 {
			return nil, fmt.Errorf("capability line precedes every reconnect handler line: %q", line)
		}
		rest := line[at+len(capLine):]
		start := strings.Index(rest, inode)
		if start < 0 {
			return nil, fmt.Errorf("capability line names no inode: %q", line)
		}
		field, _, _ := strings.Cut(rest[start+len(inode):], " ")
		ino, err := strconv.ParseUint(strings.TrimPrefix(field, "0x"), 16, 64)
		if err != nil || !strings.HasPrefix(field, "0x") || ino == 0 {
			return nil, fmt.Errorf("capability line inode is not canonical hex: %q", line)
		}
		last := &fragments[len(fragments)-1]
		last.Inodes = append(last.Inodes, ino)
	}
	return fragments, nil
}

// splitHeldFlockFragment returns the index of the fragment that carried the
// locked inode's capability. It requires the fragment sequence of the handler
// log (every fragment but the last marked "more"), only sent inodes, each at
// most once and in ascending order across the exchange (the native capability
// map and the sender's order), and the locked inode exactly once in a nonfinal
// fragment: the lock file is the first split file, whose capability does not
// sort into the last message.
func splitHeldFlockFragment(fragments []splitReconnectFragment, handlers int, sent []uint64, lock uint64) (int, error) {
	if len(fragments) < 2 || len(fragments) != handlers {
		return 0, fmt.Errorf("per-capability log has %d fragments, handler log %d", len(fragments), handlers)
	}
	found, previous := -1, uint64(0)
	for i, f := range fragments {
		if f.More != (i < len(fragments)-1) {
			return 0, fmt.Errorf("fragment %d of %d has more=%v", i, len(fragments), f.More)
		}
		for _, ino := range f.Inodes {
			if !slices.Contains(sent, ino) || ino <= previous {
				return 0, fmt.Errorf("fragment %d reconnected inode %#x that was not sent, repeats or is out of order", i, ino)
			}
			previous = ino
			if ino == lock {
				found = i
			}
		}
	}
	if found < 0 {
		return 0, fmt.Errorf("no fragment reconnected the locked inode %#x on a cached auth inode", lock)
	}
	if !fragments[found].More {
		return 0, fmt.Errorf("the locked inode %#x was reconnected by the final fragment %d", lock, found)
	}
	return found, nil
}

// TestCephFSHeldFlockSplitFragmentAttribution is a cluster-free check of the
// log parser and of the receipt and attribution refusals.
func TestCephFSHeldFlockSplitFragmentAttribution(t *testing.T) {
	line := func(ino uint64) string {
		return fmt.Sprintf("2026-11-11T00:00:00.000+0000 7f0 15 mds.0.server open cap realm 0x1 on [inode %#x [2,head] /d/f auth v2 s=0 n(v0 1=1+0) caps={} | caps=0 0x55]", ino)
	}
	const more, final = "t  7 mds.0.server handle_client_reconnect client.42 (more)", "t  7 mds.0.server handle_client_reconnect client.42"
	lines := []string{more, line(0x1), line(0x10000000000), line(0x10000000001), final, line(0x10000000002)}
	fragments, err := splitReconnectCapFragments(lines, 42)
	want := []splitReconnectFragment{{More: true, Inodes: []uint64{0x1, 0x10000000000, 0x10000000001}}, {Inodes: []uint64{0x10000000002}}}
	if err != nil || len(fragments) != 2 || fragments[0].More != want[0].More || fragments[1].More != want[1].More ||
		!slices.Equal(fragments[0].Inodes, want[0].Inodes) || !slices.Equal(fragments[1].Inodes, want[1].Inodes) {
		t.Fatal("per-capability reconnect lines parsed differently", fragments, err)
	}
	for _, bad := range [][]string{
		{line(0x1), more, final},
		{more, "t 15 mds.0.server open cap realm 0x1 on [inode 1099511627776 [2,head]", final},
		{more, "t 15 mds.0.server open cap realm 0x1 on nothing", final},
		{more, "t 10 mds.0.cache unrelated", final},
		{"t  7 mds.0.server handle_client_reconnect client.42 foreign", final},
	} {
		if _, err := splitReconnectCapFragments(bad, 42); err == nil {
			t.Fatal("ambiguous per-capability reconnect log accepted", bad)
		}
	}
	sent := []uint64{0x1, 0x10000000000, 0x10000000001, 0x10000000002}
	if at, err := splitHeldFlockFragment(fragments, 2, sent, 0x10000000001); err != nil || at != 0 {
		t.Fatal("locked inode not attributed to its nonfinal fragment", at, err)
	}
	clone := func(edit func(f []splitReconnectFragment)) []splitReconnectFragment {
		f := []splitReconnectFragment{{More: true, Inodes: slices.Clone(want[0].Inodes)}, {Inodes: slices.Clone(want[1].Inodes)}}
		edit(f)
		return f
	}
	for name, tc := range map[string]struct {
		fragments []splitReconnectFragment
		handlers  int
		lock      uint64
	}{
		"lock in the final fragment":     {fragments, 2, 0x10000000002},
		"lock inode never reconnected":   {clone(func(f []splitReconnectFragment) { f[0].Inodes = f[0].Inodes[:2] }), 2, 0x10000000001},
		"handler count differs":          {fragments, 3, 0x10000000001},
		"single fragment":                {fragments[1:], 1, 0x10000000002},
		"final fragment marked more":     {clone(func(f []splitReconnectFragment) { f[1].More = true }), 2, 0x10000000001},
		"nonfinal fragment not marked":   {clone(func(f []splitReconnectFragment) { f[0].More = false }), 2, 0x10000000001},
		"inode that was not sent":        {clone(func(f []splitReconnectFragment) { f[1].Inodes[0] = 0x10000000009 }), 2, 0x10000000001},
		"inode repeated across messages": {clone(func(f []splitReconnectFragment) { f[1].Inodes[0] = 0x10000000001 }), 2, 0x10000000001},
		"inodes out of order":            {clone(func(f []splitReconnectFragment) { slices.Reverse(f[0].Inodes) }), 2, 0x10000000001},
	} {
		if _, err := splitHeldFlockFragment(tc.fragments, tc.handlers, sent, tc.lock); err == nil {
			t.Fatal("wrong fragment attribution accepted:", name)
		}
	}

	held := &splitHeldFlock{Directory: "ceph-msgr-completion-x", File: "split-000", Ino: 0x10000000001, Lock: cephfsFilelock{Client: 42, Owner: cephFlockGoOwner, Type: cephFlockExclusive}}
	state := nativeMDSCephFSFlockState{Ino: held.Ino}
	receipt := func(errno int, edit func(*nativeMDSCephFSCompletionReceipt)) nativeMDSCephFSCompletionReceipt {
		r := nativeMDSCephFSCompletionReceipt{RequestID: 9, Action: "split-flock-ex-nb", Target: held.File,
			Mutation: &nativeMDSCephFSMutation{Kind: "native-split-flock-ex-nb", RequestID: 9, State: "applied"},
			Native: &nativeMDSCephFSOutput{Path: "/" + held.Directory + "/" + held.File, NativeGlobalID: 77,
				Flock: &nativeMDSCephFSFlockProbe{Owner: cephFlockNativeOwner, Errno: errno, Acquired: errno == 0, Unlocked: errno == 0, ElapsedNS: 1, Before: state, After: state}}}
		if edit != nil {
			edit(&r)
		}
		return r
	}
	var baseline *nativeMDSCephFSFlockState
	if err := checkSplitFlockProbe(receipt(11, nil), held, 11, &baseline); err != nil || baseline == nil || *baseline != state {
		t.Fatal("blocked split probe refused", err)
	}
	if err := checkSplitFlockProbe(receipt(0, nil), held, 0, &baseline); err != nil {
		t.Fatal("acquiring split probe refused", err)
	}
	for name, tc := range map[string]struct {
		got, want int
		edit      func(*nativeMDSCephFSCompletionReceipt)
	}{
		"acquired while the lock must be held": {0, 11, nil},
		"blocked after the explicit unlock":    {11, 0, nil},
		"another file":                         {11, 11, func(r *nativeMDSCephFSCompletionReceipt) { r.Target = "split-001" }},
		"another path":                         {11, 11, func(r *nativeMDSCephFSCompletionReceipt) { r.Native.Path += "x" }},
		"another inode":                        {11, 11, func(r *nativeMDSCephFSCompletionReceipt) { r.Native.Flock.Before.Ino++; r.Native.Flock.After.Ino++ }},
		"the Go client itself":                 {11, 11, func(r *nativeMDSCephFSCompletionReceipt) { r.Native.NativeGlobalID = 42 }},
		"acquired but not released":            {0, 0, func(r *nativeMDSCephFSCompletionReceipt) { r.Native.Flock.Unlocked = false }},
		"uncertain native process":             {11, 11, func(r *nativeMDSCephFSCompletionReceipt) { r.Mutation.State = "uncertain" }},
		"file changed during the probe":        {11, 11, func(r *nativeMDSCephFSCompletionReceipt) { r.Native.Flock.After.MTime++ }},
		"file changed between probes": {11, 11, func(r *nativeMDSCephFSCompletionReceipt) {
			r.Native.Flock.Before.Version++
			r.Native.Flock.After.Version++
		}},
		"another native owner": {11, 11, func(r *nativeMDSCephFSCompletionReceipt) { r.Native.Flock.Owner = cephFlockGoOwner }},
	} {
		if err := checkSplitFlockProbe(receipt(tc.got, tc.edit), held, tc.want, &baseline); err == nil {
			t.Fatal("wrong split lock probe accepted:", name)
		}
	}
}
