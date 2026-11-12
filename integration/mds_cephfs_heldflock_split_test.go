package integration_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Held whole-file FLOCK locks on inodes of a split client reconnect (issue
// #10). The recipe is the split reconnect of
// mds_cephfs_multi_reconnect_test.go with exclusive locks held on
// predetermined split files of the plan. The blob builder is the test-only
// one of mds_cephfs_heldflock_test.go; the product carries
// MDSCapReconnect.Flock as opaque bytes and implements no lock policy.

// TestCephMDSCephFSHeldFlockSplitReconnectIntegration holds one lock before
// the owned MDS restart, attaches the native-layout lock blob to exactly that
// inode's reconnect capability and requires native contention, the original
// owner and one explicit release on the replacement incarnation. The locked
// file is the first split file, whose capability a nonfinal fragment carries.
func TestCephMDSCephFSHeldFlockSplitReconnectIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION=1 for owned held-lock split reconnect acceptance")
	}
	runCephFSSplitReconnect(t, splitHeldFlockRecipe(false))
}

// TestCephMDSCephFSHeldFlockSplitFragmentsIntegration holds two
// locks, of two owners on two inodes: the first split file, whose capability
// a nonfinal fragment carries, and the last one, whose capability the final
// fragment carries. Each capability record carries its own one-record blob,
// so lock blobs travel in more than one MClientReconnect. After the restart
// both locks must be restored independently and released one at a time.
func TestCephMDSCephFSHeldFlockSplitFragmentsIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION=1 for owned two-fragment held-lock split reconnect acceptance")
	}
	runCephFSSplitReconnect(t, splitHeldFlockRecipe(true))
}

// splitHeldFlockRecipe returns the held-lock set of the single-lock cell, or
// with fragments set of the cell that also locks the last split file. The
// two locks have distinct owners, so a blob attached to the other inode's
// capability restores a lock that GETFILELOCK does not name on either inode.
func splitHeldFlockRecipe(fragments bool) *splitHeldFlock {
	first := &splitHeldLock{Name: "nonfinal", Probe: "split-flock-ex-nb", Owner: cephFlockGoOwner}
	if !fragments {
		return &splitHeldFlock{Results: "mds-cephfs-heldflock-split-reconnect-results.json", Locks: []*splitHeldLock{first}}
	}
	return &splitHeldFlock{Results: "mds-cephfs-heldflock-split-fragments-results.json",
		Locks: []*splitHeldLock{first, {Name: "final", Probe: "split-final-flock-ex-nb", Owner: cephFlockGoSecondOwner, Final: true}}}
}

// splitHeldLock is one held lock of the split recipe and its retained record.
type splitHeldLock struct {
	Name string
	// Probe is the closed native probe of the completion control that is
	// bound to this lock's file.
	Probe string
	Owner uint64
	// Final is whether the lock file is the last split file, whose
	// capability the final fragment must carry. Otherwise it is the first
	// split file and a fragment marked "more" must carry it.
	Final bool
	File  string
	Ino   uint64
	Lock  cephfsFilelock
	// AcquireTID and UnlockTID are recorded as soon as Submit accepted the
	// call, before its reply is awaited, so a call whose reply is lost or
	// refused is still counted with its TID.
	AcquireTID uint64
	UnlockTID  uint64
	// AcquireOutcome and UnlockOutcome are the outcome of the wait for each
	// call's first reply: empty while it was not awaited to the end, else
	// the reply result or the wait error.
	AcquireOutcome, UnlockOutcome string
	ReconnectBlobHex              string
	// Fragment is the index of the fragment whose handler reconnected this
	// inode's capability. It is nil until the attribution of this lock
	// succeeded, so a record written after a failed attribution does not
	// name the first fragment for a lock that was never assigned one.
	Fragment *int
	// ProbeBefore and ProbeAfter hold one entry per probe attempt, also for
	// an attempt that produced no receipt or a receipt that was refused.
	ProbeBefore         []splitFlockAttempt
	ProbeAfter          []splitFlockAttempt
	GetBefore, GetAfter *cephfsFilelock
	// AcquireTIDInHistoricOps is whether the original acquire TID appears in
	// the one bounded dump_historic_ops snapshot read right after adoption.
	// False does not show that the MDS never received such a request: an
	// entry can age out of that snapshot. The no-second-acquisition claim
	// rests on the accepted SETFILELOCK calls, on the common submission
	// ledger holding no other one (splitHeldFlockLedgerMatches), on the
	// empty reconnect resubmissions and on the documented no-blob controls.
	AcquireTIDInHistoricOps *bool
	// baseline is the native state of the lock file at its first probe.
	baseline *nativeMDSCephFSFlockState
}

// splitHeldFlock is the retained record of the held locks in one split
// reconnect.
type splitHeldFlock struct {
	// Results is the evidence file the recipe writes.
	Results   string
	Directory string
	// Locks are in acquisition order, which is also the order of the
	// explicit unlocks.
	Locks []*splitHeldLock
	// SetFileLockSubmissions counts application Submit calls of SETFILELOCK
	// that Submit accepted.
	SetFileLockSubmissions int
	// SentInodes are the inodes of every capability in the reconnect state.
	SentInodes []uint64
	// CapLines are the replacement MDS's handler and per-capability lines
	// and Fragments their parsed form.
	CapLines            []string
	Fragments           []splitReconnectFragment
	MDSBefore, MDSAfter nativeMDSCephFSDaemon
}

// mdsFirstReplyCall is the part of an accepted MDS request call that
// awaitAcceptedMDSCall uses.
type mdsFirstReplyCall interface {
	Transaction() uint64
	Wait(context.Context) (cephmsgr.MDSReply, error)
}

// awaitAcceptedMDSCall reports a call that Submit accepted to accepted
// before it waits for the first reply, and the outcome of that wait to
// settled before it returns. A caller that then fails on the outcome has
// already recorded both.
func awaitAcceptedMDSCall(ctx context.Context, call mdsFirstReplyCall, accepted func(uint64), settled func(cephmsgr.MDSReply, error)) (cephmsgr.MDSReply, error) {
	if accepted != nil {
		accepted(call.Transaction())
	}
	reply, err := call.Wait(ctx)
	if settled != nil {
		settled(reply, err)
	}
	return reply, err
}

// acceptFlock records one SETFILELOCK call of lock that Submit accepted.
func (h *splitHeldFlock) acceptFlock(lock *splitHeldLock, unlock bool, tid uint64) {
	if unlock {
		lock.UnlockTID = tid
	} else {
		lock.AcquireTID = tid
	}
	h.SetFileLockSubmissions++
}

// settleFlock records the outcome of the wait for that call's first reply.
func (l *splitHeldLock) settleFlock(unlock bool, reply cephmsgr.MDSReply, err error) {
	outcome := "reply result " + strconv.FormatInt(int64(reply.Result), 10)
	if err != nil {
		outcome = "wait error: " + err.Error()
	}
	if unlock {
		l.UnlockOutcome = outcome
	} else {
		l.AcquireOutcome = outcome
	}
}

// splitSubmission is one entry of the split recipe's common submission
// ledger: every request that Submit accepted, recorded before its reply is
// awaited, whichever call site submitted it.
type splitSubmission struct {
	Name string
	Op   uint32
	TID  uint64
}

// splitHeldFlockLedgerMatches checks the common submission ledger against
// the held-lock record. The accepted SETFILELOCK calls in it must be exactly
// one acquire per lock in order, then one unlock per lock in the same order,
// with the TIDs the hooks recorded and result 0 for each. A lock request
// submitted outside the hooked call sites, a second acquisition, a missing
// unlock or another order is refused.
func splitHeldFlockLedgerMatches(ledger []splitSubmission, h *splitHeldFlock) error {
	var got, want []uint64
	for _, s := range ledger {
		if s.Op == cephmsgr.MDSOpSetFileLock {
			got = append(got, s.TID)
		}
	}
	for _, l := range h.Locks {
		want = append(want, l.AcquireTID)
	}
	for _, l := range h.Locks {
		want = append(want, l.UnlockTID)
	}
	if !slices.Equal(got, want) || slices.Contains(want, 0) || h.SetFileLockSubmissions != len(want) {
		return fmt.Errorf("accepted SETFILELOCK TIDs in the submission ledger %v differ from the held-lock calls %v (%d counted)", got, want, h.SetFileLockSubmissions)
	}
	for _, l := range h.Locks {
		if l.AcquireOutcome != "reply result 0" || l.UnlockOutcome != "reply result 0" {
			return fmt.Errorf("SETFILELOCK of the %s lock ended as %q and %q", l.Name, l.AcquireOutcome, l.UnlockOutcome)
		}
	}
	return nil
}

// splitHeldFlockAttach puts each held lock's caller-built native-layout blob
// on exactly that inode's reconnect capability and returns the sent inodes.
// The collected state must not carry a lock blob of its own, and afterwards
// exactly the locked inodes carry one, each a single flock record of its own
// lock.
func splitHeldFlockAttach(caps []cephmsgr.MDSCapReconnect, h *splitHeldFlock) ([]uint64, error) {
	if slices.ContainsFunc(caps, func(c cephmsgr.MDSCapReconnect) bool { return len(c.Flock) != 0 }) {
		return nil, errors.New("collected reconnect state already carries a lock blob")
	}
	for _, l := range h.Locks {
		at := slices.IndexFunc(caps, func(c cephmsgr.MDSCapReconnect) bool { return c.Ino == l.Ino })
		if at < 0 || l.Ino == 0 || len(caps[at].Flock) != 0 {
			return nil, fmt.Errorf("collected reconnect state lacks the capability of the %s lock inode %#x or two locks share it", l.Name, l.Ino)
		}
		blob := cephfsFilelockBlob(nil, []cephfsFilelock{l.Lock})
		caps[at].Flock, l.ReconnectBlobHex = blob, hex.EncodeToString(blob)
	}
	var sent []uint64
	carried := 0
	for _, c := range caps {
		sent = append(sent, c.Ino)
		if len(c.Flock) == 0 {
			continue
		}
		carried++
		at := slices.IndexFunc(h.Locks, func(l *splitHeldLock) bool { return l.Ino == c.Ino })
		fcntl, flock, err := cephfsDecodeFilelockBlob(c.Flock)
		if at < 0 || err != nil || len(fcntl) != 0 || len(flock) != 1 || flock[0] != h.Locks[at].Lock {
			return nil, fmt.Errorf("capability of inode %#x carries a blob that is not its own single lock record: %x %v", c.Ino, c.Flock, err)
		}
	}
	if carried != len(h.Locks) {
		return nil, fmt.Errorf("%d capabilities carry a lock blob for %d held locks", carried, len(h.Locks))
	}
	return sent, nil
}

// splitFlockAttempt is one attempt of a lock's closed native probe.
type splitFlockAttempt struct {
	// Receipt is the native receipt with its FLOCK payload. It is nil when
	// the control request failed or its receipt carried no payload: that
	// attempt observed no errno, which is not errno 0.
	Receipt *heldFlockProbe
	// Error is the control error, the missing payload or the way the receipt
	// differs from the expected lock state. It is empty for an accepted
	// receipt. A refused receipt keeps the errno it reported in Receipt.
	Error string
}

// errno renders the observed errno of the attempt, or "absent" when the
// attempt has no receipt.
func (a splitFlockAttempt) errno() string {
	if a.Receipt == nil {
		return "absent"
	}
	return strconv.Itoa(a.Receipt.Probe.Errno)
}

// splitFlockAttemptOf turns the outcome of one request of the lock's closed
// nonblocking exclusive FLOCK probe into its attempt record and checks the
// receipt. The completion control admits only that one predetermined split
// file. The file is empty and must not change: the baseline is fixed by the
// lock's first accepted probe.
func splitFlockAttemptOf(r nativeMDSCephFSCompletionReceipt, requestErr error, held *splitHeldFlock, lock *splitHeldLock, wantErrno int) (splitFlockAttempt, error) {
	if requestErr != nil {
		err := fmt.Errorf("native split flock probe %s: %w", lock.Probe, requestErr)
		return splitFlockAttempt{Error: err.Error()}, err
	}
	if r.Native == nil || r.Native.Flock == nil {
		err := errors.New("native split flock receipt lacks its probe")
		return splitFlockAttempt{Error: err.Error()}, err
	}
	out := splitFlockAttempt{Receipt: &heldFlockProbe{Action: r.Action, NativeGlobalID: r.Native.NativeGlobalID, RequestID: r.RequestID, MDSGID: r.MDSGID, MDSNonce: r.MDSNonce, MDSIncarnation: r.MDSIncarnation, Probe: *r.Native.Flock}}
	err := checkSplitFlockProbe(r, held, lock, wantErrno)
	if err != nil {
		out.Error = err.Error()
	}
	return out, err
}

func checkSplitFlockProbe(r nativeMDSCephFSCompletionReceipt, held *splitHeldFlock, lock *splitHeldLock, wantErrno int) error {
	p := *r.Native.Flock
	if r.Action != lock.Probe || r.Target != lock.File || r.Native.Path != "/"+held.Directory+"/"+lock.File || r.Native.NativeGlobalID == lock.Lock.Client {
		return fmt.Errorf("native split flock probe is another action, names another file or the Go client: action %q target %q path %q", r.Action, r.Target, r.Native.Path)
	}
	if r.Mutation == nil || r.Mutation.Kind != "native-"+lock.Probe || r.Mutation.State != "applied" || r.Mutation.RequestID != r.RequestID ||
		p.Owner != cephFlockNativeOwner || p.Errno != wantErrno || p.Acquired != (wantErrno == 0) || p.Unlocked != p.Acquired || p.ElapsedNS <= 0 ||
		p.Before != p.After || p.Before.Ino != lock.Ino || p.Before.Size != 0 || p.Before.DataHex != "" {
		return fmt.Errorf("native split flock probe result or owned file state differs: want errno %d on inode %d, probe %+v mutation %+v", wantErrno, lock.Ino, p, r.Mutation)
	}
	if lock.baseline == nil {
		first := p.Before
		lock.baseline = &first
	} else if *lock.baseline != p.Before {
		return fmt.Errorf("split flock probes changed native bytes, size, version or second-resolution mtime: %+v then %+v", *lock.baseline, p.Before)
	}
	return nil
}

// splitFlockProbeAll probes every lock file once, in order, appends each
// attempt to that lock's before or after record and returns the observed
// errnos and the joined failures. A file whose attempt has no receipt is
// reported as "absent", not as an errno. Every file is probed even when an
// earlier one differs, so a failure shows the state of all of them.
func splitFlockProbeAll(ctx context.Context, control *mdsCephFSCompletionControl, held *splitHeldFlock, after bool, want func(*splitHeldLock) int) ([]string, error) {
	var errnos []string
	var failed error
	for _, lock := range held.Locks {
		r, requestErr := control.request(ctx, lock.Probe)
		attempt, err := splitFlockAttemptOf(r, requestErr, held, lock, want(lock))
		errnos = append(errnos, lock.recordProbe(after, attempt))
		if err != nil {
			failed = errors.Join(failed, fmt.Errorf("%s lock file: %w", lock.Name, err))
		}
	}
	return errnos, failed
}

// recordProbe appends the attempt to the lock's before or after record and
// returns its rendered errno.
func (l *splitHeldLock) recordProbe(after bool, attempt splitFlockAttempt) string {
	if after {
		l.ProbeAfter = append(l.ProbeAfter, attempt)
	} else {
		l.ProbeBefore = append(l.ProbeBefore, attempt)
	}
	return attempt.errno()
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
// capability of the inode ino. It requires the fragment sequence of the
// handler log (every fragment but the last marked "more"), only sent inodes,
// each at most once and in ascending order across the exchange (the native
// capability map and the sender's order), and the inode exactly once. With
// final unset the inode must be in a fragment marked "more": the first split
// file's capability does not sort into the last message. With final set it
// must be in the last, unmarked fragment: the last split file's capability
// sorts behind every other one.
func splitHeldFlockFragment(fragments []splitReconnectFragment, handlers int, sent []uint64, ino uint64, final bool) (int, error) {
	if len(fragments) < 2 || len(fragments) != handlers {
		return 0, fmt.Errorf("per-capability log has %d fragments, handler log %d", len(fragments), handlers)
	}
	found, previous := -1, uint64(0)
	for i, f := range fragments {
		if f.More != (i < len(fragments)-1) {
			return 0, fmt.Errorf("fragment %d of %d has more=%v", i, len(fragments), f.More)
		}
		for _, got := range f.Inodes {
			if !slices.Contains(sent, got) || got <= previous {
				return 0, fmt.Errorf("fragment %d reconnected inode %#x that was not sent, repeats or is out of order", i, got)
			}
			previous = got
			if got == ino {
				found = i
			}
		}
	}
	if found < 0 {
		return 0, fmt.Errorf("no fragment reconnected the locked inode %#x on a cached auth inode", ino)
	}
	if final && fragments[found].More {
		return 0, fmt.Errorf("the locked inode %#x was reconnected by the nonfinal fragment %d, not by the final one", ino, found)
	}
	if !final && !fragments[found].More {
		return 0, fmt.Errorf("the locked inode %#x was reconnected by the final fragment %d", ino, found)
	}
	return found, nil
}

// splitHeldFlockFragments attributes every held lock to its fragment and
// requires that no two locks share an inode. A lock's Fragment is set only
// by its own successful attribution: after a failure the failed lock and
// every later one stay unassigned. With a final and a nonfinal
// lock the lock blobs therefore travelled in different messages.
func splitHeldFlockFragments(h *splitHeldFlock, handlers int) error {
	for _, l := range h.Locks {
		l.Fragment = nil
	}
	seen := map[uint64]bool{}
	for _, l := range h.Locks {
		if seen[l.Ino] {
			return fmt.Errorf("two held locks name the inode %#x", l.Ino)
		}
		seen[l.Ino] = true
		at, err := splitHeldFlockFragment(h.Fragments, handlers, h.SentInodes, l.Ino, l.Final)
		if err != nil {
			return fmt.Errorf("%s lock: %w", l.Name, err)
		}
		l.Fragment = &at
	}
	return nil
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
	if at, err := splitHeldFlockFragment(fragments, 2, sent, 0x10000000001, false); err != nil || at != 0 {
		t.Fatal("locked inode not attributed to its nonfinal fragment", at, err)
	}
	if at, err := splitHeldFlockFragment(fragments, 2, sent, 0x10000000002, true); err != nil || at != 1 {
		t.Fatal("locked inode not attributed to the final fragment", at, err)
	}
	if _, err := splitHeldFlockFragment(fragments, 2, sent, 0x10000000001, true); err == nil {
		t.Fatal("an inode of a nonfinal fragment was accepted as carried by the final one")
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
		if _, err := splitHeldFlockFragment(tc.fragments, tc.handlers, sent, tc.lock, false); err == nil {
			t.Fatal("wrong fragment attribution accepted:", name)
		}
		// The structural refusals do not depend on the wanted fragment.
		if name != "lock in the final fragment" && name != "single fragment" {
			if _, err := splitHeldFlockFragment(tc.fragments, tc.handlers, sent, 0x10000000002, true); err == nil && name != "lock inode never reconnected" {
				t.Fatal("wrong fragment sequence accepted for a final-fragment lock:", name)
			}
		}
	}

	held := splitHeldFlockRecipe(false)
	held.Directory = "ceph-msgr-completion-x"
	lock := held.Locks[0]
	lock.File, lock.Ino, lock.Lock = "split-000", 0x10000000001, cephfsFilelock{Client: 42, Owner: cephFlockGoOwner, Type: cephFlockExclusive}
	checkSplitFlockProbeRefusals(t, held, lock, "split-final-flock-ex-nb")
}

// splitFlockTestReceipt builds the probe receipt of lock with the given errno.
func splitFlockTestReceipt(held *splitHeldFlock, lock *splitHeldLock, errno int, edit func(*nativeMDSCephFSCompletionReceipt)) nativeMDSCephFSCompletionReceipt {
	state := nativeMDSCephFSFlockState{Ino: lock.Ino}
	r := nativeMDSCephFSCompletionReceipt{RequestID: 9, Action: lock.Probe, Target: lock.File,
		Mutation: &nativeMDSCephFSMutation{Kind: "native-" + lock.Probe, RequestID: 9, State: "applied"},
		Native: &nativeMDSCephFSOutput{Path: "/" + held.Directory + "/" + lock.File, NativeGlobalID: 77,
			Flock: &nativeMDSCephFSFlockProbe{Owner: cephFlockNativeOwner, Errno: errno, Acquired: errno == 0, Unlocked: errno == 0, ElapsedNS: 1, Before: state, After: state}}}
	if edit != nil {
		edit(&r)
	}
	return r
}

// checkSplitFlockProbeRefusals accepts the two correct receipts of lock and
// refuses every wrong one; otherProbe is the probe action of another file.
func checkSplitFlockProbeRefusals(t *testing.T, held *splitHeldFlock, lock *splitHeldLock, otherProbe string) {
	t.Helper()
	state := nativeMDSCephFSFlockState{Ino: lock.Ino}
	if err := checkSplitFlockProbe(splitFlockTestReceipt(held, lock, 11, nil), held, lock, 11); err != nil || lock.baseline == nil || *lock.baseline != state {
		t.Fatal("blocked split probe refused", lock.Name, err)
	}
	if err := checkSplitFlockProbe(splitFlockTestReceipt(held, lock, 0, nil), held, lock, 0); err != nil {
		t.Fatal("acquiring split probe refused", lock.Name, err)
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
		"another native owner":             {11, 11, func(r *nativeMDSCephFSCompletionReceipt) { r.Native.Flock.Owner = cephFlockGoOwner }},
		"the probe action of another file": {11, 11, func(r *nativeMDSCephFSCompletionReceipt) { r.Action = otherProbe }},
		"the mutation of another probe":    {11, 11, func(r *nativeMDSCephFSCompletionReceipt) { r.Mutation.Kind = "native-" + otherProbe }},
		"no mutation record":               {11, 11, func(r *nativeMDSCephFSCompletionReceipt) { r.Mutation = nil }},
	} {
		if err := checkSplitFlockProbe(splitFlockTestReceipt(held, lock, tc.got, tc.edit), held, lock, tc.want); err == nil {
			t.Fatal("wrong split lock probe accepted:", lock.Name, name)
		}
	}
}

// TestCephFSHeldFlockSplitFragmentsChecks is a cluster-free check of the
// two-lock recipe: which blob goes on which capability, the attribution of
// the two locked inodes to a nonfinal and to the final fragment, the probe
// receipts of the second lock file and the submission ledger.
func TestCephFSHeldFlockSplitFragmentsChecks(t *testing.T) {
	const gid, dir, first, last = 42, 0x10000000000, 0x10000000001, 0x10000000020
	single := splitHeldFlockRecipe(false)
	if len(single.Locks) != 1 || single.Locks[0].Final || single.Locks[0].Probe != "split-flock-ex-nb" || single.Locks[0].Owner != cephFlockGoOwner ||
		single.Results != "mds-cephfs-heldflock-split-reconnect-results.json" {
		t.Fatal("the single-lock recipe changed", *single.Locks[0])
	}
	fresh := func() *splitHeldFlock {
		h := splitHeldFlockRecipe(true)
		h.Directory = "ceph-msgr-completion-x"
		h.Locks[0].File, h.Locks[0].Ino = "split-000", first
		h.Locks[1].File, h.Locks[1].Ino = "split-031", last
		for _, l := range h.Locks {
			l.Lock = cephfsFilelock{Client: gid, Owner: l.Owner, Type: cephFlockExclusive}
		}
		return h
	}
	h := fresh()
	if len(h.Locks) != 2 || h.Locks[0].Final || !h.Locks[1].Final || h.Locks[0].Probe != "split-flock-ex-nb" || h.Locks[1].Probe != "split-final-flock-ex-nb" ||
		h.Locks[0].Owner == h.Locks[1].Owner || h.Locks[1].Owner == cephFlockGoOwner+1 || h.Locks[1].Owner == cephFlockNativeOwner || h.Locks[1].Owner>>63 != 1 ||
		h.Results != "mds-cephfs-heldflock-split-fragments-results.json" || h.Results == single.Results {
		t.Fatal("the two-fragment recipe does not hold a nonfinal and a final lock of distinct high-bit owners")
	}

	// Blob attachment: one single-record blob on each locked inode only.
	caps := func() []cephmsgr.MDSCapReconnect {
		c := []cephmsgr.MDSCapReconnect{{Ino: 1}, {Ino: dir}}
		for ino := uint64(first); ino <= last; ino++ {
			c = append(c, cephmsgr.MDSCapReconnect{Ino: ino})
		}
		return c
	}
	state := caps()
	sent, err := splitHeldFlockAttach(state, h)
	if err != nil || len(sent) != 34 || sent[0] != 1 || sent[33] != last {
		t.Fatal("blob attachment refused", err, len(sent))
	}
	for _, c := range state {
		want := []byte(nil)
		for _, l := range h.Locks {
			if l.Ino == c.Ino {
				want = cephfsFilelockBlob(nil, []cephfsFilelock{l.Lock})
				if len(want) != 49 || l.ReconnectBlobHex != hex.EncodeToString(want) {
					t.Fatal("recorded blob differs from the single-record layout", l.Name)
				}
			}
		}
		if !slices.Equal(c.Flock, want) {
			t.Fatalf("capability of inode %#x carries %x, want %x", c.Ino, c.Flock, want)
		}
	}
	if slices.Equal(state[2].Flock, state[33].Flock) {
		t.Fatal("the two lock blobs are identical, so a swapped attachment would go unnoticed")
	}
	for name, edit := range map[string]func([]cephmsgr.MDSCapReconnect, *splitHeldFlock) []cephmsgr.MDSCapReconnect{
		"state already carries a blob": func(c []cephmsgr.MDSCapReconnect, _ *splitHeldFlock) []cephmsgr.MDSCapReconnect {
			c[5].Flock = cephfsFilelockBlob(nil, nil)
			return c
		},
		"final lock inode absent": func(c []cephmsgr.MDSCapReconnect, _ *splitHeldFlock) []cephmsgr.MDSCapReconnect { return c[:33] },
		"nonfinal lock inode absent": func(c []cephmsgr.MDSCapReconnect, _ *splitHeldFlock) []cephmsgr.MDSCapReconnect {
			return slices.Delete(c, 2, 3)
		},
		"both locks on one inode": func(c []cephmsgr.MDSCapReconnect, h *splitHeldFlock) []cephmsgr.MDSCapReconnect {
			h.Locks[1].Ino = first
			return c
		},
		"lock without an inode": func(c []cephmsgr.MDSCapReconnect, h *splitHeldFlock) []cephmsgr.MDSCapReconnect {
			h.Locks[1].Ino = 0
			return c
		},
	} {
		bad := fresh()
		if _, err := splitHeldFlockAttach(edit(caps(), bad), bad); err == nil {
			t.Fatal("wrong reconnect lock state accepted:", name)
		}
	}

	// Fragment attribution of both locked inodes.
	inodes := func(from, to uint64) []uint64 {
		var out []uint64
		for ino := from; ino <= to; ino++ {
			out = append(out, ino)
		}
		return out
	}
	const cut = 0x1000000001b
	layout := func(edit func(f []splitReconnectFragment)) []splitReconnectFragment {
		f := []splitReconnectFragment{{More: true, Inodes: append([]uint64{1, dir}, inodes(first, cut)...)}, {Inodes: inodes(cut+1, last)}}
		if edit != nil {
			edit(f)
		}
		return f
	}
	h.SentInodes, h.Fragments = sent, layout(nil)
	if h.Locks[0].Fragment != nil || h.Locks[1].Fragment != nil {
		t.Fatal("a lock names a fragment before its attribution")
	}
	fragmentIs := func(l *splitHeldLock, want int) bool { return l.Fragment != nil && *l.Fragment == want }
	if err := splitHeldFlockFragments(h, 2); err != nil || !fragmentIs(h.Locks[0], 0) || !fragmentIs(h.Locks[1], 1) ||
		!h.Fragments[*h.Locks[0].Fragment].More || h.Fragments[*h.Locks[1].Fragment].More {
		t.Fatal("the two locked inodes were not attributed to a nonfinal and the final fragment", err)
	}
	three := []splitReconnectFragment{{More: true, Inodes: append([]uint64{1, dir}, inodes(first, first+9)...)}, {More: true, Inodes: inodes(first+10, cut)}, {Inodes: inodes(cut+1, last)}}
	h.Fragments = three
	if err := splitHeldFlockFragments(h, 3); err != nil || !fragmentIs(h.Locks[0], 0) || !fragmentIs(h.Locks[1], 2) {
		t.Fatal("attribution over three fragments differs", err)
	}
	// A failed attribution of the final lock leaves it unassigned in the
	// persisted record, distinct from the nonfinal lock's valid fragment 0,
	// and a later failure of the first lock clears the earlier assignment.
	h.Fragments = layout(func(f []splitReconnectFragment) { f[1].Inodes = f[1].Inodes[:len(f[1].Inodes)-1] })
	if err := splitHeldFlockFragments(h, 2); err == nil || !fragmentIs(h.Locks[0], 0) || h.Locks[1].Fragment != nil {
		t.Fatal("a failed attribution of the final lock assigned it a fragment or lost the nonfinal one", err)
	}
	if raw, err := json.Marshal(h.Locks); err != nil || strings.Count(string(raw), `"Fragment":0`) != 1 || strings.Count(string(raw), `"Fragment":null`) != 1 {
		t.Fatal("the persisted record does not keep the unattributed lock apart from fragment 0", err, string(raw))
	}
	if err := splitHeldFlockFragments(h, 3); err == nil || h.Locks[0].Fragment != nil || h.Locks[1].Fragment != nil {
		t.Fatal("a refused exchange kept an earlier fragment assignment", err)
	}
	for name, tc := range map[string]struct {
		fragments []splitReconnectFragment
		handlers  int
		edit      func(*splitHeldFlock)
	}{
		"final lock inode in the nonfinal fragment": {layout(func(f []splitReconnectFragment) {
			f[0].Inodes, f[1].Inodes = append(f[0].Inodes, f[1].Inodes...), nil
		}), 2, nil},
		"nonfinal lock inode in the final fragment": {layout(func(f []splitReconnectFragment) {
			f[1].Inodes, f[0].Inodes = append(slices.Clone(f[0].Inodes[2:]), f[1].Inodes...), f[0].Inodes[:2]
		}), 2, nil},
		"final lock inode never reconnected":    {layout(func(f []splitReconnectFragment) { f[1].Inodes = f[1].Inodes[:len(f[1].Inodes)-1] }), 2, nil},
		"nonfinal lock inode never reconnected": {layout(func(f []splitReconnectFragment) { f[0].Inodes = slices.Delete(f[0].Inodes, 2, 3) }), 2, nil},
		"handler count differs":                 {layout(nil), 3, nil},
		"unsplit exchange":                      {[]splitReconnectFragment{{Inodes: sent}}, 1, nil},
		"final fragment marked more":            {layout(func(f []splitReconnectFragment) { f[1].More = true }), 2, nil},
		"nonfinal fragment not marked":          {layout(func(f []splitReconnectFragment) { f[0].More = false }), 2, nil},
		"final lock inode repeated": {layout(func(f []splitReconnectFragment) {
			f[0].Inodes = append(f[0].Inodes, last)
		}), 2, nil},
		"inode that was not sent":     {layout(func(f []splitReconnectFragment) { f[1].Inodes[0] = last + 9 }), 2, nil},
		"both locks name one inode":   {layout(nil), 2, func(h *splitHeldFlock) { h.Locks[1].Ino, h.Locks[1].Final = first, false }},
		"locks swapped between files": {layout(nil), 2, func(h *splitHeldFlock) { h.Locks[0].Ino, h.Locks[1].Ino = last, first }},
	} {
		bad := fresh()
		bad.SentInodes, bad.Fragments = sent, tc.fragments
		if tc.edit != nil {
			tc.edit(bad)
		}
		if err := splitHeldFlockFragments(bad, tc.handlers); err == nil {
			t.Fatal("wrong two-lock fragment attribution accepted:", name)
		}
	}

	// Probe receipts of both lock files; a receipt of one file is refused
	// for the other lock.
	probes := fresh()
	checkSplitFlockProbeRefusals(t, probes, probes.Locks[0], probes.Locks[1].Probe)
	checkSplitFlockProbeRefusals(t, probes, probes.Locks[1], probes.Locks[0].Probe)
	cross := fresh()
	for i, l := range cross.Locks {
		other := cross.Locks[1-i]
		if err := checkSplitFlockProbe(splitFlockTestReceipt(cross, other, 11, nil), cross, l, 11); err == nil {
			t.Fatal("the probe receipt of the other lock file was accepted for the lock:", l.Name)
		}
	}
	if cross.Locks[0].baseline != nil || cross.Locks[1].baseline != nil {
		t.Fatal("a refused receipt fixed a baseline")
	}

	// Probe attempts: a lost control request and a receipt without a FLOCK
	// payload are recorded as absent, a refused receipt keeps its errno.
	attempts := fresh()
	nonfinal, final := attempts.Locks[0], attempts.Locks[1]
	ok, err := splitFlockAttemptOf(splitFlockTestReceipt(attempts, nonfinal, 11, nil), nil, attempts, nonfinal, 11)
	if err != nil || ok.Receipt == nil || ok.Error != "" || nonfinal.recordProbe(true, ok) != "11" {
		t.Fatal("an accepted receipt was not recorded with its errno", err, ok)
	}
	lost, err := splitFlockAttemptOf(nativeMDSCephFSCompletionReceipt{}, errors.New("controller gone"), attempts, final, 11)
	if err == nil || lost.Receipt != nil || !strings.Contains(lost.Error, "controller gone") || final.recordProbe(true, lost) != "absent" {
		t.Fatal("a failed control request was recorded as a probe result", err, lost)
	}
	bare := splitFlockTestReceipt(attempts, final, 11, nil)
	bare.Native.Flock = nil
	empty, err := splitFlockAttemptOf(bare, nil, attempts, final, 11)
	if err == nil || empty.Receipt != nil || empty.Error == "" || final.recordProbe(true, empty) != "absent" {
		t.Fatal("a receipt without its FLOCK payload was recorded as a probe result", err, empty)
	}
	// The dropped-final-blob outcome: a real receipt that acquired the file.
	dropped, err := splitFlockAttemptOf(splitFlockTestReceipt(attempts, final, 0, nil), nil, attempts, final, 11)
	if err == nil || dropped.Receipt == nil || dropped.Receipt.Probe.Errno != 0 || dropped.Error == "" || final.recordProbe(true, dropped) != "0" {
		t.Fatal("a receipt that disagrees with the expected lock state lost its errno", err, dropped)
	}
	raw, err := json.Marshal(final.ProbeAfter)
	if err != nil || len(final.ProbeAfter) != 3 || len(nonfinal.ProbeAfter) != 1 || strings.Count(string(raw), `"Receipt":null`) != 2 || strings.Count(string(raw), `"errno":0`) != 1 {
		t.Fatal("the persisted attempts do not keep the absent results apart from errno 0", err, string(raw))
	}

	// Submission ledger: acquire, acquire, unlock, unlock among the other
	// requests of the recipe.
	done := fresh()
	for i, l := range done.Locks {
		done.acceptFlock(l, false, uint64(40+i))
		l.settleFlock(false, cephmsgr.MDSReply{}, nil)
	}
	for i, l := range done.Locks {
		done.acceptFlock(l, true, uint64(80+i))
		l.settleFlock(true, cephmsgr.MDSReply{}, nil)
	}
	ledger := []splitSubmission{{"mkdir", cephmsgr.MDSOpMkdir, 2}, {"create", cephmsgr.MDSOpCreate, 3},
		{"acquire nonfinal", cephmsgr.MDSOpSetFileLock, 40}, {"acquire final", cephmsgr.MDSOpSetFileLock, 41}, {"get", cephmsgr.MDSOpGetFileLock, 42},
		{"get", cephmsgr.MDSOpGetFileLock, 70}, {"unlock nonfinal", cephmsgr.MDSOpSetFileLock, 80}, {"get", cephmsgr.MDSOpGetFileLock, 90},
		{"unlock final", cephmsgr.MDSOpSetFileLock, 81}, {"unlink", cephmsgr.MDSOpUnlink, 95}}
	if err := splitHeldFlockLedgerMatches(ledger, done); err != nil {
		t.Fatal(err)
	}
	lockAt := func(l []splitSubmission, n int) int {
		for i, s := range l {
			if s.Op == cephmsgr.MDSOpSetFileLock {
				if n == 0 {
					return i
				}
				n--
			}
		}
		t.Fatal("ledger has too few lock requests")
		return -1
	}
	for name, edit := range map[string]func([]splitSubmission, *splitHeldFlock) []splitSubmission{
		// A lock request submitted through the plain submit helper.
		"second acquisition outside the hooks": func(l []splitSubmission, _ *splitHeldFlock) []splitSubmission {
			return slices.Insert(l, 6, splitSubmission{"reacquire final", cephmsgr.MDSOpSetFileLock, 71})
		},
		"missing unlock": func(l []splitSubmission, _ *splitHeldFlock) []splitSubmission {
			return slices.Delete(l, lockAt(l, 3), lockAt(l, 3)+1)
		},
		"another TID": func(l []splitSubmission, _ *splitHeldFlock) []splitSubmission {
			l[lockAt(l, 1)].TID = 99
			return l
		},
		"unlocks in the other order": func(l []splitSubmission, _ *splitHeldFlock) []splitSubmission {
			a, b := lockAt(l, 2), lockAt(l, 3)
			l[a].TID, l[b].TID = l[b].TID, l[a].TID
			return l
		},
		"count differs from the ledger": func(l []splitSubmission, h *splitHeldFlock) []splitSubmission {
			h.SetFileLockSubmissions++
			return l
		},
		"refused unlock": func(l []splitSubmission, h *splitHeldFlock) []splitSubmission {
			h.Locks[1].settleFlock(true, cephmsgr.MDSReply{Result: -11}, nil)
			return l
		},
		"unlock never awaited": func(l []splitSubmission, h *splitHeldFlock) []splitSubmission {
			h.Locks[0].UnlockOutcome = ""
			return l
		},
		"lost acquire reply": func(l []splitSubmission, h *splitHeldFlock) []splitSubmission {
			h.Locks[0].settleFlock(false, cephmsgr.MDSReply{}, errors.New("lost"))
			return l
		},
		"lock never unlocked": func(l []splitSubmission, h *splitHeldFlock) []splitSubmission {
			h.Locks[1].UnlockTID = 0
			h.SetFileLockSubmissions--
			return slices.Delete(l, lockAt(l, 3), lockAt(l, 3)+1)
		},
	} {
		bad := fresh()
		for i, l := range bad.Locks {
			*l = *done.Locks[i]
		}
		bad.SetFileLockSubmissions = done.SetFileLockSubmissions
		if err := splitHeldFlockLedgerMatches(edit(slices.Clone(ledger), bad), bad); err == nil {
			t.Fatal("wrong SETFILELOCK submission ledger accepted:", name)
		}
	}
	// The single-lock cell uses the same check with its two calls.
	one := splitHeldFlockRecipe(false)
	one.acceptFlock(one.Locks[0], false, 40)
	one.Locks[0].settleFlock(false, cephmsgr.MDSReply{}, nil)
	one.acceptFlock(one.Locks[0], true, 80)
	one.Locks[0].settleFlock(true, cephmsgr.MDSReply{}, nil)
	oneLedger := []splitSubmission{{"acquire", cephmsgr.MDSOpSetFileLock, 40}, {"get", cephmsgr.MDSOpGetFileLock, 41}, {"unlock", cephmsgr.MDSOpSetFileLock, 80}}
	if err := splitHeldFlockLedgerMatches(oneLedger, one); err != nil {
		t.Fatal(err)
	}
	if splitHeldFlockLedgerMatches(append(oneLedger, splitSubmission{"again", cephmsgr.MDSOpSetFileLock, 81}), one) == nil {
		t.Fatal("a third SETFILELOCK of the single-lock cell was accepted")
	}
}

// acceptedFlockCall is an accepted SETFILELOCK call whose wait checks what
// the record already holds when it starts.
type acceptedFlockCall struct {
	tid   uint64
	reply cephmsgr.MDSReply
	err   error
	check func()
}

func (c acceptedFlockCall) Transaction() uint64 { return c.tid }

func (c acceptedFlockCall) Wait(context.Context) (cephmsgr.MDSReply, error) {
	c.check()
	return c.reply, c.err
}

// An accepted SETFILELOCK is in the record with its TID before its reply is
// awaited, whatever the wait then returns.
func TestCephFSHeldFlockSplitRecordsAcceptedSetFileLock(t *testing.T) {
	held := splitHeldFlockRecipe(false)
	lock := held.Locks[0]
	accepted := func(unlock bool) func(uint64) { return func(tid uint64) { held.acceptFlock(lock, unlock, tid) } }
	settled := func(unlock bool) func(cephmsgr.MDSReply, error) {
		return func(r cephmsgr.MDSReply, err error) { lock.settleFlock(unlock, r, err) }
	}
	// The acquire reached the MDS and its reply is lost.
	lost := errors.New("mds session ended")
	reply, err := awaitAcceptedMDSCall(context.Background(), acceptedFlockCall{tid: 41, err: lost, check: func() {
		if lock.AcquireTID != 41 || held.SetFileLockSubmissions != 1 || lock.AcquireOutcome != "" {
			t.Fatal("the accepted acquire was not recorded before its reply was awaited", *lock)
		}
	}}, accepted(false), settled(false))
	if !errors.Is(err, lost) || reply.Result != 0 || lock.AcquireTID != 41 || held.SetFileLockSubmissions != 1 || lock.AcquireOutcome != "wait error: mds session ended" {
		t.Fatal("record after a failed wait for the acquire", err, *lock)
	}
	// The unlock is accepted and refused with a signed result.
	reply, err = awaitAcceptedMDSCall(context.Background(), acceptedFlockCall{tid: 57, reply: cephmsgr.MDSReply{Result: -11}, check: func() {
		if lock.UnlockTID != 57 || held.SetFileLockSubmissions != 2 || lock.UnlockOutcome != "" || lock.AcquireTID != 41 {
			t.Fatal("the accepted unlock was not recorded before its reply was awaited", *lock)
		}
	}}, accepted(true), settled(true))
	if err != nil || reply.Result != -11 || lock.UnlockOutcome != "reply result -11" || lock.AcquireOutcome != "wait error: mds session ended" || held.SetFileLockSubmissions != 2 {
		t.Fatal("record after a refused unlock", err, *lock)
	}
	// Calls that are not SETFILELOCK carry no hooks.
	if _, err := awaitAcceptedMDSCall(context.Background(), acceptedFlockCall{tid: 60, check: func() {}}, nil, nil); err != nil ||
		lock.AcquireTID != 41 || lock.UnlockTID != 57 || held.SetFileLockSubmissions != 2 || lock.AcquireOutcome != "wait error: mds session ended" || lock.UnlockOutcome != "reply result -11" {
		t.Fatal("a call without hooks changed the record", err, *lock)
	}
	// An accepted call of one lock leaves the other lock's record alone.
	two := splitHeldFlockRecipe(true)
	two.acceptFlock(two.Locks[1], false, 9)
	two.Locks[1].settleFlock(false, cephmsgr.MDSReply{}, nil)
	if two.SetFileLockSubmissions != 1 || two.Locks[1].AcquireTID != 9 || two.Locks[0].AcquireTID != 0 || two.Locks[0].AcquireOutcome != "" {
		t.Fatal("an accepted call was recorded on the other lock")
	}
}
