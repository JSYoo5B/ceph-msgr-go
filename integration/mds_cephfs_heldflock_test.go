package integration_test

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Held whole-file FLOCK across an owned MDS restart (issue #10). The blob
// builder and probe checks below are test-only consumer code: the product
// carries MDSCapReconnect.Flock as opaque bytes and implements no lock policy.

// cephfsFilelockBlob builds the Client::_encode_filelocks layout that
// Server::recover_filelocks decodes: a u32 fcntl count and that many
// ceph_filelock records, then a u32 flock count and records. Each record is
// start, length, client, owner and pid as little-endian u64 plus the type byte.
func cephfsFilelockBlob(fcntl, flock []cephfsFilelock) []byte {
	var blob []byte
	for _, locks := range [][]cephfsFilelock{fcntl, flock} {
		blob = binary.LittleEndian.AppendUint32(blob, uint32(len(locks)))
		for _, l := range locks {
			for _, v := range []uint64{l.Start, l.Length, l.Client, l.Owner, l.PID} {
				blob = binary.LittleEndian.AppendUint64(blob, v)
			}
			blob = append(blob, l.Type)
		}
	}
	return blob
}

// TestCephFSHeldFlockBlobMatchesNativeLayout compares the builder with the
// native-encoded blob retained in the reconnect provenance (one byte-range
// fcntl lock and one whole-file flock written by Client::_encode_filelocks
// code) and checks the held-lock blob this acceptance sends.
func TestCephFSHeldFlockBlobMatchesNativeLayout(t *testing.T) {
	data, err := os.ReadFile("../internal/mds/testdata/mds-reconnect-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var proof struct {
		Fixtures map[string]struct {
			Decoded struct {
				Caps []struct {
					FlockLen int    `json:"flock_len"`
					Hex      string `json:"flockbl_hex"`
				} `json:"caps"`
			} `json:"decoded_reconnect"`
		} `json:"fixtures"`
	}
	if err := json.Unmarshal(data, &proof); err != nil {
		t.Fatal(err)
	}
	var native [][]byte
	for _, f := range proof.Fixtures {
		for _, c := range f.Decoded.Caps {
			if c.FlockLen != 0 {
				raw, err := hex.DecodeString(c.Hex)
				if err != nil || len(raw) != c.FlockLen {
					t.Fatal("native flock blob is not valid hex of its stated length", err)
				}
				native = append(native, raw)
			}
		}
	}
	if len(native) != 1 {
		t.Fatalf("expected exactly one native flock blob, found %d", len(native))
	}
	built := cephfsFilelockBlob(
		[]cephfsFilelock{{Start: 4096, Length: 8192, Client: 4100, Owner: 0x8888, PID: 4242, Type: cephFlockShared}},
		[]cephfsFilelock{{Client: 4100, Owner: 0x9999, PID: 4243, Type: cephFlockExclusive}})
	if !slices.Equal(built, native[0]) {
		t.Fatalf("builder differs from the native layout:\n got %x\nwant %x", built, native[0])
	}

	held := cephfsFilelock{Client: 4711, Owner: cephFlockGoOwner, Type: cephFlockExclusive}
	blob := cephfsFilelockBlob(nil, []cephfsFilelock{held})
	if len(blob) != 4+4+41 || binary.LittleEndian.Uint32(blob[0:4]) != 0 || binary.LittleEndian.Uint32(blob[4:8]) != 1 {
		t.Fatalf("held-lock blob counts or size differ: %x", blob)
	}
	decoded, err := cephfsDecodeFilelock(blob[8:])
	if err != nil || decoded != held {
		t.Fatal("held-lock record does not decode to the requested lock", decoded, err)
	}
	if got := cephfsFlockArgs(cephFlockExclusive, held.Owner); got[1] != cephFlockExclusive || binary.LittleEndian.Uint64(got[2:10]) != held.Owner {
		t.Fatal("acquire arguments do not name the same owner as the reconnect record")
	}
	if len(cephfsFilelockBlob(nil, nil)) != 8 {
		t.Fatal("an empty lock blob must still carry both counts")
	}
}

// heldFlockRecipe is the set of whole-file FLOCK locks that the Go recovery
// session holds across the owned MDS restart: one lock of Type for each owner,
// acquired in order. The same order is the order of the reconnect blob and of
// the explicit unlocks after the restart.
type heldFlockRecipe struct {
	Name string
	// Results is the evidence file the recovery recipe writes.
	Results string
	Type    byte
	Owners  []uint64
}

var (
	// exclusiveHeldFlock is the held lock of TestCephMDSCephFSRecoveryIntegration.
	exclusiveHeldFlock = heldFlockRecipe{Name: "exclusive-one-owner", Results: "mds-cephfs-recovery-results.json", Type: cephFlockExclusive, Owners: []uint64{cephFlockGoOwner}}
	// sharedHeldFlock is the held lock set of
	// TestCephMDSCephFSSharedFlockRecoveryIntegration.
	sharedHeldFlock = heldFlockRecipe{Name: "shared-two-owners", Results: "mds-cephfs-sharedflock-recovery-results.json", Type: cephFlockShared,
		Owners: []uint64{cephFlockGoOwner, cephFlockGoSecondOwner}}
)

// cephFlockGoSecondOwner is the second Go lock owner of the shared recipe.
// cephFlockGoOwner+1 is the owner of every GETFILELOCK test lock, so the two
// held owners and the testing owner are all distinct. The high bit marks an
// owner that identifies the lock without the pid (ceph_filelock_owner_equal
// in mds/flock.h).
const cephFlockGoSecondOwner uint64 = 0x80000000474f0003

// TestCephMDSCephFSSharedFlockRecoveryIntegration runs the recovery recipe of
// TestCephMDSCephFSRecoveryIntegration with two shared whole-file FLOCK locks
// of two distinct Go owners on one owned file in place of the one exclusive
// lock. It needs a fresh mds-cephfs fixture of its own: the recipe performs
// the fixture's only owned MDS restart.
func TestCephMDSCephFSSharedFlockRecoveryIntegration(t *testing.T) {
	runCephFSRecovery(t, sharedHeldFlock)
}

// locks returns the held locks of the recipe for the given client GlobalID.
func (r heldFlockRecipe) locks(client uint64) []cephfsFilelock {
	locks := make([]cephfsFilelock, 0, len(r.Owners))
	for _, owner := range r.Owners {
		locks = append(locks, cephfsFilelock{Client: client, Owner: owner, Type: r.Type})
	}
	return locks
}

// callName labels one SETFILELOCK of the recipe in the request evidence.
func (r heldFlockRecipe) callName(op string, owner int) string {
	kind := "exclusive"
	if r.Type == cephFlockShared {
		kind = "shared"
	}
	if len(r.Owners) == 1 {
		return "flock-" + kind + "-" + op
	}
	return fmt.Sprintf("flock-%s-%s-owner-%d", kind, op, owner)
}

// heldFlockProbeWant is one closed native probe and the errno it must return.
type heldFlockProbeWant struct {
	Action string
	Errno  int
}

// probes returns the native probes that must pass, in order, while the first
// held of the recipe's locks are released and the others still held. With
// every lock released a native exclusive acquire succeeds. An exclusive lock
// refuses a native exclusive acquire. Shared locks also refuse it, and while
// all of them are held a native shared acquire must succeed beside them.
func (r heldFlockRecipe) probes(held int) []heldFlockProbeWant {
	switch {
	case held == 0:
		return []heldFlockProbeWant{{"flock-ex-nb", 0}}
	case r.Type == cephFlockShared && held == len(r.Owners):
		return []heldFlockProbeWant{{"flock-sh-nb", 0}, {"flock-ex-nb", 11}}
	default:
		return []heldFlockProbeWant{{"flock-ex-nb", 11}}
	}
}

// heldFlockHolder checks a GETFILELOCK result for an exclusive test lock of
// another owner. Server::handle_client_file_readlock (mds/Server.cc) answers
// with the first overlapping lock of another owner in held_locks order
// (ceph_lock_state_t::look_for_lock and get_overlapping_locks in
// mds/flock.cc). held_locks is a multimap keyed by start, so whole-file locks
// keep their insertion order: SETFILELOCK grants before the restart and the
// reconnect blob order (Server::recover_filelocks) after it. The answer must
// therefore be exactly the first lock still held. A restored set that lost or
// reordered a lock names another one.
func heldFlockHolder(got cephfsFilelock, held []cephfsFilelock) error {
	if len(held) == 0 || got != held[0] {
		return fmt.Errorf("GETFILELOCK names %+v, not the first held Go lock of %+v", got, held)
	}
	return nil
}

// cephfsDecodeFilelockBlob reads a reconnect lock blob the way
// Server::recover_filelocks (mds/Server.cc) does: a u32 fcntl count and its
// records, then a u32 flock count and its records. Unlike the MDS it also
// refuses trailing bytes, so the test sees exactly what it sent.
func cephfsDecodeFilelockBlob(blob []byte) (fcntl, flock []cephfsFilelock, err error) {
	read := func() ([]cephfsFilelock, error) {
		if len(blob) < 4 {
			return nil, errors.New("lock blob ends before a lock count")
		}
		n := binary.LittleEndian.Uint32(blob)
		blob = blob[4:]
		if uint64(n)*41 > uint64(len(blob)) {
			return nil, fmt.Errorf("lock blob holds fewer than %d records", n)
		}
		locks := make([]cephfsFilelock, 0, n)
		for range n {
			l, err := cephfsDecodeFilelock(blob[:41])
			if err != nil {
				return nil, err
			}
			locks, blob = append(locks, l), blob[41:]
		}
		return locks, nil
	}
	if fcntl, err = read(); err != nil {
		return nil, nil, err
	}
	if flock, err = read(); err != nil {
		return nil, nil, err
	}
	if len(blob) != 0 {
		return nil, nil, fmt.Errorf("lock blob has %d trailing bytes", len(blob))
	}
	return fcntl, flock, nil
}

// TestCephFSSharedFlockRecipeBlobAndChecks is a cluster-free check of the
// shared recipe's reconnect blob, of its probe sequence and of the
// GETFILELOCK holder check.
func TestCephFSSharedFlockRecipeBlobAndChecks(t *testing.T) {
	const gid = 4711
	locks := sharedHeldFlock.locks(gid)
	if len(locks) != 2 || locks[0].Owner == locks[1].Owner || cephFlockGoSecondOwner == cephFlockGoOwner+1 || cephFlockGoSecondOwner == cephFlockNativeOwner ||
		cephFlockGoSecondOwner>>63 != 1 || locks[0].Type != cephFlockShared || locks[1].Type != cephFlockShared {
		t.Fatal("the shared recipe does not hold two shared locks of distinct high-bit owners", locks)
	}
	if got := exclusiveHeldFlock.locks(gid); len(got) != 1 || got[0] != (cephfsFilelock{Client: gid, Owner: cephFlockGoOwner, Type: cephFlockExclusive}) {
		t.Fatal("the exclusive recipe changed", got)
	}
	// Client::_encode_filelocks writes each held lock as one ceph_filelock
	// in held_locks order; with both locks at start 0 that is the order of
	// acquisition. The single-record blob is the layout compared with the
	// native encoder in TestCephFSHeldFlockBlobMatchesNativeLayout.
	blob := cephfsFilelockBlob(nil, locks)
	first, second := cephfsFilelockBlob(nil, locks[:1]), cephfsFilelockBlob(nil, locks[1:])
	want := slices.Concat(first[:4], binary.LittleEndian.AppendUint32(nil, 2), first[8:], second[8:])
	if len(blob) != 4+4+2*41 || !slices.Equal(blob, want) {
		t.Fatalf("two-lock blob is not the counts and the two native-layout records:\n got %x\nwant %x", blob, want)
	}
	fcntl, flock, err := cephfsDecodeFilelockBlob(blob)
	if err != nil || len(fcntl) != 0 || !slices.Equal(flock, locks) {
		t.Fatal("two-lock blob does not decode to the held locks", fcntl, flock, err)
	}
	for name, bad := range map[string][]byte{
		"empty":              nil,
		"no flock count":     blob[:4],
		"one record missing": blob[:len(blob)-41],
		"truncated record":   blob[:len(blob)-1],
		"trailing bytes":     append(slices.Clone(blob), 0),
		"count overflow":     slices.Concat(blob[:4], []byte{0xff, 0xff, 0xff, 0xff}, blob[8:]),
	} {
		if _, _, err := cephfsDecodeFilelockBlob(bad); err == nil {
			t.Fatal("malformed lock blob accepted:", name)
		}
	}
	// Without the second owner's record the MDS would restore one lock only.
	if _, flock, err := cephfsDecodeFilelockBlob(first); err != nil || len(flock) != 1 {
		t.Fatal("single-owner blob decoded differently", flock, err)
	}

	type step = []heldFlockProbeWant
	for name, tc := range map[string]struct {
		recipe heldFlockRecipe
		held   int
		want   step
	}{
		"shared, both held":     {sharedHeldFlock, 2, step{{"flock-sh-nb", 0}, {"flock-ex-nb", 11}}},
		"shared, one released":  {sharedHeldFlock, 1, step{{"flock-ex-nb", 11}}},
		"shared, both released": {sharedHeldFlock, 0, step{{"flock-ex-nb", 0}}},
		"exclusive, held":       {exclusiveHeldFlock, 1, step{{"flock-ex-nb", 11}}},
		"exclusive, released":   {exclusiveHeldFlock, 0, step{{"flock-ex-nb", 0}}},
	} {
		if got := tc.recipe.probes(tc.held); !slices.Equal(got, tc.want) {
			t.Fatal("probe sequence differs:", name, got)
		}
	}
	if sharedHeldFlock.callName("unlock", 1) != "flock-shared-unlock-owner-1" || exclusiveHeldFlock.callName("acquire", 0) != "flock-exclusive-acquire" {
		t.Fatal("SETFILELOCK call labels differ")
	}

	if err := heldFlockHolder(locks[0], locks); err != nil {
		t.Fatal("the first held Go lock was refused as the GETFILELOCK holder", err)
	}
	if err := heldFlockHolder(locks[1], locks[1:]); err != nil {
		t.Fatal("the remaining lock was refused after the first unlock", err)
	}
	for name, tc := range map[string]struct {
		got  cephfsFilelock
		held []cephfsFilelock
	}{
		// A restore that carried only the second owner names it first.
		"second owner while both held": {locks[1], locks},
		"released owner":               {locks[0], locks[1:]},
		"no conflict":                  {cephfsFilelock{Client: gid, Owner: cephFlockGoOwner + 1, Type: cephFlockUnlock}, locks[1:]},
		"another client":               {cephfsFilelock{Client: gid + 1, Owner: cephFlockGoSecondOwner, Type: cephFlockShared}, locks[1:]},
		"exclusive type":               {cephfsFilelock{Client: gid, Owner: cephFlockGoSecondOwner, Type: cephFlockExclusive}, locks[1:]},
		"byte range":                   {cephfsFilelock{Client: gid, Owner: cephFlockGoSecondOwner, Type: cephFlockShared, Length: 1}, locks[1:]},
		"native probe lock":            {cephfsFilelock{Client: 77, Owner: cephFlockNativeOwner, Type: cephFlockShared}, locks[1:]},
	} {
		if heldFlockHolder(tc.got, tc.held) == nil {
			t.Fatal("wrong GETFILELOCK holder accepted:", name)
		}
	}
	if heldFlockHolder(locks[0], nil) == nil {
		t.Fatal("a holder was accepted with no lock held")
	}

	var h heldFlockEvidence
	for i, l := range slices.Concat(locks, locks) {
		kind, op := l.Type, "acquire"
		if i >= len(locks) {
			kind, op = cephFlockUnlock, "unlock"
		}
		h.acceptFlock(sharedHeldFlock.callName(op, i%len(locks)), l.Owner, kind)(uint64(10 + i))
		h.settleFlock(cephmsgr.MDSReply{}, nil)
	}
	if err := heldFlockCallsExact(h.Calls, locks); err != nil || h.SetFileLockSubmissions != 4 {
		t.Fatal("the shared recipe's SETFILELOCK calls were refused", err, h.Calls)
	}
	edit := func(f func([]heldFlockCall) []heldFlockCall) []heldFlockCall { return f(slices.Clone(h.Calls)) }
	for name, calls := range map[string][]heldFlockCall{
		"second acquisition": edit(func(c []heldFlockCall) []heldFlockCall {
			return slices.Insert(c, 2, heldFlockCall{Owner: locks[0].Owner, Type: cephFlockShared, TID: 99, Outcome: "reply result 0"})
		}),
		"missing unlock":    edit(func(c []heldFlockCall) []heldFlockCall { return c[:3] }),
		"unlock order":      edit(func(c []heldFlockCall) []heldFlockCall { c[2], c[3] = c[3], c[2]; return c }),
		"exclusive acquire": edit(func(c []heldFlockCall) []heldFlockCall { c[1].Type = cephFlockExclusive; return c }),
		"refused unlock":    edit(func(c []heldFlockCall) []heldFlockCall { c[3].Outcome = "reply result -11"; return c }),
		"unawaited reply":   edit(func(c []heldFlockCall) []heldFlockCall { c[0].Outcome = ""; return c }),
		"wait error": edit(func(c []heldFlockCall) []heldFlockCall {
			c[2].Outcome = "wait error: context deadline exceeded"
			return c
		}),
	} {
		if heldFlockCallsExact(calls, locks) == nil {
			t.Fatal("wrong SETFILELOCK submissions accepted:", name)
		}
	}
	var failed heldFlockEvidence
	failed.acceptFlock("flock-shared-acquire-owner-0", locks[0].Owner, cephFlockShared)(7)
	failed.settleFlock(cephmsgr.MDSReply{}, errors.New("lost"))
	if failed.SetFileLockSubmissions != 1 || failed.Calls[0].TID != 7 || failed.Calls[0].Outcome != "wait error: lost" {
		t.Fatal("an accepted SETFILELOCK whose wait failed was not recorded with its TID", failed.Calls)
	}
}

// heldFlockCall is one application SETFILELOCK call that Submit accepted.
type heldFlockCall struct {
	Name  string
	Owner uint64
	// Type is the requested lock type: shared, exclusive or unlock.
	Type byte
	TID  uint64
	// Outcome is the outcome of the wait for the first reply: empty while it
	// was not awaited to the end, else the reply result or the wait error.
	Outcome string
}

// heldFlockEvidence is the retained record of one held-lock restart.
type heldFlockEvidence struct {
	Recipe          string
	Directory, File string
	Ino             uint64
	GlobalID        uint64
	CephFSNonce     uint32
	NativeAddress   string
	// Locks are the granted locks in acquisition order, which is also their
	// order in the reconnect blob and the order of the unlocks.
	Locks []cephfsFilelock
	// Calls are recorded as soon as Submit accepted them, before their replies
	// are awaited, so a call whose reply is lost or refused is still counted
	// with its TID. SetFileLockSubmissions counts them.
	Calls                  []heldFlockCall
	SetFileLockSubmissions int
	ReconnectBlobHex       string
	NativeBefore           *nativeMDSCephFSFlockState
	ProbeBefore            []heldFlockProbe
	ProbeAfter             []heldFlockProbe
	GetBefore, GetAfter    *cephfsFilelock
	// GetBetween holds the GETFILELOCK result after each unlock that left
	// another lock held.
	GetBetween          []cephfsFilelock
	MDSBefore, MDSAfter nativeMDSCephFSDaemon
	// AcquireTIDInHistoricOps is whether an original acquire TID appears in
	// the one bounded dump_historic_ops snapshot read right after adoption.
	// False does not show that the MDS never received such a request: an
	// entry can age out of that snapshot. The no-second-acquisition claim
	// rests on Calls and the empty reconnect resubmissions.
	AcquireTIDInHistoricOps *bool
}

// acceptFlock returns the hook that records one SETFILELOCK call that Submit
// accepted.
func (h *heldFlockEvidence) acceptFlock(name string, owner uint64, kind byte) func(uint64) {
	return func(tid uint64) {
		h.Calls = append(h.Calls, heldFlockCall{Name: name, Owner: owner, Type: kind, TID: tid})
		h.SetFileLockSubmissions++
	}
}

// heldFlockCallsExact checks that the application submitted exactly one
// SETFILELOCK acquire per held lock, in order, and then exactly one unlock per
// owner in the same order, each answered with result 0. A second acquisition
// of any owner, before or after the restart, is refused.
func heldFlockCallsExact(calls []heldFlockCall, locks []cephfsFilelock) error {
	if len(calls) != 2*len(locks) {
		return fmt.Errorf("%d SETFILELOCK submissions for %d held locks", len(calls), len(locks))
	}
	for i, c := range calls {
		l, want := locks[i%len(locks)], locks[i%len(locks)].Type
		if i >= len(locks) {
			want = cephFlockUnlock
		}
		if c.Owner != l.Owner || c.Type != want || c.TID == 0 || c.Outcome != "reply result 0" {
			return fmt.Errorf("SETFILELOCK submission %d differs: %+v", i, c)
		}
	}
	return nil
}

// settleFlock records the outcome of the wait for the last accepted call.
func (h *heldFlockEvidence) settleFlock(reply cephmsgr.MDSReply, err error) {
	c := &h.Calls[len(h.Calls)-1]
	c.Outcome = "reply result " + strconv.FormatInt(int64(reply.Result), 10)
	if err != nil {
		c.Outcome = "wait error: " + err.Error()
	}
}

type heldFlockProbe struct {
	Action         string
	NativeGlobalID uint64
	RequestID      int64
	MDSGID         uint64
	MDSNonce       uint32
	MDSIncarnation int32
	Probe          nativeMDSCephFSFlockProbe
}

// heldFlockProbeNative runs one closed nonblocking native FLOCK probe on the
// metadata fixture's go-file and checks the receipt. The owned file must not
// change: the baseline is fixed by the first probe.
func heldFlockProbeNative(ctx context.Context, control *mdsCephFSRecoveryControl, want heldFlockProbeWant, ino uint64, baseline **nativeMDSCephFSFlockState) (heldFlockProbe, error) {
	r, err := control.request(ctx, want.Action, "go-file")
	if err != nil {
		return heldFlockProbe{}, fmt.Errorf("native %s probe: %w", want.Action, err)
	}
	if r.Native == nil || r.Native.Flock == nil {
		return heldFlockProbe{}, errors.New("native flock receipt lacks its probe")
	}
	p := *r.Native.Flock
	out := heldFlockProbe{Action: r.Action, NativeGlobalID: r.Native.NativeGlobalID, RequestID: r.RequestID, MDSGID: r.MDSGID, MDSNonce: r.MDSNonce, MDSIncarnation: r.MDSIncarnation, Probe: p}
	return out, checkHeldFlockProbe(r, want, ino, control.goGIDs, baseline)
}

func checkHeldFlockProbe(r nativeMDSCephFSReceipt, want heldFlockProbeWant, ino uint64, goGIDs map[uint64]bool, baseline **nativeMDSCephFSFlockState) error {
	p := *r.Native.Flock
	if r.Action != want.Action || r.Native.Action != want.Action || goGIDs[r.Native.NativeGlobalID] || r.Native.NativeGlobalID == 0 {
		return fmt.Errorf("native flock receipt is for %q/%q or from a Go client, want %q", r.Action, r.Native.Action, want.Action)
	}
	if r.Mutation == nil || r.Mutation.Kind != "native-"+want.Action || r.Mutation.State != "applied" || r.Mutation.RequestID != r.RequestID ||
		p.Owner != cephFlockNativeOwner || p.Errno != want.Errno || p.Acquired != (want.Errno == 0) || p.Unlocked != p.Acquired || p.ElapsedNS <= 0 ||
		p.Before != p.After || p.Before.Ino != ino || p.Before.Size != 0 || p.Before.DataHex != "" {
		return fmt.Errorf("native %s probe result or owned file state differs: want errno %d, probe %+v mutation %+v", want.Action, want.Errno, p, r.Mutation)
	}
	if *baseline == nil {
		first := p.Before
		*baseline = &first
	} else if **baseline != p.Before {
		return fmt.Errorf("flock probes changed native bytes, size, version or second-resolution mtime: %+v then %+v", **baseline, p.Before)
	}
	return nil
}

// heldFlockObserve runs the probes in order, appends every receipt it got to
// probes and returns the first failure.
func heldFlockObserve(ctx context.Context, control *mdsCephFSRecoveryControl, wants []heldFlockProbeWant, ino uint64, baseline **nativeMDSCephFSFlockState, probes *[]heldFlockProbe) error {
	for _, want := range wants {
		p, err := heldFlockProbeNative(ctx, control, want, ino, baseline)
		if p.Action != "" {
			*probes = append(*probes, p)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// TestCephFSHeldFlockProbeReceiptChecks is a cluster-free check of the
// probe receipt refusals of the recovery recipe.
func TestCephFSHeldFlockProbeReceiptChecks(t *testing.T) {
	const ino = 0x10000000002
	state := nativeMDSCephFSFlockState{Ino: ino, Version: 4, MTime: 1}
	goGIDs := map[uint64]bool{42: true}
	receipt := func(action string, errno int, edit func(*nativeMDSCephFSReceipt)) nativeMDSCephFSReceipt {
		var r nativeMDSCephFSReceipt
		raw := fmt.Sprintf(`{"request_id":9,"action":%q,"mutation":{"kind":"native-%s","request_id":9,"state":"applied"}}`, action, action)
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			t.Fatal(err)
		}
		r.Native = &nativeMDSCephFSOutput{Action: action, Path: "/d/go-file", NativeGlobalID: 77,
			Flock: &nativeMDSCephFSFlockProbe{Owner: cephFlockNativeOwner, Errno: errno, Acquired: errno == 0, Unlocked: errno == 0, ElapsedNS: 1, Before: state, After: state}}
		if edit != nil {
			edit(&r)
		}
		return r
	}
	var baseline *nativeMDSCephFSFlockState
	for _, want := range sharedHeldFlock.probes(2) {
		if err := checkHeldFlockProbe(receipt(want.Action, want.Errno, nil), want, ino, goGIDs, &baseline); err != nil {
			t.Fatal("correct probe refused", want, err)
		}
	}
	if baseline == nil || *baseline != state {
		t.Fatal("the first probe did not fix the baseline", baseline)
	}
	shared, exclusive := heldFlockProbeWant{"flock-sh-nb", 0}, heldFlockProbeWant{"flock-ex-nb", 11}
	for name, tc := range map[string]struct {
		want heldFlockProbeWant
		got  nativeMDSCephFSReceipt
	}{
		"shared acquire refused beside the shared locks": {shared, receipt("flock-sh-nb", 11, nil)},
		"exclusive acquired while a lock is held":        {exclusive, receipt("flock-ex-nb", 0, nil)},
		"exclusive receipt for a shared probe":           {shared, receipt("flock-ex-nb", 0, nil)},
		"shared receipt for an exclusive probe":          {exclusive, receipt("flock-sh-nb", 11, nil)},
		"mutation of the other probe": {exclusive, receipt("flock-ex-nb", 11, func(r *nativeMDSCephFSReceipt) {
			r.Mutation.Kind = "native-flock-sh-nb"
		})},
		"mutation of another request": {exclusive, receipt("flock-ex-nb", 11, func(r *nativeMDSCephFSReceipt) { r.Mutation.RequestID = 8 })},
		"uncertain native process":    {exclusive, receipt("flock-ex-nb", 11, func(r *nativeMDSCephFSReceipt) { r.Mutation.State = "uncertain" })},
		"no mutation record":          {exclusive, receipt("flock-ex-nb", 11, func(r *nativeMDSCephFSReceipt) { r.Mutation = nil })},
		"the Go client itself":        {exclusive, receipt("flock-ex-nb", 11, func(r *nativeMDSCephFSReceipt) { r.Native.NativeGlobalID = 42 })},
		"acquired but not released":   {shared, receipt("flock-sh-nb", 0, func(r *nativeMDSCephFSReceipt) { r.Native.Flock.Unlocked = false })},
		"another native owner":        {exclusive, receipt("flock-ex-nb", 11, func(r *nativeMDSCephFSReceipt) { r.Native.Flock.Owner = cephFlockGoOwner })},
		"another inode": {exclusive, receipt("flock-ex-nb", 11, func(r *nativeMDSCephFSReceipt) {
			r.Native.Flock.Before.Ino++
			r.Native.Flock.After.Ino++
		})},
		"file changed during the probe": {exclusive, receipt("flock-ex-nb", 11, func(r *nativeMDSCephFSReceipt) { r.Native.Flock.After.MTime++ })},
		"file changed between probes": {exclusive, receipt("flock-ex-nb", 11, func(r *nativeMDSCephFSReceipt) {
			r.Native.Flock.Before.Version++
			r.Native.Flock.After.Version++
		})},
	} {
		if err := checkHeldFlockProbe(tc.got, tc.want, ino, goGIDs, &baseline); err == nil {
			t.Fatal("wrong held-lock probe accepted:", name)
		}
	}
}
