package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// snapLifecycleModel is the native observation a snap id is expected to
// resolve to: the pinned rados mapext full map and the native bytes and stat
// of the head, observed just before the write that cloned it.
type snapLifecycleModel struct {
	Full  []cephmsgr.OSDExtent `json:"native_full_extents"`
	State nativeXattrSnapshot  `json:"state"`
}

// snapLifecycleClones is one native rados listsnaps listing as clone id to
// its sorted snapshot ids; the head is cephmsgr.OSDHeadSnapshot.
func snapLifecycleClones(t *testing.T, listing snapCLIListing) map[uint64][]uint64 {
	t.Helper()
	clones := map[uint64][]uint64{}
	for _, clone := range listing.Clones {
		id := clone.ID.ID
		if clone.ID.Head {
			id = cephmsgr.OSDHeadSnapshot
		}
		if _, ok := clones[id]; ok {
			t.Fatal("native listsnaps repeats a clone", id)
		}
		ids := []uint64{}
		for _, snap := range clone.Snapshots {
			ids = append(ids, snap.ID)
		}
		slices.Sort(ids)
		clones[id] = ids
	}
	return clones
}

func TestSnapLifecycleClones(t *testing.T) {
	var listing snapCLIListing
	raw := `{"name":"x","seq":5,"clones":[{"id":5,"snapshots":[{"id":5},{"id":4}],"size":3,"overlaps":[]},{"id":"head","snapshots":[],"size":4}]}`
	if err := json.Unmarshal([]byte(raw), &listing); err != nil {
		t.Fatal(err)
	}
	got := snapLifecycleClones(t, listing)
	want := map[uint64][]uint64{5: {4, 5}, cephmsgr.OSDHeadSnapshot: {}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("clones %v, want %v", got, want)
	}
}

// TestCephOSDSnapshotLifecycleReadIntegration reads self-managed snapshots
// through their removal and trim: the SPARSE_READ and READ compound that
// librbd sends at an image's read snap
// (ceph/ceph/blob/v20.2.4/src/librbd/io/ObjectRequest.cc), and ISDIRTY at a
// snap. One owned single-PG pool gets self-managed snaps s1 < s2; Go writes
// under explicit SnapContexts leave six objects: one clone naming both snaps,
// separate clones for s1 and s2, an object unchanged since before s1, a head
// deleted under s2, a dirty clone under an undirtied head and a clean clone
// under a dirty head. Native rados listsnaps checks every clone list.
//
// The pinned find_object_context (src/osd/PrimaryLogPG.cc) returns the head
// when the snap id is above the snapset seq, even for a removed id;
// otherwise the first clone at or above the id, or -ENOENT before any
// operation runs when that clone's snap list does not name the id or the id
// is still in the OSDMap removed_snaps_queue. The trimmer (trim_object)
// drops the id from each clone's list and deletes a clone left without one.
// make_writeable gives a clone the head's flags from before the cloning write
// (osd_types.cc copy_user_bits), so ISDIRTY at a snap reads the clone's
// FLAG_DIRTY, which a later UNDIRTY of the head leaves set.
//
// A compound SPARSE_READ or READ that resolves to a clone is compared with
// the intersection of the pinned rados mapext full map of the head, observed
// natively just before the cloning write, with the operation's range, and
// with that head's native bytes; native bytes, stat and user version at the
// same snap id must equal those of that head. (The CLI selects only pool
// snapshots, so it cannot map the clone itself; the store clones the head's
// extents.) Every SPARSE_READ must equal the same operation sent alone at the
// snap, and each -ENOENT must leave every per-op result untouched and agree
// with a native read at the same id.
//
// The cells run in four phases: live; pending, with nosnaptrim set and both
// MGRs paused under fixture-owned leases before s1 is removed, and
// reached when the native pg query and osd dump show s1 in the snap_trimq and
// the removed_snaps_queue; trimmed, after nosnaptrim is cleared, reached when
// s1 is in the PG's purged_snaps while still queued; and pruned, after the
// MGRs resume, reached when s1 is in neither set and no longer queued. The
// waits end on these native observations, never on elapsed time alone. At
// s1 only the unchanged object reads, and it reads its head; s2 reads the
// same in every phase. Refused shapes at a self-managed snap are not sent and
// leave the transaction IDs consecutive; a fresh session reads the same bytes
// and the native heads and clones are unchanged at the end.
//
// Not verified: EC, replica reads, more than one PG, a removal while the PG
// is inactive, faults, and a native client executing the compound or ISDIRTY
// (no native client composes the compound, and none observes the dirty flag;
// ISDIRTY's expectation is source-derived). No stock daemon answers the
// compound with a later operation's error
// (TestCephOSDReadCompoundLaterErrorIntegration).
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-replicated-pg-reply-v8.t-osdsnapshotlifecycleread key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=replicated-pg-reply-v8
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-replicated-pg-request-v9-reply-v8.t-osdsnapshotlifecycleread key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=replicated-pg-request-v9-reply-v8
//scenario:covers CLIENT to OSD
//fixture:isolated pauses both MGRs, toggles nosnaptrim and removes self-managed snapshots in an owned pool it deletes
func TestCephOSDSnapshotLifecycleReadIntegration(t *testing.T) {
	// The owned pool takes the selected profile's HASHPSPOOL policy (the
	// legacy-v6 OSD suites also select this test); one PG makes the
	// observed PG the PG of every owned object.
	fixture, request, hash := osdFixture, osdObjectRequest, false
	if isReplicatedPGFixtureProfile() {
		fixture, request, hash = replicatedPGFixture, replicatedPGRequest, true
	}
	c, _, base, _ := fixture(t)
	if base.PoolType == 3 {
		t.Skip("SPARSE_READ compound acceptance requires a replicated pool; EC is outside this subset")
	}
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Second)
	defer cancel()

	type cell struct {
		Phase   string            `json:"phase"`
		Name    string            `json:"name"`
		Snap    uint64            `json:"snap"`
		Reply   cephmsgr.OSDReply `json:"reply"`
		Error   string            `json:"error,omitempty"`
		Native  int               `json:"native_code"`
		Version uint64            `json:"native_version"`
	}
	receipt := struct {
		Pool         int64                                     `json:"pool_id"`
		Snaps        map[string]uint64                         `json:"snaps"`
		Models       map[string]snapLifecycleModel             `json:"models"`
		Managers     []string                                  `json:"paused_managers"`
		Removal      cephmsgr.PoolOpResult                     `json:"snap_removal"`
		States       map[string]copyPurgedTrimState            `json:"trim_states"`
		Observations map[string]int                            `json:"observations"`
		Clones       map[string]map[string]map[uint64][]uint64 `json:"clones"`
		Cells        []cell                                    `json:"cells"`
	}{Snaps: map[string]uint64{}, Models: map[string]snapLifecycleModel{}, States: map[string]copyPurgedTrimState{}, Observations: map[string]int{}, Clones: map[string]map[string]map[uint64][]uint64{}}
	t.Cleanup(func() {
		raw, err := json.Marshal(receipt)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(control, "osd-snapshot-lifecycle-read-results.json"), raw)
		}
		if err != nil {
			t.Error("snapshot lifecycle read evidence", err)
		}
	})
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		if r := nativePool(t, cleanup, "cleanup", nil); !r.PoolsRemoved {
			t.Error("owned pool was not removed")
		}
	})
	created := nativePool(t, ctx, "create", map[string]any{"slot": 1, "pg_num": 1, "size": base.Replicas, "min_size": base.MinSize, "hashpspool": hash})
	if created.PoolID <= 0 || created.PoolID == base.Pool || created.PGNum != 1 {
		t.Fatal("owned pool identity differs", created)
	}
	receipt.Pool = created.PoolID

	const (
		shared, split, unchanged = "snaplife-shared", "snaplife-split", "snaplife-unchanged"
		deleted, dirty, clean    = "snaplife-deleted", "snaplife-dirty", "snaplife-clean"
	)
	objects := []string{shared, split, unchanged, deleted, dirty, clean}
	routes := map[string]osdFixtureOracle{}
	for _, name := range objects {
		route := poolPlace(t, ctx, 1, name, "")
		if route.PoolType != 1 || route.Shard != -1 || route.ID != route.PrimaryID || route.PGID != fmt.Sprintf("%d.0", receipt.Pool) {
			t.Fatal("snapshot lifecycle target is not the owned PG's explicit replicated primary", route)
		}
		routes[name] = route
	}
	conn := mutationConnection(t, c, ctx, routes[shared])
	defer func() { conn.Close() }()
	epoch := uint32(0)
	send := func(name string, configure func(*cephmsgr.OSDRequest), ops ...cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
		r := request(routes[name], ops...)
		r.MapEpoch = max(r.MapEpoch, epoch)
		if configure != nil {
			configure(&r)
		}
		return conn.Request(ctx, r)
	}
	tick := uint32(0)
	write := func(name string, snapc cephmsgr.OSDSnapContext, op cephmsgr.OSDOperation) {
		t.Helper()
		tick++
		reply, err := send(name, func(r *cephmsgr.OSDRequest) {
			r.SnapContext = snapc
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700012000 + tick, Nanoseconds: tick}
		}, op)
		if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("owned snapshot lifecycle write", name, reply.Code, err)
		}
	}
	data := func(offset uint64, label byte, n int) cephmsgr.OSDOperation {
		b := bytes.Repeat([]byte{label, 0, 255, byte(offset >> 12)}, (n+3)/4)[:n]
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: offset, Length: uint64(n), Data: b}
	}
	undirty := cephmsgr.OSDOperation{Code: cephmsgr.OSDUndirty}
	model := func(key, name string) {
		t.Helper()
		m := poolSparseMapAt(t, ctx, name, "").SparseMap
		if m.Code != 0 || len(m.Full) == 0 {
			t.Fatal("native head map before the cloning write missing", name, m.Code)
		}
		receipt.Models[key] = snapLifecycleModel{Full: m.Full, State: m.State}
	}
	allocate := func(label string) uint64 {
		r := nativePool(t, ctx, "selfmanaged", map[string]any{"slot": 1, "action": "create"})
		if r.SnapID == 0 {
			t.Fatal("native self-managed snapshot allocation", r)
		}
		receipt.Snaps[label] = r.SnapID
		return r.SnapID
	}

	// Before the snaps: holes between three allocated ranges on the shared,
	// split and deleted objects, two ranges on the unchanged one, and a dirty
	// head on dirty and an undirtied head on clean.
	for _, name := range []string{shared, split, deleted} {
		for _, offset := range []uint64{8192, 0x11000, 0x20000} {
			write(name, cephmsgr.OSDSnapContext{}, data(offset, 'p', 4))
		}
	}
	write(unchanged, cephmsgr.OSDSnapContext{}, data(0, 'u', 64))
	write(unchanged, cephmsgr.OSDSnapContext{}, data(0x9000, 'u', 16))
	write(dirty, cephmsgr.OSDSnapContext{}, data(0, 'd', 32))
	write(clean, cephmsgr.OSDSnapContext{}, data(0, 'd', 32))
	write(clean, cephmsgr.OSDSnapContext{}, undirty)
	s1, s2 := allocate("s1"), allocate("s2")
	if s1 >= s2 {
		t.Fatal("native snap ids must increase", s1, s2)
	}
	c1 := cephmsgr.OSDSnapContext{Sequence: s1, Snapshots: []uint64{s1}}
	c2 := cephmsgr.OSDSnapContext{Sequence: s2, Snapshots: []uint64{s2, s1}}
	// The split object is cloned twice: under c1 (clone s1 naming s1), then
	// under c2 (clone s2 naming only s2, because s1 is not above the
	// snapset seq any more).
	model("split@s1", split)
	write(split, c1, data(0, 'b', 16))
	write(split, c1, data(0x24000, 'b', 4))
	model("split@s2", split)
	write(split, c2, data(0x10000, 'c', 64))
	// Every other cloning write runs under c2: one clone naming s1 and s2.
	model("shared", shared)
	write(shared, c2, data(0, 'h', 16))
	write(shared, c2, data(0x10000, 'h', 64))
	write(shared, c2, data(0x24000, 'h', 4))
	model("deleted", deleted)
	write(deleted, c2, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete})
	model("dirty", dirty)
	write(dirty, c2, data(32, 'e', 8))
	write(dirty, cephmsgr.OSDSnapContext{}, undirty)
	model("clean", clean)
	write(clean, c2, data(32, 'e', 8))
	model("unchanged", unchanged)
	headAfter := map[string]poolSparseMap{}
	for _, name := range objects {
		headAfter[name] = poolSparseMapAt(t, ctx, name, "")
	}
	if headAfter[deleted].SparseMap.Code != -2 {
		t.Fatal("deleted head still observed natively", headAfter[deleted].SparseMap.Code)
	}

	observe := func(t *testing.T, label string, listed []string) copyPurgedTrimState {
		t.Helper()
		var result struct {
			SnapTrim copyPurgedTrimState `json:"snap_trim"`
		}
		nativePoolResult(t, ctx, "snaptrim", map[string]any{"slot": 1, "object": shared, "objects": listed}, &result)
		s := result.SnapTrim
		if s.PoolID != receipt.Pool || s.PGID != fmt.Sprintf("%d.0", receipt.Pool) || len(s.ListSnaps) != len(listed) {
			t.Fatalf("%s: native snaptrim observation is not the owned PG's: %+v", label, s)
		}
		receipt.States[label] = s
		return s
	}
	// listing checks the native clone lists against the model: before the
	// trim both snaps, after it s2 only, and split's clone s1 deleted.
	head := cephmsgr.OSDHeadSnapshot
	listing := func(t *testing.T, label string, trimmed bool) {
		t.Helper()
		s := observe(t, label, objects)
		both := []uint64{s1, s2}
		if trimmed {
			both = []uint64{s2}
		}
		want := map[string]map[uint64][]uint64{
			shared:    {s2: both, head: {}},
			split:     {s1: {s1}, s2: {s2}, head: {}},
			unchanged: {head: {}},
			deleted:   {s2: both},
			dirty:     {s2: both, head: {}},
			clean:     {s2: both, head: {}},
		}
		if trimmed {
			delete(want[split], s1)
		}
		receipt.Clones[label] = map[string]map[uint64][]uint64{}
		for _, name := range objects {
			got := snapLifecycleClones(t, s.ListSnaps[name].listing(t, name))
			receipt.Clones[label][name] = got
			if !reflect.DeepEqual(got, want[name]) {
				t.Fatalf("%s: native clones of %s are %v, want %v", label, name, got, want[name])
			}
		}
	}
	listing(t, "live-listed", false)

	sr := func(offset, length uint64) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDSparseRead, Offset: offset, Length: length, SparseRead: cephmsgr.OSDSparseReadInput{MaxOutputBytes: uint32(length) + 4096}}
	}
	rd := func(offset, length uint64) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Offset: offset, Length: length}
	}
	hint := func(op cephmsgr.OSDOperation, flags uint32) cephmsgr.OSDOperation {
		op.Flags = cephmsgr.OSDOperationFlags(flags)
		return op
	}
	const sequential, noCache = 0x8, 0x40
	vector := []cephmsgr.OSDOperation{sr(0, 0x10000), hint(rd(0x10000, 0x1000), noCache), hint(sr(0x11000, 0x20000), sequential), rd(8190, 10)}

	// compound reads object at snap and compares it with the model the snap
	// id is expected to resolve to; an empty key expects -ENOENT.
	compound := func(t *testing.T, phase, object string, snap uint64, key string) {
		t.Helper()
		native := poolObserveSnap(t, ctx, 1, object, "", "", snap)
		at := func(r *cephmsgr.OSDRequest) { r.ReadSnapshot = snap }
		reply, err := send(object, at, vector...)
		entry := cell{Phase: phase, Name: object, Snap: snap, Reply: reply, Native: native.Code, Version: native.Version}
		if err != nil {
			entry.Error = err.Error()
		}
		receipt.Cells = append(receipt.Cells, entry)
		if key == "" {
			var remote *cephmsgr.OSDError
			var unknown *cephmsgr.OutcomeUnknownError
			if native.Code != -2 || errors.As(err, &unknown) || !errors.As(err, &remote) || remote.Code != -2 || reply.Code != -2 || len(reply.RawData) != 0 || len(reply.Operations) != len(vector) || reply.Submission.Transaction == 0 {
				t.Fatalf("%s %s@%d: compound differs from the expected native -ENOENT (native %d): code %d err %v", phase, object, snap, native.Code, reply.Code, err)
			}
			for i, got := range reply.Operations {
				if got.Result != 0 || got.PayloadLength != 0 || got.Offset != vector[i].Offset || got.Length != vector[i].Length || got.SparseRead.Extents != nil {
					t.Fatalf("%s %s@%d: early aggregate error claimed an executed prefix at %d: %+v", phase, object, snap, i, got)
				}
			}
			t.Logf("%s %s@%d: -ENOENT, tid %d, native -2", phase, object, snap, reply.Transaction)
			return
		}
		m := receipt.Models[key]
		if !reflect.DeepEqual(native, m.State) {
			t.Fatalf("%s %s@%d: native state at the snap is not the %s head before its cloning write: %+v", phase, object, snap, key, native)
		}
		if err != nil || reply.Code != 0 || len(reply.Operations) != len(vector) {
			t.Fatalf("%s %s@%d: Go snapshot compound failed: code %d err %v", phase, object, snap, reply.Code, err)
		}
		if reply.UserVersion != m.State.Version || reply.Submission.Transaction == 0 || reply.Transaction != reply.Submission.Transaction || reply.Object != object {
			t.Fatalf("%s %s@%d: compound version/identity differs from the native model: %d, %d", phase, object, snap, reply.UserVersion, m.State.Version)
		}
		var raw []byte
		for i, op := range vector {
			got := reply.Operations[i]
			length, extents, want := sparseReadCompoundExpect(op, m.Full, m.State.Data)
			if got.Code != op.Code || got.Flags != uint32(op.Flags) || got.Result != 0 || got.Offset != op.Offset || got.Length != length || got.PayloadLength != uint32(len(got.Data)) {
				t.Fatalf("%s %s@%d: per-op %d union/result differs from the native model: %+v, length %d", phase, object, snap, i, got, length)
			}
			if op.Code == cephmsgr.OSDRead {
				if !bytes.Equal(got.Data, want) {
					t.Fatalf("%s %s@%d: READ %d bytes differ from the native model", phase, object, snap, i)
				}
			} else {
				if !reflect.DeepEqual(got.SparseRead.Extents, extents) || !bytes.Equal(got.SparseRead.Data, want) || uint32(len(got.Data)) > op.SparseRead.MaxOutputBytes {
					t.Fatalf("%s %s@%d: SPARSE_READ %d map/bytes differ from the native model: %v, want %v", phase, object, snap, i, got.SparseRead.Extents, extents)
				}
				alone, err := send(object, at, op)
				if err != nil || !reflect.DeepEqual(alone.Operations[0].SparseRead, got.SparseRead) || !bytes.Equal(alone.Operations[0].Data, got.Data) {
					t.Fatalf("%s %s@%d: compound SPARSE_READ %d differs from standalone: %v", phase, object, snap, i, err)
				}
			}
			raw = append(raw, got.Data...)
		}
		if !bytes.Equal(reply.RawData, raw) {
			t.Fatalf("%s %s@%d: per-op payloads do not exactly partition the raw reply data", phase, object, snap)
		}
		t.Logf("%s %s@%d: %s model, version %d, tid %d, raw %d bytes", phase, object, snap, key, m.State.Version, reply.Transaction, len(raw))
	}
	// isDirty sends ISDIRTY at snap (0 is the head); want nil expects -ENOENT.
	isDirty := func(t *testing.T, phase, object string, snap uint64, key string, want *bool) {
		t.Helper()
		native := poolObserveSnap(t, ctx, 1, object, "", "", snap)
		reply, err := send(object, func(r *cephmsgr.OSDRequest) { r.ReadSnapshot = snap }, cephmsgr.OSDOperation{Code: cephmsgr.OSDIsdirty})
		entry := cell{Phase: phase, Name: object + "/isdirty", Snap: snap, Reply: reply, Native: native.Code, Version: native.Version}
		if err != nil {
			entry.Error = err.Error()
		}
		receipt.Cells = append(receipt.Cells, entry)
		if want == nil {
			var remote *cephmsgr.OSDError
			if native.Code != -2 || !errors.As(err, &remote) || remote.Code != -2 || reply.Code != -2 || len(reply.Operations) != 1 || reply.Operations[0].Result != 0 || reply.Operations[0].PayloadLength != 0 {
				t.Fatalf("%s %s@%d: ISDIRTY differs from the expected native -ENOENT (native %d): code %d err %v", phase, object, snap, native.Code, reply.Code, err)
			}
			return
		}
		if snap != 0 && !reflect.DeepEqual(native, receipt.Models[key].State) {
			t.Fatalf("%s %s@%d: native state at the snap is not the %s head before its cloning write", phase, object, snap, key)
		}
		if err != nil || reply.Code != 0 || len(reply.Operations) != 1 || reply.Operations[0].Result != 0 || reply.Operations[0].PayloadLength != 1 || reply.UserVersion != native.Version {
			t.Fatalf("%s %s@%d: ISDIRTY reply differs: code %d uv %d native %d err %v", phase, object, snap, reply.Code, reply.UserVersion, native.Version, err)
		}
		if reply.Operations[0].Dirty != *want {
			t.Fatalf("%s %s@%d: ISDIRTY reports %v, want %v", phase, object, snap, reply.Operations[0].Dirty, *want)
		}
	}
	yes, no := true, false
	cells := func(t *testing.T, phase string, removed bool) {
		t.Helper()
		at1 := func(key string) string {
			if removed {
				return ""
			}
			return key
		}
		compound(t, phase, shared, s1, at1("shared"))
		compound(t, phase, shared, s2, "shared")
		compound(t, phase, split, s1, at1("split@s1"))
		compound(t, phase, split, s2, "split@s2")
		compound(t, phase, deleted, s1, at1("deleted"))
		compound(t, phase, deleted, s2, "deleted")
		// The unchanged object's snapset seq is below every snap id, so
		// even the removed s1 reads its head.
		compound(t, phase, unchanged, s1, "unchanged")
		compound(t, phase, unchanged, s2, "unchanged")
		if removed {
			isDirty(t, phase, dirty, s1, "dirty", nil)
		} else {
			isDirty(t, phase, dirty, s1, "dirty", &yes)
		}
		isDirty(t, phase, dirty, s2, "dirty", &yes)
		isDirty(t, phase, dirty, 0, "", &no)
		isDirty(t, phase, clean, s2, "clean", &no)
		isDirty(t, phase, clean, 0, "", &yes)
	}
	t.Run("live", func(t *testing.T) {
		cells(t, "live", false)
		t.Run("refused-shapes-not-sent", func(t *testing.T) {
			at := func(r *cephmsgr.OSDRequest) { r.ReadSnapshot = s2 }
			baseline := []cephmsgr.OSDOperation{sr(0, 8), rd(0, 8)}
			first, err := send(shared, at, baseline...)
			if err != nil {
				t.Fatal(err)
			}
			for name, tc := range map[string]struct {
				configure func(*cephmsgr.OSDRequest)
				ops       []cephmsgr.OSDOperation
			}{
				"snapcontext": {func(r *cephmsgr.OSDRequest) { at(r); r.SnapContext = c2 }, baseline},
				"mapext":      {at, []cephmsgr.OSDOperation{{Code: cephmsgr.OSDMapExt, Length: 8, MapExt: cephmsgr.OSDMapExtInput{MaxOutputBytes: 64}}, sr(0, 8)}},
				"isdirty":     {at, []cephmsgr.OSDOperation{sr(0, 8), {Code: cephmsgr.OSDIsdirty}}},
				"failok":      {at, []cephmsgr.OSDOperation{hint(sr(0, 8), uint32(cephmsgr.OSDFailOK)), rd(0, 8)}},
			} {
				reply, err := send(shared, tc.configure, tc.ops...)
				var server *cephmsgr.OSDError
				var unknown *cephmsgr.OutcomeUnknownError
				if err == nil || errors.As(err, &server) || errors.As(err, &unknown) || reply.Submission != (cephmsgr.OSDSubmission{}) || reply.Transaction != 0 {
					t.Fatal("unreviewed self-managed snapshot shape was transmitted", name, reply.Submission, err)
				}
			}
			next, err := send(shared, at, baseline...)
			if err != nil || next.Transaction != first.Transaction+1 || !bytes.Equal(next.RawData, first.RawData) {
				t.Fatal("a refusal consumed a TID or broke the connection", first.Transaction, next.Transaction, err)
			}
		})
	})
	if t.Failed() {
		return
	}

	// Pending: nosnaptrim keeps s1 in the PG's snap_trimq; paused MGRs keep
	// it queued after the trim.
	flagged := false
	t.Cleanup(func() {
		if flagged {
			restore, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			if err := copyMonCommand(restore, c, `{"prefix":"osd unset","key":"nosnaptrim"}`); err != nil {
				t.Error("osd unset nosnaptrim", err)
			}
		}
	})
	flagged = true
	if err := copyMonCommand(ctx, c, `{"prefix":"osd set","key":"nosnaptrim"}`); err != nil {
		t.Fatal("osd set nosnaptrim", err)
	}
	var dump struct {
		ActiveName string `json:"active_name"`
		Standbys   []struct {
			Name string `json:"name"`
		} `json:"standbys"`
	}
	raw, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"mgr dump","format":"json"}`)})
	if err != nil || json.Unmarshal(raw.Data, &dump) != nil || dump.ActiveName == "" {
		t.Fatal("mgr dump", err)
	}
	managers := []string{dump.ActiveName}
	for _, s := range dump.Standbys {
		managers = append(managers, s.Name)
	}
	// Each MGR is paused under its own lease; a lease's cleanup resumes it
	// after a failure.
	leases := map[string]*testcluster.DaemonLease{}
	resume := func(ctx context.Context) error {
		var errs []error
		for _, name := range managers {
			if lease := leases[name]; lease != nil {
				if _, err := resumeDaemon(ctx, lease); err != nil {
					errs = append(errs, err)
				} else {
					delete(leases, name)
				}
			}
		}
		return errors.Join(errs...)
	}
	for _, name := range managers {
		leases[name] = pauseDaemon(t, ctx, control, "mgr", name, leaseUntil(ctx))
	}
	receipt.Managers = managers
	receipt.Removal, err = c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: uint32(receipt.Pool), Op: cephmsgr.SelfManagedSnapshotRemove, Snapshot: s1})
	if err != nil || receipt.Removal.Code != 0 || receipt.Removal.Epoch == 0 {
		t.Fatal("remove self-managed snap", s1, receipt.Removal, err)
	}
	epoch = receipt.Removal.Epoch
	wait := func(t *testing.T, label string, passing []string, want string, bound time.Duration) copyPurgedTrimState {
		t.Helper()
		started := time.Now()
		for count := 1; ; count++ {
			s := observe(t, label, nil)
			receipt.Observations[label] = count
			phase := copyPurgedPhase(s, s1)
			if phase == want {
				t.Logf("%s after %s and %d native observations: %+v", label, time.Since(started).Round(time.Millisecond), count, s)
				return s
			}
			if !slices.Contains(passing, phase) {
				t.Fatalf("%s: unexpected trim phase %q: %+v", label, phase, s)
			}
			if time.Since(started) > bound {
				t.Fatalf("%s: the removed snapshot did not reach %s: %+v", label, want, s)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	// phase runs the cells in one trim phase and requires the same phase
	// after them, with the clone lists the phase implies.
	phase := func(name, want string, trimmed bool) {
		t.Run(name, func(t *testing.T) {
			cells(t, name, true)
			listing(t, name+"-listed", trimmed)
			if got := copyPurgedPhase(receipt.States[name+"-listed"], s1); got != want {
				t.Fatalf("the %s state did not hold through the cells: %q", want, got)
			}
		})
	}
	wait(t, "pending", []string{"queued"}, "pending", 60*time.Second)
	phase("pending", "pending", false)
	if t.Failed() {
		return
	}

	flagged = false
	if err := copyMonCommand(ctx, c, `{"prefix":"osd unset","key":"nosnaptrim"}`); err != nil {
		flagged = true
		t.Fatal("osd unset nosnaptrim", err)
	}
	wait(t, "trimmed", []string{"pending"}, "purged", 120*time.Second)
	phase("trimmed", "purged", true)
	if t.Failed() {
		return
	}

	if err := resume(ctx); err != nil {
		t.Fatal("resume the MGRs", err)
	}
	pruned := wait(t, "pruned", []string{"purged", "unqueued"}, "pruned", 180*time.Second)
	epoch = uint32(max(int64(epoch), pruned.OSDMapEpoch, pruned.Epoch))
	phase("pruned", "pruned", true)
	if t.Failed() {
		return
	}

	// A fresh session reads the same bytes at s2, and the native heads and
	// the clones at s2 are unchanged.
	first := receipt.Cells
	conn.Close()
	conn = mutationConnection(t, c, ctx, routes[shared])
	compound(t, "fresh-session", shared, s2, "shared")
	compound(t, "fresh-session", split, s2, "split@s2")
	if a, b := first[1].Reply.RawData, receipt.Cells[len(receipt.Cells)-2].Reply.RawData; !bytes.Equal(a, b) {
		t.Fatal("fresh-session read differs from the live read")
	}
	for _, name := range objects {
		if after := poolSparseMapAt(t, ctx, name, ""); !reflect.DeepEqual(after.SparseMap.State, headAfter[name].SparseMap.State) || !reflect.DeepEqual(after.SparseMap.Full, headAfter[name].SparseMap.Full) {
			t.Fatal("snapshot lifecycle reads changed a native head", name)
		}
	}
}
