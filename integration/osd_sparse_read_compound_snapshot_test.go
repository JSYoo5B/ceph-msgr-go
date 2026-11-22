package integration_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// poolSparseMap is the sparsemap mode of the owned-pool oracle
// (tools/osd_pool_mode_sparsemap_oracle.py): the pinned rados mapext full
// map of one owned object at the head or a pool snapshot, with native stat
// and bytes at that snapshot around it.
type poolSparseMap struct {
	SparseMap struct {
		Pool      string               `json:"pool"`
		Object    string               `json:"object"`
		Snap      string               `json:"snap"`
		SnapID    uint64               `json:"snap_id"`
		Code      int32                `json:"code"`
		Full      []cephmsgr.OSDExtent `json:"native_full_extents"`
		State     nativeXattrSnapshot  `json:"state"`
		Unchanged bool                 `json:"unchanged"`
	} `json:"sparse_map"`
}

func poolSparseMapAt(t *testing.T, ctx context.Context, object, snap string) poolSparseMap {
	t.Helper()
	var result poolSparseMap
	nativePoolResult(t, ctx, "sparsemap", map[string]any{"slot": 1, "object": object, "snap": snap}, &result)
	m := result.SparseMap
	if m.Object != object || m.Snap != snap || !m.Unchanged || m.Code != 0 && m.Code != -2 || int(m.Code) != m.State.Code {
		t.Fatal("native snapshot allocation map identity differs", m.Object, m.Snap, m.Code, m.State.Code)
	}
	return result
}

// TestCephOSDSparseReadCompoundSnapshotIntegration sends the SPARSE_READ and
// READ compound at a pool snapshot, as librbd sends its object read at the
// image's read snap (ceph/ceph/blob/v20.2.4/src/librbd/io/ObjectRequest.cc
// ObjectReadRequest::read_object). An owned replicated pool gets one native
// pool snapshot; Go writes before and after it leave four objects: a clone
// with holes whose head was rewritten and extended, an object unchanged since
// the snapshot, one created after it and one deleted after it.
//
// Each case compares every per-op union, result, extent map and byte with the
// intersection of the pinned rados mapext full map at the same snapshot with
// the operation's range, plus native bytes, stat and user version at that
// snapshot (the native client does not execute the compound). Every
// SPARSE_READ must equal the same operation sent alone at the snapshot. The
// pinned find_object_context resolves the snap id: the clone for the
// rewritten and the deleted object, so the clone's size, not the head's,
// clips the range; the head for the unchanged object and for a snap id above
// every allocated one; and -ENOENT before any operation runs for the object
// created after the snapshot, which leaves every per-op result zero. Native
// head and snapshot state, bytes and maps are unchanged afterwards. Refused
// shapes at the snapshot (SnapDir, a SnapContext, FAILOK, and
// STAT, MAPEXT, ISDIRTY or UNDIRTY in the vector) are not sent and leave the
// transaction IDs consecutive.
//
// The same compound with a replica-read flag, at the head and at a snapshot,
// is TestCephOSDSparseReadCompoundReplicaReadIntegration's. Not verified:
// self-managed snapshots, removed or trimmed snapshots, EC, mid-vector daemon
// errors, faults and resubmission.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-replicated-pg-reply-v8.t-osdsparsereadcompoundsnapshot key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=replicated-pg-reply-v8
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-replicated-pg-request-v9-reply-v8.t-osdsparsereadcompoundsnapshot key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=replicated-pg-request-v9-reply-v8
//scenario:covers CLIENT to OSD
//fixture:shareable pool snapshot, writes and reads in one owned pool only; checked owned-pool cleanup removes it
func TestCephOSDSparseReadCompoundSnapshotIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	if base.PoolType == 3 {
		t.Skip("SPARSE_READ compound acceptance requires a replicated pool; EC is outside this subset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	const (
		clone, unchanged, created, deleted = "compound-snap-clone", "compound-snap-unchanged", "compound-snap-created", "compound-snap-deleted"
		snapName                           = "compound-snap"
	)
	routes := map[string]osdFixtureOracle{}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	place := func() {
		for _, name := range []string{clone, unchanged, created, deleted} {
			route := poolPlace(t, ctx, 1, name, "")
			if route.PoolType != 1 || route.Shard != -1 || route.ID != route.PrimaryID {
				t.Fatal("snapshot compound target is not an explicit replicated primary", route)
			}
			routes[name] = route
			if connections[route.ID] == nil {
				connections[route.ID] = mutationConnection(t, c, ctx, route)
			}
		}
	}
	place()
	send := func(name string, configure func(*cephmsgr.OSDRequest), ops ...cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
		r := pools.request(routes[name], ops...)
		if configure != nil {
			configure(&r)
		}
		return connections[routes[name].ID].Request(ctx, r)
	}
	tick := uint32(0)
	write := func(name string, op cephmsgr.OSDOperation) {
		t.Helper()
		tick++
		reply, err := send(name, func(r *cephmsgr.OSDRequest) {
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700009000 + tick, Nanoseconds: tick}
		}, op)
		if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("owned snapshot compound write", name, reply.Code, err)
		}
	}
	data := func(offset uint64, label byte, n int) cephmsgr.OSDOperation {
		b := bytes.Repeat([]byte{label, 0, 255, byte(offset >> 12)}, (n+3)/4)[:n]
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: offset, Length: uint64(n), Data: b}
	}
	// Before the snapshot: holes between three allocated ranges on the clone
	// object and the deleted object, a dense head and one range on the
	// unchanged object.
	for _, name := range []string{clone, deleted} {
		for _, offset := range []uint64{8192, 0x11000, 0x20000} {
			write(name, data(offset, 'p', 4))
		}
	}
	write(unchanged, data(0, 'u', 64))
	write(unchanged, data(0x9000, 'u', 16))
	snap := nativePool(t, ctx, "snapshot", map[string]any{"slot": 1, "name": snapName, "action": "create"})
	if snap.SnapID == 0 {
		t.Fatal("native pool snapshot id missing")
	}
	// Routes read after the snapshot carry a map epoch that includes it, so
	// the OSD applies the pool's new SnapContext before it executes a write.
	place()
	// After it: the pool's SnapContext clones the rewritten and the deleted
	// object; the rewrite fills holes and extends the head past the clone.
	write(clone, data(0, 'h', 16))
	write(clone, data(0x10000, 'h', 64))
	write(clone, data(0x24000, 'h', 4))
	write(created, data(0, 'c', 32))
	write(deleted, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete})

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
	const (
		random, sequential, willNeed, dontNeed, noCache = 0x4, 0x8, 0x10, 0x20, 0x40
	)
	above := snap.SnapID + 1000
	cases := []struct {
		name, object string
		snap         uint64
		// observe names the native observation the snap id resolves to: the
		// pool snapshot, or the head for an id above every allocated one.
		observe string
		ops     []cephmsgr.OSDOperation
	}{
		{"clone-librbd-mixed", clone, snap.SnapID, snapName, []cephmsgr.OSDOperation{sr(0, 0x10000), rd(0x10000, 0x1000), hint(sr(0x11000, 0x20000), sequential)}},
		{"clone-fadvise-each", clone, snap.SnapID, snapName, []cephmsgr.OSDOperation{hint(sr(0, 0x9000), random), hint(rd(4096, 4096), noCache), hint(sr(0x10ffe, 8), dontNeed), hint(rd(0x20000, 16), willNeed)}},
		{"clone-size-clips", clone, snap.SnapID, snapName, []cephmsgr.OSDOperation{sr(0x20000, 0x10000), rd(0x20002, 100), sr(0x24000, 16), rd(0x24000, 4)}},
		{"unchanged-resolves-head", unchanged, snap.SnapID, snapName, []cephmsgr.OSDOperation{sr(0, 0x10000), rd(0, 16), sr(0x8ffe, 8)}},
		{"deleted-head-clone", deleted, snap.SnapID, snapName, []cephmsgr.OSDOperation{sr(0, 0x10000), rd(8190, 10), sr(0x11000, 0x20000)}},
		{"above-seq-resolves-head", clone, above, "", []cephmsgr.OSDOperation{sr(0, 0x10000), rd(0x10000, 0x40), sr(0x20000, 0x10000)}},
	}
	headBefore := map[string]poolSparseMap{}
	for _, name := range []string{clone, unchanged, created, deleted} {
		headBefore[name] = poolSparseMapAt(t, ctx, name, "")
	}
	// The clone differs from the head: its map and bytes are older, so a
	// read that ignored the snap id would fail every clone case below.
	cloneMap := poolSparseMapAt(t, ctx, clone, snapName)
	if cloneMap.SparseMap.Code != 0 || reflect.DeepEqual(cloneMap.SparseMap.Full, headBefore[clone].SparseMap.Full) || bytes.Equal(cloneMap.SparseMap.State.Data, headBefore[clone].SparseMap.State.Data) || cloneMap.SparseMap.SnapID != snap.SnapID {
		t.Fatal("native clone is not distinct from the rewritten head", cloneMap.SparseMap.Full, headBefore[clone].SparseMap.Full)
	}
	if headBefore[deleted].SparseMap.Code != -2 {
		t.Fatal("deleted head still observed natively", headBefore[deleted].SparseMap.Code)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			native := poolSparseMapAt(t, ctx, tc.object, tc.observe).SparseMap
			if native.Code != 0 || len(native.Full) == 0 {
				t.Fatal("independent native allocation map at the snapshot missing", native.Code)
			}
			at := func(r *cephmsgr.OSDRequest) { r.ReadSnapshot = tc.snap }
			reply, err := send(tc.object, at, tc.ops...)
			if err != nil || reply.Code != 0 || len(reply.Operations) != len(tc.ops) {
				t.Fatal("Go snapshot SPARSE_READ compound failed", reply.Code, err)
			}
			if reply.UserVersion != native.State.Version || reply.Submission.Transaction == 0 || reply.Transaction != reply.Submission.Transaction || reply.Object != tc.object {
				t.Fatal("compound version/identity differs from independent native stat at the snapshot", reply.UserVersion, native.State.Version)
			}
			var raw []byte
			for i, op := range tc.ops {
				got := reply.Operations[i]
				length, extents, want := sparseReadCompoundExpect(op, native.Full, native.State.Data)
				if got.Code != op.Code || got.Flags != uint32(op.Flags) || got.Result != 0 || got.Offset != op.Offset || got.Length != length || got.PayloadLength != uint32(len(got.Data)) {
					t.Fatal("per-op union/result differs from the native snapshot model", i, got.Code, got.Length, length, got.Result)
				}
				if op.Code == cephmsgr.OSDRead {
					if !bytes.Equal(got.Data, want) {
						t.Fatal("READ bytes differ from native bytes at the snapshot", i)
					}
				} else {
					if !reflect.DeepEqual(got.SparseRead.Extents, extents) || !bytes.Equal(got.SparseRead.Data, want) || uint32(len(got.Data)) > op.SparseRead.MaxOutputBytes {
						t.Fatal("SPARSE_READ map/bytes differ from the native map and bytes at the snapshot", i, got.SparseRead.Extents, extents)
					}
					alone, err := send(tc.object, at, op)
					if err != nil || !reflect.DeepEqual(alone.Operations[0].SparseRead, got.SparseRead) || !bytes.Equal(alone.Operations[0].Data, got.Data) {
						t.Fatal("compound SPARSE_READ differs from standalone at the snapshot", i, err)
					}
				}
				raw = append(raw, got.Data...)
			}
			if !bytes.Equal(reply.RawData, raw) {
				t.Fatal("per-op payloads do not exactly partition the raw reply data")
			}
			after := poolSparseMapAt(t, ctx, tc.object, tc.observe).SparseMap
			if !reflect.DeepEqual(after.State, native.State) || !reflect.DeepEqual(after.Full, native.Full) {
				t.Fatal("Go snapshot compound altered bytes/mtime/version/allocation")
			}
			t.Logf("%s: snap %d, %d ops, native map %v, version %d, raw=%d bytes; scope=native full-object mapext and read at the resolved snapshot, per-op intersection model", tc.name, tc.snap, len(tc.ops), native.Full, native.State.Version, len(raw))
		})
	}
	t.Run("created-after-snapshot-ambiguous", func(t *testing.T) {
		native := poolSparseMapAt(t, ctx, created, snapName).SparseMap
		if native.Code != -2 || headBefore[created].SparseMap.Code != 0 {
			t.Fatal("independent native absence at the snapshot not observed", native.Code)
		}
		ops := []cephmsgr.OSDOperation{sr(0, 8), rd(0, 8), sr(8, 8)}
		reply, err := send(created, func(r *cephmsgr.OSDRequest) { r.ReadSnapshot = snap.SnapID }, ops...)
		var remote *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		if errors.As(err, &unknown) || !errors.As(err, &remote) || remote.Code != -2 || reply.Code != -2 || len(reply.RawData) != 0 || len(reply.Operations) != len(ops) {
			t.Fatal("snapshot-absent compound differs from native ENOENT", reply.Code, err)
		}
		for i, got := range reply.Operations {
			if got.Result != 0 || got.PayloadLength != 0 || got.Offset != ops[i].Offset || got.Length != ops[i].Length || got.SparseRead.Extents != nil {
				t.Fatal("early aggregate error claimed an executed prefix", i, got)
			}
		}
		head, err := send(created, nil, ops...)
		if err != nil || head.Code != 0 {
			t.Fatal("signed snapshot error poisoned the session or the head read", head.Code, err)
		}
	})
	t.Run("refused-shapes-not-sent", func(t *testing.T) {
		at := func(r *cephmsgr.OSDRequest) { r.ReadSnapshot = snap.SnapID }
		baseline := []cephmsgr.OSDOperation{sr(0, 8), rd(0, 8)}
		first, err := send(clone, at, baseline...)
		if err != nil {
			t.Fatal(err)
		}
		for name, tc := range map[string]struct {
			configure func(*cephmsgr.OSDRequest)
			ops       []cephmsgr.OSDOperation
		}{
			"snapdir": {func(r *cephmsgr.OSDRequest) { r.ReadSnapshot = ^uint64(0) }, baseline},
			"snapcontext": {func(r *cephmsgr.OSDRequest) {
				at(r)
				r.SnapContext = cephmsgr.OSDSnapContext{Sequence: snap.SnapID, Snapshots: []uint64{snap.SnapID}}
			}, baseline},
			"failok":  {at, []cephmsgr.OSDOperation{hint(sr(0, 8), uint32(cephmsgr.OSDFailOK)), rd(0, 8)}},
			"stat":    {at, []cephmsgr.OSDOperation{sr(0, 8), {Code: cephmsgr.OSDStat}}},
			"mapext":  {at, []cephmsgr.OSDOperation{{Code: cephmsgr.OSDMapExt, Length: 8, MapExt: cephmsgr.OSDMapExtInput{MaxOutputBytes: 64}}, sr(0, 8)}},
			"isdirty": {at, []cephmsgr.OSDOperation{sr(0, 8), {Code: cephmsgr.OSDIsdirty}}},
			"undirty": {nil, []cephmsgr.OSDOperation{sr(0, 8), {Code: cephmsgr.OSDUndirty}}},
		} {
			reply, err := send(clone, tc.configure, tc.ops...)
			var server *cephmsgr.OSDError
			var unknown *cephmsgr.OutcomeUnknownError
			if err == nil || errors.As(err, &server) || errors.As(err, &unknown) || reply.Submission != (cephmsgr.OSDSubmission{}) || reply.Transaction != 0 {
				t.Fatal("unreviewed snapshot compound was transmitted", name, reply.Submission, err)
			}
		}
		next, err := send(clone, at, baseline...)
		if err != nil || next.Transaction != first.Transaction+1 || !bytes.Equal(next.RawData, first.RawData) {
			t.Fatal("a refusal consumed a TID or broke the connection", first.Transaction, next.Transaction, err)
		}
	})
	for _, name := range []string{clone, unchanged, created, deleted} {
		if after := poolSparseMapAt(t, ctx, name, ""); !reflect.DeepEqual(after.SparseMap.State, headBefore[name].SparseMap.State) || !reflect.DeepEqual(after.SparseMap.Full, headBefore[name].SparseMap.Full) {
			t.Fatal("snapshot reads changed a native head", name)
		}
	}
	if after := poolSparseMapAt(t, ctx, clone, snapName); !reflect.DeepEqual(after.SparseMap, cloneMap.SparseMap) {
		t.Fatal("snapshot reads changed the native clone")
	}
}
