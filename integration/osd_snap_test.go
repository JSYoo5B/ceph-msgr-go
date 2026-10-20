package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// TestCephOSDSnapshotIntegration compares Go snapshot reads, SnapContext
// writes, ORDERSNAP, LIST_SNAPS and ROLLBACK with native librados on owned
// objects. Snap ids are allocated natively (a MON pool operation outside the
// product) and the shared fixture pool enters self-managed snapshot mode.
func TestCephOSDSnapshotIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 420*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	v1, v2 := []byte{0, 'v', '1', 0xff, 's', 'n', 'a', 'p'}, []byte("v2-longer\x00tail-of-head")
	attr, key := []byte("snap-attr"), []byte("snap-key")
	keys := [][]byte{key}
	seed := &omapWriteSeed{Baseline: v1, Attributes: []nativeXattrEntry{{Name: attr, Value: []byte("a1")}}, Entries: []omapNativeEntry{{Key: key, Value: []byte("o1")}}}
	type step struct {
		Name   string             `json:"name"`
		Go     *cephmsgr.OSDReply `json:"go_reply,omitempty"`
		Native any                `json:"native,omitempty"`
	}
	var evidence []step
	record := func(name string, reply *cephmsgr.OSDReply, native any) {
		evidence = append(evidence, step{name, reply, native})
	}
	defer func() {
		data, err := json.Marshal(evidence)
		if err == nil && len(data) <= 512<<10 {
			err = os.WriteFile(filepath.Join(dir, "osd-snap-results.json"), data, 0644)
		}
		if err != nil {
			t.Error("bounded snapshot evidence", err)
		}
	}()
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanupSnapOracle(t, base)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: seed, Reserve: 5})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 6 || !bytes.Equal(owned.State.Data, v1) {
		t.Fatal("exclusive native snapshot baseline differs")
	}
	// G is the Go object and T its native twin; A/AN are absent objects for
	// Go/native rollback; B/BN are created under a SnapContext by Go/native.
	g, twin, absent, absentNative, b, bNative := owned.Objects[0], owned.Objects[1], owned.Objects[2], owned.Objects[3], owned.Objects[4], owned.Objects[5]
	snaps := nativeSnapCreate(t, ctx, base, 2)
	s1, s2 := snaps[0], snaps[1]
	above := s2 + 1000 // never allocated by this test; above every snapset seq
	c1 := cephmsgr.OSDSnapContext{Sequence: s1, Snapshots: []uint64{s1}}
	c2 := cephmsgr.OSDSnapContext{Sequence: s2, Snapshots: []uint64{s2, s1}}
	native1 := snapOracleContext{Seq: s1, Snaps: []uint64{s1}}
	native2 := snapOracleContext{Seq: s2, Snaps: []uint64{s2, s1}}
	t.Logf("allocated self-managed snaps %d and %d", s1, s2)

	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	tick := uint32(0)
	send := func(name string, object string, exists bool, edit func(*cephmsgr.OSDRequest), ops ...cephmsgr.OSDOperation) (cephmsgr.OSDRequest, cephmsgr.OSDReply, error) {
		t.Helper()
		routing := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": object, "namespace": ""})
		selected, want := routing.Placement, int32(-2)
		if exists {
			want = 0
		}
		if routing.Actual.Code != want || selected.Name != object || selected.Pool != base.Pool || selected.ID < 0 || selected.Address == "" {
			t.Fatal("caller-owned native placement/existence differs", name, routing.Actual.Code)
		}
		if connections[selected.ID] == nil {
			connections[selected.ID] = mutationConnection(t, c, ctx, selected)
		}
		request := osdObjectRequest(selected, ops...)
		if edit != nil {
			edit(&request)
		}
		reply, err := connections[selected.ID].Request(ctx, request)
		record(name, &reply, nil)
		return request, reply, err
	}
	mutate := func(snapc cephmsgr.OSDSnapContext, mtime bool) func(*cephmsgr.OSDRequest) {
		return func(r *cephmsgr.OSDRequest) {
			r.SnapContext = snapc
			if mtime {
				tick++
				r.ModificationTime = cephmsgr.OSDTime{Seconds: 1760100000 + tick, Nanoseconds: tick}
			}
		}
	}
	committed := func(name string, reply cephmsgr.OSDReply, err error) {
		t.Helper()
		if err != nil || reply.Code != 0 || reply.Flags&uint64(cephmsgr.OSDReplyOnDisk) == 0 {
			t.Fatalf("%s: Go snapshot mutation not committed: %v code=%d flags=%#x", name, err, reply.Code, reply.Flags)
		}
	}
	refused := func(name string, reply cephmsgr.OSDReply, err error, code int32) {
		t.Helper()
		var server *cephmsgr.OSDError
		if !errors.As(err, &server) || server.Code != code || reply.Code != code {
			t.Fatalf("%s: Go result differs: %v code=%d, want %d", name, err, reply.Code, code)
		}
	}
	nativeWrite := func(name, object string, snapc snapOracleContext, ordered bool, want int32, ops ...snapOracleAction) snapOracleReceipt {
		t.Helper()
		tick++
		result := nativeSnapWrite(t, ctx, base, object, keys, snapc, ordered, omapWriteMTime{Seconds: 1760100000 + tick, Nanos: tick}, ops...)
		record(name, nil, result)
		if result.SnapContext == nil || result.SnapContext.Code != 0 || result.Aggregate == nil || *result.Aggregate != want {
			t.Fatalf("%s: native snapshot write differs: snapc=%+v aggregate=%v", name, result.SnapContext, result.Aggregate)
		}
		return result
	}
	nativeRead := func(name, object string, snap uint64) snapOracleRead {
		t.Helper()
		read := nativeSnapRead(t, ctx, base, object, snap, keys)
		record(name, nil, read)
		return read
	}
	readOps := []cephmsgr.OSDOperation{{Code: cephmsgr.OSDStat}, {Code: cephmsgr.OSDRead, Length: 4096},
		{Code: cephmsgr.OSDGetXattrs, Xattr: cephmsgr.OSDXattrInput{MaxOutputBytes: 4096}},
		{Code: cephmsgr.OSDGetOmapValues, Omap: cephmsgr.OSDOmapInput{MaxEntries: 16, MaxOutputBytes: 4096}}}
	// goRead reads object at snap with Go and requires the same bytes, stat,
	// xattrs, OMAP and user version as the native read at that snap. A
	// whiteout head is absent to native placement but its clones remain.
	whiteouts := map[string]bool{}
	goRead := func(name, object string, snap uint64, data []byte, attrValue, omapValue string) {
		t.Helper()
		_, reply, err := send(name, object, !whiteouts[object], func(r *cephmsgr.OSDRequest) { r.ReadSnapshot = snap }, readOps...)
		if err != nil || reply.Code != 0 {
			t.Fatalf("%s: Go snapshot read failed: %v", name, err)
		}
		nativeSnap := snap
		if snap == 0 {
			nativeSnap = snapOracleHead
		}
		read := nativeRead(name+" native", object, nativeSnap)
		stat, ops := read.Operations[0], reply.Operations
		goXattrs := xattrEncodedInventory(t, ops[2].Data)
		nativeXattrs := map[string]string{}
		for name, value := range read.Xattrs() {
			nativeXattrs[name] = string(value)
		}
		goOmap, more := decodeOmapRead(t, cephmsgr.OSDGetOmapValues, ops[3].Data)
		nativeOmap := read.Omap(t)
		if read.Aggregate != 0 || !bytes.Equal(ops[1].Data, data) || !bytes.Equal(read.Operations[1].Data, data) ||
			binary.LittleEndian.Uint64(ops[0].Data) != *stat.Size || int64(binary.LittleEndian.Uint32(ops[0].Data[8:])) != *stat.Seconds || int64(binary.LittleEndian.Uint32(ops[0].Data[12:])) != *stat.Nanos ||
			!reflect.DeepEqual(goXattrs, nativeXattrs) || goXattrs[string(attr)] != attrValue ||
			more == nil || *more || len(goOmap) != len(nativeOmap) || len(goOmap) != 1 || string(goOmap[0].Key) != string(key) || string(goOmap[0].Value) != omapValue || !bytes.Equal(nativeOmap[string(key)], goOmap[0].Value) ||
			reply.UserVersion != read.Version || read.ObjectInfo == nil || read.ObjectInfo.UserVersion != reply.UserVersion {
			t.Fatalf("%s: Go snapshot read differs from native: data=%q native=%q xattrs=%v/%v omap=%v version=%d/%d", name, ops[1].Data, read.Operations[1].Data, goXattrs, nativeXattrs, goOmap, reply.UserVersion, read.Version)
		}
		t.Logf("%s: snap=%#x data=%q size=%d mtime=%d.%09d user_version=%d object_info{oid.snapid=%d version=%s}", name, snap, ops[1].Data, *stat.Size, *stat.Seconds, *stat.Nanos, reply.UserVersion, read.ObjectInfo.OID.Snap, read.ObjectInfo.Version)
	}
	// sameListing compares a Go listing with the native CLI listing of the
	// same object, and with the native CLI listing of its twin.
	sameListing := func(name string, got cephmsgr.OSDSnapshotList, natives ...snapToolResult) {
		t.Helper()
		for _, result := range natives {
			listing := result.listing(t)
			if listing.Seq != int64(got.Sequence) || len(listing.Clones) != len(got.Clones) {
				t.Fatalf("%s: listing differs from native: Go %+v native %s", name, got, result.JSON)
			}
			for i, clone := range got.Clones {
				n := listing.Clones[i]
				if n.ID.Head != (clone.ID == cephmsgr.OSDHeadSnapshot) || !n.ID.Head && n.ID.ID != clone.ID || n.Size != clone.Size || len(n.Snapshots) != len(clone.Snapshots) || len(n.Overlaps) != len(clone.Overlap) {
					t.Fatalf("%s: clone %d differs from native: Go %+v native %s", name, i, clone, result.JSON)
				}
				for j, snap := range clone.Snapshots {
					if n.Snapshots[j].ID != snap {
						t.Fatalf("%s: clone %d snaps differ: Go %v native %s", name, i, clone.Snapshots, result.JSON)
					}
				}
				for j, overlap := range clone.Overlap {
					if n.Overlaps[j].Start != overlap.Offset || n.Overlaps[j].Length != overlap.Length {
						t.Fatalf("%s: clone %d overlap differs: Go %v native %s", name, i, clone.Overlap, result.JSON)
					}
				}
			}
		}
	}
	goList := func(name, object string, exists bool) cephmsgr.OSDReply {
		t.Helper()
		_, reply, err := send(name, object, exists, nil, cephmsgr.OSDOperation{Code: cephmsgr.OSDListSnapshots, ListSnapshots: cephmsgr.OSDListSnapshotsInput{MaxOutputBytes: 4096}})
		if err != nil {
			t.Fatalf("%s: Go snapshot listing failed: %v", name, err)
		}
		return reply
	}
	nativeList := func(name, object string) snapToolResult {
		t.Helper()
		result := nativeListSnaps(t, ctx, base, object)
		record(name, nil, result)
		return result
	}
	absentListing := func(name string, result snapToolResult) {
		t.Helper()
		if result.Exit == 0 || result.Code == nil || *result.Code != -2 {
			t.Fatalf("%s: native listing of an absent object differs: exit=%d %s", name, result.Exit, result.Stderr)
		}
	}

	t.Run("listing an absent object is ENOENT", func(t *testing.T) {
		_, reply, err := send("go list absent", absent, false, nil, cephmsgr.OSDOperation{Code: cephmsgr.OSDListSnapshots, ListSnapshots: cephmsgr.OSDListSnapshotsInput{MaxOutputBytes: 4096}})
		refused("list absent", reply, err, -2)
		absentListing("native list absent", nativeList("native list absent", absent))
	})
	if t.Failed() {
		return
	}

	v2Ops := []cephmsgr.OSDOperation{{Code: cephmsgr.OSDWriteFull, Length: uint64(len(v2)), Data: v2},
		{Code: cephmsgr.OSDSetXattr, Data: []byte("a2"), Xattr: cephmsgr.OSDXattrInput{Name: string(attr)}},
		{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: string(key), Value: []byte("o2")}}}}}
	nativeV2 := []snapOracleAction{snapWriteFull(v2), snapSetXattr(attr, []byte("a2")), snapOmapSet(omapNativeEntry{Key: key, Value: []byte("o2")})}
	v3 := append(append(append([]byte{}, v2[:2]...), 'X', 'Y'), v2[4:]...)
	t.Run("SnapContext writes clone the head like native", func(t *testing.T) {
		_, reply, err := send("go v2 under {S1,[S1]}", g, true, mutate(c1, true), v2Ops...)
		committed("v2 under {S1,[S1]}", reply, err)
		head := nativeRead("native head after v2", g, snapOracleHead)
		if reply.UserVersion != head.Version || !bytes.Equal(head.Operations[1].Data, v2) {
			t.Fatal("Go SnapContext write version/data differs from the native head", reply.UserVersion, head.Version)
		}
		clone := nativeRead("native S1 after Go v2", g, s1)
		if clone.Aggregate != 0 || !bytes.Equal(clone.Operations[1].Data, v1) || clone.ObjectInfo == nil || clone.ObjectInfo.OID.Snap != int64(s1) || clone.ObjectInfo.UserVersion != owned.State.Version {
			t.Fatal("Go SnapContext write did not clone the v1 head at S1", clone.Aggregate, clone.ObjectInfo)
		}
		_, reply, err = send("go partial write under {S2,[S2,S1]}", g, true, mutate(c2, true), cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: 2, Length: 2, Data: []byte("XY")})
		committed("partial write under {S2,[S2,S1]}", reply, err)
		nativeWrite("native twin v1", twin, snapOracleContext{}, false, 0, snapWriteFull(v1), snapSetXattr(attr, []byte("a1")), snapOmapSet(omapNativeEntry{Key: key, Value: []byte("o1")}))
		nativeWrite("native twin v2 under {S1,[S1]}", twin, native1, false, 0, nativeV2...)
		nativeWrite("native twin partial write under {S2,[S2,S1]}", twin, native2, false, 0, snapWrite(2, []byte("XY")))
		t.Logf("Go clone writes: v2 user_version=%d, clone S1 user_version=%d", head.Version, clone.ObjectInfo.UserVersion)
	})
	if t.Failed() {
		return
	}

	t.Run("reads at snapshots match native", func(t *testing.T) {
		goRead("read at S1", g, s1, v1, "a1", "o1")
		goRead("read at S2", g, s2, v2, "a2", "o2")
		goRead("read at head", g, 0, v3, "a2", "o2")
		goRead("read above every snapset seq", g, above, v3, "a2", "o2")
	})
	if t.Failed() {
		return
	}

	t.Run("listing matches native CLI, dencoder and twin", func(t *testing.T) {
		reply := goList("go list two clones", g, true)
		got, raw := reply.Operations[0].Snapshots, reply.Operations[0].Data
		if got.Sequence != s2 || len(got.Clones) != 3 || got.Clones[0].ID != s1 || got.Clones[1].ID != s2 || got.Clones[2].ID != cephmsgr.OSDHeadSnapshot ||
			!reflect.DeepEqual(got.Clones[1].Snapshots, []uint64{s2}) || got.Clones[2].Size != uint64(len(v3)) {
			t.Fatalf("Go listing of two clones differs: %+v", got)
		}
		sameListing("two clones", got, nativeList("native list two clones", g), nativeList("native twin list two clones", twin))
		decoded := nativeDecodeSnapList(t, ctx, base, raw)
		record("dencoder decode of Go raw listing", nil, decoded)
		d := decoded.dencoderListing(t)
		if d.Seq != got.Sequence || len(d.Clones) != len(got.Clones) {
			t.Fatalf("ceph-dencoder listing of Go raw bytes differs: %s", decoded.JSON)
		}
		for i, clone := range got.Clones {
			var overlaps [][2]uint64
			for _, o := range clone.Overlap {
				overlaps = append(overlaps, [2]uint64{o.Offset, o.Length})
			}
			n := d.Clones[i]
			if n.Head != (clone.ID == cephmsgr.OSDHeadSnapshot) || !n.Head && n.ID != clone.ID || n.Size != clone.Size || !reflect.DeepEqual(n.Overlaps, overlaps) || len(n.Snapshots) != len(clone.Snapshots) || len(clone.Snapshots) != 0 && !reflect.DeepEqual(n.Snapshots, clone.Snapshots) {
				t.Fatalf("ceph-dencoder clone %d differs from Go: %+v %s", i, clone, decoded.JSON)
			}
		}
		t.Logf("Go listing %+v; ceph-dencoder %s", got, decoded.JSON)
	})
	if t.Failed() {
		return
	}

	t.Run("ORDERSNAP refuses an older SnapContext like native", func(t *testing.T) {
		before := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: g, InventoryKeys: keys}).State
		_, reply, err := send("go ordered stale write", g, true, func(r *cephmsgr.OSDRequest) {
			mutate(c1, true)(r)
			r.OrderSnapshots = true
		}, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 5, Data: []byte("stale")})
		refused("ordered stale write", reply, err, cephmsgr.OSDOldSnapContext)
		after := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: g, InventoryKeys: keys}).State
		if !reflect.DeepEqual(before, after) || reply.UserVersion != 0 {
			t.Fatal("refused ORDERSNAP write changed the object or reported a version")
		}
		order := nativeWrite("native twin ordered stale write", twin, native1, true, -85, snapWriteFull([]byte("stale")))
		if order.OperateFlags != 1024|512 || !reflect.DeepEqual(order.Before, order.After) {
			t.Fatal("native ORDERSNAP refusal changed the twin or lost its flag")
		}
	})
	if t.Failed() {
		return
	}

	t.Run("rollback restores a clone like native", func(t *testing.T) {
		request, reply, err := send("go rollback to S1", g, true, mutate(c2, true), cephmsgr.OSDOperation{Code: cephmsgr.OSDRollback, Snapshot: s1})
		committed("rollback to S1", reply, err)
		if reply.Operations[0].Snapshot != s1 || len(reply.Operations[0].Data) != 0 {
			t.Fatal("Go rollback echo differs", reply.Operations[0])
		}
		native := nativeSnapRollback(t, ctx, base, twin, keys, s1, native2)
		record("native twin rollback to S1", nil, native)
		if native.Rollback == nil || native.Rollback.Code == nil || *native.Rollback.Code != 0 || !bytes.Equal(native.After.Data, v1) {
			t.Fatal("native twin rollback differs", native.Rollback)
		}
		goRead("read head after rollback", g, 0, v1, "a1", "o1")
		head := nativeRead("native head after Go rollback", g, snapOracleHead)
		if *head.Operations[0].Seconds != int64(request.ModificationTime.Seconds) || *head.Operations[0].Nanos != int64(request.ModificationTime.Nanoseconds) || head.Version != reply.UserVersion {
			t.Fatal("Go rollback did not apply its explicit mtime/version", head.Operations[0], head.Version)
		}
		if !reflect.DeepEqual(xattrInventory(native.After.nativeXattrSnapshot), map[string]string{string(attr): "a1"}) || !equalOmapModel(omapWriteInventory(t, native.After), map[string][]byte{string(key): []byte("o1")}) {
			t.Fatal("native twin rollback did not restore S1 metadata")
		}
		listed := goList("go list after rollback", g, true)
		sameListing("after rollback", listed.Operations[0].Snapshots, nativeList("native list after rollback", g), nativeList("native twin list after rollback", twin))
		t.Logf("rollback: Go user_version=%d flags=%#x mtime=%d.%09d listing %+v", reply.UserVersion, reply.Flags, *head.Operations[0].Seconds, *head.Operations[0].Nanos, listed.Operations[0].Snapshots)
	})
	if t.Failed() {
		return
	}

	t.Run("rollback without mtime keeps the head mtime", func(t *testing.T) {
		before := nativeRead("native head before zero-mtime rollback", g, snapOracleHead)
		_, reply, err := send("go zero-mtime rollback to S1", g, true, mutate(c2, false), cephmsgr.OSDOperation{Code: cephmsgr.OSDRollback, Snapshot: s1})
		committed("zero-mtime rollback", reply, err)
		after := nativeRead("native head after zero-mtime rollback", g, snapOracleHead)
		if *after.Operations[0].Seconds != *before.Operations[0].Seconds || *after.Operations[0].Nanos != *before.Operations[0].Nanos || after.Version <= before.Version || !bytes.Equal(after.Operations[1].Data, v1) {
			t.Fatal("zero-mtime rollback changed mtime/data or kept the version", before.Operations[0], after.Operations[0], before.Version, after.Version)
		}
	})
	if t.Failed() {
		return
	}

	t.Run("rollback above the snapset seq only advances the version", func(t *testing.T) {
		before := nativeRead("native head before rollback above seq", g, snapOracleHead)
		_, reply, err := send("go rollback above seq", g, true, mutate(c2, true), cephmsgr.OSDOperation{Code: cephmsgr.OSDRollback, Snapshot: above})
		committed("rollback above seq", reply, err)
		after := nativeRead("native head after rollback above seq", g, snapOracleHead)
		native := nativeSnapRollback(t, ctx, base, twin, keys, above, native2)
		record("native twin rollback above seq", nil, native)
		if after.Version != reply.UserVersion || after.Version <= before.Version || !bytes.Equal(after.Operations[1].Data, v1) ||
			native.Rollback == nil || native.Rollback.Code == nil || *native.Rollback.Code != 0 || !bytes.Equal(native.After.Data, native.Before.Data) || native.After.Version <= native.Before.Version {
			t.Fatal("rollback above the snapset seq differs from a version-only change", before.Version, after.Version)
		}
	})
	if t.Failed() {
		return
	}

	t.Run("delete under SnapContext leaves a listed whiteout", func(t *testing.T) {
		_, reply, err := send("go delete under {S2,[S2,S1]}", g, true, mutate(c2, true), cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete})
		committed("delete under snapc", reply, err)
		whiteouts[g] = true
		nativeWrite("native twin delete under {S2,[S2,S1]}", twin, native2, false, 0, snapDelete())
		_, reply, err = send("go read whiteout head", g, false, nil, readOps...)
		refused("read whiteout head", reply, err, -2)
		goRead("read at S1 after delete", g, s1, v1, "a1", "o1")
		listed := goList("go list whiteout", g, false)
		got := listed.Operations[0].Snapshots
		if len(got.Clones) != 2 || got.Clones[0].ID != s1 || got.Clones[1].ID != s2 {
			t.Fatalf("Go listing of a whiteout differs: %+v", got)
		}
		sameListing("whiteout", got, nativeList("native list whiteout", g), nativeList("native twin list whiteout", twin))
	})
	if t.Failed() {
		return
	}

	t.Run("rollback of an absent object succeeds without creating it", func(t *testing.T) {
		_, reply, err := send("go rollback absent", absent, false, mutate(c2, true), cephmsgr.OSDOperation{Code: cephmsgr.OSDRollback, Snapshot: s1})
		committed("rollback absent", reply, err)
		native := nativeSnapRollback(t, ctx, base, absentNative, keys, s1, native2)
		record("native rollback absent", nil, native)
		if native.Rollback == nil || native.Rollback.Code == nil || *native.Rollback.Code != 0 || native.After.Code != -2 {
			t.Fatal("native rollback of an absent object differs", native.Rollback, native.After.Code)
		}
		absentListing("Go-rolled absent object", nativeList("native list Go-rolled absent", absent))
		absentListing("native-rolled absent object", nativeList("native list native-rolled absent", absentNative))
		t.Logf("absent rollback: Go code=%d user_version=%d flags=%#x", reply.Code, reply.UserVersion, reply.Flags)
	})
	if t.Failed() {
		return
	}

	t.Run("rollback without a covering clone deletes the head", func(t *testing.T) {
		_, reply, err := send("go create under {S2,[S2,S1]}", b, false, mutate(c2, true), cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 2, Data: []byte("b1")})
		committed("create under snapc", reply, err)
		reply = goList("go list snapset without clones", b, true)
		if got := reply.Operations[0].Snapshots; got.Sequence != s2 || len(got.Clones) != 1 || got.Clones[0].ID != cephmsgr.OSDHeadSnapshot {
			t.Fatalf("new object under a SnapContext has an unexpected snapset: %+v", got)
		}
		_, reply, err = send("go rollback without clone", b, true, mutate(c2, true), cephmsgr.OSDOperation{Code: cephmsgr.OSDRollback, Snapshot: s1})
		committed("rollback without clone", reply, err)
		nativeWrite("native create under {S2,[S2,S1]}", bNative, native2, false, 0, snapWriteFull([]byte("b1")))
		native := nativeSnapRollback(t, ctx, base, bNative, keys, s1, native2)
		record("native rollback without clone", nil, native)
		if native.Rollback == nil || native.Rollback.Code == nil || *native.Rollback.Code != 0 || native.After.Code != -2 {
			t.Fatal("native rollback without a covering clone differs", native.Rollback, native.After.Code)
		}
		absentListing("Go rollback without clone", nativeList("native list Go rollback without clone", b))
		absentListing("native rollback without clone", nativeList("native list native rollback without clone", bNative))
	})
}
