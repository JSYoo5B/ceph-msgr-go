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
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephOSDCopyFromCloneIDIntegration crosses both copy opcodes with
// SourceCloneID (CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE) on a source with two
// clones: clone s2 covering s2 and s1, and clone s3 covering s3. The flag
// reaches the OSD's own source read as CEPH_OSD_FLAG_MAP_SNAP_CLONE
// (PrimaryLogPG::_copy_some), and find_object_context then takes the snap id
// as a clone id: s2 and s3 copy their clones, s1 (covered by clone s2 but not
// a clone id) is -ENOENT where the flagless copy of s1 copies clone s2, and
// an id above the snapset seq copies the head. The clone's version is checked
// like any source version. The flag path consults neither the clone's
// snapshot list nor the OSDMap removed_snaps_queue: after s3 is removed under
// nosnaptrim, the flagless copy of s3 is -ENOENT while the flagged one still
// copies the untrimmed clone s3. Once the native trim removed clone s3, the
// flagged copy of s3 is -ENOENT and clone s2 is still copied. Every failure
// comes after the copy started (per-op -EINPROGRESS) and leaves the
// destination unchanged; every success replaces a distinct native sentinel
// with the selected object, checked by independent native reads, object_info
// and rados listsnaps. A clone id at the head is refused before submission.
// Sources: ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc (do_osd_ops
// COPY_FROM, _copy_some, do_op, find_object_context). Not verified: the
// FLUSH and RWORDERED copy flags (refused, see internal/osd/copyfrom.go), a
// missing or degraded clone, an erasure-coded pool and other profiles.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-replicated-pg-reply-v8.t-osdcopyfromcloneid key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=replicated-pg-reply-v8
//scenario:covers CLIENT to OSD
//fixture:isolated allocates and removes self-managed snapshots and sets and clears nosnaptrim, leaving the fixture pool flag set
func TestCephOSDCopyFromCloneIDIntegration(t *testing.T) {
	fixture, request := osdFixture, osdObjectRequest
	if isReplicatedPGFixtureProfile() {
		fixture, request = replicatedPGFixture, replicatedPGRequest
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Second)
	defer cancel()

	type state struct {
		Read snapOracleRead `json:"read"`
		List snapToolResult `json:"list_snaps"`
	}
	type cell struct {
		Name     string                    `json:"name"`
		Input    cephmsgr.OSDCopyFromInput `json:"input"`
		MapEpoch uint32                    `json:"map_epoch"`
		Seed     cephmsgr.OSDReply         `json:"seed_reply"`
		Before   state                     `json:"before"`
		Reply    cephmsgr.OSDReply         `json:"reply"`
		Error    string                    `json:"error,omitempty"`
		After    state                     `json:"after"`
	}
	receipt := struct {
		Snaps       []uint64                  `json:"snaps"`
		Clones      map[string]snapOracleRead `json:"source_clones"`
		Head        snapOracleRead            `json:"source_head"`
		Source      snapToolResult            `json:"source_list_snaps"`
		NativeReads map[string]snapOracleRead `json:"native_snap_reads"`
		Removal     cephmsgr.PoolOpResult     `json:"snap_removal"`
		Pending     snapToolResult            `json:"source_list_snaps_pending"`
		Trimmed     snapToolResult            `json:"source_list_snaps_trimmed"`
		Cells       []cell                    `json:"cells"`
		Refusals    []string                  `json:"local_refusals"`
		Cleaned     bool                      `json:"cleaned"`
	}{Clones: map[string]snapOracleRead{}, NativeReads: map[string]snapOracleRead{}}
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanupSnapOracle(t, base)
			receipt.Cleaned = !t.Failed()
		}
		raw, err := json.Marshal(receipt)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-copyfrom-clone-id-results.json"), raw)
		}
		if err != nil {
			t.Error("clone id copy evidence", err)
		}
	})

	// One native source and one destination per opcode.
	baseline := []byte("clone-s2-bytes\x00\xff")
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline, Attributes: []nativeXattrEntry{{Name: []byte("marker"), Value: []byte("clone-s2")}}}, Reserve: 2})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 3 {
		t.Fatal("native seed", owned)
	}
	src := owned.Objects[0]
	place := func(name string) osdFixtureOracle {
		return nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
	}
	read := func(t *testing.T, name string, snap uint64) snapOracleRead {
		return nativeSnapRead(t, ctx, base, name, snap, nil)
	}
	list := func(t *testing.T, name string) snapToolResult { return nativeListSnaps(t, ctx, base, name) }
	observe := func(t *testing.T, name string) state { return state{read(t, name, snapOracleHead), list(t, name)} }

	srcRoute := place(src)
	sourceConn := mutationConnection(t, c, ctx, srcRoute)
	defer sourceConn.Close()
	if _, err := sourceConn.Request(ctx, request(srcRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: []byte("header")}})); err != nil {
		t.Fatal(err)
	}
	snaps := nativeSnapCreate(t, ctx, base, 3)
	receipt.Snaps = snaps
	s1, s2, s3 := snaps[0], snaps[1], snaps[2]
	if !(s1 < s2 && s2 < s3) {
		t.Fatal("native snap ids must increase", snaps)
	}
	// The first overwrite clones the baseline as clone s2 covering s2 and s1;
	// the second clones its bytes as clone s3 covering s3 alone.
	for i, w := range []struct {
		data  string
		snapc cephmsgr.OSDSnapContext
	}{
		{"clone-s3-bytes", cephmsgr.OSDSnapContext{Sequence: s2, Snapshots: []uint64{s2, s1}}},
		{"head-data", cephmsgr.OSDSnapContext{Sequence: s3, Snapshots: []uint64{s3, s2, s1}}},
	} {
		marker := "clone-s3"
		if i == 1 {
			marker = "head"
		}
		overwrite := request(srcRoute,
			cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(w.data)), Data: []byte(w.data)},
			cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "marker"}, Data: []byte(marker)})
		overwrite.SnapContext = w.snapc
		overwrite.ModificationTime = cephmsgr.OSDTime{Seconds: 1700093000 + uint32(i), Nanoseconds: 7}
		if reply, err := sourceConn.Request(ctx, overwrite); err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("source overwrite", i, reply, err)
		}
	}
	cloneS2, cloneS3, head := read(t, src, s2), read(t, src, s3), read(t, src, snapOracleHead)
	receipt.Clones["s2"], receipt.Clones["s3"], receipt.Head, receipt.Source = cloneS2, cloneS3, head, list(t, src)
	for _, s := range []snapOracleRead{cloneS2, cloneS3, head} {
		if s.Aggregate != 0 || s.Version < 2 || s.ObjectInfo == nil || s.ObjectInfo.UserVersion != s.Version || s.Operations[0].Seconds == nil {
			t.Fatal("native source precondition", s)
		}
	}
	if cloneS2.ObjectInfo.OID.Snap != int64(s2) || cloneS3.ObjectInfo.OID.Snap != int64(s3) || head.ObjectInfo.OID.Snap != -2 ||
		!(cloneS2.Version < cloneS3.Version && cloneS3.Version < head.Version) ||
		!bytes.Equal(cloneS2.Operations[1].Data, baseline) || !bytes.Equal(cloneS3.Operations[1].Data, []byte("clone-s3-bytes")) || !bytes.Equal(head.Operations[1].Data, []byte("head-data")) {
		t.Fatalf("native clones and head differ from the model: s2=%+v s3=%+v head=%+v", cloneS2.ObjectInfo, cloneS3.ObjectInfo, head.ObjectInfo)
	}
	sl := receipt.Source.listing(t)
	if sl.Seq != int64(s3) || len(sl.Clones) != 3 || sl.Clones[0].ID.Head || sl.Clones[0].ID.ID != s2 || len(sl.Clones[0].Snapshots) != 2 ||
		sl.Clones[1].ID.Head || sl.Clones[1].ID.ID != s3 || len(sl.Clones[1].Snapshots) != 1 || sl.Clones[1].Snapshots[0].ID != s3 || !sl.Clones[2].ID.Head {
		t.Fatalf("native source listsnaps differs: %s", receipt.Source.JSON)
	}
	sameObject := func(a, b snapOracleRead) bool {
		return a.Aggregate == 0 && b.Aggregate == 0 && a.Version == b.Version && bytes.Equal(a.Operations[1].Data, b.Operations[1].Data) &&
			reflect.DeepEqual(a.Xattrs(), b.Xattrs())
	}
	// The independent native read resolves s1 to clone s2 (the contrast cell).
	if r := read(t, src, s1); !sameObject(r, cloneS2) {
		t.Fatal("native read at s1 is not clone s2", r)
	} else {
		receipt.NativeReads["s1"] = r
	}

	mtime := uint32(0)
	codes := []cephmsgr.OSDOpcode{cephmsgr.OSDCopyFrom, cephmsgr.OSDCopyFrom2}
	routes, conns := map[string]osdFixtureOracle{}, map[string]*cephmsgr.OSDConnection{}
	for _, name := range owned.Objects[1:] {
		routes[name] = place(name)
		conns[name] = mutationConnection(t, c, ctx, routes[name])
		defer conns[name].Close()
	}
	type sourceCell struct {
		name      string
		snap      uint64
		cloneID   bool
		version   uint64
		aggregate int32
		source    *snapOracleRead // the copied object on success
	}
	cells := func(t *testing.T, code cephmsgr.OSDOpcode, dst string, epoch uint32, list []sourceCell) {
		t.Helper()
		o, route := conns[dst], routes[dst]
		for _, tc := range list {
			label := fmt.Sprintf("sentinel-%x-%s", code, tc.name)
			data := []byte(label)
			seedReq := request(route,
				cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data},
				cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "marker"}, Data: data},
				cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "destination-only"}, Data: data})
			mtime++
			seedReq.ModificationTime = cephmsgr.OSDTime{Seconds: 1700094000, Nanoseconds: mtime}
			seedReply, err := o.Request(ctx, seedReq)
			if err != nil || seedReply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
				t.Fatal("destination seed", label, seedReply, err)
			}
			before := observe(t, dst)
			if before.Read.Aggregate != 0 || !bytes.Equal(before.Read.Operations[1].Data, data) || before.Read.Version != seedReply.UserVersion ||
				!reflect.DeepEqual(copyClassificationXattrs(before.Read.Xattrs()), map[string]string{"marker": label, "destination-only": label}) {
				t.Fatalf("%s: native seed differs: %+v", tc.name, before.Read.Operations[:3])
			}
			op := cephmsgr.OSDOperation{Code: code, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: base.Pool, SourceName: src, SourceSnapshot: tc.snap, SourceVersion: tc.version, SourceCloneID: tc.cloneID}}
			r := request(route, op)
			if epoch > r.MapEpoch {
				r.MapEpoch = epoch
			}
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700095000}
			reply, err := o.Request(ctx, r)
			entry := cell{Name: fmt.Sprintf("%x/%s", code, tc.name), Input: op.CopyFrom, MapEpoch: r.MapEpoch, Seed: seedReply, Before: before, Reply: reply, After: observe(t, dst)}
			if err != nil {
				entry.Error = err.Error()
			}
			receipt.Cells = append(receipt.Cells, entry)
			t.Logf("%s: aggregate=%d tid=%d uv=%d epoch=%d err=%v", entry.Name, reply.Code, reply.Transaction, reply.UserVersion, reply.MapEpoch, err)
			var server *cephmsgr.OSDError
			var unknown *cephmsgr.OutcomeUnknownError
			if tc.aggregate == 0 && (err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0) ||
				tc.aggregate != 0 && (!errors.As(err, &server) || server.Code != tc.aggregate || errors.As(err, &unknown)) {
				t.Fatalf("%s: copy result %+v %v", tc.name, reply, err)
			}
			perOp := int32(0)
			if tc.aggregate != 0 {
				perOp = -115
			}
			if reply.Code != tc.aggregate || reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.Object != dst ||
				reply.MapEpoch < route.Epoch || reply.MapEpoch < r.MapEpoch || reply.RetryAttempt != 0 || len(reply.Operations) != 1 || len(reply.RawData) != 0 {
				t.Fatalf("%s: raw receipt %+v", tc.name, reply)
			}
			if res := reply.Operations[0]; res.Code != code || res.Result != perOp || res.CopyFromSnapshot != tc.snap || res.CopyFromVersion != tc.version || res.PayloadLength != 0 || len(res.Data) != 0 {
				t.Fatalf("%s: per-op echo %+v", tc.name, res)
			}
			l := entry.After.List.listing(t)
			if len(l.Clones) != 1 || !l.Clones[0].ID.Head || l.Seq != 0 {
				t.Fatalf("%s: native destination listsnaps is not head-only: %s", tc.name, entry.After.List.JSON)
			}
			if tc.aggregate != 0 {
				if !reflect.DeepEqual(entry.Before, entry.After) {
					t.Fatalf("%s: failed copy changed the destination", tc.name)
				}
				continue
			}
			after, so := entry.After.Read, tc.source.Operations[0]
			if after.Aggregate != 0 || after.ObjectInfo == nil || !bytes.Equal(after.Operations[1].Data, tc.source.Operations[1].Data) ||
				!reflect.DeepEqual(copyClassificationXattrs(after.Xattrs()), copyClassificationXattrs(tc.source.Xattrs())) ||
				after.Version != tc.source.Version || after.ObjectInfo.UserVersion != tc.source.Version || reply.UserVersion != after.Version ||
				*after.Operations[0].Seconds != *so.Seconds || *after.Operations[0].Nanos != *so.Nanos || l.Clones[0].Size != uint64(len(tc.source.Operations[1].Data)) {
				t.Fatalf("%s: native destination differs from the copied source: %+v uv=%d", tc.name, after.Operations[:3], reply.UserVersion)
			}
		}
	}

	t.Run("live", func(t *testing.T) {
		for index, code := range codes {
			dst := owned.Objects[1+index]
			cells(t, code, dst, 0, []sourceCell{
				{"clone-s2", s2, true, 0, 0, &cloneS2},
				{"clone-s3", s3, true, 0, 0, &cloneS3},
				{"covered-not-clone", s1, true, 0, -2, nil},
				{"covered-resolved", s1, false, 0, 0, &cloneS2},
				{"above-seq", cephmsgr.OSDMaxSnapshot, true, 0, 0, &head},
				{"clone-s2-version", s2, true, cloneS2.Version, 0, &cloneS2},
				{"clone-s2-newer-version", s2, true, cloneS3.Version, -75, nil},
			})
			if t.Failed() {
				return
			}
			// Known-unsent: a clone id at the head. TIDs stay consecutive.
			o, route := conns[dst], routes[dst]
			readReq := request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 64})
			first, err := o.Request(ctx, readReq)
			if err != nil {
				t.Fatal(err)
			}
			before := observe(t, dst)
			for _, snap := range []uint64{0, cephmsgr.OSDHeadSnapshot} {
				in := cephmsgr.OSDCopyFromInput{SourcePool: base.Pool, SourceName: src, SourceSnapshot: snap, SourceCloneID: true}
				reply, err := o.Request(ctx, request(route, cephmsgr.OSDOperation{Code: code, CopyFrom: in}))
				var server *cephmsgr.OSDError
				var unknown *cephmsgr.OutcomeUnknownError
				if err == nil || errors.As(err, &server) || errors.As(err, &unknown) || reply.Submission != (cephmsgr.OSDSubmission{}) || reply.Transaction != 0 {
					t.Fatal("local refusal was submitted", snap, reply, err)
				}
				receipt.Refusals = append(receipt.Refusals, fmt.Sprintf("%x/clone-id-head-%x", code, snap))
			}
			next, err := o.Request(ctx, readReq)
			if err != nil || next.Transaction != first.Transaction+1 || !reflect.DeepEqual(before, observe(t, dst)) {
				t.Fatal("refusal consumed a TID, altered the destination or broke the connection", next, err)
			}
		}
	})
	if t.Failed() {
		return
	}

	// Pending trim: nosnaptrim is set before s3 is removed, so the PG keeps s3
	// in its snap_trimq and clone s3, now covering only a removed snapshot,
	// stays on disk.
	snapTrim := holdSnapTrim(t, ctx, c)
	removed, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: uint32(base.Pool), Op: cephmsgr.SelfManagedSnapshotRemove, Snapshot: s3})
	receipt.Removal = removed
	if err != nil || removed.Code != 0 || removed.Epoch == 0 {
		t.Fatal("remove self-managed snap", s3, removed, err)
	}
	receipt.Pending = list(t, src)
	if pl := receipt.Pending.listing(t); len(pl.Clones) != 3 || pl.Clones[1].ID.Head || pl.Clones[1].ID.ID != s3 {
		t.Fatalf("clone s3 was trimmed despite nosnaptrim: %s", receipt.Pending.JSON)
	}
	if r := read(t, src, s3); r.Aggregate != -2 {
		t.Fatal("native read of a snapshot in the removed queue", r.Aggregate)
	} else {
		receipt.NativeReads["s3-pending"] = r
	}
	t.Run("pending", func(t *testing.T) {
		for index, code := range codes {
			cells(t, code, owned.Objects[1+index], removed.Epoch, []sourceCell{
				{"removed-pending-resolved", s3, false, 0, -2, nil},
				{"removed-pending-clone-id", s3, true, 0, 0, &cloneS3},
			})
			if t.Failed() {
				return
			}
		}
	})
	if t.Failed() {
		return
	}

	// Trimmed: clear nosnaptrim and wait until the native listing has no clone s3.
	snapTrim.release(t, ctx)
	started := time.Now()
	for {
		receipt.Trimmed = list(t, src)
		if l := receipt.Trimmed.listing(t); len(l.Clones) == 2 && l.Clones[0].ID.ID == s2 && l.Clones[1].ID.Head {
			break
		}
		if time.Since(started) > 90*time.Second {
			t.Fatalf("native trim of clone s3 did not complete: %s", receipt.Trimmed.JSON)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("native trim removed clone s3 after %s", time.Since(started).Round(time.Millisecond))
	t.Run("trimmed", func(t *testing.T) {
		for index, code := range codes {
			cells(t, code, owned.Objects[1+index], removed.Epoch, []sourceCell{
				{"removed-trimmed-clone-id", s3, true, 0, -2, nil},
				{"clone-s2-after-trim", s2, true, 0, 0, &cloneS2},
			})
		}
	})
	if !sameObject(head, read(t, src, snapOracleHead)) || !sameObject(cloneS2, read(t, src, s2)) {
		t.Fatal("copies altered the source head or clone s2")
	}
}
