package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Cross both copy opcodes with removed source snapshots, destination
// SnapContexts that name removed snapshots, a source that is the destination
// itself, and extreme source snapshot ids and versions. Every expectation is
// derived from the pinned Tentacle source and checked against independent
// native librados reads, object_info and rados listsnaps.
//
// The source of a copy is read by the OSD's own Objecter at the requested
// snap id, so find_object_context resolves it like a client read: an id above
// the source snapset seq (CEPH_MAXSNAP, an id the pool never allocated, a
// snapshot removed after the source's last write, pending trim or trimmed)
// selects the head, whose user version the version assertion then checks. At
// or below that seq, an id the covering clone does not list, or one in the
// OSDMap removed_snaps_queue, is -ENOENT, and a removed snapshot whose clone
// was trimmed no longer has a clone and is -ENOENT too. These failures happen
// after the copy started: aggregate code
// with per-op -EINPROGRESS, destination unchanged. Version 1 below the source
// is -ERANGE and the largest version -EOVERFLOW, like ASSERT_VER. The
// destination SnapContext is filtered by filter_snapc (snap_trimq and
// purged_snaps) before make_writeable: a context naming only removed
// snapshots clones nothing but still raises the snapset seq, and one with a
// live older snapshot clones the pre-copy head under the context seq (the
// removed id) holding only the live snapshot. A copy whose source hobject
// equals the destination (same pool, name, key as hobject_t stores it,
// namespace and the head) is -EINVAL before it starts (aggregate and per-op);
// the same object at any other snap id is refused locally because the copy
// blocks the destination head and its own source read then waits on that
// head. Sources: src/osd/PrimaryLogPG.cc (do_osd_ops COPY_FROM,
// start_copy, _copy_some, do_op, maybe_await_blocked_head,
// find_object_context, execute_ctx, make_writeable) and src/osd/PG.cc
// (filter_snapc, on_active_advmap) at v20.2.4. The pending state holds the
// nosnaptrim flag before the removal, so each PG keeps the removed ids in its
// snap_trimq. Destination filtering after trimming (purged_snaps) is
// TestCephOSDCopyFromPurgedSnapContextIntegration's. Not verified: a removed
// source snapshot on an erasure-coded pool, and the self-copy wait itself,
// which is never sent.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-replicated-pg-reply-v8.t-osdcopyfromsnapshotedges key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=replicated-pg-reply-v8
//scenario:covers CLIENT to OSD
//fixture:isolated allocates and removes self-managed snapshots and sets and clears nosnaptrim, leaving the fixture pool flag set
func TestCephOSDCopyFromSnapshotEdgesIntegration(t *testing.T) {
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
		Name        string                    `json:"name"`
		Input       cephmsgr.OSDCopyFromInput `json:"input"`
		SnapContext cephmsgr.OSDSnapContext   `json:"snap_context"`
		MapEpoch    uint32                    `json:"map_epoch"`
		Seed        *cephmsgr.OSDReply        `json:"seed_reply,omitempty"`
		Before      state                     `json:"before"`
		Reply       cephmsgr.OSDReply         `json:"reply"`
		Error       string                    `json:"error,omitempty"`
		After       state                     `json:"after"`
		Clone       *snapOracleRead           `json:"destination_clone,omitempty"`
	}
	receipt := struct {
		Snaps       []uint64                  `json:"snaps"`
		Clone       snapOracleRead            `json:"source_clone"`
		Head        snapOracleRead            `json:"source_head"`
		Source      snapToolResult            `json:"source_list_snaps"`
		NativeReads map[string]snapOracleRead `json:"native_snap_reads"`
		Removals    []cephmsgr.PoolOpResult   `json:"snap_removals"`
		Pending     snapToolResult            `json:"source_list_snaps_pending"`
		Trimmed     snapToolResult            `json:"source_list_snaps_trimmed"`
		Cells       []cell                    `json:"cells"`
		Refusals    []string                  `json:"local_refusals"`
		Cleaned     bool                      `json:"cleaned"`
	}{NativeReads: map[string]snapOracleRead{}}
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanupSnapOracle(t, base)
			receipt.Cleaned = !t.Failed()
		}
		raw, err := json.Marshal(receipt)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-copyfrom-snapshot-edges-results.json"), raw)
		}
		if err != nil {
			t.Error("snapshot edge copy evidence", err)
		}
	})

	// One native source and seven absent reserved names: per opcode a
	// destination for the source cells and two fresh destinations for the
	// removed-snapshot contexts, plus one name that stays absent.
	baseline := []byte("clone-bytes-0123456789\x00\xff")
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline, Attributes: []nativeXattrEntry{{Name: []byte("marker"), Value: []byte("clone")}}}, Reserve: 7})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 8 {
		t.Fatal("native seed", owned)
	}
	src, absent := owned.Objects[0], owned.Objects[3]
	place := func(name string) osdFixtureOracle {
		return nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
	}
	read := func(t *testing.T, name string, snap uint64) snapOracleRead {
		return nativeSnapRead(t, ctx, base, name, snap, nil)
	}
	list := func(t *testing.T, name string) snapToolResult { return nativeListSnaps(t, ctx, base, name) }
	observe := func(t *testing.T, name string) state { return state{read(t, name, snapOracleHead), list(t, name)} }
	headOnly := func(t *testing.T, what string, l snapCLIListing, seq uint64, size uint64) {
		t.Helper()
		if l.Seq != int64(seq) || len(l.Clones) != 1 || !l.Clones[0].ID.Head || l.Clones[0].Size != size || len(l.Clones[0].Overlaps) != 0 {
			t.Fatalf("%s: native listsnaps differs from head-only seq %d size %d: %+v", what, seq, size, l)
		}
	}

	srcRoute := place(src)
	sourceConn := mutationConnection(t, c, ctx, srcRoute)
	defer sourceConn.Close()
	// Raise the source user version first so version 1 is below the clone's.
	if _, err := sourceConn.Request(ctx, request(srcRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: []byte("header")}})); err != nil {
		t.Fatal(err)
	}
	// s0 exists but is not in the overwrite's context, so clone s1 does not
	// cover it; s2 stays live; s1 and s3 are removed later. s3 is newer than
	// every source write.
	snaps := nativeSnapCreate(t, ctx, base, 4)
	receipt.Snaps = snaps
	s0, s1, s2, s3 := snaps[0], snaps[1], snaps[2], snaps[3]
	if !(s0 < s1 && s1 < s2 && s2 < s3) {
		t.Fatal("native snap ids must increase", snaps)
	}
	unallocated := s3 + 1000
	overwrite := request(srcRoute,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 9, Data: []byte("head-data")},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "marker"}, Data: []byte("head")})
	overwrite.SnapContext = cephmsgr.OSDSnapContext{Sequence: s1, Snapshots: []uint64{s1}}
	overwrite.ModificationTime = cephmsgr.OSDTime{Seconds: 1700090000, Nanoseconds: 11}
	if reply, err := sourceConn.Request(ctx, overwrite); err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("source overwrite", reply, err)
	}
	receipt.Clone, receipt.Head, receipt.Source = read(t, src, s1), read(t, src, snapOracleHead), list(t, src)
	for _, s := range []snapOracleRead{receipt.Clone, receipt.Head} {
		if s.Aggregate != 0 || s.Version < 2 || s.ObjectInfo == nil || s.ObjectInfo.UserVersion != s.Version || s.Operations[0].Seconds == nil {
			t.Fatal("native source precondition", s)
		}
	}
	if receipt.Clone.ObjectInfo.OID.Snap != int64(s1) || receipt.Head.ObjectInfo.OID.Snap != -2 || receipt.Clone.Version >= receipt.Head.Version ||
		!bytes.Equal(receipt.Clone.Operations[1].Data, baseline) || !bytes.Equal(receipt.Head.Operations[1].Data, []byte("head-data")) {
		t.Fatalf("native clone and head differ from the model: clone=%+v head=%+v", receipt.Clone.ObjectInfo, receipt.Head.ObjectInfo)
	}
	sl := receipt.Source.listing(t)
	if sl.Seq != int64(s1) || len(sl.Clones) != 2 || sl.Clones[0].ID.Head || sl.Clones[0].ID.ID != s1 || len(sl.Clones[0].Snapshots) != 1 || sl.Clones[0].Snapshots[0].ID != s1 || !sl.Clones[1].ID.Head {
		t.Fatalf("native source listsnaps differs: %s", receipt.Source.JSON)
	}
	// The independent native client resolves the same ids the same way: the
	// extreme and unallocated ids read the head, s0 reads nothing.
	sameObject := func(a, b snapOracleRead) bool {
		return a.Aggregate == 0 && b.Aggregate == 0 && a.Version == b.Version && bytes.Equal(a.Operations[1].Data, b.Operations[1].Data) &&
			reflect.DeepEqual(a.Xattrs(), b.Xattrs())
	}
	nativeAt := func(t *testing.T, label string, snap uint64) snapOracleRead {
		t.Helper()
		r := read(t, src, snap)
		receipt.NativeReads[label] = r
		return r
	}
	for label, snap := range map[string]uint64{"maxsnap": cephmsgr.OSDMaxSnapshot, "unallocated": unallocated, "above-seq": s3} {
		if r := nativeAt(t, label, snap); !sameObject(r, receipt.Head) {
			t.Fatalf("native read at %s (%d) is not the head: %+v", label, snap, r)
		}
	}
	if r := nativeAt(t, "uncovered", s0); r.Aggregate != -2 {
		t.Fatal("native read of an id the clone does not cover", r.Aggregate)
	}

	mtime := uint32(0)
	seed := func(t *testing.T, o *cephmsgr.OSDConnection, route osdFixtureOracle, label string) cephmsgr.OSDReply {
		t.Helper()
		data := []byte(label)
		r := request(route,
			cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data},
			cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "marker"}, Data: data},
			cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "destination-only"}, Data: data})
		mtime++
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700091000, Nanoseconds: mtime}
		reply, err := o.Request(ctx, r)
		if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("destination seed", label, reply, err)
		}
		return reply
	}
	seeded := func(t *testing.T, what string, s state, label string, reply cephmsgr.OSDReply) {
		t.Helper()
		want := map[string]string{"marker": label, "destination-only": label}
		if s.Read.Aggregate != 0 || s.Read.ObjectInfo == nil || !bytes.Equal(s.Read.Operations[1].Data, []byte(label)) ||
			!reflect.DeepEqual(copyClassificationXattrs(s.Read.Xattrs()), want) || s.Read.Version != reply.UserVersion {
			t.Fatalf("%s: native seed differs: %+v", what, s.Read.Operations[:3])
		}
	}
	// copied checks the native destination head against the selected source.
	copied := func(t *testing.T, what string, after snapOracleRead, source snapOracleRead, reply cephmsgr.OSDReply) {
		t.Helper()
		oi, so := after.ObjectInfo, source.Operations[0]
		if after.Aggregate != 0 || oi == nil || !bytes.Equal(after.Operations[1].Data, source.Operations[1].Data) || oi.Size != uint64(len(source.Operations[1].Data)) ||
			!reflect.DeepEqual(copyClassificationXattrs(after.Xattrs()), copyClassificationXattrs(source.Xattrs())) ||
			after.Version != source.Version || oi.UserVersion != source.Version || reply.UserVersion != after.Version ||
			*after.Operations[0].Seconds != *so.Seconds || *after.Operations[0].Nanos != *so.Nanos || oi.TruncateSeq != 0 || oi.TruncateSize != 0 {
			t.Fatalf("%s: native destination differs from the copied source: after=%+v oi=%+v reply-uv=%d", what, after.Operations[:3], oi, reply.UserVersion)
		}
	}
	// replied checks the raw receipt: aggregate, ONDISK on success, request
	// identity and the echoed union, with the given per-op result.
	replied := func(t *testing.T, what string, entry cell, err error, dst string, code cephmsgr.OSDOpcode, route osdFixtureOracle, aggregate, perOp int32) {
		t.Helper()
		reply := entry.Reply
		var server *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		if aggregate == 0 && (err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0) ||
			aggregate != 0 && (!errors.As(err, &server) || server.Code != aggregate || errors.As(err, &unknown)) {
			t.Fatalf("%s: copy result %+v %v", what, reply, err)
		}
		wantSnap := entry.Input.SourceSnapshot
		if wantSnap == 0 {
			wantSnap = cephmsgr.OSDHeadSnapshot
		}
		if reply.Code != aggregate || reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.Object != dst ||
			reply.MapEpoch < route.Epoch || reply.MapEpoch < entry.MapEpoch || reply.RetryAttempt != 0 || len(reply.Operations) != 1 || len(reply.RawData) != 0 {
			t.Fatalf("%s: raw receipt %+v", what, reply)
		}
		r := reply.Operations[0]
		if r.Code != code || r.Result != perOp || r.CopyFromSnapshot != wantSnap || r.CopyFromVersion != entry.Input.SourceVersion || r.PayloadLength != 0 || len(r.Data) != 0 {
			t.Fatalf("%s: per-op echo %+v", what, r)
		}
	}
	run := func(t *testing.T, name string, o *cephmsgr.OSDConnection, route osdFixtureOracle, dst string, op cephmsgr.OSDOperation, snapc cephmsgr.OSDSnapContext, epoch uint32, seedReply *cephmsgr.OSDReply) (cell, error) {
		t.Helper()
		before := observe(t, dst)
		r := request(route, op)
		r.SnapContext = snapc
		if epoch > r.MapEpoch {
			r.MapEpoch = epoch
		}
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700092000}
		reply, err := o.Request(ctx, r)
		entry := cell{Name: name, Input: op.CopyFrom, SnapContext: snapc, MapEpoch: r.MapEpoch, Seed: seedReply, Before: before, Reply: reply, After: observe(t, dst)}
		if err != nil {
			entry.Error = err.Error()
		}
		t.Logf("%s: aggregate=%d tid=%d uv=%d epoch=%d err=%v", name, reply.Code, reply.Transaction, reply.UserVersion, reply.MapEpoch, err)
		return entry, err
	}
	record := func(entry cell) { receipt.Cells = append(receipt.Cells, entry) }

	type sourceCell struct {
		name      string
		snap      uint64
		version   uint64
		aggregate int32
		source    *snapOracleRead // the copied object on success
	}
	clone, head := receipt.Clone, receipt.Head
	// sourceCells reseeds the opcode's destination before every cell, so a
	// success must replace a distinct sentinel and a failure keep it entirely.
	sourceCells := func(t *testing.T, code cephmsgr.OSDOpcode, o *cephmsgr.OSDConnection, route osdFixtureOracle, dst string, epoch uint32, cells []sourceCell) {
		t.Helper()
		for _, tc := range cells {
			op := cephmsgr.OSDOperation{Code: code, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: base.Pool, SourceName: src, SourceSnapshot: tc.snap, SourceVersion: tc.version}}
			label := fmt.Sprintf("sentinel-%x-%s", code, tc.name)
			seedReply := seed(t, o, route, label)
			entry, err := run(t, fmt.Sprintf("%x/%s", code, tc.name), o, route, dst, op, cephmsgr.OSDSnapContext{}, epoch, &seedReply)
			record(entry)
			seeded(t, tc.name, entry.Before, label, seedReply)
			headOnly(t, tc.name+" before", entry.Before.List.listing(t), 0, uint64(len(label)))
			if tc.aggregate == 0 {
				replied(t, tc.name, entry, err, dst, code, route, 0, 0)
				copied(t, tc.name, entry.After.Read, *tc.source, entry.Reply)
				headOnly(t, tc.name+" after", entry.After.List.listing(t), 0, uint64(len(tc.source.Operations[1].Data)))
			} else {
				replied(t, tc.name, entry, err, dst, code, route, tc.aggregate, -115)
				if !reflect.DeepEqual(entry.Before, entry.After) {
					t.Fatalf("%s: failed copy changed the destination", tc.name)
				}
			}
		}
	}

	codes := []cephmsgr.OSDOpcode{cephmsgr.OSDCopyFrom, cephmsgr.OSDCopyFrom2}
	routes := map[string]osdFixtureOracle{}
	conns := map[string]*cephmsgr.OSDConnection{}
	for _, name := range owned.Objects[1:] {
		routes[name] = place(name)
		conns[name] = mutationConnection(t, c, ctx, routes[name])
		defer conns[name].Close()
	}
	destination := func(index, kind int) string { return owned.Objects[map[int]int{0: 1, 1: 4, 2: 6}[kind]+index] }

	t.Run("live", func(t *testing.T) {
		for index, code := range codes {
			dst := destination(index, 0)
			sourceCells(t, code, conns[dst], routes[dst], dst, 0, []sourceCell{
				{"nosnap", cephmsgr.OSDHeadSnapshot, 0, 0, &head},
				{"maxsnap", cephmsgr.OSDMaxSnapshot, 0, 0, &head},
				{"unallocated", unallocated, 0, 0, &head},
				{"maxsnap-clone-version", cephmsgr.OSDMaxSnapshot, clone.Version, -34, nil},
				{"maxsnap-head-version", cephmsgr.OSDMaxSnapshot, head.Version, 0, &head},
				{"uncovered", s0, 0, -2, nil},
				{"head-version-1", 0, 1, -34, nil},
				{"head-version-max", 0, math.MaxUint64, -75, nil},
				{"clone-version-1", s1, 1, -34, nil},
				{"clone-version-max", s1, math.MaxUint64, -75, nil},
			})
			if t.Failed() {
				return
			}

			// The source itself as destination, at its head: -EINVAL before the
			// copy starts, per-op too, and neither the head nor its clone list
			// changes. A key equal to the name is the same hobject.
			for _, tc := range []struct {
				name, dst, key string
			}{{"self-head", src, ""}, {"self-head-key", src, src}, {"self-absent", absent, ""}} {
				route, o := srcRoute, sourceConn
				if tc.dst != src {
					route, o = routes[tc.dst], conns[tc.dst]
				}
				op := cephmsgr.OSDOperation{Code: code, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: base.Pool, SourceName: tc.dst, SourceKey: tc.key}}
				entry, err := run(t, fmt.Sprintf("%x/%s", code, tc.name), o, route, tc.dst, op, cephmsgr.OSDSnapContext{}, 0, nil)
				record(entry)
				replied(t, tc.name, entry, err, tc.dst, code, route, -22, -22)
				if !reflect.DeepEqual(entry.Before, entry.After) {
					t.Fatalf("%s: refused self-copy changed the object", tc.name)
				}
				if tc.dst == absent && (entry.After.Read.Aggregate != -2 || entry.After.List.Code == nil || *entry.After.List.Code != -2) {
					t.Fatal("self-absent: the object was created", entry.After.Read.Aggregate)
				}
			}

			// Known-unsent: the source object itself at a snap id other than the
			// head, and SNAPDIR. TIDs stay consecutive and nothing changes.
			readReq := request(srcRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 64})
			first, err := sourceConn.Request(ctx, readReq)
			if err != nil {
				t.Fatal(err)
			}
			before := observe(t, src)
			for _, refusal := range []struct {
				name string
				in   cephmsgr.OSDCopyFromInput
			}{
				{"self-clone", cephmsgr.OSDCopyFromInput{SourceName: src, SourceSnapshot: s1}},
				{"self-clone-key", cephmsgr.OSDCopyFromInput{SourceName: src, SourceKey: src, SourceSnapshot: s1}},
				{"self-uncovered", cephmsgr.OSDCopyFromInput{SourceName: src, SourceSnapshot: s0}},
				{"self-maxsnap", cephmsgr.OSDCopyFromInput{SourceName: src, SourceSnapshot: cephmsgr.OSDMaxSnapshot}},
				{"self-clone-version", cephmsgr.OSDCopyFromInput{SourceName: src, SourceSnapshot: s1, SourceVersion: clone.Version}},
				{"snapdir", cephmsgr.OSDCopyFromInput{SourceName: destination(index, 0), SourceSnapshot: math.MaxUint64}},
			} {
				in := refusal.in
				in.SourcePool = base.Pool
				reply, err := sourceConn.Request(ctx, request(srcRoute, cephmsgr.OSDOperation{Code: code, CopyFrom: in}))
				var server *cephmsgr.OSDError
				var unknown *cephmsgr.OutcomeUnknownError
				if err == nil || errors.As(err, &server) || errors.As(err, &unknown) || reply.Submission != (cephmsgr.OSDSubmission{}) || reply.Transaction != 0 {
					t.Fatal("local refusal was submitted", refusal.name, reply, err)
				}
				receipt.Refusals = append(receipt.Refusals, fmt.Sprintf("%x/%s", code, refusal.name))
			}
			next, err := sourceConn.Request(ctx, readReq)
			if err != nil || next.Transaction != first.Transaction+1 || !reflect.DeepEqual(before, observe(t, src)) {
				t.Fatal("refusal consumed a TID, altered the source or broke the connection", next, err)
			}
		}
	})
	if t.Failed() {
		return
	}
	if !reflect.DeepEqual(receipt.Clone, read(t, src, s1)) || !reflect.DeepEqual(receipt.Head, read(t, src, snapOracleHead)) || !reflect.DeepEqual(receipt.Source, list(t, src)) {
		t.Fatal("live cells altered the source clone, head or clone list")
	}

	// Pending trim: nosnaptrim is set before s1 and s3 are removed, so every
	// PG keeps them in its snap_trimq and the source clone s1 stays on disk.
	snapTrim := holdSnapTrim(t, ctx, c)
	epoch := uint32(0)
	for _, snap := range []uint64{s1, s3} {
		removed, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: uint32(base.Pool), Op: cephmsgr.SelfManagedSnapshotRemove, Snapshot: snap})
		receipt.Removals = append(receipt.Removals, removed)
		if err != nil || removed.Code != 0 || removed.Epoch == 0 {
			t.Fatal("remove self-managed snap", snap, removed, err)
		}
		epoch = max(epoch, removed.Epoch)
	}
	receipt.Pending = list(t, src)
	if pl := receipt.Pending.listing(t); len(pl.Clones) != 2 || pl.Clones[0].ID.Head || pl.Clones[0].ID.ID != s1 {
		t.Fatalf("clone s1 was trimmed despite nosnaptrim: %s", receipt.Pending.JSON)
	}
	if r := nativeAt(t, "removed-pending", s1); r.Aggregate != -2 {
		t.Fatal("native read of a snapshot in the removed queue", r.Aggregate)
	}
	if r := nativeAt(t, "removed-above-seq-pending", s3); !sameObject(r, head) {
		t.Fatal("native read of a removed id above the source seq is not the head", r)
	}
	t.Run("pending", func(t *testing.T) {
		for index, code := range codes {
			dst := destination(index, 0)
			sourceCells(t, code, conns[dst], routes[dst], dst, epoch, []sourceCell{
				{"removed-pending-clone", s1, 0, -2, nil},
				{"removed-pending-above-seq", s3, 0, 0, &head},
			})
			if t.Failed() {
				return
			}
			op := cephmsgr.OSDOperation{Code: code, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: base.Pool, SourceName: src}}

			// Only removed snapshots: filter_snapc empties the list, so there is
			// no clone, but the snapset seq still becomes the context seq.
			dst = destination(index, 1)
			label := fmt.Sprintf("removed-only-%x", code)
			seedReply := seed(t, conns[dst], routes[dst], label)
			entry, err := run(t, fmt.Sprintf("%x/snapc/removed-only", code), conns[dst], routes[dst], dst, op, cephmsgr.OSDSnapContext{Sequence: s3, Snapshots: []uint64{s3}}, epoch, &seedReply)
			record(entry)
			seeded(t, "removed-only", entry.Before, label, seedReply)
			headOnly(t, "removed-only before", entry.Before.List.listing(t), 0, uint64(len(label)))
			replied(t, "removed-only", entry, err, dst, code, routes[dst], 0, 0)
			copied(t, "removed-only", entry.After.Read, head, entry.Reply)
			headOnly(t, "removed-only after", entry.After.List.listing(t), s3, uint64(len(head.Operations[1].Data)))

			// A removed newest snapshot over a live one: the clone takes the
			// context seq s3 as its id but holds only s2, with no overlap.
			dst = destination(index, 2)
			label = fmt.Sprintf("removed-newest-%x", code)
			seedReply = seed(t, conns[dst], routes[dst], label)
			entry, err = run(t, fmt.Sprintf("%x/snapc/removed-newest", code), conns[dst], routes[dst], dst, op, cephmsgr.OSDSnapContext{Sequence: s3, Snapshots: []uint64{s3, s2}}, epoch, &seedReply)
			destClone := read(t, dst, s2)
			entry.Clone = &destClone
			record(entry)
			seeded(t, "removed-newest", entry.Before, label, seedReply)
			headOnly(t, "removed-newest before", entry.Before.List.listing(t), 0, uint64(len(label)))
			replied(t, "removed-newest", entry, err, dst, code, routes[dst], 0, 0)
			copied(t, "removed-newest", entry.After.Read, head, entry.Reply)
			l := entry.After.List.listing(t)
			if l.Seq != int64(s3) || len(l.Clones) != 2 || l.Clones[0].ID.Head || l.Clones[0].ID.ID != s3 || len(l.Clones[0].Snapshots) != 1 || l.Clones[0].Snapshots[0].ID != s2 ||
				l.Clones[0].Size != uint64(len(label)) || len(l.Clones[0].Overlaps) != 0 || !l.Clones[1].ID.Head || l.Clones[1].Size != uint64(len(head.Operations[1].Data)) {
				t.Fatalf("removed-newest: native destination listsnaps differs: %s", entry.After.List.JSON)
			}
			if co := destClone.ObjectInfo; destClone.Aggregate != 0 || co == nil || co.OID.Snap != int64(s3) || !bytes.Equal(destClone.Operations[1].Data, []byte(label)) ||
				destClone.Version != entry.Before.Read.Version || !reflect.DeepEqual(destClone.Xattrs(), entry.Before.Read.Xattrs()) {
				t.Fatalf("removed-newest: native read at s2 is not the pre-copy head: %+v %+v", destClone.Operations[:3], co)
			}
		}
	})
	if t.Failed() {
		return
	}

	// Trimmed: clear nosnaptrim and wait until the source clone is gone.
	snapTrim.release(t, ctx)
	started := time.Now()
	for {
		receipt.Trimmed = list(t, src)
		if l := receipt.Trimmed.listing(t); len(l.Clones) == 1 && l.Clones[0].ID.Head {
			break
		}
		if time.Since(started) > 90*time.Second {
			t.Fatalf("native trim of clone s1 did not complete: %s", receipt.Trimmed.JSON)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("native trim removed clone s1 after %s", time.Since(started).Round(time.Millisecond))
	if r := nativeAt(t, "removed-trimmed", s1); r.Aggregate != -2 {
		t.Fatal("native read of a trimmed snapshot", r.Aggregate)
	}
	t.Run("trimmed", func(t *testing.T) {
		for index, code := range codes {
			dst := destination(index, 0)
			sourceCells(t, code, conns[dst], routes[dst], dst, epoch, []sourceCell{
				{"removed-trimmed-clone", s1, 0, -2, nil},
				{"removed-trimmed-above-seq", s3, 0, 0, &head},
			})
		}
	})
	if !sameObject(receipt.Head, read(t, src, snapOracleHead)) {
		t.Fatal("copies altered the source head")
	}
}
