package integration_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// TestCephOSDErasureOptimizedK2M2ReadCompoundIntegration classifies the
// read-only compound shapes the client admits for an erasure-coded
// placement, one shape at a time, on an optimized isa k=2,m=2 pool over the
// four-OSD BlueStore topology. One head is seeded natively with 8 KiB and
// two xattrs; the pool takes no OMAP. Each shape is sent by Go to the
// primary shard and as one librados C read op of the same operations
// (tools/osd_pool_mode_readcompound_oracle.py), and judged like
// TestCephOSDReadCompoundLaterErrorIntegration: aggregate, every operation's
// identity and result, the output of every executed operation against the
// native output and the seed, and nothing after the failing one.
//
// Accepted: extent reads with output-free guards (READ with READ, ASSERT_VER
// and CMPEXT, READ with a true CMPXATTR, whose result 1 is the aggregate),
// operations with output without an extent read (STAT, GETXATTRS and a
// read-mode CALL of cls_hello say_hello; GETXATTRS with a true CMPXATTR), and
// the OMAP reads, which an EC pool answers as an object without OMAP: empty
// pages, an empty by-keys map and an OMAP_CMP of an absent key against an
// empty value that holds (pinned PrimaryLogPG.cc do_osd_ops reads OMAP only
// when the object has it, and supports_omap is false for EC in osd_types.h).
//
// Refused by the server, each a server restriction or a server answer, not
// a client feature: an OMAP write gets -EOPNOTSUPP (-95), so an EC object
// never has OMAP; a read-mode CALL whose method reads object data (cls_hello
// replay, cls_cxx_read) gets -EOPNOTSUPP at that operation, since SYNC_READ
// is refused on EC, with the earlier STAT output kept; rbd.get_object_prefix
// gets -ENOENT at that operation for the absent OMAP key; a false CMPXATTR and
// a CMPEXT mismatch after an asynchronous READ keep that READ's bytes, and
// ASSERT_VER below the version fails first with no output.
//
// Refused by the client before submission, with the next transaction ID
// unchanged: the CHECKSUM and SPARSE_READ compound subsets, which stay
// replicated-only, and every vector of an extent read beside an operation
// with output (osd.ErasureReexecutedVector). The pinned EC primary queues an
// extent read and runs the vector again once it completes, without clearing
// earlier outputs, so an operation with output that runs in more than one
// pass returns its output once per pass. The native C client observes the
// passes through a read-mode CALL of cls_hello say_hello: its output comes
// back twice after a READ (which queues its read and lets the pass go on)
// and before a CMPEXT (which queues its read and ends the pass), once after
// a CMPEXT, three times before a CMPEXT that a READ follows (a third pass for
// that READ), and once beside STAT without an extent read. The pool's bytes,
// version, mtime and xattrs are unchanged at the end.
//
// Not verified: other EC widths and plugins, a non-optimized EC pool, a
// degraded EC PG, CHECKSUM and SPARSE_READ standalone on EC, and the
// object-hash profile, on which the client names no placement and so cannot
// refuse the doubled shapes.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2readcompound key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:shareable read-only vectors on one head of an owned EC pool; checked cleanup removes the pool and its profile
func TestCephOSDErasureOptimizedK2M2ReadCompoundIntegration(t *testing.T) {
	width := erasureK2M2
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != width.topology || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" || !isReplicatedPGFixtureProfile() {
		t.Skipf("%s needs the BlueStore OSD topology %q and a replicated-PG profile", width, width.topology)
	}
	c, _, _, _ := replicatedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		r := nativePool(t, cleanup, "cleanup", nil)
		if !r.PoolsRemoved {
			t.Error("owned erasure-coded pool was not removed")
		}
		if slices.Contains(r.ECProfilesRemaining, width.profile) {
			t.Error("owned erasure-code profile was not removed", width.profile)
		}
	})
	created := nativePool(t, ctx, "create", map[string]any{"slot": 1, "pg_num": 8, "hashpspool": isReplicatedPGFixtureProfile(), "erasure": true, "width": width.name, "optimized": true, "plugin": "isa"})
	if created.PoolType != 3 || !created.ECOptimized || created.Width != width.name || created.ErasureCodeProfile != width.profile {
		t.Fatal("pool is not the requested erasure-coded shape", created.PoolType, created.ECOptimized, created.Width, created.ErasureCodeProfile)
	}
	facts := erasurePoolMapFacts(t, ctx, c, created, width, true)
	const object = "read-compound-ec"
	data := make([]byte, 8192)
	for i := range data {
		data[i] = byte(i*29 + 3)
	}
	model := seedReadCompound(t, ctx, 1, object, data, map[string][]byte{"a": []byte("alpha"), "b": {0, 255, 'b', 0}}, nil)
	model.omap = map[string][]byte{}
	before := poolObserve(t, ctx, 1, object, "", "")
	route := poolPlace(t, ctx, 1, object, "")
	facts.checkPlacement(t, route)
	if route.PoolType != 3 || route.ID != route.PrimaryID || len(route.Acting) != width.size() {
		t.Fatal("EC compound target is not a healthy primary-shard placement", route)
	}
	o := mutationConnection(t, c, ctx, route)
	defer o.Close()
	v := model.version
	wrong := bytes.Clone(data[16:20])
	wrong[3] ^= 0xff
	cases := []struct {
		name    string
		ops     []readCompoundOp
		code    int32
		failing int
	}{
		{"read-read", []readCompoundOp{rcRead(0, 32), rcRead(32, 32)}, 0, -1},
		{"assert-read-cmpext", []readCompoundOp{rcAssert(v), rcRead(0, 16), rcCmpExt(16, data[16:20])}, 0, -1},
		{"read-cmpxattr-true", []readCompoundOp{rcRead(0, 16), rcCmpXattr("a", cephmsgr.OSDXattrEqual, []byte("alpha"))}, 1, -1},
		{"stat-xattrs-say-hello", []readCompoundOp{rcStat(), rcXattrs(), rcCall("hello", "say_hello", []byte("ec"), rcHello("ec"))}, 0, -1},
		{"xattrs-cmpxattr-true", []readCompoundOp{rcXattrs(), rcCmpXattr("b", cephmsgr.OSDXattrEqual, []byte{0, 255, 'b', 0})}, 1, -1},
		{"omap-reads-empty", []readCompoundOp{rcKeys("", 8), rcVals("", "", 8), rcByKeys("k1"), rcOmapCmp("k1", nil, cephmsgr.OSDOmapEqual)}, 0, -1},
		{"stat-replay-eopnotsupp", []readCompoundOp{rcStat(), rcCall("hello", "replay", nil, nil)}, -95, 1},
		{"xattrs-object-prefix-enoent", []readCompoundOp{rcXattrs(), rcCall("rbd", "get_object_prefix", nil, nil), rcCall("hello", "say_hello", []byte("z"), rcHello("z"))}, -2, 1},
		{"read-cmpxattr-false-assert", []readCompoundOp{rcRead(0, 16), rcCmpXattr("a", cephmsgr.OSDXattrEqual, []byte("wrong")), rcAssert(v)}, -125, 1},
		{"read-cmpext-mismatch-read", []readCompoundOp{rcRead(0, 8), rcCmpExt(16, wrong), rcRead(8, 8)}, -4098, 1},
		{"assert-lower-read", []readCompoundOp{rcAssert(v - 1), rcRead(0, 8)}, -34, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops := make([]cephmsgr.OSDOperation, len(tc.ops))
			for i, op := range tc.ops {
				ops[i] = op.op
			}
			reply, err := o.Request(ctx, replicatedPGRequest(route, ops...))
			native := nativeReadCompoundRun(t, ctx, 1, object, tc.ops)
			t.Logf("Go %s; native aggregate %d", rcSummary(tc.ops, reply), native.Native.Aggregate)
			checkReadCompound(t, tc.name, tc.ops, tc.code, tc.failing, model, reply, err, native)
		})
		if t.Failed() {
			return
		}
	}
	t.Run("omap-write-eopnotsupp", func(t *testing.T) {
		set := cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: "k1", Value: []byte("v1")}}}}
		r := replicatedPGRequest(route, set)
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000800}
		reply, err := o.Request(ctx, r)
		var server *cephmsgr.OSDError
		if !errors.As(err, &server) || server.Code != -95 || reply.Code != -95 || len(reply.Operations) != 1 || reply.Operations[0].Code != cephmsgr.OSDSetOmapValues {
			t.Fatal("an EC OMAP write was not refused with -95", reply.Code, err)
		}
	})
	t.Run("re-execution-doubles-a-call-natively", func(t *testing.T) {
		say := rcCall("hello", "say_hello", []byte("ec"), rcHello("ec"))
		for _, tc := range []struct {
			name  string
			ops   []readCompoundOp
			times int
		}{
			{"read-then-call", []readCompoundOp{rcRead(0, 8), say}, 2},
			{"call-then-cmpext", []readCompoundOp{say, rcCmpExt(16, data[16:20])}, 2},
			{"cmpext-then-call", []readCompoundOp{rcCmpExt(16, data[16:20]), say}, 1},
			{"call-cmpext-read", []readCompoundOp{say, rcCmpExt(16, data[16:20]), rcRead(0, 8)}, 3},
			{"stat-then-call", []readCompoundOp{rcStat(), say}, 1},
		} {
			n := nativeReadCompoundRun(t, ctx, 1, object, tc.ops)
			call := n.Native.Operations[slices.IndexFunc(tc.ops, func(op readCompoundOp) bool { return op.op.Code == cephmsgr.OSDCall })]
			if n.Native.Aggregate != 0 || call.Prval == nil || *call.Prval != 0 || !bytes.Equal(call.Output, bytes.Repeat(rcHello("ec"), tc.times)) {
				t.Fatalf("%s: native say_hello output %q, want it %d times", tc.name, call.Output, tc.times)
			}
			t.Logf("%s: native C read op returned say_hello's output %d times", tc.name, tc.times)
		}
	})
	t.Run("client-refused-shapes-not-sent", func(t *testing.T) {
		baseline := replicatedPGRequest(route, rcRead(0, 8).op, rcAssert(v).op)
		first, err := o.Request(ctx, baseline)
		if err != nil {
			t.Fatal(err)
		}
		say := rcCall("hello", "say_hello", []byte("ec"), nil).op
		getxattr := cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "a", MaxOutputBytes: 64}}
		for name, tc := range map[string]struct {
			ops  []cephmsgr.OSDOperation
			want error
		}{
			"checksum-read":    {[]cephmsgr.OSDOperation{rcChecksum(2, 0, 16, 8, 1).op, rcRead(0, 8).op}, nil},
			"sparse-read-read": {[]cephmsgr.OSDOperation{{Code: cephmsgr.OSDSparseRead, Length: 16, SparseRead: cephmsgr.OSDSparseReadInput{MaxOutputBytes: 4096}}, rcRead(0, 8).op}, nil},
			"read-stat":        {[]cephmsgr.OSDOperation{rcRead(0, 32).op, rcStat().op}, cephmsgr.ErrUnsupportedOSDOperation},
			"stat-read":        {[]cephmsgr.OSDOperation{rcStat().op, rcRead(0, 64).op}, cephmsgr.ErrUnsupportedOSDOperation},
			"xattrs-read":      {[]cephmsgr.OSDOperation{rcXattrs().op, rcRead(0, 16).op}, cephmsgr.ErrUnsupportedOSDOperation},
			"cmpext-getxattr":  {[]cephmsgr.OSDOperation{rcCmpExt(16, data[16:20]).op, getxattr}, cephmsgr.ErrUnsupportedOSDOperation},
			"read-omap-keys":   {[]cephmsgr.OSDOperation{rcRead(0, 8).op, rcKeys("", 8).op}, cephmsgr.ErrUnsupportedOSDOperation},
			"read-say-hello":   {[]cephmsgr.OSDOperation{rcRead(0, 8).op, say}, cephmsgr.ErrUnsupportedOSDOperation},
		} {
			reply, err := o.Request(ctx, replicatedPGRequest(route, tc.ops...))
			var server *cephmsgr.OSDError
			var unknown *cephmsgr.OutcomeUnknownError
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) || errors.As(err, &server) || errors.As(err, &unknown) || reply.Submission != (cephmsgr.OSDSubmission{}) || reply.Transaction != 0 {
				t.Fatal("a client-refused EC compound was transmitted", name, reply.Submission, err)
			}
		}
		next, err := o.Request(ctx, baseline)
		if err != nil || next.Transaction != first.Transaction+1 || !bytes.Equal(next.RawData, first.RawData) {
			t.Fatal("a refusal consumed a TID or broke the connection", first.Transaction, next.Transaction, err)
		}
	})
	if after := poolObserve(t, ctx, 1, object, "", ""); after.Code != 0 || after.Version != before.Version || !bytes.Equal(after.Data, before.Data) || after.Seconds != before.Seconds || after.Nanos != before.Nanos || !sameXattrMaps(rcNativeAttrs(after.Attributes), rcNativeAttrs(before.Attributes)) {
		t.Fatal("EC read compounds changed the native head")
	}
}
