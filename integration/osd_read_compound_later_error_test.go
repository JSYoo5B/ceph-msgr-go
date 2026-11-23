package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// TestCephOSDReadCompoundLaterErrorIntegration sends admitted read-only
// compounds in which a later operation fails natively, after earlier reads
// produced output and before further operations: a false CMPXATTR and
// OMAP_CMP (-ECANCELED), ASSERT_VER below and above the object's version
// (-ERANGE, -EOVERFLOW), a CMPEXT mismatch (-MAX_ERRNO minus the mismatch
// index), and read-mode class methods that fail: cls_hello say_hello with
// more than 100 bytes of input (-EINVAL), lock.assert_locked on an unlocked
// object (-EBUSY) and rbd.get_object_prefix on an object without that OMAP
// key (-ENOENT). The checksum subset gets ASSERT_VER below the version after
// a CHECKSUM and a READ (-ERANGE). One owned replicated head is seeded
// natively with bytes, xattrs and OMAP entries.
//
// Each vector is also sent as one librados C read op of the same operations
// in the same order (tools/osd_pool_mode_readcompound_oracle.py). The Go
// aggregate must equal the native aggregate and the pinned source's code
// (PrimaryLogPG.cc do_osd_ops at v20.2.4 stops at the first negative result);
// every operation before the failing one must have its source-derived result
// and the native callback's, and its output (bytes, stat, xattr map, OMAP
// page, method output, checksum values) must equal the native output and the
// seed; the failing operation has the code and no output; every later one
// has result 0 and no output in Go, and the native buffers of those are left
// unfilled. Two vectors that succeed (every read kind together, and a
// successful rbd.get_object_prefix after reads on a second head) bound the
// model. In the first a true CMPXATTR's result 1 is carried, in Go and in
// the native callbacks, into the per-op results of the OMAP reads and the
// OMAP_CMP after it and into the aggregate: the pinned do_osd_ops keeps one
// running result, which those operations set only when they fail. Afterwards the native bytes, version, mtime, xattrs and OMAP of both
// heads are unchanged, and a fresh session reads the seeded bytes.
//
// The SPARSE_READ/READ compound has no such case: on a replicated BlueStore
// primary the pinned do_read and do_sparse_read fail only when the object
// store's read fails, and that -EIO is turned into a repair of the primary
// copy (rep_repair_primary_object), which parks the request instead of
// answering it, so no stock daemon answers that compound with a later
// operation's error. Not verified: EC (TestCephOSDErasureOptimizedK2M2ReadCompoundIntegration),
// FAILOK, injected store errors and faults.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-replicated-pg-reply-v8.t-osdreadcompoundlatererror key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=replicated-pg-reply-v8
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-replicated-pg-request-v9-reply-v8.t-osdreadcompoundlatererror key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=replicated-pg-request-v9-reply-v8
//scenario:covers CLIENT to OSD
//fixture:shareable read-only vectors on two heads of one owned pool; checked owned-pool cleanup removes it
func TestCephOSDReadCompoundLaterErrorIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	if base.PoolType == 3 {
		t.Skip("the replicated read compounds; EC is TestCephOSDErasureOptimizedK2M2ReadCompoundIntegration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	const main, prefixed = "read-compound-later-main", "read-compound-later-prefixed"
	data := make([]byte, 64)
	for i := range data {
		data[i] = byte(i*37 + 11)
	}
	model := seedReadCompound(t, ctx, 1, main, data,
		map[string][]byte{"a": []byte("alpha"), "b": {0, 255, 'b', 0}},
		map[string][]byte{"k1": []byte("v1"), "k2": {0, 'v', '2'}, "k3": []byte("v3")})
	// cls_rbd read_key decodes the OMAP value as an encoded string.
	objectPrefix := append([]byte{12, 0, 0, 0}, "rbd_data.rc1"...)
	second := seedReadCompound(t, ctx, 1, prefixed, []byte{}, map[string][]byte{}, map[string][]byte{"object_prefix": objectPrefix})
	before := map[string]omapWriteState{main: poolOmapState(t, ctx, 1, main), prefixed: poolOmapState(t, ctx, 1, prefixed)}

	route := poolPlace(t, ctx, 1, main, "")
	o := mutationConnection(t, c, ctx, route)
	defer o.Close()
	routeSecond := poolPlace(t, ctx, 1, prefixed, "")
	oSecond := mutationConnection(t, c, ctx, routeSecond)
	defer oSecond.Close()

	v := model.version
	wrong := bytes.Clone(data[4:8])
	wrong[1] ^= 0xff
	long := bytes.Repeat([]byte{'x'}, 101)
	cases := []struct {
		name    string
		head    readCompoundModel
		ops     []readCompoundOp
		code    int32
		failing int
	}{
		{"every-read-kind-succeeds", model, []readCompoundOp{rcRead(0, 16), rcStat(), rcXattrs(), rcCmpXattr("a", cephmsgr.OSDXattrEqual, []byte("alpha")),
			rcKeys("", 8), rcVals("k1", "k", 8), rcByKeys("k2", "k9"), rcOmapCmp("k1", []byte("v1"), cephmsgr.OSDOmapEqual)}, 1, -1},
		{"cmpxattr-false-after-read-stat", model, []readCompoundOp{rcRead(0, 16), rcStat(), rcCmpXattr("a", cephmsgr.OSDXattrEqual, []byte("wrong")), rcRead(16, 16)}, -125, 2},
		{"assert-lower-after-xattrs-read", model, []readCompoundOp{rcXattrs(), rcRead(8, 24), rcAssert(v - 1), rcStat()}, -34, 2},
		{"assert-higher-after-xattrs", model, []readCompoundOp{rcXattrs(), rcAssert(v + 1), rcRead(0, 8)}, -75, 1},
		{"omapcmp-false-after-vals-read", model, []readCompoundOp{rcVals("", "", 8), rcRead(0, 4), rcOmapCmp("k1", []byte("nope"), cephmsgr.OSDOmapEqual), rcXattrs()}, -125, 2},
		{"cmpext-mismatch-after-match-read", model, []readCompoundOp{rcCmpExt(0, data[:4]), rcRead(32, 8), rcCmpExt(4, wrong), rcStat()}, -4096, 2},
		{"say-hello-einval-after-read", model, []readCompoundOp{rcRead(0, 8), rcCall("hello", "say_hello", long, nil), rcCall("hello", "say_hello", []byte("x"), rcHello("x"))}, -22, 1},
		{"assert-locked-ebusy-after-omap", model, []readCompoundOp{rcStat(), rcByKeys("k2", "k9"), rcCall("lock", "assert_locked", rbdLockAssertInput("ceph-msgr-read-compound", ""), nil),
			rcCall("hello", "say_hello", nil, rcHello(""))}, -16, 2},
		{"object-prefix-enoent-after-reads", model, []readCompoundOp{rcRead(0, 8), rcKeys("", 8), rcCall("rbd", "get_object_prefix", nil, nil), rcCall("hello", "say_hello", []byte("y"), rcHello("y"))}, -2, 2},
		{"checksum-then-assert-lower", model, []readCompoundOp{rcChecksum(2, 0, 16, 8, 1), rcRead(0, 8), rcAssert(v - 1), rcChecksum(1, 0, 8, 0, 2)}, -34, 2},
		{"object-prefix-succeeds-after-reads", second, []readCompoundOp{rcVals("", "", 4), rcRead(0, 8), rcCall("rbd", "get_object_prefix", nil, objectPrefix)}, 0, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, r := o, route
			if tc.head.object == prefixed {
				conn, r = oSecond, routeSecond
			}
			ops := make([]cephmsgr.OSDOperation, len(tc.ops))
			for i, op := range tc.ops {
				ops[i] = op.op
			}
			reply, err := conn.Request(ctx, pools.request(r, ops...))
			native := nativeReadCompoundRun(t, ctx, 1, tc.head.object, tc.ops)
			t.Logf("Go %s; native aggregate %d", rcSummary(tc.ops, reply), native.Native.Aggregate)
			checkReadCompound(t, tc.name, tc.ops, tc.code, tc.failing, tc.head, reply, err, native)
			if tc.code < 0 {
				// The same connection serves the next request.
				next, err := conn.Request(ctx, pools.request(r, rcRead(0, 4).op))
				if err != nil || next.Transaction != reply.Transaction+1 || !bytes.Equal(next.Operations[0].Data, tc.head.data[:min(4, len(tc.head.data))]) {
					t.Fatal("a later-op failure broke the connection", err)
				}
			}
		})
		if t.Failed() {
			return
		}
	}
	for _, name := range []string{main, prefixed} {
		after := poolOmapState(t, ctx, 1, name)
		b := before[name]
		if after.Code != 0 || after.Version != b.Version || after.Seconds != b.Seconds || after.Nanos != b.Nanos || !bytes.Equal(after.Data, b.Data) ||
			!sameXattrMaps(callXattrs(after), callXattrs(b)) || !sameOmap(t, after, b) {
			t.Fatal("read compounds changed a native head", name)
		}
	}
	options := integrationOptions(t)
	options.EnableOSD = true
	if isReplicatedPGFixtureProfile() {
		options.OSDProfile = replicatedPGFixtureOSDProfile(t)
	}
	fresh, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	again := mutationConnection(t, fresh, ctx, route)
	defer again.Close()
	reply, err := again.Request(ctx, pools.request(route, rcRead(0, 64).op))
	if err != nil || !bytes.Equal(reply.Operations[0].Data, data) || reply.UserVersion != model.version {
		t.Fatal("a fresh session does not read the seeded head", err)
	}
	t.Logf("later-operation failures kept earlier outputs and left later operations untouched, Go and native; heads unchanged at version %d; scope=one native C read op per vector, convenience callbacks", model.version)
}
