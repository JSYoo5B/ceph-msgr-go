package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// nativeOpReceipt is the owned-pool oracle's mode nativeop
// (tools/osd_pool_mode_nativeop_oracle.py): one native write op, flagged
// read or rados CLI call on one owned object, between two native snapshots.
type nativeOpReceipt struct {
	NativeOp struct {
		Action    string `json:"action"`
		Aggregate *int32 `json:"aggregate"`
		Calls     []struct {
			Index int   `json:"index"`
			Prval int32 `json:"prval"`
		} `json:"calls"`
		OperateCalls int `json:"operate_calls"`
		Read         *struct {
			Aggregate int32  `json:"aggregate"`
			Data      []byte `json:"data"`
		} `json:"read"`
		Tool *struct {
			Tool   string `json:"tool"`
			Exit   int    `json:"exit"`
			Code   *int32 `json:"code"`
			Stderr string `json:"stderr"`
		} `json:"tool"`
		Before nativeXattrSnapshot `json:"before"`
		After  nativeXattrSnapshot `json:"after"`
	} `json:"native_op"`
}

// ecOp is one operation in both forms: the Go operation and the native
// write-op action of mode nativeop that sends the same operation.
type ecOp struct {
	op     cephmsgr.OSDOperation
	native map[string]any
	// echo is the opcode the OSD echoes when it differs from the one sent:
	// a ZERO that reaches the object's end is executed, and echoed, as a
	// TRUNCATE (pinned PrimaryLogPG::do_osd_ops, "munge ZERO -> TRUNCATE").
	echo cephmsgr.OSDOpcode
}

func ecWriteFull(data []byte) ecOp {
	return ecOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data}, native: map[string]any{"op": "writefull", "data": b64(data)}}
}

func ecWrite(offset uint64, data []byte) ecOp {
	return ecOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: offset, Length: uint64(len(data)), Data: data}, native: map[string]any{"op": "write", "offset": offset, "data": b64(data)}}
}

func ecAppend(data []byte) ecOp {
	return ecOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(data)), Data: data}, native: map[string]any{"op": "append", "data": b64(data)}}
}

func ecZero(offset, length uint64) ecOp {
	return ecOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDZero, Offset: offset, Length: length}, native: map[string]any{"op": "zero", "offset": offset, "length": length}}
}

func ecTruncate(offset uint64) ecOp {
	return ecOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDTruncate, Offset: offset}, native: map[string]any{"op": "truncate", "offset": offset}}
}

func ecSetXattr(name string, value []byte) ecOp {
	return ecOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: name}, Data: value},
		native: map[string]any{"op": "setxattr", "name": b64([]byte(name)), "value": b64(value)}}
}

func ecDelete() ecOp {
	return ecOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete}, native: map[string]any{"op": "delete"}}
}

func ecOmapSet(key string, value []byte) ecOp {
	return ecOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: key, Value: value}}}},
		native: map[string]any{"op": "set", "entries": []map[string]string{{"key": b64([]byte(key)), "value": b64(value)}}}}
}

func ecOmapRemove(key string) ecOp {
	return ecOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveOmapKeys, Omap: cephmsgr.OSDOmapInput{Keys: []string{key}}},
		native: map[string]any{"op": "remove", "keys": []string{b64([]byte(key))}}}
}

func ecOmapClear() ecOp {
	return ecOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDClearOmap}, native: map[string]any{"op": "clear"}}
}

func ecOmapRemoveRange(begin, end string) ecOp {
	return ecOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveOmapKeyRange, Omap: cephmsgr.OSDOmapInput{RangeBegin: begin, RangeEnd: end}},
		native: map[string]any{"op": "rmrange", "begin": b64([]byte(begin)), "end": b64([]byte(end))}}
}

// ecCallWrite is a write-mode CALL without input.
func ecCallWrite(class, method string) ecOp {
	return ecOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: cephmsgr.OSDCallWrite, MaxOutputBytes: 64}},
		native: map[string]any{"op": "call", "class": class, "method": method, "input": ""}}
}

// nativeOpWrite runs the operations as one native write op on the object of
// the owned pool in slot 1.
func nativeOpWrite(t *testing.T, ctx context.Context, object string, ops []ecOp) nativeOpReceipt {
	t.Helper()
	actions := make([]map[string]any, len(ops))
	for i, op := range ops {
		actions[i] = op.native
	}
	var r nativeOpReceipt
	nativePoolResult(t, ctx, "nativeop", map[string]any{"action": "write", "slot": 1, "object": object, "operations": actions}, &r)
	if r.NativeOp.Action != "write" || r.NativeOp.Aggregate == nil || r.NativeOp.OperateCalls != 1 {
		t.Fatal("native write-op receipt is incomplete", object)
	}
	return r
}

// sameNativeHead reports two native snapshots of one object that agree in
// presence, bytes, user version, mtime and xattrs.
func sameNativeHead(a, b nativeXattrSnapshot) bool {
	return a.Code == b.Code && a.Version == b.Version && a.Seconds == b.Seconds && a.Nanos == b.Nanos && bytes.Equal(a.Data, b.Data) &&
		sameXattrMaps(rcNativeAttrs(a.Attributes), rcNativeAttrs(b.Attributes))
}

// judgeRefusedVector checks a Go reply to a vector the pool refuses at
// operation failing with code: a signed server answer with that aggregate,
// every operation echoed in order, the failing one with the code, the others
// with result 0, and no output (pinned PrimaryLogPG::do_osd_ops stops at the
// first failing operation and records its result on it).
func judgeRefusedVector(ops []ecOp, failing int, code int32, reply cephmsgr.OSDReply, err error) error {
	var server *cephmsgr.OSDError
	var unknown *cephmsgr.OutcomeUnknownError
	if errors.As(err, &unknown) {
		return fmt.Errorf("the outcome is unknown, not a signed refusal: %v", err)
	}
	if !errors.As(err, &server) || server.Code != code || reply.Code != code {
		return fmt.Errorf("aggregate %d (error %v), want the server's %d", reply.Code, err, code)
	}
	if reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction {
		return fmt.Errorf("the refusal carries no submission of its own: %+v", reply.Submission)
	}
	if len(reply.Operations) != len(ops) {
		return fmt.Errorf("%d operations echoed, sent %d", len(reply.Operations), len(ops))
	}
	for i, got := range reply.Operations {
		want := int32(0)
		if i == failing {
			want = code
		}
		echo := ops[i].op.Code
		if ops[i].echo != 0 {
			echo = ops[i].echo
		}
		if got.Code != echo || got.Result != want || len(got.Data) != 0 {
			return fmt.Errorf("operation %d: opcode %#x result %d with %d output bytes, want opcode %#x result %d and no output", i, got.Code, got.Result, len(got.Data), echo, want)
		}
	}
	return nil
}

func ecRequest(route osdFixtureOracle, seconds uint32, ops []ecOp) cephmsgr.OSDRequest {
	list := make([]cephmsgr.OSDOperation, len(ops))
	for i, op := range ops {
		list[i] = op.op
	}
	r := replicatedPGRequest(route, list...)
	r.ModificationTime = cephmsgr.OSDTime{Seconds: seconds}
	return r
}

// erasureOwnedPool creates the owned erasure-coded pool of a restriction
// test in slot 1 and registers its checked removal.
func erasureOwnedPool(t *testing.T, ctx context.Context, width erasureWidth, optimized, overwrites bool) poolReceipt {
	t.Helper()
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
	created := nativePool(t, ctx, "create", map[string]any{"slot": 1, "pg_num": 8, "hashpspool": isReplicatedPGFixtureProfile(), "erasure": true, "width": width.name, "optimized": optimized, "plugin": "isa", "overwrites": overwrites})
	if created.PoolType != 3 || created.Size != width.size() || created.ECOptimized != optimized || created.Width != width.name || created.ErasureCodeProfile != width.profile ||
		created.ECOverwrites == nil || *created.ECOverwrites != overwrites {
		t.Fatal("pool is not the requested erasure-coded shape", created.PoolType, created.Size, created.ECOptimized, created.Width, created.ErasureCodeProfile, created.ECOverwrites)
	}
	return created
}

// TestCephOSDErasureOptimizedK2M2UnsupportedOperationsIntegration sends every
// operation an erasure-coded pool refuses that this client can put on the
// wire, on an optimized isa k=2,m=2 pool with overwrites over the four-OSD
// BlueStore topology, and compares each with the native client.
//
// The pinned PrimaryLogPG::do_osd_ops (v20.2.4) answers -EOPNOTSUPP (-95)
// for the five OMAP writes (OMAPSETVALS, OMAPSETHEADER, OMAPCLEAR,
// OMAPRMKEYS, OMAPRMKEYRANGE: pg_pool_t::supports_omap is false for an
// erasure-coded pool, and the check comes before the object is looked at),
// for MAPEXT, and for a write-mode CALL whose method reads object data
// (cls_hello turn_it_to_11: cls_cxx_read is a SYNC_READ, refused on EC). One
// head is seeded natively with 8 KiB and one xattr. Each operation is sent
// by Go to the primary shard, alone and beside a WRITEFULL, a SETXATTR or an
// APPEND in the same vector, and to an absent object. Required each time: a
// signed server refusal under its own submission, never an unknown outcome;
// aggregate -95; every operation echoed, the refused one with -95 and the
// others with result 0 and no output; and the native head unchanged in
// bytes, xattrs, user version and mtime, an absent object staying absent,
// so the mutation staged before the refused operation was not applied. The
// native client then sends the same vector as one librados C write op to the
// same object (OMAPSETHEADER and MAPEXT through the pinned rados CLI, which
// is the only native builder for them) and must get the same code and leave
// the same head. These are the pool's restrictions; the client sends the
// operations unchanged and advertises nothing for them.
//
// Refused by the client before submission, with the transaction ID not
// consumed: BalanceReads and LocalizeReads with the erasure-coded placement,
// on the connection to the primary and on a connection the caller opened to
// the OSD of another shard. Nothing reaches that OSD: a request with either
// flag is never built for an erasure-coded shard. The native client's read
// with the same flag returns the object's bytes; which OSD served it is not
// observed here (the pinned Objecter::_calc_target picks a replica only on
// a replicated pool). Every owned OSD keeps its up_from across the test.
//
// Not verified: a request on the object-hash profile, which names no
// placement and so cannot be refused locally; TMAP and SYNC_READ, which this
// client has no opcode for; SET_CHUNK, the tier and cache operations, which
// the client refuses for an erasure-coded placement in their own contracts;
// other plugins; a degraded PG.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2unsupportedoperations key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:shareable refused vectors on two objects of an owned EC pool; checked cleanup removes the pool and its profile
func TestCephOSDErasureOptimizedK2M2UnsupportedOperationsIntegration(t *testing.T) {
	erasureUnsupportedOperations(t, erasureK2M2, true)
}

// The same operations and refusals on an optimized isa k=3,m=1 pool, whose
// primary shares the PG with two shards that may not lead.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk3m1unsupportedoperations key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:shareable refused vectors on two objects of an owned EC pool; checked cleanup removes the pool and its profile
func TestCephOSDErasureOptimizedK3M1UnsupportedOperationsIntegration(t *testing.T) {
	erasureUnsupportedOperations(t, erasureK3M1, true)
}

// The same operations and refusals on an optimized isa k=4,m=2 pool over the
// six-OSD topology.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2unsupportedoperations key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six
//scenario:covers CLIENT to OSD
//fixture:shareable refused vectors on two objects of an owned EC pool; checked cleanup removes the pool and its profile
func TestCephOSDErasureOptimizedK4M2UnsupportedOperationsIntegration(t *testing.T) {
	erasureUnsupportedOperations(t, erasureK4M2, true)
}

// The same operations and refusals on an isa k=2,m=2 pool with overwrites
// and without allow_ec_optimizations: the restrictions belong to the pool
// type, not to the optimized write path.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasurek2m2unsupportedoperations key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:shareable refused vectors on two objects of an owned EC pool; checked cleanup removes the pool and its profile
func TestCephOSDErasureK2M2UnsupportedOperationsIntegration(t *testing.T) {
	erasureUnsupportedOperations(t, erasureK2M2, false)
}

func erasureUnsupportedOperations(t *testing.T, width erasureWidth, optimized bool) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != width.topology || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" || !isReplicatedPGFixtureProfile() {
		t.Skipf("%s needs the BlueStore OSD topology %q and a replicated-PG profile", width, width.topology)
	}
	c, _, _, _ := replicatedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	created := erasureOwnedPool(t, ctx, width, optimized, true)
	facts := erasurePoolMapFacts(t, ctx, c, created, width, optimized)
	const object, absent = "ec-unsupported", "ec-unsupported-absent"
	data := make([]byte, 8192)
	for i := range data {
		data[i] = byte(i*31 + 7)
	}
	poolSeed(t, ctx, 1, object, "", data, []nativeXattrEntry{{Name: []byte("a"), Value: []byte("alpha")}})
	route := poolPlace(t, ctx, 1, object, "")
	facts.checkPlacement(t, route)
	gone := poolPlace(t, ctx, 1, absent, "")
	facts.checkPlacement(t, gone)
	upFrom := erasureOSDUpFrom(t, ctx, c)
	connections := map[int32]*cephmsgr.OSDConnection{}
	connection := func(r osdFixtureOracle) *cephmsgr.OSDConnection {
		if connections[r.ID] == nil {
			connections[r.ID] = mutationConnection(t, c, ctx, r)
			t.Cleanup(func() { connections[r.ID].Close() })
		}
		return connections[r.ID]
	}
	replaced := bytes.Repeat([]byte{0xa5}, 4096)
	cases := []struct {
		name    string
		object  string
		ops     []ecOp
		failing int
		// tool names the rados CLI command that is the native form of a
		// single operation the C write op cannot carry.
		tool, value string
	}{
		{name: "omap-set", object: object, ops: []ecOp{ecOmapSet("k", []byte("v"))}},
		{name: "omap-set-header", object: object, ops: []ecOp{{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: []byte("hdr")}}}}, tool: "setomapheader", value: "hdr"},
		{name: "omap-clear", object: object, ops: []ecOp{ecOmapClear()}},
		{name: "omap-remove-keys", object: object, ops: []ecOp{ecOmapRemove("k")}},
		{name: "omap-remove-range", object: object, ops: []ecOp{ecOmapRemoveRange("a", "z")}},
		{name: "writefull-then-omap-set", object: object, ops: []ecOp{ecWriteFull(replaced), ecOmapSet("k", []byte("v"))}, failing: 1},
		{name: "setxattr-then-omap-clear", object: object, ops: []ecOp{ecSetXattr("b", []byte("beta")), ecOmapClear()}, failing: 1},
		{name: "omap-remove-keys-then-append", object: object, ops: []ecOp{ecOmapRemove("k"), ecAppend([]byte("tail"))}},
		{name: "call-write-reading-data", object: object, ops: []ecOp{ecCallWrite("hello", "turn_it_to_11")}},
		{name: "omap-set-absent", object: absent, ops: []ecOp{ecOmapSet("k", []byte("v"))}},
		{name: "omap-clear-absent", object: absent, ops: []ecOp{ecOmapClear()}},
		{name: "writefull-then-omap-set-absent", object: absent, ops: []ecOp{ecWriteFull(replaced), ecOmapSet("k", []byte("v"))}, failing: 1},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target := route
			if tc.object == absent {
				target = gone
			}
			before := poolObserve(t, ctx, 1, tc.object, "", "")
			if (before.Code == -2) != (tc.object == absent) {
				t.Fatal("the object's presence before the vector differs", before.Code)
			}
			reply, err := connection(target).Request(ctx, ecRequest(target, 1700004000+uint32(i), tc.ops))
			if err := judgeRefusedVector(tc.ops, tc.failing, -95, reply, err); err != nil {
				t.Fatal("Go:", err)
			}
			if after := poolObserve(t, ctx, 1, tc.object, "", ""); !sameNativeHead(before, after) {
				t.Fatalf("the refused Go vector changed the native head: code %d version %d size %d, before code %d version %d size %d", after.Code, after.Version, len(after.Data), before.Code, before.Version, len(before.Data))
			}
			var code int32
			var n nativeOpReceipt
			if tc.tool != "" {
				nativePoolResult(t, ctx, "nativeop", map[string]any{"action": "tool", "slot": 1, "object": tc.object, "tool": tc.tool, "value": tc.value}, &n)
				if n.NativeOp.Tool == nil || n.NativeOp.Tool.Code == nil || n.NativeOp.Tool.Exit == 0 {
					t.Fatalf("the native rados %s was not refused with a readable code: %+v", tc.tool, n.NativeOp.Tool)
				}
				code = *n.NativeOp.Tool.Code
			} else {
				n = nativeOpWrite(t, ctx, tc.object, tc.ops)
				code = *n.NativeOp.Aggregate
			}
			if code != -95 || !sameNativeHead(before, n.NativeOp.Before) || !sameNativeHead(before, n.NativeOp.After) {
				t.Fatalf("native: code %d, head after code %d version %d size %d; want -95 and the head unchanged at version %d", code, n.NativeOp.After.Code, n.NativeOp.After.Version, len(n.NativeOp.After.Data), before.Version)
			}
			t.Logf("Go and native: -95 at operation %d of %d, head unchanged (code %d, version %d)", tc.failing, len(tc.ops), before.Code, before.Version)
		})
		if t.Failed() {
			return
		}
	}
	t.Run("mapext", func(t *testing.T) {
		before := poolObserve(t, ctx, 1, object, "", "")
		ops := []ecOp{{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDMapExt, Length: uint64(len(data)), MapExt: cephmsgr.OSDMapExtInput{MaxOutputBytes: 4096}}}}
		reply, err := connection(route).Request(ctx, replicatedPGRequest(route, ops[0].op))
		if err := judgeRefusedVector(ops, 0, -95, reply, err); err != nil {
			t.Fatal("Go:", err)
		}
		var n nativeOpReceipt
		nativePoolResult(t, ctx, "nativeop", map[string]any{"action": "tool", "slot": 1, "object": object, "tool": "mapext", "value": ""}, &n)
		if n.NativeOp.Tool == nil || n.NativeOp.Tool.Code == nil || *n.NativeOp.Tool.Code != -95 || !sameNativeHead(before, n.NativeOp.After) {
			t.Fatalf("native rados mapext: %+v", n.NativeOp.Tool)
		}
	})
	t.Run("replica-directed-reads-not-sent", func(t *testing.T) {
		o := connection(route)
		read := cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 64}
		first, err := o.Request(ctx, replicatedPGRequest(route, read))
		if err != nil || !bytes.Equal(first.Operations[0].Data, data[:64]) {
			t.Fatal("plain read at the primary", err)
		}
		// The first position of the acting set the primary does not hold.
		other := route.Acting[(int(route.Shard)+1)%len(route.Acting)]
		elsewhere := poolPlaceOn(t, ctx, 1, object, "", &other)
		if elsewhere.ID != other || elsewhere.ID == route.ID || elsewhere.PrimaryID != route.ID {
			t.Fatalf("native placement of osd.%d in the acting set differs: %+v", other, elsewhere)
		}
		away := connection(elsewhere)
		// Ordered: the requests naming the primary come first, so a client
		// that sent one would fail here before any reaches another shard.
		for _, tc := range []struct {
			name              string
			on                *cephmsgr.OSDConnection
			route             osdFixtureOracle
			op                cephmsgr.OSDOperation
			balance, localize bool
		}{
			{"balance read at the primary", o, route, read, true, false},
			{"localize stat at the primary", o, route, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, false, true},
			{"both flags at the primary", o, route, read, true, true},
			{"balance read at another shard's OSD", away, elsewhere, read, true, false},
			{"localize read at another shard's OSD", away, elsewhere, read, false, true},
		} {
			r := replicatedPGRequest(tc.route, tc.op)
			r.Placement.Primary = route.ID
			r.BalanceReads, r.LocalizeReads = tc.balance, tc.localize
			reply, err := tc.on.Request(ctx, r)
			var server *cephmsgr.OSDError
			var unknown *cephmsgr.OutcomeUnknownError
			if !errors.Is(err, cephmsgr.ErrUnsupportedOSDOperation) || errors.As(err, &server) || errors.As(err, &unknown) || reply.Submission != (cephmsgr.OSDSubmission{}) || reply.Transaction != 0 {
				t.Fatalf("%s: an erasure-coded replica-directed read was transmitted: submission %+v, error %v", tc.name, reply.Submission, err)
			}
		}
		next, err := o.Request(ctx, replicatedPGRequest(route, read))
		if err != nil || next.Transaction != first.Transaction+1 || !bytes.Equal(next.RawData, first.RawData) {
			t.Fatal("a refusal consumed a transaction ID or broke the connection", first.Transaction, next.Transaction, err)
		}
		for _, flag := range []string{"balance", "localize"} {
			var n nativeOpReceipt
			nativePoolResult(t, ctx, "nativeop", map[string]any{"action": "read", "slot": 1, "object": object, "offset": 0, "length": 64, "flag": flag}, &n)
			if n.NativeOp.Read == nil || n.NativeOp.Read.Aggregate < 0 || !bytes.Equal(n.NativeOp.Read.Data, data[:64]) {
				t.Fatalf("native read with %s: %+v", flag, n.NativeOp.Read)
			}
		}
	})
	if after := erasureOSDUpFrom(t, ctx, c); !maps.Equal(upFrom, after) {
		t.Fatal("an owned OSD restarted or left during the test", upFrom, after)
	}
	if final := poolObserve(t, ctx, 1, object, "", ""); !bytes.Equal(final.Data, data) || !sameXattrMaps(rcNativeAttrs(final.Attributes), map[string][]byte{"a": []byte("alpha")}) {
		t.Fatal("the seeded head differs at the end")
	}
	if final := poolObserve(t, ctx, 1, absent, "", ""); final.Code != -2 {
		t.Fatal("the absent object exists at the end", final.Code)
	}
}

// erasureOSDUpFrom reads, through the Go client's own MON session, the
// up_from epoch of every OSD that is up and in: a daemon that restarted or
// left shows a different epoch or is missing.
func erasureOSDUpFrom(t *testing.T, ctx context.Context, c *cephmsgr.Client) map[int32]uint32 {
	t.Helper()
	dump, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"osd dump","format":"json"}`)})
	if err != nil {
		t.Fatal("authenticated osd dump", err)
	}
	var osdMap struct {
		OSDs []struct {
			ID     int32  `json:"osd"`
			Up     int    `json:"up"`
			In     int    `json:"in"`
			UpFrom uint32 `json:"up_from"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(dump.Data, &osdMap); err != nil || len(osdMap.OSDs) == 0 {
		t.Fatal("authenticated osd dump lists no OSDs", err)
	}
	result := map[int32]uint32{}
	for _, osd := range osdMap.OSDs {
		if osd.Up == 1 && osd.In == 1 {
			result[osd.ID] = osd.UpFrom
		}
	}
	return result
}
