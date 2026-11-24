package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// TestCephOSDErasureK2M2AlignedAppendIntegration runs one write sequence on
// an isa k=2,m=2 pool that was left without allow_ec_overwrites, over the
// four-OSD BlueStore topology. Such a pool takes data only at the end of an
// object and only at a stripe boundary (pg_pool_t::requires_aligned_append,
// alignment stripe_width; pinned PrimaryLogPG::do_osd_ops, v20.2.4):
//
//   - WRITE and APPEND are refused with -EOPNOTSUPP (-95) when the offset is
//     not a multiple of stripe_width, when it is not the object's size, and
//     when an absent object is written anywhere but at offset 0;
//   - TRUNCATE is refused with -95 whatever its size, also on an absent
//     object; ZERO is refused with -95 inside the object and on an absent
//     object, and, because a ZERO that reaches the object's end is executed
//     and echoed as a TRUNCATE, there too; a ZERO that starts at or past the
//     end is a no-op answered 0 before the pool is consulted;
//   - WRITEFULL (a truncate to 0 and a write at 0), a WRITE at offset 0 of an
//     absent object, an APPEND while the size is on a stripe boundary, a WRITE
//     at that boundary, SETXATTR and DELETE are applied.
//
// The stripe width is read from the native map, not assumed. Every step is
// sent by Go to the primary shard on one object and by the native client as
// one librados C write op on a twin object, in the same order. A refused
// step must be a signed -95 under its own submission with per-operation
// results (the refused operation -95, the others 0), leave the Go object's
// native head unchanged in bytes, xattrs, user version and mtime, and get
// -95 natively with the twin unchanged; this includes a vector that stages a
// SETXATTR before the refused ZERO. An applied step must be ONDISK, advance
// the user version, and leave both objects with the same bytes and xattrs.
// The no-op must be answered 0 by both and leave both heads unchanged.
// The client sends every operation unchanged; the refusals are the pool's.
//
// Not verified: an optimized pool (allow_ec_optimizations needs overwrites),
// WRITESAME, CREATE, ROLLBACK and snapshots on such a pool, other plugins,
// and a degraded PG.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasurek2m2alignedappend key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:shareable writes to three objects of an owned EC pool without overwrites; checked cleanup removes the pool and its profile
func TestCephOSDErasureK2M2AlignedAppendIntegration(t *testing.T) {
	erasureAlignedAppend(t, erasureK2M2)
}

// The same sequence on an isa k=4,m=2 pool without overwrites over the
// six-OSD topology, whose stripe is twice as wide.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasurek4m2alignedappend key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six
//scenario:covers CLIENT to OSD
//fixture:shareable writes to three objects of an owned EC pool without overwrites; checked cleanup removes the pool and its profile
func TestCephOSDErasureK4M2AlignedAppendIntegration(t *testing.T) {
	erasureAlignedAppend(t, erasureK4M2)
}

// alignedAppendStep is one step of the sequence and what the pinned pool
// does with it: it applies it, answers 0 without changing anything (noop),
// or refuses it at operation failing.
type alignedAppendStep struct {
	name          string
	ops           []ecOp
	applied, noop bool
	failing       int
}

// alignedAppendSteps is the sequence for a stripe of the given width, on an
// object that is absent at the start. The sizes in the names follow from the
// applied steps before them.
func alignedAppendSteps(stripe uint64) []alignedAppendStep {
	fill := func(seed byte, n uint64) []byte {
		data := make([]byte, n)
		for i := range data {
			data[i] = seed + byte(i*13)
		}
		return data
	}
	zeroToEnd := ecZero(stripe, 2*stripe)
	zeroToEnd.echo = cephmsgr.OSDTruncate
	return []alignedAppendStep{
		{name: "zero of an absent object", ops: []ecOp{ecZero(0, 16)}},
		{name: "truncate of an absent object", ops: []ecOp{ecTruncate(0)}},
		{name: "write at a stripe boundary of an absent object", ops: []ecOp{ecWrite(stripe, fill(1, 64))}},
		{name: "write inside the first stripe of an absent object", ops: []ecOp{ecWrite(100, fill(2, 64))}},
		{name: "write at offset 0 of an absent object", ops: []ecOp{ecWrite(0, fill(3, stripe))}, applied: true},
		{name: "append at size one stripe", ops: []ecOp{ecAppend(fill(4, stripe))}, applied: true},
		{name: "write at the end, two stripes", ops: []ecOp{ecWrite(2*stripe, fill(5, stripe/2))}, applied: true},
		{name: "append at an unaligned size", ops: []ecOp{ecAppend(fill(6, 10))}},
		{name: "write at the unaligned end", ops: []ecOp{ecWrite(2*stripe+stripe/2, fill(7, stripe/2))}},
		{name: "overwrite of the first stripe", ops: []ecOp{ecWrite(0, fill(8, stripe))}},
		{name: "write at a boundary past the end", ops: []ecOp{ecWrite(3*stripe, fill(9, stripe))}},
		{name: "zero inside the object", ops: []ecOp{ecZero(0, 16)}},
		{name: "zero reaching the end", ops: []ecOp{zeroToEnd}},
		{name: "zero starting at the end", ops: []ecOp{ecZero(2*stripe+stripe/2, 16)}, noop: true},
		{name: "zero past the end", ops: []ecOp{ecZero(4*stripe, stripe)}, noop: true},
		{name: "truncate to one stripe", ops: []ecOp{ecTruncate(stripe)}},
		{name: "truncate to zero", ops: []ecOp{ecTruncate(0)}},
		{name: "setxattr", ops: []ecOp{ecSetXattr("a", []byte("alpha"))}, applied: true},
		{name: "setxattr then zero", ops: []ecOp{ecSetXattr("b", []byte("beta")), ecZero(0, 16)}, failing: 1},
		{name: "writefull of an unaligned length", ops: []ecOp{ecWriteFull(fill(10, stripe+100))}, applied: true},
		{name: "append after the unaligned writefull", ops: []ecOp{ecAppend(fill(11, stripe))}},
		{name: "writefull of two stripes", ops: []ecOp{ecWriteFull(fill(12, 2*stripe))}, applied: true},
		{name: "append at size two stripes", ops: []ecOp{ecAppend(fill(13, 7))}, applied: true},
		{name: "delete", ops: []ecOp{ecDelete()}, applied: true},
	}
}

func erasureAlignedAppend(t *testing.T, width erasureWidth) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != width.topology || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" || !isReplicatedPGFixtureProfile() {
		t.Skipf("%s needs the BlueStore OSD topology %q and a replicated-PG profile", width, width.topology)
	}
	c, _, _, _ := replicatedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	created := erasureOwnedPool(t, ctx, width, false, false)
	stripe := erasureStripeWithoutOverwrites(t, ctx, c, created, width)
	const object, twin = "ec-append", "ec-append-native"
	route := poolPlace(t, ctx, 1, object, "")
	if route.Pool != created.PoolID || route.PoolType != 3 || route.ID != route.PrimaryID || len(route.Acting) != width.size() || route.Shard < 0 || route.Acting[route.Shard] != route.ID {
		t.Fatalf("native placement is not a full primary-shard placement of the owned pool: %+v", route)
	}
	o := mutationConnection(t, c, ctx, route)
	defer o.Close()
	t.Logf("%s pool %d without overwrites, stripe_width %d, pg %s acting %v primary osd.%d shard %d", width, created.PoolID, stripe, route.PGID, route.Acting, route.ID, route.Shard)
	for i, step := range alignedAppendSteps(stripe) {
		t.Run(step.name, func(t *testing.T) {
			before := poolObserve(t, ctx, 1, object, "", "")
			twinBefore := poolObserve(t, ctx, 1, twin, "", "")
			reply, err := o.Request(ctx, ecRequest(route, 1700005000+uint32(i), step.ops))
			after := poolObserve(t, ctx, 1, object, "", "")
			n := nativeOpWrite(t, ctx, twin, step.ops)
			native := *n.NativeOp.Aggregate
			if !sameNativeHead(twinBefore, n.NativeOp.Before) {
				t.Fatal("the twin changed between the observation and the native write op")
			}
			if step.noop {
				if err != nil || reply.Code != 0 || native != 0 || !sameNativeHead(before, after) || !sameNativeHead(twinBefore, n.NativeOp.After) {
					t.Fatalf("a ZERO at or past the end is not a no-op answered 0: Go code %d error %v head changed %v; native %d twin changed %v",
						reply.Code, err, !sameNativeHead(before, after), native, !sameNativeHead(twinBefore, n.NativeOp.After))
				}
				return
			}
			if !step.applied {
				if err := judgeRefusedVector(step.ops, step.failing, -95, reply, err); err != nil {
					t.Fatal("Go:", err)
				}
				if !sameNativeHead(before, after) {
					t.Fatalf("the refused Go step changed the native head: code %d version %d size %d, before code %d version %d size %d", after.Code, after.Version, len(after.Data), before.Code, before.Version, len(before.Data))
				}
				if native != -95 || !sameNativeHead(twinBefore, n.NativeOp.After) {
					t.Fatalf("native: code %d, twin after code %d version %d size %d; want -95 and the twin unchanged", native, n.NativeOp.After.Code, n.NativeOp.After.Version, len(n.NativeOp.After.Data))
				}
				return
			}
			if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
				t.Fatalf("Go: an admitted step was not applied ONDISK: code %d, error %v", reply.Code, err)
			}
			if native != 0 {
				t.Fatalf("native: an admitted step was refused with %d", native)
			}
			deleted := step.ops[0].op.Code == cephmsgr.OSDDelete
			if deleted != (after.Code == -2) || deleted != (n.NativeOp.After.Code == -2) {
				t.Fatalf("presence after the step: Go object %d, twin %d", after.Code, n.NativeOp.After.Code)
			}
			if !deleted && (after.Version <= before.Version || after.Version != reply.UserVersion) {
				t.Fatalf("user version %d after the step, %d before, reply %d", after.Version, before.Version, reply.UserVersion)
			}
			if !bytes.Equal(after.Data, n.NativeOp.After.Data) || !sameXattrMaps(rcNativeAttrs(after.Attributes), rcNativeAttrs(n.NativeOp.After.Attributes)) {
				t.Fatalf("the Go object (%d bytes) and the native twin (%d bytes) differ after the step", len(after.Data), len(n.NativeOp.After.Data))
			}
		})
		if t.Failed() {
			return
		}
	}
}

// erasureStripeWithoutOverwrites reads the owned pool from the Go client's
// own MON session and returns its stripe_width, after requiring an
// erasure-coded pool of the width's size and profile that has neither
// ec_overwrites nor ec_optimizations.
func erasureStripeWithoutOverwrites(t *testing.T, ctx context.Context, c *cephmsgr.Client, created poolReceipt, width erasureWidth) uint64 {
	t.Helper()
	dump, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"osd dump","format":"json"}`)})
	if err != nil {
		t.Fatal("authenticated osd dump", err)
	}
	stripe, err := stripeWithoutOverwrites(dump.Data, created.PoolID, width)
	if err != nil {
		t.Fatal(err)
	}
	return stripe
}

func stripeWithoutOverwrites(dump []byte, pool int64, width erasureWidth) (uint64, error) {
	var osdMap struct {
		Pools []erasureMapPool `json:"pools"`
	}
	if err := json.Unmarshal(dump, &osdMap); err != nil {
		return 0, err
	}
	index := slices.IndexFunc(osdMap.Pools, func(p erasureMapPool) bool { return p.ID == pool })
	if index < 0 {
		return 0, errors.New("the owned EC pool is absent from the authenticated map")
	}
	p := osdMap.Pools[index]
	flags := strings.Split(p.FlagNames, ",")
	if p.Type != 3 || p.Size != width.size() || p.Profile != width.profile || slices.Contains(flags, "ec_overwrites") || slices.Contains(flags, "ec_optimizations") {
		return 0, errors.New("the owned pool is not the erasure-coded width without overwrites: flags " + p.FlagNames)
	}
	if p.Stripe <= 0 || p.Stripe%width.k != 0 {
		return 0, errors.New("the owned pool has no stripe_width that is a multiple of k")
	}
	return uint64(p.Stripe), nil
}

// The pool check accepts only the erasure-coded width without the overwrite
// and optimization flags, and the sequence keeps its refused and applied
// steps apart at both stripe widths.
func TestErasureAlignedAppendSequenceAndPoolCheck(t *testing.T) {
	pool := func(edit func(map[string]any)) []byte {
		p := map[string]any{"pool": 5, "type": 3, "size": 4, "min_size": 3, "erasure_code_profile": erasureK2M2.profile, "flags_names": "hashpspool", "stripe_width": 8192}
		edit(p)
		raw, err := json.Marshal(map[string]any{"pools": []any{p}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	if stripe, err := stripeWithoutOverwrites(pool(func(map[string]any) {}), 5, erasureK2M2); err != nil || stripe != 8192 {
		t.Fatal("the pool without overwrites was refused", stripe, err)
	}
	for name, edit := range map[string]func(map[string]any){
		"overwrites":      func(p map[string]any) { p["flags_names"] = "hashpspool,ec_overwrites" },
		"optimizations":   func(p map[string]any) { p["flags_names"] = "hashpspool,ec_optimizations" },
		"replicated":      func(p map[string]any) { p["type"] = 1 },
		"another width":   func(p map[string]any) { p["size"] = 3 },
		"another profile": func(p map[string]any) { p["erasure_code_profile"] = erasureK2M1.profile },
		"no stripe":       func(p map[string]any) { p["stripe_width"] = 0 },
		"odd stripe":      func(p map[string]any) { p["stripe_width"] = 4097 },
		"another pool":    func(p map[string]any) { p["pool"] = 6 },
	} {
		if _, err := stripeWithoutOverwrites(pool(edit), 5, erasureK2M2); err == nil {
			t.Fatal("a pool that is not the width without overwrites was accepted:", name)
		}
	}
	// A model of pinned do_osd_ops for the operations of the sequence must
	// agree with every step's declared outcome.
	for _, stripe := range []uint64{8192, 16384} {
		size, exists := uint64(0), false
		for _, step := range alignedAppendSteps(stripe) {
			next, present, failing := size, exists, -1
			for i, op := range step.ops {
				offset, length := op.op.Offset, uint64(len(op.op.Data))
				refused := false
				switch op.op.Code {
				case cephmsgr.OSDAppend:
					offset = next
					fallthrough
				case cephmsgr.OSDWrite:
					refused = offset%stripe != 0 || !present && offset != 0 || present && offset != next
					if !refused {
						next, present = offset+length, true
					}
				case cephmsgr.OSDZero:
					// At or past the end of an existing object: a no-op.
					refused = !(present && length == 0 && op.op.Length >= 1 && offset >= next)
					if op.echo != 0 != (present && op.op.Length >= 1 && offset < next && offset+op.op.Length >= next) {
						t.Fatal("the step's echoed opcode disagrees with the ZERO to TRUNCATE rewrite", step.name)
					}
				case cephmsgr.OSDTruncate:
					refused = true
				case cephmsgr.OSDWriteFull:
					next, present = length, true
				case cephmsgr.OSDDelete:
					next, present = 0, false
				case cephmsgr.OSDSetXattr:
					present = true
				default:
					t.Fatal("the sequence holds an operation the model does not know", step.name)
				}
				if refused {
					failing = i
					break
				}
			}
			changes := step.applied && !step.noop
			if (step.applied || step.noop) != (failing < 0) || failing >= 0 && failing != step.failing || step.noop != (failing < 0 && next == size && present == exists && step.ops[0].op.Code == cephmsgr.OSDZero) || step.applied && step.noop {
				t.Fatalf("stripe %d, step %q: the model refuses at %d, the step declares applied=%v failing=%d", stripe, step.name, failing, step.applied, step.failing)
			}
			if changes {
				size, exists = next, present
			}
		}
		if exists {
			t.Fatal("the sequence does not end with the object deleted")
		}
	}
}
