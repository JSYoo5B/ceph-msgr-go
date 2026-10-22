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

// sparseReadCompoundExpect derives one operation's result from independent
// native observations: the pinned rados mapext CLI's full allocation map and
// the C librados stat/read bytes. The native client did not execute the Go
// compound or a partial SPARSE_READ; this is an intersection model over
// native readback, labeled as such in each case log.
func sparseReadCompoundExpect(op cephmsgr.OSDOperation, full []cephmsgr.OSDExtent, data []byte) (uint64, []cephmsgr.OSDExtent, []byte) {
	size := uint64(len(data))
	if op.Code == cephmsgr.OSDRead {
		if op.Offset >= size {
			return 0, nil, nil
		}
		end := min(op.Offset+op.Length, size)
		return end - op.Offset, nil, data[op.Offset:end]
	}
	end := min(op.Offset+op.Length, size)
	extents, bytesOut := []cephmsgr.OSDExtent{}, []byte{}
	for _, e := range full {
		lo, hi := max(e.Offset, op.Offset), min(e.Offset+e.Length, end)
		if lo < hi {
			extents = append(extents, cephmsgr.OSDExtent{Offset: lo, Length: hi - lo})
			bytesOut = append(bytesOut, data[lo:hi]...)
		}
	}
	return uint64(len(bytesOut)), extents, bytesOut
}

func TestCephOSDSparseReadCompoundIntegration(t *testing.T) {
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
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
			defer stop()
			if !nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed {
				t.Error("owned SPARSE_READ compound cleanup failed")
			}
		}
	})
	baseline := bytes.Repeat([]byte{0, 255, 65, 10}, 8)
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline}, Reserve: 2})
	prepared = true
	if len(owned.Objects) != 3 || !bytes.Equal(owned.State.Data, baseline) {
		t.Fatal("native SPARSE_READ compound ownership/baseline missing")
	}
	dense, sparse, absent := owned.Objects[0], owned.Objects[1], owned.Objects[2]
	routes := map[string]osdFixtureOracle{}
	connections := map[string]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	request := func(name string, ops ...cephmsgr.OSDOperation) cephmsgr.OSDRequest {
		if isReplicatedPGFixtureProfile() {
			return replicatedPGRequest(routes[name], ops...)
		}
		return osdObjectRequest(routes[name], ops...)
	}
	for _, name := range owned.Objects {
		route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
		if route.Pool != base.Pool || route.Name != name || route.PoolType != 1 || route.Shard != -1 || route.ID != route.PrimaryID {
			t.Fatal("compound target is not an explicit replicated primary", route)
		}
		routes[name] = route
		connections[name] = mutationConnection(t, c, ctx, route)
	}
	// Explicit consumer-side writes leave holes between three allocated
	// ranges. The active store decides the allocation map; the native CLI
	// observes it independently.
	for _, offset := range []uint64{8192, 0x11000, 0x20000} {
		write := cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: offset, Length: 4, Data: []byte{0, 255, 66, byte(offset >> 12)}}
		if reply, err := connections[sparse].Request(ctx, request(sparse, write)); err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("owned sparse object write", reply, err)
		}
	}
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
	cases := []struct {
		name, object string
		ops          []cephmsgr.OSDOperation
	}{
		{"dense-mixed", dense, []cephmsgr.OSDOperation{sr(0, 16), rd(16, 16), sr(8, 8)}},
		{"dense-eof", dense, []cephmsgr.OSDOperation{sr(29, 100), rd(30, 10), sr(64, 1), rd(64, 1)}},
		{"librbd-mixed", sparse, []cephmsgr.OSDOperation{sr(0, 0x10000), rd(0x10000, 0x1000), hint(sr(0x11000, 0x20000), sequential)}},
		{"sparse-pair-holes", sparse, []cephmsgr.OSDOperation{sr(0, 4096), sr(8190, 10), rd(8190, 10)}},
		{"fadvise-each", sparse, []cephmsgr.OSDOperation{hint(sr(0, 0x9000), random), hint(rd(4096, 4096), noCache), hint(sr(0x10ffe, 8), dontNeed), hint(rd(0x20000, 16), willNeed)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			native := mapExtOracle(t, ctx, tc.object, 0, ^uint64(0))
			if native.Code != 0 || !native.Exact || len(native.Full) == 0 || !reflect.DeepEqual(native.Extents, native.Full) {
				t.Fatal("independent native full allocation map missing", native)
			}
			data := native.Before.Data
			reply, err := connections[tc.object].Request(ctx, request(tc.object, tc.ops...))
			if err != nil || reply.Code != 0 || len(reply.Operations) != len(tc.ops) {
				t.Fatal("Go SPARSE_READ compound failed", reply.Code, err)
			}
			if reply.UserVersion != native.Before.Version || reply.Submission.Transaction == 0 || reply.Transaction != reply.Submission.Transaction || reply.Object != tc.object {
				t.Fatal("compound version/identity differs from independent native stat", reply.UserVersion, native.Before.Version)
			}
			var raw []byte
			for i, op := range tc.ops {
				got := reply.Operations[i]
				length, extents, want := sparseReadCompoundExpect(op, native.Full, data)
				if got.Code != op.Code || got.Flags != uint32(op.Flags) || got.Result != 0 || got.Offset != op.Offset || got.Length != length || got.PayloadLength != uint32(len(got.Data)) {
					t.Fatal("per-op union/result differs from native readback model", i, got.Code, got.Length, length, got.Result)
				}
				if op.Code == cephmsgr.OSDRead {
					if !bytes.Equal(got.Data, want) {
						t.Fatal("READ bytes differ from native read", i)
					}
				} else {
					if !reflect.DeepEqual(got.SparseRead.Extents, extents) || !bytes.Equal(got.SparseRead.Data, want) || uint32(len(got.Data)) > op.SparseRead.MaxOutputBytes {
						t.Fatal("SPARSE_READ map/bytes differ from native allocation map and bytes", i, got.SparseRead.Extents, extents)
					}
					// The same operation alone on the same connection: the
					// compound adds no state the standalone read lacks.
					alone, err := connections[tc.object].Request(ctx, request(tc.object, op))
					if err != nil || !reflect.DeepEqual(alone.Operations[0].SparseRead, got.SparseRead) || !bytes.Equal(alone.Operations[0].Data, got.Data) {
						t.Fatal("compound SPARSE_READ differs from standalone", i, err)
					}
				}
				raw = append(raw, got.Data...)
			}
			if !bytes.Equal(reply.RawData, raw) {
				t.Fatal("per-op payloads do not exactly partition the raw reply data")
			}
			after := mapExtOracle(t, ctx, tc.object, 0, ^uint64(0))
			if !reflect.DeepEqual(after.Before, native.Before) || !reflect.DeepEqual(after.Full, native.Full) {
				t.Fatal("Go SPARSE_READ compound altered bytes/mtime/version/allocation")
			}
			t.Logf("%s: %d ops, native full map %v, version %d, raw=%d bytes; scope=native full-range mapext and dense read, per-op intersection model", tc.name, len(tc.ops), native.Full, native.Before.Version, len(raw))
		})
	}
	t.Run("missing-object-ambiguous", func(t *testing.T) {
		native := mapExtOracle(t, ctx, absent, 0, ^uint64(0))
		if native.Code != -2 {
			t.Fatal("independent native absence not observed", native.Code)
		}
		ops := []cephmsgr.OSDOperation{sr(0, 8), rd(0, 8), sr(8, 8)}
		reply, err := connections[absent].Request(ctx, request(absent, ops...))
		var remote *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		if errors.As(err, &unknown) || !errors.As(err, &remote) || remote.Code != -2 || reply.Code != -2 || len(reply.RawData) != 0 || len(reply.Operations) != len(ops) {
			t.Fatal("missing-object compound differs from native ENOENT", reply.Code, err)
		}
		for i, got := range reply.Operations {
			if got.Result != 0 || got.PayloadLength != 0 || got.Offset != ops[i].Offset || got.Length != ops[i].Length || got.SparseRead.Extents != nil {
				t.Fatal("early aggregate error claimed an executed prefix", i, got)
			}
		}
		fresh, err := connections[absent].Request(ctx, request(absent, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}))
		if !errors.As(err, &remote) || fresh.Code != -2 || errors.As(err, &unknown) {
			t.Fatal("signed compound error poisoned the session", err)
		}
	})
	t.Run("refused-shapes-not-sent", func(t *testing.T) {
		for name, ops := range map[string][]cephmsgr.OSDOperation{
			"stat":   {sr(0, 8), {Code: cephmsgr.OSDStat}},
			"mapext": {{Code: cephmsgr.OSDMapExt, Length: 8, MapExt: cephmsgr.OSDMapExtInput{MaxOutputBytes: 64}}, rd(0, 8)},
			"failok": {hint(sr(0, 8), uint32(cephmsgr.OSDFailOK)), rd(0, 8)},
			"write":  {sr(0, 8), {Code: cephmsgr.OSDWrite, Length: 1, Data: []byte{1}}},
		} {
			reply, err := connections[dense].Request(ctx, request(dense, ops...))
			var unknown *cephmsgr.OutcomeUnknownError
			if err == nil || errors.As(err, &unknown) || reply.Submission.Transaction != 0 {
				t.Fatal("unreviewed compound was transmitted", name, reply.Submission, err)
			}
		}
	})
	t.Run("local-output-budget-invalidates-only-its-connection", func(t *testing.T) {
		// OpenOSD reuses an existing handle for the same OSD on one client.
		// A separately authenticated client supplies an independent session.
		options := integrationOptions(t)
		options.EnableOSD = true
		if isReplicatedPGFixtureProfile() {
			options.OSDProfile = replicatedPGFixtureOSDProfile(t)
		}
		independent, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer independent.Close()
		limited := mutationConnection(t, independent, ctx, routes[sparse])
		defer limited.Close()
		small := sr(8190, 10)
		small.SparseRead.MaxOutputBytes = 8
		reply, err := limited.Request(ctx, request(sparse, rd(0, 4), small))
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &unknown) || reply.Submission.Transaction == 0 || len(reply.RawData) <= 12 {
			t.Fatal("compound output budget did not preserve the submitted reply", reply, err)
		}
		if reply.Operations[1].SparseRead.Extents != nil {
			t.Fatal("over-budget compound exposed decoded output")
		}
		if _, err := connections[sparse].Request(ctx, request(sparse, rd(0, 4), sr(8190, 10))); err != nil {
			t.Fatal("independent connection damaged", err)
		}
	})
}
