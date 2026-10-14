package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

type mutationSnapshot struct {
	Code    int32  `json:"code"`
	Data    []byte `json:"data"`
	Version uint64 `json:"version"`
	Seconds uint32 `json:"seconds"`
	Nanos   uint32 `json:"nanos"`
}
type mutationOracle struct {
	Code        int32            `json:"code"`
	Actual      mutationSnapshot `json:"actual"`
	Native      mutationSnapshot `json:"native"`
	Placement   osdFixtureOracle `json:"placement"`
	Comparisons []int32          `json:"comparisons"`
}

func nativeMutation(t *testing.T, ctx context.Context, input any) mutationOracle {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "osd-mutation-input.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = testcluster.ControlDaemon(ctx, dir, "verify", "osd-mutation", "compare"); err != nil {
		t.Fatal("native mutation oracle", err)
	}
	data, err = os.ReadFile(filepath.Join(dir, "osd-mutation-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result mutationOracle
	if err = json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func mutationConnection(t *testing.T, c *cephmsgr.Client, ctx context.Context, oracle osdFixtureOracle) *cephmsgr.OSDConnection {
	t.Helper()
	o, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: oracle.ID, Address: oracle.Address})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func restoreMutationObject(t *testing.T, c *cephmsgr.Client, oracle osdFixtureOracle, original []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	o := mutationConnection(t, c, ctx, oracle)
	defer o.Close()
	r := osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(original)), Data: original})
	r.ModificationTime = cephmsgr.OSDTime{Seconds: oracle.Seconds, Nanoseconds: oracle.Nanoseconds}
	if _, err := o.Request(ctx, r); err != nil {
		t.Error("restore fixture object and mtime", err)
	}
}

func TestCephOSDMutationsIntegration(t *testing.T) {
	c, ctx, oracle, original := osdFixture(t)
	// This consumer test uses the already routed fixture object and restores it
	// for read-only acceptance tests. Placement remains entirely caller-owned.
	defer restoreMutationObject(t, c, oracle, original)
	o := mutationConnection(t, c, ctx, oracle)
	baseline := []byte{0, 255, 65, 66, 67, 68, 69, 70, 128, 10}
	dataOp := func(code cephmsgr.OSDOpcode, offset uint64, data []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: code, Offset: offset, Length: uint64(len(data)), Data: data}
	}
	cases := []struct {
		name   string
		absent bool
		delta  int64
		code   int32
		ops    []cephmsgr.OSDOperation
	}{
		{name: "write hole", ops: []cephmsgr.OSDOperation{dataOp(cephmsgr.OSDWrite, 14, []byte{0, 254})}},
		{name: "replace shorter", ops: []cephmsgr.OSDOperation{dataOp(cephmsgr.OSDWriteFull, 0, []byte{128, 0, 255})}},
		{name: "empty full", ops: []cephmsgr.OSDOperation{dataOp(cephmsgr.OSDWriteFull, 0, nil)}},
		{name: "append", ops: []cephmsgr.OSDOperation{dataOp(cephmsgr.OSDAppend, 0, []byte{0, 254, 10})}},
		{name: "truncate shorter", ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDTruncate, Offset: 4}}},
		{name: "truncate extend", ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDTruncate, Offset: 15}}},
		{name: "zero interior", ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDZero, Offset: 2, Length: 3}}},
		{name: "zero tail normalized", ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDZero, Offset: 4, Length: 20}}},
		{name: "zero beyond eof", ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDZero, Offset: 14, Length: 3}}},
		{name: "create exclusive", absent: true, ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDCreate, Flags: cephmsgr.OSDExclusiveCreate}}},
		{name: "create existing", ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDCreate}}},
		{name: "create exclusive fails", code: -17, ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDCreate, Flags: cephmsgr.OSDExclusiveCreate}}},
		{name: "delete", ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDDelete}}},
		{name: "delete absent", absent: true, code: -2, ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDDelete}}},
		{name: "version matches", ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDAssertVersion}, dataOp(cephmsgr.OSDAppend, 0, []byte{99})}},
		{name: "version older", delta: -1, code: -34, ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDAssertVersion}, dataOp(cephmsgr.OSDAppend, 0, []byte{99})}},
		{name: "version newer", delta: 1, code: -75, ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDAssertVersion}, dataOp(cephmsgr.OSDAppend, 0, []byte{99})}},
		{name: "compare matches", ops: []cephmsgr.OSDOperation{dataOp(cephmsgr.OSDCompareExtent, 1, baseline[1:4]), dataOp(cephmsgr.OSDWrite, 2, []byte{99, 100})}},
		{name: "rollback after staged write", code: -4097, ops: []cephmsgr.OSDOperation{dataOp(cephmsgr.OSDWrite, 8, []byte{88}), dataOp(cephmsgr.OSDCompareExtent, 0, []byte{0, 255, 0}), dataOp(cephmsgr.OSDAppend, 0, []byte{99})}},
		{name: "compare failok commits", ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDCompareExtent, Flags: cephmsgr.OSDFailOK, Length: 1, Data: []byte{99}}, dataOp(cephmsgr.OSDAppend, 0, []byte{99})}},
		{name: "ordered payload vector", ops: []cephmsgr.OSDOperation{dataOp(cephmsgr.OSDWriteFull, 0, []byte{1, 0}), dataOp(cephmsgr.OSDAppend, 0, []byte{255, 3}), dataOp(cephmsgr.OSDWrite, 1, []byte{128})}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seed := osdObjectRequest(oracle, dataOp(cephmsgr.OSDWriteFull, 0, baseline))
			seed.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000000, Nanoseconds: 123456789}
			before, err := o.Request(ctx, seed)
			if err != nil {
				t.Fatal("seed", err)
			}
			if tc.absent {
				if _, err = o.Request(ctx, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete})); err != nil {
					t.Fatal(err)
				}
			}
			for i := range tc.ops {
				if tc.ops[i].Code == cephmsgr.OSDAssertVersion {
					tc.ops[i].Version = uint64(int64(before.UserVersion) + tc.delta)
				}
			}
			r := osdObjectRequest(oracle, tc.ops...)
			r.ClientIncarnation = 7
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000001, Nanoseconds: 987654321}
			reply, err := o.Request(ctx, r)
			var server *cephmsgr.OSDError
			if tc.code < 0 {
				if !errors.As(err, &server) || server.Code != tc.code {
					t.Fatalf("server result=%d error=%v", reply.Code, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if reply.Code != tc.code || reply.Submission.Transaction != reply.Transaction || reply.Submission.ClientIncarnation != 7 || reply.Submission.MapEpoch != oracle.Epoch || len(reply.Operations) != len(tc.ops) || tc.code == 0 && reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
				t.Fatalf("completion metadata: %+v", reply)
			}
			native := nativeMutation(t, ctx, map[string]any{"compare": true, "exists": !tc.absent, "baseline": baseline, "operations": tc.ops, "version_delta": tc.delta, "seconds": r.ModificationTime.Seconds, "nanos": r.ModificationTime.Nanoseconds})
			if (native.Code != reply.Code && !(tc.name == "compare failok commits" && native.Code == -5 && reply.Code == 0)) || native.Actual.Code != native.Native.Code || !bytes.Equal(native.Actual.Data, native.Native.Data) || native.Actual.Code == 0 && (native.Actual.Seconds != native.Native.Seconds || native.Actual.Nanos != native.Native.Nanos) {
				t.Fatalf("Go/native code or object differs: Go=%d native=%+v", reply.Code, native)
			}
			j := 0
			for i, op := range tc.ops {
				if op.Code == cephmsgr.OSDCompareExtent {
					if reply.Operations[i].Result != native.Comparisons[j] {
						t.Fatalf("compare per-op=%d native=%d", reply.Operations[i].Result, native.Comparisons[j])
					}
					j++
				}
			}
		})
		if t.Failed() {
			break
		}
	}
}

func TestCephOSDMutationLostReplyIntegration(t *testing.T) {
	observer, ctx, oracle, original := osdFixture(t)
	defer restoreMutationObject(t, observer, oracle, original)
	options := integrationOptions(t)
	options.EnableOSD = true
	var armed atomic.Bool
	blocked := make(chan struct{}, 1)
	dial := options.DialContext
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		_, kind, _ := testcluster.SplitPeer(endpoint)
		if kind != "osd.0" {
			return conn, nil
		}
		return &testcluster.LostReplyConn{Conn: conn, Armed: &armed, Closed: make(chan struct{}), SignalBlocked: func() {
			select {
			case blocked <- struct{}{}:
			default:
			}
		}}, nil
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	o := mutationConnection(t, c, ctx, oracle)
	armed.Store(true)
	tail := []byte("one explicit append\x00\xff")
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		reply cephmsgr.OSDReply
		err   error
	}
	finished := make(chan result, 1)
	go func() {
		r, e := o.Request(callCtx, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail}))
		finished <- result{r, e}
	}()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("receive fault not activated", ctx.Err())
	}
	want := append(append([]byte(nil), original...), tail...)
	for {
		native := nativeMutation(t, ctx, map[string]any{"compare": false})
		if bytes.Equal(native.Actual.Data, want) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("append not visible", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	cancel()
	got := <-finished
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(got.err, &unknown) || !errors.Is(got.err, context.Canceled) || got.reply.Submission.Transaction == 0 || got.reply.Submission.GlobalID == 0 || got.reply.Submission.MapEpoch != oracle.Epoch || got.reply.Transaction != 0 {
		t.Fatalf("lost response metadata: %+v %v", got.reply, got.err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	armed.Store(false)
	o = mutationConnection(t, c, ctx, oracle)
	read, err := o.Request(ctx, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want) + 32)}))
	if err != nil || read.Transaction <= got.reply.Submission.Transaction || !bytes.Equal(read.Operations[0].Data, want) {
		t.Fatal("fresh session identity/readback", err)
	}
	time.Sleep(200 * time.Millisecond)
	if native := nativeMutation(t, ctx, map[string]any{"compare": false}); !bytes.Equal(native.Actual.Data, want) {
		t.Fatal("mutation was replayed")
	}
	t.Log("native readback proved one append; local cancellation retained request identity, fresh session did not replay")
}

func TestCephOSDMutationRestartIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("restart persistence requires the sparse-file BlueStore fixture")
	}
	c, ctx, oracle, original := osdFixture(t)
	defer func() { restoreMutationObject(t, c, oracle, original) }()
	o := mutationConnection(t, c, ctx, oracle)
	want := append([]byte{0, 255, 128}, bytes.Repeat([]byte("persisted-by-bluestore"), 512)...)
	reply, err := o.Request(ctx, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(want)), Data: want}))
	if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("ONDISK write before restart", err)
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if err = testcluster.ControlDaemon(ctx, dir, "stop", "osd", "0"); err != nil {
		t.Fatal(err)
	}
	o.Close()
	if err = testcluster.ControlDaemon(ctx, dir, "start", "osd", "0"); err != nil {
		t.Fatal(err)
	}
	native := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true})
	if !bytes.Equal(native.Actual.Data, want) || native.Placement.Epoch <= oracle.Epoch {
		t.Fatal("native readback after restart", native)
	}
	// The consumer refreshes routing from the native map; the product neither
	// retries the write nor selects a replacement primary itself.
	fresh := native.Placement
	fresh.Seconds, fresh.Nanoseconds = oracle.Seconds, oracle.Nanoseconds
	oracle = fresh
	// Publish refreshed native routing for subsequent fixture tests as well.
	path := filepath.Join(dir, "osd-oracle.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err = json.Unmarshal(data, &metadata); err != nil {
		t.Fatal(err)
	}
	metadata["address"], metadata["epoch"] = oracle.Address, oracle.Epoch
	data, err = json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	o = mutationConnection(t, c, ctx, oracle)
	read, err := o.Request(ctx, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want))}))
	if err != nil || read.Transaction <= reply.Transaction || !bytes.Equal(read.Operations[0].Data, want) {
		t.Fatal("Go readback after restart", err)
	}
	t.Log("ONDISK write survived orderly BlueStore OSD restart; independent native and Go readback matched")
}
