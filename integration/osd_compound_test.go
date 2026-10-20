package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

func nativeCompoundSnapshot(t *testing.T, ctx context.Context, label string) mutationSnapshot {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	input, err := json.Marshal(map[string]string{"label": label})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "osd-compound-input.json"), input, 0600); err != nil {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-compound", "snapshot"); err != nil {
		t.Fatal("native compound observation", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "osd-compound-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Label    string           `json:"label"`
		Snapshot mutationSnapshot `json:"snapshot"`
	}
	if err := json.Unmarshal(data, &result); err != nil || result.Label != label {
		t.Fatal("native compound observation phase", err, result.Label)
	}
	return result.Snapshot
}

func TestCephOSDCompoundIntegration(t *testing.T) {
	c, ctx, oracle, original := osdFixture(t)
	defer restoreMutationObject(t, c, oracle, original)
	o := mutationConnection(t, c, ctx, oracle)
	baseline := []byte{0, 255, 65, 66, 67, 68, 69, 70, 128, 10, 88, 89}
	write := func(code cephmsgr.OSDOpcode, offset uint64, data []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: code, Offset: offset, Length: uint64(len(data)), Data: data}
	}
	read := func(offset, length uint64) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Offset: offset, Length: length}
	}
	stat := cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}
	type output struct {
		data   []byte
		size   *uint64
		result int32
	}
	statOutput := func(size uint64) output { return output{size: &size} }
	// Source-derived expectations, checked against a real Tentacle daemon:
	// PrimaryLogPG::do_read (5835-5944) bounds reads with ctx->new_obs.oi.size,
	// then reads committed storage through pgbackend->objects_read_sync. STAT
	// (6265-6284) returns staged size but pre-finish_ctx mtime. Transactions are
	// submitted only after prepare_transaction succeeds (8968 onward).
	// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc
	// Independent C stat2/read before and after verify committed bytes/version.
	// They are not a native reconstruction of this mixed READ/WRITE vector.
	cases := []struct {
		name    string
		ops     []cephmsgr.OSDOperation
		outputs []output
		after   []byte
		code    int32
	}{
		{
			name: "interleaved payload boundaries",
			ops: []cephmsgr.OSDOperation{read(1, 3), stat, write(cephmsgr.OSDWrite, 2, []byte{9, 0}), read(1, 4),
				write(cephmsgr.OSDAppend, 0, []byte{254, 0}), stat, read(11, 8)},
			outputs: []output{{data: baseline[1:4]}, statOutput(12), {}, {data: baseline[1:5]}, {}, statOutput(14), {data: baseline[11:]}},
			after:   []byte{0, 255, 9, 0, 67, 68, 69, 70, 128, 10, 88, 89, 254, 0},
		},
		{
			name:    "writefull stages size before read",
			ops:     []cephmsgr.OSDOperation{stat, write(cephmsgr.OSDWriteFull, 0, []byte{99, 0, 254}), read(1, 2), stat, read(3, 5)},
			outputs: []output{statOutput(12), {}, {data: baseline[1:3]}, statOutput(3), {}},
			after:   []byte{99, 0, 254},
		},
		{
			name:    "truncate growth reads prior storage EOF",
			ops:     []cephmsgr.OSDOperation{{Code: cephmsgr.OSDTruncate, Offset: 16}, stat, read(10, 6)},
			outputs: []output{{}, statOutput(16), {data: baseline[10:]}},
			after:   append(append([]byte(nil), baseline...), 0, 0, 0, 0),
		},
		{
			name: "failed guard rolls back staged mutation and preserves earlier output",
			ops: []cephmsgr.OSDOperation{read(1, 2), {Code: cephmsgr.OSDTruncate, Offset: 4}, stat, read(0, 10),
				{Code: cephmsgr.OSDAssertVersion}, write(cephmsgr.OSDAppend, 0, []byte{77}), read(1, 1)},
			outputs: []output{{data: baseline[1:3]}, {}, statOutput(4), {data: baseline[:4]}, {result: -75}, {}, {}},
			after:   baseline,
			code:    -75,
		},
		{
			name: "failok stat keeps error and later create commits",
			ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDDelete}, {Code: cephmsgr.OSDStat, Flags: cephmsgr.OSDFailOK},
				{Code: cephmsgr.OSDCreate, Flags: cephmsgr.OSDExclusiveCreate}, stat, read(0, 2)},
			outputs: []output{{}, {result: -2}, {}, statOutput(0), {}},
			after:   []byte{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seed := osdObjectRequest(oracle, write(cephmsgr.OSDWriteFull, 0, baseline))
			seed.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000100, Nanoseconds: 234567890}
			if _, err := o.Request(ctx, seed); err != nil {
				t.Fatal("seed", err)
			}
			before := nativeCompoundSnapshot(t, ctx, "before")
			if before.Code != 0 || !bytes.Equal(before.Data, baseline) || before.Version == 0 {
				t.Fatalf("independent native baseline: %+v", before)
			}
			for i := range tc.ops {
				if tc.ops[i].Code == cephmsgr.OSDAssertVersion {
					tc.ops[i].Version = before.Version + 1
				}
			}
			r := osdObjectRequest(oracle, tc.ops...)
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000101, Nanoseconds: 987654321}
			reply, err := o.Request(ctx, r)
			var server *cephmsgr.OSDError
			if tc.code < 0 {
				if !errors.As(err, &server) || server.Code != tc.code {
					t.Fatalf("aggregate=%d error=%v", reply.Code, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if reply.Code != tc.code || len(reply.Operations) != len(tc.outputs) || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
				t.Fatalf("compound completion: %+v", reply)
			}
			var joined []byte
			for i, expected := range tc.outputs {
				actual := reply.Operations[i]
				want := expected.data
				if expected.size != nil {
					want = make([]byte, 16)
					binary.LittleEndian.PutUint64(want, *expected.size)
					binary.LittleEndian.PutUint32(want[8:], before.Seconds)
					binary.LittleEndian.PutUint32(want[12:], before.Nanos)
				}
				if actual.Result != expected.result || actual.PayloadLength != uint32(len(want)) || !bytes.Equal(actual.Data, want) {
					t.Fatalf("op %d result=%d length=%d data=%x; want result=%d length=%d data=%x", i, actual.Result, actual.PayloadLength, actual.Data, expected.result, len(want), want)
				}
				joined = append(joined, want...)
			}
			if !bytes.Equal(reply.RawData, joined) {
				t.Fatal("raw reply data disagrees with ordered per-op payload boundaries")
			}
			after := nativeCompoundSnapshot(t, ctx, "after")
			if after.Code != 0 || !bytes.Equal(after.Data, tc.after) {
				t.Fatalf("native committed bytes=%x code=%d; want=%x", after.Data, after.Code, tc.after)
			}
			if tc.code < 0 {
				if after.Version != before.Version || after.Seconds != before.Seconds || after.Nanos != before.Nanos {
					t.Fatal("failed compound changed native user version or mtime")
				}
			} else if after.Version <= before.Version || after.Seconds != r.ModificationTime.Seconds || after.Nanos != r.ModificationTime.Nanoseconds {
				t.Fatal("successful compound lacks native version or mtime update")
			}
		})
		if t.Failed() {
			break
		}
	}
	t.Log("mixed vector outputs exercised a native Tentacle daemon; independent stat2/read verified committed bytes and rollback; C write_op does not reproduce mixed vectors")
}
