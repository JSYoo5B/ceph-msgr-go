package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

// The independent snapshot-read, MAPEXT and CHECKSUM vectors prove the constituent
// wire fields. These tests check their composition in both existing layouts;
// they do not label Go-composed messages as new native snapshot vectors.
func TestReadMetadataSnapshotWireAndRestrictions(t *testing.T) {
	for _, operation := range []struct {
		name string
		op   Operation
	}{
		{"crc32c", Operation{Code: Checksum, Offset: 2, Length: 8, Checksum: ChecksumInput{Type: ChecksumCRC32C, ChunkSize: 4, Seed: 0xffffffff}}},
		{"xxhash32", Operation{Code: Checksum, Offset: 2, Length: 8, Checksum: ChecksumInput{Type: ChecksumXXHash32, ChunkSize: 4, Seed: 0xffffffff}}},
		{"xxhash64", Operation{Code: Checksum, Offset: 2, Length: 8, Checksum: ChecksumInput{Type: ChecksumXXHash64, ChunkSize: 4, Seed: 0xfedcba9876543210}}},
		{"mapext", Operation{Code: MapExt, Offset: 3, Length: 97, MapExt: MapExtInput{MaxOutputBytes: 128}}},
	} {
		for _, version := range []int{6, 8} {
			t.Run(fmt.Sprintf("%s/v%d", operation.name, version), func(t *testing.T) {
				r := nativeV8Request("checksum")
				r.Operations = []Operation{operation.op}
				encode := func(r V8Request) (msgr.MessageData, error) {
					if version == 6 {
						return EncodeRequest(r.Request, oracleFeatures, 1024)
					}
					return EncodeRequestV8(r, v8OracleFeatures, 1024)
				}
				head, err := encode(r)
				if err != nil {
					t.Fatal(err)
				}
				// The native MOSDOp tail is snap/seq/count/retry/features, then
				// v6's default reqid envelope. There is no snapshot-specific op union.
				tailBytes, flagsOffset := 32, 32
				if version == 6 {
					tailBytes, flagsOffset = 59, 8
				}
				tail := len(head.Front) - tailBytes
				for _, snap := range []uint64{1, 4, MaxSnap, NoSnap, 0} {
					r.ReadSnap = snap
					message, err := encode(r)
					if err != nil {
						t.Fatal(snap, err)
					}
					wantSnap := snap
					if snap == 0 {
						wantSnap = NoSnap
					}
					want := bytes.Clone(head.Front)
					binary.LittleEndian.PutUint64(want[tail:], wantSnap)
					if !bytes.Equal(message.Front, want) || !bytes.Equal(message.Data, head.Data) || binary.LittleEndian.Uint64(message.Front[tail+8:]) != 0 || binary.LittleEndian.Uint32(message.Front[tail+16:]) != 0 || binary.LittleEndian.Uint32(message.Front[flagsOffset:]) != uint32(FlagRead|FlagOnDisk|FlagReturnVec) {
						t.Fatal("read snap changed operation, flags, seed, or write context", snap)
					}
					if version == 8 {
						target, err := DecodeRequestV8AdmissionTarget(message, 1024)
						if err != nil || target.Object.Snap != NoSnap || target.PG != r.PG || target.Object.Hash != r.Hash {
							t.Fatal("snapshot metadata read changed head-object backoff identity", target, err)
						}
					}
				}
				for _, tc := range []struct {
					name string
					edit func(*V8Request)
				}{
					{"SNAPDIR", func(r *V8Request) { r.ReadSnap = SnapDir }},
					{"write seq", func(r *V8Request) { r.SnapSeq = 4 }},
					{"write snaps", func(r *V8Request) { r.Snaps = []uint64{4} }},
					{"write context", func(r *V8Request) { r.SnapSeq, r.Snaps = 4, []uint64{4} }},
					{"ORDERSNAP", func(r *V8Request) { r.OrderSnap = true }},
					{"rollback union", func(r *V8Request) { r.Operations[0].Snap = 4 }},
				} {
					bad := r
					bad.ReadSnap = 4
					bad.Operations = append([]Operation(nil), r.Operations...)
					tc.edit(&bad)
					if _, err := encode(bad); !errors.Is(err, ErrRequest) {
						t.Fatal(tc.name, err)
					}
				}
				for _, op := range []Operation{{Code: ListWatchers, ListWatchers: ListWatchersInput{MaxOutputBytes: 64}}} {
					bad := r
					bad.ReadSnap, bad.Operations = 4, []Operation{op}
					if _, err := encode(bad); !errors.Is(err, ErrRequest) {
						t.Fatal("head-only operation admitted at a snapshot", op.Code, err)
					}
				}
				if _, err := encode(V8Request{Request: Request{MapEpoch: 11, Locator: r.Locator, Object: r.Object, Hash: r.Hash, ReadSnap: 4, Operations: []Operation{r.Operations[0], {Code: Stat}}}, PG: r.PG, Shard: -1}); !errors.Is(err, ErrOperation) {
					t.Fatal("snapshot metadata compound admitted", err)
				}
			})
		}
	}
}
