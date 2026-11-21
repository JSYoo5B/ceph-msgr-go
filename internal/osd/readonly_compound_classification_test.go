package osd

import (
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// TestStandaloneReadOnlyOpcodesStayOutOfCompounds classifies MAPEXT, ISDIRTY
// and UNDIRTY individually: no native client composes them (librados
// IoCtxImpl::mapext builds a one-operation request; librados is_dirty/undirty
// users and ceph_test_rados send each alone), so each stays a standalone
// operation. Beside every read the compound subsets admit, before or after
// it, at the head or at a read snap, it is refused before encoding on v6, v8
// and v9; alone it still encodes.
func TestStandaloneReadOnlyOpcodesStayOutOfCompounds(t *testing.T) {
	standalone := map[string]Operation{
		"mapext":  {Code: MapExt, Length: 8, MapExt: MapExtInput{MaxOutputBytes: 64}},
		"isdirty": {Code: Isdirty},
		"undirty": {Code: Undirty},
	}
	companions := map[string]Operation{
		"read":        {Code: Read, Length: 8},
		"stat":        {Code: Stat},
		"sparse-read": {Code: SparseRead, Length: 8, SparseRead: SparseReadInput{MaxOutputBytes: 64}},
		"checksum":    {Code: Checksum, Length: 8, Checksum: ChecksumInput{Type: ChecksumCRC32C}},
		"assert":      {Code: AssertVersion, Version: 9},
		"getxattr":    {Code: GetXattr, Xattr: XattrInput{Name: "a", MaxOutputBytes: 64}},
	}
	for name, op := range standalone {
		pairs := map[string]Operation{}
		for companion, other := range companions {
			pairs[companion] = other
		}
		for companion, other := range standalone {
			pairs[companion] = other
		}
		for companion, other := range pairs {
			for _, snap := range []uint64{0, 3} {
				for _, ops := range [][]Operation{{op, other}, {other, op}} {
					if _, _, err := ValidateOperations(ops, wire.DefaultLimit); !errors.Is(err, ErrOperation) {
						t.Fatal("standalone opcode admitted in a compound", name, companion, err)
					}
					r := sparseReadCompoundRequest(ops)
					r.ReadSnap = snap
					for version, encode := range inactiveCopyFromEncoders() {
						if m, err := encode(r); err == nil || m.Type != 0 || len(m.Front) != 0 || len(m.Data) != 0 {
							t.Fatal("standalone opcode compound encoded", name, companion, snap, version, err)
						}
					}
				}
			}
		}
		for version, encode := range inactiveCopyFromEncoders() {
			if _, err := encode(sparseReadCompoundRequest([]Operation{op})); err != nil {
				t.Fatal("standalone opcode refused alone", name, version, err)
			}
		}
	}
}
