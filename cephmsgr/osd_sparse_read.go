package cephmsgr

import "github.com/jsyoo5b/ceph-msgr-go/internal/osd"

// OSDSparseRead reads allocated ranges and their bytes from an explicitly
// targeted replicated-pool head object. The sole operation must have no flags,
// snapshot metadata, or implicit whole-object range. Length must be positive
// and bounded. Allocation granularity depends on the backing object store.
const OSDSparseRead OSDOpcode = OSDOpcode(osd.SparseRead)

// OSDSparseReadInput bounds accepted encoded output, including eight bytes of
// counts and sixteen bytes per extent. It is not sent to the server.
type OSDSparseReadInput struct{ MaxOutputBytes uint32 }

// OSDSparseReadResult preserves ordered allocated ranges and their
// concatenated bytes; holes are omitted. OSDOperationResult.Data retains the
// original encoded map and byte buffer. Its Length is the returned byte count.
type OSDSparseReadResult struct {
	Extents []OSDExtent
	Data    []byte
}

func publicSparseRead(data []byte) OSDSparseReadResult {
	decoded, err := osd.DecodeSparseRead(data)
	if err != nil {
		return OSDSparseReadResult{}
	}
	result := OSDSparseReadResult{Extents: make([]OSDExtent, len(decoded.Extents)), Data: decoded.Data}
	for i, e := range decoded.Extents {
		result.Extents[i] = OSDExtent{Offset: e.Offset, Length: e.Length}
	}
	return result
}
