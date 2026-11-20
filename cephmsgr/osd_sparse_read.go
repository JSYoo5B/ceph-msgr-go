package cephmsgr

import "github.com/JSYoo5B/ceph-msgr-go/internal/osd"

// OSDSparseRead reads allocated ranges and their bytes from an explicitly
// targeted replicated-pool object: the head, or at ReadSnapshot the clone (or
// head) the OSD resolves for that snapshot. Only fadvise cache hints are
// accepted as operation flags; there is no write context or implicit
// whole-object range. Length must be positive and bounded. Allocation
// granularity depends on the backing object store.
//
// It may be standalone, or combined only with further OSDSparseRead and
// OSDRead operations (the native librbd object read) in a head-only request
// to the replicated primary, without FAILOK, extent truncate fields,
// snapshot, replica reads or resubmission. Decoded compound results appear
// only after the whole reply validates; on a negative aggregate they cover
// the completed prefix. The returned Length is the byte count, which may be
// shorter than the range; past EOF the map and bytes are empty. A failed
// operation carries no output. The local budget neither caps server work nor
// truncates output: output over budget fails the session. Snapshot and
// replica-read compounds, guards in compounds, MAPEXT compounds and EC are
// not supported.
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
