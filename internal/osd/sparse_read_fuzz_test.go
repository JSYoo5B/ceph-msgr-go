package osd

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

// These are malformed-input invariants rather than native interoperability
// evidence. The reference checks use fixed-width bytes directly, without the
// product's wire decoder or sparse encoder.
func FuzzSparseReadDecodeAndReply(f *testing.F) {
	for _, seed := range [][]byte{
		{}, {0, 0, 0, 0, 0, 0, 0, 0},
		sparseReadBytes([]byte{0, 255, 65}, Extent{3, 2}, Extent{8, 1}),
		{255, 255, 255, 255, 0, 0, 0, 0},
		sparseReadBytes([]byte{1}, Extent{math.MaxUint64, 1}),
		sparseReadBytes(nil, Extent{3, 0}),
	} {
		f.Add(seed, uint64(3), uint64(97), uint64(3), uint64(3), uint32(512), true)
	}
	f.Fuzz(func(t *testing.T, data []byte, requestedOffset, requestedLength, replyOffset, replyLength uint64, budget uint32, success bool) {
		if len(data) > 4096 {
			t.Skip()
		}
		decoded, err := DecodeSparseRead(data)
		if err == nil {
			if len(data) < 8 {
				t.Fatal("accepted missing counts")
			}
			count := uint64(binary.LittleEndian.Uint32(data))
			if count > uint64((len(data)-8)/16) || count != uint64(len(decoded.Extents)) {
				t.Fatal("allocation escaped encoded extent count")
			}
			mapEnd := 4 + int(count)*16
			byteCount := uint64(binary.LittleEndian.Uint32(data[mapEnd:]))
			if byteCount != uint64(len(data)-mapEnd-4) || !bytes.Equal(decoded.Data, data[mapEnd+4:]) {
				t.Fatal("accepted trailing, missing or changed bytes")
			}
			var total, end uint64
			for i, e := range decoded.Extents {
				off := binary.LittleEndian.Uint64(data[4+i*16:])
				size := binary.LittleEndian.Uint64(data[12+i*16:])
				if e.Offset != off || e.Length != size || size == 0 || size > math.MaxUint64-off || i > 0 && off < end || size > byteCount-total {
					t.Fatal("accepted invalid extent map")
				}
				end = off + size
				total += size
			}
			if total != byteCount {
				t.Fatal("extent bytes not accounted exactly once")
			}
		}
		requested := Operation{Code: SparseRead, Offset: requestedOffset, Length: requestedLength, SparseRead: SparseReadInput{MaxOutputBytes: budget}}
		reply := OperationResult{Code: SparseRead, Offset: replyOffset, Length: replyLength, PayloadLength: uint32(len(data)), Data: data}
		if ValidateSparseReadReply(requested, reply, success) != nil {
			return
		}
		if requestedOffset != replyOffset || replyLength > requestedLength || uint32(len(data)) > budget {
			t.Fatal("accepted reply escaped range or output budget")
		}
		if !success {
			if len(data) != 0 || replyLength != requestedLength {
				t.Fatal("failed reply changed union or returned data")
			}
			return
		}
		if err != nil || replyLength != uint64(len(decoded.Data)) {
			t.Fatal("accepted inconsistent decoded reply")
		}
		for _, e := range decoded.Extents {
			if e.Offset < requestedOffset {
				t.Fatal("extent precedes requested range")
			}
			delta := e.Offset - requestedOffset
			if delta > requestedLength || e.Length > requestedLength-delta {
				t.Fatal("extent exceeds requested range")
			}
		}
	})
}
