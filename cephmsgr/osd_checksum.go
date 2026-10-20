package cephmsgr

import "github.com/JSYoo5B/ceph-msgr-go/internal/osd"

// OSDChecksum reads CRC32C, xxHash32, or xxHash64 checksums from an explicitly
// targeted object at ReadSnapshot (zero selects the head). It must be the sole
// operation, without flags or a write SnapContext. Length must be positive and
// bounded; implicit whole-object reads are unsupported.
const OSDChecksum OSDOpcode = OSDOpcode(osd.Checksum)

const (
	OSDChecksumXXHash32 uint8 = osd.ChecksumXXHash32
	OSDChecksumXXHash64 uint8 = osd.ChecksumXXHash64
	OSDChecksumCRC32C   uint8 = osd.ChecksumCRC32C
)

// OSDChecksumInput selects a checksum algorithm and its seed.
// ChunkSize zero produces one checksum; otherwise Length must be a multiple
// of ChunkSize. CRC32C and xxHash32 require a seed fitting uint32; xxHash64
// accepts all uint64 values. Seeds use the algorithm's little-endian width.
type OSDChecksumInput struct {
	ChunkSize uint32
	Type      uint8
	Seed      uint64
}

// OSDChecksumResult preserves the echoed checksum union and decoded values.
// Values preserve raw Ceph digests, widened to uint64 for 32-bit algorithms. An empty
// slice with empty Data means the requested offset was at or past EOF.
type OSDChecksumResult struct {
	ChunkSize uint32
	Type      uint8
	Values    []uint64
}

func publicChecksum(result osd.ChecksumResult, data []byte) OSDChecksumResult {
	values, _ := osd.DecodeChecksum(result.Type, data) // the decoder checks count bounds; validation follows conversion
	return OSDChecksumResult{ChunkSize: result.ChunkSize, Type: result.Type, Values: values}
}
