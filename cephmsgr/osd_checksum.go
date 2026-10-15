package cephmsgr

import "github.com/jsyoo5b/ceph-msgr-go/internal/osd"

// OSDChecksum reads CRC32C checksums from an explicitly targeted head object.
// It must be the sole operation, without flags or snapshot metadata. Length
// must be positive and bounded; implicit whole-object reads are unsupported.
const OSDChecksum OSDOpcode = OSDOpcode(osd.Checksum)

const OSDChecksumCRC32C uint8 = osd.ChecksumCRC32C

// OSDChecksumInput selects CRC32C (Type=OSDChecksumCRC32C) and its seed.
// ChunkSize zero produces one checksum; otherwise Length must be a multiple
// of ChunkSize. xxHash algorithms are currently rejected before transmission.
type OSDChecksumInput struct {
	ChunkSize uint32
	Type      uint8
	Seed      uint32
}

// OSDChecksumResult preserves the echoed checksum union and decoded values.
// Values are raw Ceph CRC32C values, with the caller's supplied seed. An empty
// slice with empty Data means the requested offset was at or past EOF.
type OSDChecksumResult struct {
	ChunkSize uint32
	Type      uint8
	Values    []uint32
}

func publicChecksum(result osd.ChecksumResult, data []byte) OSDChecksumResult {
	values, _ := osd.DecodeChecksum(data) // the decoder checks count bounds; validation follows conversion
	return OSDChecksumResult{ChunkSize: result.ChunkSize, Type: result.Type, Values: values}
}
