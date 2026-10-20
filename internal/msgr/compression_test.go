package msgr

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestCompressionRoundTrip(t *testing.T) {
	inputs := [][]byte{
		nil,
		{},
		[]byte("x"),
		bytes.Repeat([]byte("ceph messenger compression\x00\xff"), 200),
		append([]byte{0, 0xff, 1, 2, 3}, bytes.Repeat([]byte("a"), 5000)...),
	}
	for _, method := range SupportedCompressionMethods {
		for i, in := range inputs {
			out, err := compressSegment(method, in)
			if err != nil {
				t.Fatalf("method %d input %d compress: %v", method, i, err)
			}
			back, err := decompressSegment(method, out)
			if err != nil {
				t.Fatalf("method %d input %d decompress: %v", method, i, err)
			}
			if !bytes.Equal(back, in) {
				t.Fatalf("method %d input %d round trip differs: %q vs %q", method, i, back, in)
			}
		}
	}
}

// The zstd onwire format is a 4-byte little-endian original length then a
// standard zstd frame (magic 0x28 0xB5 0x2F 0xFD), matching Ceph's
// ZstdCompressor.
func TestCompressionZstdFormat(t *testing.T) {
	src := bytes.Repeat([]byte("zstd frame bytes"), 100)
	out, err := compressSegment(CompAlgZstd, src)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) < 8 {
		t.Fatalf("zstd output too short: %d", len(out))
	}
	if got := binary.LittleEndian.Uint32(out); got != uint32(len(src)) {
		t.Fatalf("zstd length prefix %d, want %d", got, len(src))
	}
	if magic := binary.LittleEndian.Uint32(out[4:]); magic != 0xFD2FB528 {
		t.Fatalf("zstd magic after the prefix is %#x", magic)
	}
	if _, err := decompressSegment(CompAlgZstd, out[:4]); err == nil {
		t.Fatal("a zstd segment with no frame after the prefix must fail")
	}
}

// A snappy segment is the raw block format, whose first bytes are the varint
// uncompressed length (Ceph SnappyCompressor), so a short literal round-trips
// without any added header.
func TestCompressionSnappyBlock(t *testing.T) {
	src := []byte("snappy")
	out, err := compressSegment(CompAlgSnappy, src)
	if err != nil {
		t.Fatal(err)
	}
	// Block format leads with the uncompressed length as a varint; for 6 bytes
	// that is a single byte 0x06.
	if len(out) == 0 || out[0] != byte(len(src)) {
		t.Fatalf("snappy block does not lead with the uncompressed length: %x", out)
	}
	back, err := decompressSegment(CompAlgSnappy, out)
	if err != nil || !bytes.Equal(back, src) {
		t.Fatalf("snappy round trip: %q %v", back, err)
	}
}

func TestCompressionSupportedMethods(t *testing.T) {
	for _, m := range SupportedCompressionMethods {
		if !compressionSupported(m) {
			t.Fatalf("advertised method %d has no codec", m)
		}
	}
	if compressionSupported(CompAlgZlib) || compressionSupported(CompAlgNone) {
		t.Fatal("zlib/none must not report a codec yet")
	}
}
