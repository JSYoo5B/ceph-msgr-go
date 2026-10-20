package msgr

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

// CompressionAlgorithm numbers match Ceph's Compressor::CompressionAlgorithm
// (src/compressor/Compressor.h): the id the client lists in CompressionRequest
// and the server echoes in CompressionDone.
type CompressionAlgorithm uint32

const (
	CompAlgNone   CompressionAlgorithm = 0
	CompAlgSnappy CompressionAlgorithm = 1
	CompAlgZlib   CompressionAlgorithm = 2
	CompAlgZstd   CompressionAlgorithm = 3
	CompAlgLZ4    CompressionAlgorithm = 4
	CompAlgBrotli CompressionAlgorithm = 5
)

// SupportedCompressionMethods are the algorithms this client implements, in the
// preference order sent in CompressionRequest. Only algorithms with a pure-Go
// codec and an unambiguous Ceph onwire format are listed; zlib is deferred
// until its window-size format is pinned, so the server never selects it.
var SupportedCompressionMethods = []CompressionAlgorithm{CompAlgZstd, CompAlgSnappy}

// compressionSupported reports whether the negotiated method has a codec here.
func compressionSupported(method CompressionAlgorithm) bool {
	return method == CompAlgSnappy || method == CompAlgZstd
}

// CompressionSupported reports whether this client can use the method a server
// selected in CompressionDone.
func CompressionSupported(method CompressionAlgorithm) bool { return compressionSupported(method) }

// ErrCompression marks a compression or decompression failure. A decode failure
// is a framing error from the caller's point of view.
var ErrCompression = errors.New("ceph messenger: compression")

// defaultCompressMinSize is the smallest total segment size this client
// compresses; smaller frames go onwire uncompressed (flag clear). It is a
// local efficiency choice — the peer accepts uncompressed frames regardless.
const defaultCompressMinSize = 512

// EnableCompression records the negotiated method so Write compresses each
// segment. Called once, after CompressionDone, on CRC-mode connections.
func (w *Writer) EnableCompression(method CompressionAlgorithm, minSize int) error {
	if w.err != nil || w.comp != CompAlgNone {
		return errors.New("ceph messenger: invalid compression transition")
	}
	if !compressionSupported(method) {
		return fmt.Errorf("%w: unsupported method %d", ErrCompression, method)
	}
	if minSize <= 0 {
		minSize = defaultCompressMinSize
	}
	w.comp, w.compMin = method, minSize
	return nil
}

// EnableCompression records the negotiated method so received frames flagged
// compressed are decompressed with it.
func (r *Reader) EnableCompression(method CompressionAlgorithm) error {
	if r.err != nil || r.comp != CompAlgNone {
		return errors.New("ceph messenger: invalid compression transition")
	}
	if !compressionSupported(method) {
		return fmt.Errorf("%w: unsupported method %d", ErrCompression, method)
	}
	r.comp = method
	return nil
}

func segmentsTotal(segs [][]byte) int {
	total := 0
	for _, s := range segs {
		total += len(s)
	}
	return total
}

func compressSegments(method CompressionAlgorithm, segs [][]byte) ([][]byte, error) {
	out := make([][]byte, len(segs))
	for i, s := range segs {
		c, err := compressSegment(method, s)
		if err != nil {
			return nil, err
		}
		out[i] = c
	}
	return out, nil
}

// decompressFrame decompresses each segment in place when flags marks the frame
// compressed. A compressed frame without a negotiated codec, or one whose
// decompressed total exceeds the reader's limit, is a framing error.
func (r *Reader) decompressFrame(f *Frame, flags byte) error {
	if flags&FrameEarlyDataCompressed == 0 {
		return nil
	}
	if !compressionSupported(r.comp) {
		return fmt.Errorf("%w: compressed frame with no negotiated codec", ErrFrame)
	}
	var total uint64
	for i := range f.Segments {
		if len(f.Segments[i]) == 0 {
			continue
		}
		d, err := decompressSegment(r.comp, f.Segments[i])
		if err != nil {
			return fmt.Errorf("%w: %w", ErrFrame, err)
		}
		total += uint64(len(d))
		if total > uint64(r.limit) {
			return wire.ErrLimit
		}
		f.Segments[i] = d
	}
	return nil
}

// One shared zstd encoder and decoder serve every connection. EncodeAll and
// DecodeAll are safe for concurrent use and run on the calling goroutine, so no
// streaming writer fans a frame across goroutines (msgr frames are small). The
// decoder's window is bounded to cap per-frame memory.
var (
	zstdEncoder = sync.OnceValue(func() *zstd.Encoder {
		e, err := zstd.NewWriter(nil,
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			panic(fmt.Sprintf("ceph messenger: zstd encoder: %v", err))
		}
		return e
	})
	zstdDecoder = sync.OnceValue(func() *zstd.Decoder {
		d, err := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxWindow(16<<20))
		if err != nil {
			panic(fmt.Sprintf("ceph messenger: zstd decoder: %v", err))
		}
		return d
	})
)

// compressSegment returns the onwire bytes for one segment under method,
// matching the Ceph Compressor plugin's compress() output exactly.
func compressSegment(method CompressionAlgorithm, src []byte) ([]byte, error) {
	switch method {
	case CompAlgSnappy:
		// Raw snappy block format: self-describing (leading varint length),
		// no extra prefix (SnappyCompressor.h).
		return snappy.Encode(nil, src), nil
	case CompAlgZstd:
		// ZstdCompressor: a 4-byte little-endian original length, then a
		// standard zstd frame.
		out := binary.LittleEndian.AppendUint32(make([]byte, 0, 4+len(src)/2), uint32(len(src)))
		return zstdEncoder().EncodeAll(src, out), nil
	default:
		return nil, fmt.Errorf("%w: unsupported method %d", ErrCompression, method)
	}
}

// decompressSegment reverses compressSegment for one received segment.
func decompressSegment(method CompressionAlgorithm, src []byte) ([]byte, error) {
	switch method {
	case CompAlgSnappy:
		out, err := snappy.Decode(nil, src)
		if err != nil {
			return nil, fmt.Errorf("%w: snappy: %w", ErrCompression, err)
		}
		return out, nil
	case CompAlgZstd:
		if len(src) < 4 {
			return nil, fmt.Errorf("%w: zstd segment shorter than its length prefix", ErrCompression)
		}
		want := binary.LittleEndian.Uint32(src)
		out, err := zstdDecoder().DecodeAll(src[4:], make([]byte, 0, want))
		if err != nil {
			return nil, fmt.Errorf("%w: zstd: %w", ErrCompression, err)
		}
		if uint32(len(out)) != want {
			return nil, fmt.Errorf("%w: zstd length %d differs from the prefix %d", ErrCompression, len(out), want)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%w: unsupported method %d", ErrCompression, method)
	}
}
