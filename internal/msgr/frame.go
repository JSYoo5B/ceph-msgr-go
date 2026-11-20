package msgr

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Tag is a msgr2.1 frame tag, as numbered by src/msg/async/frames_v2.h at
// Ceph v20.2.4 (commit 7f793731f1b39eb4f465e960113d2363c311b964); the
// handshake follows ProtocolV2.cc at the same tag. This is the client side
// only: no server-side tag handling exists, because the repository has no
// listening Messenger. Each tag's comment states its phase, what the Go
// client does with it and what it refuses. A tag received outside the phase
// that expects it is a terminal ErrFrame, never ignored.
type Tag uint8

const (
	// Hello (banner exchange) is sent first with the client's entity type and
	// target address; exactly one Hello is received, and its entity type
	// must equal the requested role. Wrong role, extra segments or trailing
	// bytes are refused.
	Hello Tag = iota + 1
	// AuthRequest (authentication) is sent once with CephX method 2 and one
	// requested connection mode, CRC or secure, never both.
	AuthRequest
	// AuthBadMethod (authentication) is received as a typed authentication
	// error carrying the server method and int32 result code. It is always
	// terminal: no method downgrade or retry.
	AuthBadMethod
	// AuthReplyMore (authentication) is received and answered by the CephX
	// state machine. A malformed payload is a protocol failure; at most 8
	// rounds are accepted.
	AuthReplyMore
	// AuthRequestMore (authentication) is sent in answer to AuthReplyMore.
	AuthRequestMore
	// AuthDone (authentication) is received with the global ID; the selected
	// mode must equal the requested one and secure mode needs its secret.
	// A mode mismatch, a missing secret or no AuthDone within 8 rounds is
	// refused.
	AuthDone
	// AuthSignature (authentication) is sent and received in both modes: the
	// client signs its received transcript and verifies the peer's signature
	// over its sent transcript with the CephX session key before admission.
	// A mismatch is an authentication failure.
	AuthSignature
	// ClientIdent (identification) is sent with the role's supported and
	// required feature masks, session flags and cookie.
	ClientIdent
	// ServerIdent (identification) is received; the target address must be
	// among the server's addresses, the daemon ID must match where required,
	// and its features and flags must be acceptable. Otherwise the session
	// is refused.
	ServerIdent
	// IdentMissingFeatures (identification) is received as ErrFeatures with
	// the peer's 64-bit missing mask in the error text (no structured
	// field). It is always terminal; nothing is silently downgraded.
	IdentMissingFeatures
	// SessionReconnect (resume) is sent only for an explicit stateful resume
	// (fresh MDS or CephFS) with the next global and connect sequences.
	SessionReconnect
	// SessionReset (resume) is received as the terminal ErrSessionReset of a
	// stateful session; a request whose transmission began keeps an unknown
	// outcome.
	SessionReset
	// SessionRetry (resume) is received; the connect sequence is advanced and
	// the reconnect resent, at most 8 times. Sequence overflow or a
	// malformed body is refused.
	SessionRetry
	// SessionRetryGlobal (resume) is received; the global sequence is
	// advanced and the reconnect resent, at most 8 times. Sequence overflow
	// or a malformed body is refused.
	SessionRetryGlobal
	// SessionReconnectOK (resume) is received; the stream resumes and
	// unacknowledged messages above the peer's sequence are retransmitted.
	SessionReconnectOK
	// Wait (resume) is received and refused: no wait or replace policy is
	// implemented.
	Wait
	// Message (established) is sent and received. Sequence, ACK sequence and
	// stateful gaps are validated, duplicates dropped and replies matched to
	// pending requests by type and transaction. A zero sequence, an ACK
	// beyond what was sent, a stateful gap or a wrong reply type is refused.
	Message
	// Keepalive (established) is sent periodically; a received Keepalive is
	// echoed as KeepaliveAck. A segment other than 8 bytes is refused.
	Keepalive
	// KeepaliveAck (established) is received and refreshes liveness; the
	// keepalive timeout is the only liveness policy. A segment other than 8
	// bytes is refused.
	KeepaliveAck
	// Ack (established) is sent for received messages and received for sent
	// ones, releasing acknowledged messages. A sequence beyond what was sent
	// or a wrong length is refused.
	Ack
	// CompressionRequest (post-auth) is sent only when the peer banner
	// supports compression; it offers snappy and zstd on CRC connections and
	// declines on secure ones.
	CompressionRequest
	// CompressionDone (post-auth) is received; a selected method must have
	// been offered and implemented.
	CompressionDone
)

// FrameEarlyDataCompressed is the preamble flag bit (msgr2.1
// FRAME_EARLY_DATA_COMPRESSED) that marks a frame whose segments are each
// compressed with the negotiated method.
const FrameEarlyDataCompressed byte = 0x1

var (
	ErrFrame   = errors.New("ceph messenger: malformed frame")
	ErrCRC     = errors.New("ceph messenger: CRC mismatch")
	ErrAborted = errors.New("ceph messenger: aborted frame")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Ceph uses an uncomplemented CRC with explicit seed. Go's Update complements
// both ends. Preambles use seed 0; segments use seed 0xffffffff.
func CRC(seed uint32, p []byte) uint32 { return ^crc32.Update(^seed, castagnoli, p) }

type Frame struct {
	Tag       Tag
	Segments  [][]byte
	Alignment []uint16
}

type Reader struct {
	input  io.Reader
	limit  uint32
	crypto *gcmState
	comp   CompressionAlgorithm
	err    error
}
type Writer struct {
	output  io.Writer
	limit   uint32
	crypto  *gcmState
	comp    CompressionAlgorithm
	compMin int
	err     error
}

func NewReader(r io.Reader, limit uint32) *Reader {
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	return &Reader{input: r, limit: limit}
}
func NewWriter(w io.Writer, limit uint32) *Writer {
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	return &Writer{output: w, limit: limit}
}

// trailingSegments returns the number of segments to transmit, dropping empty
// trailing segments as Ceph does (always keeping at least the first).
func trailingSegments(segs [][]byte) int {
	n := len(segs)
	for n > 1 && len(segs[n-1]) == 0 {
		n--
	}
	return n
}

// buildPreamble encodes the 32-byte preamble for the given onwire segments
// (already compressed when flags marks it). The caller has dropped trailing
// empty segments.
func buildPreamble(tag Tag, segs [][]byte, align []uint16, limit uint32, flags byte) ([]byte, error) {
	n := len(segs)
	if tag < Hello || tag > CompressionDone || n < 1 || n > 4 {
		return nil, ErrFrame
	}
	p := make([]byte, 32)
	p[0], p[1] = byte(tag), byte(n)
	var total uint64
	for i, seg := range segs {
		total += uint64(len(seg))
		if total > uint64(limit) {
			return nil, wire.ErrLimit
		}
		binary.LittleEndian.PutUint32(p[2+i*6:], uint32(len(seg)))
		alignment := uint16(8)
		if i < len(align) {
			alignment = align[i]
		}
		binary.LittleEndian.PutUint16(p[6+i*6:], alignment)
	}
	p[26] = flags
	binary.LittleEndian.PutUint32(p[28:], CRC(0, p[:28]))
	return p, nil
}

func parsePreamble(p []byte, limit uint32) (Frame, []uint32, byte, error) {
	var f Frame
	if len(p) != 32 {
		return f, nil, 0, ErrFrame
	}
	if CRC(0, p[:28]) != binary.LittleEndian.Uint32(p[28:]) {
		return f, nil, 0, ErrCRC
	}
	n := int(p[1])
	f.Tag = Tag(p[0])
	flags := p[26]
	if n < 1 || n > 4 || f.Tag < Hello || f.Tag > CompressionDone || p[27] != 0 || flags&^FrameEarlyDataCompressed != 0 {
		return f, nil, 0, ErrFrame
	}
	sizes := make([]uint32, n)
	f.Alignment = make([]uint16, n)
	var total uint64
	for i := 0; i < 4; i++ {
		size := binary.LittleEndian.Uint32(p[2+i*6:])
		align := binary.LittleEndian.Uint16(p[6+i*6:])
		if i >= n {
			if size != 0 || align != 0 {
				return f, nil, 0, ErrFrame
			}
			continue
		}
		sizes[i], f.Alignment[i] = size, align
		total += uint64(size)
	}
	if total > uint64(limit) {
		return f, nil, 0, wire.ErrLimit
	}
	if n > 1 && sizes[n-1] == 0 {
		return f, nil, 0, ErrFrame
	}
	f.Segments = make([][]byte, n)
	return f, sizes, flags, nil
}

func (w *Writer) Write(f Frame) error {
	if w.err != nil {
		return w.err
	}
	if f.Tag < Hello || f.Tag > CompressionDone || len(f.Segments) < 1 || len(f.Segments) > 4 {
		return ErrFrame
	}
	n := trailingSegments(f.Segments)
	segs := f.Segments[:n]
	var flags byte
	// Compression runs before CRC and encryption (Ceph asm_compress). It is
	// only enabled on CRC-mode connections for now, so w.crypto is nil here.
	if w.comp != CompAlgNone && w.crypto == nil && segmentsTotal(segs) >= w.compMin {
		compressed, err := compressSegments(w.comp, segs)
		if err != nil {
			return err
		}
		segs, flags = compressed, FrameEarlyDataCompressed
	}
	p, err := buildPreamble(f.Tag, segs, f.Alignment, w.limit, flags)
	if err != nil {
		return err
	} // Nothing has been transmitted or sealed.
	var out []byte
	if w.crypto != nil {
		out, err = w.secureFrame(p, segs)
	} else {
		out = p
		if len(segs[0]) > 0 {
			out = append(out, segs[0]...)
			out = binary.LittleEndian.AppendUint32(out, CRC(^uint32(0), segs[0]))
		}
		if n > 1 {
			for _, seg := range segs[1:] {
				out = append(out, seg...)
			}
			out = append(out, 0x0e)
			for i := 1; i < 4; i++ {
				var crc uint32
				if i < n {
					crc = CRC(^uint32(0), segs[i])
				}
				out = binary.LittleEndian.AppendUint32(out, crc)
			}
		}
	}
	if err == nil {
		err = WriteFull(w.output, out)
	}
	if err != nil {
		w.err = err
	} // Nonces/partial writes cannot be rolled back.
	return err
}

func (r *Reader) readBytes(n uint64) ([]byte, error) {
	// Padding, tags and epilogues need only a small overhead beyond logical size.
	if n > uint64(r.limit)+128 {
		return nil, wire.ErrLimit
	}
	p := make([]byte, int(n))
	_, err := io.ReadFull(r.input, p)
	return p, err
}

func (r *Reader) Read() (Frame, error) {
	if r.err != nil {
		return Frame{}, r.err
	}
	f, err := r.read()
	if err != nil && !errors.Is(err, ErrAborted) {
		r.err = err
	}
	return f, err
}

func (r *Reader) read() (Frame, error) {
	if r.crypto != nil {
		return r.readSecure()
	}
	p, err := r.readBytes(32)
	if err != nil {
		return Frame{}, err
	}
	f, sizes, flags, err := parsePreamble(p, r.limit)
	if err != nil {
		return Frame{}, err
	}
	for i, size := range sizes {
		f.Segments[i], err = r.readBytes(uint64(size))
		if err != nil {
			return Frame{}, err
		}
		if i == 0 && size > 0 {
			crc, err := r.readBytes(4)
			if err != nil {
				return Frame{}, err
			}
			if CRC(^uint32(0), f.Segments[i]) != binary.LittleEndian.Uint32(crc) {
				return Frame{}, ErrCRC
			}
		}
	}
	if len(sizes) > 1 {
		epi, err := r.readBytes(13)
		if err != nil {
			return Frame{}, err
		}
		for i := 1; i < len(sizes); i++ {
			want := CRC(^uint32(0), f.Segments[i])
			if binary.LittleEndian.Uint32(epi[1+(i-1)*4:]) != want {
				return Frame{}, ErrCRC
			}
		}
		if err := checkStatus(epi[0]); err != nil {
			return Frame{}, err
		}
	}
	if err := r.decompressFrame(&f, flags); err != nil {
		return Frame{}, err
	}
	return f, nil
}

func checkStatus(status byte) error {
	switch status & 0x0f {
	case 0x0e:
		return nil
	case 0x01:
		return ErrAborted
	default:
		return fmt.Errorf("%w: late status %#x", ErrFrame, status)
	}
}
