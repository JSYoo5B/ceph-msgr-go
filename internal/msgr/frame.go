package msgr

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

type Tag uint8

const (
	Hello Tag = iota + 1
	AuthRequest
	AuthBadMethod
	AuthReplyMore
	AuthRequestMore
	AuthDone
	AuthSignature
	ClientIdent
	ServerIdent
	IdentMissingFeatures
	SessionReconnect
	SessionReset
	SessionRetry
	SessionRetryGlobal
	SessionReconnectOK
	Wait
	Message
	Keepalive
	KeepaliveAck
	Ack
	CompressionRequest
	CompressionDone
)

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
	err    error
}
type Writer struct {
	output io.Writer
	limit  uint32
	crypto *gcmState
	err    error
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

func preamble(f Frame, limit uint32) ([]byte, int, error) {
	if f.Tag < Hello || f.Tag > CompressionDone || len(f.Segments) < 1 || len(f.Segments) > 4 {
		return nil, 0, ErrFrame
	}
	n := len(f.Segments)
	for n > 1 && len(f.Segments[n-1]) == 0 {
		n--
	}
	p := make([]byte, 32)
	p[0], p[1] = byte(f.Tag), byte(n)
	var total uint64
	for i, seg := range f.Segments[:n] {
		total += uint64(len(seg))
		if total > uint64(limit) {
			return nil, 0, wire.ErrLimit
		}
		binary.LittleEndian.PutUint32(p[2+i*6:], uint32(len(seg)))
		alignment := uint16(8)
		if i < len(f.Alignment) {
			alignment = f.Alignment[i]
		}
		binary.LittleEndian.PutUint16(p[6+i*6:], alignment)
	}
	binary.LittleEndian.PutUint32(p[28:], CRC(0, p[:28]))
	return p, n, nil
}

func parsePreamble(p []byte, limit uint32) (Frame, []uint32, error) {
	var f Frame
	if len(p) != 32 {
		return f, nil, ErrFrame
	}
	if CRC(0, p[:28]) != binary.LittleEndian.Uint32(p[28:]) {
		return f, nil, ErrCRC
	}
	n := int(p[1])
	f.Tag = Tag(p[0])
	if n < 1 || n > 4 || f.Tag < Hello || f.Tag > CompressionDone || p[26] != 0 || p[27] != 0 {
		return f, nil, ErrFrame
	}
	sizes := make([]uint32, n)
	f.Alignment = make([]uint16, n)
	var total uint64
	for i := 0; i < 4; i++ {
		size := binary.LittleEndian.Uint32(p[2+i*6:])
		align := binary.LittleEndian.Uint16(p[6+i*6:])
		if i >= n {
			if size != 0 || align != 0 {
				return f, nil, ErrFrame
			}
			continue
		}
		sizes[i], f.Alignment[i] = size, align
		total += uint64(size)
	}
	if total > uint64(limit) {
		return f, nil, wire.ErrLimit
	}
	if n > 1 && sizes[n-1] == 0 {
		return f, nil, ErrFrame
	}
	f.Segments = make([][]byte, n)
	return f, sizes, nil
}

func (w *Writer) Write(f Frame) error {
	if w.err != nil {
		return w.err
	}
	p, n, err := preamble(f, w.limit)
	if err != nil {
		return err
	} // Nothing has been transmitted or sealed.
	var out []byte
	if w.crypto != nil {
		out, err = w.secureFrame(p, f.Segments[:n])
	} else {
		out = p
		if len(f.Segments[0]) > 0 {
			out = append(out, f.Segments[0]...)
			out = binary.LittleEndian.AppendUint32(out, CRC(^uint32(0), f.Segments[0]))
		}
		if n > 1 {
			for _, seg := range f.Segments[1:n] {
				out = append(out, seg...)
			}
			out = append(out, 0x0e)
			for i := 1; i < 4; i++ {
				var crc uint32
				if i < n {
					crc = CRC(^uint32(0), f.Segments[i])
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
	f, sizes, err := parsePreamble(p, r.limit)
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
		if err := checkStatus(epi[0]); err != nil {
			return Frame{}, err
		}
		for i := 1; i < 4; i++ {
			var want uint32
			if i < len(sizes) {
				want = CRC(^uint32(0), f.Segments[i])
			}
			if binary.LittleEndian.Uint32(epi[1+(i-1)*4:]) != want {
				return Frame{}, ErrCRC
			}
		}
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
