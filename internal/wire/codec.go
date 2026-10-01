// Package wire implements the bounded little-endian encodings used by Ceph.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var (
	ErrLimit   = errors.New("ceph wire: length limit exceeded")
	ErrVersion = errors.New("ceph wire: unsupported encoding version")
)

const DefaultLimit = 16 << 20

type Encoder struct{ Data []byte }

func (e *Encoder) U8(v uint8)      { e.Data = append(e.Data, v) }
func (e *Encoder) U16(v uint16)    { e.Data = binary.LittleEndian.AppendUint16(e.Data, v) }
func (e *Encoder) U32(v uint32)    { e.Data = binary.LittleEndian.AppendUint32(e.Data, v) }
func (e *Encoder) U64(v uint64)    { e.Data = binary.LittleEndian.AppendUint64(e.Data, v) }
func (e *Encoder) Raw(v []byte)    { e.Data = append(e.Data, v...) }
func (e *Encoder) Bytes(v []byte)  { e.U32(uint32(len(v))); e.Raw(v) }
func (e *Encoder) String(v string) { e.Bytes([]byte(v)) }
func (e *Encoder) Struct(version, compat uint8, payload []byte) {
	e.U8(version)
	e.U8(compat)
	e.Bytes(payload)
}

// Decoder owns no input memory. Slice-returning methods borrow from the input.
// The first error is sticky; failed reads do not move the cursor.
type Decoder struct {
	data  []byte
	pos   int
	limit uint32
	err   error
}

func NewDecoder(data []byte) *Decoder { return NewDecoderLimit(data, DefaultLimit) }
func NewDecoderLimit(data []byte, limit uint32) *Decoder {
	d := &Decoder{data: data, limit: limit}
	if uint64(len(data)) > uint64(limit) {
		d.err = ErrLimit
	}
	return d
}
func (d *Decoder) Err() error     { return d.err }
func (d *Decoder) Remaining() int { return len(d.data) - d.pos }
func (d *Decoder) Fail(err error) {
	if d.err == nil {
		d.err = err
	}
}
func (d *Decoder) Raw(n uint32) []byte {
	if d.err != nil {
		return nil
	}
	if n > d.limit {
		d.Fail(ErrLimit)
		return nil
	}
	if uint64(n) > uint64(d.Remaining()) {
		d.Fail(io.ErrUnexpectedEOF)
		return nil
	}
	p := d.data[d.pos : d.pos+int(n)]
	d.pos += int(n)
	return p
}
func (d *Decoder) U8() uint8 {
	p := d.Raw(1)
	if p == nil {
		return 0
	}
	return p[0]
}
func (d *Decoder) U16() uint16 {
	p := d.Raw(2)
	if p == nil {
		return 0
	}
	return binary.LittleEndian.Uint16(p)
}
func (d *Decoder) U32() uint32 {
	p := d.Raw(4)
	if p == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(p)
}
func (d *Decoder) U64() uint64 {
	p := d.Raw(8)
	if p == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(p)
}
func (d *Decoder) Bytes() []byte  { return d.Raw(d.U32()) }
func (d *Decoder) String() string { return string(d.Bytes()) }
func (d *Decoder) Bool() bool {
	v := d.U8()
	if v > 1 {
		d.Fail(errors.New("ceph wire: invalid bool"))
	}
	return v == 1
}

// Count validates a collection before allocating. minSize is the minimum wire
// size of one element; maxCount caps even collections of empty elements.
func (d *Decoder) Count(minSize, maxCount uint32) int {
	n := d.U32()
	if n > maxCount || uint64(n)*uint64(minSize) > uint64(d.Remaining()) {
		d.Fail(ErrLimit)
		return 0
	}
	if d.err != nil {
		return 0
	}
	return int(n)
}

// Struct consumes a length-delimited versioned envelope. A newer version can be
// read when its compatibility version permits it; the caller may skip appended
// fields in the returned decoder without affecting the containing stream.
func (d *Decoder) Struct(supported uint8) (uint8, *Decoder) {
	v, compat := d.U8(), d.U8()
	p := d.Bytes()
	if v == 0 || compat > v || compat > supported {
		d.Fail(ErrVersion)
	}
	child := NewDecoderLimit(p, d.limit)
	child.Fail(d.err)
	return v, child
}

func (d *Decoder) Done() error {
	if d.err != nil {
		return d.err
	}
	if d.Remaining() != 0 {
		return fmt.Errorf("ceph wire: %d trailing bytes", d.Remaining())
	}
	return nil
}
