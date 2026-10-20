// Package msgr implements Tentacle's msgr2.1 transport wire format.
package msgr

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	BannerPrefix        = "ceph v2\n"
	Revision1    uint64 = 1
	Compression  uint64 = 2
)

var ErrFeatures = errors.New("ceph messenger: unsupported required features")

type Banner struct{ Supported, Required uint64 }

func (b Banner) Encode() []byte {
	p := append([]byte(BannerPrefix), 16, 0)
	p = binary.LittleEndian.AppendUint64(p, b.Supported)
	return binary.LittleEndian.AppendUint64(p, b.Required)
}

func ReadBanner(r io.Reader, local Banner) (Banner, error) {
	var b Banner
	p := make([]byte, 10)
	if _, err := io.ReadFull(r, p); err != nil {
		return b, err
	}
	if !bytes.Equal(p[:8], []byte(BannerPrefix)) {
		return b, errors.New("ceph messenger: invalid msgr2 banner")
	}
	n := binary.LittleEndian.Uint16(p[8:])
	if n != 16 {
		return b, fmt.Errorf("ceph messenger: unsupported banner length %d", n)
	}
	p = make([]byte, 16)
	if _, err := io.ReadFull(r, p); err != nil {
		return b, err
	}
	b.Supported = binary.LittleEndian.Uint64(p)
	b.Required = binary.LittleEndian.Uint64(p[8:])
	if b.Required&^b.Supported != 0 || local.Required&^b.Supported != 0 || b.Required&^local.Supported != 0 {
		return b, ErrFeatures
	}
	return b, nil
}

func WriteFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n < 0 || n > len(p) {
			return io.ErrShortWrite
		}
		p = p[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
