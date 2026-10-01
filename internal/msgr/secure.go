package msgr

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"math"
)

var ErrAuthentication = errors.New("ceph messenger: frame authentication failed")
var ErrNonce = errors.New("ceph messenger: nonce counter exhausted")

type gcmState struct {
	aead      cipher.AEAD
	nonce     [12]byte
	exhausted bool
}

func newGCM(key, nonce []byte) (*gcmState, error) {
	if len(key) != 16 || len(nonce) != 12 {
		return nil, errors.New("ceph messenger: invalid connection secret")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	s := &gcmState{aead: aead}
	copy(s.nonce[:], nonce)
	return s, nil
}

func (s *gcmState) next() ([12]byte, error) {
	if s.exhausted {
		return [12]byte{}, ErrNonce
	}
	nonce := s.nonce
	v := binary.LittleEndian.Uint64(s.nonce[4:])
	if v == math.MaxUint64 {
		s.exhausted = true
	} else {
		binary.LittleEndian.PutUint64(s.nonce[4:], v+1)
	}
	return nonce, nil
}
func (s *gcmState) seal(p []byte) ([]byte, error) {
	n, err := s.next()
	if err != nil {
		return nil, err
	}
	return s.aead.Seal(nil, n[:], p, nil), nil
}
func (s *gcmState) open(p []byte) ([]byte, error) {
	n, err := s.next()
	if err != nil {
		return nil, err
	}
	plain, err := s.aead.Open(nil, n[:], p, nil)
	if err != nil {
		return nil, ErrAuthentication
	}
	return plain, nil
}

// EnableSecure is called once, after AUTH_DONE, before AUTH_SIGNATURE. Readers
// and writers have independent nonce state and must each have a single owner.
func (r *Reader) EnableSecure(key, nonce []byte) error {
	if r.crypto != nil || r.err != nil {
		return errors.New("ceph messenger: invalid secure transition")
	}
	s, err := newGCM(key, nonce)
	if err == nil {
		r.crypto = s
	}
	return err
}
func (w *Writer) EnableSecure(key, nonce []byte) error {
	if w.crypto != nil || w.err != nil {
		return errors.New("ceph messenger: invalid secure transition")
	}
	s, err := newGCM(key, nonce)
	if err == nil {
		w.crypto = s
	}
	return err
}

func padded(n int) int    { return (n + 15) &^ 15 }
func pad(p []byte) []byte { out := make([]byte, padded(len(p))); copy(out, p); return out }

func (w *Writer) secureFrame(p []byte, segs [][]byte) ([]byte, error) {
	first := make([]byte, 80)
	copy(first, p)
	copy(first[32:], segs[0])
	out, err := w.crypto.seal(first)
	if err != nil {
		return nil, err
	}
	if len(segs[0]) > 48 {
		part, err := w.crypto.seal(pad(segs[0][48:]))
		if err != nil {
			return nil, err
		}
		out = append(out, part...)
	}
	if len(segs) > 1 {
		var tail []byte
		for _, seg := range segs[1:] {
			tail = append(tail, pad(seg)...)
		}
		tail = append(tail, 0x0e)
		tail = append(tail, make([]byte, 15)...)
		part, err := w.crypto.seal(tail)
		if err != nil {
			return nil, err
		}
		out = append(out, part...)
	}
	return out, nil
}

func (r *Reader) readSecure() (Frame, error) {
	p, err := r.readBytes(96)
	if err != nil {
		return Frame{}, err
	}
	first, err := r.crypto.open(p)
	if err != nil {
		return Frame{}, err
	}
	f, sizes, err := parsePreamble(first[:32], r.limit)
	if err != nil {
		return Frame{}, err
	}
	n := min(sizes[0], 48)
	f.Segments[0] = append([]byte(nil), first[32:32+n]...)
	if sizes[0] > 48 {
		p, err = r.readBytes(uint64(padded(int(sizes[0]-48))) + 16)
		if err != nil {
			return Frame{}, err
		}
		part, err := r.crypto.open(p)
		if err != nil {
			return Frame{}, err
		}
		f.Segments[0] = append(f.Segments[0], part[:sizes[0]-48]...)
	}
	if len(sizes) > 1 {
		var total uint64 = 32 // epilogue and auth tag
		for _, size := range sizes[1:] {
			total += uint64(padded(int(size)))
		}
		p, err = r.readBytes(total)
		if err != nil {
			return Frame{}, err
		}
		tail, err := r.crypto.open(p)
		if err != nil {
			return Frame{}, err
		}
		pos := 0
		for i, size := range sizes[1:] {
			f.Segments[i+1] = tail[pos : pos+int(size)]
			pos += padded(int(size))
		}
		if err := checkStatus(tail[pos]); err != nil {
			return Frame{}, err
		}
	}
	return f, nil
}
