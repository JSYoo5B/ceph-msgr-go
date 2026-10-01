// Package cephx implements Tentacle credential and ticket cryptography.
package cephx

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

const (
	AES     uint16 = 1
	AES256K uint16 = 2
	magic   uint64 = 0xff009cad8826aa55
)

var ErrKey = errors.New("cephx: invalid or unsupported key")
var ErrIntegrity = errors.New("cephx: ciphertext integrity check failed")

// Key is immutable. Credentials are never included in String or error output.
type Key struct {
	kind   uint16
	secret []byte
}

func (k Key) Type() uint16     { return k.kind }
func (k Key) String() string   { return fmt.Sprintf("CephX key(type=%d)", k.kind) }
func (k Key) GoString() string { return k.String() }

func ParseKey(encoded string) (Key, error) {
	if len(encoded) > 128 {
		return Key{}, ErrKey
	}
	p, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return Key{}, ErrKey
	}
	d := wire.NewDecoder(p)
	k := DecodeKey(d)
	if d.Done() != nil {
		return Key{}, ErrKey
	}
	return k, nil
}

func DecodeKey(d *wire.Decoder) Key {
	typ := d.U16()
	d.U32()
	ns := d.U32()
	n := d.U16()
	p := d.Raw(uint32(n))
	if ns >= 1_000_000_000 || (typ != AES && typ != AES256K) || (typ == AES && n != 16) || (typ == AES256K && n != 32) {
		d.Fail(ErrKey)
	}
	if d.Err() != nil {
		return Key{}
	}
	return Key{kind: typ, secret: append([]byte(nil), p...)}
}

func (k Key) Signature(p []byte) []byte {
	h := hmac.New(sha256.New, k.secret)
	h.Write(p)
	return h.Sum(nil)
}

func (k Key) derive(usage uint32, kind byte, n int) []byte {
	p := make([]byte, 14)
	binary.BigEndian.PutUint32(p, 1)
	binary.BigEndian.PutUint32(p[4:], usage)
	p[8] = kind
	binary.BigEndian.PutUint32(p[10:], uint32(n*8))
	h := hmac.New(sha512.New384, k.secret)
	h.Write(p)
	return h.Sum(nil)[:n]
}

func (k Key) Encrypt(usage uint32, p []byte) ([]byte, error) {
	if len(p) > wire.DefaultLimit {
		return nil, wire.ErrLimit
	}
	conf := make([]byte, 16)
	if k.kind == AES256K {
		if _, err := rand.Read(conf); err != nil {
			return nil, err
		}
	}
	return k.encrypt(usage, p, conf)
}

func (k Key) encrypt(usage uint32, p, conf []byte) ([]byte, error) {
	switch k.kind {
	case AES:
		block, err := aes.NewCipher(k.secret)
		if err != nil {
			return nil, ErrKey
		}
		n := 16 - len(p)%16
		out := make([]byte, len(p)+n)
		copy(out, p)
		for i := len(p); i < len(out); i++ {
			out[i] = byte(n)
		}
		cipher.NewCBCEncrypter(block, []byte("cephsageyudagreg")).CryptBlocks(out, out)
		return out, nil
	case AES256K:
		if len(conf) != 16 {
			return nil, ErrKey
		}
		plain := append(append([]byte(nil), conf...), p...)
		block, err := aes.NewCipher(k.derive(usage, 0xaa, 32))
		if err != nil {
			return nil, ErrKey
		}
		out := encryptCTS(block, plain)
		h := hmac.New(sha512.New384, k.derive(usage, 0x55, 24))
		h.Write(make([]byte, 16))
		h.Write(out)
		return append(out, h.Sum(nil)[:24]...), nil
	default:
		return nil, ErrKey
	}
}

func (k Key) Decrypt(usage uint32, p []byte) ([]byte, error) {
	if len(p) > wire.DefaultLimit {
		return nil, wire.ErrLimit
	}
	switch k.kind {
	case AES:
		if len(p) == 0 || len(p)%16 != 0 {
			return nil, ErrIntegrity
		}
		block, err := aes.NewCipher(k.secret)
		if err != nil {
			return nil, ErrKey
		}
		out := append([]byte(nil), p...)
		cipher.NewCBCDecrypter(block, []byte("cephsageyudagreg")).CryptBlocks(out, out)
		n := int(out[len(out)-1])
		valid := subtle.ConstantTimeLessOrEq(1, n) & subtle.ConstantTimeLessOrEq(n, 16)
		for i := 1; i <= 16; i++ {
			check := subtle.ConstantTimeLessOrEq(i, n)
			valid &= 1 ^ (check & (1 ^ subtle.ConstantTimeByteEq(out[len(out)-i], byte(n))))
		}
		if valid != 1 {
			return nil, ErrIntegrity
		}
		return out[:len(out)-n], nil
	case AES256K:
		if len(p) < 40 {
			return nil, ErrIntegrity
		}
		body, tag := p[:len(p)-24], p[len(p)-24:]
		h := hmac.New(sha512.New384, k.derive(usage, 0x55, 24))
		h.Write(make([]byte, 16))
		h.Write(body)
		if !hmac.Equal(tag, h.Sum(nil)[:24]) {
			return nil, ErrIntegrity
		}
		block, err := aes.NewCipher(k.derive(usage, 0xaa, 32))
		if err != nil {
			return nil, ErrKey
		}
		out := decryptCTS(block, body)
		return out[16:], nil
	default:
		return nil, ErrKey
	}
}

// Seal/Open apply Ceph's version + magic envelope. The caller handles the
// outer uint32-length encoding used for encrypted bufferlists.
func (k Key) Seal(usage uint32, payload []byte) ([]byte, error) {
	e := wire.Encoder{}
	e.U8(1)
	e.U64(magic)
	e.Raw(payload)
	return k.Encrypt(usage, e.Data)
}
func (k Key) Open(usage uint32, ciphertext []byte) ([]byte, error) {
	p, err := k.Decrypt(usage, ciphertext)
	if err != nil {
		return nil, err
	}
	d := wire.NewDecoder(p)
	v, m := d.U8(), d.U64()
	if v != 1 || m != magic || d.Err() != nil {
		return nil, ErrIntegrity
	}
	return d.Raw(uint32(d.Remaining())), nil
}

// CBC-CS3: swap the final two CBC blocks, truncating the last output block to
// the original tail size. A single block is ordinary CBC with the zero IV.
func encryptCTS(block cipher.Block, p []byte) []byte {
	out := make([]byte, (len(p)+15)&^15)
	copy(out, p)
	cipher.NewCBCEncrypter(block, make([]byte, 16)).CryptBlocks(out, out)
	if len(out) == 16 {
		return out
	}
	n := len(out)
	prefix := n - 32
	tail := len(p) - prefix - 16
	last := append([]byte(nil), out[n-16:]...)
	penultimate := append([]byte(nil), out[n-32:n-16]...)
	copy(out[prefix:], last)
	copy(out[prefix+16:], penultimate[:tail])
	return out[:len(p)]
}
func decryptCTS(block cipher.Block, p []byte) []byte {
	if len(p) == 16 {
		out := make([]byte, 16)
		block.Decrypt(out, p)
		return out
	}
	prefix := ((len(p)+15)/16 - 2) * 16
	tail := len(p) - prefix - 16
	x := make([]byte, 16)
	block.Decrypt(x, p[prefix:prefix+16])
	penultimate := append([]byte(nil), x...)
	copy(penultimate, p[prefix+16:])
	cbc := make([]byte, prefix+16)
	copy(cbc, p[:prefix])
	copy(cbc[prefix:], penultimate)
	cipher.NewCBCDecrypter(block, make([]byte, 16)).CryptBlocks(cbc, cbc)
	for i := 0; i < tail; i++ {
		x[i] ^= penultimate[i]
	}
	return append(cbc, x[:tail]...)
}
