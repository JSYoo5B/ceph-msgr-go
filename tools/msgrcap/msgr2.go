package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"time"
)

// A deliberately small msgr2.1 (rev1) frame walker. It is written separately
// from internal/msgr so that capture interpretation does not merely replay the
// product codec, and it never emits segment payload bytes in reports.

const (
	bannerPrefix = "ceph v2\n"
	modeCRC      = 1
	modeSecure   = 2
	tagAuthDone  = 6
	tagMessage   = 17
	tagAck       = 20
	maxSegments  = 4
)

var tagNames = map[uint8]string{
	1: "HELLO", 2: "AUTH_REQUEST", 3: "AUTH_BAD_METHOD", 4: "AUTH_REPLY_MORE",
	5: "AUTH_REQUEST_MORE", 6: "AUTH_DONE", 7: "AUTH_SIGNATURE", 8: "CLIENT_IDENT",
	9: "SERVER_IDENT", 10: "IDENT_MISSING_FEATURES", 11: "SESSION_RECONNECT",
	12: "SESSION_RESET", 13: "SESSION_RETRY", 14: "SESSION_RETRY_GLOBAL",
	15: "SESSION_RECONNECT_OK", 16: "WAIT", 17: "MESSAGE", 18: "KEEPALIVE2",
	19: "KEEPALIVE2_ACK", 20: "ACK", 21: "COMPRESSION_REQUEST", 22: "COMPRESSION_DONE",
}

func tagName(t uint8) string {
	if n, ok := tagNames[t]; ok {
		return n
	}
	return fmt.Sprintf("UNKNOWN(%d)", t)
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Ceph's crc32c uses an explicit seed without final complement.
func cephCRC(seed uint32, p []byte) uint32 { return ^crc32.Update(^seed, castagnoli, p) }

type bannerInfo struct {
	Supported uint64 `json:"supported_features"`
	Required  uint64 `json:"required_features"`
}

type messageInfo struct {
	Seq     uint64 `json:"seq"`
	Tid     uint64 `json:"tid"`
	Type    uint16 `json:"type"`
	AckSeq  uint64 `json:"ack_seq"`
	Version uint16 `json:"version"`
}

type frameInfo struct {
	Index      int          `json:"index"`
	Offset     int64        `json:"stream_offset"`
	OnWire     int64        `json:"onwire_bytes"`
	Time       time.Time    `json:"completed_at"`
	Tag        string       `json:"tag"`
	TagNumber  uint8        `json:"tag_number"`
	Segments   []uint32     `json:"segment_lengths"`
	Compressed bool         `json:"compressed,omitempty"`
	Encrypted  bool         `json:"decrypted_secure,omitempty"`
	Message    *messageInfo `json:"message,omitempty"`
	AckSeq     *uint64      `json:"ack_seq,omitempty"`
	GlobalID   *uint64      `json:"global_id,omitempty"`
	ConMode    *uint32      `json:"con_mode,omitempty"`
	AuthMethod *uint32      `json:"auth_method,omitempty"`
	Modes      []uint32     `json:"preferred_modes,omitempty"`
	EntityType *uint8       `json:"entity_type,omitempty"`

	payload0 []byte // first segment, retained in memory only
}

type streamResult struct {
	Bytes       int64       `json:"reassembled_bytes"`
	Banner      *bannerInfo `json:"banner,omitempty"`
	Frames      []frameInfo `json:"frames,omitempty"`
	OpaqueBytes int64       `json:"opaque_secure_bytes,omitempty"`
	Remainder   int64       `json:"undecoded_tail_bytes,omitempty"`
	Stop        string      `json:"stop_reason,omitempty"`
}

// gcm mirrors the msgr2.1 secure nonce sequence: a 12-byte nonce whose last
// eight bytes are a little-endian counter incremented after every seal/open.
type gcm struct {
	aead  cipher.AEAD
	nonce [12]byte
}

func newGCMState(key, nonce []byte) (*gcm, error) {
	if len(key) != 16 || len(nonce) != 12 {
		return nil, errors.New("invalid connection secret")
	}
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	a, err := cipher.NewGCM(b)
	if err != nil {
		return nil, err
	}
	g := &gcm{aead: a}
	copy(g.nonce[:], nonce)
	return g, nil
}

func (g *gcm) open(p []byte) ([]byte, error) {
	n := g.nonce
	v := binary.LittleEndian.Uint64(g.nonce[4:])
	if v == math.MaxUint64 {
		return nil, errors.New("nonce exhausted")
	}
	binary.LittleEndian.PutUint64(g.nonce[4:], v+1)
	out, err := g.aead.Open(nil, n[:], p, nil)
	if err != nil {
		return nil, errors.New("AES-GCM authentication failed")
	}
	return out, nil
}

type walker struct {
	stream []byte
	pos    int64
	dir    *direction
	res    *streamResult
	limit  uint32
}

func (w *walker) need(n int64) ([]byte, bool) {
	if w.pos+n > int64(len(w.stream)) {
		return nil, false
	}
	p := w.stream[w.pos : w.pos+n]
	w.pos += n
	return p, true
}

func (w *walker) banner() bool {
	p, ok := w.need(26)
	if !ok {
		w.res.Stop = "stream ended before a complete msgr2 banner"
		return false
	}
	if !bytes.Equal(p[:8], []byte(bannerPrefix)) {
		w.res.Stop = "not a msgr2 banner (msgr1 or another protocol)"
		return false
	}
	if binary.LittleEndian.Uint16(p[8:]) != 16 {
		w.res.Stop = "unsupported msgr2 banner payload length"
		return false
	}
	w.res.Banner = &bannerInfo{binary.LittleEndian.Uint64(p[10:]), binary.LittleEndian.Uint64(p[18:])}
	return true
}

type preamble struct {
	tag   uint8
	sizes []uint32
	align []uint16
	flags uint8
}

func (w *walker) parsePreamble(p []byte) (preamble, error) {
	var pr preamble
	if cephCRC(0, p[:28]) != binary.LittleEndian.Uint32(p[28:]) {
		return pr, errors.New("preamble CRC mismatch")
	}
	pr.tag, pr.flags = p[0], p[26]
	n := int(p[1])
	if n < 1 || n > maxSegments || p[27] != 0 {
		return pr, errors.New("invalid preamble segment count or reserved byte")
	}
	var total uint64
	for i := 0; i < n; i++ {
		pr.sizes = append(pr.sizes, binary.LittleEndian.Uint32(p[2+i*6:]))
		pr.align = append(pr.align, binary.LittleEndian.Uint16(p[6+i*6:]))
		total += uint64(pr.sizes[i])
	}
	if total > uint64(w.limit) {
		return pr, fmt.Errorf("frame of %d bytes exceeds the analysis limit", total)
	}
	return pr, nil
}

// crcFrame decodes one rev1 CRC-mode frame (also used for every pre-auth
// frame in both connection modes).
func (w *walker) crcFrame() (frameInfo, [][]byte, error) {
	start := w.pos
	p, ok := w.need(32)
	if !ok {
		return frameInfo{}, nil, errIncomplete
	}
	pr, err := w.parsePreamble(p)
	if err != nil {
		return frameInfo{}, nil, err
	}
	segs := make([][]byte, len(pr.sizes))
	for i, size := range pr.sizes {
		if segs[i], ok = w.need(int64(size)); !ok {
			return frameInfo{}, nil, errIncomplete
		}
		if i == 0 && size > 0 {
			c, ok := w.need(4)
			if !ok {
				return frameInfo{}, nil, errIncomplete
			}
			if cephCRC(^uint32(0), segs[0]) != binary.LittleEndian.Uint32(c) {
				return frameInfo{}, nil, errors.New("segment 0 CRC mismatch")
			}
		}
	}
	if len(pr.sizes) > 1 {
		epi, ok := w.need(13)
		if !ok {
			return frameInfo{}, nil, errIncomplete
		}
		for i := 1; i < len(pr.sizes); i++ {
			if cephCRC(^uint32(0), segs[i]) != binary.LittleEndian.Uint32(epi[1+(i-1)*4:]) {
				return frameInfo{}, nil, fmt.Errorf("segment %d CRC mismatch", i)
			}
		}
		if epi[0]&0x0f != 0x0e {
			return frameInfo{}, nil, fmt.Errorf("frame late status %#x (aborted or invalid)", epi[0])
		}
	}
	return w.info(start, pr), segs, nil
}

func padded(n uint32) int64 { return (int64(n) + 15) &^ 15 }

// secureFrame decrypts one rev1 secure frame with the derived stream state.
func (w *walker) secureFrame(g *gcm) (frameInfo, [][]byte, error) {
	start := w.pos
	p, ok := w.need(96)
	if !ok {
		return frameInfo{}, nil, errIncomplete
	}
	first, err := g.open(p)
	if err != nil {
		return frameInfo{}, nil, err
	}
	pr, err := w.parsePreamble(first[:32])
	if err != nil {
		return frameInfo{}, nil, err
	}
	segs := make([][]byte, len(pr.sizes))
	inline := min(pr.sizes[0], 48)
	segs[0] = append([]byte(nil), first[32:32+inline]...)
	if pr.sizes[0] > 48 {
		if p, ok = w.need(padded(pr.sizes[0]-48) + 16); !ok {
			return frameInfo{}, nil, errIncomplete
		}
		rest, err := g.open(p)
		if err != nil {
			return frameInfo{}, nil, err
		}
		segs[0] = append(segs[0], rest[:pr.sizes[0]-48]...)
	}
	if len(pr.sizes) > 1 {
		total := int64(32)
		for _, s := range pr.sizes[1:] {
			total += padded(s)
		}
		if p, ok = w.need(total); !ok {
			return frameInfo{}, nil, errIncomplete
		}
		tail, err := g.open(p)
		if err != nil {
			return frameInfo{}, nil, err
		}
		var pos int64
		for i, s := range pr.sizes[1:] {
			segs[i+1] = tail[pos : pos+int64(s)]
			pos += padded(s)
		}
		if tail[pos]&0x0f != 0x0e {
			return frameInfo{}, nil, fmt.Errorf("frame late status %#x (aborted or invalid)", tail[pos])
		}
	}
	f := w.info(start, pr)
	f.Encrypted = true
	return f, segs, nil
}

var errIncomplete = errors.New("incomplete frame at end of reassembled stream")

func (w *walker) info(start int64, pr preamble) frameInfo {
	return frameInfo{
		Index:      len(w.res.Frames),
		Offset:     start,
		OnWire:     w.pos - start,
		Time:       w.dir.timeAt(w.pos),
		Tag:        tagName(pr.tag),
		TagNumber:  pr.tag,
		Segments:   pr.sizes,
		Compressed: pr.flags&1 != 0,
	}
}

// annotate extracts only identity/control metadata from known frames.
func annotate(f *frameInfo, segs [][]byte) {
	if f.Compressed || len(segs) == 0 {
		return
	}
	s := segs[0]
	f.payload0 = s
	u32 := func(o int) uint32 { return binary.LittleEndian.Uint32(s[o:]) }
	u64 := func(o int) uint64 { return binary.LittleEndian.Uint64(s[o:]) }
	switch f.TagNumber {
	case 1: // HELLO: entity type, peer address
		if len(s) >= 1 {
			v := s[0]
			f.EntityType = &v
		}
	case 2: // AUTH_REQUEST: method, preferred modes, payload
		if len(s) >= 8 {
			m, n := u32(0), int(u32(4))
			f.AuthMethod = &m
			for i := 0; i < n && i < 8 && 8+4*i+4 <= len(s); i++ {
				f.Modes = append(f.Modes, u32(8+4*i))
			}
		}
	case tagAuthDone: // global_id, con_mode, payload
		if len(s) >= 12 {
			g, m := u64(0), u32(8)
			f.GlobalID, f.ConMode = &g, &m
		}
	case tagMessage: // ceph_msg_header2
		if len(s) == 41 {
			f.Message = &messageInfo{Seq: u64(0), Tid: u64(8), Type: binary.LittleEndian.Uint16(s[16:]),
				Version: binary.LittleEndian.Uint16(s[20:]), AckSeq: u64(28)}
		}
	case tagAck:
		if len(s) == 8 {
			v := u64(0)
			f.AckSeq = &v
		}
	}
}

// authDonePayload returns the CephX payload carried by AUTH_DONE.
func authDonePayload(f frameInfo) ([]byte, bool) {
	s := f.payload0
	if f.TagNumber != tagAuthDone || len(s) < 16 {
		return nil, false
	}
	n := binary.LittleEndian.Uint32(s[12:])
	if uint64(n) != uint64(len(s)-16) {
		return nil, false
	}
	return s[16:], true
}
