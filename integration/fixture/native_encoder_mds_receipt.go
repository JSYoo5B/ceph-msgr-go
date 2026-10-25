package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"strconv"
)

func nativeProofNumber(m map[string]any, field string, want uint64) bool {
	n, ok := m[field].(json.Number)
	got, err := strconv.ParseUint(n.String(), 10, 64)
	return ok && err == nil && got == want
}

// sessionCursor walks the fresh native front only to bind the independent
// native decoder receipt to its bytes; it is not a product codec.
type sessionCursor struct {
	b  []byte
	ok bool
}

func (c *sessionCursor) take(n uint64) []byte {
	if !c.ok || n > uint64(len(c.b)) {
		c.ok = false
		return nil
	}
	v := c.b[:n]
	c.b = c.b[n:]
	return v
}

func (c *sessionCursor) u8() uint8 {
	if v := c.take(1); v != nil {
		return v[0]
	}
	return 0
}

func (c *sessionCursor) u32() uint32 {
	if v := c.take(4); v != nil {
		return binary.LittleEndian.Uint32(v)
	}
	return 0
}

func (c *sessionCursor) u64() uint64 {
	if v := c.take(8); v != nil {
		return binary.LittleEndian.Uint64(v)
	}
	return 0
}

func (c *sessionCursor) str() string { return string(c.take(uint64(c.u32()))) }

// structBody consumes an ENCODE_START(1, 1) envelope and returns its body.
func (c *sessionCursor) structBody() *sessionCursor {
	v, compat := c.u8(), c.u8()
	body := c.take(uint64(c.u32()))
	return &sessionCursor{b: body, ok: c.ok && v == 1 && compat == 1}
}

func nativeProofSigned(m map[string]any, field string, want int64) bool {
	n, ok := m[field].(json.Number)
	got, err := strconv.ParseInt(n.String(), 10, 64)
	return ok && err == nil && got == want
}

func nativeReceiptHasKeys(m map[string]any, keys ...string) bool {
	for _, key := range keys {
		if _, ok := m[key]; !ok {
			return false
		}
	}
	return len(m) == len(keys)
}

func (c *sessionCursor) u16() uint16 {
	if v := c.take(2); v != nil {
		return binary.LittleEndian.Uint16(v)
	}
	return 0
}

// envelope consumes an ENCODE_START(version, compat) envelope and returns
// its body; any other version/compat poisons the returned cursor.
func (c *sessionCursor) envelope(version, compat uint8) *sessionCursor {
	v, cv := c.u8(), c.u8()
	body := c.take(uint64(c.u32()))
	return &sessionCursor{b: body, ok: c.ok && v == version && cv == compat}
}

func (c *sessionCursor) done() bool { return c.ok && len(c.b) == 0 }

// Every decoder receipt class of the MDS suites; a capability or reconnect
// vector must carry exactly its own.
var nativeMDSReceiptClasses = []string{"decoded_caps", "decoded_cap_release", "decoded_lease", "decoded_snap", "decoded_quota",
	"decoded_request", "decoded_forward", "decoded_reply", "decoded_session", "decoded_map", "decoded_reconnect"}

// nativeReceiptBinder collects receipt fields that differ from the bytes.
type nativeReceiptBinder struct{ bad []string }

func (b *nativeReceiptBinder) number(m map[string]any, key string, got uint64) {
	if !nativeProofNumber(m, key, got) {
		b.bad = append(b.bad, key)
	}
}

func (b *nativeReceiptBinder) signed(m map[string]any, key string, got int64) {
	if !nativeProofSigned(m, key, got) {
		b.bad = append(b.bad, key)
	}
}

func (b *nativeReceiptBinder) hex(m map[string]any, key string, got []byte) {
	if m[key] != hex.EncodeToString(got) {
		b.bad = append(b.bad, key)
	}
}

func (b *nativeReceiptBinder) time(m map[string]any, key string, c *sessionCursor) {
	t, _ := m[key].(map[string]any)
	if t == nil || len(t) != 2 {
		b.bad = append(b.bad, key)
	}
	b.number(t, "sec", uint64(c.u32()))
	b.number(t, "nsec", uint64(c.u32()))
}

func (b *nativeReceiptBinder) ids(m map[string]any, key string, n uint32, c *sessionCursor) {
	ids, _ := m[key].([]any)
	if ids == nil || uint64(len(ids)) != uint64(n) {
		b.bad = append(b.bad, key)
		ids = nil
	}
	for _, id := range ids {
		number, isNumber := id.(json.Number)
		if !isNumber || number.String() != strconv.FormatUint(c.u64(), 10) {
			b.bad = append(b.bad, key)
		}
	}
}
