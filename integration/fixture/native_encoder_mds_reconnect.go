package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
)

// The reconnect family is closed the same way: its type-23 header rules and
// empty front can never be selected by another specification.
func closedMDSReconnectEncoderSpec(spec nativeEncoderSpec) bool {
	return spec.command == mdsReconnectEncoderSpec.command && spec.wrapper == mdsReconnectEncoderSpec.wrapper &&
		spec.driver == mdsReconnectEncoderSpec.driver && slices.Equal(spec.names, mdsReconnectEncoderSpec.names) &&
		spec.mdsReconnect && !spec.mdsMaps && !spec.mdsSessions && !spec.mdsRequests && !spec.mdsCaps && spec.requests == 0 && spec.events == 0 && !spec.controls &&
		spec.requestVersion == 0 && !spec.mixedV8 && spec.replyVersion == 0
}

// nativeReconnectVector is the closed contract of one fresh native
// MClientReconnect vector (messages/MClientReconnect.h, mds/mdstypes.cc
// cap_reconnect_t/snaprealm_reconnect_t at the pinned commit): type 23 at its
// declared HEAD_VERSION 5/COMPAT_VERSION 4, no transaction ID, an empty front
// and middle, and the cap map, realm vector and more flag in the data segment.
type nativeReconnectVector struct {
	caps, realms int
	locked       int // caps carrying a Client::_encode_filelocks blob
	more         bool
}

const (
	nativeReconnectType    = 23
	nativeReconnectVersion = 5
	nativeReconnectCompat  = 4
)

var nativeReconnectVectors = map[string]nativeReconnectVector{
	"mds-reconnect-native-reconnect-two-caps": {caps: 2, realms: 2, locked: 1},
	"mds-reconnect-native-reconnect-more":     {caps: 1, more: true},
	"mds-reconnect-native-reconnect-empty":    {realms: 1},
}

// Closed receipt schemas of the reconnect suite.
var (
	nativeReconnectReceiptKeys = []string{"message_type", "version", "compat_version", "tid", "caps", "realms", "more"}
	nativeReconnectCapKeys     = []string{"ino", "path", "cap_id", "wanted", "issued", "snaprealm", "pathbase", "flock_len",
		"flockbl_hex", "snap_follows", "fcntl_locks", "flock_locks"}
	nativeReconnectRealmKeys = []string{"ino", "seq", "parent"}
	nativeFilelockKeys       = []string{"start", "length", "client", "owner", "pid", "type"}
)

func validateNativeMDSReconnectFixture(name string, fixture map[string]any, segments map[string][]byte) error {
	want, ok := nativeReconnectVectors[name]
	if !ok {
		return fmt.Errorf("native proof has unknown MDS reconnect vector %s", name)
	}
	h, front, data := segments["header"], segments["front"], segments["data"]
	if len(h) != 41 || len(front) != 0 || len(data) == 0 || binary.LittleEndian.Uint64(h[:8]) != 1 ||
		binary.LittleEndian.Uint64(h[8:16]) != 0 || binary.LittleEndian.Uint16(h[16:18]) != nativeReconnectType ||
		binary.LittleEndian.Uint16(h[18:20]) != 127 || binary.LittleEndian.Uint16(h[20:22]) != nativeReconnectVersion ||
		binary.LittleEndian.Uint32(h[22:26]) != 0 || binary.LittleEndian.Uint16(h[26:28]) != 0 ||
		binary.LittleEndian.Uint64(h[28:36]) != 3 || h[36] != 1 ||
		binary.LittleEndian.Uint16(h[37:39]) != nativeReconnectCompat || binary.LittleEndian.Uint16(h[39:41]) != 0 {
		return fmt.Errorf("native proof has unsupported %s reconnect header", name)
	}
	// The payload never leaves the data segment; a recorded middle segment
	// is outside this closed contract.
	for _, key := range []string{"middle_hex", "middle_bytes", "middle_sha256"} {
		if _, present := fixture[key]; present {
			return fmt.Errorf("native proof has an unreviewed %s middle segment", name)
		}
	}
	metadata, ok := fixture["native_header"].(map[string]any)
	if !ok {
		return fmt.Errorf("native proof lacks %s header metadata", name)
	}
	for field, n := range map[string]uint64{"seq": 1, "tid": 0, "type": nativeReconnectType, "priority": 127, "version": nativeReconnectVersion, "data_off": 0, "ack_seq": 3, "flags": 1, "compat": nativeReconnectCompat, "reserved": 0, "front_len": 0, "middle_len": 0, "data_len": uint64(len(data))} {
		if !nativeProofNumber(metadata, field, n) {
			return fmt.Errorf("native proof has inconsistent %s header %s", name, field)
		}
	}
	if len(metadata) != 13 {
		return fmt.Errorf("native proof has unreviewed %s header metadata", name)
	}
	for _, segment := range []string{"front", "data", "header"} {
		if fixture[segment+"_sha256"] != fmt.Sprintf("%x", sha256.Sum256(segments[segment])) {
			return fmt.Errorf("native proof has inconsistent %s %s digest", name, segment)
		}
	}
	for _, key := range nativeMDSReceiptClasses {
		if _, present := fixture[key]; present != (key == "decoded_reconnect") {
			return fmt.Errorf("native proof has a foreign %s decoder receipt class", name)
		}
	}
	receipt, ok := fixture["decoded_reconnect"].(map[string]any)
	if !ok || !nativeReceiptHasKeys(receipt, nativeReconnectReceiptKeys...) {
		return fmt.Errorf("native proof lacks %s complete independent decoder receipt", name)
	}
	for field, n := range map[string]uint64{"message_type": nativeReconnectType, "version": nativeReconnectVersion, "compat_version": nativeReconnectCompat, "tid": 0} {
		if !nativeProofNumber(receipt, field, n) {
			return fmt.Errorf("native proof has inconsistent %s decoded %s", name, field)
		}
	}
	if bad := validateNativeMDSReconnectData(want, receipt, &sessionCursor{b: data, ok: true}); len(bad) != 0 {
		return fmt.Errorf("native proof has inconsistent %s decoded %v", name, bad)
	}
	return nil
}

// validateNativeMDSReconnectData walks the fresh native MClientReconnect v5
// data segment (map<inodeno_t, cap_reconnect_t> with v2/compat1 entries,
// vector<snaprealm_reconnect_t> with v1/compat1 entries, then the bool more)
// only to bind the independent native decoder receipt to those bytes.
func validateNativeMDSReconnectData(want nativeReconnectVector, r map[string]any, c *sessionCursor) []string {
	b := &nativeReceiptBinder{}
	caps, _ := r["caps"].([]any)
	if n := c.u32(); caps == nil || uint64(len(caps)) != uint64(n) || len(caps) != want.caps {
		b.bad = append(b.bad, "caps")
		caps = nil
	}
	locked := 0
	var previous uint64
	for i, entry := range caps {
		m, _ := entry.(map[string]any)
		if !nativeReceiptHasKeys(m, nativeReconnectCapKeys...) {
			b.bad = append(b.bad, "cap receipt")
		}
		ino := c.u64()
		if i > 0 && ino <= previous {
			b.bad = append(b.bad, "cap map order") // std::map keys are unique and ordered
		}
		previous = ino
		b.number(m, "ino", ino)
		body := c.envelope(2, 1)
		if m["path"] != body.str() {
			b.bad = append(b.bad, "path")
		}
		b.number(m, "cap_id", body.u64())
		b.number(m, "wanted", uint64(body.u32()))
		b.number(m, "issued", uint64(body.u32()))
		b.number(m, "snaprealm", body.u64())
		b.number(m, "pathbase", body.u64())
		flockLen := body.u32()
		b.number(m, "flock_len", uint64(flockLen))
		flock := body.take(uint64(flockLen))
		b.hex(m, "flockbl_hex", flock)
		b.filelocks(m, flock)
		b.number(m, "snap_follows", body.u64())
		if !body.done() {
			b.bad = append(b.bad, "cap envelope")
		}
		if len(flock) != 0 {
			locked++
		}
	}
	realms, _ := r["realms"].([]any)
	if n := c.u32(); realms == nil || uint64(len(realms)) != uint64(n) || len(realms) != want.realms {
		b.bad = append(b.bad, "realms")
		realms = nil
	}
	for _, entry := range realms {
		m, _ := entry.(map[string]any)
		if !nativeReceiptHasKeys(m, nativeReconnectRealmKeys...) {
			b.bad = append(b.bad, "realm receipt")
		}
		body := c.envelope(1, 1)
		for _, key := range nativeReconnectRealmKeys {
			b.number(m, key, body.u64())
		}
		if !body.done() {
			b.bad = append(b.bad, "realm envelope")
		}
	}
	more := c.u8()
	if more > 1 || r["more"] != (more == 1) || (more == 1) != want.more {
		b.bad = append(b.bad, "more")
	}
	if !c.done() || locked != want.locked {
		b.bad = append(b.bad, "reconnect lock coverage or trailing bytes")
	}
	return b.bad
}

// filelocks binds a Client::_encode_filelocks blob (a fcntl lock count and
// raw ceph_filelock records, then the same for flock locks) to the receipt
// the native decoder produced in the Server::recover_filelocks layout. An
// empty blob carries no locks.
func (b *nativeReceiptBinder) filelocks(m map[string]any, blob []byte) {
	lc := &sessionCursor{b: blob, ok: true}
	for _, key := range []string{"fcntl_locks", "flock_locks"} {
		locks, _ := m[key].([]any)
		if locks == nil {
			b.bad = append(b.bad, key)
			continue
		}
		if len(blob) == 0 {
			if len(locks) != 0 {
				b.bad = append(b.bad, key)
			}
			continue
		}
		if n := lc.u32(); uint64(len(locks)) != uint64(n) {
			b.bad = append(b.bad, key)
			locks = nil
		}
		for _, entry := range locks {
			l, _ := entry.(map[string]any)
			if !nativeReceiptHasKeys(l, nativeFilelockKeys...) {
				b.bad = append(b.bad, key)
			}
			for _, field := range nativeFilelockKeys[:5] {
				b.number(l, field, lc.u64())
			}
			b.number(l, "type", uint64(lc.u8()))
		}
	}
	if len(blob) != 0 && !lc.done() {
		b.bad = append(b.bad, "filelock trailing bytes")
	}
}
