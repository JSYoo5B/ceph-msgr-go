package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
)

// The capability family is closed the same way: its type 0x310-0x314 header
// rules and middle segment can never be selected by another specification.
func closedMDSCapsEncoderSpec(spec nativeEncoderSpec) bool {
	return spec.command == mdsCapsEncoderSpec.command && spec.wrapper == mdsCapsEncoderSpec.wrapper &&
		spec.driver == mdsCapsEncoderSpec.driver && slices.Equal(spec.names, mdsCapsEncoderSpec.names) &&
		spec.mdsCaps && !spec.mdsMaps && !spec.mdsSessions && !spec.mdsRequests && !spec.mdsReconnect && spec.requests == 0 && spec.events == 0 && !spec.controls &&
		spec.requestVersion == 0 && !spec.mixedV8 && spec.replyVersion == 0
}

// nativeCapsVector is the closed expectation for one capability-family vector
// (messages/MClientCaps.h, MClientCapRelease.h, MClientLease.h, MClientSnap.h
// and MClientQuota.h at the pinned commit). MClientCaps declares 12/1 and is
// kept at 12 only with FLOCK and EXPORT_PEER; MClientCapRelease declares 2/1;
// lease, snap and quota keep the Message default version 1 whose compat
// Message::encode normalizes to 1. Only the client flush and its FLUSH_ACK
// carry a transaction ID (the client flush TID 77).
type nativeCapsVector struct {
	typ, version uint16
	tid          uint64
	receipt      string
	op           uint32 // caps op, lease action or snap op
	middle       bool   // a nonempty xattr middle segment
	realms       int    // snap realms in the caps snap blob or snap trace
	items        int    // cap release items or snap split inodes
	splitRealms  int
}

const (
	nativeCapsFlushTID   = 77
	nativeCapsHeadBytes  = 92 // sizeof(struct ceph_mds_caps_head)
	nativeCapsBodyBytes  = 84 // sizeof(struct ceph_mds_caps_non_export_body)
	nativeCapsPeerBytes  = 21 // sizeof(struct ceph_mds_cap_peer)
	nativeCapsTypeCaps   = 0x310
	nativeCapsTypeLease  = 0x311
	nativeCapsTypeSnap   = 0x312
	nativeCapsTypeCapRel = 0x313
	nativeCapsTypeQuota  = 0x314
)

var nativeCapsVectors = map[string]nativeCapsVector{
	"mds-caps-native-caps-grant":     {typ: nativeCapsTypeCaps, version: 12, receipt: "decoded_caps", op: 0, middle: true, realms: 1},
	"mds-caps-native-caps-export":    {typ: nativeCapsTypeCaps, version: 12, receipt: "decoded_caps", op: 3},
	"mds-caps-native-caps-import":    {typ: nativeCapsTypeCaps, version: 12, receipt: "decoded_caps", op: 4, realms: 2},
	"mds-caps-native-caps-flush-ack": {typ: nativeCapsTypeCaps, version: 12, tid: nativeCapsFlushTID, receipt: "decoded_caps", op: 8},
	"mds-caps-native-caps-update":    {typ: nativeCapsTypeCaps, version: 12, tid: nativeCapsFlushTID, receipt: "decoded_caps", op: 5, middle: true},
	"mds-caps-native-cap-release":    {typ: nativeCapsTypeCapRel, version: 2, receipt: "decoded_cap_release", items: 2},
	"mds-caps-native-lease-revoke":   {typ: nativeCapsTypeLease, version: 1, receipt: "decoded_lease", op: 1},
	"mds-caps-native-lease-release":  {typ: nativeCapsTypeLease, version: 1, receipt: "decoded_lease", op: 2},
	"mds-caps-native-snap-split":     {typ: nativeCapsTypeSnap, version: 1, receipt: "decoded_snap", op: 3, realms: 2, items: 2, splitRealms: 1},
	"mds-caps-native-quota":          {typ: nativeCapsTypeQuota, version: 1, receipt: "decoded_quota"},
}

// nativeCapsReceiptKeys is the closed receipt schema per message type.
var nativeCapsReceiptKeys = map[uint16][]string{
	nativeCapsTypeCaps: {"message_type", "version", "tid", "op", "ino", "realm", "cap_id", "seq", "issue_seq", "caps", "wanted", "dirty", "migrate_seq",
		"snap_follows", "snap_trace_len", "uid", "gid", "mode", "nlink", "xattr_len", "xattr_version", "size", "max_size", "truncate_size", "truncate_seq",
		"mtime", "atime", "ctime", "layout", "time_warp_seq", "peer", "snapbl_hex", "snap_realms", "xattrbl_hex", "xattrs", "flockbl_hex",
		"inline_version", "inline_data_hex", "osd_epoch_barrier", "oldest_flush_tid", "caller_uid", "caller_gid", "btime", "change_attr", "flags",
		"nfiles", "nsubdirs", "fscrypt_auth_hex", "fscrypt_file_hex"},
	nativeCapsTypeCapRel: {"message_type", "version", "tid", "num", "caps", "osd_epoch_barrier"},
	nativeCapsTypeLease:  {"message_type", "version", "tid", "action", "mask", "ino", "first", "last", "seq", "duration_ms", "dname"},
	nativeCapsTypeSnap: {"message_type", "version", "tid", "op", "split", "num_split_inos", "num_split_realms", "trace_len", "split_inos", "split_realms",
		"trace_hex", "snap_realms"},
	nativeCapsTypeQuota: {"message_type", "version", "tid", "ino", "rctime", "rbytes", "rfiles", "rsubdirs", "max_bytes", "max_files"},
}

func validateNativeMDSCapsFixture(name string, fixture map[string]any, segments map[string][]byte) error {
	want, ok := nativeCapsVectors[name]
	if !ok {
		return fmt.Errorf("native proof has unknown MDS capability vector %s", name)
	}
	h, front, middle, data := segments["header"], segments["front"], segments["middle"], segments["data"]
	if len(h) != 41 || len(data) != 0 || binary.LittleEndian.Uint64(h[:8]) != 1 ||
		binary.LittleEndian.Uint64(h[8:16]) != want.tid || binary.LittleEndian.Uint16(h[16:18]) != want.typ ||
		binary.LittleEndian.Uint16(h[18:20]) != 127 || binary.LittleEndian.Uint16(h[20:22]) != want.version ||
		binary.LittleEndian.Uint32(h[22:26]) != 0 || binary.LittleEndian.Uint16(h[26:28]) != 0 ||
		binary.LittleEndian.Uint64(h[28:36]) != 3 || h[36] != 1 ||
		binary.LittleEndian.Uint16(h[37:39]) != 1 || binary.LittleEndian.Uint16(h[39:41]) != 0 {
		return fmt.Errorf("native proof has unsupported %s capability-family header", name)
	}
	// Only MClientCaps has a middle (xattr) segment, and only these vectors
	// carry xattrs in it.
	if (len(middle) != 0) != want.middle {
		return fmt.Errorf("native proof has unexpected %s middle segment", name)
	}
	metadata, ok := fixture["native_header"].(map[string]any)
	if !ok {
		return fmt.Errorf("native proof lacks %s header metadata", name)
	}
	for field, n := range map[string]uint64{"seq": 1, "tid": want.tid, "type": uint64(want.typ), "priority": 127, "version": uint64(want.version), "data_off": 0, "ack_seq": 3, "flags": 1, "compat": 1, "reserved": 0, "front_len": uint64(len(front)), "middle_len": uint64(len(middle)), "data_len": 0} {
		if !nativeProofNumber(metadata, field, n) {
			return fmt.Errorf("native proof has inconsistent %s header %s", name, field)
		}
	}
	if len(metadata) != 13 {
		return fmt.Errorf("native proof has unreviewed %s header metadata", name)
	}
	for _, segment := range []string{"front", "middle", "data", "header"} {
		if fixture[segment+"_sha256"] != fmt.Sprintf("%x", sha256.Sum256(segments[segment])) {
			return fmt.Errorf("native proof has inconsistent %s %s digest", name, segment)
		}
	}
	for _, key := range nativeMDSReceiptClasses {
		if _, present := fixture[key]; present != (key == want.receipt) {
			return fmt.Errorf("native proof has a foreign %s decoder receipt class", name)
		}
	}
	receipt, ok := fixture[want.receipt].(map[string]any)
	if !ok || !nativeReceiptHasKeys(receipt, nativeCapsReceiptKeys[want.typ]...) {
		return fmt.Errorf("native proof lacks %s complete independent decoder receipt", name)
	}
	for field, n := range map[string]uint64{"message_type": uint64(want.typ), "version": uint64(want.version), "tid": want.tid} {
		if !nativeProofNumber(receipt, field, n) {
			return fmt.Errorf("native proof has inconsistent %s decoded %s", name, field)
		}
	}
	var bad []string
	c := &sessionCursor{b: front, ok: true}
	switch want.typ {
	case nativeCapsTypeCaps:
		bad = validateNativeMDSCapsFront(want, receipt, c, middle)
	case nativeCapsTypeCapRel:
		bad = validateNativeMDSCapReleaseFront(want, receipt, c)
	case nativeCapsTypeLease:
		bad = validateNativeMDSLeaseFront(want, receipt, c)
	case nativeCapsTypeSnap:
		bad = validateNativeMDSSnapFront(want, receipt, c)
	case nativeCapsTypeQuota:
		bad = validateNativeMDSQuotaFront(receipt, c)
	}
	if len(bad) != 0 {
		return fmt.Errorf("native proof has inconsistent %s decoded %v", name, bad)
	}
	return nil
}

// realms binds a SnapRealmInfoNew trace to the natively decoded realms.
func (b *nativeReceiptBinder) realms(blob []byte, receipt any, want int) {
	realms, ok := receipt.([]any)
	if !ok {
		b.bad = append(b.bad, "snap realms")
		return
	}
	sc := &sessionCursor{b: blob, ok: true}
	count := 0
	for len(sc.b) > 0 && sc.ok && count < len(realms) {
		m, _ := realms[count].(map[string]any)
		body := sc.envelope(1, 1)
		for _, key := range []string{"ino", "created", "parent", "parent_since", "seq"} {
			b.number(m, key, body.u64())
		}
		snaps, prior := body.u32(), body.u32()
		b.number(m, "num_snaps", uint64(snaps))
		b.number(m, "num_prior_parent_snaps", uint64(prior))
		b.ids(m, "my_snaps", snaps, body)
		b.ids(m, "prior_parent_snaps", prior, body)
		b.time(m, "last_modified", body)
		b.number(m, "change_attr", body.u64())
		if !body.done() || len(m) != 11 {
			b.bad = append(b.bad, "snap realm envelope")
		}
		count++
	}
	if !sc.done() || count != len(realms) || count != want {
		b.bad = append(b.bad, "snap realm count or trailing bytes")
	}
}

// validateNativeMDSCapsFront walks the fresh native MClientCaps v12 front
// (head, export peer or non-export body, snap trace, flock blob, import peer
// and the v4..v12 extensions) and its middle xattr blob only to bind the
// independent native decoder receipt to those bytes.
func validateNativeMDSCapsFront(want nativeCapsVector, r map[string]any, c *sessionCursor, middle []byte) []string {
	b := &nativeReceiptBinder{}
	op := c.u32()
	b.number(r, "op", uint64(op))
	for _, key := range []string{"ino", "realm", "cap_id"} {
		b.number(r, key, c.u64())
	}
	for _, key := range []string{"seq", "issue_seq", "caps", "wanted", "dirty", "migrate_seq"} {
		b.number(r, key, uint64(c.u32()))
	}
	b.number(r, "snap_follows", c.u64())
	snapLen := c.u32()
	b.number(r, "snap_trace_len", uint64(snapLen))
	for _, key := range []string{"uid", "gid", "mode", "nlink"} {
		b.number(r, key, uint64(c.u32()))
	}
	xattrLen := c.u32()
	b.number(r, "xattr_len", uint64(xattrLen))
	b.number(r, "xattr_version", c.u64())
	layout, _ := r["layout"].(map[string]any)
	if layout == nil || len(layout) != 5 {
		b.bad = append(b.bad, "layout")
	}
	peer := func() {
		p, _ := r["peer"].(map[string]any)
		if p == nil || len(p) != 5 {
			b.bad = append(b.bad, "peer")
		}
		b.number(p, "cap_id", c.u64())
		b.number(p, "issue_seq", uint64(c.u32()))
		b.number(p, "mseq", uint64(c.u32()))
		b.signed(p, "mds", int64(int32(c.u32())))
		b.number(p, "flags", uint64(c.u8()))
	}
	if op == 3 {
		// EXPORT: the peer zero-padded to the non-export body size; the
		// native decoder leaves the body fields at their defaults.
		peer()
		if pad := c.take(nativeCapsBodyBytes - nativeCapsPeerBytes); pad == nil || !bytes.Equal(pad, make([]byte, len(pad))) {
			b.bad = append(b.bad, "export padding")
		}
		zero := &sessionCursor{b: make([]byte, 64), ok: true}
		for _, key := range []string{"size", "max_size", "truncate_size", "truncate_seq", "time_warp_seq"} {
			b.number(r, key, 0)
		}
		for _, key := range []string{"mtime", "atime", "ctime"} {
			b.time(r, key, zero)
		}
		for _, key := range []string{"stripe_unit", "stripe_count", "object_size"} {
			b.number(layout, key, 0)
		}
		b.signed(layout, "pool_id", -1)
	} else {
		for _, key := range []string{"size", "max_size", "truncate_size"} {
			b.number(r, key, c.u64())
		}
		b.number(r, "truncate_seq", uint64(c.u32()))
		for _, key := range []string{"mtime", "atime", "ctime"} {
			b.time(r, key, c)
		}
		su, sc, os, cas, osu, unused, pool := c.u32(), c.u32(), c.u32(), c.u32(), c.u32(), c.u32(), c.u32()
		b.number(layout, "stripe_unit", uint64(su))
		b.number(layout, "stripe_count", uint64(sc))
		b.number(layout, "object_size", uint64(os))
		// file_layout_t::from_legacy maps a zeroed legacy layout to pool -1;
		// to_legacy always writes zero cas_hash, object_stripe_unit and unused.
		wantPool := int64(int32(pool))
		if pool == 0 && su == 0 && sc == 0 && os == 0 {
			wantPool = -1
		}
		b.signed(layout, "pool_id", wantPool)
		if cas != 0 || osu != 0 || unused != 0 {
			b.bad = append(b.bad, "legacy layout words")
		}
		b.number(r, "time_warp_seq", uint64(c.u32()))
	}
	snap := c.take(uint64(snapLen))
	b.hex(r, "snapbl_hex", snap)
	b.realms(snap, r["snap_realms"], want.realms)
	if uint64(xattrLen) != uint64(len(middle)) {
		b.bad = append(b.bad, "xattr_len differs from middle")
	}
	b.hex(r, "xattrbl_hex", middle)
	xattrs, _ := r["xattrs"].([]any)
	if xattrs == nil {
		b.bad = append(b.bad, "xattrs")
	}
	if len(middle) != 0 {
		xc := &sessionCursor{b: middle, ok: true}
		if n := xc.u32(); uint64(n) != uint64(len(xattrs)) {
			b.bad = append(b.bad, "xattr count")
			xattrs = nil
		}
		for _, entry := range xattrs {
			x, _ := entry.(map[string]any)
			if len(x) != 2 || x["name"] != xc.str() || x["value_hex"] != hex.EncodeToString(xc.take(uint64(xc.u32()))) {
				b.bad = append(b.bad, "xattr")
			}
		}
		if !xc.done() {
			b.bad = append(b.bad, "xattr trailing bytes")
		}
	} else if len(xattrs) != 0 {
		b.bad = append(b.bad, "xattrs without middle")
	}
	b.hex(r, "flockbl_hex", c.take(uint64(c.u32())))
	if op == 4 {
		peer()
	} else if op != 3 && r["peer"] != nil {
		// The native decoder assigns the peer only for EXPORT and IMPORT.
		b.bad = append(b.bad, "unexpected peer")
	}
	b.number(r, "inline_version", c.u64())
	b.hex(r, "inline_data_hex", c.take(uint64(c.u32())))
	b.number(r, "osd_epoch_barrier", uint64(c.u32()))
	b.number(r, "oldest_flush_tid", c.u64())
	b.number(r, "caller_uid", uint64(c.u32()))
	b.number(r, "caller_gid", uint64(c.u32()))
	if layout["pool_ns"] != c.str() {
		b.bad = append(b.bad, "pool_ns")
	}
	b.time(r, "btime", c)
	b.number(r, "change_attr", c.u64())
	b.number(r, "flags", uint64(c.u32()))
	b.signed(r, "nfiles", int64(c.u64()))
	b.signed(r, "nsubdirs", int64(c.u64()))
	b.hex(r, "fscrypt_auth_hex", c.take(uint64(c.u32())))
	b.hex(r, "fscrypt_file_hex", c.take(uint64(c.u32())))
	if !c.done() || op != want.op {
		b.bad = append(b.bad, "caps identity or trailing bytes")
	}
	return b.bad
}

func validateNativeMDSCapReleaseFront(want nativeCapsVector, r map[string]any, c *sessionCursor) []string {
	b := &nativeReceiptBinder{}
	n := c.u32()
	b.number(r, "num", uint64(n))
	items, _ := r["caps"].([]any)
	if items == nil || uint64(len(items)) != uint64(n) || len(items) != want.items {
		b.bad = append(b.bad, "caps")
		items = nil
	}
	for _, entry := range items {
		item, _ := entry.(map[string]any)
		b.number(item, "ino", c.u64())
		b.number(item, "cap_id", c.u64())
		b.number(item, "migrate_seq", uint64(c.u32()))
		b.number(item, "issue_seq", uint64(c.u32()))
		if len(item) != 4 {
			b.bad = append(b.bad, "cap item")
		}
	}
	b.number(r, "osd_epoch_barrier", uint64(c.u32()))
	if !c.done() {
		b.bad = append(b.bad, "cap release trailing bytes")
	}
	return b.bad
}

func validateNativeMDSLeaseFront(want nativeCapsVector, r map[string]any, c *sessionCursor) []string {
	b := &nativeReceiptBinder{}
	action := c.u8()
	b.number(r, "action", uint64(action))
	b.number(r, "mask", uint64(c.u16()))
	for _, key := range []string{"ino", "first", "last"} {
		b.number(r, key, c.u64())
	}
	b.number(r, "seq", uint64(c.u32()))
	b.number(r, "duration_ms", uint64(c.u32()))
	if r["dname"] != c.str() {
		b.bad = append(b.bad, "dname")
	}
	if !c.done() || uint32(action) != want.op {
		b.bad = append(b.bad, "lease identity or trailing bytes")
	}
	return b.bad
}

func validateNativeMDSSnapFront(want nativeCapsVector, r map[string]any, c *sessionCursor) []string {
	b := &nativeReceiptBinder{}
	op := c.u32()
	b.number(r, "op", uint64(op))
	b.number(r, "split", c.u64())
	inos, realms, traceLen := c.u32(), c.u32(), c.u32()
	b.number(r, "num_split_inos", uint64(inos))
	b.number(r, "num_split_realms", uint64(realms))
	b.number(r, "trace_len", uint64(traceLen))
	b.ids(r, "split_inos", inos, c)
	b.ids(r, "split_realms", realms, c)
	trace := c.take(uint64(traceLen))
	b.hex(r, "trace_hex", trace)
	b.realms(trace, r["snap_realms"], want.realms)
	if !c.done() || op != want.op || int(inos) != want.items || int(realms) != want.splitRealms {
		b.bad = append(b.bad, "snap identity or trailing bytes")
	}
	return b.bad
}

func validateNativeMDSQuotaFront(r map[string]any, c *sessionCursor) []string {
	b := &nativeReceiptBinder{}
	b.number(r, "ino", c.u64())
	b.time(r, "rctime", c)
	for _, key := range []string{"rbytes", "rfiles", "rsubdirs"} {
		b.signed(r, key, int64(c.u64()))
	}
	quota := c.envelope(1, 1)
	b.signed(r, "max_bytes", int64(quota.u64()))
	b.signed(r, "max_files", int64(quota.u64()))
	if !quota.done() || !c.done() {
		b.bad = append(b.bad, "quota envelope or trailing bytes")
	}
	return b.bad
}
