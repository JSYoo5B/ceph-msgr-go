package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
)

// The request/reply family is closed the same way: its type-24/25/26 header
// rules can never be selected by an OSD, map or session specification.
func closedMDSRequestEncoderSpec(spec nativeEncoderSpec) bool {
	return spec.command == mdsRequestEncoderSpec.command && spec.wrapper == mdsRequestEncoderSpec.wrapper &&
		spec.driver == mdsRequestEncoderSpec.driver && slices.Equal(spec.names, mdsRequestEncoderSpec.names) &&
		spec.mdsRequests && !spec.mdsMaps && !spec.mdsSessions && !spec.mdsCaps && !spec.mdsReconnect && spec.requests == 0 && spec.events == 0 && !spec.controls &&
		spec.requestVersion == 0 && !spec.mixedV8 && spec.replyVersion == 0
}

// nativeRequestVector is the closed expectation for one MDS request-family
// vector (messages/MClientRequest.h, MClientRequestForward.h, MClientReply.h
// at the pinned commit). MClientRequest declares header 6/1; the forward and
// reply keep the Message default version 1 whose compat Message::encode
// normalizes to 1. Every vector carries the explicit transaction ID 41.
type nativeRequestVector struct {
	typ, version uint16
	receipt      string
	op           uint32
	// Request expectations.
	flags, retry, forward uint32
	releases, gids        int
	// Reply expectations; entries and realms are counts, -1 for no blob.
	result                        int64
	safe, dentry, target          bool
	dentryName                    string
	readdirEntries, realms, trace int
}

const (
	nativeRequestTID       = 41
	nativeRequestHeadBytes = 118 // sizeof(struct ceph_mds_request_head), head v3
	nativeRequestArgsBytes = 56  // sizeof(union ceph_mds_request_args)
)

// nativeRequestReceiptKeys is the closed receipt schema per message type; a
// JSON null still has to be present so deleting it cannot go unnoticed.
var nativeRequestReceiptKeys = map[uint16][]string{
	24: {"message_type", "version", "tid", "source_head_version", "source_mds_features_hex", "head_version", "oldest_client_tid", "mdsmap_epoch", "flags",
		"num_retry", "num_fwd", "num_releases", "op", "caller_uid", "caller_gid", "ino", "args_hex", "ext_num_retry", "ext_num_fwd", "struct_len",
		"owner_uid", "owner_gid", "path_ino", "path", "path2_ino", "path2", "releases", "stamp", "gid_list", "alternate_name_hex", "fscrypt_auth_hex", "fscrypt_file_hex"},
	25: {"message_type", "version", "tid", "dest_mds", "num_fwd", "must_resend"},
	26: {"message_type", "version", "tid", "op", "result", "mdsmap_epoch", "safe", "is_dentry", "is_target", "trace_bytes", "extra_bytes", "snap_bytes", "trace", "readdir", "snap_realms"},
}

var nativeRequestVectors = map[string]nativeRequestVector{
	"mds-request-native-request-lookup":        {typ: 24, version: 6, receipt: "decoded_request", op: 0x100, flags: 2, gids: 2},
	"mds-request-native-request-mkdir-release": {typ: 24, version: 6, receipt: "decoded_request", op: 0x1220, flags: 2, retry: 0x101, forward: 2, releases: 1, gids: 1},
	"mds-request-native-request-rename":        {typ: 24, version: 6, receipt: "decoded_request", op: 0x1204, flags: 3, retry: 1},
	"mds-request-native-forward":               {typ: 25, version: 1, receipt: "decoded_forward"},
	"mds-request-native-reply-lookup": {typ: 26, version: 1, receipt: "decoded_reply", op: 0x100, safe: true, dentry: true, target: true,
		dentryName: "file", readdirEntries: -1, realms: 2},
	"mds-request-native-reply-readdir": {typ: 26, version: 1, receipt: "decoded_reply", op: 0x305, safe: true, target: true,
		readdirEntries: 2, realms: 1},
	"mds-request-native-reply-unsafe-error": {typ: 26, version: 1, receipt: "decoded_reply", op: 0x1203, result: -2,
		readdirEntries: -1, trace: -1},
}

func validateNativeMDSRequestFixture(name string, fixture map[string]any, segments map[string][]byte) error {
	want, ok := nativeRequestVectors[name]
	if !ok {
		return fmt.Errorf("native proof has unknown MDS request vector %s", name)
	}
	h, front, data := segments["header"], segments["front"], segments["data"]
	if len(h) != 41 || len(data) != 0 || binary.LittleEndian.Uint64(h[:8]) != 1 ||
		binary.LittleEndian.Uint64(h[8:16]) != nativeRequestTID || binary.LittleEndian.Uint16(h[16:18]) != want.typ ||
		binary.LittleEndian.Uint16(h[18:20]) != 127 || binary.LittleEndian.Uint16(h[20:22]) != want.version ||
		binary.LittleEndian.Uint32(h[22:26]) != 0 || binary.LittleEndian.Uint16(h[26:28]) != 0 ||
		binary.LittleEndian.Uint64(h[28:36]) != 3 || h[36] != 1 ||
		binary.LittleEndian.Uint16(h[37:39]) != 1 || binary.LittleEndian.Uint16(h[39:41]) != 0 {
		return fmt.Errorf("native proof has unsupported %s request-family header", name)
	}
	metadata, ok := fixture["native_header"].(map[string]any)
	if !ok {
		return fmt.Errorf("native proof lacks %s header metadata", name)
	}
	for field, n := range map[string]uint64{"seq": 1, "tid": nativeRequestTID, "type": uint64(want.typ), "priority": 127, "version": uint64(want.version), "data_off": 0, "ack_seq": 3, "flags": 1, "compat": 1, "reserved": 0, "front_len": uint64(len(front)), "middle_len": 0, "data_len": 0} {
		if !nativeProofNumber(metadata, field, n) {
			return fmt.Errorf("native proof has inconsistent %s header %s", name, field)
		}
	}
	for _, segment := range []string{"front", "data", "header"} {
		if fixture[segment+"_sha256"] != fmt.Sprintf("%x", sha256.Sum256(segments[segment])) {
			return fmt.Errorf("native proof has inconsistent %s %s digest", name, segment)
		}
	}
	for _, key := range []string{"decoded_request", "decoded_forward", "decoded_reply", "decoded_session", "decoded_map"} {
		if _, present := fixture[key]; present != (key == want.receipt) {
			return fmt.Errorf("native proof has a foreign %s decoder receipt class", name)
		}
	}
	receipt, ok := fixture[want.receipt].(map[string]any)
	if !ok || !nativeReceiptHasKeys(receipt, nativeRequestReceiptKeys[want.typ]...) {
		return fmt.Errorf("native proof lacks %s complete independent decoder receipt", name)
	}
	for field, n := range map[string]uint64{"message_type": uint64(want.typ), "version": uint64(want.version), "tid": nativeRequestTID} {
		if !nativeProofNumber(receipt, field, n) {
			return fmt.Errorf("native proof has inconsistent %s decoded %s", name, field)
		}
	}
	c := &sessionCursor{b: front, ok: true}
	var err error
	switch want.typ {
	case 24:
		err = validateNativeMDSRequestFront(want, receipt, c)
	case 25:
		dest, fwd, resend := int32(c.u32()), int32(c.u32()), c.u8()
		if !c.done() || dest != 1 || fwd != 3 || resend != 1 || !nativeProofSigned(receipt, "dest_mds", int64(dest)) ||
			!nativeProofSigned(receipt, "num_fwd", int64(fwd)) || receipt["must_resend"] != true {
			err = errors.New("forward")
		}
	case 26:
		err = validateNativeMDSReplyFront(want, receipt, c)
	}
	if err != nil {
		return fmt.Errorf("native proof has inconsistent %s decoded %w", name, err)
	}
	return nil
}

// validateNativeMDSRequestFront walks the fresh native MClientRequest front
// (head v3, filepaths, releases, stamp, gids, alternate name, fscrypt blobs)
// only to bind the independent native decoder receipt to those bytes.
func validateNativeMDSRequestFront(want nativeRequestVector, receipt map[string]any, c *sessionCursor) error {
	var bad []string
	expect := func(m map[string]any, key string, got uint64) {
		if !nativeProofNumber(m, key, got) {
			bad = append(bad, key)
		}
	}
	expectHex := func(key string, got []byte) {
		if receipt[key] != hex.EncodeToString(got) {
			bad = append(bad, key)
		}
	}
	headVersion := c.u16()
	expect(receipt, "head_version", uint64(headVersion))
	expect(receipt, "source_head_version", uint64(headVersion))
	expect(receipt, "oldest_client_tid", c.u64())
	expect(receipt, "mdsmap_epoch", uint64(c.u32()))
	flags := c.u32()
	expect(receipt, "flags", uint64(flags))
	legacyRetry, legacyForward := c.u8(), c.u8()
	expect(receipt, "num_retry", uint64(legacyRetry))
	expect(receipt, "num_fwd", uint64(legacyForward))
	releases := c.u16()
	expect(receipt, "num_releases", uint64(releases))
	op := c.u32()
	expect(receipt, "op", uint64(op))
	expect(receipt, "caller_uid", uint64(c.u32()))
	expect(receipt, "caller_gid", uint64(c.u32()))
	expect(receipt, "ino", c.u64())
	expectHex("args_hex", c.take(nativeRequestArgsBytes))
	retry, forward := c.u32(), c.u32()
	expect(receipt, "ext_num_retry", uint64(retry))
	expect(receipt, "ext_num_fwd", uint64(forward))
	structLen := c.u32()
	expect(receipt, "struct_len", uint64(structLen))
	expect(receipt, "owner_uid", uint64(c.u32()))
	expect(receipt, "owner_gid", uint64(c.u32()))
	for _, path := range []string{"path", "path2"} {
		if c.u8() != 1 {
			bad = append(bad, path+" version")
		}
		expect(receipt, path+"_ino", c.u64())
		if receipt[path] != c.str() {
			bad = append(bad, path)
		}
	}
	list, _ := receipt["releases"].([]any)
	if list == nil || len(list) != int(releases) {
		bad = append(bad, "releases")
		list = nil
	}
	for _, entry := range list {
		r, _ := entry.(map[string]any)
		expect(r, "ino", c.u64())
		expect(r, "cap_id", c.u64())
		for _, key := range []string{"caps", "wanted", "seq", "issue_seq", "mseq", "dname_seq"} {
			expect(r, key, uint64(c.u32()))
		}
		n := c.u32()
		expect(r, "dname_len", uint64(n))
		if r["dname"] != string(c.take(uint64(n))) {
			bad = append(bad, "release dname")
		}
	}
	stamp, _ := receipt["stamp"].(map[string]any)
	expect(stamp, "sec", uint64(c.u32()))
	expect(stamp, "nsec", uint64(c.u32()))
	gids, _ := receipt["gid_list"].([]any)
	n := c.u32()
	if gids == nil || uint64(n) != uint64(len(gids)) || len(gids) != want.gids {
		bad = append(bad, "gid_list")
		gids = nil
	}
	for _, gid := range gids {
		number, isNumber := gid.(json.Number)
		if !isNumber || number.String() != strconv.FormatUint(c.u64(), 10) {
			bad = append(bad, "gid")
		}
	}
	for _, key := range []string{"alternate_name_hex", "fscrypt_auth_hex", "fscrypt_file_hex"} {
		expectHex(key, c.take(uint64(c.u32())))
	}
	// Closed identity: current head v3 with its struct_len, the legacy u8
	// counts as truncated copies of the 32-bit counts, and the reviewed op.
	if !c.done() || headVersion != 3 || structLen != nativeRequestHeadBytes || op != want.op || flags != want.flags ||
		retry != want.retry || forward != want.forward || uint32(legacyRetry) != retry&0xff || uint32(legacyForward) != forward&0xff ||
		int(releases) != want.releases || receipt["source_mds_features_hex"] != "08000000ffffff0000000000" {
		bad = append(bad, "request identity or trailing bytes")
	}
	if len(bad) != 0 {
		return fmt.Errorf("request %v", bad)
	}
	return nil
}

// validateNativeMDSReplyFront binds the reply head, blob extents and the
// natively decoded trace/readdir/snap structure to the fresh native bytes.
// Inode records are bound by envelope and identity prefix; the product test
// compares every decoded inode field with the receipt.
func validateNativeMDSReplyFront(want nativeRequestVector, receipt map[string]any, c *sessionCursor) error {
	var bad []string
	expect := func(m map[string]any, key string, got uint64) {
		if !nativeProofNumber(m, key, got) {
			bad = append(bad, key)
		}
	}
	op, result, epoch := c.u32(), int32(c.u32()), c.u32()
	safe, dentry, target := c.u8(), c.u8(), c.u8()
	trace := c.take(uint64(c.u32()))
	extra := c.take(uint64(c.u32()))
	snap := c.take(uint64(c.u32()))
	expect(receipt, "op", uint64(op))
	if !nativeProofSigned(receipt, "result", int64(result)) {
		bad = append(bad, "result")
	}
	expect(receipt, "mdsmap_epoch", uint64(epoch))
	expect(receipt, "safe", uint64(safe))
	expect(receipt, "is_dentry", uint64(dentry))
	expect(receipt, "is_target", uint64(target))
	expect(receipt, "trace_bytes", uint64(len(trace)))
	expect(receipt, "extra_bytes", uint64(len(extra)))
	expect(receipt, "snap_bytes", uint64(len(snap)))
	if !c.done() || op != want.op || int64(result) != want.result || epoch != 9 || (safe == 1) != want.safe ||
		(dentry == 1) != want.dentry || (target == 1) != want.target || safe > 1 || dentry > 1 || target > 1 {
		bad = append(bad, "reply identity or trailing bytes")
	}
	inode := func(c *sessionCursor, r any) {
		m, _ := r.(map[string]any)
		body := c.envelope(8, 1)
		expect(m, "ino", body.u64())
		expect(m, "snapid", body.u64())
		expect(m, "rdev", uint64(body.u32()))
		expect(m, "version", body.u64())
		expect(m, "xattr_version", body.u64())
		if !body.ok || len(body.b) == 0 {
			bad = append(bad, "inode envelope")
		}
	}
	dirstat := func(c *sessionCursor, r any) {
		m, _ := r.(map[string]any)
		body := c.envelope(1, 1)
		expect(m, "frag", uint64(body.u32()))
		if !nativeProofSigned(m, "auth", int64(int32(body.u32()))) {
			bad = append(bad, "dirstat auth")
		}
		dist, _ := m["dist"].([]any)
		if n := body.u32(); dist == nil || uint64(n) != uint64(len(dist)) {
			bad = append(bad, "dirstat dist")
			dist = nil
		}
		for _, rank := range dist {
			number, isNumber := rank.(json.Number)
			if !isNumber || number.String() != strconv.FormatInt(int64(int32(body.u32())), 10) {
				bad = append(bad, "dirstat rank")
			}
		}
		if !body.done() {
			bad = append(bad, "dirstat envelope")
		}
	}
	lease := func(c *sessionCursor, r any) {
		m, _ := r.(map[string]any)
		body := c.envelope(2, 1)
		expect(m, "mask", uint64(body.u16()))
		expect(m, "duration_ms", uint64(body.u32()))
		expect(m, "seq", uint64(body.u32()))
		if m["alternate_name_hex"] != hex.EncodeToString(body.take(uint64(body.u32()))) || !body.done() {
			bad = append(bad, "lease")
		}
	}
	if len(trace) == 0 {
		if receipt["trace"] != nil || want.trace != -1 {
			bad = append(bad, "trace presence")
		}
	} else if t, ok := receipt["trace"].(map[string]any); !ok || want.trace == -1 || !nativeReceiptHasKeys(t, "dir_inode", "dir", "dentry_name", "lease", "target") {
		bad = append(bad, "trace presence")
	} else {
		tc := &sessionCursor{b: trace, ok: true}
		if dentry == 1 {
			inode(tc, t["dir_inode"])
			dirstat(tc, t["dir"])
			name := tc.str()
			if t["dentry_name"] != name || name != want.dentryName {
				bad = append(bad, "dentry name")
			}
			lease(tc, t["lease"])
		} else if t["dir_inode"] != nil || t["dir"] != nil || t["dentry_name"] != nil || t["lease"] != nil {
			bad = append(bad, "unexpected dentry receipt")
		}
		if target == 1 {
			inode(tc, t["target"])
		} else if t["target"] != nil {
			bad = append(bad, "unexpected target receipt")
		}
		if !tc.done() {
			bad = append(bad, "trace trailing bytes")
		}
	}
	if len(extra) == 0 {
		if receipt["readdir"] != nil || want.readdirEntries != -1 {
			bad = append(bad, "readdir presence")
		}
	} else if r, ok := receipt["readdir"].(map[string]any); !ok || op != 0x305 {
		bad = append(bad, "readdir presence")
	} else {
		ec := &sessionCursor{b: extra, ok: true}
		dirstat(ec, r["dir"])
		n := ec.u32()
		expect(r, "numdn", uint64(n))
		expect(r, "flags", uint64(ec.u16()))
		entries, _ := r["entries"].([]any)
		if entries == nil || uint64(len(entries)) != uint64(n) || len(entries) != want.readdirEntries {
			bad = append(bad, "readdir entries")
			entries = nil
		}
		for _, entry := range entries {
			m, _ := entry.(map[string]any)
			if m["name"] != ec.str() {
				bad = append(bad, "readdir name")
			}
			lease(ec, m["lease"])
			inode(ec, m["inode"])
		}
		if !ec.done() {
			bad = append(bad, "readdir trailing bytes")
		}
	}
	realms, _ := receipt["snap_realms"].([]any)
	if realms == nil {
		bad = append(bad, "snap realms")
	}
	sc := &sessionCursor{b: snap, ok: true}
	count := 0
	for len(sc.b) > 0 && sc.ok && count < len(realms) {
		m, _ := realms[count].(map[string]any)
		body := sc.envelope(1, 1)
		for _, key := range []string{"ino", "created", "parent", "parent_since", "seq"} {
			expect(m, key, body.u64())
		}
		snaps, prior := body.u32(), body.u32()
		expect(m, "num_snaps", uint64(snaps))
		expect(m, "num_prior_parent_snaps", uint64(prior))
		for _, list := range []struct {
			key string
			n   uint32
		}{{"my_snaps", snaps}, {"prior_parent_snaps", prior}} {
			ids, _ := m[list.key].([]any)
			if ids == nil || uint64(len(ids)) != uint64(list.n) {
				bad = append(bad, list.key)
				ids = nil
			}
			for _, id := range ids {
				number, isNumber := id.(json.Number)
				if !isNumber || number.String() != strconv.FormatUint(body.u64(), 10) {
					bad = append(bad, list.key)
				}
			}
		}
		modified, _ := m["last_modified"].(map[string]any)
		expect(modified, "sec", uint64(body.u32()))
		expect(modified, "nsec", uint64(body.u32()))
		expect(m, "change_attr", body.u64())
		if !body.done() {
			bad = append(bad, "snap realm envelope")
		}
		count++
	}
	wantRealms := max(want.realms, 0)
	if !sc.done() || count != len(realms) || count != wantRealms {
		bad = append(bad, "snap realm count or trailing bytes")
	}
	if len(bad) != 0 {
		return fmt.Errorf("reply %v", bad)
	}
	return nil
}
