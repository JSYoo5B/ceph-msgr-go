package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
)

// The session family is closed in the same way: its type-22 header rules can
// never be selected by an OSD or map specification.
func closedMDSSessionEncoderSpec(spec nativeEncoderSpec) bool {
	return spec.command == mdsSessionEncoderSpec.command && spec.wrapper == mdsSessionEncoderSpec.wrapper &&
		spec.driver == mdsSessionEncoderSpec.driver && slices.Equal(spec.names, mdsSessionEncoderSpec.names) &&
		spec.mdsSessions && !spec.mdsMaps && !spec.mdsRequests && !spec.mdsCaps && !spec.mdsReconnect && spec.requests == 0 && spec.events == 0 && !spec.controls &&
		spec.requestVersion == 0 && !spec.mixedV8 && spec.replyVersion == 0
}

// nativeSessionVector is the closed expectation for one MClientSession vector.
// Version 1 carries only the 28-byte ceph_mds_session_head; version 7 adds
// metadata, supported features, metric spec, flags, cap auths and the oldest
// client TID (messages/MClientSession.h at the pinned commit).
type nativeSessionVector struct {
	version                uint16
	op, maxCaps, maxLeases uint32
	flags                  uint32
	seq                    uint64
	metadata               map[string]string
	capAuths               int
	// features and metric are the native feature_bitset_t and metric_spec_t
	// encode images; empty selects the default (empty) images.
	features, metric     string
	oldestTID, sourceTID uint64
}

const (
	nativeEmptyBitset     = "00000000"
	nativeEmptyMetricSpec = "0101" + "04000000" + nativeEmptyBitset
)

// nativeSessionHeadBytes is the packed ceph_mds_session_head size.
const nativeSessionHeadBytes = 28

var nativeSessionVectors = map[string]nativeSessionVector{
	"mds-session-native-request-open": {version: 7, op: 0, metadata: map[string]string{"entity_id": "admin", "root": "/"},
		features: "08000000" + "ff01000000000000", oldestTID: ^uint64(0), sourceTID: ^uint64(0)},
	"mds-session-native-request-renewcaps": {version: 1, op: 4, seq: 5, oldestTID: ^uint64(0), sourceTID: 9},
	"mds-session-native-request-close":     {version: 1, op: 2, oldestTID: ^uint64(0), sourceTID: ^uint64(0)},
	"mds-session-native-open": {version: 7, op: 1, capAuths: 1, features: "08000000" + "ffffff0000000000",
		metric: "0101" + "0c000000" + "08000000" + "ffff000000000000", oldestTID: ^uint64(0), sourceTID: ^uint64(0)},
	"mds-session-native-reject": {version: 7, op: 11, flags: 1, metadata: map[string]string{"error_string": "blocklisted (blacklisted)"},
		oldestTID: ^uint64(0), sourceTID: ^uint64(0)},
	"mds-session-native-renewcaps":    {version: 1, op: 5, seq: 5, oldestTID: ^uint64(0), sourceTID: ^uint64(0)},
	"mds-session-native-recall-state": {version: 1, op: 7, maxCaps: 100, maxLeases: 7, oldestTID: ^uint64(0), sourceTID: ^uint64(0)},
	"mds-session-native-stale":        {version: 1, op: 6, oldestTID: ^uint64(0), sourceTID: ^uint64(0)},
}

func validateNativeMDSSessionFixture(name string, fixture map[string]any, segments map[string][]byte) error {
	want, ok := nativeSessionVectors[name]
	if !ok {
		return fmt.Errorf("native proof has unknown MDS session vector %s", name)
	}
	h, front, data := segments["header"], segments["front"], segments["data"]
	if len(h) != 41 || len(data) != 0 || binary.LittleEndian.Uint64(h[:8]) != 1 ||
		binary.LittleEndian.Uint64(h[8:16]) != 0 || binary.LittleEndian.Uint16(h[16:18]) != 22 ||
		binary.LittleEndian.Uint16(h[18:20]) != 127 || binary.LittleEndian.Uint16(h[20:22]) != want.version ||
		binary.LittleEndian.Uint32(h[22:26]) != 0 || binary.LittleEndian.Uint16(h[26:28]) != 0 ||
		binary.LittleEndian.Uint64(h[28:36]) != 3 || h[36] != 1 ||
		binary.LittleEndian.Uint16(h[37:39]) != 1 || binary.LittleEndian.Uint16(h[39:41]) != 0 {
		return fmt.Errorf("native proof has unsupported %s session header", name)
	}
	metadata, ok := fixture["native_header"].(map[string]any)
	if !ok {
		return fmt.Errorf("native proof lacks %s header metadata", name)
	}
	for field, n := range map[string]uint64{"seq": 1, "tid": 0, "type": 22, "priority": 127, "version": uint64(want.version), "data_off": 0, "ack_seq": 3, "flags": 1, "compat": 1, "reserved": 0, "front_len": uint64(len(front)), "middle_len": 0, "data_len": 0} {
		if !nativeProofNumber(metadata, field, n) {
			return fmt.Errorf("native proof has inconsistent %s header %s", name, field)
		}
	}
	for _, segment := range []string{"front", "data", "header"} {
		if fixture[segment+"_sha256"] != fmt.Sprintf("%x", sha256.Sum256(segments[segment])) {
			return fmt.Errorf("native proof has inconsistent %s %s digest", name, segment)
		}
	}
	receipt, ok := fixture["decoded_session"].(map[string]any)
	if !ok {
		return fmt.Errorf("native proof lacks %s independent session decoder receipt", name)
	}
	for field, n := range map[string]uint64{"message_type": 22, "version": uint64(want.version), "op": uint64(want.op), "seq": want.seq, "stamp_sec": 0, "stamp_nsec": 0, "max_caps": uint64(want.maxCaps), "max_leases": uint64(want.maxLeases), "flags": uint64(want.flags), "oldest_client_tid": want.oldestTID, "source_oldest_client_tid": want.sourceTID} {
		if !nativeProofNumber(receipt, field, n) {
			return fmt.Errorf("native proof has inconsistent %s decoded session %s", name, field)
		}
	}
	c := &sessionCursor{b: front, ok: true}
	if c.u32() != want.op || c.u64() != want.seq || c.u64() != 0 || c.u32() != want.maxCaps || c.u32() != want.maxLeases || !c.ok {
		return fmt.Errorf("native proof has inconsistent %s session head", name)
	}
	keys, _ := receipt["metadata"].(map[string]any)
	auths, _ := receipt["cap_auths"].([]any)
	features, _ := receipt["supported_features_hex"].(string)
	metric, _ := receipt["metric_spec_hex"].(string)
	wantFeatures, wantMetric := want.features, want.metric
	if wantFeatures == "" {
		wantFeatures = nativeEmptyBitset
	}
	if wantMetric == "" {
		wantMetric = nativeEmptyMetricSpec
	}
	if keys == nil || auths == nil || len(keys) != len(want.metadata) || len(auths) != want.capAuths || features != wantFeatures || metric != wantMetric {
		return fmt.Errorf("native proof lacks %s decoded session extension receipt", name)
	}
	for k, v := range want.metadata {
		if keys[k] != v {
			return fmt.Errorf("native proof has inconsistent %s decoded session metadata", name)
		}
	}
	if want.version == 1 {
		// Native decoding leaves every extension field at its default.
		if len(front) != nativeSessionHeadBytes {
			return fmt.Errorf("native proof has unsupported %s head-only session", name)
		}
		return nil
	}
	n := c.u32()
	if !c.ok || uint64(n) != uint64(len(want.metadata)) {
		return fmt.Errorf("native proof has inconsistent %s session metadata count", name)
	}
	seen := make(map[string]bool, n)
	for range n {
		k, v := c.str(), c.str()
		if !c.ok || seen[k] || keys[k] != v {
			return fmt.Errorf("native proof has inconsistent %s session metadata", name)
		}
		seen[k] = true
	}
	for _, image := range []string{features, metric} {
		raw, err := hex.DecodeString(image)
		if err != nil || len(raw) < 4 || !bytes.Equal(c.take(uint64(len(raw))), raw) {
			return fmt.Errorf("native proof has inconsistent %s session bitset image", name)
		}
	}
	if c.u32() != want.flags || uint64(c.u32()) != uint64(want.capAuths) || !c.ok {
		return fmt.Errorf("native proof has inconsistent %s session flags/cap auth count", name)
	}
	for _, entry := range auths {
		auth, ok := entry.(map[string]any)
		body := c.structBody()
		match := body.structBody()
		uid := int64(match.u64())
		gids, _ := auth["gids"].([]any)
		count := match.u32()
		if !ok || !match.ok || uint64(count) != uint64(len(gids)) || !nativeProofSigned(auth, "uid", uid) {
			return fmt.Errorf("native proof has inconsistent %s session cap auth match", name)
		}
		for _, gid := range gids {
			n, isNumber := gid.(json.Number)
			if !isNumber || n.String() != strconv.FormatUint(uint64(match.u32()), 10) {
				return fmt.Errorf("native proof has inconsistent %s session cap auth gids", name)
			}
		}
		path, fs, squash := match.str(), match.str(), match.u8()
		readable, writeable := body.u8(), body.u8()
		if !match.ok || len(match.b) != 0 || !body.ok || len(body.b) != 0 || squash > 1 || readable > 1 || writeable > 1 ||
			auth["path"] != path || auth["fs_name"] != fs || auth["root_squash"] != (squash == 1) ||
			auth["readable"] != (readable == 1) || auth["writeable"] != (writeable == 1) {
			return fmt.Errorf("native proof has inconsistent %s session cap auth", name)
		}
	}
	if c.u64() != want.oldestTID || !c.ok || len(c.b) != 0 {
		return fmt.Errorf("native proof has inconsistent %s session oldest client TID or trailing bytes", name)
	}
	return nil
}
