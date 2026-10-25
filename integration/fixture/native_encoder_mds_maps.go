package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
)

// Only this fixed family can enter the map-header path. In particular, callers
// cannot use a version/type parameter to relax an existing OSD proof contract.
func closedMDSMapEncoderSpec(spec nativeEncoderSpec) bool {
	return spec.command == mdsMapEncoderSpec.command && spec.wrapper == mdsMapEncoderSpec.wrapper &&
		spec.driver == mdsMapEncoderSpec.driver && slices.Equal(spec.names, mdsMapEncoderSpec.names) &&
		spec.mdsMaps && !spec.mdsSessions && !spec.mdsRequests && !spec.mdsCaps && !spec.mdsReconnect && spec.requests == 0 && spec.events == 0 && !spec.controls &&
		spec.requestVersion == 0 && !spec.mixedV8 && spec.replyVersion == 0
}

func validateNativeMDSMapFixture(name string, fixture map[string]any, segments map[string][]byte) error {
	typ, version, compat := uint64(21), uint64(2), uint64(1)
	expectedEpoch := uint32(0)
	expectedEntries := uint64(0)
	switch name {
	case "mds-map-native-fsmap-empty":
		typ, version, compat = 103, 1, 1
	case "mds-map-native-fsmap-multi":
		typ, version, compat, expectedEpoch = 103, 1, 1, 17
		expectedEntries = 2
	case "mds-map-native-mdsmap-empty":
	case "mds-map-native-mdsmap-populated":
		expectedEpoch = 23
		expectedEntries = 2
	case "mds-map-native-mdsmap-unknown":
		expectedEpoch = 24
		expectedEntries = 2
	default:
		return fmt.Errorf("native proof has unknown MDS map vector %s", name)
	}
	h, front, data := segments["header"], segments["front"], segments["data"]
	if len(h) != 41 || len(data) != 0 || binary.LittleEndian.Uint64(h[:8]) != 1 ||
		binary.LittleEndian.Uint64(h[8:16]) != 0 || uint64(binary.LittleEndian.Uint16(h[16:18])) != typ ||
		binary.LittleEndian.Uint16(h[18:20]) != 127 || uint64(binary.LittleEndian.Uint16(h[20:22])) != version ||
		binary.LittleEndian.Uint32(h[22:26]) != 0 || binary.LittleEndian.Uint16(h[26:28]) != 0 ||
		binary.LittleEndian.Uint64(h[28:36]) != 3 || h[36] != 1 ||
		uint64(binary.LittleEndian.Uint16(h[37:39])) != compat || binary.LittleEndian.Uint16(h[39:41]) != 0 {
		return fmt.Errorf("native proof has unsupported %s map header", name)
	}
	metadata, ok := fixture["native_header"].(map[string]any)
	if !ok {
		return fmt.Errorf("native proof lacks %s header metadata", name)
	}
	for field, want := range map[string]uint64{"seq": 1, "tid": 0, "type": typ, "priority": 127, "version": version, "data_off": 0, "ack_seq": 3, "flags": 1, "compat": compat, "reserved": 0, "front_len": uint64(len(front)), "middle_len": 0, "data_len": 0} {
		if !nativeProofNumber(metadata, field, want) {
			return fmt.Errorf("native proof has inconsistent %s header %s", name, field)
		}
	}
	for _, segment := range []string{"front", "data", "header"} {
		if fixture[segment+"_sha256"] != fmt.Sprintf("%x", sha256.Sum256(segments[segment])) {
			return fmt.Errorf("native proof has inconsistent %s %s digest", name, segment)
		}
	}
	decoded, ok := fixture["decoded_map"].(map[string]any)
	if !ok {
		return fmt.Errorf("native proof lacks %s independent map decoder receipt", name)
	}
	var epoch uint32
	var encoded []byte
	fsid := ""
	if typ == 103 {
		if len(front) < 22 {
			return fmt.Errorf("native proof has short %s FSMapUser front", name)
		}
		epoch = binary.LittleEndian.Uint32(front[:4])
		encoded = front[4:]
		if encoded[0] != 1 || encoded[1] != 1 || uint64(binary.LittleEndian.Uint32(encoded[2:6])) != uint64(len(encoded)-6) || uint64(binary.LittleEndian.Uint32(encoded[14:18])) != expectedEntries {
			return fmt.Errorf("native proof has unsupported %s FSMapUser structure", name)
		}
	} else {
		if len(front) < 38 {
			return fmt.Errorf("native proof has short %s MDSMap front", name)
		}
		fsid = hex.EncodeToString(front[:16])
		if fsid != "00112233445566778899aabbccddeeff" {
			return fmt.Errorf("native proof has foreign %s fixture FSID", name)
		}
		epoch = binary.LittleEndian.Uint32(front[16:20])
		n := uint64(binary.LittleEndian.Uint32(front[20:24]))
		if n > uint64(len(front)-28) || n != uint64(len(front)-28) || binary.LittleEndian.Uint32(front[24+int(n):]) != 0 {
			return fmt.Errorf("native proof has invalid %s MDSMap blob/name extent", name)
		}
		encoded = front[24 : 24+int(n)]
		if len(encoded) < 10 || encoded[0] != 5 || encoded[1] != 4 || uint64(binary.LittleEndian.Uint32(encoded[2:6])) != uint64(len(encoded)-6) {
			return fmt.Errorf("native proof has unsupported %s current MDSMap structure", name)
		}
	}
	if epoch != expectedEpoch || binary.LittleEndian.Uint32(encoded[6:10]) != epoch ||
		!nativeProofNumber(decoded, "message_type", typ) || !nativeProofNumber(decoded, "epoch", uint64(epoch)) || !nativeProofNumber(decoded, "entry_count", expectedEntries) ||
		decoded["fsid_hex"] != fsid || decoded["encoded_map_sha256"] != fmt.Sprintf("%x", sha256.Sum256(encoded)) {
		return fmt.Errorf("native proof has inconsistent %s map identity/decoder receipt", name)
	}
	return nil
}
