package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
)

func validateNativeEncoderHeader(name string, fixture map[string]any, segments map[string][]byte, request, event bool) error {
	return validateNativeEncoderHeaderFor(name, fixture, segments, request, event, false)
}

func validateNativeEncoderHeaderFor(name string, fixture map[string]any, segments map[string][]byte, request, event, control bool) error {
	return validateNativeEncoderHeaderClass(name, fixture, segments, request, event, control, 0)
}

func validateNativeEncoderHeaderClass(name string, fixture map[string]any, segments map[string][]byte, request, event, control bool, requestVersion uint16) error {
	return validateNativeEncoderHeaderVersions(name, fixture, segments, request, event, control, requestVersion, 0)
}

func validateNativeEncoderHeaderVersions(name string, fixture map[string]any, segments map[string][]byte, request, event, control bool, requestVersion, replyVersion uint16) error {
	metadata, ok := fixture["native_header"].(map[string]any)
	if !ok {
		return fmt.Errorf("native proof lacks %s native header metadata", name)
	}
	h := segments["header"] // Validated as exactly 41 bytes before this call.
	// Pinned include/msgr.h ceph_msg_header2 is packed little-endian. Segment
	// lengths belong to the native Message envelope, not this header2 struct.
	expected := map[string]uint64{
		"seq": binary.LittleEndian.Uint64(h[0:8]), "tid": binary.LittleEndian.Uint64(h[8:16]),
		"type": uint64(binary.LittleEndian.Uint16(h[16:18])), "priority": uint64(binary.LittleEndian.Uint16(h[18:20])),
		"version": uint64(binary.LittleEndian.Uint16(h[20:22])), "data_off": uint64(binary.LittleEndian.Uint16(h[26:28])),
		"ack_seq": binary.LittleEndian.Uint64(h[28:36]), "flags": uint64(h[36]),
		"compat": uint64(binary.LittleEndian.Uint16(h[37:39])), "reserved": uint64(binary.LittleEndian.Uint16(h[39:41])),
		"front_len": uint64(len(segments["front"])), "middle_len": 0, "data_len": uint64(len(segments["data"])),
	}
	typ, compat, version := uint64(43), uint64(2), uint64(6)
	if replyVersion != 0 {
		version = uint64(replyVersion)
		if expected["tid"] != 77 {
			return fmt.Errorf("native proof has unsupported %s explicit reply identity", name)
		}
	}
	if request {
		typ, compat = 42, 3
		if requestVersion != 0 {
			version = uint64(requestVersion)
			if expected["tid"] != 77 {
				return fmt.Errorf("native proof has unsupported %s explicit request identity", name)
			}
		}
	}
	if event {
		typ, compat, version = 44, 1, 3
	}
	if control {
		typ, compat, version = 61, 1, 1
		if expected["tid"] != 0 || expected["data_len"] != 0 {
			return fmt.Errorf("native proof has unsupported %s control identity or data", name)
		}
	}
	if expected["type"] != typ || expected["version"] != version || expected["compat"] != compat || binary.LittleEndian.Uint32(h[22:26]) != 0 || expected["reserved"] != 0 {
		return fmt.Errorf("native proof has unsupported %s header encoding", name)
	}
	for field, want := range expected {
		number, ok := metadata[field].(json.Number)
		got, err := strconv.ParseUint(number.String(), 10, 64)
		if !ok || err != nil || got != want {
			return fmt.Errorf("native proof has inconsistent %s header %s", name, field)
		}
	}
	return nil
}

func validateNativeBackoffDecoder(name string, fixture map[string]any, front []byte) error {
	// The current spg_t occupies 24 bytes, followed by epoch/op/ID. Native
	// separately decodes all ranges too; these receipts bind its observed
	// control metadata to the fresh encoded front, without using Go's codec.
	decoded, ok := fixture["decoded_operations"].([]any)
	if !ok || len(decoded) != 1 || len(front) < 37 {
		return fmt.Errorf("native proof lacks %s independent control decoder receipt", name)
	}
	op, ok := decoded[0].(map[string]any)
	if !ok || front[28] < 1 || front[28] > 3 {
		return fmt.Errorf("native proof has invalid %s control opcode", name)
	}
	for field, want := range map[string]uint64{
		"code": uint64(front[28]), "map_epoch": uint64(binary.LittleEndian.Uint32(front[24:28])), "id": binary.LittleEndian.Uint64(front[29:37]),
	} {
		number, ok := op[field].(json.Number)
		got, err := strconv.ParseUint(number.String(), 10, 64)
		if !ok || err != nil || got != want {
			return fmt.Errorf("native proof has inconsistent %s decoded control %s", name, field)
		}
	}
	if _, ok := op["single"].(bool); !ok {
		return fmt.Errorf("native proof lacks %s decoded control range classification", name)
	}
	return nil
}

func validateNativeRequestV8Decoder(name string, fixture map[string]any, front []byte) error {
	// Current MOSDOp v8 begins with spg_t (24 bytes), raw hash, epoch and
	// flags, osd_reqid_t v2 (27 bytes), a fixed 24-byte trace, then client_inc.
	// Bind the independent native decoder receipt to these source-defined
	// fields; actual PG and object hash must remain distinct.
	routing, ok := fixture["decoded_routing"].(map[string]any)
	if !ok || len(front) < 91 || front[0] != 1 || front[1] != 1 || binary.LittleEndian.Uint32(front[2:6]) != 18 || front[6] != 1 || front[36] != 2 || front[37] != 2 || binary.LittleEndian.Uint32(front[38:42]) != 21 {
		return fmt.Errorf("native proof lacks %s current v8 routing decoder receipt", name)
	}
	for field, want := range map[string]uint64{
		"pool": binary.LittleEndian.Uint64(front[7:15]), "seed": uint64(binary.LittleEndian.Uint32(front[15:19])),
		"raw_hash": uint64(binary.LittleEndian.Uint32(front[24:28])), "client_inc": uint64(binary.LittleEndian.Uint32(front[87:91])),
	} {
		number, ok := routing[field].(json.Number)
		got, err := strconv.ParseUint(number.String(), 10, 64)
		if !ok || err != nil || got != want {
			return fmt.Errorf("native proof has inconsistent %s v8 routing %s", name, field)
		}
	}
	for field, want := range map[string]int64{
		"shard": int64(int8(front[23])), "reqid_inc": int64(int32(binary.LittleEndian.Uint32(front[59:63]))),
	} {
		number, ok := routing[field].(json.Number)
		got, err := strconv.ParseInt(number.String(), 10, 64)
		if !ok || err != nil || got != want {
			return fmt.Errorf("native proof has inconsistent %s v8 routing %s", name, field)
		}
	}
	operations, ok := fixture["decoded_operations"].([]any)
	if !ok || len(operations) == 0 {
		return fmt.Errorf("native proof lacks %s independent v8 operation decoder receipt", name)
	}
	return nil
}

func validateNativeReplyV8Decoder(name string, fixture map[string]any, data []byte) error {
	operations, ok := fixture["decoded_operations"].([]any)
	if !ok || len(operations) == 0 || len(operations) > 64 {
		return fmt.Errorf("native proof lacks %s independent reply-v8 operation decoder receipt", name)
	}
	position := 0
	for _, entry := range operations {
		op, ok := entry.(map[string]any)
		if !ok {
			return fmt.Errorf("native proof has invalid %s decoded reply operation", name)
		}
		result, ok := op["result"].(json.Number)
		if _, err := strconv.ParseInt(result.String(), 10, 32); !ok || err != nil {
			return fmt.Errorf("native proof has invalid %s signed reply result", name)
		}
		length, numeric := op["payload_len"].(json.Number)
		count, err := strconv.ParseUint(length.String(), 10, 32)
		hexData, present := op["outdata_hex"].(string)
		output, hexErr := hex.DecodeString(hexData)
		if !numeric || err != nil || !present || hexErr != nil || count != uint64(len(output)) || count > uint64(len(data)-position) || !bytes.Equal(output, data[position:position+len(output)]) {
			return fmt.Errorf("native proof has inconsistent %s decoded reply output", name)
		}
		position += len(output)
	}
	if position != len(data) {
		return fmt.Errorf("native proof has unclaimed %s decoded reply output", name)
	}
	return nil
}
