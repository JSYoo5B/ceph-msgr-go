package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

func readNativeEncoderProof(path, wrapperHash string) (map[string]any, error) {
	return readNativeEncoderProofFor(path, wrapperHash, mutationEncoderSpec)
}

func readNativeEncoderProofFor(path, wrapperHash string, spec nativeEncoderSpec) (map[string]any, error) {
	if spec.mdsMaps && !closedMDSMapEncoderSpec(spec) {
		return nil, errors.New("unsupported native MDS map encoder specification")
	}
	if spec.mdsSessions && !closedMDSSessionEncoderSpec(spec) {
		return nil, errors.New("unsupported native MDS session encoder specification")
	}
	if spec.mdsRequests && !closedMDSRequestEncoderSpec(spec) {
		return nil, errors.New("unsupported native MDS request encoder specification")
	}
	if spec.mdsCaps && !closedMDSCapsEncoderSpec(spec) {
		return nil, errors.New("unsupported native MDS capability encoder specification")
	}
	if spec.mdsReconnect && !closedMDSReconnectEncoderSpec(spec) {
		return nil, errors.New("unsupported native MDS reconnect encoder specification")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64<<10), 2<<20)
	var proof map[string]any
	for scanner.Scan() {
		var candidate map[string]any
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.UseNumber()
		if decoder.Decode(&candidate) == nil && candidate["ceph_commit"] != nil {
			proof = candidate
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if proof["ceph_commit"] != nativeEncoderCommit || proof["image"] != profileDefaultImage || proof["native_payload_encoder_verified"] != true || proof["native_message_header_verified"] != true || proof["wrapper_sha256"] != wrapperHash {
		return nil, errors.New("native proof lacks the exact source, image, wrapper or successful payload/header encoding receipt")
	}
	if spec.command != mutationEncoderSpec.command && proof["native_payload_decoder_verified"] != true {
		return nil, errors.New("native proof lacks its independent decoder receipt")
	}
	if spec.mdsMaps || spec.mdsSessions || spec.mdsRequests || spec.mdsCaps || spec.mdsReconnect {
		if err := validateNativeMDSMapSourceProof(proof); err != nil {
			return nil, err
		}
	}
	fixtures, ok := proof["fixtures"].(map[string]any)
	if !ok || len(fixtures) != len(spec.names) {
		return nil, fmt.Errorf("native proof lacks its %d encoder vectors", len(spec.names))
	}
	for i, name := range spec.names {
		fixture, ok := fixtures[name].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("native proof lacks %s", name)
		}
		// Only the capability suite carries a (possibly empty) middle segment,
		// and only the reconnect suite an empty front (its payload is data).
		names := []string{"header", "front", "data"}
		if spec.mdsCaps {
			names = []string{"header", "front", "middle", "data"}
		}
		segments := make(map[string][]byte, len(names))
		for _, segment := range names {
			h, ok := fixture[segment+"_hex"].(string)
			data, err := hex.DecodeString(h)
			number, numeric := fixture[segment+"_bytes"].(json.Number)
			count, countErr := number.Int64()
			if !ok || err != nil || !numeric || countErr != nil || count != int64(len(data)) || segment == "front" && (len(data) == 0) != spec.mdsReconnect || segment == "header" && len(data) != 41 {
				return nil, fmt.Errorf("native proof has invalid %s %s bytes", name, segment)
			}
			segments[segment] = data
		}
		if spec.mdsMaps {
			if err := validateNativeMDSMapFixture(name, fixture, segments); err != nil {
				return nil, err
			}
			continue
		}
		if spec.mdsSessions {
			if err := validateNativeMDSSessionFixture(name, fixture, segments); err != nil {
				return nil, err
			}
			continue
		}
		if spec.mdsRequests {
			if err := validateNativeMDSRequestFixture(name, fixture, segments); err != nil {
				return nil, err
			}
			continue
		}
		if spec.mdsCaps {
			if err := validateNativeMDSCapsFixture(name, fixture, segments); err != nil {
				return nil, err
			}
			continue
		}
		if spec.mdsReconnect {
			if err := validateNativeMDSReconnectFixture(name, fixture, segments); err != nil {
				return nil, err
			}
			continue
		}
		requestVersion := spec.requestVersion
		if spec.replyVersion != 0 && (spec.replyVersion != 8 || spec.requests != 0 || spec.events != 0 || spec.controls || spec.requestVersion != 0 || spec.mixedV8) {
			return nil, errors.New("native proof has unsupported all-reply v8 configuration")
		}
		if spec.mixedV8 && (requestVersion != 8 || spec.requests < 1 || spec.requests >= len(spec.names)) {
			return nil, errors.New("native proof has unsupported mixed v8-request/v6-reply configuration")
		}
		if requestVersion != 0 && (requestVersion != 8 || !spec.mixedV8 && spec.requests != len(spec.names) || spec.events != 0 || spec.controls) {
			return nil, errors.New("native proof has unsupported request-version suite configuration")
		}
		request := spec.replyVersion == 0 && (i == 0 || i < spec.requests)
		if err := validateNativeEncoderHeaderVersions(name, fixture, segments, request, spec.events > 0 && i >= len(spec.names)-spec.events, spec.controls, requestVersion, spec.replyVersion); err != nil {
			return nil, err
		}
		if spec.controls {
			if err := validateNativeBackoffDecoder(name, fixture, segments["front"]); err != nil {
				return nil, err
			}
		}
		if requestVersion == 8 && (i == 0 || i < spec.requests) {
			if err := validateNativeRequestV8Decoder(name, fixture, segments["front"]); err != nil {
				return nil, err
			}
		}
		if spec.replyVersion == 8 {
			if err := validateNativeReplyV8Decoder(name, fixture, segments["data"]); err != nil {
				return nil, err
			}
		}
	}
	return proof, nil
}

func validateNativeMDSMapSourceProof(proof map[string]any) error {
	if proof["canonical_message_encode_verified"] != true {
		return errors.New("native MDS proof lacks fresh canonical Message::encode verification")
	}
	// This archive digest is shared with the independently retained Tentacle
	// native proofs. A correct-looking commit label alone is insufficient.
	if proof["source_archive_url"] != "https://codeload.github.com/ceph/ceph/tar.gz/"+nativeEncoderCommit ||
		proof["source_archive_sha256"] != "f2144fc4375532342657dc531e256f04aab51ad35e46beec6d6b940eeccbbc1d" {
		return errors.New("native MDS proof differs from the pinned source archive")
	}
	for _, field := range []string{"wrapper_sha256", "generated_config_sha256", "native_library_sha256", "binary_sha256", "build_driver_sha256"} {
		text, ok := proof[field].(string)
		digest, err := hex.DecodeString(text)
		if !ok || err != nil || len(digest) != sha256.Size || text != strings.ToLower(text) {
			return fmt.Errorf("native MDS proof lacks its %s provenance digest", field)
		}
	}
	if proof["launcher_sha256"] != proof["build_driver_sha256"] {
		return errors.New("native MDS launcher differs from its build driver")
	}
	loader, ok := proof["loader_audit"].(string)
	if !ok || strings.Contains(loader, "not found") || !strings.Contains(loader, "libceph-common.so.2 => /native-cache/work/build/lib/libceph-common.so.2") {
		return errors.New("native MDS proof does not load its matching pinned ceph-common")
	}
	return nil
}
