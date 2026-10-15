package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	containerexec "github.com/testcontainers/testcontainers-go/exec"
)

const nativeEncoderCommit = "7f793731f1b39eb4f465e960113d2363c311b964"

type nativeEncoderSpec struct {
	command, wrapper, driver string
	names                    []string
	// requests is the number of leading MOSDOp vectors in names; zero
	// keeps the original single request followed by replies.
	requests       int
	events         int    // trailing native MWatchNotify vectors use header version 3
	controls       bool   // all vectors are MOSDBackoff controls, version 1 and TID zero
	requestVersion uint16 // zero retains v6; explicit v8 suites declare their request class
	mixedV8        bool   // explicitly permits leading v8 requests followed by native v6 replies
	replyVersion   uint16 // explicit all-reply v8 suite; zero preserves the first-request default
	mdsMaps        bool   // closed five-vector MFSMapUser/MMDSMap suite, never an OSD header override
	mdsSessions    bool   // closed eight-vector MClientSession suite, never an OSD or map override
}

var mutationEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-mutations", wrapper: "osd_mutation_native_encoder.cc", driver: "osd_mutation_native_encoder.py",
	names: []string{"mutation-native-request-v6-all", "mutation-native-reply-v6-all-success", "mutation-native-reply-v6-exclusive-eexist", "mutation-native-reply-v6-cmpext-mismatch", "mutation-native-reply-v6-truncate"},
}

var xattrEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-xattrs", wrapper: "osd_xattr_native_encoder.cc", driver: "osd_xattr_native_encoder.py",
	names: []string{"xattr-native-request-v6-all", "xattr-native-reply-v6-read-mixed", "xattr-native-reply-v6-cmp-false", "xattr-native-reply-v6-set-refused", "xattr-native-reply-v6-rm-refused"},
}

var omapEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-omap", wrapper: "osd_omap_native_encoder.cc", driver: "osd_omap_native_encoder.py",
	names: []string{"omap-native-request-v6-read-all", "omap-native-reply-v6-read-mixed", "omap-native-reply-v6-keys-final", "omap-native-reply-v6-vals-empty", "omap-native-reply-v6-bykeys-empty-value"},
}

var omapWriteEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-omap-writes", wrapper: "osd_omap_write_native_encoder.cc", driver: "osd_omap_write_native_encoder.py",
	names: []string{"omap-write-native-request-v6-all", "omap-write-native-reply-v6-all-success", "omap-write-native-reply-v6-header-empty", "omap-write-native-reply-v6-rmkeys-enoent", "omap-write-native-reply-v6-failok-clear"},
}

var omapCmpEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-omap-cmp", wrapper: "osd_omap_cmp_native_encoder.cc", driver: "osd_omap_cmp_native_encoder.py",
	names: []string{"omap-cmp-native-request-v6-all", "omap-cmp-native-reply-v6-success", "omap-cmp-native-reply-v6-canceled", "omap-cmp-native-reply-v6-failok", "omap-cmp-native-reply-v6-enoent"},
}

var omapRangeEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-omap-range", wrapper: "osd_omap_range_native_encoder.cc", driver: "osd_omap_range_native_encoder.py",
	names: []string{"omap-range-native-request-v6-all", "omap-range-native-reply-v6-success", "omap-range-native-reply-v6-enoent", "omap-range-native-reply-v6-failok", "omap-range-native-reply-v6-eopnotsupp"},
}

var callEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-call", wrapper: "osd_call_native_encoder.cc", driver: "osd_call_native_encoder.py",
	names: []string{"call-native-request-v6-all", "call-native-reply-v6-read-output", "call-native-reply-v6-positive", "call-native-reply-v6-eoverflow", "call-native-reply-v6-eopnotsupp"},
}

var allocHintEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-alloc-hint", wrapper: "osd_alloc_hint_native_encoder.cc", driver: "osd_alloc_hint_native_encoder.py",
	names: []string{"alloc-hint-native-request-v6-all", "alloc-hint-native-reply-v6-success", "alloc-hint-native-reply-v6-failok-refused", "alloc-hint-native-reply-v6-enospc", "alloc-hint-native-reply-v6-max"},
}

var snapEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-snap", wrapper: "osd_snap_native_encoder.cc", driver: "osd_snap_native_encoder.py",
	names:    []string{"snap-native-request-v6-read", "snap-native-request-v6-write", "snap-native-request-v6-rollback", "snap-native-request-v6-list", "snap-native-reply-v6-list", "snap-native-reply-v6-rollback", "snap-native-reply-v6-eoldsnapc", "snap-native-reply-v6-read"},
	requests: 4,
}

var watchersEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-watchers", wrapper: "osd_watchers_native_encoder.cc", driver: "osd_watchers_native_encoder.py",
	names: []string{"watchers-native-request-v6-list", "watchers-native-reply-v6-populated", "watchers-native-reply-v6-empty", "watchers-native-reply-v6-enoent", "watchers-native-reply-v6-eacces"},
}

var mapextEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-mapext", wrapper: "osd_mapext_native_encoder.cc", driver: "osd_mapext_native_encoder.py",
	names: []string{"mapext-native-request-v6-map", "mapext-native-reply-v6-populated", "mapext-native-reply-v6-empty", "mapext-native-reply-v6-enoent", "mapext-native-reply-v6-eacces"},
}

var checksumEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-checksum", wrapper: "osd_checksum_native_encoder.cc", driver: "osd_checksum_native_encoder.py",
	names: []string{"checksum-native-request-v6-crc32c", "checksum-native-reply-v6-success", "checksum-native-reply-v6-trimmed", "checksum-native-reply-v6-eof", "checksum-native-reply-v6-unaligned"},
}

var sparseReadEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-sparse-read", wrapper: "osd_sparse_read_native_encoder.cc", driver: "osd_sparse_read_native_encoder.py",
	names: []string{"sparse-read-native-request-v6-map", "sparse-read-native-reply-v6-populated", "sparse-read-native-reply-v6-empty", "sparse-read-native-reply-v6-enoent", "sparse-read-native-reply-v6-eacces"},
}
var pgListEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-pg-list", wrapper: "osd_pg_list_native_encoder.cc", driver: "osd_pg_list_native_encoder.py",
	names: []string{"pg-list-native-request-v6-list", "pg-list-native-reply-v6-populated", "pg-list-native-reply-v6-empty-end", "pg-list-native-reply-v6-einval", "pg-list-native-reply-v6-eacces"},
}

var watchNotifyEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-watch-notify", wrapper: "osd_watch_notify_native_encoder.cc", driver: "osd_watch_notify_native_encoder.py", requests: 6, events: 4,
	names: []string{"watch-notify-native-request-v6-register", "watch-notify-native-request-v6-unwatch", "watch-notify-native-request-v6-ping", "watch-notify-native-request-v6-reconnect", "watch-notify-native-request-v6-notify", "watch-notify-native-request-v6-ack", "watch-notify-native-reply-v6-register", "watch-notify-native-reply-v6-unwatch", "watch-notify-native-reply-v6-ping", "watch-notify-native-reply-v6-reconnect", "watch-notify-native-reply-v6-notify", "watch-notify-native-reply-v6-ack", "watch-notify-native-event-v3-notify", "watch-notify-native-event-v3-complete", "watch-notify-native-event-v3-timeout", "watch-notify-native-event-v3-disconnect"},
}

var backoffEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-backoff", wrapper: "osd_backoff_native_encoder.cc", driver: "osd_backoff_native_encoder.py", controls: true,
	names: []string{"backoff-native-event-v1-block-single", "backoff-native-event-v1-block-range", "backoff-native-event-v1-ack-range", "backoff-native-event-v1-unblock-range", "backoff-native-event-v1-block-sharded"},
}

var requestV8EncoderSpec = nativeEncoderSpec{
	command: "encode-osd-request-v8", wrapper: "osd_request_v8_native_encoder.cc", driver: "osd_request_v8_native_encoder.py", requests: 5, requestVersion: 8,
	names: []string{"request-v8-native-read", "request-v8-native-mutation", "request-v8-native-snapshot", "request-v8-native-checksum", "request-v8-native-shard"},
}

var pgListV8EncoderSpec = nativeEncoderSpec{
	command: "encode-osd-pg-list-v8", wrapper: "osd_pg_list_v8_native_encoder.cc", driver: "osd_pg_list_v8_native_encoder.py", requests: 1, requestVersion: 8, mixedV8: true,
	names: []string{"pg-list-v8-native-request-v8-list", "pg-list-v8-native-reply-v6-populated", "pg-list-v8-native-reply-v6-empty-end", "pg-list-v8-native-reply-v6-einval", "pg-list-v8-native-reply-v6-eacces"},
}

var replyV8EncoderSpec = nativeEncoderSpec{
	command: "encode-osd-reply-v8", wrapper: "osd_reply_v8_native_encoder.cc", driver: "osd_reply_v8_native_encoder.py", replyVersion: 8,
	names: []string{"reply-v8-native-success", "reply-v8-native-enoent", "reply-v8-native-unknown-error", "reply-v8-native-mutation-ack", "reply-v8-native-redirect"},
}

var mdsMapEncoderSpec = nativeEncoderSpec{
	command: "encode-mds-maps", wrapper: "mds_map_native_encoder.cc", driver: "mds_map_native_encoder.py", mdsMaps: true,
	names: []string{"mds-map-native-fsmap-empty", "mds-map-native-fsmap-multi", "mds-map-native-mdsmap-empty", "mds-map-native-mdsmap-populated", "mds-map-native-mdsmap-unknown"},
}

var mdsSessionEncoderSpec = nativeEncoderSpec{
	command: "encode-mds-sessions", wrapper: "mds_session_native_encoder.cc", driver: "mds_session_native_encoder.py", mdsSessions: true,
	names: []string{"mds-session-native-request-open", "mds-session-native-request-renewcaps", "mds-session-native-request-close", "mds-session-native-open", "mds-session-native-reject", "mds-session-native-renewcaps", "mds-session-native-recall-state", "mds-session-native-stale"},
}

// The offline encoder is a development oracle. It starts no Ceph daemon and
// installs native build packages only in its owned disposable container.
func nativeEncoderCommand(ctx context.Context, args []string) (result error) {
	return runNativeEncoder(ctx, args, mutationEncoderSpec)
}

func runNativeEncoder(ctx context.Context, args []string, spec nativeEncoderSpec) (result error) {
	flags := flag.NewFlagSet(spec.command, flag.ContinueOnError)
	project := flags.String("project", "../..", "product checkout")
	cache := flags.String("cache", "", "optional persistent native build cache")
	output := flags.String("output", "", "required native proof JSON output")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *output == "" {
		return fmt.Errorf("%s requires -output PATH and no positional arguments", spec.command)
	}
	root, err := filepath.Abs(*project)
	if err != nil {
		return err
	}
	wrapper := filepath.Join(root, "tools", spec.wrapper)
	input, err := os.ReadFile(wrapper)
	if err != nil {
		return err
	}
	wrapperHash := fmt.Sprintf("%x", sha256.Sum256(input))
	if *cache == "" {
		*cache, err = os.MkdirTemp("", "ceph-native-encoder-")
		if err != nil {
			return err
		}
		defer func() {
			var cleanup *cleanupFailure
			if !errors.As(result, &cleanup) {
				_ = os.RemoveAll(*cache)
			}
		}()
	} else if err := os.MkdirAll(*cache, 0700); err != nil {
		return err
	}
	*cache, err = filepath.Abs(*cache)
	if err != nil {
		return err
	}
	log, err := os.OpenFile(*output+".log", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer log.Close()
	fmt.Println("Native encoder cache:", *cache)
	build, stop := context.WithTimeout(ctx, 28*time.Minute)
	defer stop()
	target, err := testcontainers.GenericContainer(build, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: profileDefaultImage, Entrypoint: []string{"/usr/bin/sleep"}, Cmd: []string{"infinity"},
			Labels: map[string]string{"ceph-msgr-go.native-encoder": "true"},
			Mounts: testcontainers.ContainerMounts{testcontainers.BindMount(*cache, "/native-cache")},
		}, Started: true,
	})
	terminated := false
	terminate := func() error {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if target != nil && !terminated {
			if err := target.Terminate(cleanup); err != nil {
				return &cleanupFailure{cause: err}
			}
			terminated = true
		}
		return nil
	}
	defer func() { result = errors.Join(result, terminate()) }()
	if err != nil {
		return err
	}
	// A shared SDK cache can retain every oracle target. Supply all original
	// wrapper sources during configuration, then build only the selected target.
	files := []string{spec.driver, mutationEncoderSpec.wrapper, xattrEncoderSpec.wrapper, omapEncoderSpec.wrapper, omapWriteEncoderSpec.wrapper, omapCmpEncoderSpec.wrapper, omapRangeEncoderSpec.wrapper, callEncoderSpec.wrapper, allocHintEncoderSpec.wrapper, snapEncoderSpec.wrapper, watchersEncoderSpec.wrapper, mapextEncoderSpec.wrapper, checksumEncoderSpec.wrapper, sparseReadEncoderSpec.wrapper, pgListEncoderSpec.wrapper, watchNotifyEncoderSpec.wrapper, allocHintEncoderSpec.driver}
	files = append(files, backoffEncoderSpec.wrapper, requestV8EncoderSpec.wrapper)
	files = append(files, pgListV8EncoderSpec.wrapper)
	files = append(files, replyV8EncoderSpec.wrapper, mdsMapEncoderSpec.wrapper, mdsSessionEncoderSpec.wrapper)
	if spec.command == sparseReadEncoderSpec.command {
		files = append(files, "build_sparse_read_oracle.py", "osd_sparse_read_oracle.cc")
	}
	for _, name := range files {
		if err := target.CopyFileToContainer(build, filepath.Join(root, "tools", name), "/tmp/"+name, 0600); err != nil {
			return err
		}
	}
	code, stream, err := target.Exec(build, []string{"python3", "-u", "/tmp/" + spec.driver, "--build-native", "--work", "/native-cache/work", "--wrapper", "/tmp/" + spec.wrapper}, containerexec.Multiplexed())
	if stream != nil {
		_, copyErr := io.Copy(log, io.LimitReader(stream, 2<<20))
		err = errors.Join(err, copyErr)
	}
	if err != nil || code != 0 {
		return fmt.Errorf("native encoder exit=%d, build log %s: %w", code, *output+".log", errors.Join(err, errors.New("native encoding failed")))
	}
	if err := log.Close(); err != nil {
		return err
	}
	proof, err := readNativeEncoderProofFor(*output+".log", wrapperHash, spec)
	if err != nil {
		return err
	}
	if spec.mdsMaps || spec.mdsSessions {
		driver, err := os.ReadFile(filepath.Join(root, "tools", spec.driver))
		if err != nil {
			return err
		}
		if proof["build_driver_sha256"] != fmt.Sprintf("%x", sha256.Sum256(driver)) {
			return errors.New("native MDS proof differs from its compiled driver")
		}
	}
	proof["builder_container_id"] = target.GetContainerID()
	if err := terminate(); err != nil {
		return err
	}
	proof["builder_termination_completed"] = true
	data, err := json.MarshalIndent(proof, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*output, append(data, '\n'), 0600); err != nil {
		return err
	}
	fmt.Println("Native front/data/header proof:", *output)
	return nil
}

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
	if spec.mdsMaps || spec.mdsSessions {
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
		segments := make(map[string][]byte, 3)
		for _, segment := range []string{"header", "front", "data"} {
			h, ok := fixture[segment+"_hex"].(string)
			data, err := hex.DecodeString(h)
			number, numeric := fixture[segment+"_bytes"].(json.Number)
			count, countErr := number.Int64()
			if !ok || err != nil || !numeric || countErr != nil || count != int64(len(data)) || segment == "front" && len(data) == 0 || segment == "header" && len(data) != 41 {
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

// Only this fixed family can enter the map-header path. In particular, callers
// cannot use a version/type parameter to relax an existing OSD proof contract.
func closedMDSMapEncoderSpec(spec nativeEncoderSpec) bool {
	return spec.command == mdsMapEncoderSpec.command && spec.wrapper == mdsMapEncoderSpec.wrapper &&
		spec.driver == mdsMapEncoderSpec.driver && slices.Equal(spec.names, mdsMapEncoderSpec.names) &&
		spec.mdsMaps && !spec.mdsSessions && spec.requests == 0 && spec.events == 0 && !spec.controls &&
		spec.requestVersion == 0 && !spec.mixedV8 && spec.replyVersion == 0
}

// The session family is closed in the same way: its type-22 header rules can
// never be selected by an OSD or map specification.
func closedMDSSessionEncoderSpec(spec nativeEncoderSpec) bool {
	return spec.command == mdsSessionEncoderSpec.command && spec.wrapper == mdsSessionEncoderSpec.wrapper &&
		spec.driver == mdsSessionEncoderSpec.driver && slices.Equal(spec.names, mdsSessionEncoderSpec.names) &&
		spec.mdsSessions && !spec.mdsMaps && spec.requests == 0 && spec.events == 0 && !spec.controls &&
		spec.requestVersion == 0 && !spec.mixedV8 && spec.replyVersion == 0
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

func nativeProofNumber(m map[string]any, field string, want uint64) bool {
	n, ok := m[field].(json.Number)
	got, err := strconv.ParseUint(n.String(), 10, 64)
	return ok && err == nil && got == want
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
