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
	"strconv"
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
	requests int
	events   int  // trailing native MWatchNotify vectors use header version 3
	controls bool // all vectors are MOSDBackoff controls, version 1 and TID zero
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
	files = append(files, backoffEncoderSpec.wrapper)
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
		if err := validateNativeEncoderHeaderFor(name, fixture, segments, i == 0 || i < spec.requests, spec.events > 0 && i >= len(spec.names)-spec.events, spec.controls); err != nil {
			return nil, err
		}
		if spec.controls {
			if err := validateNativeBackoffDecoder(name, fixture, segments["front"]); err != nil {
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
	if request {
		typ, compat = 42, 3
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
