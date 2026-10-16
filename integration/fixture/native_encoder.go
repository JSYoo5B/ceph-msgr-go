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
	mdsRequests    bool   // closed seven-vector MClientRequest/Forward/Reply suite, never another override
	mdsCaps        bool   // closed ten-vector MClientCaps/CapRelease/Lease/Snap/Quota suite with a middle segment
	mdsReconnect   bool   // closed three-vector MClientReconnect suite with an empty front and the payload in the data segment
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

// truncateEncoderSpec covers the extent truncate fields (truncate_size,
// truncate_seq) that CephFS data I/O sends on READ, WRITE and TRUNCATE.
var truncateEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-truncate", wrapper: "osd_truncate_native_encoder.cc", driver: "osd_truncate_native_encoder.py",
	names:    []string{"truncate-native-request-v6-read", "truncate-native-request-v6-write", "truncate-native-request-v6-trunc", "truncate-native-reply-v6-read-clipped", "truncate-native-reply-v6-write-trimmed", "truncate-native-reply-v6-trunc-noop"},
	requests: 3,
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

var mdsRequestEncoderSpec = nativeEncoderSpec{
	command: "encode-mds-requests", wrapper: "mds_request_native_encoder.cc", driver: "mds_request_native_encoder.py", mdsRequests: true,
	names: []string{"mds-request-native-request-lookup", "mds-request-native-request-mkdir-release", "mds-request-native-request-rename", "mds-request-native-forward", "mds-request-native-reply-lookup", "mds-request-native-reply-readdir", "mds-request-native-reply-unsafe-error"},
}

var mdsCapsEncoderSpec = nativeEncoderSpec{
	command: "encode-mds-caps", wrapper: "mds_caps_native_encoder.cc", driver: "mds_caps_native_encoder.py", mdsCaps: true,
	names: []string{"mds-caps-native-caps-grant", "mds-caps-native-caps-export", "mds-caps-native-caps-import", "mds-caps-native-caps-flush-ack", "mds-caps-native-caps-update", "mds-caps-native-cap-release", "mds-caps-native-lease-revoke", "mds-caps-native-lease-release", "mds-caps-native-snap-split", "mds-caps-native-quota"},
}

var mdsReconnectEncoderSpec = nativeEncoderSpec{
	command: "encode-mds-reconnect", wrapper: "mds_reconnect_native_encoder.cc", driver: "mds_reconnect_native_encoder.py", mdsReconnect: true,
	names: []string{"mds-reconnect-native-reconnect-two-caps", "mds-reconnect-native-reconnect-more", "mds-reconnect-native-reconnect-empty"},
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
	files = append(files, replyV8EncoderSpec.wrapper, mdsMapEncoderSpec.wrapper, mdsSessionEncoderSpec.wrapper, mdsRequestEncoderSpec.wrapper)
	files = append(files, mdsCapsEncoderSpec.wrapper, mdsReconnectEncoderSpec.wrapper)
	files = append(files, truncateEncoderSpec.wrapper)
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
	if spec.mdsMaps || spec.mdsSessions || spec.mdsRequests || spec.mdsCaps || spec.mdsReconnect {
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
		spec.mdsMaps && !spec.mdsSessions && !spec.mdsRequests && !spec.mdsCaps && !spec.mdsReconnect && spec.requests == 0 && spec.events == 0 && !spec.controls &&
		spec.requestVersion == 0 && !spec.mixedV8 && spec.replyVersion == 0
}

// The session family is closed in the same way: its type-22 header rules can
// never be selected by an OSD or map specification.
func closedMDSSessionEncoderSpec(spec nativeEncoderSpec) bool {
	return spec.command == mdsSessionEncoderSpec.command && spec.wrapper == mdsSessionEncoderSpec.wrapper &&
		spec.driver == mdsSessionEncoderSpec.driver && slices.Equal(spec.names, mdsSessionEncoderSpec.names) &&
		spec.mdsSessions && !spec.mdsMaps && !spec.mdsRequests && !spec.mdsCaps && !spec.mdsReconnect && spec.requests == 0 && spec.events == 0 && !spec.controls &&
		spec.requestVersion == 0 && !spec.mixedV8 && spec.replyVersion == 0
}

// The request/reply family is closed the same way: its type-24/25/26 header
// rules can never be selected by an OSD, map or session specification.
func closedMDSRequestEncoderSpec(spec nativeEncoderSpec) bool {
	return spec.command == mdsRequestEncoderSpec.command && spec.wrapper == mdsRequestEncoderSpec.wrapper &&
		spec.driver == mdsRequestEncoderSpec.driver && slices.Equal(spec.names, mdsRequestEncoderSpec.names) &&
		spec.mdsRequests && !spec.mdsMaps && !spec.mdsSessions && !spec.mdsCaps && !spec.mdsReconnect && spec.requests == 0 && spec.events == 0 && !spec.controls &&
		spec.requestVersion == 0 && !spec.mixedV8 && spec.replyVersion == 0
}

// The capability family is closed the same way: its type 0x310-0x314 header
// rules and middle segment can never be selected by another specification.
func closedMDSCapsEncoderSpec(spec nativeEncoderSpec) bool {
	return spec.command == mdsCapsEncoderSpec.command && spec.wrapper == mdsCapsEncoderSpec.wrapper &&
		spec.driver == mdsCapsEncoderSpec.driver && slices.Equal(spec.names, mdsCapsEncoderSpec.names) &&
		spec.mdsCaps && !spec.mdsMaps && !spec.mdsSessions && !spec.mdsRequests && !spec.mdsReconnect && spec.requests == 0 && spec.events == 0 && !spec.controls &&
		spec.requestVersion == 0 && !spec.mixedV8 && spec.replyVersion == 0
}

// The reconnect family is closed the same way: its type-23 header rules and
// empty front can never be selected by another specification.
func closedMDSReconnectEncoderSpec(spec nativeEncoderSpec) bool {
	return spec.command == mdsReconnectEncoderSpec.command && spec.wrapper == mdsReconnectEncoderSpec.wrapper &&
		spec.driver == mdsReconnectEncoderSpec.driver && slices.Equal(spec.names, mdsReconnectEncoderSpec.names) &&
		spec.mdsReconnect && !spec.mdsMaps && !spec.mdsSessions && !spec.mdsRequests && !spec.mdsCaps && spec.requests == 0 && spec.events == 0 && !spec.controls &&
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

func nativeReceiptHasKeys(m map[string]any, keys ...string) bool {
	for _, key := range keys {
		if _, ok := m[key]; !ok {
			return false
		}
	}
	return len(m) == len(keys)
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

// Every decoder receipt class of the MDS suites; a capability or reconnect
// vector must carry exactly its own.
var nativeMDSReceiptClasses = []string{"decoded_caps", "decoded_cap_release", "decoded_lease", "decoded_snap", "decoded_quota",
	"decoded_request", "decoded_forward", "decoded_reply", "decoded_session", "decoded_map", "decoded_reconnect"}

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
