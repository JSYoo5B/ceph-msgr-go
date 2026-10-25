package main

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

var writeSameEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-writesame", wrapper: "osd_writesame_native_encoder.cc", driver: "osd_writesame_native_encoder.py",
	names: []string{"writesame-native-request-v6-all", "writesame-native-reply-v6-success", "writesame-native-reply-v6-refused", "writesame-native-reply-v6-failok", "writesame-native-reply-v6-zero"},
}

var u64XattrEncoderSpec = nativeEncoderSpec{
	command: "encode-osd-u64-xattrs", wrapper: "osd_u64_xattr_native_encoder.cc", driver: "osd_u64_xattr_native_encoder.py", requests: 2,
	names: []string{"u64-xattr-native-request-v6-read", "u64-xattr-native-request-v6-write", "u64-xattr-native-reply-v6-true", "u64-xattr-native-reply-v6-false", "u64-xattr-native-reply-v6-invalid"},
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
	command: "encode-osd-checksum", wrapper: "osd_checksum_native_encoder.cc", driver: "osd_checksum_native_encoder.py", requests: 3,
	names: []string{"checksum-native-request-v6-crc32c", "checksum-native-request-v6-xxhash32", "checksum-native-request-v6-xxhash64", "checksum-native-reply-v6-success", "checksum-native-reply-v6-trimmed", "checksum-native-reply-v6-eof", "checksum-native-reply-v6-unaligned", "checksum-native-reply-v6-xxhash32-success", "checksum-native-reply-v6-xxhash32-trimmed", "checksum-native-reply-v6-xxhash32-eof", "checksum-native-reply-v6-xxhash32-unaligned", "checksum-native-reply-v6-xxhash64-success", "checksum-native-reply-v6-xxhash64-trimmed", "checksum-native-reply-v6-xxhash64-eof", "checksum-native-reply-v6-xxhash64-unaligned"},
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
