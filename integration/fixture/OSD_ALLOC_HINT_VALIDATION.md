# O2g allocation hint validation

Run window: 2026-10-15 Asia/Seoul. Product subset: CEPH_OSD_OP_SETALLOCHINT
0x2223 with expected object size, expected write size and the pinned hint flag
bits. [ALLOC_HINT_CONTRACT.md](../../internal/osd/ALLOC_HINT_CONTRACT.md)
records the pinned source semantics. Source is Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`; image is
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Independent codec and local boundaries

The first `encode-osd-alloc-hint` SDK run compiled a fresh wrapper against the
same generated headers and libceph-common as the earlier proofs and used the
pinned `set_alloc_hint` builder, which adds FAILOK to every hint. Five triples
were encoded and separately decoded. Go request header/front/data matches all
41/293/3 bytes for [SETALLOCHINT 4194304/65536/flags 257, WRITEFULL `abc`,
SETALLOCHINT 0/0/0] when the Go hint operations carry FAILOK like the builder;
without FAILOK the encoding differs, which a test checks. Replies keep the
echoed union and no output: success, maximum values with all eleven flags,
FAILOK -95 before a committed WRITEFULL (an older-OSD shape), and a logical
-28 on the record_write_error path that pinned Tentacle cannot actually
produce for this op. Provenance and the offline seq/ACK/priority caveat are in
the [native fixtures](../../internal/osd/testdata/alloc-hint-native-README.md).

Internal and public synthetic tests cover the source-derived union literal,
unknown hint bits, data, extent, version, xattr, OMAP and class fields on the
hint, hint fields on other opcodes, EXCL, maximum values before a read, a CALL
after a staged hint, union echo validation, FAILOK -95 retention, a changed
echo and a commit without ONDISK as uncertain outcomes.

## Live native and Go hints

The oracle seeds an owned object and reserves an absent name. Each Go vector
is mirrored on a separate native object with `rados_write_op_set_alloc_hint2`
(which adds FAILOK) and the same explicit mtime. Both objects' committed
object_info is read through the empty xattr name (OI_ATTR `_`) and decoded by
the pinned `ceph-dencoder type object_info_t ... dump_json`.

| Actual profile | Observed result |
| --- | --- |
| CRC/AES entity/service, IPv4, Darwin arm64 host relay, three BlueStore/min2 | 6/6 subcases in 5.52s on the first run |
| secure/AES256K entity/service, IPv4, Linux arm64 container, three BlueStore/min2, CGO0 binaries | whole OSD suite 28/28 top-level, allocation hint 6/6; no FAIL/SKIP |

| Case | object_info after the Go request (equal to the native mirror) |
| --- | --- |
| Hint on the seeded object | expected_object_size 4194304, expected_write_size 65536, flags 257; bytes kept, size 6 |
| Maximum sizes and all eleven flags | 2^64-1, 2^64-1, flags 2047 |
| Zero hint | all three fields reset to 0 |
| Hint on an absent object | object created with size 0; 1048576, 4096, APPEND_ONLY (16) |
| FAILOK hint, then WRITEFULL `abc` | 8192, 512, IMMUTABLE (32); size 3 |
| Unknown flag bit 2048 | native server recorded 2048 unvalidated; Go refused before transmission |

Each Go hint committed with ONDISK, advanced the user version (equal to
object_info user_version) and applied the request mtime. Native cleanup removed
every owned name; owned containers and network were independently absent
afterwards. Logs and diagnostics: `/tmp/ceph-osd-alloc-hint-crc-aes-20261015-first` and
`/tmp/ceph-osd-whole-secure-aes256k-container-allochint-20261015-first`.

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDAllocationHintIntegration$' \
sh integration/run.sh
```

## Remaining gaps

The object store's use of the hint (BlueStore allocation or compression
behavior), EC pools, response loss/SIGKILL, broader profiles and remote CI for
this subset need separate evidence.
