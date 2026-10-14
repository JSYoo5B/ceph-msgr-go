# O2b bounded OMAP read validation

Run window: 2026-10-15 Asia/Seoul. Product subset: OMAPGETKEYS 0x1211,
OMAPGETVALS 0x1212 and OMAPGETVALSBYKEYS 0x1214. Header, comparisons, mutations,
range removal, snapshots, EC and automatic pagination remain unsupported.
This is a partial O2 milestone. [OMAP_CONTRACT.md](../../internal/osd/OMAP_CONTRACT.md)
records the pinned source and distinct wire semantics.

All daemon lifecycle uses public ceph-testcontainers-go v0.2.0. The native C
oracle, C++ encoder and CLI/controller live outside product dependencies.
The product still builds and runs with CGO0. Source is Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`; image is
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Current pool is replicated, legacy placement/HASHPSPOOL disabled, PG count 1,
three sparse-file BlueStore OSDs with min_size 2; feature mask is unchanged.

## Independent codec and local boundaries

The first public SDK encoder run compiled fresh native builders, encoded five
message triples and separately decoded each in new native objects. Matching
generated headers/library and exact source/hash provenance are retained with
the [native fixtures](../../internal/osd/testdata/omap-native-README.md).
Go request header/front/data matches all 325/75 bytes; four reply payloads keep
union input lengths, per-op result 0, output segments, empty values and exact
page-more versus BYKEYS-no-suffix framing. Logical replies/header seq/ACK are
offline inputs, not server execution or live transport observations.

Internal admission/output tests bound lengths before allocation, sort copied
BYKEYS keys without mutating callers, reject duplicates/irrelevant variants,
validate strict order/cursor/prefix/membership/count/bool/suffix/budgets and
retain failed/unexecuted operation metadata without invented collections.
The initial source-derived fuzz target passed 6,830,600 executions in 20s;
this initial fuzz count is not native-seeded interoperability evidence.
Public tests include 24 actual-sent synthetic cases and 4 untouched native
reply cases, with pre-TID/no-send validation, exact local submission and raw
reply retention, terminal malformed-reply errors and no automatic pagination
or reconnect/replay. Focused CGO0/race and whole Go/vet checks passed.

## Live native C and Go pages

The native oracle exclusively creates one UUID-suffixed owned object with
binary contents, separate attrs and ten OMAP entries, including empty/NUL/
non-UTF8 keys and empty/binary values. Callers obtain native MON map/dump
identity for that exact object and explicitly open the primary endpoint.
Go never computes placement or selects a namespace. Native C SET2 only seeds
the oracle; it does not prove product OMAP mutation support.

| Actual profile | Observed result |
| --- | --- |
| CRC/AES entity/service, IPv4, Darwin arm64 host relay, three BlueStore/min2 | first focused run 23/23 in 10.02s |
| secure/AES256K entity/service, IPv4, Linux arm64 container, three BlueStore/min2, CGO0 binaries | first whole OSD suite 22/22 top-level, OMAP 23/23 in 9.94s, xattr 43/43 in 16.12s; no FAIL/SKIP |

Cases cover keys/vals count 0/1/2/exact/final/MaxUint64, prefix before/equal/after
cursor, BYKEYS empty set/missing/empty/binary keys, and mixed OMAP/READ/STAT
segmentation. Count 0 reports actual more=true; exact/final pages more=false.
Before and after every Go read, independent native observations preserve
complete object bytes, attrs, OMAP, user version and nanosecond mtime.
Native iterator buffers are copied before next/free; NULL key ends iteration,
while a NULL value with length 0 is an existing empty value. Native inventory
rejects truncated pages and explicitly reads the empty key through BYKEYS2.

Missing object results are deliberately separate: native aggregate and Go
aggregate are -2, actual native C decoder callbacks are -5 for both operations,
and raw Go wire per-op results are 0 with no output. Pinned Objecter.h:687-713
decodes empty output for nonnegative per-op wire result and changes prval to
EIO on buffer error; this source supports the callback-overwrite inference.
It is not a server/wire -5 or evidence that all C callbacks equal wire results.
Both distinct observations are retained without fabrication or retry.

The full secure suite includes xattr namespace/limits/readonly/Close/SIGKILL
and prior O1/map/renewal/concurrency/recovery. Client selector runs zero tests
and is not counted. Secure container output 131066 bytes was readable by the
host with public receipt mode 0644 inside private IPC. Native UUID cleanup
returned 0; scoped server settings returned to their original effective values.
Each fixture's ten owned containers and network were independently absent;
frozen source hashes were unchanged. Logs/profiles/bounded result/cleanup:

- `/tmp/ceph-omap-reads-crc-aes-20261015-first` (focused; result 130846 bytes).
- `/tmp/ceph-osd-whole-secure-aes256k-container-20261015-first` (whole; 48 source hashes).

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDOmapReadsIntegration$' \
sh integration/run.sh
```

Native C cursor/prefix arguments remain NUL-terminated; NUL pagination inputs
are rejected by this oracle, not by the Go wire API. Fresh C++ codec vectors
cover those length-aware inputs; an independent live C++ pagination comparison
remains unexecuted. Configured entry/byte cap, OMAP permissions, broader
store/pool/namespace profiles and remote CI for this new subset need separate
evidence. No header/CMP/write/EC/snapshot or all-O2 completion is claimed.
