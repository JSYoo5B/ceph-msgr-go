# MAPEXT, CRC32C CHECKSUM and watcher rerun acceptance — 2026-10-15

Tested product source: `6556509abac28ebd20505d1f896dce70bd9bda61`. The change adds bounded,
standalone head-object extent maps and CRC32C checksums, and permits a completed
native watcher scope to start again on the same disposable fixture. It does
not add automatic placement/routing, new negotiated features, watch/notify,
backoff, EC or MDS support.

## Independent wire evidence

Each new opcode has five fresh native message vectors produced from pinned
Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`:

- MAPEXT: request, populated extent map, empty map, ENOENT and EACCES.
- CHECKSUM: CRC32C request with explicit seed/chunks, normal output, aligned EOF trimming, EOF with no output and unaligned-trim EINVAL.

Native MOSDOp/MOSDOpReply encode their payloads from fresh logical objects,
then separate native decoders check union/results/data. The native MessageFrame
header uses explicit offline seq/ACK inputs. Go request front/data/header,
internal reply decoding and public response conversion match the stored bytes.
The logical output extents and CRC values in these wire vectors are not live
server execution. EACCES is a wire refusal vector, not native capability proof.
Provenance records source/archive/submodule/config/wrapper/driver/library/binary
and segment hashes and successful builder cleanup in
`internal/osd/testdata/{mapext,checksum}-native-provenance.json`.

The first CHECKSUM wrapper build used an earlier seven-argument builder call;
the pinned const-buffer overload requires the eighth Context pointer. The
corrected eight-argument wrapper was rebuilt, encoded and independently decoded
successfully. This failure established no wire or daemon incompatibility.
Native proof logs are `/private/tmp/ceph-mapext-native-proof.json.log` and
`/private/tmp/ceph-checksum-native-proof2.json.log`; the failed CHECKSUM build
is retained as `/private/tmp/ceph-checksum-native-proof.json.log`.

## Actual daemon profiles and cases

Both final runs used Ceph 20.2.4 at the exact pinned commit above and image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Daemons were Linux arm64. The fixture provider was v0.2.0; this work did not
change or review its repository. Native helpers and dependencies remain in
the development fixture module; product builds remain CGO=0 native Go.

| Profile | CHECKSUM | MAPEXT | Watcher listing/scope | Result |
| --- | --- | --- | --- | --- |
| Darwin arm64 host/IPv4 relay; CRC; AES client/service; three BlueStore replicas | 7 | 8 | 6 | 21/21 passed; no subcase skipped |
| CGO=0 Linux arm64 container; IPv6; secure; AES256K client/service; single MemStore | 7 | 8 | 6 | 21/21 passed; no subcase skipped |

CHECKSUM compares Go output bytes and CRC values with the independent C
`rados_checksum` API on binary contents. Cases are one CRC over the requested
extent, seeded chunks, aligned EOF trimming, unaligned EOF trimming with
`-EINVAL`, exact EOF and past EOF with no output, and missing-object `-ENOENT`.
Object bytes and version remain unchanged. Echoed type/chunk metadata survives
both successful and failed replies. Native librados can route/retry its reads;
that test-client behavior is not imported into the Go transport.

MAPEXT compares dense/sparse full maps directly with native `rados mapext`,
plus partial range, EOF clipping, beyond-EOF, missing-object and output-budget
cases. The CLI exposes a full-object operation only: partial/EOF expected maps
are the intersection of that independently observed full map with the explicit
requested range, recorded with `native_same_range=false`; they are not claimed
as direct same-range native calls. Full requests record `native_same_range=true`.
Store-specific allocation layouts are kept; no hardware/physical allocation
claim follows from the logical map. Reads preserve object data/mtime/version.
A separately authenticated client proves over-budget output fails its own
connection while the original client still reads successfully.

Watcher cases cover empty head, missing-object error, two native watch3
registrations matching Go/native CLI identity/cookie/address and registration
timeouts, an over-budget isolated client, successful unwatch and a second
complete registration/unwatch scope on the same fixture. Token-specific
status/log receipts are retained, and the standard diagnostic log path refers
to the current owned log. Mock tests additionally cover changed owned objects,
active/failed scopes, ownership mismatches, startup failure, concurrent control
and foreign log paths. The live repeated-scope case is not a full suite run
with `-count=2`.

## First failures and independent review

The first combined daemon run is retained separately:
`/private/tmp/ceph-read-metadata-crc-aes-first.log` and matching diagnostics.
CHECKSUM's first six cases passed; its missing-object test wrongly equated
aggregate `-ENOENT` with the per-op result, which was zero on this early
admission path. The C convenience API exposes only the aggregate. The corrected
test preserves both Go fields and does not claim a C per-op oracle.
MAPEXT's seven map/range/error cases passed; the last test accidentally reused
the existing same-client/same-OSD handle. Using an independent client corrected
the test setup. Neither failure was fixed by changing server-result semantics.

Independent review also found and fixed two issues before final acceptance:
a completed watcher scope's retained status/log prevented a subsequent start;
and CHECKSUM validation accepted a successful zero-length/count-zero output
that cannot occur for this API's positive-length requests. Native EOF instead
keeps the original length and sends no data. Separate regressions now protect
both cases. Other protocol/union/seed/EOF/admission review found no additional
actionable defect; this is not a claim of exhaustive verification.

Final logs and whitelisted diagnostics:

- `/private/tmp/ceph-read-metadata-crc-aes-final.log` and matching directory.
- `/private/tmp/ceph-read-metadata-secure-aes256k-ipv6-final.log` and matching directory.

Each directory retains fixture/source/image/profile metadata and
`osd-{mapext,checksum,watchers}-results.json`. Disposable fixture cleanup
completed after both successful runs.

## Local checks and remaining gaps

Product `CGO_ENABLED=0 go test ./...`, vet, cephmsgr/internal-osd race tests,
nested fixture CGO=0 unit tests/vet and all 269 Python tool tests passed on the
final code. Native provenance tests re-read the exact stored segment hashes
and reject mismatched generator/header receipts. The earlier watcher decoder
fuzz result is recorded in [OSD_WATCHERS_VALIDATION.md](OSD_WATCHERS_VALIDATION.md).

These are local checks and selected native profiles; remote CI success has
not been established here. Other checksum algorithms, snapshot/compound forms,
MAPEXT same-range native API calls for partial ranges, scoped read-only
credentials, mutation races, EC, broader OS/address/configuration combinations
and final full I/O/environment milestones remain unsupported or unverified.

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSD(ListWatchers|MapExt|Checksum)Integration$' \
sh integration/run.sh
```
