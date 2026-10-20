# Read-only CHECKSUM compounds — 2026-10-18

Five Linux configurations passed `TestCephOSDChecksumCompoundIntegration` on
the same final v2 source: **50 leaves passed, no skips**. Each configuration
compared ten Go read-only compounds with ten native C API compound operations
on an immutable owned binary object. Each of the five negative cases was
followed by a fresh read through the same Go OSD connection. The selected
receipt is [OSD_CHECKSUM_COMPOUND_LINUX_20261018.json](OSD_CHECKSUM_COMPOUND_LINUX_20261018.json).

The product continues to use `OSDRequest.Operations` and each operation's
`Checksum.Values`. No routing policy, feature bit or automatic replay was
added. The new compound subset admits CHECKSUM with CHECKSUM, READ, STAT,
ASSERT_VER and CMPEXT on replicated heads, under its documented bounds.
Standalone CHECKSUM and its existing snapshot behavior remain separate.

## Actual configurations and source

Pinned Ceph is Tentacle 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`. All runs used Linux amd64 and image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

| Configuration | Request / reply profile | Key / service cipher | Network / runtime | OSD shape | UTC start → end | Top-level test |
| --- | --- | --- | --- | --- | --- | --- |
| crc-host | legacy-v6 / v6 | AES / AES | IPv4 host | 3 BlueStore, replicas 3/min_size 2 | 08:27:56.267852 → 08:29:12.601547 | PASS 8.45s |
| secure-ipv6 | legacy-v6 / v6 | AES256K / AES256K | IPv6 container | 1 MemStore, replicas 1/min_size 1 | 08:27:54.545450 → 08:28:59.874464 | PASS 6.86s |
| v8-crc-host | replicated-pg-v8 / v6 | AES / AES | IPv4 host | 3 BlueStore, replicas 3/min_size 2 | 08:27:57.290232 → 08:29:16.540202 | PASS 8.25s |
| v9-secure-ipv6 | replicated-pg-request-v9-reply-v8 / v8 | AES256K / AES256K | IPv6 container | 1 MemStore, replicas 1/min_size 1 | 08:27:56.813429 → 08:28:59.629996 | PASS 6.65s |
| reply-v8-secure-ipv6 | replicated-pg-reply-v8 / v8 | AES256K / AES256K | IPv6 container | 1 MemStore, replicas 1/min_size 1 | 08:29:42.685465 → 08:30:40.083591 | PASS 6.95s |

The executed source is base
`source-1` plus 18 overlay files. The audit
matched all selected files and every one of the 712 Go files against the
preserved executed stage. Its sorted Go inventory SHA256 is
`96b650eff2ffa62c7269036f7356d84ef4fce878e4b87f671851293f7a4865a7`.
The JSON preserves the selected file hashes, source-manifest hash and inventory
format. Later documentation edits are not represented as executed source.

## Closed live matrix

The existing exclusive OMAP-write fixture owns one main object and one
reserved absent name. Main bytes are fixed to
`00ff43484b00800d0a11223344556677` (16 bytes). Its native user version V and
mtime are observed before every native operation and after every Go compound.
The original mtime `1700000000.123456789`, bytes, version, attributes and other
native observed state remained unchanged. Successful Go replies matched their
own object's independently observed version: V=2 in the two legacy rows and
V=1 in the three modern rows. Error reply versions are retained without being
compared as successful version evidence.

In the table, C32 is CRC32C, X32 is xxHash32, X64 is xxHash64;
`C(type,offset,length,chunk,seed)` denotes one CHECKSUM. S is STAT, R is READ,
A is ASSERT_VER and M is CMPEXT. All requests are head-only, with no FAILOK,
CALL, mutation, write SnapContext or extra request flags.

| Case | One compound vector | Aggregate / raw per-op results | Independently observed output |
| --- | --- | --- | --- |
| mixed | C(C32,0,8,4,0x1234), S, R(4,8), C(X64,0,16,0,0xfedcba9876543210) | 0 / `[0,0,0,0]` | Checksum count 2; stat; 8 read bytes; checksum count 1 |
| distinct-seeds | C(X32,0,8,0,0xffffffff), C(X64,8,8,4,0x8000000000000001) | 0 / `[0,0]` | Separate 4/8-byte seeds and count 1/2 outputs |
| eof-prefix | C(C32,16,8,4,1), R(0,8), S | 0 / `[0,0,0]` | Empty checksum, original returned length 8; following READ starts at RawData offset 0 |
| trimmed | C(C32,12,8,0,7), S | 0 / `[0,0]` | Returned checksum length 4, count 1; stat follows |
| checksum-before-assert | C(C32,0,8,4,1), A(V+1), C(X64,0,8,0,2), R(0,4) | -75 / `[0,-75,0,0]` | Earlier checksum data and public Values survive; suffix empty |
| assert-first | A(V+1), C(C32,0,8,0,1), S | -75 / `[-75,0,0]` | Empty checksum/STAT suffix; no checksum Values |
| unaligned-after-read | R(0,4), C(C32,12,8,8,3), S, C(X32,0,8,0,4) | -22 / `[0,-22,0,0]` | First READ survives; failing checksum returns length 4/no data; suffix empty |
| checksum-before-cmpext | C(C32,0,8,0,5), M(0,wrong first four bytes), S | -4097 / `[0,-4097,0]` | First checksum survives; mismatch index 2; STAT empty |
| cmpext-first | M(0,correct first four bytes), C(X64,0,8,0,6), R(8,8) | 0 / `[0,0,0]` | Guard succeeds; checksum and 8 read bytes follow |
| missing | S, C(C32,0,8,0,1), R(0,4) on reserved absent name | -2 / `[0,0,0]` | Empty data and unchanged unions; no operation is attributed as executed |

Each negative aggregate remained a signed `OSDError` with raw output,
submission identity and earlier validated checksum Values where present.
The same Go OSD connection then accepted a new TID: a successful four-byte
READ for present objects, or a known -ENOENT READ for the absent object.
There are **ten compound calls and five fresh calls per configuration**,
with public metadata TIDs 1–15 and one client GID. These are fresh operations,
not re-execution of an earlier request. The receipt includes all 25 followups.

## What the native C API proves

Each native case constructs one `rados_read_op_t` containing the selected
checksum/read/stat/guard actions, then calls `rados_read_op_operate` once.
The receipt preserves the aggregate result, buffer contents, initialized
sentinels, exposed callback prvals and independent before/after observations.
There are **ten native operate invocations per configuration**, not a measured
total C API call count or native wire-send count. librados can internally
retry read requests; this adds no Go routing or replay behavior.

The C API's convenience results are not a raw MOSDOpReply observer:

- `rados_read_op_assert_version` has no prval pointer. Its aggregate error is
  compared independently; its raw -75 per-op result is observed in the Go
  reply, not invented in the C receipt.
- An untouched STAT callback receives raw result zero and empty output. Its
  decoder can replace the convenience prval with `-EIO` (-5). This occurred
  for STAT after a terminal guard/checksum failure and for the early absent
  case. Go retained raw per-op zero and no output. Native size/time sentinels
  remained intact. The test deliberately checks these different representations.
- The C API does not expose the returned extent union. CHECKSUM's returned
  length 4 on aligned trim or unaligned failure, and original length 8 at EOF,
  are actual Go wire observations checked against the bounded source-derived
  recipe. They are not described as independently captured native unions.
- Native STAT2 returns size/seconds/nanoseconds as C outputs. They are compared
  with Go's 16-byte STAT payload; no native wire payload is fabricated.

Pinned source anchors are `Objecter.h:623–636` for separate checksum actions,
`Objecter.cc:3752–3786` and `Objecter.h:351–382` for callback prval handling,
`PrimaryLogPG.cc:5533–5710` for checksum alignment/EOF handling, and
`:8205–8213` for first ordinary negative-result termination. Untouched raw
zero is not a general execution marker. Prefix Values are exposed only after
the entire compound identity, shape, payload and completion boundary validate.

## Independent raw and source audit

- Object/PG identity, flags, aggregate code, retry attempt, version fields and
  the operation count/order.
- Each 28-byte operation union, including CHECKSUM type/chunk/padding and
  ASSERT_VER's version at union offset 8, plus the separate signed rval array.
- Every `payload_len` split, full RawData consumption, each checksum's count,
  digest width and independently decoded public Values.
- Profile-specific tails: v6's empty locator/redirect versus v8's false
  redirect boolean, followed by all trace fields. No tail bytes remain.

Legacy reply PG seeds are compared to raw object hashes. Modern reply seeds
are compared to `Placement.PG.Seed`; object hashes remain independently checked
against native caller routing. GID/TID are preserved public submission metadata.
Raw Messenger headers were not retained, so GID/TID wire encoding and the
header version/compatibility fields are not independently decoded here. Tail
policy follows the declared profile.

Each configuration has nine fetched public files, independently hashed with
byte counts. Intermediate native receipts are typed embedded snapshots with
producer-recorded hashes of original JSON bytes; those intermediate JSON byte
streams are not independently available. The retained final standalone native
receipt's original hash and byte count are independently recomputed and match
its producer record. The selected JSON omits unused zero-valued API fields,
retaining the relevant requests, raw replies, native callbacks, state and hashes.

## Offline request proof and checks

The separate [15-vector proof](../../internal/osd/testdata/checksum-compound-native-provenance.json)
and [README](../../internal/osd/testdata/checksum-compound-native-README.md)
cover five fresh canonical MOSDOp layouts × v6/v8/v9. A separate fresh native
object decodes each request. Source/provenance and header/front/middle/data
hashes are bound; complete request bytes match the Go encoders in the test
suite. This proof contains **requests only**, no constructed replies, checksum
values or server-execution claim. Its header is an offline MessageFrame
projection, not live ProtocolV2 execution.

The fresh native builder ran UTC 08:16:12.790541–08:19:03.600465, exited zero,
and removed its named container. Provenance SHA256 is
`f7970a5ac5c4d2ae9ad040034ddd0a904937029ca7f4265294d8d6cfe543c290`.
The audit rechecked its wrapper/driver and build-log hashes and segment hashes.

All six final Linux checks exited zero on the executed v2 source:

| Check | Command |
| --- | --- |
| Root, CGO disabled | `go test ./...` |
| Race | `go test -race ./integration ./cephmsgr` |
| Root vet | `go vet ./...` |
| Nested fixture | `go test -mod=readonly ./...` |
| Nested fixture vet | `go vet -mod=readonly ./...` |
| Python | `python3 -m unittest discover -s tools -p 'test_*.py'` — 455 tests |

These checks include the unchanged standalone checksum algorithm/EOF/snapshot
coverage and new admission/budget/classification regressions. Product source
review also found that initial public conversion could retain checksum Values
before a malformed compound was rejected. The owner now clears compound Values
until whole-vector validation succeeds. Post-fix malformed positive-aggregate /
negative-guard and redirect regressions check the boundary. There is no retained
historical pre-fix execution log for that finding; source review plus post-fix
regressions are the evidence. It is separate from the expectation errors below.

## Separate CI selection followup

After the runtime source was frozen, six existing ordinary modern CI rows
were extended to select `TestCephOSDEnforceSnapContextIntegration` and
`TestCephOSDChecksumCompoundIntegration`. The auditor compared the old/new
workflow and actually declared Go test names: the matrix remains 63 rows,
the other 57 rows are identical, and each changed row adds exactly those two
tests while retaining its previous selections. No new matrix row is added.

Only these two files differ from the executed v2 source:

| File | SHA256 |
| --- | --- |
| `.github/workflows/ci.yml` | `a099130ad25205e6a415219912918203f493e20435a9b65a12ff49ba4e6fbdfd` |
| `tools/test_ci_osd_profiles.py` | `eb6246e6669f34e2c931fe8431d0cf4efc7e580c55037361609e8dec98a0e0f1` |

The followup stage retains all 712 Go files and the same runtime inventory
hash `96b650eff2ffa62c7269036f7356d84ef4fce878e4b87f671851293f7a4865a7`.
A separate Linux Python run passed all 455 tests, UTC
08:37:13.280366–08:37:19.632381. Its source hashes, receipt and log are under
`CI_selection_followup` in the JSON. They are not substituted for the final
native or six-check receipts above. This proves the selection contract and
Python checks, not execution or completion of the new GitHub matrix.

## Preserved first failures

Three different test-model failures remain identified separately in the JSON:

1. The first offline producer run generated 75 raw files, then its Python
   expectation failed because it placed ASSERT_VER's version at union offset
   0 instead of 8. The canonical native producer and existing Go encoding were
   correct. The old driver/log/receipt and all 75 original raw files are
   preserved and hash-checked. The provenance records a read-only recheck with
   the corrected model; the final proof comes from a separate fresh run.
2. Both first live runs passed four leaves, then failed only the integration
   assertion for the same ASSERT_VER union: actual Offset=0 and Length=V+1,
   while the test expected the reverse. Original failed logs/receipts and 10/8
   container plus two-network cleanup audits remain preserved. The test-only
   assertion was corrected; native producer and product code did not change.
3. The first full Go check failed six existing snapshot compound expectations:
   unsupported compound CHECKSUM still refused, now as `ErrRequest` rather
   than the old `ErrOperation`. The CHECKSUM expectation changed; MAPEXT's
   existing `ErrOperation` expectation was retained. This was not a snapshot
   operation being admitted.

The first live/full source had 17 overlays and 712 Go files, inventory
`f8bd3db47468b9a1c4f9dbd5b5a10189f72cdf9e45441acebb27d948506080d6`.
The auditor matched that separate executed stage and verified that only
`integration/osd_checksum_compound_test.go` and
`internal/osd/checksum_snapshot_test.go` differed before the final stage.
Neither failed first run is counted as a final success or substituted for a
final-source profile.

## Cleanup and limits

Native cleanup removed the owned main object and confirmed the reserved name
absent; no snapshot IDs were allocated. Independent audits confirm **44 distinct
owned containers and five networks absent** after the final runs. First-failure
cleanup is preserved separately and excluded from that final count.

Live coverage is the closed replicated, head-only read-only matrix in the five
configurations above. Snapshot compounds, EC, extra flags, CALL, mutation,
live faults and explicit resubmission were not live-tested. Synthetic tests
cover malformed replies, cancellation/Close, redirect/no-follow and no-replay
boundaries; these are not native fault-injection claims. Read output and ONDISK
flags do not establish mutation, rollback or extra durability behavior. Local
checks and these native runs do not establish whole-CI success.
