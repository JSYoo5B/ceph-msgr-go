# Bounded xattr codec and native interoperability

The limited O2a implementation supports GET/GETALL, STRING CMP, SET and RM.
This document retains the earlier native-only prerequisite separately from
later fresh codec and Go/native daemon acceptance. The independent adapter
invokes Tentacle's public librados C API; no Go wire encoder generates its
native comparison requests. [METADATA_CONTRACT.md](../../internal/osd/METADATA_CONTRACT.md)
records supported ordering/bounds and the broader incomplete O2 checklist.

Reference source: Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The first successful native matrix on 2026-10-15 used released
ceph-testcontainers-go v0.2.0, CRC, AES entity/service keys, IPv4, Darwin arm64
host relay and three BlueStore OSDs with replica size 3/min_size 2. The fixture
manifest records its actual architecture/image/FSID and daemon release; librados
ABI version `[3,0,0]` is recorded separately and does not identify Ceph's release.

## Observed native behavior

`TestCephOSDXattrNativeOracleIntegration` passed in 0.31s. Its bounded receipt
and independent Go assertions checked these facts on a dedicated disposable
object, removed afterward:

- Four attributes retained binary NUL/0xff bytes, an empty value, a leading
  underscore name and the comparison operand. Direct GET byte counts and
  GETXATTRS iterator output matched the original values; absent attributes
  returned ENODATA. Object bytes remained unchanged.
- One compound SET/RM replaced binary data and removed the empty attribute,
  increasing the version without changing unrelated object content.
- All six STRING comparison operators ran in both read and write vectors,
  with caller operands less than/equal to/greater than the stored binary
  operand: 36 cases. The caller operand is on the left. Ordinary false guards
  returned ECANCELED and preserved prior content, attributes, version and mtime.
  Successful write guards committed their distinct marker; read guards did not
  change object state.
- A false guard after an earlier staged SET rolled back the whole vector.
  FAILOK allowed the later marker and earlier SET to commit. Final native
  stat2 version/size/nanosecond mtime bracketed each set of observation reads.

The C API has no compound single-GETXATTR builder in the pinned header. Direct
GET and compound LIST are separately labeled. CMPXATTR/SET/RM callbacks also
do not expose raw per-op results; positive CMP true result 1 is pinned server
source evidence, not an observed C callback result. This oracle does not prove
wire union encoding, Go output budgets, Go sent-outcome semantics, staged xattr
read visibility, non-UTF-8 names on the server, configured size/name refusal,
permission refusal, U64 mode, restart persistence or crash recovery.

Eleven adapter regression tests passed. They cover binary-safe pointer/length
bindings, NULL value with length zero versus NULL-name iterator end, copying
before native buffers are freed, name validation and observation-version rules.
A daemonless pinned container also constructed/released all six read/write
comparison builders plus binary/empty SET/RM; that result alone was not treated
as server interoperability.

## First failures and correction

The first live fixture failed; the generic controller discarded the traceback,
so that run has no conclusive cause receipt. Its original log remains at
`/tmp/ceph-xattr-native-first.log` and its ownership files remain separate.
The second fresh fixture retained a bounded native command result: exit 1,
complete, no timeout/truncation. It failed the oracle's post-GET version check,
not a Go xattr request.

Pinned [PrimaryLogPG's read completion](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L9201)
does not set user_version for ENODATA.
[Objecter](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.cc#L3680)
copies that default reply version and
[IoCtxImpl](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/librados/IoCtxImpl.cc#L723)
updates last_objver even for a negative read. Thus absent-attribute GET can
replace the ioctx's last version with zero; it does not establish object change.
The oracle now performs one final successful stat2 and compares its version,
size and mtime to the initial stat2. Regressions accept a negative GET followed
by unchanged stat and reject independently changed final version/size/mtime.
No mutation or failed GET is retried.

The third fresh fixture passed with adapter SHA256
`a8a3c725a68d281de22920efee347ea5f7be72a2e7f38b107ca96f22fbe25b53`.
Evidence is `/tmp/ceph-xattr-native-third.log`,
`/tmp/ceph-xattr-native-third-diag/osd-xattr-native.json` and its bounded command
receipt. Every attempt's ten owned containers and network were independently
absent after cleanup. Native command stdout/stderr is now retained within
explicit limits; keys, configuration and private auth state are not inputs to
this receipt.

## Reproduce

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc \
CEPH_MSGR_TEST_RUN='^TestCephOSDXattrNativeOracleIntegration$' \
go -C integration/fixture run . run -project ../.. \
  -diagnostics /tmp/ceph-xattr-native-diagnostics
python3 -m unittest discover -s tools -p 'test_osd_xattr_native.py'
```

All daemon lifecycle stays in the public fixture APIs. The C adapter and Ceph
tools are development-only; the Go product's build/runtime/dependency graph
remains CGO-free. The wrapper is independently written against public headers
with LGPL-2.1 provenance and copies no native implementation.

## Fresh codec and public client verification

Five [fresh native encoder/decoder vectors](../../internal/osd/testdata/xattr-native-README.md)
verify the five unions, binary/non-UTF8 names, empty SET, all six STRING IDs,
native map output, GET normalization, signed +1 and constructed -125/-27/-2
reply cases. Go matches the complete 592-byte request front/80-byte data and
41-byte header, decodes the four replies and compares raw fields/output with
the separate native decoder receipt. These constructed replies alone are not
server-handled results or a test of the configured native limits.

Public peer tests retain these native reply bytes and inject separately labeled
faults. They check pre-TID invalid-name/flag/union/budget/staged-read refusal;
positive results, FAILOK and known aggregate errors; and sent malformed output,
reserved union bytes, over-budget data and ACK-only mixed mutations. Failures
retain OutcomeUnknown, exact local submission and available raw metadata,
terminate the session and never reconnect or replay the request. Internal
native-seeded decode fuzzing passed 209,833 executions in a 20-second run;
focused public/internal race tests and whole CGO0 Go/vet checks passed.

## Go/native daemon acceptance

The new `TestCephOSDXattrsIntegration` passed 43 subcases: 42 independent
Go/native vectors plus one mixed GET/GETALL/READ/STAT case. It uses the existing
caller-routed fixture head object; the native oracle reads that actual object,
resets a separate mirror once, executes one native vector, compares state and
removes only the mirror. Original bytes, prior attrs and nanosecond mtime are
restored afterward; version increases are retained rather than fabricated away.

The 36 read/write comparison vectors exercise all six operators with lesser,
equal and greater binary operands. Actual Go replies preserve true per-op +1
and false -125; the C API comparison result is aggregate/state evidence, not an
invented raw callback. Additional vectors cover binary/empty/underscore/non-UTF8
SET/RM, guard-before-SET/RM refusal, FAILOK commit, missing attribute versus
missing object, raw list encoding and mixed output boundaries.

| Profile | Observed result | Scope |
| --- | --- | --- |
| Secure AES256K entity/service, IPv4, Linux arm64 container, 3 BlueStore replicas/min2 | 43/43 in 16.94s | Focused Go/native xattr suite |
| CRC AES256K entity/service, IPv4, Darwin arm64 host relay, single MemStore | 43/43 in 16.09s on first run | Focused xattr; ONDISK is not restart persistence |
| CRC AES entity/service, IPv4, Darwin arm64 host relay, 3 BlueStore replicas/min2, race | 16/16 whole OSD top-level tests; xattr 43/43 in 17.31s | Local instrumented O1/xattr regression, not Linux CI proof |

All fixtures use the pinned image/source and public v0.2.0 daemon APIs. The
whole race suite has no FAIL/SKIP/DATA RACE. Its client package selector runs
zero tests and is not counted as another pass. Owned containers/networks were
independently absent afterward:10 for each three-BlueStore fixture,8 for the
single-MemStore fixture. The product remains CGO0; CGO1 instruments host tests.

First Go/native execution passed 37 cases then failed a test vector that put
SET before CMP, an ordering intentionally rejected before TID/send. The test
was corrected to CMP-before-SET/RM; product policy was not relaxed. Its first
log `/tmp/ceph-xattr-go-first.log` remains separate. Successful evidence:

- `/tmp/ceph-xattr-go-second.log` and `/tmp/ceph-xattr-go-second-diag`.
- `/tmp/ceph-xattr-memstore-crc-aes256k-20261015/run.log`, source/profile,
  ownership and cleanup receipts in that directory.
- `/tmp/ceph-osd-race-whole-20261015-02/run.log`, execution-inputs and
  cleanup-and-result receipts in that directory.

The comparison adapter revision SHA256 is
`dd49b11630915deac1f36f7160de3d91a2d8d181324fb11f227150c8144c291e`.
The earlier a8a3 native-only proof remains a separate revision. Root CLI
`encode-osd-xattrs` also successfully built, encoded and separately decoded
the fresh vectors; its owned builder was removed. Sharing that SDK cache with
`encode-osd-mutations` must preserve both wrapper source paths at configuration
time; regenerating the original mutation target verifies its unchanged bytes.

Exact main head `03ad44d5f3dafddf8aeb3293484cd49c9af2310a` passed all 42
jobs of [CI 38633315993](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38633315993)
on its first run. Original job logs show the native C oracle and 43 Go/native
xattr subcases passing in each of nine OSD profiles: CRC/secure, AES/AES256K,
IPv4/IPv6, container/host relay, single MemStore/BlueStore and three BlueStore
replicas. These are 387 profile-specific subcase executions, not 387 distinct
behaviors. The host three-BlueStore CRC/AES race job also passed without a data
race. Evidence is retained in `/tmp/ceph-ci-03ad44d-watch`; this CI success
does not establish a cause or fix for the earlier auth-epoch intermittency.

## Sent mixed mutation and handle Close

`TestCephOSDXattrOutcomeIntegration` passed its first native run in 1.65s on
CRC/AES entity/service, IPv4, Darwin arm64 host relay, three BlueStore
replicas/min2. One actual APPEND+SETXATTR vector used a binary value and a
non-UTF-8 attribute name. The receive fault withholds reads without capturing
reply bytes or authentication material. Independent native reads establish
the exact append, attribute, higher version and explicit nanosecond mtime
before the Go handle is closed while its caller context is still live.

The absent reply retains `OutcomeUnknown` plus `ErrClosed` and the exact local
submission. It has no invented remote transaction, flags, per-op result or raw
reply. A fresh caller-selected connection has a larger local TID, reads the
same bytes/attribute/stat and preserves the native version and inventory.
No append is replayed. Explicit cleanup restores original bytes, prior attrs
and mtime; all ten owned containers and the network were independently absent.
This is execution/readback evidence, not a received ONDISK or C API raw per-op
receipt. Evidence and frozen source/profile/cleanup receipts are retained at
`/private/tmp/ceph-xattr-outcome-crc-aes-ymt7zj1m`; reproduce with
`CEPH_MSGR_TEST_RUN='^TestCephOSDXattrOutcomeIntegration$'` and the CRC/AES
three-BlueStore host profile above.

## Mixed metadata after primary SIGKILL

`TestCephOSDXattrCrashIntegration` passed its first native run in 20.05s on
secure AES256K entity/service, IPv4, Linux arm64 container, three sparse-file
BlueStore replicas/min2. A single APPEND+SET(binary)+RM(existing attribute)
vector returned ONDISK. Independent native reads verified complete bytes,
attribute inventory, user version and explicit nanosecond mtime before the
actual selected primary `osd.1` was killed. The public fixture receipt proves
exit 137 without OOM and identifies the owned container.

The remaining two replicas form a degraded readable PG. They are not called
clean. Native MON placement supplies a new explicit target, map epoch and
endpoint/nonce; Go STAT/READ/GET/GETALL and the removed attribute's known
ENODATA preserve the same state and higher local TID. Native snapshots before
and after these reads have identical inventory, bytes, version and mtime.
No recovery write replays the vector. Cleanup restarts the owned primary,
waits for three-replica clean state, obtains fresh routing and restores only
the test's attrs and original bytes/mtime.

All ten owned containers and the network were independently absent afterward.
Evidence and frozen execution-input/cleanup receipts are retained at
`/tmp/ceph-xattr-crash-secure-20261015-01`. Snapshot comparisons occur inside
the test; the ordinary native receipt file is replaced by later observations
and cleanup, so it is not a persisted snapshot history. Reproduce with
`CEPH_MSGR_TEST_RUN='^TestCephOSDXattrCrashIntegration$'` and the secure
AES256K three-BlueStore container profile. This verifies daemon SIGKILL;
host/device power loss and media durability remain unverified.

## Configured server limits and rollback

`TestCephOSDXattrServerLimitsIntegration` passed its first native run, six of
six cases in 7.20s, on CRC/AES entity/service, IPv4, Darwin arm64 host relay,
three BlueStore replicas/min2. Public v0.2.0 `TemporaryConfig` handles own
exact prior MON DB entry/absence and restore without overriding outside
changes. Bounded `AdminSocket(config get)` gates verify both selected effective
options on every owned OSD, covering the actual object and native mirror even
if their placement differs. Receipts bind each daemon to its owned container.
Only these two numeric options and selected prior entries are retained.

Effective `osd_max_attr_size`/`osd_max_attr_name_len` were 0/100 on all three
OSDs, became 32/32, and returned to 0/100. Both prior central entries were
absent and were removed on restore. Values and names of length 32 committed
with ONDISK; a 33-byte value returned native/Go `-EFBIG` (-27), and a 33-byte
name returned `-ENAMETOOLONG` (-36). Each refusal also followed an earlier
staged SET: complete bytes, attrs, version and mtime remained unchanged,
proving rollback. Go preserves the aggregate and raw per-op errors as known
server refusals; a negative receipt has no mandatory ONDISK requirement.
Native C callbacks still do not expose raw SET per-op results.

Independent cleanup confirmed all ten containers and the network absent.
Evidence, selected limits receipt and frozen source/profile/cleanup receipts
are retained at `/tmp/ceph-xattr-limits-crc-aes-20261015-first`. Reproduce with
`CEPH_MSGR_TEST_RUN='^TestCephOSDXattrServerLimitsIntegration$'` and the CRC/AES
three-BlueStore host profile. Meaningful fixture unit tests cover every-owned
daemon gates, partial uncertain config application, restoration conflict,
reverse cleanup, cancellation and selected bounded JSON. Their race/vet
checks and whole fixture CGO0 checks passed.

## Read-only capabilities

`TestCephOSDXattrPermissionsIntegration` passed a corrected fresh native run
in 1.26s on CRC/AES256K entity/service, IPv4, Darwin arm64 host relay, single
MemStore. The public fixture creates `client.readonly` with OSD allow-r.
The native adapter admits only the fixed test/readonly identities; mirror
setup, state observations and cleanup use client.test, while its single
comparison vector uses client.readonly. No caller-controlled auth path or
privileged retry is provided. Existing default client.test behavior is retained.

Binary GET/GETALL and independent native readonly listing succeed. Native and
Go SET/RM return aggregate EPERM (-1), while the actual Go per-op result is 0:
capability admission rejects the vector before executing an operation.
Raw flags `0x4000024` echo the request, including ONDISK, and do not establish
execution or persistence of a refused write. The result remains a known
`OSDError`, with raw reply/local identity and no OutcomeUnknown. The same
readonly session continues reading at higher TIDs. Native bytes, full attrs,
version and nanosecond mtime remain unchanged. Native C raw SET/RM callbacks
remain unavailable; the actual Go wire result is separately retained.

The first run failed a test expectation of command-style EACCES (-13) before
the Go SET. Its generic native receipt was overwritten by cleanup, so the
first exact native code cannot be reconstructed from that log. Pinned
PrimaryLogPG capability admission uses EPERM; only the corrected fresh run
actually records native/Go -1. The first log and evidence limitation are
retained at `/private/tmp/ceph-xattr-permission-crc-aes256k-rnnkf64f`.
The corrected proof, frozen source/profile and five bounded phase receipts
are retained at
`/private/tmp/ceph-xattr-permission-corrected-crc-aes256k-sys6jup7`.
All eight owned containers and the network were independently removed.
Reproduce with `CEPH_MSGR_TEST_RUN='^TestCephOSDXattrPermissionsIntegration$'`
and the CRC/AES256K single-MemStore host profile. Python unit tests verify
fixed role admission, vector connection selection and cleanup ownership.

## Caller-prepared namespace isolation

`TestCephOSDXattrNamespacesIntegration` passed in 6.34s on a third fresh
CRC/AES entity/service, IPv4, Darwin arm64 host relay, three-BlueStore/min2
fixture. The same bare object name in `ceph-msgr/ns-alpha` and
`ceph-msgr/네임스페이스` has separate native routing and state. Native MON JSON
reported raw PGs `2.305acbaf` and `2.8724e2c9`, mapped PG `2.0`, epoch 21,
primary 1 and nonce 568705511. Callers supply each namespace/hash/epoch/target
to Go; no placement calculation or automatic namespace selection is added.

Explicit Go WRITEFULL+binary/empty SET, GET/GETALL/READ, subsequent SET/RM and
known ENODATA match native snapshots. Each operation preserves the peer
namespace and the original default-namespace object. Version/mtime comparison
stays within each object. Cleanup explicitly deletes only the two newly owned
objects, independently verifies absence and default-object stability, and
confirms all ten owned containers and the network removed.

The first trial failed before writes and discarded the generic native stderr,
so its exact cause was not observed. A second unchanged trial, with the new
bounded native command receipt, shows the native CLI rejecting Unicode nspace
as invalid printable characters and EINVAL. This is CLI frontend validation,
not a Go/OSD namespace failure. The adapter now issues read-only `osd map` and
`osd dump` JSON through public librados `rados_mon_command`, keeping the same
epoch-consistency and identity checks. Per-query native timeouts are finite
whole seconds within the remaining budget; subsecond exhaustion fails before
send. Native-allocated buffers are bounded before copying and always freed.

First/second logs and independent cleanup receipts remain at
`/tmp/ceph-xattr-namespaces-first*` and `...-second*`; successful frozen source,
log, selected receipts and cleanup are at `...-third*`. The successful adapter
SHA256 is `d58b2de61a2ab9ceaa9032168ce8f3c54a02abbe07d66ddca686d49513aaf956`.
Its 22 Python tests cover namespace/object identity, epoch read budgets,
native JSON calls, error/free paths and bounded outputs. Reproduce with
`CEPH_MSGR_TEST_RUN='^TestCephOSDXattrNamespacesIntegration$'` and the CRC/AES
three-BlueStore host profile.

Numeric CMP, omap/class/snapshot and broad pool/EC profiles remain incomplete.
The P1 O2 milestone and final environment are not complete.

Reproduce the Go/native profile with
`CEPH_MSGR_TEST_RUN='^TestCephOSDXattrsIntegration$'` and the same fixture flags
above (or the secure AES256K/container profile). Whole OSD fixture selection
includes both native-only preparation and the new Go interoperability test.
