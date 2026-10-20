# O0/O1 mutation validation

Run window: 2026-10-15, Asia/Seoul. Initial mutation work started on
`codex/osd-mutations` after the fixture CI correction `source-1`; subsequent
checks extend that implementation. This record describes local native
acceptance and does not claim all remote CI profiles passed for the new code.

Provider: published ceph-testcontainers-go v0.2.0. All MON/MGR/OSD daemons and
their stop/start lifecycle use its public APIs. Native librados ctypes calls
run only in the disposable oracle container, outside product dependencies.
No native daemon bootstrap script or product CGO/native dependency was added.

Pinned source/image: Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`,
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Docker Desktop Linux arm64; host relay tests use Darwin arm64. The topology is
three MONs and two MGRs, legacy CRUSH, HASHPSPOOL disabled and no upmap.
Single-OSD profiles use replicated size/min_size 1. The separate three-OSD
BlueStore profile uses size 3/min_size 2. EC I/O is unimplemented and unverified.

| Profile | Actual acceptance |
| --- | --- |
| secure/aes256k, IPv4, single BlueStore OSD, container | all eleven selected OSD/MON-map tests passed; 21 mutation/guard cases, five mixed read/stat/mutation vectors, lost reply, orderly restart, SIGKILL, native stat/read, renewal and feature rejection |
| secure/aes256k, IPv4, three BlueStore OSDs, replicas 3/min_size 2, container | all twelve selected OSD/MON-map tests passed; 25 mutation/guard cases including four CMPEXT EOF cases, five mixed vectors, maps/renewal, lost reply, orderly restart and SIGKILL; actual primary SIGKILL preserved independent native/Go bytes, version and nanosecond mtime on the surviving two replicas without another write |
| CRC/aes256k, IPv4, single MemStore OSD, host relay | nine selected tests passed; 25 mutation/guard cases, five mixed vectors, MON/OSD maps, lost reply, native stat/read, renewal and feature rejection; three BlueStore-only restart/crash/replica tests skipped |
| CRC/AES client and service keys, IPv4, three BlueStore OSDs, replicas 3/min_size 2, host relay | all twelve selected tests passed on the first attempt; 25 mutation/guard cases, five mixed vectors, actual primary SIGKILL, restart/crash, maps, lost reply and renewal |
| secure/aes256k client and service keys, IPv6, three BlueStore OSDs, replicas 3/min_size 2, container | all twelve selected tests passed on the first native attempt; 25 mutation/guard cases, five mixed vectors, actual primary SIGKILL, restart/crash, maps, lost reply and renewal; every owned OSD public endpoint matched the selected IPv6 family |

Native vector comparisons use a separate native object reset to the same
baseline. Both paths receive explicit matching nanosecond mtimes. Native
version guards compare the corresponding native object's current version plus
the same relative assertion; different objects need not share version numbers.
Aggregate/per-op errors and independent binary readback are compared. Coverage:
hole writes, shorter/empty replacement, append, truncate shrink/extension,
interior/tail/beyond-EOF zero, create and exclusive refusal, delete/ENOENT,
version match/older/newer, extent match/mismatch, rollback after staged write,
FAILOK commit and ordered payload segmentation. The expanded 25-case run adds
four CMPEXT EOF cases: comparison past EOF with zero padding (reply extent
length 2), comparison wholly beyond EOF (reply length 0), EOF mismatch rollback,
and EOF mismatch with FAILOK commit. The mismatch code and normalized extent
are preserved independently from aggregate completion and committed readback.

## Concurrent compound mutations

`TestCephOSDMutationConcurrencyIntegration` passed on its first standalone
native run in 3.17 seconds, on `source-2` plus the new test. The pinned
CRC/AES/AES IPv4 three-BlueStore/min_size 2 fixture ran the CGO=0 Darwin arm64
client through the host relay. This is an additional test, not a rerun of the
twelve-test profile above.

Each of two subcases submits sixteen concurrent two-operation requests on
one explicitly targeted OSD connection and the same object. Independent
WRITE extents reconstruct sixteen tagged binary records; two APPENDs within
each compound request must remain one complete record. The append order is
unspecified, so native readback checks the record multiset for missing,
duplicated or torn records. Both subcases compare ONDISK completion, per-op
codes/extents/results, unique submission TIDs and user versions, final native
bytes/version/nanosecond mtime, and a later Go read. TIDs are compared as a
set between sequential before/after requests, never in goroutine completion
order. The two subcases completed 32 compound requests/64 mutation ops;
write TIDs were 2–17 with native final version 18, and append TIDs were 20–35
with native final version 35.

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDMutationConcurrencyIntegration$' \
sh integration/run.sh
```

The lost-reply test sends one append and blocks receive bytes only after OSD
authentication. An independent native read proves execution before cancellation.
The canceled call retains OutcomeUnknownError and local submission identity.
A caller-chosen fresh session reads the bytes with a larger TID and does not
repeat the append. The native observer checks the bytes again after reopening.

BlueStore restart acceptance checks ONDISK completion, orderly Stop/Start,
independent native readback and a new Go read with native-refreshed epoch and
endpoint nonce. Original fixture bytes/mtime and routing metadata are restored
for subsequent tests. The added forced-termination test confirms SIGKILL exit
137 without OOM, then restarts the single BlueStore daemon and matches bytes,
version and nanosecond mtime through independent native and fresh Go reads.
Neither orderly restart nor SIGKILL simulates host/device power failure.
MemStore ONDISK is deliberately not treated as restart persistence.

The three-replica test kills the native map's actual primary after ONDISK
completion, rather than assuming osd.0 owns the object. Native placement
returns a later epoch and another primary in the surviving two-OSD acting set.
The caller explicitly opens that OSD and reads the committed binary bytes with
a larger TID. Native and Go user versions match the original completed write;
native nanosecond mtime is preserved. A second native read confirms that the
read-only recovery did not execute another write. The original primary is
restored during cleanup. This establishes that daemon-failure scenario only.

All CGO=0 root Go tests and vet passed, as did the fixture module tests/vet.
Race tests passed for internal/osd, internal/session and cephmsgr. The latest
166 Python checks and two added fixture readiness/profile unit tests passed.
Existing
independent native decoder bytes for stat/read remain unchanged. New unit tests
check packed mutation unions and data boundaries without an encoder/decoder
roundtrip, reject unsupported fields/flags and read-budget overflow, require
ONDISK and preserve unknown-outcome identity. Five independently Python-packed
[mutation vectors](../../internal/osd/testdata/mutation-README.md) passed the
pinned native ceph-dencoder: a nine-opcode compound request, all-op success,
EEXIST, compare-extent mismatch and zero-to-truncate replies. Re-export keeps
nonempty decoded payload and does not independently invoke the native payload
encoder. The separately constructed fresh native encoder vectors below close
that bounded mutation/guard payload gap.

## Fresh native payload encoding

Five separate [native encoder vectors](../../internal/osd/testdata/mutation-native-README.md)
were generated from new MOSDOp/MOSDOpReply objects using unchanged pinned
Ceph implementation and a matching freshly built ceph-common library. Direct
native encode_payload calls assert v6; no decoded payload was imported/reused.
Go matches the 519-byte request front and 11-byte binary data completely.
Four native reply constructors preserve flags, operation unions/results,
versions, placement and raw bytes. The new TRUNCATE fixture is
not live ZERO normalization evidence.

Five mixed read/stat/mutation vectors verify ordered output boundaries, staged
size versus committed read bytes, partial/EOF reads after write-full or truncate,
rollback with earlier outputs preserved, and FAILOK stat followed by create.
Expectations are derived from pinned PrimaryLogPG and exercised on the native
daemon; independent native stat2/read checks object state before and after.
The C librados write_op interface does not reproduce mixed read/write vectors,
so these are execution/readback checks, not native mixed-payload encoder proof.

## Observed failures and corrections

The first native vector experiment reached FAILOK comparison and found native
librados returning -EIO while the Go wire aggregate was success. Native bytes
and nanosecond mtime were identical and the comparison per-op value was -4095.
Pinned Objecter.h's cmpext callback throws for mismatch even under FAILOK.
The test now explicitly compares that native wrapper error separately from the
raw wire aggregate and verifies the committed data and per-op code. This was
not converted into a fabricated server refusal or hidden by rerunning.

The first BlueStore full experiment passed all 21 vectors and restart readback
but failed a fresh lost-reply handshake because the receive fault was still
armed. It also exposed stale nonce metadata for later read tests after OSD
restart. The fault is now disarmed before new authentication, and native
refreshed routing is published to subsequent fixture consumers. The final full
BlueStore suite passed all nine tests; the first failed run remains recorded
separately from that later acceptance.

A native mixed-vector experiment exposed a pinned Tentacle checksum path:
a nonempty WRITEFULL followed by an offset-zero full READ compared committed
bytes with the staged size/digest and marked the object unreadable. Source
analysis identifies the same risk for offset-zero WRITE, APPEND delegating to
WRITE on a staged empty object, and CMPEXT delegating to SYNC_READ. Those three
extensions are source-derived risk, not additional live failure observations.
Offset-zero READ/CMPEXT covering any earlier nonempty WRITEFULL/APPEND/offset-zero
WRITE payload is conservatively rejected before transmission, even across
intervening operations and without assuming the previous object size. Six
unit combinations verify rejection before send. Partial/EOF acceptance does
not remove the restriction; native checksum checking is not disabled.

Later status: a client race was identified as the common cause of four
auth-epoch failures (`source-8`, `source-9`, `source-10`, `source-11`) and fixed in
the product by `source-12`; the `source-13` recurrence above was not among them
and is not re-attributed. See the
[root cause](SERVICE_KEY_EPOCH_FAILURE_20261015.md#root-cause-ci-triage-2026-10-16)
and the [local soak after the fix](SERVICE_KEY_EPOCH_FAILURE_20261015.md#local-soak-after-the-fix-2026-10-16)
(680 of 680 first cold MGR authentications; CI amd64 runners were not
soaked). The other failure observations are not re-attributed to that cause.

```sh
CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION=1 sh integration/run.sh
```

Remaining work is listed in [the wire contract](../../internal/osd/CONTRACT.md):
broader replicated/address/OS matrices, host/device power-loss persistence,
EC and native unsolicited event/control/lifetime acceptance. Fresh mutation
payload encoder evidence is separate from the older decoder fixtures.
The SPEC O0/O1 milestones remain partially complete.

## Later CI failures and selected-OSD map preparation

The other failure was the secure/AES256K IPv4 three-BlueStore map-delivery
test, not mutation/concurrency readback. Its retained applied-map gate queried
osd.0 while the caller selected restarted osd.1. Preparation now carries the
actual target ID and verifies its owning daemon, whoami, FSID, newest epoch and
trim boundary. The test records incremental/full phase and observes bounded
RPC/map waits together, retaining completed causes regardless of select order.
Two gate regressions and six wait regressions passed; wait tests also passed
race instrumentation.

## Native OSD race profile

CI now adds the same bounded profile as a distinct Linux host race row while
retaining its ordinary counterpart. The local Darwin pass is not evidence
that this new Linux AMD64 CI row passed, and CGO=1 instruments development
tests only; product builds and the relay remain CGO-free. Host/device power
loss, other pool/features/stores and general staged-read semantics remain open.
