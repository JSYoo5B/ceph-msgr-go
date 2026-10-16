# O0/O1 mutation validation

Run window: 2026-10-15, Asia/Seoul. Initial mutation work started on
`codex/osd-mutations` after the fixture CI correction `a25426b`; subsequent
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
native run in 3.17 seconds, on `819781e` plus the new test. The pinned
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

Evidence: `/tmp/ceph-osd-concurrency-crc-aes.log`,
`/tmp/ceph-osd-concurrency-crc-aes-ownership.json`, and
`/tmp/ceph-osd-concurrency-crc-aes-cleanup.log`. Independent inspection found
all ten owned containers and the owned network removed. This adds no product
routing, placement, native dependency or mutation replay. The normal OSD
selection includes the new test; the other CI profiles need their own result.

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

The separate `TestCephOSDMutationCloseIntegration` passed on its first native
CRC/AES/AES IPv4 three-BlueStore host-relay run in 2.66 seconds, on `f4632b5`
plus the new test. Native readback first proves the single append executed
with its explicit mtime/version, then OSDConnection.Close ends the pending
wait while the caller context remains live. OutcomeUnknown+ErrClosed retains
local submission identity without fabricated remote reply fields. A fresh,
caller-selected connection reads the same bytes with a larger TID, and final
native bytes/version/mtime remain unchanged. All ten owned containers and the
owned network were independently absent after cleanup. Evidence is
`/tmp/ceph-osd-close-crc-aes.log` and its ownership/cleanup records.

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

A later actual cached build emitted five 41-byte native header2 serializations
through MessageFrame::Encode after Message::encode(MSG_CRC_ALL). Native
type/version/compatibility, TID and envelope lengths are preserved; seq 1,
ACK seq 3 and priority 127 are explicit offline inputs. Go framing matches
the complete request header, and all five headers decode with their unchanged
front/data. This is native envelope/serialization proof, not live
ProtocolV2::write_message or server execution. The matching library stayed
unchanged while the wrapper was recompiled and linked; copied-source mtime is
refreshed so the build cache cannot reuse an older wrapper. The proof and
compile/link log are `/tmp/ceph-mutation-native-header-crc-proof.json` and its
`.log`; builder `aa566d24fd8b` was independently absent after termination.
The initial stale-wrapper failure has only its retained tool-output excerpt,
as the first log path was reused; it was not counted as a successful proof.

The stored receipt records source/submodule/configuration/wrapper/library
hashes and actual loader/execution results. The public Testcontainers command
`encode-osd-mutations` was itself run with a retained cache and reproduced all
ten front/data segments identically. Independent inspection confirmed its
owned builder `c81959def195` was deleted. Proof/log:
`/tmp/ceph-native-public-runner-proof.json` and the same path plus `.log`.
Development packages remain in the owned builder; the product has no native
build/runtime/transitive dependency.

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

Local logs: `/tmp/osd-mutations-secure.log` (initial FAILOK experiment),
`/tmp/osd-mutations-bluestore.log` (first full experiment),
`/tmp/osd-mutations-bluestore2.log` (focused correction),
`/tmp/osd-mutations-bluestore-full.log` (final full suite),
`/tmp/osd-mutations-crc.log` (CRC host acceptance).
Final expanded single-BlueStore acceptance is recorded in
`/tmp/ceph-osd-o1-complete-profiles.log` (eleven selected tests, including all
five mixed vectors and single-OSD SIGKILL). Three-replica primary-loss acceptance
is recorded in `/tmp/ceph-osd-3replicas.log` (one dedicated test). Expanded
three-replica acceptance is in `/tmp/ceph-osd-3replicas-full3.log`: all twelve
selected tests, 25 mutation/guard cases and all five mixed vectors passed.
Expanded CRC/MemStore host acceptance is in `/tmp/ceph-osd-crc-final.log`: nine
selected tests passed, including all 25 mutation/guard cases and five mixed
vectors; three BlueStore-only tests skipped. Both expanded runs cleaned up
their disposable containers.
The additional CRC/AES/AES IPv4 host-relay three-BlueStore profile passed all
twelve tests on its first attempt at product source
`0943727781a4d41d5092d38ab600f03478dba6ef`, recorded in
`/tmp/ceph-osd-3replicas-crc-aes.log`. It includes all 25 mutation/guard cases,
five mixed vectors and actual-primary SIGKILL readback. Independent Docker
inspection matched the ten owned container IDs and network recorded in
`/tmp/ceph-osd-3replicas-crc-aes-ownership.txt`; all ten containers and the network
were absent after cleanup, as recorded in
`/tmp/ceph-osd-3replicas-crc-aes-cleanup.log`. This is local profile evidence,
not a whole remote CI acceptance claim.
Product checksum/EOF corrections were pushed to main in `cbabcbd`; fixture
auth precondition work was pushed in `b3bf303`. These hashes identify changes,
not a claim that every remote CI matrix passed.
These paths are local execution evidence, not tracked repository artifacts.

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

The first whole three-OSD run failed stale target IDs in restart/crash adapters;
the second encountered an old map during backfill. Fixture preparation now
uses public WaitForClean with a 30-second bound and refreshes native placement
at each three-OSD test's start and cleanup. The corrected third whole run passed
all twelve selected tests. This changes disposable fixture readiness and
caller-owned targets; the product does not add placement or mutation replay.
The readiness question and detailed evidence are retained in
[upstream #65 follow-up](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/65#issuecomment-6173762797).

The auth readiness question [upstream #71](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/71)
is separate from OSD mutation acceptance. CI AES/AES IPv4 failed a cold MGR
connection with -13 after the second rotating service-key wipe; the native
daemon reported ticket bad magic. The exact profile passed two local wipe
cycles. That is intermittency/diagnostic evidence, not a demonstrated fix of
the readiness recipe or its cause. Separate MGR precondition changes passed
four selected native tests. This proves those fixture preconditions, not a
causal fix for intermittent rotating-key rejection.

The rejection recurred on consumer `2d1cc4624cafeb64072ce64d56a2ede032fbddda`,
[CI job 115993789742](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38605086391/job/115993789742),
this time on the first wipe. All nine native readiness probes passed with
MonMap epoch 4/auth epoch 10 and active mgr.a/global ID 4167, before the cold
Go client's first MGR authentication returned -13 and mgr.a logged ticket-info
bad magic. The initial and renewal Go MON connections were both mon.a. The
retained public observations still do not identify the effective service-key
generation used at issuance or MGR lookup. The recurrence is retained in
[issue #71's follow-up](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/71#issuecomment-6174955026).
Subsequent successful CI runs do not establish a fix for this failure.

Later status: a client race was identified as the common cause of four
auth-epoch failures (`b01a3cf`, `37d7a4d`, `494071b`, `af2c4e9`) and fixed in
the product by `3d2645e`; the `2d1cc46` recurrence above was not among them
and is not re-attributed. See the
[root cause](SERVICE_KEY_EPOCH_FAILURE_20261015.md#root-cause-ci-triage-2026-10-16)
and the [local soak after the fix](SERVICE_KEY_EPOCH_FAILURE_20261015.md#local-soak-after-the-fix-2026-10-16)
(680 of 680 first cold MGR authentications; CI amd64 runners were not
soaked). The failure records in this section are kept as written. Upstream
#71 remains open there; the root cause and evidence were posted as [a comment](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/71#issuecomment-6208315401) on 2026-10-16.

The conditional native AUTH preparation branch is now separately verified by
`TestCephNativeManagerPreparationIntegration` in
`/tmp/ceph-native-mgr-conditional.log` (PASS, 48.95 seconds). Public Pause/Resume
held the native standby mgr.b for 45 seconds with ordinary 12-second tickets.
The native MgrMap independently showed the missing standby and its latest
native AUTH -13 before preparation ran. Exactly one receipt was retained in
`/tmp/ceph-native-mgr-conditional-diagnostics/manager-preparation.jsonl`; one
public Stop/Start restored mgr.b with a new global ID while active mgr.a and
its ID were unchanged. A second healthy preparation neither restarted a
daemon nor added a receipt. This verifies disposable native fixture
precondition repair, not a Go CephX protocol fix or Go MGR authentication replay.
The lazy Go MGR path was not exercised by this preparation test. Issue #65's
API/recipe question remains open, and #71's rotating-key failure is separate.

The opt-in `CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION=1` selects this one test.
CI includes this isolated AES/aes256k IPv4 profile. The local evidence above
is Linux arm64. The Linux amd64 row also passed on exact consumer source
`6658d14` in [CI run 38605940894](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38605940894),
whose 40 jobs all passed, and on `d39cc93` in
[CI run 38608642377](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38608642377),
whose 41 jobs all passed, including the added IPv6 three-BlueStore row.
Reproducible execution is:

```sh
CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION=1 sh integration/run.sh
```

Remaining work is listed in [the wire contract](../../internal/osd/CONTRACT.md):
broader replicated/address/OS matrices, host/device power-loss persistence,
EC and native unsolicited event/control/lifetime acceptance. Fresh mutation
payload encoder evidence is separate from the older decoder fixtures.
The SPEC O0/O1 milestones remain partially complete.

## Later CI failures and selected-OSD map preparation

Consumer `7d33e14` passed all 41 jobs in
[CI run 38613204323](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38613204323).
Subsequent `f4632b5` [CI run 38613806089](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38613806089)
passed 39/41. Its IPv6 AES256K service-key epoch row rejected the second
cycle's first MGR authentication even though the renewed cached blob remained
unchanged between observations. The first-failure artifact and limitations are
retained in [issue #71](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/71#issuecomment-6175912840).
This cause was unresolved when recorded; no wipe/authentication retry was
added. The later [root cause](SERVICE_KEY_EPOCH_FAILURE_20261015.md#root-cause-ci-triage-2026-10-16),
fixed by `3d2645e`, was derived from four other failures; this record was not
re-attributed to it.

`49e7599` [CI run 38622629859](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38622629859)
passed 40/41, including every OSD row and both auth-epoch rows. Its Windows
fixture test failed because CRLF checkout changed the exact C++ source hash.
`9be36ae` pins that file to LF and [CI run 38623451052](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38623451052)
passed all 41 jobs, including that Windows test. Runtime provenance hash
validation remains exact. This separate pass does not resolve auth intermittency.
`ef50577` [CI run 38624472769](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38624472769)
also passed all 41 jobs after rejecting aggregate-success replies containing
an ordinary failed operation. Those malformed sent-mutation replies retain
unknown outcomes, raw signed results and local submission identity; FAILOK,
known aggregate refusals and positive results remain accepted.

The other failure was the secure/AES256K IPv4 three-BlueStore map-delivery
test, not mutation/concurrency readback. Its retained applied-map gate queried
osd.0 while the caller selected restarted osd.1. Preparation now carries the
actual target ID and verifies its owning daemon, whoami, FSID, newest epoch and
trim boundary. The test records incremental/full phase and observes bounded
RPC/map waits together, retaining completed causes regardless of select order.
Two gate regressions and six wait regressions passed; wait tests also passed
race instrumentation.

The first corrected whole native secure/AES256K IPv4 three-BlueStore run passed
all 14 selected tests, including the added Close and concurrency cases, 25
mutation/guard cases and five mixed vectors. Map delivery passed in 6.88s
after the preceding crash test. Source was `f4632b5` plus the Close/gate/wait
changes. All ten owned containers and the network were independently removed.
Evidence: `/tmp/ceph-osd-selected-gate-secure.log` and ownership/cleanup files.
The original CI artifact remains at `/tmp/ceph-f463-osd3-ipv4-failure`; its OSD
logs truncate after the failure and cannot reconstruct the exact failed phase
or RPC outcome. Wrong-target gating is established; the new local pass does
not prove every possible missing-map cause resolved or replace exact-head CI.

IPv6 acceptance source was `6658d14` plus the IPv6 profile/oracle change.
The earlier `IPv4`-only adapter preflight rejection is preserved separately in
`/tmp/ceph-osd-3replicas-ipv6-secure.log`; no daemon was created in that attempt.
The first actual native run passed in
`/tmp/ceph-osd-3replicas-ipv6-secure-native.log`. Ownership and independent
cleanup checks are in `/tmp/ceph-osd-3replicas-ipv6-secure-ownership.txt` and
`/tmp/ceph-osd-3replicas-ipv6-secure-cleanup.log`: all ten owned containers and
the dedicated network were absent after termination. The provider's public
WithAddressFamily IPv6 option applies to MON/MGR/OSD; no provider or product
feature mask was changed. Python oracle tests now total 170 passes.

## Native OSD race profile

A separate CRC/AES/AES IPv4 host-relay three-BlueStore run passed all 15
selected API tests with race instrumentation and no failures, skips or race
report. Both API/client executables were verified as `-race=true`/CGO=1,
Darwin arm64, Go 1.27.2; daemons used the pinned Linux arm64 image. The client
package selected zero tests and is not counted as another acceptance pass.
The suite included 25 mutation/guard cases, five mixed vectors, 32 concurrent
compound requests, Close/lost-response handling, restart/SIGKILL/replicas,
maps/stat/read/renewal/feature rejection and the native-only xattr prerequisite.
All ten owned containers and the network were independently absent afterward.
Evidence: `/tmp/ceph-osd-race-whole-20261015-01/run.log`,
`execution-inputs.json` and `cleanup-and-result.json` in that directory.

CI now adds the same bounded profile as a distinct Linux host race row while
retaining its ordinary counterpart. The local Darwin pass is not evidence
that this new Linux AMD64 CI row passed, and CGO=1 instruments development
tests only; product builds and the relay remain CGO-free. Host/device power
loss, other pool/features/stores and general staged-read semantics remain open.
