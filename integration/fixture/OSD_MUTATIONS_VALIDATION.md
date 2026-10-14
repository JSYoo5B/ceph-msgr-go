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
encoder. True native payload encoder fixtures remain outstanding.

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
is Linux arm64; the added Linux amd64 CI result must be checked separately.
Reproducible execution is:

```sh
CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION=1 sh integration/run.sh
```

Remaining work is listed in [the wire contract](../../internal/osd/CONTRACT.md):
broader replicated/address/OS matrices, host/device power-loss persistence,
EC, true native mutation payload encoder fixtures and unsolicited event/control
contracts. The SPEC O0/O1 milestones remain partially complete.

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
