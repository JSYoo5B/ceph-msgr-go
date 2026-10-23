# OSD soak

The later [immutable workload receipts](OSD_SOAK_RECEIPTS_VALIDATION.md)
checkpoint preserves separate final observations for every test/worker and
records workload timing, fault phases and cleanup independently. Its source
and acceptance are separate from the historical runs below.

Run 2026-10-17, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture. Tests: `TestCephOSDSoakIntegration` and
`TestCephOSDSoakFailoverIntegration`, selected alone by
`CEPH_MSGR_TEST_OSD_SOAK=<duration>` (at least 30 s, OSD fixture only); each
runs for that duration.

## Workload

Four workers, each with its own client (OpenOSD shares one handle per OSD
within a client, and Close ends it for every holder) and its own
native-reserved object, run at a steady pace (2 ms between operations):

- APPEND of 1-512 random bytes (most operations), tracked as expected bytes;
- a full READ compared with the expected bytes;
- SETXATTR `soak` with a changing value;
- a TRUNCATE to zero once the object passes 64 KiB;
- connection replacement (Close and OpenOSD);
- an APPEND under a 1-15 ms deadline. When it ends as an unknown outcome the
  worker resolves it only by explicit resubmission under the original
  transaction ID on a fresh connection, retrying with a raised attempt while
  the resubmission is itself unknown.

At the end the native client must read exactly each worker's expected bytes
and xattr, at least 100 appends, 10 reads and 10 replacements must have run,
and goroutines must return to their starting level.

## Observed

| Profile | Duration | Appends | Reads | Replacements | Unknown, all resolved |
| --- | --- | --- | --- | --- | --- |
| legacy-v6, three BlueStore, secure AES256K IPv4 | 2 min | 76032 | 12671 | 12669 | 768 |
| replicated-pg-v8, three BlueStore, secure AES256K IPv6 | 1 min | 42007 | 6999 | 6999 | 241 |

Every short-deadline append that expired had been transmitted (none ended
unsent), and every one resolved on its first resubmission without a second
execution: the native bytes matched. Goroutines stayed at 47 throughout.

A later pg-v8 run passed the workload (72142 appends, 1010 unknown, all
resolved) but its native cleanup of the owned objects took more than 60
seconds right after the writes; the soak cleanups now allow 180 seconds, and
the rerun passed with 847 unknowns resolved.

## Under OSD failure (`TestCephOSDSoakFailoverIntegration`)

Three BlueStore replicas only. Four workers, each with its own client and
object, append 1-256 bytes every 10 ms, set an xattr, truncate past 32 KiB and
reread every 25 operations. Every 15 seconds the test goroutine SIGKILLs one
OSD of a worker object's acting set (alternately its primary and another
member), restarts it after 12 seconds and waits for clean PGs; one OSD is down
at a time. Workers behave as a caller: a write without a reply within 6
seconds ends unknown and is resubmitted only under its original transaction
ID, incarnation and a raised attempt, to the primary of a freshly queried
native placement; a write that never left is sent again as a new request;
failed reads are asked again. Native oracle and daemon control calls are
serialized because they share control files.

| Profile | Duration | OSD kills | Appends | Unknown, all resolved |
| --- | --- | --- | --- | --- |
| legacy-v6 (one PG, primary osd.1 each time), secure AES256K IPv4 | 2 min | 3 | 23078 | 21 |
| replicated-pg-v8, secure AES256K IPv6 | 2 min | 3 (osd.0, osd.2, osd.2) | 25125 | 13 |

Native bytes and xattrs matched for every object after the PGs were clean
again, so no write executed twice across primary changes and no write was
lost. The pace keeps the writes of other workers during one resolution well
inside the PG log's duplicate tracking; a resubmission later than that could
execute again, which is the documented limit of server-side duplicate
detection.

## OSD I/O across a MON leader stop (`TestCephOSDIOAcrossMonLeaderStopIntegration`)

A client seeded only with the leader MON opens an OSD connection and appends
every 20 ms. The leader is stopped: the client's MON session recovers to
another member by itself (a MON command then succeeds) while the appends keep
completing on the open OSD session, none unknown. After the leader restarts
and the fixture's settled-election barrier sees the full quorum twice, native
bytes equal every acknowledged append.

| Profile | Writes | After the stop | MON session |
| --- | --- | --- | --- |
| legacy-v6 single BlueStore, secure AES256K IPv4 | 401 | 118 | leader -> second member |
| replicated-pg-v8 three BlueStore, secure AES256K IPv6 | 389 | 113 | leader -> second member |

Two earlier attempts failed for test reasons, kept here because they show
real limits: sharing one client let one worker's Close end another worker's
handle (`client closed`); and unthrottled 4 KiB appends filled the 1 GiB
sparse BlueStore devices within about 40 seconds, after which writes (and the
native cleanup) were never answered, as a full pool does by design. A reread
of a 16 MiB object was also refused locally by the frame bound.

Not claimed: a long soak under MON failure, two OSDs down at once, PG splits, ticket
renewal during the soak (the default ticket lifetime exceeds these runs),
EC pools, or durations beyond the runs above. CI runs both for 4 minutes on
legacy-v6 three BlueStore.

## 2026-10-18 CI capacity failure and bounded correction

The first CI run of `source-1`, run
`39078413970`, job `117530521781`, passed the four-minute failover soak
(257.25 seconds overall, six OSD kills), then failed the ordinary soak after
420.91 seconds. Its counters stopped advancing by 90 seconds at 56,430 appends;
all four workers eventually reported sent SETXATTR/APPEND outcomes unknown due
to the unchanged enclosing deadline. This is retained as a failure, not a pass
or an authentication-renewal diagnosis.

The independent native diagnostics reported `OSD_FULL` for `osd.2`, `POOL_FULL`
for both pools, and epoch 71 with that OSD's `full` state. All three OSDs remained
up/in and both PGs were active+clean. Total device capacity was 3,221,225,472 bytes
(three 1 GiB devices), native allocated usage was 2,404,966,400 bytes, and logical
object data was only 560,737 bytes. Truncating the small live objects did not
bound total BlueStore/RocksDB allocation. The pinned `PrimaryLogPG.cc:2137-2160`
discards ordinary write-ordered operations whose submitted epoch precedes the
full transition; its full-pool checks also reject growing operations. No reply
therefore does not imply that the client may replay a mutation as a new one.

First-failure evidence remains in the original job log and artifact
`ceph-diagnostics-30` (artifact ID `12117823701`, archive SHA256
`0b1136596764dc6837691e8627d64c4da733497d5d3f33d59a15c69cb96482e6`).

The correction gives only the closed three-BlueStore soak selection four GiB
per OSD through the producer's existing `WithOSDBlockSize` option. The total is
12 GiB of logical sparse device capacity. Sparse files do not reserve 12 GiB
of RAM or disk in advance; their physical disk allocation can grow to that
capacity, with additional space needed for daemon databases, logs and container
layers. No tmpfs is introduced. Other selections omit the option and retain the
producer's 1 GiB default. Full flags/ratios, the four-minute workloads, deadlines,
request identity and unknown-outcome handling are unchanged.

The capacity selector has local regression coverage. **Four GiB sufficiency is
unverified until both complete four-minute workloads pass sequentially on the
same fresh fixture**, with native byte/xattr comparisons and owned cleanup.
Shorter runs do not satisfy that acceptance. The reproducible CI profile is
`CEPH_MSGR_TEST_OSD=1`, `CEPH_MSGR_TEST_OSD_STORE=bluestore`,
`CEPH_MSGR_TEST_OSD_REPLICAS=3`, `CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6`,
`CEPH_MSGR_TEST_OSD_SOAK=4m`, `CEPH_MSGR_TEST_KEY_TYPE=aes256k`,
`CEPH_MSGR_TEST_SERVICE_CIPHER=aes256k`, `CEPH_MSGR_TEST_CONNECTION_MODE=secure`,
`CEPH_MSGR_TEST_IP_FAMILY=4`, `CEPH_MSGR_TEST_RUNTIME=container`, and
`CEPH_MSGR_TEST_RUN=^TestCephOSDSoak(Failover)?Integration$`.

### First corrected four-minute pair: passed on 2026-10-18

The previously pending four-minute pair above passed on its first corrected
run, sequentially on one fresh three-BlueStore fixture. The manifest records
the same pinned Ceph image, `legacy-v6`, secure AES256K/AES256K, IPv4 and
container runtime; the producer's original and effective identities are both
v0.3.0 with module checksum
`h1:LRXc5mVpqw+/xDRWsv+m8ouLzkVDtB385RoQZ/7ygOY=`. The patched provider source
SHA256 remained
`6f2d6e2e50f170dbe31453d183e11e4b7a0110622114f8d746685f9935765e84`.
This execution used Linux arm64 containers;
the amd64 GitHub runner remains separate verification.

| Workload | Elapsed | Appends | Reads | Unknown / explicit resubmissions | Other checks |
| --- | --- | --- | --- | --- | --- |
| Four-minute failover | 256.13 s | 46,657 | 2,032 | 42 / 42 | Seven OSD kills, 48 connection replacements, zero unsent retries; native bytes/xattrs matched four objects |
| Four-minute ordinary | 243.90 s | 137,290 | 22,880 | 2,237 / 2,237 | 22,879 xattrs, 528 truncations, 22,879 connection replacements, zero unsent; native bytes/xattrs matched four objects |

The log has both test PASS results and no FAIL, SKIP or panic. The ordinary
workload continued beyond the earlier 90-second stall, with sampled goroutine
counts of 45–47 and the final goroutine-bound assertion passing. The separate
client test binary had no matching tests; it is not counted as additional
coverage. The native comparisons are proven by the test assertions and PASS
summaries; the per-object observation file is overwritten by the final cleanup,
so this run does not retain a separate complete raw byte receipt for all eight
final object observations.

Four retained live status snapshots (`start`, `mid`, `late`, `final-window`)
each reported 12,884,901,888 bytes total, establishing three 4 GiB devices.
None had OSD_FULL or POOL_FULL. This is sampled status evidence, not continuous
health history or a physical disk-allocation measurement. The final-window
snapshot at epoch 76 had three OSDs up/in, two active+clean PGs, and
2,628,337,664 bytes used (the largest retained sample), with continuing writes.
Expected fault-window peering/degradation appeared in earlier samples.
MON_DISK_LOW and the existing legacy CRUSH/application warnings remained;
the run is not claimed to have HEALTH_OK throughout.

- Run log: `1b2046f47c751dde132200d467e5388dcd5b26056d7f817f3467419d12e2b38b`
- Manifest: `265615f5baf4f497b6c042fc1b73281e4bd4cd990e4c02199f469edf9fc205fd`
- Final cleanup: `88ead67c1a5c91c87b0ed6bdc6c9434658f2a8a3ea3445a221166afdfb72ee49`
- Final-window status: `0fa22a603c84fa8b763d882c93462a0f1f3e81bb423ef2a47eb5db56e6a6f240`

This satisfies the corrected capacity acceptance only for this local profile
and duration. It does not replace the first CI failure, establish a new CI
matrix pass, or extend the earlier remaining coverage claims to longer soaks,
other configurations or continuous health/resource monitoring.
