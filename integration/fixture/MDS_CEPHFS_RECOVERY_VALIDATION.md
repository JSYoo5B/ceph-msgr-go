# CephFS recovery acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go,
without CGO, librados, go-ceph, Ceph CLI subprocesses or native runtime
dependencies. The disposable provider, its native CLI, the native LibCephFS
oracle and the owned MDS are development-only. This acceptance covers four
parts of [the CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md):
journal flush and arrival order, the shared client address instance
(fencing), the end of a lossless Messenger resume that meets a restarted MDS,
and the CephFS reconnect to a new MDS incarnation with explicit request
resubmission. The owned recipe, its restart steps and its cleanup are
described in [MDS_CEPHFS_FIXTURE.md](MDS_CEPHFS_FIXTURE.md).
The original results below describe the 2026-10-16 recipe. The later
pending-flush extension and the journal fault boundary have their own
sections at the end; the leaf table describes the current recipe.

## Test shape

`TestCephMDSCephFSRecoveryIntegration` uses the profile and gates of the
metadata acceptance (`CEPH_MSGR_TEST_MDS_CEPHFS=1`, MDS_FRESH=1, neither
MDS_COMMANDS nor MDS_SESSIONS, `legacy-v6`) and runs before
`TestCephMDSCephFSIntegration` in the same fixture. It requires that no
restart was adopted yet. Every Go client authenticates as
`client.mds-cephfs-recovery-<token>` (MON `allow r`, MDS
`allow rw fsname=<owned fs>`, OSD `allow r tag cephfs data=<owned fs>`):

- the restart client: `EnableMDS` and `EnableCephFS`, one MDS connection,
  OSD dials refused;
- the fenced client: `EnableMDS`, `EnableCephFS` and `EnableOSD` with the
  default v6 object-hash OSD profile;
- the reader: `EnableOSD` only, so its OSD connection carries its own
  per-connection nonce.

MDS dials pass through counted, closable stream relays with a dial log. The
restart client's dialer can hold its next MDS dial until the test releases
it; this is a test-only control of the client's own dial path, used once to
submit a request while the transport is down. Sessions send `entity_id`,
`hostname` (`ceph-msgr-go-<token8>-recovery` or `-fenced`) and `root=/`, with
a 2 s RenewInterval and 1024 event slots. Requests use caller and owner
UID/GID 0 and the MDSMap epoch of the latest native observation.

The test-only capability agent of the metadata acceptance consumes every
session event. For this test it also records each inode's first dentry, and
restates `Client::send_reconnect` for the reconnect state: one entry per held
cap with `caps_wanted()` (open-file wants plus the wanted mask last sent,
FILE_BUFFER implying FILE_EXCL), the implemented mask as issued, the short
path (parent inode and dentry name, or the inode and an empty path), the cap's
realm, no snap follows and no file-lock blob, plus each realm once with the
sequence and parent from the latest snap trace. On the reconnected session it
continues with cap, issue and migrate sequences reset to zero, as the native
client does. It also restates `handle_cap_import` and `handle_cap_export`;
neither message arrived.

Object placement for the fenced data object comes from the MON through the
fenced client's raw `osd map` command (`raw_pgid` seed as the object hash)
and the primary's v2 address from `osd dump`; this interpretation is
test-only.

| Leaf | Checked behavior |
| --- | --- |
| `open_recovery_session` | Restart client session Open; the MDS OPEN advertises LUMINOUS (7), REPLY_ENCODING (9), MULTI_RECONNECT (12) and bits 18 to 20. Native `session ls`: `open`, the recovery identity, `client_features` `0x00000000001c83ff`, the metadata sent, 0 caps, address `<ip>:0/<State().CephFSNonce>` |
| `journal_flush_unsafe_to_safe` | MKDIR of the recovery directory, then three rounds of two CREATEs (`O_CREAT\|O_EXCL\|O_RDWR`): `flush-plain-i` waits for its safe reply untouched, `flush-requested-i` calls `FlushJournal` as soon as its unsafe reply arrives. Every safe reply arrives with result 0; the created ino in each first reply's extra equals the trace target; the native listing has the six names with the same inos. Latencies are recorded, not asserted |
| `eviction_fences_shared_nonce` | Native one-shot create and write of `fenced` (5000 bytes, `fsync`). The fenced client opens its CephFS session (native address `<ip>:0/<nonce>`), reads `<ino hex>.00000000` from the data pool through the explicit OSD API (stat and read; bytes equal the native payload). The fixture evicts that session; the new blocklist entries are exactly `<ip>:0/<CephFSNonce>`. The Go session ends Lost; the same OSD read on the same OSD connection, naming the OSDMap epoch after the eviction, fails with OSD result -108 or a connection-level refusal; the reader reads the same bytes. The restart client's session is unchanged |
| `mds_restart_reconnect_replay` | The fixture arms the restart. The restart client creates `held` and waits for its safe reply without a flush (aligning with a journal tick), keeps its handles open and opens `held` for read. It creates the directory `boundary` (safe), the fixture pauses the armed incarnation's journal submitter, and the client creates `boundary/replayed`, which must answer unsafe. The client then asks for a journal flush; the armed incarnation's op tracker must still hold that create in flight, early-replied and uncommitted, and the call must not be safe. Then the fixture fires `ceph mds fail <armed GID>` and the next MDS redial is held while `unanswered` is submitted, then released. The replayed create must not have become safe. The session must end Lost with both calls reporting an unknown outcome. Once native `fs dump` shows the same daemon name in rank 0 with a new GID and nonce in `up:reconnect`, the test closes the old connection, opens the new address and calls `ReconnectSession` with the agent's caps and realms and `Resubmit` of both calls. OPEN must arrive (`ReconnectedFrom` names the Lost session); both calls keep their TIDs and complete with result 0, the replayed create reporting the ino of its unsafe reply. After the MDS is `up:active` the fixture adopts the incarnation; native checks follow (below) |
| `cleanup_unlink_release_close` | UNLINK of the ten files and RMDIR of `boundary` and the recovery directory through the reconnected session (safe result 0), native ENOENT for the directory, `ReleaseCaps` of every held cap and native `num_caps` 0, confirmed `Close` and the session absent natively. The restart client never dialed an OSD |

After adoption the restart leaf also requires: `mds_log_pause` false on the
new incarnation; the new incarnation's op tracker (`dump_historic_ops`,
read right after adoption) holding the replayed create with ` REPLAY` in its
description, so the new incarnation executed it and the old one never
journaled it, and the resubmitted `unanswered` create; the old
incarnation's address blocklisted by the MON; native `session ls` showing
the Go session `open` at the same `ip:0/nonce` with `num_caps` above 0;
native `statx` of `boundary/replayed` and `unanswered` equal to the inos the
Go replies reported; and the native listing of the recovery directory equal
to its ten names (nine files and `boundary`) with their inos.

## Results

Both selected configurations passed on 2026-10-16 (KST), both tests, all
leaves, no skips. The tested source is HEAD
`source-1` plus only this change set,
exported to a separate directory with `git archive` (modified and new files
copied over); the two documentation edits made after the runs
(`MDS_CEPHFS_VALIDATION.md` pointer, this record) are the only differences
from the tested export. Source SHA-256:

| File | SHA-256 |
| --- | --- |
| `integration/mds_cephfs_recovery_test.go` | `3611d0de450b98ad27c8ebe62d9f773376ff68894afd6527aa891382365b1e5b` |
| `integration/mds_cephfs_test.go` | `1f9723433abb6150ac5bedc25fae2080f7ee7f30775f2d186788d700b70b18a6` |
| `integration/mds_cephfs_agent_test.go` | `6aefe86ac108042ccb6ccfb0109d8de86fd44210181a1b16e6c5c38b7f7ab798` |
| `integration/fixture/mds_cephfs.go` | `eb3329f68541199776702bef8a683c4c4d40cd1f038fbfd18e9c08f0b235446c` |
| `integration/fixture/mds_cephfs_recovery.go` | `98f6ead7a41f02c84c30f6e6919aadff86c2903eb9c8628148aa0628bae110e2` |
| `integration/fixture/profile.go` | `83580d418bded826912968cf779c839ac707d06469720751bbaf97ca421fa398` |
| `integration/relay/main.go` | `21f47ff3d48b6a58c0e2f119450df6730fa511346bf7a4f64f963d8c196661c3` |

Image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
(arm64), daemon version
`ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`.
Selector `^TestCephMDSCephFS(Recovery)?Integration$`.

| Configuration | Controller wall time (UTC, 2026-10-15) | Recovery test | Metadata test | Result |
| --- | --- | --- | --- | --- |
| Darwin arm64 host runtime, IPv4 relay, CRC, AES client and service keys, single BlueStore OSD | 18:10:04 to 18:13:35 (3 min 31 s) | 54.32 s | 65.15 s | 5/5 and 9/9 passed |
| CGO=0 Linux arm64 container runtime, IPv6, secure, AES256K client and service keys, single MemStore OSD | 18:13:35 to 18:17:14 (3 min 39 s) | 55.25 s | 76.12 s | 5/5 and 9/9 passed |

Recovery leaf durations (CRC / secure, seconds): open 2.21 / 2.10, journal
flush 12.61 / 13.22, eviction 10.04 / 9.73, restart 20.79 / 21.57, cleanup
8.64 / 8.62. The metadata test ran after the restart against the adopted
incarnation (`v2:172.19.0.10:6800/2780415422` and
`v2:[fd62:93fe:2c9e::a]:6800/301562001`) with the same nine leaves as its own
record.

### Journal flush

Unsafe-to-safe latency of each CREATE, from the unsafe reply's receive time
to the safe reply's (milliseconds, CRC / secure):

| Round | Without flush | With `FlushJournal` |
| --- | --- | --- |
| 0 | 490.7 / 1099.8 | 7.8 / 1.7 |
| 1 | 4983.8 / 4998.4 | 4.3 / 0.9 |
| 2 | 5002.5 / 5001.8 | 4.1 / 1.0 |

`FlushJournal` was sent 0.06 to 0.19 ms after the unsafe reply arrived.
Without it the safe reply waits for the MDS journal flush on its 5 s tick
(`mds_tick_interval` 5); with it the MDS flushed at once. Every CREATE
answered unsafe first (0.4 to 2.9 ms after Submit); MKDIR, the session's
first inode allocation, answered safe at once. Created inos were
`0x10000000001` to `0x10000000006`. Arrival numbers interleave cap pushes
between the unsafe and safe replies of the same request (for example
first reply 13, safe reply 17 in round 1).

### Fencing through the shared nonce

| Value | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| Fenced client GlobalID, `CephFSNonce` | 5208, 4148620095 | 5205, 4095565260 |
| Native session address | `172.19.0.9:0/4148620095` | `[fd62:93fe:2c9e::9]:0/4095565260` |
| `fenced` ino, data object | `0x100000003e9`, `100000003e9.00000000` | same |
| Placement (`osd map`) | pool 4, raw PG `4.da500b18`, PG `4.0`, acting primary 0, epoch 26 | same |
| Primary OSD address | `v2:172.19.0.8:6800/3445992005` | `v2:[fd62:93fe:2c9e::8]:6800/2790508815` |
| Read before eviction | result 0, ops `[0,0]`, stat size 5000, 5000 bytes equal to the native payload | same |
| New blocklist entry, exactly one | `172.19.0.9:0/4148620095` until `2026-10-15T19:11:30.244914Z` | `[fd62:93fe:2c9e::9]:0/4095565260` until `2026-10-15T19:14:58.616547Z` |
| Session end | Lost 2.04 s after the eviction request (fixture acknowledgement after 3.70 s) | Lost 1.61 s after (acknowledgement 3.31 s) |
| Read after eviction (epoch 27, same OSD connection) | OSD result `-108`, `OSDError{Code: -108}`, ops `[0,0]`, no data, 0.83 ms | `-108`, 0.25 ms |
| Separate reader | GlobalID 5463, result 0, 5000 equal bytes | GlobalID 5466, same |

The eviction is `tell mds.<fs>:0 session evict id=<gid>`: the MDS blocklists
the session's `ip:0/nonce`, waits for that OSDMap, then kills the session and
marks its connection down (no CLOSE). The Go Messenger resume redialed the
same MDS address once and was answered with SESSION_RESET; the session ended
Lost with `io: read/write on closed pipe; stateful resume failed: ceph
messenger session reconnect: ceph messenger: peer reset the stateful
session` (the first part is the test relay's closed pipe). The OSD
connection was not closed by the OSD; the request with the blocklisted
source address was answered with `-EBLOCKLISTED` (108) by
`PrimaryLogPG::do_op`. The reader's OSD connection, same IP and its own
nonce, was not fenced. The reader has no shared nonce (`CephFSNonce` 0); its
per-connection nonce is not exposed by the public API and was not recorded.

### MDS restart, resume RESET and reconnect

| Value | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| Restart client GlobalID, `CephFSNonce`, native address | 4959, 965683299, `172.19.0.9:0/965683299` | 4953, 1551771648, `[fd62:93fe:2c9e::9]:0/1551771648` |
| Armed (old) daemon GID, incarnation, address | 4659, 6, `172.19.0.10:6800/2177714133` | 14504, 6, `[fd62:93fe:2c9e::a]:6800/4192204523` |
| Adopted (new) daemon GID, incarnation, address | 15485, 10, `172.19.0.10:6800/2780415422` | 15485, 10, `[fd62:93fe:2c9e::a]:6800/301562001` |
| `held` create unsafe-to-safe (tick alignment) | 743.6 ms | 1056.4 ms |
| `replayed` unsafe reply after that safe reply | 3.6 ms | 3.3 ms |
| `mds fail` acknowledged by the fixture | 305 ms after the request | 403 ms |
| First redial after the transport fault | 258.5 ms | 341.6 ms |
| That held redial (held 20.1 / 12.7 ms) | proxy refused it (`EOF`) | `connect: connection refused` |
| Next redial | 52 ms later, connected, SESSION_RESET | 51 ms later, connected, SESSION_RESET |
| Session Lost | 313.1 ms after the request, 54.6 ms after the first redial | 393.1 ms, 51.6 ms |
| Resumes before Lost | 0 | 0 |
| Native rank 0 states (ms after the request) | failed at 717 (old GID absent, old address blocklisted), `up:replay` 1727, `up:reconnect` 2936, `up:active` 5717 | failed at 905, `up:replay` 2117, `up:reconnect` 2722, `up:active` 5899 |
| `ReconnectSession` started / OPEN received | 2936.9 ms; OPEN after 3.36 ms | 2722.8 ms; OPEN after 0.78 ms |

Times are relative to the test writing the `restart-fire` request. The error
chain at Lost was, on both profiles, `io: read/write on closed pipe; stateful
resume failed: ceph messenger session reconnect: ceph messenger: peer reset
the stateful session`: `errors.Is(err, session.ErrSessionReset)` held, there
was no resume deadline and no dial refusal at the end. The respawned process
was already listening and answered the resume with RESET about 0.3 to 0.4 s
after `mds fail`, while the MON still showed rank 0 failed; the MON assigned
the new GID to rank 0 only later. Both pending calls then reported
`ceph: request outcome unknown: <the same chain>`; the replayed call's `Wait`
still returned its unsafe reply.

Reconnect state (both runs): ten caps, all cap ID 1, realm 1, and one realm
`{ino 1, seq 1, parent 0}`:

| Inode | Short path | Wanted | Issued |
| --- | --- | --- | --- |
| 1 | (1, "") | `-` | `pAsLsXs` |
| `0x10000000000` | (1, `ceph-msgr-cephfs-<token>-recovery`) | `-` | `pAsLsXs` |
| `0x10000000001` to `0x10000000006` | (dir, `flush-*`) | `-` | `pAsLsXsFs` |
| `0x10000000007` | (dir, `held`) | `pFscr` | `pAsLsXsFsxcrwb` |
| `0x10000000008` | (dir, `replayed`) | `pAsxXsxFsxcrwb` | `pAsxLsXsxFsxcrwb` |

`held` was issued `pAsLsXsFsxcrwb` by the read OPEN (the only client of the
file); `replayed` keeps the caps and wanted mask of its unsafe create reply.

Resubmission and replay outcome (both runs):

| Call | Sent as | MDS handling (op tracker of the new incarnation) | Replies on the reconnected session |
| --- | --- | --- | --- |
| TID 10 `replayed` (had an unsafe reply) | `RETRY=1 REPLAY`, target ino `0x10000000008` | `client_request(client.<gid>:10 create ... RETRY=1 REPLAY QUEUED_FOR_REPLAY ...)`, result 0, events `initiated`, `header_read`, `throttled`, `all_read`, `dispatched`, `acquired locks`, `submit entry: journal_and_reply`, `journal_committed`, `replying`, `finishing request`, `cleaned up request`, `done` (no `early_replied`); 1.36 / 1.66 s from reconnect to completion | one safe reply, result 0, no trace, extra `0800000000010000` (ino `0x10000000008`, equal to the unsafe reply's) |
| TID 11 `unanswered` (never sent to any MDS) | `RETRY=1`, no REPLAY | `client_request(client.<gid>:11 create ... RETRY=1 ...)`, result 0, same events plus `early_replied`, held until the MDS was active, 2.37 / 2.67 s | unsafe reply with dentry and target trace (caps `pAsxLsXsxFsxcrwb`), extra ino `0x10000000009`, safe reply 3.5 / 1.6 ms after `FlushJournal` |

The new incarnation's op tracker held an MDRequest for the replayed TID, so
it executed the create again with the preset inode during client replay; it
did not answer from a completed-request record. The `replayed` create had
not been journaled before the old daemon was removed (its unsafe reply came
3 ms after a journal tick and the daemon was gone about 0.3 s later). The
completed-request path of a REPLAY was not exercised.

Native state after adoption: `replayed` exists once at ino
`1099511627784`, `unanswered` at `1099511627785`, both mode `0100644`, nlink
1; the directory lists exactly the ten names with the Go-reported inos. The
Go session is `open` with the same GlobalID and address and `num_caps` 11.
The old incarnation's address was blocklisted by the MON until the next day
(`172.19.0.10:6800/2177714133` until `2026-10-16T18:11:38.064682Z`,
`[fd62:93fe:2c9e::a]:6800/4192204523` until `2026-10-16T18:15:06.459616Z`).

Arrival order on the reconnected session (both runs): OPEN 1, an
MClientSnap UPDATE of the global snap realm (split 3, 70-byte trace) 3, a
GRANT on `replayed` (seq 1, `pAsxLsXsxFsxcrwb`) 4, the replayed create's safe
reply 5, the `unanswered` unsafe reply 7, a GRANT on `unanswered` 8, its safe
reply 9. Numbers 2 and 6 were session messages that produce no event (by
elimination, RENEWCAPS acknowledgements). The GRANT for the replayed inode arrived before the reply of the
request that re-created it. After the MDS became active it revoked `Fsxrw`
from `replayed` and `unanswered`, `Fx` from the directory and `Fsxrw` then
`Fb` from `held` although the open read handle still wanted `pFscr`; the
agent acknowledged each with the retained caps and closed the handle at the
end of the leaf.

### Cleanup verification

Both `mds-cephfs-cleanup.json` receipts report `completed: true`: native
mutations `native-create-write`, `native-create` and `native-write` applied,
the eviction `evicted`, the restart `adopted`; no residual session of the
three identities; both owned directories absent natively; two new blocklist
entries, each attributed and removed with `osd blocklist rm` and absent from
the final listing (`mds-restart` for the old incarnation's address,
`eviction` for the fenced client's address); nothing kept; three identities
deleted. Both `mds-fresh-cleanup.json` receipts report `cephfs_verified`,
filesystem, pools and fresh identity removed and `completed: true`. All nine
owned containers and the fixture network of each run were confirmed absent
with `docker inspect`.

| SHA-256 | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| `mds-cephfs-recovery-results.json` | `ebcd74edbfc1d64f24ad263c8a505d795ceb636796821915e402435c4ac9237c` | `d0218416e3c17dfd14a24f4e3569c598c8f5aa9b044a1b507db3668deb49250b` |
| `mds-cephfs-results.json` | `e1acff48289244ca43b194d19888606e991fb64cd56d65e738f21ad8ff7d8dbd` | `613e888738b981c4ab269347c1c4688c2db839997cdaa819f311ad28a5d48a56` |
| `mds-cephfs-owned.json` | `30e83b06ab778b13fbbc9c654be525172f20487ebeae2a40713a5901df0b27f8` | `6fd11a88f5f66b635f132c0ec5e634697d544272d52985d1694712ad8fb027a3` |
| `mds-cephfs-cleanup.json` | `28d6544d5a14ae139edeab56cb6b4475031a9407691b876154237a38920b621f` | `9ef762b2f18a65a81e2c1691006b0c63e27f1a2a7edf5a3ae7e3b5942b017f28` |
| `mds-cephfs-target.json` | `d47c726206e5ae40c7f183d85f20b4a284af84cd2e065d6ceee2a3fc50b141cf` | `c2ac1d25fea05a588af6f304f7168422776cd4aa63bb43fdf774db38c7d4e090` |
| `mds-fresh-cleanup.json` | `aa0bbfe4e8fdfe7ade1f3a6d788c6efdc6829c0f66fb4e1f1bb3e6bcb813a5c1` | `4233eb6392fbe90b0e6b421ca483e7d4049088cf877eaa3dcb08b26eb11932dc` |

## Earlier attempts

- Before any test code, a manual `ceph mds fail <fs>:0` on a fixture
  without clients showed the respawn: same daemon name and port, new GID and
  nonce, `up:replay` within 0.8 s, `up:active` within 3.8 s, and the old v2
  address blocklisted. That fixture's cleanup failed, as designed, on the
  unattributed blocklist entry the manual command created.
- The first CRC run of the test (working tree) failed in
  `journal_flush_unsafe_to_safe` with `ceph: client closed`: the restart
  client was created inside the first leaf, so that subtest's cleanup closed
  it. This was a test defect, fixed by creating the client at test scope.
  The fixture cleanup of that run reported the abandoned recovery session,
  skipped the namespace readback and did not report completion.
- Three later working-tree runs (CRC twice, secure once) passed every leaf
  of both tests with the same behavior. The first of them predates the op
  tracker evidence and the exact loss timestamps; the others predate the
  explicit `up:reconnect` assertion and one error-message wording change.
  None is the acceptance record.

## Findings and limits

- No product defect was found. Live behavior matched the contract: RESET
  ends a lossless resume and the session becomes Lost with an unknown
  outcome for pending calls; `ReconnectSession` reattaches the Lost session
  to the new incarnation during `up:reconnect` with OPEN and a new
  generation; resubmitted calls keep their TIDs, a call with an unsafe reply
  is replayed with REPLAY and its target inode (the MDS kept the ino), a call
  without a reply is sent as a retry and executes as new after the MDS is
  active; an evicted client's shared `ip:0/nonce` is fenced at the OSD with
  -108 while a client with another nonce on the same IP is not;
  `FlushJournal` turns a seconds-long unsafe window into milliseconds.
- `cephmsgr/MDS_CEPHFS_CONTRACT.md` still says, under Limits, that no live
  MDS restart, live fencing or resume RESET has been exercised. That
  statement is now outdated; the product document was left unchanged here.
- A consumer that inserts capabilities only from reply traces must accept a
  push for an inode before the reply that re-creates it: after reconnect the
  MDS granted the replayed inode's cap (Arrival 4) before the replayed
  request's traceless safe reply (Arrival 5). Here the agent already held the
  cap from the unsafe reply before the restart, as a native client does.
- The first part of every resume error chain in these runs is the test
  relay's `io: read/write on closed pipe`; without the relay the transport
  would report the peer's close directly. The redial hold adds 13 to 20 ms
  to the measured fault-to-Lost interval.
- Not exercised: the completed-request path of a REPLAY (a create journaled
  before the failure), a request that reached the old daemon but whose reply
  was lost, reconnect denial (CLOSE) or a missed reconnect window, standby
  takeover and multiple ranks, file locks and cap snaps in the reconnect
  state, non-root realms, dirty or flushing caps across the restart, a
  reconnect split into several messages, a respawn onto a different port (the
  host relay admits it; the port never changed), and an OSD connection-level
  refusal (only -108 occurred).
- These are local selected-configuration results, separate from remote CI.
  Both CephFS CI rows now select `^TestCephMDSCephFS(Recovery)?Integration$`;
  they were not run remotely here. The push acceptance later widened the selector to
  `^TestCephMDSCephFS(Push|Recovery)?Integration$` (see
  [MDS_CEPHFS_PUSH_VALIDATION.md](MDS_CEPHFS_PUSH_VALIDATION.md)).

## Explicit pending head flush before reconnect (2026-10-18)

The restart leaf now retains both RDWR and RD handles on `held`. Once the
old session is Lost and the owned replacement MDS is `up:reconnect`, the
test explicitly stages a dirty FILE_WR UPDATE for a sparse size of 4096.
Its cap sequence, issue sequence and migrate sequence are zero; it keeps
the cap ID and metadata, the root realm's current snap follows and a
caller-registered flush TID. `ReconnectSession` sends the selected unsafe
and unanswered metadata requests, this UPDATE, then MClientReconnect.
The new test agent carries the pending TID before consuming any events.

After OPEN and adoption of the new incarnation, the test requires a
FLUSH_ACK with the exact TID, inode and dirty mask. Independent native
LibCephFS `statx` must then report that inode with size 4096. Both open
handles remain held until these checks finish. The recovery client's
OSD dial count must still be zero: this verifies sparse size metadata,
without object writeback. Existing replay, fencing, native directory and
cleanup checks remain required, followed by the ordinary metadata suite.

This UPDATE is first sent after Lost. It verifies explicit caller-selected
pre-reconnect delivery, not retransmission of a flush sent before the
failure or native early-kick selection policy. UPDATE/FLUSHSNAP ordering,
payload ownership, complete-list validation, frame bounds, cancellation,
transport loss and CLOSE/REJECT uncertainty also have synthetic coverage.
Dirty-data cap-snap recovery, uncertain FLUSHSNAP replay and OSD writeback
across MDS failover remain unverified.

Both configurations passed on Linux amd64 with CGO disabled for the Go
test binaries, Go 1.26.9 and a compatible Docker engine. The fixture provider was the
published `ceph-testcontainers-go v0.3.0`; both used the same pinned
Tentacle image and commit as above. Selector:
`^TestCephMDSCephFS(Push|Recovery)?Integration$`.

| Configuration | UTC controller time | Push / recovery / metadata | Leaves |
| --- | --- | --- | --- |
| IPv4 host, CRC, AES client/service keys, single BlueStore | 03:42:39–03:48:40 | 76.10 / 75.92 / 89.74 s | 22/22 passed |
| IPv6 container, secure, AES256K client/service keys, single MemStore | 03:44:33–03:50:24 | 73.57 / 72.86 / 85.10 s | 22/22 passed |

Each run received FLUSH_ACK for TID 1, inode `0x10000000025`, dirty FILE_WR
4096; native `statx` reported size 4096. The sent UPDATE retained cap ID 1,
oldest TID 1 and snap follows 1. The MDS GID changed 14581→17900 (CRC) and
14551→17870 (secure), with new nonces and incarnation 6→10. Reconnect OPEN
took 1.82 / 1.196 ms. Each run removed all nine owned containers and its
network, including the fixture's namespace, configuration and blocklist
cleanup checks.

[Machine-readable evidence](MDS_PENDING_CAP_FLUSH_LINUX_20261018.json)
records source hashes, public raw evidence hashes, times, the exact ACK and
native stat, daemon identities and cleanup. The tested source was
`source-2` plus the six Go files whose
hashes it lists; only documentation and this result record changed after
the source snapshot. Full root and fixture Go tests, root and fixture vet,
race checks for `cephmsgr` and `internal/mds`, and 422 Python tool tests
passed on the same snapshot. These are local selected-configuration
results, separate from CI at the final commit.

## Journal fault boundary and early-failure reconciliation

The restart leaf used to depend on timing: it created `replayed` right
after a journal tick and fired `ceph mds fail` as soon as the unsafe reply
arrived, expecting the old incarnation to be removed before its next
journal flush. On the IPv4 host CRC row the create sometimes became safe
first and the leaf failed with "the journal tick won the race". The MDS
recent-event dump of one such run showed the create journaled 0.6 ms after
a tick's `EUpdate scatter_writebehind` and a second writebehind about 10 ms
later, before the map removed the daemon. `Locker::scatter_writebehind`
submits an `EUpdate` and calls `mdlog->flush()`; `Locker::scatter_tick`
nudges every scatterlock that has been dirty for
`mds_scatter_nudge_interval` (5 s), and the creates of this leaf dirty the
recovery directory's locks. A safe reply before the restart shows that the
flush happened; no client-side observation gives an interval in which no
flush can happen. The leaf then returned with the restart issued but never
adopted, and the metadata test that follows met the failed incarnation.

### Fault boundary

The boundary is now held on the server, on the armed incarnation only
(source paths at the reference tag):

- `mds/MDLog.cc`: `_submit_thread` appends nothing while `pause`
  (`mds_log_pause`) is set; `submit_entry` only queues the event, and
  `flush` with queued events only queues a flush marker. A request whose
  event is queued after the pause takes effect cannot become safe on that
  incarnation, whichever flush is requested: a client
  `REQUEST_FLUSH_MDLOG`, a scatter writebehind or the tick.
- `mds/Server.cc`: `journal_and_reply` sends the early (unsafe) reply and
  submits the event in the same step, so an unsafe reply shows that the
  request reached journal submission. The `journal_committed` op event is
  marked only by the commit completion (`ServerLogContext::pre_finish`).
- `mds/MDSRank.cc`: `MDSRankDispatcher::handle_conf_change` applies MDLog
  options on the MDSRank finisher, after `config set` has returned; the
  `scrub abort` command is queued to the same finisher and answered from
  it (`ScrubStack::scrub_abort` on an idle single rank aborts nothing).
  `common/Finisher.h` runs one thread, so that answer orders the pause
  before the create.
- `mds/MDSDaemon.cc`: a daemon removed from the map respawns; the runtime
  option does not survive the re-exec.

The leaf creates the fresh directory `boundary` (safe) and the fixture
applies the pause (`restart-pause`: idle `scrub status`, `config set`,
`scrub abort`, `config get` = `true`, bracketed by the identity proof of
the armed GID). The client then creates `boundary/replayed` and requires an
unsafe reply. That reply records the TID, the inode and the incarnation
(GID, nonce, incarnation) that answered. The test then forces the window
that used to be lost: it sends `FlushJournal` (on a running submitter this
makes a create safe within milliseconds, see
[Journal flush](#journal-flush)). Then `restart-inflight` reads
`dump_ops_in_flight` on the armed incarnation, and the create must still be
in flight with `early_replied` and no `journal_committed` event. Only then
does `restart-fire` run (the fixture refuses it otherwise). After the
restart, the new incarnation's op tracker must hold the replayed create as
an MDRequest with ` REPLAY`: a create that the old incarnation had
journaled would have been answered from the session's completed-request
record without an MDRequest. That check is the post-hoc proof that the
boundary held.

The replayed create has its own fresh parent because a create wrlocks its
parent's filelock and nestlock. A scatter nudge on a dirty lock submits a
writebehind and leaves the lock unstable until that entry commits, which
never happens while the journal is paused. `boundary` carries no aged dirty
scatter state, so its locks cannot be held by a writebehind during the
window; propagation to the recovery directory uses `wrlock_try` and does
not block.

### Reconciliation after an early failure

If the restart leaf fails, its cleanup reconciles under a fresh 4-minute
context, and its errors are added to the original failure, never
substituted for it (`Reconcile.FailedAfterPhase` names the last milestone
reached):

- a restart that was never fired: `restart-resume` lifts an applied pause;
- a fired restart: the old connection is closed, `restart-status` is read
  until the same daemon name holds rank 0 `up:active` with a new GID and
  the armed GID is gone from the map (a client that never reconnects keeps
  the new incarnation in `up:reconnect` until the MDS reconnect timeout),
  then `restart-adopt` proves and adopts it;
- then a session of the current incarnation (the open one, or a fresh
  client of the recovery identity) unlinks `boundary/replayed`, removes
  `boundary`, the recovery files and directory, and the metadata plan's
  `go-file` and directory, accepting ENOENT for entries never created. It
  releases its caps and closes. Native `statx` must report both
  directories absent and `session ls` no owned session.

No blocklist entry is removed here. The fixture cleanup keeps the fencing
entries of an armed incarnation that is still mapped and removes only
entries it can attribute, after this reconciliation. Its step 0 also lifts
a pause that the test could not lift, for example after a control failure.

### Results (2026-11-10)

Tested source: commit `884f5944e0ca3307030b2896df7cc92e5df1daa3` (only
documentation changed afterwards). Linux amd64 host, Docker 29.9.1, Go
1.26.9 for the fixture and test binaries with CGO disabled, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
(`ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`),
the environment of the native scenario job for each catalog row, and the
native result gate (`tools/native_test_results.py`) around `integration/run.sh`.
One fixture ran at a time on the engine.

| Scenario | Push / recovery / metadata | Gate |
| --- | --- | --- |
| `mds-cephfs.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-1c00e7` | 74.20 / 88.02 / 109.85 s, 29 leaves and tests passed | passed |
| `mds-cephfs.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-suite-1c00e7` | 73.47 / 89.44 / 104.53 s, 29 leaves and tests passed | passed |

CRC row boundary evidence: the replayed create (TID 17, ino
`0x10000000029`) was answered unsafe by the armed incarnation (GID 14572,
incarnation 6). `FlushJournal` followed 43 µs after that reply. In the
same run a `FlushJournal` after an unsafe reply made the other creates safe
in 6.2 to 11.2 ms. 2.60 s after the unsafe reply the armed incarnation's op
tracker still held the create with `early_replied` and
`submit entry: journal_and_reply` and no `journal_committed` event. The
adopted incarnation (GID 8412, incarnation 10, `mds_log_pause` false)
executed it as `RETRY=1 REPLAY QUEUED_FOR_REPLAY`. The cleanup receipt
completed with the pause `discarded` and the `mds-restart` and `eviction`
entries removed.

The secure row showed the same: create TID 17 unsafe from GID 14543
(incarnation 6), `FlushJournal` 32 µs later (0.39 to 0.44 ms to safe for
the other creates in that run), still held 2.70 s later, then executed with
REPLAY by GID 18259 (incarnation 9); the cleanup receipt completed.

### Forced mutations (2026-11-10, CRC row, same host and image)

Both mutations were temporary local edits, run once each and reverted
before the tested commit.

- Pause not applied (the fixture recorded `applied` without the
  `config set`): the forced `FlushJournal` made the create safe before the
  op-tracker read 2.70 s later (`in_flight` false, `held` false). The leaf
  failed with "the replayed create is not held uncommitted on the armed
  incarnation after a forced journal flush", the fixture refused nothing
  and the restart was never fired. Reconciliation (phase
  `replayed-unsafe`) lifted the pause record through `restart-resume`,
  removed the namespace through the open session, and found both
  directories and every owned session gone natively. The metadata test then
  passed 13/13 and the cleanup receipt completed. This is the failure the
  old leaf met by timing: without the boundary a flush in the window makes
  the create safe; with it the same flush does not.
- Injected `t.Fatal` right after `restart-fire` returned (phase `fired`,
  fire `applied`): reconciliation closed the old connection and read the
  rank status. The new incarnation stayed `up:reconnect` for about 50 s
  (the client never reconnected) and then became `up:active`; the
  reconciliation adopted it (incarnation 10). A fresh client of the
  recovery identity removed the namespace; the unlink of
  `boundary/replayed` returned ENOENT, as the paused create had never been
  journaled. Both directories and every owned session were absent
  natively, after 65 s in all. The metadata test passed 13/13 against the
  adopted incarnation. The cleanup receipt completed: pause `discarded`,
  `mds-restart`, `eviction` and `owned-session` (the address of the
  session that missed its reconnect window) entries removed, none kept.
  The recovery test kept the injected failure as its result.

### Limits

- arm64 hosts and CI were not run for this change.
- The barrier relies on the MDSRank finisher being a single FIFO thread and
  on `scrub abort` being answered from it on an idle rank 0, as read at the
  reference tag. Another release would need the same reading.
- The reconciliation was exercised for a failure before the fire and right
  after it, not for failures during reconnect or after adoption.
- A fixture control failure stops the control loop, so the test cannot
  reconcile after one; the fixture cleanup then lifts a held pause and
  keeps the fencing of a still-mapped incarnation.
