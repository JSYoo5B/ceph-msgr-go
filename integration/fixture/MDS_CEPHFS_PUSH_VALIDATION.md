# CephFS push acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go,
without CGO, librados, go-ceph, Ceph CLI subprocesses or native runtime
dependencies. The disposable provider, its native CLI, the native LibCephFS
oracles and the owned MDS are development-only. This acceptance covers the
MDS pushes of [the CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md)
that previously had only synthetic coverage: MClientLease REVOKE answered
with `SendLease` RELEASE, MClientSnap and MClientQuota delivered through
`Next` (with `MDSReply.DecodeSnapTrace` for the realm trace), RECALL_STATE
answered with `ReleaseCaps`, and FLUSHMSG answered with `AcknowledgeFlush`,
together with the push sequence and `Arrival` order of all of them. The owned
recipe, its push extension and its cleanup are described in
[MDS_CEPHFS_FIXTURE.md](MDS_CEPHFS_FIXTURE.md).

## Test shape

`TestCephMDSCephFSPushIntegration` (`integration/mds_cephfs_push_test.go`)
uses the profile and gates of the other CephFS acceptances
(`CEPH_MSGR_TEST_MDS_CEPHFS=1`, MDS_FRESH=1, neither MDS_COMMANDS nor
MDS_SESSIONS, `legacy-v6`, one OSD) and the selector
`^TestCephMDSCephFS(Push|Recovery)?Integration$`. Go runs the files in name
order, so it runs first, before the recovery acceptance restarts the owned
MDS; it reads the current target from `mds-cephfs-target.json` and therefore
also works after a restart. It leaves no snapshot, quota, recall setting or
session behind for the two later tests.

Two Go clients authenticate as `client.mds-cephfs-<token>`, each with its own
GlobalID: the Go client and the holder. Both enable only `EnableMDS` and
`EnableCephFS`, dial MDS through counting relays that refuse OSD dials, and
open a session on the explicit `MDSTarget` with metadata `entity_id`,
`hostname` (`ceph-msgr-go-<token8>-push` or `-push-holder`) and `root=/`, a
2 s RenewInterval and 1024 event slots. Each session has its own instance of
the test-only capability agent (`integration/mds_cephfs_agent_test.go`),
which for this acceptance also restates, from the pinned `client/Client.cc`:

- `handle_lease`: every REVOKE is answered with a RELEASE carrying the
  revoke's sequence, mask, inode, first and last snap and dentry name; a hold
  can delay that answer once for one dentry name;
- `handle_snap`: the first realm of a SPLIT trace must be the split realm;
  cached caps listed in `split_inos` move into it unless their realm is
  newer; every realm of the trace is recorded; nothing is sent (no dirty
  data, so no cap snap);
- `handle_quota`: the quota and rstat are stored on the cached inode;
- RECALL_STATE (`trim_caps`): walking caps in the order they were added, it
  releases unopened non-root, non-directory caps until at most `max_caps`
  remain, in one MClientCapRelease with the OSD epoch barrier, at once (the
  native client flushes on its next tick);
- FLUSHMSG: acknowledged after any pending release (none are pending, since
  releases are sent at once), recording the FLUSHMSG sequence against the
  number of capability, lease, snapshot and quota pushes the agent handled
  before it (`got_mds_push`).

Every Go mutation asks for `FlushJournal` after its unsafe reply. Native
actions run as `client.mds-cephfs-push-native-<token>` (MDS `allow rwps`),
and every receipt must prove the unchanged current incarnation.

| Leaf | Checked behavior |
| --- | --- |
| `open_push_sessions` | Both sessions Open with push sequence 0; OPEN advertises bits 7, 9, 18, 19 and 20. Native `session ls`: both `open`, `client_features` `0x00000000001c83ff`, the metadata sent, 0 caps and leases, not blocklisted. The Go client creates the push directory |
| `dentry_lease_revoke_released` | The holder creates `lease` and `lease/victim` (as the directory's only client it becomes its loner and keeps Fsx). The Go client looks up `victim` under `lease`: the reply carries a LeaseStat with a valid mask, a sequence and a duration, and Go's directory cap has no Fs/Fx. The agent holds the release for that dentry for 2 s; the fixture renames `victim` to `renamed` natively. Exactly one REVOKE arrives after the lookup reply: mask 1 (`1\|CEPH_LOCK_DN`), the directory inode, the lookup's lease sequence, last `CEPH_NOSNAP`, dentry `victim`; the RELEASE equals it with action RELEASE and is sent 2 s later. The native rename's own clock covers the hold (at least 1.95 s, at most 12 s, well below the 30 s lease), `renamed` has the victim inode and `victim` is ENOENT; the push sequence grew |
| `snapshot_realm_split_push` | Go creates `snap` and `snap/snap-file`. The fixture creates the snapshot natively (`mkdir snap/.snap/<name>`): its `statx` is a directory with the inode of `snap`. Go receives MClientSnap UPDATE of the global realm (split 3, one realm, ino 3), then SPLIT with split = `snap`, `split_inos` containing `snap` and `snap-file`, no split realms, and a trace whose first realm is `snap` (parent 1, one snapshot) and whose last is the root; the agent moved both caps into the new realm. The holder receives the global UPDATE. Both push sequences grew |
| `quota_push_on_native_setxattr` | Go creates `quota` and `quota/quota-file`. The fixture sets `ceph.quota.max_bytes` natively and reads back the same decimal. Go receives MClientQuota for `quota` with exactly that MaxBytes, MaxFiles 0, RSubdirs at least 1, applied to the cached directory, then a SPLIT creating the quota realm (`quota`, parent 1, no snapshots, `split_inos` with `quota` and `quota-file`) after the quota push |
| `recall_state_released_to_target` | Go creates `recall` and 24 files, so it holds more caps than 12 (native `num_caps` checked; the holder holds at most 8). The fixture sets `mds_min_caps_per_client` 8 and `mds_max_caps_per_client` 12 on the owned daemon and returns once the daemon reports both. RECALL_STATE arrives with `max_caps` 8; the agent releases down to 8; native `num_caps` drops to at most 12. The maximum is then restored |
| `flushmsg_acknowledged_after_cache_drop` | Go reads `recall` again (READDIR, 24 entries), so it again holds more than 8 caps. The fixture runs `cache drop 10`. RECALL_STATE (`max_caps` 8) and then FLUSHMSG arrive; the FLUSHMSG sequence equals the pushes the agent counted before it; the acknowledgement follows the release. `cache drop` reports client recall 0 with a positive count and journal flush 0, and finishes in less than its 1 s gather timer, which only the acknowledgement could end. Native `num_caps` is at most 8. The minimum is restored and no central entry remains |
| `cleanup_snapshot_quota_namespace_release_close` | Native snapshot removal (ENOENT afterwards): Go receives MClientSnap DESTROY of the global realm (split 3) and a SPLIT of `snap` with no inodes whose realm has no snapshot. Native quota clear (readback absent): Go receives MClientQuota with MaxBytes 0. Go unlinks the 27 files and removes the five directories (safe result 0); native `statx` of the push directory is ENOENT. Each client releases every cap (native `num_caps` to 0), closes with a confirmed CLOSE and is absent natively; each agent ends with `ErrMDSSessionClosed`, no error, no deferred push, and counted exactly as many pushes as the session's `PushSequence`. One MDS dial and no OSD dial per client |

## Results

Both selected configurations passed on 2026-10-16 (KST), all three tests and
all leaves, no skips. The tested source is HEAD
`c3bc350dfd31f828d9597f83c2ba0d8ed42b1b2c` plus only this change set,
exported to a separate directory with `git archive` (modified and new files
copied over). This record is the only file of the change set written after
the runs. The SHA-256 over the sorted per-file SHA-256 list of the export is
`b04363277d4117d034eb570a095cde5351a6eee0f1df897c92ea5fb5e03b13c2`. Source
SHA-256:

| File | SHA-256 |
| --- | --- |
| `integration/mds_cephfs_push_test.go` | `b81f5e3fb3385c37c0ce6338183e0d07643a8c2699354af8655ae86f245c8ebb` |
| `integration/mds_cephfs_agent_test.go` | `2d9910b994406a321c187d2025193e690f406e55fc0e49ef9bbc78698421b84a` |
| `integration/mds_cephfs_test.go` | `01e4c4acbd36721efa710f00a160607918bec64e2e67a48f264fe1405dc142c7` |
| `integration/mds_cephfs_recovery_test.go` (unchanged) | `3611d0de450b98ad27c8ebe62d9f773376ff68894afd6527aa891382365b1e5b` |
| `integration/fixture/mds_cephfs_push.go` | `96e2730da2ccf0aee6a5036bbfe88a0a5453a5bd955a3361ce2dfa7fbad416c1` |
| `integration/fixture/mds_cephfs.go` | `f150c626df9110fb4f5f6fe537d49b3ee311ed3a1e583306d31d2d081eb50c07` |
| `integration/fixture/mds_cephfs_recovery.go` (unchanged) | `98f6ead7a41f02c84c30f6e6919aadff86c2903eb9c8628148aa0628bae110e2` |
| `integration/fixture/profile.go` | `59676a3caeda93aeb8a763365086cd9d192a8e59adcbb42e08c5ae3d604e75f0` |

Image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
(arm64), daemon version
`ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`.

| Configuration | Controller wall time (UTC, 2026-10-15) | Push test | Recovery test | Metadata test | Result |
| --- | --- | --- | --- | --- | --- |
| Darwin arm64 host runtime, IPv4 relay, CRC, AES client and service keys, single BlueStore OSD | 19:08:39 to 19:13:24 (4 min 45 s) | 56.06 s | 57.60 s | 75.54 s | 7/7, 5/5 and 9/9 passed |
| CGO=0 Linux arm64 container runtime, IPv6, secure, AES256K client and service keys, single MemStore OSD | 19:13:32 to 19:18:17 (4 min 45 s) | 58.12 s | 56.01 s | 75.80 s | 7/7, 5/5 and 9/9 passed |

Push leaf durations (CRC / secure, seconds): open 4.23 / 4.51, lease
4.01 / 4.10, snapshot 3.01 / 4.10, quota 1.91 / 3.10, recall 12.95 / 13.12,
FLUSHMSG 8.92 / 9.22, cleanup 21.01 / 19.94. Each native push action took
1.9 to 4.0 s (mount, operation, shutdown and the two identity checks); the
rename took 3.9 s including its 2 s wait. The 66 Go requests of each run
include 62 mutations, all answered unsafe and made safe by `FlushJournal`
0.8 to 3.6 ms (CRC) and 0.2 to 0.7 ms (secure) after the unsafe reply.

Both runs observed the same values unless noted. Inodes: push directory
`0x10000000000`; `lease` `0x100000003e9` and `victim` `0x100000003ea` (the
holder session's preallocated range); `snap` `0x10000000001`, `snap-file`
`0x10000000002`, `quota` `0x10000000003`, `quota-file` `0x10000000004`,
`recall` `0x10000000005`, `recall-00..23` `0x10000000006` to
`0x1000000001d`.

### Dentry lease revoke

The holder's MKDIR left it `pAsLsXsFsx` on `lease` (loner, Fsx); the Go
client's lookup gave it `pAsLsXs` on the directory and LeaseStat mask 7
(`CEPH_LEASE_VALID` 3 plus `PRIMARY_LINK` 4), duration 30000 ms, sequence 3,
no alternate name (Arrival 7). The sequence is the session's third: each new
dentry lease takes `++session->lease_seq` and the issue adds one, and the Go
client's MKDIR of the push directory had already received a lease (sequence
2, below).

| Value | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| REVOKE (Arrival 8, the Go session's second push) | action 1, mask 1, ino `0x100000003e9`, first 2, last `0xfffffffffffffffe`, seq 3, dentry `victim` | same |
| RELEASE sent | 2002 ms after the REVOKE, identical fields with action 2 | 2000 ms |
| REVOKE after the rename request was written | 850 ms (the driver's mount) | 915 ms |
| Native `fs.rename` duration | 2.007 s | 2.004 s |
| Native result | `renamed` ino `0x100000003ea`, mode `0100644`; `victim` ENOENT | same |

The rename therefore waited for the RELEASE and not for the 30 s lease
expiry (`Locker::handle_client_lease` removes the lease only when the
sequence matches). The holder answered REVOKEs on `victim` while the Go
lookup took its read locks (down to `pAsLsXsFc`) and, during the rename, on
`lease` (Fx, then Fs) and on `victim` (to `pAsXs`); the Go client received
one REVOKE on the victim inode (to `pAsXs`).

Leases also arrive where this acceptance did not look for them: every MKDIR
reply of the Go client carried a dentry lease for the new name (mask 7,
sequences 2, 4, 5 and 6), because the parent's filelock is locked during
MKDIR. When the Go client later removed `quota` and `recall` itself, the MDS
revoked its own leases on those names (sequences 5 and 6 on CRC, 6 only on
secure: leases older than 30 s had already expired) and the RMDIR proceeded
after the agent's immediate RELEASE.

### Snapshot realm

Native snapshot creation (`mkdir`) took 3.0 / 1.8 ms; `statx` of
`snap/.snap/ceph-msgr-snap-<token8>` returned a directory with the inode of
`snap`. The Go client received, in order (CRC Arrival 18 and 19, secure 19
and 20; the holder received the first at Arrival 18 in both runs):

- UPDATE (op 0), split 3, 78-byte trace: the global realm `{ino 3, seq 2,
  snaps [2]}`;
- SPLIT (op 3), split `0x10000000001`, `split_inos` [`0x10000000001`,
  `0x10000000002`], no split realms, 148-byte trace: realm
  `{ino 0x10000000001, created 2, parent 1, parent_since 2, seq 2, snaps [2],
  change_attr 1}` then the root `{ino 1, created 0, parent 0, parent_since 1,
  seq 1}`. The agent moved both caps from realm 1 to the new realm.

Removal (cleanup, `rmdir`): DESTROY (op 2) split 3 with the global realm at
seq 3 and no snapshots, then SPLIT of `0x10000000001` with no inodes and the
realm at seq 3, no snapshots, change_attr 2.

### Quota

The quota is token-bound: 23343104 bytes (CRC) and 218468352 bytes (secure);
the native readback returned the same decimal. Go received, 0.7 / 0.25 ms
apart:

- MClientQuota (CRC Arrival 29, secure 30): ino `0x10000000003`, MaxBytes the
  set value, MaxFiles 0, RBytes 0, RFiles 1, RSubdirs 1 and an rctime;
- SPLIT (one Arrival later) creating the quota realm: `{ino 0x10000000003,
  created 1, parent 1, parent_since 3, seq 1}` then the root, `split_inos`
  [`0x10000000003`, `0x10000000004`].

`Server::handle_set_vxattr` broadcasts the quota while it projects the
change and the realm split follows the journal commit, hence the order.
Clearing (cleanup) produced one MClientQuota with MaxBytes 0 and the same
rstat; the native readback then reported the vxattr absent (ENODATA).

### RECALL_STATE

| Value | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| Go caps before (native `num_caps` / agent) | 33 / 33 | 33 / 33 |
| Recall settings applied (daemon readback) | min 8, max 12, two central entries on `mds.<daemon>` | same |
| RECALL_STATE after the settings request was written | 2.886 s (0.716 s before the fixture returned) | 2.836 s (0.865 s before) |
| RECALL_STATE | Arrival 111, `max_caps` 8, `max_leases` 0 | Arrival 113, same |
| Released | 25 caps in 176 µs: `victim`, `snap-file`, `quota-file`, `recall-00..21` | 25 caps in 73 µs |
| Native `num_caps` | 33 to 8 at the next observation | same |
| RECALL_STATE messages in the leaf | 1 | 1 |

The recall asks for `mds_min_caps_per_client`, not the maximum
(`Server::recall_client_state`), and the upkeep thread recalled only once
because the release arrived within its 1 s period. Restoring the maximum
left the minimum at 8 and one central entry.

### FLUSHMSG after `cache drop`

| Value | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| Go caps after READDIR (native / agent) | 30 / 30 | 30 / 30 |
| RECALL_STATE | Arrival 118, `max_caps` 8, 22 released in 0.14 ms | Arrival 121, 22 released in 0.04 ms |
| FLUSHMSG | Arrival 119, sequence 39, agent-counted pushes 39, acknowledged 0.11 ms after receipt | Arrival 122, sequence 40, counted 40, 0.002 ms |
| `cache drop` | client recall 0 `(0) Success`, recalled 22, journal flush 0, 25 dentries trimmed, duration 8.9 ms | recalled 22, 25 trimmed, 2.6 ms |
| Native `num_caps` after | 8 | 8 |

`Server::flush_session` sends FLUSHMSG with the session's push sequence
(`Session::wait_for_flush`), so equality with the agent's count shows the
library and the MDS counted the same pushes. `C_Drop_Cache` waits for the
flush gather with a 1 s timer before it recalls again; the gather completed
within milliseconds, which only `finish_flush_session` on our FLUSHMSG_ACK
explains here. Restoring the minimum returned the daemon to 100 and 1048576
with no central entry.

### Push sequence and arrival order

Push sequence after each leaf (Go client / holder): open 0 / 0, lease 3 / 8,
snapshot 8 / 9 (CRC) and 9 / 9 (secure), quota 13 / 9 and 14 / 9, recall
38 / 9 and 39 / 9, FLUSHMSG 39 / 9 and 40 / 9; at the end 63 / 15 on both,
equal to the pushes each agent counted. The secure Go session received one
more capability push before the snapshot (a REVOKE on `snap-file` right after
its create), which shifts later Arrival and sequence numbers by one. Within
each leaf the recorded order was: lookup reply, lease REVOKE, then a cap
REVOKE on the victim; global UPDATE, SPLIT, then a cap REVOKE on `snap`;
MClientQuota, SPLIT, then a cap REVOKE on `quota`; RECALL_STATE immediately
followed by FLUSHMSG.

### Cleanup verification

Both `mds-cephfs-cleanup.json` receipts report `completed: true`: push
mutations `push-rename`, `push-snapshot-create`, `push-snapshot-remove`,
`push-quota-set`, `push-quota-clear` and `push-cache-drop` applied,
`push-config-min` and `push-config-max` restored, recall settings at 100 and
1048576 with no central entry (`push_config_restored`), the other tests'
mutations as in their records, no residual session of the four identities,
all three owned directories absent natively, the two new blocklist entries
(the recovery test's eviction and the old MDS incarnation) attributed,
removed and absent, nothing kept, four identities deleted. Both
`mds-fresh-cleanup.json` receipts report `cephfs_verified`, filesystem, pools
and fresh identity removed and `completed: true`. All nine owned containers
and the fixture network of each run were confirmed absent with
`docker inspect`.

| SHA-256 | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| `mds-cephfs-push-results.json` | `9952e3aa0bce56718bf8999bf5be57e92ba44b86ee2b8c9c8baa2c478fc8d9f8` | `4e7798d3caec7cf76458b26d4fef53207150e7239c327d24565a37b31af3f1b1` |
| `mds-cephfs-recovery-results.json` | `2e1acb31a4242ba75900ed571cb6529e2e5f0f53d6bb4772675947b396154f26` | `da5357e8417bb7995e17361414f2f721fb67c3dc67578672e096cde3f5e4cda7` |
| `mds-cephfs-results.json` | `7bb4dd3063f8ba250a7adc483755042aaad01e751a87dfdef3e574deec439e95` | `91159d123147c2e35f3876a5aca32aaa89697da4d7a52ab937e56be4434c30c3` |
| `mds-cephfs-owned.json` | `d0e9d9096482584211ea57767eeb956879860505624593b947c99f16631ee3ea` | `2d85b6239ca7cde73b7ac60c8570f23cfb7b4c7899931c1985581ae76ce45011` |
| `mds-cephfs-cleanup.json` | `573f1e904c2afbe93fbba011b2ea57c8c5928e52021624faf36d6a317ff1b56e` | `62edf0cb2e19a2ac0170bad728c2bb1dcc17157ed91564956dbe9afcaba367e5` |
| `mds-cephfs-target.json` | `6bd7fef48561ecf5fc25314fac54c8496c934121ddd55185376c38f45072419b` | `3364e74516ab1e9022574f6a586bfe4d4c334d1b95e51fbe6abcda5f651767d4` |
| `mds-fresh-cleanup.json` | `e24565eefb9a47bb91ca482d029b297c6b8c528d81f92f7e75a262719948f022` | `bce0844042ef22f45aa9e697d1e2ef257c1603dccd349a179fc092376a74bfe2` |

Public evidence roots and logs are under
`/private/tmp/claude-501/-Users-jsyoo5b-Workspace-ceph-msgr-go/41987d68-a044-4af9-869c-6a98932340c0/scratchpad/`:
`pdiag-acrc/` with `prun-acrc.log`, and `pdiag-asec/` with `prun-asec.log`
(export in `pexport1/`).

## Earlier attempts

One development run (CRC, working tree, `pdiag-dcrc1/`) passed all three
tests with the same push behavior. It predates two test edits: the RECALL_STATE
delay is now measured from the request as well as from the fixture's return
(the recall precedes that return), and the `cache drop` duration below its
1 s gather timer became an assertion. It is not the acceptance record. No
run failed.

## Findings and limits

- No product defect was found. Every push decoded and was counted exactly
  as the MDS counts it: the FLUSHMSG sequence equalled the pushes received
  before it, and `PushSequence` equalled the agent's count at the end. The
  MDS accepted the library's MClientLease RELEASE, MClientCapRelease and
  FLUSHMSG_ACK, each shown by a native effect (the rename completing after
  the held release, `num_caps` dropping, the flush gather ending).
- The library offers no standalone decoder for the snap trace of a push:
  `MDSSnap.Trace` (and `MDSCaps.SnapTrace`) decode only through
  `MDSReply{SnapTrace: trace}.DecodeSnapTrace()`. It works, but a consumer
  must know to borrow the reply type.
- Consumers should expect dentry leases on their own MKDIR replies and
  REVOKEs of their own leases caused by their own RMDIR/UNLINK; the MDS
  waits for that RELEASE (or the 30 s expiry) before completing the request.
- `cephmsgr/MDS_CEPHFS_CONTRACT.md` still lists lease, FLUSHMSG,
  RECALL_STATE, snap and quota pushes as not exercised live; that statement
  is now outdated. The product document was left unchanged here.
- Not exercised: FORCE_RO (reachable on a single rank only through the
  `force_readonly` admin command or a metadata write failure; the rank then
  stays read-only until it restarts, which would break the two later tests
  sharing this fixture), lease RENEW in either direction, leases on remote
  or null dentries, MDS-initiated lease expiry, cap snaps and FLUSHSNAP
  (no dirty data), SPLIT with child realms, snapshots of nested realms and
  renames across realms, `max_files` quotas and quota pushes caused by rstat
  changes near the limit, RECALL_STATE `max_leases`, recall throttling and
  the steady-state skip, trimming of directory caps and dentries (the agent
  trims only unopened files), FLUSHMSG from export or migration, and
  multiple ranks.
- These are local selected-configuration results, separate from remote CI.
  Both CephFS CI rows now select
  `^TestCephMDSCephFS(Push|Recovery)?Integration$`; they were not run
  remotely here.
