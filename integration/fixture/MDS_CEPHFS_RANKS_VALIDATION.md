# Multi-rank CephFS acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go,
without CGO, librados, go-ceph, Ceph CLI subprocesses or native runtime
dependencies. The disposable provider, its native CLI, the native LibCephFS
oracle and the owned MDS daemons are development-only. This acceptance
covers the parts of [the CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md)
that need more than one active rank: one client's CephFS sessions with two
ranks through explicit per-rank `MDSTarget`s, a request forward returned as
`*MDSRequestForwardedError` and resubmitted by the caller, the MClientCaps
EXPORT and IMPORT pushes and the exporter's FLUSHMSG of a subtree migration,
and `ReconnectSession` to a standby daemon that took over rank 0. The owned
recipe, its controls and its cleanup are described in
[MDS_CEPHFS_RANKS_FIXTURE.md](MDS_CEPHFS_RANKS_FIXTURE.md).

## Test shape

`TestCephMDSCephFSRanksIntegration` (`integration/mds_cephfs_ranks_test.go`)
runs only with `CEPH_MSGR_TEST_MDS_CEPHFS_RANKS=1`, MDS_FRESH=1, none of
MDS_COMMANDS, MDS_SESSIONS or MDS_CEPHFS, and `legacy-v6`; the selector is
`^TestCephMDSCephFSRanksIntegration$`. One Go client authenticates as
`client.mds-cephfs-ranks-<token>` (MON `allow r`, MDS
`allow rw fsname=<owned fs>`) with `EnableMDS` and `EnableCephFS`,
`MaxMDSConnections` 2 and OSD dials refused. Its MDS dials pass through
counted stream relays with a dial log (the recovery acceptance's dialer,
without its hold). Both sessions send `entity_id`, `hostname`
(`ceph-msgr-go-<token8>-ranks`) and `root=/`, with a 2 s RenewInterval and
1024 event slots. Requests use caller and owner UID/GID 0 and the MDSMap
epoch of the latest native receipt; every mutation asks for `FlushJournal`
after an unsafe first reply.

The test-only capability agent (`integration/mds_cephfs_agent_test.go`)
runs once per session. For this acceptance the agents of the two ranks
share one lock, as the native client handles every MDS message under
`client_lock`, and restate from the pinned `client/Client.cc`:

- `handle_cap_export`: an EXPORT whose cap ID matches this rank's cap moves
  the cap to the peer rank (add it there with the peer cap ID,
  `issue_seq - 1` as sequence and the peer's migrate sequence, or only raise
  an already installed peer cap whose sequence is older) and removes this
  rank's cap without a release; an EXPORT for a cap that is already gone is
  ignored;
- `handle_cap_import`: the IMPORT's snap trace is recorded, its cap is
  installed (`add_update_cap`) on this rank, the peer rank's cap named by the
  peer cap ID is removed (released only when the peer flags carry
  CEPH_CAP_FLAG_RELEASE), and the IMPORT is then handled as a GRANT, as
  `handle_caps` does.

The inode is the client's one cache: a rank learns an inode's attributes,
dentry and open state from the other rank's entry. Reconnect state and the
continuation after reconnect are those of the recovery acceptance
(`send_reconnect`, sequences reset). A forward is answered as
`Client::handle_client_request_forward` decides: a new request to the
destination rank with the forward count the MDS reported (the product gives
it a new TID).

| Leaf | Checked behavior |
| --- | --- |
| `open_sessions_on_two_ranks` | `OpenMDS` with the rank-0 and rank-1 addresses from the native map and one `OpenSession` each: both Open, the client's GlobalID, the explicit target, push sequence 0, OPEN advertising bits 7, 9, 12 (MULTI_RECONNECT) and 18 to 20, distinct generations. Native `session ls` on each rank: `open`, the identity, `client_features` `0x00000000001c83ff`, the metadata sent, 0 caps and leases, and the same address `<ip>:0/<CephFSNonce>` on both ranks |
| `forward_to_pinned_rank_and_resubmit` | Go creates the owned directory, `fwd` and `fwd/target` through rank 0. The fixture pins `fwd` to rank 1 (fresh-mount readback 1 by a mount other than the setting one) and waits until both ranks agree that rank 1 is its single authority. The migration of `target`'s cap passes the leaf-3 checks. A LOOKUP of `target` in `fwd` sent to rank 0 with getattr mask `pAsLsXsFs` plus CEPH_STAT_RSTAT (which requires the authority) ends with `MDSRequestForwardedError{DestinationMDS: 1, Forward: 1, MustResend: true}` for its own TID, and the rank-0 session stays Open. The caller sends it again to rank 1 with `Forward` 1 and a new TID: a safe result 0 with `target`'s inode, dentry and directory. Native `forward` counters: rank 0 +1, rank 1 unchanged. Native `statx` equals the reply's inode, mode, owner, nlink and size |
| `cap_export_import_on_migration` | Go creates `mig` and `mig/file-0..2` through rank 0 and holds their caps there. The fixture pins `mig` to 0 first, and both ranks report it as a rank-0 subtree (auxiliary subtree, `export_pin` 0); then pins it to 1 and waits for convergence on rank 1. Per moved cap: exactly one EXPORT on the rank-0 session naming rank 1's new cap as its peer and one IMPORT on the rank-1 session naming the rank-0 cap as its peer; the agents end with the cap on rank 1 only. FLUSHMSG reached the exporting session before every EXPORT, its sequence equal to the pushes counted there, and was acknowledged. Native counters: rank 0 `exported` +1 and rank 1 `imported` +1, equal `exported_inodes` and `imported_inodes` deltas of at least the moved caps. Native `num_caps`: rank 0 down and rank 1 up by the moved caps. A GETATTR of `file-0` on rank 1 reports the imported cap ID |
| `standby_takeover_reconnect` | The fixture arms the takeover (current rank-0 daemon, the standby, rank 1) and runs `ceph mds fail <rank-0 GID>`. The rank-0 session becomes Lost after Messenger resume; the test closes that connection, waits until the standby holds rank 0 in `up:reconnect`, opens the standby's address and calls `ReconnectSession` with the rank-0 agent's caps and realms: OPEN, a new generation reconnected from the Lost one, no resubmitted calls. After rank 0 is `up:active` the fixture adopts the new topology: the standby in rank 0 with its own GID and a new incarnation, rank 1 unchanged, the failed GID gone and blocklisted, the failed daemon back as the standby with a new GID. Native `session ls` on the new rank-0 daemon: the session `open` with as many caps as were reconnected, same address. Rank 1: native caps unchanged, session still Open in its generation with no resume and renewals continuing. A GETATTR on the new rank 0 succeeds |
| `cleanup_restore_pins_unlink_release_close` | The fixture restores both pins to their baseline -1 (readback by another mount). Go unlinks the four files through rank 1 and removes `fwd`, `mig` and the owned directory through rank 0 (results 0, no forward). Native `statx` of the owned directory is ENOENT. Each session releases every cap: native `num_caps` reaches 0 on both ranks. Both sessions close with a confirmed CLOSE and are absent natively; each agent ends with `ErrMDSSessionClosed`, no error and no deferred push. No OSD dial |

Every native receipt proves the topology current at that point: before the
takeover the startup daemons of both ranks and the standby (name, GID,
incarnation, endpoint, nonce), after it the adopted ones; a receipt that
names any other daemon fails the test.

## Results

Both selected configurations passed on 2026-10-16 (KST), all five subtests,
no skips. The tested source is HEAD
`source-1` plus only this change set,
exported to a separate directory with `git archive` (modified and new files
copied over). This record is the only file of the change set written after
the runs. The export holds 1184 files; the SHA-256 of its sorted per-file
SHA-256 list (`find . -type f -print0 | LC_ALL=C sort -z | xargs -0 shasum
-a 256`, run in the export root) is
`23fc72f496e9aa2d7dfd5e0d7d7d2be31e2d7e6e126a2b2f2242f5f905c48042`, checked
again before the second run. Source SHA-256:

| File | SHA-256 |
| --- | --- |
| `integration/mds_cephfs_ranks_test.go` | `01a6e1586fa92bf1c9b08bd61bd8e58a40e0e65d826b79d2d5020e23db9ade5e` |
| `integration/mds_cephfs_agent_test.go` | `357eb3c7c5ea5e2f0c45a31b7f0966708a911237b32e9277eb3f65f3c525ec03` |
| `integration/fixture/mds_cephfs_ranks.go` | `9ff7657988c161f7f959e6a0fe2236701b4cf047b8b4b2328752a722f6e233b4` |
| `integration/fixture/mds_cephfs_ranks_test.go` | `2597be53a8b0d8dfe5fec1e839d0bbd9c09c58734bcf37e175d4c46c37c24155` |
| `integration/fixture/mds_fresh.go` | `186de2826f672185c1c5f78f0a05f0258ddd033853bd8499d860067e0ee861c4` |
| `integration/fixture/profile.go` | `980e62d9cf7bed0e86925fdc63176941d9f95de635f96541a8debc26e1ce510e` |
| `integration/fixture/profile_test.go` | `6292e1011ffc3977908397d2fc3e0887b82baca06dc7e06f346f0839cd5d59ea` |
| `integration/fixture/provider.go` | `e0dee166bdc1e0f2e254d12f98dde05582da71ad6edacbb79f8b0f34ff54e0a6` |
| `integration/fixture/control.go` | `593d8f131134ea17a5e254e3e92eeefd854197c1945e9c2f975061d998623e85` |
| `integration/fixture/main.go` | `2a75e4891360d2489db2ce87e66dd7faf7d4fe29caa473459d811047e207fbb4` |
| `integration/fixture/diagnostics.go` | `f1922b0c5f4b2a3e6cba1be02a214766db377182e94e44ccf7e24e0038db1c35` |
| `integration/fixture/MDS_CEPHFS_RANKS_FIXTURE.md` | `dc8f4c28ead59b4e621b922a9824dad2d62596a3f2f05b8f77548ff5a0eb3501` |
| `integration/fixture/MDS_CEPHFS_FIXTURE.md` | `a782a5a4bd083d7336514b5356cf1cf5a925aade1b6f1a1be7a0856cf91d1c41` |
| `integration/fixture/MDS_CEPHFS_VALIDATION.md` | `714d602f51b00733d79a7c0067cee3d020dfb0339db9ad5c2882129842efae19` |
| `.github/workflows/ci.yml` | `46be7ffcea9f829b71d9607c48f2f6dd6b20cb4d4ff9b2bdb25d1cc5c9fedd8c` |
| `tools/test_ci_osd_profiles.py` | `dda99b0a9008ec0703dd28865d2703b1768f0c612515a47cae464cf1958007f0` |

Image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
(arm64, Docker Desktop), daemon version
`ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`,
provider `github.com/JSYoo5B/ceph-testcontainers-go` v0.2.0. Topology: three
MON, two MGR, one OSD, the owned filesystem with two active MDS ranks and one
standby (three owned MDS containers), and the native oracle container.

| Configuration | Controller wall time (UTC, 2026-10-15) | Test | Result |
| --- | --- | --- | --- |
| Darwin arm64 host runtime, IPv4 relay, CRC, AES client and service keys, single BlueStore OSD | 19:58:37 to 20:00:49 (2 min 12 s) | 42.22 s | 5/5 passed |
| CGO=0 Linux arm64 container runtime, IPv6, secure, AES256K client and service keys, single MemStore OSD | 20:00:59 to 20:03:08 (2 min 9 s) | 42.17 s | 5/5 passed |

Leaf durations (CRC / secure, seconds): open 1.52 / 1.41, forward 12.54 /
13.53, migration 10.93 / 11.12, takeover 9.36 / 9.38, cleanup 7.85 / 6.72.
Each native action took 0.2 to 1.8 s except the placement waits (2.3 to 5.2
s, including the convergence polls). Each run made 32 native requests.

Both runs observed the same values unless noted. The provider assigned the
ranks independently of the daemon names: in both runs daemon `-1` started in
rank 0 and daemon `-0` in rank 1, with daemon `-2` as the standby.

| Startup | CRC/AES IPv4 | secure/AES256K IPv6 |
| --- | --- | --- |
| rank 0 | `-1`, GID 4671, incarnation 6, `172.19.0.11:6800/193353784` | `-1`, GID 4668, incarnation 6, `[fd62:93fe:2c9e::b]:6800/3408828841` |
| rank 1 | `-0`, GID 14507, incarnation 8, `172.19.0.10:6800/239341542` | `-0`, GID 14504, incarnation 8, `[fd62:93fe:2c9e::a]:6800/2494894726` |
| standby | `-2`, GID 14524, `172.19.0.12:6800/2810001375` | `-2`, GID 14530, `[fd62:93fe:2c9e::c]:6800/1306071156` |
| FSMap / MDSMap epoch | 9 / 9 | 9 / 9 |
| Go client | GlobalID 5106, address `172.19.0.9:0/2666407642` on both ranks | GlobalID 5106, address `[fd62:93fe:2c9e::9]:0/2053608117` on both ranks |

The rank-0 and rank-1 sessions had local generations 1 and 2; both OPENs
advertised `ffffff0000000000`.

### Forward

| Value | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| LOOKUP on rank 0 | TID 4, forward 0, MDSMap epoch 10 | same |
| MClientRequestForward | destination 1, forward 1, must resend, 801 µs after Submit | same values, 243 µs |
| Resubmission on rank 1 | TID 5, forward 1: safe first reply, result 0, Arrival 9, ino `0x10000000002`, dentry `target`, cap ID 10 `pAsLsXsFscr`, 607 µs | same, Arrival 10, 270 µs |
| Native `forward` counter | rank 0 0 to 1, rank 1 1 to 1 | same |
| Native `statx` of `fwd/target` | ino `0x10000000002`, mode `0100644`, uid/gid 0, nlink 1, size 0, equal to the reply | same |

Cap ID 10 in the rank-1 reply is the cap that the IMPORT of the `fwd`
migration installed for `target` (below).

### Cap migration

`fwd` (inode `0x10000000001`; pin -1 to 1) and `mig` (inode
`0x10000000003`; pin -1 to 0, then 0 to 1) each moved as one subtree; after
convergence rank 1 reported both with `is_auth` true, `auth_first` 1,
`auth_second` -2 and `export_pin` 1, and rank 0 reported them as non-auth
subtrees with the same single authority. The setting and reading mounts had
different native GlobalIDs for every pin change.

| Value | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| `fwd` convergence | 6 polls, 4064 ms | 5 polls, 3471 ms |
| `fwd` FLUSHMSG (rank 0) | Arrival 16, sequence 5 = pushes counted, acknowledged 14 µs later | Arrival 18, sequence 6 = pushes counted, 21 µs |
| `fwd` EXPORT (rank 0) | Arrival 17, ino `0x10000000002`, cap 1, migrate seq 0, body zero; peer cap 10, issue seq 1, migrate seq 1, mds 1, flags 0 | Arrival 19, same fields |
| `fwd` IMPORT (rank 1) | Arrival 6, cap 10, seq 1, issue seq 1, migrate seq 1, `pAsLsXsFs`, wanted none, realm 1; peer cap 1, issue seq 2, migrate seq 0, mds 0, flags 1 (AUTH) | Arrival 7, same except peer issue seq 3 |
| `fwd` counters | rank 0 `exported` 0 to 1, `exported_inodes` 0 to 1; rank 1 `imported` 0 to 1, `imported_inodes` 0 to 1 | same |
| `fwd` native `num_caps` (rank 0, rank 1) | (4, 0) to (3, 1) | same |
| `mig` on rank 0 before | auxiliary subtree, `is_auth`, `auth_first` 0, `export_pin` 0 | same |
| `mig` convergence | 2 polls, 1172 ms | 2 polls, 1182 ms |
| `mig` FLUSHMSG (rank 0) | Arrival 40, sequence 12 = pushes counted, acknowledged 17 µs later | Arrival 41, sequence 13, 6 µs |
| `mig` EXPORTs (rank 0) | Arrival 41 to 43 for `0x10000000005`, `0x10000000006`, `0x10000000004`; cap 1 each; peers caps 30, 29, 28, issue seq 1, migrate seq 1, mds 1 | Arrival 42 to 44 for `0x10000000005`, `0x10000000006`, `0x10000000004`; peers caps 28, 30, 29 |
| `mig` IMPORTs (rank 1) | Arrival 16 to 18 for `0x10000000004` (cap 28), `0x10000000006` (29), `0x10000000005` (30); seq 1, migrate seq 1, `pAsLsXsFs`; peer cap 1, issue seq 2, migrate seq 0, mds 0, flags 1 | Arrival 16 to 18 for `0x10000000005` (28), `0x10000000004` (29), `0x10000000006` (30); same fields |
| Handled first | IMPORT for `0x10000000004` and `0x10000000006`, EXPORT for `0x10000000005` | EXPORT for all three |
| `mig` counters | rank 0 `exported` 1 to 2, `exported_inodes` 1 to 4; rank 1 `imported` 1 to 2, `imported_inodes` 1 to 4 | same |
| `mig` native `num_caps` | (7, 2) to (4, 5) | same |
| GETATTR `file-0` on rank 1 | cap ID 28 (its IMPORT cap), `pAsLsXsFscr` | cap ID 29 (its IMPORT cap) |

The EXPORT and IMPORT of one cap arrived on the two connections within
0.3 ms of each other, in no fixed order; the agents reached the same
final state in both orders, which both occurred. No IMPORT asked for a
release (flags 1 only), so no cap release was sent for a migration. The
EXPORT body is zero apart from its cap ID, migrate sequence and peer; the
IMPORT's peer carries the exporter's cap sequence and its migrate sequence
minus one. Native rank 1's `forward` counter rose from 0 to 7 over the run
(1 during the `fwd` pin, 3 before and 3 during the `mig` pin to rank 1).
No Go request was forwarded by rank 1, so these were requests of the native
oracle's mounts.

### Standby takeover and reconnect

| Value | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| Armed rank 0 | `-1`, GID 4671, incarnation 6 | `-1`, GID 4668, incarnation 6 |
| `mds fail` acknowledged | 404 ms after the request | 301 ms |
| Rank-0 session Lost | 353 ms after the request; resume redials: EOF, then the respawned daemon on the same address answered RESET (`SessionReset`); 0 resumes | 312 ms; redials: connection refused, then RESET |
| Timeline (ms after the request) | `up:replay` 907, `up:resolve` 2118 (failed daemon already back as standby, GID 18643), `up:reconnect` 2723, `up:rejoin` 3986, `up:active` 4740 | `up:replay` 814, `up:resolve` 2039 (standby GID 8712), `up:reconnect` 2750, `up:rejoin` 4011, `up:active` 4768 |
| Reconnect | `OpenMDS` of `v2:172.19.0.12:6800/2810001375` 2723 ms after the request; `ReconnectSession` OPEN in 4.2 ms | `v2:[fd62:93fe:2c9e::c]:6800/1306071156` at 2750 ms; OPEN in 1.1 ms |
| Reconnect state | caps for the root (`pAsLsXs`), the owned directory (`pAsLsXsFsx`, path `<dir>` under the root), `fwd` and `mig` (`pAsLsXs`, paths under the owned directory), wanted none; realm 1 sequence 1 | same |
| Reconnected session | Open, generation 3 reconnected from generation 1, no resubmitted calls | same |
| Adopted | rank 0 `-2` GID 14524, incarnation 12, same address; rank 1 unchanged; standby `-1` GID 18643, `172.19.0.11:6800/3334631130` | rank 0 `-2` GID 14530, incarnation 12; rank 1 unchanged; standby `-1` GID 8712, `[fd62:93fe:2c9e::b]:6800/3407725211` |
| Old incarnation fencing | `172.19.0.11:6800/193353784` blocklisted | `[fd62:93fe:2c9e::b]:6800/3408828841` |
| Native rank 0 after | session `open`, 4 caps (as reconnected), 0 leases (1 before), same address | same |
| Rank 1 across the takeover | native 5 caps and 1 lease before and after; session Open, generation 2, push sequence 4 before and after, 0 resumes, renewals 14 to 18 | same |
| GETATTR of the owned directory on the new rank 0 | result 0, cap ID 1 `pAsLsXsFsx`, Arrival 6 | same |

The MDS dial log has exactly five MDS dials: the two session opens, the two
resume redials of the failed address and the reconnect dial.

### Cleanup

Pin restoration: both directories 1 to -1, applied and read back -1 by a
different mount. The four unlinks on rank 1 and three rmdirs on rank 0
completed without forwards; the rank-1 agent answered one dentry-lease
REVOKE (`target` in `fwd`, mask 1, sequence 2) right before the reply of
the `target` unlink. Native `num_caps` (rank 0,
rank 1) went from (4, 6) to (0, 0) after one MClientCapRelease per session
(rank 0: the root, owned directory, `fwd`, `mig`; rank 1: `fwd` (cap 11),
`target` (10), `mig` (49) and the three migrated files). Both CLOSEs were
confirmed.

### Cleanup verification

Both `mds-cephfs-ranks-cleanup.json` receipts report `completed: true`: the
stable topology returned (the failed daemon as the standby), no residual
session of the two identities, both pins `directory-absent` (the directories
were removed with their pins after the test's own restoration), the owned
directory absent natively, one new blocklist entry (the failed rank-0
incarnation's address) attributed as `mds-takeover`, removed with
`osd blocklist rm` and absent from the final listing, nothing kept, and both
identities deleted. Both `mds-fresh-cleanup.json` receipts report
`cephfs_ranks_verified`, filesystem, pools and fresh identity removed and
`completed: true`. All eleven owned containers (three MON, two MGR, one OSD,
three MDS, control and native oracle) and the fixture network of each run
were confirmed absent with `docker inspect`.

| SHA-256 | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| `mds-cephfs-ranks-results.json` | `7513b20e01efc94ec85bd8af447f65469f5fcb58a7f4a7ac4b3fd47111a740c1` | `20343f4801369ebe10a0943053f9c7b6e5942be971785c06254bebb8e67b7c1f` |
| `mds-cephfs-ranks-oracle.json` | `6d5d873f3feb9a93ed4dcaa37d18497c98f05ee1a22f3058a02d654dda9ef242` | `62572fa83047aa5d0b508e0357e080ae3650adb9c666ec2430dc218999d0dbd5` |
| `mds-cephfs-ranks-owned.json` | `f544d82115f7e3664cff94fcd05aea71c09a57b3f8e9788473c35ecb73a8e92e` | `cd4f835169da6298d222fdccdb9cb4d4fb59aedd0f96b01c257eaac8066e66af` |
| `mds-cephfs-ranks-cleanup.json` | `1a34b55d6d15d8614c5927b746ff83ff1d1500f6501173762f2782b6170fb5e5` | `978af8074ec5b476b07e477489328a373d9fd60d014ea87f2518b9f1fece8761` |
| `mds-fresh-cleanup.json` | `381bee3a8a9292704850b30732ea65221220c1201548a59de4ea0b4210c5cdc7` | `350b119d8ea3c9c83703dd063979288e58e19a9e0b9976b5cbde92d66fb7d263` |

## Earlier attempts

- Before any fixture code, a provider probe on a one-MON, one-MemStore-OSD
  cluster confirmed that `cephfs.Config{ActiveMDS: 2, StandbyMDS: 1}` comes
  up with one OSD, that a native `ceph.dir.pin` setxattr moves a non-empty
  directory to rank 1 within about a second, the `get subtrees` and
  `perf dump mds` formats, and the takeover sequence after
  `ceph mds fail <rank-0 GID>` without clients (replay, resolve, reconnect,
  rejoin, active within about 5 s; the failed daemon back as a standby on
  the same port with a new GID and nonce; its old address blocklisted).
- The first CRC run of the test (working tree) passed the four leaves and
  failed in the cleanup subtest: the test required each per-rank view's
  daemon epoch to reach the receipt's map epoch, but the fixture reads the
  views before its final proof and the MDSMap epoch moved from 16 to 17
  meanwhile (the final proof itself checks each daemon's applied epoch).
  This was a test defect, fixed by dropping that comparison; the fixture's
  sessions field also lost `omitempty`, so an empty session list is not
  reported as absent. The fixture cleanup of that run completed (the test's
  own cleanup closed both sessions).
- A secure working-tree run then passed every subtest. It predates the
  required FLUSHMSG ordering checks. Neither working-tree run is the
  acceptance record.
- The change set also touches what the single-rank CephFS profile shares
  (the capability agent's lock and the fresh oracle's topology check). After
  the two recorded runs, the same source passed that profile
  (`CEPH_MSGR_TEST_MDS_CEPHFS=1`, CRC/AES IPv4 host BlueStore): push 7/7
  (56.08 s), recovery 5/5 (57.29 s) and metadata 9/9 (75.03 s), with both
  cleanup receipts `completed`. This is a regression check, not part of this
  record.

## Findings and limits

- No product defect was found. Live behavior matched the contract: explicit
  targets give one client one session per rank under one GlobalID and one
  shared address instance; a forward resolves the call with
  `MDSRequestForwardedError` (destination, count, must-resend) without
  ending the session or resending; counted pushes include EXPORT and IMPORT
  and FLUSHMSG is answered by `AcknowledgeFlush` before the export
  proceeds; MClientCaps v12 EXPORT and IMPORT decode with their peers;
  Messenger resume to a failed daemon's address ends in RESET (a respawned
  process on the same address) and the session becomes Lost;
  `ReconnectSession` to the standby that took over the rank is accepted
  during `up:reconnect` while the other rank's session is undisturbed.
- A forward has no `Arrival`, so a consumer cannot order it against the
  session's pushes by that counter. Native clients resend a forwarded request
  with its original TID; the product requires a new TID. The MDS gave the
  forward before executing anything, so both are equivalent here.
- A consumer with sessions to several ranks must accept EXPORT and IMPORT of
  one cap in either order: they arrive on different connections, and both
  orders occurred in the CRC run.
- `cephmsgr/MDS_CEPHFS_CONTRACT.md` still lists forwards, EXPORT/IMPORT and
  FLUSHMSG from migration, multiple ranks and standby takeover as not
  exercised live. That statement is now outdated; the product document was
  left unchanged here.
- Not exercised: a forward of a mutation, more than one forward of one
  request, a request in flight while its subtree migrates (an export
  freezing it), an IMPORT asking for a release, caps on open or dirty files
  across a migration, a migration back to rank 0, takeover of rank 1,
  standby-replay, a takeover with resubmitted requests or file locks, a
  reconnect denied or missing its window, more than two ranks, the MDS
  balancer and ephemeral pins, and an OSD connection.
- These are local selected-configuration results, separate from remote CI.
  The two new CI rows select `^TestCephMDSCephFSRanksIntegration$`; they
  were not run remotely here.

## Commands follow rank targets (2026-10-18)

Leaf `commands_follow_rank_targets`, after the standby takeover: the ranks
identity's MDS grant is not admin, so the test creates
`client.ceph-msgr-ranks-commands` (`mon 'allow r'`, `mds 'allow *'`) through
the fixture admin's MON `auth get-or-create` and removes it with `auth rm`.
With it, an MDS `status` command sent to rank 1's explicit target answers
`whoami` 1, state `up:active` and rank 1's GID; the same command sent to the
target of the daemon that took over rank 0 answers `whoami` 0 with the
former standby's GID. A first attempt with the fixture admin identity was
refused at MDS authentication (-13: that identity has no MDS grant). Passed
in CRC/AES BlueStore IPv4 and secure AES256K MemStore IPv6.
