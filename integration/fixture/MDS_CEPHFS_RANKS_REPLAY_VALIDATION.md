# Standby-replay and three-rank CephFS acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go,
without CGO, librados, go-ceph, Ceph CLI subprocesses or native runtime
dependencies. The disposable provider, its native CLI, the native LibCephFS
oracle and the owned MDS daemons are development-only. This acceptance
covers the multi-rank paths the [ranks record](MDS_CEPHFS_RANKS_VALIDATION.md)
left unexercised: a forwarded mutation, a request forwarded twice, requests
in flight while their subtree is frozen for export, three active ranks, and
a standby-replay follower's takeover of rank 0 with resubmitted requests.
The fixture extension is described in
[MDS_CEPHFS_RANKS_FIXTURE.md](MDS_CEPHFS_RANKS_FIXTURE.md#standby-replay-extension).

## Test shape

`TestCephMDSCephFSRanksReplayIntegration`
(`integration/mds_cephfs_ranks_replay_test.go`) runs only with
`CEPH_MSGR_TEST_MDS_CEPHFS_RANKS=1` and
`CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY=1` (selector
`^TestCephMDSCephFSRanksReplayIntegration$`). The Go identity, caps,
relayed dialer, session options (2 s renewal, 1024 events), capability
agents with one shared lock and OSD refusal are those of the ranks
acceptance, with `MaxMDSConnections` 3 and one session per rank. Every
forward is answered as `Client::handle_client_request_forward` does: the
same request with the same TID and the reported forward count, sent with
`ResubmitForwarded` on the destination rank's session. An unsafe reply is
followed by `FlushJournal` and the safe reply. For this acceptance the agent
can hold the acknowledgement of the next FLUSHMSG (test-only, bounded to
20 s); the native client acknowledges after flushing its cap releases, so a
later acknowledgement is ordinary client behavior.

| Leaf | Checked behavior |
| --- | --- |
| `open_sessions_on_three_ranks` | One session per rank from the native map: Open, the client's GlobalID, the explicit target, push sequence 0, the required OPEN features; native `session ls` on each rank shows the identity, metadata, no caps and the same `<ip>:0/<CephFSNonce>` |
| `enable_standby_replay` | `replay-enable`: the owned standby (same name, GID, endpoint, nonce) becomes `up:standby-replay` following rank 0 with every rank unchanged; all three sessions remain open natively |
| `forward_chain_across_ranks` | Go creates the owned directory, `chain`, `chain/hop` and `chain/hop/anchor` on rank 0; `chain` is pinned to rank 1 and `chain/hop` to rank 2 (each converged, caps settled on the pinned rank). A CREATE and a MKDIR addressed by their full path from the root are sent to rank 0. Each must end on rank 2 with result 0 after at least one forward, forward counts 1, 2, ... in order, the TID kept by every `ResubmitForwarded`, MustResend set. Native `forward` counter deltas per rank must equal the forwards Go received from that rank. Native listing of `chain/hop`: exactly `anchor`, `forwarded-file`, `forwarded-dir` with the reply inodes |
| `inflight_mutation_during_export` | Go creates `inflight` and `inflight/anchor` on rank 0, arms the hold of rank 0's next FLUSHMSG acknowledgement and pins `inflight` to rank 1. When the FLUSHMSG arrives (the subtree is frozen and the export waits for this session), Go submits a CREATE `held-file` and a MKDIR `held-dir` to rank 0, observes them for 500 ms, then releases the acknowledgement. Each call must end with result 0, exactly once: if forwarded, the forward arrives after the FLUSHMSG on the same session and the call completes on rank 1 by `ResubmitForwarded`. The EXPORT of `anchor` arrives after the FLUSHMSG; the held acknowledgement carries the counted push sequence. Native listing of `inflight`: exactly the three entries with the reply inodes |
| `standby_replay_takeover_resubmit` | Go creates `replay`, `safe-0` and `safe-1` on rank 0 (unsafe then safe). `log-pause` pauses rank 0's journal; a CREATE `inflight-file` and a MKDIR `inflight-dir` are answered unsafe and stay unsafe 1.5 s after a `FlushJournal`. The fixture arms the takeover (the follower as spare) and fails rank 0. The rank-0 session becomes Lost and both calls end `OutcomeUnknownError`. When the follower holds rank 0 in `up:reconnect`, Go closes the old connection, opens the follower's address and calls `ReconnectSession` with the rank-0 caps, realms and both calls in `Resubmit`: OPEN, a new generation reconnected from the Lost one, two returned calls with their TIDs. Both complete with result 0; a returned inode (trace or CREATE extra) must equal the unsafe reply's. The new rank 0's op tracker must show both TIDs executed with REPLAY. Adoption: the follower in rank 0 with a new incarnation, ranks 1 and 2 unchanged, the failed daemon back as rank 0's new follower with a new GID, its old address blocklisted. Native: the reconnected session open on the new rank 0 at the same address; ranks 1 and 2 keep their native cap counts, generations and zero resumes. A GETATTR on the new rank 0 succeeds. Native listing of `replay`: exactly the four entries with the unsafe replies' inodes |
| `cleanup_restore_pins_unlink_release_close` | Pins of `chain/hop`, `chain` and `inflight` back to -1 (read back by another mount); every created entry removed, each removal following any forward; native `statx` of the owned directory ENOENT; every cap released (native `num_caps` 0 on all ranks); confirmed CLOSE on all three sessions, absent natively; no OSD dial |

Every native receipt proves the current topology (all ranks and the spare,
in the current shape) and each rank daemon's own status.

## Results

Both selected configurations passed on 2026-10-16 (KST), all six subtests,
no skips. The tested source is commit
`b81e556` on branch `worktree-agent-a0f2be8078f8c250c`
(parent `b50939e`), exported to a separate directory with `git archive`;
this record is the only file written after the runs. The export holds 1249
files; the SHA-256 of its sorted per-file SHA-256 list (`find . -type f
-print0 | LC_ALL=C sort -z | xargs -0 shasum -a 256`, run in the export root)
is `c51df5b10beda503415884b7aa5f5dd0b41839dc03408b4c8e2adf36310c5e46`.

| File | SHA-256 |
| --- | --- |
| `integration/mds_cephfs_ranks_replay_test.go` | `65fc24291cc7a64f2ff31ac97c1769a665c9b1e43ca1f77b3069fef3e3719411` |
| `integration/mds_cephfs_ranks_test.go` | `87138deb1de5fa543dc40de939cdca416a21a0a26143727286a0f0d0acff8703` |
| `integration/mds_cephfs_agent_test.go` | `19f95d50e0ae327b89ebe030fa04c933633125fc60ae4e52413c5dabffa583f8` |
| `integration/fixture/mds_cephfs_ranks.go` | `d8c8db3d9c2f9a32e5a73ad336c84d77e5c50f7aff20e33517b1b49c2571c66c` |
| `integration/fixture/mds_cephfs_ranks_replay_test.go` | `6aa7000d44fbef21fd1a49434af695ce3c39a55827afdb3555fb3c6f37660de2` |
| `integration/fixture/profile.go` | `5d7c399fe5947b66a19bb428d0bf9cc53eb2af33dc4d8625960cc20eaf993d49` |
| `.github/workflows/ci.yml` | `5eae7fcf6ea136d53b390cd0897dc1159de7db85309ae5e0924f493e231753e9` |
| `tools/test_ci_osd_profiles.py` | `c102835f570bee902484d8c7005fff387cdb074f82eb632953a26576489f324e` |

Image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
(arm64, Docker Desktop 29.8.2), provider
`github.com/JSYoo5B/ceph-testcontainers-go` v0.2.0. Topology: three MON,
two MGR, one OSD, the owned filesystem with three active ranks and one
spare (four owned MDS containers), and the native oracle container.

| Configuration | Controller wall time (KST) | Test | Result |
| --- | --- | --- | --- |
| Darwin arm64 host runtime, IPv4 relay, CRC, AES client and service keys, single BlueStore OSD | 12:57:26 to 13:00:08 (2 min 42 s) | 68.29 s | 6/6 passed |
| CGO=0 Linux arm64 container runtime, IPv6, secure, AES256K client and service keys, single MemStore OSD | 13:00:18 to 13:02:52 (2 min 34 s) | 65.28 s | 6/6 passed |

Leaf durations (CRC / secure, seconds): open 1.82 / 1.91, replay enable
3.51 / 3.80, forward chain 21.66 / 20.53, frozen export 12.17 / 12.48,
takeover 16.34 / 15.52, cleanup 12.78 / 11.04.

| Startup | CRC/AES IPv4 | secure/AES256K IPv6 |
| --- | --- | --- |
| rank 0 | `-0`, GID 4653, incarnation 6, `172.19.0.10:6800/2830068463` | `-0`, GID 4647, incarnation 6, `[fd62:93fe:2c9e::a]:6800/3062294036` |
| rank 1 | `-3`, GID 4689, incarnation 8 | `-1`, GID 4656, incarnation 8 |
| rank 2 | `-1`, GID 14510, incarnation 10 | `-2`, GID 14521, incarnation 10 |
| standby | `-2`, GID 14518, `172.19.0.12:6800/167241638` | `-3`, GID 14533, `[fd62:93fe:2c9e::d]:6800/279387855` |
| FSMap / MDSMap epoch | 11 / 11 | 11 / 11 |
| Go client | GlobalID 5184, `172.19.0.9:0/724321393` on all ranks | GlobalID 5205, `[fd62:93fe:2c9e::9]:0/1543577064` on all ranks |

### Standby replay

`fs set allow_standby_replay true` produced the follower within one poll
(986 ms / 1138 ms including the command): the standby kept its GID, endpoint
and nonce, changed to `up:standby-replay` with rank 0 (incarnation 0) and
moved from the FSMap standby list into the MDSMap `info`; the epoch went 11
to 12 (CRC) and 11 to 13 (secure). No rank or session changed.

### Forward chain

Both runs observed the same values unless noted.

| Value | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| `chain` to rank 1 | 5 polls, 4608 ms; EXPORT on rank 0 for `hop` (`0x10000000002`) and `anchor` (`0x10000000003`), IMPORT on rank 1 (caps 11, 10) | 5 polls, 4599 ms; same inodes and caps |
| `chain/hop` to rank 2 | 3 polls, 2598 ms; EXPORT of `anchor` on rank 1 (cap 10), IMPORT on rank 2 (cap 1) | 3 polls, 2612 ms; same |
| CREATE `forwarded-file` (TID 5, path from the root, MDSMap epoch 14 / 15) | rank 0 forward to 1 (count 1, Arrival 29, 691 µs), rank 1 forward to 2 (count 2, Arrival 19, 1227 µs), rank 2 safe first reply result 0, ino `0x30000000000`, caps `pAsxLsXsxFsxcrwb`, 2985 µs | same hops at 242 and 407 µs, reply at 1001 µs |
| MKDIR `forwarded-dir` (TID 6) | forwards 0 to 1 to 2 (Arrivals 30, 20), unsafe then safe on rank 2, ino `0x30000000001` | same |
| Native `forward` counters | rank 0 0 to 2, rank 1 1 to 3, rank 2 0 to 0, equal to the forwards Go received | same |
| Op trackers | rank 0: `forwarding request to mds.1` after four lock acquisitions; rank 1 (description `FWD`): `forwarding request to mds.2`; rank 2: executed (MKDIR `early_replied`, CREATE without early reply) | same; rank 1's entry for the MKDIR had already left its 20-entry history |
| Native listing of `chain/hop` | `anchor` `0x10000000003`, `forwarded-file` `0x30000000000`, `forwarded-dir` `0x30000000001` | same |

The rank-0 traversal from the root reached `chain`'s dirfrag, whose
authority it knew as rank 1, and forwarded there; rank 1 forwarded on to
rank 2 for `hop`'s dirfrag. The CREATE on rank 2 had no early reply: it was
the session's first CREATE there, without preallocated inodes, and
`Server::early_reply` refuses an allocated inode.

### Mutations in a frozen export

| Value | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| Held FLUSHMSG on rank 0 | Arrival 45, sequence 13 = pushes counted, 1728 ms after the pin request started | Arrival 44, sequence 12, 3248 ms |
| Submission | CREATE TID 9 and MKDIR TID 10 within 1 ms of the FLUSHMSG | same |
| During the 500 ms hold | neither call resolved | same |
| Acknowledgement | sent 502 ms after the FLUSHMSG | 502 ms |
| EXPORT of `anchor` on rank 0 | Arrival 46 (after the FLUSHMSG), cap 1 to rank 1 cap 34 | Arrival 45, same caps |
| Outcome | both forwarded by rank 0 to rank 1 (count 1, MustResend; Arrivals 48 and 47), about 2.9 s after submission; resubmitted with the same TIDs; CREATE safe first (result 0, ino `0x20000000000`), MKDIR unsafe then safe (ino `0x20000000001`) | forwards at Arrivals 47 and 46, about 2.8 s after submission; same results and inodes |
| Rank 0 op tracker | `dispatched`, `forwarding request to mds.1` | same |
| Rank 1 op tracker | description `FWD`, executed once (MKDIR `early_replied`) | same |
| Native `forward` counters | rank 0 2 to 4, rank 1 6 to 9 (native oracle mounts included), rank 2 0 | same |
| Native listing of `inflight` | `anchor`, `held-file`, `held-dir` with the reply inodes | same |

The requests waited on the frozen dirfrag and were forwarded once the
export finished; neither was executed by the exporter, and each executed
once on the importer.

### Standby-replay takeover

| Value | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| Before | `safe-0`, `safe-1` unsafe then safe (ino `0x10000000007`, `0x10000000008`); journal pause applied on rank 0 (readback `true`) | same |
| Paused requests | CREATE TID 14 unsafe (ino `0x10000000009`), MKDIR TID 15 unsafe (ino `0x1000000000a`); still unsafe 1.5 s after `FlushJournal` | same |
| Armed | rank 0 `-0` GID 4653 incarnation 6; spare the follower `-2` GID 14518 | rank 0 `-0` GID 4647; follower `-3` GID 14533 |
| `mds fail` acknowledged | 303 ms after the request | 301 ms |
| Rank-0 session Lost | 339 ms; resume redials: EOF, then RESET (`SessionReset`); both paused calls `OutcomeUnknownError` | 339 ms; connection refused, then RESET |
| Follower states (ms after the request) | `up:replay` 843, `up:resolve` 2051, `up:reconnect` 2655, `up:clientreplay` 4887, `up:active` 6400 | 840, 1955, 3077, 4834, 5691 |
| Reconnect | `ReconnectSession` at 2655 ms, OPEN in 3.0 ms: 9 caps, 1 realm, two resubmitted calls with TIDs 14 and 15; new generation 4 reconnected from generation 1 | at 3077 ms, OPEN in 0.9 ms; same |
| Resubmitted replies | both safe first, result 0, traceless; the CREATE's extra names `0x10000000009`, the MKDIR has no extra | same |
| New rank 0 op tracker | both TIDs `RETRY=1 REPLAY QUEUED_FOR_REPLAY`, executed (`acquired locks`, `journal_committed`), the CREATE after `failed to wrlock, waiting` | same |
| Adopted | rank 0 GID 14518 incarnation 16; the failed `-0` back as rank 0's follower, GID 20050; FSMap 22 | rank 0 GID 14533 incarnation 17; follower `-0` GID 20390; FSMap 23 |
| Old incarnation fencing | `172.19.0.10:6800/2830068463` blocklisted | `[fd62:93fe:2c9e::a]:6800/3062294036` |
| Native caps (ranks 0, 1, 2) | (9, 5, 4) before and after; rank 0 session `open` at the same address, 0 leases | same |
| Ranks 1 and 2 | generations 2 and 3, push sequences 7 and 2, 0 resumes, before and after | same |
| Native listing of `replay` | the four entries with the unsafe replies' inodes | same |

The MDS dial log has six dials per run: the three session opens, two resume
redials of the failed address and the reconnect dial.

For comparison, the plain-standby takeover of the base ranks acceptance on
the same exported source (CRC/AES IPv4 host BlueStore, two ranks, regression
run below) reached `up:replay` 907, `up:resolve` 2117, `up:reconnect` 2721,
`up:rejoin` 4085 and `up:active` 4842 ms after its request. The follower
reached replay, resolve and reconnect at the same pace. With an almost empty
journal, standby replay saved no measurable time; the transitions follow MON
map updates. Its later `up:active` includes client replay of the two
resubmitted requests and a third rank in resolve and rejoin. `up:rejoin` fell
between two 250 ms samples in both runs.

### Cleanup

All entries were removed (forwards followed where needed), every cap was
released (native `num_caps` (9, 6, 4) to (0, 0, 0)) and all three CLOSEs
were confirmed. Both `mds-cephfs-ranks-cleanup.json` receipts report
`completed: true`: the three-rank topology with rank 0's follower returned,
the journal pause `discarded` with its respawned incarnation, the three pins
`directory-absent`, the owned directory absent, the failed incarnation's
address attributed as `mds-takeover`, removed and verified absent, nothing
kept, both identities deleted. Both `mds-fresh-cleanup.json` receipts report
`cephfs_ranks_verified` and `completed: true`. The owned containers and
network were gone after each run (only the shared Testcontainers reaper
remained, which exits by itself).

| SHA-256 | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| `mds-cephfs-ranks-replay-results.json` | `171a449e69e4a7b588bf10343979770120b146125835bbcd808068023b25a32c` | `b21c2eef6ddb0a918e6b51f6cc1f1fe4200080a6511af7b8307c2e80edb5dde9` |
| `mds-cephfs-ranks-cleanup.json` | `f75528b58b6f9356c85a8c1cc08158134f887e29c35b153a2ff6853d43536d58` | `3cf8852b24005cbe99ffa0498f3cccc207afbebb22814697e1fed9fd6d9ed4cb` |
| `mds-fresh-cleanup.json` | `f78de1dce72da0b54429abe02772da81695f58968c555e5e938d822e71ad9fd7` | `e6b643db7c4b47d4865b5f64045f487bd42e26c1f13b9c495069efea46fe84e7` |

### Regression of the base ranks acceptance

The same export passed `TestCephMDSCephFSRanksIntegration`
(`CEPH_MSGR_TEST_MDS_CEPHFS_RANKS=1` only, CRC/AES IPv4 host BlueStore,
13:03:01 to 13:05:10): 5/5 in 42.22 s, both cleanup receipts `completed`.

## Earlier attempts

- The first working-tree run passed the first four leaves and failed the
  takeover leaf: the resubmitted REPLAY MKDIR was answered safe and
  traceless, and the test required a trace inode. The test now accepts a
  traceless REPLAY answer and checks the inodes natively. Its fixture cleanup
  completed. A second working-tree run passed all six leaves. Neither is
  part of this record.

## Findings and limits

- No product defect was found. `ResubmitForwarded` carried one request
  across two forwards with its TID and the MDS's forward counts; the forwards
  of requests that waited on a frozen export arrived after the session's
  FLUSHMSG and the importer executed them once; `ReconnectSession` to a
  standby-replay follower in `up:reconnect` resubmitted journal-paused
  requests that the new rank 0 executed with REPLAY.
- A REPLAY request executed during client replay is answered without a
  trace (both CREATE and MKDIR, safe first). A consumer learns a replayed
  MKDIR's inode only from its earlier unsafe reply; a CREATE's extra still
  names it.
- The CREATE that a rank executes with an allocated inode (no preallocated
  inodes for the session there) has no early reply. A consumer cannot rely
  on an unsafe reply preceding the safe one.
- `cephmsgr.MDSRequest`'s comment still says resubmission after a forward
  gets a new TID; `ResubmitForwarded` keeps the TID. The product comment and
  `cephmsgr/MDS_CEPHFS_CONTRACT.md` (which lists forwarded mutations,
  requests in flight during migration, standby-replay and more than two ranks
  as not exercised live) were left unchanged here.
- Not exercised: a request in flight when its rank fails while forwarded, a
  migration back to rank 0, takeover of a rank other than 0, a follower of a
  rank other than 0, journal entries flushed but not acknowledged before the
  takeover (completed-request answers from a follower), the MDS balancer and
  ephemeral pins, caps on dirty files across a migration, and an OSD
  connection.
- These are local selected-configuration results, separate from remote CI.
  The two new CI rows select `^TestCephMDSCephFSRanksReplayIntegration$`;
  they were not run remotely here.
