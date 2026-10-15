# CephFS metadata acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go,
without CGO, librados, go-ceph, Ceph CLI subprocesses or native runtime
dependencies. The disposable provider, its native CLI, the native LibCephFS
oracle and the owned MDS are development-only. This acceptance covers the
opt-in CephFS metadata API of
[the CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md):
`Options.EnableCephFS`, `MDSSession.Submit`, `MDSRequestCall.Wait`/`WaitSafe`,
the reply decoders, MDS pushes through `Next`, `SendCaps`, `ReleaseCaps`, and
the lossless Messenger resume of the CephFS profile. The owned recipe and its
cleanup are described in [MDS_CEPHFS_FIXTURE.md](MDS_CEPHFS_FIXTURE.md).

## Test shape

`TestCephMDSCephFSIntegration` skips unless `CEPH_MSGR_TEST_MDS_CEPHFS=1` and
requires MDS_FRESH=1, neither MDS_COMMANDS nor MDS_SESSIONS, and the
`legacy-v6` OSD recipe. One Go client authenticates as
`client.mds-cephfs-<token>` (MON `allow r`, MDS `allow rw fsname=<owned fs>`),
enables only `EnableMDS` and `EnableCephFS`, dials MDS through a counting
stream relay that refuses any OSD dial, and opens one explicit `MDSTarget`
built from the owned native map (FSCID, rank 0, exact v2 `address/nonce`). The
session sends metadata `entity_id`, `hostname` and `root=/` with a 2 s
RenewInterval and 1024 event slots. Requests use caller and owner UID/GID 0
and the MDSMap epoch of the latest native observation.

A test-only capability agent (`integration/mds_cephfs_agent_test.go`) consumes
every session event from OPEN to CLOSE. It restates the pinned native client
for a client that uses no data: `add_update_cap` for every InodeStat with caps
in the first reply of each request (target, directory and readdir entries),
`handle_cap_grant` for GRANT/REVOKE, `check_caps` with `send_cap` to answer
revokes (UPDATE with the revoke's sequence, the tracked issue/migrate
sequences, the retained caps, current wanted caps, dirty 0, no realm, caller
UID/GID -1, unknown dir counts, last known size and times, the OSD epoch
barrier), `handle_lease` (RELEASE echoing a revoke), FLUSHMSG acknowledgement,
and `remove_cap` for the final release. Open handles are modeled by mode
(`get_open_ref`/`put_open_ref`); a close runs `check_caps` at once where the
native client sends the same UPDATE after `client_caps_release_delay`. Pushes
for an inode whose reply was not applied yet are deferred until it is; their
order relative to replies is known only from receive timestamps.

Every native receipt must match the owned target (FSID, FSCID, filesystem
name, daemon GID, rank 0, endpoint, nonce, incarnation), re-read before and
after the action, and the daemon's applied MDSMap epoch must be at least the
receipt's map epoch. Native oracle GlobalIDs must differ from the Go client's.
The run recorded below used the startup daemon. Since the recovery
acceptance was added to the same fixture, the test reads the target from
`mds-cephfs-target.json`, which names the incarnation the fixture adopted
after its owned restart (see
[MDS_CEPHFS_RECOVERY_VALIDATION.md](MDS_CEPHFS_RECOVERY_VALIDATION.md)).

| Leaf | Checked behavior |
| --- | --- |
| `open_cephfs_session` | Session Open with the client's GlobalID and target, push sequence 0, OPEN supported features `ffffff0000000000` (bits 9, 18, 19, 20 present), one cap auth: UID -1, no GIDs or path, the owned filesystem, readable and writeable, no root squash. Native `session ls`: `open`, auth `client`/`mds-cephfs-<token>`, `client_features` `0x00000000001c83ff`, metric flags `0x0`, exactly the metadata sent, 0 caps and leases, address not blocklisted |
| `mkdir_matches_native_stat` | MKDIR of the plan directory under ino 1 (mode `040755`, WANT_DENTRY): result 0; dentry trace with root as parent and the name; snap trace decodes with realm 1 first; target ino, mode, UID/GID equal native `statx`; wire nlink 1 equals the native `2 + nsubdirs` presentation |
| `create_reports_created_inode` | CREATE `O_CREAT\|O_EXCL\|O_RDWR`, mode 0644, of `go-file`: result 0, mode `0100644`, UID/GID 0, nlink 1, size 0; the first reply's extra is exactly 8 bytes, the little-endian created ino, equal to the target; the agent closes the handle; native `statx` equal |
| `setxattr_matches_native_getxattr` | SETXATTR on the file ino with path2 `user.ceph-msgr` and a 28-byte binary value in the data segment: result 0; native `getxattr` returns identical bytes; GETATTR with Xs in the mask returns the same value through `DecodeMDSXattrs` |
| `readdir_matches_native_listing` | Native creation of `native-0..2` (journaled, once); READDIR frag 0, 1024 entries, REPLY_BITFLAGS: four entries whose names, inos, modes, UID/GID and nlink equal native `readdir` `d_ino` and `statx`; FRAG_END set |
| `lookup_existing_and_missing` | LOOKUP of `native-0` returns its ino and attributes equal native `statx`; LOOKUP of `missing` returns -2 with no target; native `statx` of it is ENOENT |
| `revoke_acknowledged_before_native_write` | Go opens the file for read (OPEN `O_RDONLY`) and holds Fr; the agent holds the first revoke on that ino for 2 s. A native one-shot writer opens `O_WRONLY`, writes 64 bytes, `fsync`s and closes. A revoke arrived, its acknowledgement was sent 2 s later with the remaining caps, the writer's own clock shows the write completing no earlier than that, an independent native read returns the bytes, and the push sequence grew |
| `messenger_resume_keeps_session` | Closing the client-side MDS stream (relay) while idle: `Resumes()` becomes 1 within the bound with one new MDS dial, the session never leaves Open, and a LOOKUP then succeeds. With the relay holding every incoming byte from before Submit, a GETATTR is submitted and the stream closed immediately: the call completes after the second resume. Native `session ls` shows the same GlobalID open at the same `IP:port/nonce`, not blocklisted |
| `cleanup_unlink_release_close` | UNLINK of the four files and RMDIR of the directory through Go (safe result 0); native `statx` of all five is ENOENT; `ReleaseCaps` for every held cap with its tracked cap ID, issue and migrate sequences and the OSD epoch barrier; native `num_caps` drops; `Close` is confirmed and the session is absent natively; the agent ends with `ErrMDSSessionClosed`, no deferred push and no error; MDS dials equal 1 + resumes, OSD dials 0 |

## Results

Both selected configurations passed on 2026-10-16 (KST), all nine leaves, no
skips. The tested source is HEAD `16f77b970c992d18bc82ee3bf66c2e56a9847c7c`
plus only this change set, exported to a separate directory with
`git archive`; nothing else from the shared working tree was included.
Acceptance source SHA-256: `integration/mds_cephfs_test.go`
`c714c55577fa2061dcb4ccf043488f3b71bc501a998f50c36d109157463fd5d9`,
`integration/mds_cephfs_agent_test.go`
`d85b77024cfcbb90f8d1f6f50461c70c5501ade6ebd1239ddb12f69e7fcd0093`,
`integration/fixture/mds_cephfs.go`
`9a3948a96205924c4bc45fd36629cf4d5b9e0cf97a71908a20ab3cc3e1f8b7ca`.

Image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
(arm64), daemon version
`ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`.

| Configuration | Controller wall time (UTC) | Test | Result |
| --- | --- | --- | --- |
| Darwin arm64 host runtime, IPv4 relay, CRC, AES client and service keys, single BlueStore OSD | 17:02:10 to 17:04:52 (2 min 42 s) | 76.72 s | 9/9 passed |
| CGO=0 Linux arm64 container runtime, IPv6, secure, AES256K client and service keys, single MemStore OSD | 17:04:52 to 17:07:26 (2 min 34 s) | 71.26 s | 9/9 passed |

Leaf durations (CRC / secure, seconds): open 2.31 / 2.10, mkdir 2.01 / 2.00,
create 6.33 / 2.20, setxattr 6.10 / 6.10, readdir 5.01 / 4.11, lookup
4.01 / 4.00, revoke 8.11 / 8.42, resume 2.31 / 2.21, cleanup 40.50 / 40.11.
Each native action took 1.9 to 3.0 s (mount, operation, shutdown and the two
identity checks); the writer took 3.9 s including its 2 s wait. The cleanup
leaf is dominated by five safe replies that waited for the MDS journal flush.

### Observed wire and native values

Both runs observed the same values unless noted.

- Declared CephFS client features (raw) `ff831c0000000000`; native
  `client_features.feature_bits` `0x00000000001c83ff`; metric flags `0x0`.
  `cephmsgr.MDSClientFeatures()` still returns the session-only set
  `ff01000000000000` (`0x1ff`) for this client (see findings).
- OPEN supported features `ffffff0000000000`, metric flags
  `ffff000000000000`, cap auth `{UID:-1 GIDs:[] Path:"" FilesystemName:<owned fs> RootSquash:false Readable:true Writeable:true}`.
- Every reply carried a 70-byte snap trace decoding to the single root realm
  (ino 1). Read operations answered safe at once. MKDIR answered safe without
  an unsafe reply (first allocation on a session without preallocated inodes;
  `Server::early_reply` refuses `alloc_ino`). CREATE, SETXATTR, the four
  UNLINKs and RMDIR answered unsafe within 9 ms and safe 0.19 to 5.0 s later,
  when the MDS flushed its journal on its tick; nothing asked for an earlier
  flush (see findings). Unsafe replies carry MDSMap epoch 0; safe replies and
  read replies carry the current epoch 7. Safe replies after an unsafe one had
  empty trace and extra.
- CREATE first-reply extra `0100000000010000`: the created ino
  `0x10000000001` (1099511627777), equal to the trace target. Reply caps
  `pAsxLsXsxFsxcrwb`, wanted `pAsxXsxFsxcrwb`; the close UPDATE retained
  `pAsLsXsFs` with wanted `-`.
- GETATTR returned exactly one xattr, `user.ceph-msgr`, at xattr version 2.
- READDIR flags `0x0701` (FRAG_END, FRAG_COMPLETE, HASH_ORDER, OFFSET_HASH),
  dirstat frag 0, auth -1, no distribution; 1523-byte extra.
- LOOKUP of `missing`: result -2 with a dentry trace (parent and null dentry)
  and no target.
- Revoke loop: the read OPEN reply cap was `pAsLsXsFscr`, wanted `pFscr`,
  cap ID 1, seq 7, mseq 0, realm 1, flags AUTH. The writer's open made the MDS
  revoke `Fsc` (REVOKE seq 8 to `pAsLsXsFr`); after the 2 s hold (2002 / 2001 ms)
  the agent sent UPDATE seq 8 caps `pAsLsXsFr` wanted `pFscr`. The writer's
  open returned in 0.55 / 0.54 ms, its write completed 2.014 s (CRC) and
  2.003 s (secure) after start, `fsync` and close right after. The MDS then
  revoked `Fr` (seq 9, acknowledged at once) and re-granted `Fc` (seq 10) and
  `Fscr` (seq 11) after the writer closed. The push sequence went 6 to 10 in
  this leaf.
- Other revokes answered during the run: `Fx` then `Fs` on the new directory
  for each native operation in it, `Xs` on the file for Go's own SETXATTR, and
  `Ls` on each file and the directory during Go's UNLINK/RMDIR, followed by
  GRANTs with wanted `Ls` announcing nlink 0. In each run the agent applied
  23 reply caps and handled 14 REVOKEs and 8 GRANTs with 15 UPDATEs; no lease
  revoke, FLUSHMSG, RECALL_STATE, snap or quota push arrived, and no push had
  to be deferred. The final push sequence was 22; the MDS OSD epoch barrier
  was 26.
- Release: six caps (`1`, the directory, `go-file` and the three native files;
  issue sequences 2, 16, 7, 2, 2, 1, migrate sequence 0) with barrier 26.
  Native `num_caps` went from 6 to 0 at the next observation; the root cap was
  released like the others and no pin remained.
- Resume: the idle close resumed in 4 ms (CRC) and 2 ms (secure); the request
  submitted with incoming bytes held and the stream closed at once completed
  3.8 ms and 0.65 ms after the close; no MDS byte had arrived before that
  close. The session state stayed Open throughout, each resume dialed the MDS
  once more (3 MDS dials, 0 OSD dials in total), and native `session ls`
  showed the same address before and after (`172.19.0.9:0/<nonce>` through
  the host relay, `[fd62:93fe:2c9e::9]:0/<nonce>` in the container), not
  blocklisted.

### Cleanup verification

Both `mds-cephfs-cleanup.json` receipts report `completed: true`: both native
mutations journaled `applied`, no residual session of either identity, the
owned directory absent natively (`namespace_restored: true`), no new blocklist
entry and both identities deleted. Both `mds-fresh-cleanup.json` receipts
report `cephfs_verified`, filesystem, pools and fresh identity removed and
`completed: true`. All nine owned containers and the fixture network of each
run were confirmed absent with `docker inspect` afterwards; containers of
other concurrently running fixtures are not counted.

| SHA-256 | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| `mds-cephfs-results.json` | `6cbd6058cc6fb6bf3ed001bc2e9795c97286d1ad55fea65eadd890e9e64b402a` | `20c8c1190854792a88667b64f59bfec0a91fea72fcf32af694e1677e6e912aac` |
| `mds-cephfs-owned.json` | `07a5ab14a9152a09d709d3b8e6b5dfb39eb33e6fefdefaffa96d95d3c9095203` | `f5e6354ae26754e90da65373a81f3bdfe4e390addf6d4543cf64dff6e769bc05` |
| `mds-cephfs-cleanup.json` | `cf7be85e63d14f0da70f3045b3fd8aa45d9d8f160769787113452f2de5cc6ff9` | `cbc52644aff638e7e8cbaad06bfff133abae8dab65fdc9e77c02fbcb12477ddc` |
| `mds-fresh-cleanup.json` | `570a0ac5994e94cf37eb9cbe6fcd767eafddb4c52bd14f39d806c4f1aac4b2ce` | `ae1ddf3312ce2d6e86e1b3e5f3d10450b4e3cdf880197af051bf678d53d7bc6a` |

Public evidence roots and logs are under
`/private/tmp/claude-501/-Users-jsyoo5b-Workspace-ceph-msgr-go/41987d68-a044-4af9-869c-6a98932340c0/scratchpad/`:
`diag-crc4/` with `run-crc4.log`, and `diag-sec3/` with `run-sec3.log`.

## Earlier attempts

The first CRC run failed in `mkdir_matches_native_stat`: native `statx`
reported nlink 2 for the new directory while the wire InodeStat had nlink 1.
The pinned `Client::fill_statx` presents a linked directory as
`2 + nsubdirs`; the test compared raw values. This was a test defect, now
compared through that rule. Cleanup of that run completed and reported the
directory left behind (`namespace_restored: false`), discarded with the
verified filesystem removal.

Two passing runs (`diag-crc2`, `diag-sec1`) used the same HEAD with an earlier
in-flight resume case that closed the stream without holding incoming bytes,
so a reply could in principle have arrived on the old connection. Two further
passing runs (`diag-crc3`, `diag-sec2`) used the final test logic before the
pure `TestCephFSCapAgentHelpers` unit test was appended to the agent file.
All four passed the nine leaves with the same cap, reply and resume behavior
but are not the acceptance record.

## Findings and limits

- Resolved after this record: `Client.MDSClientFeatures`,
  `MDSCephFSClientFeatures`, corrected status comments, `FlushJournal`
  (REQUEST_FLUSH_MDLOG) and a shared reply/push `Arrival` order now address
  the first three findings below and the ordering note.
- `cephmsgr.MDSClientFeatures()` and its doc ("the exact CephFS client
  feature bitset declared in REQUEST_OPEN") describe only the session-only
  profile. With `EnableCephFS` the declared set is `0x1c83ff`, as natively
  observed, and no public accessor reports it. `MDSSessionStatus` still
  documents that the push sequence stays zero, which is no longer true with
  `EnableCephFS` (it reached 22 here).
- Safe replies of mutations arrive only when the MDS flushes its journal,
  here up to 5 s after the unsafe reply. Native clients shorten this by
  sending `CEPH_SESSION_REQUEST_FLUSH_MDLOG` (`Client::flush_mdlog`) when they
  wait for safe replies; the library has no such session request.
- Replies (through `Wait`) and pushes (through `Next`) are separate streams;
  a consumer that needs the native arrival order between them has only the
  `Received` timestamps. No push had to be deferred in these runs.
- Not verified live: lease revoke and release, FLUSHMSG, RECALL_STATE, snap
  and quota pushes, FLUSH/FLUSH_ACK and dirty metadata, cap EXPORT/IMPORT,
  forwards, multiple ranks or failover, a resume after MDS restart (RESET),
  resume during a mutation's unsafe-to-safe window, request releases embedded
  in MClientRequest, async or replayed requests, quota and snapshots. The
  agent models a client without file data: no used, dirty or flushing caps.
- These are local selected-configuration results, separate from remote CI.
- Journal flush, fencing through the shared address nonce, a resume that
  meets a restarted MDS (RESET) and CephFS reconnect with replayed requests
  were later accepted live in
  [MDS_CEPHFS_RECOVERY_VALIDATION.md](MDS_CEPHFS_RECOVERY_VALIDATION.md).
- Dentry lease revoke and release, snapshot-realm and quota pushes,
  RECALL_STATE answered by cap releases and FLUSHMSG acknowledgement were
  later accepted live in
  [MDS_CEPHFS_PUSH_VALIDATION.md](MDS_CEPHFS_PUSH_VALIDATION.md).
- Sessions with two active ranks, a request forward resubmitted to the
  destination rank, cap EXPORT/IMPORT of a subtree migration (with the
  exporter's FLUSHMSG) and a standby takeover of rank 0 followed by a
  reconnect were later accepted live on a separate two-rank fixture in
  [MDS_CEPHFS_RANKS_VALIDATION.md](MDS_CEPHFS_RANKS_VALIDATION.md).
