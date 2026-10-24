# Interrupted CephFS writeback across restart, revoke and eviction

Reference: Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. This is the second checkpoint
of issue #10. It reuses the owned [data fixture](MDS_CEPHFS_DATA_FIXTURE.md)
and its single owned restart, like the
[cap-send recovery](MDS_CEPHFS_CAPSEND_RECOVERY_VALIDATION.md) recipe.

## Product contracts under test

No product code changed. The recipe exercises three existing contracts on
file data:

- `OSDConnection.Request` ends an admitted request whose local wait ends
  before a reply with `OutcomeUnknownError` and never retries it
  (`cephmsgr/osd_request.go`).
- An `EnableCephFS` client uses one address nonce (`State.CephFSNonce`) for
  every MDS and OSD connection. An MDS eviction blocklists `ip:0/nonce`, and
  the OSD then answers that client's object requests with `-EBLOCKLISTED`
  (`cephmsgr/MDS_CEPHFS_CONTRACT.md`, "Shared client address instance").
- `SendCaps` flushes only the metadata the caller chooses.

Which writes count as completed, the size a metadata flush may report and
what recovery may resend are decisions of the test-only consumer
(`integration/mds_cephfs_writeback_test.go`):

- an object write is `completed` only with result 0 and ONDISK, `uncertain`
  on `OutcomeUnknownError`, `fenced` on `-EBLOCKLISTED` and `refused`
  otherwise;
- a metadata flush reports at most the end of the file range that completed
  writes cover without a gap;
- a completed write is never repeated, and an uncertain write is never
  resent automatically;
- a buffered writeback that the revoke callback starts and that does not
  complete is latched (`cephfsWritebackLatch`): the data stays buffered, so
  FILE_BUFFER stays in use, later pushes never restart the callback, new
  writes, fsync flushes and cap snaps of that data are refused, and only an
  explicit decision resolves it. `evictBuffered` discards the data and
  releases a revoked Fb; `recoverBuffered` writes chosen data again only
  when every unfinished extent is known not applied, never when one is
  uncertain. The latch keeps each extent's outcome.

## Acceptance recipe

Selector: `^TestCephMDSCephFSWritebackFenceIntegration$`. Every other leaf
is shared with `^TestCephMDSCephFSDataIntegration$`. This selector adds two
leaves in order: `revoke_time_writeback_interrupted_latched_and_evicted`
(below) and the restart leaf
`interrupted_writeback_fenced_across_mds_restart_and_eviction`. With
the plan layout (64 KiB stripe unit, two 128 KiB objects per period),
generation A covers objects 0 and 1. B's tail (bytes 200000..220000) and C's
tail (bytes 220000..240000) each fall into one extent of object 1.

1. The leaf creates `capsnap-file` with Fw and a max size of 240000. It
   writes generation A, 200000 bytes, as two object writes. Both must be
   `completed`. The consumer then marks only the committed end dirty and
   sends an UPDATE with `FLAG_SYNC`. Its FLUSH_ACK must match the TID, inode,
   dirty Fw and cap ID: this is the accepted metadata flush.
2. `osd-drop-on` sets `debug_ms=10/10` and then `ms_blackhole_client=true`
   on the single owned OSD, each read back. The consumer writes B's tail with
   a 5 s local wait. The write must end `uncertain`, with its submission
   identity (GlobalID, map epoch) kept. `osd-log` must return exactly one
   attributed pair: the OSD's ProtocolV2 `received message ... from=client.<gid>
   type=42 osd_op()` line and the same thread's `handle_message blackhole`
   line, both on the connection from the registered peer nonce. The message
   was dropped after Messenger receipt and before dispatch. `osd-drop-off`
   restores and reads back both baselines. The committed end stays 200000,
   the agent holds no dirty or flushing metadata, and Go STAT and the native
   object view both show A's object sizes, so B did not reach its object.
3. The owned rank-0 restart fires. The session ends Lost with SESSION_RESET.
   The client reconnects to the new GID, nonce and incarnation with its caps
   and realms and chooses no flush. Adoption follows `up:active`. The Go
   object-request count is unchanged across the restart, so A is not
   repeated and B is not retried. Native `statx` must report size 200000 and
   A's mtime: the accepted flush survived the restart.
4. A native LibCephFS read of the file must return A's 200000 bytes (SHA-256
   and every 16 KiB chunk). At least one capability revoke must have reached
   the writer after B became uncertain, and no cap update after the accepted
   flush may carry dirty metadata.
5. `evict` runs `session evict id=<gid>` on the owned MDS admin socket with
   `mds_session_blocklist_on_evict=true` read back. The receipt must name the
   observed session address, and that exact address must be blocklisted.
   The Go session must end Lost. The consumer then writes C's tail through
   the same client's existing OSD connection, using the current OSDMap
   epoch. The write must end `fenced` (`-EBLOCKLISTED`), so it is known not
   applied. The resend rule must refuse A and B and admit only the fenced
   write.
6. A new client instance (new GlobalID and nonce, same identity) opens a
   session. Go STAT and the native object view must still show A's object
   sizes, so neither B nor C landed. A Go LOOKUP must report size 200000 and
   A's mtime. Go READ of the head and a native read must both return A, and
   native `statx` must report size 200000 and A's mtime. The leaf must have
   issued exactly four object writes: two completed, one uncertain and one
   fenced. The shared cleanup leaf then runs with the new session, and
   fixture cleanup removes the eviction's blocklist entry as an owned-session
   entry.

When the revoke-time leaf ran first, step 5 also writes new probe bytes to
that leaf's B extent of `shared-file` through the evicted instance's existing
OSD connection. The OSD must answer `-EBLOCKLISTED` (`fenced`); B's uncertain
write itself is never issued again. Step 6 then requires a native read and a
Go head read of `shared-file` to return the A-branch image, and the restart
leaf to have issued exactly five object writes.

### Revoke-time writeback interrupted inside the callback

The leaf reuses `shared-file` after the shared concurrent-reader leaf
closed it (200000 bytes of the plan payload). A overwrites file range
0..65536, one extent of object 0; B overwrites 65536..131072, one extent of
object 1. Both stay inside the file, so the file size cannot distinguish the
branches; their bytes do.

1. The client opens the file read-write and waits for Fwb. A is written
   with a 30 s wait and must be `completed` (result 0, ONDISK). The consumer
   marks the file dirty and sends an UPDATE with `FLAG_SYNC`; its FLUSH_ACK
   must match the TID, inode, dirty Fw and cap ID.
2. `observe` must show the session address carrying the shared nonce. B is
   registered as buffered data whose only writeback is the revoke callback,
   which holds it for 15 s before writing it through the ledger with a 5 s
   wait.
3. `reader-start` starts the native `read-shared` reader in the background
   while no drop is active. LibCephFS checks pool permission with its own
   OSD operations (`Client::check_pool_perm`) before it waits for
   capabilities, so those operations must not meet the drop. Its open makes
   the MDS revoke FILE_BUFFER; the REVOKE first flushes the dirty metadata
   with Fb still implemented. Once that revoke is recorded, `osd-drop-on`
   sets and reads back the OSD drop (the run's first), inside the
   callback's hold. The callback then writes B, and its writeback must have
   begun after the drop was applied. The write must end `uncertain` with its
   submission identity (GlobalID, map epoch), object, offset, length and
   request digest kept, and the latch must be `uncertain` from the
   `revoke-callback` with B's single extent outcome. `osd-log` must return
   exactly one attributed receipt/blackhole pair for this client.
   `reader-status` must report the reader still running.
4. `osd-drop-off` restores and reads back both baselines. After a further
   3 s the reader must still be running, the ledger and the Go object log
   must hold exactly A and one B write, the latch must be unresolved and
   FILE_BUFFER must still be implemented.
5. The consumer's explicit decision `evictBuffered` sends an UPDATE without
   Fb. `reader-wait` must return the reader's terminal result: SHA-256 and
   every 16 KiB chunk of the A-branch image (not A+B) and native size
   200000. The native read time must exceed the interval between the drop's
   restoration and the decision, so the reader was pending while the
   obligation was held.
6. Go STAT and the native object view must show the unchanged object sizes,
   and Go READ of the head must return the A-branch image. Resolved must hold
   one `evicted` latch that kept the `uncertain` outcome. Every cap update
   after buffering and before the decision must carry Fb, the first one
   after it must not, exactly one revoke-time writeback must be recorded
   (failed), the resend rule must still refuse B and the leaf must have
   issued exactly two object writes.

Metadata is not required to stay at A: the revoke's metadata flush can
report B's mtime before the data completes, and FLUSH_ACK is not
data-writeback completion. The native size here equals the existing file
size by construction.

Native basis (LGPL-2.1 references, no native code copied):
msg/Connection.cc `is_blackhole` (client peers only); msg/async/ProtocolV2.cc
`handle_message` (the debug 5 receipt line and the blackhole branch after
`in_seq` accounting) and `_conn_prefix`; messages/MOSDOp.h `print` (an
undecoded op prints `osd_op()`); osd/PrimaryLogPG.cc `do_op`
(`-EBLOCKLISTED`); mds/MDSRank.cc `evict_client`; mds/Server.cc
`kill_session`; client/Client.cc `_write`, `check_caps` and `send_cap`.

## Fixture controls

`osd-drop-on`, `osd-drop-off`, `osd-log`, `reader-start`, `reader-status`,
`reader-wait` and `evict` are refused unless the profile's selector is the
writeback-fence selector. The OSD drop is issued at most twice per run, the
second only after the first is restored, never during a restart or beside an
MDS journal pause or client blackhole. The background reader starts once,
only while no OSD drop is active; while it runs, other native work,
eviction, MDS faults and restart arm are refused, an OSD drop may follow,
and its result is collected once after that drop is restored. Cleanup waits for a running reader before its native readback. Symmetrically, `log-pause`, `blackhole-on` and
`client-drop-on` are refused while the OSD drop is in any active or uncertain
state; the debug change and owned restorations stay available. Native
LibCephFS work is refused while it is active (including intended
and uncertain states). Its baseline must report `ms_blackhole_client=false`.
Cleanup restores an override that is still active and reads it back, before
any native namespace check. The eviction is issued once, only for a session
of the owned Go identity whose address an earlier `observe` registered, and
only while no restart is pending and no client blackhole is active. See
[MDS_CEPHFS_DATA_FIXTURE.md](MDS_CEPHFS_DATA_FIXTURE.md).

## Native results

### Revoke-time writeback leaf (2026-10-24)

Tested PR head `0f81810287d6a60bac1471296322cfb3d50b5f4c` (#80) through the
trusted default-branch `/ci run` workflow:
[run 40033958831](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/40033958831).
GitHub-hosted Ubuntu runners, image `quay.io/ceph/ceph:v20.2.4`. Every
scenario job checked out that head; Basic PR CI passed at it
([run 40033946708](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/40033946708)).

| Configuration | Entire test | Revoke-time leaf | Restart leaf | Cleanup leaf | Result |
| --- | --- | --- | --- | --- | --- |
| IPv4 host, CRC, AES client/service keys, single BlueStore | 232.76 s | 48.00 s | 65.09 s | 22.06 s | 13/13 leaves PASS |
| IPv6 container, secure, AES256K client/service keys, single MemStore | 292.37 s | 55.41 s | 82.99 s | 28.42 s | 13/13 leaves PASS |

The same run passed both `mds-cephfs-data` and both `mds-cephfs-capsend`
rows at that head (12/12 leaves each), so the shared leaves and the other
restart recipes are unaffected.

An earlier head, `eb38ad2dda7b70950127df4b4fd68f783ab869db`, started the
reader behind the applied drop
([run 40032171886](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/40032171886)).
In both profiles the leaf latched B as uncertain, the reader stayed pending
through the drop and its restoration, and the eviction UPDATE released Fb,
but the reader then timed out after 75 s and the MDS sent the writer no
further cap message. Reading `Client::check_pool_perm` suggests that the
reader's pool-permission OSD operations met the drop; the native logs that
would confirm it were not retained. The later pass does not prove that
explanation. It shows only that the reordered recipe works.

### Restart leaf (2026-10-23)

Tested PR head `20b47052b4acd2ebedbd9df967c08154dda6fe08` on main
`a617c2305b7abbcc4d8bd16b029ba5a8158cf6d7`, through the trusted
default-branch workflow:
[run 39902496939](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39902496939).
GitHub-hosted Ubuntu runners, image `quay.io/ceph/ceph:v20.2.4`. Basic PR CI
also passed at this head
([run 39902488705](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39902488705)).

| Configuration | Entire test | Writeback-fence leaf | Cleanup leaf | Result |
| --- | --- | --- | --- | --- |
| IPv4 host, CRC, AES client/service keys, single BlueStore | 239.73 s | 81.62 s | 29.23 s | 12/12 leaves PASS |
| IPv6 container, secure, AES256K client/service keys, single MemStore | 234.17 s | 77.98 s | 28.52 s | 12/12 leaves PASS |

The result gate required a complete PASS of the selected top-level test in
both jobs, and the job logs show the checkout of the exact head. Every
assertion listed in the recipe executed and passed. The per-run public
evidence file (`mds-cephfs-data-results.json`, with the write ledger,
submission identities, OSD drop lines, revoke events and eviction receipt)
is written inside the fixture control directory. Successful CI runs do not
archive it, so exact TIDs, GIDs, epochs and log lines are not reproduced
here.

## Evidence kinds

- Synthetic: consumer outcome classes, committed-end bound and resend rule
  (`TestCephFSWritebackLedgerClassifiesAndBoundsMetadata`); the writeback
  latch under repeated revokes, refusals, eviction, known-unapplied recovery,
  classification and reconnect handoff (`TestCephFSRevokeWritebackLatch*`);
  fixture selector admission, request shapes, OSD drop admission (including
  the second drop), background reader admission and exclusion, control
  refusals and OSD drop-log attribution
  (`integration/fixture/mds_cephfs_data_writeback_test.go`).
- Live native interoperability: the CI jobs above.
- No native codec vectors changed.

## Limits

- The interruption is a drop injected after OSD Messenger receipt. A frame
  lost before the OSD received it, a transport failure between admission and
  write, and a write that applied while its reply was lost are not
  reproduced here. The native object view resolves B as not applied; the
  consumer still treats it as uncertain.
- The restart leaf never marks unconfirmed data dirty. Native
  `Client::_write` with buffered data can report a size before its
  writeback completes; the revoke-time leaf keeps B inside the existing file
  size, so a buffered size beyond the committed end is not exercised.
- The revoke-time leaf interrupts one extent with the same post-receipt
  drop and resolves it only by eviction. Explicit recovery of a
  known-unapplied latch is covered by cluster-free tests only, and a
  revoke-time writeback that partially applies is not reproduced.
- One writer, one object of each interrupted range, a single OSD and the
  default `mds_session_blocklist_on_evict` were exercised. MDS file-size
  recovery after an eviction that leaves a partially applied range, multiple
  writers and persistence across an OSD restart (MemStore) remain outside
  this recipe.
- Large reconnect state ([MDS_MULTI_RECONNECT_VALIDATION.md](MDS_MULTI_RECONNECT_VALIDATION.md))
  and held-lock recovery ([MDS_FILELOCK_RESTART_VALIDATION.md](MDS_FILELOCK_RESTART_VALIDATION.md))
  are separate checkpoints.
