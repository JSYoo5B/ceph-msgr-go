# Held FLOCK locks in a nonfinal and the final fragment of a split reconnect

Ceph Tentacle 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`, pinned
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
This is the checkpoint of issue #10 for locks on several inodes of one split
reconnect. [MDS_FILELOCK_SPLIT_RECONNECT_VALIDATION.md](MDS_FILELOCK_SPLIT_RECONNECT_VALIDATION.md)
holds one lock on an inode whose capability record a nonfinal `(more)`
message carries. Here the Go session holds two locks on two inodes: that one
and one whose capability record the final message carries. Lock blobs
therefore travel in more than one `MClientReconnect` of the same exchange.

It adds no product source and no session feature. No communication defect was
found: the first run passed in both profiles. It adds to the closed
[completion fixture](MDS_CEPHFS_COMPLETION_FIXTURE.md) one more nonblocking
exclusive lock probe, bound to the last split file and admitted only under
this acceptance's exclusive selector, and two catalog rows.

## What the pinned source says

- `Server::handle_client_reconnect` (`mds/Server.cc`) runs the same capability
  loop for every message of a split reconnect. For each capability on a
  cached auth inode it calls `recover_filelocks` with that capability's
  `flockbl`. The loop does not depend on `has_more()`; only the OPEN reply,
  `rejoin_recovered_client` and the removal from the reconnect gather set are
  reserved for the final message.
- `Server::recover_filelocks` returns at once for an empty blob. Otherwise it
  inserts the decoded records into the inode's lock state and sets each
  record's client to the session's.
- `Client::send_reconnect` (`client/Client.cc`) encodes the locks of each inode
  into that inode's capability record with `_encode_filelocks`, and starts a
  new message with `mark_more()` when the current one reaches its size bound.
  A native client whose state exceeds that bound sends lock blobs in several
  messages the same way.

## Recipe

`TestCephMDSCephFSHeldFlockSplitFragmentsIntegration` runs the split reconnect
recipe (`runCephFSSplitReconnect` in
`integration/mds_cephfs_multi_reconnect_test.go`) with two locks held. The two
`mds-cephfs-heldflock-split-fragments` rows select it with
`^TestCephMDSCephFSHeldFlockSplitFragmentsIntegration$` on a fresh completion
fixture each. The single-lock acceptance and the lock-free split recipe keep
their rows and selectors.

The two lock files are fixed by the plan, not chosen from an observed layout:
the first split file (`split-000-...`) and the last one (`split-031-...`).
Capabilities are sent in ascending inode order and the files are created in
plan order, so the first file's record sorts into the first message and the
last file's record into the last one. The test does not rely on that
expectation. It requires the placement from the replacement MDS's log (see
below) and fails when either lock is in the other kind of fragment.

1. `retain_real_capabilities_beyond_one_frame` is unchanged: the owned
   directory and the 32 plan files with 225-byte names, every create handle
   kept open read-write.
2. `hold_flock_on_split_member`: Go submits one exclusive whole-file
   SETFILELOCK on the first split file with owner `0x80000000474f0001` and one
   on the last split file with owner `0x80000000474f0003`. The two owners
   differ so that the two blobs differ: a blob attached to the other inode's
   capability would restore an owner that GETFILELOCK does not accept on
   either inode. The closed native probes `split-flock-ex-nb` (first file) and
   `split-final-flock-ex-nb` (last file) must both return EAGAIN from a client
   that is not the Go GlobalID, each on its own inode, with the native inode,
   size 0, version, mtime and empty content unchanged. A Go GETFILELOCK with a
   third owner on each inode must return that inode's own Go owner, the
   session GlobalID and type exclusive.
3. `split_reconnect_to_new_incarnation`: after the owned restart the test
   consumer sets `MDSCapReconnect.Flock` of each locked inode to the
   `Client::_encode_filelocks`-layout blob of its own lock (fcntl count 0,
   flock count 1, one 41-byte `ceph_filelock`; 49 bytes). The collected state
   must carry no blob before that, and afterwards exactly the two locked
   capabilities carry one, each decoding to its own single record. The unsplit
   size check (more than 8 KiB) and the reconnect-only 8 KiB message budget
   are unchanged, `Resubmit` and `PendingCapFlushes` are empty and SETFILELOCK
   is not submitted again. The handler lines must be ordered `(more)`
   fragments followed by one final line. The per-capability log must place the
   first file's capability in a fragment marked `(more)` and the last file's
   capability in the final fragment.
4. `held_flock_restored_then_released`, directly after OPEN, `up:active` and
   adoption: the MDS GID and nonce differ and rank stays 0. The one
   `dump_historic_ops` snapshot read right after adoption holds no
   `client_request` for either original SETFILELOCK TID. That snapshot is
   bounded, so an aged-out entry would look the same; it is recorded per lock
   as `AcquireTIDInHistoricOps` and is not proof that the new incarnation
   never received such a request. Both native probes run before any of their
   results is checked, and both must return EAGAIN with unchanged file state.
   GETFILELOCK on each inode must return that inode's original owner and the
   GlobalID. Go then unlocks the first file's lock: the native probe of the
   first file must acquire and release (errno 0), the probe of the last file
   must still return EAGAIN and GETFILELOCK on the last file must still name
   its owner. Go unlocks the last file's lock: both native probes must
   acquire and release.
5. `native_namespace_and_go_lookups_after_split` and
   `cleanup_unlink_release_close` are unchanged.

### No second acquisition

SETFILELOCK is submitted exactly four times: two acquires, then two unlocks.
Each accepted call is recorded with its TID before its reply is awaited, and
the outcome of that wait is kept separately, as in the single-lock cell. The
recipe also appends every request that `Submit` accepted to one common ledger
(name, opcode, TID) inside its only submission helper, before the reply is
awaited. At the end of step 4 the SETFILELOCK entries of that ledger must be
exactly the acquire TIDs in lock order followed by the unlock TIDs in lock
order, each call must have ended with result 0, and the submission count must
match. A lock request submitted outside the hooked call sites, a second
acquisition, a missing unlock or another order fails the leaf. The reconnect
passes no request to resubmit, and `ReconnectSession` must return no call.

## Which fragments carried the locks

The method is the one of the single-lock record: the level-15 line
`open cap realm <realm> on [inode <ino> ...` that
`Server::handle_client_reconnect` writes for each capability on a cached auth
inode, in the branch that calls `recover_filelocks` with that capability's
blob, attributed to the handler line before it. A blob is a field of the
capability record (`cap_reconnect_t::flockbl`), so it travels in the message
that carries the record.

The test requires as many fragments as handler lines with the same `(more)`
order, only inodes that were sent, each at most once and in ascending order
across the exchange, and each locked inode exactly once: the first file's in a
fragment marked `(more)`, the last file's in the unmarked final fragment. Two
locks naming one inode are refused.

What this does and does not show: the log identifies, for each locked inode,
the message whose handler reconnected its capability record. The MDS writes no
line for `recover_filelocks` itself. That each lock exists afterwards is shown
by the native probes and GETFILELOCK, and that each blob is what restored its
lock is shown by the two control runs below.

## Daemon-free checks

- `TestCephFSHeldFlockSplitFragmentsChecks` (cluster-free list): the two-lock
  recipe; blob attachment and its refusals (a state that already carries a
  blob, a missing lock inode, two locks on one inode); attribution of both
  locked inodes over two and three fragments and its refusals (either lock in
  the other kind of fragment, either inode missing or repeated, an unsent
  inode, a wrong fragment count or marker, an unsplit exchange, swapped
  locks); probe receipts of both lock files, including the receipt, action
  or mutation kind of the other file; and the submission ledger (a lock
  request outside the hooks, a missing unlock, another TID, another order, a
  count mismatch, a refused, lost or unawaited call).
- `TestCephFSHeldFlockSplitFragmentAttribution` and
  `TestCephFSHeldFlockSplitRecordsAcceptedSetFileLock` cover the single-lock
  cases on the same code.
- Fixture module: the new selector is exact; `split-final-flock-ex-nb` takes
  no argument, is bound to the last of the 32 split files and is refused under
  every other selector, including the single-lock one; the native step refuses
  a probe whose target is not its own file before it journals anything; lock
  receipts are accepted only for the two lock files and only for the path the
  driver was started with; the native driver compiles and names exactly those
  two paths.

Temporarily weakening eleven of these checks one at a time (each fragment-kind
refusal, the ledger comparison, the probe action and mutation-kind checks, the
single-record check, the shared-inode refusal, the selector gate of the new
probe, the target check of the native step, the receipt path check and the
minimum plan size) made the tests above fail each time. The count comparison
at the end of the blob attachment is implied by the checks before it and no
test depends on it.

## Runs

All runs below used Linux amd64 hosts with Docker 29.9.1 and Go 1.26.9, the
pinned image and the catalog row's parameters under the result gate
(`tools/native_test_results.py`, one expected test per row), one `go test`
process on the engine. Every source tree was uploaded as `git archive` of the
named commit, with the commit ID recorded on the host before the run. Exact-head
CI is separate from these runs.

### First runs, at `4d30b061aeef7857cfbfe4a4408d14225c4be570`

| Row | Gate | Parent | Lock leaf | Reconnect leaf | Restored-lock leaf |
| --- | --- | --- | --- | --- | --- |
| CRC/AES IPv4 host BlueStore | passed | 64.86 s | 5.40 s | 25.76 s | 19.21 s |
| secure/aes256k IPv6 container MemStore | passed | 64.38 s | 5.40 s | 26.52 s | 18.51 s |

These were the first runs of the acceptance and all six leaves passed in
both. Both observed 34 capabilities and one realm, 9,539 bytes unsplit, two
fragments with 29 and 5 native capability lines, the first file's inode
`0x10000000001` in fragment 0 (`(more)`) and the last file's inode
`0x10000000020` in fragment 1 (final), each with a 49-byte blob. Native probe
errnos were EAGAIN on both files before the restart and after it on a new MDS
GID, then 0 and EAGAIN after the first unlock, then 0 and 0 after the second.
The native version of both lock files was 0 throughout.

### Control runs by temporary mutation

Each control is the tree of `4d30b061aeef7857cfbfe4a4408d14225c4be570` with
one six-line insertion in `runCephFSSplitReconnect`, directly after the blob
attachment and its checks, that clears `Flock` on the capability of one
locked inode. The insertions were applied to the uploaded tree as a patch and
were never committed.

| Control | Row | Result |
| --- | --- | --- |
| Blob dropped from the last file's capability (the final fragment) | CRC/AES IPv4 host BlueStore | Gate failed as intended. The lock and reconnect leaves passed (9,490 bytes unsplit, the same two fragments with 29 and 5 capability lines). `held_flock_restored_then_released` failed at the probe check after adoption with errnos EAGAIN and 0: the native client acquired and released the last file, while the first file was still refused. |
| Blob dropped from the first file's capability (the nonfinal fragment) | secure/aes256k IPv6 container MemStore | Gate failed as intended, in the same leaf at the same check, with errnos 0 and EAGAIN: the native client acquired and released the first file, while the last file was still refused. |

Each control is one run in one profile. Together with the passing runs they
show that each lock is restored by the blob on its own capability record, in
the nonfinal and in the final message independently, and that a blob in one
message does not restore the other inode's lock.

### At `8b8889eb79e839c57194b596dab22c70b0dae39b`

Source `8b8889eb79e839c57194b596dab22c70b0dae39b` holds the fixture, test and
catalog of this change; it differs from the first-run source by one code
comment, and later commits of the change touch documentation only. Each row
ran once, in sequence per host, and passed its gate with no failed, skipped or
missing selected test.

| Row | Gate | Parent | Lock leaf | Reconnect leaf | Restored-lock leaf |
| --- | --- | --- | --- | --- | --- |
| `mds-cephfs-heldflock-split-fragments`, CRC/AES IPv4 host BlueStore | passed | 64.46 s | 5.40 s | 25.94 s | 18.61 s |
| `mds-cephfs-heldflock-split-fragments`, secure/aes256k IPv6 container MemStore | passed | 64.49 s | 5.30 s | 26.72 s | 18.51 s |
| `mds-cephfs-heldflock-split-reconnect`, CRC/AES IPv4 host BlueStore | passed | 51.70 s | 2.70 s | 26.65 s | 7.90 s |
| `mds-cephfs-heldflock-split-reconnect`, secure/aes256k IPv6 container MemStore | passed | 50.57 s | 2.70 s | 25.92 s | 7.90 s |

Both two-lock rows repeated the values of the first runs: 9,539 bytes
unsplit, fragments of 29 and 5 capability lines, inode `0x10000000001` in
fragment 0 (`(more)`) and inode `0x10000000020` in fragment 1 (final), native
errnos EAGAIN and EAGAIN after the restart, 0 and EAGAIN after the first
unlock, 0 and 0 after the second. Both single-lock rows observed 9,490 bytes
unsplit, the same fragments, the locked inode in fragment 0, EAGAIN after the
restart and 0 after the unlock, now with the submission-ledger check.

The other rows that share the touched code passed their gates at the same
source: both `mds-cephfs-multi-reconnect` rows (the lock-free recipe through
the shared runner, 37.93 s and 38.17 s) and both `mds-cephfs-completion` rows
(the fixture whose native driver and lock-receipt decoder changed, 225.71 s
and 226.53 s).

No run of this acceptance failed other than the two intended controls.

## Changes to the merged single-lock cell

`TestCephMDSCephFSHeldFlockSplitReconnectIntegration` keeps its selector,
rows, lock file, owner, probe, fragment requirement and leaf names, and runs
through the same generalized code:

- Its record is now a list with one lock. The per-lock fields (`File`, `Ino`,
  `Lock`, the TIDs and outcomes, the blob, the fragment index, the probes and
  GETFILELOCK results, `AcquireTIDInHistoricOps`) moved under
  `facts.held_flock.Locks[0]`, and `LockFragment` is now `Fragment` there.
- It gained the submission-ledger check described above. Before, it counted
  only the calls made at its two hooked call sites.
- Its probe receipt check additionally requires the receipt's action name and
  the mutation kind of its own probe.
- The closed native driver of the completion fixture admits two fixed lock
  paths instead of one. Under the single-lock selector the fixture can still
  only request the first one: the second probe action is refused there.

## Limits

- Two exclusive whole-file FLOCK locks of two owners of one client session on
  two predetermined inodes, two fragments, one lock in each. More than one
  lock per fragment, more than two fragments in a live run, several records
  in one blob combined with a split, shared or mixed lock types, waiting
  locks, FCNTL byte ranges, several clients and interrupt races are not
  exercised. The pinned Python binding exposes only `flock`, so the native
  probe cannot take a byte-range lock.
- No dirty cap state, request replay or pending cap flush accompanies the
  reconnect. Connection loss between fragments, standby takeover, multi-rank
  split reconnect and crash durability of lock state are not exercised.
  In-memory lock state is not a journal durability promise.
- The no-blob comparisons exist only as the two temporary mutation runs above,
  one profile each, not as retained leaves: the test holds its locks across
  the fixture's one restart.
- A lost SETFILELOCK reply has daemon-free accounting coverage only.
- The blobs are an explicit caller choice. The product does not infer held
  locks from capabilities and does not populate reconnect state with locks.
- A passing leaf does not establish POSIX lock semantics or a typed lock API.
- The detailed ledger (`facts.held_flock` and `submitted` in
  `mds-cephfs-heldflock-split-fragments-results.json`) is written inside the
  job and is not uploaded for a successful run (issue #1).

## Source and licensing

Wire and daemon facts are independently restated from LGPL-2.1 sources at the
pinned commit: `messages/MClientReconnect.h`, `mds/mdstypes.h`
(`cap_reconnect_t`), `mds/Server.cc` (`handle_client_reconnect`,
`recover_filelocks`), `mds/CInode.cc` (inode log form) and
`client/Client.cc` (`send_reconnect`, `_encode_filelocks`). No upstream code
is incorporated. Native tools and libcephfs remain development dependencies.
