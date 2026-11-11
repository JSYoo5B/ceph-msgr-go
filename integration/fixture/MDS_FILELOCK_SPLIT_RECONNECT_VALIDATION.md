# Held FLOCK on an inode of a split reconnect

Ceph Tentacle 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`, pinned
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
This is the held-lock-in-split-reconnect checkpoint of issue #10. It composes
the split reconnect of
[MDS_MULTI_RECONNECT_VALIDATION.md](MDS_MULTI_RECONNECT_VALIDATION.md) with the
held whole-file FLOCK of
[MDS_FILELOCK_RESTART_VALIDATION.md](MDS_FILELOCK_RESTART_VALIDATION.md). Those
two records establish split receipt and ordinary held-lock recovery
separately; neither gives conflict evidence for an inode that is part of a
split reconnect, because the existing native lock probe admits only the
metadata fixture's `go-file`.

It adds no product source and no session feature. It adds to the closed
[completion fixture](MDS_CEPHFS_COMPLETION_FIXTURE.md) one nonblocking
exclusive lock probe bound to one predetermined split file, a level-15 debug
window and a per-capability reconnect log read, all admitted only under this
acceptance's exclusive selector, and two catalog rows.

## Recipe

`TestCephMDSCephFSHeldFlockSplitReconnectIntegration` runs the split reconnect
recipe (`runCephFSSplitReconnect` in
`integration/mds_cephfs_multi_reconnect_test.go`) with one lock held. The two
`mds-cephfs-heldflock-split-reconnect` rows select it with
`^TestCephMDSCephFSHeldFlockSplitReconnectIntegration$` on a fresh completion
fixture each. The existing `TestCephMDSCephFSMultiReconnectIntegration` runs
the same recipe without a lock and keeps its rows, selector and level-10 debug
window.

1. `retain_real_capabilities_beyond_one_frame` is unchanged: the Go session
   creates the owned directory and the 32 plan files with 225-byte names and
   keeps every create handle open read-write.
2. `hold_flock_on_split_member`: Go submits one exclusive whole-file
   SETFILELOCK with the fixed owner `0x80000000474f0001` on the first split
   file (`split-000-...`, the plan's predetermined lock file). The closed
   native `split-flock-ex-nb` probe must return EAGAIN from a client that is
   not the Go GlobalID, on the same inode, with the native inode, size 0,
   version, mtime and empty content unchanged. A Go GETFILELOCK with a
   different owner must return the Go owner, the session GlobalID and type
   exclusive.
3. `split_reconnect_to_new_incarnation`: after the owned restart the test
   consumer sets `MDSCapReconnect.Flock` of exactly that inode to the
   `Client::_encode_filelocks`-layout blob of the unsplit case (fcntl count 0,
   flock count 1, one 41-byte `ceph_filelock`). No other capability carries a
   blob. The unsplit size check (more than 8 KiB) and the reconnect-only 8 KiB
   message budget are unchanged, `Resubmit` and `PendingCapFlushes` are empty
   and SETFILELOCK is not submitted again. The replacement MDS's
   `handle_client_reconnect` lines for this GlobalID must be ordered `(more)`
   fragments followed by one final line, as in the split recipe. The
   per-capability log (below) must place the locked inode's capability in a
   nonfinal fragment.
4. `held_flock_restored_then_released`, directly after OPEN, `up:active` and
   adoption: the MDS GID and nonce differ and rank stays 0; the new
   incarnation's historic ops hold no `client_request` for the original
   SETFILELOCK TID; the native probe must again return EAGAIN on the same
   inode with the same native file state; GETFILELOCK on the reconnected
   session must return the original owner and GlobalID; Go unlocks once; the
   native probe must then acquire and release (errno 0). No second Go lock
   acquisition is made anywhere in the test: SETFILELOCK is submitted exactly
   twice, the acquire and the unlock.
5. `native_namespace_and_go_lookups_after_split` and
   `cleanup_unlink_release_close` are unchanged: native listing and 32 Go
   LOOKUPs, no OSD dial, then unlink, rmdir, cap release, confirmed CLOSE and
   absence from native `session ls`.

The blob builder is the test-only one of
`integration/mds_cephfs_heldflock_test.go`, which the cluster-free
`TestCephFSHeldFlockBlobMatchesNativeLayout` compares byte for byte with a
native-encoded blob. The probe and attribution checks are test-only consumer
code in `integration/mds_cephfs_heldflock_split_test.go`.

## Which fragment carried the lock

The handler lines of the split recipe give fragment order only. This
acceptance also identifies the fragment that carried the locked inode's
capability record, from the replacement MDS's own log:

- A lock blob is a field of the capability record (`cap_reconnect_t::flockbl`
  in `mds/mdstypes.h`), so it travels in the message that carries that
  inode's capability.
- `Server::handle_client_reconnect` (`mds/Server.cc`) logs its handler line
  with the `(more)` marker, then walks the capabilities of that one message.
  For a capability on a cached auth inode it writes `open cap realm <realm>
  on [inode <ino> ...` at level 15 and, in the same branch, calls
  `CInode::reconnect_cap`, `MDCache::add_reconnected_cap` and
  `recover_filelocks` with that capability's blob. That line is written
  nowhere else in the MDS.
- The fixture's `debug-caps-on` sets `debug_mds 15/15` on the replacement
  daemon while it is in `up:reconnect`, and `reconnect-cap-log` returns this
  client's handler lines and the capability lines that follow each of them, up
  to the next reconnect handler line of any client.

The test requires as many fragments as handler lines with the same `(more)`
order, only inodes that were sent, each at most once and in ascending order
across the exchange, and the locked inode exactly once, in a fragment marked
`(more)`. The cluster-free `TestCephFSHeldFlockSplitFragmentAttribution`
covers the parser and the refusals (a lock in the final fragment, a missing
or repeated inode, an inode that was not sent, a wrong fragment count or
marker, and wrong probe receipts).

What this does and does not show: the log identifies the message whose handler
reconnected the locked inode's capability record, and that record is where the
blob is. The MDS writes no log line for `recover_filelocks` itself, so the log
does not show the lock being inserted. That the lock exists afterwards is shown
by the native probe and by GETFILELOCK, not by the log.

## Runs

All runs below used a Linux amd64 host with Docker 29.9.1, the pinned image
and the catalog row's parameters under the result gate
(`tools/native_test_results.py`), one `go test` process on the engine.

### Before the first commit

| Run | Source | Result |
| --- | --- | --- |
| First run, CRC/AES IPv4 host BlueStore row | Working tree on `f9610126d0aaa4db341bf5f014274eda33839554`, before the first commit of this change | Gate passed; all six leaves passed at the first attempt (parent 51.44 s; lock leaf 2.80 s, reconnect leaf 25.95 s, restored-lock leaf 8.00 s). 34 capabilities and one realm, 9,490 bytes unsplit, two fragments; 29 and 5 native capability lines; the locked inode `0x10000000001` in fragment 0 (`(more)`). |
| Control run by temporary mutation, same row and tree | The same tree with one line changed so that the blob is built but not attached to the capability; reverted afterwards and never committed | Gate failed as intended. The lock leaf and the reconnect leaf passed (9,441 bytes unsplit, the same two fragments with 29 and 5 capability lines, the locked inode's capability again in fragment 0). `held_flock_restored_then_released` failed at its first check: the native probe on the same inode acquired and released the lock (errno 0) where EAGAIN was required. |

The control run is the restart-without-blob comparison for this fixture: the
same lock, restart and fragments, and only the blob missing. With the blob the
replacement MDS refuses the native lock; without it the native client acquires.
It is one run in one profile, made by a temporary edit rather than a retained
leaf, because the test holds one lock across the fixture's one restart.

In both runs the native `statx` version of the never-written lock file was 0
before and after the restart. That is why this probe compares the version
instead of requiring a nonzero one. The unsplit record's first rejected probe
(a shape check that requires a nonzero version, on a different file and
fixture) remains unexplained there; this observation does not establish its
cause.

No run of this acceptance failed other than the intended control run.

## Limits

- One exclusive whole-file FLOCK of one owner on one predetermined inode, one
  client session, two fragments, the lock in the first one. A lock carried by
  the final fragment, locks on several inodes or in several fragments, shared
  locks, waiting locks, FCNTL byte ranges, several owners or clients and
  interrupt races are not exercised.
- No dirty cap state, request replay or pending cap flush accompanies the
  reconnect. Connection loss between fragments, standby takeover, multi-rank
  split reconnect and crash durability of lock state are not exercised.
  In-memory lock state is not a journal durability promise.
- The level-15 window is specific to this acceptance. The lock-free split
  recipe still runs at level 10 and identifies fragment order only.
- The blob is an explicit caller choice. The product does not infer held locks
  from capabilities and does not populate reconnect state with locks.
- A passing leaf does not establish POSIX lock semantics or a typed lock API.
- The detailed ledger (`facts.held_flock` in
  `mds-cephfs-heldflock-split-reconnect-results.json`: TIDs, GlobalID, MDS
  identities, blob hex, probe receipts, the per-capability lines and parsed
  fragments) is written inside the job and is not uploaded for a successful
  run (issue #1).

## Source and licensing

Wire and daemon facts are independently restated from LGPL-2.1 sources at the
pinned commit: `messages/MClientReconnect.h`, `mds/mdstypes.h`
(`cap_reconnect_t`), `mds/Server.cc` (`handle_client_reconnect`,
`recover_filelocks`), `mds/CInode.cc` (inode log form) and
`client/Client.cc` (`_encode_filelocks`). No upstream code is incorporated.
Native tools and libcephfs remain development dependencies.
