# FCNTL byte-range locks across an owned MDS restart

Ceph Tentacle 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`, pinned
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
This is the FCNTL part of the held-lock checkpoint of issue #10. It runs the
unsplit held-lock recipe of
[MDS_FILELOCK_RESTART_VALIDATION.md](MDS_FILELOCK_RESTART_VALIDATION.md) with
two FCNTL byte-range locks and one FLOCK lock on one owned file. The earlier
held-lock cells sent reconnect blobs whose FCNTL count was 0, and their native
probes could only take whole-file FLOCK locks. This cell sends a blob with
records in both sections and observes the restored ranges with native record
locks.

It adds no product source and no session feature. The product carries
`MDSCapReconnect.Flock` as opaque bytes; the lock arguments, the blob and the
expected results are test-only consumer code.

## Native FCNTL probe

The pinned Python binding offers `LibCephFS.flock` only. The metadata
fixture's closed action `fcntl-probe` therefore opens a second native mount
through `ctypes` on the image's `libcephfs.so.2` (`ceph_create`,
`ceph_conf_read_file`, `ceph_conf_set`, `ceph_select_filesystem`,
`ceph_mount`, `ceph_open`, `ceph_getlk`, `ceph_setlk`, `ceph_close`,
`ceph_shutdown` in `include/cephfs/libcephfs.h`), with the same native
identity as the FLOCK probes. The fixture owns the request set
(`mdsCephFSFcntlProbes` in `mds_cephfs.go`); the caller selects only the
action. For each of seven requests the driver sends `ceph_getlk`, then a
nonblocking `ceph_setlk` and, if that acquired, a `ceph_setlk` unlock of the
same range. `Client::_do_filelock` sets the owner's high bit itself, so the
native owner is `0x8000000063657068`, the owner of the FLOCK probes.

| Request | Type | Range | Purpose |
| --- | --- | --- | --- |
| 0 | write | byte 0 | first byte of the exclusive range |
| 1 | write | byte 15 | last byte of the exclusive range |
| 2 | write | bytes 16 to 31 | the gap between the ranges |
| 3 | read | bytes 32 to 47 | a shared request over the whole shared range |
| 4 | write | byte 47 | last byte of the shared range |
| 5 | write | byte 48 to the end of the file | the request after the shared range (length 0) |
| 6 | read | byte 8 | a shared request inside the exclusive range |

Each result records the request, the errno (0 or EAGAIN; any other error
fails the native process), whether the range was acquired and released, and
the lock that `ceph_getlk` reported: the conflicting lock's type, start,
length and pid, or `F_UNLCK` with the request's own range and pid 0 when
nothing conflicts (`ceph_lock_state_t::look_for_lock` in `mds/flock.cc`,
`Client::_do_filelock` in `client/Client.cc`). The first mount reads the
file's inode, size, version, mtime and bytes before and after the requests.
The fixture refuses a result that is not this request set in this order, a
conflict that does not overlap its request or is between two shared locks,
an acquire without its release, one mount for both roles, or a changed file.
Like the FLOCK probes, each run is journaled as a native mutation before it
starts and is never replayed after an uncertain outcome. The capability
`mds-held-fcntl` alone admits the action.

## Recipe

`TestCephMDSCephFSFcntlRangeRecoveryIntegration` runs `runCephFSRecovery`
(`integration/mds_cephfs_recovery_test.go`) with the lock set
`fcntlHeldLocks` (`integration/mds_cephfs_heldfcntl_test.go`). The two
`mds-cephfs-fcntl-recovery` rows select it with
`^TestCephMDSCephFSFcntlRangeRecoveryIntegration$` and the capability
`mds-held-fcntl`, each on a fresh metadata fixture, because the recipe
performs the fixture's only owned restart. The broad `mds-cephfs` selector
does not match it. Every leaf of the recovery recipe runs; only the held-lock
steps below differ from the exclusive case.

1. `held_flock_acquired_before_restart`: the Go recovery session creates the
   plan directory and `go-file`, sets and natively compares the xattr and
   keeps a read-write handle, as in the exclusive case. It then submits three
   SETFILELOCK requests in order: FCNTL exclusive, bytes 0 to 15, owner
   `0x80000000474f0001`, pid 4101; FCNTL shared, bytes 32 to 47, owner
   `0x80000000474f0003`, pid 4103; FLOCK exclusive, whole file, owner
   `0x80000000474f0005`. The three owners, the GETFILELOCK test owner
   `0x80000000474f0002` and the native owner are all distinct. Each call is
   recorded with its TID as soon as `Submit` accepted it. With all three
   granted, the native `flock-ex-nb` probe must return EAGAIN and the native
   FCNTL probe must give EAGAIN for requests 0, 1 and 6 with the exclusive
   range (write, 0, 16, pid 4101) as the conflict, EAGAIN for request 4 with
   the shared range (read, 32, 16, pid 4103), and an acquire and release for
   requests 2, 3 and 5. The native inode, size 0, version, mtime and empty
   content must not change across all probes. A Go GETFILELOCK of a
   whole-file exclusive FCNTL test lock must return the exclusive range, and
   one of a FLOCK test lock the FLOCK lock, each with the session GlobalID.
2. `mds_restart_reconnect_replay` runs unchanged until the reconnect state is
   built. The test consumer then sets `MDSCapReconnect.Flock` of that one
   inode to a `Client::_encode_filelocks`-layout blob: FCNTL count 2 with the
   two ranges in start order, then FLOCK count 1 with the FLOCK record (131
   bytes). No other capability carries a blob, and no SETFILELOCK is
   submitted again.
3. After OPEN, `up:active` and adoption, still in the restart leaf: the MDS GID
   and nonce differ and rank stays 0; the one bounded `dump_historic_ops`
   snapshot read right after adoption holds no `client_request` for any
   acquire TID; the native FLOCK and FCNTL probes must give the results of
   step 1 again; both GETFILELOCK answers must again name the exclusive range
   and the FLOCK lock.
4. Go unlocks the three owners in acquisition order, each over its own rule,
   pid and range. After the first unlock the FCNTL probe must acquire
   requests 0, 1 and 6 and still refuse request 4 with the shared range as
   the conflict, the FLOCK probe must still return EAGAIN, and GETFILELOCK
   must name the shared range and the FLOCK lock. After the second unlock
   every FCNTL request must acquire, the FLOCK probe must still return EAGAIN,
   the FCNTL GETFILELOCK must return the unlocked test lock (no conflict) and
   the FLOCK GETFILELOCK the FLOCK lock. After the third the FLOCK probe must
   acquire. The record must hold exactly the three acquires before the
   restart and the three unlocks after it, each answered with result 0, and
   the accepted SETFILELOCK TIDs of the recipe's common request ledger must
   be exactly those six calls in that order.
5. The cleanup leaf is unchanged.

FCNTL and FLOCK locks are separate lock states of the MDS inode
(`get_fcntl_lock_state` and `get_flock_lock_state` in `mds/CInode.h`;
`Server::recover_filelocks` fills each from its section of the blob), so the
FLOCK lock refuses only the FLOCK probe and the FCNTL ranges refuse only the
FCNTL requests. The step after the second unlock shows this from both sides.

The expected probe results are computed rather than listed per step:
`heldFcntlExpected` takes the FCNTL locks still held, and a request conflicts
with the first of them in start order that overlaps it unless both are
shared. `TestCephFSFcntlRecipeBlobAndChecks` pins the result of that
computation for every step against a written table, requires that each
request overlaps at most one held range, pins the packed filelock_change
arguments and the two-section blob (each section is the single-record layout
compared with the native encoder in
`TestCephFSHeldFlockBlobMatchesNativeLayout`), keeps the probe sequences of
the FLOCK-only recipes, and refuses wrong GETFILELOCK answers and wrong
submission records (an unlock over another range, pid or rule).
`TestCephFSFcntlProbeReceiptChecks` refuses wrong probe receipts. The
fixture's `TestMDSCephFSFcntlProbeOutputShape` refuses malformed native
output and `TestMDSCephFSHeldFcntlCapabilityAdmission` the action and the
selector without the capability.

## Runs

All runs below used a Linux amd64 host with Docker 29.9.1, the pinned image
and the catalog row's environment from `python3 tools/scenario_plan.py env`
under the result gate (`tools/native_test_results.py`), one `go test`
process per engine.

### Before the first commit

The first run, on the CRC/AES IPv4 host BlueStore row, used a working-tree
upload whose test and fixture code is that of `312ad3f`. Its gate passed and
all six leaves passed at the first attempt (parent 108.97 s; lock leaf
8.13 s, restart leaf 59.57 s). It is superseded by the runs at `312ad3f`
below.

### At `312ad3f`

These runs used source `312ad3f60a412cbab9d78644e34ef552439b4473`; the upload
was a `git archive` of that commit and the commit ID was recorded on the host
before the run. Each row ran once, in one sequence on one engine, and passed
its gate with no failed, skipped or missing selected test. Only documentation
changed after that commit.

| Row | Parent | Lock leaf | Restart leaf |
| --- | --- | --- | --- |
| `mds-cephfs-fcntl-recovery`, CRC/AES IPv4 host BlueStore | 107.80 s | 8.12 s | 59.25 s |
| `mds-cephfs-fcntl-recovery`, secure/aes256k IPv6 container MemStore | 108.37 s | 8.11 s | 59.47 s |

The rows that share the changed recipe and lock helpers passed in the same
sequence: both `mds-cephfs` rows (three selected tests each; the exclusive
held-lock leaves took 5.54 s and 40.87 s, and 5.61 s and 41.23 s), both
`mds-cephfs-sharedflock-recovery` rows (97.94 s and 97.83 s) and both
`mds-cephfs-heldflock-split-reconnect` rows (51.51 s and 52.10 s).

### Control runs by temporary mutation

Each control run used a `git archive` of `312ad3f` with one line changed in
the reconnect step, applied on the host before the run and never committed.

| Run | Blob | Result |
| --- | --- | --- |
| CRC/AES IPv4 host BlueStore row | FLOCK record only (FCNTL count 0) | Gate failed as intended in the restart leaf; the lock leaf passed. After adoption the native FLOCK probe still returned EAGAIN, and the FCNTL probe acquired request 0 (write, byte 0) with `ceph_getlk` reporting no conflict where EAGAIN with the exclusive range (pid 4101) was required. |
| Secure/aes256k IPv6 container MemStore row | The two FCNTL records only (FLOCK count 0) | Gate failed as intended in the restart leaf; the lock leaf passed. After adoption the native `flock-ex-nb` probe acquired and released (errno 0) where EAGAIN was required. |

The first control shows that the restored ranges come from the FCNTL section
of the blob and that the FLOCK record does not stand in for them; the second
that the FLOCK record comes from its own section. Each is one run in one
profile.

No run of this acceptance failed other than the two intended control runs.
Exact-head CI is separate from these runs.

## Limits

- One client session, one inode, an unsplit reconnect. The FCNTL ranges are
  disjoint and belong to distinct owners, so no range was merged or split by
  the MDS for one owner; partial unlocks inside a range, adjacent ranges of
  one owner and overlapping shared ranges of several owners are not run.
- Waiting and interrupted FCNTL locks, owners of several clients, FCNTL
  ranges in a split reconnect, dirty cap state of the locked inode, standby
  takeover and crash durability of lock state are not exercised. In-memory
  lock state is not a journal durability promise.
- The restart is the fixture's single owned restart, so the restart without
  the FCNTL or the FLOCK records is shown only by the temporary mutation runs
  above, not by a retained leaf.
- The second native mount is a development fixture tool that calls the
  image's libcephfs through `ctypes`; it is not a product dependency.
- The blob is an explicit caller choice. The product does not infer held locks
  from capabilities, populate reconnect state with locks, or implement POSIX
  record-lock semantics.
- The detailed ledger (`facts.held_flock` in
  `mds-cephfs-fcntl-recovery-results.json`: submission records with TIDs,
  GlobalID, MDS identities, blob hex, every probe result and the GETFILELOCK
  answers) is written inside the job and is not uploaded for a successful run
  (issue #1).

## Source and licensing

Wire and daemon facts are independently restated from LGPL-2.1 sources at the
pinned commit: `include/ceph_fs.h` (`filelock_change`, `CEPH_LOCK_FCNTL`),
`include/cephfs/libcephfs.h` (`ceph_getlk`, `ceph_setlk`), `client/Client.cc`
(`_do_filelock`, `_encode_filelocks`), `mds/Server.cc`
(`recover_filelocks`, `handle_client_file_readlock`), `mds/flock.h`
(`ceph_filelock_owner_equal`) and `mds/flock.cc` (`look_for_lock`,
`get_overlapping_locks`). No upstream code is incorporated. Native tools and
libcephfs remain development dependencies.
