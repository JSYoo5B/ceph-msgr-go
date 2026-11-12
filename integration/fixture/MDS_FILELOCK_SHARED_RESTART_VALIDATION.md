# Shared FLOCK locks of two owners across an owned MDS restart

Ceph Tentacle 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`, pinned
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
This is the shared and multiple-owner held-lock checkpoint of issue #10. It
runs the unsplit held-lock recipe of
[MDS_FILELOCK_RESTART_VALIDATION.md](MDS_FILELOCK_RESTART_VALIDATION.md) with
two shared whole-file FLOCK locks of two distinct Go owners on one owned file
in place of the one exclusive lock. The exclusive record shows that one lock
of one owner comes back; it cannot show that a reconnect record with several
locks restores each of them, or that an unlock of one owner leaves the other
owner's lock in place on the new incarnation.

It adds no product source, no session feature and no fixture action. The
metadata fixture already admits the closed nonblocking `flock-sh-nb` and
`flock-ex-nb` probes on `go-file`
([MDS_FILELOCK_VALIDATION.md](MDS_FILELOCK_VALIDATION.md)). It adds an exact
selector to that fixture's profile, a public results file name and two catalog
rows.

## Recipe

`TestCephMDSCephFSSharedFlockRecoveryIntegration` runs the recovery recipe
(`runCephFSRecovery` in `integration/mds_cephfs_recovery_test.go`) with the
lock set `sharedHeldFlock` (`integration/mds_cephfs_heldflock_test.go`). The
two `mds-cephfs-sharedflock-recovery` rows select it with
`^TestCephMDSCephFSSharedFlockRecoveryIntegration$` on a fresh metadata
fixture each, because the recipe performs the fixture's only owned restart.
The broad `mds-cephfs` selector does not match it, and
`TestCephMDSCephFSRecoveryIntegration` keeps its one exclusive lock, its rows
and its selector. Every leaf of the recovery recipe runs; only the held-lock
steps below differ.

1. `held_flock_acquired_before_restart`: the Go recovery session creates the
   plan directory and `go-file`, sets and natively compares the xattr and
   keeps a read-write handle, as in the exclusive case. It then submits two
   shared whole-file SETFILELOCK requests, owner `0x80000000474f0001` and
   then owner `0x80000000474f0003`. Both owners have the high bit set, so the
   MDS identifies them by client and owner without the pid
   (`ceph_filelock_owner_equal` in `mds/flock.h`); the GETFILELOCK test owner
   `0x80000000474f0002` and the native owner differ from both. Each call is
   recorded with its TID as soon as `Submit` accepted it, before its reply is
   awaited, with the outcome of that wait kept separately. With both locks
   granted, the closed native `flock-sh-nb` probe must acquire and release
   (errno 0) and `flock-ex-nb` must return EAGAIN, each from a client that is
   not the Go GlobalID, on the same inode, with the native inode, size 0,
   version, mtime and empty content unchanged across all probes. A Go
   GETFILELOCK of an exclusive test lock must return the first owner's shared
   lock with the session GlobalID.
2. `mds_restart_reconnect_replay` runs unchanged until the reconnect state is
   built. The test consumer then sets `MDSCapReconnect.Flock` of that one
   inode to a `Client::_encode_filelocks`-layout blob: fcntl count 0, flock
   count 2 and two 41-byte `ceph_filelock` records (start 0, length 0, the Go
   GlobalID, the owner, pid 0, type shared) in acquisition order. No other
   capability carries a blob. Neither SETFILELOCK is submitted again, and
   neither is among the resubmitted calls.
3. After OPEN, `up:active` and adoption, still in the restart leaf: the MDS GID
   and nonce differ and rank stays 0; the one `dump_historic_ops` snapshot
   read right after adoption holds no `client_request` for either original
   acquire TID (a bounded snapshot: an absent TID may also have aged out, so
   this is not proof that no such request arrived); the native shared probe
   must again acquire and release and the native exclusive probe must again
   return EAGAIN; GETFILELOCK on the reconnected session must name the first
   owner's lock.
4. Go unlocks the first owner only. The native exclusive probe must still
   return EAGAIN, which only the second owner's restored lock can cause, and
   GETFILELOCK must now name the second owner's lock. Go then unlocks the
   second owner, and the native exclusive probe must acquire and release
   (errno 0). The record must hold exactly two acquires before the restart
   and two unlocks after it, one per owner and in that order, each answered
   with result 0. No second Go acquisition happens anywhere.
5. The cleanup leaf is unchanged: it unlinks the lock file and removes the
   plan directory through Go.

The GETFILELOCK expectation is exact rather than "one of the held locks".
`Server::handle_client_file_readlock` answers with the first overlapping lock
of another owner (`ceph_lock_state_t::look_for_lock` and
`get_overlapping_locks` in `mds/flock.cc`). `held_locks` is a multimap keyed
by start; equal keys keep insertion order, which is the grant order before the
restart and the blob order after it (`Server::recover_filelocks`). A restored
set that lost the first owner's record therefore names the second owner.

The blob builder, its decoder and the probe and holder checks are test-only
consumer code. `TestCephFSHeldFlockBlobMatchesNativeLayout` compares the
builder byte for byte with the native-encoded flock blob in
`internal/mds/testdata/mds-reconnect-native-provenance.json`. The cluster-free
`TestCephFSSharedFlockRecipeBlobAndChecks` requires the two-record blob to be
the counts followed by the two single-record layouts, decodes it the way
`Server::recover_filelocks` reads it, refuses malformed blobs, pins the probe
sequence of both recipes and refuses wrong GETFILELOCK holders and wrong
submission records (a second acquisition, a missing or reordered unlock, a
refused or unawaited reply). `TestCephFSHeldFlockProbeReceiptChecks` refuses
wrong probe receipts (wrong action or errno, another mutation record, an
uncertain native process, the Go client itself, another owner or inode, and a
file that changed during or between probes).

## Runs

All runs below used a Linux amd64 host with Docker 29.9.1, the pinned image
and the catalog row's parameters under the result gate
(`tools/native_test_results.py`), one `go test` process per engine.

### Before the first commit

| Run | Source | Result |
| --- | --- | --- |
| First run, CRC/AES IPv4 host BlueStore row | Working tree on `762ddf0c237efe06ac725194b3897295718badcf` whose test and fixture code is that of `766bddf` | Gate passed; all six leaves passed at the first attempt (parent 97.71 s; lock leaf 8.22 s, restart leaf 48.21 s). |
| First run, secure/aes256k IPv6 container MemStore row | The same tree | Gate passed; all six leaves passed at the first attempt (parent 98.65 s; lock leaf 8.01 s, restart leaf 48.77 s). |

Both runs observed the same shape: two acquire TIDs and two unlock TIDs, all
with result 0 and nothing else; a 90-byte blob with fcntl count 0, flock
count 2 and the two shared records in acquisition order; native
`flock-sh-nb` errno 0 and `flock-ex-nb` EAGAIN before the restart and again
on the new MDS GID after it; EAGAIN after the first unlock; errno 0 after the
second. GETFILELOCK named the first owner before and after the restart and
the second owner after the first unlock. The historic-ops snapshot held
neither acquire TID. The native version of the lock file was 1 throughout.

### Control runs by temporary mutation

Each control run used source `766bddf` with one line changed in the
reconnect step, so that the blob carried only one of the two records (flock
count 1). Both edits were reverted and never committed.

| Run | Blob | Result |
| --- | --- | --- |
| CRC/AES IPv4 host BlueStore row | First owner's record only | Gate failed as intended. The lock leaf passed. After adoption the native shared probe acquired, the native exclusive probe returned EAGAIN and GETFILELOCK named the first owner, as in the passing runs. After Go unlocked the first owner, the native exclusive probe acquired and released (errno 0) where EAGAIN was required. |
| Secure/aes256k IPv6 container MemStore row | Second owner's record only | Gate failed as intended. The lock leaf passed and the two native probes after adoption gave the passing runs' results. GETFILELOCK after the restart named the second owner where the first was required. |

The first control shows that the check after the first unlock depends on the
second record: one restored shared lock gives the same native picture as two
until one owner unlocks. The second shows that the exact GETFILELOCK check
catches a restored set that kept a different owner, which the native probes
alone do not distinguish. Each is one run in one profile.

### At `c628a9a0`

Source `c628a9a0` holds the test, fixture, catalog and documentation of this
change except this section. Each row ran once, in sequence per profile, and
passed its gate with no failed, skipped or missing selected test.

| Row | Parent | Lock leaf | Restart leaf |
| --- | --- | --- | --- |
| `mds-cephfs-sharedflock-recovery`, CRC/AES IPv4 host BlueStore | 98.49 s | 8.24 s | 48.94 s |
| `mds-cephfs-sharedflock-recovery`, secure/aes256k IPv6 container MemStore | 97.69 s | 8.11 s | 47.94 s |

Both profiles observed the shape of the first runs: four SETFILELOCK
submissions (two shared acquires, two unlocks) with result 0, the 90-byte
two-record blob, native errno 0 and EAGAIN before and after the restart on a
new MDS GID, EAGAIN after the first unlock and errno 0 after the second,
GETFILELOCK naming the first owner before and after the restart and the
second owner after the first unlock, neither acquire TID in the historic-ops
snapshot, and native version 1 throughout.

The rows that share the touched recipe and helpers passed at the same source:
both `mds-cephfs` rows (three selected tests each; the exclusive held-lock
leaves `held_flock_acquired_before_restart` and `mds_restart_reconnect_replay`
took 5.53 s and 40.49 s, and 5.31 s and 40.65 s) and both
`mds-cephfs-heldflock-split-reconnect` rows (51.22 s and 50.61 s).

No run of this acceptance failed other than the two intended control runs.
Exact-head CI is separate from these runs.

## Limits

- Two shared whole-file FLOCK locks of two owners of one client session on
  one inode, in an unsplit reconnect. Waiting locks, FCNTL byte ranges, a mix
  of shared and exclusive locks, owners of several clients, locks on several
  inodes, a lock in the final fragment or in several fragments of a split
  reconnect and interrupt races are not exercised. The pinned Python binding
  offers `LibCephFS.flock` only, so the native probe cannot take FCNTL ranges.
- The restart is the fixture's single owned restart, so the restart without
  the second record is shown only by the temporary mutation runs above, not by
  a retained leaf.
- The recovery recipe's other state (resubmitted creates, the pending cap
  flush) travels in the same reconnect, but no dirty cap state of the locked
  inode accompanies the locks. Standby takeover and crash durability of lock
  state are not exercised. In-memory lock state is not a journal durability
  promise.
- The blob is an explicit caller choice. The product does not infer held locks
  from capabilities and does not populate reconnect state with locks.
- A passing leaf does not establish POSIX lock semantics or a typed lock API.
- The detailed ledger (`facts.held_flock` in
  `mds-cephfs-sharedflock-recovery-results.json`: submission records with
  TIDs, GlobalID, nonce, MDS identities, blob hex, probe receipts and the
  GETFILELOCK answers) is written inside the job and is not uploaded for a
  successful run (issue #1).

## Source and licensing

Wire and daemon facts are independently restated from LGPL-2.1 sources at the
pinned commit: `client/Client.cc` (`_encode_filelocks`), `mds/Server.cc`
(`recover_filelocks`, `handle_client_file_readlock`), `mds/flock.h`
(`ceph_filelock_owner_equal`), `mds/flock.cc` (`look_for_lock`,
`get_overlapping_locks`) and `pybind/cephfs/cephfs.pyx` (`flock`). No upstream
code is incorporated. Native tools and libcephfs remain development
dependencies.
