# Held FLOCK across an owned MDS restart

Ceph Tentacle 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`, pinned
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
This is the held-lock checkpoint proposed in issue #10. It composes the closed
nonblocking native FLOCK probe of
[MDS_FILELOCK_VALIDATION.md](MDS_FILELOCK_VALIDATION.md) with the owned MDS
restart of [MDS_CEPHFS_RECOVERY_VALIDATION.md](MDS_CEPHFS_RECOVERY_VALIDATION.md).
It adds no product source, fixture action, session feature mask or catalog row.

## Recipe

The leaf `held_flock_acquired_before_restart` and the existing
`mds_restart_reconnect_replay` and `cleanup_unlink_release_close` leaves of
`TestCephMDSCephFSRecoveryIntegration` carry the lock. The scenario selector is
unchanged (`^TestCephMDSCephFS(Push|Recovery)?Integration$`, both `mds-cephfs`
rows).

1. Before the restart is armed, the Go recovery session creates the ordinary
   plan directory and `go-file` (journal flush requested), closes the create
   handle, sets the plan xattr, compares it with the closed native `getxattr`,
   reopens the file read-write and keeps that handle. The closed native probe
   admits only this path, which is why the lock file is not in the recovery
   plan directory.
2. Go submits one exclusive whole-file SETFILELOCK with the fixed owner
   `0x80000000474f0001`. The closed native `flock-ex-nb` probe must return
   EAGAIN with unchanged native inode, size, version, mtime and bytes, and a Go
   GETFILELOCK with a different owner must return the Go owner, the session
   GlobalID and type exclusive.
3. The existing restart recipe runs unchanged. When its reconnect state is
   built, the test consumer sets `MDSCapReconnect.Flock` of that one inode to a
   `Client::_encode_filelocks`-layout blob: fcntl count 0, flock count 1 and one
   41-byte `ceph_filelock` (start 0, length 0, the Go GlobalID, the owner,
   pid 0, type exclusive). No other cap carries a blob. SETFILELOCK is not
   submitted again and the lock call is not among the resubmitted calls.
4. After the new incarnation is adopted: the MDS GID and nonce differ from the
   armed daemon and rank stays 0; the new incarnation's historic-ops list holds
   no `client_request` for the original SETFILELOCK TID; the native probe must
   again return EAGAIN with the same native file state; GETFILELOCK on the
   reconnected session must return the original owner and GlobalID; one
   explicit Go unlock follows; the native probe must then acquire and release
   (errno 0). The session keeps its GlobalID and the shared CephFS nonce, which
   the existing native session-address check covers.
5. The cleanup leaf unlinks the lock file and removes the plan directory
   through Go, so the ordinary metadata acceptance that follows in the same
   fixture still creates them. If an earlier leaf fails, the namespace is left
   to fixture teardown, which removes the whole filesystem.

The blob builder and the probe checks are test-only consumer code
(`integration/mds_cephfs_heldflock_test.go`). The cluster-free test
`TestCephFSHeldFlockBlobMatchesNativeLayout` compares the builder byte for byte
with the native-encoded flock blob retained in
`internal/mds/testdata/mds-reconnect-native-provenance.json`.

## Runs

| Run | Source head | Result |
| --- | --- | --- |
| CI command 40058427514 | `99b737ea8ad0f4413fe81ed5445f82e1ffb7f0de` | Both profiles failed in the new leaf before any lock was taken: the fixture rejected the first native `flock-ex-nb` receipt ("output shape differs") and the ordinary acceptance then failed its mkdir. The retained diagnostics omit the rejected native output, so the cause was not observed. The fixture's shape check requires a nonzero native version; a file that has never been modified is a candidate explanation, but it is unconfirmed. |
| CI command 40059034267 | `db00ddd7d131427b357e2db83c2e2f0d7d1b490a` (setxattr added) | The lock leaf passed in both profiles. The restart leaf then failed with an unexpected flush acknowledgement on the lock file's inode: a mechanical variable rename had redirected the existing pending size flush from the plan's held file to the lock file. That is a test defect, fixed in the next head, and it is not evidence about lock recovery. |
| CI command 40059624448 | `4a0288a403b04450ea0d61821d3019a5ba36c2f3` | Both `mds-cephfs` rows passed (attempt 1, controller `50f27fb7c9204138e7111cbeca323a33d676c947`). |

In 40059624448 the three selected tests passed in both profiles. The lock leaf
took 5.91 s (CRC/AES IPv4 host BlueStore) and 5.02 s (secure/aes256k IPv6
container MemStore); the restart leaf 42.79 s and 35.07 s; the recovery parent
91.09 s and 82.83 s; the ordinary acceptance, with its three FLOCK leaves,
117.37 s and 111.50 s. The post-restart native conflict, original GETFILELOCK
identity, absence of a tracked request for the original SETFILELOCK TID, unlock
and native acquire are assertions of those leaves; their pass is the
evidence. The detailed ledger (`facts.held_flock` in the recovery results:
TIDs, GlobalID, nonce, MDS identities, blob hex, probe receipts) is written
inside the job and is not uploaded for a successful run, so exact values are
not retained. The failed run's diagnostics retain only the pre-restart part.

## Limits

- The restart is the fixture's single owned restart, so there is no control
  run that restarts without the blob. The oracle shows that the lock is present
  after a restart carrying the caller-built blob; it does not show that the
  new MDS would have lost the lock without it.
- One exclusive whole-file FLOCK of one owner, one client session. Waiting
  locks, FCNTL byte ranges, shared locks, several owners or clients, interrupt
  races, interaction with dirty cap state, standby takeover and crash
  durability of lock state are not exercised. In-memory lock state is not a
  journal durability promise.
- The blob is an explicit caller choice. The product does not infer held locks
  from capabilities and does not populate reconnect state with locks.
- A passing leaf does not establish POSIX lock semantics or a typed lock API.
