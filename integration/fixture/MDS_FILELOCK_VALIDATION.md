# Raw CephFS FLOCK validation

Run 2026-10-18 against Ceph Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`, pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Live source: base `source-1` plus the four source hashes in
`MDS_FILELOCK_LINUX_20261018.json`. The later combined Go/fixture checks use
base `source-2` with those same FLOCK sources; the intervening MPing addition
does not alter MDS transport or request behavior.

This adds no product API: the consumer sends raw
`MDSOpSetFileLock`/`MDSOpGetFileLock` with 56-byte `Args` and decodes the
41-byte `Extra`. The fixed file, Go/native identities, filesystem, permissions
and capability consumer are the existing owned CephFS fixture's. Source:
`include/ceph_fs.h::filelock_change/ceph_filelock`,
`Client::_flock/_do_filelock` and `Server::handle_client_file_setlock/readlock`.
FLOCK uses rule 2, whole-file start/length/pid zero, owner with bit 63 set and
wait 0. The server fills the conflict's client field from the authenticated
GlobalID. Lock construction, ownership and policy remain consumer code.

## Observed

The new `filelock_nonblocking_native_conflict` leaf runs between the existing
lookup and native writer tests, using the Go-created empty `go-file`:

1. Go opens RDWR and holds an exclusive raw FLOCK. Separate native LibCephFS
   EX|NB and SH|NB probes each return EAGAIN (11).
2. Go GETFILELOCK returns the exact Go GlobalID, owner, exclusive type and
   whole-file range in its raw Extra; Go explicitly unlocks.
3. Go holds a shared FLOCK. Native SH|NB succeeds and unlocks; EX|NB returns
   EAGAIN. Go explicitly unlocks, then native EX|NB succeeds and unlocks.
4. All five native probes preserve bytes, size, statx version and
   second-resolution mtime before/after and across probes. Go's before/after
   inode preserves Ino, Size, ChangeAttr and nanosecond-resolution MTime.
   Native statx version and Go ChangeAttr both equal 1. Full Go inode records,
   native probe GlobalIDs, errors/timings, conflict and unlock replies are
   retained, including failures.
5. The following native writer/revoke, Messenger resume and namespace cleanup
   tests pass. Native successful probes unlock in a finally block before
   close/shutdown. Go cleanup explicitly chooses one normal unlock if needed;
   an already submitted unlock is only waited on again, never retransmitted.

| Profile | UTC interval | FLOCK leaf | Selected leaves | Controller |
| --- | --- | --- | --- | --- |
| CRC/AES, IPv4 host, one BlueStore OSD | 05:55:58–06:03:00 | 16.907 s | 23/23 | exit 0 |
| secure/AES256K, IPv6 container, one MemStore OSD | 05:56:00–06:02:05 | 13.407 s | 23/23 | exit 0 |

Each selection includes seven push, five recovery and eleven metadata leaves.
The separate internal-client binary has no matching test and adds no live
coverage. The fixture verifies namespace and owned blocklist restoration,
zero residual owned sessions, removal of four CephFS identities, then removal
of the filesystem, its pools and the base identity. Each run's nine owned
containers and network are independently absent after teardown. The receipt
preserves these readbacks and SHA-256 values for all eleven original public
files; credentials and private ownership/keyring files are excluded.

## First failure and inode counter semantics

Both first runs passed native lock conflicts, GETFILELOCK and explicit
unlocks, but failed a test assertion equating the OPEN and GETATTR wire inode
Version. They remain failed executions. The observed Version was 13 then 80,
while ChangeAttr stayed 1 and MaxSize grew from 0 to 4 MiB during capability
negotiation.

Pinned `CInode.cc:3951–3952` encodes wire Version as
`2 * inode_version + is_projected()`. `Locker.cc:4055–4180` can journal a
write-capability range/max-size update and advance this internal version even
with dirty=0. Native `Client.cc:8833–8837` instead returns change_attr as statx
version, independently encoded by `CInode.cc:4262`. Internal wire Version is
therefore retained as an observation, not asserted to be a file-content
counter. The corrected assertion compares ChangeAttr with native statx
version while retaining the size, bytes and precise MTime checks.

The first runs did not retain the OPEN MTime's nanoseconds, so they do not
retrospectively prove that equality. The successful runs record both full
inodes before the assertion and confirm exact nanosecond equality. Their
13-to-80 internal Version transition is likewise preserved.

## Checks and limits

The combined source passed complete `CGO_ENABLED=0 go test ./...`, related
integration/cephmsgr race tests, root/fixture vet, all fixture unit tests and
423 Python checks on Linux. Earlier synthetic wire tests separately verify
that SETFILELOCK is excluded from oldest_client_tid while ordinary requests,
GETFILELOCK, forwarding and explicitly selected reconnect requests retain
their expected cursor/identity behavior.

Run on a Linux Docker host with `CEPH_MSGR_TEST_OSD=1`,
`CEPH_MSGR_TEST_MDS_FRESH=1`, `CEPH_MSGR_TEST_MDS_CEPHFS=1` and the default
legacy-v6 one-OSD profile via `sh integration/run.sh`; choose the key,
connection, address/runtime and OSD store variables from the table above.
The selector remains the existing closed CephFS family, not a new fixture.

Not established here: FCNTL byte-range locks, blocking lock waits, interrupts,
waiting-lock oldest cursor on a native daemon, permission denial, arbitrary filenames/content, or typed
lock workflows. In-memory FLOCK state is not a journal durability promise.
No automatic lock acquire replay or local-wait remote cancellation is added.
A held whole-file lock across an owned MDS restart is recorded in
[MDS_FILELOCK_RESTART_VALIDATION.md](MDS_FILELOCK_RESTART_VALIDATION.md).

The later bounded [wait and interrupt completion checkpoint](MDS_FILELOCK_COMPLETION_VALIDATION.md)
separately verifies the original call's grant after local Wait expiry and an
explicit interrupt paired with the original call's -EINTR. The limits above
describe this nonblocking checkpoint rather than that subsequent evidence.
