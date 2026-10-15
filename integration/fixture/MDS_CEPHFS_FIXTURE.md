# Disposable CephFS metadata fixture

`CEPH_MSGR_TEST_MDS_CEPHFS=1` is a closed development-only extension of
`CEPH_MSGR_TEST_MDS_FRESH=1`. It selects only
`^TestCephMDSCephFSIntegration$` and excludes both
`CEPH_MSGR_TEST_MDS_COMMANDS=1` and `CEPH_MSGR_TEST_MDS_SESSIONS=1`. Like
those profiles it requires one OSD with `legacy-v6`: the owned metadata/data
pools disable HASHPSPOOL (16/1 PGs, replication/min_size 1/1) so the public
Go client enables only MDS, without EnableOSD or the modern OSD profile's
extra MON admission bits. No MON feature exception is added. The fixture
termination budget is two minutes, as for the session profile.

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Relied-upon behavior is read from
`client/Client.cc` (capability and lease handling), `pybind/cephfs/cephfs.pyx`
(the native driver's API), `mds/Server.cc` (open, create, readdir, reply extra),
`mds/Locker.cc` (cap issue, revoke, update and release), `mds/CInode.cc`
(`encode_inodestat`) and `mds/SessionMap.cc` (`Session::dump`) (LGPL-2.1
references; no code is copied).

## Identities and published receipts

The fresh fixture supplies the UUID filesystem, its active rank-0 MDS and the
exact FSID/FSCID/name, daemon GID, incarnation and v2 address/nonce.
Preparation then records the complete OSD blocklist, requires an empty native
`session ls`, and creates two identities, journaling each name before the
provider call:

| Identity | Caps | Use |
| --- | --- | --- |
| `client.mds-cephfs-<token>` | MON `allow r`, MDS `allow rw fsname=<owned fs>` | the Go client (metadata only; no OSD or MGR caps) |
| `client.mds-cephfs-native-<token>` | MON `allow r`, MDS `allow rw fsname=<owned fs>`, OSD `allow rw tag cephfs data=<owned fs>` | the native LibCephFS oracle |

`CEPH_MSGR_MDS_CEPHFS_IDENTITY` and `CEPH_MSGR_MDS_CEPHFS_KEY_FILE` name the Go
identity and `<controlDir>/mds-cephfs.key`. The native keyring stays at
`/out/mds-cephfs-native.keyring` for the fixture container only. Keys and
keyrings are mode 0600 and excluded from diagnostics.

`mds-cephfs-oracle.json` publishes source pin, token, FSID, FSCID, filesystem
name, daemon GID, both identities and their caps, the baseline blocklist and
the fixed plan:

- directory `ceph-msgr-cephfs-<token>` under the filesystem root;
- files `go-file` (created by Go), `native-0`, `native-1`, `native-2`
  (created natively) and the absent name `missing`;
- xattr `user.ceph-msgr` with a 28-byte binary value (`00 ff`, `ceph-msgr`,
  `00`, the raw 16-byte token);
- a 64-byte write payload (SHA-256 of `ceph-msgr-cephfs-payload:<token>`,
  twice).

`mds-cephfs-owned.json` is the journal. It is rewritten before each identity
creation and each native mutation (intent), and after the mutation's outcome.

## Closed control

```go
testcluster.RequestControl(ctx, dir,
    "mds-cephfs", "mds-cephfs-completed",
    token, action, argument)
```

The request is `<requestID> <ownership token> <action> [argument]`. IDs are
positive canonical decimals and strictly increase. Targets are closed names
mapped to plan paths; no caller path, value or command reaches the fixture.

| Action | Argument | Native effect |
| --- | --- | --- |
| `observe` | one GlobalID | read only: `tell mds.<fs>:0 session ls`, `osd blocklist ls` |
| `stat` | `dir`, `go-file`, `native-0..2`, `missing` | read only: LibCephFS `statx` (ino, mode, uid, gid, nlink, size) or ENOENT |
| `getxattr` | `go-file` | read only: `getxattr(user.ceph-msgr)` |
| `list` | `dir` | read only: `opendir`/`readdir` names and `d_ino`, then `statx` per entry |
| `read` | `go-file` | read only: open `O_RDONLY`, read payload length + 1 bytes |
| `create-native` | none | mutation, once: `open(O_CREAT\|O_EXCL\|O_WRONLY, 0644)` and close of `native-0..2` |
| `write` | `go-file` | mutation, once: open `O_WRONLY`, write the payload at 0, `fsync`, close, with monotonic timestamps after each step |

Each native action runs one fixed Python driver through the provider's
`RunNativeCommand` in the fixture container (30 s bound, 45 s for `write`;
bounded stdout/stderr). The driver mounts the owned filesystem by name,
re-checks that every path is the owned directory or a direct child of it,
prints one JSON object and always calls `shutdown`, which unmounts and closes
its own CephFS session. Output is decoded with unknown fields refused and the
exact per-action shape required; only ENOENT is accepted as a stat failure.
A failed, killed or truncated process is an error, never partially trusted.

Every action proves the owned target before and after it runs (FSID, FSCID,
daemon GID, rank 0, endpoint, nonce, incarnation, pools) and then writes
`mds-cephfs-result.json` with the map state, the daemon's applied MDSMap
epoch, the parsed native output (or the session listing and blocklist for
`observe`), the process receipt and the mutation record. The controller holds
the shared MDS fresh scope lock and is bounded to 90 seconds.

## Uncertain outcomes and cleanup

A native mutation is journaled `intended` before its process starts, then
`applied` or `uncertain`. It is never reissued: a second request of the same
kind is refused whatever the first outcome. The Go test removes the namespace
with its own unlink and rmdir requests and verifies ENOENT natively.

Cleanup runs once, inside the cached MDS fresh cleanup and before the owned
filesystem is removed, with its own 45-second bound. It writes an `attempted`
receipt to `mds-cephfs-cleanup.json`, then:

1. Lists native sessions and reports any that still belong to either identity.
   It never evicts.
2. Unless the Go session is still present (a native mount could then wait on
   revocations from a client that no longer answers), stats the owned
   directory natively. `namespace_restored` records whether it is gone;
   residual entries are discarded by the verified filesystem removal.
3. Compares the blocklist with the baseline. This acceptance never evicts, so
   any new entry is reported and fails cleanup; nothing is removed.
4. Deletes both identities. An identity whose creation outcome is unknown
   fails cleanup instead of reporting success.

`completed` is true only when the namespace check ran, the blocklist is
unchanged and both identities were removed. The MDS fresh cleanup receipt adds
`cephfs_verified`; filesystem, pool and identity removal proceed even if this
verification failed, and both errors are reported.

## Unit coverage

Fixture unit tests cover the exclusive profile and selector, legacy pools,
credential environment, identity caps, the deterministic plan and its path
admission, closed actions and replay protection, native output shapes,
scope rejection before native work, one-shot mutations and cleanup without a
baseline. They do not establish live MDS behavior; see
[MDS_CEPHFS_VALIDATION.md](MDS_CEPHFS_VALIDATION.md).
