# Disposable CephFS metadata fixture

`CEPH_MSGR_TEST_MDS_CEPHFS=1` is a closed development-only extension of
`CEPH_MSGR_TEST_MDS_FRESH=1`. It selects only
`^TestCephMDSCephFS(Recovery)?Integration$` and excludes both
`CEPH_MSGR_TEST_MDS_COMMANDS=1` and `CEPH_MSGR_TEST_MDS_SESSIONS=1`. Like
those profiles it requires one OSD with `legacy-v6`: the owned metadata/data
pools disable HASHPSPOOL (16/1 PGs, replication/min_size 1/1), so the public
Go clients need no modern OSD profile and no extra MON admission bits. The
metadata acceptance enables only MDS; the recovery acceptance's fencing
clients also enable OSD with the default v6 object-hash profile. No MON
feature exception is added. The fixture termination budget is two minutes,
as for the session profile.

Both tests share one fixture. `go test` runs them in source order, so
`TestCephMDSCephFSRecoveryIntegration` (`mds_cephfs_recovery_test.go`) runs
first and restarts the owned MDS daemon; `TestCephMDSCephFSIntegration` then
targets the incarnation the fixture adopted (see
[current target](#current-target-and-the-owned-restart)).

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Relied-upon behavior is read from
`client/Client.cc` (capability and lease handling, `send_reconnect`),
`pybind/cephfs/cephfs.pyx` (the native driver's API), `mds/Server.cc` (open,
create, readdir, reply extra, `kill_session`, reconnect and replay dispatch),
`mds/Locker.cc` (cap issue, revoke, update and release), `mds/CInode.cc`
(`encode_inodestat`), `mds/SessionMap.cc` (`Session::dump`),
`mds/MDSRank.cc` (`evict_client`, `dump_historic_ops`), `mds/MDSDaemon.cc`
(respawn after removal from the map), `mon/MDSMonitor.cc` (`fail_mds_gid`),
`mon/OSDMonitor.cc` (blocklist of an address vector) and
`osd/PrimaryLogPG.cc` (`do_op` blocklist check) (LGPL-2.1 references; no code
is copied).

## Identities and published receipts

The fresh fixture supplies the UUID filesystem, its active rank-0 MDS and the
exact FSID/FSCID/name, daemon GID, incarnation and v2 address/nonce.
Preparation then records the complete OSD blocklist, requires an empty native
`session ls`, and creates three identities, journaling each name before the
provider call:

| Identity | Caps | Use |
| --- | --- | --- |
| `client.mds-cephfs-<token>` | MON `allow r`, MDS `allow rw fsname=<owned fs>` | the metadata acceptance's Go client (no OSD or MGR caps) |
| `client.mds-cephfs-native-<token>` | MON `allow r`, MDS `allow rw fsname=<owned fs>`, OSD `allow rw tag cephfs data=<owned fs>` | the native LibCephFS oracle |
| `client.mds-cephfs-recovery-<token>` | MON `allow r`, MDS `allow rw fsname=<owned fs>`, OSD `allow r tag cephfs data=<owned fs>` | the recovery acceptance's Go clients (CephFS sessions and data-pool reads; no writes, no MGR) |

`CEPH_MSGR_MDS_CEPHFS_IDENTITY` and `CEPH_MSGR_MDS_CEPHFS_KEY_FILE` name the
first Go identity and `<controlDir>/mds-cephfs.key`;
`CEPH_MSGR_MDS_CEPHFS_RECOVERY_IDENTITY` and
`CEPH_MSGR_MDS_CEPHFS_RECOVERY_KEY_FILE` name the recovery identity and
`<controlDir>/mds-cephfs-recovery.key`. The native keyring stays at
`/out/mds-cephfs-native.keyring` for the fixture container only. Keys and
keyrings are mode 0600 and excluded from diagnostics.

`mds-cephfs-oracle.json` publishes source pin, token, FSID, FSCID, filesystem
name, startup daemon GID, the three identities and their caps, the baseline
blocklist, the data pool ID and name (`<fs>-data`) and two fixed plans. The
metadata plan:

- directory `ceph-msgr-cephfs-<token>` under the filesystem root;
- files `go-file` (created by Go), `native-0`, `native-1`, `native-2`
  (created natively) and the absent name `missing`;
- xattr `user.ceph-msgr` with a 28-byte binary value (`00 ff`, `ceph-msgr`,
  `00`, the raw 16-byte token);
- a 64-byte write payload (SHA-256 of `ceph-msgr-cephfs-payload:<token>`,
  twice).

The recovery plan:

- directory `ceph-msgr-cephfs-<token>-recovery` under the filesystem root;
- Go-created files `flush-plain-0..2`, `flush-requested-0..2`, `held`,
  `replayed` and `unanswered`, and the natively created `fenced`;
- a 5000-byte binary payload for `fenced` (a SHA-256 chain starting from
  `ceph-msgr-cephfs-fenced:<token>`).

`mds-cephfs-owned.json` is the journal. It is rewritten before each identity
creation and each native mutation, eviction or restart step (intent), and
after its outcome. It also records the current owned MDS incarnation and the
session addresses of owned Go identities that the tests named.

## Current target and the owned restart

Every action proves the current owned rank-0 incarnation before and after it
runs: FSID, FSCID, daemon name, GID, rank 0, endpoint, nonce, incarnation and
pools. That incarnation starts as the fresh fixture's daemon and changes only
through the owned restart below. `mds-cephfs-target.json` publishes it with
`adopted_restarts` (0 or 1); both tests read it instead of the startup oracle
when they build their MDS target and check receipts.

The restart is four closed steps, journaled as one record that is never
reissued:

1. `restart-arm` (identity-checked): records the current incarnation as the
   armed daemon and its `ip:port/nonce` addresses, and refuses if one is
   already blocklisted. Only one restart can ever be armed.
2. `restart-fire`: journals `intended`, then runs `ceph mds fail <armed GID>`
   once and records `applied` or `uncertain` with the bounded CLI output.
   It skips the identity proof because the test calls it right after an
   unsafe reply, before the next journal tick; the armed GID names exactly
   the proven incarnation, so a different daemon cannot be failed. The MON
   removes that GID from the map and blocklists its address vector; the
   daemon process respawns in its own container and registers again with a
   new GID and nonce.
3. `restart-status` (read only, repeatable): reads one native `fs dump` and
   reports rank 0 (name, GID, state, incarnation, endpoint, nonce), failed
   ranks, owned standbys, whether the armed GID is still mapped and the
   blocklist entries of the armed addresses. Only the filesystem identity
   (FSCID, name, pools, `max_mds` 1) must be unchanged; any rank state is
   reported as observed.
4. `restart-adopt`: requires the same daemon name in rank 0 `up:active` with
   a different GID, nonce and incarnation, the same FSCID and pools, and the
   armed GID absent from the map. It records the old incarnation's
   blocklist entries, makes the new incarnation current, rewrites the target
   file and then runs the ordinary post-action proof against it.

Between `restart-fire` and `restart-adopt` every other action is refused
before native work, since no identity proof is possible.

## Closed control

```go
testcluster.RequestControl(ctx, dir,
    "mds-cephfs", "mds-cephfs-completed",
    token, action, argument)
```

The request is `<requestID> <ownership token> <action> [argument]`. IDs are
positive canonical decimals and strictly increase. Targets are closed names
mapped to plan paths; no caller path, value or command reaches the fixture.
One request is outstanding at a time.

| Action | Argument | Native effect |
| --- | --- | --- |
| `observe` | one GlobalID | read only: `tell mds.<fs>:0 session ls`, `osd blocklist ls`; records the address of an owned Go identity's session with that GlobalID |
| `stat` | `dir`, `go-file`, `native-0..2`, `missing` | read only: LibCephFS `statx` (ino, mode, uid, gid, nlink, size) or ENOENT |
| `getxattr` | `go-file` | read only: `getxattr(user.ceph-msgr)` |
| `list` | `dir` | read only: `opendir`/`readdir` names and `d_ino`, then `statx` per entry |
| `read` | `go-file` | read only: open `O_RDONLY`, read payload length + 1 bytes |
| `create-native` | none | mutation, once: `open(O_CREAT\|O_EXCL\|O_WRONLY, 0644)` and close of `native-0..2` |
| `write` | `go-file` | mutation, once: open `O_WRONLY`, write the payload at 0, `fsync`, close, with monotonic timestamps after each step |
| `recovery-stat` | `dir` or a recovery file name | read only: `statx` in the recovery directory |
| `recovery-list` | none | read only: listing of the recovery directory; only plan names are admitted |
| `recovery-write` | none | mutation, once: create `fenced` (`O_CREAT\|O_EXCL\|O_WRONLY`, 0644), write the 5000-byte payload, `fsync`, close, `statx` |
| `evict` | one GlobalID | mutation, once per GlobalID: the target must be an open session of the recovery identity whose address is not yet blocklisted; journals intent, runs `tell mds.<fs>:0 session evict id=<gid>`, then waits until the session is gone and its exact address is blocklisted, recording the entry's expiry |
| `recovery-ops` | one GlobalID | read only: `tell mds.<fs>:0 dump_historic_ops`, reduced to client requests of that GlobalID (description, result, flag point, event names); admitted only for an observed owned session |
| `restart-arm`, `restart-fire`, `restart-status`, `restart-adopt` | none | the owned restart above |

Each native file action runs one fixed Python driver through the provider's
`RunNativeCommand` in the fixture container (30 s bound, 45 s for `write`
and `recovery-write`; bounded stdout/stderr). The driver mounts the owned
filesystem by name, re-checks that every path is one of the two owned
directories (both carry the `ceph-msgr-cephfs-` prefix) or a direct child of
it, prints one JSON object and always calls `shutdown`, which unmounts and
closes its own CephFS session. Output is decoded with unknown fields refused
and the exact per-action shape required; only ENOENT is accepted as a stat
failure. A failed, killed or truncated process is an error, never partially
trusted.

Every action except `restart-fire` and `restart-status` proves the current
target before and after it runs and then writes `mds-cephfs-result.json`
with the map state, the daemon's applied MDSMap epoch, the parsed native
output (or the session listing and blocklist), the process receipt and the
mutation or restart record. The controller holds the shared MDS fresh scope
lock and is bounded to 90 seconds. Any refused or failed action stops the
fixture's control loop (`provider.error`), as before.

## Uncertain outcomes and cleanup

A native mutation is journaled `intended` before its process starts, then
`applied` or `uncertain`; an eviction ends `evicted` or `uncertain`; the
restart ends `applied`, `uncertain` or `adopted`. None is ever reissued: a
second request of the same kind is refused whatever the first outcome. The
Go tests remove both namespaces with their own unlink and rmdir requests and
verify ENOENT natively.

Cleanup runs once, inside the cached MDS fresh cleanup and before the owned
filesystem is removed, with its own 45-second bound. It writes an `attempted`
receipt to `mds-cephfs-cleanup.json`, then:

1. Lists native sessions and reports any that still belong to the three
   identities. It never evicts.
2. Unless a Go session is still present (a native mount could then wait on
   revocations from a client that no longer answers), stats both owned
   directories natively. `namespace_restored` records whether both are gone;
   residual entries are discarded by the verified filesystem removal.
3. Compares the blocklist with the baseline and removes only new entries it
   can attribute to an owned mutation: the eviction entry (exact address and
   unchanged expiry), the armed incarnation's addresses once the armed GID
   is absent from the FSMap, and addresses of owned Go sessions the tests
   observed (for example an eviction at the end of a missed reconnect
   window). Each removal uses `osd blocklist rm <address>` and is verified
   by a final listing. Any other new entry is reported in
   `kept_new_blocklist`, left untouched, and fails cleanup.
4. Deletes the three identities. An identity whose creation outcome is
   unknown fails cleanup instead of reporting success.

`completed` is true only when the namespace check ran, every new entry was
attributed, removed and verified absent, and all three identities were
removed. The MDS fresh cleanup receipt adds `cephfs_verified`; filesystem,
pool and identity removal proceed even if this verification failed, and both
errors are reported.

## Host relay

In the host runtime the test reaches daemons through the fixture relay,
whose allowlist is `peers.json`. An owned MDS entry admits any port of that
daemon's container IP, as for MGR entries: a respawned MDS may bind another
port. The observed respawns kept the same port.

## Unit coverage

Fixture unit tests cover the exclusive profile and selector, legacy pools,
credential environment (including the recovery identity), identity caps, the
deterministic plans and their path admission, closed actions and replay
protection, native output shapes for both directories, transitional rank
status parsing (failed rank, replay state, owned standby, address family),
the ordering and one-shot rules of the restart steps, blocklist attribution,
op-tracker filtering by GlobalID, scope rejection before native work,
one-shot mutations and cleanup without a baseline. They do not establish
live MDS behavior; see [MDS_CEPHFS_VALIDATION.md](MDS_CEPHFS_VALIDATION.md)
and [MDS_CEPHFS_RECOVERY_VALIDATION.md](MDS_CEPHFS_RECOVERY_VALIDATION.md).
