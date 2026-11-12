# Disposable CephFS metadata fixture

`CEPH_MSGR_TEST_MDS_CEPHFS=1` is a closed development-only extension of
`CEPH_MSGR_TEST_MDS_FRESH=1`. It selects only
`^TestCephMDSCephFS(Push|Recovery)?Integration$`, or, when that exact
selector is given, only `^TestCephMDSCephFSSharedFlockRecoveryIntegration$`
(the recovery recipe with two shared locks; see
[shared held-lock restart](MDS_FILELOCK_SHARED_RESTART_VALIDATION.md)), and excludes both
`CEPH_MSGR_TEST_MDS_COMMANDS=1` and `CEPH_MSGR_TEST_MDS_SESSIONS=1`. Like
those profiles it requires one OSD with `legacy-v6`: the owned metadata/data
pools disable HASHPSPOOL (16/1 PGs, replication/min_size 1/1), so the public
Go clients need no modern OSD profile and no extra MON admission bits. The
metadata and push acceptances enable only MDS; the recovery acceptance's
fencing clients also enable OSD with the default v6 object-hash profile. No
MON feature exception is added. The fixture termination budget is two minutes,
as for the session profile.

The multi-rank acceptance uses its own profile and fixture,
`CEPH_MSGR_TEST_MDS_CEPHFS_RANKS=1`; see
[MDS_CEPHFS_RANKS_FIXTURE.md](MDS_CEPHFS_RANKS_FIXTURE.md). The
request-completion acceptance (lost replies, replay from the
completed-request record, denied and late reconnects, read-only, lease RENEW
and cap snap flush) uses `CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION=1`; see
[MDS_CEPHFS_COMPLETION_FIXTURE.md](MDS_CEPHFS_COMPLETION_FIXTURE.md).

The three tests share one fixture. `go test` runs them in source file
order: `TestCephMDSCephFSPushIntegration` (`mds_cephfs_push_test.go`) runs
first and removes or restores everything it changes (its snapshot, quota and
recall settings; see [push extension](#push-extension));
`TestCephMDSCephFSRecoveryIntegration` (`mds_cephfs_recovery_test.go`) then
restarts the owned MDS daemon; `TestCephMDSCephFSIntegration` targets the
incarnation the fixture adopted (see
[current target](#current-target-and-the-owned-restart)). The push and
metadata tests read the current target, so they also run after a restart.
The recovery test creates the metadata test's plan directory and `go-file`
before the restart, because the closed native FLOCK probe admits only that
path ([held-lock restart](MDS_FILELOCK_RESTART_VALIDATION.md)), and removes
them again before it ends so the metadata test still creates them. If its
restart leaf fails, the recovery test reconciles under a fresh bounded
context before the metadata test runs: it lifts the journal pause of an
unfired restart, or waits for the issued restart's new incarnation to be
`up:active` and adopts it, then removes its namespace and closes its
sessions (see
[MDS_CEPHFS_RECOVERY_VALIDATION.md](MDS_CEPHFS_RECOVERY_VALIDATION.md#journal-fault-boundary-and-early-failure-reconciliation)).

`TestCephMDSCephFSSharedFlockRecoveryIntegration` runs the same recovery
recipe with two shared locks in place of the one exclusive lock. Because the
recipe performs the fixture's only owned restart, it has its own fresh
fixture: the two `mds-cephfs-sharedflock-recovery` rows select it alone, and
the broad selector above does not match it. It uses only actions this fixture
already admits (the closed `flock-sh-nb` and `flock-ex-nb` probes on
`go-file`), so no action is added for it. Its results are written to
`mds-cephfs-sharedflock-recovery-results.json`.

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Relied-upon behavior is read from
`client/Client.cc` (capability and lease handling, `send_reconnect`),
`pybind/cephfs/cephfs.pyx` (the native driver's API), `mds/Server.cc` (open,
create, readdir, reply extra, `kill_session`, reconnect and replay dispatch),
`mds/Locker.cc` (cap issue, revoke, update and release), `mds/CInode.cc`
(`encode_inodestat`), `mds/SessionMap.cc` (`Session::dump`),
`mds/MDSRank.cc` (`evict_client`, `dump_historic_ops`), `client/Client.cc`
(`resolve_mds`, `mds_command`) and `mds/FSMap.cc` (`parse_role`) for the
target of a `tell mds.<GID>`, `mds/MDSDaemon.cc`
(respawn after removal from the map), `mon/MDSMonitor.cc` (`fail_mds_gid`),
`mon/OSDMonitor.cc` (blocklist of an address vector) and
`osd/PrimaryLogPG.cc` (`do_op` blocklist check), and for the push extension
`mds/Locker.cc` (`issue_client_lease`, `revoke_client_leases`,
`handle_client_lease`), `mds/Server.cc` (`handle_client_mksnap`,
`_mksnap_finish`, `_rmsnap_finish`, the `ceph.quota` vxattr,
`recall_client_state`, `flush_session`), `mds/MDCache.cc`
(`broadcast_quota_to_client`, `do_realm_invalidate_and_update_notify`,
`notify_global_snaprealm_update`, `upkeep_main`), `mds/MDSRank.cc`
(`cache drop`) and `mds/MDSAuthCaps.cc` (LGPL-2.1 references; no code is
copied).

## Identities and published receipts

The fresh fixture supplies the UUID filesystem, its active rank-0 MDS and the
exact FSID/FSCID/name, daemon GID, incarnation and v2 address/nonce.
Preparation then records the complete OSD blocklist, requires an empty native
`session ls`, records the push baseline (below), and creates four identities,
journaling each name before the provider call:

| Identity | Caps | Use |
| --- | --- | --- |
| `client.mds-cephfs-<token>` | MON `allow r`, MDS `allow rw fsname=<owned fs>` | the metadata acceptance's Go client (no OSD or MGR caps) |
| `client.mds-cephfs-native-<token>` | MON `allow r`, MDS `allow rw fsname=<owned fs>`, OSD `allow rw tag cephfs data=<owned fs>` | the native LibCephFS oracle |
| `client.mds-cephfs-recovery-<token>` | MON `allow r`, MDS `allow rw fsname=<owned fs>`, OSD `allow r tag cephfs data=<owned fs>` | the recovery acceptance's Go clients (CephFS sessions and data-pool reads; no writes, no MGR) |
| `client.mds-cephfs-push-native-<token>` | MON `allow r`, MDS `allow rwps fsname=<owned fs>` | the push LibCephFS oracle: rename, snapshots (`s`, MAY_SNAPSHOT) and `ceph.quota` vxattrs (`p`, MAY_SET_VXATTR); metadata only, no OSD |

`CEPH_MSGR_MDS_CEPHFS_IDENTITY` and `CEPH_MSGR_MDS_CEPHFS_KEY_FILE` name the
first Go identity and `<controlDir>/mds-cephfs.key` (the metadata test and
both clients of the push test use it, each with its own GlobalID);
`CEPH_MSGR_MDS_CEPHFS_RECOVERY_IDENTITY` and
`CEPH_MSGR_MDS_CEPHFS_RECOVERY_KEY_FILE` name the recovery identity and
`<controlDir>/mds-cephfs-recovery.key`. The keys of the native and push-native
identities are never written to a host file: the fixture keeps them in memory and
sends each as one bounded standard-input record to each native worker
(`NativeCommand.Input`, ceph-testcontainers-go v0.3.0; see
`native_key_input.go`). The worker prints only a byte-count receipt, and the
fixture checks `InputWritten`, `InputDelivered`, `InputEOF` and that receipt
separately from `Complete()`. Keys and
keyrings are mode 0600 and excluded from diagnostics.

`mds-cephfs-oracle.json` publishes source pin, token, FSID, FSCID, filesystem
name, startup daemon GID, the four identities and their caps, the baseline
blocklist, the data pool ID and name (`<fs>-data`), the push baseline and
three fixed plans. The metadata plan:

- directory `ceph-msgr-cephfs-<token>` under the filesystem root;
- files `go-file` (created by Go), `native-0`, `native-1`, `native-2`
  (created natively) and the absent name `missing`;
- xattr `user.ceph-msgr` with a 28-byte binary value (`00 ff`, `ceph-msgr`,
  `00`, the raw 16-byte token);
- a 64-byte write payload (SHA-256 of `ceph-msgr-cephfs-payload:<token>`,
  twice).

The recovery plan:

- directory `ceph-msgr-cephfs-<token>-recovery` under the filesystem root;
- Go-created files `flush-plain-0..2`, `flush-requested-0..2`, `held` and
  `unanswered`, the Go-created subdirectory `boundary` holding only the
  Go-created file `replayed`, and the natively created `fenced`;
- a 5000-byte binary payload for `fenced` (a SHA-256 chain starting from
  `ceph-msgr-cephfs-fenced:<token>`).

The push plan:

- directory `ceph-msgr-cephfs-<token>-push` under the filesystem root, with
  subdirectories `lease`, `snap`, `quota` and `recall`;
- `lease/victim` (created by the Go holder client) renamed natively to
  `lease/renamed`; `snap/snap-file`, `quota/quota-file` and
  `recall/recall-00..23` (created by Go);
- snapshot `snap/.snap/ceph-msgr-snap-<token[:8]>`;
- quota `ceph.quota.max_bytes` = 16 MiB + 4096 x (first two token bytes,
  big-endian) on `quota`;
- recall settings `mds_min_caps_per_client` 8 and `mds_max_caps_per_client`
  12, and a `cache drop` timeout of 10 s.

`mds-cephfs-owned.json` is the journal. It is rewritten before each identity
creation and each native mutation, configuration change, eviction or restart
step (intent), and after its outcome. It also records the current owned MDS incarnation and the
session addresses of owned Go identities that the tests named.

## Current target and the owned restart

Every action proves the current owned rank-0 incarnation before and after it
runs: FSID, FSCID, daemon name, GID, rank 0, endpoint, nonce, incarnation and
pools. That incarnation starts as the fresh fixture's daemon and changes only
through the owned restart below. `mds-cephfs-target.json` publishes it with
`adopted_restarts` (0 or 1); both tests read it instead of the startup oracle
when they build their MDS target and check receipts.

The restart is a set of closed steps, in the order the test uses them,
journaled as one record that is never reissued:

1. `restart-arm` (identity-checked): records the current incarnation as the
   armed daemon and its `ip:port/nonce` addresses, and refuses if one is
   already blocklisted. Only one restart can ever be armed.
2. `restart-pause` (identity-checked, once): the bounded journal fault
   boundary on the armed incarnation. It requires an idle scrub stack
   (`scrub status`), journals `intended`, then runs `tell mds.<armed GID>
   config set mds_log_pause true`, `tell mds.<armed GID> scrub abort` and a
   `config get` readback, and records `applied` or `uncertain`. Every one of
   these commands, the idle check included, is addressed to the armed GID
   and not to the rank. The CLI resolves a role such as `mds.<fs>:0` to
   whichever daemon holds the rank when the command runs, so a command sent
   that way would follow a replacement. A decimal target that is not a rank
   of the filesystem (`max_mds` is 1, so only 0 is a rank) is a daemon GID:
   `Client::resolve_mds` uses it only while that GID is in the FSMap it just
   fetched and returns ENOENT otherwise, and the command goes to that
   incarnation's own addresses. If the armed incarnation leaves the map at
   any point of the sequence, the remaining commands fail, the pause is
   recorded `uncertain`, and the daemon that took over rank 0 has received
   none of them. While the option is set,
   `MDLog::_submit_thread` appends no journal entry, so a request journaled
   afterwards cannot become safe on that incarnation; a journal flush
   (`MDLog::flush`, from a client `REQUEST_FLUSH_MDLOG`, a scatter
   writebehind or the tick) only queues a flush marker behind the held
   entries. `MDSRankDispatcher::handle_conf_change` applies the option on
   the MDSRank finisher thread, after `config set` has returned; `scrub
   abort` on an idle rank 0 is answered from that same single finisher
   (`handle_asok_command`, `ScrubStack::scrub_abort`), so its answer shows
   that the pause was applied before the test sends the request it must
   hold. The setting is runtime-only: the respawned incarnation starts
   without it. The identity proof before and after additionally requires
   the armed GID in rank 0 around the whole step.
3. `restart-inflight <GlobalID>:<TID>` (identity-checked, once, read only):
   reads `dump_ops_in_flight` of the armed GID (same addressing as the
   pause) and records whether that client request of an
   observed owned session is in flight, early-replied (unsafe) and without
   a `journal_committed` event (`held`). A request that is not held is
   reported, not refused, so the test fails with the control still usable.
4. `restart-fire`: requires an applied pause and a held in-flight record.
   It journals `intended`, then runs `ceph mds fail <armed GID>` once and
   records `applied` or `uncertain` with the bounded CLI output. It skips
   the identity proof; the armed GID names exactly the proven incarnation,
   so a different daemon cannot be failed. The MON removes that GID from
   the map and blocklists its address vector; the daemon process respawns
   in its own container and registers again with a new GID and nonce.
5. `restart-status` (read only, repeatable): reads one native `fs dump` and
   reports rank 0 (name, GID, state, incarnation, endpoint, nonce), failed
   ranks, owned standbys, whether the armed GID is still mapped and the
   blocklist entries of the armed addresses. Only the filesystem identity
   (FSCID, name, pools, `max_mds` 1) must be unchanged; any rank state is
   reported as observed.
6. `restart-adopt`: requires the same daemon name in rank 0 `up:active` with
   a different GID, nonce and incarnation, the same FSCID and pools, the
   armed GID absent from the map and, after a pause, `mds_log_pause` false
   on the new incarnation, read from the adopted GID (the pause is then
   recorded `discarded` with the removed incarnation). It records the old incarnation's blocklist
   entries, makes the new incarnation current, rewrites the target file and
   then runs the ordinary post-action proof against it.
7. `restart-resume` (identity-checked, once): only for a restart that was
   never fired; sets `mds_log_pause` false on the armed GID with the same
   barrier and readback and records `resumed` (or `resume-uncertain`). The test uses it
   to reconcile a failure between the pause and the fire.

Between `restart-fire` and `restart-adopt` every other action is refused
before native work, since no identity proof is possible.

## Push extension

The push acceptance needs native operations that make the MDS push to a Go
session, and two daemon settings that make it recall capabilities.
Preparation records the baseline it must return to and refuses to adopt
anything else:

- the filesystem's `allow_snaps` flag from `fs get` must already be true
  (snapshots are enabled by default for new filesystems); filesystem flags
  are never changed;
- no central configuration entry may exist for `mds_min_caps_per_client` or
  `mds_max_caps_per_client` in any section; the owned daemon's effective
  values (`tell mds.<fs>:0 config get`, 100 and 1048576 observed) are recorded
  together with the section `mds.<owned daemon name>`.

The push driver is a second fixed Python LibCephFS program run as the push
identity (30 s bound for `stat`, 45 s otherwise). It admits only the push
directory, its children and grandchildren, and
`<dir>/<child>/.snap/<name>` for snapshot actions; extra arguments are only
a rename destination under the same rules or a decimal quota. `rename`
reports the monotonic time `fs.rename` took and `statx` of both names;
`mksnap`/`rmsnap` run `mkdir`/`rmdir` in `.snap` and `statx` the snapshot;
`setquota` sets `ceph.quota.max_bytes` and reads it back (ENODATA is reported
as absent). The output shape is checked as for the other drivers; the quota
readback is recorded for the test to judge rather than required.

The recall settings are owned provider overrides
(`override.SetConfig`/`Restore`) on the owned daemon's section.
`push-recall-on` journals and applies the minimum, then the maximum, and
returns once the daemon reports both; the upkeep thread then recalls any
session above the maximum down to the minimum
(`Server::recall_client_state`). `push-recall-max-off` restores the maximum
only, so that `push-cache-drop` recalls through `cache drop` (which also
sends FLUSHMSG and waits for its acknowledgement) without the upkeep thread
racing it; `push-recall-off` restores what is left. Each restore waits until
the daemon reports the baseline value and, after the last one, requires that
no central entry remains.

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
| `restart-arm`, `restart-pause`, `restart-fire`, `restart-status`, `restart-adopt`, `restart-resume` | none | the owned restart and journal pause above |
| `restart-inflight` | `<GlobalID>:<TID>` of an observed owned session | read only: `tell mds.<armed GID> dump_ops_in_flight`, reduced to that request; requires the applied pause of an unfired restart |
| `push-stat` | `dir`, `lease`, `victim`, `renamed`, `snap`, `snap-file`, `snapshot`, `quota`, `quota-file`, `recall` | read only: `statx` through the push driver |
| `push-rename` | none | mutation, once: rename `lease/victim` to `lease/renamed` |
| `push-snapshot-create`, `push-snapshot-remove` | none | mutation, once each: `mkdir`/`rmdir` of the planned snapshot; removal requires an applied creation |
| `push-quota-set`, `push-quota-clear` | none | mutation, once each: `ceph.quota.max_bytes` to the planned value, then to 0; clearing requires an applied set |
| `push-recall-on`, `push-recall-max-off`, `push-recall-off` | none | the owned recall overrides above; on applies once, the restores require the owned override |
| `push-cache-drop` | none | mutation, once, with the minimum override applied: `tell mds.<fs>:0 cache drop 10`, reduced to the client recall return code and count, journal flush return code, trimmed dentries and duration |

Each native file action runs one fixed Python driver through the provider's
`RunNativeCommand` in the fixture container (30 s bound, 45 s for `write`
and `recovery-write`; bounded stdout/stderr). The driver mounts the owned
filesystem by name, re-checks that every path is one of the two owned
directories (both carry the `ceph-msgr-cephfs-` prefix), a direct child of
it or the recovery directory's `boundary/replayed`, prints one JSON object and always calls `shutdown`, which unmounts and
closes its own CephFS session. Output is decoded with unknown fields refused
and the exact per-action shape required; only ENOENT is accepted as a stat
failure. A failed, killed or truncated process is an error, never partially
trusted.

Every action except `restart-fire` and `restart-status` proves the current
target before and after it runs and then writes `mds-cephfs-result.json`
with the map state, the daemon's applied MDSMap epoch, the parsed native
output (or the session listing and blocklist, the recall settings readback
or the `cache drop` result), the process receipt and the mutation or restart
record. The controller holds the shared MDS fresh scope
lock and is bounded to 90 seconds. Any refused or failed action stops the
fixture's control loop (`provider.error`), as before.

## Uncertain outcomes and cleanup

A native mutation is journaled `intended` before its process starts, then
`applied` or `uncertain`; an eviction ends `evicted` or `uncertain`; the
restart ends `applied`, `uncertain` or `adopted`; a recall override ends
`applied` or `uncertain`, then `restore-intended` and `restored` or
`restore-uncertain`. None is ever reissued: a second request of the same
kind is refused whatever the first outcome. The Go tests remove the three
namespaces with their own unlink and rmdir requests (the push test first
removes its snapshot and quota natively) and verify ENOENT natively. If a
push leaf fails, the push test still asks for the recall restore and the
snapshot and quota removal that its applied steps allow, so the later tests
in the fixture do not inherit them.

Cleanup runs once, inside the cached MDS fresh cleanup and before the owned
filesystem is removed, with its own 45-second bound. It writes an `attempted`
receipt to `mds-cephfs-cleanup.json`, then:

0. Lifts a journal pause that may still hold the armed incarnation (any
   state but `resumed` or `discarded`): if the armed GID is gone from the
   FSMap the pause is recorded `discarded` (every pause command was
   addressed to that GID, so no other daemon carries the pause); if it still
   holds rank 0 the fixture sets `mds_log_pause` false on the armed GID with
   the barrier and readback, bracketed by two rank status reads naming the
   armed GID. This restore of
   runtime state is applied even after an uncertain step; a mapped armed
   GID outside rank 0 keeps the pause and fails cleanup
   (`journal_pause_restored`).
1. Restores both recall overrides through their provider handles (a
   confirmed restore is a no-op; the provider refuses an outside edit) and
   waits up to 15 s for the daemon's baseline values with no central entry
   left (`push_config_restored`).
2. Lists native sessions and reports any that still belong to the four
   identities. It never evicts.
3. Unless a Go session is still present (a native mount could then wait on
   revocations from a client that no longer answers), stats the three owned
   directories natively. `namespace_restored` records whether all are gone;
   residual entries, including a snapshot or quota left in the push
   directory, are discarded by the verified filesystem removal.
4. Compares the blocklist with the baseline and removes only new entries it
   can attribute to an owned mutation: the eviction entry (exact address and
   unchanged expiry), the armed incarnation's addresses once the armed GID
   is absent from the FSMap, and addresses of owned Go sessions the tests
   observed (for example an eviction at the end of a missed reconnect
   window). Each removal uses `osd blocklist rm <address>` and is verified
   by a final listing. Any other new entry is reported in
   `kept_new_blocklist`, left untouched, and fails cleanup.
5. Deletes the four identities. An identity whose creation outcome is
   unknown fails cleanup instead of reporting success.

`completed` is true only when the namespace check ran, every new entry was
attributed, removed and verified absent, the recall settings were back at
their baseline, no journal pause remained and all four identities were
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
the ordering and one-shot rules of the restart steps and of the journal
boundary (pause, in-flight check, fire, resume; refused before native work),
the held-request rule on op-tracker output, the scrub status shape, the
journal pause cleanup without a held pause, blocklist attribution,
op-tracker filtering by GlobalID, scope rejection before native work,
one-shot mutations and cleanup without a baseline; for the push extension the
token-bound plan and its path admission, the closed push actions, the push
driver output shapes, the `cache drop`, `config get` and `allow_snaps`
readbacks, the ordering and one-shot rules of the push steps (refused before
native work) and an unknown push identity outcome. They do not establish
live MDS behavior; see [MDS_CEPHFS_VALIDATION.md](MDS_CEPHFS_VALIDATION.md),
[MDS_CEPHFS_RECOVERY_VALIDATION.md](MDS_CEPHFS_RECOVERY_VALIDATION.md) and
[MDS_CEPHFS_PUSH_VALIDATION.md](MDS_CEPHFS_PUSH_VALIDATION.md).
