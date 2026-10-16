# Disposable multi-rank CephFS fixture

`CEPH_MSGR_TEST_MDS_CEPHFS_RANKS=1` is a closed development-only extension of
`CEPH_MSGR_TEST_MDS_FRESH=1`. It selects only
`^TestCephMDSCephFSRanksIntegration$` and excludes
`CEPH_MSGR_TEST_MDS_COMMANDS=1`, `CEPH_MSGR_TEST_MDS_SESSIONS=1` and
`CEPH_MSGR_TEST_MDS_CEPHFS=1`: those profiles assume one active rank, and
this one changes the topology (pins, a standby takeover) that their receipts
prove. Like them it requires one OSD with `legacy-v6`, so the Go client
enables only MDS and needs no modern OSD profile and no extra MON admission
bits. No MON feature exception is added. The fixture termination budget is
three minutes.

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Relied-upon behavior is read from
`mds/MDBalancer.cc` (`handle_export_pins`: a pin to another rank exports a
non-empty directory, a pin to the own rank makes an auxiliary subtree),
`mds/Migrator.cc` (export, `flush_client_sessions`,
`finish_export_inode_caps`, `finish_import_inode_caps`), `mds/MDCache.cc`
(`path_traverse`, `request_forward`), `mds/MDSRank.cc`
(`forward_message_mds`, `command_get_subtrees`, the `mds` perf counters),
`mds/Server.cc` (`handle_client_getattr`, the `ceph.dir.pin` vxattr,
`flush_session`, `handle_client_reconnect`), `mon/MDSMonitor.cc`
(`fail_mds_gid`), `mds/MDSDaemon.cc` (respawn after removal from the map)
and `client/Client.cc` (`handle_client_request_forward`,
`handle_cap_export`, `handle_cap_import`, `add_update_cap`). The pin recipe
follows the guidance in JSYoo5B/ceph-testcontainers-go#48. These are LGPL-2.1
references; no code is copied.

## Topology

The fresh fixture creates the owned filesystem with
`cephfs.Config{ActiveMDS: 2, StandbyMDS: 1}` (provider
`github.com/JSYoo5B/ceph-testcontainers-go` v0.2.0) and the same legacy
no-HASHPSPOOL pools as the other MDS profiles (16 and 1 PGs, replication and
min_size 1/1, one OSD; no second OSD is needed). The provider sets
`refuse_standby_for_another_fs` and no standby replay. The fresh oracle
(`mds-fresh-oracle.json`) still describes rank 0; for this profile it
requires `max_mds` 2, ranks 0 and 1 `up:active`, exactly one owned standby
and three owned daemons, all with filesystem affinity. The MDS balancer is
not enabled (`balance_automate` stays false), so only export pins move
subtrees.

## Identities and published receipts

Preparation proves the stable topology (below) and that rank 0 is the fresh
oracle's daemon, records the complete OSD blocklist, requires empty native
`session ls` on both ranks, and creates two identities, journaling each name
before the provider call:

| Identity | Caps | Use |
| --- | --- | --- |
| `client.mds-cephfs-ranks-<token>` | MON `allow r`, MDS `allow rw fsname=<owned fs>` | the Go client (no OSD or MGR caps) |
| `client.mds-cephfs-ranks-native-<token>` | MON `allow r`, MDS `allow rwp fsname=<owned fs>` | the native LibCephFS oracle: `statx` and the `ceph.dir.pin` vxattr (`p`, MAY_SET_VXATTR); metadata only |

`CEPH_MSGR_MDS_CEPHFS_RANKS_IDENTITY` and `CEPH_MSGR_MDS_CEPHFS_RANKS_KEY_FILE`
name the Go identity and `<controlDir>/mds-cephfs-ranks.key`. The native
key is never written to a host file: the fixture keeps it in memory and
sends it as one bounded standard-input record to each native worker
(`NativeCommand.Input`, ceph-testcontainers-go v0.3.0; see
`native_key_input.go`). The worker prints only a byte-count receipt, and the
fixture checks `InputWritten`, `InputDelivered`, `InputEOF` and that receipt
separately from `Complete()`. Keys are mode 0600 and excluded from diagnostics.
Every owned daemon (both ranks and the standby) is added to `peers.json`, so
the host relay admits the standby once it takes over rank 0.

`mds-cephfs-ranks-oracle.json` publishes source pin, token, FSID, FSCID,
filesystem name, both identities and their caps, the baseline blocklist, the
startup topology, the startup per-rank views and the fixed plan:

- directory `ceph-msgr-ranks-<token>` under the filesystem root;
- `fwd` with the file `fwd/target` (forward leaf);
- `mig` with the files `mig/file-0..2` (migration leaf).

The Go client creates and removes all of it. The fixture only stats it and
pins `fwd` and `mig`. `mds-cephfs-ranks-owned.json` is the journal: it is
rewritten before each identity creation, pin change and takeover step
(intent) and after its outcome, and records the startup and current
topology, the pin baselines and the session addresses of the owned Go
identity that the test named.

## Topology proof

A topology is one native `fs dump`: the owned FSCID, name and pools must be
unchanged and no rank damaged; ranks, daemons (name, GID, rank, state,
incarnation, the v2 endpoint and nonce of the selected family) and owned
standbys are reported as observed. It is stable when `max_mds` is 2, `in` is
[0, 1], nothing failed, ranks 0 and 1 are `up:active` on two owned daemons,
exactly one owned daemon is `up:standby` with this filesystem's affinity, no
other standby exists and every daemon has an address and nonzero nonce.

Every action except `takeover-fire` and `takeover-status` proves the stable
topology with the current daemons (both ranks' name, GID, rank, incarnation,
endpoint and nonce, and the standby's name, GID, endpoint and nonce) before
it runs, and again afterwards together with each rank's own `status`
(`whoami`, GID, `up:active`, applied MDSMap epoch at least the map epoch).
Per-rank commands address the proven daemon by name (`tell mds.<daemon>`).

## Closed control

```go
testcluster.RequestControl(ctx, dir,
    "mds-cephfs-ranks", "mds-cephfs-ranks-completed",
    token, action, arguments...)
```

The request is `<requestID> <ownership token> <action> [arguments]`. IDs are
positive canonical decimals and strictly increase. Targets are closed plan
names; pin values are `-1`, `0` or `1`. No caller path, value or command
reaches the fixture. One request is outstanding at a time; the controller
holds the shared MDS fresh scope lock and is bounded to 110 seconds.

| Action | Arguments | Native effect |
| --- | --- | --- |
| `observe` | one GlobalID | read only, per rank: `session ls`, `get subtrees` (entries of the owned tree and the root, plus the total count), `perf dump mds` (`forward`, `exported`, `exported_inodes`, `imported`, `imported_inodes`) and `status`; then `osd blocklist ls`. Records the address of an owned Go session with that GlobalID |
| `stat` | `dir`, `fwd`, `fwd/target`, `mig`, `mig/file-0..2` | read only: LibCephFS `statx` (ino, mode, uid, gid, nlink, size) or ENOENT |
| `pin` | `fwd` or `mig`, value | mutation, see below |
| `pin-restore` | `fwd` or `mig` | mutation: back to the value read before the first owned change |
| `placement` | `fwd` or `mig`, rank 0 or 1 | read only, bounded wait (60 s, 250 ms period) until both ranks agree on the directory's authority, then the views with sessions |
| `takeover-arm`, `takeover-fire`, `takeover-status`, `takeover-adopt` | none | the owned takeover below |

Native actions run one fixed Python LibCephFS driver through the provider's
`RunNativeCommand` in the fixture container (30 s bound, bounded
stdout/stderr). It mounts the owned filesystem by name as the native
identity, re-checks that the path is the owned directory or below it (at
most two levels), prints one JSON object and always calls `shutdown`.
`pin-get` reports `statx` and, for an existing directory, the
`ceph.dir.pin` virtual xattr; `pin-set` sets it. Output is decoded with
unknown fields refused and the exact per-action shape required; a failed,
killed or truncated process is an error, never partially trusted.

### Owned pins

A pin change uses three separate mounts, so the setting mount's own readback
is never evidence (the restoration trap described in #48):

1. a fresh mount reads the current value and inode; the first owned change
   of a directory records that value as its baseline; a later change
   requires the same inode and the value the fixture last applied;
2. the change is journaled `intended`, a second mount sets the value and
   shuts down, and the outcome is journaled;
3. a third fresh mount (a different native GlobalID) must read the requested
   value on the same inode; the change is then `applied`, otherwise
   `uncertain`.

A value equal to the current one is recorded `unchanged` without a set. A
directory with an uncertain change refuses further changes. A stored pin is
a requested policy, not proof of placement: `placement` separately requires
that the pinned rank's `get subtrees` lists the path as its own subtree
(`is_auth`, `auth_first` equal to the rank, `auth_second` -2, single
authority) and that the other rank does not claim it or reports the same
single authority. A mixed snapshot is observed again; at the bound the
receipt reports `converged` false instead of failing the control loop.
Restoring the baseline (-1 for the Go-created directories) removes the local
pin; it does not move authority back.

### Owned standby takeover

The takeover is four closed steps, journaled as one record that is never
reissued:

1. `takeover-arm` (proved): records the current rank-0 daemon, its
   `ip:port/nonce` addresses, the standby expected to take over and rank 1,
   and refuses if an armed address is already blocklisted. Only one
   takeover can ever be armed.
2. `takeover-fire`: journals `intended`, then runs
   `ceph mds fail <armed GID>` once and records `applied` or `uncertain` with
   the bounded CLI output. The MON removes that GID, blocklists its address
   vector and assigns rank 0 to the standby; the failed daemon respawns in
   its container and registers again as a standby with a new GID and nonce.
3. `takeover-status` (read only, repeatable): one native `fs dump` reported
   as observed (any rank state), whether the armed GID is still mapped and
   the blocklist entries of the armed addresses.
4. `takeover-adopt`: waits (60 s) until the topology is stable with the
   armed standby holding rank 0 `up:active` under its own GID in a new
   incarnation, rank 1 unchanged, the armed GID absent and the failed
   daemon back as the one owned standby with a new GID. It records the old
   incarnation's blocklist entries, makes this topology current and then
   runs the ordinary post-action proof.

Between `takeover-fire` and `takeover-adopt` every other action is refused
before native work.

## Cleanup

Cleanup runs once, inside the cached MDS fresh cleanup and before the owned
filesystem is removed, with its own 100-second bound. It writes an
`attempted` receipt to `mds-cephfs-ranks-cleanup.json`, then:

1. Waits up to 60 s for the stable topology. The fixture never restarts a
   daemon: after the takeover the failed daemon returns as the standby by
   itself. Without a stable topology no native mount is attempted.
2. Lists native sessions on both ranks and reports any of the two
   identities. It never evicts.
3. Unless a Go session is still present (a native mount could then wait on
   revocations from a client that no longer answers), returns each pinned
   directory that still exists to its baseline with the same three-mount
   recipe (a removed directory took its pin with it), then stats the owned
   directory natively. `namespace_restored` records whether it is gone.
4. Compares the blocklist with the baseline and removes only new entries it
   can attribute to an owned mutation: the failed rank-0 incarnation's
   addresses once its GID is absent from the FSMap, and addresses of owned
   Go sessions the test observed. Each removal uses
   `osd blocklist rm <address>` and is verified by a final listing. Any other
   new entry is reported in `kept_new_blocklist`, left untouched, and fails
   cleanup.
5. Deletes both identities. An identity whose creation outcome is unknown
   fails cleanup instead of reporting success.

`completed` is true only when the topology returned, the pins were restored
or removed with their directories, the namespace check ran, every new
blocklist entry was attributed, removed and verified absent, and both
identities were removed. The MDS fresh cleanup receipt adds
`cephfs_ranks_verified`; filesystem, pool and identity removal proceed even
if this verification failed, and both errors are reported.

## Standby-replay extension

`CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY=1` extends this profile (it
requires `CEPH_MSGR_TEST_MDS_CEPHFS_RANKS=1` and keeps every exclusion
above). It selects only `^TestCephMDSCephFSRanksReplayIntegration$`, starts
the owned filesystem with `cephfs.Config{ActiveMDS: 3, StandbyMDS: 1}` (four
owned MDS containers; the provider still sets no standby replay) and has a
four-minute termination budget. The base profile's plan, actions and
receipts are unchanged. Additional relied-upon behavior:
`mon/MDSMonitor.cc` (`maybe_promote_standby` assigns a free standby to the
lowest followable rank, `fail_mds_gid` does not blocklist a standby-replay
daemon), `mds/FSMap.cc` (`find_replacement_for` prefers the rank's
standby-replay follower, `promote` moves it to `up:replay`),
`mds/MDLog.cc` (`mds_log_pause`), `mds/Migrator.cc` (`export_frozen` flushes
client sessions after the freeze and `export_go` waits for their
acknowledgements; `export_finish` unfreezes and retries waiters) and
`mds/MDCache.cc` (`path_traverse` waits on a frozen dirfrag, forwards to the
known dirfrag authority, and discovers instead when the forward count
exceeds the depth).

The extension's steady topology is three active ranks and one spare. Before
`replay-enable` the spare is the owned `up:standby`; afterwards it is an
owned `up:standby-replay` follower of rank 0 (an MDSMap `info` entry with
rank 0, not a standby), and every proof uses that shape.

The plan adds `chain` with `chain/hop` and its files `anchor`,
`forwarded-file`, `forwarded-dir`; `inflight` with `anchor`, `held-file`,
`held-dir`; and `replay` with `safe-0`, `safe-1`, `inflight-file`,
`inflight-dir`. The Go client creates and removes all of it. The driver
admits paths up to three levels below the owned directory and adds `list`
(readdir with each entry's `statx`, at most 64 entries, `d_ino` equal to the
`statx` inode).

| Action | Arguments | Native effect |
| --- | --- | --- |
| `pin`, `placement` | also `chain/hop`, `chain`, `inflight`; ranks up to 2 | as above |
| `list` | `dir`, `chain/hop`, `inflight`, `replay` | read only: native listing |
| `ops` | rank, an observed owned GlobalID | read only: that rank daemon's `dump_historic_ops`, client requests of that GlobalID only |
| `ops-in-flight` | rank, an observed owned GlobalID | read only: that rank daemon's `dump_ops_in_flight`, client requests of that GlobalID only, in `ops_in_flight` with the sampled daemon in `ops_daemon`. The replay leaf takes two samples of rank 0 at least 3 s apart while its journal is paused (the ceph-testcontainers-go v0.3.0 unsafe-window receipt) |
| `replay-enable` | none | mutation: journals `intended`, runs `fs set <owned fs> allow_standby_replay true` once, then waits (60 s, 100 ms period) until the owned standby, same name, GID, endpoint and nonce, follows rank 0 with every rank unchanged; the followed topology becomes current |
| `log-pause`, `log-resume` | none | mutation: runtime-only `mds_log_pause` of the current rank-0 incarnation through `tell mds.<name> config set`, read back with `config get`; the pause requires the read value `false`, is issued once before the takeover and is journaled first; the resume restores the read value |

While a pause is in effect only `observe`, `ops`, `ops-in-flight`, `placement`,
`log-resume` and the takeover actions run: a native mount opens a journaled
session on every rank and would wait on rank 0. The takeover arms the spare
(standby or follower) and records ranks 1 and 2; adoption requires the
spare in rank 0 with a new incarnation, ranks 1 and 2 unchanged and the
failed daemon back as the spare in the current shape (the new follower of
rank 0 once replay is enabled). A pause of the failed incarnation is then
recorded `discarded`: the runtime value went with the respawned process.

Cleanup waits for the steady shape (either shape if the enable's outcome is
not `following`), restores a pause its incarnation still holds or records it
`discarded` when that GID is gone, and skips native mounts when a pause
cannot be attributed. `runtime_restored` is part of `completed`. Pins are
restored nested directory first. The `allow_standby_replay` flag belongs to
the owned filesystem and is removed with it; no other blocklist entry is
expected, since failing a follower does not blocklist it.

## Unit coverage

Fixture unit tests cover the exclusive profile and selector (including its
exclusion of the single-rank CephFS profile), the two-rank filesystem
configuration, the fresh oracle's two-rank acceptance and missing-standby
refusal, the credential environment, identity caps, the token-bound plan and
its path admission, closed actions and replay protection, stable and
transitional topology parsing with identity comparison, subtree, counter and
daemon-status parsing, placement convergence (including an ambiguous
authority and an auxiliary subtree on rank 0), native output shapes, the
takeover pending rule, scope rejection before native work and cleanup
without a baseline. The extension's tests
(`mds_cephfs_ranks_replay_test.go`) cover its profile and selector, the
three-rank filesystem configuration, the extension plan, its actions and
their refusal in the base profile, three-rank and follower topologies
(including a follower of another rank), three-rank placement, the listing
shape and the journal pause's active states. They do not establish live MDS
behavior; see [MDS_CEPHFS_RANKS_VALIDATION.md](MDS_CEPHFS_RANKS_VALIDATION.md)
and [MDS_CEPHFS_RANKS_REPLAY_VALIDATION.md](MDS_CEPHFS_RANKS_REPLAY_VALIDATION.md).
