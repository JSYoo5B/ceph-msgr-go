# Disposable CephFS request-completion fixture

`CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION=1` is a closed development-only
extension of `CEPH_MSGR_TEST_MDS_FRESH=1`. It selects only
`^TestCephMDSCephFSCompletionIntegration$` and excludes
`CEPH_MSGR_TEST_MDS_COMMANDS=1`, `CEPH_MSGR_TEST_MDS_SESSIONS=1`,
`CEPH_MSGR_TEST_MDS_CEPHFS=1` and `CEPH_MSGR_TEST_MDS_CEPHFS_RANKS=1`: the
CephFS profile's recovery acceptance owns exactly one restart of its daemon,
and this one restarts the owned daemon up to four times and changes daemon
settings that every other MDS receipt would observe. Like the other MDS
profiles it requires one OSD with `legacy-v6` (no-HASHPSPOOL pools, 16/1 PGs,
replication and min_size 1/1) and one active rank 0 without standby. The Go
clients enable MDS only, except the missed-window client and a separate
reader, which also enable OSD with the default v6 object-hash profile. No MON
feature exception is added. The fixture termination budget is three
minutes.

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Relied-upon behavior is read from
`msg/Connection.cc` (`is_blackhole`: `ms_blackhole_client` applies to every
connection whose peer is a client), `msg/async/ProtocolV2.cc`
(`handle_message` acknowledges and drops a received client message) and
`msg/async/AsyncConnection.cc` (`send_message` drops an outgoing one and logs
it at level 0), `mds/MDLog.cc` (`mds_log_pause` stops the submit thread),
`mds/Server.cc` (`early_reply`, `journal_and_reply`, `reply_client_request`
recording the completed request, `dispatch` during reconnect,
`handle_client_request` and its completed-request path,
`handle_client_reconnect`, `reconnect_tick`, `force_clients_readonly`),
`mds/MDCache.cc` (`force_readonly`), `mds/Locker.cc`
(`handle_client_lease` RENEW, `handle_client_caps` FLUSHSNAP),
`mds/SessionMap.cc` and `mds/mdstypes.cc` (`Session::dump`,
`session_info_t::dump` with `completed_requests`), `mds/MDSRank.cc` (admin
socket commands, `evict_client`), `mds/MDSDaemon.cc` (respawn, a new
connection's closed session), `mon/MDSMonitor.cc` (`fail_mds_gid`) and
`osd/PrimaryLogPG.cc` (`do_op` blocklist check). These are LGPL-2.1
references; no code is copied.

## Why the admin socket

`ms_blackhole_client` drops every message to and from a client peer,
including the MCommand of `ceph tell mds.<x>` (the CLI is a client). Every
command addressed to the MDS daemon therefore runs through its admin socket,
`ceph --admin-daemon /var/run/ceph/ceph-mds.<id>.asok`, executed with the
provider's `RunNativeCommand` in the daemon's own container (it is attached to
the cluster network; the daemon respawns in place, so the socket path stays).
MON commands (`fs dump`, `fs get`, `osd blocklist ls`, `mds fail`, central
configuration) keep using the control container. The same reason makes the
blackhole drop the Go client's REQUEST_FLUSH_MDLOG, so the journal is flushed
with the admin socket's `flush journal`.

A delivered request whose reply is lost cannot be produced with the blackhole
alone: the messenger acknowledges and drops the request itself. The fixture
instead pauses the journal (`mds_log_pause`) and disables early replies
(`mds_early_reply false`) first, so the MDS executes the request in memory and
waits for the journal; it then turns the blackhole on and resumes the journal,
which commits the request and drops its safe reply.

## Identities and published receipts

Preparation proves the fresh fixture's single active rank 0, records the
complete OSD blocklist and requires an empty admin-socket `session ls`. It
reads the daemon's baseline values of `ms_blackhole_client`,
`mds_early_reply`, `mds_log_pause`, `debug_mds`, `debug_ms`,
`mds_deny_all_reconnect`, `mds_reconnect_timeout`,
`mds_session_blocklist_on_timeout` and `mds_session_blocklist_on_evict`,
refuses anything but the defaults for the changed ones (a reconnect timeout
above the planned 5 s, both blocklist options true), refuses an existing
central entry of any changed option and requires that the filesystem already
allows snapshots. It then creates two identities, journaling each name before
the provider call:

| Identity | Caps | Use |
| --- | --- | --- |
| `client.mds-cephfs-completion-<token>` | MON `allow r`, MDS `allow rw fsname=<owned fs>`, OSD `allow r tag cephfs data=<owned fs>` | every Go client (CephFS sessions and data-pool reads; no writes, no MGR) |
| `client.mds-cephfs-completion-native-<token>` | MON `allow r`, MDS `allow rws fsname=<owned fs>`, OSD `allow rw tag cephfs data=<owned fs>` | the native LibCephFS oracle: listing, `statx`, one create+write, one snapshot (`s`, MAY_SNAPSHOT) |

`CEPH_MSGR_MDS_CEPHFS_COMPLETION_IDENTITY` and
`CEPH_MSGR_MDS_CEPHFS_COMPLETION_KEY_FILE` name the Go identity and
`<controlDir>/mds-cephfs-completion.key`. The native key
is never written to a host file: the fixture keeps it in memory and
sends it as one bounded standard-input record to each native worker
(`NativeCommand.Input`, ceph-testcontainers-go v0.3.0; see
`native_key_input.go`). The worker prints only a byte-count receipt, and the
fixture checks `InputWritten`, `InputDelivered`, `InputEOF` and that receipt
separately from `Complete()`. Keys are mode
0600 and excluded from diagnostics.

`mds-cephfs-completion-oracle.json` publishes source pin, token, FSID, FSCID,
filesystem name, startup daemon GID, both identities and caps, the baseline
blocklist and daemon settings, the data pool ID and name and the plan:

- directory `ceph-msgr-completion-<token>` under the filesystem root;
- children `lease`, `replayed`, `lost-file`, `lost-dir`, `fenced`, `ro-file`,
  `ro-dir`, `snap`, and grandchildren `lease/leased`, `snap/snap-file`;
- snapshot `snap/.snap/completion-<token[:8]>`;
- a 5000-byte binary payload for `fenced` (a SHA-256 chain starting from
  `ceph-msgr-cephfs-completion-fenced:<token>`);
- the short reconnect window, 5 s.

`mds-cephfs-completion-target.json` publishes the current owned incarnation
and the number of adopted restarts. `mds-cephfs-completion-owned.json` is the
journal: it is rewritten before each identity creation, native mutation,
runtime or central setting change, eviction and restart step (intent) and
after its outcome, and records the session addresses of owned Go sessions the
test named.

## Closed control

```go
testcluster.RequestControl(ctx, dir,
    "mds-cephfs-completion", "mds-cephfs-completion-completed",
    token, action, arguments...)
```

The request is `<requestID> <ownership token> <action> [arguments]`. IDs are
positive canonical decimals and strictly increase. Targets are closed plan
names; GlobalIDs and address nonces are canonical decimals. No caller path,
value or command reaches the fixture. One request is outstanding at a time;
the controller holds the shared MDS fresh scope lock and is bounded to 110
seconds.

| Action | Arguments | Native effect |
| --- | --- | --- |
| `observe` | GlobalID | read only: `session ls` (with the completed-request record of that GlobalID: `num_completed_requests`, each TID and created inode, requests in flight, reconnecting) and `osd blocklist ls`; records the address of an owned Go session |
| `ops-in-flight`, `historic-ops` | GlobalID | read only: `dump_ops_in_flight` or `dump_historic_ops`, reduced to client requests of an observed owned session (description, flag point, result, event names) |
| `stat` | `dir`, a child, a grandchild or `snapshot` | read only: LibCephFS `statx` or ENOENT |
| `list` | none | read only: `opendir`/`readdir` of the directory with `statx` per entry; only plan children are admitted, each once |
| `write-fenced` | none | mutation, once: create `fenced` (`O_CREAT\|O_EXCL\|O_WRONLY`, 0644), write the payload, `fsync`, close, `statx` |
| `snapshot-create`, `snapshot-remove` | none | mutation, once each: `mkdir`/`rmdir` of the planned snapshot; removal requires an applied creation |
| `log-pause`, `early-reply-off`, `blackhole-on`, `debug-on` | none | runtime change through the admin socket (below) |
| `log-resume`, `early-reply-on`, `blackhole-off`, `debug-off` | none | restore of that runtime change to the baseline |
| `flush-journal` | none | admin socket `flush journal`; return code 0 required |
| `force-readonly` | none | mutation, once: admin socket `force_readonly` |
| `config-read` | none | read only: the closed option list through `config get` |
| `deny-on`, `deny-off`, `window-on`, `window-off` | none | central override of the owned daemon's section (below) |
| `evict` | GlobalID | mutation, once per GlobalID: admin socket `session evict id=<gid>` of an open or stale session of the Go identity whose address is not blocklisted; waits until the session is gone and the address blocklisted |
| `mds-log-mark` | none | read only: records the current line count of the owned daemon's container log |
| `mds-log` | GlobalID, address nonce | read only: log lines after the mark (below) |
| `restart-arm`, `restart-fire`, `restart-status`, `restart-adopt` | none | an owned restart (below) |

Every action outside a pending restart proves the current owned incarnation
before and after it runs (the MON view: FSID, FSCID, daemon name, GID, rank 0,
endpoint, nonce, incarnation, pools) and then the admin socket's `status`:
the same GID, `whoami` 0, `up:active` and an applied MDSMap epoch at least the
map's. The receipt (`mds-cephfs-completion-result.json`) carries the map,
the daemon status, sessions and completed-request records, blocklist, native
output and process receipt, the mutation, runtime or central record, the
configuration readback, restart record and rank status, op-tracker entries,
the journal flush result and log lines.

### Runtime changes

A runtime change sets each of its options with `config set` on the admin
socket and requires `config get` to report the value (`applied`, otherwise
`uncertain`). It is bound to the daemon incarnation that answered and never
reaches the central database, so a daemon restart discards it; firing a
restart marks the armed incarnation's changes `discarded-by-restart`, and the
test reads the new incarnation's values back. `log-pause` sets
`mds_log_pause true`, `early-reply-off` sets `mds_early_reply false`,
`blackhole-on` sets `ms_blackhole_client true`, and `debug-on` sets
`debug_mds 10/10` and `debug_ms 1/5`. A change of one kind is in effect at
most once per incarnation; a restore requires it and sets the recorded
baseline. Native driver actions and evictions are refused while the
blackhole or the journal pause is in effect (a native mount and a session
close would wait forever). Only `debug-on`/`debug-off` are admitted during a
pending restart, on the respawned daemon.

### Central overrides

`deny-on` sets `mds_deny_all_reconnect true` and `window-on` sets
`mds_reconnect_timeout 5` in the section `mds.<owned daemon>` through the
provider's `TemporaryConfig`; they take effect in the next incarnation (the
first is also a runtime option and must be reported by the running daemon).
Each kind is applied once, only one is unrestored at a time, and `-off`
restores it through its handle, then requires the daemon to report the
baseline for `mds_deny_all_reconnect` and no central entry of either option.

### Owned restarts

At most four restarts, strictly sequential, each four closed steps journaled
as one record and never reissued:

1. `restart-arm` (proved): records the current incarnation and its
   `ip:port/nonce` addresses; refused if the previous restart is not adopted,
   the budget is spent or an armed address is already blocklisted.
2. `restart-fire`: journals `intended`, runs `ceph mds fail <armed GID>` once
   and records `applied` or `uncertain` with the bounded output. The daemon
   respawns in its container with a new GID and nonce.
3. `restart-status` (read only, repeatable): one native `fs dump` reported as
   observed, whether the armed GID is still mapped and the blocklist entries
   of the armed addresses.
4. `restart-adopt`: requires the same daemon name in rank 0 `up:active` with a
   new GID, nonce and incarnation, unchanged FSCID and pools and the armed GID
   absent; records the old incarnation's blocklist entries, makes the new
   incarnation current and rewrites the target file.

Between fire and adopt only `restart-status`, `restart-adopt`, `config-read`,
`debug-on`, `debug-off`, `mds-log-mark` and `mds-log` are admitted. The
daemon-local ones first require that the admin socket's `status` names the GID
the FSMap holds in rank 0 for the owned daemon name, whoami 0, and not the
armed GID; everything else is refused before native work.

### Daemon log

The MDS runs in the foreground and logs to its container output. `mds-log`
reads that output (bounded to 256 MiB, lines cut at 8 KiB) from the mark and
keeps lines that name `client.<gid>` (followed by `:`, space, `)`, `,` or
`]`), lines with a client message (`client_reply(`, `client_caps(`,
`client_lease(`, `client_session(`, `client_snap(`, `client_request(`) that
are blackholed or addressed to `:0/<nonce>`, and lines with one of a closed
set of markers (the completed-request conversion and replay queue, read-only,
`flushsnap`, reconnect denial, timeout and eviction, and closed-session
reconnect lines). At most 600 lines of at most 2048 bytes are returned with
the count of dropped ones.

## Cleanup

Cleanup runs once, inside the cached MDS fresh cleanup and before the owned
filesystem is removed, with its own 100-second bound. It writes an
`attempted` receipt to `mds-cephfs-completion-cleanup.json`, then:

1. If a restart is pending, waits up to 60 s for an active new incarnation
   (it becomes active by itself when its reconnect window closes).
2. Restores every runtime change still in effect on the current incarnation
   through the admin socket (a change of an earlier incarnation is recorded
   as discarded by the restart), restores both central overrides through
   their handles, and requires the daemon's blackhole, early-reply, journal
   pause, debug and deny values to equal the baseline and no central entry
   to remain (`config_restored`).
3. Lists sessions and reports any of the two identities. It never evicts.
4. Unless a Go session is still present or the settings were not restored,
   stats the owned directory natively; `namespace_restored` records whether
   it is gone.
5. Compares the blocklist with the baseline and removes only new entries it
   can attribute to an owned mutation: an eviction entry (exact address and
   unchanged expiry), the addresses of an armed incarnation whose GID left
   the FSMap, and addresses of owned Go sessions the test observed (a denied
   or late reconnect evicts with a blocklist entry). Each removal uses
   `osd blocklist rm <address>` and is verified by a final listing; any other
   new entry is reported in `kept_new_blocklist` and fails cleanup.
6. Deletes both identities. An identity whose creation outcome is unknown
   fails cleanup.

`completed` is true only when the settings were restored, the namespace check
ran, every new blocklist entry was attributed, removed and verified absent and
both identities were removed. The MDS fresh cleanup receipt adds
`cephfs_completion_verified`; filesystem, pool and identity removal proceed
even if this verification failed, and both errors are reported.

## Unit coverage

Fixture unit tests cover the exclusive profile and selector (including its
exclusion of the other MDS profiles and RBD), the credential environment,
identity caps, the token-bound plan and its path admission, closed actions
and replay protection, native output shapes, the admin socket status,
configuration and completed-request readbacks, the log filter and its bounds,
the runtime and central change rules (one change per kind and incarnation,
restores only of owned changes, refusal during a restart, native work
refused while blackholed or paused), the restart order and budget, blocklist
attribution across several restarts, scope rejection before native work and
cleanup without a baseline. They do not establish live MDS behavior; see
[MDS_CEPHFS_COMPLETION_VALIDATION.md](MDS_CEPHFS_COMPLETION_VALIDATION.md).
