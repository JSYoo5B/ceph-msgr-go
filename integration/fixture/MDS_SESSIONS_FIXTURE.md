# Disposable CephFS client session fixture

`CEPH_MSGR_TEST_MDS_SESSIONS=1` is a closed development-only extension of
`CEPH_MSGR_TEST_MDS_FRESH=1`. It selects only
`^TestCephMDSSessionsIntegration$`, excludes `CEPH_MSGR_TEST_MDS_COMMANDS=1`
and, like the command fixture, requires one OSD with `legacy-v6`. The owned
metadata/data pools disable HASHPSPOOL (16/1 PGs, replication/min_size 1/1)
so the public Go client can enable only MDS, without EnableOSD or the modern
OSD profile's extra MON admission bits. No MON feature exception is added.
Other profiles keep their pools, selectors and one-minute cleanup budget; this
profile's fixture termination budget is two minutes.

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The relied-upon server behavior is
read from `mds/Server.cc` (`handle_client_session`, `find_idle_sessions`,
`update_required_client_features`), `mds/MDSRank.cc` (`evict_client`,
`dump_sessions`), `mds/SessionMap.cc` (`Session::dump`), `mds/mdstypes.cc`
(`feature_bitset_t::print`, `client_metadata_t::dump`) and
`mon/FSCommands.cc` (LGPL-2.1 references; no code is copied).

## Identities and published receipts

The existing fresh fixture supplies the UUID filesystem, its active rank-0 MDS,
exact FSID/FSCID/name, daemon GID, incarnation and v2 address/nonce, and the
read-only `client.mds-fresh-<token>` identity (MON `allow r`, MDS
`allow r fsname=<owned fs>`). The session clients use that identity.

Preparation adds one extra identity, `client.mds-sessions-foreign-<token>`,
with MON `allow r` and MDS `allow r fsname=ceph-msgr-foreign-<token>`, a
filesystem name that does not exist. It has no MGR or OSD caps.
`CEPH_MSGR_MDS_SESSIONS_FOREIGN_IDENTITY` and
`CEPH_MSGR_MDS_SESSIONS_FOREIGN_KEY_FILE` name it and
`<controlDir>/mds-sessions-foreign.key`. Keys and keyrings are mode 0600 and
excluded from diagnostics.

Before the identity is created, preparation records an independent baseline:
`fs get` (`refuse_client_session`, `required_client_features`,
`session_timeout`, `session_autoclose`), the provider's session timeouts,
the central configuration database (no `mds_defer_session_stale` entry may
exist), the active daemon's runtime `mds_defer_session_stale`,
`mds_session_blocklist_on_evict` and `mds_session_blocklist_on_timeout`
(`tell mds.<fs>:0 config get`), and the complete native OSD blocklist. The
baseline must have no refusal, no required client features, defer-stale and
evict-blocklist enabled, and timeouts other than the stale window.

`mds-sessions-oracle.json` publishes source pin, token, FSID, FSCID, filesystem
name, daemon GID, both identities, the foreign filesystem name, the baseline
and the stale window. `mds-sessions-owned.json` is the mutation journal. It is
rewritten before every native mutation (intent), after its reply, and after
each observation that binds a GlobalID to a session address.

## Closed control

```go
testcluster.RequestControl(ctx, dir,
    "mds-sessions", "mds-sessions-completed",
    token, action, globalIDs...)
```

The request is `<requestID> <ownership token> <action> [GlobalIDs]`. IDs are
positive canonical decimals and strictly increase; GlobalIDs are distinct
positive canonical decimals. Only these actions exist:

| Action | Arguments | Native effect |
| --- | --- | --- |
| `observe` | 1 to 8 GlobalIDs | read only |
| `refuse-on` / `refuse-off` | none | `fs set <fs> refuse_client_session true/false` |
| `require-on` / `require-off` | none | `fs required_client_features <fs> add/rm reply_encoding` |
| `require-reclaim-on` / `require-reclaim-off` | none | `fs required_client_features <fs> add/rm reclaim_client` |
| `stale-on` / `stale-off` | none | `session_timeout` 30 s and `session_autoclose` 120 s through the provider's `TemporarySessionTimeouts`, plus central `mds mds_defer_session_stale=false` through `TemporaryConfig`; restore through both handles |
| `evict` | one GlobalID | `tell mds.<fs>:0 session evict id=<gid>` |

Each toggle is applied at most once and restored at most once. `refuse-on`
and both `require-*-on` actions require the map at its baseline, so at most one
requirement is applied at a time. Each requirement has its own journal kind
(`required_client_features` for `reply_encoding`,
`required_client_features_reclaim_client` for `reclaim_client`); the feature
is fixed by the action, never by caller input. Both `require-*-on` actions
additionally require an empty native session list, because
`update_required_client_features` evicts and blocklists every open session
lacking the new feature. Cleanup removes each requirement whose journal kind
was touched and that native state still shows. `evict` requires the GlobalID to be natively `open`
and its exact address absent from the baseline and current blocklist.

`mds_defer_session_stale=false` is part of the stale window because, with the
native default, `find_idle_sessions` never marks a session without capabilities
stale. Such a session stays open until `session_autoclose` and is then evicted
directly. The library session holds no capabilities in this stage.

Every mutation is followed by a settle wait: `fs get` must show the requested
state, and the active daemon's `tell ... status` `mdsmap_epoch` must reach that
map epoch, so the next session request is evaluated with it. The stale window
also waits for the daemon's runtime `mds_defer_session_stale` value. An
eviction is confirmed when the GlobalID is absent from `session ls` and its
exact address appears in `osd blocklist ls`; the entry's expiration is
journaled.

Mutations prove the owned target first: FSID, FSCID, daemon GID, rank 0,
endpoint, nonce, incarnation and pools must equal the startup oracle. Every
action, including `observe`, repeats this check afterwards. A successful action
then writes `mds-sessions-result.json` with the map state, daemon map epoch,
daemon defer-stale value, parsed and raw native `session ls` for rank 0, the
blocklist, the baseline blocklist and the mutation record. Parsed sessions keep
ID, state, CephX auth name, entity type, address type, exact
`IP:port/nonce`, cap and lease counts, `client_features.feature_bits`,
`metric_spec.metric_flags.feature_bits` and every other client metadata
key/value. The controller holds the shared MDS fresh scope lock and is bounded
to 90 seconds. Errors are written into the result before the controller fails.
Native results are evidence for the Go test, which applies its own checks.

Addresses of sessions observed for GlobalIDs that the test supplied are
recorded as owned. Only these addresses can authorize a later blocklist
removal.

## Uncertain outcomes and cleanup

A failed or interrupted native mutation is journaled `uncertain` (or
`restore-uncertain`) and never reissued by the controller; the control loop
fails and the test sees the provider error. The provider handles for the
stale window keep their own uncertainty state.

Cleanup runs once, inside the cached MDS fresh cleanup and before the owned
filesystem is removed, with its own 45-second bound. It first writes an
`attempted` receipt to `mds-sessions-cleanup.json`. It then:

1. Restores the defer-stale configuration and session timeouts through their
   provider handles (no-ops after a confirmed restore; outside edits refused).
2. Reads `fs get` independently. A refusal flag or a `reply_encoding` or
   `reclaim_client` requirement that differs from the baseline is reverted only
   if the journal records an owned mutation of that kind. It then waits for the map and the
   daemon epoch to match the baseline.
3. Verifies that no central `mds_defer_session_stale` entry remains and that
   the daemon's runtime value is back to the baseline.
4. Lists the blocklist. A new exact entry is removed with
   `osd blocklist rm <IP:port/nonce>` only if its address belongs to an owned
   session. An eviction entry whose expiration differs from the journaled one
   is an outside edit and is not removed. New entries without owned provenance
   are reported in `unowned_new_blocklist` and left untouched. A final listing
   must not contain any removed entry.
5. Deletes the foreign identity. If its creation outcome is unknown, cleanup
   reports failure instead of success.

The receipt records reconciliation, restored flags, the final map, the daemon
value, removed/unowned/final blocklist entries, identity removal and errors.
`completed` is true only when every restoration was verified. The MDS fresh
cleanup receipt adds `sessions_restored`; filesystem, pool and identity removal
then proceed even if session restoration failed, and both errors are reported.

## Unit coverage

Fixture unit tests cover the exclusive profile and selector, legacy pool
configuration, foreign caps, closed actions and replay protection, native
session listing, map and daemon parsers (IPv4 and IPv6 addresses), exact
blocklist lookup, scope rejection before native work, and cleanup without a
baseline. They do not establish live MDS behavior, which needs the separate
acceptance run recorded in [MDS_SESSIONS_VALIDATION.md](MDS_SESSIONS_VALIDATION.md).
