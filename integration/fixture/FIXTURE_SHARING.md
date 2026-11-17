# Fixture sharing classification

Context: issue #178, part of #171. Before several catalog rows can run on one
fixture, each test must be known not to leave state that a later test could
depend on or be hurt by. This document defines the classification and where
it is declared. It runs nothing on a shared fixture: the default and periodic
coverage still start one fixture per row.

## Declaration

Every native test (`TestCeph...Integration` in `integration` and `cephmsgr`)
declares its value beside its function, as the last line of its doc comment:

```go
// TestCephExampleIntegration reads the OSDMap.
//
//fixture:shareable reads MON maps and command descriptions only
func TestCephExampleIntegration(t *testing.T) {
```

The value is `shareable` or `isolated`. The reason is one line of printable
ASCII, at most 120 bytes and without a final period. It names what the test
changes (daemons, cluster flags, configuration, owned pools, MDS state) and
what its cleanup proves.

A test is `shareable` only if, when it runs, it changes nothing outside the
pools, objects, images or filesystem entries it creates and removes again
with a checked cleanup, or its cleanup already proves the start state: it
restores the change with a checked command and, for a daemon it stopped,
waits for the state the change disturbed (a settled full MON quorum, every
OSD up and in with every PG `active+clean` and no pg_temp). Everything else is
`isolated`, including a test that kills, stops or pauses a daemon without
such a proof, runs `mgr fail`, leaves a configuration entry, a CRUSH rule, a
blocklist entry, a pool flag or a self-managed snapshot behind, or rewrites
the fixture's own object without verifying it. A test is classified by what
it does when it runs; its skip conditions do not matter. The
[start-state check](FIXTURE_START_STATE.md) is the gate between tests; the
classification decides which tests are offered to it.

## Shared state and the start-state check

The start-state check sees only what it compares. A leftover in a compared
category stops the next test at the check, so a test that changes such state
may restore it as an ordinary last step. A leftover in a category the check
does not compare would reach the next test unseen. A test that changes such
state is `shareable` only if its restoration runs on every path: it is
registered (`t.Cleanup` or `defer`) before the first change, uses its own
context that is still usable after the body's context ended, and fails the
test unless it confirms the restored state. A restoration that runs only as
the last step of the body, that is registered after the change, or that
ignores the command's result makes the test `isolated`. So does reading
state that other tests change and the check does not compare (cluster log
history).

Compared by the start-state check:

| Category | Condition |
| --- | --- |
| MON, MGR, OSD and MDS containers of the snapshot are running; the fixture's other containers keep their state; no foreign Testcontainers container | `daemon-running`, `container-state`, `foreign-container` |
| MON quorum names; MGR availability | `mon-quorum`, `mgr-available` |
| OSD IDs, up and in | `osd-set`, `osd-up`, `osd-in` |
| every PG `active+clean`; no pg_temp | `pg-clean`, `pg-temp` |
| pool names and pool flags | `pool`, `pool-flags` |
| erasure-code profile names and CRUSH rule names | `erasure-code-profile`, `crush-rule` |
| OSDMap flags (`noout` and the others) | `osd-flags` |
| central configuration (`ceph config dump`) | `config` |
| runtime overrides of the snapshot's daemons (admin socket `config diff`) | `daemon-config` |

Not compared, so a `shareable` test restores it on every path or does not
change it:

| Category | Examples of changes | Classification |
| --- | --- | --- |
| cluster and audit log | `log` appends; log watches from version 1 | `isolated` for a test that appends or reads log history (below) |
| config-key store | `config-key set`, `rm`, `del` | restoration on every path, confirmed absent |
| auth entities, keys and caps | `auth get-or-create`, `caps`, `import`, `rm` | restoration on every path, confirmed |
| OSDMap blocklist | `osd blocklist add` | restoration on every path, confirmed |
| upmap, primary_temp, primary affinity, CRUSH weights and reweight | `osd pg-upmap-items`, `osd reweight` | no shareable test changes them |
| CRUSH hierarchy (buckets and hosts) | `osd crush add-bucket`, `move` | restoration on every path, confirmed |
| pool properties other than name and flags: size, PG count, application, quota, pool snapshots | `osd pool set`, PG split and merge | restoration on every path, confirmed natively |
| objects, xattrs and OMAP in the fixture's pools | the fixture object, owned UUID objects, cache pool heads | fixture object: restoration registered before the change and verified natively; owned objects: cleanup registered before they are created (below) |
| CephFS entries, snapshots, quotas and client sessions | `mkdir`, `create`, snapshots, quotas | restoration on every path, confirmed |
| MGR service map | native service holders | restoration on every path, confirmed absent |
| MGR modules and the active MGR | `mgr module disable`, `mgr fail` | no shareable test changes them |
| health mutes | `health mute` | restoration on every path, confirmed |
| in-memory daemon state outside the configuration | PG `scrubdebug` flags | restoration on every path, confirmed by a fresh read |
| runtime overrides of a daemon outside the snapshot (an MDS the fixture does not record as owned) | MDS `config set` | restoration on every path, confirmed by a fresh read |
| MON leader, election epoch and map epochs | MON stop and restart; any map change | not restorable; no shareable test depends on a particular leader or epoch value |

The cluster log cannot be restored, and every MON command adds an audit
entry, so it changes in every test. A test that appends entries or reads
retained history (a `WatchLogs` from version 1) therefore depends on what the
tests before it did and is `isolated`. A test that only causes daemon or
audit entries stays `shareable` as long as no shareable test reads log
history.

Owned UUID objects have names no other test uses. A leftover of one, after a
test failed before its cleanup knew the object, is not read by any other
test as a precondition: the tests that list PGs or read pool statistics
compare the Go result with a native read taken at the same time, not with a
start value. Such objects therefore do not make a test `isolated` as long as
its checked cleanup is registered before the first object is created.

## Rows

A row's fixture configuration is its parameters without `test_run`. A row is
`shareable` only when every test its effective selector selects in either
suite is `shareable`. The effective selector is the row's `test_run`, or the
fixture's own default selection when the row has none (`loadProfile`). A
broad row, whose default selects the whole suite, is therefore `isolated` as
long as any native test is.

The fixture derives the row values: `TestCatalogRowsDeclareFixtureSharing`
in [`sharing_test.go`](sharing_test.go) reads the declarations with
`go/parser` from the test files the build context selects (the same files as
the native inventory of `ci_catalog_test.go`), passes every row through the fixture's `loadProfile` as the
native workflow forwards it, and reports

- a native test without a declaration, with two, or with an invalid value or
  reason;
- a `//fixture:` comment that is not on a native test's doc comment;
- a row the fixture refuses, a row whose selector selects no native test and
  a row that selects a test without a valid declaration (that row has no
  value).

`python3 tools/scenario_plan.py validate` runs it and fails on every problem
and on every row without a value. `python3 tools/scenario_plan.py sharing`
prints each configuration with its shareable and isolated rows and the job
count of the default coverage if each configuration's shareable rows shared
one fixture. Both need the fixture module's Go toolchain.

Adding a native test adds its declaration in its own file. No shared list
changes.

## Findings at classification

The classification covers 255 native tests and 130 rows: 81 tests and 19
rows are `shareable`. The 130 rows have 101 configurations; five
configurations have more than one shareable row, so the default coverage
would need 124 jobs instead of 130.

A first pass declared 103 tests `shareable`. Checking each of them against
the categories above made 22 of them `isolated`:

- cluster log: `TestCephLogRecoveryIntegration`, `TestCephLogWatchIntegration`,
  `TestCephMonPingIntegration`, `TestCephNamedMonitorTellIntegration` and
  `TestCephConfigStreamIntegration` append entries and read history from
  version 1; `TestCephHostnameConfigIntegration` watches the log and adds
  CRUSH buckets;
- config-key: `TestCephCommandConstructionIntegration` removes its key only as
  the last step, `TestCephCRCConnectionModeIntegration` registers its removal
  after the set and ignores the result code, and
  `TestCephMonitorWriteStallIntegration` leaves an applied key when it fails;
- auth: `TestCephOSDCommandCapsChangeIntegration`,
  `TestCephOSDCommandCapsRenewalIntegration` and
  `TestCephMonCommandCapsChangeIntegration` create their entity before
  registering its removal and ignore the `auth rm` result;
  `TestCephRevokedCredentialRecoveryIntegration` and
  `TestCephReplacedCredentialIntegration` restore the fixture's
  `client.revocable` from a copy written in the test, not from the start
  state;
- blocklist: `TestCephOSDWatchBlocklistIntegration` registers the removal
  after the entry is added;
- health mutes: `TestCephDigestIntegration` leaves its sticky mute when the
  unmute fails;
- CephFS: `TestCephMDSCephFSPushIntegration` reverts its snapshot, quota and
  files only in its last step, and `TestCephMDSCephFSIntegration` removes its
  directory only in a final subtest;
- service map: `TestCephServiceMapNativeHolderIntegration`,
  `TestCephMonServiceMapLargeMapIntegration`,
  `TestCephMonServiceMapLifecycleIntegration` and
  `TestCephMonServiceMapLifecycleFailedMonHoldIntegration` kill a live holder
  in cleanup without proving that its service map entry is gone.

`TestCephAppliedMonitorMutationIsNotReplayedIntegration` keeps `shareable`:
its config-key removal is deferred before the set, uses its own context and
fails the test unless a native read confirms the key is absent.
`TestCephConfigCommandsIntegration` removes its central configuration entry
as its last step, which the `config` condition covers.

Two leftovers of the shared owned-pool cleanup (`mode_cleanup` in
[`tools/osd_pool_oracle.py`](../../tools/osd_pool_oracle.py)) make every test
that removes its owned pools through it `isolated`, including every
four-OSD erasure-capacity control:

- it sets `mon_allow_pool_delete=true` in the MON section of the central
  configuration and never removes it. The fixture starts without that entry
  ([`osd_cache_pools.go`](osd_cache_pools.go) restores its own window), so
  the start-state `config` condition fails afterwards;
- an erasure-coded owned pool is created without a rule name, so the MON
  creates a CRUSH rule named after the pool, and nothing removes it; the
  `crush-rule` condition fails afterwards.

The outage controls' own proofs are recorded in their reasons. The
`osd-ready clean` wait covers OSDs up and in and every PG `active+clean`, but
not pg_temp or the original placement.
