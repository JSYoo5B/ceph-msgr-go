# Fixture start-state check

Context: issue #179, part of #171. A fixture that is shared between tests
must not let one test's leftover state become the next test's precondition.
The start-state check finds such leftovers and names each one.

## Start snapshot

`startCluster` records the snapshot after every preparation step of the
profile has settled (OSD pools and CRUSH hierarchy, MDS filesystems, RBD and
cache pools, the native oracle worker and, for the host runtime, the relay),
immediately before the control loop starts. No control request has run
yet, so the snapshot is the state every test of the fixture starts from.
Every read below must complete for the snapshot, including the admin socket
of every daemon. A snapshot failure
does not fail the fixture: it is kept, and every later check fails with the
`start-snapshot` condition and its cause.

The check compares the snapshot kept in the fixture's memory. A copy is
written to `start-state-snapshot.json` in the control directory as evidence,
readable by its owner only (mode 0600). In that copy the value of every
configuration entry and runtime override whose option name contains `key`,
`secret`, `password` or `token` is left out and marked `"value_redacted":
true`.

The snapshot holds the decoded fields listed below and the IDs of the
fixture's own containers: those of `fixture-ownership.json` (the native
oracle client, each MON, MGR, OSD and MDS daemon and the control container)
and any Testcontainers container of the fixture's own session that is
listed at that moment.

## Reads

One observation is made of independent reads:

1. the Docker container list, first;
2. the MON reads `ceph status`, `ceph osd dump`,
   `ceph osd erasure-code-profile ls`, `ceph osd crush rule ls` and
   `ceph config dump`, from the native oracle client;
3. `config diff` on the admin socket of each MON, MGR, OSD and MDS daemon of
   the snapshot (MON, MGR and OSD through the provider's daemon handle, an
   MDS in its own container), skipped for a daemon whose container the
   listing shows not running.

Each MON read and each admin socket read runs concurrently after the
listing. Each condition is computed from the one read it depends on (the
table below names it). A read that fails or prints output that does not
decode fails the `observation` condition, naming the read, and hides only
the conditions computed from it. Every other read is still compared: with
`ceph status` showing peering PGs and `ceph osd dump` timing out, the report
names both `pg-clean` and the `observation` of `ceph osd dump`; with the
MONs stopped, it still names `daemon-running` from the listing and
`daemon-config` from the admin sockets.

## Control

A test requests the check with
`testcluster.ControlDaemon(ctx, dir, "verify", "start-state", "<seconds>")`,
which publishes `verify-start-state <id> <seconds>`. The budget is optional,
in whole seconds from 1 to 300, and defaults to 30 seconds. The check polls
once a second until no condition fails or the budget ends; each MON read,
admin socket read and the Docker listing has its own 10-second bound. In a
poll that the end of the budget cut short, a read that did not complete
keeps the failures its latest completed read found, if it found any, and
is otherwise reported as its `observation`; the reads the poll completed
replace their earlier failures. The result of the last poll is written to
`start-state.json` before the acknowledgement `start-state-verify.<id>`:

```json
{"started_at": "...", "budget_ms": 30000, "elapsed_ms": 1234, "polls": 2,
 "passed": false, "failures": [{"condition": "osd-flags", "detail": "OSDMap flags set [noout], cleared []"}]}
```

A failed check is a result. The fixture keeps serving, so a caller can
restore the state and check again. Only an invalid request (a budget outside
the range or extra fields) is a fixture control failure.

## Conditions

Each failure names one condition. A read that fails or returns output that
does not decode fails the `observation` condition for that poll; the
conditions of every other read are still compared.

| Condition | Requirement | Native source |
| --- | --- | --- |
| `daemon-running` | Every MON, MGR, OSD and MDS container of the snapshot exists and is `running` (not paused, exited or removed). | Docker container list |
| `container-state` | The native oracle client and the control container exist and keep their start state. | Docker container list |
| `mon-quorum` | The quorum names equal those at start. | `ceph status`: `quorum_names` |
| `mgr-available` | A manager is available if one was at start. | `ceph status`: `mgrmap.available` |
| `osd-set` | The OSD IDs equal those at start. | `ceph osd dump`: `osds[].osd` |
| `osd-up` | Every OSD is up. | `ceph osd dump`: `osds[].up` |
| `osd-in` | Every OSD is in. | `ceph osd dump`: `osds[].in` |
| `pg-clean` | Every PG is exactly `active+clean`, the states cover every PG, and PG statistics exist if PGs existed at start. | `ceph status`: `pgmap.pgs_by_state`, `pgmap.num_pgs` |
| `pg-temp` | No pg_temp entry exists. | `ceph osd dump`: `pg_temp` |
| `pool` | The pool names equal those at start. | `ceph osd dump`: `pools[].pool_name` |
| `pool-flags` | Every pool of the start state keeps its flags. | `ceph osd dump`: `pools[].flags_names` |
| `erasure-code-profile` | The erasure-code profiles equal those at start. | `ceph osd erasure-code-profile ls` |
| `crush-rule` | The CRUSH rules equal those at start. | `ceph osd crush rule ls` |
| `osd-flags` | The OSDMap flags equal those at start. | `ceph osd dump`: `flags` |
| `config` | Every central configuration entry (section, mask, name) has its start value; none was added or removed. | `ceph config dump` |
| `daemon-config` | Every daemon read has exactly the runtime overrides it had at start, with the same values: none added, changed or removed. | admin socket `config diff`: the `override` level of each option |
| `foreign-container` | Every container with the `org.testcontainers=true` label belongs to the fixture's start set. | Docker container list |

Configuration and override values in a report are quoted and cut to 128
bytes; a value whose option name contains `key`, `secret`, `password` or
`token` is shown as `<redacted>`.

## Runtime overrides

Ceph keeps each option's value per level (`src/common/config.cc` at
`v20.2.4`): `default`, `mon` (the MON configuration database), `file`,
`env`, `cmdline` and `override`. `ceph tell <daemon> config set`, an MCommand
`config set` sent to a daemon, an admin socket `config set` and `injectargs`
all set the `override` level, and `config unset` removes it; `config diff`
lists every option with a non-default level. `daemon-config` compares the
`override` level only. The `mon` level follows the MON database, which
`config` already compares, and the other levels do not change at runtime.

The fixture and integration paths that change daemon-local runtime
configuration, found with `git grep` for `config set`, `config unset`,
`injectargs`, `tell` and `--admin-daemon` in `integration` and
`integration/fixture`:

| Path | Daemon | Options |
| --- | --- | --- |
| `mgr_tell_outcome.go` (MGR Tell outcome cells) | named MGR | `log_max_recent` |
| `daemon_command_outcome.go` (daemon command outcome cells) | OSD, MDS | `log_max_recent` |
| `copyfrom_error.go` | OSD | `osd_debug_inject_copyfrom_error`, `osd_copyfrom_max_chunk` |
| `runtime_config.go` (`override.SetDebugLevel` windows and direct admin socket sets) | OSD, MDS | debug levels, `mds_log_pause`, `ms_blackhole_client` and the MDS CephFS profiles' other runtime options |
| `mds_cephfs_ranks.go`, `mds_cephfs_recovery.go` (`tell mds.<name or GID> config set`) | MDS | `mds_log_pause` and the ranks profile's runtime options |
| `osd_command_crash_test.go` (MCommand `config set` to an OSD) | OSD | `osd_bench_small_size_max_iops` |

All of them set the `override` level, so a leftover of any of them fails
`daemon-config` on a daemon of the snapshot. The check does not list these
options: it compares every override of every daemon.

## Limits

- `daemon-config` reads the daemons of the snapshot: each MON, MGR and OSD,
  and the MDS daemons of `fixture-ownership.json` (the `mds_fresh`
  profiles). An MDS, RGW or mirror daemon that the fixture does not record
  as owned is not read.
- A daemon restarted after the snapshot loses its runtime overrides; one
  that had an override at start then fails `daemon-config` with that
  override removed.
- Pools are compared by name and flags. Size, PG count, application
  metadata and objects are not compared.
- Auth entities, OSDMap blocklist entries, upmap entries and primary_temp
  are not compared.
- A container that replaced a daemon's container fails both `daemon-running`
  (the start container is gone) and `foreign-container` (the new one is not
  in the start set).

## Native evidence

One exploratory run on a Linux amd64 host with Docker 29.9.1 used catalog
row `osd.aes.ip4` (one memstore OSD, legacy-v6, aes keys, IPv4, secure
Messenger, container runtime) with the pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
and the native result gate. For that run only, a temporary test
`TestCephFixtureStartStateExploratoryIntegration` was added to `integration`
and the row's selector pointed at it; neither is part of the repository.
The test changed the fixture through the Go client's MON commands and
called the check after each change and each restoration:

| Step | Budget | Result | Condition and detail |
| --- | --- | --- | --- |
| fresh fixture | 60 s | passed, 1 poll, 1.2 s | |
| `osd set noout` | 3 s | failed, 2 polls | `osd-flags`: OSDMap flags set [noout] |
| `osd unset noout` | 60 s | passed, 1 poll | |
| `config set osd osd_recovery_sleep 0.25` | 3 s | failed, 2 polls | `config`: osd/osd_recovery_sleep set, absent at start |
| `config rm` | 60 s | passed, 1 poll | |
| `osd pool create start-state-leftover` | 3 s | failed, 2 polls | `pool`: pools added [start-state-leftover] |
| pool removed (with a temporary `mon_allow_pool_delete`, removed again) | 120 s | passed, 1 poll | |
| `osd out 0` | 3 s | failed, 2 polls | `osd-in`: osd.0 is out |
| `osd in 0` | 180 s | passed, 1 poll | |

The test passed in 23.69 s and the gate reported its PASS. One poll took
about 1.2 s. An earlier run of the same recipe showed that the second poll
of a 3-second budget was cut by the budget and its context error replaced
the `osd-flags` finding; the check now keeps the last completed comparison
in that case.

A second exploratory test on the same row and image, at the commit that
added `daemon-config` and the independent sources, changed daemon-local
runtime configuration through the Go client's MGR and MON tell commands
and stopped two of the three MONs through the fixture's daemon control:

| Step | Budget | Result | Condition and detail |
| --- | --- | --- | --- |
| fresh fixture | 60 s | passed, 1 poll, 1.3 s | |
| MGR tell `config set log_max_recent 10002` | 3 s | failed, 2 polls | `daemon-config`: mgr.a runtime override log_max_recent set to "10002", absent at start |
| MGR tell `config unset log_max_recent` | 60 s | passed, 1 poll | |
| MGR tell `injectargs --log_max_recent=10003` | 3 s | failed, 2 polls | `daemon-config`: mgr.a runtime override log_max_recent set to "10003", absent at start |
| MGR tell `config unset log_max_recent` | 60 s | passed, 1 poll | |
| MON tell `config set log_max_recent 10004` | 3 s | failed, 2 polls | `daemon-config`: mon.a runtime override log_max_recent set to "10004", absent at start |
| MON tell `config unset log_max_recent` | 60 s | passed, 1 poll | |
| MGR override 10005 left, mon.b and mon.c stopped | 25 s | failed, 4 polls | `observation`: ceph osd dump: context deadline exceeded; `daemon-running`: mon.b and mon.c containers exited; `daemon-config`: mgr.a override 10005 |
| mon.b and mon.c started, quorum of three back, override unset | 180 s | passed, 1 poll | |

The test passed in 42.86 s and the gate reported its PASS; the row took
101 s and cleanup left no Testcontainers container. One poll still took
about 1.3 s with the admin socket reads. In the stopped-MON step the
reported MON read is `ceph osd dump`, so `ceph status` returned in that
poll; why it did is not established. A first run of the same recipe
failed only because the test required the text `ceph status` in that
step; the check's report was the same.

The stopped OSD, non-clean PG, pg_temp, foreign container and MDS admin
socket paths are covered by the fixture unit tests only.
