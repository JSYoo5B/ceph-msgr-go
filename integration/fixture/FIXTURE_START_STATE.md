# Fixture start-state check

Context: issue #179, part of #171. A fixture that is shared between tests
must not let one test's leftover state become the next test's precondition.
The start-state check finds such leftovers and names each one.

## Start snapshot

`startCluster` records the snapshot after every preparation step of the
profile has settled (OSD pools and CRUSH hierarchy, MDS filesystems, RBD and
cache pools, the native oracle worker and, for the host runtime, the relay),
immediately before the control loop starts. No control request has run
yet, so the snapshot is the state every test of the fixture starts from. It
is written to `start-state-snapshot.json` in the control directory. A
snapshot failure does not fail the fixture: it is kept, and every later
check fails with the `start-snapshot` condition and its cause.

The snapshot holds the decoded fields listed below and the IDs of the
fixture's own containers: those of `fixture-ownership.json` (the native
oracle client, each MON, MGR, OSD and MDS daemon and the control container)
and any Testcontainers container of the fixture's own session that is
listed at that moment.

## Control

A test requests the check with
`testcluster.ControlDaemon(ctx, dir, "verify", "start-state", "<seconds>")`,
which publishes `verify-start-state <id> <seconds>`. The budget is optional,
in whole seconds from 1 to 300, and defaults to 30 seconds. The check polls
once a second until no condition fails or the budget ends; each native
command and the Docker listing has its own 10-second bound. The result of the
last poll is written to `start-state.json` before the acknowledgement
`start-state-verify.<id>`:

```json
{"started_at": "...", "budget_ms": 30000, "elapsed_ms": 1234, "polls": 2,
 "passed": false, "failures": [{"condition": "osd-flags", "detail": "OSDMap flags set [noout], cleared []"}]}
```

A failed check is a result. The fixture keeps serving, so a caller can
restore the state and check again. Only an invalid request (a budget outside
the range or extra fields) is a fixture control failure.

## Conditions

Each failure names one condition. A native command that fails or returns
output that does not decode fails the `observation` condition for that poll.

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
| `foreign-container` | Every container with the `org.testcontainers=true` label belongs to the fixture's start set. | Docker container list |

Configuration values in a report are quoted and cut to 128 bytes; a value
whose option name contains `key`, `secret`, `password` or `token` is shown as
`<redacted>`.

## Limits

- `config` compares the MON configuration database only. A runtime override
  made through a daemon's admin socket or `ceph tell ... config set` does
  not appear in `ceph config dump` and is not detected.
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
in that case. The stopped OSD, non-clean PG, pg_temp and foreign container
conditions are covered by the fixture unit tests only.
