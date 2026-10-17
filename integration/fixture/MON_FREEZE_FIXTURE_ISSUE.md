# Open issue: an OSD keeps an old OSDMap after a MON freeze

Status: open, recorded 2026-10-18. Investigation continues elsewhere.

This is a fixture-isolation problem, not a client defect. The two
connected-MON tests pass on their own. Afterwards, the disposable cluster is
sometimes left in a state where the OSD no longer follows the OSDMap, and the
next test that creates a pool fails.

## Tests involved

`integration/mon_connected_stall_test.go`:

- `TestCephMonPoolOpConnectedStallIntegration` freezes the peon the client is
  connected to (`docker pause`). It then resumes the peon and restarts it with
  `recycleResumedMon`.
- `TestCephMonPoolOpConnectedStopIntegration` freezes the leader and stops the
  connected peon while requests wait on it. It then resumes the leader,
  starts the peon and restarts the resumed leader.

Both tests are opt-in with `CEPH_MSGR_TEST_MON_FREEZE=1` until this issue is
resolved, so the default CI selector, which matches `MonPoolOp`, skips them.

## Reproduction

The Ceph 20.2.4 pinned image runs in a container on Docker Desktop (macOS).
The profile is legacy-v6, single BlueStore OSD, three MONs (a, b, c).

```sh
CEPH_MSGR_TEST_MON_FREEZE=1 CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_RUN='^TestCephMonPoolOp(ConnectedStall|ConnectedStop|)Integration$' \
sh integration/run.sh
```

Go runs the files alphabetically, so the freeze test runs first, then the stop
test, then `TestCephMonPoolOpIntegration`. Set `CEPH_MSGR_TEST_DIAGNOSTICS` to
keep `osd-pool-command.json`.

## Symptom

The test that follows the freeze test fails while creating its owned pool. The
native oracle (`tools/osd_pool_oracle.py` `mode_create`) runs
`rados_write_full` on a probe object, which returns `-110` after the 45 s
`rados_osd_op_timeout`. In a debug run that left the cluster in this state:

- `pg ls-by-pool` showed the new pool's only PG as `unknown`, with empty
  up/acting sets and `up_primary -1`.
- `ceph -s` reported `1 pg inactive`, quorum `a,b,c` (leader a), and 1 OSD up
  and in.
- The OSD's own `status` (OSD command) reported `newest_map 17` and did not
  change for more than 70 s, although the cluster had created newer epochs
  (a fourth pool).

In every run the frozen MON was the peon `c` (172.19.0.4); the leader was `a`.

## Frequency

| Variant | Runs | Failed |
| --- | --- | --- |
| Freeze, then resume and settle the election | 5 (including one debug run) | 3 |
| Freeze, then resume, restart the resumed MON and settle | 3 | 1 |

Restarting the resumed MON did not remove the failure. The freeze test's own
assertions passed in every run.

## Tried

- Settled-election barrier after the resume: the quorum was always full and
  stable, but the OSD's map still did not advance.
- Stop and start of the resumed MON (`recycleResumedMon`): still failed in
  1 of 3 runs.

## Not tried yet

- Which MON holds `osd.0`'s session when the OSD is stuck:
  `MonTellTo(name, {"prefix":"sessions"})` for a, b and c. Comparing the OSD's
  `newest_map` with `MapVersion(osdmap)` at the start of the next test was
  prepared but not run.
- Whether the OSD catches up by itself after a longer wait (beyond 70 s).
- Nudging the OSD with a request that carries a newer map epoch, which makes
  the OSD subscribe for that epoch.
- Restarting the OSD. This is not usable on MemStore profiles, which lose the
  fixture objects, and it changes the OSD address and routing.
- Whether the three-OSD and request-v9 (IPv6, MemStore) profiles show the
  same thing when the tests run in sequence. The freeze test alone passed on
  request-v9 IPv6 MemStore.

## Evidence that stands

Each of these was verified within its own test and is recorded in
MON_POOL_OP_VALIDATION.md:

- When the connected MON is frozen, the in-flight pool op, statfs and map
  version requests end as unknown outcomes at the 2 s keepalive.
- The client moves to another MON and nothing is replayed.

The freeze test passed on legacy-v6 single BlueStore and on request-v9 IPv6
single MemStore.

The stop variant passed every run in which it got past its pool setup
(legacy-v6 only):

- The three requests ended as unknown outcomes about 0.2 s after `docker stop`
  (EOF).
- Fresh requests were answered about 7.6 s after the leader resumed.
- The held allocation never executed.

These stop results are not yet in the validation records or SPEC.
