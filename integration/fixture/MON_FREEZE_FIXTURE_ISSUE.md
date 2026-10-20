# Open issue: an OSD keeps an old OSDMap after a MON freeze

Status: open, recorded and investigated 2026-10-18.

This is an observed fixture-isolation failure; its root cause and any client
contribution are not established. The two connected-MON tests pass on their
own. Afterwards, the disposable cluster is
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

## Remaining verification

- Which MON held `osd.0`'s session in the **original failing macOS run**.
  The Linux reproduction below records the OSD's MON before the freeze and
  its absence afterwards. `sessions` does not expose subscriptions; its
  `osd_epoch` is a MON send cursor, not an OSD apply ACK.
- Whether the original macOS failure recovers after a longer wait. The
  separate Linux reproduction did not recover during the additional 120 s
  observation after the failed native write.
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

## Linux investigation, 2026-10-18

The baseline is product/test source
`source-1`, with provider v0.3.0 and the
same pinned Ceph 20.2.4 image digest used above. Experiments ran on Linux
amd64, with a compatible Docker engine
to Docker: secure/AES256K client and service keys, IPv4, three MONs, two MGRs,
one BlueStore OSD, legacy-v6. These are fresh Linux fixtures, not a repeat of
the original macOS/arm64 environment.

### Negative controls

1. The original combined selector passed all three top-level tests in one
   container-runtime fixture: stall 30.43 s, stop 26.19 s, ordinary pool op
   20.64 s. The OSD's `newest_map` advanced from 17 to 44. Go froze `mon.b`,
   while the native OSD's MON session stayed on `mon.c`.
2. A separate fixture ran stall/stop twice each, as four container test
   invocations. Every invocation passed. A new pool and one explicit native
   `rados put` after each invocation also succeeded (four writes). OSD maps
   advanced from 14 to 49. The controller used `serve` in host mode to retain
   the fixture; the Go tests still ran inside the oracle container. The four
   accompanying `pg ls-by-pool` diagnostic calls timed out with exit 124;
   they do not establish PG state or an OSD failure.

These controls show why a passing connected-client freeze alone does not
establish that the OSD's own MON connection survives.

### Reproduction without the Go product client

A third fresh fixture used only native Ceph commands after bootstrap. No
`cephmsgr` client or integration test binary was executed in this experiment.

1. Read the OSD's `messenger dump` for `msgr=client`, select its single
   connected MON by `target_addr`, and match that address to the owned MON
   container. It was the peon `mon.c`; `mon.a` was leader.
2. Pause that owned MON for 20 s, resume it, and wait for three consecutive
   unchanged full-quorum observations. Do not restart either daemon.
3. Create a distinct one-PG pool and set size/min_size to 1. Read the MON's
   current OSDMap and the OSD's local status. Observe for 30 s without object
   I/O, then attempt one native write with a 45 s operation timeout.
4. After that write fails, observe for a further 120 s without restarting,
   resetting authentication, replaying the write, or sending object I/O.

Observed results:

- MON OSDMap epoch was 17; OSD `newest_map` remained 14. The local OSD stayed
  `active`, but its MON connections closed and then disappeared, and no MON
  retained an `osd.0` session.
- The native write returned `(110) Connection timed out` after 45.077 s.
- All twelve subsequent PG samples still showed `3.0` as `unknown`, with
  empty up/acting sets and both primaries -1. The last OSD status, more than
  three minutes after quorum recovery, still reported epoch 14.
- Effective `auth_mon_ticket_ttl` and `auth_service_ticket_ttl` were both
  12 s on all three MONs. The OSD's MON ping interval/timeout/hunt interval
  were 10/30/3 s.
- Native OSD logs recorded rotating-key expiry warnings during the pause,
  three `handle_auth_bad_method ... (13) Permission denied` messages during
  the hunt, then inability to obtain current OSD service secrets.

The experiment driver refused its planned MON-restart intervention because
there was no longer a connected MON to select, then cleaned up the fixture.
Its nonzero exit is retained as a driver guard, not reported as a passing
test or a successful restart experiment. Object-request delivery was not
proved after the native authentication failures, so this is not evidence
that a newer-epoch STAT/READ nudge was delivered and failed.

This reproduces the stale-map/new-pool symptom without the Go product
client. It does not establish that every earlier failure had the same cause.

### Interpretation and bounded mitigation

The pinned native `MonClient.cc` defines `_hunting()` by a nonempty pending
connection set. Failed authentication removes each candidate; failure of the
last candidate can leave neither an active nor a pending connection. The
ordinary tick's hunt/active branches then do not start a new session. This
source path is consistent with the observed authentication failures and
empty connection set; the internal pointers and the precise rejection reason
were not traced, so it remains a source-supported explanation rather than a
complete native-state proof. No clock skew was independently measured.

The fixture now gives the opt-in `CEPH_MSGR_TEST_MON_FREEZE=1` profile a
120 s AUTH lifetime, so the existing bounded pause need not consume the
native daemon's reclaim proof. Service tickets remain 12 s. Ordinary profiles
remain 12/12 s, and explicit short-ticket profiles retain their 6.5 s AUTH
lifetime. This changes development fixture configuration only; no product
authentication, security mode, GID policy or automatic retry was changed.

A fresh native-only control changed **only AUTH TTL to 120 s**, confirmed by
local readback on all three MONs; service TTL stayed 12 s. It repeated the
same 20 s freeze of the OSD's actual `mon.c` session twice in one fixture:
plain resume, then resume plus restart of that MON. Before any object I/O,
all seven samples in each 30 s window reported the current epoch (17 and 20
respectively). Both native writes succeeded (0.084/0.098 s), and native PG
queries reported `active+clean`. The sampled OSD GID remained 14189 and its
address nonce remained 1690627099 throughout; the OSD was never restarted.
This is evidence for the bounded fixture mitigation, not a general native
reconnection fix or an exhaustive root-cause proof.

The first combined Go-selector check with the AUTH-only mitigation retained
the tests' old 3 s `ConnectTimeout`. Stall passed (30.22 s), but stop failed
on its **fresh** MapVersion call with `OutcomeUnknownError: session retired`
(36.47 s); the following ordinary pool-op test still passed (21.33 s). This
failure is retained separately from the native stale-map reproduction.
`ConnectTimeout` also bounds `retireMonitor`'s wait for admitted requests.
These two subject clients now use the existing leader-stop tests' 60 s
setup/drain option. Their 2 s keepalive failure detection and 300 s operation
contexts are unchanged; no failed request is caught and retried.

A final fresh container-runtime fixture with both adjustments passed the
original selector: stall 78.53 s, stop 26.12 s, ordinary pool op 20.71 s.
The frozen requests still ended unknown at 2.036 s; stopped requests ended
unknown 0.501 s after stop began. Fresh recovery took 60.008 s in the stall
case and 7.416 s after leader resume in the stop case. The larger option also
permits a longer failed connection attempt, so this pass does not establish
fast recovery. No product timeout default was changed.

Keep the tests opt-in. The original macOS environment, MemStore, request-v9
and three-OSD variants of this mitigation still need verification.

MON `sessions` and OSD `status`/`messenger dump` were sampled through local
admin sockets with bounded replies and timeouts. Sampling is not atomic.
`sessions.osd_epoch` records maps sent by the MON, not maps applied by the
OSD, and `sessions` contains no subscription list. Quorum and map-delivery
readiness remain separate observations.

The [machine-readable receipts](MON_FREEZE_LINUX_20261018.json) include all
six fixture identities, failed and successful runs, sampled epochs and OSD
identities, effective TTL readback, final changed-Go-file hashes, and SHA256
hashes of the local public evidence. All 48 owned containers and six owned
networks were confirmed absent after cleanup. The fixture package's full
`CGO_ENABLED=0 go test .` and `go vet .` passed on Linux; focused TTL regression
checks also passed. These checks are distinct from the earlier baseline
commit's 71 successful CI jobs and do not claim full CI at the new commit.
