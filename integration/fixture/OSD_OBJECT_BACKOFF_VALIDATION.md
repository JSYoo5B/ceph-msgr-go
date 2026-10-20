# Object-scope backoff and in-flight primary failover — 2026-10-16

This records the earlier connection-wide gate at its tested source. The
subsequent product scope correction and new native runs are recorded in
[range-scoped backoff admission](OSD_BACKOFF_SCOPE_VALIDATION.md). The
earlier observations below are preserved as historical evidence.

This acceptance extends the fixed `OSDProfileReplicatedPG` profile to a
disposable pool with three BlueStore replicas. It exercises two native paths
that the single-OSD PG-wide backoff acceptance ([recipe](OSD_BACKOFF_RECIPE.md),
[contract](../../internal/osd/BACKOFF_CONTRACT.md)) does not reach: a
single-object MOSDBackoff BLOCK/UNBLOCK pair released by recovery, and the
loss of a primary while a Go write is held in flight on it. No product code
changed. The product adds no placement, retry, replay or range scheduling here.

Test: `integration/osd_object_backoff_test.go`
(`TestCephOSDObjectBackoffFailoverIntegration`). Native recipe:
`tools/osd_object_backoff_oracle.py` (mock regressions in
`tools/test_osd_object_backoff_oracle.py`), dispatched by the fixture control
`verify-osd-object-backoff`, which accepts only `osd=1`, a replicated-PG
profile, three replicas and BlueStore (`integration/fixture/control.go`,
`object_backoff_control_test.go`).

## Source-grounded trigger

Ceph v20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`:

- `PrimaryLogPG::do_op` (`PrimaryLogPG.cc:2206-2234`): a write-ordered op on an
  object that is missing on an acting replica calls
  `add_backoff(session, head, head)` only when the session negotiated
  RADOS_BACKOFF and `osd_backoff_on_degraded` is true. The pinned default is
  false (`common/options/global.yaml.in:3237-3242`, marked "mainly for
  debug"); the op is then parked in `waiting_for_degraded_object` instead. An
  object missing on the primary backs off by default only when it is unfound.
- Both branches call `maybe_kick_recovery` (`PrimaryLogPG.cc:618-643`), which
  recovers that object immediately, regardless of the `norecover` flag.
- `PG::add_backoff` (`PG.cc:570-594`) sends BLOCK with begin == end == the
  head object at the PG's current map epoch and drops the op without queueing
  it. `on_global_recover` (`PrimaryLogPG.cc:505-523`) calls
  `release_backoffs(soid)`, and `PG::release_backoffs` (`PG.cc:596-657`) sends
  UNBLOCK with the original bounds and ID.
- Native `Objecter::handle_osd_backoff` (`Objecter.cc:3835-3920`) ACKs with
  the original epoch and resends in-range ops on UNBLOCK.

A default-configuration Tentacle cluster therefore sends object-scope BLOCK
only for unfound objects. This acceptance sets `osd_backoff_on_degraded=true`
in the disposable cluster's `osd` config section for leaf 1, and removes it
for leaf 2 so that leaf 2 uses the default parking path. The recovery push to
the stale replica is held by freezing (`docker pause`) that replica's
container, so recovery cannot complete until the test resumes it.

## Recipe

`prepare` records the original OSD map flags and the relevant config entries,
then sets `noout`, `norecover`, `nobackfill` and `global/osd_heartbeat_grace=60`.
The grace keeps a frozen replica from being marked down during the hold. All
three OSDs must be up and in. `restore` unsets only the flags it added,
reinstates or removes exactly the recorded config entries, waits for all PGs
`active+clean` and verifies the flags and config equal the recorded originals.
The Go test always calls it, including on failure, after resuming frozen and
starting stopped OSDs. The object names are the fixture's owned OMAP-write
UUID names (one seeded and eight reserved). They are removed after restoration.

For each degraded object the test stops a non-primary acting replica, waits
for it to be down with the PG active, writes the object natively, starts the
replica, and waits until the PG is active and degraded with that replica back
in the acting set (`num_objects_degraded >= 1`, no unfound). It then freezes
the replica.

Leaf 1 (object BLOCK, `osd_backoff_on_degraded=true`):

1. A Go APPEND on the degraded object X is sent on connection `o` to the
   primary. The Go client receives BLOCK, checks its identity and ACKs it
   explicitly with the original epoch.
2. Native `dump_historic_ops` on the primary must contain the Go ACK_BLOCK
   (client, PG, ID, epoch, single-object bounds, `reached_pg`).
3. A STAT of another object Y on the same primary, on the same Go connection,
   must be a known local unsent refusal (`ErrOSDBackoff`). A second
   independently authenticated Go client stats Y and reads X successfully.
4. A native `rados append` on X (debug_objecter 15) must receive the same
   single-object BLOCK. A native `rados stat` of Y completes during the hold.
5. After at least 6 s of BLOCK the replica is resumed. The Go connection must
   receive the matched UNBLOCK (same ID, raw bounds, PG, session). The native
   client must log UNBLOCK, resend and complete.
6. The blocked Go APPEND receives no reply within 3 s. It is canceled and
   must report `OutcomeUnknownError` with its original submission tuple. The
   native snapshot must equal base + native tail, so the dropped Go write did
   not execute. The caller then submits the same APPEND as a new request on
   the same connection. It must execute exactly once (bytes, mtime and user
   version against native). A second ACK of the released BLOCK is refused.

Leaf 2 (in-flight failover, default configuration):

1. A Go APPEND on degraded object X2 is sent to its primary P. Native
   `dump_ops_in_flight` on P must show it with flag point and event
   `waiting for degraded object` for the Go client. No backoff control
   arrives within 2 s.
2. A native `rados append` on X2 must be parked the same way (two ops).
3. P is SIGKILLed (exit 137, not OOM) and the stale replica is resumed.
4. The Go request must report `OutcomeUnknownError` with its original
   transaction, incarnation, epoch and placement. The product does not replay
   it. A later request on the failed connection fails.
5. Native placement must show a replacement primary on a newer epoch. The
   native client must resend from P to the replacement and complete. The
   native snapshot must equal base + native tail.
6. The caller resubmits the same APPEND explicitly to the replacement primary
   with the new epoch on a new connection. It must execute exactly once.
7. P is restarted and the fixture restored. Both objects' native bytes,
   versions and mtimes must be unchanged after recovery, which also shows
   that the killed op was neither resurrected nor replayed.

## Results

Both runs used exported source `6b3d156e9da894a1cbd8122441c4a85527bf0449`
(`git archive`; this document was added afterwards and does not change tested
code). Image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
daemon version `20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964)`, arm64,
pool `ceph-msgr-osd-fixture` (id 2, 8 PGs, size 3, min_size 2, HASHPSPOOL,
legacy CRUSH), profile `replicated-pg-v8`.

| Profile | Result | Test / total time |
| --- | --- | --- |
| Darwin arm64 host, IPv4, CRC, AES/AES, 3 BlueStore | PASS, restore and cleanup passed | 46.49 s / 100 s |
| Linux arm64 container (CGO=0), IPv6, secure, AES256K/AES256K, 3 BlueStore | PASS, restore and cleanup passed | 42.87 s / 93 s |

Observed MOSDBackoff fields (Go decoded; BLOCK and UNBLOCK are identical
except op and Matched):

| Field | CRC/IPv4 | Secure/IPv6 |
| --- | --- | --- |
| PG (pool.seed), shard | 2.5, -1 | 2.7, -1 |
| ID | 1 | 1 |
| BLOCK / UNBLOCK op | 1 / 3 | 1 / 3 |
| BLOCK / UNBLOCK map epoch | 26 / 26 | 26 / 26 |
| begin = end hobject | pool 2, hash 1844240221, ns "", key "", snap head (2^64-2), max false, 156 raw bytes | pool 2, hash 3980574071, same shape |
| source / global ID / generation | osd.1, 4725, 1 | osd.1, 4734, 1 |
| native ACK history | client.4725, 2.5, id 1, e26, op 2, single object, reached_pg | client.4734, 2.7, id 1, e26, same |

The begin/end object is the reserved degraded object itself (`raw_pgid`
`2.6decdf5d` and `2.ed42bd77`). The native Objecter logged the same PG, ID 1,
epoch 26 and single-object bounds for its own BLOCK and UNBLOCK.

Timings (Go wall clock on the test host, native from the oracle container's
clock and Objecter log):

| Measure | CRC/IPv4 | Secure/IPv6 |
| --- | --- | --- |
| Go APPEND send → BLOCK received | 1.9 ms | 0.7 ms |
| BLOCK hold (frozen replica) | 6.00 s | 6.00 s |
| resume → Go UNBLOCK received | 201.9 ms | 104.9 ms |
| native start → native BLOCK (log) | ~18 ms | ~18 ms |
| resume → native append complete | 127.1 ms | 92.2 ms |
| Go resubmission after UNBLOCK, RTT | 2.0 ms | 2.5 ms |
| primary SIGKILL → Go unknown outcome | 303.8 ms | 303.7 ms |
| primary SIGKILL → native append complete | 1291.7 ms | 2102.5 ms |

The resume → UNBLOCK figures include the fixture control round trip that
issues the resume. They are not a daemon-internal latency.

Native and Go behaviour on the same object:

- The native client sent tid 1, received BLOCK, held it, resent tid 1 on
  UNBLOCK and completed. The rados CLI also issues a trailing zero-length
  APPEND as tid 2, so the native user version advanced by two (1 → 3 CRC,
  2 → 4 secure) for one data append. The Go caller's APPEND was dropped by the
  OSD at BLOCK, stayed unanswered, and became an unknown outcome on
  cancellation. Its explicit resubmission executed once (version 3 → 4 CRC,
  4 → 5 secure).
- In leaf 2 the native client sent tid 1 to the killed primary, resent it to
  the same OSD after the connection reset, then to the replacement primary
  (osd.2 on CRC, osd.0 on secure) on epoch 33, and completed. The Go request
  reported `request outcome unknown: EOF` with transaction 4, incarnation 91,
  epoch 31 and the old primary osd.1. Its explicit resubmission (transaction 5,
  incarnation 93, epoch 33) executed once (version 3 → 4 on both profiles).
  Final bytes were base + native tail + Go tail on both objects, unchanged
  after the old primary returned and recovery completed.

## Product findings

- The gate is connection-wide, as `BACKOFF_CONTRACT.md` states. An object
  BLOCK for X makes the same connection refuse unrelated objects (Y STAT
  returned `ErrOSDBackoff`, unsent) until UNBLOCK and ACK. Native Objecter
  holds only ops contained in the blocked range. The requested behaviour
  "other objects proceed" is therefore not met on the blocked connection. It
  is met only by a second Client.
- `Client.OpenOSD` returns the already held connection for the same OSD ID, so
  a second handle to the same OSD shares the gate and transaction counter.
  The first CRC attempt assumed a second handle would be an independent
  session and failed on that assumption. An independent session needs a
  second `Dial`.
- A write that triggered BLOCK is dropped by the OSD and never answered. The
  product reports it only as an unknown outcome when the caller cancels.
  Callers must resubmit after UNBLOCK. This matches the no-replay policy, but
  the BLOCK/UNBLOCK events do not say which pending request was dropped.
- After the primary is killed, a later request on that connection returns a
  bare `EOF` without a submission transaction. It is not wrapped as an unknown
  outcome, which is consistent with it never being sent.

## Not verified

- Object BLOCK under the default configuration (only unfound objects back off
  by default). The unfound path and its release were not exercised.
- UNBLOCK delivered by an interval change. `on_change` calls
  `clear_backoffs()` without sending UNBLOCK (`PrimaryLogPG.cc:13039`); the
  failover leaf deliberately used the non-backoff parking path.
- Late ACK after an object UNBLOCK, range (non-equal) object bounds, more than
  one simultaneous object backoff, split during an object backoff, EC pools,
  `replicated-pg-reply-v8` with three replicas, and remote CI. Only the two
  local runs above were executed.
- Execution of a parked write that survives a non-killed primary. In leaf 2
  the parked op was lost with the killed process; a parked op on a primary
  that keeps running would execute after recovery and was not measured.

## First failures

Earlier attempts on the same fixture, all CRC/IPv4 host:

| Source | Failure | Cause |
| --- | --- | --- |
| `58042e2` | second handle stat refused with `ErrOSDBackoff` | `OpenOSD` returned the shared connection (finding above); test changed to a second Client |
| `9ce1389` | native client log not parsed | Objecter log/stderr not captured; oracle now forces `--log-to-file` and returns bounded log tails |
| `da84c9d` | native version expectation | rados CLI trailing empty APPEND advances the version by two |
| `5b78153` | held-op predicate | the live dump reports `waiting for degraded object` as the flag point, not `delayed`; in this failure path the owned-object cleanup command then timed out after a successful restore (cause not determined, no native trace retained) and the cleanup receipt was lost; the receipt is now written in a deferred block |

Every attempt observed a real single-object BLOCK. Attempts from `9ce1389`
on also confirmed native reception of the Go ACK. All attempts restored flags and configuration. Cleanup passed except in
`5b78153`.

## CI

Two rows select only this test: CRC/AES IPv4 host and secure/AES256K IPv6
container, each `replicated-pg-v8`, three BlueStore replicas.
`tools/test_ci_osd_profiles.py` now counts 54 rows, restricts the existing v8
row checks to one replica, and checks the new rows' selector and profiles.
