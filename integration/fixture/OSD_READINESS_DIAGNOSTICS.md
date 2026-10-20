# OSD fixture recovery readiness

Context: issue #3 and the merge hold on PR #16. The original failed runs
remain evidence of incomplete restoration, failed owned-pool cleanup and
cascading controller failures. Their later MGR clean observations do not
establish that the controller observed clean state before its deadline.

## Recovery boundary

Pinned Tentacle 20.2.4 source
[`PeeringState.cc:6283-6323`](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PeeringState.cc#L6283)
sets `PG_STATE_RECOVERY_TOOFULL` after a denied recovery reservation and
schedules `DoRecovery` after `osd_recovery_retry_interval`. The pinned
[`osd.yaml.in`](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/options/osd.yaml.in#L86)
default is 30 seconds. Clearing OSD fullness is distinct from that scheduled
recovery attempt and its subsequent MON/MGR status propagation. A 30-second
observation window cannot cover a full retry interval plus those steps.

A bounded Linux amd64 diagnostic experiment used three BlueStore replicas,
secure Messenger, aes256k client/service keys, IPv6, container runtime and
legacy-v6, with the pinned image:
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
It read an effective retry interval of 30 seconds from every OSD. At +0.517 s,
OSDMap epoch 39 already had the original 0.85/0.90/0.95 ratios and all three
OSDs up/in without fullness flags, while one PG still reported
`active+recovery_toofull+degraded`. The first clean observation was +28.217 s
at the same OSDMap epoch. The standalone test passed in 64.87 s and its
containers were removed. This corroborates the source-backed retry boundary;
it does not trace each scheduler event or substitute for the original full
sequence or final-SHA CI. Later changes also add post-recovery data receipts
and deadline regressions, which were not part of that first experiment.

## Current bounded controls

- `verify-osd-ready clean` retains its 30-second budget. Mutation fixture
  restoration explicitly selects `recovery`, with a 90-second state budget
  covering three default retry intervals. Each status invocation has a
  separate 10-second bound. Recovery first reads each OSD's effective retry
  interval with the same per-command bound; it does not change that setting.
- `recoverMutationFixture` allows 180 seconds for daemon restart, those
  observations, recovery and native placement. Object restoration retains
  its independent 15-second bound. The full-failover test retains its
  480-second operation context and 300-second failure cleanup context.
- The predicate remains manager available, every owned OSD up/in, at least
  one PG and every PG exactly `active+clean`. Permanent unclean state fails;
  canceled or expired invocations cannot establish readiness from late output.
- `osd-readiness.jsonl` retains monotonic per-poll offsets, status summaries,
  separately observed OSDMap epochs, restored ratios and per-OSD up/in/flags
  for recovery waits. Records contain at most 256 observations (with an
  explicit omission count), and the append-only file is bounded to 8 MiB.
  It is collected on success as well as failure.
- Full-failover acceptance independently rechecks the forced object's bytes
  and version, the held object's absence and restored ratios after recovery.
  `osd-full-failover-results.json` records these observations; native
  owned-pool removal is asserted and logged separately from container teardown.
- Requests published after an existing provider/oracle failure are identified
  as unexecuted cascading failures. The originating error remains intact.

## Verification

`go -C integration/fixture test -run 'Test(OSDClean|WaitOSDClean)' ./...`
exercises delayed clean recovery, permanently unclean state, status invocation
failures, pre-cancellation, late clean output and error-cause preservation.
`internal/testcluster` tests cover originating and cascading worker errors.
These are synthetic regressions, separate from native interoperability.

Native validation must retain the unfiltered legacy-v6 row, including its
original preceding tests. Repeat on secure/aes256k container IPv4/IPv6 and
compare CRC/aes host IPv4. A standalone pass is insufficient. Final-SHA CI,
restored native state, object postconditions, owned-pool cleanup and fixture
teardown remain independently required; a retry alone does not explain any
original failure. Product routing, retry and mutation semantics are unchanged.
