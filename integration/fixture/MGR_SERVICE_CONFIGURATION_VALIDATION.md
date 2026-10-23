# MGR service configuration: native CLIENT broadcast boundary

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`.
Image: `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Source boundary and the first failed expectation

The initial implementation at
`cc3204f92ce1e6b8e347b1cbfeff4d5240fce175` expected a native runtime period
change from 1 to 2 seconds to push a second MMgrConfigure to the existing Go
service session. Its first CRC/AES/IPv4/host execution failed that expectation:
the native control read back period 2, while `MgrService.Configure` remained
1 second with threshold 5. The receive wait expired after ten seconds.
This is a failed acceptance expectation, not an established Go receive defect.

Pinned `src/mgr/DaemonServer.cc` explains the boundary independently of a later
successful test. `handle_open` first sends the initial MMgrConfigure, but lines
635-642 add a connection to `daemon_connections` only when its peer type is
not CLIENT **and** its service name is empty. `handle_conf_change` at lines
3702-3723 broadcasts changed periods/thresholds only to that set. An opted-in
CLIENT service registration therefore receives the initial values and is
excluded from runtime broadcasts. No daemon-role impersonation or new product
feature bit is added to manufacture a reachable CLIENT path.

The corrected checkpoint retains the existing session and independently checks
that new, explicit registrations receive the current values. Synthetic repeated
pushes verify Go's receive/timer behavior separately; they do not establish that
this native CLIENT broadcast is reachable.

## Reproducible selected native checkpoint

`TestCephMgrServiceRegistrationIntegration/configuration_changes` is a required
child of the existing exclusive parent. It runs before that parent's normal
acknowledged close and native removal, using its already-visible service and
caller status/task status. It adds no scenario, workflow event or default CI
integration workload. `/ci run group:mgr-service` continues to select the whole
parent and its existing comparison/recovery/close/unavailable children.

The fixture freezes the owned active MGR name/GID and reads its three selected
options through its native admin socket. It retains only effective values and
runtime `override` presence/value from `config diff` plus independent `config
get` reads. The complete native diff, including unrelated private configuration,
is never published. Every snapshot is bracketed by unchanged native MgrMap
identity/epoch reads.

The closed, once-only action sequence is:

1. Disable `mgr_stats_period_autotune` temporarily on the owned active daemon.
2. Apply a 1-second period, then 2 seconds, then threshold 6 without changing
   the period, then return to 1 second with threshold 6.
3. Require the exact native effective runtime override after every change.
   Existing Go `Configure` remains the initial 1 second/threshold 5. Across
   each phase, retain three strictly progressing native `last_beacon` samples;
   caller status/task status and original service GID/start epoch must persist.
4. For each changed phase, explicitly open a distinct probe registration. Its
   initial Configure must be 2 seconds/5, 2 seconds/6 or 1 second/6 respectively.
   Native ServiceMap observation must match its caller GID/start epoch and
   metadata/status; acknowledged close and native removal are required.
5. Restore threshold, period and autotune in reverse order, preserving an
   originally absent runtime override with `config unset`. An override that
   existed is restored with its original value. Independently read back both
   effective values and presence; changing the same daemon name's new GID is
   refused rather than treated as ownership of a replacement.

Native `last_beacon` values are sampled observations. The receipt does not
infer wire-transmission counts, exact beacon scheduling intervals or absence
of every possible message. The original Go service is never implicitly
re-registered. Three distinct probe registrations are deliberate caller actions,
not replay of an uncertain registration.

Configuration mutations are journaled before submission and never retried on
an uncertain result. Socket commands have a 15-second bound; each requested
phase has a 45-second control bound and normal cleanup has a separate 60-second
context. Fixture teardown also attempts unperformed restoration. Each restoration
command is submitted at most once, even if acknowledgement/readback is missing.
Only this disposable fixture's selected daemon options are changed; the host
and persistent MON configuration database are untouched.

`mgr-service-config-native.json` retains the selected option baseline/current
state, ordered mutation/restoration outcomes and restoration confirmation.
`mgr-service-configuration.json` retains manager/client identity, registration
start epoch, native/configured values, probe removals and twelve progressing
beacon samples. Both are on the diagnostic allowlist. The parent retains its
normal independent acknowledged close/removal and selftest-module restoration.

## Synthetic and concurrency coverage

`TestMgrServiceReconfigurationControlsBeaconAndKeepsSession` sends period
1 -> 0 -> 2, a threshold-only change and rapid 0 -> 3 -> 0 -> 1 pushes over the
same connection that received MMgrOpen. It checks the latest Configure snapshot,
automatic-report suppression/resumption, one open/dial, acknowledged close and
no reports after shutdown. Concurrent Configure readers exercise the shared
receive/timer state under the race detector. One already-admitted report is
allowed during transition; the zero-period silence assertion begins only after
a 1.1-second settling interval and lasts another 1.2 seconds.

`TestMgrServiceReconfigurationCannotEnableDisabledBeacons` observes all pushed
configurations with `DisableBeacon`, requires no automatic report across a
1.2-second final one-second period, and still sends an explicit report. The
synthetic peer shares Go framing and is separate from fresh native codec vectors
and daemon interoperability. Fixture regressions check bounded/type-checked
configuration projection, removal of absent overrides, preservation of an
existing override, continuation after an uncertain restoration and no retry.

Native source authority also includes `src/common/ceph_context.cc` (`config
set`, `config unset`, `config get`, `config diff`), `src/common/config.cc`
(`CONF_OVERRIDE`, `rm_val`, `diff`) and `src/common/options/mgr.yaml.in`
(period/threshold/autotune types and bounds), all at the pinned reference.
These are LGPL-2.1 source references; no native implementation is copied into Go.

## Results

The implementation under native validation is
`98b0b27b305d45cdaba8193b9b66e83057958b1f`.
Both complete parents and every required child passed on the corrected source:

| Keys / mode / address / runtime | Complete runner / parent / configuration child |
| --- | --- |
| AES client/service, CRC, IPv4, host | PASS 341.438 s / PASS 286.59 s / PASS 38.73 s |
| aes256k client/service, secure, IPv6, container | PASS 347.627 s / PASS 296.36 s / PASS 41.64 s |

The [retained selected receipts](MGR_SERVICE_CONFIGURATION_LINUX_20261023.json)
bind actual image IDs/profiles and exact source, including the original failed
expectation. CRC kept manager a/GID 14525 and caller GID 4665; secure kept
manager a/GID 4659 and caller GID 4680. Both original services kept start epoch
2. Both sets of new probes had start epochs 4, 6 and 8, received 2/5, 2/6 and
1/6 respectively, and were acknowledged closed and absent natively. Each
original registration kept 1/5 across all twelve strictly progressing native
beacon samples with caller status/task status intact.

Both baselines were period 1, autotune true and threshold 5, with all three
runtime overrides absent. Both final native snapshots matched those effective
values and absence exactly. All five mutation actions and three reverse-order
unset actions were recorded. The parent then passed normal close/removal,
native field comparison, renewal/active-MGR change, both unconfirmed-close
faults and initially-unavailable setup. Both parent metadata receipts also
confirmed selftest-module restoration. Secondary `client` selections contained
no matching tests and contribute no native acceptance evidence.

For the first failed CRC execution, the runner took 343.561 s; the parent
failed after 280.05 s and configuration child after 25.51 s. Its other required
children passed and its changed options were restored. Each of the three runs
logged termination of eight owned fixture containers. Independent inspection
subsequently confirmed those exact eight containers, its reaper and its owned
network absent. Reapers still present in immediate post-run inventory were
checked again later; an early inventory was not treated as completed cleanup.
No unrelated resources were removed or host configuration changed.

Linux/amd64, Go 1.26.9, Docker 29.9.1, Testcontainers-Go v0.44.0 and
ceph-testcontainers-go v0.3.0 were used. Separate Linux checks passed product
unit/vet, MGR service unit/race tests with `-count=3`, integration vet, fixture
unit/vet, 635 Python tests and the existing 76-scenario catalog. The corrected
native consumer also passed its follow-up compilation/static checks. Later
receipt/contract documentation changes are not another native execution.
Basic CI is a separate result and does not start Ceph.

These direct runs do not claim comment-triggered execution or every native
mode/topology/platform. Live repeated MMgrConfigure delivery to CLIENT service
sessions remains outside the pinned native broadcast path; synthetic receive
coverage cannot turn it into a daemon interoperability result. Broader fixture
coverage remains under issue #14.
