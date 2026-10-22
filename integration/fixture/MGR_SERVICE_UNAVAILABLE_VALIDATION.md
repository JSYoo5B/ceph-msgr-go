# Initial service registration without an active MGR

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`.
Image: `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Native tested source: `4a1d2e44d81990a6f88302712f1070c3dd109bea`.
Later documentation-only changes do not represent another native execution.

The existing opt-in `^TestCephMgrServiceRegistrationIntegration$` selector now
requires `initially_unavailable` after its existing comparison/recovery/close
children. Each disposable fixture has three MONs, two MGRs, one MemStore OSD,
legacy-v6 placement, ordinary 12-second tickets and native 60-second service
beacon grace. This adds no scenario, product API, feature mask or default CI.

## Controlled absence and independent observation

The test submits MON `mgr set down true` once on the owned fixture. The pinned
`MgrMonitor::prepare_command` sets `MgrMap::FLAG_DOWN` and drops the active
manager; `promote_standby` refuses activation while that flag is set. The native
daemons remain owned and running: this is actual absence of an active MGR in
the authoritative map, not a stopped-daemon or zero-daemon bootstrap test.

The new closed `mgrstate` oracle queries native MON `mgr dump` through librados
and retains only epoch, flags, availability, active name and GID. It contacts no
MGR. Missing fields, failed commands, inconsistent identity or invalid scalar
types fail instead of looking like absence. This independent oracle must see
the down flag, no active GID/name, availability false and a newer epoch.
Fresh Go clients must also receive the corresponding unavailable map.

## Required local and native postconditions

1. A 600-millisecond setup deadline returns a nil handle and
   `context.DeadlineExceeded`, without `ErrMgrServiceOutcomeUnknown`.
2. Explicit setup cancellation returns a nil handle and `context.Canceled`,
   also without an unknown remote outcome.
3. Closing another client during setup returns `ErrClosed` and no handle.
4. A separately started initial registration remains pending while unavailable.
   A test-only context wrapper signals entry into setup without changing the
   cancellation semantics. All clients together must make zero MGR dial calls
   before activation is restored.
5. A single inverse MON mutation clears the down flag. Native observation must
   show a ready active identity and newer map; the pending registration then
   completes. Native ServiceMap metadata/GID must match the Go caller, its
   subscribed active identity must match the native map, and exactly one MGR
   dial must have occurred.
6. Native ServiceMap observations after activation must show every deadline,
   cancellation and closed-client identity absent. The local dial count must
   remain one, independently excluding a late local setup attempt. These are
   sampled map observations and local call counts, not server execution counts.
7. After acknowledged close of the successful pending registration, a new,
   explicit call reuses the deadline-canceled identity. Native observation must
   see that registration; acknowledged close must remove it. Exactly two MGR
   dial calls are allowed across the two deliberate successful registrations.

The canceled registrations are never resubmitted automatically. The explicit
reuse is a new caller action after a known pre-send failure, separate from
replaying an uncertain mutation. Both successful identities must finally be
absent from native ServiceMap observation.

## Ownership, bounds and source trace

The MGR-down flag belongs only to this disposable fixture. On a test failure,
cleanup can submit its inverse once using a fresh MON-only client and a separate
60-second context, then independently observe readiness. Before normal restore
is submitted, the fallback is disarmed so an uncertain restoration is never
replayed. No daemon restart, host configuration change or native command retry
is introduced. Other fixture state and the parent metadata module retain their
existing teardown ownership.

Every entered setup must return within a separate five-second local completion
bound after cancellation. Successful service cleanup uses ten-second contexts.
The existing parent has an eight-minute context and the MON-only oracle retains
the 80-second fixture command supervisor. Native absence checks for canceled
identities wait at most ten seconds and require one second of stable absence.

`mgr-service-unavailable.json` retains only map identities/flags, known-error
classification booleans, local dial counts, caller GID, native manager GID,
explicit reuse, removal and restoration. It excludes endpoints, raw error text,
credentials and system inventory. The diagnostic allowlist includes the receipt.

Source authority: `src/mon/MonCommands.h` (`mgr set down` arguments),
`src/mon/MgrMonitor.cc` (`prepare_command`, `promote_standby`) and
`src/mon/MgrMap.h` (`FLAG_DOWN`, native dump). Product setup remains
`cephmsgr/mgr_service.go`: reserve identity, wait for availability/current auth,
dial once, and only then queue MMgrOpen. No native source is copied into Go.

## Results

Both profiles passed on their first native execution of the source above:

| Keys / mode / address / runtime | Complete parent / unavailable child | Native epochs before / down / restored |
| --- | --- | --- |
| AES client/service, CRC, IPv4, host | PASS 249.06 s / PASS 11.78 s | 12 / 13 / 16 |
| aes256k client/service, secure, IPv6, container | PASS 251.04 s / PASS 11.65 s | 12 / 13 / 16 |

Both down maps had flags 1, availability false, empty active name and GID 0.
Both restored maps had flags 0 and availability true. CRC restored mgr.b GID
4929 and registered caller GID 4938; secure restored mgr.a GID 14704 and caller
GID 4944. Native maps and subscribed Go identities matched. All known-failure,
canceled-absence, explicit-reuse, removal and restoration booleans were true.
The local dial counters were 0 before restoration and 2 after the two explicit
successful registrations on both profiles.

The existing comparison, recovery and both close-fault children also passed in
both complete parents. The secondary `client` package selection had no matching
tests and adds no native evidence. Both fixture runners exited successfully;
each logged termination of eight owned fixture containers, client containers
and owned reapers exited, and both final Docker inventories were empty. Both
parent metadata receipts confirmed selftest-module restoration. No unrelated
resources were removed. Linux/amd64, Go 1.26.9, Docker 29.9.1, Testcontainers-Go
v0.44.0 and ceph-testcontainers-go v0.3.0 were used.

Separate Linux checks passed all 563 Python tools tests, the 65-entry scenario
catalog, MGR service unit tests and `-race -count=3`, shared oracle request
serialization, integration/product vet, fixture unit/vet and CGO-free product
dependency policy. The new synthetic regression requires the identity slot and
setup gate to be reserved before interruption, then proves their release, zero
MGR connections/opens, and explicit reuse after restoring the synthetic map.
The native oracle regressions reject missing/invalid identity and command
refusal. These are separate evidence from actual daemon observations.

The direct native executions here do not represent a comment-triggered CI run.
`/ci run group:mgr-service` selects the existing parent and all required children.
Older lifecycle, codec, recovery, close, metadata and field-comparison records
retain their original scopes. Zero-daemon cold bootstrap, other topologies,
additional platforms/key/mode combinations and every possible failure schedule
are outside this selected-profile receipt.
