# Go and native librados service field comparison

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`.
Image: `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Native tested source: `b1323d481f4a8a769259e531f898cbeffb2ed9a4`.
Later documentation-only changes do not represent another native execution.

The existing opt-in `^TestCephMgrServiceRegistrationIntegration$` selector now
requires `native_registration_comparison` before the existing recovery and
close-fault children. The disposable fixture has three MONs, two MGRs, one
MemStore OSD, legacy-v6 placement, ordinary 12-second tickets and native
60-second service beacon grace. Basic CI remains cluster-free; the existing
`/ci run group:mgr-service` selection includes this requirement.

## Independent comparison

Go and native librados register the same service under distinct daemon names,
with identical caller metadata (including an intentional non-ASCII value) and
three exact status maps: initial registration, replacement removing a previous
key, and empty replacement. Independent native MON `service dump` and MGR
`service status` queries must observe each phase. Each registration must keep
its authenticated GID and start epoch throughout the status transitions;
the two clients must have distinct nonzero GIDs and valid map/start epochs.

For Go, `OpenMgrService` supplies the initial status; explicit `Report` calls
replace and clear it. A separate nil report must preserve the replacement.
Automatic Go beacons are disabled in this child. The nil-report observation
requires a strictly newer native `last_beacon` from the same MGR, so a stale
sample taken before receipt cannot establish preservation. No local clock is
used to authorize that observation.

For native librados, the closed `servicecompare` oracle creates a separate
registration handle, calls `rados_service_register` once, and calls
`rados_service_update_status` once for each status phase. The original native
handle independently observes it and remains alive after registration shutdown.
The C API supplies the first status through an update after registration;
the comparison establishes observed field equality, not identical open/report
message sequences or wire bytes.

`RadosClient::service_daemon_register` also collects native system metadata.
Only the explicitly supplied comparison keys are projected into the public
receipt; native system/host metadata is neither copied into Go nor retained
there. Go's caller metadata is checked exactly. This does not claim equality
of the complete native and Go metadata maps.

Both registrations must disappear from native ServiceMap observation for three
consecutive seconds. Go Close requires its normal echo. Native
`rados_shutdown` is a void C API: return alone does not prove an acknowledged
close, so independent removal is required afterward. It is not a captured
native close-echo claim.

## Source trace and limits

The development-only ctypes calls follow the pinned public API declarations in
`src/include/rados/librados.h`, the C dictionary decoder in
`src/librados/librados_c.cc`, and `RadosClient::service_daemon_register` /
`service_daemon_update_status` in `src/librados/RadosClient.cc`.
`src/mgr/MgrClient.cc` sends the resulting registration, dirty status reports
and shutdown close. `src/mgr/DaemonServer.cc` applies reports and formats the
independently queried status/beacon state. No native code is incorporated into
the Go product, and product dependencies remain CGO-free.

The public C service API has no metadata-update or task-status setter here.
Applied Go metadata is covered separately by the
[daemon-state checkpoint](MGR_SERVICE_METADATA_VALIDATION.md); task status has
independent native observation and fresh native codec vectors in earlier
records. This field comparison does not extend those scopes or introduce
native client policy into the product.

## Failure boundaries and cleanup

`servicewatch` preserves its existing subset matching by default. The new
`exact_status` option requires a real status entry equal to the entire expected
map, including an empty map. A missing entry, retained old key or stale beacon
cannot pass. Optional beacon inputs are bounded and compared only within the
same native MGR's timestamp representation.

Native C dictionaries reject empty keys and embedded NULs before connecting;
maps, strings, identities and phase counts are bounded. This restriction is
specific to the C oracle representation; it does not narrow Go wire strings.
Only the exclusive MGR service fixture admits `servicecompare`.

Native status observations wait at most 20 seconds per phase, removal at most
90 seconds with a three-second stability requirement, and the supervisor bounds
the complete native comparison command to 240 seconds. The parent retains its
eight-minute context. Every acquired native registration handle is shut down
once, including when an API call, observation or identity check fails. Mutating
API calls are not retried by the oracle; this is not a count of native wire
transmissions or server executions. Failure still tears down the owned fixture.

`mgr-service-comparison.json` retains only caller-field projections, GIDs,
map/start epochs, exact status phases, nil preservation, sampled stable removal
and the field-equality result. It omits endpoints, system inventory, credentials
and error text; the fixture diagnostic allowlist includes the receipt.
`present_after_shutdown` and `removal_seconds` apply only to the native section;
the shared Go section's default false/zero values are not measurements.

## First native failure and corrected acceptance boundary

The first CRC/AES/IPv4/host execution tested
`734570e46936231854e1d63f67e8658bde8b4f7f`. The parent failed at 52.13 seconds;
the comparison child failed at 43.51 seconds. The retained Go receipt confirms
all three exact status phases, nil preservation and 3.006 seconds of stable
removal. The native oracle reached its post-shutdown removal check but raised
`native registration remains after shutdown` after its initial 30-second watch.
Its supervisor recorded exit 1, complete output, no timeout and no truncation.
The native comparison result was not published because that exception aborted
the handler. Reaching the final watch is control-flow evidence, not a retained
native result for the earlier phases.

Subsequent recovery, close and metadata-module restoration requests were refused
by the stopped provider before execution. They are cascading failures, not
independent protocol failures. The metadata receipt reports restoration false;
the diagnostic report is incomplete. All eight owned fixture containers were
nevertheless logged terminated, and the client/reaper exited. Fixture destruction
is separate from successful module restoration or service removal.

The initial test imposed prompt removal on the void native shutdown API.
Pinned `MgrClient::shutdown` waits only the configured unregister interval
(`mgr_client_service_daemon_unregister_timeout`, default one second); it does
not return a removal acknowledgement. Pinned `DaemonServer::ms_handle_reset`
does not unregister CLIENT services, and native beacon grace is 60 seconds.
The corrected test records native presence immediately after shutdown return
and allows a bounded 90-second read-only watch through that grace. It does not
retry registration, status updates or shutdown. Missing/failed inventory still
fails, and final stable removal remains mandatory.

The first execution's logs do not establish why prompt native removal was not
observed. No particular lost-close, reconnect or native bug is claimed. Later
passing observations below do not retroactively explain that event or turn the
first run into a pass.

## Results

The corrected source above passed both complete parents and all required
comparison/recovery/close children:

| Keys / mode / address / runtime | Parent / comparison | Native presence after shutdown / removal wait |
| --- | --- | --- |
| AES client/service, CRC, IPv4, host | PASS 240.97 s / PASS 75.71 s | true / 64.160 s |
| aes256k client/service, secure, IPv6, container | PASS 238.51 s / PASS 75.41 s | true / 64.154 s |

Both caller-field equality and nil preservation were true. CRC Go/native GIDs
were 4677/14639 with start epochs 5/7; secure GIDs were 4656/4746 with start
epochs 5/7. Each registration preserved its own identity throughout the phases.
Both native removal observations ended with about 3.006 seconds of consecutive
absence; the Go removal observations also required three seconds.

Existing recovery children passed (84.03 s CRC, 80.97 s secure), as did both
close-fault children (receive 6.61/7.61 s and discarded send 65.81/65.81 s).
Both metadata receipts confirmed module restoration. The secondary `client`
package selection contained no matching tests and adds no native evidence.
Both fixture runners exited successfully, each logged termination of its eight
owned fixture containers, and the client containers/reapers exited. The final
Docker inventory after the sequential runs was empty. No unrelated resources
were removed. Linux/amd64, Go 1.26.9, Docker 29.9.1, Testcontainers-Go v0.44.0
and ceph-testcontainers-go v0.3.0 were used.

Separate Linux checks passed all 560 Python tool tests, the 65-entry scenario
catalog, shared request serialization, integration vet, fixture unit/vet and
the CGO-free product dependency policy. New oracle regressions distinguish
exact empty status from an omitted check, require a new beacon, reject invalid
C dictionaries, preserve one-shot mutation/handle ownership and refuse success
without independently observed removal. Synthetic checks and direct native
results remain separate from final-head Basic CI.

Earlier records retain their original source identities and verification
boundaries. Initial registration without an active MGR, additional profiles and
other failure configurations remain unverified in #6. A selected-profile field
comparison is not a complete RADOS client or release-wide compatibility claim.
