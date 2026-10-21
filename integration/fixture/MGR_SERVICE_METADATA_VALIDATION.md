# Native MGR service metadata application

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`.
Image: `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Native tested source: `e8c18f7c9f9913f6abacf12184943fd5b8d0c46e`.
Later documentation-only changes do not represent another native execution.

The existing opt-in `^TestCephMgrServiceRegistrationIntegration$` selector
now requires native observation of applied metadata before its existing
renewal/failover and unconfirmed-close children. Each disposable fixture has
three MONs, two MGRs, one MemStore OSD, legacy-v6 placement, ordinary 12-second
tickets and native 60-second service beacon grace. This does not add a scenario
or change the cluster-free default PR checks.

## Independent state and source trace

`MMgrUpdate` changes the MGR's daemon-state index, not the MON's persisted
ServiceMap. A native `service dump` or `service status` cannot prove metadata
application. The closed `servicemetadata` oracle therefore reads
`MgrModule.get_metadata(service, daemon)` inside the native MGR through its
upstream `selftest` module. The pinned source trace is:

- `src/mgr/DaemonServer.cc`, `handle_open` and `handle_update`: update the
  service daemon's entry. An empty update map is ignored.
- `src/mgr/DaemonState.h`, `DaemonStateIndex::update_metadata`: replace metadata
  and rebuild its index, removing keys omitted by the new map.
- `src/mgr/ActivePyModules.cc`, `get_metadata_python`: read the native
  `daemon_state` entry, adding a separate `hostname` projection.
- `src/pybind/mgr/mgr_module.py`, `get_metadata`: expose that native state to
  an in-process module. This method is not exposed by the `cli_api` reflector.
- `src/pybind/mgr/selftest/module.py`: initialize the interpreter with `mgr`
  bound to the module and expose `mgr self-test eval`.

No Ceph source is copied into the oracle. Ceph/native tools remain development
dependencies of the disposable fixture, separate from the native Go product.

## Required observations

The native oracle must see exact caller metadata, excluding only the separate
native `hostname` projection:

1. Initial open: role, daemon and a registration-only key.
2. First nonempty replacement: role/daemon plus a zone field, with the
   registration-only key absent. A subset match is insufficient.
3. After a locally refused empty update: the previous metadata is unchanged.
   The Go encoder already rejects empty updates; no server clearing is claimed.
4. Second nonempty replacement: only the new role remains, with daemon and
   zone keys removed.

The existing native MON dump must still contain the initial metadata and the
authenticated Go GID; native MGR status and MON task status must match the
explicit report. Acknowledged close must remove the ServiceMap entry. These
checks keep daemon-state application separate from registration visibility,
local queue admission and the report's optional-map semantics.

## Module ownership and bounds

Only the exclusive MGR service fixture accepts this new control mode. Before
opening the Go client, the native oracle inventories enabled modules and enables
`selftest` once if absent. It polls only the read-only Python-version command
until ready. No force-enable, background selftest workload, generic source
input, arbitrary native command or product module policy is added.

The eval source is a fixed JSON-printing `get_metadata` expression; service and
daemon names must match the closed ASCII name grammar. Expected maps and wait
times are bounded. Observations poll reads only, and exact replacement requires
old keys to disappear. A denied command, missing daemon or stale map cannot
count as a match. JSON/projection errors fail rather than silently passing.

Cleanup disables the module only if this test enabled it, submits that mutation
once, and verifies its removal from the native inventory. Pre-existing module
state is preserved. Cleanup uses its own 60-second context even if the test
context was canceled. Unknown module mutations are never replayed; fixture
teardown still owns the complete disposable cluster on failure.

Preparation waits at most 45 seconds, individual metadata observations at most
30 seconds, under the existing 80-second fixture command supervisor. The parent
has an eight-minute context. `mgr-service-metadata.json` retains four sanitized
snapshots and module ownership/restoration, omitting native hostname and all
error text. The fixture diagnostic allowlist includes this receipt.

## Results and limits

Both profiles passed on their first native execution of the source above:

| Keys / mode / address / runtime | Complete parent result | Metadata / module restoration |
| --- | --- | --- |
| AES client/service, CRC, IPv4, host | PASS 155.36 s | Four exact snapshots / true |
| aes256k client/service, secure, IPv6, container | PASS 159.99 s | Four exact snapshots / true |

Both recovery children passed (75.22 s CRC, 78.57 s secure); both close-fault
children also passed (receive 6.61/6.70 s, discarded send 63.91/66.11 s). The
secondary `client` package had no matching tests and adds no native evidence.
Both fixture runners exited successfully. The owned fixture/client containers
and reapers exited; post-run Docker inventories were empty on both runners.
No unrelated resources were removed. Linux/amd64, Go 1.26.9, Docker 29.9.1,
Testcontainers-Go v0.44.0 and ceph-testcontainers-go v0.3.0 were used.

Separate Linux checks passed all 553 Python tool tests, the 65-entry scenario
catalog, shared request serialization, integration vet, fixture unit/vet and
the CGO-free product dependency policy. Oracle regressions cover stale/missing
state, exact key removal, empty versus missing metadata, name/source injection
refusal, one-shot module mutations, readiness polling, pre-existing module
ownership and verified restoration. These are synthetic checks, distinct from
the native profiles above.

The existing `/ci run group:mgr-service` selection includes this requirement;
the runs recorded here are direct native executions, not comment-triggered CI.
Older lifecycle, codec, recovery and close records retain their original scopes.
Same-field native librados service-registration comparison, additional profiles
and other failure configurations remain open in #6. No automatic registration,
metadata-update replay or higher-level daemon policy is introduced.
