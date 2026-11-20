# v0.2.0 fixture migration validation

Run window: 2026-10-14–15 (Asia/Seoul). Consumer base revision:
`source-1`, with the migration changes in the
working tree. This is local validation, not a claim about a remote CI commit.

Provider: published `github.com/JSYoo5B/ceph-testcontainers-go v0.2.0`, release
commit `853805a2a6e24314042d80d95d451c085c857251`. The nested module uses the
published checksum; there is no workspace or `replace` override. The product
module still uses only the Go standard library.

Native baseline: Tentacle 20.2.4, Linux arm64, Docker Desktop;
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Host tests use Darwin arm64 and the owned bridge relay. Three MONs and zero/two
MGRs run in provider-owned daemon containers. The optional OSD is MemStore,
with the legacy straw CRUSH map, one replica/PG and no HASHPSPOOL/upmap.

## Checks

- `CGO_ENABLED=0 go test ./...` and `CGO_ENABLED=0 go vet ./...`: passed.
- Nested fixture `go test ./...`, `go vet ./...` and `go mod verify`: passed.
- Whole product-module `go test -race ./...`: passed (development CGO=1
  instrumentation only).
- Python verification tools: all 144 tests passed.
- Windows amd64 public/internal test binaries: CGO=0 cross-compilation passed.
  Windows execution and WSL lifecycle were not exercised on this Mac.

## Native executions

The ordinary selector and all dedicated-profile selectors are retained. A
skip in an ordinary fixture is not counted as a dedicated-profile pass.
Containers, networks and private key files were cleaned up after each run.

| Profile | Result |
| --- | --- |
| Default secure/aes256k/IPv4, both ordinary suites | Public API: 40 passed, 9 dedicated-profile skips; internal client: 14 passed, 5 skips |
| Focused map wait, membership, raw MON OSDMaps, Tell catalogs | 5 public tests passed |
| Mapped IPv6/secure/aes256k | Native metadata plus Go commands, two renewals, learned-MON recovery, lazy MGR takeover and Close passed |
| IPv6/secure/aes256k | Numeric and DNS seeds, generic peer addresses, snapshot renewal and concurrent MON/MGR commands passed |
| CRC host/aes256k | Generic hostname peer and concurrent MON/MGR command transport passed |
| Initially zero MGRs | Delayed creation of both managers and availability passed |
| IPv6 host idle/120s tickets | Effective native timing and application-idle MON/MGR sessions passed |
| MON CRC versus secure client, IPv6/aes256k | Explicit mode rejection passed |
| MGR CRC versus secure client, IPv4/aes | Rejection, public temporary secure policy and both MGR restarts passed |
| 1.5s MON ticket/12s service ticket | Four Go renewal cycles before expiry passed |
| IPv6 service-key epoch/120s tickets | Native key-set disposal, epoch observation and both recovery cycles passed |
| Expired/discarded ticket proof | Full MON stop, expiry, b/c/a restart and both authentication tests passed |
| OSD secure/aes256k/container | All six existing MON/OSD map, stat/read, renewal and unsupported-feature tests passed |
| OSD CRC/aes256k/host relay | The same six tests passed |
| AES client/AES256K service, secure | Concurrent native MON/MGR transport and ticket cipher checks passed |
| AES256K client/AES service, secure | The same checks passed |
| Tentacle 20.2.3/AES, secure | Native MON/MGR transport and concurrent commands passed |
| Darwin host race/aes256k/secure | Concurrent MON/MGR transport, lost mutation reply and Close during MGR handshake passed under race instrumentation |

This does not certify every CI matrix combination, Linux amd64, Windows
runtime, or an hour-long workload. The existing CI matrix still exercises its
AES/mixed cipher, patch release, address/security and race combinations using
the new runner. Future mutation/MDS/EC/RBD/CephFS/daemon-to-daemon coverage in
SPEC.md remains a separate requirement.

## Preserved failures and upstream questions

Bootstrap generation, required pool names, all-pool flag rejection and polling
cancellation have focused pure-Go/race tests. A fresh secure/AES256K IPv4 host
MemStore run passed five selected native tests with the bootstrap default.
Native OSDMap epoch 12 identified `.mgr` ID 1 and data-pool ID 2 with flags 0;
Go MON admission and explicit OSD/native bytes comparisons passed. All eight
owned containers and the network were independently absent after cleanup.
The earlier table used the historical workaround; the new source snapshot,
two-profile results and remaining authentication-observer limitation are in
[CONSUMER_QUESTIONS_VALIDATION.md](CONSUMER_QUESTIONS_VALIDATION.md).

Private credentials/configuration are excluded from preserved diagnostics.
Provider diagnostics and crash metadata now use the public v0.2.0 collectors;
the old single-container crash archive parser was removed with its tests.
Consumer-specific command/schema interpretation and native oracle receipts
remain test-only.

## First main CI failure and correction

- Linux root container tests published control requests with mode 0600. The
  non-root host controller could not read them. `provider.error` explicitly
  recorded permission denied for `stop-mons`, `restart-mon` and
  `verify-osd-epoch`. This also affects secure-MGR restoration requests.
  Control requests now use 0644 inside the private fixture directory, as do
  non-secret map blobs selected for diagnostic collection. Keys remain private.
- Native MGR takeover can respawn a daemon and change its authenticated GID.
  The host fixture's initial native map had standby `a/14989`; the later native
  map had active `a/15016`. The test incorrectly required the old GID throughout
  takeover. It now checks the promoted daemon name and validates its current
  GID against a fresh native map at the same epoch.
- Waiting tests now report controller/oracle failure files immediately instead
  of reducing the first failure to later context deadline errors.

Correction validation: all root CGO=0 Go tests and vet, the fixture module tests,
and 144 Python tests passed. Focused live Tentacle 20.2.4 secure/aes256k
container tests for MGR standby takeover and public snapshot renewal/MON restart
passed. Cross-UID Linux behavior still requires the new remote CI run; macOS
Docker Desktop does not reproduce Linux bind-mount ownership exactly. No test
was skipped and no timeout increased to hide the original failures.

## 2026-10-16 fixture hardening against recurring CI flakes

The 1.5 s MON ticket row above is superseded. Triage of 45 main runs found the
short-ticket MGR -13 (3 of 43 runs, aes/aes IPv4) to be a native fixture
artifact: native MonClients renew at expiry minus a quarter of the validity and
only on their one-second tick, so under a 1.5 s AUTH TTL with one-second MON
key rotation the MONs refused their AUTH reclaim for good. The MGR's beacon
MonClient then stopped refreshing rotating keys and refused current Go MGR
tickets (`could not find secret_id`). Changes, all fixture/test/CI only:

- Short-ticket AUTH TTL is 6.5 s (still fractional; MGR tickets keep 12 s,
  `mon_tick_interval` stays 1). `TestCephFractionalTicketRenewalIntegration`
  accepts an AUTH validity in (0, 6.5 s] that is not a whole second, requires
  the MGR validity above it, and has a 60 s context. Before dialing, and again
  after an initial MGR failure, it asks the fixture for a native MGR keyring
  gate (`verify-mgr-keyring`, receipt `manager-keyring-gate.json`). An owned
  MGR whose latest process logged a MonClient -13, or whose MgrMap GID an owned
  MON refused to reclaim, is reported as a fixture error.
- The host relay listens on 30400 (below the Linux ephemeral range) instead of
  40000 and the fixture fails fast when `relay.log` reports a bind error.
- The OSD forced crash sends SIGKILL through Docker ContainerKill. Docker stop
  sent SIGTERM first, and `osd_fast_shutdown` could exit 0 before the SIGKILL.
- A control failure that coincides with owned daemon crash metadata is
  reported as a native crash fixture error (`native-crash.json`), naming the
  ceph-mgr null `auth_server` segfault in `ProtocolV2::handle_auth_request`.
  Each native `mgr dump` of the MGR preparation is bounded to 15 s.
- CI pre-pulls `testcontainers/ryuk:0.14.0` with retries and bounds and retries
  the Windows WSL rootfs download and import.

Local evidence (Darwin arm64, Docker Desktop, Tentacle 20.2.4 pinned digest,
commit `e0173c303a25125669069ca98779a6999e31844a` exported with `git archive`):

| Profile | Result |
| --- | --- |
| aes/aes secure IPv4 container, short-tickets=1 (CI row) | 20 of 20 runs passed; all 20 pre-dial gates clean; 80 renewal cycles, AUTH validity 6.5 s (6 capped values between 6.08 s and 6.26 s), MGR validity 12 s |
| aes/aes CRC IPv4 host relay, OSD=1 bluestore x3, selector `^TestCeph(OSDReplicas\|OSDMutationCrash\|OSDXattrCrash)Integration$` | 3 of 3 tests passed in one run; relay.log empty; every forced crash produced exit 137 without OOM |

Not verified locally: the Windows WSL step, the CI reaper pre-pull, a live
native ceph-mgr segfault (the classification is unit-tested against the
recorded crash metadata shape), and the absence of `could not find secret_id`
in MGR logs of passing runs (daemon logs are archived only on failure).
