# v0.2.0 fixture migration validation

Run window: 2026-10-14–15 (Asia/Seoul). Consumer base revision:
`7aa7ce2ae03ec73ceb85fa1862d3d915180fee0c`, with the migration changes in the
working tree. This is local validation, not a claim about a remote CI commit.

Provider: published `github.com/JSYoo5B/ceph-testcontainers-go v0.2.0`, release
commit `853805a2a6e24314042d80d95d451c085c857251`. The nested module uses the
published checksum; there is no workspace or `replace` override. The product
module still uses only the Go standard library.

Native baseline: Tentacle 20.2.4, Linux arm64, Docker Desktop 29.8.2;
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

| Profile | Result | Local evidence |
| --- | --- | --- |
| Default secure/aes256k/IPv4, both ordinary suites | Public API: 40 passed, 9 dedicated-profile skips; internal client: 14 passed, 5 skips | `/tmp/ceph-v020-full2.log` |
| Focused map wait, membership, raw MON OSDMaps, Tell catalogs | 5 public tests passed | `/tmp/ceph-v020-focused.log` |
| Mapped IPv6/secure/aes256k | Native metadata plus Go commands, two renewals, learned-MON recovery, lazy MGR takeover and Close passed | `/tmp/ceph-v020-mapped.log` |
| IPv6/secure/aes256k | Numeric and DNS seeds, generic peer addresses, snapshot renewal and concurrent MON/MGR commands passed | `/tmp/ceph-v020-ipv6.log` |
| CRC host/aes256k | Generic hostname peer and concurrent MON/MGR command transport passed | `/tmp/ceph-v020-crc-host.log` |
| Initially zero MGRs | Delayed creation of both managers and availability passed | `/tmp/ceph-v020-delayed.log` |
| IPv6 host idle/120s tickets | Effective native timing and application-idle MON/MGR sessions passed | `/tmp/ceph-v020-idle.log` |
| MON CRC versus secure client, IPv6/aes256k | Explicit mode rejection passed | `/tmp/ceph-v020-mode-mon.log` |
| MGR CRC versus secure client, IPv4/aes | Rejection, public temporary secure policy and both MGR restarts passed | `/tmp/ceph-v020-mode-mgr.log` |
| 1.5s MON ticket/12s service ticket | Four Go renewal cycles before expiry passed | `/tmp/ceph-v020-short.log` |
| IPv6 service-key epoch/120s tickets | Native key-set disposal, epoch observation and both recovery cycles passed | `/tmp/ceph-v020-epoch.log` |
| Expired/discarded ticket proof | Full MON stop, expiry, b/c/a restart and both authentication tests passed | `/tmp/ceph-v020-expired.log` |
| OSD secure/aes256k/container | All six existing MON/OSD map, stat/read, renewal and unsupported-feature tests passed | `/tmp/ceph-v020-osd3.log` |
| OSD CRC/aes256k/host relay | The same six tests passed | `/tmp/ceph-v020-osd-crc.log` |
| AES client/AES256K service, secure | Concurrent native MON/MGR transport and ticket cipher checks passed | `/tmp/ceph-v020-mixed-aes.log` |
| AES256K client/AES service, secure | The same checks passed | `/tmp/ceph-v020-mixed-service.log` |
| Tentacle 20.2.3/AES, secure | Native MON/MGR transport and concurrent commands passed | `/tmp/ceph-v020-tentacle203.log` |
| Darwin host race/aes256k/secure | Concurrent MON/MGR transport, lost mutation reply and Close during MGR handshake passed under race instrumentation | `/tmp/ceph-v020-host-race.log` |

This does not certify every CI matrix combination, Linux amd64, Windows
runtime, or an hour-long workload. The existing CI matrix still exercises its
AES/mixed cipher, patch release, address/security and race combinations using
the new runner. Future mutation/MDS/EC/RBD/CephFS/daemon-to-daemon coverage in
SPEC.md remains a separate requirement.

## Preserved failures and upstream questions

The first full consumer run timed out in the native MonMap oracle after
adding a deliberately unreachable fourth MON. The oracle worker stopped,
and subsequent receipt-dependent tests failed. Independent Go transport
continued to work. The later focused and complete runs passed after an
explicit attached-client MON-config refresh and stronger diagnostics. This
is not proof of a root-cause fix; the original failure is retained in
`/tmp/ceph-v020-full.log` and was reported as a usage/diagnostic question in
[upstream #65](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/65).
The test now removes the spare member once during cleanup only if normal
removal has not already been submitted; uncertain mutations are not replayed.

The first valid public OSD topology started and its native byte/stat2 oracle
passed, but every Go test was correctly rejected at MON SESSION_IDENT with
required feature mask `0x0000000040000000`. Native OSDMap diagnostics showed
`.mgr` still had HASHPSPOOL despite the data pool's `NoHashPSPool` setting.
Evidence is retained in `/tmp/ceph-v020-osd2.log`. The original adapter cleared
that flag once on the known internal `.mgr` pool through the public `Ceph`
command. Its native before/after receipt showed `.mgr` flags 1 at epoch 12 and
flags 0 at epoch 13. Those are historical post-bootstrap workaround results.

The current OSD recipe instead writes
`[global] osd_pool_default_flag_hashpspool = false` into `bootstrap.conf` and
passes it through `WithConfigFile` before `ceph.Run`, following the public
recipe in [upstream #68](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/68).
The explicit data pool still uses `CreatePool(NoHashPSPool: true)`. Preparation
does not change `.mgr` flags afterward. It waits within the startup/parent
context for the data pool and, for two-manager profiles, the background-created
`.mgr` pool, using only bounded read-only native OSDMap observations. Receipts
require a positive epoch, the fixture's exact FSID, unique actual pool IDs/names
and present numeric flags; malformed observations fail rather than becoming
readiness polling. Any pool's
numeric HASHPSPOOL bit or flag name fails preparation immediately. The current
`pool-policy.json` records the bootstrap recipe and the verified actual policy;
it does not fabricate a before/after mutation. Existing non-OSD/idle/ticket
settings and product advertised feature masks are unchanged.

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

The first merged migration commit, `bb90440020579b5ebf83c610ff41d41490a59478`,
failed [CI run 38589085119](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38589085119).
Fourteen jobs failed; unit, fixture-tool and Windows real-Ceph jobs passed.
The original failure logs and artifacts were inspected before changing code.

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

The correction commit `a25426ba653564226f02db1d3728ef0b218ff612` subsequently
passed all 36 jobs in [CI run 38592987116](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38592987116),
including the previously failed Linux container controls, MGR takeover,
expired-ticket recovery, both legacy-cipher OSD profiles, race sessions and
Windows native Ceph execution. This confirms the correction on Linux CI; the
first failed run remains distinct evidence and was not rerun to erase it.
