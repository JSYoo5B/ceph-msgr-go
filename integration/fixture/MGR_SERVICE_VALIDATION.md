# MGR service registration: selected native observation

Date: 2026-10-21. Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`.
Image: `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Linux/amd64, Go 1.26.9, Docker 29.9.1, Testcontainers-Go v0.44.0.
Each disposable fixture used three MONs, two MGRs and one memstore OSD with the
legacy-v6 profile. This is communication-layer evidence, not an RGW service or
complete daemon implementation.

| Tested commit | Keys / mode / address / runtime | Selected test result |
| --- | --- | --- |
| `dec9084f0a858e51224f16a9178c1218f8dd8e12` | aes client and service keys, CRC, IPv4, host | PASS, 6.81 s |
| `66084e170a7f5805bd9126ac28e9f08f8c92b25a` | aes256k client and service keys, secure, IPv6, container | PASS, 6.61 s |

The only changes between these tested commits remove a duplicate workflow
environment entry introduced by integrating the CI prerequisite and strengthen
its uniqueness assertion. Product code, oracle and integration test are identical.
Subsequent evidence-documentation changes do not constitute a new native run.

## What actually ran

Both runs explicitly enabled `CEPH_MSGR_TEST_MGR_SERVICE=1` and selected
`^TestCephMgrServiceRegistrationIntegration$`. Each recorded a complete top-level
PASS for that test. The `client` secondary package selected no tests; its zero
exit is allowed but contributes no native acceptance evidence. The result gate
reported the expected API test as PASS with no missing or skipped required test.

The Go client received MMgrConfigure (period 1 second, threshold 5). The test
then observed, through independent public librados MON/MGR commands:

1. `service dump` contained the Go daemon's initial metadata and authenticated
   client GID; `service status` contained its initial daemon status. The Go
   ServiceMap watch agreed on the GID and registration metadata.
2. After a queued metadata update and report, status and task status matched
   the report. The ServiceMap retained the initial metadata, consistent with
   pinned `DaemonServer::handle_update`, which changes only daemon-state metadata.
   The test does not observe application of the replacement metadata.
3. After the MMgrClose echo, the native service dump no longer contained the
   daemon. A report on the closed Go handle was refused locally.

All eight owned fixture containers in each run terminated successfully; owned
reapers also exited. No unrelated runner resources were removed.

## Reproduction

From the repository root, on an authorized Linux Docker runner:

```sh
export CEPH_MSGR_TEST_MGR_SERVICE=1 CEPH_MSGR_TEST_OSD=1
export CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 CEPH_MSGR_TEST_OSD_STORE=memstore
export CEPH_MSGR_TEST_OSD_REPLICAS=1 CEPH_MSGR_TEST_MGR_COUNT=2
export CEPH_MSGR_TEST_RUN='^TestCephMgrServiceRegistrationIntegration$'
export CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes
export CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4
export CEPH_MSGR_TEST_RUNTIME=host
sh integration/run.sh
```

For the second profile, change both key types to `aes256k`, mode to `secure`,
IP family to `6` and runtime to `container`. The catalog group is `mgr-service`.
The trusted workflow plumbing was landed separately in PR #28 so this
feature's catalog can be requested. These direct runs do not establish live
PR-comment handling or a full periodic matrix result.

## Failure history and remaining boundaries

The earlier native run of PR #26 at
`79394ebbabe5eb99c78dd10c28e9e4da7f37b902` failed before its first observation:
Go serialized an unset optional predicate as `task_status: null`, rejected by
the Python oracle. The caller now omits unset predicates. A shared JSON fixture
is checked against actual Go serialization and consumed by the Python oracle
tests for open, report and absence. This explains that input failure; the later
passes do not erase it. The separately identified metadata predicate bug was
source-backed and had not been reached by that first run.

Linux product unit/race tests, product vet, fixture unit tests/vet, the shared
Go request test and 515 Python tool tests passed. Actionlint 1.7.7 first caught
the integration-time duplicate environment entry and passed after its removal.
These checks are separate from the two native runs.

Fresh native encoder vectors, independently observed applied metadata updates,
service-ticket renewal, real MGR failover, unacknowledged-close grace expiry,
and comparison against a native registration with identical fields remain
unverified. Keep #6 open; this checkpoint covers selected open/report/close
observations only.
