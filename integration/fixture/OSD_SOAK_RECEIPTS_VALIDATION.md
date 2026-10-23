# Immutable receipts for the bounded OSD soak pair

Issue #14 follow-up: preserve final native observations before the ordinary
OMAP observer file is overwritten by cleanup. Product behavior, request
identity, explicit unknown-outcome resolution, the existing four-minute
workloads and the scenario catalog are unchanged. The test-only consumer
continues to use caller-supplied placement and the existing OSD API.

Reference: Ceph Tentacle v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
fixture provider `ceph-testcontainers-go` v0.3.0. Native comparison reuses the
existing pinned librados C API oracle; no Ceph source or new protocol codec
is copied. Native tools remain development-only dependencies.

## Workload receipt boundary

`integration/osd_soak_receipt_test.go` writes each stage with `O_EXCL` and
never truncates or replaces it. The collector admits only closed
`osd-soak-v1-*` names, regular files, at most 256 receipts and at most 512 KiB
per receipt. Repeated diagnostic copying is idempotent only for identical
bytes; a different receipt at the same destination is refused. Credentials,
configuration and the whole fixture directory are not selected.

For each of `steady` (`TestCephOSDSoakIntegration`) and `failover`
(`TestCephOSDSoakFailoverIntegration`):

- `started.json` records configured duration, the controller's source SHA
  assertion and the fixture manifest hash; `setup.json` records setup time.
- Four distinct `worker-0.json` through `worker-3.json` records retain the
  expected synthetic bytes/xattr, native bytes/xattrs/version/mtime/code,
  lengths and SHA-256 digests, native source/version and independent-C-API
  attribution, the comparison result and native observation duration.
  Each is written immediately after that worker's native comparison and
  before assertions or cleanup can replace the reusable oracle output.
- `workload.json` records configured duration, actual monotonic interval,
  total test time, status, counters, first observed error and still-unresolved
  original submissions. The interval begins with the existing workload
  deadline and ends when the last worker exits, including in-flight drain
  and client shutdown. It excludes setup, final observation, object cleanup
  and fixture termination. A worker's unknown submission is removed from
  the unresolved ledger only by a successful reply with its original
  GlobalID/incarnation/TID; a different client's reply cannot resolve it.
- `cleanup.json` records the independent native removal verdict and object
  codes, separately timed. The workload verdict runs after this cleanup,
  so matched bytes do not imply successful cleanup.

All filenames have the `osd-soak-v1-<test>-` prefix. The failover test also
retains a distinct `fault-NNN-kill.json`, `-restart.json` and `-clean.json`
after each completed phase, including victim identity, order and elapsed
time, followed by `recovery-clean.json`. Existing faults still run one OSD
at a time. If a test fails, already-written observations remain unchanged;
missing stages and the original test failure remain visible. There is no
new mutation retry or uncertain-outcome policy.

After owned fixture termination, `osd-soak-v1-fixture-termination.json`
records suite exit, termination verdict/time and configured duration/source
and image, alongside the observed image ID/architecture and manifest hash.
Git checkouts require the source assertion to equal their actual HEAD.
Archive-based runs must supply `CEPH_MSGR_TEST_SOURCE_SHA`; that value is
explicitly a controller assertion, not a native observation of Go source.

`tools/osd_soak_receipts.py verify` requires externally expected revision,
image and duration. It independently checks all eight worker identities,
full expected/native bytes and xattrs, lengths/digests, versions, both
workload durations/counters, unresolved outcomes, every fault/recovery
stage, owned-object cleanup and fixture termination. It emits receipt
hashes only after all checks pass. Missing, duplicate, truncated, wrong
revision/image, shortened or incompletely cleaned results cannot pass.

These are workload-specific receipts. Issue #1 owns the common successful
artifact collection/manifest contract; its schema-v1 cap-send projection
does not yet consume this raw soak schema. This change does not duplicate
that bundle or claim successful raw artifact publication on GitHub.

## Reproduction and verification

Run the unchanged closed pair sequentially on one fresh fixture:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes256k CEPH_MSGR_TEST_SERVICE_CIPHER=aes256k \
CEPH_MSGR_TEST_CONNECTION_MODE=secure CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=container CEPH_MSGR_TEST_OSD=1 \
CEPH_MSGR_TEST_OSD_STORE=bluestore CEPH_MSGR_TEST_OSD_REPLICAS=3 \
CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 CEPH_MSGR_TEST_OSD_SOAK=4m \
CEPH_MSGR_TEST_RUN='^TestCephOSDSoak(Failover)?Integration$' \
sh integration/run.sh
```

The selected recipe retains three four-GiB BlueStore devices, the existing
30-minute suite and 45-minute job budgets, and recovery/cleanup time in
addition to both four-minute workloads. No duration is increased.

This document defines the implemented receipt contract and required native
acceptance. The associated pull request records the tested source SHA,
actual execution, receipt hashes and exact-head CI; the historical soak
runs do not prove this checkpoint. The first Linux static check found that
an empty new source env key altered the strict non-soak environment
contract; source propagation was restricted to soak tests, preserving other
profiles. That static failure is separate from native acceptance.

This checkpoint does not establish longer soaks, continuous capacity or
resource monitoring, ticket renewal, network attribution, all native paths
or final issue #14 completion. Earlier capacity failures and corrected
duration-specific results remain in [OSD_SOAK_VALIDATION.md](OSD_SOAK_VALIDATION.md).
