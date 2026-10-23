# Successful native evidence bundle, version 1

This is a bounded retention checkpoint for issue #1. It reuses the existing
trusted scenario controller and required-test gate. It does not add another
planner, authorization path, scheduling queue or fixture capability.

## Admitted pilot

Only the two explicit `mds-cephfs-capsend` rows, selecting
`^TestCephMDSCephFSCapSendRecoveryIntegration$`, request a successful bundle.
Their existing CRC/AES IPv4 host BlueStore and secure/aes256k IPv6 container
MemStore profiles remain unchanged. Other successful native scenarios still
retain their ordinary result/test artifacts without this pilot bundle.

The collector is copied from the calling workflow's trusted checkout before
checking out tested code. It uses only Python's standard library. The job
checks that the checkout equals the captured tested SHA before executing it.
No status/comment write permission is added to the test job.

The fixture's metadata directory is **input only**. The collector reads exactly:

- `fixture-manifest.json`;
- `mds-cephfs-data-results.json`;
- `mds-cephfs-data-cleanup.json`;
- `mds-fresh-cleanup.json`;
- the trusted gate's separately supplied `native-tests.json`.

Other input metadata is ignored rather than copied. The input contains
endpoints, configuration and larger protocol objects; it is never the artifact
upload directory. Authentication material, keyrings, Ceph configuration, full
fixture IPC, process output, native log lines and failure diagnostics are
excluded from the successful bundle.

## Closed output inventory

`tools/native_evidence.py collect` projects reviewed fields into five files:

| File | Retained boundary |
| --- | --- |
| `manifest.json` | Version, execution context, configured/observed distinction and payload size/hash inventory |
| `tests.json` | Exactly the required API parent PASS, expected selector, no missing/failed/skipped result |
| `cap-send.json` | Client GID/nonce, old/new MDS GIDs, inode/flush TIDs, send generation/admission classes, correlated ACK identities/order, native apply branch classifications, equal before/after object-request counts and selected native stat fields |
| `cleanup.json` | Reviewed DATA and filesystem cleanup booleans: namespace/layout/config/blocklist and client/native identity restoration, filesystem/pool removal |
| `runtime.json` | Inspected Docker image ID/architecture, native daemon version, provider version and separately labeled fixture-reported profile |

No arbitrary string from the native result is retained. Branch/send labels
are closed enums, identities are bounded integers, and hashes/versions/profile
values have closed validators. Raw log lines and full native stat/capability
objects remain omitted. The native test asserts its larger protocol contract;
this bundle preserves only the listed reviewed projection, not every assertion's
raw input or an independently reproduced oracle.

Cleanup booleans describe the owned native state before fixture termination.
The successful fixture process also requires its orderly container/network
teardown. The v1 bundle is not a separate Docker inventory or post-termination
inspection receipt.

## Manifest and independent expected context

The manifest's `schema_version` is integer **1** and its `kind` is
`native-success`. Every field is required and unknown output fields are refused.

| Field | Meaning |
| --- | --- |
| `execution_source` | `github-actions` or `linux-runner`; a local numeric run ID is not an Actions run ID |
| `tested_sha` | Exact source selected for native test execution |
| `controller_sha` | Exact trusted collector/controller source; a local collector source remains distinct from a default-branch Actions controller |
| `run_id`, `attempt` | Positive bounded integers; different attempts retain distinct identities |
| `scenario`, `selector` | One admitted scenario ID and its exact required parent selector |
| `profile` | Normalized requested matrix parameters, including explicit defaults; requested settings are not observations that every protocol path ran |
| `configured_image` | The admitted digest-pinned registry reference requested for the fixture |
| `observed` | Same validated structure as `runtime.json`; the Docker image ID is an inspected image/config identity, not relabeled as the configured registry manifest digest |
| `files` | Exactly four unique payload names with byte size and lowercase SHA-256 |
| `total_bytes` | Sum of the four payload sizes; the total limit also includes `manifest.json` |

The observed daemon version must name Ceph Tentacle 20.2.4 at
`7f793731f1b39eb4f465e960113d2363c311b964`. The configured image is
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`;
provider version is v0.3.0. A new image/provider/profile is an explicit contract
change, not an automatic fallback.

`verify` requires an expected run request independently of the manifest.
The request has `execution_source`, `tested_sha`, `controller_sha`, `run_id`,
`attempt`, and the selected `matrix` (including its `scenario`). It must match
all manifest context fields. Supplying the manifest's own identity back as
its expected context establishes only self-consistency, not attribution.

Collection refuses an existing output directory instead of overwriting a prior
attempt. Actions artifact names include run ID, attempt, job index and scenario;
no overwrite option is used. This is bounded attempt isolation, not a central
deduplication service or unlimited historical storage.

## Bounds and refusal behavior

Each source document is limited to 16 MiB. Gate/output documents and each
exported file are limited to 256 KiB, and the complete bundle is limited to
1 MiB including its manifest. Duplicate JSON keys, invalid JSON, nonregular
files, symlinks, missing/oversized inputs, unexpected output files, duplicate
inventory entries, bad hashes, mismatched context/profile/runtime facts and
incomplete cleanup are refused. Privacy validation also rejects unreviewed
path/address/remote-access strings; the closed projection is the primary
protection against arbitrary private strings.

All outputs are validated before the successful upload step. The dedicated
artifact contains only `success-evidence/`, uses compression level 9 and expires
after **seven days**. Existing failed-test diagnostics retain their separate
14-day policy. No entire IPC/metadata directory is added to either upload.

A native test can pass while required evidence is incomplete. `result.json`
records `test_result` and `evidence_result` independently; the collection status
retains a fixed public failure code. Collection or artifact-upload failure makes
the pilot's final evidence gate fail and prevents overall native acceptance,
while preserving the actual test PASS. Missing/invalid required-test evidence
is `unknown`, never an invented PASS. Canceled or skipped native jobs do not
become accepted evidence. No automatic retry replaces the first attempt.

## Local interface and verification limits

With a separately prepared run request and retained private metadata:

```sh
python3 tools/native_evidence.py collect \
  --request .artifacts/cap-send-request.json \
  --receipts .artifacts/metadata --tests .artifacts/native-tests.json \
  --output .artifacts/cap-send-v1 --status .artifacts/evidence-status.json
python3 tools/native_evidence.py verify \
  --request .artifacts/cap-send-request.json --bundle .artifacts/cap-send-v1
```

Without `--request`, the trusted workflow supplies `EXECUTION_SOURCE`,
`TESTED_SHA`, `CONTROLLER_SHA`, `RUN_ID`, `RUN_ATTEMPT` and `MATRIX_JSON`.
The verifier consumes data and never executes a receipt, native command or
embedded path. Hashes provide file integrity, not a signature or proof that
an otherwise fabricated record came from native execution.

Daemon-free rejection tests validate the contract; a native run and hosted
artifact upload are separate evidence. Before this workflow change reaches
the default branch, PR-comment runs still use the previous trusted workflow
and do not run this new collector. An older successful request must not be
reported as a hosted successful-bundle upload.

Soak-specific immutable observations and their failure/cleanup acceptance
remain in issue #14. They may consume the common manifest/verification
contract through a separately reviewed schema extension; v1 does not accept
soak files or broaden its admitted selectors.

The [pilot validation record](NATIVE_EVIDENCE_VALIDATION.md) separates its actual
Linux native run, retained projection and pre-merge hosted-workflow boundary.
