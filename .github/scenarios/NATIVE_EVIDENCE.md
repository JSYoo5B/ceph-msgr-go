# Successful native evidence bundle, version 1

This is a bounded retention checkpoint for issue #1. It reuses the existing
trusted scenario controller and required-test gate. It does not add another
planner, authorization path, scheduling queue or fixture capability.

## Families

A family is the set of rows that request a successful bundle and the closed
projection it retains. Each family is one module,
`tools/native_evidence_<name>.py`, beside the collector. `native_evidence.py`
imports every module whose file name matches that rule and keeps only the
mechanics the families share: the run context, input reading, bounds, privacy
checks, the observed runtime file and the manifest. A family module declares:

| Name | Declares |
| --- | --- |
| `KIND` | The manifest `kind` of its bundles |
| `FILES` | Its closed payload file set, always with `tests.json` and `runtime.json` |
| `PROFILES` | The normalized matrix profiles it admits |
| `CAPABILITIES` | The fixture capability value its rows may carry in the matrix |
| `claims(matrix)` | Whether a job's matrix requests this family's bundle |
| `scenario_id`, `selector` | The admitted scenario ID and manifest selector of a profile |
| `runtime_facts`, `gate`, `project`, `validate` | Its fixture-manifest checks, required-test gate, projection of the input metadata, and re-validation of a downloaded bundle |

The collector refuses every family when one declaration is incomplete or
imported from another directory, when two families share a `KIND` or admit
one profile, or when an admitted profile's matrix is claimed by any family
but its own. A matrix that two families claim is refused
(`ambiguous-evidence-family`). A request that no family claims, or whose
matrix cannot be read, has its test result classified with the cap-send gate
and is then refused; that fallback is fixed and a new family does not change
it.

The workflow does not list families. After copying the collector and every
`native_evidence_*.py` module from the calling workflow's trusted checkout,
and before checking out tested code, the job runs
`native_evidence.py family`, which prints the claimed family's name (or
nothing) as the step output `family`. The collection, completeness and result
steps run for a job whose output is not empty. The tested checkout supplies
data only; it never supplies a family module.

To add a family, add its module and its daemon-free tests
(`tools/test_native_<name>_evidence.py`). The module docstring describes what
the bundle retains and omits; a new family does not edit this file, the
collector or the workflow. The family sections below
describe the families declared before family modules existed, because other
documents link to them.

The collector uses only Python's standard library. The job checks that the
checkout equals the captured tested SHA before executing it. No
status/comment write permission is added to the test job.

The fixture's metadata directory is **input only**. A family reads only the
files it names, together with the trusted gate's separately supplied
`native-tests.json`. Other input metadata is ignored rather than copied. The
input contains endpoints, configuration and larger protocol objects; it is
never the artifact upload directory. Authentication material, keyrings, Ceph
configuration, full fixture IPC, process output, native log lines and failure
diagnostics are excluded from the successful bundle.

## Common files

Every bundle has `manifest.json`, the family's `tests.json` and
`runtime.json`, and the family's own payload files:

| File | Retained boundary |
| --- | --- |
| `manifest.json` | Version, family `kind`, execution context, configured/observed distinction and payload size/hash inventory |
| `tests.json` | The family's required API parent PASS results and selector, with no missing, failed or skipped result |
| `runtime.json` | Inspected Docker image ID/architecture, native daemon version, provider version and separately labeled fixture-reported profile |

No arbitrary string from the native result is retained. Labels are closed
enums, identities are bounded integers, and hashes/versions/profile values
have closed validators. Raw log lines and full native objects remain omitted.
The native test asserts its larger protocol contract; a bundle preserves only
its family's reviewed projection, not every assertion's raw input or an
independently reproduced oracle.

## Manifest and independent expected context

The manifest's `schema_version` is integer **1** and its `kind` is the
family's `KIND`. Every field is required and unknown output fields are refused.

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
| `files` | Exactly the family's payload names, each once, with byte size and lowercase SHA-256 |
| `total_bytes` | Sum of the payload sizes; the total limit also includes `manifest.json` |

The observed daemon version must name Ceph Tentacle 20.2.4 at
`7f793731f1b39eb4f465e960113d2363c311b964`. The configured image is
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`;
`collect` requires provider version v0.5.0, the release that
`integration/fixture/go.mod` pins. `verify` also admits v0.3.0 and v0.4.0, the
releases named by bundles retained before the fixture moved to v0.4.0 and to
v0.5.0, such as
[cap-send-native-v1](testdata/cap-send-native-v1/manifest.json); a bundle's
recorded release is never rewritten. A new image/provider/profile is an
explicit contract change, not an automatic fallback.

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
the family's final evidence gate fail and prevents overall native acceptance,
while preserving the actual test PASS. Missing/invalid required-test evidence
is `unknown`, never an invented PASS. Canceled or skipped native jobs do not
become accepted evidence. No automatic retry replaces the first attempt.

## Cap-send recovery

Declared in `tools/native_evidence_capsend.py`, with `kind`
**`native-success`**. It admits the two explicit `mds-cephfs-capsend` rows,
selecting `^TestCephMDSCephFSCapSendRecoveryIntegration$` on their CRC/AES
IPv4 host BlueStore and secure/aes256k IPv6 container MemStore profiles. The
collector reads exactly `fixture-manifest.json`,
`mds-cephfs-data-results.json`, `mds-cephfs-data-cleanup.json`,
`mds-fresh-cleanup.json` and the gate's `native-tests.json`. Beside the common
files it retains:

| File | Retained boundary |
| --- | --- |
| `cap-send.json` | Client GID/nonce, old/new MDS GIDs, inode/flush TIDs, send generation/admission classes, correlated ACK identities/order, native apply branch classifications, equal before/after object-request counts and selected native stat fields |
| `cleanup.json` | Reviewed DATA and filesystem cleanup booleans: namespace/layout/config/blocklist and client/native identity restoration, filesystem/pool removal |

Branch/send labels are closed enums. Full native stat and capability objects
remain omitted. Cleanup booleans describe the owned native state before
fixture termination. The successful fixture process also requires its orderly
container/network teardown. The v1 bundle is not a separate Docker inventory
or post-termination inspection receipt.

The [cap-send validation record](NATIVE_EVIDENCE_VALIDATION.md) separates its
actual Linux native run, retained projection and pre-merge hosted-workflow
boundary.

## Four-minute OSD soak family

Declared in `tools/native_evidence_soak.py`, with `kind`
**`native-soak-success`**. It admits exactly one
profile: `osd.aes256k.ip4.osd-store-bluestore.osd-replicas-3.osd-soak-4m` (aes256k
client key and service cipher, secure mode, IPv4, container runtime, three
BlueStore replicas, `osd_soak=4m`, the pinned image and provider above). The
manifest `selector` is the fixture's own selection
`^TestCephOSDSoak(Failover)?Integration$`, since the scenario row carries no
`test_run`. Any other row with an `osd_soak` value is still claimed by this family and is refused by the collector as an unsupported profile, so it
fails as `incomplete` rather than skipping retention.

**Expected inventory comes from outside the receipts.** The collector fixes
the workload pair (`steady`, `failover`), the 240 s duration, the tested
revision and the image. The broad test gate lists no expected tests for this
row, so the collector additionally requires its `native-tests.json` to show
exactly `api/TestCephOSDSoakIntegration` and
`api/TestCephOSDSoakFailoverIntegration` as PASS with no other result, problem
or skip. A receipt's own `expected_tests` is checked against this fixed pair,
never used to define it.

**Trusted validator.** `native-scenarios.yml` copies `osd_soak_receipts.py`
from the calling workflow's trusted checkout next to the collector, before the
tested checkout. The family module imports it from its own directory and
refuses a module found anywhere else. The tested checkout's copy (which the
fixture runs after termination for the job log) is not used for retained
evidence. The validator checks selected labels, worker identity and object
binding, native bytes/xattrs/version, native mtime (unsigned 32-bit seconds,
nanoseconds below 1e9), fault intervals, owned-object cleanup and fixture
termination.

**Retention decision: fixed projection, not raw receipts.** Soak receipts may be
512 KiB each (the fixture's diagnostic-copy bound) and carry base64 expected
and native bytes, so eight maximum-sized worker receipts alone exceed the 1 MiB
bundle limit. Raising the budget would only move the ceiling. The bundle
therefore retains a closed projection plus hashes, with the common 256 KiB per
file and 1 MiB total limits:

| File | Retained boundary |
| --- | --- |
| `manifest.json` | The common manifest with the soak `kind` |
| `tests.json` | The two required API parent PASS results and both selectors |
| `soak.json` | Tested revision, configured duration, observation count (8); per workload setup time, owned-object count, actual workload/test intervals, closed counter set, four worker observations (version, native mtime seconds/nanos, byte lengths, SHA-256 of the data and of the xattr value, observation time) and for failover each fault phase (cycle, phase, time since workload start, duration, victim OSD index) and the final clean-PG verdict |
| `cleanup.json` | Per workload the native removal verdict, duration and the five object result codes (success or native absence) in ownership order, plus fixture termination success and duration |
| `receipts.json` | Fixture manifest hash and, for each of the 22 raw files (two workloads, termination, fault receipts and the fixture manifest at one fault cycle), exact byte size and SHA-256 |
| `runtime.json` | The common runtime file |

**What the projection cannot do.** The raw expected/native bytes, xattr values,
object names, setup names, error strings and unresolved-outcome ledgers are
omitted. A reader can check the retained hashes against a raw receipt if it is
obtained elsewhere, but cannot recompute the native comparison, the
xattr equality or the byte-level match from the bundle. Retained times and
identities are the validator's accepted values, not independently re-measured.
The 22-entry inventory is for a single fault cycle; each additional cycle adds
three receipts, with at most 64 cycles. Cleanup flags describe the owned native
objects and the fixture's orderly termination, not an independent Docker or
network inventory.

**Verdicts.** A native PASS with failed collection or upload remains
`test_result=passed`, `evidence_result=incomplete`; the final step fails the job
and the status file carries a fixed public code (for example
`soak-receipts-invalid`) without echoing receipt contents. Retention is
seven days, in the common `native-success-<run>-<attempt>-<job>-<scenario>`
artifact name. Failed or canceled runs keep only their
existing diagnostic behaviour, and cancellation evidence remains unverified.

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

Daemon-free rejection tests (`tools/test_native_*evidence*.py`) validate the
contract, and `tools/test_native_evidence_families.py` checks the family
declarations and the workflow's family decision for every planned job; a
native run and hosted artifact upload are separate evidence. Before this workflow change reaches
the default branch, PR-comment runs still use the previous trusted workflow
and do not run this new collector. An older successful request must not be
reported as a hosted successful-bundle upload.

## Shared-FLOCK recovery

Declared in `tools/native_evidence_flock.py`. The two
`mds-cephfs-sharedflock-recovery` rows use the metadata fixture and
require `TestCephMDSCephFSSharedFlockRecoveryIntegration` to pass. Their
manifest kind is `native-shared-flock-success`. The common manifest, runtime
and tests files identify the trusted controller, tested source, scenario,
run and attempt. The metadata and fresh-filesystem cleanup flags are in
`cleanup.json`.

In `flock.json`, `operations` retains the two original shared acquires and
two explicit unlocks, in submission order, with their owner, transaction ID
and signed first reply result. Collection checks each settled call against
the corresponding accepted SETFILELOCK in the independent request ledger.
`accepted_tids` preserves the ledger order for download verification.
The first two post-restart native probes establish the baseline with both
locks held. Each subsequent exclusive probe carries its unlock transaction
ID and native request ID, errno, acquired flag and unlocked flag. After the
first unlock the remaining owner blocks the probe with EAGAIN. After the
second, the probe acquires and releases the lock.

Use the independent verification procedure above with the actual selected
shared-FLOCK row. Download the successful bundle from a run using the updated
trusted controller and compare it with expected context from that run. The
full request ledger, native addresses, paths, error text and reconnect blob
stay outside the bundle. The same input, file and total size bounds apply.

## Interrupted attempts

The trusted `native_terminal.py` recorder runs in an `always()` step. It
re-evaluates the original required-test snapshot against the available log,
including a log whose test command ended before writing `native-tests.json`.
The resulting `scenario-result/tests.json` preserves the observed verdicts,
missing requirements and gate failures. `result.json` records the tested and
controller SHAs, scenario set, run ID and attempt, job/step outcomes and these
independent observations.

- `test_evidence` is `missing` when the snapshot or log is unavailable,
  `partial` when required results are missing, and `completed` when every
  required parent has a terminal result. A completed FAIL or SKIP still fails
  the gate. Broad selectors retain the existing at-least-one-PASS contract.
- `test_result=passed` requires both the reconstructed gate and a successful
  test step. A cancelled step can retain a completed parent PASS while its
  attempt remains unaccepted. Missing evidence cannot produce acceptance.
- `evidence_collection` and `evidence_upload` retain their separate step
  outcomes. An admitted bundle requires both to succeed. `native_cleanup`
  is verified only when collection validated the family's reviewed cleanup
  fields. Other cases remain unverified, including an interrupted job whose
  containers were subsequently removed.
- `result_artifact_upload=unobserved` describes the recorder's knowledge.
  Its result precedes its own upload. Check the attempt's upload step and
  artifact listing to establish publication. Missing files fail the upload.
  The recorder exits unsuccessfully for an unaccepted attempt.

Scenario and diagnostic artifact names contain run ID and attempt, as successful
bundles already do. A later attempt creates a distinct artifact. Local recording
refuses an existing result directory. Retention still expires old artifacts,
so this provides bounded preservation within the configured retention period.

`always()` cannot guarantee publication when GitHub terminates a runner or
cancels before the trusted tools are copied. If execution stops before recording
or upload, the attempt-specific Actions job/step state and any already uploaded
artifacts remain the available evidence. An absent result artifact does not
establish whether the test ran, finished, or cleaned up. Runner disposal does
not independently verify native cleanup flags. Never fill the missing attempt
from a later attempt's artifacts or infer PASS from a later request status.

The bounded Linux interruption record is in
[terminal evidence validation](NATIVE_TERMINAL_VALIDATION.md). Hosted cancellation
and API-visible attempt preservation require a separate live Actions exercise.
