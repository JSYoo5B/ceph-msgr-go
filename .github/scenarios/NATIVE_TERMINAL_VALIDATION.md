# Interrupted native evidence validation

## Bounded Linux interruption

On 2026-11-18 (Asia/Seoul), an owned Linux amd64 Docker 29.9.1 fixture ran the
catalog scenario
`mds-cephfs-capsend.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfscapsendrecovery`.
The native test and fixture sources were from `28c4d84`, with the terminal
recording changes subsequently committed as `3345ca7`. The image was
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
This exercised one IPv4 CRC/AES host BlueStore profile.

The controller first listed the exact required parent test, built the fixture
binary outside the checkout, and started its normal `run -project` entry point.
It sent SIGTERM to that owned process immediately after the log reported
`=== RUN   TestCephMDSCephFSCapSendRecoveryIntegration`. Startup was bounded to
420 seconds and termination to another 120 seconds. The fixture exited with
code 1 after 127.531 seconds overall. The child test was killed before a parent
verdict. This is the injected interruption's observed failure.

The data cleanup record retained an open client session and reported
`namespace_checked=false`, `namespace_restored=false`,
`layout_restored=false` and `completed=false`. Fresh-filesystem cleanup
reported filesystem, pools and identity removed while retaining
`completed=false` because the DATA checks had failed. Fixture termination
removed the daemon containers. The transient remaining reaper had disappeared
when inspected separately after the process exit. These observations preserve
the failed cleanup verification alongside infrastructure disposal.

## Hosted boundary

A live GitHub cancellation has not been performed for this change. The
implementation role's permissions leave Actions cancellation to the review
role. Hosted dispatch, cancellation timing, upload execution and retention
remain unverified. PR-comment native runs use the default branch's trusted
workflow until this change is merged.

For the hosted exercise, retain the request's tested/controller SHA, exact
scenario, run ID and attempt before cancellation. Cancel only that owned run.
Read its attempt-specific job and step conclusions, available test/result
artifacts, successful-bundle collection/upload outcomes and cleanup evidence.
Record unavailable evidence explicitly. When a later attempt is authorized,
keep the first attempt's outcome and verify that the two artifact identities
remain distinct. The controller status context must also include the attempt
to prevent a later attempt from replacing the first terminal status.

The local receipt uses `execution_source=linux-runner`, run ID `2026111802`
and attempt `1`, native source
`28c4d84f5d17ea481577d96a6484df94f9ef18ec` and recorder source
`3345ca7dd6e6753ef19c987ae58439d6c367cea0`. The reconstructed gate reports
partial evidence with no required parent PASS, `accepted=false`,
`evidence_result=incomplete` and `native_cleanup=unverified`.
Its result SHA-256 is
`5e4517ecb1cd6553e6192a03b3df71a9bbeb9a1a268e450d7e9d628923ed044e`.
An independent Docker inspection found zero owned containers and the recorded
network absent. It did not change the native cleanup verdict.

The first tooling check failed because the transfer omitted Git metadata and
the merge-proof regression suite requires a tracked-file inventory. This was
a validation harness error. The corrected Linux checkout initializes its own
Git inventory from the source revision's tracked paths.

## Later local attempt

A fresh owned fixture ran the same row as attempt `2` of local run
`2026111802`. It exited 0 after 285.755 seconds. The required parent PASS,
result gate and reviewed successful-bundle collection all passed. Its terminal
record retained `test_result=passed`, `test_evidence=completed`,
`evidence_collection=success` and `native_cleanup=verified`.

No artifact service was used for either local attempt. Accordingly the second
record retained `evidence_upload=unobserved`, `evidence_result=incomplete`
and `accepted=false`. The upload-dependent workflow acceptance rule was
exercised without inventing an upload success. The first result's bytes and
SHA-256 remained unchanged after the second record was written.
