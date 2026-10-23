# Successful-native evidence pilot validation

This record validates the bounded cap-send bundle in
[NATIVE_EVIDENCE.md](NATIVE_EVIDENCE.md). It does not establish a hosted
successful-evidence upload before the trusted workflow reaches main.

## Native execution and first collection

Tested and collector source: `b537120efe464d9f21f76fe3fd9f5152cd91e4fc`.
Date: 2026-10-23. Execution source: **linux-runner**, local run ID
**2026102301**, attempt **1**. This local ID is not a GitHub Actions run.

The uniquely owned source export was checked against all 1,825 tracked-file
hashes before execution. No remote deletion or host configuration change was
needed. Linux/amd64, Go 1.26.9, Docker 29.9.1, provider v0.3.0:

| Input | Executed value |
| --- | --- |
| Scenario | `mds-cephfs-capsend.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfscapsendrecovery` |
| Selector | `^TestCephMDSCephFSCapSendRecoveryIntegration$` |
| Connection | CRC, AES client key and AES service cipher, IPv4, host runtime |
| Storage | One BlueStore OSD, pool size/min-size one, legacy-v6 |
| Ceph | Tentacle v20.2.4, `7f793731f1b39eb4f465e960113d2363c311b964` |
| Configured image | `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9` |

The fixture process exited zero after **254.25 seconds**, including setup,
all native leaves and teardown. The required API parent actually passed in
**144.61 seconds**, and all **12 children** passed. The result gate observed
exactly its expected parent with no problems. The collector then produced a
complete five-file bundle, and its independent-context verifier passed.

After fixture teardown, individual Docker inspections confirmed all **nine
owned containers** and the **owned network** absent. Only this execution's
resources were inspected or removed. Namespace/config/blocklist restoration,
client/native identity removal and owned filesystem/pool removal are retained
as checked booleans in `cleanup.json`; Docker ownership identifiers remain
private and are not part of the bundle.

## Retained reviewed projection

The [five retained files](testdata/cap-send-native-v1/manifest.json) total
**5,954 bytes**. The manifest's payload-only `total_bytes` is **3,348**;
its own size is **2,606**. The configured image reference and inspected
Docker image ID are labeled separately even though their digest strings
happen to agree in this run.

| Payload | Bytes | SHA-256 |
| --- | --- | --- |
| `cap-send.json` | 1,935 | `3b676f8eed435e654119159f5fc265b49c007cbb38b74669f8baf7d63b925fc9` |
| `cleanup.json` | 579 | `a088dc18786c5241957353b9c2a199950216d2592e4d177745878c0ed2c16db7` |
| `runtime.json` | 495 | `8bed16e65c5093913d27788a4e1781abcab64c663b6d61dafd1d7ecdd60b430f` |
| `tests.json` | 339 | `0a4c57d5a75645733b0c8651f57463a86ca51ed609f934433ff749a1e8989759` |

The retained outcome projection records flush TIDs **6 and 7**, one inode,
old/new MDS GIDs, old admission versus refusal, new-generation resubmit versus
first-send, both correlated ACK identities and their arrival order, two native
`applied` branch classifications, and **177 before/177 after** object-request
counts. Native stat records the final size **220,000** on the same inode.
This preserves reviewed identities and selected outcomes; raw daemon lines,
full capability objects, endpoints, errors and configuration are omitted.

A later regression revalidates these unchanged native files against the
independently recorded tested/collector SHA, run ID/attempt and profile.
That is verification of retained data, not another daemon execution or a
replacement attribution for the original run.

## Daemon-free checks and first check failure

At the initial source, **647 Python tests** and the **76-row** mandatory
catalog check passed on Linux. The first actionlint invocation stopped before
validating workflows because automatic project discovery requires Git metadata
that the source export intentionally omitted. Supplying the explicit
`.github/workflows/*.yml` paths then passed actionlint v1.7.7 and fixture unit
checks. This invocation correction is not evidence of a product or workflow
syntax defect.

Subsequent daemon-free controls additionally ensure an invalid test record is
`unknown` rather than an invented PASS, enforce projection collection counts
before iteration, and verify the retained native bundle. Final-source CI and
check results are recorded separately on the PR, rather than assigning the
initial checks to a later commit.

## Remaining acceptance

Only the selected CRC/IPv4/host native profile is newly executed here. The
second admitted secure/IPv6/container profile has synthetic normalization
coverage; this record does not claim its new native bundle execution.

The collector and artifact wiring are from the trusted calling workflow.
Before this PR is merged independently, ordinary comment requests use the
previous main workflow: they can establish exact-head native test success,
but cannot establish a hosted upload of this new bundle. A hosted post-merge
pilot must separately inspect its manifest, included-file hashes, attempt and
artifact retention. This PR must remain open for independent review; no CI
result grants automatic merge permission.

Other scenario projections, raw-log retention, signatures, long campaigns,
semantic deduplication, queue policy and soak-specific immutable observations
remain outside this checkpoint. Issue #1 remains open for those separate
requirements; issue #14 can build on this reviewed manifest contract.
