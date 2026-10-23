# Selected native late COPY_FROM2 error acceptance

Test/controller checkpoint: `17471623b68ef98175f4c2f9eccf0a21df2d49a2`.
Reference: Ceph v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`, pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The [sanitized receipt](testdata/copyfrom-late-error-crc-v6.json) retains the
actual raw Go reply front, signed results, source-version union, local
submission metadata, native state, selected effective option and cleanup.
It includes only synthetic fixture object names and public profile metadata.

## Source boundary and test ownership

The pinned [option declaration](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/options/global.yaml.in#L3312-L3317)
defaults `osd_debug_inject_copyfrom_error` to false. Its declaration alone
does not establish runtime mutability. The
[process_copy_chunk branch](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L9776-L9840)
follows cursor completion and digest checks, returns EIO and precedes
construction of `fill_in_final_tx`.
[CopyFromCallback](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L344-L368)
replies with that error and closes the context without re-entering execution.
This source attribution identifies the injector's phase; this run does not
claim a retained internal cursor trace or request-specific staging barrier.
No Ceph source is incorporated into the product.

`TestCephOSDCopyFromLateErrorIntegration` is an isolated selector, accepted
by the fault controller only with one owned BlueStore OSD. The fixture's
actual test environment (host and container) receives an internal marker only
for that exact selector/profile; the test skips before accessing fixture state
when the marker is absent. Broad suites and other dedicated rows receive an
empty marker even if the invoking environment supplies one. It does not run
alongside other copy tests. The disposable fixture also owns three MONs,
two MGRs, its control/native-client containers and network. The authorized
Linux runner's run lock excludes other Docker runs during this test.
No host setting or unrelated daemon is changed.

The controller uses only the owned OSD's local admin socket. Every get/set
has a five-second context and a bounded, selected-key receipt. It reads the
original false value and `osd_copyfrom_max_chunk`, sets the injector once,
and separately reads back true before admitting a copy. Restoration sets
the original value once with a fresh bounded context and separately reads
it back. An uncertain apply retains restoration ownership; uncertain
restoration is observed again instead of blindly replaying its setter.
Teardown retains restoration failures and still terminates owned resources.
Fixture unit tests exercise these partial-acknowledgement and effective-state
gates; those tests are distinct from daemon acceptance.

## Operations and actual result

One native C API preparation owns three names: source, initially absent
native-copy target and initially absent Go-copy target. Source data is 21
bytes with one binary xattr, two bounded OMAP entries and a binary header.
It is below the independently observed 8,388,608-byte copy chunk limit.
The test requires that limit to exceed 64 KiB, well above its bounded metadata.
Go sets the source header and seeds the other destination with different
bytes, xattr, OMAP/header, version and mtime before enabling the fault.
Native librados reads plus the pinned rados header reader independently
record both preconditions.

The calibration executes one pinned `rados cp` to its separate absent owned
target. The existing helper's distinct-name, ownership and absence guards
remain intact; its fixed argv, 30-second timeout and no-retry policy are
unchanged. This native CLI exits **1** and the target remains ENOENT before
and after. That exit is a process status, not a signed wire errno and not
a native twin of the following compound.

The Go request is submitted once against the populated destination:
COPY_FROM2 of the exact native source version, followed by SETXATTR
`late-copy-suffix`. No retry, resubmission, positive copy or daemon kill
follows it. The selected cell actually passed without skips:

| Field | Observation |
| --- | --- |
| Platform/profile | Linux/amd64, CRC/AES client and service, IPv4 host, legacy-v6 request/reply, one BlueStore OSD |
| Effective injector | false -> true -> false, independent reads after both mutations |
| Go aggregate / per-op results | -5 / [-115, 0] |
| Original source union | version 3, head snapshot 18446744073709551614 |
| Local submission / reply | TID 3, global ID 4638, attempt 0, epoch 13 |
| Native pool / primary / actual PG / object hash | 2 / 0 / 0 / 2270748240 |
| Error reply metadata | flags 67108900, user version 0, zero operation output lengths/data, retained raw front |
| Native source | version 3, mtime 1700011000.123456789; all state unchanged |
| Native populated destination | version 4, mtime 1700011001.987654321; all state unchanged |
| Read after restoration | same connection TID 4, native version 4 and original destination bytes |
| Integration selector duration | 3.83 seconds |

The typed Go error is OSDError with signed code -5, not OutcomeUnknownError.
The copy operation retains -115 (EINPROGRESS), while the untouched suffix
retains zero. **That zero is not proof the suffix executed.** Exact native
before/after equality covers bytes, complete known xattr/OMAP inventory,
header, mtime and version; the suffix marker is absent. The source remains
unchanged. The read-only follow-up after restoration checks the same
connection and next TID without submitting a positive copy.

Object cleanup has a separate 40-second context and removed all three owned
names. Runner completion exited zero, and direct post-run inspection
confirmed all eight recorded containers and their network were absent.
The partial result is written even if cleanup itself fails. The diagnostic
allowlist includes `osd-copyfrom-error-option.json` and
`osd-copyfrom-late-error-results.json`; private ownership files are used
only for teardown verification. The initial selected native run passed. Independent review later found that
the initial parent test could enter broad single-BlueStore suites while its
controller correctly refused a broad selector. Dedicated-row success did not
exercise that selection boundary. The fixture-derived marker and cluster-free
skip/environment regressions correct that test-selection bug; they do not
change the observed daemon error or weaken the controller's exclusivity.

## Reproduction and limits

On an authorized Linux runner, hold its run lock, use the pinned image and:

```sh
CEPH_MSGR_TEST_OSD=1 \
CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=1 \
CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 \
CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc \
CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDCopyFromLateErrorIntegration$' \
CEPH_MSGR_TEST_TIMEOUT=8m \
go -C integration/fixture run . run -project ../..
```

The operation context is 240 seconds. The controller rejects a broad test
selector, another topology or a pre-enabled injector. Its dedicated scenario ID is
`osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-osdcopyfromlateerror`.
Request that one row through the implemented CI comment command; basic PR CI
builds/unit-checks the fixture but does not execute this daemon test. The
local checkpoint above and a later exact-head CI run remain distinct evidence. The client-package suite has no
matching selector and contributes no daemon evidence.

This is one late server-error checkpoint. It does not establish destination
loss after source-read completion, temporary-object staging/cleanup,
mid-vector termination, arbitrary compound rollback, all chunk sizes,
COPY_FROM Go fault execution, snapshots, EC, modern wire versions,
secure/aes256k/IPv6 or other topologies. The source-based phase explanation
is separate from actual native object-state and Go wire observations.
Keep #7 open for its remaining fault and compound requirements.
