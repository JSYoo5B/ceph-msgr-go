# MGR service message codec vectors

Development reference: Ceph Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`.
The wrapper includes the unchanged LGPL-2.1 native headers and invokes
`Message::encode`, the respective native payload decoders, and `MessageFrame`.
No Ceph implementation is copied into the product. Native libraries and build
packages are restricted to the disposable development container.

Generated on 2026-10-21 UTC on Linux/amd64 in the pinned image below, using
GCC toolset 14 and the official CMake Debug configuration (`-O0 -g0`). Source,
configuration, binary, library and producer identities are retained in
[mgr-service-native-provenance.json](mgr-service-native-provenance.json).

The producer uses fresh native objects with explicit test inputs. A separate
fresh native object decodes each payload and checks every supplied field and
the unused fields. Decoded objects never become encoder inputs. Reports use
Ceph's native versioned-envelope macros for the empty counter buffer, matching
`MgrClient::_send_report` with no selected counters.

| Vector | Native class/version | Boundary |
| --- | --- | --- |
| open-empty | MMgrOpen 3/1 | Explicit service daemon, empty metadata/status/config |
| open-populated | MMgrOpen 3/1 | Sorted metadata keys, binary NUL/0xff value, initial status |
| update | MMgrUpdate 2/1 | Metadata-update flag, replacement metadata, empty ignored status |
| report-omitted | MMgrReport 9/1 | Both optional maps absent, empty versioned counter envelope |
| report-empty | MMgrReport 9/1 | Both maps present with zero entries, distinct from omission |
| report-populated | MMgrReport 9/1 | Replacement status and task status, binary value |
| configure | MMgrConfigure 4/1 | Period/threshold, no OSD/MDS extensions |
| close | MMgrClose 1/1 | Service/daemon identity, shared close/echo wire format |

`TestMgrServiceNativeVectors` compares all four Go frame segments for every
outgoing message and decodes native configure/close payloads. It verifies the
closed eight-vector set, segment digests, source/build/producer identities and
the report presence distinction. These retained codec checks execute without
CGO or Ceph. Python receipt-rejection tests use explicitly synthetic artifacts;
they are not native vectors or independent wire proofs.

The 41-byte native header is an offline `ceph_msg_header2` projection through
`MessageFrame`, with sequence 1, acknowledgement sequence 3, default priority,
zero transaction ID and `MSG_CRC_ALL`. This is not a live ProtocolV2 capture.
Configure versions 1-3 and nonempty daemon extensions retain separate synthetic
coverage; they are not fresh current-source native vectors. No daemon execution,
metadata application, permission, renewal, failover, close acknowledgement or
expiry result follows from these bytes.

## Reproduction

Run on an authorized Linux/amd64 Docker runner with its shared execution lock.
From the repository root, copy the four producer/bootstrap files into an owned
workspace; the existing bootstrap updates its wrapper timestamp, so mount the
copy writable. Do not mount the source checkout writable into the container.

```sh
mkdir -p .artifacts/mgr-service-native/tools .artifacts/mgr-service-native/work
cp tools/mgr_service_native_encoder.py tools/mgr_service_native_encoder.cc \
   tools/mon_ping_native_encoder.py tools/mon_ping_native_encoder.cc \
   .artifacts/mgr-service-native/tools/
docker run --rm --name mgr-service-native-encoder \
  -v "$PWD/.artifacts/mgr-service-native/tools:/tools" \
  -v "$PWD/.artifacts/mgr-service-native/work:/work" \
  --entrypoint python3 \
  quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9 \
  /tools/mgr_service_native_encoder.py --work /work \
  --wrapper /tools/mgr_service_native_encoder.cc \
  > .artifacts/mgr-service-native/build.log 2>&1
```

The existing MPing driver bootstraps the pinned source, official CMake
configuration and matching ceph-common library. Its four MPing outputs do not
count toward the eight MGR vectors. The new target is a separate wrapper linked
to that same library; the driver verifies its loader before producing vectors.
The last JSON line is the receipt, with exact producer, source/archive,
submodule, configuration, binary/library and segment hashes. Retain it as
`mgr-service-native-provenance.json` only after successful generation and privacy
review. The collector refuses missing/extra artifacts, foreign envelopes and
contradictory decoded-field receipts. A preexisting MGR output directory is
refused; archive the previous owned output before another fresh run.

An initial preparation attempt with a read-only tools mount stopped at the
bootstrap's wrapper timestamp update, before compiling any vector. Correcting
that mount is an execution setup change, not a wire or product fix. A slow
Boost download was interrupted before vector generation and replaced with a
cached archive matching the upstream CMake SHA-256
`af57be25cb4c4f4b413ed692fe378affb4352ea50fbe294a11ef548f4d527d89`.
