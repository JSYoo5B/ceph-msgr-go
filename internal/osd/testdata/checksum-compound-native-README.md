# Fresh native CHECKSUM compound request vectors

[The retained proof](checksum-compound-native-provenance.json) contains
15 fresh MOSDOp requests from Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`: five fixed layouts across
v6, v8 and untraced v9. They combine CRC32C, xxHash32 and xxHash64 CHECKSUM
with READ, STAT, ASSERT_VER or binary CMPEXT. The layouts cover distinct
four/eight-byte seeds, concatenated inputs, nonzero offsets and operation
positions needed for later EOF and error-boundary tests. These requests
alone establish no checksum values, object contents or execution outcomes.

Each vector uses a fresh native ObjectOperation and MOSDOp. The wrapper calls
canonical `Message::encode` once and decodes into a separate fresh object,
without re-encoding it. The Go test matches complete header/front/middle/data,
all packed operation unions and seed/CMPEXT inputs in order. It checks request
identity, the distinction between raw object hash and actual modern PG seed,
modern backoff admission identity, zero mtime, head/empty context and tracing.
Only implemented default feature inputs are used; cache-pool bit 35 is absent.

The native 41-byte Messenger v2 header uses offline sequence 1, TID 77,
ACK sequence 3 and priority 127 through `MessageFrame::Encode`. The SDK uses
WITH_JAEGER=OFF. This is serialization evidence and does not execute a live
ProtocolV2 session. No constructed reply vectors are included in this proof.

The builder reuses unchanged MPing SDK and IgnoreCache provenance helpers.
Hashes bind the new wrapper and driver, the shared wrapper and both imported
drivers. The official archive, pinned submodules, eight unchanged codec
sources, generated configuration, compiler, matching library and executable
are recorded. Compile-database binding, amd64 ELF/loader checks and refusal of
an existing output directory keep native execution tied to the owned SDK.

The first execution generated all 75 files but failed the Python model's
ASSERT_VER expectation: native encodes an unused u64 followed by version u64,
where the model had put the version first. The canonical producer and existing
Go encoding were correct. Its driver, build log, cleanup receipt and all raw
outputs were preserved; the corrected model independently accepted all 15
first-run requests. The final fresh builder ran on 2026-10-18 from 08:16:12
to 08:19:03 UTC, exited zero, and its named container was absent afterward.
The proof retains both execution boundaries and build-log hashes. All 15
final native requests match the Go encoder.

Inside an owned Linux amd64 development container using the pinned image in
the proof, mount the five tools as siblings and use a fresh SDK work directory,
or a cache without encoded-checksum-compound:

```sh
python3 tools/osd_checksum_compound_native_encoder.py --build-native \
  --work .artifacts/native-sdk \
  --wrapper tools/osd_checksum_compound_native_encoder.cc
```

The last structured JSON line is this proof; the preceding one belongs to
the shared MPing build. Preserve the log, attach the actual execution receipt
and log hash, and independently inspect owned container cleanup. Compare with:

```sh
CGO_ENABLED=0 go test ./internal/osd -run '^TestChecksumCompound' -count=1
```

Ceph development headers and libraries carry LGPL-2.1 notices. Native tools
remain development-only; only wire facts and provenance enter the pure-Go
product repository.
