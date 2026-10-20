# Fresh native IGNORE_CACHE request vectors

[The retained proof](ignore-cache-native-provenance.json) contains 24 fresh
MOSDOp requests generated on 2026-10-18 from Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. It covers v6, v8 and untraced v9,
standalone STAT, READ, WRITEFULL and DELETE, each with IgnoreCache disabled
and enabled. Normal/IgnoreCache pairs use identical bit-35 feature inputs
and differ only by request flag `0x8000` in their front segments.

The development-only C++ wrapper constructs a new ObjectOperation and MOSDOp
for every vector. It invokes canonical native `Message::encode` once, then
decodes into a separate fresh object and verifies its fields. It never
re-encodes a decoded object. Header, front, middle and data bytes and hashes
are retained. The Go test compares complete segments, packed operation unions,
request identity, map epoch, locator, snapshot fields, retry count and mtime.

The 41-byte Messenger v2 header comes from native `MessageFrame::Encode`
with explicit offline sequence 1, TID 77, ACK sequence 3 and priority 127.
This proves header serialization for those inputs. It does not exercise a
live `ProtocolV2` session or prove server execution, placement or durability.
The modern requests contain the native empty legacy trace, and v9 additionally
contains an invalid empty OTEL envelope with the SDK built WITH_JAEGER=OFF.

The driver reuses the unchanged MPing SDK builder. Both wrappers, both build
drivers, the official source archive and pinned submodules, generated CMake
configuration, compiler, matching library and native binary have retained
digests. Eight codec source files are checked byte-for-byte against the
official source archive before and after generation. ELF architecture and
`ldd` checks bind the executable to the owned Linux amd64 library. Cached
CMake targets must point to the exact mounted wrapper path. An existing
output directory is refused to preserve earlier evidence.

The actual owned daemonless builder ran from 07:01:19 to 07:04:22 UTC and
exited successfully; an independent Docker inspection found its named
container absent after cleanup. The proof retains the execution receipt and
the complete build log's SHA-256. The source/build provenance is separate
from any live cache fixture evidence.

To regenerate inside an owned Linux amd64 development container with the
pinned image recorded in the receipt, mount the tools as explicit siblings
and use a fresh SDK work directory (or a cache whose encoded-ignore-cache
output has not already been created):

```sh
python3 tools/osd_ignore_cache_native_encoder.py --build-native \
  --work .artifacts/native-sdk \
  --wrapper tools/osd_ignore_cache_native_encoder.cc
```

The last structured JSON line is the IgnoreCache proof; the preceding one
belongs to the shared MPing SDK build. Preserve both logs, and verify owned
container cleanup independently before accepting the generated proof. Run
the product comparison with:

```sh
CGO_ENABLED=0 go test ./internal/osd -run TestIgnoreCache -count=1
```

Native Ceph headers/library carry LGPL-2.1 notices. Only independently
constructed wire facts and provenance are retained here; native development
dependencies do not enter the Go product's build or runtime.
