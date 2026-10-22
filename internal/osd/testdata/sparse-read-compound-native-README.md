# Fresh native SPARSE_READ/READ compound request vectors

[The retained proof](sparse-read-compound-native-provenance.json) contains
15 fresh MOSDOp requests from Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`: five fixed layouts across v6, v8
and untraced v9. Each layout combines SPARSE_READ with READ or a second
SPARSE_READ in one native `ObjectOperation`, in the order the native librbd
object read builds them (`librbd/io/ObjectRequest.cc:238-246`). Fadvise hints
are applied with `set_last_op_flags`, which is how neorados places them
(`neorados/RADOS.cc:449-452`). The `librbd-mixed` layout carries one hint on
its last operation, as librbd does. The `fadvise-each` layout carries a
different hint on every operation through the same native builder, and
`eof-layout` places offsets past a later object end. These are request
layouts only. They establish no allocation maps, object bytes, EOF behavior
or execution outcomes.

Each vector uses a fresh native ObjectOperation and MOSDOp. The wrapper calls
canonical `Message::encode` once and decodes into a separate fresh object,
without re-encoding it. The native decoder must return every packed union,
including its operation flags, unchanged, with no input bytes. The Python
driver checks the decoded operations against its own independent model of the
packed unions. The Go test then matches the complete header, front, middle and
data segments, all packed operation unions and flags in order, request
identity, the raw object hash versus actual modern PG seed, modern backoff
admission identity, zero mtime, head/empty snapshot context and tracing. Only
implemented default feature inputs are used; cache-pool bit 35 is absent.

The native 41-byte Messenger v2 header uses offline sequence 1, TID 77,
ACK sequence 3 and priority 127 through `MessageFrame::Encode`. The SDK uses
WITH_JAEGER=OFF. This is serialization evidence and does not execute a live
ProtocolV2 session. No constructed reply vectors are included in this proof.

## Generation

The builder reuses the unchanged MPing SDK and IgnoreCache provenance helpers.
Hashes bind the new wrapper and driver, the shared wrapper and both imported
drivers. The official archive, pinned submodules, eight unchanged codec
sources, generated configuration, compiler, matching library and executable
are recorded. Compile-database binding, amd64 ELF/loader checks and refusal of
an existing output directory keep native execution tied to the owned SDK.

The vectors were generated on 2026-10-22 by a disposable GitHub Actions job
on a GitHub-hosted `ubuntu-latest` runner
([run 39810414495](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39810414495),
scratch branch `claude/native-vectors-sparse-read-compound`, not merged). The
job copied this repository's `tools/` directory, fetched the pinned Ceph Git
tree listing with the job token, pulled the pinned image and ran the driver
once in a fresh named container from that image, with an empty SDK work
directory mounted at `/native-cache`:

```sh
docker run --name <unique> --entrypoint python3 \
  -v <work>:/native-cache -v <tools copy>:/native-src/tools <pinned image> \
  /native-src/tools/osd_sparse_read_compound_native_encoder.py --build-native \
  --work /native-cache/work \
  --wrapper /native-src/tools/osd_sparse_read_compound_native_encoder.cc
```

The first and only attempt ran from 16:02:39 to 16:10:13 UTC, built the SDK
from the official archive, exited zero, and its container was absent after
removal. The proof's `execution` object records that receipt and
`build_log_sha256` hashes the complete driver output whose last line is the
proof. The workflow artifact retained the raw encoded files; every retained
hexadecimal segment was compared with those files before this proof was
committed. The 15 native requests match the Go encoder:

```sh
CGO_ENABLED=0 go test ./internal/osd -run '^TestSparseReadCompound' -count=1
```

Ceph development headers and libraries carry LGPL-2.1 notices. Native tools
remain development-only; only wire facts and provenance enter the pure-Go
product repository.
