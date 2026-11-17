# Fresh native manifest tier operation request vectors

[The retained proof](tier-op-native-provenance.json) contains nine fresh MOSDOp
requests from Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`:
three closed shapes across v6, v8 and untraced v9. Each request carries one
inputless operation built by the native `ObjectOperation` helpers
(`osdc/Objecter.h` `tier_promote`, `tier_flush`, `tier_evict`): a zero packed
union, no per-operation flag and no payload.

| Shape | Opcode | Request flags beyond ONDISK and RETURNVEC | mtime | Go client |
| --- | --- | --- | --- | --- |
| `promote` | TIER_PROMOTE `0x2229` | WRITE | 1700000000.123456789 | encoded, byte-identical |
| `flush` | TIER_FLUSH `0x822b` | READ, IGNORE_CACHE, IGNORE_OVERLAY | none | encoded, byte-identical |
| `evict` | TIER_EVICT `0x822c` | READ, IGNORE_CACHE, IGNORE_OVERLAY | none | encoded, byte-identical |

The flags follow the native callers at the pinned commit (`tools/rados/rados.cc`):
`rados tier-promote` submits an `ObjectWriteOperation` through
`IoCtx::operate`, so Objecter adds WRITE and the request carries an mtime;
`rados tier-flush` and `rados tier-evict` submit an `ObjectReadOperation`
through `aio_operate` with `OPERATION_IGNORE_CACHE|OPERATION_IGNORE_OVERLAY`,
which `translate_flags` maps to wire `0x8000|0x20000`, and
`Objecter::get_read_flags` adds READ. ONDISK and RETURNVEC are set explicitly
so that the Go encoding can be compared byte for byte; the fixed mtime is a
test input. The features are the default implemented set without
OSD_CACHEPOOL (bit 35): these operations target a pool that is not a cache
tier, and MOSDOp encodes its features in the front, so the bit's absence is
part of the request bytes. These vectors establish request bytes only: no
manifest, chunk, dedup or dirty state and no server result.

Each vector uses a fresh native ObjectOperation and MOSDOp. The wrapper checks
that the builder produced exactly one operation without input or per-op flag,
with WRITE mode for promote and CACHE mode for flush and evict, calls
canonical `Message::encode` once, and decodes into a separate fresh object
without re-encoding it. The Python driver compares the decoded request with
its own independent model of opcodes, flags and mtimes, and locates the
packed operation exactly once in the front. Request identity, locator, PG
seeds, the modern trace envelopes and the 41-byte offline Messenger v2 header
follow the existing request-vector convention (sequence 1, TID 77, ACK
sequence 3, priority 127).

## Generation

The builder reuses the unchanged MPing SDK and IgnoreCache provenance helpers;
hashes bind the new wrapper and driver
(`tools/osd_tier_op_native_encoder.{cc,py}`), the shared wrapper and drivers,
the official archive, pinned submodules, unchanged codec sources, generated
configuration, compiler, library and executable.

The vectors were generated on 2026-11-17 in one disposable container of the
pinned image on a Linux amd64 host (Docker 29.9.1). The in-container steps
are those of the [cache-tier operation vectors](cache-op-native-README.md),
which ran on a hosted runner instead: the driver, its wrapper and the three
shared helper files were copied into a fresh container, the driver ran once
with `--build-native`, and the container was removed. Its first and only attempt
ran from 05:21:26 to 05:31:08 UTC, exited zero, and its container was absent
after removal; the proof's `execution` object and `build_log_sha256` record
it. The proof's wrapper and driver hashes equal the committed files, which
`TestTierOperationsRetainFreshNativeRequests` checks.

## Go comparison

The Go encoder implements these operations (see
[MANIFEST_CONTRACT.md](../MANIFEST_CONTRACT.md#go-tier-operation-contract)).
For each vector, the Go request with the matching inputs equals the native
header, front, middle and data:

```sh
CGO_ENABLED=0 go test ./internal/osd -run '^TestTierOperation' -count=1
```

Ceph development headers and libraries carry LGPL-2.1 notices. Native tools
remain development-only; only wire facts and provenance enter the pure-Go
product repository.
