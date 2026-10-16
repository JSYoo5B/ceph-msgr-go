# CHECKSUM algorithm expansion — 2026-10-16

Reference: Ceph Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`.

The existing bounded standalone head CHECKSUM operation now accepts xxHash32
and xxHash64 alongside CRC32C. This extends the existing request and reply
codecs; there is no product hashing dependency, routing, native runtime,
snapshot operation or automatic replay. Product builds remain CGO-disabled.

## Wire and API

The wire algorithm IDs are 0 (xxHash32), 1 (xxHash64), and 2 (CRC32C).
Seed and digest widths are respectively 4, 8 and 4 bytes, always little-endian;
the count is always a little-endian uint32. Each chunk starts with the same
supplied seed. The public Seed and Values now use uint64, preserving full
xxHash64 bits and zero-extending 32-bit results. Seeds above uint32 for
32-bit algorithms are rejected before transmission. Output budgets include
the correct per-algorithm width, and malformed counts or partial values
are refused. No compatibility aliases or implicit narrowing were added.

The native sources are include/rados.h:549-553,660-666,
include/rados/librados.h:1585-1618, osdc/Objecter.h:623-650,
osd/PrimaryLogPG.cc:5533-5711 and common/Checksummer.h at the pinned commit.
They carry the upstream LGPL terms recorded in the checksum contract;
no native implementation is copied into the Go product. The native xxHash
implementation's BSD license adds no product dependency: Go transports the
seed and server-produced values, rather than calculating those hashes.

## Independent evidence

Native encoding vectors and live executions are separate proofs. The native
C++ wrapper constructs requests and logical replies with the pinned Ceph
encoders; its explicit output digests are not evidence of live hashing.
The test-only Python adapter calls the independent C rados_checksum API.
Its ABI tests check both 32-bit and full 64-bit seeds/results, bounded counts,
EOF sentinel handling and signed errors.

The live Go suite selects eight cases per algorithm: nonzero and zero seed
full reads, chunked binary data, aligned/unaligned EOF trimming, exact/past
EOF and a missing owned object. Every nonzero xxHash64 seed has high bits
set. Each case compares raw output bytes and interpreted values with
librados, preserves aggregate errors and checks unchanged object state.
Native client retries do not imply Go retries. A missing object's per-op
status is not inferred from the C API's aggregate error.

## Results

The pinned native build emitted 15 independently encoded/decoded vectors
(one request and four reply shapes for each algorithm). The shared build
driver stayed byte-identical, and all earlier CRC header/front/data bytes
are identical. Each algorithm selection rebuilt and executed the same fresh
wrapper with the matching native library; the merged provenance preserves
all three build receipts. The fixture's retained-segment proof test passed.

An initial setup invocation was interrupted before encoding after a local
path error; its owned builder was terminated. The corrected native run
completed all three encodings. Its controller had compiled the earlier
registration list and refused a reply header as a request. The registration
order was corrected (all three requests precede replies); the current
reader then validated the original full native log, and the retained proof
was checked again. No daemon or product failure occurred in these encoder
setup checks, and the interrupted/refused records are retained locally:
`/private/tmp/checksum-xxhash-proof-20261016.json.log` and
`/private/tmp/checksum-xxhash-proof-20261016-second.json.log`.
The observed builder
`a94580ac66664bf964f80c46f043fa0b9aa8b3875ce57bae31f291ec4380fa4c`
was terminated before live acceptance.

Whole-product `CGO_ENABLED=0 go test ./...` and `CGO_ENABLED=0 go vet ./...`
passed. Focused normal and race checks for the checksum/v8/public native
paths passed; the Python oracle's six ABI/bounds tests passed. The
integration suite compiled and skipped by default without opt-in inputs.

Both first live attempts passed against
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

| Configuration | Cases | Selected suite time | Result |
| --- | --- | --- | --- |
| CRC/AES client and service, IPv4 host, 3 BlueStore replicas | 24 (8 per algorithm) | 14.56 s | PASS |
| secure/AES256K client and service, IPv6 container, single MemStore | 24 (8 per algorithm) | 13.81 s | PASS |

All cases compare raw native/Go output, full-width interpreted values,
aggregate codes and unchanged head bytes/version/mtime. The xxHash64 seeded
cases use `0xfedcba98ffffffff` and `0xfedcba9812345678`, exercising bits that
would be lost by a uint32 seed. These are local native results; they do not
claim completed remote CI. The fixture module pins v0.3.0 in go.mod, while
these original manifests retain an older hardcoded `provider_version`
label of v0.2.0. The label is not used as evidence of the linked version.
The earlier CRC-only results remain in
[the original read-metadata acceptance](OSD_READ_METADATA_VALIDATION.md).

Retained local artifact roots and adjacent logs:

- `/private/tmp/ceph-checksum-algorithms-crc-aes-20261016-first/`
- `/private/tmp/ceph-checksum-algorithms-secure-aes256k-ipv6-20261016-first/`

Results receipt (`osd-checksum-results.json`) SHA-256:

- CRC: `3a6e727bdd94855f279771e197a6b5c6fc7bbf8cd51cd1bc75c63b383cc4ba9f`.
- Secure: `8cfb010d0587e48cd7235b560a6bed6637688da7f5fd275144bf2ea9724dbf09`.

Frozen live test source:
`df8f033f75046a579173d570972993a5213439d0ba0e75ddb96eb7738cd2d82d`.
Frozen independent C adapter:
`5f00bd3ccaf0e35b4ba4f69d0a10569a6ee45518a7f5bbf9a4d7f252cddb393f`.
Native wrapper:
`f5755769cf4230607d3326662f33a4d4c7faa354d4d6a131cc7c188a9f19560b`.
The public package also cross-compiled for Windows/amd64 with CGO disabled;
the nested fixture's whole test suite and vet passed.

## Limits

Compound requests, snapshot checksums, implicit unbounded whole-object
requests, EC pools and automatic placement/retry remain outside this change.
