# Bounded CHECKSUM contract

Reference: Ceph v20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Source provenance is recorded
in `checksum.go` and the independent test-only native wrappers. Upstream
headers and source carry LGPL-2.1 notices (the public librados header also
permits LGPL-3.0). The implementation translates wire semantics; it does not
incorporate native code into the Go product.

`OSDChecksum` (`0x121f`) is one explicitly targeted, read-only head operation.
Callers supply placement, map epoch, object identity, offset and positive
length. Its `Checksum` input selects `OSDChecksumXXHash32` (0),
`OSDChecksumXXHash64` (1), or `OSDChecksumCRC32C` (2), a uint64 seed, and an
optional uint32 chunk size. The 32-bit algorithms reject seeds above
MaxUint32 before transmission; no high bits are silently discarded. Chunk size zero means one checksum over the
returned extent; otherwise the requested length must be a multiple of the
chunk size. Both read length and maximum count-prefixed output are bounded
before transmission. Unknown algorithms, flags, snapshot metadata, compound
operations, wrapping extents and implicit whole-object reads are rejected.

The 28-byte union is little-endian offset (8), length (8), chunk size (4),
type (1), and seven zero bytes. The input payload is the seed as four
little-endian bytes for CRC32C/xxHash32 or eight for xxHash64. A successful
output holds a little-endian uint32 count followed by exactly that many
algorithm-width little-endian values. `Values` preserves every bit as uint64
(zero-extending 32-bit values). Each chunk uses the supplied seed afresh;
this API does not implement hashing or reinterpret values as a Go hash state. Raw data and the echoed union remain available.

The server can trim length at EOF. With a nonzero chunk size, a trimmed
length that is not chunk aligned returns signed `-EINVAL`; that changed
length must remain visible on error. When offset is at or past EOF, Ceph
returns success immediately, keeps the original length, and emits no output.
The API preserves this distinct empty-output case. `-ENOENT` remains a signed
server error. On the observed missing-object admission path, the aggregate
result is `-ENOENT` while the per-operation result is zero; the C convenience
API exposes only the aggregate result. Both Go result fields are preserved
independently. Local cancellation stops waiting, and requests are not replayed.

`tools/osd_checksum_native_encoder.cc` constructs a fresh native request and
four logical native reply shapes for each algorithm (normal, trimmed, EOF, unaligned refusal),
encodes native MOSDOp/MOSDOpReply and an offline MessageFrame header, and
separately decodes each message. This proves encoding compatibility only;
the explicit logical digest values in those vectors are not live execution.
`tools/osd_checksum_oracle.py` independently calls the pinned C
`rados_checksum` against owned disposable objects and records bytes, values,
and unchanged native state. `TestCephOSDChecksumIntegration` compares those
results with Go on binary object contents, multiple seeds, chunked/un-chunked
reads, aligned and unaligned trimming, exact/past EOF and missing objects.
The seven-case suite passed on CRC/AES IPv4 host-relay three-replica BlueStore
and secure/AES256K IPv6 CGO=0 Linux-container single MemStore. The
[validation record](../../integration/fixture/OSD_READ_METADATA_VALIDATION.md)
keeps the first test-assertion failure, corrected native runs and their scope
separate from wire encoding and unit evidence.

The [algorithm expansion record](../../integration/fixture/OSD_CHECKSUM_ALGORITHMS_VALIDATION.md)
distinguishes the new xxHash codec and native evidence from the earlier CRC run.

Checksum compounds, snapshot checksums, automatic placement,
erasure-coded pools and routing/retry policy remain unsupported here. The
broader SPEC environment and object-I/O milestones remain in force.
