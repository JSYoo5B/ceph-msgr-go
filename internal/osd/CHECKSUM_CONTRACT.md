# Bounded CHECKSUM contract

Reference: Ceph v20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Source provenance is recorded
in `checksum.go` and the independent test-only native wrappers. Upstream
headers and source carry LGPL-2.1 notices (the public librados header also
permits LGPL-3.0). The implementation translates wire semantics; it does not
incorporate native code into the Go product.

`OSDChecksum` (`0x121f`) is one explicitly targeted, read-only operation at the caller's read snapshot (zero selects the head).
Callers supply placement, map epoch, object identity, offset and positive
length. Its `Checksum` input selects `OSDChecksumXXHash32` (0),
`OSDChecksumXXHash64` (1), or `OSDChecksumCRC32C` (2), a uint64 seed, and an
optional uint32 chunk size. The 32-bit algorithms reject seeds above
MaxUint32 before transmission; no high bits are silently discarded. Chunk size zero means one checksum over the
returned extent; otherwise the requested length must be a multiple of the
chunk size. Both read length and maximum count-prefixed output are bounded
before transmission. Unknown algorithms, flags, write SnapContext metadata,
unsupported compound combinations, wrapping extents and implicit whole-object
reads are rejected.

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

The [snapshot acceptance](../../integration/fixture/OSD_SNAPSHOT_CHECKSUM_VALIDATION.md)
separately covers caller-selected clones and head resolution using the same
operation. The server does not echo the read snap id; caller submission state
remains the source of that selection. Backoff still uses head object identity.

## Bounded read-only compounds

A vector containing multiple operations and at least one CHECKSUM admits only
CHECKSUM, READ, STAT, ASSERT_VER and CMPEXT. It is head-only and explicitly
targets a replicated primary. FAILOK, CALL, mutations, other operation families,
snapshot selection, replica reads, EC placement and explicit resubmission are
refused for this subset. Existing standalone checksum and snapshot behavior
remains unchanged. Existing ordering, lock and redirect flags retain their
ordinary validators; this adds no discovery of remote manifests, routing or
policy. The default-flag wire proof does not verify every flag combination.

Each operation retains its own extent, seed width, output count and payload
budget. Request inputs and maximum reply outputs are summed with the enclosing
message budget. Independently, the sum of CHECKSUM, READ and CMPEXT lengths
must fit the local frame limit, so a small digest output cannot multiply the
previous per-request logical read bound. Overflow is checked before addition.
Each reply operation consumes exactly its payload_len bytes; an empty EOF
checksum cannot consume the following READ or STAT output.

Tentacle stops at the first ordinary negative operation but preserves earlier
output (PrimaryLogPG.cc:8205–8213, 4310–4321, 9183–9222). For this no-FAILOK
subset, the signed aggregate must equal that first negative result. Earlier
operations validate as a completed prefix; the failing operation retains its
actual error union and bounded output, and later operations must retain zero
result, empty output and unchanged input union. A missing-object admission
error may instead have all-zero per-op results and no output: preserve that
ambiguity without attributing an executed prefix or synthesizing a failed op.

Only after the complete compound reply validates are earlier CHECKSUM Values
decoded, including on a valid negative aggregate. Malformed replies preserve
raw bytes but expose no compound checksum Values and fail the session through
the existing uncertain-outcome path. Zero per-op results alone are never
execution receipts. Error user versions are not success/durability evidence.

The [15-request fresh native proof](testdata/checksum-compound-native-README.md)
compares full v6/v8/v9 request segments for five layouts. Its nonzero offsets
and guard positions are serialization inputs; no server result is inferred
from an offline request. Native C convenience callback results are distinct
from raw wire rvals: ASSERT_VER has no C prval pointer, and an untouched STAT
callback can turn raw zero plus empty output into -EIO. A live comparison
must preserve those boundaries rather than inventing matching raw native rvals.

The [compound acceptance record](../../integration/fixture/OSD_CHECKSUM_COMPOUND_VALIDATION.md)
covers ten cases in each of five Linux configurations: legacy-v6 with CRC/AES
IPv4 host three BlueStore OSDs or secure/AES256K IPv6 container single MemStore,
request-v8/reply-v6 with the former, and request-v8/reply-v8 or
request-v9/reply-v8 with the latter. It compares native compound outputs,
preserves error-prefix Values and untouched suffix metadata, observes unchanged
native object bytes/version/mtime, and checks a fresh read on the same OSD
connection after each negative result. It does not verify compound snapshots,
EC, faults, explicit resubmission or additional request flag combinations.

Automatic placement, erasure-coded pools and routing/retry policy remain unsupported here. The
broader SPEC environment and object-I/O milestones remain in force.
