# Tentacle O2b bounded OMAP read contract

Reference: Ceph 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.
Source audit: 2026-10-15, against the local SDK used by the native mutation and
xattr encoder oracles. GETKEYS, GETVALS and GETVALSBYKEYS are now implemented
with fresh native encoder/decoder vectors and separate live native-C/Go read
acceptance. [OSD_OMAP_VALIDATION.md](../../integration/fixture/OSD_OMAP_VALIDATION.md)
records exact profiles and gaps. Header, comparisons and writes below remain
proposals, not implemented support. [CONTRACT.md](CONTRACT.md) supplies existing v6
identity, explicit placement, context, per-op results and uncertain-outcome
rules; [METADATA_CONTRACT.md](METADATA_CONTRACT.md) concerns the separate xattr
subset. Neither supplies OMAP comparison or pagination semantics.

## Implemented reads and proposed follow-up subset

The [opcode list][opcodes], [native read builders][read-builders] and
[native write builders][write-builders] support these candidates:

| Operation | Opcode | Candidate scope | Native request data | Native output |
| --- | --- | --- | --- | --- |
| OMAPGETKEYS | 0x1211 | Implemented bounded single page | string start_after, u64 max_to_get | u32 count, strings, bool more |
| OMAPGETVALS | 0x1212 | Implemented bounded single filtered page | string start_after, u64 max_to_get, string filter_prefix | u32 count, key/value blobs, bool more |
| OMAPGETHEADER | 0x1213 | Include bounded raw bytes | Empty | Raw header bytes, no internal length prefix |
| OMAPGETVALSBYKEYS | 0x1214 | Implemented bounded requested key set | Native encoded set of strings | Native encoded map; no more suffix |
| OMAP_CMP | 0x1219 | Include EQ/LT/GT assertions after independent oracle | Native encoded map of key to (value bufferlist, int comparison) | No output; success 0 or signed error |
| OMAPSETVALS | 0x2215 | Include bounded encoded entries | Native encoded string-to-bufferlist map | No output |
| OMAPSETHEADER | 0x2216 | Include bounded raw header | Raw header bytes | No output |
| OMAPCLEAR | 0x2217 | Include only with native header-clear evidence | Empty | No output |
| OMAPRMKEYS | 0x2218 | Include bounded encoded key set | Native encoded set of strings | No output |
| OMAPRMKEYRANGE | 0x222c | Defer first implementation | string key_begin, string key_end | No output |

The initial read milestone deliberately excludes header and comparison until
their independent execution oracles are ready. The four listed write proposals
follow separately, using the same explicitly targeted replicated BlueStore
profile as O1/O2a. RMKEYRANGE is a separate milestone: the audited builder and
handler exist, but endpoint ordering, half-open range behavior and empty/reversed
ranges need their own native acceptance. TMAP conversion, class calls, allocation
hints, snapshots, rollback and watch remain separate work. No compatibility
wrapper for an earlier Go API or earlier Ceph release is proposed.

The native 38-byte operation envelope remains opcode u16, flags u32, union
28 bytes and payload_len u32. For GETKEYS, GETVALS, BYKEYS, CMP, SETVALS,
SETHEADER and RMKEYS the builders set extent.offset=0 and extent.length to the
**encoded input byte length**. The remaining union bytes are zero. For
GETHEADER/CLEAR the whole union and input payload are zero. This extent.length
is not an object read/write range or an output budget. Its canonical value must
be derived from admitted input, with overflow checks; callers must not supply
contradictory union fields. Replies retain the operation union, while native
MOSDOpReply derives payload_len from each outdata buffer. [Native vector merging][merge]
concatenates input/output buffers in submitted operation order.

## Exact collection and output framing

For this subset, a string/bufferlist field is u32 byte length followed by bytes.
A set is u32 count followed by its strings in native set order. A map is u32
count followed by each key string and value bufferlist in native map order.
An OMAP_CMP assertion appends a signed 32-bit comparison identifier after the
value bytes; it is not xattr's u8 comparison union or STRING/U64 mode.

GETKEYS output consists of an encoded set followed by one bool byte. GETVALS
output consists of an encoded map followed by one bool byte. BYKEYS output is
only the map. GETHEADER output is the raw header, including an empty buffer.
Preserve each entire raw per-op output and the original reply data segment.
An optional bounded collection decoder may expose ordered raw byte entries;
it must not turn this transport into a storage map abstraction, normalize names,
infer missing values, or replace the original encoding.

The [Tentacle handlers][read-handlers] explicitly encode the GETKEYS/GETVALS
more byte. Native Objecter's [collection callback][collection-callback] can
infer truncation when older server output omits that byte. That compatibility
branch is not the proposed product contract: Tentacle-only decoding should
require the appropriate suffix, validate the bool, consume exactly the payload,
and reject trailing bytes or truncated counts/fields. BYKEYS must not acquire
an invented more field. Unknown per-op/aggregate signed errors stay available
without mapping malformed local output into a fabricated server error.

GETKEYS/GETVALS preserve ascending native key iteration order. BYKEYS returns a
native ordered map of found keys, not request order. Missing requested keys are
omitted; they are not represented by empty values. An existing empty value is
an entry with a zero-length value, so an iterator's NULL value pointer is not an
end marker. Map/set input should be encoded in native byte-key order with
unique keys; duplicate input keys must have an explicit local validation rule,
not undocumented last-writer replacement through a language map constructor.
The audited SET/RM [transaction parsers][transaction-parsers] validate framing
and forward the encoded data; they do not establish canonical key uniqueness
or ordering checks for arbitrary non-native inputs.

Keys, start_after and filter_prefix are length-aware bytes in the wire format.
The audited BlueStore [database key builder][store-key] appends the full string;
it does not apply xattr's underscore prefix, Unicode rules or strlen conversion.
Do not transfer xattr empty/NUL-name restrictions to OMAP as server facts.
Binary/non-UTF-8/NUL and empty keys need explicit native characterization before
claiming compatibility. Values and headers are arbitrary bytes; an empty value
is not removal. No client UTF-8 conversion, trimming or case folding is proposed.

## Cursor, prefix and limit semantics

GETKEYS seeks strictly after start_after using UPPER_BOUND. GETVALS starts at
max(start_after, filter_prefix), uses LOWER_BOUND when filter_prefix is greater
than start_after, otherwise UPPER_BOUND, then returns only keys having the exact
byte prefix. These comparisons concern keys, not OMAP_CMP value comparisons.
An empty start_after is an actual empty-string boundary: it is not a distinct
"before every key" sentinel. The source therefore excludes an empty key from
an unfiltered first page starting after empty. BYKEYS can address explicit keys
independently; do not promise enumeration of all possible keys with that cursor.

GETKEYS reports more when the backend iterator stopped with a key remaining.
GETVALS sets more when count/byte limits stop inside the matching prefix; stopping
because the prefix no longer matches does not set more. Exact-limit final pages,
zero requested count, a prefix before/after/equal to the cursor, empty keys and
changes between pages all require native tests. The product must return one
page and its raw more state; automatic pagination, cursor selection and merging
pages belong to callers. There is no snapshot/consistency guarantee across
separate requests in this proposal.

The [configuration defaults][limits] are osd_max_omap_entries_per_request=1_K
and osd_max_omap_bytes_per_request=1_G. GETKEYS/GETVALS clamp max_to_get to the
configured entry limit. The handlers test the accumulated encoded entry-body
length **before** adding the next entry; one item can exceed the byte threshold,
and the outer count/more framing adds another five bytes. This is not a hard
upper bound for the complete reply and is not an EFBIG refusal. A large first
value may already exceed a caller's output budget. BYKEYS and GETHEADER do not
apply these two pagination caps in the audited handlers.

Every read operation therefore needs a positive local output-byte budget;
collection decoding also needs bounded counts and individual lengths. Enforce
aggregate output/frame bounds before allocating and validate actual output
after receipt. The count limit and local budget serve different purposes. A
wire max_to_get is a u64; the output count is u32. Do not hardcode the server
configuration default as the client's budget, treat more as proof that an
output budget was honored, or manufacture a remote byte-limit argument.
Proposed local validation must cover zero/huge counts and all overflow cases;
the proposed raw single-page API admits zero max_to_get with a positive local
output budget and preserves its actual more state. Native acceptance for that
case remains required; do not claim the server returns EINVAL for zero.

A pinned native-builder caveat must be retained for future oracle selection:
the [optional<string_view> GETVALS overload][optional-builder] encodes
*start_after when filter_prefix is present, rather than *filter_prefix. This
is directly visible source behavior; no runtime failure was exercised here.
Use the correct std::string overload for a fresh native encoder oracle, and
record overload selection. The [C get_vals/get_vals2 wrappers][c-pagination]
pass C strings to that overload. Do not copy the apparent optional-overload typo
into the Go wire contract or relabel it as testcontainers behavior.

## Comparison semantics and results

[OMAP_CMP's handler][comparison] supports EQ=1, LT=5 and GT=3. NE=2, GTE=4,
LTE=6 and unknown identifiers reach EINVAL in this handler even though the
constants also exist for xattr. There is no xattr comparison mode or numeric
U64 interpretation here. The **stored value is on the left**, caller operand on
the right. Success is 0, not xattr STRING comparison's positive 1. A failing
assertion gives ECANCELED; the first failing assertion in native key order stops
evaluation. A missing key supplies an empty buffer; a missing/whiteout object
has a separate ENOENT check. Thus empty EQ may pass for a missing key on an
existing object, but must not erase the missing-object distinction.

Values are compared through native bufferlist operators. Equality is
length-aware. Ordered comparison iterates char bytes, and the [pinned native
operator][signed-char] uses char while its [build flags][char-build] select
-fsigned-char. In this audited build, high-bit values
therefore have signed-char ordering; do not derive a Go-side unsigned
bytes.Compare result or reuse xattr's string/string_view comparison oracle.
Binary asymmetry such as 0x80 versus 0x7f and equal-prefix/different-length values
must be covered by an independent native execution oracle. This source audit
establishes the required oracle cases, not their live results. Raw communication
need not evaluate comparisons locally at all.

Multiple assertions serialize as key -> (operand bytes, int32 identifier).
Preserve aggregate/per-op codes separately, including FAILOK failures. Do not
infer aggregate results from one assertion, flip LT/GT to reuse an xattr helper,
or expose a typed numeric/storage comparison layer.

## Mutation, completion and admission

The [write handlers][write-handlers] require pool.info.supports_omap(); the
[pool helper][pool-support] returns false for erasure-coded pools. Their refusal
is EOPNOTSUPP. The first proposed profile remains replicated BlueStore. Read
handlers do not all contain that same support gate, so an EC write refusal must
not be advertised as verified behavior for every OMAP read path. Unsupported
pool/map requirements still follow the product's existing explicit rejection
rules; no automatic routing, placement or topology selection is added.

SETVALS/SETHEADER can create a missing head object through
maybe_create_new_object. CLEAR/RMKEYS check missing/whiteout objects and return
ENOENT. Missing keys on an existing BlueStore object can be removed without
individual missing-key results. Empty sets/maps, repeated keys supplied through
an unsafe non-native encoding, clear on an object without OMAP and version/mtime
changes need separate native characterization. Never infer "no metadata change"
from empty input or a backend no-op.

CLEAR is not just deletion of values: [BlueStore's clear implementation][clear]
removes the OMAP range including its header and clears the OMAP flag. Header
preservation after RMKEYS and header removal after CLEAR are acceptance cases.
SETHEADER with zero bytes supplies an empty raw header; it is not CLEAR. None
of these operations changes object byte content by design of the handler, but
object bytes/stat2/version/mtime must still be checked in the independent oracle.

All five read/compare candidates contribute READ; SETVALS, SETHEADER, CLEAR and
RMKEYS contribute WRITE. Preserve RETURNVEC and the existing ONDISK request
policy. Per-op FAILOK is the proposed only allowed flag; EXCL, FADVISE and
unknown flags are rejected before transmission. Read-only success has no
mutation durability claim. Successful vectors containing a write require
ONDISK; known negative refusal is a known server result without that success
requirement. ACK/NVRAM alone does not prove persistence. The OMAP feature bit
in [ceph_features.h][features] is retired; an opcode declaration is not a reason
to widen the negotiated feature mask or reuse MON-only admission exceptions.

Write handlers stage transaction operations; reads consult the backend. First
implementation should admit read/compare-before-write order and conservatively
reject OMAP writes followed by OMAP read/compare in the same vector until native
staged-read characterization exists. Source ordering is not an interoperability
test or a read-your-staged-OMAP guarantee. Failing ordinary guards and FAILOK
need pre/post inventories plus byte/stat2/version/mtime checks. The audited
[failure branch][failure] retains a negative per-op value under FAILOK, but does
not suppress EAGAIN/EINPROGRESS; preserve that distinction.

Permission admission can reject with EPERM before operation execution in
[PrimaryLogPG's capability path][permissions]. Preserve its actual aggregate
and per-op wire metadata; do not assume every operation echoes that aggregate
permission code in its own result. Malformed decoded operation inputs reach
EINVAL in several handlers. Backend failure paths differ: GETKEYS aborts on a
negative iterator result; GETHEADER/BYKEYS ignore the returned store status,
and GETVALS has a local iterator result shadowing the outer operation result
before goto fail. No generic backend-error propagation guarantee follows from
this audit, and no storage corruption/failure experiment is proposed to prove it.

Caller-provided object/pool/namespace/locator/hash, OSD endpoint and nonce,
map epoch/incarnation and client-wide TID remain the routing identity. Keep the
submitted raw OMAP input available when a sent mutation, mixed request, invalid
reply, cancelled wait or lost connection leaves execution uncertain. A fresh
session must not replay SETVALS/SETHEADER/CLEAR/RMKEYS or an uncertain compound.
Local timeout is not remote cancellation or rollback. Preserve raw payloads,
epoch/version metadata and known server refusals independently of local errors.

## Native acceptance plan and current gaps

Current implementation uses `OSDOperation.Omap` / `OSDOmapInput`, with zero
caller extent/Version/Data/Xattr fields and locally derived wire input length.
Local minimum output budgets are five bytes for pages and four for BYKEYS.
Canonical encoding sorts copied requested keys and rejects duplicates before
TID/send. Raw output stays available; strict bounded validation rejects wrong
order/count/cursor/prefix/membership/bool/suffix and echoed union fields.
There is no exported storage map decoder or automatic next-page request.

Five fresh native message triples and Go header/front/data comparisons passed.
Separate CRC/AES host three-BlueStore native/Go reads passed 23 cases on the
first run. Secure/AES256K CGO0 container three-BlueStore then passed the whole
22-test OSD suite, including those 23 read cases. Binary/empty/NUL keys and
empty values are verified through BYKEYS; NUL pagination cursor/prefix inputs
have fresh native codec evidence but no independent live C++ comparison yet.
Actual missing-object C decoder callbacks (-5) and raw Go wire per-op0 differ;
native callback values are not asserted to equal wire results. Detailed
receipts and source-supported interpretation are in the validation record.

Use public ceph-testcontainers-go v0.2.0 fixture/bootstrap/control APIs with the
pinned image and caller-owned placement. Reuse the current single/three-BlueStore
replicated profiles only as recipes; their O1/O2a pass records are not OMAP
results. Record exact image/source, cipher/address/auth profile, replicas/min_size,
OSDMap epoch, explicit primary identity and cleanup for every new run.

| Acceptance area | Proposed independent evidence | Current status |
| --- | --- | --- |
| Canonical wire | Fresh ObjectOperation/MOSDOp/MOSDOpReply encode plus separate native decode, request union input lengths, map/set/assertion encodings, suffix/no-suffix/header raw output, Go byte comparison | Three read opcodes/five fresh triples passed; header/assertion/write vectors pending |
| Bytes and absence | Binary/NUL/non-UTF-8/empty key and value; absent key vs empty value vs absent object; raw header empty/nonempty; exact object bytes/stat2/version/mtime | Binary/empty/NUL BYKEYS and missing object passed with unchanged native state; header unimplemented |
| Pagination | Count 0/1/exact/final/huge, cursor/prefix relations, empty-key boundary, byte cap overshoot, more true/false, single-page raw ordering | 23-case live read matrix passed in two bounded profiles; configured caps and live NUL cursor/prefix comparison pending |
| Comparison | EQ/LT/GT, unsupported IDs, stored-left asymmetry, high-bit signed-char order, missing key/object, multi-assertion order, success0/ECANCELED/raw per-op | Not executed |
| Mutations | SET before independent GET/BYKEYS, SETHEADER/header read, RMKEYS retains header, CLEAR removes header+values, absent keys/object, empty operations and object creation | Not executed |
| Atomicity/FAILOK | Compare-before-write, failed guard rollback, FAILOK then later write; staged writes followed by reads characterized separately | Not executed; first-ordering restriction proposed |
| Limits/permission | Disposable OSD entry cap 2 and small byte cap, local oversize/budget, read-only credential EPERM without state change; restore settings and identity | Recipe proposed, not executed |
| Recovery/uncertainty | ONDISK write then actual primary SIGKILL on size3/min_size2; independently native bytes/OMAP/header/version/mtime; lost response preserves TID/submission and no replay | Not executed |
| Scope separation | Namespace isolation, key order preservation, supported replicated pools; EC/snapshot/range-delete matrices tracked separately | Not executed |

The pinned public C API provides read/write OMAP cmp/cmp2, set/set2,
rm_keys/rm_keys2, clear, get_keys2/get_vals2 with pmore, and BYKEYS/BYKEYS2.
Use length-bearing *2 variants and [rados_omap_get_next2][c-iterator] for binary
keys/values; terminate on a NULL key pointer, not a NULL value or zero value
length. Copy all buffers before iterator end and release every iterator/op.
Per-op prval and aggregate operate results must be captured independently.
A C wrapper that decodes a native iterator does not preserve the original raw
wire map/more bytes; fresh native codec fixtures remain a separate prerequisite.

C pagination cursors/prefixes remain NUL-terminated strings even in get_vals2;
its "2" adds pmore, not cursor lengths. Binary NUL cursor/prefix acceptance
therefore needs a separate C++ or direct Go-vs-server oracle, not an invented
C function signature. The audited public C header/API has no OMAP header
get/set entrypoint; native C++ ObjectOperation/librados or a bounded binary CLI
oracle is required for GETHEADER/SETHEADER and header effects of CLEAR. This
is an oracle gap, not permission to add native product dependencies. The
current prepared C-only xattr oracle cannot be relabeled as that coverage.

## Provenance

This independent contract paraphrases pinned primary source audited locally
and keeps later limited implementation/verification evidence separate from
the remaining proposals.
Objecter, PrimaryLogPG, buffer, transaction, pool and BlueStore sources carry
LGPL-2.1 notices; rados.h permits LGPL-2.1 or LGPL-3.0. [COPYING][license]
records file/default licensing. No Ceph implementation, native runtime or
native transitive dependency is incorporated into the Go product.

[opcodes]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/rados.h#L294
[read-builders]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.h#L953
[write-builders]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.h#L1346
[read-handlers]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L7726
[merge]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/osd_types.h#L4333
[collection-callback]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.h#L672
[transaction-parsers]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/os/Transaction.cc#L18
[store-key]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/os/bluestore/BlueStore.cc#L4871
[limits]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/options/global.yaml.in#L3812
[optional-builder]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.h#L1012
[c-pagination]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/librados/librados_c.cc#L4387
[comparison]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L7879
[signed-char]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/buffer.h#L1275
[char-build]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/CMakeLists.txt#L99
[write-handlers]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L7955
[pool-support]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/osd_types.h#L1787
[clear]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/os/bluestore/BlueStore.cc#L18257
[features]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/ceph_features.h#L100
[failure]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L8197
[permissions]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L2083
[c-iterator]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/rados/librados.h#L1772
[license]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/COPYING
