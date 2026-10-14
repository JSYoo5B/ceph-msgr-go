# Tentacle O2a bounded xattr contract

Reference: Ceph 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.
Source audit and limited implementation: 2026-10-15. GET/GETALL, STRING CMP,
SET and RM are implemented after contract review and fresh native codec proof.
[OSD_XATTR_VALIDATION.md](../../integration/fixture/OSD_XATTR_VALIDATION.md)
separates native-only preparation from subsequent Go/native interoperability:
43 subcases passed in a secure/aes256k IPv4 three-BlueStore profile. This does
not complete the broader O2 milestone or the remaining acceptance matrix.
[CONTRACT.md](CONTRACT.md) supplies existing v6 object identity, per-op result,
ONDISK, cancellation and unknown-outcome rules. Snapshot contexts, omap,
class calls, allocation hints, rollback and watch remain separate later work.

## Source-backed operation subset

[The opcode list and packed union][rados] define these xattr operations:

| Opcode | Value | O2a implementation | Input data / union | Output |
| --- | --- | --- | --- | --- |
| GETXATTR | 0x1301 | Include | Raw name bytes; name_len set, value_len/comparison fields zero | Raw value bytes; reply value_len becomes output length |
| GETXATTRS | 0x1302 | Include | No input; entire union zero | Native encoded name-to-value map, not concatenated raw values |
| CMPXATTR | 0x1303 | Include string mode first; U64 only after its own oracle | Raw name then comparison operand; both lengths and comparison op/mode set | No output bytes; signed per-op result preserved, including positive true result |
| SETXATTR | 0x2301 | Include | Raw name then value; both lengths set, comparison fields zero | No output bytes |
| SETXATTRS | 0x2302 | Exclude | Declared opcode does not establish a supported server path | No verified handler |
| RESETXATTRS | 0x2303 | Exclude | Declared opcode does not establish a supported server path | No verified handler |
| RMXATTR | 0x2304 | Include | Raw name; name_len set, value_len/comparison fields zero | No output bytes |

Each operation still has the 38-byte packed envelope: opcode u16, flags u32,
28-byte union and payload_len u32. The [xattr union][xattr-union] occupies its first ten bytes:
name_len u32, value_len u32, comparison op u8, comparison mode u8; the remaining
18 bytes must be zero in this implemented subset. These are little-endian lengths,
not extent offsets. Input data is raw `name || value`, with no trailing NUL,
name prefix, terminator or length prefix inside that operation's data segment.
The whole vector concatenates each operation's input in order.
[Objecter's builders][builders] establish this encoding independently of Go.

[PrimaryLogPG][read-handlers] implements GETXATTR, GETXATTRS and CMPXATTR;
[its mutation handlers][write-handlers] implement SETXATTR and RMXATTR.
SETXATTRS/RESETXATTRS do not appear in its operation dispatch; its
[default branch][failure] returns `-EOPNOTSUPP`. This is a source audit,
not an observed native invocation of those unsupported opcodes. A similarly
named native builder is insufficient evidence of server semantics.

## Names, values and server bounds

The implementation requires a **nonempty, NUL-free name** before transmission.
Go string storage can preserve byte sequences including non-UTF-8 bytes;
there is no Unicode normalization, trimming, lowercasing or inferred text
encoding. Values are arbitrary bytes, including empty, NUL and non-UTF-8.
Nil and an empty value both encode length zero, never attribute deletion.

The server prepends one underscore to user names. Empty input therefore maps
to `_`, which is [OI_ATTR, the internal object-info attribute][internal-attrs].
The audited handlers have no explicit empty-name rejection. GET/CMP could
expose that internal attr and SET/RM could alias its write path. This is a
source-derived hazard, not a native corruption experiment or a claim of every
server outcome; do not send those mutations merely to demonstrate the risk.
[IoCtxImpl entrypoints][ioctx] and the C wrapper directly build these operations
without an empty-name check visible in that path. They are not a substitute
for product validation. GETXATTRS [filters storage attrs][filter] to keys longer
than one byte beginning with `_`, removes exactly one prefix byte and excludes
internal `_`; it consequently does not make the empty-name hazard safe.

[BlueStore attr operations][bluestore] use `name.c_str()` for attr lookup,
set and removal. An embedded NUL can truncate/alias a name even though the
Messenger union carries a length. Reject every NUL in the submitted name,
not just a leading one. A user name `_foo` becomes storage name `__foo` and is
returned as `_foo`; do not reject or strip that user prefix as if it were the
internal object-info name. Native acceptance for non-UTF-8 names remains to be
recorded; raw received map bytes must not be rewritten meanwhile.

SETXATTR [checks][write-handlers] value_len against `osd_max_attr_size` only
when that configuration is positive (`-EFBIG`), and name_len against the
minimum of store maximum and `osd_max_attr_name_len` (`-ENAMETOOLONG`). The
[default configuration][limits] is attr size 0 (no configured size cap) and
name length 100. [BlueStore's store bound][store-bound] is 256. These are
server/profile facts, not portable constants to hardcode into the Go encoder.
Client limits must bound allocation/wire size while allowing the server to
return its configured rejection. Actual lengths, not rune counts, govern.

## Comparison semantics

[Comparison op/mode constants][comparison-ids] are EQ 1, NE 2, GT 3, GTE 4,
LT 5, LTE 6; STRING 1 and U64 2. Reject unknown op/mode locally and require
zero comparison fields on non-CMP operations. This implementation does not
support arbitrary packed-union flags or fall back to a different mode.

[The comparison helpers][comparison] compare the **caller operand on the left**
against the stored value on the right. In STRING mode the name "string" does
not impose text semantics: C++ string/string_view use length-aware byte
comparison, including NUL values. False becomes `-ECANCELED`; true produces
positive 1, retained as the per-op result. Do not rewrite that to zero, swap
GT/LT, or infer aggregate results from one operation. The existing Go API
already treats only negative aggregate codes as server errors.

U64 input is exactly one little-endian wire u64 after the name. Stored xattr
bytes are parsed as decimal text, not decoded as a stored little-endian u64.
The helper treats empty/missing attr bytes as zero, rejects numeric parse
errors/overflow with `-EINVAL`, and checks `from_chars`' error but not its
final pointer. Acceptance must therefore explicitly characterize numeric
prefixes such as `19suffix`, whitespace/signs, embedded NUL and overflow; no
Go conversion of the stored bytes should conceal the native result.

CMP permits `-ENODATA`/`-EEXIST` from attr lookup to reach comparison with the
empty buffer; a missing attr can satisfy equality with an empty STRING operand
or U64 zero. A missing object is a separate outer admission/lookup error.
Test these identities separately rather than promising all missing attrs fail.
The C [read/write comparison wrappers][c-comparison] always select STRING mode;
U64 needs a native C++ ObjectOperation or equivalent independently verified
oracle, not an invented C API mode argument.

## Proposed Go communication fields and results

The public ordered operation has a value variant `Xattr OSDXattrInput` with
`Name string`, `Comparison OSDXattrComparison` and local `MaxOutputBytes uint32`.
Existing `Data []byte` carries the raw SET value or CMP operand; lengths are
derived from admitted bytes. CMP selects STRING mode explicitly; no public
numeric mode or fallback exists. GETXATTRS accepts no name/operand. Extent and
version inputs must be zero, and non-xattr opcodes require the zero variant.
`OSDOperationResult.Xattr` exposes raw NameLength/ValueLength and comparison
op/mode separately; it does not fabricate returned names or extent offsets.

A positive caller budget is needed for GETXATTR/GETXATTRS because the request
has no remote output-size field. Bound each output and the aggregate vector
budget within MaxFrameSize before admission; validate actual output afterward.
This local budget does not request pagination or compel the OSD to truncate.
Oversized received frames/results terminate the session with the existing
limit/error and sent-outcome rules, including mixed-vector mutation uncertainty.
All u32 lengths, header/data aggregation and sums require overflow-safe checks.
Preserve the existing 1–64 operation bound and copy admitted input bytes.

Return the raw xattr union fields separately from extent metadata; never expose
name_len/value_len as fabricated byte offsets. GETXATTR legitimately normalizes
value_len to received value length, unlike SET/CMP/RM echo validation. Reply
payload boundaries and opcode/flags must still match the submitted vector.
GETXATTRS keeps the encoded native map in per-op `Data` and the original reply
data segment: u32 count followed by length-prefixed name and value bytes in
native map order. Optional later wire decoding may expose ordered raw byte
entries with bounded lengths/counts, but must retain the original encoding.
No typed storage map, key normalization or xattr policy belongs to this layer.
The submitted name is local input identity; the reply union echoes lengths,
not independently returned name bytes. Label any retained name accordingly.

For all included operations accept only the existing per-op FAILOK flag;
EXCL and unknown/FADVISE flags are rejected before transmission. GET/GETALL/CMP
contribute READ; SET/RM contribute WRITE. Keep RETURNVEC and the current
ONDISK request behavior. Read/compare-only success does not require mutation
ONDISK. Any successful vector containing SET/RM does; negative known server
refusals remain known results and do not require ONDISK. ACK/NVRAM alone never
establish metadata durability; MemStore does not establish restart persistence.

Caller-owned pool/namespace/locator/object/hash, explicit OSD endpoint/nonce,
map epoch, incarnation and client-wide TID remain unchanged. No internal
placement, name-based routing, map application, redirect following or retry
is introduced. Retain server codes, per-op codes/output, epochs/versions and
local submission when cancellation, malformed output or transport failure
leaves execution uncertain. No SET/RM/compound is automatically resubmitted.

## Ordered vector semantics

The native transaction remains one object vector, with staged writes discarded
on an ordinary failing guard. FAILOK retains the negative per-op result while
allowing the vector to continue; [its server branch][failure] does not suppress
`-EAGAIN` or `-EINPROGRESS`. Preserve both aggregate and per-op codes.

Do not promise generic read-your-staged-xattrs. In the replicated path the
SET/RM handlers write the transaction while [getattr/getattrs][filter] access
the existing object backend; no immediate staged attr read is established by
those handlers. SET→GET, SET→CMP, RM→GET and RM→GETALL need independent native
characterization before acceptance. This is source-derived uncertainty, not an
observed failed native vector. Compare-before-write guard vectors are the
first supported ordering. A successful SET can create a previously
missing head object through `maybe_create_new_object`; creation/version/mtime
and unrelated object bytes must be checked independently. Missing-attr RM on
an existing BlueStore object is a native no-op in the audited backend; missing
object, permission refusal and native store errors remain distinct cases.

## Native fixture and acceptance prerequisites

Add test-only oracle functions, reusing the existing disposable testcontainers
fixture and explicit routing/native stat/read controls:

- Prepare isolated objects plus baseline attr inventories, bytes, stat2 version
  and nanosecond mtime; record exact Ceph/image, pool/replica/CRUSH/cipher/address
  profile and caller placement. Keep test object/attr names separate per oracle.
- Native binary set/get/remove using librados length parameters and bounded
  buffers. List via `rados_getxattrs` iterator, terminating on NULL name, not
  NULL value: an empty value legitimately has NULL pointer and length zero.
  Copy explicit lengths with binary-safe buffers; `ctypes.c_char_p`/C strings
  must not truncate values at NUL. [The C iterator][c-iterator] defines this.
- Native atomic read/write compound including CMP/SET/RM and per-op result
  handling; expose operator/mode explicitly and use C++ for U64. Existing
  native write-op callbacks may not provide every raw per-op wire result;
  report that gap rather than fabricating the Go result from native readback.
- Independent fresh MOSDOp/MOSDOpReply native payload encoder/decoder vectors
  for each included union/result, mixed outputs and normalized GET value_len.
  Re-exporting retained decoded payload does not prove native encoding.
- Bounded test configuration recipe for attr name/size limits and read-only
  credentials; preserve known refusal, restore configuration, and independently
  verify container/network cleanup. No product CLI/librados/CGO dependency.

| Case | Required native/Go comparison | Current status |
| --- | --- | --- |
| Binary values and empty value | NUL/0xff/newline payload; length-zero vs removed; set/get/list exact bytes and no unrelated object-content change | Limited Go/native profile and fresh wire vectors passed |
| Names and namespace | Normal, leading underscore, non-UTF-8 names; local rejection of empty/NUL names before TID/send; different object namespace isolation | Byte-name profile and pre-send rejection passed; namespace isolation pending; no unsafe alias mutation performed |
| Length limits | Configured name limit and value-size cap yield native `-ENAMETOOLONG`/`-EFBIG`; local frame/budget limits reject before send, received oversize retains uncertainty | CRC/AES three-BlueStore six-case native/Go 32/33 boundaries and staged-SET rollback passed; all-owned effective settings and exact prior entry/absence restored with public v0.2.0 handles |
| Compare operators | Caller-left asymmetric GT/LT, all six ops, binary/empty STRING, true positive result, false `-ECANCELED`; missing attr vs missing object | Go/native matrix 36 cases and missing identities passed; actual Go per-op +1 preserved, C callback still unavailable |
| U64 followup | LE operand 0/max; stored decimal, empty/missing, prefix, bad signs/whitespace/NUL, overflow; invalid input length/mode locally rejected | Unsupported; separate oracle prerequisite |
| Atomicity and FAILOK | Compare-before-SET/RM success; false guard after an earlier staged mutation restores pre-state; FAILOK permits later writes and retains per-op failure | Go/native guard-before-write and FAILOK passed; staged SET-before-CMP remains rejected and native-only rollback evidence |
| Mixed requests | Xattr GET+READ+STAT output boundaries; SET/RM+data mutation followed by independent fresh reads/stat2; staged xattr reads characterized separately | Fresh vectors and GET/GETALL/READ/STAT passed; one APPEND+SET actual native execution/readback passed; staged xattr reads pending |
| Refusal and uncertainty | Permission rejection vs missing object/attr; sent append+xattr vector loses response, retains identity/outcome, fresh session never replays it | CRC/AES three-BlueStore APPEND+SET receive withholding/Close passed; CRC/AES256K single-MemStore readonly GET/GETALL and known SET/RM EPERM passed with unchanged native state; raw Go per-op0 and echoed flags retained, same session remains usable |
| Completion and recovery | SET/RM ONDISK then native inventory/version/mtime after primary SIGKILL on BlueStore; read-only result has no durability claim | Secure AES256K three-BlueStore APPEND+SET+RM ONDISK followed by actual-primary SIGKILL and two-replica native/Go inventory/bytes/version/mtime passed; hardware power loss unverified |

The subsequent native-only oracle passed binary/empty/prefixed inventory,
compound SET/RM, 36 STRING comparison cases, rollback and FAILOK in one
CRC/AES IPv4 three-BlueStore host profile. The table above remains the full
Go/native acceptance checklist, not a claim that this limited C API result
completed it. Fresh xattr wire vectors independently verify the Go codecs;
the subsequent live Go replies preserve raw positive CMP results. The C API
callback still cannot expose those per-op values. Limited Go/native
acceptance subsequently passed binary/empty/non-UTF8 names, 36 read/write
STRING comparison vectors, guard-before-SET/RM failures and FAILOK, missing
attr/object identities and mixed GET/GETALL/READ/STAT. One sent APPEND+SET
withheld-reply/Close test independently observed execution, unknown local
completion and fresh-session no-replay. A separate secure AES256K
three-BlueStore APPEND+SET+RM vector survived actual-primary SIGKILL with
unchanged native/Go metadata on two replicas. Public scoped configuration
and all-owned effective gates verified name/value limits and rollback after
an earlier SET. Readonly native/Go SET/RM return EPERM with unchanged state;
their raw Go per-op0 and echoed ONDISK do not establish execution. Namespace
matrices remain pending;
the OSD feature mask has not widened. Existing O1 results cannot be relabeled
as this new matrix.
Broader pool/EC/snapshot and staged-read semantics remain excluded
until their own contracts and independent acceptance are complete.

## Provenance

This independently written contract uses pinned primary source only. The
Objecter, PrimaryLogPG, IoCtxImpl, librados implementation and BlueStore sources
carry LGPL-2.1 notices; rados.h allows LGPL-2.1 or LGPL-3.0. Repository
[COPYING][license] supplies file/default notices. No source implementation or
native dependency is incorporated into the Go product.

[rados]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/rados.h#L341
[xattr-union]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/rados.h#L594
[builders]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.h#L190
[read-handlers]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L6423
[write-handlers]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L7562
[failure]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L8197
[internal-attrs]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/osd_types.h#L5977
[ioctx]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/librados/IoCtxImpl.cc#L1522
[filter]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L15993
[bluestore]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/os/bluestore/BlueStore.cc#L18162
[limits]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/options/global.yaml.in#L3803
[store-bound]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/os/bluestore/BlueStore.h#L3211
[comparison-ids]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/rados.h#L503
[comparison]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L5004
[c-comparison]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/librados/librados_c.cc#L3670
[c-iterator]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/librados/librados_c.cc#L2015
[license]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/COPYING
