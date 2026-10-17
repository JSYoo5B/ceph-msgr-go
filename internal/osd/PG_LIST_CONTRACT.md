# Explicit PG namespace listing

PGNLS (`0x1505`) implements one bounded, read-only namespace-aware page on an
explicitly selected replicated PG. It is separate from object request vectors:
`OSDConnection.ListPG` takes the caller's actual folded PG seed, primary OSD,
map epoch, namespace, count, sequence epoch and raw cursor. The product neither
walks a pool nor computes placement, hashes names, refreshes maps or retries.

## Wire contract

The reference is Ceph v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`, specifically
`include/rados.h` (`PGNLS`, `PGOP`, `ceph_osd_op.pgls`),
`osdc/Objecter.h` (`pg_nls`), `messages/MOSDOp.h` (empty-object locator and v6
branch), `common/hobject.{h,cc}`, and `osd/PrimaryLogPG.cc:1261-1445,1642-1649`.
These LGPL-2.1 sources were consulted; no native implementation was copied.

The existing OBJECTLOCATOR/PGID64-only v6 negotiation can carry PGNLS without
NEW_OSDOP_ENCODING, RESEND_ON_SPLIT or additional negotiated bits. The dedicated
request sets READ, ONDISK, RETURNVEC and PGOP (`0x0400`), an empty object, the
head snapshot, no write snap context, zero mtime and retry attempt zero. Empty
objects cause native MOSDOp to encode an explicit locator hash equal to the PG
seed, with object_locator_t v6 compatibility 6. The raw PG seed is already the
actual folded PG seed supplied by the caller. Ordinary object Request refuses
PGNLS, so it cannot accidentally send the PG opcode without PGOP.

The operation union is count (u64), start_epoch (u32), then sixteen zero bytes.
The input is hobject_t v4/compat3. Empty input selects the canonical native
minimum sentinel (pool INT64_MIN, other fields zero/empty). Reply cursors retain
both decoded fields and the exact compatible raw envelope. Caller-supplied raw
cursors are parsed before transmission; the product does not interpret PG
bounds or advance a cursor. Older hobject formats, PG filters and snapshots
are outside this subset.

A successful pg_nls_response_t v1/compat1 contains the next hobject cursor and
an ordered vector of namespace/object/locator strings. Embedded NULs in strings
are preserved. Aggregate result 0 means continuation; aggregate result 1 means
end of this PG. The individual rval remains zero even on an end page. Cursor
Max is a global sentinel and is distinct from End. A page can be empty before
End, for example after namespace filtering. Enumeration is not an atomic
inventory or snapshot. Map changes, mutations and splits can change what a
later page observes; callers own that policy.

Count bounds server entries, while MaxOutputBytes is a local budget, at least
49 bytes for the empty page/current minimum cursor. The request validates both
whole-frame budgets before sending. Reply identity, union echo and reserved
fields, payload length, count, envelope lengths and versions are checked before
exposing a decoded page. A malformed or over-budget reply retires only the
connection and preserves raw bytes/submission as an unknown outcome. Signed
server errors retain their native codes. Redirects are exposed, never followed.

## Evidence and remaining limits

`tools/osd_pg_list_native_encoder.cc` independently constructs one request and
four replies (populated, empty/end, EINVAL, EACCES), directly invokes the pinned
native encoders, then separately decodes each fresh message and pg_nls payload.
Go compares request header/front/data and decodes native reply envelopes and
cursor fields. These offline vectors do not prove daemon execution.

`integration/osd_pg_list_test.go` compares complete inventories reached through
Go count-one pages with pinned `rados --pgid ... --namespace ... --format json
ls`, which independently uses native PGNLS. It checks default/isolated/empty
namespaces, raw cursor progression, explicit target identity and page bounds.
The CLI's complete inventory is compared; native and Go page boundaries are
not claimed identical. This test is opt-in and its presence is not live proof.
Validation records must identify the executed fixture/profile and result.

All-namespaces `"\x01"` is supported by the raw namespace field, but the CLI
oracle deliberately admits one explicit namespace only. Nonempty locator-key
objects, multiple actual PGs, map/split changes between pages, authorization
changes and EC remain unverified by this initial live test. There is no watch,
notification, backoff, autonomous pool iterator or snapshot inventory in this
implementation.

The first CRC/AES host three-BlueStore execution passed all four PG listing
subcases (six default-namespace entries, one isolated entry and an empty
namespace, plus local target rejection). Its top-level test failed during
cleanup because deferred Close ran before the isolated-object deletion through
an alias of the same cached OSD connection. Cleanup ordering was corrected so
the deletion precedes every connection Close; this first run is not recorded as
a full pass. The second CRC/AES host three-BlueStore execution independently
passed all four subcases and the complete PG test including cleanup (3.42s);
it retained six/one/zero entries in seven count-one Go pages per namespace.
The combined run still failed its separate watch/notify test, so this is a
PG-specific result rather than a whole-suite pass.

## Filtered listing (PGNLS_FILTER)

`OSDPGListRequest.Filter` sends PGNLS_FILTER (0x1506) instead of PGNLS. As
Objecter's add_pgls_filter does, the operation's indata is the strings `pg`
and `filter`, the filter name, the filter's raw input and then the cursor; the
operation union, flags, routing and reply handling are those of PGNLS, and the
reply must echo the filtered opcode. The OSD (PrimaryLogPG do_pg_op,
get_pgls_filter) builds the filter from the name: `plain` is built in
(osd_types.cc PGLSPlainFilter: keep objects whose attribute key equals the
value byte for byte, drop objects without it), any other name must be
`<class>.<filter>` from an object class it may load. Objects the filter drops
still advance the cursor, so a filtered page can be empty before the PG's end.
`OSDPlainPGListFilter` encodes the plain filter's input (attribute key, then
value, each length-prefixed); the key is the OSD's attribute key, `_` + name
for a user xattr. The OSD reads the filter input and the cursor from one
stream, so the client cannot detect input that consumes cursor bytes; it only
bounds the name (1-256 bytes, no NUL) and the frame.

Live ([record](../../integration/fixture/OSD_PG_LIST_FILTER_VALIDATION.md)):
the plain filter and cls_hello's `hello.hello` class filter returned exactly
the natively observed matches; a class
outside `osd_class_load_list` is refused with -EPERM (-1), a missing filter of a
loadable class and input running past the end with -EINVAL (-22).
