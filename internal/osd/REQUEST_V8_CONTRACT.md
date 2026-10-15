# Explicit actual-PG request framing

`EncodeRequestV8` is an internal, independently selectable MOSDOp v8 encoder.
Public Request selects it only after `Options.OSDProfile=OSDProfileReplicatedPG`
is chosen before Dial. The encoder itself advertises no features; the fixed
profile combines it with explicit placement and the bounded backoff writer gate.
See [O4 contract](O4_CONTRACT.md) and [native acceptance](../../integration/fixture/OSD_REPLICATED_PG_VALIDATION.md).

The pinned reference is Ceph v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`: `messages/MOSDOp.h:368-397,446-454,582-614`,
`osd/osd_types.h` (`spg_t`, `osd_reqid_t`), `include/ceph_features.h` and
`msg/Message.cc:1026-1034`. These LGPL-2.1 definitions were consulted, without
copying native implementation code.

The encoder requires OBJECTLOCATOR, PGID64, NEW_OSDOP_ENCODING (bit 56), and the
complete RESEND_ON_SPLIT mask (bit 21 plus incarnation-2 marker bit 57). The
same bit 21 also denotes RADOS_BACKOFF, SERVER_LUMINOUS, OSDMAP_PG_UPMAP and
CRUSH_CHOOSE_ARGS; this framing primitive alone does not implement that combined
feature contract. Advertising bit 21 while retaining v6 would be incorrect:
v6 transmits raw object hash where modern native peers would interpret the
placement group as actual. Backoff gates, map/CRUSH requirements and tested
placement configurations must be handled before enabling the shared feature.

`V8Request` embeds the existing bounded object request and adds a caller-selected
actual PG and signed shard byte. Pool identities must agree. The replicated
shard is explicitly -1 (NO_SHARD); nonnegative shard values are accepted as
raw framing metadata only, with no claim of EC execution or EC result support.
Raw object hash stays in `Request.Hash` and is encoded independently of the
actual PG seed. No hashing, folding, placement or target selection occurs.

The v8 prefix is spg_t v1/compat1, raw hash, epoch, mode flags, osd_reqid_t
v2/compat2 and an empty three-u64 native trace. Body fields remain client
incarnation, mtime, locator, object, operation unions, snapshot context, attempt
zero and negotiated features. Unlike v6, default reqid.inc must equal the
client incarnation; the daemon derives the authenticated entity and header TID
without losing that incarnation. The encoder refuses SERVER_SQUID's full mask
(which selects v9 and its additional OpenTelemetry trace), NEW_OSDOPREPLY_ENCODING,
and incomplete incarnation masks. It performs no automatic resend, retry,
redirect or map refresh despite the feature's historical name.

Input and reply budgets, operation validation, snapshots and unknown-operation
refusals are equivalent to the existing bounded object contract. The v8 front
requires 23 additional bytes over v6 for the same logical request. Both request
and expected reply whole-frame budgets are checked before allocation.

`tools/osd_request_v8_native_encoder.{cc,py}` independently construct five fresh
native requests: stat/read, mutation, snapshot write context, CRC32C checksum,
and an explicit raw shard. Native encode_payload is invoked directly, and a
separate native object executes both partial decode and finish_decode. The
native receipt checks actual PG, raw hash, shard, client incarnation and derived
request identity. Go compares native request header/front/data. These offline
vectors establish wire parity only; they do not prove daemon I/O, replicated
routing, EC behavior, backoff handling, split recovery or release compatibility.
