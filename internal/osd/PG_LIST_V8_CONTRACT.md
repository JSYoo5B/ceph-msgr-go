# Explicit PGNLS through the v8 request envelope

Reference: Ceph v20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`.
Independently implemented from LGPL-2.1 source; no upstream implementation copied.
This extends the existing [PGNLS payload contract](PG_LIST_CONTRACT.md) with a
separate `EncodePGListRequestV8`, without changing the generic object-vector
encoder or the legacy PGNLS encoder.

## Source and identity

- `osdc/Objecter.h:3144-3212` prepares an empty-object PG read and caller-supplied
  precalculated PG metadata. `op_target_t::get_hobj` selects the explicit locator
  hash when present, otherwise the raw PG hash.
- `osdc/Objecter.cc:2997-3027,3098-3125,3388-3392` keeps the raw PG and actual
  folded PG separate. The native client applies its map before constructing
  MOSDOp; this product never performs that placement step.
- `messages/MOSDOp.h:129-134` uses the empty object's raw hash in the explicit-hash
  object_locator_t variant. Its v8 branch (`:369-399`) writes actual spg_t,
  separate raw hash, epoch, flags, reqid, trace and the remaining request body.
- `msg/Message.cc:1026-1035` always writes the three-u64 trace.

The dedicated public request already requires an actual folded PG from the
caller. This codec uses that explicit seed for both the actual PG and the
separate raw-hash slot, with NO_SHARD (-1), empty object and empty locator key.
It does not infer either from the continuation cursor. The locator hash is the
same seed and selects object_locator_t v6/compat6, including for seed zero.
That is the native empty-object exception to the normal named-object locator
with hash -1/compat3. General native Objecter enumeration may start with a raw
hash and fold it through its map; reproducing that placement algorithm remains
outside this dedicated explicit-target API.

The v8 reqid retains ClientInc, including nonzero/nondefault values. Its entity
and TID fields are default so the daemon derives authenticated identity/header
TID. The separate client_inc has the same value. The trace is three zero u64s,
read-only mtime is zero, object name is empty, the vector has one PGNLS and PGOP,
and the head snapshot/empty write SnapContext/attempt zero are unchanged.
StartEpoch and the exact raw cursor stay caller-controlled protocol metadata.

Frame admission includes the 23-byte increase over v6: 250 fixed bytes including
the header, plus namespace and cursor bytes. Reply admission still includes the
conservative ordinary v6 reply envelope and requested local output budget
(the v8 no-redirect envelope is smaller). No cursor count,
allocation or input copy escapes those bounds. Unsupported feature combinations,
SERVER_SQUID/v9 and malformed cursors are refused. The fixed ReplyV8 profile
permits NEW_OSDOPREPLY_ENCODING bit58 in the negotiated-feature tail and selects
its strict v8 reply decoder; the v6 baseline vectors below remain unchanged.

## Native evidence and integration boundary

[Retained native provenance](testdata/pg-list-v8-native-provenance.json) records
five freshly constructed messages: one v8 PGNLS request and four actual native
v6 replies (populated, empty/end, EINVAL and EACCES). Names retain both versions:
`pg-list-v8-native-request-v8-list` and `pg-list-v8-native-reply-v6-*`.
The separate reply version is the pinned encoder's actual selection, not a
relabeled request envelope. Reply pg_t preserves the request's actual PG.

The independent wrapper and own Python driver leave all existing v6 drivers
and provenance unchanged. Native decoding uses both decode_payload and
finish_decode for the v8 request, verifies actual PG/raw hash/shard/incarnation,
and verifies the empty object's explicit locator hash. Native reply decoding
separately checks every union/result/output. Decoded objects are never re-encoded.
The builder was terminated; its matching native library loader audit, source,
configuration, binary, driver and per-segment hashes are retained in the manifest.

Go tests passed native header/front/data parity and all four reply decodes,
feature/input rejection, exact request/reply byte budgets, cursor ownership,
raw-cursor continuation without target replacement, and explicit locator/reqid
fields. Dedicated harness tests preserve mixed v8-request/v6-reply proof classes
alongside existing v6, watch, backoff and all-request-v8 evidence checks.
Public ListPG selects this encoder for either replicated-PG profile without
new input fields; the runs below used the original v6-reply profile. Default and isolated namespace inventories match count-one Go
pages in both CRC/AES host IPv4 with three BlueStore replicas and secure/AES256K
IPv6 container with one MemStore. Wrong-primary requests fail locally.
See [native acceptance](../../integration/fixture/OSD_REPLICATED_PG_VALIDATION.md).
Split recovery, automatic resend, pool traversal and placement remain caller
responsibilities or separately unverified behavior.
