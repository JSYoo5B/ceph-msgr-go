# Tentacle backoff transport and identity order

Reference: Ceph `v20.2.4`, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.
This codec is independently implemented from the following LGPL-2.1 sources;
no upstream C++ implementation is copied into the Go product:

- `messages/MOSDBackoff.h:22-73`: message 61, header v1/compat1, current payload.
- `osd/osd_types.h` (`spg_t::encode/decode`): struct v1/compat1 containing pg_t
  v1 and signed one-byte shard_id_t. `include/types.h:554-577` defines the shard.
- `common/hobject.cc:110-162`: current hobject v4/compat3 field encoding,
  maximum sentinel, and older minimum-sentinel canonicalization.
- `common/hobject.h:276-303,349-368`: canonical bitwise object comparison.
- `include/rados.h`: BLOCK 1, ACK_BLOCK 2, UNBLOCK 3.
- `osd/PG.cc:570-657`: session-local IDs and original block bounds; unblock can
  carry a newer map epoch than its corresponding block.
- `osdc/Objecter.cc:3835-3920`: current-connection lookup and original-epoch ACK.

## Wire behavior

`Backoff` preserves actual PG/shard, map epoch, unsigned connection-local ID,
opcode, begin/end hobject identities and owned raw front bytes. The nested
`PGCursor` type supplies the existing exact raw hobject envelope and decoded
locator key, object name, namespace, snapshot, hash, maximum marker and signed
pool. The PG seed denotes an actual mapped PG; it is not the raw object hash.
The codec retains all signed shard byte values and full uint64 IDs, including
zero. This preserves wire identity and does not assert product support for EC
or every possible identity.

The front contains `spg_t`, u32 epoch, u8 opcode, u64 ID, begin hobject, end
hobject. No middle or data is permitted. The message header and nested structs
must match the current pinned encodings exactly. Future compatible hobject
fields are refused because their effects on gate ordering need independent
verification. Malformed booleans, current-envelope suffixes, unsupported
opcodes, truncated/oversized fields, reversed bounds and noncanonical maximum
sentinels are rejected. The old `pool=-1` minimum alias is refused rather than
sorted as a current bound: native canonicalizes that alias before comparison,
and preserving its raw signed pool as an ordinary current object would be wrong.

All input and output are bounded by the configured frame budget, including the
41-byte message header. Decode owns `RawFront`; cursor Raw slices view that
owned buffer. Encode verifies raw cursor bytes and their typed projection agree.
It preserves the original cursor envelopes without silently substituting another
object. Callers must not mutate event metadata while acknowledging it.

`CompareBackoffObjects` follows native order: maximum marker, signed pool,
bit-reversed hash, unsigned namespace bytes, effective locator key, object name,
then unsigned snapshot. An empty locator key uses the object name. Adjacent
numeric hashes are not adjacent in this order. `BackoffContains` implements
`[begin,end)`; equal bounds denote one canonical object. It compares identities
only: the session, actual PG and shard must be matched separately by the caller.

`EncodeBackoffAck` only accepts BLOCK and changes only its opcode. It preserves
the original epoch, PG/shard, ID and bounds, with priority 127. A later local map
epoch must not replace the block epoch: the OSD uses it to discard stale ACKs
after a split. The receiver must install a send restriction before sending the
protocol acknowledgement on that same authenticated connection.

## Integration boundary

The codec itself does not advertise features, install gates or submit work.
The integrated opt-ins `OSDProfileReplicatedPG` and
`OSDProfileReplicatedPGReplyV8` select the
fixed OSD mask with HASHPSPOOL, NEW_OSDOP_ENCODING and the incarnation-aware
bit-21 RESEND_ON_SPLIT/RADOS_BACKOFF/PG_UPMAP/CHOOSE_ARGS contract. Both profiles
are selected before Dial; their MON sessions additionally declare HASHPSPOOL.
ReplyV8 additionally requires OSD bit58 and a strict v8 reply decoder; it shares
the same backoff queue, explicit ACK and single-writer gate.
The default `OSDProfileObjectHash` keeps the restricted v6 behavior and rejects
type 61. There is no raw feature-mask override. The [O4 contract](O4_CONTRACT.md)
records the implemented profile and its restricted native acceptance.

The session dispatcher identifies type 61 independently of pending operation
TIDs, even if a backoff header TID collides with a pending request. The event is
bound to the authenticated OSD session generation. Connection replacement drops
its gates; old IDs cannot control a new session. BLOCK/UNBLOCK establish no
execution outcome for work already sent. Native Objecter's automatic resend on
UNBLOCK must not be copied: current project policy keeps replay and routing
with the caller. Source admission is repeated before publication so an identity
change during decoding cannot install an old session's gate.

`NextBackoff(ctx)` exposes controls without sending ACK or applying maps. Queued
events have a 64-entry and conservative `MaxFrameSize` byte bound. The retained
set has the same independent bounds, shared by active restrictions and released
pending-ACK tombstones. Overflow fails the connection; accepted events drain
before its stable terminal error. Identical duplicate BLOCKs retain the same
receipt. Reused retained IDs with different identities/epochs fail the session.
Unknown UNBLOCKs remain observable with Matched=false.

`AckBackoff(ctx, block)` uses a private receipt tied to the original session and
event; changing public metadata cannot select a different ACK. It preserves
the original epoch even when UNBLOCK or a newer map arrived. Duplicate queued
ACKs and foreign/replaced receipts are refused. ctx controls queue admission
only: after success, caller cancellation cannot discard the queued protocol
message. Queue success proves neither full transmission nor remote receipt.

The installed `BeforeWrite` barrier checks each queued, unstarted type-42
request against the retained restrictions on its authenticated connection.
Only matching actual PG/shard and head hobject ranges return ErrOSDBackoff;
unrelated objects and PGs can proceed on the same connection. For an
erasure-coded PG every shard matches, because a request's shard may be encoded
in the pre-Tentacle primary-first numbering (`OSDPrimaryFirst`) while BLOCK
names the OSD's actual shard; the gate over-refuses rather than letting a
blocked object through. Local refusal
is a known unsent outcome, not an unknown-execution claim. Already started
work retains the normal reply/context/session-failure semantics. No request is
automatically replayed after UNBLOCK. The control ACK progresses on the same
ordered writer, outside the ordinary application-call admission slots; a
previously started frame finishes first.

The gate extracts identity from the library's encoded v8 MOSDOp, with bounded
current envelopes, locator/PG consistency and structural-tail checks. This
extractor is not a general native request decoder or operation validator. It
uses the caller's actual PG/shard and raw hash, locator pool/namespace/effective
key and object name, with CEPH_NOSNAP as the head identity even for snapshot
reads, as Objecter::op_target_t::get_hobj does. Empty PGNLS object names retain
their own native head identity. No OSDMap folding, placement or feature
inference is performed. Unsupported or malformed local encodings fail closed
before transmission. At most 64 retained restrictions are scanned; range
comparison uses signed pool, bit-reversed raw hash and native hobject ordering.
The request epoch does not bypass an existing restriction.

UNBLOCK before explicit ACK is a normal supported ordering. Pinned
`PG::release_backoffs` leaves a NEW server entry in STATE_DELETING until ACK
(`osd/PG.cc:645-655`, `osd/Session.cc:67-70`). The client consequently keeps
the original receipt and gate after that UNBLOCK. ACK queue admission alone
does not open the gate: a matching request already queued ahead of ACK must
still be refused; unrelated requests need not wait for that ACK. At ACK's actual turn, the single-writer hook retires a
released tombstone. Later object frames cannot overtake that ACK. If ACK has
already reached its writer turn when UNBLOCK arrives, UNBLOCK can retire the
restriction directly. Writer failure stops the session instead of releasing
later work onto an incomplete ACK stream.

Context cancellation, Close, concurrent controls, source replacement and first
terminal-cause preservation have synthetic coverage. UNBLOCK-before-ACK
tombstones retain the same range restriction until the ACK's writer turn.
This is neither placement calculation nor automatic resend policy.

## Verification assets

`backoff_test.go` exercises bit-reversed hashes, signed pool and binary string
ordering, effective keys, snapshots, equal-bound single-object semantics,
half-open endpoints, raw ownership, exact frame budgets, original-epoch ACK
bytes and malformed/truncated controls.

`tools/osd_backoff_native_encoder.cc` constructs five fresh native messages:
BLOCK single binary object, BLOCK range, original-epoch ACK_BLOCK, newer-epoch
UNBLOCK, and a sharded PG with minimum/maximum bounds. Every native message is
separately decoded and its full identity checked; decoded objects are never
re-encoded. The native Message envelope and explicit offline v2 header projection
are serialized independently. `TestBackoffMatchesFreshNativeEncoder` compares
Go header/front bytes and decoded fields with those outputs. Explicit sequence
1 and ACK sequence 3 are offline inputs and prove no live session behavior.
The independent Python build driver is adapted from this repository's
allocation-hint development driver and keeps that older driver's provenance
unchanged. It checks control message version 1, pinned sources, matching native
library and loader identity.

The [retained native proof](testdata/backoff-native-provenance.json) and
[reproduction record](testdata/backoff-native-README.md) contain the five fresh
vectors built on 2026-10-15, with matching native library loader audit and
confirmed builder termination. All five native Go byte comparisons passed.
Dedicated control provenance tests and the existing operation/watch16
provenance tests also passed.

Native wire vectors are distinct from actual OSD execution. Initial secure
aes256k IPv6 single-MemStore acceptance observed a real BLOCK and UNBLOCK,
explicit ACK queue submission, local unsent APPEND refusal, a canceled sent
STAT with unknown outcome, and independent native readback proving the later
caller-selected append occurred once. That initial log alone did not establish
remote ACK reception. Final native normal/late-ACK runs both passed on CRC/AES,
IPv4 host/single BlueStore and secure/aes256k, IPv6 container/single MemStore.
Each used an eight-PG HASHPSPOOL pool and the controlled legacy CRUSH profile.
The observer matched completed-operation history against client entity/type 8,
client GlobalID, OSD ID, actual PG, original epoch, connection-local ID,
ACK_BLOCK opcode 2 and reached_pg. The late-ACK cases also completed a fresh
append exactly once and a read on the same connection after UNBLOCK and late
ACK. All four cases and both fixture cleanup/export paths passed.

Deterministic API/session regressions cover UNBLOCK before ACK, bounded pending
receipts, cancellation after ACK queue admission, and an ordinary object queued
before late ACK followed by a fresh object. The first is refused, then ACK and
the fresh frame reach the wire in that order. Native single-object blocks,
split/merge, primary changes and broader profile combinations remain separate
acceptance requirements. No newer reply bit or EC behavior is enabled by these
tests.
