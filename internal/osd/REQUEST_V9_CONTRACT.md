# Untraced request v9 and reply v8 profile

Reference: Ceph Tentacle v20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. This checkpoint implements the Go
codec and fixed public profile. Independent native v9 vectors and daemon
acceptance are separate requirements; no new live configuration is claimed
by the synthetic tests below.

`OSDProfileReplicatedPGRequestV9ReplyV8` must be selected before Dial and requires
EnableOSD. Requests use type 42, version 9, compat 3. Replies use only the
existing strict type 43, version 8, compat 2 decoder, including raw signed
aggregate/per-operation results, completion flags, redirects and three-u64
trace metadata. It never falls back to another wire version.

## Wire scope

The request prefix is the existing spg_t v1/compat1, separate raw hash, epoch,
flags, reqid v2/compat2 and empty three-u64 legacy trace. It then adds the native
OTEL envelope before client_inc: version 1, compat 1, little-endian body length
1, is_valid=false. These seven bytes are `01 01 01 00 00 00 00`. The remaining
locator, object, opcode unions, data, snapshots, retry attempt zero and feature
tail have the same semantics as v8. The extra seven bytes are included in the
whole-frame preallocation budget; existing request and expected-output limits
remain enforced.

The public request types introduce no trace argument. Nonempty OTEL input,
span generation, trace state, sampling/export policy and an OpenTelemetry
runtime are not provided. This is the canonical untraced encoding, rather
than an invented truncated valid context. The old three-u64 reply trace remains
raw wire metadata and is independent of the new request envelope.

`V9Request` and `EncodeRequestV9` accept only NO_SHARD (-1). Actual PG and raw
hash remain caller-supplied and independent; public placement must name this
connection's primary and the same pool. No CRUSH calculation or object routing
is performed. `EncodePGListRequestV9` handles dedicated head-only PGNLS: empty
oid, the explicit-hash locator variant, PG seed as raw hash, and the caller's
opaque bounded cursor. It is not a pool scan or split-retry policy.

The outgoing backoff hook selects `DecodeRequestV9AdmissionTarget` only for
this profile. It validates type/version/compat, the empty OTEL envelope and
structural tail before projecting actual PG/shard and the head hobject. Snapshot
reads retain head backoff identity; PGNLS retains empty oid and its explicit
hash. A v8 request, nonempty/unknown/malformed trace, bad length or truncated
front fails closed instead of bypassing an active range. v8 profiles continue
using their strict v8 admission projection.

BLOCK, UNBLOCK, explicit original-epoch ACK, late-ACK tombstones and queue order
are unchanged. Unrelated objects/PGs can progress under a block; matching
requests fail locally before write. UNBLOCK does not replay them. Session loss,
started cancellation or Close preserve the existing OutcomeUnknownError and
submission identity. Redirects do not cause automatic follow or resubmission.

## Features and source provenance

OSD role 4 supported and required masks are the existing ReplyV8 profile plus
the full SERVER_SQUID incarnation-2 mask (49|57): bits
8, 9, 21, 30, 49, 56, 57 and 58. Missing any required capability fails setup.
The request codec itself needs 8/9/21/49/56/57; the public profile additionally
requires HASHPSPOOL 30 and reply encoding 58. Existing v6/v8 profiles do not
gain bit 49 or change their codecs. MON retains the existing replicated-profile
HASHPSPOOL admission bit 30; SERVER_SQUID is not copied to MON, MGR or MDS.
CRUSH_MSR (38|57) is separate and remains withheld. This profile establishes no
new placement algorithm, general CRUSH topology, erasure-coded I/O or daemon
server role.

Pinned source consulted without copying C++ implementation:

- `src/messages/MOSDOp.h:398–427,436–445`: v9 field order and two-stage decode;
  header HEAD_VERSION 9 and COMPAT_VERSION 3.
- `src/msg/Message.cc:1026–1071`: fixed legacy trace and OTEL dispatch.
- `src/common/tracer.h`: canonical !HAVE_JAEGER empty encoder; the enabled
  encoder's valid form instead has 26 body bytes (true, trace ID16, span ID8,
  flags1), which this untraced profile does not emit or accept as local input.
- `src/include/ceph_features.h:161,259,263` and `src/msg/Policy.h:43–49`:
  SERVER_SQUID is in native supported defaults regardless of HAVE_JAEGER.
- `src/osdc/Objecter.cc:3389–3417`: reqid, placement and optional trace input.
- `src/messages/MOSDOpReply.h:195–225`: reply v8 selection and legacy trace;
  SERVER_SQUID adds no reply OTEL envelope.

Ceph source is LGPL-2.1. The Go product has no new module dependency, CGO,
Ceph library, subprocess or trace SDK requirement. The current native development
cache uses official WITH_JAEGER=OFF and generated HAVE_JAEGER undef. That can
prove untraced native v9, but cannot prove valid nonempty contexts. Any future
valid-context work requires a genuine pinned SDK-enabled native build rather
than altering fallback classes or mixing SDK-enabled headers with an OFF library.

## Verification boundary

Product unit/synthetic tests cover the seven-byte insertion and bounds, strict
feature masks and role isolation, actual outgoing Request/ListPG envelopes,
signed results/FAILOK/completion and no replay, strict reply selection, head
backoff identity, PGNLS explicit-hash projection, matching/unrelated ranges,
late ACK ordering, truncated/malformed/nonempty traces and rejection of older
request headers by the v9 gate. Retained v8 native bytes support unchanged
body regressions; those projections are not called fresh v9 native proof.

Native vectors, exact tested configurations and first-failure records must be
added in a separate validation record once those executions have occurred.
