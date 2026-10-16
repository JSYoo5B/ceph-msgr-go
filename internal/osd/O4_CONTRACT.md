# O4: caller-placed replicated sessions

Reference: Ceph `v20.2.4`, commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The cited Ceph sources carry LGPL-2.1
notices. This is an independent protocol description; no native implementation
is copied. Native tools remain development dependencies.

Status, 2026-10-15: the opt-in public profile, v8 request codecs, actual-PG reply
validation and bounded BACKOFF dispatcher/writer gate are implemented. The
local native runs described below passed. This is partial
O4 acceptance, not general replicated-pool, default-CRUSH or split-recovery
compatibility. Implementation, synthetic tests, offline vectors, local native
runs and completed CI are distinct evidence states.

## Public profile and caller placement

Select the profile before `Dial`, because MON admission precedes OSD target
selection and HASHPSPOOL can be required cluster-wide:

```go
options.EnableOSD = true
options.OSDProfile = cephmsgr.OSDProfileReplicatedPG
client, err := cephmsgr.Dial(ctx, options)
// Handle err and close client using the caller's lifetime policy.

connection, err := client.OpenOSD(ctx, cephmsgr.OSDTarget{
    ID: callerPrimary, Address: callerEndpoint,
})
// Handle err before using connection.
request.Placement = &cephmsgr.OSDReplicatedPlacement{
    PG: cephmsgr.OSDPG{Pool: uint64(request.Object.Pool), Seed: callerActualPG},
    Primary: callerPrimary,
}
reply, err := connection.Request(ctx, request)
```

The snippet assumes caller-owned context, routing values, options and request;
it is a shape example, not an autonomous routing helper. `Options.OSDProfile`
is a closed enum, with no public raw-feature-mask override:

| Value | Request contract | Placement |
| --- | --- | --- |
| `OSDProfileObjectHash` (zero/default) | Existing restricted Tentacle v6 | Must be nil; `Object.Hash` is the raw hash |
| `OSDProfileReplicatedPG` | Tentacle v8 object/PG requests and v6 replies | Mandatory per object request; actual PG is distinct from raw hash |
| `OSDProfileReplicatedPGReplyV8` | Same v8 requests with strict v8/compat2 replies and required OSD bit58 | Same explicit replicated placement |
| `OSDProfileReplicatedPGRequestV9ReplyV8` | Untraced v9/compat3 requests and strict v8/compat2 replies; required OSD SERVER_SQUID49/57 | Same explicit replicated placement and NO_SHARD; no nonempty OTEL input |

The third profile is detailed in [the v8 reply contract](OSD_REPLY_V8_CONTRACT.md).
The fourth is detailed in [the untraced v9 contract](REQUEST_V9_CONTRACT.md);
its native acceptance status is tracked separately from the earlier profiles.
It is selected before Dial and does not change either prior profile.
An unknown enum fails locally; selecting either replicated profile without
EnableOSD fails before network activity. The profile applies to this Client's
MON and OSD sessions and cannot be changed on an existing client. `OSDTarget`
continues to identify only the OSD ID/address. There is no target-level Profile.

`OSDReplicatedPlacement.PG.Pool` must match the nonnegative object pool, and
Primary must equal the connection target ID. A pointer distinguishes absent
placement from valid PG seed zero. The public profile fixes replicated shard
identity to native NO_SHARD (-1). It exposes no arbitrary EC shard override.
Requests and retained watch/notify handles copy the placement metadata. Callers
must keep their request inputs stable while the operation is in progress.

`Object.Hash` stays the raw object hash. `MapEpoch` describes the caller's
selected placement tuple. The library does not fold hashes, evaluate CRUSH,
apply maps, select primaries, refresh epochs, follow redirects or replay work.
The same profile selects private WATCH/NOTIFY requests and dedicated PGNLS;
ListPG already carries explicit actual PG and primary metadata.

`OSDReply.PG` and object-request `OSDSubmission.Placement` preserve actual PG
identity in the replicated profile. Default object-hash replies expose the raw
hash in their PG seed. Submission identity also retains TID, authenticated
GlobalID, client incarnation, chosen epoch and attempt zero.

## Fixed feature policy and wire

`include/ceph_features.h:114-118` defines RESEND_ON_SPLIT, RADOS_BACKOFF,
OSDMAP_PG_UPMAP, CRUSH_CHOOSE_ARGS and SERVER_LUMINOUS on the same incarnation-2
mask `(1<<21)|(1<<57)`. They cannot be enabled independently. The replicated
OSD profile adds bits 21, 30 (OSDHASHPSPOOL) and 56 (NEW_OSDOP_ENCODING) to the
existing OBJECTLOCATOR/PGID64/common transport set and requires the complete
selected contract from its peer. Bit 57 was already a common incarnation bit.

The opt-in MON session additionally advertises/requires HASHPSPOOL, bit 30.
This follows the selected client-wide pool capability; it is separate from the
existing narrow MON-only CRUSH admission exception. OSD sessions do not inherit
that exception. Other required feature bits still fail negotiation explicitly.
No feature bit identifies the release family or proves CRUSH placement support.

The previously verified `OSDProfileReplicatedPG` retains legacy CRUSH.
Ordinary optimal CRUSH can require
CRUSH_TUNABLES5, which shares bit 58 with NEW_OSDOPREPLY_ENCODING
(`ceph_features.h:178-179`). That bit remains omitted from that v6-reply profile; default optimal CRUSH,
MSR, EC and other unimplemented requirements are not admitted by it. The new
ReplyV8 profile additionally requires OSD bit58 for its audited reply decoder;
this does not establish default-CRUSH/map topology compatibility. Its native
acceptance is separate from the older runs documented below.
OSD admission policy derives requirements from all pools/CRUSH/upmaps, including
`.mgr`, through `OSD::check_osdmap_features` and `OSDMap::get_features(TYPE_CLIENT)`
(`osd/OSD.cc:8806-8826`, `osd/OSDMap.cc:1754-1848`).

Bit 21 cannot be added to the old v6 encoder alone. Without bit 56 native
`MOSDOp` still selects v6 and transmits raw PG/hash; bit-21 server dispatch
instead treats `get_spg()` as actual PG. See `messages/MOSDOp.h:313-395` and
`osd/OSD.cc:7618-7632,7729-7746`. The implemented v8/compat3 layout separates:

1. Actual `spg_t` v1/compat1 (nested pg_t and signed shard -1).
2. Raw hobject hash u32, map epoch u32, flags u32 and osd_reqid_t.
3. Three-u64 native trace, client incarnation, mtime, locator and object name.
4. Operation vector, snapshots, retry attempt zero and negotiated features.

The default reqid preserves client incarnation even when entity/TID use native
server derivation. This differs from the legacy v6 restoration branch. Bit 58
remains independent: `MOSDOpReply` selects v6/compat2 without it, but its PG now
comes from the actual request PG (`messages/MOSDOpReply.h:195-224`). Both codecs
bound complete request and anticipated reply frames before transmission. The
internal signed-shard codec is a framing primitive, not public EC support.

## BACKOFF admission and outcome semantics

`MOSDBackoff` is type 61, header v1/compat1: actual spg_t, map epoch u32,
opcode u8, connection-local ID u64 and begin/end hobject envelopes. BLOCK=1,
ACK_BLOCK=2, UNBLOCK=3. `[begin,end)` is a range; equality denotes one object.
The canonical order is max marker, signed pool, bit-reversed hash, namespace,
effective locator key, object name and snapshot (`common/hobject.h:349-365`).
The codecs preserve raw bytes and current exact nested versions.

`NextBackoff(ctx)` exposes controls with authenticated target, GlobalID and
connection generation. Source admission is repeated before publication.
Receiving/dequeuing a control sends no ACK and never applies a map or retries a
request. `AckBackoff(ctx, block)` explicitly queues the original epoch/PG/ID/
range on the same session and request priority. A private receipt prevents
public event metadata from selecting a different ACK. Successful return proves
local queue admission, not daemon receipt or processing.

The queue and retained controls each have a 64-entry and conservative
MaxFrameSize byte bound. Active restrictions and released pending-ACK receipts
share that retained-control budget. Overflow fails the session; accepted events drain before its stable
terminal error. A valid active BLOCK gates only MOSDOps with the same actual
PG/shard and a head hobject inside its range. Unrelated objects and PGs proceed
on the same connection. Released pending-ACK receipts retain that same scoped
restriction until the original ACK reaches its writer turn; see the
[backoff contract](BACKOFF_CONTRACT.md) for identity and ordering details.

The session's single writer checks the gate immediately before starting an
ordinary message. Blocked, unstarted work fails locally with ErrOSDBackoff,
without a sequence number or an uncertain-execution claim. ACK/control work
continues. A frame that already started before BLOCK must finish before the
queued ACK. This ordering prevents matching object work after ACK while a
restriction remains active; native `Session::check_backoff` can assert on an
ignored acknowledged restriction (`osd/Session.cc:88-104`).

UNBLOCK exposes whether it matches a locally observed BLOCK. If the original
ACK has reached its writer turn, the restriction can be removed. Otherwise its
receipt remains as a bounded tombstone and retains the conservative gate until
the writer reaches that ACK. No sent or locally refused request is automatically
submitted again. Already sent work can remain pending until its ordinary reply,
context, Close or session failure. A canceled sent call preserves OutcomeUnknown;
a subsequent operation is a separate caller decision with separate identity.
No automatic reconnect, redirect, retry or mutation replay is implemented.

Native release of an unacknowledged BLOCK retains a server STATE_DELETING
record until its ACK arrives (`osd/PG.cc:645-655`, `osd/Session.cc:67-70`). The
implementation therefore permits the original late ACK after UNBLOCK. ACK
queue admission leaves the gate closed, so an ordinary request already ahead
of that ACK is refused instead of overtaking it. The ACK's own single-writer
hook retires a released tombstone; later frames follow the ACK in wire order.
`AckBackoff` contexts govern queue admission only. After successful admission,
caller cancellation cannot withdraw that protocol message. Session failure
still stops transmission, and successful queueing is not remote receipt.
Deterministic synthetic regressions cover this ordering, context cancellation
after ACK queue admission, receipt identity and the shared tombstone budget.

Split/force-resend handling is also incomplete. The pinned server can silently
drop a request older than same_primary_since, the pool force-resend epoch or
last_epoch_split (`osd/PG.cc:1934-1976`). No signed not-executed result is
promised. Raw map delivery and context-bounded unknown outcomes preserve this
uncertainty; new-map receipt is not permission to replay. Callers must apply maps
and obtain fresh placement before a separately chosen operation. There is no
implemented automatic split detection or routing-invalidation result.

## Fixture and observed evidence

The fixture selector is `CEPH_MSGR_TEST_OSD_PROFILE=replicated-pg-v8` together
with `CEPH_MSGR_TEST_OSD=1`. The default `legacy-v6` recipe remains separate.
The v8 recipe uses eight replicated data PGs and HASHPSPOOL from global
bootstrap, including automatic `.mgr` pools. It retains legacy straw CRUSH,
disables CRUSH updates and the balancer, and does not enable EC.
[Fixture preparation](../../integration/fixture/OSD_REPLICATED_PG_V8_FIXTURE.md)
defines its admission assertions. The restricted oracle is not weakened for the
new variant. PG upmap/items are preserved only in the opt-in observer; upmap
primaries and newer CRUSH settings remain rejected and are not live-verified.

Independent native metadata already provides raw_pgid, actual pgid/hash/pg_seed,
epoch, primary, endpoint/nonce and acting/up sets in osd-oracle.json. The
mutation oracle obtains fresh per-object/namespace placement. Go integration
consumes these observations; the product computes none of them.

The following local run logs were inspected on 2026-10-15:

| Profile | Observed result | Evidence boundary |
| --- | --- | --- |
| CRC/AES, IPv4 host, three BlueStore OSDs, eight PGs | Final PGNLS plus base-I/O selection passed eight subcases | Native complete namespace inventories matched count-one v8 cursor pages; explicit-primary rejection and five base-I/O cases passed |
| secure/aes256k, IPv6 container, one MemStore OSD, eight PGs | Final PGNLS plus base-I/O selection passed eight subcases | Native complete inventories matched pages, including an empty continuation page; explicit-primary rejection and five base-I/O cases passed |
| CRC/AES, IPv4 host, one BlueStore OSD, eight PGs | Normal ACK and late ACK after UNBLOCK both passed | Normal PG 2.4 epochs 21/22; late PG 2.7 epochs 15/16; ID 1, independent original-epoch ACK reception, one append and same-session read |
| secure/aes256k, IPv6 container, one MemStore OSD, eight PGs | Normal ACK and late ACK after UNBLOCK both passed | Normal PG 2.0 epochs 21/22; late PG 2.0 epochs 15/16; ID 1, independent original-epoch ACK reception, one append and same-session read |

These are local run results, not completed remote CI for the final feature
commit. Final PGNLS/base-I/O logs are
`/private/tmp/ceph-osd-replicated-pg-list-v8-crc-aes-final.log` and
`/private/tmp/ceph-osd-replicated-pg-list-v8-secure-aes256k-ipv6-final.log`.
Final normal/late ACK logs are `/private/tmp/ceph-osd-backoff-crc-aes-final.log`
and `/private/tmp/ceph-osd-backoff-secure-aes256k-ipv6-final.log`; their fixtures
also completed cleanup/export. Completed native operation history matched the
client entity/type 8, client GlobalID, OSD, actual PG, original epoch, ID,
ACK_BLOCK opcode 2 and reached_pg event. This establishes attributed reception
and PG delivery; it is not direct inspection of internal server ACKED state.

Fresh native v8 request vectors and native BACKOFF vectors support the offline
wire comparison. Synthetic tests separately cover fixed-profile negotiation,
placement validation, source replacement, gate/ACK ordering, controls sharing a
pending TID, cancellation, bounds, Close and no replay. These do not establish primary-change or every BACKOFF ordering.

The subsequent [actual split/merge acceptance](../../integration/fixture/OSD_SPLIT_VALIDATION.md)
passed one case each on CRC/AES single BlueStore and secure/AES256K IPv6 single
MemStore. It holds an independently observed mutation's reply across an actual
8→16 PG split, preserves two uncertain submissions' original identities,
attributes a stale parent envelope in native history, and issues separate
caller-placed child I/O before restoring all actual/target/pending PG counts to
eight and cleaning owned heads. Native envelope reception and an unchanged
snapshot do not establish nonexecution; no uncertain mutation is replayed.
This remains selected single-OSD evidence, with no automatic split handling.

Remaining native acceptance includes single-object bounds, primary changes,
upmap placement, all operation families
on the final profile, ticket renewal and broader security/topology combinations.
The distinct `OSDProfileReplicatedPGReplyV8` now activates bit58 only for its
strict OSD v8 reply contract. Its [actual validation](../../integration/fixture/OSD_REPLY_V8_VALIDATION.md)
used legacy CRUSH/HASHPSPOOL/eight PGs with one OSD: CRC/AES host IPv4 BlueStore
and secure/AES256K container IPv6 MemStore. Thirteen existing cases passed per
configuration in earlier runs whose overall result failed a new test's incorrect
zero-version expectation. With product code unchanged, the corrected three new
cases passed separately in focused runs (2.78s CRC; 3.45s secure). No combined
sixteen-case all-pass run is claimed. Failed replies can retain nonzero raw
legacy/replay/user versions and earlier output; those values and negative-reply
ONDISK flags do not establish commit or nonexecution. Default optimal CRUSH/EC
compatibility and broader final-profile coverage remain unverified.
