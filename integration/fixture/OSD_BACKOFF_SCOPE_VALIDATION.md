# Range-scoped OSD backoff admission — 2026-10-16

Reference: Ceph Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Product code remains native Go
with no CGO, Ceph CLI, librados, go-ceph or native runtime dependency.
Native clients, daemon controls and containers are development-only.

The later [failure-observation budget record](OSD_BACKOFF_FAILURE_BUDGET_VALIDATION.md)
preserves separate amd64 CI failures and the fixture correction. The local
first-attempt results below remain their original, narrower evidence.

## Change and protocol boundary

The earlier [object-backoff acceptance](OSD_OBJECT_BACKOFF_VALIDATION.md)
recorded a real product limitation: one object BLOCK refused every new
MOSDOp on that authenticated connection. This change restricts the local
writer gate to the daemon's actual PG/shard and hobject range, allowing
unrelated work to progress on the same connection. It adds no routing,
waiting scheduler or automatic retry/replay.

`Objecter::op_target_t::get_hobj` (Objecter.h:1893-1905) uses the locator
pool/namespace/effective key, object name and raw hash, always with
CEPH_NOSNAP. Actual mapped PG/shard and the read snapshot are separate
fields. The gate follows those identities: snapshot reads use the head
hobject, and PGNLS keeps its native empty-object identity rather than its
page cursor. Half-open ranges and equal-bound single objects use native
bitwise hobject ordering. The request's map epoch does not bypass a BLOCK.

`DecodeRequestV8AdmissionTarget` is a bounded identity projection from
trusted locally encoded requests, not a general native request decoder or
operation validator. Unsupported envelopes, inconsistent locator/PG pools
and truncated structural tails fail closed before transmission. No feature
mask, map or CRUSH exception was added. Both replicated request profiles
use the same gate; their reply codecs remain independent.

ACK uses the retained original epoch and private receipt. UNBLOCK before
ACK retains only the original range restriction until ACK reaches the
single writer. Matching queued work ahead of that ACK stays unsent;
unrelated work may pass. Already-started frames and sent requests keep
their original outcome rules. The original BLOCK-triggering append is not
automatically resubmitted.

## Synthetic and build checks

The new decoder is compared directly with six retained independent pinned
native encodings: read, mutation, snapshot, checksum, shard and PGNLS.
Every proper truncation of the read/PGNLS front is refused. Changed
headers/envelopes/pools/shards/limits are checked separately. These are the
existing fresh C++ encoder assets and provenance, not Go encoder round trips.

Focused normal and race checks cover actual PG/pool/shard, raw hash,
namespace/effective key, snapshot head, half-open endpoints, PGNLS identity,
malformed local messages and both authenticated replicated profiles. The
late-ACK ordering test admits unrelated work during a released-but-unacked
range while still refusing matching work, then sends a fresh matching
request only after the original ACK's writer turn. Shared Session logic
was not changed.

Whole-product `CGO_ENABLED=0 go test ./...` and `go vet ./...` passed.
The focused race checks passed for internal/osd, cephmsgr and
internal/session. The public package also cross-compiled with CGO disabled
for Windows/amd64. Native default opt-in compilation/skip is separate from
the live acceptance below.

## Live acceptance shape

The selected test is `TestCephOSDObjectBackoffFailoverIntegration`, using
fixture v0.3.0, three BlueStore replicas and the replicated-PG v8 request/v6
reply profile. It prefers two native-mapped objects X/Y in the same PG and
primary; the receipt records the actual routes and whether they share a PG.
A fallback to the same primary never counts as same-PG evidence.

While recovery is held by pausing the stale replica, the primary sends an
object BLOCK for X. On the exact Go handle that received and explicitly
ACKed it, Y STAT and READ must complete with native size/mtime/version and
bytes. A fresh X STAT must return ErrOSDBackoff as known unsent, preserving
its allocated submission TID but no wire reply TID/front and no unknown
outcome. The original X append must remain unresolved. An independent
native snapshot confirms the Y reads changed no bytes/version/mtime.

The existing cancellation/UNBLOCK/explicit single append and primary-SIGKILL
phases remain: a sent unknown mutation is never automatically retried, and
a separate caller-selected request on the new primary executes once.
This is not a proof that a durable Y write can finish while its replica is
paused; this change's live concurrency proof uses STAT and READ.

## Live results

Both first attempts passed against the immutable image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
These are local native acceptance results, separate from remote CI.

| Configuration | Selected test | Y STAT / READ | Same actual PG | Result |
| --- | --- | --- | --- | --- |
| CRC / AES client and service, IPv4 host | 42.41 s | 0.561 / 0.475 ms | 2.7 | PASS |
| secure / AES256K client and service, IPv6 container | 41.16 s | 0.238 / 0.188 ms | 2.7 | PASS |

In both receipts X and Y use the exact same primary address, authenticated
GlobalID and connection generation. Y STAT/READ use TIDs 2/3 and match the
independent native bytes, version and mtime; its before/after snapshots are
identical. The matching X refusal retains submission TID 4 but has wire
reply TID 0, an empty reply front and no unknown-outcome marker. The
original X append stays pending until the caller cancels its local wait.
The retained original-epoch ACK agrees with native daemon receipt history.

Both original sent-mutation cancellation and in-flight primary SIGKILL
preserve unknown outcomes. Native readback contains no implicitly replayed
Go tail; each explicit fresh append subsequently increments the native
version once, reaches ONDISK and adds exactly one tail. The native
failover moves CRC primary 2 to 1 and secure primary 1 to 0 with newer map
epochs. Each run restores the changed configuration, leaves all OSDs up/in
and PGs active+clean, removes the nine owned objects and snapshots, and
terminates all ten fixture-owned containers. `restored` and `cleaned` are
true with empty `cleanup_failures` in both receipts. An independent audit
checked the raw receipts and matched each owned container to its
termination log.

Local retained artifact roots (not runtime or repository dependencies):

`osd-object-backoff-results.json` SHA-256:

- CRC: `9a580e34c5be2cc7f6fd1141fb9a11f6075911fd8e4bfed6f3e23837035eefa2`.
- Secure: `a365bf96ab204fcd7f71c5c06af50d786716608eb51f22ef53507460ef39e2da`.

`osd-object-backoff-state.json` SHA-256:

- CRC: `4c549db6030d75d2993437f8f55e90dcd7ecd504fb12a2a6af7c529a895ec9d3`.
- Secure: `16b33afb037581bbfbbf3115ccd676e750e7d46b661e07a097c76d5a0a9c92c9`.

Frozen acceptance source SHA-256:
`89a6fca02e138431544f99ef1b43e4ba12d7d2c639391b62732c20ba1ab473c4`.
Frozen request identity decoder:
`dd651ef84be9a1b98a5eb8bc9e908bf5b3dae6d38e83596e29944a1c8c11e606`.
Frozen backoff gate:
`4492158baf7f0ba8fe552697468a1ebd754f984f62c4757a5006d1a1dec2a355`.

## Limits

Actual non-equal range BLOCKs, multiple simultaneous native object BLOCKs,
EC pools and live three-replica reply-v8 remain outside this acceptance;
non-equal bounds and bounded retained restrictions have synthetic coverage.
Native Objecter waits/resends matching operations; this client preserves
known-unsent refusal and requires caller decisions. The scope correction
does not change that policy or claim all native scheduling semantics.
