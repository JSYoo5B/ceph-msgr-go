# Applied EC mutation: explicit resubmission after primary loss

This is a narrow follow-up to [EC in-flight acceptance](OSD_ERASURE_VALIDATION.md)
and issue #9. It exercises a deliberate caller decision after independently
observed application. The product does not decide to repeat an uncertain
mutation, compute placement or reconnect the failed OSD connection.

## Source and profile

The first native experiment tested
`bb573535fe0c0a892fbccaa85b1f300e4831720f`, attempt 1, on Linux/amd64.
Subsequent test changes also require nonzero nanosecond mtime, all xattrs,
the original GlobalID and a bounded completed resubmission; they are separate
from this initial receipt and must pass the final PR revision's native gate.

- Ceph v20.2.4, source `7f793731f1b39eb4f465e960113d2363c311b964`.
- Image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
  The fixture manifest observed that digest and the matching daemon version.
- Fixture provider `github.com/JSYoo5B/ceph-testcontainers-go v0.3.0`.
- Secure, aes256k client and service keys, IPv4, container test runtime.
- Three MONs, two MGRs and four BlueStore OSDs. The owned pool used isa
  k=2,m=2, `allow_ec_overwrites`, `allow_ec_optimizations`, size 4, min_size 3
  and eight PGs. The fixture's bootstrap replicated pools retained size 3.
- `replicated-pg-v8`: request v8/reply v6, without SERVER_TENTACLE. The native
  map supplies actual primary/shard and primary-first renumbering facts.

The selected parent is
`TestCephOSDErasureOptimizedK2M2AppliedResubmitIntegration`. Its only child
must execute: `applied mutation resubmitted once with fresh transaction control`.
The parent has no optional child skip. The trusted result gate requires its
actual API-suite PASS; an empty secondary client suite supplies no evidence.

## Experiment and observed results

Seed one owned object, independently read its native bytes/version/mtime, then
submit one non-idempotent APPEND plus SETXATTR. The connection wrapper withholds
reads after admission. Native readback must observe the appended bytes before
the original primary is SIGKILLed. Canceling the local wait preserves its
`OutcomeUnknownError` and original submission identity. This is an applied
but locally unanswered request, not proof that the server was still processing
it at death.

The first experiment observed:

| Observation | Result |
| --- | --- |
| Original placement | Pool 3, PG 3.1, epoch 29, acting `[0,2,3,1]`, primary osd.0, actual shard 0 |
| Native pre-state | User version 1; mtime 1700005000 |
| Original application | One append, xattr `ec-inflight` = bytes `00 ff 49`, user version 2, mtime 1700005001 |
| Caller result | `OutcomeUnknownError` from local cancellation; TID 1, incarnation 7, original epoch/placement retained |
| Replacement placement | Epoch 32, primary osd.3, actual shard 2, acting `[2147483647,2,3,1]`, primary-first pg_temp; pool size 4/nonprimary count 1 maps actual shard 2 to wire shard 1 |
| Explicit resubmission | One caller invocation, same client/TID/incarnation, RetryAttempt 1; aggregate 0, flags `0x400002c` including ONDISK, user version 2, reply attempt 1 |
| Native duplicate check | Identical one-tail bytes, xattr and mtime; user version stayed 2 |
| Positive control | Fresh TID 2 appended the same tail again; two tails, user version 3, mtime 1700005002 |
| Recovery | Killed OSD restarted, PGs clean; epoch 36, primary osd.0/shard 0, same final native bytes/xattr/version/mtime |

The resubmission completed 3.62 seconds after the first call began. The test
limits this experiment to a three-minute observation window; that limit is
not a guarantee that a PG log retains arbitrary requests for three minutes.
The semantic-input SHA-256 excludes changed map/placement/retry fields and
must match across the two caller invocations. It is not a captured-wire hash.

The original error remains unknown after both native observation and the
later successful reply. No loop retries a refused or uncertain resubmission.
The fresh-TID control makes the independent byte/version oracle sensitive to
a real second execution. Connection counters separately confirm that the
product did not open a replacement OSD connection before the explicit caller
decision; they are not server-execution counters.

The API parent passed in 20.96 seconds and its child in 11.76 seconds. Complete
fixture execution, including build/startup and termination, took 97 seconds.
The owned EC pool and profile were removed. All 11 ownership-recorded containers
and the owned network were independently absent after teardown; no unowned
resource was removed. Separate Linux checks passed 635 Python tests, the
77-row mandatory catalog, CGO-free product build/vet, focused EC-fact race
tests, fixture unit tests and fixture vet.

## Reproduction and final-revision gate

On an authorized Linux runner, use the existing fixture run/check locks and:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes256k \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes256k \
CEPH_MSGR_TEST_CONNECTION_MODE=secure \
CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=container \
CEPH_MSGR_TEST_OSD=1 \
CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=4 \
CEPH_MSGR_TEST_OSD_PROFILE=replicated-pg-v8 \
CEPH_MSGR_TEST_RUN='^TestCephOSDErasureOptimizedK2M2AppliedResubmitIntegration$' \
sh integration/run.sh
```

The dedicated mandatory scenario is
`osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2appliedresubmit`.
The PR's implemented comment selector requests that row at its captured head.
Basic CI, the initial native receipt above and final-head native CI are
different evidence; the PR records their exact SHAs, run IDs and attempts.

## Limits

This proves duplicate handling only for one retained applied request on the
selected profile. It does not establish universal exactly-once execution,
PG-log retention bounds, requests held inside the dying OSD, other EC widths,
signed refusal after the epoch change or direct recovered-shard bytes. With
three shards remaining, k=2 is sufficient without using the returned shard.
The work adds no product API, automatic replay, routing or feature advertisement.
