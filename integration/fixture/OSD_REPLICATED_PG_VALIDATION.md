# Explicit replicated PG and backoff acceptance — 2026-10-15

The fixed `Options.OSDProfile=OSDProfileReplicatedPG` is selected before Dial
with `EnableOSD=true`. MON admission adds HASHPSPOOL for this selection; OSD
negotiation adds the fixed actual-PG/v8-request/backoff bundle. The default
object-hash profile and MON CRUSH admission exception remain separate. No
arbitrary feature option, placement calculation, automatic routing or operation
replay is introduced.

## Scope and native wire evidence

All native evidence uses Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964` and image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The daemons are Linux arm64. Fixture dependencies remain in the development
module using provider v0.2.0; product builds remain native Go with CGO=0.

Fresh native construction and separate native decoding establish five vectors
for each of these contracts:

- v8 object requests: stat/read, mutation, snapshot context, checksum and raw
  shard framing. Actual PG and raw hash, incarnation and native request identity
  are independently checked. Raw shard framing does not establish EC support.
- v8 PGNLS: one v8 request and four actual v6 replies, preserving the empty
  object's explicit locator hash and opaque continuation cursor.
- Backoff: BLOCK single-object/range, original-epoch ACK, newer-epoch UNBLOCK
  and raw sharded bounds. These five were retained in the preceding checkpoint.
- Reply v8: success, ENOENT, arbitrary signed refusal, mutation completion and
  redirect. This is a standalone decoder foundation only. Bit 58 is not
  advertised and public reply selection remains v6.

Each suite has an independent build driver and stored source/configuration,
native library/binary, wrapper/driver and segment hashes. Native Message headers
use explicit offline sequence/ACK inputs; those projections are not daemon
execution. Builders were terminated and the shared build cache was released.

## Actual daemon profiles

Every profile uses legacy CRUSH, HASHPSPOOL and eight replicated data-pool PGs.
Caller metadata comes from independent native maps. The product supplies no
hash folding or CRUSH algorithm. The fixture refuses newer CRUSH requirements
and primary upmaps; no default optimal-CRUSH or EC compatibility is claimed.

| Profile | Selected cases | Result |
| --- | --- | --- |
| Darwin arm64 host, IPv4 relay, CRC/AES, three BlueStore replicas | Object I/O 5 + PGNLS 3 | 8/8 passed; no skips; cleanup completed |
| CGO=0 Linux arm64 container, IPv6, secure/AES256K, one MemStore | Object I/O 5 + PGNLS 3 | 8/8 passed; no skips; cleanup completed |
| Darwin arm64 host, IPv4 relay, CRC/AES, one BlueStore | Normal ACK + ACK after UNBLOCK | 2/2 passed; cleanup/export completed |
| CGO=0 Linux arm64 container, IPv6, secure/AES256K, one MemStore | Normal ACK + ACK after UNBLOCK | 2/2 passed; cleanup/export completed |

Object cases compare stat/read with native bytes, mtime and user version, then
select an owned nonzero actual PG distinct from the raw hash for write/readback.
Wrong primary, wrong pool and missing placement are locally refused without a
transaction. Withheld replies test cancellation and Close after a native
exactly-once append; OutcomeUnknown, TID and selected placement survive, with
no additional OSD dial or replay. A caller-selected new session reads the
independent native result.

PGNLS compares count-one pages with the native complete inventory for a selected
actual PG in default and isolated namespaces. Empty intermediate pages and
native page boundaries are retained. A wrong primary is refused locally;
original fixture bytes, mtime and version remain unchanged. Explicit PG selection
also survives uncertain ListPG results in local Submission metadata.

Backoff uses only the owned single-OSD pool: size/min_size changes from 1/1 to
2/2 create a peered, inactive PG, then restoration produces UNBLOCK. A native
query supplies the OSD-applied epoch. The test receives real BLOCK and preserves
the original epoch, PG/shard, range, ID and authenticated session identity.
Blocked APPEND is a known local unsent refusal. A started STAT canceled by the
caller stays uncertain; UNBLOCK does not replay it.

Normal ACK is verified through native completed operation history. Delayed ACK
restores the PG before acknowledging, receives UNBLOCK, keeps the local gate
closed, then acknowledges the original BLOCK. A fresh append and read on the
same session complete without a residual daemon gate. Native bytes, mtime and
version establish exactly one explicit append. ACK receipts match source type
8, global ID, OSD, PG, original epoch, ID, opcode 2 and `reached_pg`. They prove
native reception, not direct observation of internal STATE_ACKED. Only minimal
identity is retained; full histories and client addresses are not archived.
The opt-in fixture retains 1024 operation-history entries for short ACK controls.

## Failures, review and ordering corrections

The first v8 attempt selected the profile after MON connection. MON correctly
refused missing HASHPSPOOL bit 30. Moving selection to Options before Dial
resolved admission without broadening the unconditional MON exception. Its log
is retained at `/private/tmp/ceph-osd-replicated-pg-v8-crc-aes-first.log`.

Independent review found that UNBLOCK before an explicit ACK leaves a native
DELETING receipt until ACK arrives. The client now retains a bounded tombstone
and the original private ACK receipt. A second review found that opening the
gate immediately when ACK enters the queue lets earlier queued objects overtake
it. Gate release now occurs at the single writer's ACK turn; earlier queued
objects are refused locally, and later objects follow ACK. Successful control
queue admission cannot be revoked by subsequent caller-context cancellation.
Deterministic partial-frame/FIFO race tests and the actual delayed-ACK profiles
cover these corrections. Active entries and tombstones share the 64-entry and
configured byte bound; overflow fails the session without dropping evidence.

The earlier successful secure backoff run at
`/private/tmp/ceph-osd-backoff-secure-aes256k-ipv6-first.log` covered local ACK
submission only. The final runs additionally require exact native reception.
These are distinct evidence boundaries.

## Reproduction and retained evidence

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_PROFILE=replicated-pg-v8 \
CEPH_MSGR_TEST_OSD_STORE=bluestore CEPH_MSGR_TEST_OSD_REPLICAS=3 \
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_CONNECTION_MODE=crc \
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSD(ReplicatedPG|PGListV8)Integration$' \
sh integration/run.sh
```

Backoff requires one OSD and selects
`^TestCephOSDBackoff(LateAck)?Integration$`. The secure profile selects container
runtime, MemStore, AES256K client/service and IPv6. Recipe ownership, finite
timeouts, automatic restoration and cleanup are described in
[OSD_BACKOFF_RECIPE.md](OSD_BACKOFF_RECIPE.md).

Final logs, each with a matching diagnostic directory under `/private/tmp`:

- `ceph-osd-replicated-pg-list-v8-crc-aes-final.log`
- `ceph-osd-replicated-pg-list-v8-secure-aes256k-ipv6-final.log`
- `ceph-osd-backoff-crc-aes-final.log`
- `ceph-osd-backoff-secure-aes256k-ipv6-final.log`

Selected native profiles do not establish split/force-resend recovery, actual
upmap changes, single-object backoff, backoff during authentication renewal or
failover, broad platform/configuration coverage, or default optimal CRUSH.
Standalone reply-v8 parity is not public v8-reply interoperability. Remote CI
success is separate from these local results.

Final local checks passed: whole-product `CGO_ENABLED=0 go test ./...` and vet;
cephmsgr/internal-osd/internal-session race tests; fixture-module CGO=0 tests and
vet; and all 310 Python tool tests. Native interoperability evidence is the
separate selected profile execution above, not these unit-test results.
