# Tentacle O0/O1 object request contract

Reference: Ceph 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.
This is an independently implemented wire translation. The cited Ceph source
files carry LGPL-2.1 notices; no C++ implementation is incorporated.

[rados.h](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/rados.h)
defines the packed operations and flags. [MOSDOp.h](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MOSDOp.h)
defines request encoding and default identity. [MOSDOpReply.h](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MOSDOpReply.h)
defines raw completion and per-op outputs. [PrimaryLogPG.cc](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc)
defines execution, normalization, guards, rollback and completion.

The exhaustive `__CEPH_FORALL_OSD_OPS` inventory is recorded in
[opcodes-tentacle.csv](opcodes-tentacle.csv). Entries marked unsupported cannot
be transmitted or accepted as ordinary operation replies. Mode bits alone do
not establish semantics: notably CALL uses a read bit but its class determines
its behavior. The inventory does not claim that deferred payloads, control
messages or unsolicited events have been implemented. The separate
[control/event contract](EVENT_CONTRACT.md) inventories current map/reply
delivery, future backoff/watch wire requirements and their acceptance gaps.

## Bounded v6 vector

The additional five xattr operations have their own active packed union and
local output budgets in [METADATA_CONTRACT.md](METADATA_CONTRACT.md). Existing
identity, per-op results, ONDISK and unknown-outcome rules also apply to them.
Three bounded OMAP reads, the raw header read, OMAP_CMP and four OMAP writes
(SETVALS, SETHEADER, CLEAR, RMKEYS) use derived input-length extents;
[OMAP_CONTRACT.md](OMAP_CONTRACT.md) records their distinct union, pagination,
comparison, write ordering and native acceptance. OMAP range removal,
class/snapshot, U64 comparison and bulk xattr variants remain unsupported.

Negotiated OBJECTLOCATOR and PGID64 select the supported request/reply v6
branch; NEW_OSDOP_ENCODING and NEW_OSDOPREPLY_ENCODING are rejected. The caller
supplies pool, namespace, locator key, object, raw placement hash, target OSD,
numeric address/nonce and OSDMap epoch. The raw hash echoed in pg_t is not an
actual mapped PG number. Only head/no-snapshot context and no EC shard are
encoded. There is no hashing, placement, redirect following or retry policy.

Each packed operation is 38 bytes: opcode u16, flags u32, 28-byte union,
payload length u32. Input bytes concatenate in vector order in the message
data segment. All inputs are copied before submission. The caller must not
mutate slices until Request returns. Returned outputs and raw bytes are owned
by the reply. Aggregate front/data and possible read/stat output are bounded by
MaxFrameSize; vectors have 1–64 operations. Extents cannot overflow uint64.

| Public code | Wire code | Union/input contract | Output |
| --- | --- | --- | --- |
| Read | 0x1201 | offset, positive bounded length; no input | bounded raw bytes; native may shorten extent |
| Stat | 0x1202 | zero extents, no input | 16 bytes: size u64, seconds u32, nanos u32 |
| AssertVersion | 0x1208 | positive Version in second union u64; other fields zero | no data; older -ERANGE, newer -EOVERFLOW |
| CompareExtent | 0x1220 | offset, length == len(Data), positive length; native comparison zero-extends bytes past EOF | no data; reply extent may shorten at committed EOF, including zero; mismatch code -4095 minus relative mismatch offset |
| Write | 0x2201 | offset, length == len(Data) | no data |
| WriteFull | 0x2202 | offset zero, length == len(Data); empty replacement allowed | no data |
| Truncate | 0x2203 | offset is new size; length zero, no input | no data |
| Zero | 0x2204 | offset/length; no input | no data; may normalize reply opcode to Truncate |
| Delete | 0x2205 | zero extents, no input | no data |
| Append | 0x2206 | offset zero, length == len(Data) | no data |
| Create | 0x220d | zero extents, no input; EXCL optional | no data; EXCL existing object returns -EEXIST |

Version is rejected for other operations. Unknown flags are rejected before
transmission. EXCL (1) is accepted only on Create; FAILOK (2) retains an
operation's negative result while allowing subsequent vector operations to
continue. Truncate sequences, category payloads and FADVISE input flags are
unsupported. A zero range covering an object's tail can become truncation;
zero beyond EOF is a native no-op, not an extension promise.

Mutation operations contribute WRITE; reads, stat and guards contribute READ.
Requests add ONDISK and RETURNVEC. No separate preliminary ACK is requested.
Explicit raw ModificationTime encodes utime_t; nanoseconds must be below 1e9.
Zero is transmitted unchanged and preserves native zero-mtime behavior. No
local clock is silently substituted. Nonzero mtime on a read-only vector is
rejected because it would not set the object's mtime.

A following READ or CMPEXT at offset 0 covering any preceding nonempty
WRITEFULL, APPEND or offset-zero WRITE payload length is rejected before
transmission. The conservative check retains the minimum positive payload
length across intervening operations without assuming a known committed size.
An actual native WRITEFULL/full-READ vector triggered a checksum failure:
PrimaryLogPG::do_read compared committed bytes with the staged size/digest and
marked the primary object unreadable. WRITE, APPEND and CMPEXT share relevant
write/read paths in the pinned source; their same risk is source-derived,
not an additional observed native failure. Unit tests reject all six guarded
write/read-or-compare combinations before submission. Partial and EOF reads
after replacement are separately verified; native checksum checks are not
disabled to claim broader support.

## Identity, results and uncertainty

Session.Call allocates a client-wide nonzero TID. The default v6 reqid lets the
server derive authenticated client identity and header TID, while restoring
ClientIncarnation. Attempt is zero. Submission preserves these local inputs
even if the admitted call fails; it is not a server reply or execution receipt.
The server's PG/hash, epoch, versions, raw flags, aggregate signed code, per-op
signed codes and payload boundaries remain separate. Missing-object rejection
can precede vector evaluation, leaving some per-op codes zero.

One object vector is one native transaction. Ordinary guard failure prevents
its staged writes from becoming visible. FAILOK intentionally changes that
contract. An aggregate success with a negative ordinary, non-FAILOK per-op
result is a malformed completion receipt. The session fails and a sent
mutation retains its unknown outcome, original results and local submission;
it is never replayed. Known negative aggregate refusals and positive result
codes remain distinct. Ordered operations do not promise generic read-your-staged-writes
behavior or atomicity across different requests/objects. Reply normalization
must correspond to the submitted operation; unrelated opcode/offset/length
or payload changes fail the session as an unknown outcome.

A successful mutation must carry OSD ONDISK. ACK-only or ONNVRAM-only success
cannot complete this API; unexpected completion fails the session and returns
OutcomeUnknownError. A negative server refusal is a known result and does not
need an ONDISK flag. Messenger ACK proves neither object execution nor object
durability. MemStore can report ONDISK while losing contents on restart.

Cancellation after transmission begins, transport loss, malformed reply or
Close with a sent pending mutation preserves OutcomeUnknownError and the
submission identity. No mutation is automatically resubmitted. Reopening an
explicitly chosen OSD starts a fresh session with increasing TIDs, not a replay.

## Verification and remaining acceptance

Live tests compare 25 mutation/guard/compound cases with an independent native
librados vector on a separate disposable object, including binary readback,
nanosecond mtime, exclusive create, version bounds, extent comparison,
rollback and FAILOK. Native librados's cmpext callback can itself return -EIO
under FAILOK although the wire aggregate is success and writes commit; its
negative per-op value and independent readback are checked separately. Four
native EOF comparison cases cover zero-extension past EOF, wholly beyond EOF,
mismatch rollback and FAILOK commit. Returned CMPEXT extent lengths shorten to
2 or 0 while comparison still checks the zero-extended requested range; these
normalized lengths remain visible rather than failing the session. See
[Objecter.h](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.h#L444).

The single-OSD BlueStore profile passes eleven native tests, including five
mixed read/stat/mutation vectors, orderly restart and SIGKILL (exit 137, no
OOM) readback. ONDISK completion is followed by independent native and fresh
Go reads with caller-owned routing; bytes, version and nanosecond mtime are
checked. Mixed vectors exercise actual Tentacle execution and independent
native pre/post stat2/read. The native C write_op API does not independently
reconstruct their mixed read/write payloads.

The expanded three-OSD BlueStore profile uses replicas 3/min_size 2 and passes
all twelve selected OSD/MON-map tests, including the 25 mutation/guard cases,
five mixed vectors, renewal, map delivery, lost reply and restart/crash paths.
After an
ONDISK binary write the actual primary is killed with SIGKILL. Independent
native placement selects the surviving primary; the caller explicitly opens
that Go target. The surviving two-replica acting set preserves bytes, version
and mtime without another write or automatic mutation replay. This proves the
observed daemon failure scenario, not host/device power-loss persistence.

Lost-response tests independently observe one append before canceling the local wait and verify a fresh session does not
repeat it. Unit tests additionally reject ACK-only completion, malformed
inputs and uint64 read-budget overflow, and retain submission metadata.

Verified profiles use Tentacle 20.2.4, legacy CRUSH, no HASHPSPOOL/upmap,
with single-replica MemStore/BlueStore and three-replica/min_size 2 BlueStore.
Five [mutation decoder vectors](testdata/mutation-README.md) independently
construct fields in Python and pass native ceph-dencoder decoding/re-export.
Message::encode retains nonempty decoded payload, so re-export does not prove
the native payload encoder. Five separate
[fresh native encoder vectors](testdata/mutation-native-README.md) now verify
mutation/guard request front/data and four native reply constructors, plus
five 41-byte native header2 serializations with explicit offline transport
inputs. They do not execute a live ProtocolV2 session or those server requests.
Broader replicated/address/OS matrices, host/device power-loss durability, EC
and native O0/O3 event/lifetime acceptance remain work. O0/O1 are
not globally complete. The expanded CRC/aes256k single-MemStore host-relay run
also passes nine selected tests with all 25 mutation/guard cases and five mixed
vectors; BlueStore-only restart/crash/replica tests are skipped.

The additional CRC/AES client-and-service-key IPv4 host-relay profile with
three BlueStore replicas/min_size 2 passes all twelve selected tests on its
first attempt, including 25 mutation/guard cases, five mixed vectors and
actual-primary SIGKILL readback. Verified product source is
`0943727781a4d41d5092d38ab600f03478dba6ef`. Independent inspection confirmed all
ten owned containers and the network were gone after cleanup. This expands
one local profile and does not establish a whole CI or native payload-encoder
proof.

The IPv6 secure/aes256k three-BlueStore-replica container profile passes all
12 selected tests, 25 mutation/guard cases and five mixed vectors on Linux
arm64. The oracle verifies every owned OSD's numeric IPv6 public endpoint,
port and nonce, including the replacement primary. This is distinct from the
IPv4 host-relay evidence and does not establish other IPv6 configurations.
