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
messages or unsolicited events have been specified or implemented.

## Bounded v6 vector

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
| CompareExtent | 0x1220 | offset, length == len(Data), positive length | no data; mismatch code -4095 minus relative mismatch offset |
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
contract. Ordered operations do not promise generic read-your-staged-writes
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

Live tests compare 21 mutation/guard/compound cases with an independent native
librados vector on a separate disposable object, including binary readback,
nanosecond mtime, exclusive create, version bounds, extent comparison,
rollback and FAILOK. Native librados's cmpext callback can itself return -EIO
under FAILOK although the wire aggregate is success and writes commit; its
negative per-op value and independent readback are checked separately. See
[Objecter.h](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.h#L444).

BlueStore tests verify ONDISK then orderly OSD stop/start and native/Go
readback using refreshed caller-owned routing. This is not a power-loss or
multi-replica durability claim. Lost-response tests independently observe one
append before canceling the local wait and verify a fresh session does not
repeat it. Unit tests additionally reject ACK-only completion, malformed
inputs and uint64 read-budget overflow, and retain submission metadata.

The verified fixture remains one OSD, replicas/min_size 1, legacy CRUSH, no
HASHPSPOOL/upmap, Tentacle 20.2.4 IPv4. Broader replicated profiles, full native
payload encoder fixtures for all new opcodes, mixed read-output/mutation
vectors, forced crash durability and unsolicited O0/O3 control/event contracts
remain separate acceptance work. O0/O1 are not globally complete.
