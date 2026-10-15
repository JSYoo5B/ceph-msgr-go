# Tentacle O2f object class CALL contract

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`.
Implementation and source audit: 2026-10-15. Native evidence is recorded in
[OSD_CALL_VALIDATION.md](../../integration/fixture/OSD_CALL_VALIDATION.md).
[CONTRACT.md](CONTRACT.md) supplies the v6 identity, explicit placement,
context, per-op result and uncertain-outcome rules this subset builds on.

## Scope

CEPH_OSD_OP_CALL (0x1401) is a raw transport primitive: the caller supplies
the class name, method name and input bytes and receives the method's signed
per-op return and raw output. The product adds no class-specific schema,
selects no class, and never retries or resends a call. RBD, lock, version or
any other class semantics belong to the consumer layer.

## Wire

The packed `cls` union (include/rados.h:600-605) is class_len u8, method_len u8,
argc u8 and indata_len le32 at the start of the 28-byte operation union; the
pinned builder (osdc/Objecter.h:227-242) leaves argc 0 and appends class,
method and input with no separator or padding. payload_len is the sum of the
three lengths. The OSD sets `extent.length` (union bytes 8-15) to the method
output length (osd/PrimaryLogPG.cc:6257) and the reply echoes the remaining
union. Lengths above 255 would be truncated by the native u8 assignment, and
the OSD opens the class through c_str(), so the client requires 1-255 bytes
without NUL for both names.

## Mode, capability and refusals

The OSD ignores the request READ/WRITE flags for CALL and classifies the method
by its registered flags (osd/osd_op_util.cc:88-97 and 187-205). The client's
`ClassRead`/`ClassWrite` mode therefore selects only the request flag and
whether a mutation mtime is admitted; a read-mode request to a write method
still executes as a write. Every sent CALL keeps the uncertain-outcome contract.

Before execution, `maybe_init_op_info` (PrimaryLogPG.cc:2050-2055) refuses a
class outside `osd_class_load_list` with -EPERM and an unknown method with
-EOPNOTSUPP; such replies leave per-op results 0 and carry ONDISK without ACK.
OSD capabilities then require `allow *`, `allow class <name>` or the
class-read/class-write (`x`) grants; `allow r` is refused with -EPERM. A read
method on a missing object returns -ENOENT before execution. At runtime a
method reading without its RD flag or writing without WR gets -EIO
(PrimaryLogPG.cc:6245-6254).

## Results and completion

The method return becomes the per-op result and positive values are kept; the
aggregate can be positive. In a vector that stages a write, a successful
result whose output exceeds `osd_max_write_op_reply_len` (64 bytes by default)
becomes -EOVERFLOW for the aggregate and the op, the output is cleared while
`extent.length` keeps the original length, and the transaction is not
submitted (PrimaryLogPG.cc:4287-4323). As for OMAP, the primary's cached
object_info has already advanced at that point (finish_ctx, 9154): live
Tentacle reported a newer user version and the refused request's mtime with
unchanged data and xattrs. A failing write-classified method takes the
`update_log_only` path and returns its output with the error. Read-path output
is bounded only by the frame. A method returning -EAGAIN or -EINPROGRESS
produces no reply (4271-4285); the client's context bounds the wait and the
outcome is uncertain.

Reply validation requires the echoed class/method/input lengths, argc 0, zero
truncate fields, output within the positive local `MaxOutputBytes`, and an
output length equal to the returned bytes except for -EOVERFLOW with cleared
output. Successful vectors in write mode require ONDISK.

## Ordering and flags

A class method can read committed or staged state inconsistently and can stage
data, xattr and OMAP writes itself (osd/objclass.cc:105-339). The client
therefore rejects a CALL after any staged write in the vector (including FAILOK
writes and DELETE) and any read, guard or CALL after a CALL. Guards and reads
before a CALL and writes after it are admitted. FAILOK on CALL is rejected
because a failing FAILOK method may leave staged writes; EXCL and unknown flags
are rejected as for every opcode.

## Provenance

This contract paraphrases pinned LGPL-2.1 sources (rados.h permits LGPL-2.1 or
LGPL-3.0) and copies no implementation. Live tests use only the stock `hello`
class shipped in the pinned image.
