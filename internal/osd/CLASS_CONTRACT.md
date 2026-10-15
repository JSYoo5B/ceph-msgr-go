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
result whose own output exceeds `osd_max_write_op_reply_len` (64 bytes by
default, per operation) becomes -EOVERFLOW for the aggregate and the op, the output is cleared while
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

A class method's helpers are sub-operations of the vector's transaction
(osd/objclass.cc:105-339). `cls_cxx_stat` and the length of a data read see
the staged object state (`ctx->new_obs`), the read bytes come from committed
storage, and a full-object read is checked against the staged data digest;
xattr and OMAP reads consult the committed backend of a replicated pool. The
method can stage data, xattr and OMAP writes itself. Live Tentacle confirmed
the staged stat and committed data views: CREATE then `hello.record_hello` on
a fresh object fails with -EEXIST, and `record_hello` then `hello.replay` on
a fresh object fails with -ENOENT, neither committing anything. A write
method followed by a full-reading method is harmful: `turn_it_to_11` then
`replay` on three BlueStore replicas never replied, the primary logged more
than a thousand `full-object read crc` mismatches with `missing primary copy`
repairs in four seconds and health raised OSD_TOO_MANY_REPAIRS, with the
object unchanged once the client closed. With one replica the source would
leave the object unfound.

The client therefore admits a CALL after guards and reads, after a CREATE
without FAILOK (with or without EXCL) and after read-mode CALLs, and rejects a
CALL after a write-mode CALL or after any other staged write (including
FAILOK writes, a FAILOK CREATE and DELETE). The read mode is the caller's
declaration that the method is registered read-only. A read-only method
that writes has the write staged and its result turned into -EIO, which
aborts the vector because a CALL never carries FAILOK, so later methods
observe no staged change from it. Declaring a write method as read mode voids that protection, since the
OSD classifies by registration. The admitted forms include the native librbd
compounds: `create(true)` then `rbd.set_id` on `rbd_id.<name>`
(librbd/image/CreateRequest.cc) and `lock.assert_locked` (read mode) then
`rbd.object_map_update` (librbd/object_map/UpdateRequest.cc). An explicit
read or guard after a CALL stays rejected; writes after a CALL are admitted.

The first failing operation aborts the transaction: later operations are not
executed and return result 0 with their request union echoed, output length 0
and no output, and nothing is committed. The aggregate result is the return
of the last executed operation, so a write method returning 42 followed by a
successful OMAPSETVALS yields aggregate 0 with per-op results 42 and 0. The
`osd_max_write_op_reply_len` bound applies per operation: outputs of 58 and 18
bytes in one vector succeed, while a 68-byte output turns that operation and
the aggregate into -EOVERFLOW with its output cleared, other operations keep
their results and outputs, and nothing is committed. FAILOK on CALL is
rejected because a failing FAILOK method may leave staged writes; EXCL and
unknown flags are rejected as for every opcode. Every observation above
matched an independent native librados client sending the same vector; see
[the compound record](../../integration/fixture/OSD_CALL_COMPOUND_VALIDATION.md).

## Provenance

This contract paraphrases pinned LGPL-2.1 sources (rados.h permits LGPL-2.1 or
LGPL-3.0) and copies no implementation. Live tests use only the stock `hello`
class shipped in the pinned image.
