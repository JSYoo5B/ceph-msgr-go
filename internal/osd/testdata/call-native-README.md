# Fresh Tentacle CALL codec fixtures

Ceph 20.2.4 source `7f793731f1b39eb4f465e960113d2363c311b964`; pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

The first public `encode-osd-call` SDK run compiled and linked the
independently written C++ wrapper against the official generated headers and
the matching libceph-common. Fresh ObjectOperation/MOSDOp/MOSDOpReply objects
directly run `encode_payload`; different native objects decode every emitted
payload. Decoded objects are never encoder inputs. No Go byte buffers, retained
decoded fronts, server traffic or authentication material supply the native
encoding. The library hash equals the mutation, xattr, OMAP read, OMAP
header/write, OMAP_CMP and OMAPRMKEYRANGE proofs; only the new CALL wrapper
target was compiled and linked.

Five triples have a 41-byte native MessageFrame header, front and data:

| Name | Front/data bytes | Logical case |
| --- | --- | --- |
| request-v6-all | 249/27 | CMPXATTR `a` EQ `v` (STRING); write-mode CALL `hello.write_return_data` with input `5400ff` |
| reply-v6-read-output | 201/14 | ACK\|ONDISK; read-mode CALL `hello.say_hello` input `Tester`; aggregate and per-op 0; output `Hello, Tester!`; output length 14 |
| reply-v6-positive | 201/18 | ACK\|ONDISK; write-mode CALL `hello.write_return_data` with empty input; aggregate and per-op 42; output `you might see this`; output length 18 |
| reply-v6-eoverflow | 201/0 | ACK\|ONDISK; write-mode CALL `hello.write_too_much_return_data`; aggregate and per-op -75; output cleared; output length 520 |
| reply-v6-eopnotsupp | 201/0 | ONDISK; read-mode CALL `hello.nope`; aggregate -95; per-op 0; output length 0 (refused before execution) |

Files use the `call-native-` prefix. Request incarnation is 1, TID 77, epoch
11, pool 7/`route-key`/`tenant`, raw hash `0x11223344` and object
`call-codec`. Write-mode vectors (request-v6-all, positive, eoverflow) carry
the explicit mtime 1700000000 seconds plus 5 nanoseconds; read-mode vectors
carry zero mtime. CALL is RD|EXEC in `rados.h`, so `ceph_osd_op_mode_modify`
cannot select WRITE, and the OSD classifies a call by its registered method
flags (`osd_op_util.cc` 189-204). The wrapper therefore takes the CALL
READ/WRITE request bit as the explicit caller mode, like the Go
`ClassInput.Mode`; CMPXATTR keeps the native `ceph_osd_op_mode_read`
derivation. ONDISK and RETURNVEC are explicitly requested, so the request
flags are `0x4000034` (READ|WRITE|ONDISK|RETURNVEC).

The wrapper calls `cmpxattr(std::string_view, uint8_t, uint8_t, const
buffer::list&)` (Objecter.h 922-925) and then `call(const char*, const char*,
buffer::list&)` (1404-1406), the only three-argument `call` overload, with a
non-const buffer list. Its `add_call` (227-242) stores u8 `class_len` 5, u8
`method_len` 17, `argc` 0 and u32 `indata_len` 3, leaves the remaining 21
union bytes zero and appends class, method and input. The native request union
is `0511000300000000` followed by zeros with `payload_len` 25 (`0x19`), and the
request data is `6176` (CMPXATTR name and operand) followed by
`68656c6c6f` `77726974655f72657475726e5f64617461` `5400ff`. Go `EncodeRequest`
produces the identical header, front and data.

In a reply the cls fields overlap `extent.offset`. The CALL handler records the
method output length in the overlapping `extent.length` (union bytes 8-15;
PrimaryLogPG.cc 6257) before appending the output. The first 16 decoded union
bytes are `0509000600000000` `0e00000000000000` (read-output),
`0511000000000000` `1200000000000000` (positive), `051a000000000000`
`0802000000000000` (eoverflow) and `0504000000000000` `0000000000000000`
(eopnotsupp); the remaining 12 bytes are zero. Native `merge_osd_op_vector_out_data` derives `payload_len` 14,
18, 0 and 0 from the output. Go decodes these as `ClassResult` length 5/9/6/14,
5/17/0/18, 5/26/0/520 and 5/4/0/0 with `Argc` 0, and `ValidateClassReply`
accepts all four (the eoverflow output length 520 with empty payload is
admitted only because the result is -75).

The public MOSDOpReply constructor keeps the request mode bits and replaces
only the completion bits with the explicit acktype. Decoded reply flags are
`0x4000015` (ACK|ONDISK|READ|RETURNVEC) for read-output, `0x4000025`
(ACK|ONDISK|WRITE|RETURNVEC) for positive and eoverflow, and `0x4000014`
(ONDISK|READ|RETURNVEC) for eopnotsupp. Each acktype and version follows the
pinned path for that case:

- read-output: a CALL without a staged write ends in `complete_read_ctx`,
  which adds ACK|ONDISK (PrimaryLogPG.cc 9220) and sets `eversion_t()` with the
  object user version (9208-9213). The logical user version is 19, so the
  legacy version is 0'19 and the replay version is 0'0.
- positive: `write_return_data` (cls_hello.cc 146-170) stages a setxattr,
  returns 42 and 18 output bytes. `do_osd_ops` keeps 42 (8205-8213),
  `prepare_transaction` returns it (9042), the RETURNVEC bound admits 18 bytes
  (4288-4301), `set_reply_versions(at_version, user_at_version)` runs (4325)
  and the on-commit callback adds ACK|ONDISK (4387). The logical versions are
  11'20 and 20.
- eoverflow: `write_too_much_return_data` (cls_hello.cc 172-188) appends a
  52-byte sentence ten times, so 6257 records 520. The RETURNVEC bound then sets
  the aggregate and per-op result to -EOVERFLOW and clears the output
  (4294-4300). The negative result takes `complete_read_ctx` (4316-4322), which
  adds ACK|ONDISK and sets no versions, so all versions stay zero.
- eopnotsupp: `OpInfo::set_from_op` maps the `get_method_flags` -ENOENT for an
  unregistered method to -EOPNOTSUPP (osd_op_util.cc 189-195), and `do_op`
  calls `reply_op_error` (PrimaryLogPG.cc 2051-2054) before execution.
  `OSDService::reply_op_error` (OSD.cc 1635-1648) keeps only the request's
  ACK|ONDISK bits, which is ONDISK for this request, with zero versions and the
  unexecuted operation echo (per-op result 0, output length 0).

Earlier native vector sets used a plain ONDISK acktype for successful writes.
The pinned commit callback (PrimaryLogPG.cc 4387) adds ACK|ONDISK, so the
positive vector uses ACK|ONDISK; Go accepts both because it requires only
ONDISK for a successful mutation.

Go compares the complete request header/front/data, decodes every reply and
returns each through the public client. The positive 42 aggregate is a
successful reply with per-op result 42 and its output; the -75 and -95
aggregates remain known `*OSDError` results without replay or reconnection.


The runner supplies all seven original wrapper sources when sharing the SDK
cache; each new CMake target is guarded by source existence and the selected
driver builds only its own target. Ceph implementations remain unchanged.
Native build libraries are test tooling only. Source headers and library carry
LGPL-2.1 notices (`rados.h` permits LGPL-2.1 or LGPL-3.0); this independently
written development wrapper copies no implementation, and no native dependency
enters the Go product.
