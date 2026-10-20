# Fresh Tentacle OMAP_CMP codec fixtures

Ceph 20.2.4 source `7f793731f1b39eb4f465e960113d2363c311b964`; pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

The first public `encode-osd-omap-cmp` SDK run compiled and linked the
independently written C++ wrapper against the official generated headers and
the matching libceph-common. Fresh ObjectOperation/MOSDOp/MOSDOpReply objects
directly run `encode_payload`; different native objects decode every emitted
payload. Decoded objects are never encoder inputs. No Go byte buffers, retained
decoded fronts, server traffic or authentication material supply the native
encoding. The library hash equals the mutation, xattr, OMAP read and OMAP
header/write proofs; only the new OMAP_CMP wrapper target was compiled and
linked.

Five triples have a 41-byte native MessageFrame header, front and data:

| Name | Front/data bytes | Logical case |
| --- | --- | --- |
| request-v6-all | 253/76 | OMAP_CMP with empty key `(empty, EQ)`, NUL key `(80, LT)`, binary key `6280` `(00ff410a, GT)` and `z` `(5a, EQ)`; then SETVALS `{k: v}` (compare-before-write) |
| reply-v6-success | 247/0 | ONDISK; aggregate 0; OMAP_CMP and SETVALS results 0 without output |
| reply-v6-canceled | 247/0 | ACK\|ONDISK; aggregate -125 (ECANCELED); OMAP_CMP -125, unexecuted SETVALS 0 |
| reply-v6-failok | 247/0 | ONDISK; OMAP_CMP `{k: (78, EQ)}` with FAILOK result -125, then SETVALS `{k: v}` result 0; aggregate 0 |
| reply-v6-enoent | 205/0 | ACK\|ONDISK; read-only OMAP_CMP `{k: (empty, EQ)}` with aggregate and per-op result -2 |

Files use the `omap-cmp-native-` prefix. Request incarnation is 1, TID 77,
epoch 11, pool 7/`route-key`/`tenant`, raw hash `0x11223344` and object
`omap-cmp-codec`. Requests containing a write carry the explicit mtime
1700000000 seconds plus 5 nanoseconds; the read-only OMAP_CMP source request of
the enoent reply uses zero mtime. Native `ceph_osd_op_mode_read` and
`ceph_osd_op_mode_modify` derive the READ/WRITE request bits (OMAP_CMP is RD),
and ONDISK plus RETURNVEC are explicitly requested. The wrapper passes each
assertion set as a native `std::map<std::string, std::pair<bufferlist, int>>`,
so native byte order is the reference for Go's canonical sorting; Go receives
the same assertions in a non-sorted order. The pinned Objecter has
`omap_cmp(const std::map&, int*)` and `omap_cmp(bufferlist&&, int*)` overloads;
a named map lvalue and a typed null `int*` select the map builder. Native packed
extent lengths derive from encoded input bytes (62 for the four-assertion
comparison, 18 and 17 for the single-assertion comparisons, 14 for SETVALS).
Separate native decoders verify operation unions, flags, input bytes and
results.

The public MOSDOpReply constructor keeps the request mode bits and replaces
only the completion bits with the explicit logical acktype. The decoded reply
flags are therefore `0x4000034` (ONDISK|WRITE|READ|RETURNVEC) for success and
failok, `0x4000035` (ACK|ONDISK|WRITE|READ|RETURNVEC) for canceled and
`0x4000015` (ACK|ONDISK|READ|RETURNVEC) for enoent. Every reply carries user
version 19 and replay version 11'20. Go compares the complete request
header/front/data, decodes every reply and returns each through the public
client; the -125 and -2 aggregates remain known `*OSDError` results.

The refused acktypes and per-op results follow the pinned PrimaryLogPG.cc. A
failed `do_osd_ops` in a vector that may write sets `update_log_only`
(`prepare_transaction`, lines 8991-8998), and `execute_ctx` adds ACK|ONDISK
before `record_write_error` (line 4366). A read-only refusal through
`complete_read_ctx` adds ACK|ONDISK (line 9220). `do_osd_ops` stops at the
first ordinary failure, so the SETVALS after a failed comparison keeps result 0.
The enoent vector represents the OMAP_CMP handler's own `!obs.exists` or
whiteout check (lines 7879-7885), reached when an object context exists. A
read-only request for an object with no object context is instead refused by
`do_op` through `OSDService::reply_op_error` (PrimaryLogPG.cc 2443-2456,
OSD.cc 1635-1648). That path keeps only the request's ACK/ONDISK bits (ONDISK
for these requests) and the request's per-op results (0), and it is not encoded
here.


The runner supplies all five original wrapper sources when sharing the SDK
cache; each new CMake target is guarded by source existence and the selected
driver builds only its own target. Ceph implementations remain unchanged.
Native build libraries are test tooling only. Source headers and library carry
LGPL-2.1 notices (`rados.h` permits LGPL-2.1 or LGPL-3.0); this independently
written development wrapper copies no implementation, and no native dependency
enters the Go product.
