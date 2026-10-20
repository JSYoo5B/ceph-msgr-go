# Fresh Tentacle OMAP header/write codec fixtures

Ceph 20.2.4 source `7f793731f1b39eb4f465e960113d2363c311b964`; pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

The first public `encode-osd-omap-writes` SDK run compiled and linked the
independently written C++ wrapper against the official generated headers and
the matching libceph-common. Fresh ObjectOperation/MOSDOp/MOSDOpReply objects
directly run `encode_payload`; different native objects decode every emitted
payload. Decoded objects are never encoder inputs. No Go byte buffers, retained
decoded fronts, server traffic or authentication material supply the native
encoding. The library hash equals the mutation, xattr and OMAP read proofs;
only the new header/write wrapper target was compiled and linked.

Five triples have a 41-byte native MessageFrame header, front and data:

| Name | Front/data bytes | Logical case |
| --- | --- | --- |
| request-v6-all | 369/70 | GETHEADER; SETVALS with empty key and value, NUL key, binary key/value and `z`; binary SETHEADER `00ff480a80`; RMKEYS empty/NUL/`b`; CLEAR with FAILOK |
| reply-v6-all-success | 375/5 | ONDISK; raw 5-byte header output without a length prefix; four mutation results 0 without output |
| reply-v6-header-empty | 207/0 | ACK; single GETHEADER result 0 with an empty raw header |
| reply-v6-rmkeys-enoent | 207/0 | no completion bit; RMKEYS `{k}` with aggregate and per-op result -2 |
| reply-v6-failok-clear | 249/0 | ONDISK; CLEAR with FAILOK per-op -2, then SETVALS `{k: v}` result 0 and aggregate 0 |

Files use the `omap-write-native-` prefix. Request incarnation is 1, TID 77,
epoch 11, pool 7/`route-key`/`tenant`, raw hash `0x11223344` and object
`omap-write-codec`. Requests containing a write carry the explicit mtime
1700000000 seconds plus 5 nanoseconds; the GETHEADER-only source request uses
zero mtime. Native `ceph_osd_op_mode_read`/`ceph_osd_op_mode_modify` derive the
READ/WRITE request bits, and ONDISK plus RETURNVEC are explicitly requested.
The wrapper passes the SETVALS map and RMKEYS set as native `std::map`/`std::set`
members, so native byte order is the reference for Go's canonical sorting.
The pinned Objecter has two `omap_get_header` pointer overloads; typed null
outputs select the classic `(bufferlist*, int*)` builder. Native packed extent
lengths derive from encoded input bytes (47/5/18 in the all-operations vector,
0 for GETHEADER and CLEAR). Separate native decoders verify operation unions,
flags, input/output bytes and results.

The public MOSDOpReply constructor keeps the request mode bits and replaces
only the completion bits with the explicit logical acktype. The decoded reply
flags are therefore `0x4000034` (ONDISK|WRITE|READ|RETURNVEC), `0x4000011`
(ACK|READ|RETURNVEC), `0x4000020` (WRITE|RETURNVEC) and `0x4000024`
(ONDISK|WRITE|RETURNVEC). Every reply carries user version 19 and replay
version 11'20. Go compares the complete request header/front/data and retains
each reply segment, per-op result and raw output through the public client.


The runner supplies all four original wrapper sources when sharing the SDK
cache; each CMake target is guarded by source existence and the selected driver
builds only its own target. Ceph implementations remain unchanged. Native build
libraries are test tooling only. Source headers and library carry LGPL-2.1
notices (`rados.h` permits LGPL-2.1 or LGPL-3.0); this independently written
development wrapper copies no implementation, and no native dependency enters
the Go product.
