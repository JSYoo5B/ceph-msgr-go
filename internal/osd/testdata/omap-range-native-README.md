# Fresh Tentacle OMAPRMKEYRANGE codec fixtures

Ceph 20.2.4 source `7f793731f1b39eb4f465e960113d2363c311b964`; pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

The first public `encode-osd-omap-range` SDK run compiled and linked the
independently written C++ wrapper against the official generated headers and
the matching libceph-common. Fresh ObjectOperation/MOSDOp/MOSDOpReply objects
directly run `encode_payload`; different native objects decode every emitted
payload. Decoded objects are never encoder inputs. No Go byte buffers, retained
decoded fronts, server traffic or authentication material supply the native
encoding. The library hash equals the mutation, xattr, OMAP read, OMAP
header/write and OMAP_CMP proofs; only the new OMAPRMKEYRANGE wrapper target
was compiled and linked.

Five triples have a 41-byte native MessageFrame header, front and data:

| Name | Front/data bytes | Logical case |
| --- | --- | --- |
| request-v6-all | 293/29 | OMAPRMKEYRANGE `[6200, 63ff)` (embedded NUL begin, high-byte end); OMAPRMKEYRANGE `[empty, empty)` with FAILOK; OMAPRMKEYRANGE `[empty, ff)` |
| reply-v6-success | 291/0 | ONDISK; aggregate 0; all three range results 0 without output |
| reply-v6-enoent | 207/0 | ACK\|ONDISK; OMAPRMKEYRANGE `[61, 62)` with aggregate and per-op result -2 |
| reply-v6-failok | 249/0 | ONDISK; OMAPRMKEYRANGE `[61, 62)` with FAILOK result -2, then SETVALS `{k: v}` result 0; aggregate 0 |
| reply-v6-eopnotsupp | 207/0 | ACK\|ONDISK; OMAPRMKEYRANGE `[61, 62)` with aggregate and per-op result -95 (logical erasure pool refusal shape) |

Files use the `omap-range-native-` prefix. Request incarnation is 1, TID 77,
epoch 11, pool 7/`route-key`/`tenant`, raw hash `0x11223344` and object
`omap-range-codec`. Every source request contains a write and carries the
explicit mtime 1700000000 seconds plus 5 nanoseconds. Native
`ceph_osd_op_mode_modify` derives the WRITE request bit (OMAPRMKEYRANGE is WR,
so no READ bit appears), and ONDISK plus RETURNVEC are explicitly requested.
The wrapper passes each endpoint as a length-aware `std::string` to the single
pinned builder `omap_rm_range(std::string_view, std::string_view)`
(Objecter.h 1395-1401), which encodes begin then end as u32-length strings and
sets `extent.offset` 0 and `extent.length` to the encoded length (12, 8 and 9
for the request ranges, 10 for `[61, 62)`, 14 for SETVALS). The request data is
`0200000062000200000063ff` `0000000000000000` `0000000001000000ff`. Separate
native decoders verify operation unions, flags, input bytes and results. Every
request range is ordered (begin is not greater than end), so Go's local
refusal of a reversed range is not exercised by these vectors.

The public MOSDOpReply constructor keeps the request mode bits and replaces
only the completion bits with the explicit logical acktype. The decoded reply
flags are therefore `0x4000024` (ONDISK|WRITE|RETURNVEC) for success and
failok and `0x4000025` (ACK|ONDISK|WRITE|RETURNVEC) for enoent and eopnotsupp.
Every reply carries user version 19 and replay version 11'20. Go compares the
complete request header/front/data, decodes every reply and returns each
through the public client; the -2 and -95 aggregates remain known `*OSDError`
results without replay or reconnection.

The refused acktypes and per-op results follow the pinned PrimaryLogPG.cc. The
OMAPRMKEYRANGE handler (lines 8067-8093) first refuses a pool without OMAP
support with -EOPNOTSUPP (8069-8072; `supports_omap` in osd_types.h 1787-1789 is
false only for erasure pools) and then a missing or whiteout object with
-ENOENT (8076-8079). A failed `do_osd_ops` in a vector that may write sets
`update_log_only` (`prepare_transaction`, lines 8991-8998), and `execute_ctx`
adds ACK|ONDISK before `record_write_error` (line 4366). FAILOK keeps the
per-op result and resets the running result (`do_osd_ops`, 8205-8213), so the
following SETVALS commits through the ordinary ONDISK completion. A live -ENOENT
write refusal also calls `set_enoent_reply_versions(info.last_update,
info.last_user_version)` (4362-4364); those live version values are not
represented by the constructed 11'20/19 versions.

Message::encode supplies the native envelope/footer after direct payload
encoding. Native MessageFrame uses explicit offline sequence 1, ACK 3 and
priority 127; these are not live ProtocolV2 session/ACK observations. Replies
contain constructed logical results. They do not establish live OSD range
removal, execution, staging, durability, ENOENT or EOPNOTSUPP semantics,
erasure pool behavior, capabilities or server limits, and this directory makes
no claim about live server behavior. Exact library, binary, wrapper and driver
hashes and the decoded operations are in `omap-range-native-provenance.json`.
The owned builder was independently absent after public termination. Runtime
proof and log: `/tmp/ceph-omap-range-native-encoder-attempt1-proof.json` and
`.json.log`.

```sh
cd integration/fixture
GOWORK=off go run . encode-osd-omap-range -project ../.. \
  -cache /private/tmp/ceph-native-encoder-cache \
  -output /tmp/ceph-omap-range-native-encoder-attempt1-proof.json
```

The runner supplies all six original wrapper sources when sharing the SDK
cache; each new CMake target is guarded by source existence and the selected
driver builds only its own target. Ceph implementations remain unchanged.
Native build libraries are test tooling only. Source headers and library carry
LGPL-2.1 notices (`rados.h` permits LGPL-2.1 or LGPL-3.0); this independently
written development wrapper copies no implementation, and no native dependency
enters the Go product.
