# Fresh Tentacle SETALLOCHINT codec fixtures

Ceph 20.2.4 source `7f793731f1b39eb4f465e960113d2363c311b964`; pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

The first public `encode-osd-alloc-hint` SDK run compiled and linked the
independently written C++ wrapper against the official generated headers and
the matching libceph-common. Fresh ObjectOperation/MOSDOp/MOSDOpReply objects
directly run `encode_payload`; different native objects decode every emitted
payload. Decoded objects are never encoder inputs. No Go byte buffers, retained
decoded fronts, server traffic or authentication material supply the native
encoding. The library hash equals the mutation, xattr, OMAP read, OMAP
header/write, OMAP_CMP, OMAPRMKEYRANGE and CALL proofs; only the new
SETALLOCHINT wrapper target was compiled and linked.

Five triples have a 41-byte native MessageFrame header, front and data:

| Name | Front/data bytes | Logical case |
| --- | --- | --- |
| request-v6-all | 293/3 | SETALLOCHINT 4194304/65536/`0x101` (SEQUENTIAL_WRITE\|COMPRESSIBLE) with FAILOK; WRITEFULL `abc`; SETALLOCHINT 0/0/0 with FAILOK |
| reply-v6-success | 291/0 | ACK\|ONDISK; the request-v6-all vector with aggregate and per-op results 0/0/0 |
| reply-v6-failok-refused | 249/0 | ACK\|ONDISK; SETALLOCHINT 4194304/65536/`0x101` with FAILOK result -95, then WRITEFULL `abc` result 0; aggregate 0 |
| reply-v6-enospc | 207/0 | ACK\|ONDISK; SETALLOCHINT 4194304/65536/`0x101` with FAILOK; aggregate and per-op result -28 (logical refusal shape) |
| reply-v6-max | 207/0 | ACK\|ONDISK; SETALLOCHINT 2^64-1/2^64-1/`0x7ff` (all eleven pinned flag bits) with FAILOK; aggregate and per-op 0 |

Files use the `alloc-hint-native-` prefix. Request incarnation is 1, TID 77,
epoch 11, pool 7/`route-key`/`tenant`, raw hash `0x11223344` and object
`alloc-hint-codec`. Every source request writes and carries the explicit mtime
1700000000 seconds plus 5 nanoseconds. SETALLOCHINT and WRITEFULL are WR in
`rados.h` (321 and 274), so native `ceph_osd_op_mode_modify` derives the WRITE
request bit alone; ONDISK and RETURNVEC are explicitly requested, so the request
flags are `0x4000024` (WRITE|ONDISK|RETURNVEC).

The wrapper calls the public `set_alloc_hint(uint64_t, uint64_t, uint32_t)`
builder (Objecter.h 1631-1641) and `write_full(buffer::list&&)` (584-589).
`set_alloc_hint` calls `add_alloc_hint` (303-310), which stores le64
`expected_object_size`, le64 `expected_write_size` and le32 `flags` in the
packed `alloc_hint` union (`rados.h` 651-655), and then always sets the per-op
FAILOK flag. The Go and public operations therefore request `OpFailOK` /
`OSDFailOK` explicitly; Go sends only the flags its caller sets. The native
request operations are:

- `2322` `02000000` `0000400000000000` `0000010000000000` `01010000`
  `0000000000000000` `00000000` (SETALLOCHINT, FAILOK, 4 MiB, 64 KiB, `0x101`,
  eight zero union bytes, payload_len 0)
- `0222` `00000000` with extent 0~3 and payload_len 3 (WRITEFULL)
- `2322` `02000000` followed by 28 zero union bytes and payload_len 0

The request data is `616263`. Go `EncodeRequest` and the public
`OSDConnection.Request` produce the identical header, front and data. The same
request without FAILOK on the first hint encodes differently.

In every reply the union is echoed unchanged. The decoded union bytes are
`0000400000000000` `0000010000000000` `01010000` plus eight zero bytes for the
4 MiB hint, 28 zero bytes for the zero hint and
`ffffffffffffffff` `ffffffffffffffff` `ff070000` plus eight zero bytes for the
max vector. Native `merge_osd_op_vector_out_data` derives `payload_len` 0 for
every reply operation, including the WRITEFULL echo (extent 0~3). Go decodes
`AllocHintInput{4194304, 65536, 257}`, `{0, 0, 0}` and
`{18446744073709551615, 18446744073709551615, 2047}` with per-op flags 2, and
`ValidateAllocHintReply` accepts every echo. A nonzero byte in the eight-byte
tail of the native max union is refused by `DecodeReply`, and the max echo is
refused for a request with a different size or flag set, or without FAILOK.

The public MOSDOpReply constructor (MOSDOpReply.h 143-155) keeps the request
mode bits and replaces only the completion bits with the explicit acktype, so
every decoded reply carries flags `0x4000025` (ACK|ONDISK|WRITE|RETURNVEC).
Each acktype and version follows the pinned path for that case:

- success and max: the SETALLOCHINT handler sets result 0 and stages the hint
  (PrimaryLogPG.cc 6717-6730), `set_reply_versions(at_version,
  user_at_version)` runs (4325) and the on-commit callback adds ACK|ONDISK
  (4387). The logical versions are 11'20 and 20.
- failok-refused: the logical shape of an older OSD that does not know the
  opcode. An unrecognized opcode takes the `do_osd_ops` default -EOPNOTSUPP
  (8197-8202); FAILOK keeps the per-op result and resets the running result
  (8205-8210), so the following WRITEFULL commits through the same ACK|ONDISK
  on-commit completion (4387) with versions 11'20 and 20.
- enospc: a logical refusal shape on the `update_log_only` path. A failed
  `do_osd_ops` in a vector that may write sets `update_log_only` (8991-8998);
  `execute_ctx` skips `complete_read_ctx` (4316), runs
  `set_reply_versions(at_version, user_at_version)` (4325) with the unchanged
  object user version (4247-4248), adds ACK|ONDISK (4366) and calls
  `record_write_error` (4370). The logical versions are replay 11'20 and user
  19, so the native legacy version is 11'19. The pinned SETALLOCHINT handler
  itself always returns 0, and a pool-full FULL_TRY refusal (9012-9024) would
  instead keep per-op result 0 and take `complete_read_ctx`; no live ENOSPC
  source for a per-op -28 is claimed.

Go compares the complete request header/front/data, decodes every reply and
returns each through the public client. The FAILOK -95 stays in the successful
reply's per-op result without an error; the -28 aggregate remains a known
`*OSDError` result. A cancelled wait after transmission stays an
`OutcomeUnknownError`, and no vector is replayed or reconnected.

Message::encode supplies the native envelope/footer after direct payload
encoding. Native MessageFrame uses explicit offline sequence 1, ACK 3 and
priority 127; these are not live ProtocolV2 session/ACK observations. Replies
contain constructed logical results. They do not establish live hint
recording in object_info, object store allocation behavior, staging,
durability, older-OSD or ENOSPC semantics, capabilities or server limits, and
this directory makes no claim about live server behavior. Exact library,
binary, wrapper and driver hashes and the decoded operations are in
`alloc-hint-native-provenance.json`. The owned builder was independently absent
after public termination. Runtime proof and log:
`/tmp/ceph-alloc-hint-native-encoder-attempt1-proof.json` and `.json.log`.

```sh
cd integration/fixture
GOWORK=off go run . encode-osd-alloc-hint -project ../.. \
  -cache /private/tmp/ceph-native-encoder-cache \
  -output /tmp/ceph-alloc-hint-native-encoder-attempt1-proof.json
```

The runner supplies all eight original wrapper sources when sharing the SDK
cache; each new CMake target is guarded by source existence and the selected
driver builds only its own target. Ceph implementations remain unchanged.
Native build libraries are test tooling only. Source headers and library carry
LGPL-2.1 notices (`rados.h` permits LGPL-2.1 or LGPL-3.0); this independently
written development wrapper copies no implementation, and no native dependency
enters the Go product.
