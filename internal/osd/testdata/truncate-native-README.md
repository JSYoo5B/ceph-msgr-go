# Fresh Tentacle extent truncate-field codec fixtures

Ceph 20.2.4 source `7f793731f1b39eb4f465e960113d2363c311b964`; pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

The public `encode-osd-truncate` SDK run compiled and linked the independently
written C++ wrapper `tools/osd_truncate_native_encoder.cc` against the
official generated headers and the matching libceph-common (the library hash
equals the snapshot proof; only the new wrapper target was compiled and
linked). Fresh ObjectOperation/MOSDOp/MOSDOpReply objects directly run
`encode_payload`; different native objects decode every emitted payload and
compare each packed `ceph_osd_op` (including `truncate_size` and
`truncate_seq`), the input/output data and the message fields with the
encoder inputs. Decoded objects are never encoder inputs, and no Go byte
buffer, server traffic or authentication material supplies the encoding.
[The receipt](truncate-native-provenance.json) records the source and
submodule archives, the generated configuration, wrapper, driver, binary and
library hashes, the loader audit, the encoding log and the builder cleanup.

Six triples have a 41-byte native MessageFrame header, front and data. The
first three are MOSDOp requests (type 42, compat 3), the last three
MOSDOpReply replies (type 43, compat 2). Sequence 1, ACK sequence 3 and
priority 127 are explicit offline inputs.

| Name | Front/data bytes | Logical case |
| --- | --- | --- |
| request-v6-read | 212/0 | READ 4~12, truncate_size 10, truncate_seq 3, at the head (NOSNAP); READ\|ONDISK\|RETURNVEC (`0x4000014`); zero mtime |
| request-v6-write | 212/8 | WRITE `truncate` at 8, truncate_size 10, truncate_seq 2; snap context 1/[]; WRITE\|ONDISK\|RETURNVEC (`0x4000024`); mtime 1739999999 s + 5 ns |
| request-v6-trunc | 212/0 | TRUNCATE offset 10, length 0, truncate_size 10, truncate_seq 3; snap context 1/[]; `0x4000024`; mtime 1739999999 s + 6 ns |
| reply-v6-read-clipped | 202/6 | ACK\|ONDISK; READ echoed with extent length 6 and output `ABCDEF`, truncate fields unchanged; versions 0'0, user 21 |
| reply-v6-write-trimmed | 202/0 | ACK\|ONDISK; WRITE echoed with extent length 2, truncate fields unchanged, no output; versions 11'22, user 22 |
| reply-v6-trunc-noop | 202/0 | ACK\|ONDISK; TRUNCATE echoed unchanged, result 0; versions 11'23, user 23 |

Files use the `truncate-native-` prefix. Request incarnation 1, TID 77, epoch
11, pool 7/`route-key`/`tenant`, raw hash `0x11223344`, object `trunc-codec`,
retry attempt 0.

## Builders and request bytes

The CephFS data path sends the inode's truncate fields with every extent
operation: `Client::_write` calls `Filer::write_trunc` and `Client::_read_sync`
calls `Filer::read_trunc` with `in->truncate_size` and `in->truncate_seq`
(Client.cc 11488-11496, 11991-11994). `Striper::file_to_extents` turns the
file truncate size into a per-object size (`object_truncate_size`, Striper.cc
309-347; 0 and all ones pass unchanged), and `Objecter::sg_read_trunc` and
`sg_write_trunc` send it per object extent (Objecter.h 3956-4012). The
wrapper uses:

- READ: the public `ObjectOperation::add_data(CEPH_OSD_OP_READ, 4, 12, …)`
  (Objecter.h 176-181) and then the fields `Objecter::read_trunc` sets
  (3383-3402): truncate_size 10, truncate_seq 3, op flags 0.
- WRITE: the public `ObjectOperation::write(off, bl, truncate_size,
  truncate_seq)` builder (570-577), the fields `Objecter::write_trunc` sets
  (3550-3573).
- TRUNCATE: the public `ObjectOperation::truncate(10)` (606-609), then the
  sequence fields as `Objecter::trunc` sets them (3643-3661): offset =
  truncate_size = 10, truncate_seq 3. The MDS's object trimming uses
  TRIMTRUNC instead (`Filer::truncate`, Filer.cc 405-482), which the product
  does not send.

The extent unions (offset, length, truncate_size, truncate_seq) are:

- READ `0112` `00000000` `0400000000000000` `0c00000000000000`
  `0a00000000000000` `03000000`, payload_len 0
- WRITE `0122` `00000000` `0800000000000000` `0800000000000000`
  `0a00000000000000` `02000000`, payload_len 8
- TRUNCATE `0322` `00000000` `0a00000000000000` `0000000000000000`
  `0a00000000000000` `03000000`, payload_len 0

`TestTruncateFieldRequestsMatchFreshNativeEncoder` (this package) requires
Go `EncodeRequest` to produce the native front, data and header exactly and
that the same requests with zero truncate fields do not.

## Replies

The replies are freshly constructed codec inputs, not daemon-produced
replies. All use the public MOSDOpReply constructor with acktype ACK|ONDISK
(the execute_ctx reply finished by complete_read_ctx, PrimaryLogPG.cc 9220,
or the on-commit callback, 4387) and public `claim_ops` with the union echoes,
explicit outputs, per-op result 0 and input data cleared.

- read-clipped: `do_read` (5849-5861) with a logical object truncate_seq 2,
  older than the op's 3, and an object size above truncate_size 10 cuts the
  extent to 10 - 4 = 6 and returns six bytes.
- write-trimmed: the WRITE handler (6768-6777) with a logical object
  truncate_seq 3, newer than the op's 2, and an object size 10 below 8 + 8
  cuts the extent length to 2 and the input to two bytes. Result 0.
- trunc-noop: a sequenced TRUNCATE no newer than the object's truncate_seq is
  a no-op (6986-6992) that still commits; the extent is echoed unchanged.

`TestDecodeFreshNativeTruncateFieldReplies` decodes them with `DecodeReply`
(lengths 6, 2 and 0, both truncate fields echoed), and
`TestOSDTruncateFieldNativeRepliesValidate` (cephmsgr) requires the public
reply check to accept each reply for its request and to refuse it for a
request with another truncate sequence or without truncate fields.

These vectors claim no OSD execution, trimming, durability or cap behavior;
the live behavior is recorded in
`integration/fixture/MDS_CEPHFS_DATA_VALIDATION.md`. No MOSDOp v8 vector was
generated: the CephFS data profile sends v6 requests (no `Placement`), and
`EncodeRequestV8` encodes the extent union with its own copy of the same
field order (`request_v8.go`), which these vectors do not cover.

Reproduce with the development-only fixture module's public Testcontainers
lifecycle:

```sh
cd integration/fixture && go run . encode-osd-truncate -project ../.. \
  -cache .artifacts/native-sdk -output .artifacts/native-proof.json
CGO_ENABLED=0 go test ./internal/osd -run 'TruncateField'
CGO_ENABLED=0 go test ./cephmsgr -run TestOSDTruncateFieldNativeRepliesValidate
```

The referenced Ceph headers/library carry LGPL-2.1 notices (`rados.h` permits
LGPL-2.1 or LGPL-3.0). These files are wire facts generated by an independent
development wrapper, not copied native implementation or a product
dependency. Source file hashes are in the provenance receipt.
