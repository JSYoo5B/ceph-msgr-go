# Fresh Tentacle xattr encoder and decoder fixtures

These five front/data/header triples come from unchanged pinned Ceph 20.2.4
implementation at commit `7f793731f1b39eb4f465e960113d2363c311b964` in an owned
daemonless Testcontainers builder. The C++ wrapper invokes native
ObjectOperation xattr builders, creates fresh MOSDOp/MOSDOpReply objects and
calls encode_payload directly. Separate new native objects then decode those
bytes and verify every union, input/output segment and signed result. Decoded
objects are never re-encoded or used to generate these fixtures.

The [provenance receipt](xattr-native-provenance.json) records exact source,
submodule/configuration/wrapper/driver/library/binary hashes, native header and
decoded-op receipts, actual compilation and public lifecycle cleanup.
The matching library hash equals the earlier mutation proof; only the new
xattr wrapper target was compiled and linked.

- Request: 592-byte front, 80-byte data, 11 ordered operations. Native builders
  supply GET/GETALL, all six STRING comparison IDs, binary and empty SET, and
  RM. Name `_raw\xff`, value `00ff410a` and FAILOK on LTE are explicit inputs.
- Mixed reply: GET output4, native-encoded name/value map34, raw per-op
  results `[0,0,1]`; total data38 and GET value_len normalization4.
- Negative replies: constructed CMP ECANCELED(-125), SET EFBIG(-27), and RM
  ENOENT(-2), each with its raw union and no output bytes.
- Five native MessageFrame headers: type42/43, version6, compatibility3/2,
  TID77, explicit sequence1/ACK3/default priority127, CRC-enabled COMPLETE1.

The reply results/output are explicitly constructed logical codec inputs
following audited server shapes. They do not execute comparison, attr lookup,
configured limits or a live server handler. The native map encoder and payload
encoder derive actual wire bytes; they do not establish storage effects.
Header projection is offline, not ProtocolV2 session execution. Actual
Go/native daemon behavior is recorded separately in
[OSD_XATTR_VALIDATION.md](../../../integration/fixture/OSD_XATTR_VALIDATION.md).

```sh
go -C integration/fixture run . encode-osd-xattrs -project ../.. \
  -cache .artifacts/native-sdk \
  -output .artifacts/native-proof.json
CGO_ENABLED=0 go test ./internal/osd -run 'Xattr'
```

The public runner keeps mutation and xattr vector sets separate, requires the
independent xattr decoder receipt, bounds logs and terminates its owned builder
before writing a successful proof. Shared SDK configuration can retain both
targets, so it copies both original wrapper sources; the selected driver builds
only its own target. The mutation wrapper/driver/provenance are unchanged.
Ceph native headers/library have LGPL-2.1 provenance; this independently written
development wrapper copies no implementation and adds no product dependency.
