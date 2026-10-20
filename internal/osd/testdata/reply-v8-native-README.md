# Fresh native MOSDOpReply v8 vectors

Pinned Ceph v20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`.
Independent wrapper: `tools/osd_reply_v8_native_encoder.cc`.
Independent driver: `tools/osd_reply_v8_native_encoder.py`.
Existing v6 drivers and provenance are unchanged.

The pinned MOSDOpReply class declares HEAD_VERSION 8 and compatibility 2. Its
native payload encoder selects v8 with NEW_OSDOPREPLY_ENCODING, and v6 when
that feature is absent. These five outputs are all replies: there is no leading
request vector, although each fresh reply constructor receives a separately
constructed native request as logical input.

Names are `reply-v8-native-{success,enoent,unknown-error,mutation-ack,redirect}`.
Every native header is type 43, version 8/compatibility 2, TID 77, priority 127.
Offline sequence 1 and ACK sequence 3 are explicit header-projection inputs.
The cases preserve binary read/stat outputs, signed ENOENT and unknown -12345,
per-operation results and boundaries, actual PG 7.0x44, map epoch 12, retry zero,
replay/user/legacy versions, and conditional redirect metadata. The mutation-ack
vector intentionally lacks ONDISK: encoding and decoding that acknowledgement
is not evidence of mutation completion or durability.

The wrapper directly encodes fresh native objects and separately decodes native
replies, checking header/identity/flags/status/versions, every packed operation,
output bytes and redirect values. Decoded messages are never re-encoded. There
is no socket capture, Go-generated native input, daemon request execution or
ProtocolV2 live-session claim.

The retained `reply-v8-native-provenance.json` records pinned source/submodules,
wrapper and own-driver hashes, matching native library loader audit, generated
configuration, binary and per-segment hashes, native headers, decoded operation
receipts and builder termination. The harness explicitly classifies all five as
replies rather than applying its legacy first-vector-is-request default. It
rejects wrong wire classes/versions/compatibility/TID and incomplete decoding
receipts, including out-of-range signed results and inconsistent output bounds.
Existing operation, watch, backoff and request-v8 classifications remain intact.

Reproduction from `integration/fixture`, with exclusive native cache access:


These files establish independent native wire evidence only. Product feature
advertisement, public decoder selection and real profile interoperability need
separate integration and live-daemon verification.
