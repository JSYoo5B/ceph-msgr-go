# Fresh native MOSDOp v8 request vectors

Ceph v20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`.
Generated on 2026-10-15 by `tools/osd_request_v8_native_encoder.cc` and its
independent driver `tools/osd_request_v8_native_encoder.py`.

The pinned `messages/MOSDOp.h` class has HEAD_VERSION 9 and compatibility 3.
The independently constructed vectors select its exact v8 branch with
NEW_OSDOP_ENCODING and RESEND_ON_SPLIT, without SERVER_SQUID. That branch
encodes actual spg_t separately from the hobject's raw hash. It is not the v6
raw-pg encoding and does not include the v9 OpenTelemetry trace field.

Five fresh native requests cover read/stat, create/write mutation, write snapshot
context with a nondefault incarnation, checksum input, and an explicit shard.
Every vector is type 42, header version 8/compatibility 3, TID 77 and priority
127. Sequence 1 and ACK sequence 3 are explicit offline transport inputs.
The default examples use actual PG pool 7/seed 0x44 and raw object hash
0x11223344. The shard example uses shard 3, actual seed 0x55 and raw hash
0xabcdef01. The snapshot example preserves client/reqid incarnation 99.

The native wrapper constructs logical inputs without Go bytes or socket capture,
invokes native encode_payload, then independently allocates a native decoder,
runs decode_payload and finish_decode, and checks packed operations, payloads,
actual PG/shard, raw hobject and incarnation-derived reqid/TID. Decoded messages
are never re-encoded. A separate native header2 projection records the native
Message envelope, not a live ProtocolV2 session.

`request-v8-native-provenance.json` records pinned source/submodule hashes,
wrapper and own driver hashes, matching native library loader audit, binary and
configuration hashes, segment hashes, native headers and independent routing
receipts. The owned builder was terminated and termination is confirmed there.
The fixture harness checks all five request classes and header versions, and
binds the native routing receipt to source-defined fields of the fresh front.
It rejects request/reply/watch/backoff misclassification, v6/v7/v9 substitution,
wrong compatibility or TID, missing native decoding, changed routing identity,
and missing/extra vectors. Existing v6/watch16/backoff5 validation is preserved.

This is wire interoperability evidence. Sharded encoding proves no EC I/O,
feature-negotiation, split/retry, live OSD targeting or native execution claim.
Product feature advertisement and runtime acceptance require their own tests.

Reproduce from `integration/fixture` with the native cache exclusively held:


