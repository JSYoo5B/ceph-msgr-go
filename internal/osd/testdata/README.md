# Tentacle native encoder fixtures

These bytes are canonical output of `ceph-dencoder` from the pinned Ceph
20.2.4 development image recorded in `provenance.json`. They are encoder
oracle fixtures, not captures from a live OSD or proof of object placement.
No Go codec in this package generated the reference inputs or outputs.

Independent Python `struct.pack` reference envelopes were built from the
pinned `MOSDOp.h`, `MOSDOpReply.h`, `osd_types.{h,cc}`, `rados.h`, `msgr.h`,
and `zipkin_trace.h` field definitions. Native Ceph decoded each envelope,
printed the recorded summary, then encoded it with the feature mask in
`provenance.json`. Native re-encoded front/data matched each reference input.
The test fixtures retain that native output, with the development envelope
header/footer and length prefixes removed. The `.hex` data contains no keys.

The native oracle invocation was:

```sh
ceph-dencoder type MOSDOp import request-v6-input.bin decode dump_json \
  set_features 3026423347916342070 encode export request-v6-native.bin
ceph-dencoder type MOSDOpReply import reply-v6-success-input.bin decode dump_json \
  set_features 3026423347916342070 encode export reply-v6-success-native.bin
```

The error and redirect fixtures use the same reply invocation. Request fields
are incarnation 1, epoch 11, pool 7, key `route-key`, namespace `tenant`, raw
hash `0x11223344`, object `osd-codec`, stat then read offset 2/length 4, nosnap,
empty snap context, attempt 0, default reqid and Messenger TID 77. Success
output has stat size 6/mtime 1000 seconds + 55 nanoseconds and binary read bytes
`00 ff 41 0a`. The missing-object reply preserves `-2` overall and for both
operations. Redirect output names pool 9, key `redirect`, namespace `tenant`,
and object `next-object`. Each reply has the current v6 24-byte trace suffix.

The cited Ceph sources carry LGPL-2.1 notices. This package independently
implements the wire definitions and includes serialized fixture data only;
it does not incorporate Ceph native implementation code.
