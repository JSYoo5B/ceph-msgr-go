# Tentacle native codec fixtures

[Fresh mutation payload/header encoder fixtures](mutation-native-README.md) have a
separate construction, execution and provenance receipt. The files described
below remain independently packed/native-decoder fixtures.

[Fresh xattr encoder/decoder fixtures](xattr-native-README.md) independently
construct five message triples and decode them in separate native objects;
their provenance and live daemon scope are recorded separately.

[Fresh OMAP read encoder/decoder fixtures](omap-native-README.md) independently
construct five message triples, including raw binary cursor/prefix inputs and
page-more/BYKEYS output framing. Their offline payload/header scope is separate
from the live native-C/Go page acceptance.

[Fresh OMAP header/write encoder/decoder fixtures](omap-write-native-README.md)
independently construct five message triples: a GETHEADER plus
SETVALS/SETHEADER/RMKEYS/CLEAR(FAILOK) request, and ONDISK, ACK, ENOENT and
FAILOK replies. They are offline codec proof only and claim no live OSD
execution, staging or durability.

[Fresh OMAP_CMP encoder/decoder fixtures](omap-cmp-native-README.md)
independently construct five message triples: a four-assertion OMAP_CMP
(EQ/LT/GT over empty, NUL and binary keys) followed by SETVALS, and success,
ECANCELED, FAILOK and read-only ENOENT replies. They are offline codec proof
only and claim no live comparison evaluation, execution or durability.

[Fresh OMAPRMKEYRANGE encoder/decoder fixtures](omap-range-native-README.md)
independently construct five message triples: three range removals (binary
endpoints with an embedded NUL and a high byte, an empty FAILOK range and an
empty begin), and success, ENOENT, FAILOK-then-SETVALS and EOPNOTSUPP replies.
They are offline codec proof only and claim no live range removal, erasure
pool refusal, execution or durability.

These independently constructed bytes were accepted by `ceph-dencoder` from
the pinned Ceph 20.2.4 development image recorded in `provenance.json`. They
are native decoder fixtures, not native payload encoder output, captures from
a live OSD, or proof of object placement.
No Go codec in this package generated the reference inputs or outputs.

Independent Python `struct.pack` reference envelopes were built from the
pinned `MOSDOp.h`, `MOSDOpReply.h`, `osd_types.{h,cc}`, `rados.h`, `msgr.h`,
and `zipkin_trace.h` field definitions. Native Ceph decoded each envelope,
printed the recorded summary, then re-exported the message envelope with the
feature mask in `provenance.json`. Re-exported front/data matched each input.
`Message::encode` retains a nonempty decoded payload without calling
`encode_payload` again, so this invocation does not independently validate the
feature-selected payload encoder. The negotiated v6 path and live reply
semantics are separately verified against actual Tentacle OSDs.
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
