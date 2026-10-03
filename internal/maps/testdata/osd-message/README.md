# Native-validated Tentacle OSD map envelopes

The reference envelopes were independently built with Python `struct.pack`
from the fixed Ceph 20.2.4 message definitions, then accepted by the pinned
native `ceph-dencoder type MOSDMap` decoder. No Go codec created these bytes.
The image digest, native envelope hashes and summaries are in `provenance.json`.
These fixtures establish native decoder acceptance, not live map delivery or
selection of a Messenger feature branch.

The opaque nested blobs originate from native `OSDMap select_test 2` and
`OSDMap::Incremental select_test 1` generation with feature mask
`3026423347916342070`. The independent reference builder assigns the matching
FSID and epochs in their classic v6 headers. Separate native decoders confirmed
FSID `00112233-4455-6677-8899-aabbccddeeff`, full epoch 10 and incremental epochs
11/12. The product envelope decoder neither inspects nor applies those blobs.

Native envelope validation used:

```sh
ceph-dencoder type MOSDMap import osdmap-v1-reference.bin decode dump_json \
  encode export osdmap-v1-native-validated.bin
ceph-dencoder type MOSDMap import osdmap-v4-reference.bin decode dump_json \
  encode export osdmap-v4-native-validated.bin
```

The `.front.hex` fixtures retain native-exported message fronts. **Message
export retains an already decoded payload** and recomputes envelope CRCs; it
does not call `MOSDMap::encode_payload` again. Feature-selected encoding must
instead be established from the fixed source and actual interoperability.

[MOSDMap.h](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MOSDMap.h)
defines FSID, the incremental then full ordered containers, and the v4
trim/newest fields plus obsolete removed-snapshot map. Tentacle always encodes
that final map as an empty count; the decoder explicitly checks it and retains
it in `RawFront`. A nonempty suffix is unsupported rather than discarded.
The OSD's map-sharing path uses canonical encoding features; with this client's
current features it may select v1. MON map construction uses peer encoding
features directly and can retain v4. Supporting that Tentacle v1 wire branch
does not add support for an earlier Ceph release family.

The referenced native sources carry LGPL-2.1 notices. This package independently
implements the envelope semantics and includes serialized test data only; it
does not incorporate native implementation code, keys or runtime dependencies.
