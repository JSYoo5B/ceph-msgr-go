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

[Fresh CALL encoder/decoder fixtures](call-native-README.md) independently
construct five message triples: a CMPXATTR guard before a write-mode
`hello.write_return_data` call with binary input, and read output, positive
method result 42, EOVERFLOW-cleared output and pre-execution EOPNOTSUPP
replies. They are offline codec proof only and claim no live class loading,
method execution, write-reply bound or durability.

[Fresh SETALLOCHINT encoder/decoder fixtures](alloc-hint-native-README.md)
independently construct five message triples: two builder-FAILOK allocation
hints around a WRITEFULL, and commit success, FAILOK EOPNOTSUPP-then-WRITEFULL,
ENOSPC and maximum size/all-flag replies. They are offline codec proof only and
claim no live hint recording, object store allocation, older-OSD behavior,
ENOSPC semantics or durability.

[Fresh snapshot context, ROLLBACK and LIST_SNAPS encoder/decoder fixtures](snap-native-README.md)
independently construct eight message triples: a READ/STAT request at clone
snap 4, an ORDERSNAP WRITEFULL and a ROLLBACK with snap context 7/[7,4,2], a
LIST_SNAPS request at SNAPDIR, and listing, rollback, EOLDSNAPC and clone read
replies. They are offline codec proof only and claim no live snapshot
creation, clone layout, rollback execution, EOLDSNAPC detection or durability.

[Fresh extent truncate-field encoder/decoder fixtures](truncate-native-README.md)
independently construct six message triples: READ, WRITE and TRUNCATE
requests carrying truncate_size/truncate_seq as the CephFS data path sends
them, and a clipped READ, a trimmed old WRITE and a no-op sequenced TRUNCATE
reply. They are offline codec proof only and claim no live truncation,
trimming or durability.

[Fresh SPARSE_READ/READ compound request fixtures](sparse-read-compound-native-README.md)
independently construct fifteen requests: five librbd-shaped layouts of
SPARSE_READ with READ or SPARSE_READ, with native fadvise hints, across v6, v8
and v9. They are offline request codec proof only and claim no allocation
map, EOF behavior or execution outcome.

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

## LIST_WATCHERS fresh native vectors (2026-10-15)

The five `watchers-native-*` vectors and
[provenance](watchers-native-provenance.json) were directly encoded from fresh
pinned Ceph 20.2.4 native messages, then separately decoded. LIST_WATCHERS
output itself uses the native watcher-list encoder and decoder, including
IPv4/IPv6 entries, maximum cookie and empty list. Go supplies logical inputs
independently; it does not supply encoder bytes. Offline header seq/ACK inputs
are not a live ProtocolV2 session. EACCES/ENOENT vectors are explicit logical
refusals, not daemon permission evidence. See
[the contract](../WATCHERS_CONTRACT.md) and
[daemon acceptance](../../../integration/fixture/OSD_WATCHERS_VALIDATION.md).

```sh
cd integration/fixture
go run . encode-osd-watchers -cache .artifacts/native-sdk -output .artifacts/watchers-proof.json
```

## MAPEXT and CRC32C CHECKSUM fresh native vectors (2026-10-15)

`mapext-native-*` and `checksum-native-*` are five fresh native message
encoder/decoder vectors per opcode. Each corresponding provenance JSON
records the pinned source/build/wrapper and all header/front/data hashes.
The native wrappers supply logical outputs to the real reply encoder. They
do not prove server allocation layout or actual checksum calculations; the
separate native CLI/librados daemon tests provide those observations.
Go encodes its own request and compares the resulting bytes and public reply
conversion against these stored native payloads.

```sh
cd integration/fixture
go run . encode-osd-mapext -cache .artifacts/native-sdk -output .artifacts/mapext-proof.json
go run . encode-osd-checksum -cache .artifacts/native-sdk -output .artifacts/checksum-proof.json
```

## CHECKSUM xxHash expansion (2026-10-16)

The checksum generator now retains 15 native message vectors: one request
and four logical replies per CRC32C, xxHash32 and xxHash64. The original CRC
header/front/data bytes remain identical. Algorithm selections run through
the unchanged shared five-vector builder separately; the merged provenance
retains each selection's actual wrapper, binary, library, configuration and
build-driver hashes. Other generators and their historical proof hashes are
unchanged. The xxHash64 vectors include seed and result bits above uint32.
These encoded logical replies prove wire format, not native hash execution;
see [the algorithm acceptance](../../../integration/fixture/OSD_CHECKSUM_ALGORITHMS_VALIDATION.md)
for separate live evidence.

## U64 CMPXATTR fresh native vectors (2026-10-16)

`u64-xattr-native-*` is a separate five-vector suite: two fresh native
requests (read and compare-before-SET) and three logical replies (true +1,
false -125, invalid numeric -22). Pinned Ceph directly encoded each fresh
object, then separately decoded it. Go compares complete front/data/header
bytes and preserves v6/compat2, the U64 union and signed results. These logical
reply inputs are not live daemon execution evidence. Existing historical
STRING and other proofs/segments were not rewritten.

`u64-xattr-native-provenance.json` retains exact source/build/wrapper/launcher
and all segment hashes, plus successful builder termination. The same driver
builds an independent public-C++ uint64 comparison client for opt-in live
acceptance; its raw comparison per-op callback is unavailable. See
[OSD_U64_XATTR_VALIDATION.md](../../../integration/fixture/OSD_U64_XATTR_VALIDATION.md)
for the two separately verified 52-case configurations and their limits.

```sh
cd integration/fixture
go run . encode-osd-u64-xattrs -cache .artifacts/native-sdk -output .artifacts/u64-xattr-proof.json
```
