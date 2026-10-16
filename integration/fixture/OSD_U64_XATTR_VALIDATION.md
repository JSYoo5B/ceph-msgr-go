# U64 CMPXATTR verification

On 2026-10-16, the separate pinned native encoder/decoder build and both live
U64 comparison profiles passed on their first attempts. This completes the
bounded U64 xattr comparison addition; it does not complete O2 or broaden
placement, replay, EC, bulk xattr, cancellation or failover claims.

## Scope and source

Reference: Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Audited sources:
`include/rados.h` (mode 2 and packed union), `osdc/Objecter.h` (raw name and
operand builder), `librados/librados_cxx.cc:130-136` (uint64 overload), and
`osd/PrimaryLogPG.cc:5027-5043,6462-6530` (stored decimal parsing, caller-left
operators, true +1, false -ECANCELED and invalid stored EINVAL). These
LGPL-2.1 sources were inspected; no implementation was copied.

`OSDXattrInput.Mode` must explicitly select `OSDXattrString` (1) or
`OSDXattrU64` (2). Zero and unknown CMP modes are rejected without fallback.
U64 operands remain exactly eight caller-supplied little-endian bytes in
`Data`; the client does not parse or normalize stored values. Non-CMP mode
fields must remain zero. Existing staged SET/RM-before-CMP refusal, FAILOK,
completion and unknown-outcome rules are preserved.

## Independent native wire and client evidence

`tools/osd_u64_xattr_native_encoder.cc` constructed two fresh native requests
(read comparison and comparison-before-SET) and three logical native replies
(true +1, false -125, invalid numeric -22). Pinned Ceph separately decoded
every encoded object. Go compares complete front/data/header bytes and
consumes the v6/compat2 replies, union fields, signed results and raw output.
These logical replies are wire evidence, not observations of server execution.

Only the new `u64-xattr-native-*` files and provenance were added. Existing
STRING, CHECKSUM and other historical vectors/proofs were not rewritten.
The shared native build driver remains unchanged. The native envelope proof
covers explicit offline sequence/TID/ACK inputs and native MessageFrame
serialization; it does not claim a live native ProtocolV2 session capture.

The same owned build container compiled `osd_u64_xattr_oracle.cc` against
pinned public headers and the matching image's installed `librados.so.2`.
Its loader audit resolved 27 dependencies with no missing libraries and
loaded `/usr/lib64/librados.so.2.0.0`; runtime search paths were
`/usr/lib64:/usr/lib64/ceph`. This C++ helper uses the real
`ObjectOperation::cmpxattr(name,op,uint64_t)` overload. The C ABI is
STRING-only and is used solely for owned object seeding and independent
before/after state observations. Native routing/retry behavior does not
become Go routing or replay behavior.

The native C++ API does not expose the individual comparison result callback.
Every live receipt labels `raw_cmp_per_op_result_available=false`. Native
aggregate results and observed effects are compared with Go; Go's individual
results are independently retained and checked, not invented native prvals.

## Live results

Both runs used the pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
and the actual linked fixture provider v0.3.0. Both explicitly used
`legacy-v6`, with independent native C++ and Go requests against separate
owned objects seeded with the same bytes and attributes.

| Profile | Cases | Test duration | Result and owned cleanup |
| --- | --- | --- | --- |
| CRC/AES, IPv4, host, 3-replica BlueStore | 52 | 31.79 s | PASS, exit 0; 2 owned objects removed; all 10 owned containers terminated |
| secure/AES256K key and service cipher, IPv6, container, single MemStore | 52 | 29.84 s | PASS, exit 0; 2 owned objects removed; all 8 owned containers terminated |

The 52-case matrix checks all six caller-left operators around a value above
2^63 in both read and write guards; UINT64_MAX; empty/missing attribute zero;
missing object; decimal and embedded-NUL suffixes; leading zeros; invalid
whitespace, plus/minus signs, nonnumeric and NUL prefixes; decimal overflow;
previously staged data rollback; and FAILOK continuation.

Observed examples in both profiles:

| Case | Native / Go aggregate | Go per-op results and state |
| --- | --- | --- |
| Valid standalone numeric comparison | +1 / +1 | +1 retained; bytes/attrs/mtime/version unchanged |
| False comparison | -125 / -125 | -125 retained; known refusal |
| Invalid stored decimal / overflow | -22 / -22 | -22 retained; known refusal |
| Missing object | -2 / -2 | Operation result remains 0 because object lookup prevented execution |
| WRITE before false numeric guard | -125 / -125 | `[0,-125,0,0]`; complete bytes/attrs/mtime/version rollback |
| FAILOK false guard before SET/RM | 0 / 0 | `[-125,0,0]`; later SET/RM committed with ONDISK and a newer version |
| FAILOK invalid stored value before SET/RM | 0 / 0 | `[-22,0,0]`; later SET/RM committed with ONDISK and a newer version |

`19suffix` and `19\0tail` both compared as 19, confirming the pinned server's
`from_chars` success check without requiring full string consumption. Product
code never converted those stored bytes. Successful guarded writes matched
the native twin's bytes, attributes and explicit mtime; each object's own
version advancement was checked without equating independent object versions.
Go preserved raw fronts/data, TID/global identity, submitted epoch, received
map epoch, retry attempt and signed per-op values.

## Reproduction and retained identities

Source checkpoint: `e3a3ba2bb4c94dc8b49d734f12a1aeb9e34dc95a` plus the separate
new native vector consumer tests. Product/oracle/acceptance files were frozen
before both live runs. SHA-256:

- `internal/osd/xattr.go`: `9a5d728a0a9225524fdc2ad879cf989ffd5b5656f1d24633b0a8633eb1e6901f`
- `cephmsgr/osd_xattr.go`: `8ea085e8bd60240825ca4e9b5bf5b895a69bca2f47193b2017dab37b3db12436`
- Native wire wrapper: `9ccff84ce9d65bb9ad36df1b33d7fa9b44a0d60518a5f17f3d808977c38cfbee`
- Native C++ oracle source: `f99e7ac5a5a159e19c50b981317da711dd610c8c4a7c054c72ced7d7a31d6094`
- Native C++ binary: `c515d3d6c68f091303ddbf99b931f856f2a9ec5e2926dd955210b4ea439f9d81`
- Python oracle: `dd98a86c5bd5c11ae0f132b7253e71ea4ebb12acecbdac76bb778ebe431b9fc9`
- Acceptance source: `d311b4160a6bb1b46bc5a5ce8cf147a46ee457e2d19dcdddde0b223cea95fd20`

Native build receipt:
`/private/tmp/u64-xattr-native-proof-20261016-first.json`, with adjacent log.
The builder
`3af2f2155ca83be9c4c0daf13e7e1468a34a9fcbec1f81f632164e9055c17bce`
terminated successfully. Complete build/source/header/payload identities are
retained in `internal/osd/testdata/u64-xattr-native-provenance.json`.

Live evidence directories, with adjacent `.log` files:

- `/private/tmp/u64-xattr-crc-3blue-20261016-first`
- `/private/tmp/u64-xattr-secure-aes256k-ipv6-memstore-20261016-first`

All 18 owned containers were absent from a subsequent Docker inventory and
both owned networks were absent after teardown. Each directory contains its
manifest, ownership and independent C++ build proof,
`osd-u64-xattr-results.json` (all 52 cases and raw Go metadata), last native
request/result and final owned-object cleanup. No failed live/native attempt
or application retry was needed for these two configurations.

Local verification also passed full CGO-disabled product and fixture tests,
product/fixture vet, focused normal and race tests (including fresh native
consumers), and five closed Python oracle tests. Live v8 request or reply
profiles, U64 snapshot comparisons, EC, primary-failure persistence and hardware
power-loss behavior were not exercised by this addition. No new release or
exact-final-main CI pass is claimed here.
