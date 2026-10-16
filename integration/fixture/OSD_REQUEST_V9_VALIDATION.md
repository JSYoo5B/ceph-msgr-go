# Explicit untraced request-v9 / reply-v8 validation

Prepared 2026-10-16. Reference Ceph 20.2.4 source
`7f793731f1b39eb4f465e960113d2363c311b964`; image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Product contract: `internal/osd/REQUEST_V9_CONTRACT.md`.

## Fresh independent native wire proof

Five fresh native requests cover read (actual PG distinct from raw hash),
binary mutation with explicit mtime, incarnation/snapshot context, checksum
union and dedicated PGNLS with empty oid and explicit locator hash. Each
canonical `Message::encode` selected Type42/v9/compat3 from the full native
SERVER_SQUID feature mask. A separate without-SQUID control selected v8.
Fresh native partial/final decoding checked routing, flags, operation data,
incarnations, snapshot context, feature tail and IsValid=false. Go compares
all front/data/header bytes against these actual independent outputs.

The native build used official WITH_JAEGER=OFF and generated HAVE_JAEGER
undef, not a replacement tracing class or mixed header/library ABI. Exact
source, toolchain, generated configuration, archive, libceph-common, wrapper
and driver hashes are retained. Generated config SHA-256:
`822c996c07edba730f5f54ec99022ebfdecfbc55b92bdbaf53fe775548a5f5af`.
No OpenTelemetry SDK or native library was added to the Go product.

Proof, first execution:
`/private/tmp/request-v9-native-proof-20261016-first.json` and adjacent log.
Retained provenance:
`internal/osd/testdata/request-v9-native/provenance.json`.
Retained provenance SHA-256:
`090177c076d3a49d44ba70573a514375061a5d9534e33c86ae227e1cd8a57082`.
The original export SHA differs because the import replaces segment hex with
byte counts/hashes; raw export SHA-256 is
`c25f0128890d0e3c0988c21ae9681a13c7b5a97fe1b26ad4ec28fbcd69406170`.
All fifteen actual native binary segments match their imported hex files.
The exact historical host runner is retained as `builder-runner.go.txt`;
its hash is
`62dbeced708b67d0e4d8983f58fd53fd491c127090a2e7b588dda70e5707e602`.
The runner's fixed temporary path is historical evidence, not a product API.

Independent audit:
`/private/tmp/request-v9-independent-audit-20261016.json`.
The owned builder was terminated and independently absent in Docker:
`/private/tmp/request-v9-native-cleanup-audit-20261016-first.json`.
This proves offline native encoding/decoding and the 41-byte header projection;
it is separate from live ProtocolV2 evidence below. Valid nonempty OTEL was
not generated, accepted as local input or claimed supported.

The published producer can be rerun in the pinned disposable image by mounting
`tools` read-only and an isolated native cache, then executing
`python3 /tools/osd_request_v9_native_encoder.py --build-native --work /native-cache/work --wrapper /tools/osd_request_v9_native_encoder.cc`.
Its stdout preserves the raw producer proof. Container ownership/termination
receipts are a separate host-runner responsibility. Never overwrite retained
first-attempt evidence when reproducing it.

## Product and synthetic regressions

The final source before daemon execution is
`050bb5c8b85dd36b52b15d9b1405c897b5db6aab`, based on main `648140e`.
CGO-disabled root tests/vet, nested fixture tests/vet and all 413 Python tests
passed. Focused race checks cover native v9 comparison, handshake, backoff,
watch reconnect and started mutation cancellation/Close.

Independent product review found a missing version selector in watch reconnect
preflight: it called the v8 encoder even for the new v9 profile and therefore
refused the full SQUID feature mask locally. The final source uses the selected
v9 codec. Regression checks actual REGISTER/PING/RECONNECT v9, generation7 to8,
failed preflight preserving old ownership/stream slot/TID, and superseding the
old owner only after the valid explicit reconnect. Started binary APPEND
cancellation and Close retain unknown outcomes/submission identity without
replay or another dial. The focused independent review passed after that fix.
An initial synthetic Ping fixture wrongly echoed generation0 instead of the
existing generation7; the fixture was corrected without a product workaround.

## Actual daemon acceptance

The new fixture profile `replicated-pg-request-v9-reply-v8` is explicitly
restricted to a single OSD and excludes MDS/RBD workflow opt-ins. The product
keeps caller-supplied actual PG/raw hash and NO_SHARD. The selected OSD tests
cover base I/O, PGNLS, backoff/late ACK, explicit watch reconnect, PG split,
strict reply-v8 behavior and the new v9 native read/write/frame observation.
MDS map tests are outside this focused OSD selection.

Both first daemon executions passed eight OSD top-level tests / sixteen leaf
cases, with exit 0 and no skipped selected cases. The actual linked provider
was v0.3.0 with checksum
`h1:LRXc5mVpqw+/xDRWsv+m8ouLzkVDtB385RoQZ/7ygOY=`. Both daemons ran Linux
arm64 and the pinned image above.

| First execution | Actual profile | New v9 focus | Full selected OSD cases |
| --- | --- | --- | --- |
| CRC | AES client/service, IPv4, host client, single BlueStore | PASS, 1.05 s | 16/16 PASS |
| Secure | AES256K client/service, IPv6, container client, single MemStore | PASS, 1.42 s | 16/16 PASS |

CRC captured eight actual transferred plaintext messages after authentication:
four request42/v9/compat3 and four response43/v8/compat2, TIDs1 to4. It checked
the full empty OTEL envelope, separate actual PG/raw hash, NO_SHARD, request
incarnation/epoch/features and PGNLS's empty oid/explicit hash. Captured response
front/data match the raw public replies. These are observed socket writes/reads,
not encoder-produced stand-ins. Secure traffic stayed encrypted; zero header
captures and `captured_crc=false` remain explicit. Its successful native I/O
observations are separate from CRC header visibility.

The new focused case issued STAT+READ, one APPEND, fresh READ and bounded PGNLS.
Native bytes changed from the 13-byte binary seed to exactly its 24-byte seed+
sentinel. Native own version matched Go's ONDISK reply (CRC6 to7, secure11 to12),
and mtime was 1700001400.123456789. The owned head was removed and the original
fixture object remained unchanged. Existing selected cases separately checked
signed refusals/earlier output, FAILOK metadata commit, watch event/ACK/UNWATCH,
explicit reconnect, backoff and late ACK ordering, started cancellation/Close
unknown outcomes and PG split/merge with original uncertain tuples preserved.
No uncertain mutation was automatically replayed by the product or test helper.

An independent raw-receipt audit passed both runs, including separate
little-endian parsing of the observed CRC prefix/body, original-epoch backoff
ACKs, late-ACK state, native PGNLS observations, unknown cancel/Close tuples,
signed refusal/FAILOK effects, split identity and explicit watch generations.
Audit records are the adjacent `-first-independent-audit.json` files for the
two original run directories below. Secure encrypted headers were not decoded.

Original artifacts and adjacent logs:

- `/private/tmp/request-v9-crc-aes-ipv4-blue-20261016-first`
- `/private/tmp/request-v9-secure-aes256k-ipv6-mem-20261016-first`

New v9 result SHA-256, respectively:
`e1b91432eff5c4d34f91548a9ace381196b2071f11551f4cd2fb33e311f9fe13` and
`4fd6e8837f9a5ca4563b699b2532ce6c3a4b2d54cf996a35b9771f8ffd7b4b7b`.
A separate read-only inventory confirmed both fixtures' sixteen containers,
the offline builder and two fixture networks absent:
`/private/tmp/request-v9-owned-cleanup-audit-20261016.json`.
The later test-log-only change replaces the reused hardcoded “v8” label with
the selected profile; no product/runtime behavior or retained run was changed.

The same two profiles are registered in CI. Complete final-SHA remote CI success
is not claimed. Valid nonempty OTEL, three-OSD v9 acceptance, mutation failover,
optimal CRUSH/EC, power-loss persistence, general routing and trace policy remain
outside these verified configurations. MemStore has no restart durability.

Reproduce a row with `CEPH_MSGR_TEST_OSD=1`,
`CEPH_MSGR_TEST_OSD_PROFILE=replicated-pg-request-v9-reply-v8`,
`CEPH_MSGR_TEST_OSD_REPLICAS=1`, that row's store/cipher/mode/family/runtime,
a unique diagnostics directory and the exact selector
`^TestCephOSD(ReplicatedPG|PGListV8|Backoff(LateAck)?|WatchReconnect|Split|ReplyV8|RequestV9)Integration$`
before running `sh integration/run.sh` from the project root.
