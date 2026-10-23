# msgr2.1 message compression — implementation plan

Status: 2026-10-20. snappy + zstd **implemented and unit-tested** (codec
round-trip + in-memory Writer/Reader frame round-trip through the real
preamble/CRC path, negotiation wired for CRC connections). **Live compressed
frames are unreachable from a client**: the pinned daemons decline compression
toward client peers, and only the declined-offer result is verified live (see
"Live finding"). Codec round trips, the declined-offer test and live compressed
frames are three distinct results. zlib and secure-mode compression are deferred. Reference: Ceph 20.2.4
commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`src/msg/async/ProtocolV2.cc`, `frames_v2.h`, `frames_v2.cc`,
`compression_onwire.cc`; LGPL-2.1, independently translated).

Implement only algorithms with a
**pure-Go** codec; defer only a format that would need CGO. zlib
(`compress/zlib`, stdlib), snappy and zstd (`github.com/klauspost/compress`,
pure Go) all qualify. CRC connections negotiate the implemented snappy and zstd codecs in
`internal/session/handshake.go`. Secure connections decline compression.

## Handshake frames (confirmed from source)

- `CompressionRequest` (Tag 21, after auth): `bool is_compress` (1 byte) then
  `std::vector<uint32_t> preferred_methods` encoded as a `u32` count followed
  by each method as `u32`, little-endian. The client offers supported methods on CRC connections; secure connections
  send `is_compress=0` with an empty method list.
- `CompressionDone` (Tag 22): `bool is_compress` (1 byte) then `u32 method`.
  The client accepts only an offered, implemented method and rejects
  unsupported selections. The
  server, in `handle_compression_done`, sets `COMP_NONE` if the two
  `is_compress` flags disagree.
- The request is only sent when the peer advertised the `COMPRESSION` msgr2
  feature (banner `Compression` bit, already advertised as supported here).

## Frame compression (confirmed from `frames_v2.cc`)

- `asm_compress()` compresses **each segment separately, including segment 0**,
  via `m_compression->tx->compress(segment_bls[i])`, swaps the compressed
  buffer in, and sets `m_descs[i].logical_len` to the **compressed** length. So
  the preamble's per-segment length is the onwire (compressed) length.
- The preamble `flags` byte gets `FRAME_EARLY_DATA_COMPRESSED` (`0x1`) when all
  segments compressed without abort; the receiver reads `m_flags = preamble.flags`
  and decompresses only when that bit is set. The flag is in the preamble, not
  the epilogue.
- Order: **compress-then-encrypt** on tx (`asm_compress` before `asm_secure_*`),
  **decrypt-then-decompress** on rx. Compression runs whenever the tx handler is
  set, independent of secure mode (the registry decides whether to enable it).
- `compression_onwire.cc` adds **no length prefix and no magic**: the onwire
  payload is exactly the codec's output, so each codec must be self-describing
  for decompression. `compressor_message` (optional int32) is unused.
- There is a minimum-size gate (`m_min_size`): small frames are sent
  uncompressed even when compression is negotiated. So the rx path must handle
  a mix of compressed and uncompressed frames (the per-frame flag says which).

## Algorithm ids and per-codec onwire format (confirmed from source)

`CompressionAlgorithm` (Compressor.h): `COMP_ALG_NONE=0, SNAPPY=1, ZLIB=2,
ZSTD=3, LZ4=4, BROTLI=5`. `CompressionMode`: `COMP_NONE=0, PASSIVE=1,
AGGRESSIVE=2, FORCE=3`. The onwire handler adds no framing of its own, so each
method's bytes are exactly its Compressor plugin's `compress()` output:

- **snappy (1):** raw snappy **block** format — `snappy::Compress` /
  `GetUncompressedLength`+`RawUncompress`. Self-describing (leading varint
  uncompressed length), **no extra prefix**. → Go: `klauspost/.../snappy`
  `Encode(nil, src)` / `Decode(nil, src)` (block API). Clean match.
- **zstd (3):** `ceph::encode((uint32_t)src.length(), dst)` (4-byte
  **little-endian** original length) **then** the zstd stream
  (`ZSTD_compressStream2`, standard frame). decompress reads the 4-byte LE
  length, then `ZSTD_decompressStream`. → Go: write `binary.LittleEndian` u32
  original length, then `zstd.EncodeAll`; on rx read the u32 then
  `zstd.DecodeAll`. No `compressor_message` used.
- **zlib (2):** writes **1 "variation mark" byte** (0 for zlib path, 1 for
  ISA-L) then the deflate stream; decompress **skips byte 0** and inflates.
  Format depends on `compressor_zlib_winsize`: negative (e.g. -15, ISA-L's
  value) = raw RFC1951 deflate; 8..15 = RFC1950 zlib. Window is also carried
  out-of-band via `compressor_message` (local, not onwire). **Deferred** until
  `compressor_zlib_winsize` default is confirmed (raw vs zlib) — then Go
  `flate` (raw) or `zlib` with a leading 0 byte.

**This round implements snappy + zstd only** (both unambiguous), and advertises
only those in `preferred_methods`, so the server picks one of them and zlib is
never selected until added.

Still to confirm for live verification: `src/compressor/CompressorRegistry.cc`
`get_mode`/`get_methods`/`get_min_compression_size` + `ms_*compress*` options to
enable compression on the disposable fixture, and whether secure connections
disable it by default (picks the test profile). The handler threads
`compressor_message` per direction but it is never sent onwire; snappy/zstd
ignore it.

## Implementation steps (once the above is confirmed)

1. Add one pure-Go dep for all three formats: **`github.com/klauspost/compress`**
   (`/zlib`, `/snappy`, `/zstd`). Chosen by the user's criteria 2026-10-20
   (maintenance → CPU-extension/SIMD → benchmark): `golang/snappy` is archived
   (deps.dev Maintained 0/10, Snyk Inactive); klauspost is actively maintained
   (changelog to 2026-02), uses amd64/arm64 assembly (opt-out `noasm`), and its
   `/snappy` is a drop-in for `golang/snappy` with the same block API. One
   module keeps the vulnerability-tracking surface small. License BSD-3-Clause
   (the `/snappy` subpackage keeps golang/snappy's BSD license) — verify the
   exact version's LICENSE on add. CGO-wrapping zstd libs (DataDog/zstd,
   valyala/gozstd) are disqualified. Its output is standard-format (RFC1950
   zlib, snappy block, zstd frame), so it interoperates with Ceph's codecs even
   though the compressed bytes differ.
2. `internal/msgr`: a codec registry mapping algorithm id → compress/decompress
   (zlib, snappy, zstd via klauspost), each matching Ceph's per-codec framing.
   **Use only the single-goroutine one-shot/block APIs** — zstd `EncodeAll`/
   `DecodeAll`, snappy block `Encode`/`Decode`, zlib `Writer` per segment —
   never the streaming `zstd.NewWriter`/S2 writers, which fan a stream across
   many goroutines (divide-and-conquer) and would spike CPU at the msgr layer
   for no benefit (frames/segments are small, and sub-min-size frames are not
   compressed at all). For zstd, create ONE shared `Encoder` and `Decoder` with
   concurrency 1 and a bounded window and reuse them across frames (EncodeAll/
   DecodeAll are safe for concurrent calls), rather than per-frame instances
   (buffer allocation is costly). Goal: per-frame compression stays "one frame,
   one core, briefly", with per-connection memory bounded.
3. `internal/session/handshake.go`: advertise the supported method ids in
   `CompressionRequest`; on `CompressionDone` store the negotiated method and
   min size on the `Transport` instead of rejecting.
4. `internal/msgr/frame.go` (and the frame reader/writer): on write, compress
   each segment when a method is set and the frame meets the min size, set the
   preamble `0x1` flag, and use compressed lengths; on read, when the flag is
   set, decompress each segment after decryption. Keep uncompressed frames
   working (min-size and negotiated-none paths).
5. (Superseded by "Live finding" below: not reachable from a client.) Live-verify against the native fixture with compression enabled: round-trip
   MON/OSD traffic, compare native observations, confirm the daemon accepts our
   compressed frames and we decode its compressed replies. Pick the profile by
   the secure-mode behavior found in item 5 above.

## Done (2026-10-20, commits bfcd296, e978fe6)

Steps 1-4 above are implemented and unit-tested:
- `go.mod`: `github.com/klauspost/compress v1.18.7` (only dependency; BSD-3).
- `internal/msgr/compression.go`: algorithm ids, snappy/zstd codecs matching the
  onwire formats above, shared concurrency-1 zstd encoder/decoder, Enable
  Compression on Reader/Writer, per-frame compress/decompress helpers.
- `internal/msgr/frame.go`, `secure.go`: preamble flags byte (bit 0) carried in
  buildPreamble/parsePreamble; Write compresses segments above `compMin` (512)
  before CRC/encrypt and sets the flag; both read paths decompress after
  decrypt/CRC and bound the decompressed total to the frame limit.
- `internal/session/handshake.go`: advertises `SupportedCompressionMethods` in
  CompressionRequest on CRC-mode connections only when the peer banner offers
  compression, enables the codec both directions
  when the server selects one, records it on `Transport.Compression`.
- Tests: `compression_test.go`, `frame_compression_test.go` (round-trip,
  format, below-min, multi-segment, mixed, unexpected-flag). Full unit suite
  green.

## Live observations and their boundary

The historical 2026-10-20 record reported OSD-peer compression configuration
(`ms_osd_compress_mode`, min size and algorithm, plus `ms_compress_secure`)
and no MON/MDS/client equivalent. After a force/zstd configuration and an OSD
restart, a CRC client's OSD connection still had no negotiated compression.
This supports a policy explanation; it is not a negotiation-frame capture.

`Transport.Compression` is exposed through the MON snapshot and
`OSDConnection.Compression()`. `TestCephMsgrCompressionOfferIntegration` checks
those values are empty and MON/OSD commands succeed. It does not observe the
peer banner compression bit, an enabled request or a disabled response, so
it cannot distinguish a declined offer from skipped negotiation. The dedicated
CRC/AES IPv4 host, single-BlueStore legacy-v6 CI row requires this parent to
PASS. The selected 2026-10-23 native run and its source are recorded separately
from the historical run in
[MSGR_COMPRESSION_VALIDATION.md](../../integration/fixture/MSGR_COMPRESSION_VALIDATION.md).

Live compressed-frame interoperability remains unverified. Codec/frame tests
provide synthetic evidence; the selected client's no-compression result does
not establish daemon-peer compression or every future native configuration.

## Remaining (optional, lower priority)

- zlib (confirm `compressor_zlib_winsize` default, then flate/zlib with the
  1-byte mark) and secure-mode tx compression — both only useful if a
  daemon-to-daemon or future client-peer compression path is ever exercised;
  not reachable from a client today.
