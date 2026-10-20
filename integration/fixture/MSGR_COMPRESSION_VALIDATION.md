# msgr2.1 message compression verification

Run 2026-10-20, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture, container runtime. Implementation plan and onwire
format: `internal/msgr/COMPRESSION_PLAN.md`.

## What is implemented

snappy and zstd onwire compression (negotiation on CRC-mode connections, and
the frame compress/decompress path with the preamble `FRAME_EARLY_DATA_COMPRESSED`
flag). zlib and secure-mode compression are deferred.

## Why live codec interop is not exercised

Ceph 20.2.4 exposes onwire-compression config **only for the OSD peer type**:
`config ls` shows `ms_osd_compress_mode` (enum `none`/`force`, default `none`,
services `[osd]`, desc "Compression policy to use in Messenger for communicating
with OSD"), `ms_osd_compress_min_size`, `ms_osd_compression_algorithm`, and
`ms_compress_secure` (default `false`). There is **no** `ms_mon_*`, `ms_mds_*`
or `ms_client_*` compress option.

Because the policy is indexed by the *peer* type, a daemon applies it only when
its peer is an OSD. A client's connection is a client peer, so the daemon uses
no policy (none) and **declines compression toward a client**. Confirmed
empirically: with `ms_osd_compress_mode=force` and `ms_osd_compression_algorithm=zstd`
set and the OSD restarted (the running daemon reports `force`/`zstd`), a CRC Go
client's OSD connection still negotiated **no** compression. So a client library
cannot make a real daemon compress toward it, and the compressed-frame decode
path cannot be driven live from a client.

The codec and frame paths are therefore verified by the `internal/msgr` unit
tests, which round-trip real frames through the preamble/CRC/flag path for both
methods (`compression_test.go`, `frame_compression_test.go`).

## Live test (`TestCephMsgrCompressionOfferIntegration`)

What *is* verified live: the compression offer is production-safe. On a CRC
connection the Go client advertises snappy/zstd in CompressionRequest; the MON
and an OSD both decline (`Snapshot().Monitor.Compression` and
`OSDConnection.Compression()` are ""), and MON and OSD commands still succeed.
This confirms the handshake change does not break real connections. Passed in
legacy-v6 single BlueStore, CRC mode with AES key and service cipher, IPv4.

## Not claimed

Live compressed-frame interop (unreachable from a client as above); zlib;
secure-mode compression; daemon-to-daemon (OSD peer) compression paths.
