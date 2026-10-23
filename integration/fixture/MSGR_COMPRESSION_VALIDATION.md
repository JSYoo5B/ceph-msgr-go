# CRC compression selection and native observations

Protocol reference: Ceph Tentacle v20.2.4 at
`7f793731f1b39eb4f465e960113d2363c311b964`.
Image:
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The implementation plan and onwire format are in
[COMPRESSION_PLAN.md](../../internal/msgr/COMPRESSION_PLAN.md).

## Implementation and observation boundary

The product implements snappy/zstd negotiation on CRC connections only when
the peer banner advertises compression, plus frame compression/decompression
with `FRAME_EARLY_DATA_COMPRESSED`. Codec and frame unit tests are synthetic
evidence. zlib and secure-mode compression remain deferred.

`TestCephMsgrCompressionOfferIntegration` observes the negotiated algorithm
through the MON snapshot and OSD connection, then checks a MON `status` and
an OSD `version` command. Empty algorithms and working commands do not
independently distinguish a declined CompressionRequest from negotiation
being skipped because the peer banner lacks compression. This test captures
neither the peer compression bit nor CompressionRequest/CompressionDone
frames. It does not retain authentication transcripts or keys.

## Historical observation, 2026-10-20

The previous record described a container-runtime, CRC/AES client and service
cipher, IPv4, legacy-v6 single-BlueStore run, but retained no exact tested
source tree or file hashes. MON and OSD reported no negotiated compression
and commands succeeded. That observation remains historical rather than
being attributed to the new source or run below.

The record also reported native `config ls` results for the OSD peer options
`ms_osd_compress_mode`, `ms_osd_compress_min_size`,
`ms_osd_compression_algorithm` and `ms_compress_secure`, and no corresponding
MON/MDS/client peer options. A native OSD restart with force/zstd still gave
an empty algorithm on a CRC client connection. This is a configuration
observation and source-policy explanation, not a captured disabled response
or live compressed-frame receipt. The old wording that both daemons
necessarily declined an observed request was stronger than these retained
observations establish.

## Selected native execution, 2026-10-23

Source: `41cfa9f2f3f8ff8d797197047b7b1e0ea2524673`, attempt **1**.
This is the implementation commit before this receipt was added. PR CI and
its final head are recorded separately in the PR, rather than assigning this
run to a later documentation commit.

| Input | Executed value |
| --- | --- |
| Scenario | `osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-msgrcompressionoffer` |
| Provider | `github.com/JSYoo5B/ceph-testcontainers-go v0.3.0` |
| Platform and Go | Linux/amd64, Go 1.26.9 |
| Connection and credentials | CRC, AES client key and AES service cipher |
| Address and runtime | IPv4, host runtime |
| Storage | One BlueStore OSD; replicated pool size/min-size one; legacy-v6 |
| Native daemons | Three MONs, two MGRs, one OSD; pinned image/source above |
| Selector | `^TestCephMsgrCompressionOfferIntegration$` |

The parent actually reported PASS in **0.21 seconds**, without SKIP. The
fixture process exited zero in **53.55 seconds**, including setup and owned
teardown. The existing native result gate inventoried exactly
`api/TestCephMsgrCompressionOfferIntegration`; it observed that PASS and no
problems. An empty secondary client suite did not substitute for the parent.

Both separately logged observations passed:

- MON negotiated algorithm empty; `status` returned code 0 and nonempty data.
- OSD negotiated algorithm empty; `version` returned code 0 and nonempty data.

After orderly fixture cleanup, individual Docker inspections confirmed all
**eight owned containers** and the **owned network** absent. No unrelated
containers were removed or host configuration changed. Credentials, private
addresses, ownership IDs and diagnostic paths are intentionally not part of
this public receipt.

The same source passed 637 Python tool tests, catalog/mandatory validation
(77 native rows), pure-Go build/dependency checks, product and integration
vet/unit tests, fixture unit/vet checks and the product unit race suite on
Linux. These checks do not replace the live native parent PASS above.

## Selection and required-result regressions

The catalog and mandatory list include the dedicated row, without broadening
existing suites or adding a workflow parameter. `TestCompressionOfferCatalogProfile`
loads that actual catalog row through the fixture's `loadProfile`, checking
CRC, enabled OSD, exact selector, image, credentials, runtime, store and
single-OSD pool topology. Python checks bind the row to only its intended
integration parent. The existing native result gate's controls reject SKIP,
missing parent, unrelated PASS, wrong suite and child-only PASS.

## Not claimed

No live compressed-frame interoperability, observed disabled negotiation
response, secure compression, zlib, daemon-peer role, new provider/image
capability or other runtime/key/address profile follows from this run.
