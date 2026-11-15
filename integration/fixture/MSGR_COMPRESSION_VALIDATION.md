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
being skipped because the peer banner lacks compression. Since the
2026-11-15 change below the test therefore also reads the negotiation back
from the bytes of the connections that served those commands: the peer
compression bit, the client's CompressionRequest and the daemon's
CompressionDone. It keeps and logs tags and those decoded fields only; it
does not retain authentication transcripts, keys, addresses or messages.
The two executions before that date observed the algorithm and the commands
only.

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

## Negotiation read back from the serving connections, 2026-11-15

Source: `21e71bbf73b0667b5b039b4a9f6994929bc088e6`, attempt **1**, from an
archive of that commit. Same scenario, selector, pinned image, provider and
topology as the 2026-10-23 execution; Linux/amd64, Go 1.26.9, Docker 29.9.1.

The subject is a client of its own whose dialer copies, per connection and
direction, every byte it writes and reads
(`integration/compression_exchange_test.go`). A CRC-mode connection carries
its frames in the clear, so after both commands returned the test reads each
copy back as a banner followed by frames. It requires, on every connection to
a MON and every connection to the owned OSD that the client still has open:

| Observation | Required | Observed, MON | Observed, OSD |
| --- | --- | --- | --- |
| Peer banner supported mask | compression bit set | `0x3` | `0x3` |
| Client banner supported mask | compression bit set | `0x3` | `0x3` |
| Client tag 21 (CompressionRequest) | enabled, methods `[3, 1]` (zstd, snappy), directly after the client's AUTH_SIGNATURE | enabled, `[3 1]`, after tag 7 | enabled, `[3 1]`, after tag 7 |
| Peer tag 22 (CompressionDone) | disabled, method 0, directly after the peer's AUTH_SIGNATURE | disabled, 0, after tag 7 | disabled, 0, after tag 7 |
| Frames after the negotiation | at least one in each direction, none with the compressed-frame flag | 6 sent, 4 received, 0 compressed | 3 sent, 2 received, 0 compressed |

The client dialed two connections in that run, one to a MON and one to the
owned OSD, and both were open when they were read. The MON `status` and the
OSD `version` command had returned code 0 with nonempty data before, so each
travelled on a connection of this table. A connection the client had already
closed (a MON it tried and did not keep) would be logged and decide nothing;
none occurred. The parent reported PASS in 0.21 seconds and the result gate
found exactly that parent, without problems. After teardown no owned
container remained on the engine.

The refusal is what the pinned source prescribes: the daemon takes the
compression policy from the connecting peer's entity type and has none for a
client, also for a client connected to an OSD
([response](https://github.com/ceph/ceph/blob/v20.2.4/src/msg/async/ProtocolV2.cc#L3094-L3123),
[policy](https://github.com/ceph/ceph/blob/v20.2.4/src/msg/compressor_registry.cc#L109-L127)).
No OSD-peer compression option was changed for this observation.

### Controls

Two temporary changes of the product source, each run on the same row from
the working tree of `21e71bb` and then discarded:

| Change | Result |
| --- | --- |
| The client sends CompressionRequest with compression disabled and no method | The parent failed on both connections: "the CompressionRequest was enabled=false methods=[], not an offer of zstd then snappy". The daemon answered with the same disabled CompressionDone, and both commands had succeeded, so the earlier form of the test would have passed |
| The client skips the negotiation although the peer banner carries the bit | The fixture's first client could not be admitted: each MON closed the connection during session identification (`EOF`). The run therefore never reached the subject's check. It shows that a Tentacle daemon whose banner carries the bit does not accept a client that omits the request |

`TestCompressionExchangeReadsOneConnection` (cluster-free) covers the reader
without a daemon: a stream delivered one byte at a time gives the same copy
and the same result, a stream cut inside a frame concludes nothing, and each
of fifteen missing or different observations is reported (a banner without
the bit on either side, no request, a disabled or reordered offer, no answer,
an enabling answer, a refusal that names a method, a request before the
signature, one direction only, a compressed later frame, bytes that are not
frames). It also reads the archived native MON capture
(`internal/msgr/testdata/ceph-mon-hello-v20.2.4.bin`), which stops before
authentication: the banner bit is found and no refusal is concluded. The
other streams of that test are written by the Go frame writer and are
synthetic.

### Limits of this observation

- One profile: CRC, AES client key and service cipher, IPv4, host runtime,
  one BlueStore OSD. Secure mode is not readable this way, and the client
  does not offer compression there.
- The frames after the negotiation are those the client had exchanged when
  the copies were read, not the whole life of the connection.
- The reader checks the preamble and segment CRCs of each frame and decodes
  the two negotiation payloads. It does not decode any other payload, so it
  does not attribute a later frame to a particular command.
- A daemon that enables compression toward a client was not observed; the
  enabling path remains covered by synthetic tests only.

## Selection and required-result regressions

The catalog and mandatory list include the dedicated row, without broadening
existing suites or adding a workflow parameter. `TestCompressionOfferCatalogProfile`
loads that actual catalog row through the fixture's `loadProfile`, checking
CRC, enabled OSD, exact selector, image, credentials, runtime, store and
single-OSD pool topology. Python checks bind the row to only its intended
integration parent. The existing native result gate's controls reject SKIP,
missing parent, unrelated PASS, wrong suite and child-only PASS.

## Not claimed

No live compressed-frame interoperability, secure compression, zlib,
daemon-peer role, new provider/image capability or other runtime/key/address
profile follows from these runs. The disabled negotiation response is
observed on the one profile of the 2026-11-15 section only.
