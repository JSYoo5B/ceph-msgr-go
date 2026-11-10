# Documentation map

This page is a navigation aid. It states which document owns each kind of
information, how to follow one feature from its public contract to its
observed native result, and which path answers three common reader questions.
It defines no contract and records no result. When this page or any summary
differs from the owning document named below, the owning document is correct.

The protocol reference for every document is Ceph `v20.2.4`, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.

## Readers and entry points

### Package users

Start with [README.md](README.md):
[which side of the connection Go implements](README.md#which-side-of-the-connection-go-implements),
[installation](README.md#installation), the first connection in
[Quick start](README.md#quick-start),
[representative operations](README.md#representative-operations) and
[limitations](README.md#limitations). The Go documentation of package
`cephmsgr` ([api.go](cephmsgr/api.go)) states context, lifetime and
unknown-outcome rules. Each README section links the public contract of its
area.

### Protocol implementers

Start with [PROTOCOL_EVIDENCE.md](cephmsgr/PROTOCOL_EVIDENCE.md). It joins
every frame tag, client message, implemented OSD opcode and role feature
profile to a caller boundary, a unit selector and an attributed native receipt
or an explicit gap. From a row, follow
[Tracing one feature](#tracing-one-feature). Requirements and completion
criteria are in [SPEC.md](SPEC.md).

### Fixture and CI contributors

Start with the [native path coverage map](integration/fixture/NATIVE_PATH_COVERAGE.md):
one row per native path of the
[final test-environment scope](SPEC.md#final-test-environment-scope), with its
setup, independent observation, fault, cleanup, catalog scenario and retained
record. Scenario selection, the result gate and the inventories are in the
[scenario README](.github/scenarios/README.md); static analysis, vulnerability
checks and structural reports are in [QUALITY.md](.github/QUALITY.md). The
fixture runner is the separate module in `integration/fixture`, started by
[integration/run.sh](integration/run.sh).

## Authoritative locations

Each kind of information has one owner. Other documents may summarize it and
must follow the owner.

| Kind of information | Owner | Kept consistent by |
| --- | --- | --- |
| Product scope, priorities, acceptance and verification requirements | [SPEC.md](SPEC.md); working rules in [AGENTS.md](AGENTS.md) | review |
| Usage, feature boundaries and limitations for package users | [README.md](README.md) and the Go documentation in `cephmsgr/` | review |
| Public API contract of one area | `cephmsgr/*_CONTRACT.md`, linked from the matching README section | review |
| Wire contract of one area | the contract beside the implementation: `internal/osd/*_CONTRACT.md`, [internal/mds/CONTRACT.md](internal/mds/CONTRACT.md), [MDS_FRESH_CONTRACT.md](internal/session/MDS_FRESH_CONTRACT.md), [MDS_SERVICE_CONTRACT.md](internal/cephx/MDS_SERVICE_CONTRACT.md), [COMPRESSION_PLAN.md](internal/msgr/COMPRESSION_PLAN.md) | review; each contract cites the pinned source files |
| Message types and header versions | [MESSAGE_LEDGER.md](cephmsgr/MESSAGE_LEDGER.md) | `TestMessageLedgerCoversEveryType` |
| Frame tags | [FRAME_TAG_LEDGER.md](internal/session/FRAME_TAG_LEDGER.md) | `TestFrameTagLedgerCoversEveryTag` |
| Messenger feature masks by role | [FEATURE_LEDGER.md](internal/session/FEATURE_LEDGER.md); bit meanings in [FEATURE_SEMANTICS.md](internal/session/FEATURE_SEMANTICS.md) | `TestFeatureLedgerMasks`, `TestFeatureSemanticsCoversEveryAdvertisedBit` |
| CephFS client feature bits: declared or withheld per profile, and the decision for each withheld bit | [CLIENT_EXTENSION_DECISIONS.md](internal/mds/CLIENT_EXTENSION_DECISIONS.md) | `TestClientExtensionDecisionsCoverEveryPinnedFeature` |
| CephX exchanges | [EXCHANGE_LEDGER.md](internal/cephx/EXCHANGE_LEDGER.md) | review |
| OSD opcode implementation status | [opcodes-tentacle.csv](internal/osd/opcodes-tentacle.csv); unsupported entries in [UNSUPPORTED_OPCODES.md](internal/osd/UNSUPPORTED_OPCODES.md) | `TestUnsupportedOpcodesAreClassified` |
| Raw command channels and their selectors | [COMMAND_CHANNEL_MATRIX.md](cephmsgr/COMMAND_CHANNEL_MATRIX.md) | `TestCommandChannelMatrixSelectorsExist` |
| Join of a wire row to its unit selector and native receipt; attribution category of each receipt | [PROTOCOL_EVIDENCE.md](cephmsgr/PROTOCOL_EVIDENCE.md) | `python3 tools/protocol_evidence.py` |
| Provenance of retained native codec vectors | the `testdata/*README.md` files and provenance records beside each package's vectors | the native-vector tests of that package |
| Scenario definitions and required rows | [catalog.json](.github/scenarios/catalog.json), `mandatory.txt` and `native-tests.txt` beside it; rules in the [scenario README](.github/scenarios/README.md) | `python3 tools/scenario_plan.py validate`, `TestCICatalogInventory` |
| Classification of native recipes per path, and where each record is | [NATIVE_PATH_COVERAGE.md](integration/fixture/NATIVE_PATH_COVERAGE.md) | `python3 tools/native_path_coverage.py` |
| Reproducible fixture setup and control | `integration/fixture/*_FIXTURE.md`, `*_RECIPE.md` and `*_ORACLE.md` | review |
| An observed native result: tested source, image, profile, outcome and what was not verified | the `integration/fixture/*_VALIDATION.md` record of that run | review; records are not rewritten by later changes |
| Successful native evidence bundle format | [NATIVE_EVIDENCE.md](.github/scenarios/NATIVE_EVIDENCE.md) | its validation record beside it |
| Licensing and source provenance | [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md), [NOTICE](NOTICE), [LICENSE](LICENSE) | review |

The automated checks validate references and inventories. None of them runs a
native test or establishes that an observation is true; the validation record
and its attributed source do.

## Tracing one feature

Follow these steps in order. Each step names the document that owns the
answer.

1. **Public contract.** Find the exported name in `cephmsgr/` and its Go
   documentation, then the public contract linked from the README section of
   that area. This states inputs, bounds, refusals and outcomes.
2. **Pinned wire reference.** Open the wire contract beside the
   implementation. It cites the pinned Ceph source files and states the exact
   encoding and the subset that is admitted. Message versions, frame tags and
   feature bits are in their ledgers.
3. **Implementation status.** Find the row in
   [PROTOCOL_EVIDENCE.md](cephmsgr/PROTOCOL_EVIDENCE.md): message, frame,
   opcode or feature profile. An OSD opcode is `implemented` or `unsupported`
   in [opcodes-tentacle.csv](internal/osd/opcodes-tentacle.csv). A CephFS
   client feature bit is declared or withheld per profile in
   [CLIENT_EXTENSION_DECISIONS.md](internal/mds/CLIENT_EXTENSION_DECISIONS.md).
   The row also names the unit or native-vector selector.
4. **Scenario.** Find the path in
   [NATIVE_PATH_COVERAGE.md](integration/fixture/NATIVE_PATH_COVERAGE.md). Its
   Evidence column names the catalog scenario ID or group and the Go test;
   its Visibility column says whether a dedicated row requires the test or a
   broad row may skip it. The scenario itself is defined in
   [catalog.json](.github/scenarios/catalog.json).
5. **Observed native result.** Open the validation record linked from the
   coverage row. The receipt row in
   [PROTOCOL_EVIDENCE.md](cephmsgr/PROTOCOL_EVIDENCE.md#native-receipts)
   states its attribution category (exact commit, file hashes or historical
   source), the profile and the actual boundary of the observation in one
   line. Receipts are not all live Go exchanges: some are offline native
   encoder vectors, and the entry says so. A receipt belongs to the source
   and profile it names and to no other.
6. **Remaining limits.** Read the "Not verified" section of the validation
   record and the limits of the wire contract. Product gaps across areas are
   listed under
   [Product wire gaps](integration/fixture/NATIVE_PATH_COVERAGE.md#product-wire-gaps-separate-from-environment-completeness).

Two distinctions apply at every step:

- Unit tests, native codec vectors, live daemon interoperability and CI are
  different evidence. A native codec vector shows that encodings match; it is
  not a live session.
- A native-only recipe and a Go-originated recipe for the same operation are
  different rows. A native-only recipe shows what the daemon does when a
  native client sends the operation. Only the Go-originated row is evidence
  for the Go request.

## Reader tasks

These three tasks were used to check the navigation. Each lists the click
path and the answer the owning documents give.

### Establish a client connection

1. [README.md](README.md), "Where to start", package user row.
2. [Installation](README.md#installation): the import path and Go version.
3. [Quick start](README.md#quick-start): `ParseKey`, `Dial` with MON
   addresses, an exact identity and `ExpectedFSID`, then `MonCommand`; the
   secure or CRC mode choice follows the example.
4. [Lifetime and recovery](README.md#lifetime-and-recovery): seed address
   forms, FSID pinning, release admission, limits and ticket renewal.
5. For authentication details, the
   [CephX exchange ledger](internal/cephx/EXCHANGE_LEDGER.md); for the masks a
   MON session advertises, the
   [feature ledger](internal/session/FEATURE_LEDGER.md).

### Can Go take an OSD's place in peering?

No. The path that establishes this:

1. [README.md](README.md#which-side-of-the-connection-go-implements): the
   product is always the connecting CLIENT entity and implements no daemon
   role.
2. [SPEC.md](SPEC.md#final-test-environment-scope): peering is a
   daemon-to-daemon path of the test environment, and daemon-to-daemon
   fixtures use actual Ceph daemons without requiring the product to implement
   Ceph servers.
3. [FRAME_TAG_LEDGER.md](internal/session/FRAME_TAG_LEDGER.md), "Boundaries":
   server-side frame tags are not implemented and there is no listening
   Messenger. [MESSAGE_LEDGER.md](cephmsgr/MESSAGE_LEDGER.md) lists every
   message type in the product tree and states that the client does not
   implement daemon peer roles; it contains no peering message.
   [EVENT_CONTRACT.md](internal/osd/EVENT_CONTRACT.md) states that
   replication, recovery, peering and heartbeat messages are fixture
   requirements, not client codecs.
4. [NATIVE_PATH_COVERAGE.md](integration/fixture/NATIVE_PATH_COVERAGE.md#daemon-to-daemon):
   rows D2D-10 and D2D-11 show how peering between real OSDs is observed
   (outcome only locally; per-step peering has no recipe).

### Trace an offset-zero Go SET_CHUNK request

1. **Public contract.** `OSDSetChunk` and `OSDChunkInput` in
   [osd_request.go](cephmsgr/osd_request.go), reached from
   [OSD object requests](README.md#osd-object-requests), "Snapshots and cache
   flags", manifest.
2. **Pinned wire reference.**
   [MANIFEST_CONTRACT.md](internal/osd/MANIFEST_CONTRACT.md#set_chunk-0x8228)
   for the native construction and server behavior, and
   [Go SET_CHUNK contract](internal/osd/MANIFEST_CONTRACT.md#go-set_chunk-contract)
   for the admitted shape: one standalone operation, source offset zero,
   exactly the WITH_REFERENCE flag, READ request mode, a target other than
   the object itself. A non-zero source offset is refused before transmission.
3. **Implementation status.** The SET_CHUNK row under
   [Implemented opcode evidence](cephmsgr/PROTOCOL_EVIDENCE.md#implemented-opcode-evidence):
   wire value `0x8228`, unit selectors
   `TestSetChunkRetainsFreshNativeRequests` (retained native request vectors,
   described in
   [set-chunk-native-README.md](internal/osd/testdata/set-chunk-native-README.md))
   and `TestSetChunkRefusalsBeforeEncoding`, native receipt R27.
4. **Scenario.** Row OSD-26 under
   [CLIENT to OSD](integration/fixture/NATIVE_PATH_COVERAGE.md#client-to-osd):
   scenario
   `osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-osdsetchunkgo`,
   test `TestCephOSDSetChunkGoIntegration`, a dedicated row. Row OSD-24 beside
   it is the native-only checkpoint (`TestCephOSDSetChunkNativeIntegration`):
   a native librados client issues SET_CHUNK there and no Go client takes
   part, so it is not evidence for the Go request.
5. **Observed native result.**
   [OSD_SET_CHUNK_GO_VALIDATION.md](integration/fixture/OSD_SET_CHUNK_GO_VALIDATION.md#results),
   summarized as receipt R27 under
   [Native receipts](cephmsgr/PROTOCOL_EVIDENCE.md#native-receipts): one run
   at one named commit on one profile (secure, aes256k, IPv6, container,
   legacy-v6, one BlueStore OSD).
6. **Remaining limits.**
   [Not verified](integration/fixture/OSD_SET_CHUNK_GO_VALIDATION.md#not-verified)
   in the same record: other profiles, a non-zero source offset, scoped
   identities, snapshot clones and a live lost reply. The lost-reply boundary
   is synthetic only
   ([Lost reply](integration/fixture/OSD_SET_CHUNK_GO_VALIDATION.md#lost-reply)).

## Keeping summaries consistent

- Change the owning document first, then every summary of it. The README,
  the SPEC evidence table and the "Product wire gaps" list of the coverage map
  are summaries.
- Keep the attribution of a native result when summarizing it: the tested
  commit or historical source label, the profile and the scenario. Do not
  merge observations from different profiles into one claim.
- A summary may be narrower than its owner. It must not state `verified`
  where the owner states `unverified`, synthetic or source-derived.
- Validation records describe one run. Later changes add a new record or a
  dated note; they do not rewrite an earlier observation.
- Paths of contracts and records are cited from code comments, tests,
  ledgers and other records. Keep a cited path stable, or update every
  citation in the same change.
