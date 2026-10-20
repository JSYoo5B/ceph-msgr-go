# Explicit MON filesystem map subscriptions — 2026-10-15

`TestCephMDSMapsIntegration` passed once in each of the two final configurations
below. Each run also passed 12 selected OSD leaf cases on the same disposable
fixture: 13/13 selected leaf cases and 7/7 top-level tests per configuration,
26/26 leaf cases and 14/14 top-level tests across the two final runs. These are
targeted development results, separate from whole-project CI, the three earlier
failed attempts, the native CLI smoke and independent binary vectors.

## Configuration and evidence boundary

The selected daemon is Ceph Tentacle 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`, Linux arm64, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The disposable recipe requires a single OSD with `replicated-pg-v8`, initially
no filesystem or standby, and no MDS daemon. The CLI creates a UUID filesystem
and two UUID pools, then removes only confirmed owned identities.

`EnableMDSMaps` is selected before Dial. This fixture separately requires
`EnableOSD` and `OSDProfileReplicatedPG` for its hashpspool admission contract;
that option requests an OSD ticket. The integration test does not open an OSD
connection, request an MDS service ticket, or establish an MDS session. The
fixture's OSD admission requirement is not an MDS-map protocol requirement.

The live independent comparison is complete native `fs dump` and `fs get` JSON:
FSID, FSCID, filesystem name, distinct FSMap/MDSMap epochs, pool IDs, enabled
state and empty daemon/rank tables. The pinned MON offers no `fs getmap` or
`mds getmap` command producing the exact subscription blobs. RawFront/RawMap
retention and envelope checks therefore do not claim live CLI blob equality.
FSMapUser contains no cluster UUID; MMDSMap carries a cluster UUID but no FSCID.
The public MDSMap filesystem ID is the caller's local subscription binding.
FSMapUser and MDSMap can legitimately have different local source generations
if authentication renewal replaces the MON connection between observations.
Each generation must be nonzero; equality is not a cross-map protocol contract.

## First failures and correction

The first two failures are preserved rather than replaced by subsequent passes.
Their retained logs show termination of all eight containers listed in each
ownership receipt. Neither failure established public subscription support.

## Corrected independent binary evidence

[The retained native provenance](../../internal/mds/testdata/mds-map-native-provenance.json)
records five newly constructed vectors: empty and multiple-filesystem FSMapUser,
and empty, populated and unknown-metadata MDSMap. The proof records canonical
encoding, separate native decoding, the pinned source/submodules, generated
configuration, native library/loader and binary hashes, and completed builder
termination. Its SHA-256 is
`eb360cd83c44a7a40581515ddb73642889ec0dfb51e1da3b17c5a84044d97985`.

The canonical native headers are type 103/version 1/compat1 and type 21/version
2/compat1. Inner FSMapUser is struct 1/compat1; current MDSMap is struct
5/compat4 with extension 19, and populated daemon records are struct 10/compat4.
These are binary codec vectors, including populated ranks and unknown metadata;
they are independent of the live fixture's empty filesystem. The explicit
offline sequence and ACK inputs are not live transport execution evidence.
The producer and audit boundary are described in
[the wire contract](../../internal/mds/CONTRACT.md).

## Final combined native runs

Independent examination of this failed run's raw JSON confirmed FSID
`a407f9b0-7651-4f42-82a7-a92a55b32a2f`, FSCID 1, FSMap epochs 1 → 2,
MDSMap epoch 2, metadata/data pool IDs 3/4 and local source generation 1.
Raw fronts decoded to the native name and pools, both cancellation flags and
the same-MON binding refusal were true, and unknown FSCID 2 timed out locally
while the fresh client's ordinary MON command succeeded. `MONPreserved=false`
records the later failing assertion. Deferred cleanup still confirmed native
filesystem removal, empty FSMap epoch 3/default FSCID -1, no unresolved mutation
or cleanup errors, and two pools retained under the unchanged deletion policy.
All 52 native commands succeeded without timeout, and all eight owned
containers have matching termination lines. The partial run's Go JSON SHA-256
is `beaf7c5fbcacc69aad70e0622e4cd1ec7a66d0cf3452752208ff19047e962655`;
its log SHA-256 is
`b5e103e5778da23db2b5dc21147cd3391f4aa931ef10ff47bef8490eb2390137`.

The corrected test records safe initial/final OSD ticket metadata, allows
legitimate renewal, and counts OSD dial attempts across both clients, requiring
zero. It preserves identity and MON readiness without requiring the physical
MON endpoint to remain unchanged during renewal. A same-source MDS rebind must
be refused; if renewal admits a new physical source, the test requires a
strictly newer local generation and the same native filesystem identity before
closing it and checking its binding fence. This path is bounded to three
attempts and a three-second map wait per new source. Ticket TTL is unchanged;
there is no forced MON reconnect.

### Executed final configurations

| Configuration | MDS case | Combined selected result |
| --- | --- | --- |
| AES client/service keys, CRC, IPv4, host client, one BlueStore OSD | 1/1 PASS, 12.74 seconds | 13/13 leaf cases, 7/7 top-level tests PASS |
| AES256K client/service keys, secure, IPv6, container client, one MemStore OSD | 1/1 PASS, 13.62 seconds | 13/13 leaf cases, 7/7 top-level tests PASS |

Both used fixture provider v0.2.0 and the pinned image above; each manifest's
running image ID matches the pinned digest. The other 12 leaf cases are two
BLOCK/ACK/UNBLOCK cases (including late ACK), three explicit-PG list cases,
five replicated-PG operation cases, one SPLIT case and one explicit WATCH
reconnect case. They are six other top-level tests, rather than twelve additional
MDS cases. Both retained logs contain the API-suite PASS and the subsequent
fixture check PASS, followed by termination of the owned containers.

### Independent map, identity and lifetime checks

| Observation | CRC/AES | Secure/AES256K/IPv6 |
| --- | --- | --- |
| Cluster FSID | `88c976ad-ec0e-4ad2-9d15-9a1799f8937f` | `ee99a1f2-bed3-4996-a303-dd1f585a7c5e` |
| Ownership token | `86cc4e70fe6c467c88cd1bc51485d30e` | `8a5d27f8785c4ffbbcb0d2a1a72b3f2f` |
| Native FSCID | 1 | 1 |
| FSMap epochs | 1 → 2 → 3 | 1 → 2 → 3 |
| Per-filesystem MDSMap epoch | 2 | 2 |
| Metadata/data pool IDs | 3 / 4 | 3 / 4 |
| Initial/created FSMapUser source | 1 / 1 | 1 / 1 |
| MDSMap / removed-FSMap source | 1 / 2 | 1 / 2 |
| OSD dial attempts across both clients | 0 | 0 |

The full UUID filesystem name in each run is `ceph-msgr-m0-<ownership token>`;
the metadata and data pool names append `-meta` and `-data`. These are separate
clusters with separate UUID names even where the assigned numeric IDs coincide.

The retained Go JSON was checked independently of the test's PASS lines. The
22-byte initial FSMapUser front decoded to epoch 1, legacy FSCID -1 and no
filesystem. The 81-byte creation front decoded to epoch 2, legacy FSCID 1 and
exactly the native FSCID/name pair. Each nested map equaled RawFront after its
outer epoch. The MDSMap front independently decoded to the native cluster UUID,
epoch 2, the complete 638-byte struct5/compat4 map with extension 19 and the
retained trailing wire name. Its raw base metadata decoded to data pool 4,
metadata pool 3, enabled=true, the native filesystem name, and empty daemon,
in/incarnation/up/failed/stopped/damaged collections. Native `fs get` equaled the
filesystem entry in the stable `fs dump`, and both native JSON results agreed
with the received typed fields. The final 22-byte FSMapUser front decoded to
epoch 3, legacy FSCID -1 and an empty directory. Raw map slices and all checked
lengths matched their enclosing fronts.

Both receipts record local Next cancellation, stream lifetime cancellation,
same-MON MDS binding refusal, ordinary MON command success on the primary and
unknown-FSCID clients, and preserved MON identity/readiness. Unknown positive
FSCID 2 returned no map during its one-second wait and ended with local
`context deadline exceeded`; no fallback filesystem was selected. Each receipt
records initial and final fixture OSD tickets with advanced expiry/renewal
times, and zero OSD dial attempts. The received removal map's local source
generation 2 also demonstrates why paired observations must allow renewal.
The initial and final peer endpoint happened to be the same in each run; that
is not a promise that authentication renewal preserves the physical connection.
`RenewedMONBindingAdmitted=false` in both cases: the optional new-source
admission branch was not exercised natively by these runs.

### Cleanup and retained evidence

For each final run, all 52 native commands completed with code zero and no
timeout. The four mutation receipts were exactly two UUID pool creations,
`fs new` and the confirmed UUID filesystem's `fs rm`. No pool-delete command or
global config-setting command was sent. Finish recorded `finished=true`, no
unresolved mutation, an empty native FSMap epoch 3/default FSCID -1 and no Go
cleanup errors. Native pool deletion was disabled, so both confirmed UUID pools
were explicitly retained for disposable fixture teardown. This is not a claim
that native pool deletion was accepted.

Each ownership receipt lists eight distinct containers, with no MDS daemon.
Every listed ID has a matching `Container terminated` line in its retained log,
including the OSD and native oracle/test container. No cleanup omission was
found. This is artifact-backed termination evidence; it is not a separate
post-run Docker or network inventory.

| Evidence | CRC/AES SHA-256 | Secure/AES256K/IPv6 SHA-256 |
| --- | --- | --- |
| `mds-maps-go.json` | `4c079fe2527e0d69201d399f6621e5f0ab1208cb07983edeed281b67d7790891` | `4f45733e1a50f1262ef937f6a9368f928c9fc1c0d0d8cf92a26fb1fde43546c3` |
| `mds-maps-owned.json` | `08d0110ab9f091d0370bb83df901785b50eb05ea85636319bb394cf5b10269e4` | `0a10a08e7caabb9eb4824b11a0d935a81ca699e90416bb50be3076aa79ab2315` |
| `mds-maps-commands.json` | `636f3135a1434d0f8ac209c990f0309cf39fd0c686d02f6173aae05cdf050b70` | `b5aec198a7018fcbd080c270229385d992581d6dccb057cbc8157b8f3c0d7eed` |
| `fixture-manifest.json` | `fecb471c26fa0fad1e63e71fe7feb7dfcd6a025d052157a82210ad3f2ebc1445` | `288d7437da945aa6346e8eed0717206212ccf7661d370f973c124d330dc8e0c2` |
| `fixture-ownership.json` | `ef5bc5333978f6a04d94bc3829143e5c09080331443c2da730b3ef7e173ac1a3` | `4d45e80b5a468bfdccbf76681c98b5f58aab8dcbea24b806c9812e815f4cfb93` |
| Run log | `42f0865835dddca05129933376cd6fa179fc4f0b3d1c9c3edd0fdc76cc0e831f` | `03804862e078a775a1907ab34ef0111002fec16f6c1bfa36601e5c04332edc5e` |

These source-file hashes were recorded from the frozen working tree after the
final runs. The native manifests identify the fixture configuration; they do
not record an exact Git commit for the development client working tree.

| Source file | SHA-256 |
| --- | --- |
| `integration/mds_maps_test.go` | `51647b6101752eae554d4145317cad557732c075681b53b768a5e00b1ca8ecac` |
| `cephmsgr/mds_maps.go` | `927c9a41aff860627055dc111508a45745519116f0cbfe98d2a4c48314992df3` |
| `internal/mds/maps.go` | `91d1f28cdc88518948766e26fa8a660009eaa4dfa76526ae075994fcbe64e6a1` |
| `tools/mds_maps_oracle.py` | `54e4c9d223b09d1fc175a003d38601dc7e5e7f23fdc8ffcaaba7defa337b2ab2` |
| `tools/mds_map_native_encoder.cc` | `2c376d57d1af6328efdd65662e7ade944eb044b44b10b50356bc12b84eac8bf2` |
| `tools/mds_map_native_encoder.py` | `74b5fa05c1fde6ff14813af07cb5cc8c79850d9c1d2a79df6bf76e7f2be0c340` |

## Limits

The development CLI lifecycle smoke is recorded separately in
[MDS_MAPS_ORACLE.md](MDS_MAPS_ORACLE.md). It does not by itself validate Go
subscriptions. The subscription scenario uses one empty filesystem without an
MDS daemon; it does not verify native populated-rank discovery, MDS service
authentication, MDS sessions, permissions/caps rejection, failover, capability
or lease events, mounting, or CephFS I/O. An unknown positive FSCID produces a
bounded local wait timeout, not an explicit native authorization error.

Local SourceGeneration identifies a delivery source, not a wire map generation
or proof of a newly generated notification. Closing/canceling a stream does not
prove remote unsubscribe or cancellation. Same-source MDS binding refusal is
local protection for uncorrelated replies. These selected native development
runs do not establish whole-project CI or coverage of later source edits.
The retained live Go map evidence includes raw fronts and nested maps, without
the Messenger message header. Current type/version/compat acceptance follows
the strict decoded-message path and independently checked canonical vectors;
it is not presented as a separately captured live header trace.
