# Tentacle FSMapUser and MDSMap wire foundation

Pinned reference: Ceph `v20.2.4`, commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Source audit: 2026-10-15.
This package decodes bounded raw map messages and map metadata. It does not
subscribe, select a filesystem or rank, authenticate/connect to an MDS, open an
MDS session, route/replay metadata operations, or implement CephFS workflows.
Adding these codecs alone advertises no Messenger or MDS client feature.
The separately implemented public MON-only subscription opt-in is specified in
[cephmsgr/MDS_MAPS_CONTRACT.md](../../cephmsgr/MDS_MAPS_CONTRACT.md).

## Exact current envelopes

| Input | Accepted wire form | Preserved metadata |
| --- | --- | --- |
| `MFSMapUser`, type 103 | Header 1/compat1; epoch u32, `FSMapUser` struct 1/compat1 | Inner epoch, signed legacy-client FSCID, ordered `fs_info_t` vector (each struct 1/compat1: signed FSCID, raw name) and owned raw front/map |
| `MMDSMap`, type 21 | Header 2/compat1; cluster FSID 16 bytes, epoch u32, length-prefixed map blob, length-prefixed map_fs_name | Cluster FSID, outer epoch/name, complete decoded inner map and owned raw front/map |
| Inner `MDSMap` | Struct 5/compat4, current extension number 19 | Base metadata, daemon/rank tables, pools, compatibility sets, flags, required-client bitset, balance and quiesce metadata |
| Inner `mds_info_t` | Struct 10/compat4 | GID/name, signed rank/incarnation/state, state sequence, original address vector, laggy time, export targets, feature/flag masks, join FSCID and compatibility set |

The minimum accepted form for each codec is exactly its current pinned form.
Future compatible map structs and trailing fields are refused until their
semantics are audited; historical peer-selected v2/v3 MDSMap and v7 daemon info
are unsupported. Address decoding reuses the existing bounded Messenger
address codec, including its compatible address-envelope behavior. A wire form
or feature bit does not prove the daemon's Ceph release family.

Only front data is permitted; nonempty middle/data segments fail. The map epoch
must match its message epoch. Epoch zero is valid for a newly constructed empty
map; it is not a subscription acknowledgement or proof that a filesystem exists.
This codec does not discard old epochs, merge maps, or infer transition events.

FSMapUser contains **no cluster FSID**. MMDSMap contains a cluster FSID but
**no FSCID**. Its historical map_fs_name field is empty in the current native
producer and does not supply an identity binding. The native Client resolves
name-to-FSCID through `fsmap.user`, then subscribes to `mdsmap.<FSCID>`; the
incoming MMDSMap does not echo that subscription ID. Public subscriptions use
explicit caller FSCID and a per-MON-session watch tombstone; they never infer
filesystem choice from a map name or treat an empty map as authorized discovery.
Cluster-FSID validation belongs to the receiving authenticated connection.

## Metadata and policy boundaries

The decoder retains every field in the current map encoding: epoch, flags,
last-failure epochs, root/tableserver ranks, session timeouts, maximum file/xattr
sizes, daemon map, ordered data pools and signed pool IDs, creation/modification
times, in/up/failed/stopped/damaged rank tables, encoded legacy incarnation table,
allowed-feature history, inline/enabled flags, filesystem name, balance policy,
standby/old-max metadata, required-client feature bytes and quiesce cluster
leader/members. Quiesce members retain wire order because the native container
is an unordered set; duplicates fail but ascending order is not required.

Known daemon states preserve their signed values: DNE 0, STOPPED -1, BOOT -4,
STANDBY -5, CREATING -6, STARTING -7, STANDBY_REPLAY -8, NULL -10,
REPLAY 8, RESOLVE 9, RECONNECT 10, REJOIN 11, CLIENTREPLAY 12, ACTIVE 13,
STOPPING 14 and DAMAGED 15. Unknown signed states remain raw. A state/rank
observation is not proof that the selected daemon is ready for this client.

Known map flags are NOT_JOINABLE bit0, ALLOW_SNAPS bit1,
ALLOW_MULTIMDS_SNAPS bit4, ALLOW_STANDBY_REPLAY bit5,
REFUSE_CLIENT_SESSION bit6, REFUSE_STANDBY_FOR_ANOTHER_FS bit7 and
BALANCE_AUTOMATE bit8. Daemon FROZEN is bit0. Unknown flag bits are retained.
No capability is inferred or advertised from these observations.

Three feature systems remain separate:

1. Messenger negotiation selects framing/authentication and map encodings.
2. `CompatSet` has compatible/read-only-compatible/incompatible masks plus named
   feature-ID maps. The decoder retains encoded masks and names exactly. It
   does not apply native in-memory lowest-bit repairs, automatically insert
   INLINE, inherit a daemon's empty compatibility set from the map, or sanitize
   pool IDs. Those native decoder/application transformations are not wire bytes.
3. MDS session required-client features use `feature_bitset_t`: a length-prefixed
   little-endian vector of u64 blocks. Raw bytes, including unknown high bits,
   are exposed separately from each daemon's raw `mds_features` u64. Preserving
   a requirement does not mean the client implements it. Future service/session
   preparation must explicitly reject unsupported requirements.

Signed FSCID/rank/pool values and raw utime_t seconds/nanoseconds are preserved;
no host clock, OS address layout, filesystem-name policy, placement or time
normalization is used. Retired standby fields remain raw wire metadata and
supply no compatibility API or standby-selection policy.

## Encoding feature prerequisites

`MMDSMap::encode_payload` keeps its fully current map only when the peer has
PGID64 (bit9), MDSENC (bit29), MSG_ADDR2 (bit59), and the complete SERVER_NAUTILUS
mask (bits2,57,28). Missing one causes native re-encoding for that peer.
`MDSMap::encode` needs PGID64 and MDSENC for struct5; daemon info needs the full
SERVER_NAUTILUS mask for struct10. Native address vectors also depend on the
address feature. Defining these requirements adds no negotiation flag.

The current MON common feature set contains these prerequisites except MDSENC.
`Options.EnableMDSMaps` therefore selects MDSENC before Dial for MON sessions
only, with the fixed current map codecs and source/identity policy documented
in the public contract. Default MON and all MGR/OSD feature masks remain
unchanged. The option requests no MDS service ticket and claims no MDS daemon
connection or session support; preserving maps alone does not establish either.

## Bounds, ownership and errors

The configured limit includes the 41-byte message header and every segment.
Standalone map blobs are bounded by the supplied blob limit. Inputs are copied
before exposure: RawFront owns the incoming message; RawMap/MDSMap.Raw and the
required-client-feature byte slice view owned storage. Strings and decoded
collections are independent projections. Callers own returned data and must
coordinate any mutation with their own users of those views.

Each collection is capped at 4096 entries, each named compatibility-feature
map at 1024 names, address vectors at the existing 64-address bound, and required
client-feature bytes at 64 KiB. Counts are checked against minimum wire size
before allocation; a current daemon-table entry needs at least 128 bytes.
Current feature-bitset bytes must be a multiple of eight. This is a local
bounded subset, not a promise to accept arbitrarily large future cluster maps.

Duplicate filesystem IDs, nonascending/duplicate native map/set keys,
inconsistent outer daemon key/GID, duplicate unordered members, invalid booleans,
truncation, oversized lengths/counts, mismatched epochs, unsupported versions and
trailing bytes fail without returning a partial decoded map. Ordered pool
vectors retain their order rather than being treated as sets. Map validity does
not establish rank-table consistency, authorization, service liveness or a safe
route; those need independent receiving/application contracts.

## Independent native evidence and limits

The [retained native proof](testdata/mds-map-native-provenance.json) contains five
fresh messages: empty/multiple-filesystem FSMapUser and empty/populated/unknown
MDSMap. The MDS maps include IPv4/IPv6 address vectors, active/replay/unknown
state 777, high unknown daemon/map flags, named compatibility feature 62,
required-client bit 511, signed sentinels and quiesce metadata.

`tools/mds_map_native_encoder.cc` constructs fresh native objects, asserts empty
front/middle/data, and invokes canonical `Message::encode` exactly once. Its
empty-payload branch dispatches the unchanged virtual payload encoder and
normalizes a zero compatibility value to the selected header version. The
MFSMapUser constructor's initial compat0 therefore becomes canonical wire
compat1; direct payload encoding would incorrectly bypass this normalization.
Compat0 is rejected by the current codec and a dedicated regression test. The
wrapper then separately decodes both message and inner map. It compares native epoch/identity, names, daemon metadata, addresses,
features and flags. Decoded messages/maps are never re-encoded. The independent
`tools/mds_map_native_encoder.py` preserves pinned source/submodule, wrapper,
driver, generated-configuration, native library/binary and segment hashes,
matching loader audit, decoded-map receipt and confirmed builder termination.
The native Message envelope is independently projected through MessageFrame.
Sequence 1 and ACK sequence 3 are explicit offline inputs, not live session proof.

Go tests decode all five native outputs and check their raw/typed metadata and
producer/segment hashes. Separate synthetic tests cover ownership, exact budgets,
all truncations, count/version/boolean/epoch failures, unknown metadata and native
compatibility-fixup separation. Race and bounded fuzz runs check the standalone
codec. These results establish current wire decoding only. No actual FSMap/MDSMap
subscription, native filesystem/rank discovery, MDS connection/session, failover,
capability event or CephFS operation has been verified by these fixtures.

The following native event identities from `ceph_fs.h` were **outside the
map-codec validation stage described above**. Later request, capability and
recovery paths are implemented and specified in
[cephmsgr/MDS_CEPHFS_CONTRACT.md](../../cephmsgr/MDS_CEPHFS_CONTRACT.md);
this historical list is not a current unsupported-feature backlog:
CLIENT_SESSION 22, RECONNECT 23, REQUEST 24, REQUEST_FORWARD 25, REPLY 26,
RECLAIM 27/RECLAIM_REPLY 28, METRICS 29, CAPS 0x310, LEASE 0x311,
SNAP 0x312, CAPRELEASE 0x313 and QUOTA 0x314. Full FSMap type45 is also outside
this FSMapUser subset. Map decode cannot stand in for these message/session,
recovery, capability/lease or client-feature contracts.

## Source provenance and reproduction

The audited files are `messages/MFSMapUser.h`, `messages/MMDSMap.h`,
`mds/FSMapUser.{h,cc}`, `mds/MDSMap.{h,cc}`, `mds/mdstypes.{h,cc}`,
`include/cephfs/types.h`, `include/CompatSet.h`, `include/ceph_features.h`,
`include/ceph_fs.h`, `msg/Message.cc` (canonical encode lines237-251), and
`client/Client.cc` at the pinned commit. Message/map/type
sources carry LGPL-2.1 notices; `ceph_fs.h` allows LGPL-2.1 or LGPL-3.0 and the
project COPYING gives the default/file-specific terms. This implementation
independently describes wire facts and incorporates no upstream native code.
Native tools and packages remain development-only dependencies.

Reproduce in `integration/fixture` with exclusive native cache access:


