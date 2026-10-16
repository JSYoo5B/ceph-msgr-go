# Exact-range snapshot MAPEXT — 2026-10-16

Reference: Ceph Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`.

The existing standalone MAPEXT request now accepts ReadSnapshot. This only
removes the earlier head restriction: the existing operation union, local
output budget, raw reply, explicit placement and backoff head identity stay
in force. Write SnapContext, operation flags and compounds remain refused.
No CRUSH, snapshot management, automatic retry or native product dependency
is added. EC remains unsupported.

## Independent oracle

The old rados CLI always requested the head's full map; its partial-range
observations were explicitly intersection models. The new test-only C++
helper executes IoCtx::mapext at exactly the caller's offset, length and
optional read snapshot. Native IoCtx::mapext returns an extent count on
success, which the receipt preserves as native_api_code; code is normalized
to zero for comparison with Go's aggregate result. The convenience method
does not update IoCtx::get_last_version. Version evidence is therefore
explicitly labeled independent selected-snapshot STAT2. The helper does not
invent a native MAPEXT reply version.

The adapter observes head and selected snapshot bytes/version/mtime before
and after the native call, restores the C read context to LIBRADOS_SNAP_HEAD
on failure too, and observes them again after the Go call. Extent count,
ordering, boundaries, signed errors and raw Go map encoding are compared.
Allocation granularity is the active store's logical map, not physical usage.

The helper was compiled against pinned source headers and the image's
installed librados.so.2 using the unchanged development-only
`tools/build_sparse_read_oracle.py`. Its loader audit resolves the selected
image library. The owned builder
`ef450b8ecfbedfbd5ca68507ca611db15fd8b51eea91e3a0787887ceb05b2ee6`
was terminated. Build/ownership/proof files are retained under
`/private/tmp/ceph-mapext-range-native-20261016/`.
Binary SHA-256: `f9dc52bba54a44ac35a5965a0acfebca80677c16cce0383242eda118f8be217f`.
Wrapper SHA-256: `0858e6d158b2f5db9249e86410d6cb9bc76952e691deff933e29db284e8b703b`.
Staging verifies pinned image/source, wrapper and binary hashes, bounded ELF
bytes and private archived provenance. Host build paths remain outside the
client environment; tests receive only an availability marker.

## Checks and results

Focused v6/v8 and public-peer tests exercise snapshot fields, read-only
restrictions, budgets, head backoff identity, raw reply/TID and signed errors.
These compose verified wire fields; no new native snapshot combination
vectors were generated. Focused normal/race checks, whole-product CGO-disabled
tests and vet passed. Whole fixture tests/vet and four adapter checks passed.

CRC/AES host IPv4, three BlueStore replicas: the first run passed all 16 new
exact-range snapshot/head cases (14.85 s) and eight existing head tests
(4.12 s, seven retained operation receipts plus local budget isolation).
The BlueStore clone map independently has ranges [0,4096) and [16384,20480),
with the requested hole empty and EOF clipping [18432,20480). Head and clone
contents differ; no map is inferred from a zero-filled byte model.
Artifact root: `/private/tmp/ceph-snapshot-mapext-crc-aes-20261016-first/`.
Snapshot receipt SHA-256:
`c1c7a06f804c32dd5fcce6e4cec89fff96373c0ada7c432fd94f80614e8789a1`.
All four owned heads and two snaps were removed, clone/whiteout trim completed
and all ten fixture-owned containers were terminated. Independent raw audit
confirmed the maps, versions, source provenance and state preservation.

The first secure/AES256K IPv6 Linux-container single-MemStore run passed
16 exact-range cases (14.01 s) and eight legacy head tests (3.92 s; seven
operation receipts). MemStore reports the clone's whole logical [0,20480)
range, including zero-filled holes; the hole-only range returns [6144,10240).
Go matches that independent native representation. This is distinct from
BlueStore's sparse extent layout above, not a physical-allocation claim.
Artifact root:
`/private/tmp/ceph-snapshot-mapext-secure-aes256k-ipv6-20261016-first/`.
Snapshot receipt SHA-256:
`f5ce8539446e23c38edaf35ac81fcddb82e0a1bc8390e4989ba0cab1cafb573f`.
Raw-receipt audit confirmed exact maps, normalized count versus aggregate,
selected STAT2 versions, unchanged head/clone state and all eight owned
containers terminated. Four owned heads and two snaps were removed, with
clone/whiteout trim and clean PGs confirmed.

Both configurations used the immutable image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Their original provider identity records v0.3.0 with matching effective module
checksum and the native MAPEXT marker. Both are first-attempt native passes;
local native results are separate from remote CI status.

Frozen acceptance source SHA-256:
`43db8f30d999ce8e361ccc4f50c9e0ab7da1d415f2f87adcdecca03415d752be`.
Frozen Python adapter SHA-256:
`36f02d66159ca781c26f4ab1100a3d1976bacc212344e8debd4f7cb040db309e`.
Frozen snapshot admission SHA-256:
`d070c4950a260514eb5d40350b8d2921d5426360a2d324139ae7ec694b97b042`.

## Reproduce and limits

Stage the binary and adjacent .proof.json via CEPH_MSGR_TEST_MAPEXT_ORACLE;
select `^TestCephOSD(SnapshotMapExt|MapExt)Integration$`. Without the explicit
native binary the new snapshot test skips and proves no interoperability.
The prior head CLI suite remains independent. This acceptance concerns only
bounded replicated pools, self-managed snapshots and the actually run
configurations. EC, compounds, physical allocation and automatic routing
remain outside its scope.
