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

## Checks and results

Focused v6/v8 and public-peer tests exercise snapshot fields, read-only
restrictions, budgets, head backoff identity, raw reply/TID and signed errors.
These compose verified wire fields; no new native snapshot combination
vectors were generated. Focused normal/race checks, whole-product CGO-disabled
tests and vet passed. Whole fixture tests/vet and four adapter checks passed.

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
