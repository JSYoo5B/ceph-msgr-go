# Owned extra pools: cross-pool COPY_FROM, namespaces and pool snapshots

Run 2026-10-17 against Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
through the testcontainers-based fixture (container runtime). Product behavior
under test is the existing public API; the new pieces are test-only:
`tools/osd_pool_oracle.py` (closed librados C-API oracle creating at most two
pools named `ceph-msgr-owned-pool-1/-2` in the fixture's own shape: pg_num 1
and no HASHPSPOOL for legacy-v6, 8 and HASHPSPOOL for replicated-pg-v8, the
fixture's size/min_size), the `verify-osd-pool` control hook and
`integration/osd_pool_test.go`. The pools are removed on cleanup and the
oracle verifies their removal.

## Cross-pool and namespace COPY_FROM2 (`TestCephOSDCrossPoolAndNamespaceCopyFromIntegration`)

Four subtests, native-seeded sources with binary data and an xattr, native
observation of source and destination:

- pool 1 to pool 2, pool 2 to pool 1 across namespaces, and one pool across two
  namespaces: the destination equals the source (bytes, xattr), the source is
  unchanged, ONDISK, the receipt user version equals the native version, and
  the reply pool is the destination's.
- A source namespace that does not match the seeded one returns a known `-2`
  and creates no destination.

## Pool snapshots (`TestCephOSDPoolSnapshotIntegration`)

A pool in pool-snapshot mode takes its SnapContext from the pool. After a
native `rados_ioctx_snap_create`, a Go WRITEFULL with no SnapContext produces a
clone: native head and snapshot reads show the new and original bytes, and the
xattr is preserved on the clone. Go read at the pool snapshot id returns the
original; LIST_SNAPS lists the pool snapshot id in a clone; a Go ROLLBACK to the
id restores the original bytes and xattr on the head.

Observed server behavior (not a product contract): a write that carries an
explicit client SnapContext in a pool-snapshot pool is neither refused nor
cloned for the client's sequence; it commits as a head overwrite.

## Profiles

Both tests passed on the first complete run in each profile: legacy-v6 single
BlueStore (default secure CephX, IPv4), legacy-v6 three BlueStore replicas,
replicated-pg-v8 single BlueStore, and request-v9/reply-v8 with secure
AES256K over IPv6 on single MemStore. A first run of the snapshot test failed
only because the test attached a modification time to read vectors, which the
product correctly refuses locally.

Not claimed: EC, pool-snapshot ROLLBACK/clone behavior under failover, a
self-managed-then-pool mode switch, CRC mode, host runtime, copies killed in
flight, more than two extra pools, and omap/header on the copied objects (the
pool oracle observes bytes and xattrs only).
