# MON pool operations (MPoolOp)

Run 2026-10-17, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture, container runtime. Contract:
`cephmsgr/POOL_OP_CONTRACT.md`. Test: `TestCephMonPoolOpIntegration` on two
owned pools (OSD_POOL_VALIDATION.md); the pool oracle gained the modes
`selfmanaged` (native `rados_ioctx_selfmanaged_snap_create/remove`) and richer
`poolinfo` (snap_seq, snap_mode, pool_snaps).

## Observed

- Self-managed ids: two Go allocations return consecutive ids; native pool
  state shows `snap_seq` equal to the second id and `snap_mode` `selfmanaged`;
  a native allocation then returns the next id, so both clients draw from one
  sequence. The id Go allocated serves as a Go write's SnapContext: native reads
  the original bytes at that snapshot id and the new bytes at the head.
- Removal: removing the id returns success, repeating it is idempotent (0), an
  id above the sequence is `-2`; removing the id that holds a clone makes the
  OSDs trim it, and the native read at that id then reports `-2` (polled up to
  two minutes; it completed well inside that).
- Pool snapshots: a Go-created snapshot appears in native pool state with
  mode `pool`; after a Go head overwrite (no SnapContext) the native read at
  the snapshot name returns the original bytes; creating it again is
  idempotent, Go removal empties the native list, removing it again is 0.
- Refusals are known signed codes with the raw result preserved: pool snapshot
  in a self-managed pool `-22`, self-managed id in a pool-snapshot pool `-22`,
  absent pool `-2` for both create and remove.
- Capabilities: the fixture's `client.readonly` (mon, mgr and osd `allow r`)
  gets a known `-1` (EPERM) reply with a map epoch, never a dropped request,
  for self-managed allocation and removal, pool snapshot creation and removal,
  a self-managed id in an absent pool and a pool snapshot in a self-managed
  pool. The capability check precedes pool existence and snapshot mode (those
  two would otherwise answer `-2` and `-22`). Native `snap_seq`, mode and the
  pool snapshot list are unchanged, and the same session still runs a MON
  read command, so the refusal is per request.
- Wire: request and reply codecs are covered by byte-level unit tests and a
  synthetic MON; the daemon decoding our request and our decoding its reply is
  the interoperability evidence. No fresh native-encoder vector exists.

## Profiles

Passed first complete run in: legacy-v6 single BlueStore, legacy-v6 three
BlueStore replicas, replicated-pg-v8 single BlueStore, request-v9/reply-v8
secure AES256K IPv6 single MemStore.

The capability subtest was added later the same day and passed in the same
four profiles.

Not claimed: grants that admit self-managed snapshots through a pool-scoped
OSD write cap without MON write (no such identity exists in the fixture),
`-EOPNOTSUPP` for filesystem pools, behavior across MON failover or a forwarded
request to a peon, POOL_OP_CREATE/DELETE, and waiting for OSDs to apply the
updated map after an allocation.
