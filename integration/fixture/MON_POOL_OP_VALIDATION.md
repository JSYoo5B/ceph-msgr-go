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
- Forwarding: one client pinned to each of the three monitors allocates a
  self-managed id; the ids are consecutive whichever MON took the request,
  two of the three went through peons (identified against
  `quorum_status`'s leader address) that forwarded to the leader and routed
  the reply back, and native `snap_seq` equals the last id. Passed in
  legacy-v6 single BlueStore (IPv4) and request-v9/reply-v8 secure AES256K
  IPv6 single MemStore.
- Lost reply (`TestCephMonPoolOpLostReplyIntegration`): a relay stops
  delivering the MON connection's bytes right after an allocation is sent.
  Native `snap_seq` shows the id consumed; the keepalive timeout (2 s) retires
  the session and the call returns an unknown outcome. The next allocation,
  on the client's recovered session, returns the id after the lost one and
  native `snap_seq` advances by exactly one more, so the lost request was
  neither replayed nor hidden. Passed in legacy-v6 single BlueStore and
  request-v9/reply-v8 secure AES256K IPv6 single MemStore (the client
  reconnected to the same MON in both).
- Leader stop (`TestCephMonPoolOpLeaderStopIntegration`): a client pinned to
  a peon allocates once, then the leader MON is stopped and the next
  allocation is sent to the same peon. It completes after about 14.7 s (the
  election) with the next id under a new leader whose quorum excludes the
  stopped MON, and native `snap_seq` equals that id, so it executed once. The
  stopped MON is restarted, the fixture's settled-election barrier (new
  closed label `restarted`) sees the full quorum twice, and a further
  allocation returns the following id. Passed in legacy-v6 single BlueStore
  and request-v9/reply-v8 secure AES256K IPv6 single MemStore.
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

- Pool-scoped grant (`TestCephMonPoolOpScopedCapsIntegration`): the test
  creates `client.ceph-msgr-scoped-snap` with `mon 'allow r'` and
  `osd 'allow rw pool=ceph-msgr-owned-pool-1'` through MON
  `auth get-or-create` (removed with `auth rm` in cleanup) and dials with the
  key the MON generated. A self-managed allocation on the granted pool
  succeeds and equals native `snap_seq`; a self-managed allocation on the
  other owned pool and a pool snapshot on the granted pool are EPERM, and the
  ungranted pool is unchanged. Passed in legacy-v6 single BlueStore and
  request-v9/reply-v8 secure AES256K IPv6 single MemStore.

- Connected MON frozen (`TestCephMonPoolOpConnectedStallIntegration`,
  2026-10-18): a client seeded only with a peon allocates once, the peon is
  frozen with `docker pause`, and a self-managed allocation, a cluster statfs
  and an OSDMap version request are sent to it together. All three end as
  unknown outcomes when the 2 s keepalive retires the session (about 2.1 s).
  The client then leaves its only seed (3 s connect timeout) for a member of
  its authenticated map and answers fresh map-version and statfs requests
  about 12.8 s later, through the re-election. While the peon is still frozen
  native `snap_seq` is unchanged, so the allocation was not replayed. After
  the peon resumes and the election settles (barrier label `restarted`), the
  frozen MON did not execute the request it had buffered (`snap_seq`
  unchanged after 5 s), and the next allocation returns the id after the
  native `snap_seq`. Passed in legacy-v6 single BlueStore (IPv4) and
  request-v9/reply-v8 secure AES256K IPv6 single MemStore; the test accepts
  either fate of the buffered request and only the dropped one was observed.
  The test is opt-in (`CEPH_MSGR_TEST_MON_FREEZE=1`): after it the fixture's
  OSD sometimes stays on an old OSDMap and a later pool creation times out
  (open issue, MON_FREEZE_FIXTURE_ISSUE.md).

- Filesystem pools (2026-10-18, `TestCephMonPoolOpFilesystemPoolIntegration`):
  two owned pools are attached to a test-only filesystem with MON `fs new`
  (no MDS needed; removed with `fs fail` and `fs rm` before the pools). On
  the data pool a pool snapshot create and a self-managed allocation are each
  a known `-95` (EOPNOTSUPP); native pool state shows no snapshot and an
  unchanged `snap_seq`. Passed in legacy-v6 single BlueStore and
  request-v9/reply-v8 secure AES256K IPv6 single MemStore.

Not claimed: a connected MON that dies
(rather than freezes) after forwarding a pool op, POOL_OP_CREATE/DELETE, and
waiting for OSDs to apply the updated map after an allocation.
