# MON pool operations (MPoolOp)

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`messages/MPoolOp.h`, `messages/MPoolOpReply.h`, `messages/PaxosServiceMessage.h`,
`include/ceph_fs.h` POOL_OP_*, `mon/OSDMonitor.cc` enforce_pool_op_caps,
preprocess_pool_op, prepare_pool_op; LGPL-2.1, independently translated).

`Client.PoolOp` sends one MPoolOp (message 49, header v4/compat 2) to the
admitted MON and returns its MPoolOpReply (message 48). It is the only route to
self-managed snapshot id allocation (what librados `selfmanaged_snap_create`
and so RBD snapshot creation use): no CLI command exists. Offered operations:
`SelfManagedSnapshotCreate` (id returned in the reply data, eight little-endian
bytes; `PoolOpResult.SnapshotID`), `SelfManagedSnapshotRemove` (by id),
`PoolSnapshotCreate` and `PoolSnapshotRemove` (by name). Pool creation and
deletion through this message and the removed AUID change are not offered; use
raw MON commands.

Server semantics observed in the source and live: a pool is in either
self-managed or pool-snapshot mode, so the other family answers `-EINVAL`; an
absent pool answers `-ENOENT` (deletion of an absent pool object answers 0);
creating an existing pool snapshot and removing an absent or already removed
one answer 0 (idempotent); removing a self-managed id above the pool's
sequence answers `-ENOENT`; an identity without the needed capability gets
`-EPERM` before pool existence or mode is checked (self-managed ids need MON
`osd` write or an OSD write grant on the pool, other kinds MON `osd` write);
pools attached to a filesystem refuse MON-managed snapshots with
`-EOPNOTSUPP`. Negative codes come back as `*PoolOpError` together with the
raw result.

The paxos version is zero, so the MON does not wait for a newer OSDMap before
acting. ctx bounds only the local wait; after transmission a cancelled wait or
a retired session returns an `OutcomeUnknownError` (the MON may have allocated
an id or changed a snapshot) and nothing is replayed. Allocating twice
allocates two ids; the caller owns id lifetime and removal. Allocated ids are
only meaningful as write SnapContexts after the pool is in self-managed mode
and the OSDs hold the updated map; this client does not wait for that.

Verification: [MON_POOL_OP_VALIDATION.md](../integration/fixture/MON_POOL_OP_VALIDATION.md).
