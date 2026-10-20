# MON statfs (MStatfs)

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`messages/MStatfs.h`, `messages/MStatfsReply.h`, `include/ceph_fs.h:218-227`,
`mon/MgrStatMonitor.cc` preprocess_statfs; LGPL-2.1, independently translated).

`Client.Statfs(ctx, dataPool)` sends one MStatfs (message 13, header v2/compat
1, FSID and an optional pool id) to the admitted MON and returns the packed
MStatfsReply (message 14): the committed map version and the four u64 figures
KB, KB used, KB available and object count. A nil pool gives the cluster total,
as librados `cluster_stat` does; a pool id gives that pool's figures, as
libcephfs `statfs` does for a data pool. The figures come from the MGR stat
service and lag the cluster by its reporting period; this client adds no
scaling or interpretation (replication factors and CephFS rounding stay with
the caller).

The MON has no error reply for this message. With insufficient `pg` read
capability, a different FSID or a removed/absent pool it ignores the request,
so the call ends only when ctx does, as an `OutcomeUnknownError` (read-only, so
nothing changed). A reply with another FSID or size fails the session as a
protocol error. Nothing is retried.

Verification: [MON_STATFS_VALIDATION.md](../integration/fixture/MON_STATFS_VALIDATION.md).
