# CephFS dirty-data cap-snap acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964` (LGPL-2.1 references;
independent test-only restatement, no native code copied).

## Implementation and current evidence

The data acceptance has a new
`dirty_data_capsnaps_across_two_snapshots` leaf. Native runs of this new leaf
are **pending**. Earlier evidence in
[MDS_CEPHFS_DATA_VALIDATION.md](MDS_CEPHFS_DATA_VALIDATION.md) and
[MDS_CEPHFS_COMPLETION_VALIDATION.md](MDS_CEPHFS_COMPLETION_VALIDATION.md)
does not establish this behavior. This record must be updated with the
actual tested source and receipts after native execution.

No product API, protocol feature or runtime dependency is added. The test
consumer uses the existing `MDSSession.SendCaps`/cap events, explicit OSD
WRITE `SnapContext`, snapshot READ and LIST_SNAPS APIs. The library still
does not implement an inode cache or writeback policy.

The opt-in test capability agent captures immutable cap-snap generations
before a snapshot trace replaces the old realm context. A SPLIT preserves
the pre-move context while using the new realm for later cap messages.
Captured data and metadata belong to the generation; later head writes
cannot overwrite its size, mtime, change attribute or snap context. The
test explicitly drains data writeback in follows order. It sends FLUSHSNAP
only after the corresponding OSD writes complete. A failed writeback or
cap send stops the traversal and is not replayed automatically.

Pending FLUSHSNAP_ACKs are matched by inode, follows, flush TID and dirty
mask. A matching ACK removes only its generation; a mismatching ACK is
reported as an agent error. Head UPDATE flushes and FLUSHSNAP share one
monotonic TID namespace and the oldest outstanding flush TID. Focused
synthetic tests cover generation ownership, SPLIT context capture,
duplicate registration, reverse ACK order, identity mismatch and failure
ordering. They do not establish Ceph interoperability.

## Owned native recipe

The existing exclusive `CEPH_MSGR_TEST_MDS_CEPHFS_DATA=1` fixture retains its
original file-data and single-snapshot leaves. It adds one fixed owned
`capsnap-file` and two fixed snapshot names derived from the ownership token:
`<planned snapshot>-cap-a` and `-cap-b`. Closed create/remove/read/stat
controls admit those targets only; mutation intent, completion and uncertain
outcomes remain journaled. No caller path or command is accepted.

The new leaf keeps the file open with Fw/Fb and requests max_size for the
largest image before dirtying it:

1. Buffer image A (200000 bytes), then create snapshot A natively. Capture
   the old snap context and A's metadata without sending FLUSHSNAP.
2. Buffer image B (220000 bytes), then create snapshot B natively. Retain
   the second generation independently. Assert both dirty generations exist
   before either cap-snap flush is sent.
3. Write back A with its old context, then send FLUSHSNAP following that
   context's sequence. Write back B with its own context and send its
   distinct FLUSHSNAP. Require both exact ACKs and per-generation timing.
4. Write image C (240000 bytes) with the context containing both snapshots,
   flush its dirty head metadata and close the file.
5. Compare Go snapshot reads and native LibCephFS reads at snapshot A,
   snapshot B and head with A, B and C respectively. Check native snapshot
   sizes and mtimes. Compare Go LIST_SNAPS and native `rados listsnaps`:
   two distinct clones, their snapshot associations and sizes, plus head.
6. Remove both added snapshots and the original snapshot; remove all six
   files, layout and directory; release caps and confirm session closure.
   Existing fixture ownership cleanup remains required.

The two dirty generations coexist by construction. Simultaneous pending
FLUSHSNAP_ACKs are a separate fact and are not claimed without observed
receipts. Synthetic reverse-order ACK coverage is labeled separately.

Pinned source basis: `client/Client.cc` `queue_cap_snap` (4245),
`finish_cap_snap` (4283), `send_flush_snap` (4311), `flush_snaps` (4359),
`put_cap_ref` (3749), `update_snap_trace`/`handle_snap` (5320/5417),
`handle_cap_flushsnap_ack` (5778); `mds/Locker.cc` `handle_client_caps`
FLUSHSNAP handling (3504). These line numbers describe the pinned source,
not the working Go files.

## Limits

Until native execution passes, dirty-data FLUSHSNAP and multi-snapshot
interoperability remain unverified. This recipe does not cover file I/O
retry, eviction/failover with dirty data, snapshot deletion while flushing,
multiple writers, inline data, fscrypt or reconnect cap-snaps. The data
fixture remains the existing single-OSD `legacy-v6` profile.
