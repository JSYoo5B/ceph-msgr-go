# CephFS dirty-data cap-snap acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964` (LGPL-2.1 references;
independent test-only restatement, no native code copied).

## Implementation and current evidence

The data acceptance's `dirty_data_capsnaps_across_two_snapshots` leaf passed
against actual Ceph in both configurations below. Earlier evidence in
[MDS_CEPHFS_DATA_VALIDATION.md](MDS_CEPHFS_DATA_VALIDATION.md) and
[MDS_CEPHFS_COMPLETION_VALIDATION.md](MDS_CEPHFS_COMPLETION_VALIDATION.md)
remains a record of its earlier, narrower tests.
The later [MDS restart extension](MDS_CEPHFS_CAPSNAP_RECOVERY_VALIDATION.md)
has separate source and evidence; the original results below did not restart
the MDS.

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

The two dirty generations coexist by construction. Both runs also recorded
local pending-ACK overlap: the second `SendCaps` completed before the first
ACK's transport-dispatch `Received` timestamp. This does not establish
concurrent server execution. Synthetic reverse-order ACK coverage remains a
separate claim.

Pinned source basis: `client/Client.cc` `queue_cap_snap` (4245),
`finish_cap_snap` (4283), `send_flush_snap` (4311), `flush_snaps` (4359),
`put_cap_ref` (3749), `update_snap_trace`/`handle_snap` (5320/5417),
`handle_cap_flushsnap_ack` (5778); `mds/Locker.cc` `handle_client_caps`
FLUSHSNAP handling (3504). These line numbers describe the pinned source,
not the working Go files.

## Native results and source identity

Both final runs used the clean isolated source at commit
`source-1`, based on `source-2`, before
this results update and the narrative-only manifest/comment correction.
The product implementation was unchanged. Image (arm64):
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Provider ceph-testcontainers-go v0.3.0, Go 1.27.2, a compatible Docker engine.
Both use one active rank-0 MDS and the existing single-OSD `legacy-v6`
profile. Selector: `^TestCephMDSCephFSDataIntegration$`.

| Configuration | Runner log window (2026-10-16, Asia/Seoul) | Entire test | New leaf | Result |
| --- | --- | --- | --- | --- |
| Darwin arm64 host, IPv4 relay, CRC, AES client/service keys, BlueStore | 18:19:30–18:23:07 | 129.98 s | 32.22 s | 12/12 leaves, exit 0 |
| CGO=0 Linux arm64 container, IPv6, secure, AES256K client/service keys, MemStore | 18:23:28–18:26:41 | 108.00 s | 23.71 s | 12/12 leaves, exit 0 |

Both runs retained two buffered generations with transaction 0 before
writeback. Snapshot IDs were 3 and 4; the previous ordinary snapshot was 2.
Contexts were `{2,[2]}`, `{3,[3,2]}` and head `{4,[4,3,2]}`. The FLUSHSNAPs
carried TIDs 6 and 7, follows 2 and 3, dirty FILE_WR (4096), sizes 200000 and
220000 and oldest flush TID 6. Each matching ACK retained inode, follows,
TID and dirty mask. The second send completed 4.958901 s / 0.704797 s before
the first ACK's transport-dispatch timestamp (CRC / secure), establishing
local pending-ACK overlap in these runs.

Go snapshot reads matched A/B and head matched C (240000 bytes) with
`bytes.Equal` assertions in the executed source. Native LibCephFS returned
matching whole-file SHA-256 and every 16 KiB chunk digest, sizes and snapshot
mtime seconds. Go and native LIST_SNAPS agreed for both objects: snapshot 3
holds A, snapshot 4 holds B, and head holds C, with each version's striped
object sizes and no clone overlap after the complete overwrites. The source
requires ONDISK completion for successful OSD writes; raw OSD reply flags
and Go object buffers are not archived, so their evidence is the executed
assertions and passing test, not an archived wire capture.

Cleanup passed in both configurations: all three snapshots removed, six
files unlinked, layout and directory removed, caps returned, confirmed
session closure, namespace/layout/blocklist restored, both data identities
removed, owned filesystem/pools removed, and all 9 recorded owned containers
terminated. The runner exited 0 only after cleanup. No daemon setting was
changed for this new leaf.

The first CRC attempt at earlier commit `source-3` also passed all 12 leaves
(118.92 s; new leaf 27.72 s) and cleanup. Independent review subsequently
found a synthetic failure-path defect: a failed cap send with an allocated
transaction could be skipped by a later explicit drain. `source-4` keeps
that failure as a barrier and adds a regression test. The earlier raw
receipt is preserved separately and does not replace either final run.

Checks before the final native runs: full default integration tests and
fixture tests with CGO=0 passed; focused generation/ACK tests passed normally
and under the race detector after the failure-barrier correction. No product
API or feature declaration changed. The final documentation also corrects
the manifest's human-readable `cephfs_data_mutations` text from five files /
one snapshot to six files / three snapshots. The archived manifests retain
the older narrative; their actual owned plans, mutation journals and cleanup
receipts contain the correct six-file/three-snapshot scope. Existing raw
artifacts were not rewritten.

Local artifacts, each with an adjacent `.log`:

`mds-cephfs-data-results.json` SHA-256:

- CRC final: `7112ff21483ead5e6a80655496bc52968d3dcfcf961c6c0600846974b55a7fe8`
- Secure final: `fa32990f9d0f5f0ed4b85511110fd48a5a2cc6fb2e233f9a8cf4f18574088b8d`

Tested source SHA-256 (before the narrative-only comment correction):

- `integration/mds_cephfs_capsnap_test.go`: `679a5f6e5817168d7df16a2a9caf7b5e76792f57720327e23d1f299c57ae0417`
- `integration/mds_cephfs_capsnap_queue_test.go`: `853291cb4a57ce89fad4d3e62619748bbd19a0156f3fa168e6b7585a9d83cfa4`
- `integration/mds_cephfs_agent_test.go`: `f203dc9d1dc666eab4e7e27bf9265b49bfa9d02c1555a12872b0c6ce98e28680`
- `integration/mds_cephfs_data_test.go`: `0cc483195ff2d3e56af0b38f2f71377ece8b40a9da31f95dbcb8badf594fa674`
- `integration/fixture/mds_cephfs_data.go`: `ab546f7c4aa1362a290ab7818bf9c20ddff53e715af783485486c24c95d394de`

## Limits

This verifies the fixed two-snapshot, one-writer recipe on the configurations
above. It does not cover file I/O retry, eviction/failover with dirty data,
snapshot deletion while flushing, multiple writers, inline data, fscrypt,
reconnect cap-snaps or concurrent server execution of the two FLUSHSNAPs.
The data fixture remains the existing single-OSD `legacy-v6` profile.
The later restart extension verifies retained, already-written snapshot
generations with their first FLUSHSNAP sent after reconnect. Its scope does
not include uncertain FLUSHSNAP replay or interrupted OSD writeback.
