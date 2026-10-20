# Pending CephFS cap snapshots across an MDS restart

Reference: Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. This extends the earlier
[two-snapshot data acceptance](MDS_CEPHFS_CAPSNAP_VALIDATION.md), using
the same closed [data fixture](MDS_CEPHFS_DATA_FIXTURE.md), identities,
file layout and A/B/C images. It adds test consumer state and native
verification, with no product dependency, automatic file I/O retry or
inode/cache policy.
The later [explicit resubmission acceptance](MDS_CEPHFS_CAPSNAP_RESUBMIT_VALIDATION.md)
also covers one old-session admission with no accepted ACK. The results
below remain the earlier first-send recipe.

## Test shape

The leaf `dirty_data_capsnaps_across_two_snapshots_and_mds_restart`:

1. Keeps two independent dirty generations across the native creation of
   snapshots A and B, with sizes 200000 and 220000. Each owns its original
   context, metadata and buffered writeback callback.
2. Explicitly completes both generations' old-context OSD writes, retaining
   the original flush TIDs and expected ACKs without sending FLUSHSNAP.
   Each of the four striped object writes must succeed with ONDISK. Raw
   reply flags, request identity, epochs, start/completion times and payload
   hashes are preserved in the public result.
3. Arms and fires exactly one restart of the owned rank-0 MDS. The old
   session must become Lost with Messenger SESSION_RESET, and the old
   agent must stop without errors or unapplied pushes. Its final snapshot
   is retained independently of the new agent.
4. Proves that the same owned daemon has a new GID, nonce and incarnation
   in `up:reconnect`. Sends held caps with the oldest pending cap-snap
   follows and the latest decoded realms through `ReconnectSession`.
   Installs all pending generations, original TIDs, ACK expectations and
   caller state before starting the new agent's event consumer.
5. Waits for `up:active` and the fixture's two-observation adoption proof,
   then explicitly sends each prepared FLUSHSNAP once. This follows the
   native deferred path for pure cap-snaps: `Client::early_kick_flushing_caps`
   does not select an early send when no ordinary flushing cap was revoked.
   The client GlobalID and shared CephFS nonce remain the same. The Go object-request count
   must not increase during reconnect or metadata flush.
6. Requires both ACKs to match inode, follows, original TID and dirty mask.
   Writes C (240000 bytes) with the new context, flushes head metadata and
   closes the handle. Go reads and independent LibCephFS whole-file/chunk
   hashes, sizes and snapshot mtime seconds must equal A/B/C. Go LIST_SNAPS
   and native object views must agree on both objects' clone IDs,
   associations, sizes and overlaps.
7. Removes all three snapshots, six files, layout and directory; releases
   caps, confirms session close, and verifies namespace, layout, identities
   and baseline blocklist restoration. Only the exact old MDS address can
   be removed after native proof that its GID is absent. Owned filesystem,
   pools, containers and network must also be removed.

Pinned source basis: `Client.cc` `send_reconnect` 3295–3366,
`send_flush_snap` 4311–4354, `flush_snaps` 4359 and
`early_kick_flushing_caps` 5181–5206. These are LGPL-2.1 references;
the Go helpers restate wire semantics without copying native code.

## Native results (2026-10-18)

Both Linux amd64 runs used the published fixture provider v0.3.0,
Go 1.26.9, a compatible Docker engine and CGO-disabled Go test binaries. The Ceph image
was `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`;
the daemon reported the pinned commit. Both selected only
`^TestCephMDSCephFSDataIntegration$`.

| Configuration | UTC controller time | Entire test | Restart leaf | Result |
| --- | --- | --- | --- | --- |
| IPv4 host, CRC, AES client/service keys, single BlueStore | 03:59:21–04:03:36 | 146.77 s | 41.90 s | 12/12 leaves, exit 0 |
| IPv6 container, secure, AES256K client/service keys, single MemStore | 03:59:20–04:03:36 | 148.07 s | 44.17 s | 12/12 leaves, exit 0 |

Both runs retained unsent TIDs 6/7, oldest TID 6, follows 2/3 and dirty
FILE_WR 4096. All four writes had ONDISK completion. The new incarnation
received eight caps and two realms: root `{1, seq 1, parent 0}` and the
directory realm `{0x10000000000, seq 4, parent 1}`. The cap-snap inode
`0x10000000004` carried follows 2. MDS GIDs changed 14575→17344 and
14552→17320 with new nonces and incarnation 6→10. The client GlobalID and
shared nonce remained unchanged; its existing OSD connection remained usable.

The object-request count was 178 immediately before restart and remained
178 after reconnect and the two explicit metadata sends. Each matching ACK
retained inode/follows/TID/dirty; the native A/B/C reads matched the planned
payload's SHA-256 and every 16 KiB chunk. Snapshot IDs 3/4 held A/B, and
head held C. Go/native clone lists matched for both objects, with no
overlap after the full overwrites. These Go byte comparisons are executed
assertions; object buffers and a complete wire capture are not archived.
The public result now does preserve raw OSD reply flags and write hashes.

Both cleanup receipts verified namespace/layout restoration, no residual
owned sessions, both identities removed, and only the exact old MDS address
removed from the baseline blocklist. Fresh cleanup confirmed filesystem and
pools removed. Each run then removed its nine owned containers and network.

[Machine-readable evidence](MDS_CAPSNAP_RECOVERY_LINUX_20261018.json)
records source hashes, public raw evidence hashes, exact flush/ACK metadata,
OSD completion flags/hashes/times, native reads and clones, adoption and
cleanup. The source snapshot was
`source-1` plus its ten listed Go files;
only documentation and this record changed afterward. Full root and fixture
Go tests, root and fixture vet, race checks for `integration` and `cephmsgr`,
and 422 Python tool tests passed on that snapshot. These are local selected
configurations, separate from CI at the final commit.

## Scope of the evidence

The FLUSHSNAPs are first sent on the new session; they were pending but
unsent on the old one. This tests retained dirty snapshot state and explicit
first transmission after failover. It does not establish replay or
deduplication after an uncertain FLUSHSNAP send, interrupted OSD writeback,
eviction with buffered data, multiple writers, concurrent server execution
or an OSD restart. MemStore ONDISK is its server completion flag, without
persistence across an OSD restart. The product still does not select
recovery state, replay file I/O or implement a capability cache.
