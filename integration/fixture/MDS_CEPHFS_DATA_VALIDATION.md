# CephFS file data acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go,
without CGO, librados, go-ceph, Ceph CLI subprocesses or native runtime
dependencies. The disposable provider, its native CLI, the native LibCephFS
and librados oracle and the owned MDS are development-only. This acceptance
covers the item [the CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md)
and SPEC.md (M2) list as not verified: OSD data I/O combined with an MDS
session, including a dirty-data cap flush. The owned recipe is described in
[MDS_CEPHFS_DATA_FIXTURE.md](MDS_CEPHFS_DATA_FIXTURE.md).

File layout striping, object naming, snap contexts, size and time
bookkeeping and every cap flush decision are test-only consumer logic in
`integration/mds_cephfs_data_test.go` and the shared capability agent
(`integration/mds_cephfs_agent_test.go`). The product supplies the MDS
session, requests, reply traces, cap pushes, `SendCaps`/`ReleaseCaps`,
`FlushJournal` and explicitly targeted OSD requests (WRITE with a
SnapContext, READ at the head or a snap id, STAT, LIST_SNAPS).

## Test shape

`TestCephMDSCephFSDataIntegration` runs alone in its profile
(`CEPH_MSGR_TEST_MDS_CEPHFS_DATA=1`, MDS_FRESH=1, `legacy-v6`). One Go client
authenticates as `client.mds-cephfs-data-<token>` with MDS, CephFS and OSD
enabled (default v6 object-hash profile), dials the MDS through a counted
relay and keeps one session (`entity_id`, `hostname`
`ceph-msgr-go-<token8>-data`, `root=/`, 2 s RenewInterval, 1024 event
slots). Requests use caller and owner UID/GID 0; every mutation waits for
its safe reply after `FlushJournal`.

Object placement is the test's own: the object locator and raw hash of each
`<ino hex>.<objno %08x>` object come from the MON's `osd map` on the data
pool, and the target is its single acting primary (one PG, one OSD).
`cephfsStriping.fileToExtents` restates `Striper::file_to_extents` (one
extent per object, sorted by object number, buffer extents per stripe unit)
and is checked offline against hand-computed extents and an
`extent_to_file` round trip (`TestCephFSStriperRestatement`). Snap contexts
restate the client `SnapRealm::build_snap_context` over the realms of reply
snap traces and snap pushes.

The capability agent gained the writer's side of the native client for this
test: a file the test wrote is registered dirty (FILE_WR) with its snap
follows; check_caps/send_cap then flush the dirty metadata in the next cap
message (op UPDATE with the dirty mask, a flush TID numbered from 1, the
oldest outstanding flush TID, the inline version) and request max_size as
get_caps does (wanted_max_size in the UPDATE); FILE_BUFFER counts as used
while buffered data is unwritten; a revoke of Fb in use starts the buffered
data writeback and the revocation completes only in the cap message after it
(handle_cap_grant, `_flushed`); FLUSH_ACK, TRUNC and max_size grants are
recorded; reply and push sizes and times follow `update_inode_file_size` and
`update_inode_file_time`. Native libcephfs sends a flush as op UPDATE with a
dirty mask, never `CEPH_CAP_OP_FLUSH` (`Client::send_cap`), and
`Locker::handle_client_caps` treats both alike (only FLUSHSNAP is special),
so the test sends UPDATE.

| Leaf | Checked behavior |
| --- | --- |
| `open_data_session_and_layout` | Session Open; native `session ls` open with the declared features, metadata and `<ip>:0/<CephFSNonce>`, no caps. Native `layout-set` creates the directory with the planned layout (readback equal). The Go LOOKUP of the directory returns the same inode and the directory layout in its InodeStat |
| `go_striped_write_flush` | CREATE `go-file` (O_CREAT\|O_EXCL\|O_RDWR): created-inode extra, layout equal to the directory's, Fw issued, max_size equal to the stripe unit. A max_size UPDATE (wanted 614523) is answered by a GRANT. The Go client writes the payload object by object with the realm's snap context, marks Fw dirty with a new mtime and flushes (FLAG_SYNC); FLUSH_ACK carries the flush TID, dirty Fw and cap ID. GETATTR returns the size and the exact mtime; the Go read-back equals the payload. After close, native `statx` (size, inode, mtime seconds, layout), native read (SHA-256 and every 16 KiB chunk) and both object views (Go STAT, native rados `stat`) equal the expected per-object sizes |
| `native_sparse_file_read` | Native writes three extents of `native-file` (holes in between). The Go LOOKUP and OPEN (read-only, Fr) report the size and layout; the Go read through the layout reads missing objects (ENOENT) and short objects as zeros and equals the planned image, the native read and both object views (written objects with their sizes, the rest absent) |
| `native_truncate_trunc_push` | Native writes `trunc-file`; the Go client opens it read-only and reads it. Native `truncate` to 280000: a TRUNC push arrives with size and truncate_size 280000 and truncate_seq +1, the agent applies it, Fr is issued again, the Go read up to the TRUNC size equals the native read. The objects reach the sizes `Filer::truncate` and the OSD TRUNCATE imply (each object of [size, old size rounded up to the period) cut to the in-object start of that range, objects wholly beyond left empty), checked by Go STAT and the native object view |
| `write_after_truncate_without_truncate_seq` | Below (product finding) |
| `concurrent_reader_flushes_buffered_write` | CREATE `shared-file` with Fwb, max_size request, 200000 bytes buffered (not yet on the OSDs), Fw dirty, a 2 s writeback hold. Native `read-shared` opens the file and reads it: the MDS revokes Go's caps; the first revoke is answered with the dirty metadata flush (FLUSH_ACK follows), a later revoke of Fb starts the held writeback, cap messages keep Fb until the writeback is done and the next one drops it. The native read returns the 200000 flushed bytes after at least the hold; both object views equal the expected sizes |
| `snapshot_write_clones_objects` | CREATE `snap-file`, write 600000 bytes with the root realm's context (sequence 1, no snaps), flush, close; LIST_SNAPS shows only heads with sequence 1. Native `mkdir .snap/<name>`: an MClientSnap SPLIT makes the directory a realm (sequence = the snap id, snaps [snap id], parent 1) and moves the Go caps. OPEN read-write: cap realm = the directory; the context becomes {snap id, [snap id]}. The Go client writes 320000 bytes at 400000 with it and flushes with follows = the snap id. LIST_SNAPS: every rewritten object has a clone with the snap id, the old size and an overlap of the old extent minus the rewritten range (`make_writeable`), untouched objects only their head; the native `rados listsnaps` view is identical. The Go snap-id read and the native read of `.snap/<name>/snap-file` equal the pre-snapshot bytes (size 600000, mtime of the first flush); the native head read equals the new image (720000 bytes) |
| `cleanup_unlink_release_close` | Native snapshot removal, UNLINK of every file, native `layout-clear` (readback absent), RMDIR of the directory, native ENOENT, release of every cap to native `num_caps` 0, confirmed Close and no session left |

### Write after a truncate without truncate_seq

The product's OSD extent operations always carry `truncate_size` and
`truncate_seq` 0 (`cephmsgr.OSDOperation`: "Truncate-sequence variants are
unsupported"). After the native truncate, the MDS's `TRIMTRUNC` gave every
trimmed object its truncate sequence. The leaf reopens `trunc-file`
read-write (Fw, a max_size grant) and writes 1000 bytes at the new end of
file, which is the in-object end of the trimmed object 2. `PrimaryLogPG`
handles the WRITE as "old write, arrived after trimtrunc" (the object's
sequence is newer than the op's 0): it cuts the extent length to 0, commits a
no-op and echoes the cut extent. The leaf requires that cephmsgr refuses that
reply as malformed (`OSD reply extent differs from request`) with an unknown
outcome, that a new OSD connection then stats the object at its trimmed size,
and that the Go and native reads still equal the truncated content. The
file's metadata is not changed.

## Results

Pending: the runs of the committed tree are recorded here.
