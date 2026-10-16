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

The tested tree is commit `e54b02101a5adfa719875c4ca635f8a235baea50`,
exported with `git archive` to a separate directory; this record (written
after the runs) is the only difference from it. Image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
(arm64), daemon version
`ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`.
Selector `^TestCephMDSCephFSDataIntegration$`.

| Configuration | Controller wall time (UTC, 2026-10-16) | Test | Result |
| --- | --- | --- | --- |
| Darwin arm64 host runtime, IPv4 relay, CRC, AES client and service keys, single BlueStore OSD | 00:37:41 to 00:40:09 (2 min 28 s) | 59.89 s | 8/8 leaves passed |
| CGO=0 Linux arm64 container runtime, IPv6, secure, AES256K client and service keys, single MemStore OSD | 00:40:37 to 00:43:06 (2 min 29 s) | 63.81 s | 8/8 leaves passed |

The shared capability agent changed for this test (the writer's side of
check_caps/send_cap, `update_inode_file_size` and `update_inode_file_time` for
grants and TRUNC), so the existing CephFS profile
(`^TestCephMDSCephFS(Push|Recovery)?Integration$`) was run from the same
export in the first configuration (00:43:21 to 00:48:05 UTC): push 7/7,
recovery 5/5 and metadata 9/9 leaves passed. The completion profile
(`^TestCephMDSCephFSCompletionIntegration$`), which also uses the agent, passed
8/8 leaves from the same export in the first configuration (00:48:50 to
00:53:15 UTC).

Leaf durations (CRC / secure, seconds): open 4.22 / 4.41, Go write 6.09 /
6.14, sparse read 6.05 / 6.43, truncate 8.06 / 8.53, truncate-sequence probe
2.32 / 3.02, concurrent reader 6.12 / 6.12, snapshot 13.55 / 14.42, cleanup
13.44 / 14.72. In both configurations the fixture cleanup completed: no
residual session, the namespace and layout gone natively, no new blocklist
entry, both identities removed (`cephfs_data_verified`), and the filesystem
and pools removed. Each run made one MDS dial and two OSD dials (the second
after the probe's refused reply).

### Observed values

Values are identical in both configurations unless two are given
(CRC / secure); inode numbers, sizes, sequences and object views were equal.
Go client GID 4932 / 4911; data pool 4, one PG on osd.0, OSDMap epoch 26 for
every object request.

Directory: inode `0x10000000000`; native readback `stripe_unit=65536
stripe_count=2 object_size=131072 pool=<fs>-data`; the Go LOOKUP InodeStat
layout {65536, 2, 131072, pool 4, no namespace}.

`go-file` (`0x10000000001`): CREATE caps `pAsxLsXsxFsxcrwb` (cap ID 1,
sequence 1, realm 1), layout equal to the directory's, max_size 65536 (the
stripe unit, `handle_client_openc`). The max_size UPDATE (caps 16349, wanted
16333, max_size 614523, size 0) was answered by a GRANT (sequence 2, Arrival
7) raising max_size from 65536 to 1310720 (`calc_new_max_size`: twice the
request rounded up to the 256 KiB period). Six WRITEs with SnapContext
{sequence 1, no snaps}: objects 0-3 of 131072 bytes, 4 of 65536 and 5 of
24699 bytes (user versions 1-6). The flush: op UPDATE, dirty Fw (4096), flush
TID 1, oldest flush TID 1, follows 1, FLAG_SYNC, size 614523, inline version
`CEPH_INLINE_NONE`; FLUSH_ACK (op 8, header TID 1, dirty Fw, cap ID 1,
sequence 2, OSD epoch barrier 26) after 1.70 / 0.44 ms. GETATTR then reported
size 614523 and the flushed mtime to the nanosecond. Native `statx`: size
614523, mtime seconds equal, change attribute 1; native read SHA-256 and
every 16 KiB chunk equal; native rados `stat` sizes equal to the Go STATs
above. The close UPDATE kept `pAsLsXsFs` (341) with wanted 0.

`native-file` (`0x100000003ea`): OPEN read-only caps `pAsLsXsFscr`, size
1620000. Objects 0 (65536 bytes), 1 (14464), 4 (130176), 5 (65536) and 12
(47136) exist; objects 2, 3 and 6-11 do not and were read as zeros (ENOENT),
the existing ones as short reads; the Go image, the planned image and the
native read are equal, and both object views agree, including object 13
absent.

`trunc-file` (`0x100000003eb`): OPEN read-only caps `pAsLsXsFscr`, size
1228800, truncate sequence 1. The native truncate produced one TRUNC push
(Arrival 24 / 25, cap sequence 3, caps `pAsLsXsFc` while Fr and Fs were revoked, size and
truncate_size 280000, truncate_seq 2, max_size 0, OSD epoch barrier 26); the
agent's size went from 1228800 to 280000. The objects were already trimmed at
the first Go STAT after the push: objects 0 and 1 kept 131072 bytes, object
2 (the start of [280000, 1310720)) was cut to 17856 and objects 3-9 to 0
bytes (they remain as empty objects). The Go read of 280000 bytes and the
native read are equal.

Truncate-sequence probe: the reopened file got Fw and a max_size GRANT of
786432. The 1000-byte WRITE at in-object offset 17856 of object 2 was
answered with result 0 and user version 35 (a committed no-op) and the cut
extent; cephmsgr returned `request outcome unknown: ... malformed frame:
invalid OSD reply: OSD reply extent differs from request` and failed the OSD
connection. A new connection stated object 2 at 17856 bytes; Go and native
reads still returned the truncated 280000 bytes.

`shared-file` (`0x10000000002`): CREATE caps `pAsxLsXsxFsxcrwb`, max_size
65536 raised to 524288 by a GRANT after the UPDATE asking for 200000. With
200000 bytes buffered and Fw dirty, the native reader's open was followed by
REVOKE sequence 4 (revoked `Fsxrw`, kept `Fcb`), answered by the dirty
metadata flush (flush TID 2, caps `pAsxLsXsxFcb`) and its FLUSH_ACK (Arrival
64 / 62); REVOKE sequence 5 (`Ax`); REVOKE sequence 7 (revoked `XxFsxcb`),
answered with Fb still implemented while the 2 s hold ran; the writeback of
objects 0 (131072 bytes) and 1 (68928) took 20 / 5 ms; the next UPDATE
dropped Fb (`pAsLsXsFrw`). The MDS then alternated revokes of Fw and Fsc that
were acknowledged at once. The native read returned all 200000 bytes 2.04 /
2.02 s after its process started (open at 12 / 11 ms), size 200000.

`snap-file` (`0x10000000003`): CREATE, max_size 1310720, six WRITEs of the
600000-byte first version with {1, no snaps}, flush TID 3 (follows 1),
close. LIST_SNAPS before the snapshot: heads only, sequence 1. The native
snapshot produced MClientSnap SPLIT (op 3, Arrival 107 / 94) of the directory
with split inodes of the directory and all five files and realm records
{`0x10000000000`, created 2, parent 1, parent_since 2, sequence 2, snaps [2]}
then {1, sequence 1}; the agent moved all six caps into the new realm. After
the read-write OPEN (cap realm `0x10000000000`, a max_size GRANT of 1310720
with size 600000) the snap context was {2, [2]}. Four WRITEs of the 320000
bytes at 400000: object 2 at 72320+58752, object 3 at 65536+65536, object 4
at 0+130176, object 5 at 0+65536; each object's user version advanced by 2
(clone and write). The flush: TID 4, follows 2, size 720000. LIST_SNAPS:
objects 0 and 1 heads only (sequence 1); objects 2-5 sequence 2 with clone 2
(snaps [2]) of sizes 131072, 131072, 65536 and 10176, overlaps [0, 72320),
[0, 65536), none and none, and heads of 131072, 131072, 130176 and 65536
bytes; the native `rados listsnaps` view is identical. The Go read at snap
id 2 and the native read of `.snap/<name>/snap-file` returned the first
version (600000 bytes, mtime of the first flush); the native head read
returned the 720000-byte image.

Cleanup: the snapshot removed, five UNLINKs and the RMDIR answered 0, the
layout removal read back as absent, native `stat` ENOENT; the release of
seven caps (root, directory, five files) dropped native `num_caps` from 7 to
0; Close was confirmed and no session remained.

### Product finding

`cephmsgr.OSDOperation` cannot carry `truncate_size` and `truncate_seq`
(the encoders write 0, `internal/osd/request.go`), and `validateOSDReply`
requires a WRITE reply's extent to equal the request
(`cephmsgr/osd_request.go`, "OSD reply extent differs from request").
Native CephFS clients send the inode's truncate sequence and size with every
extent operation (`Filer::write_trunc`, `Filer::read_trunc`). Without them a
write to an object that the MDS trimmed after a truncate is handled by
`PrimaryLogPG` as an old write that arrived after TRIMTRUNC and cut (here to
length 0, committed with result 0); the OSD's reply then carries the cut
extent, which cephmsgr treats as a malformed frame: the caller sees an
unknown outcome and loses the OSD connection, although the OSD answered a
valid reply. Reads have the matching gap: a consumer must clip reads at the
size of the newest TRUNC itself, because the OSD cannot apply a truncate
sequence the client does not send (`do_read`). Both are recorded by
`write_after_truncate_without_truncate_seq` and `native_truncate_trunc_push`.

### Not verified

Writes and reads that race an MDS truncation in flight (objects not yet
trimmed), a cap snap with dirty data (FLUSHSNAP) for a write that spans a
snapshot, several snapshots, files without the directory layout (the 4 MiB
default), pool namespaces, inline data, `O_TRUNC` opens, lazy I/O, multiple
writers in LOCK_MIX, sparse reads through `OSDSparseRead`, MDS failover or
eviction with dirty data, standby-replay, IPv4/IPv6 crossings and other OSD
stores. The multi-rank profile, which also uses the changed agent, and the
second configuration of the regression profiles were not rerun in this
change.
