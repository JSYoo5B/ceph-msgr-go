# CephFS file data acceptance

This records the earlier data acceptance. The later two-snapshot dirty-data
cap-snap results are recorded separately in
[MDS_CEPHFS_CAPSNAP_VALIDATION.md](MDS_CEPHFS_CAPSNAP_VALIDATION.md); the
historical runs and their limits below are unchanged.

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
SnapContext, READ at the head or a snap id, STAT, LIST_SNAPS, TRUNCATE) whose
READ, WRITE and TRUNCATE carry the extent truncate fields
(`OSDOperation.TruncateSize`, `TruncateSequence`).

Like the native client (`Client::_write` and `Client::_read_sync` call
`Filer::write_trunc` and `Filer::read_trunc` with `in->truncate_size` and
`in->truncate_seq`), every Go READ and WRITE of file data sends the inode's
truncate state from its InodeStat or TRUNC push. The per-object
truncate_size restates `Striper::object_truncate_size` (checked offline in
`TestCephFSStriperRestatement`: 280000 gives objects 0 and 1 131072, object 2
17856 and later objects 0). An inode the MDS created carries truncate_size
all ones and truncate_seq 1 (`Server.cc` 3639-3640); the native client sends
that pair unchanged and the OSD rewrites it to (0, 0) before executing
(`PrimaryLogPG.cc` 6088-6093). cephmsgr refuses the pair, so the test sends
the (0, 0) the OSD executes for never-truncated inodes.

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
| `write_after_truncate_with_truncate_seq` | Go reopens `trunc-file` read-write (Fw, a max_size GRANT), checks the InodeStat truncate state (280000, sequence 2) and writes 1000 bytes at the new end of file (object 2 at 17856) with truncate_size 17856 and truncate_seq 2: the OSD executes the full extent (reply Length 1000). The flush (size 281000) and GETATTR (size 281000, truncate state unchanged) follow; Go STAT, the native object view, the Go read and the native read equal the expected 281000-byte image |
| `stale_truncate_seq_write_trimmed` | Go replays a 1000-byte WRITE that straddles the end of object 2 (in-object 18356) with the previous truncate_seq 1. PrimaryLogPG cuts it at the object size: cephmsgr returns success, result 0, with the echoed Length 500 and both truncate fields; the object keeps its size, its first 500 bytes are written; both object views and both reads equal the expected image |
| `read_with_newer_truncate_seq_clips` | Go READs of object 0 (truncate_size 65536) and object 2 (10000) with truncate_seq 3, newer than both objects', return exactly the first 65536 and 10000 expected bytes with the truncate fields echoed; the same READs with the inode's state (131072/17856, sequence 2) return the whole objects. Objects, Go read and native read unchanged |
| `sequenced_truncate_noop_when_not_newer` | TRUNCATE of object 2 with offset = truncate_size = 17856 and truncate_seq 2 (equal) and 1 (older) return result 0 and leave the object at 18856 bytes. A TRUNCATE of the empty object 9 (beyond the end of file) at 0 with truncate_seq 3 applies; a 100-byte WRITE at 0 with the inode's sequence 2 is then cut to Length 0 with result 0. Objects, Go read and native read unchanged |
| `concurrent_reader_flushes_buffered_write` | CREATE `shared-file` with Fwb, max_size request, 200000 bytes buffered (not yet on the OSDs), Fw dirty, a 2 s writeback hold. Native `read-shared` opens the file and reads it: the MDS revokes Go's caps; the first revoke is answered with the dirty metadata flush (FLUSH_ACK follows), a later revoke of Fb starts the held writeback, cap messages keep Fb until the writeback is done and the next one drops it. The native read returns the 200000 flushed bytes after at least the hold; both object views equal the expected sizes |
| `snapshot_write_clones_objects` | CREATE `snap-file`, write 600000 bytes with the root realm's context (sequence 1, no snaps), flush, close; LIST_SNAPS shows only heads with sequence 1. Native `mkdir .snap/<name>`: an MClientSnap SPLIT makes the directory a realm (sequence = the snap id, snaps [snap id], parent 1) and moves the Go caps. OPEN read-write: cap realm = the directory; the context becomes {snap id, [snap id]}. The Go client writes 320000 bytes at 400000 with it and flushes with follows = the snap id. LIST_SNAPS: every rewritten object has a clone with the snap id, the old size and an overlap of the old extent minus the rewritten range (`make_writeable`), untouched objects only their head; the native `rados listsnaps` view is identical. The Go snap-id read and the native read of `.snap/<name>/snap-file` equal the pre-snapshot bytes (size 600000, mtime of the first flush); the native head read equals the new image (720000 bytes) |
| `cleanup_unlink_release_close` | Native snapshot removal, UNLINK of every file, native `layout-clear` (readback absent), RMDIR of the directory, native ENOENT, release of every cap to native `num_caps` 0, confirmed Close and no session left |

### Truncate fields

The four leaves after `native_truncate_trunc_push` exercise the OSD's
truncate-sequence handling on `trunc-file`, whose trimmed objects carry the
sequence 2 from the MDS's TRIMTRUNC and whose untouched objects 0 and 1 the
native writer created with the never-truncated pair (OSD sequence 0). A WRITE
with the current sequence executes in full; a WRITE whose sequence is older
than the object's and that extends past the object is cut at the object size
(`PrimaryLogPG.cc` 6768-6777); a READ whose sequence is newer than the
object's is clipped at truncate_size (5849-5853); a sequenced TRUNCATE is a
no-op unless its sequence is newer (6986-6992). The stale write and the
newer-sequence TRUNCATE are test constructions (a write delayed past a
truncate, a truncation the MDS did not send) on objects of the owned file;
the stale write stays inside the file size and the newer TRUNCATE touches
only an empty object beyond the end of file, so the MDS metadata stays
consistent with the objects and the native reads.

## Results

The tested tree is git tree `a7c4d10c103bc86bef0c075bdbd4d74676523ce9`: the
working tree at origin/main `bc83bf4` plus the uncommitted extent
truncate-field product change and the sparse-read changes in progress, with
this test change, exported to a separate directory before the runs; this
record (written after the runs) is the only difference from it. Image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
(arm64), daemon version
`ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`.
Selector `^TestCephMDSCephFSDataIntegration$`.

| Configuration | Controller wall time (UTC, 2026-10-16) | Test | Result |
| --- | --- | --- | --- |
| Darwin arm64 host runtime, IPv4 relay, CRC, AES client and service keys, single BlueStore OSD | 01:12:40 to 01:15:21 (2 min 41 s) | 76.79 s | 11/11 leaves passed |
| CGO=0 Linux arm64 container runtime, IPv6, secure, AES256K client and service keys, single MemStore OSD | 01:15:36 to 01:18:15 (2 min 39 s) | 74.78 s | 11/11 leaves passed |

The product's OSD reply check changed with the truncate fields (a WRITE reply
may echo a shorter Length, and both truncate fields must echo the request),
so the OSD mutation selection `^TestCephOSDMutation` was run from the same
export on the CI row with the first configuration's keys and transport (CRC,
AES/AES, IPv4, host relay, three BlueStore OSDs, replicas 3/min_size 2,
01:18:31 to 01:19:46 UTC): the mutation, lost-reply, restart, concurrency,
crash and close tests (6/6 top-level tests) passed.

Leaf durations (CRC / secure, seconds): open 5.02 / 4.21, Go write 6.08 /
7.05, sparse read 6.55 / 6.03, truncate 8.15 / 8.02, sequenced write 4.26 /
4.05, stale-sequence write 5.03 / 4.22, newer-sequence read 4.13 / 4.01,
sequenced TRUNCATE 4.03 / 4.23, concurrent reader 6.12 / 6.12, snapshot
13.65 / 13.20, cleanup 13.75 / 13.62. In both configurations the fixture
cleanup completed: no residual session, the namespace and layout gone
natively, no new blocklist entry, both identities removed
(`cephfs_data_verified`), and the filesystem and pools removed. Each run
made one MDS dial and one OSD dial.

### Observed values

Values are identical in both configurations unless two are given
(CRC / secure); inode numbers, sizes, sequences and object views were equal.
Go client GID 4929 / 4938; data pool 4, one PG on osd.0, OSDMap epoch 26 for
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
(Arrival 25, cap sequence 3, caps `pAsLsXsFc` while Fr and Fs were revoked, size and
truncate_size 280000, truncate_seq 2, max_size 0, OSD epoch barrier 26); the
agent's size went from 1228800 to 280000. The objects were already trimmed at
the first Go STAT after the push: objects 0 and 1 kept 131072 bytes, object
2 (the start of [280000, 1310720)) was cut to 17856 and objects 3-9 to 0
bytes (they remain as empty objects). The Go read of 280000 bytes and the
native read are equal.

Truncate state sent to the OSD: `go-file`, `native-file`, `shared-file` and
`snap-file` (and `trunc-file` before the truncate) had the never-truncated
InodeStat state (truncate_size 18446744073709551615, truncate_seq 1), sent as
(0, 0). After the TRUNC push the agent held (280000, 2); the Go reads of
`trunc-file` sent truncate_size 131072 to objects 0 and 1 and 17856 to object
2 with truncate_seq 2.

Sequenced write: the reopened file had caps `pAsxLsXsxFsxcrwb`, InodeStat
truncate state (280000, 2) and a max_size GRANT of 786432 (cap sequence 6).
The 1000-byte WRITE of `100000003eb.00000002` at 17856 (truncate_size 17856,
truncate_seq 2, SnapContext {1, no snaps}) returned Length 1000, user
version 35 (replay 26'35). The flush (TID 2, size 281000, truncate_seq 2) was
acknowledged after 1.31 / 0.33 ms; GETATTR reported size 281000, truncate_size
280000 and truncate_seq 2. Object 2 became 18856 bytes; both object views
otherwise kept the trimmed sizes, and the Go and native reads returned the
281000-byte image.

Stale-sequence write: the 1000-byte WRITE of object 2 at 18356 with
truncate_size 17856 and truncate_seq 1 returned result 0, user version 37
(replay 26'37) and the echoed extent offset 18356, Length 500, truncate_size
17856, truncate_seq 1. Object 2 stayed 18856 bytes, and both reads returned
the image with the first 500 payload bytes at file offset 280500.

Newer-sequence reads (truncate_seq 3): object 0 with truncate_size 65536
returned 65536 bytes (user version 22), object 2 with 10000 returned 10000
bytes (user version 37), each equal to the expected object prefix with both
truncate fields echoed. With the inode's state the same READs returned
131072 and 18856 bytes.

Sequenced TRUNCATEs: object 2 at 17856 with truncate_seq 2 and then 1
returned result 0, Length 0 and replay versions 26'40 and 26'41 while the
user version stayed 37 and the object 18856 bytes. Object 9 (empty) at 0 with
truncate_seq 3 returned replay and user version 26'42 / 42; the following
100-byte WRITE at 0 with truncate_seq 2 returned result 0 with Length 0
(replay and user version 26'43 / 43) and object 9 stayed empty.

`shared-file` (`0x10000000002`): CREATE caps `pAsxLsXsxFsxcrwb`, max_size
65536 raised to 524288 by a GRANT after the UPDATE asking for 200000. With
200000 bytes buffered and Fw dirty, the native reader's open was followed by
REVOKE sequence 4 (revoked `Fsxrw`, kept `Fcb`), answered by the dirty
metadata flush (flush TID 3, caps `pAsxLsXsxFcb`) and its FLUSH_ACK (Arrival
64 / 62); REVOKE sequence 5 (`Ax`); REVOKE sequence 7 (revoked `XxFsxcb`),
answered with Fb still implemented while the 2 s hold ran; the writeback of
objects 0 (131072 bytes) and 1 (68928) took 20 / 5 ms; the next UPDATE
dropped Fb (`pAsLsXsFrw`). The MDS then alternated revokes of Fw and Fsc that
were acknowledged at once. The native read returned all 200000 bytes 2.04 /
2.02 s after its process started (open at 12 / 11 ms), size 200000.

`snap-file` (`0x10000000003`): CREATE, max_size 1310720, six WRITEs of the
600000-byte first version with {1, no snaps}, flush TID 4 (follows 1),
close. LIST_SNAPS before the snapshot: heads only, sequence 1. The native
snapshot produced MClientSnap SPLIT (op 3, Arrival 153 / 135) of the directory
with split inodes of the directory and all five files and realm records
{`0x10000000000`, created 2, parent 1, parent_since 2, sequence 2, snaps [2]}
then {1, sequence 1}; the agent moved all six caps into the new realm. After
the read-write OPEN (cap realm `0x10000000000`, a max_size GRANT of 1310720
with size 600000) the snap context was {2, [2]}. Four WRITEs of the 320000
bytes at 400000: object 2 at 72320+58752, object 3 at 65536+65536, object 4
at 0+130176, object 5 at 0+65536; each object's user version advanced by 2
(clone and write). The flush: TID 5, follows 2, size 720000. LIST_SNAPS:
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

### Product findings

With `OSDOperation.TruncateSize` and `TruncateSequence` the Go client sends
the truncate state as the native client does, and the earlier finding (a
WRITE after a truncate was cut by the OSD and its valid reply refused as a
malformed frame) no longer occurs. Two consumer-visible behaviors remain.

A WRITE that the OSD trims (here the stale-sequence write) completes with
result 0 and a reply Length shorter than the request (500 of 1000). The
caller sees the trim only by comparing Length; the test's file writer treats
any shorter Length as an error.

cephmsgr refuses truncate_size all ones with truncate_seq 1, which is the
state of every inode the MDS created and which the native client sends with
every extent operation on a never-truncated file. The OSD executes that pair
as (0, 0), so a consumer must rewrite it to (0, 0) before sending; the test
does so.

### Not verified

A cap snap with dirty data (FLUSHSNAP) for a write that spans a
snapshot, several snapshots, files without the directory layout (the 4 MiB
default), pool namespaces, inline data, `O_TRUNC` opens, lazy I/O, multiple
writers in LOCK_MIX, sparse reads through `OSDSparseRead`, MDS failover or
eviction with dirty data, standby-replay, IPv4/IPv6 crossings and other OSD
stores. The multi-rank profile, which also uses the changed agent, and the
second configuration of the regression profiles were not rerun in this
change. For the truncate fields: a Go write that races an MDS truncation in
flight (the "write arrives before trimtrunc" path that truncates the object
first), sequenced READs and WRITEs through MOSDOp v8 (`Placement`), sparse
reads with truncate fields, and the `shared-file` revoke sequences and
arrivals above, which were recorded in the earlier run and not rechecked
against these runs (the flush TIDs, max_size grants and the snapshot SPLIT
arrival were).

## ceph-testcontainers-go v0.3.0 key input

Local runs on 2026-10-16 from clean `git archive` exports, darwin/arm64,
Go 1.27.2, Docker Engine 29.8.2 (4 CPUs, 4 GiB), Ceph image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a4...`, ceph-testcontainers-go
v0.3.0. Configuration (a): aes/aes, CRC, IPv4, runtime `host`, BlueStore.
Configuration (b): aes256k/aes256k, secure, IPv6, runtime `container`,
MemStore. Both use the legacy-v6 single-OSD fixture.

The native-only keys reach their workers as one bounded
`NativeCommand.Input` record (`native_key_input.go`); no native keyring is
written to the host control directory. Every worker run checks
`InputWritten`, `InputDelivered`, `InputEOF` and the worker's byte-count
receipt separately from `Complete()`. The attach path ran from the host
fixture into the client container under both runtimes.

`rados listsnaps` reads a 0600 keyring that the worker writes in a private
directory of the client container and removes at exit.

| Export | Configuration | Result | Wall time |
| --- | --- | --- | --- |
| `36d5734` | (a) | PASS (test 74.66 s) | 157 s |
| `36d5734` | (b) | PASS (test 78.06 s) | 159 s |
