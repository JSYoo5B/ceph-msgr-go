# O2h snapshot context, LIST_SNAPS and ROLLBACK validation

Run window: 2026-10-15 Asia/Seoul. Product subset: a read snap id
(`ReadSnapshot`), a write SnapContext with optional ORDERSNAP,
CEPH_OSD_OP_ROLLBACK 0x220e and CEPH_OSD_OP_LIST_SNAPS 0x120a.
[SNAPSHOT_CONTRACT.md](../../internal/osd/SNAPSHOT_CONTRACT.md) records the
pinned source semantics. Source is Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`; image is
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Independent codec and local boundaries

The first `encode-osd-snap` SDK run compiled a fresh wrapper against the same
generated headers and libceph-common as the earlier proofs. Four requests and
four replies were encoded and separately decoded. Go header/front/data match
all four requests byte for byte on the first comparison: READ+STAT at snap 4
(249/0 bytes), an ORDERSNAP WRITEFULL with SnapContext 7/[7,4,2] (235/2), a
ROLLBACK to snap 4 under the same context (235/0) and LIST_SNAPS at SNAPDIR
(211/0). The pinned `rollback` and `list_snaps` builders add no per-op flag.
Replies decode to a three-entry listing (clones 2 and 7 with overlaps, head
size 11, seq 7), a ROLLBACK echo with ACK|ONDISK, an EOLDSNAPC -85 refusal
whose flags follow `reply_op_error` (ONDISK and the echoed ORDERSNAP, no ACK,
zero versions) and a clone read. The same replies complete public requests
through a synthetic peer. Provenance and the offline seq/ACK/priority caveat
are in the [native fixtures](../../internal/osd/testdata/snap-native-README.md).

Internal and public synthetic tests cover read snap 0/NOSNAP as the head,
SNAPDIR refusal, a snapshot on any writing vector (including ROLLBACK), a
SnapContext or ORDERSNAP on a read vector, unordered, duplicate, zero and
above-seq snap ids, seq above CEPH_MAXSNAP, snaps beyond the frame, ROLLBACK
and LIST_SNAPS beside another operation, ROLLBACK to 0 or above MAXSNAP,
FAILOK and field isolation in both directions, the listing budget, version 1
and newer compatible listings, malformed listings (incompatible version,
trailing bytes, truncation, head not last, head with snaps, SNAPDIR clone,
oversized count), a changed ROLLBACK echo, ROLLBACK success without ONDISK,
and no resend.

## Live native and Go snapshots

The oracle allocates two self-managed snap ids with
`rados_ioctx_selfmanaged_snap_create` (the first ids on the fixture pool are 2
and 3), seeds an owned object with known bytes, one xattr and one OMAP key,
and reserves absent names. Every Go case has a native twin: the twin repeats
the same operations through librados with
`rados_ioctx_selfmanaged_snap_set_write_ctx`, `rados_ioctx_snap_set_read`,
`rados_ioctx_selfmanaged_snap_rollback` and ORDERSNAP through
`rados_write_op_operate2`. Native reads at a snap return bytes, stat, xattrs,
OMAP and the clone's object_info decoded by the pinned `ceph-dencoder`.
Listings are compared with `rados listsnaps --format json` for the Go object
and its twin, and Go's raw LIST_SNAPS bytes are decoded by `ceph-dencoder type
obj_list_snap_response_t`.

| Actual profile | Observed result |
| --- | --- |
| CRC/AES entity/service, IPv4, Darwin arm64 host relay, three BlueStore/min2 | 11/11 subcases in 17.93s on the second run |
| CRC/AES256K entity/service, IPv4, Darwin arm64 host relay, single MemStore | whole OSD suite 25 passed and 4 BlueStore-only skipped; snapshot 11/11 |
| secure/AES256K entity/service, IPv4, Linux arm64 container, three BlueStore/min2, CGO0 binaries | whole OSD suite 29/29 top-level, snapshot 11/11; no FAIL/SKIP |

| Case | Result (Go equals the native twin) |
| --- | --- |
| LIST_SNAPS on an absent object | -2; native CLI -2 |
| WRITEFULL+SETXATTR+SETVALS under {2,[2]}, then WRITE 2~2 under {3,[3,2]} | ONDISK; clone 2 keeps the seed (user version 2), clone 3 the first write (user version 4), head user version 6; each clone write consumes one extra version |
| Reads at snap 2, snap 3, head and snap 1003 | bytes, size, nanosecond mtime, xattrs, OMAP and user version equal the native read at the same snap; snap 1003 (above the snapset seq) returns the head |
| LIST_SNAPS after two clones | seq 3; clone 2 size 8 with no overlap, clone 3 size 22 with overlap [0,2) and [4,22), head size 22; equal to the CLI listing of the object and of its twin and to `ceph-dencoder` of the Go raw bytes |
| ORDERSNAP WRITEFULL under {2,[2]} after seq 3 | -85 for Go and native, user version 0, object state unchanged |
| ROLLBACK to snap 2 under {3,[3,2]} with explicit mtime | ONDISK (flags 0x4000025), seed bytes/xattr/OMAP restored like the twin, head mtime equals the request mtime, user version 12 |
| ROLLBACK to snap 2 with zero mtime | success, version advances, head mtime unchanged |
| ROLLBACK to snap 1003 | success, only the version advances, for Go and the twin |
| DELETE under {3,[3,2]} | head becomes a whiteout: Go read of the head -2, read at snap 2 still returns the seed, listing has clones 2 and 3 and no head, like the twin |
| ROLLBACK of an absent object | 0 with ACK\|ONDISK and user version 0 for Go and native; the object stays absent |
| WRITEFULL creating an object under {3,[3,2]}, then ROLLBACK to snap 2 | the new object lists seq 3 with only a head; ROLLBACK returns 0 and deletes it, for Go and native |

The first run passed the first seven subcases and then failed in the test
helper, which asked native placement for an existing head before reading a
clone of the whiteout object. The second run routes whiteout objects as
absent heads.

After the run, oracle cleanup removed owned heads (all already absent or
whiteouts) and then both snap ids; native listings returned ENOENT for every
owned name after snap trimming (1.5s) and the three-OSD fixture was clean
again; no daemon recorded a crash. Logs and diagnostics:
`/tmp/ceph-osd-snapshot-crc-aes-20261015-first`,
`/tmp/ceph-osd-snapshot-crc-aes-20261015-second`,
`/tmp/ceph-osd-whole-memstore-aes256k-crc-host-snapshot-20261015-first` and
`/tmp/ceph-osd-whole-secure-aes256k-container-snapshot-20261015-first`.

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDSnapshotIntegration$' \
sh integration/run.sh
```

## Snapshot SPARSE_READ

Run window: 2026-10-16 01:02 to 01:06 UTC. Product subset: SPARSE_READ with a
nonzero `ReadSnapshot` (admitted by `internal/osd/snapshot.go`
`requestSnapshot`; contract in
[SPARSE_READ_CONTRACT.md](../../internal/osd/SPARSE_READ_CONTRACT.md)).
`TestCephOSDSnapshotSparseReadIntegration` compares every Go request with the
independent native C++ client on the same object, snap id, offset and length:
`tools/osd_sparse_read_oracle.cc` now takes an optional read snap id and calls
`IoCtx::snap_set_read` before `IoCtx::sparse_read`; the Python observer
admits an optional `snap` (nonzero, below CEPH_NOSNAP) and still records C
librados head snapshots before and after. The helper was rebuilt for this
wrapper in an owned container from the pinned source and generated
configuration with GCC toolset 14 against the image's `librados.so.2`
(SHA-256
`b630ea9d633d724bacfb116815915c852374d0a8ca351bdc488f0bbae4fb4bac`, the same
library as the earlier helper):
wrapper SHA-256 `849917e1478b5bc8359608cd39b1d75edbfba8ba3ea32f90354cc8b57ed02557`,
binary SHA-256 `324804db60fe0da38c02351c15ec28b76c51c78ea29db1e09547a1dcc1311e75`.
The fixture staged it only after its proof matched the export's wrapper.

Each case compares the native result code with the Go reply code (and the
`OSDError` for a failure), the extent map, the bytes, the raw encoded output
(`u32` count, offset/length pairs, `u32` byte count, bytes) in both the
operation data and `RawData`, the operation offset echo and length, and the
reply user version with the native `get_last_version`. The expanded bytes
must equal the test's model of the object at that snap, a Go READ of the same
range at the same snap must return those zero-filled bytes with the same user
version, and a second native read afterwards must observe the same map,
bytes, version and head state.

Setup, all native (snapshot oracle, librados C API): two self-managed snap
ids, 2 and 3, on the fixture pool. Before any snapshot write (SnapContext
0/[]): Y = the 31-byte seed plus 4 KiB at 12 KiB; X = 4 KiB at 0 and 4 KiB at
16 KiB; W = 4 KiB at 0 and 4 KiB at 12 KiB. Under 2/[2]: X gets 4 KiB at
8 KiB and 4 KiB at 28 KiB, Z is created with 4 KiB at 4 KiB, W is deleted.
Native `rados listsnaps` then shows, on both profiles: Y seq 0, head 16384;
X seq 2, clone 2 size 20480 with overlaps 0~8192 and 12288~8192, head 32768;
Z seq 2, head 8192 and no clone; W seq 2, clone 2 size 16384 with no overlap
and no head.

| Actual profile | Controller wall time (UTC) | Result |
| --- | --- | --- |
| CRC/AES entity/service, IPv4, Darwin arm64 host relay, three BlueStore/min2 | 01:02:54 to 01:04:29 | snapshot SPARSE_READ 16/16 (14.97 s), snapshot 11/11, SPARSE_READ 10/10; no FAIL/SKIP |
| secure/AES256K entity/service, IPv6, CGO=0 Linux arm64 container, single MemStore | 01:04:41 to 01:06:01 | 16/16 (13.65 s), 11/11, 10/10; no FAIL/SKIP |

Go and native agreed in every case below. Extents are offset~length in bytes;
a success has native result = extent count and Go per-op result 0.

| Case (object, snap, offset~length) | Resolves to | BlueStore extents | MemStore extents | User version |
| --- | --- | --- | --- | --- |
| X, 2, 0~65536 | clone 2 (20480 bytes) | 0~4096, 16384~4096 | 0~20480 | 4 |
| X, 2, 2048~16384 | clone 2 | 2048~2048, 16384~2048 | 2048~16384 | 4 |
| X, 2, 6144~4096 (a hole) | clone 2 | none (raw 8 bytes) | 6144~4096 | 4 |
| X, 2, 14336~16384 | clone 2, clipped at its 20480-byte end | 16384~4096 | 14336~6144 | 4 |
| X, 2, 20480~4096 (clone EOF) | clone 2 | none, length 0 | none, length 0 | 4 |
| X, 2, 28672~4096 (past clone EOF, data at the head) | clone 2 | none, length 0 | none, length 0 | 4 |
| X, head, 0~65536 | head (32768 bytes) | 0~4096, 8192~4096, 16384~4096, 28672~4096 | 0~32768 | 7 |
| X, 3, 0~65536 | head (3 > snapset seq 2) | as the head | as the head | 7 |
| X, 1003, 0~65536 | head | as the head | as the head | 7 |
| Y, 2, 0~65536 | head (snapset seq 0) | 0~31, 12288~4096 | 0~16384 | 3 |
| Y, head, 0~65536 | head | 0~31, 12288~4096 | 0~16384 | 3 |
| Z, 2, 0~65536 | no clone covers 2 (seq 2, no clones) | -2 ENOENT | -2 ENOENT | 0 |
| Z, 3, 0~65536 | head | 4096~4096 | 0~8192 | 8 |
| W, 2, 0~65536 | clone 2 of the deleted head | 0~4096, 12288~4096 | 0~16384 | 5 |
| W, 2, 2048~12288 | clone 2 | 2048~2048, 12288~2048 | 2048~12288 | 5 |
| W, head, 0~65536 | whiteout head | -2 ENOENT | -2 ENOENT | 0 |

Reply flags were 0x4000015 for every success and 0x4000014 for both ENOENT
replies; no retry; request and reply epochs 21 (BlueStore) and 14 (MemStore)
throughout. The clone's own size bounds the read (cases at 20480 and 28672
return nothing although the head holds data there), and the extents are the
store's map of the clone object: BlueStore keeps the clone's holes
(logical extents at the written 4 KiB ranges, also through a range that
starts inside a hole), MemStore reports the whole selected range clipped to
the resolved object's end, holes included. The ENOENT for Z at snap 2 shows
that a snap id at or below the snapset seq with no covering clone is
`-ENOENT`, not the head: pinned `PrimaryLogPG::find_object_context`
returns the head only when the snap id exceeds the snapset seq
(PrimaryLogPG.cc:12169) and otherwise the first clone at or above the snap id,
or -ENOENT when there is none (12187-12194). Cleanup removed the four objects
and both snap ids; native snap trim finished in 814 ms (BlueStore) and 810 ms
(MemStore), and the fixture was clean before the snapshot test (snaps 4 and 5)
and the head SPARSE_READ test ran.

Tested source: tree `75285aba4d7d87422f1d5382a0ed4b0cdb3d7cd3` (HEAD
`3c22e90`, the staged SPARSE_READ snapshot admission, and this change's test,
fixture and tool files; not these documents), exported with `git archive` to
a separate directory: 1210 files, SHA-256 of the sorted per-file SHA-256 list
`608fd327282e22ce324c74863502909cf96b15609d74104ee89443312dbfec74`. Other
unstaged product edits present in the shared checkout at the time (a separate
truncate change) were not part of the export. Evidence:
`osd-snap-sparse-read-results.json` in each diagnostics directory.

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_SPARSE_READ_ORACLE=<helper beside its .proof.json> \
CEPH_MSGR_TEST_RUN='^TestCephOSD(Snapshot|SparseRead|SnapshotSparseRead)Integration$' \
sh integration/run.sh
```

## Shared pool mode

Snap allocation puts the shared fixture pool into self-managed snapshot mode
for the rest of the run, and every allocation or removal advances the OSDMap
epoch. Bootstrap pool policy checks run once and inspect only HASHPSPOOL and
creation. Tests that run later send an empty SnapContext to objects without a
snapset, so they create no clones; in both whole-suite runs every OSD test
after the snapshot test passed, including the xattr, crash and restart tests
on three BlueStore replicas. Pool snapshots on this pool would now be refused
by the MON; no test uses them.

## Remaining gaps

Snapshot SPARSE_READ on three-replica MemStore or single BlueStore, at a
snap id in the removed-snaps queue, on an EC pool or inside a compound vector
is not characterized. Response loss and SIGKILL during ROLLBACK or a cloning write, pool-snapshot
mode (where the server substitutes the pool context), a ROLLBACK refused with
-EBUSY by a watcher, removed snaps still queued for trimming, read-only
credentials, EC pools, broader profiles and remote CI for this subset need
separate evidence.

## SIGKILL durability (2026-10-17)

`TestCephOSDSnapshotDurabilityIntegration`: on an owned pool, a self-managed
snapshot id allocated with `Client.PoolOp` serves as the SnapContext of an
ONDISK Go WRITEFULL, which creates a clone. The object's primary is SIGKILLed
and restarted and PGs return to clean; native reads then return the new head
(with the receipt's version) and the original bytes at the snapshot id, and a
Go LIST_SNAPS still names the snapshot. Passed on legacy-v6 single BlueStore
(secure, IPv4) and replicated-pg-v8 with three BlueStore replicas. 
`TestCephOSDSnapshotLostReplyIntegration`: a relay drops the reply of a Go
WRITEFULL under a self-managed SnapContext after it executes; the caller gets
an unknown outcome, and an explicit resubmission on a fresh connection returns
success with the native head version and ONDISK. Native reads show the new head
and the original bytes at the snapshot id, and Go LIST_SNAPS shows exactly one
clone, so nothing was cloned or written twice. Passed on legacy-v6 single
BlueStore and replicated-pg-v8 with three replicas. ROLLBACK under failover
remains unverified.
