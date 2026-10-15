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

Response loss and SIGKILL during ROLLBACK or a cloning write, pool-snapshot
mode (where the server substitutes the pool context), a ROLLBACK refused with
-EBUSY by a watcher, removed snaps still queued for trimming, read-only
credentials, EC pools, broader profiles and remote CI for this subset need
separate evidence.
