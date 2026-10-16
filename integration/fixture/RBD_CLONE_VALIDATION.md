# RBD clone Messenger I/O acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go,
without CGO, librados, librbd, go-ceph, Ceph CLI subprocesses or native
runtime dependencies, and it gains no RBD or clone API: this acceptance uses
only the existing explicit OSD API (`OpenOSD`, `Request` with `OSDCall`,
`OSDSetAllocHint`, `OSDWrite`, `OSDStat`, `OSDRead`, `OSDSparseRead`,
`OSDListSnapshots`, `SnapContext` and `ReadSnapshot`). The striping map, the
parent fallback with overlap pruning, the copy-up construction, the `rbd`
class encodings and decodings and the parser of native Messenger lines are
test-only consumer code in `integration/rbd_clone_test.go`. The disposable
provider, the native librbd/librados driver and the owned pool are
development-only; see [RBD_BASE_FIXTURE.md](RBD_BASE_FIXTURE.md) (section
"Clone images and closed clone control").

This builds on the [RBD base](RBD_BASE_VALIDATION.md),
[write](RBD_WRITE_VALIDATION.md) and
[exclusive-lock](RBD_LOCK_VALIDATION.md) acceptances. Here native librbd
creates a parent image and a layering clone (clone format 2) in the owned
pool, and Go reads and writes the clone the way librbd's I/O path does:
the parent spec and overlap from the clone header, never-written clone
blocks from the parent object at the parent snapshot, copy-up writes, the
OSD clones after a clone snapshot, and reads after a native flatten. Native
librbd, librados and the native client's own Messenger requests are the
independent oracles. It does not establish RBD functional equivalence of a
consumer implementation.

## Test shape

`TestCephRBDCloneIntegration` runs only with `CEPH_MSGR_TEST_RBD=1` and
`CEPH_MSGR_TEST_OSD=1` (single OSD, `legacy-v6`); the profile's selector is
now `^TestCephRBD(Base|Write|ExclusiveLock|Clone)Integration$` and `go test`
runs the four in file order (base, clone, exclusive-lock, write). The lock
and write drivers' setup actions admit the parent and the clone in the pool.
The Go client authenticates as the base identity `client.rbd-base-<token>`
(MON `profile rbd`, OSD `profile rbd pool=<owned pool>`) with the selected key
type and `EnableOSD`. Every request uses caller-side placement from the MON
`osd map` and the primary's v2 address from `osd dump`; each reply must carry
a nonzero transaction equal to its submission, the client's GlobalID, the
object name, the raw-hash PG echo, a map epoch not older than the request's
and retry 0, and each successful mutation ONDISK and a user version. Every
request (inputs, read snapshot, SnapContext, modification time, identity,
epochs, flags, versions, per-operation results and outputs) is recorded in
`rbd-clone-results.json`, with the copy-up constructions, the parsed native
Messenger lines, LIST_SNAPS listings and image digests.

Images (plan in the fixture document): a 384 KiB parent, order 16, six
objects, written around its snapshot so that every parent object differs
between the head and the snapshot and object 4 exists only at the head; a
clone of the snapshot, clone format 2, overlap 384 KiB. The test keeps its
own model of the parent at the snapshot and the head and of the clone after
each step (the drivers' SHA-256 patterns), and checks it against native
librbd at setup.

### Native librbd forms restated by Go

From the pinned sources:

- Open and refresh (`image/RefreshRequest.cc`): one read vector
  `[get_size(head), get_features(head, read_only=0), get_flags(head),
  get_snapcontext, lock.get_info(rbd_lock)]`, then `[parent_get,
  parent_overlap_get(head)]` (`get_parent` only when the OSD class lacks
  them), and per snapshot `[snapshot_get, parent_overlap_get, get_flags,
  get_protection_status]`. `enable_sparse_copyup` is set when the cluster's
  `require_osd_release` is octopus or later.
- Clone read (`io/ObjectRequest.cc` `ObjectReadRequest`,
  `io/Utils.cc` `read_parent`): a whole 64 KiB object read is a SPARSE_READ
  (`rbd_sparse_read_threshold_bytes` 64 KiB), a smaller one a READ; on ENOENT
  the object extent is pruned by the parent overlap at the read snapshot and
  read from the parent object at the parent snapshot.
- Clone write (`AbstractObjectWriteRequest::write_object`): with a parent
  overlap and no object map, `[STAT (assert_exists), SETALLOCHINT(65536,
  65536) FAILOK, WRITE]` under the clone SnapContext; ENOENT starts a
  `CopyupRequest`.
- Copy-up (`io/CopyupRequest.cc`): the parent extent is read through the
  parent image; `prepare_copyup_data` keeps the parent's sparse extents minus
  the pending writes when the clone has no snapshot, the full extents when it
  has one. Without clone snapshots one vector `[rbd.sparse_copyup(extent map,
  data), SETALLOCHINT, WRITE]` goes under the clone SnapContext; with a clone
  snapshot `[rbd.sparse_copyup, SETALLOCHINT]` goes under an empty
  SnapContext (so the snapshot sees the parent data) and `[WRITE]` under the
  clone SnapContext, issued back to back. `cls_rbd` `sparse_copyup` and
  `copyup` return 0 without writing when the object exists.
- Flatten (`operation/FlattenRequest.cc`, `io::util::trigger_copyup`): an
  empty write request per object in the overlap; without deep-flatten and
  with a clone snapshot only the head is detached (`cls_rbd` parent detach
  keeps the spec and drops the head overlap) and the child link stays.

The product admits each of these Go vectors under the current client rule
(internal/osd/CLASS_CONTRACT.md "Ordering and flags"): read-mode CALLs after
read-mode CALLs, SETALLOCHINT and WRITE after a write-mode CALL, and a STAT
guard before writes. One librbd form is refused; see "Product finding".

| Leaf | Checked behavior |
| --- | --- |
| `clone_header_and_parent_reads` | Native `setup` created the parent, its snapshot (id 4), the post-snapshot writes and the clone; librbd reports the clone's parent spec (pool, empty namespace, parent id and name, snapshot id and name, not in trash) and `overlap()` 384 KiB, operation features clone-child (2) on the clone and clone-parent (1) on the parent, and one child of the parent snapshot. Go: `rbd.get_id` of `rbd_id.<clone>`, `get_object_prefix`, the mutable-metadata vector of five read CALLs, the parent vector `[parent_get, parent_overlap_get(head)]`, the legacy `get_parent(head)` and `op_features_get`, each per-op output equal byte for byte to the native librados `exec` of the same input. Decoded: order 16, size 384 KiB, features and incompatible features 257 (`layering` and `operations`, which clone format 2 enables), flags 0, SnapContext seq 0 without snapshots, no lockers, `ParentImageSpec{pool, "", parent id, 4}`, overlap `Some(384 KiB)`, legacy `get_parent` the same pool, image, snap and overlap. On the parent: `get_id` names the spec's image; the read vector `[get_object_prefix, get_size(4), get_snapcontext, snapshot_get(4), children_list(4), op_features_get]` equals native per op (size 384 KiB at the snapshot, SnapContext 4/[4], `SnapshotInfo` child count 1, `children_list` = the clone, op features 1). The parent objects at the snapshot equal native librados. Go then reads the whole clone as librbd does: every clone object SPARSE_READ fails with ENOENT, and each object comes from the parent object at snapshot 4 (pruned by the overlap; object 4 absent there, so zeros). The image equals the planned clone and native librbd's clone read; every parent object at the head differs from the snapshot, so a head read would have been detected. A 4 KiB READ at the end of object 5 is ENOENT on the clone and returns the clone bytes from the parent at the snapshot |
| `go_copyup_write` | Go writes 8 KiB at 20 KiB of clone object 1: the guarded vector under the clone SnapContext (seq 0) returns ENOENT with per-op results `[-2, 0, 0]`; the parent object 1 is read at snapshot 4; the copy-up vector `[rbd.sparse_copyup (write mode), SETALLOCHINT FAILOK, WRITE]` under the clone SnapContext returns per-op `[0, 0, 0]`, ONDISK, empty CALL output. The object holds the parent bytes with the Go write (48 KiB), its sparse extents are the copy-up extents and the write, and LIST_SNAPS is seq 0 with the head only. A second `rbd.sparse_copyup` (64 KiB of other bytes) and a legacy `rbd.copyup` on the existing object each return 0, leave the bytes and the user version unchanged and advance only the replay version (the OSD logs the no-op write). Native `observe-copyup`: librbd reads the Go bytes and the parent bytes around them, librados reads the same object, the parent head, snapshot and objects are unchanged, and the clone header exec outputs are unchanged |
| `native_copyup_write` | Native librbd writes 4 KiB at 4 KiB of clone object 3 (between parent holes) in its traced client. Its Messenger lines are exactly three exchanges: `[stat,set-alloc-hint object_size 65536 write_size 65536,write 4096~4096]` with `snapc 0=[]` answered -2; `[sparse-read 0~65536]` of parent object 3 at snapshot 4 answered 0; `[call rbd.sparse_copyup in=<n>b,set-alloc-hint …,write 4096~4096]` with `snapc 0=[]` answered 0, where n equals the class and method names plus the copy-up input built from the store's parent extent map minus the write. So librbd v20.2.4 sends `sparse_copyup` (Tentacle's `require_osd_release` enables it) and the same three vectors as Go. Go reads the object: the parent bytes with the native write; on BlueStore its sparse extents keep the parent holes (`4~4 KiB`, `16~4 KiB`, `40~8 KiB`), which a thick `copyup` would have filled; LIST_SNAPS seq 0, head only; the Go clone read equals native |
| `clone_snapshot_copyups` | Native `snapshot` takes clone snapshot 5. Go's mutable-metadata vector reads SnapContext 5/[5] (equal to native exec); the per-snapshot vector `[snapshot_get(5), parent_overlap_get(5), get_flags(5), get_protection_status(5)]` and `get_parent(5)` equal native exec: child count 0, overlap `Some(384 KiB)`, flags 0, unprotected, the same parent. LIST_SNAPS unchanged. Go writes 8 KiB at 44 KiB of the copied-up object 1 with the guarded vector under 5/[5]: per-op `[0, 0, 0]`, and the OSD clones the object (clone 5, size 48 KiB, overlap 0 to 44 KiB; head 52 KiB). Go writes 8 KiB at 28 KiB of the never-written object 2 in librbd's form for a clone with a snapshot: guarded ENOENT, parent read, `[rbd.sparse_copyup (the full parent extents), SETALLOCHINT]` under an empty SnapContext, then `[WRITE]` under 5/[5]; LIST_SNAPS shows clone 5 (32 KiB, overlap 0 to 28 KiB) and the 36 KiB head, as the clone model predicts. Go reads the head (the final image) and snapshot 5 (the image before these writes; objects absent at 5 fall back to the parent with the snapshot's overlap); native `observe-snapshot` librbd and librados agree at both |
| `flatten` | Native librbd flattens the clone in its traced client: 13 exchanges, all as `FlattenRequest` and `CopyupRequest` predict. Every object gets `[stat,set-alloc-hint …,write 0~0]` under 5/[5] (objects 0, 4, 5 answer -2, objects 1 to 3 answer 0); the parent objects 0, 4 and 5 are read with `sparse-read 0~65536` at snapshot 4 (object 4 answers -2 and gets no copy-up); objects 0 and 5 get `[call rbd.sparse_copyup in=<n>b,set-alloc-hint …]` under `snapc 0=[]` (n from the full parent extents) and `[write 0~0]` under 5/[5]. After it, librbd reports no parent at the head and the parent link at snapshot 5; Go `parent_get` still returns the spec, `parent_overlap_get(head)` returns `None` (`00`), legacy `get_parent(head)` overlap 0, `parent_overlap_get(5)` 384 KiB, each equal to native exec. LIST_SNAPS matches the model: the empty write under 5/[5] cloned objects 0, 3 and 5. Go reads the head without the parent (object 4 stays absent and reads zeros) and snapshot 5 with the parent fallback; both equal the native reads and librados objects; the parent is unchanged |

## Product finding

The client refuses SPARSE_READ with a read snapshot before transmission:
`internal/osd/snapshot.go` `requestSnapshot` rejects a nonzero
`ReadSnapshot` when the first operation is SPARSE_READ (also MAPEXT,
CHECKSUM, LIST_WATCHERS), and `internal/osd/SPARSE_READ_CONTRACT.md` keeps
"snapshot sparse reads" unsupported until characterized. librbd v20.2.4 sends
exactly that request for a whole-object read of a parent at the parent
snapshot (`ObjectReadRequest::read_object`, used by `read_parent` and by
`CopyupRequest::read_from_parent`) and for whole-object reads of a clone at a
clone snapshot; the native traces above show `[sparse-read 0~65536]` on
`rbd_data.<parent>.…:4`. The test records the refusal once
(`snapshot_sparse_read_refused`: "ceph osd: invalid object request", no
transaction) and falls back to a plain READ of the same range at the
snapshot, librbd's own form for reads below the threshold. The bytes are the
same; the store's hole map is not. Consequences, all recorded:

- Go's copy-up of object 1 carries one dense parent extent (zeros inside
  the parent holes) minus the write, where librbd would carry the sparse
  extents. The visible bytes, the object size and the cls behavior are the
  same; on BlueStore the copied-up object has no holes, as after a thick
  `copyup`. The full copy-up of object 2 is unaffected (its parent extent is
  contiguous).
- The copy-up input librbd builds is therefore predicted from the store's
  extent map (BlueStore: the written ranges; MemStore: the whole object up to
  its end, as `MemStore::fiemap` reports) and checked against the
  `in=<n>b` of the native requests, not built from a Go sparse read.

Admitting SPARSE_READ at a snapshot (after characterizing the replicated OSD
path at a clone, as was done for READ) would let a consumer send librbd's
exact copy-up and snapshot reads.

## Results

Both selected configurations passed on 2026-10-16 (UTC): base 5/5, clone
5/5, exclusive-lock 7/7 and write 4/4 subtests in each, no skips, no
retries. The tested source is commit
`db2df56` (this change set except this section), exported with
`git archive db2df56` to a separate directory: 1209 files, SHA-256 of the
sorted per-file SHA-256 list
(`find . -type f -print0 | LC_ALL=C sort -z | xargs -0 shasum -a 256`, run in
the export root) `1f9b654934612e28d88d2abe1c5c20453d7cb70c72600a31762dd383a58e928e`,
the same before the static checks and after the second run. In the export:
`gofmt -l .` empty; `go vet ./...` and `go test -count=1 ./...` (product
module, `GOWORK=off CGO_ENABLED=0`); `go -C integration/fixture vet
-mod=readonly ./...` and `test -count=1 -mod=readonly ./...`;
`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools -p
'test_*.py'` (360 tests) all passed.

Image `quay.io/ceph/ceph:v20.2.4`, daemon version `ceph version 20.2.4
(7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`, provider
`github.com/JSYoo5B/ceph-testcontainers-go` v0.2.0, Docker Desktop on Darwin
arm64 (4 CPUs, 4 GB). Topology: three MON, two MGR, one OSD, the native
oracle container. Command from the export root (the second configuration
changes the variables shown in the table):

```sh
GOWORK=off CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=1 CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 CEPH_MSGR_TEST_RBD=1 \
CEPH_MSGR_TEST_RUN='^TestCephRBD(Base|Write|ExclusiveLock|Clone)Integration$' CEPH_MSGR_TEST_DIAGNOSTICS=<dir> \
sh integration/run.sh
```

| Configuration | Controller wall time (UTC) | Base | Clone | Exclusive-lock | Write |
| --- | --- | --- | --- | --- | --- |
| Darwin arm64 host runtime, IPv4, CRC, AES client and service keys, single BlueStore OSD | 00:37:03 to 00:38:21 (78 s) | 5.38 s | 2.67 s | 0.81 s | 1.80 s |
| CGO=0 Linux arm64 container runtime, IPv6, secure, AES256K client and service keys, single MemStore OSD | 00:40:36 to 00:41:53 (77 s) | 5.33 s | 2.42 s | 0.78 s | 1.74 s |

Clone subtests (CRC run, seconds): header and parent reads 0.03, Go copy-up
0.36, native copy-up 0.23, clone snapshot 0.77, flatten 0.25. Native action
durations (ms, CRC / secure): setup 879 / 655, observe-copyup 116 / 97, write
124 / 122, snapshot 283 / 477, observe-snapshot 114 / 129, flatten 122 / 124
(librbd's flatten itself about 15 to 21 ms). Every native stderr was empty.
Each run sent 179 Go requests (178 transmitted, transactions 1 to 178, and
the one refused SPARSE_READ); request epochs 24 or 25 before and 25 or 26
after the clone snapshot (its snapshot ID allocation publishes a new
OSDMap).

### Observed values

| Value | CRC/AES IPv4 BlueStore | secure/AES256K IPv6 MemStore |
| --- | --- | --- |
| Pool / parent / clone | pool 3, parent `12aeb0d16ceb`, clone `12ae38c68b33` | pool 3, `127e4efb296`, `127eb27ba466` |
| Snapshots | parent 4, clone 5 | same |
| Release gates (`osd dump`) | `require_osd_release` tentacle, `require_min_compat_client` luminous, `min_compat_client` argonaut | same |
| `parent_get` | `0101 24000000 0300000000000000 00000000 0c000000 "12aeb0d16ceb" 0400000000000000` | same layout |
| `parent_overlap_get(head)` | `01 0000060000000000` (Some 384 KiB); after the flatten `00` | same |
| `get_parent(head)` after the flatten | pool 3, the parent ID, snap 4, overlap 0 | same |
| Reply flags | 0x4000015 reads, 0x4000014 ENOENT reads, 0x4000025 class and data writes, 0x4000035 guarded vectors (STAT with writes) | same |

### Copy-ups

| Step | CRC/AES BlueStore | secure/AES256K MemStore |
| --- | --- | --- |
| Go object 1, guarded vector | aggregate -2, per-op `[-2, 0, 0]`, reply user version 10 (the PG's last) | `[-2, 0, 0]`, 4 |
| Go object 1, parent READ at 4 | one dense extent 0~48 KiB (see the finding) | same |
| Go object 1, copy-up vector | extents `0~20 KiB`, `28~20 KiB`, input 41000 bytes (prefix `02000000 0000…00 00500000… 00700000… 00500000… 00a00000`), per-op `[0, 0, 0]`, user version 12; object 48 KiB, one sparse extent 0~48 KiB | same extents and input, user version 4 |
| Second `sparse_copyup` (65560 bytes) / `copyup` (65536 bytes) | result 0, user version 12 unchanged, replay version 24'13 and 24'14, bytes unchanged | 0, 4 unchanged, 25'5 and 25'6 |
| Native object 3 (trace) | `sparse_copyup in=12344b`: extents `16~4 KiB`, `40~8 KiB` (12 KiB data), equal to the prediction; object extents after: `4~4 KiB`, `16~4 KiB`, `40~8 KiB` | `in=45112b`: extents `0~4 KiB`, `8~40 KiB` (44 KiB); object one extent 0~48 KiB |
| Go object 2 under snapshot 5 | guarded `[-2, 0, 0]`; `[sparse_copyup (32792 bytes: extent 0~32 KiB), SETALLOCHINT]` with no SnapContext per-op `[0, 0]`; `[WRITE]` with 5/[5] `[0]` | same |
| Native flatten copy-ups | object 0 `in=65576b` (0~64 KiB), object 5 `in=4136b` (60~4 KiB) | both `in=65576b` (0~64 KiB) |

### LIST_SNAPS

Identical in both runs:

| Object | After the Go copy-up | After the post-snapshot writes | After the flatten |
| --- | --- | --- | --- |
| 0 | ENOENT | ENOENT | seq 5, clone 5 (64 KiB, overlap 0~64 KiB), head 64 KiB |
| 1 | seq 0, head 48 KiB | seq 5, clone 5 (48 KiB, overlap 0~44 KiB), head 52 KiB | unchanged |
| 2 | ENOENT | seq 5, clone 5 (32 KiB, overlap 0~28 KiB), head 36 KiB | unchanged |
| 3 | ENOENT (native write later: seq 0, head 48 KiB) | seq 0, head 48 KiB | seq 5, clone 5 (48 KiB, overlap 0~48 KiB), head 48 KiB |
| 4 | ENOENT | ENOENT | ENOENT |
| 5 | ENOENT | ENOENT | seq 5, clone 5 (64 KiB, overlap 0~64 KiB), head 64 KiB |

The flatten's empty writes under 5/[5] cloned the objects whose snapset
sequence was still 0 (3, and 0 and 5 right after their copy-ups under the
empty SnapContext); objects 1 and 2 already had sequence 5.

### Cleanup verification

Both runs: the journal held the base, lock and write actions and
`native-clone-setup`, `-observe-copyup` (`read_only`), `-write`,
`-snapshot`, `-observe-snapshot` (`read_only`) and `-flatten`, all
`applied`; cleanup removed the pool with the five images, found neither its
name nor its ID in a fresh native OSDMap and deleted both identities
(`completed: true`, no error).

## Not verified

- librbd's exact copy-up vector from Go: the sparse extent map of a parent
  object at a snapshot cannot be read through the product (see the finding),
  so Go's own copy-up input differs from librbd's (dense); only the native
  input length is checked against the predicted map.
- Concurrent copy-ups of the same object by two clients, a copy-up racing a
  flatten, copy-on-read (`rbd_clone_copy_on_read`), object-map or
  exclusive-lock clones, deep-flatten, clone v1 (`rbd_children`), clones in
  another pool or namespace, overlap pruning inside an object (the overlap
  here equals the image size), parent images in the trash and migration
  (`assert_snapc_seq` guard) are outside this acceptance.
- The thick `rbd.copyup` path is exercised only as the no-op on an existing
  object.

## Earlier attempts

On the working tree, before the exported runs: the first setup output was
refused by the fixture's shape check (clone format 2 reports features 257,
and pybind's `overlap()` returns 0 rather than raising for an image without a
parent); the second run stopped at the first parent read at the snapshot,
which the client refused before transmission (the finding). After the READ
fallback both configurations passed from the working tree. During one of
those runs another agent's cluster shared the Docker VM; the exported runs
ran alone.
