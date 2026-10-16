# Disposable RBD base fixture

`CEPH_MSGR_TEST_RBD=1` is a closed development-only extension of the opt-in
OSD fixture (`CEPH_MSGR_TEST_OSD=1`). It selects only
`^TestCephRBD(Base|Write|ExclusiveLock|Clone|Journal)Integration$` (an
explicit `CEPH_MSGR_TEST_RUN` must be that selector; `go test` runs them in
file order: `TestCephRBDBaseIntegration`, then `TestCephRBDCloneIntegration`,
then `TestCephRBDJournalIntegration`, then
`TestCephRBDExclusiveLockIntegration`, then `TestCephRBDWriteIntegration`) and
requires the single-OSD `legacy-v6` recipe: one OSD
(`CEPH_MSGR_TEST_OSD_REPLICAS=1`), the legacy CRUSH map and pools without
HASHPSPOOL, so the Go client keeps the default `OSDProfileObjectHash` (v6
requests with a caller-supplied raw object hash) and needs no extra MON
admission bit. It excludes every MDS profile (`CEPH_MSGR_TEST_MDS_FRESH` must
be 0) and the staged sparse-read oracle. MemStore and BlueStore, both
connection modes, both key types, IPv4 and IPv6, and host and container
runtimes are admitted. The replicated-PG profiles (`replicated-pg-v8`,
`replicated-pg-reply-v8`) are refused: their explicit-PG requests and
HASHPSPOOL pools are not verified with RBD here. The fixture termination
budget is two minutes.

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Relied-upon behavior is read from
`cls/rbd/cls_rbd.cc` (`get_id`, `get_size`, `get_object_prefix`,
`get_features`, `get_snapcontext`, `get_snapshot_name`, `snapshot_get`),
`cls/rbd/cls_rbd_types.cc` (`SnapshotInfo`, `SnapshotNamespace`),
`librbd/Operations.cc` (`C_NotifyUpdate`, `execute_resize`),
`librbd/ImageWatcher.cc` (`notify_header_update`), `librbd/watcher/Notifier.cc`
(`NOTIFY_TIMEOUT`), `librbd/WatchNotifyTypes.cc` (`NotifyMessage`,
`ResponseMessage`), `osd/PrimaryLogPG.cc` (`make_writeable`, `LIST_SNAPS`),
`os/memstore/MemStore.cc` (`fiemap`), and for the write image
`cls/rbd/cls_rbd.cc` (`set_size`, `metadata_set`, `metadata_get`,
`metadata_list`), `cls/lock/cls_lock.cc` (`lock_obj`, `remove_lock`,
`get_info`), `librbd/image/OpenRequest.cc` (no watch for read-only opens),
`librbd/ImageState.cc` (`refresh_if_required`), `librbd/internal.cc`
(`get_size`, `list_lockers`, `list_watchers`), for the lock image
`librbd/ImageCtx.cc` and `librbd/exclusive_lock/AutomaticPolicy.cc` (the
default lock policy), `librbd/ImageWatcher.cc`, `librbd/ExclusiveLock.cc`,
`librbd/internal.cc` (`is_exclusive_lock_owner`, `lock_get_owners`),
`librbd/api/DiffIterate.cc`, `librbd/object_map/DiffRequest.cc`,
`librbd/operation/ObjectMapIterate.cc` and `tools/rbd/action/ObjectMap.cc`
(`object-map check`), for the clone images `librbd/image/CloneRequest.cc`
and `AttachChildRequest.cc` (clone format 2), `librbd/image/RefreshRequest.cc`
(`parent_get`, `parent_overlap_get`, `enable_sparse_copyup`),
`librbd/io/ObjectRequest.cc`, `librbd/io/CopyupRequest.cc`,
`librbd/operation/FlattenRequest.cc`, `cls/rbd/cls_rbd.cc` (`copyup`,
`sparse_copyup`, `get_parent`, `parent_get`, `parent_overlap_get`,
`children_list`, parent detach), `msg/async/AsyncConnection.cc` and
`msg/DispatchQueue.cc` (the `debug_ms 1` lines), the `debug_rbd 20`
`convert_copyup_extent_map` line of `CopyupRequest.cc`, `messages/MOSDOp.h` and
`MOSDOpReply.h` (their print), `pybind/rbd/rbd.pyx` and
`pybind/rados/rados.pyx`, and for the journaling image
`cls/journal/cls_journal.cc`, `cls_journal_client.cc` and
`cls_journal_types.cc`, `journal/Entry.cc`, `journal/JournalMetadata.cc`,
`journal/JournalRecorder.cc`, `journal/ObjectRecorder.cc`,
`journal/Journaler.cc`, `librbd/Journal.cc`,
`librbd/journal/CreateRequest.cc`, `librbd/journal/Types.cc` and
`tools/rbd/action/Journal.cc` (LGPL-2.1 references; no code is copied). The
provider is `github.com/JSYoo5B/ceph-testcontainers-go` v0.2.0
(`CreatePool`, `rbd.InitPool`, `WaitForClean`, `CreateClient`,
`RunNativeCommand`, `RemovePool`, `DeleteClient`).

## Ownership and preparation

Preparation runs inside the provider startup, after the native oracle worker
reported ready (so the base OSD fixture, its pool policy and the worker's
configuration copy exist) and before the control loop starts. A random
128-bit token names everything the profile owns. Each mutation is recorded
in `rbd-base-owned.json` before it is issued and marked `applied` or
`uncertain` afterwards; no mutation is issued twice.

| Step | Owned state |
| --- | --- |
| `pool-create` | `ceph-msgr-rbd-<token>`: replicated, 8 PGs, size and min_size 1, application `rbd`, HASHPSPOOL cleared, its own `tc-<pool>-replicated` CRUSH rule |
| `pool-init` | `rbd pool init` through the provider (`rbd.InitPool`), then `WaitForClean`. The native OSDMap is read again: every pool must still lack HASHPSPOOL (numeric flag bit 0 and the `hashpspool` name) |
| Go identity | `client.rbd-base-<token>`: MON `profile rbd`, OSD `profile rbd pool=<pool>`. Its key is written to `rbd-base.key`; the suite receives `CEPH_MSGR_RBD_BASE_IDENTITY` and `CEPH_MSGR_RBD_BASE_KEY_FILE` |
| native identity | `client.rbd-base-native-<token>`: the same RBD profile; keyring `rbd-base-native.keyring`, used only by the native driver |
| `native-setup` | the image `ceph-msgr-image-<token>` and its snapshot `ceph-msgr-snap-<token8>` (below) |
| `native-write-<action>` | on the write test's request, the image `ceph-msgr-write-<token>` (section "Write image and closed write control") |
| `native-lock-<action>` | on the exclusive-lock test's request, the image `ceph-msgr-lock-<token>` (section "Lock image and closed lock control") |
| `native-clone-<action>` | on the clone test's request, the parent `ceph-msgr-parent-<token>`, its snapshot and the clone `ceph-msgr-clone-<token>` (section "Clone images and closed clone control") |
| `native-journal-<action>` | on the journaling test's request, the image `ceph-msgr-journal-<token>`, its journal and the native journal client `ceph-msgr-oracle-<token8>` (section "Journaling image and closed journal control") |

Both identities hold RBD's standard capability profile bound to the owned
pool. The Go client therefore authenticates and performs every RBD object
operation with the capabilities a native RBD client would be given; the
MON part (`osd r`) also serves its `osd map` and `osd dump` placement reads.

### The native image

The setup action of the native driver (Python `rbd`/`rados` bindings in the
oracle container, librbd cache disabled) creates a format 2 image of 512 KiB
with order 16 (64 KiB objects, eight data objects), features `layering` only
(no exclusive lock, object map or journal, so header notifies come only from
the explicit resizes), default striping. It writes SHA-256 counter-stream
patterns derived from the token, flushes, creates the user snapshot, writes
again and flushes:

| Phase | Image extent | Objects touched |
| --- | --- | --- |
| before the snapshot | 0 to 96 KiB | object 0 entirely, object 1 0 to 32 KiB |
| before the snapshot | 128 to 136 KiB and 168 to 176 KiB | object 2 0 to 8 KiB and 40 to 48 KiB (a 32 KiB hole inside the object) |
| before the snapshot | 272 to 276 KiB | object 4 16 to 20 KiB (a leading 16 KiB hole) |
| after the snapshot | 32 to 72 KiB | object 0 32 to 64 KiB, object 1 0 to 8 KiB |
| after the snapshot | 192 to 196 KiB | object 3 0 to 4 KiB (created after the snapshot) |

Objects 5 to 7 are never written. The driver then records, independently of
the Go client:

- librbd `stat` (size, object size and count, order, block name prefix,
  parent), `id`, `features`, `op_features`, `flags`, stripe unit and count,
  data pool and `list_snaps`;
- a native librados `exec` (`Ioctx.execute`) of eight rbd methods with their
  raw inputs and outputs: `get_id` on `rbd_id.<image>`, and on
  `rbd_header.<id>` `get_size` (head and snapshot), `get_object_prefix`,
  `get_features` (CEPH_NOSNAP, read_only false), `get_snapcontext`,
  `get_snapshot_name` and `snapshot_get` of the snapshot;
- for every data object named from librbd's prefix: librados `stat` and
  `read` at the head and, through `set_read`, at the snapshot (existence,
  size, SHA-256 and the bytes);
- librbd reads of the whole image at the head and at the snapshot;
- the pool's object listing.

The four binary views are published as `rbd-base-image-head.bin`,
`rbd-base-image-snap.bin`, `rbd-base-objects-head.bin` and
`rbd-base-objects-snap.bin` (object bytes concatenated at the recorded
offsets). The fixture refuses the output unless it has exactly the admitted
shape (unknown fields, another token, pool, image, prefix, feature set,
snapshot or object name, a short write or a binary view whose length or
SHA-256 differs from the receipt all fail preparation). The full receipt
(including the bounded native process result) is `rbd-base-setup.json`; the
suite reads `rbd-base-oracle.json` (plan, owned pool and its flags, identities,
caps and the decoded native observation).

## Closed control

The suite sends `rbd-base` requests `<id> <token> <action>` through the
ordinary control channel (`rbd-base-completed.<id>` acknowledgement). The
only action is `watch`, and it is one-shot (`native-resize` in the journal):

1. The driver opens the image read-write, which registers librbd's header
   watch, and publishes `rbd-base-watch-state.json` with phase `opened`, its
   global ID, the image size and librbd `watchers_list`.
2. It waits up to 30 s for `rbd-base-watch-proceed-1`, whose content must be
   `<token> <request id> 1`, then resizes the image to 640 KiB and publishes
   `resized-acknowledged` with the resize duration.
3. It waits for `rbd-base-watch-proceed-2` the same way, resizes to 768 KiB
   and publishes `resized-unacknowledged`.
4. It closes the image (librbd unwatches), publishes `closed` and observes
   stat, snapshots and the eight class outputs again.

A resize of a layering-only image runs locally and ends with librbd's
`C_NotifyUpdate`, one header-update notify to every header watcher with
librbd's 5 s notify timeout; a timed-out notify is logged at error level
("update notification timed-out") and the resize still succeeds. The driver
runs with `err_to_stderr` only, so the receipt's stderr holds exactly those
error lines. The receipt `rbd-base-result.json` keeps the request ID, the
journaled mutation, the native output and the bounded process result.

## Write image and closed write control

`TestCephRBDWriteIntegration` (see [RBD_WRITE_VALIDATION.md](RBD_WRITE_VALIDATION.md))
uses the same token, pool and both identities, and a second image that exists
only once the test asks for it. Its fixed plan is part of the journal from
preparation on (`write_plan` in `rbd-base-owned.json`):

| Plan value | Content |
| --- | --- |
| image | `ceph-msgr-write-<token>`, format 2, order 16 (64 KiB objects), 256 KiB (four objects), features `layering` only |
| snapshot | `ceph-msgr-wsnap-<token8>` |
| native writes before the snapshot | 0 to 64 KiB (object 0 entirely), 64 to 80 KiB (object 1 0 to 16 KiB) |
| native write after the snapshot | 72 to 80 KiB (object 1 8 to 16 KiB, so object 1 already has a clone) |
| Go writes | 16 to 32 KiB (object 0, not yet cloned), 64 to 68 KiB (object 1, already cloned), 136 to 140 KiB (object 2, absent), 188 to 196 KiB (across objects 2 and 3) |
| Go header writes | `set_size` to 320 KiB and `metadata_set` of `ceph-msgr-write.<token8>` = `go-written-<token8>`; later `set_size` to 384 KiB while librbd holds the image open |

The suite sends `rbd-write` requests `<id> <token> <action>` through the
control channel (`rbd-write-completed.<id>` acknowledgement). The actions form
a closed sequence, each admitted once and only after its predecessor:
`setup`, `observe-written`, `observe-resized`, `observe-locked`,
`observe-unlocked`, `watch`. Each is journaled in `rbd-base-owned.json` as
`native-write-<action>` before the native process starts; the observations
carry `read_only: true`. The driver (Python `rbd`/`rados` bindings, librbd
cache disabled, `err_to_stderr` only) re-checks every name against the token.

- `setup` requires the pool to hold the base image and at most the lock
  image, the clone parent and the clone (the clone and exclusive-lock tests
  run before it), creates the write image, writes the native extents of the plan around the snapshot, flushes and
  closes it.
- Every action then observes the image through a read-only librbd open
  (which registers no header watch): `stat`, features, `list_snaps`,
  `metadata_list`, `list_lockers` (an image without lockers is reported as
  empty), `watchers_list`, librbd reads of the head and of the snapshot,
  librados `stat`/`read` of every data object at the head and at the
  snapshot, and a native librados `exec` of eight methods with raw inputs and
  outputs: `rbd.get_id` on the id object, and on the header `rbd.get_size`
  (head and snapshot), `get_object_prefix`, `get_features`, `get_snapcontext`,
  `metadata_list` (start after "", at most 64) and `lock.get_info`
  (`rbd_lock`). The four binary views are published as
  `rbd-write-<action>-image-head.bin`, `-image-snap.bin`, `-objects-head.bin`
  and `-objects-snap.bin`.
- `watch` opens the image read-write (librbd's header watch), publishes
  `rbd-write-watch-state.json` with phase `opened` (librbd `size()`, `stat()`
  size and `watchers_list`), waits up to 30 s for
  `rbd-write-watch-proceed-1` (content `<token> <request id> 1`), runs a
  native `exec` of `rbd.get_size` on the header and publishes `stale`, waits
  for `rbd-write-watch-proceed-2` and publishes `refreshed`, then closes the
  image and observes it. librbd's `size()` and `stat()` refresh the image
  only after a header-update notify (`ImageState::refresh_if_required`).

The fixture checks the structure of each output (identity, names, the closed
set of sizes 256, 320 and 384 KiB, object and blob consistency, the phase
sequence); which size, metadata or lock state each step must show is judged by
the test, so a Go-side failure is reported by the test rather than stopping
the control loop. Each receipt is `rbd-write-<action>.json` (request ID,
plan, journaled action, decoded native output, bounded process result).

## Lock image and closed lock control

`TestCephRBDExclusiveLockIntegration` (see
[RBD_LOCK_VALIDATION.md](RBD_LOCK_VALIDATION.md)) uses the same token, pool and
both identities, and a third image that exists only once the test asks for it.
Its fixed plan is part of the journal from preparation on (`lock_plan` in
`rbd-base-owned.json`):

| Plan value | Content |
| --- | --- |
| image | `ceph-msgr-lock-<token>`, format 2, order 16 (64 KiB objects), 320 KiB (five objects), features 29 (`layering`, `exclusive-lock`, `object-map`, `fast-diff`), no snapshot |
| setup write (native) | 0 to 64 KiB (object 0 entirely) |
| owner write (native, takes the lock) | 64 to 80 KiB (object 1 0 to 16 KiB) |
| Go write (while Go holds the lock) | 136 to 144 KiB (object 2 8 to 16 KiB, an absent object) |
| reacquire write (native, asks Go for the lock) | 200 to 208 KiB (object 3 8 to 16 KiB, an absent object) |

Object 4 is never written, so the object map keeps one nonexistent entry. The
suite sends `rbd-lock` requests `<id> <token> <action>` through the control
channel (`rbd-lock-completed.<id>` acknowledgement). The actions form a closed
sequence, each admitted once and only after its predecessor: `setup`, `lock`,
`observe-released`. Each is journaled in `rbd-base-owned.json` as
`native-lock-<action>` before the native process starts; `observe-released`
carries `read_only: true`. The driver (Python `rbd`/`rados` bindings, librbd
cache disabled, `err_to_stderr` only, librbd's default exclusive-lock policy)
re-checks every name against the token and the feature set 29.

- `setup` requires the pool to hold the base image and at most the write
  image, the clone parent and the clone (the write test's setup admits this
  image the same way), creates the lock image, opens it read-write (librbd takes the lock for the write and
  releases it at close), writes the setup extent, flushes and closes it. It
  then sends librbd's id-object creation vector (`CreateRequest::
  create_id_object`: `create(exclusive)` then `rbd.set_id`, one python rados
  write op with RETURNVEC and the current time) twice to
  `rbd_id.ceph-msgr-setid-native-<token>` with the image id
  `f00d<first 8 token characters>`, and records both aggregate codes, the
  object's raw bytes and user version after each, and a native
  `rbd.get_id` exec. The object names no image (the pool directory does not
  list it) and goes with the owned pool.
- `lock` opens the image read-write and writes the owner extent, which takes
  the exclusive lock (automatic policy). While librbd holds the object-map
  lock (empty cookie), the same rados client sends librbd's object-map update
  vector `[lock.assert_locked(rbd_lock, exclusive, "ceph-msgr-wrong-cookie",
  ""), rbd.object_map_update(4, 5, OBJECT_EXISTS, none)]` (python rados
  write op, RETURNVEC; only the aggregate is exposed) and records its code,
  both inputs and the raw object map before and after in the `owner` phase
  (`negative_compound`). It then publishes
  `rbd-lock-state.json` after each phase: `owner`; `released`, `go-owner`
  (with librbd reads of the Go extents) and `reacquired` (after the
  reacquire write, with its duration and the Go extents read again), each
  after the proceed signal `rbd-lock-proceed-<n>` (`n` 1 to 3, content
  `<token> <request id> <n>`, 30 s each); then it waits for proceed signal 4,
  closes the image (librbd releases the lock and its watch) and publishes
  `closed`. Every phase holds librbd `is_exclusive_lock_owner()`,
  `lock_get_owners()` and `watchers_list()` of the open image (not in
  `closed`), `list_lockers()`, `flags()`, `size()` and a whole-object
  `diff_iterate` from the beginning of time of a fresh read-only open (the
  read-write instance caches lockers and flags), a native librados `exec` of
  `lock.get_info(rbd_lock)` on the header and on `rbd_object_map.<id>`, and
  the raw bytes of `rbd_object_map.<id>`. After `closed` the driver runs
  `rbd --conf <conf> --id <native identity> --keyring <keyring>
  --rbd_cache=false --log_to_stderr=false --err_to_stderr=true object-map
  check --no-progress <pool>/<image>` (60 s bound, output bounded to 4 KiB)
  and observes the image.
- `observe-released` also reads the Go test's id object
  `rbd_id.ceph-msgr-setid-go-<token>`: raw bytes and user version, and a
  native `rbd.get_id` exec (reads only).
- Every action ends with a read-only observation: `stat`, features, flags,
  `list_snaps` (none), `list_lockers`, `watchers_list`, the whole-object
  diff, a librbd read of the head, librados `stat`/`read` of every data object,
  the raw object-map object and a native librados `exec` of eight methods with
  raw inputs and outputs: `rbd.get_id` on the id object, `rbd.get_size`,
  `get_object_prefix`, `get_features`, `get_snapcontext` and
  `lock.get_info(rbd_lock)` on the header, then `lock.get_info(rbd_lock)` and
  `rbd.object_map_load` on the object map. The two binary views are published
  as `rbd-lock-<action>-image-head.bin` and `-objects-head.bin`.

The fixture checks the structure of each output (identity, names, the plan,
the closed phase sequence and its fields, the check's argument list, object
and blob consistency); which owner, lock, object-map or check result each step
must show is judged by the test. Each receipt is `rbd-lock-<action>.json`
(request ID, plan, journaled action, decoded native output, bounded process
result). The lock action is bounded to 220 s (four Go steps of at most 30 s,
the check and the observation).

## Clone images and closed clone control

`TestCephRBDCloneIntegration` (see
[RBD_CLONE_VALIDATION.md](RBD_CLONE_VALIDATION.md)) uses the same token, pool
and both identities, and two more images that exist only once the test asks
for them. Their fixed plan is part of the journal from preparation on
(`clone_plan` in `rbd-base-owned.json`):

| Plan value | Content |
| --- | --- |
| parent | `ceph-msgr-parent-<token>`, format 2, order 16 (64 KiB objects), 384 KiB (six objects), features `layering` only |
| parent snapshot | `ceph-msgr-psnap-<token8>` (not protected) |
| clone | `ceph-msgr-clone-<token>` of the parent snapshot, clone format 2, features `layering` requested, order 16; clone format 2 sets the `clone-parent` and `clone-child` operation features, so both images report features 257 (`layering` and `operations`) |
| clone snapshot | `ceph-msgr-csnap-<token8>` |
| parent writes before the snapshot | 0 to 64 KiB, 72 to 88 KiB, 104 to 112 KiB, 128 to 160 KiB, 208 to 212 KiB, 232 to 240 KiB, 380 to 384 KiB (object 4 absent at the snapshot) |
| parent writes after the snapshot | 0 to 16 KiB, 80 to 96 KiB, 160 to 192 KiB, 192 to 196 KiB, 256 to 320 KiB, 376 to 384 KiB (every object differs at the head) |
| Go copy-up write | 84 to 92 KiB (clone object 1, 20 to 28 KiB: across a parent extent and a hole) |
| native clone write | 196 to 200 KiB (clone object 3, 4 to 8 KiB, between parent holes) |
| Go writes after the clone snapshot | 108 to 116 KiB (object 1 again), 156 to 164 KiB (object 2, never written in the clone) |

The suite sends `rbd-clone` requests `<id> <token> <action>` through the
control channel (`rbd-clone-completed.<id>` acknowledgement). The actions form
a closed sequence, each admitted once and only after its predecessor:
`setup`, `observe-copyup`, `write`, `snapshot`, `observe-snapshot`,
`flatten`. Each is journaled in `rbd-base-owned.json` as
`native-clone-<action>` before the native process starts; the two
observations carry `read_only: true`. The driver (Python `rbd`/`rados`
bindings, librbd cache disabled, `err_to_stderr` only) re-checks every name
against the token, the feature set 1 and clone format 2.

- `setup` requires the pool to hold exactly the base image (the clone test
  runs right after the base test), creates the parent, writes the
  pre-snapshot extents, flushes, creates the parent snapshot, writes the
  post-snapshot extents, flushes and closes it, then clones the snapshot
  with `RBD.clone(..., features=1, order=16, clone_format=2)`.
- `write` writes the native extent to the clone and flushes, and `flatten`
  flattens the clone, each through an image opened read-write in a second
  librados client whose Messenger traffic (`debug_ms 1`) and librbd
  (`debug_rbd 20`) are logged to a private file in the oracle container. The
  driver keeps only the lines of requests sent (`-->`) and replies received
  (`<==`) for data objects (`rbd_data.`), cut to the `osd_op(...)` or
  `osd_op_reply(...)` print, at most 96 lines of at most 600 characters, and
  librbd's copy-up extent maps (`CopyupRequest::convert_copyup_extent_map`,
  kept from the method name on: the data object, the parent image extents
  and the object extents), at most 16 lines of at most 600 characters, as
  `copyup_trace`; then it deletes the file. These lines are the native op
  vectors, SnapContexts, read snapshots and results, and the sparse parent
  map each native copy-up was built from.
- `snapshot` creates the clone snapshot through a read-write open.
- Every action ends with a read-only observation: librbd views (stat,
  features, operation features, flags, snapshots, `get_parent_image_spec()`
  and `overlap()`, null without a parent) of the clone head, of the clone at
  its snapshot once it exists, of the parent and of the parent at its
  snapshot; `list_children2()` of the parent snapshot; librbd reads of all
  four; librados `stat`/`read` of every clone data object at the head (and at
  the clone snapshot) and of every parent data object at the parent
  snapshot; and native librados `exec` lists with raw inputs and outputs: on
  the clone `rbd.get_id`, then on its header `get_size`, `get_features`,
  `get_flags`, `get_snapcontext`, `lock.get_info(rbd_lock)`, `parent_get`,
  `parent_overlap_get`, `get_parent` (all at the head), `get_object_prefix`
  and `op_features_get`; once the clone snapshot exists `snapshot_get`,
  `parent_overlap_get`, `get_flags`, `get_protection_status`, `get_parent`
  and `get_size` at it; on the parent `rbd.get_id`, then `get_object_prefix`,
  `get_size`, `get_snapcontext`, `snapshot_get` and `children_list` at the
  parent snapshot and `op_features_get`. The binary views are published as
  `rbd-clone-<action>-clone-head.bin`, `-clone-objects-head.bin`,
  `-parent-head.bin`, `-parent-snap.bin`, `-parent-objects-snap.bin` and,
  with the clone snapshot, `-clone-snap.bin` and `-clone-objects-snap.bin`.

The fixture checks the structure of each output (identity, names, the plan,
image layout and features, the parent link of the clone and of its snapshot,
the exec lists, object and blob consistency, the writes, the trace bounds and
line shape); which parent, overlap, object, trace or LIST_SNAPS state each
step must show is judged by the test. Each receipt is
`rbd-clone-<action>.json` (request ID, plan, journaled action, decoded
native output, bounded process result). Each action is bounded to 90 s.

## Journaling image and closed journal control

`TestCephRBDJournalIntegration` (see
[RBD_JOURNAL_VALIDATION.md](RBD_JOURNAL_VALIDATION.md)) uses the same token,
pool and both identities, and one more image that exists only once the test
asks for it. Its fixed plan is part of the journal from preparation on
(`journal_plan` in `rbd-base-owned.json`):

| Plan value | Content |
| --- | --- |
| image | `ceph-msgr-journal-<token>`, format 2, order 16 (64 KiB objects), 256 KiB (four objects), features 69 (`layering`, `exclusive-lock`, `journaling`), default journal options (order 24, splay width 4, no journal pool) |
| native journal client | `ceph-msgr-oracle-<token8>`, empty data, registered right after creation, so its commit position stays empty and `rbd journal inspect` replays every entry |
| Go journal client | `ceph-msgr-mirror-<token8>` (registered and unregistered by the test, never by the fixture) |
| setup events | writes 0 to 8 KiB, 72 to 80 KiB, 136 to 140 KiB, 196 to 204 KiB, then a discard of 192 to 256 KiB (object 3 whole) |
| write events | writes 16 to 24 KiB, 80 to 84 KiB, then a discard of 128 to 192 KiB (object 2 whole) |
| Go writes | 40 to 44 KiB and 100 to 104 KiB, appended by the test as journal entries only |

Every write is at most 8 KiB (one AioWrite event below librbd's 16 KiB
journal payload limit) and every discard covers a whole object (librbd's
64 KiB discard granularity keeps it).

The suite sends `rbd-journal` requests `<id> <token> <action>` through the
control channel (`rbd-journal-completed.<id>` acknowledgement). The actions
form a closed sequence, each admitted once and only after its predecessor:
`setup`, `observe-registered`, `write`, `observe-committed`, `inspect`,
`replay`. Each is journaled in `rbd-base-owned.json` as
`native-journal-<action>` before the native process starts; the two
observations carry `read_only: true`. `inspect` is journaled as a mutation
because `rbd journal inspect` registers and unregisters its own `INSPECT`
journal client. The driver (Python `rbd`/`rados` bindings, librbd cache
disabled, `err_to_stderr` only) re-checks every name against the token and
the feature set 69.

- `setup` requires the pool to hold the base image and at most the clone
  test's parent and clone, creates the image, registers the native journal
  client with a librados `exec` of `journal.client_register`, then opens the
  image read-write, applies the setup events and closes it (the first write
  takes the exclusive lock and opens the journal; closing flushes the commit
  position).
- `write` applies the write events the same way.
- `replay` opens the image read-write and calls `lock_acquire(exclusive)`,
  which opens the journal and replays every entry after the image client's
  commit position, records `is_exclusive_lock_owner()` and closes the image.
- Every action ends with a read-only observation: librbd views (stat,
  features, flags, lockers, watchers) and a librbd read of the whole image;
  a native librados `exec` list with raw inputs and outputs on
  `journal.<image id>`: `get_order`, `get_splay_width`, `get_pool_id`,
  `get_minimum_set`, `get_active_set`, `client_list("", 256)`, `get_client`
  of the image client `""`, of the Go client and of the native client,
  `get_next_tag_tid` and `tag_list(0, 256, "", Some(0))`; librados
  `stat`/`read` of the journal data objects
  `journal_data.<pool id>.<image id>.<n>` for n 0 to 7 (object sets 0 and
  1); `rbd journal info --format json` and `rbd journal status --format json`
  on `<pool>/<image id>`; and for `setup`, `inspect` and `replay`
  `rbd journal inspect --verbose`, whose entries (tag, commit tid, the
  EventEntry dump) and summary the driver parses. The binary views are
  published as `rbd-journal-<action>-image-head.bin` and
  `-journal-objects.bin`.

The fixture checks the structure of each output (identity, names, the plan,
image layout and features, the exec list, object and blob consistency, the
command lines and parsed outputs, the events); which clients, positions,
tags, entries and image bytes each step must show is judged by the test.
Each receipt is `rbd-journal-<action>.json` (request ID, plan, journaled
action, decoded native output, bounded process result). Each action is
bounded to 240 s. The lock and write drivers' setup actions admit the
journaling image in the pool.

## Uncertain outcomes and cleanup

A failed, killed or truncated native process is an error: its output is
never trusted partially, the mutation stays `uncertain` and the control loop
stops (as for the other fixture controls). Cleanup runs once (in the runner
after the suites, and again from `Terminate`, which returns the cached
result) and writes `rbd-base-cleanup.json`: it removes the owned pool with
the provider's `RemovePool` (temporarily allowing pool deletion in the MON
configuration database and removing the pool's CRUSH rule; the six images, their
snapshots, the clone's parent link, image metadata, lock attributes, the lock
image's object map, the journaling image's journal header and data objects
and any lingering watch go with the pool), proves the pool's absence
by name and ID in a fresh native OSDMap, and deletes both identities. An
intended identity whose creation outcome is unknown is reported as an error.

## Published receipts

`rbd-base-owned.json`, `rbd-base-setup.json`, `rbd-base-oracle.json`,
`rbd-base-result.json`, `rbd-base-watch-state.json`, `rbd-base-cleanup.json`,
the six `rbd-write-<action>.json` receipts, `rbd-write-watch-state.json`, the
three `rbd-lock-<action>.json` receipts, `rbd-lock-state.json`, the six
`rbd-clone-<action>.json` receipts, the six `rbd-journal-<action>.json`
receipts and the suites' own `rbd-base-results.json`,
`rbd-write-results.json`, `rbd-lock-results.json`, `rbd-clone-results.json`
and `rbd-journal-results.json` are on the diagnostics allowlist.
The binary views and the keys are not archived.

## Unit coverage

`integration/fixture/rbd_base_test.go` checks the closed profile (selector,
credentials only with the opt-in, refusal of the replicated-PG profiles,
replicas 3, MDS profiles, other selectors and the sparse-read oracle), the
token-bound plan and capabilities, the closed request parser, the
closed-shape decoding of both driver outputs, the binary-view receipt check,
the one-shot journal, the cached cleanup and the unknown identity outcome,
and that the embedded driver parses as Python where an interpreter exists.
`integration/fixture/rbd_write_test.go` checks the token-bound write plan and
its clone categories, the closed and ordered write request sequence, the
closed-shape decoding of every write action (including the watch phases), the
per-action binary views, the read-only journal marking and that the write
driver parses as Python. `TestRBDBaseTestOnlyLayoutAndDecoders` in `integration/rbd_base_test.go`
checks the test-only image-to-object mapping, the overlap subtraction and the
`SnapshotInfo` decoder (including every truncation and a trailing byte);
`TestRBDWriteTestOnlyEncodings` in `integration/rbd_write_test.go` checks the
lock-class inputs, the managed-lock cookie, the metadata map, the
`cls_lock_get_info_reply` decoder with IPv4 and IPv6 addresses (every
truncation and a trailing byte) and the clone model.
`integration/fixture/rbd_lock_test.go` checks the token-bound lock plan (one
object per writer, object 4 absent), the closed and ordered lock request
sequence, the closed-shape decoding of every lock action (phase order and
fields, the check), the per-action binary views, the journal of the lock plan
and of an uncertain lock action, and that the lock driver parses as Python.
`TestRBDLockTestOnlyEncodings` in `integration/rbd_lock_test.go` checks the
watch-notify `ClientId`, `NotifyMessage` and `ResponseMessage` encodings and
decoders (every truncation, a trailing byte, compat 2, version 1 payloads,
unknown ops), the object-map lock and `assert_locked` inputs, the
`object_map_update` input, the `ceph_crc32c` restatement against Ceph's own
test vectors and the `BitVector<2>` decoder with and without CRCs (every
truncation, damaged bytes, a trailing byte).
`integration/fixture/rbd_clone_test.go` checks the token-bound clone plan and
its object layout, that the lock and write drivers admit the parent and the
clone, the closed and ordered clone request sequence and its journal targets,
the closed-shape decoding of every clone action (snapshot views only after
the snapshot, a parent-less clone head only after the flatten, the trace only
for the write and the flatten and within its bounds), the per-action binary
views, the journal of the clone plan and of an uncertain flatten, and that
the clone driver parses as Python. `TestRBDCloneTestOnlyEncodings` in
`integration/rbd_clone_test.go` checks the `ParentImageSpec`,
`std::optional<uint64_t>`, legacy `get_parent`, `children_list` and snap
context decoders (truncations and trailing bytes), the `sparse_copyup` input
encoding, the partial and full copy-up extents, extent merging, sparse
expansion and the Messenger line parser and request/reply pairing.
`integration/fixture/rbd_journal_test.go` checks the token-bound journal plan
(bounded writes, whole-object discards), that the lock and write drivers
admit the journaling image, the closed and ordered journal request sequence
and its read-only marking, the closed-shape decoding of every journal action
(the exec list, the eight journal objects, the command lines, the events only
for `setup` and `write`, the replay fields only for `replay`) and that the
journal driver parses as Python. `TestRBDJournalTestOnlyEncodings` in
`integration/rbd_journal_test.go` checks the seed-0 CRC-32C against Ceph's
own test vectors, the Entry framing, padding walk and CRC refusal, the
AioWrite and AioDiscard EventEntry encodings, `ObjectSetPosition`,
`cls::journal::Client`, `TagData`, `MirrorPeerClientMeta` and
`ImageClientMeta`, the commit-position rule, the hexdump parser and the
list inputs.
