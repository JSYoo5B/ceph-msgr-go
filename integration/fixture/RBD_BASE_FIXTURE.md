# Disposable RBD base fixture

`CEPH_MSGR_TEST_RBD=1` is a closed development-only extension of the opt-in
OSD fixture (`CEPH_MSGR_TEST_OSD=1`). It selects only
`^TestCephRBD(Base|Write)Integration$` (an explicit `CEPH_MSGR_TEST_RUN` must
be that selector; `TestCephRBDBaseIntegration` runs first, then
`TestCephRBDWriteIntegration`) and requires the single-OSD `legacy-v6` recipe: one OSD
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
(`get_size`, `list_lockers`, `list_watchers`), `pybind/rbd/rbd.pyx` and
`pybind/rados/rados.pyx` (LGPL-2.1 references; no code is copied). The
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

- `setup` requires the pool to hold exactly the base image, creates the write
  image, writes the native extents of the plan around the snapshot, flushes and
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

## Uncertain outcomes and cleanup

A failed, killed or truncated native process is an error: its output is
never trusted partially, the mutation stays `uncertain` and the control loop
stops (as for the other fixture controls). Cleanup runs once (in the runner
after the suites, and again from `Terminate`, which returns the cached
result) and writes `rbd-base-cleanup.json`: it removes the owned pool with
the provider's `RemovePool` (temporarily allowing pool deletion in the MON
configuration database and removing the pool's CRUSH rule; both images, their
snapshots, image metadata, lock attributes and any lingering watch go with the
pool), proves the pool's absence
by name and ID in a fresh native OSDMap, and deletes both identities. An
intended identity whose creation outcome is unknown is reported as an error.

## Published receipts

`rbd-base-owned.json`, `rbd-base-setup.json`, `rbd-base-oracle.json`,
`rbd-base-result.json`, `rbd-base-watch-state.json`, `rbd-base-cleanup.json`,
the six `rbd-write-<action>.json` receipts, `rbd-write-watch-state.json` and
the suites' own `rbd-base-results.json` and `rbd-write-results.json` are on
the diagnostics allowlist.
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
