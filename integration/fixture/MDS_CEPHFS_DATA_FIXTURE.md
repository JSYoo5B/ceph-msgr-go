# Disposable CephFS file data fixture

`CEPH_MSGR_TEST_MDS_CEPHFS_DATA=1` is a closed development-only extension of
`CEPH_MSGR_TEST_MDS_FRESH=1`. It selects only
`^TestCephMDSCephFSDataIntegration$` and excludes
`CEPH_MSGR_TEST_MDS_COMMANDS=1`, `CEPH_MSGR_TEST_MDS_SESSIONS=1`,
`CEPH_MSGR_TEST_MDS_CEPHFS=1`, `CEPH_MSGR_TEST_MDS_CEPHFS_RANKS=1` and
`CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION=1`: it writes file data, sets a
directory layout and takes snapshots in the owned filesystem, which the
other MDS receipts would observe. Like the other MDS profiles it requires one
OSD with `legacy-v6` (no-HASHPSPOOL pools, 16/1 PGs, replication and
min_size 1/1) and one active rank 0 without standby. Its Go client enables
MDS, CephFS and OSD sessions with the default v6 object-hash profile. No MON
feature exception is added and no daemon setting changes. The fixture
termination budget is two minutes.

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Relied-upon native behavior is
read from `client/Client.cc` (`_write`, `get_caps`, `check_caps`,
`send_cap`, `handle_cap_grant`, `handle_cap_trunc`, `handle_cap_flush_ack`,
`update_inode_file_size`, `update_inode_file_time`, the layout virtual
xattrs), `client/ClientSnapRealm.cc` (`build_snap_context`),
`osdc/Striper.cc` (`file_to_extents`, `extent_to_file`),
`osdc/Filer.cc` (`truncate`, `_do_truncate_range`), `mds/Server.cc`
(`handle_client_openc` client range, `handle_client_open`,
`handle_client_setvxattr` for `ceph.dir.layout` and its removal,
`handle_client_mksnap`), `mds/Locker.cc` (`handle_client_caps`,
`_do_cap_update`, `_update_cap_fields`, `calc_new_max_size`,
`check_inode_max_size`, `share_inode_max_size`, `issue_truncate`),
`mds/MDCache.cc` (`truncate_inode`, `_truncate_inode`, `journal_cow_dentry`),
`osd/PrimaryLogPG.cc` (`do_read`, the WRITE and TRIMTRUNC/TRUNCATE ops,
`make_writeable`), `pybind/cephfs/cephfs.pyx`, `pybind/rados/rados.pyx` and
`tools/rados/rados.cc` (`listsnaps`). These are LGPL-2.1 references; no code
is copied.

## Identities and published receipts

Preparation requires exactly one owned data pool, records the complete OSD
blocklist, requires no client session on the owned filesystem and that the
filesystem already allows snapshots. It then creates two identities,
journaling each name before the provider call:

| Identity | Caps | Use |
| --- | --- | --- |
| `client.mds-cephfs-data-<token>` | MON `allow r`, MDS `allow rw fsname=<owned fs>`, OSD `allow rw tag cephfs data=<owned fs>` | the Go client: CephFS session, `osd map`/`osd dump` reads and object reads and writes of the owned data pool; no layout or snapshot permission, no MGR |
| `client.mds-cephfs-data-native-<token>` | MON `allow r`, MDS `allow rwps fsname=<owned fs>`, OSD `allow rw tag cephfs data=<owned fs>` | the native LibCephFS oracle: directory layout (`p`), the three planned snapshots (`s`), native writes, reads, truncate and object views |

`CEPH_MSGR_MDS_CEPHFS_DATA_IDENTITY` and `CEPH_MSGR_MDS_CEPHFS_DATA_KEY_FILE`
name the Go identity and `<controlDir>/mds-cephfs-data.key`. The native
key is never written to a host file: the fixture keeps it in memory and
sends it as one bounded standard-input record to each native worker
(`NativeCommand.Input`, ceph-testcontainers-go v0.3.0; see
`native_key_input.go`). The worker prints only a byte-count receipt, and the
fixture checks `InputWritten`, `InputDelivered`, `InputEOF` and that receipt
separately from `Complete()`. For `rados listsnaps` the worker writes a
keyring at mode 0600 in a private directory of the client container and removes
it when it exits. Keys are mode 0600 and excluded from diagnostics.

`mds-cephfs-data-oracle.json` publishes the source pin, token, FSID, FSCID,
filesystem name, startup daemon GID, both identities and caps, the baseline
blocklist, the data pool ID and name, the directory layout string and the
plan:

- directory `ceph-msgr-data-<token>` under the filesystem root with the
  layout `stripe_unit=65536 stripe_count=2 object_size=131072 pool=<data
  pool>` (a period of 256 KiB over two objects; 64 KiB is the smallest
  stripe unit `file_layout_t::is_valid` admits, and a 16 KiB unit is refused
  by `setxattr` with EINVAL);
- files `go-file`, `native-file`, `trunc-file`, `shared-file`, `snap-file`,
  `capsnap-file`;
- snapshot `.snap/data-<token[:8]>` of the directory, plus
  `.snap/data-<token[:8]>-cap-a` and `-cap-b` for dirty cap-snap acceptance;
- payload seeds `ceph-msgr-cephfs-data-<purpose>:<token>` for `go-file`,
  `native-file`, `trunc-file`, `shared-file`, `snap-file-v1` and
  `snap-file-v2` and `capsnap-file-a`, `capsnap-file-b`, `capsnap-file-c`; a payload is the SHA-256 of the seed followed by the
  SHA-256 of each previous block, cut to its length;
- sizes: `go-file` 614523 bytes; `native-file` 1620000 bytes written only
  at 0+80000, 600000+120000 and 1600000+20000; `trunc-file` 1228800 bytes
  truncated to 280000; `shared-file` 200000 bytes; `snap-file` 600000 bytes,
  then 320000 bytes at 400000; cap-snap images A/B/C are
  200000/220000/240000 bytes;
- the object count of each file's object view (6, 14, 10, 2, 6 and 2).

`mds-cephfs-data-owned.json` is the journal: it is rewritten before each
identity creation and native mutation (intent) and after its outcome, and
records the session addresses of owned Go sessions the test named.

## Closed control

```go
testcluster.RequestControl(ctx, dir,
    "mds-cephfs-data", "mds-cephfs-data-completed",
    token, action, argument)
```

The request is `<requestID> <ownership token> <action> [argument]`. IDs are
positive canonical decimals and strictly increase. Targets are closed plan
names (`dir`, the six files, `snapshot`, `snapshot-file` for
`.snap/<name>/snap-file`, `capsnap-a/b` and `capsnap-a/b-file` for the
added snapshots and their `capsnap-file` views); a GlobalID is a canonical decimal. No caller path,
value or command reaches the fixture: payloads, sizes, extents, the layout
string and object counts come from the plan. One request is outstanding at a
time; the controller holds the shared MDS fresh scope lock and is bounded to
110 seconds.

| Action | Argument | Native effect |
| --- | --- | --- |
| `observe` | GlobalID | read only: `session ls` and `osd blocklist ls`; records the address of an owned Go session |
| `stat` | a target | read only: LibCephFS `statx` (mode, nlink, uid, gid, mtime seconds, ctime, ino, size, change attribute) or ENOENT, and for the directory and files the `ceph.dir.layout` or `ceph.file.layout` virtual xattr (ENODATA reported as absent) |
| `read` | a file, `snapshot-file` or `capsnap-a/b-file` | read only: open read-only, read to EOF in 64 KiB calls (bounded to 4 MiB), then `statx`; reports length, SHA-256 and the SHA-256 of each 16 KiB chunk |
| `objects` | a file | read only: `statx` for the inode, then the python rados binding's `stat` of `<ino hex>.<objno %08x>` for the planned object count; for `snap-file` and `capsnap-file` also `rados --format json listsnaps` of each existing object (clone IDs, snapshots, sizes, overlaps) |
| `capsnap-create-a`, `capsnap-create-b` | none | mutation, once each: create the two fixed snapshots; B requires an applied A |
| `capsnap-remove-a`, `capsnap-remove-b` | none | mutation, once each after the corresponding applied creation: remove the fixed snapshot |
| `list` | none | read only: `opendir`/`readdir` of the directory with `statx` per entry; only plan files are admitted, each once |
| `read-shared` | none | read only: the `read` of `shared-file` with monotonic timings (start, opened, read, closed); it waits for whatever capabilities the Go client holds |
| `layout-set` | none | mutation, once: `mkdir` of the directory (0755), `setxattr ceph.dir.layout <plan layout>`, readback of the layout and `statx` |
| `layout-clear` | none | mutation, once, after `layout-set`: `removexattr ceph.dir.layout`; the readback must be absent |
| `write-native` | none | mutation, once, after `layout-set`: create `native-file` (`O_CREAT\|O_EXCL\|O_WRONLY`, 0644), write the three planned extents of its payload, `fsync`, close, `statx` |
| `write-trunc` | none | mutation, once, after `layout-set`: create `trunc-file`, write its payload, `fsync`, close, `statx` |
| `truncate` | none | mutation, once, after `write-trunc`: `truncate` of `trunc-file` to 280000, `statx` |
| `snapshot-create`, `snapshot-remove` | none | mutation, once each: `mkdir`/`rmdir` of the planned snapshot; removal requires an applied creation |

Every action proves the owned rank-0 incarnation before and after it runs
(FSID, FSCID, daemon name, GID, rank 0, endpoint, nonce, incarnation, pools)
and then the daemon's `status` (`up:active`, an applied MDSMap epoch at least
the map's). The receipt (`mds-cephfs-data-result.json`) carries the map, the
daemon epoch, sessions and blocklist for `observe`, the native output and
process receipt, and the mutation record. Native outputs are decoded with
unknown fields refused and each action's shape required: layouts must equal
the plan layout, digests must have one chunk per 16 KiB, write sizes must
equal the plan, and object names must be `<ino hex>.<objno %08x>` of the
stat'ed inode in order.

## Cleanup

Cleanup runs once, inside the cached MDS fresh cleanup and before the owned
filesystem is removed, with its own 45-second bound. It writes an
`attempted` receipt to `mds-cephfs-data-cleanup.json`, then:

1. Lists sessions and reports any of the two identities. It never evicts.
2. Unless a Go session is still present, stats the owned directory natively:
   `namespace_restored` records whether it is gone, and `layout_restored`
   whether the directory is gone or its layout absent. A remaining directory
   (with its files, snapshot or layout) is discarded with the owned
   filesystem.
3. Compares the blocklist with the baseline and removes only new entries of
   an observed owned Go session address, verified by a final listing; any
   other new entry is reported in `kept_new_blocklist` and fails cleanup.
4. Deletes both identities. An identity whose creation outcome is unknown
   fails cleanup.

`completed` is true only when the namespace check ran, every new blocklist
entry was attributed, removed and verified absent and both identities were
removed. The MDS fresh cleanup receipt adds `cephfs_data_verified`;
filesystem, pool and identity removal proceed even if this verification
failed, and both errors are reported.

## Unit coverage

Fixture unit tests cover the exclusive profile and selector (including its
exclusion of the other MDS profiles and RBD), the credential environment,
identity caps, the token-bound plan, its layout string and path admission,
closed actions and replay protection, native output shapes, the one-shot and
ordered mutations (refused before native work), scope rejection before
native work and cleanup without a baseline. They do not establish live
behavior; see [MDS_CEPHFS_DATA_VALIDATION.md](MDS_CEPHFS_DATA_VALIDATION.md).

The new dirty-data cap-snap recipe and its separate verification status are
recorded in [MDS_CEPHFS_CAPSNAP_VALIDATION.md](MDS_CEPHFS_CAPSNAP_VALIDATION.md).
