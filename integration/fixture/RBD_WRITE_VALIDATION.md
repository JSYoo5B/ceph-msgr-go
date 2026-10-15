# RBD write Messenger I/O acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go,
without CGO, librados, librbd, go-ceph, Ceph CLI subprocesses or native
runtime dependencies, and it gains no RBD API: this acceptance uses only the
existing explicit OSD API (`OpenOSD`, `Request` with `OSDCall`,
`OSDSetAllocHint`, `OSDWrite`, `OSDStat`, `OSDRead`, `OSDListSnapshots`,
`OSDListWatchers`, `SnapContext` and `ReadSnapshot`, `WatchObject`, `Unwatch`,
`NotifyObject`). The striping map, the SnapContext taken from the header, the
`rbd` and `lock` class encodings and decodings, the managed-lock cookie and
the clone model are test-only consumer code in `integration/rbd_write_test.go`.
The disposable provider, the native librbd/librados driver and the owned pool
are development-only; see [RBD_BASE_FIXTURE.md](RBD_BASE_FIXTURE.md) (section
"Write image and closed write control").

This builds on the [RBD base acceptance](RBD_BASE_VALIDATION.md), which
covers Go as a reader. Here Go is the writer and the notifier: native librbd
creates a second image in the same owned pool, Go mutates it through the
Messenger OSD path the way librbd's own requests do, and native librbd and
librados are the independent oracles after every step. It does not establish
RBD functional equivalence of a consumer implementation.

## Test shape

`TestCephRBDWriteIntegration` runs only with `CEPH_MSGR_TEST_RBD=1` and
`CEPH_MSGR_TEST_OSD=1` (single OSD, `legacy-v6`); the profile's selector is
now `^TestCephRBD(Base|Write)Integration$`, so the base test runs first in the
same fixture. The Go client authenticates as the base identity
`client.rbd-base-<token>` (MON `profile rbd`, OSD `profile rbd pool=<owned
pool>`) with the selected key type and `EnableOSD` (default
`OSDProfileObjectHash`). Every request uses caller-side placement from the MON
`osd map` of its object and the primary's v2 address from `osd dump`. Each
reply must carry a nonzero transaction equal to its submission, the client's
GlobalID, the object name, the raw-hash PG echo, a map epoch not older than the
request's and retry 0; each successful mutation also carries ONDISK and a user
version. Every request is recorded in `rbd-write-results.json` with its
placement, inputs (opcode, flags, extent, data SHA-256 and up to 512 bytes,
class and method, allocation hint), SnapContext, modification time,
submission, identity, epochs, flags, versions and per-operation results.

The native image (plan in the fixture document): 256 KiB, order 16, layering
only; object 0 fully written and object 1 partly written before the snapshot,
object 1 rewritten after it, objects 2 and 3 absent. The native `setup` action
closes the image, so no librbd instance holds it while Go writes.

| Leaf | Checked behavior |
| --- | --- |
| `snapcontext_data_writes` | CALL `rbd.get_id`, `get_snapcontext`, `get_object_prefix` and `get_size` equal the native librados `exec` outputs byte for byte; the write SnapContext is the header's (seq = the only snapshot ID). LIST_WATCHERS of the header is empty. LIST_SNAPS of the four objects before the writes equals the test-only clone model replayed from the native writes (object 0: sequence 0, head only; object 1: one clone; objects 2 and 3: ENOENT). Go writes the four image extents of the plan as librbd's `ObjectWriteRequest` does for an image without parent or object map: per object piece one request `[SETALLOCHINT(65536, 65536, 0) with FAILOK, WRITE(offset, data)]` with the image SnapContext and no ORDERSNAP, five object requests in all (one image extent crosses objects 2 and 3). LIST_SNAPS after the writes equals the model: object 0 gained clone 4 with the overlap the Go write left, object 1 kept its clone with a smaller overlap, objects 2 and 3 were created under the SnapContext without a clone. Go STAT/READ at the head equal the expected head image (native setup head with the Go bytes); at the snapshot they equal the native setup snapshot (clones for objects 0 and 1, ENOENT for 2 and 3). The native `observe-written` action (read-only librbd open): librbd's head read equals the expected head image, librbd's snapshot read equals the pre-write snapshot read, librados head and snapshot object bytes equal the Go reads, the header SnapContext output is unchanged |
| `rbd_class_writes` | CALL `rbd.set_size(320 KiB)` and `rbd.metadata_set({ceph-msgr-write.<token8>: go-written-<token8>})` in write mode (librbd's `ResizeRequest` grow path and `MetadataSetRequest`), each alone, empty output, ONDISK. Go `get_size` at the head (320 KiB) and at the snapshot (256 KiB), `metadata_get` and `metadata_list` read them back. `observe-resized`: librbd `stat` size 320 KiB with five objects, the snapshot unchanged, `metadata_list` holds exactly the Go pair, the native `exec` outputs of `get_size` (head and snapshot) and `metadata_list` equal Go's byte for byte, the head read keeps the written bytes and reads zeros in the grown range, the snapshot read is unchanged |
| `header_lock` | Go `WatchObject` on the header (timeout 30 s); LIST_WATCHERS shows only it. `lock.get_info(rbd_lock)` equals the native exec of `observe-resized` (no lockers, type 0, empty tag). Go CALL `lock.lock` in librbd's `AcquireRequest` form: name `rbd_lock`, type EXCLUSIVE, cookie `auto <Go watch cookie>`, tag `internal`, empty description, zero duration and flags. `lock.get_info` then holds one locker: `client.<Go GlobalID>`, that cookie, zero expiration, empty description, an address of type LEGACY (1) whose endpoint and nonce equal the Go watcher's (cls_lock stores every locker address as LEGACY), type EXCLUSIVE, tag `internal`. The test restates librbd's managed-lock rules (`GetLockerRequest`, `BreakRequest`): the tag, the exclusive type, a decodable `auto <handle>` cookie with handle = the Go watch cookie and a non-blank IP. `observe-locked`: librbd `list_lockers` shows `{tag internal, exclusive, [(client.<gid>, auto <cookie>, <ip:port/nonce>)]}`, the native exec of `lock.get_info` equals Go's output byte for byte, and librbd `watchers_list` holds exactly the Go watcher with the same legacy address string and cookie = handle, so librbd's own liveness test would find the owner alive. Go CALL `lock.unlock(rbd_lock, cookie)`; `lock.get_info` lists no lockers; `observe-unlocked`: librbd lists no lockers and the native exec equals Go's output. Go `Unwatch` leaves the header without watchers. A deferred compensating unlock (never a repeated lock) runs only if the test fails between lock and unlock |
| `notify_librbd_refresh` | The native `watch` action opens the image read-write (librbd's header watch, phase `opened`: librbd `size()` and `stat()` 320 KiB); Go LIST_WATCHERS shows exactly librbd's watch (GlobalID, cookie and address equal `watchers_list`). Go CALL `rbd.set_size(384 KiB)`; Go `get_size` reads 384 KiB. Phase `stale` (after the first proceed signal): librbd `size()` and `stat()` still report 320 KiB while a native exec of `get_size` on the header returns Go's 384 KiB output byte for byte. Go `NotifyObject` on the header with librbd's `NotifyMessage(HeaderUpdatePayload)` and librbd's 5 s timeout: the completion has return code 0, the notify ID of the NOTIFY reply, no missed watchers and exactly one reply from librbd's watch (its GlobalID and cookie) with `ResponseMessage(0)`, well under the timeout; the stream then ends with `ErrOSDNotifyComplete`. Phase `refreshed` (after the second signal, same open image and watch): librbd `size()` and `stat()` report 384 KiB. The receipt is complete with stderr free of timed-out notifies; the final native exec of `get_size` equals Go's; LIST_WATCHERS after librbd's close is empty |

## Results

Both selected configurations passed on 2026-10-16 (KST): the base test
5/5 and the write test 4/4 subtests in each, no skips, no retries. The tested
source is HEAD `75f9c99017ef4cff921537b0b3d2ac6f25ba0c3e` plus only this
change set, exported to a separate directory with `git archive` (modified and
new files copied over). This record is the only file of the change set written
after the runs. The export holds 1193 files; the SHA-256 of its sorted
per-file SHA-256 list (`find . -type f -print0 | LC_ALL=C sort -z | xargs -0
shasum -a 256`, run in the export root) is
`026c390fcdbdda953e4250a1bf509af89e1920e6075c41319d489159acc3161e`, checked
again after the static checks, before the second run and after it. Source
SHA-256:

| File | SHA-256 |
| --- | --- |
| `integration/rbd_write_test.go` | `121502d559b07a3a4b3cae87c758de8a15f3cfd35a2d03147f86145fb7be56d8` |
| `integration/fixture/rbd_write.go` | `aa27373f1d9e74a356cf0a215191d80a923791cbcdbfc865932f11e25aac1b31` |
| `integration/fixture/rbd_write_test.go` | `0f6dd2e441e072a7f7dfbca5ae1282f767580cca25970790fcc0d15bcf9c70d9` |
| `integration/fixture/rbd_base.go` | `b4c07680c038d7cc28454dfc6853437434a1a84d7a8f007e11c5874d6c39cb76` |
| `integration/fixture/rbd_base_test.go` | `db5c41de8f8a45d6b0374d5e51eaee1d61beebb3d8019c3aab1c3241f6b93aa5` |
| `integration/fixture/profile.go` | `578b112da0c013314b7d1a99ab2380a55a201c544fbc598d4a17b78891d7ff22` |
| `integration/fixture/control.go` | `729c3bab585990375fd51f32a7de460ddb3bd85ef4e6a4461b75b22964d16e4f` |
| `integration/fixture/main.go` | `9bd297c203ddbcb46dae48dbe37e25776869282c4d8d9d5b907965332fcb3a8b` |
| `integration/fixture/diagnostics.go` | `bce344e6339add1f617356cf8db68a88746173b12a2546a2e3c831268761ae57` |
| `integration/fixture/RBD_BASE_FIXTURE.md` | `cea3f517c4a780a2b88c9de74e786a78b7142992206fd749180924f8ac9c94ea` |
| `.github/workflows/ci.yml` | `139a8edbfd75e902650380756f2a32f33aff3eb80634930878bf436662a225ca` |
| `tools/test_ci_osd_profiles.py` | `20eb83d1223d1111d665ef29480785bf64713deb21caa944af4df89d1eb979e7` |
| `README.md` | `b99de1d240303b3ed4cd596a3e28a399612e9bbd390ffa3c4cdddd809ebb18c1` |
| `SPEC.md` | `9d281f03e9f7d015e729056200c744637df655c15e1ba8e5932c8f60cf669043` |

In the export, before the live runs: `gofmt -l .` empty; `go vet ./...` and
`go test -count=1 ./...` (product module, `GOWORK=off CGO_ENABLED=0`); `go -C
integration/fixture vet -mod=readonly ./...` and `test -count=1
-mod=readonly ./...`; `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover
-s tools -p 'test_*.py'` (357 tests) all passed. In the working tree, `go test
-race` of the RBD tests of `./integration` and of the fixture's RBD and
profile tests also passed.

Image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
(arm64, Docker Desktop 29.8.2), daemon version
`ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`,
provider `github.com/JSYoo5B/ceph-testcontainers-go` v0.2.0. Topology: three
MON, two MGR, one OSD, the native oracle container. Command (from the export
root; the second configuration changes the variables shown in the table):

```sh
GOWORK=off CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=1 CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 CEPH_MSGR_TEST_RBD=1 \
CEPH_MSGR_TEST_RUN='^TestCephRBD(Base|Write)Integration$' CEPH_MSGR_TEST_DIAGNOSTICS=<dir> \
sh integration/run.sh
```

| Configuration | Controller wall time (UTC, 2026-10-15) | Base test | Write test | Native write setup | Result |
| --- | --- | --- | --- | --- | --- |
| Darwin arm64 host runtime, IPv4 relay, CRC, AES client and service keys, single BlueStore OSD | 21:41:19 to 21:42:32 (73 s) | 5.29 s | 2.10 s | 0.87 s | 5/5 and 4/4 passed |
| CGO=0 Linux arm64 container runtime, IPv6, secure, AES256K client and service keys (`KEY_TYPE`/`SERVICE_CIPHER` aes256k, `CONNECTION_MODE` secure, `IP_FAMILY` 6, `RUNTIME` container, `OSD_STORE` memstore), single MemStore OSD | 21:42:46 to 21:43:56 (70 s) | 5.33 s | 2.04 s | 0.85 s | 5/5 and 4/4 passed |

Write subtest durations (CRC / secure, seconds): data writes 0.24 / 0.22,
class writes 0.22 / 0.21, lock 0.42 / 0.41, notify 0.21 / 0.20. Native
action durations (ms, CRC / secure): `setup` 871 / 847, `observe-written` 85
/ 95, `observe-resized` 89 / 90, `observe-locked` 86 / 90,
`observe-unlocked` 93 / 92, `watch` 135 / 134; every native stderr was empty.
Each write run recorded 47 Go requests, transactions 1 to 49 without
duplicates (34 became the lock watch cookie, 47 the notify cookie).

### Observed values

| Value | CRC/AES IPv4 BlueStore | secure/AES256K IPv6 MemStore |
| --- | --- | --- |
| Owned pool | ID 3, flags 8192 (`selfmanaged_snaps`), no HASHPSPOOL | same |
| Go client (write test) | `client.rbd-base-0ee82568…`, GlobalID 4764 | `client.rbd-base-64664714…`, GlobalID 4755 |
| Native GlobalIDs (`setup`, observations, `watch`) | 4770; 14617, 14624, 14629, 14636; 14642 | 4761; 14611, 14609, 4779, 4785; 14635 |
| Write image | ID `12a2c1b7382d`, prefix `rbd_data.12a2c1b7382d`, order 16, 256 KiB, features 1 | ID `1299da24584e`, otherwise the same |
| Snapshot / SnapContext | ID 4, `ceph-msgr-wsnap-0ee82568`, 262144 bytes; seq 4, snaps [4] | ID 4, `ceph-msgr-wsnap-64664714`; seq 4, snaps [4] |
| Request map epochs | all write-test requests and replies 25 | all 24 |
| Reply flags | 0x4000025 for the five data writes, the five write-mode CALLs and WATCH/UNWATCH, 0x4000015 for reads, read CALLs, LIST_* and NOTIFY, 0x4000014 for the ENOENT vectors | same |

The base test passed unchanged in both runs (its own record covers it); in
the CRC run the OSDMap advanced during its watch leaf (notify IDs with epoch
23 and 24 in the high word), and its checks admit a newer reply epoch.

### Data writes and clones

Go object writes (both runs; data bytes are token-bound, so their SHA-256
differs per run):

| Image extent | Object, offset+length | User version (CRC / secure) |
| --- | --- | --- |
| 16384+16384 | 0, 16384+16384 | 4 / 4 |
| 65536+4096 | 1, 0+4096 | 6 / 4 |
| 139264+4096 | 2, 8192+4096 | 1 / 5 |
| 192512+8192 | 2, 61440+4096 and 3, 0+4096 | 2, 3 / 6, 7 |

User versions are PG-wide (`at_version`), not per object; nothing relies on
their values beyond being nonzero.

| Object | LIST_SNAPS before | LIST_SNAPS after | Snapshot read (Go and librados) |
| --- | --- | --- | --- |
| 0 | sequence 0; head 65536 | sequence 4; clone 4 [4] size 65536 overlap 0+16384, 32768+32768; head 65536 | 65536 bytes, the pre-write bytes (new clone) |
| 1 | sequence 4; clone 4 [4] size 16384 overlap 0+8192; head 16384 | sequence 4; clone 4 [4] size 16384 overlap 4096+4096; head 16384 | 16384 bytes (existing clone) |
| 2 | ENOENT (-2) | sequence 4; head 65536 | ENOENT |
| 3 | ENOENT (-2) | sequence 4; head 4096 | ENOENT |

Native views after the writes: librbd head read SHA-256
`2d3aa7b62dbf1b62890fd321fc2bae1a6fab70543672bf2d0631af8eb48f0320` (CRC) and
`01e0d0e06b6a13695f9d127ae9b0c602fa58b50b7fc46aadc25f70cdbb70c9bb` (secure),
each equal to the test's expected image; librbd snapshot read equal to the
setup's snapshot read (`c04bc8ca…` / `b2a3ac2f…`); librados objects 151552
bytes at the head (81920 before) and 81920 bytes at the snapshot (unchanged).

### Class writes, metadata and lock

Raw values of the CRC run (the secure run differs only in the image ID, key,
value, GlobalID, nonce and address); every Go output equaled the native exec
output of the corresponding observation:

| Method (input) | Output or effect |
| --- | --- |
| `rbd.get_snapcontext` | `0400000000000000` `01000000` `0400000000000000` |
| `rbd.set_size` (`0000050000000000`, write) | empty, result 0, user version 12; later (`0000060000000000`) user version 19 |
| `rbd.metadata_set` (`01000000` `18000000` key `13000000` value, write) | empty, result 0 |
| `rbd.get_size` head / snapshot after the grow | `10 0000050000000000` / `10 0000040000000000` |
| `rbd.metadata_get` (`18000000` key) | `13000000` + `go-written-0ee82568` |
| `rbd.metadata_list` (`00000000` `4000000000000000`) | `01000000` `18000000` key `13000000` value |
| `lock.get_info` (`0101 0c000000 08000000 rbd_lock`) before | `0101 09000000` `00000000` `00` `00000000` (no lockers, type NONE, empty tag) |
| `lock.lock` (`0101 31000000 08000000 rbd_lock 01 07000000 "auto 34" 08000000 "internal" 00000000 0000000000000000 00`, write) | empty, result 0 |
| `lock.get_info` locked | `0101 60000000 01000000`, locker ID `0101 14000000 08 9c12000000000000 07000000 "auto 34"`, locker info `0101 2f000000 0000000000000000` + address `01 0101 1c000000 01000000 0adcf13c 10000000 0200 0000 ac130009 0000000000000000` + `00000000`, then `01` and `08000000 "internal"` |
| `lock.unlock` (`0101 17000000 08000000 rbd_lock 07000000 "auto 34"`, write) | empty, result 0 |
| `lock.get_info` after unlock | `0101 11000000` `00000000` `01` `08000000 "internal"` (no lockers; type and tag stay) |

The locker address of the secure run is `01 0101 28000000 01000000 3a14d0df
1c000000 0a00 0000 00000000 fd6293fe2c9e0000000000000000 0009 00000000`
(family 10, port 0, `fd62:93fe:2c9e::9`). Native `list_lockers`:
`client.4764`, `auto 34`, `172.19.0.9:0/1022483466` (secure: `client.4755`,
`auto 34`, `[fd62:93fe:2c9e::9]:0/3754955834`), tag `internal`, exclusive;
`watchers_list` held the same address string with cookie 34. The Go watch is
listed by the OSD with type 2 (MSGR2) and the locker with type 1 (LEGACY),
same endpoint and nonce.

### Notify to librbd

| Value | CRC/AES IPv4 | secure/AES256K IPv6 |
| --- | --- | --- |
| librbd watch | GlobalID 14642, cookie 281473366174176, `172.19.0.9:0/4277841032` (type 3) | 14635, cookie 281472493759040, `[fd62:93fe:2c9e::9]:0/2296400928` |
| Phases (`size()`, `stat()` size) | opened 327680, 327680; stale 327680, 327680 with header exec `10 0000060000000000`; refreshed 393216, 393216 | same |
| Go NOTIFY | cookie 47, timeout 5 s, payload `07010400000003000000`, reply data `0800000019000000` (notify ID 107374182408 = 0x19_00000008) | cookie 47, reply data `0800000018000000` (103079215112 = 0x18_00000008) |
| Completion | 0.636 ms after sending; return code 0; no missed watchers; one reply from (14642, 281473366174176) with `01010400000000000000` | 0.358 ms; same with (14635, 281472493759040) |

Raw MWatchNotify of the CRC completion: front
`01022f000000000000001300000000000000080000001900000000000000000000009c12000000000000`
(message version 1, opcode 2 = complete, cookie 47, obsolete version field 19,
notify ID, empty front payload, return code 0, notifier GlobalID 4764 = the Go
client); data segment
`010000003239000000000000e02901a0ffff00000a0000000101040000000000000000000000`
(one reply: GlobalID 14642, cookie, 10 bytes `ResponseMessage(0)`; zero
missed).

### Cleanup verification

Both runs: the journal held `pool-create`, `pool-init`, `native-setup`,
`native-resize`, `native-write-setup`, the four
`native-write-observe-*` actions (`read_only: true`) and `native-write-watch`,
all `applied`; cleanup removed the pool (both images, the metadata and the
lock attribute with it), found neither its name nor its ID in a fresh native
OSDMap and deleted both identities (`completed: true`, no error). No lock was
left: the unlocked observations listed no lockers before cleanup. The
fixture's other containers were removed by the provider.

## Earlier attempts

No live run failed. The first failure of the change set was a unit
expectation: `TestRBDWriteTestOnlyEncodings` expected a 41-byte
`cls_lock_lock_op` payload where the encoding (and later the OSD) has 49 bytes
(0x31), an arithmetic slip in the expected hex, corrected before any live run.
Two development runs from the working tree (CRC/AES host IPv4 BlueStore, then
secure/AES256K container IPv6 MemStore) passed before the recorded ones.
Between them the test was changed twice: the lock evidence stored the
unlocked decoding under `Locked` because both shared one variable (an
evidence-only aliasing bug, the checks were unaffected), and a LIST_WATCHERS
before the data writes was added as direct evidence that no librbd instance
held the image.

## Findings and limits

No product defect and no missing wire capability was found for these
leaves: data writes with a caller SnapContext (with SETALLOCHINT + FAILOK in
the same vector), write-mode class CALLs on the header, the lock class,
WATCH/UNWATCH, LIST_WATCHERS, LIST_SNAPS and NOTIFY with its completion
carried what librbd needs and matched the native oracles byte for byte.
Observations:

- cls_lock stores the locker address as LEGACY (type 1) while the OSD lists
  the Go watch as MSGR2 (type 2); librbd compares legacy strings
  (`ip:port/nonce`), which are equal, so the managed-lock liveness rule holds
  for a Go owner.
- After `lock.unlock` the lock attribute stays with type EXCLUSIVE and tag
  `internal` and no lockers; librbd `list_lockers` reports that as empty.
- LIST_SNAPS of an absent object returns ENOENT (-2) with reply flags
  0x4000014, like STAT and READ.
- A notify completion's front carries the obsolete version field (the header's
  user version, 19 / 15) and the notifier GlobalID (the Go client); the
  watchers' replies are in the message's data segment.
- librbd keeps its cached size after a foreign `set_size` until a header
  update notify arrives and then refreshes on the next `size()`/`stat()` of
  the same open image; Go's notify and librbd's `ResponseMessage(0)` close
  that loop without a reopen.

Not verified here:

- images with exclusive-lock, object-map, fast-diff, deep-flatten or
  journaling (no lock-owner protocol, AcquiredLock/RequestLock/ReleasedLock
  notifies or object-map updates from Go), clones/parents/copy-up, larger
  orders, fancy striping, a separate data pool, namespaces, group or mirror
  snapshots;
- Go creating or removing snapshots (`selfmanaged_snap_create`,
  `rbd.snapshot_add`), shrinking (trim), discard, zero, WRITEFULL, write-same,
  compare-and-write, ORDERSNAP or EOLDSNAPC with an RBD image, snap trimming;
- `break_lock`, blocklisting, shared locks, lock contention with librbd,
  `metadata_remove` and other `rbd` write methods;
- concurrent Go and librbd writers, notify timeouts or several watchers in the
  Go-as-notifier direction, watch reconnect, renewal, cancellation, uncertain
  outcomes and daemon failures in this RBD context;
- the replicated-PG profiles, three replicas, EC pools, combinations other than
  the two recorded, the CI rows on GitHub runners (selector changed, not
  executed), Windows and long-running operation.
