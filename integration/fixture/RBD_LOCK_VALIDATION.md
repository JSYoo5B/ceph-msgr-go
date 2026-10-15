# RBD exclusive-lock Messenger I/O acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go,
without CGO, librados, librbd, go-ceph, Ceph CLI subprocesses or native
runtime dependencies, and it gains no RBD or lock API: this acceptance uses
only the existing explicit OSD API (`OpenOSD`, `Request` with `OSDCall`,
`OSDSetAllocHint`, `OSDWrite`, `OSDStat`, `OSDRead`, `OSDListWatchers`,
`WatchObject`, `Next`, `Acknowledge`, `Ping`, `Unwatch`, `NotifyObject`). The
watch-notify payloads (`NotifyMessage`, `ClientId`, `ResponseMessage`), the
lock-owner rules, the `lock` and `rbd` object-map class encodings, the
`BitVector<2>` decoding with `ceph_crc32c` and the image layout are test-only
consumer code in `integration/rbd_lock_test.go`. The disposable provider, the
native librbd/librados driver, `rbd object-map check` and the owned pool are
development-only; see [RBD_BASE_FIXTURE.md](RBD_BASE_FIXTURE.md) (section "Lock
image and closed lock control").

This builds on the [RBD base](RBD_BASE_VALIDATION.md) and
[RBD write](RBD_WRITE_VALIDATION.md) acceptances. Here Go takes part in
librbd's exclusive-lock protocol on an image with `exclusive-lock`,
`object-map` and `fast-diff` while native librbd holds the same image open
read-write: Go reads the owner, asks for the lock, takes it when native
librbd hands it over, updates the object map and writes as librbd's writer
would, and gives the lock back when native librbd asks for it. Native librbd,
librados and `rbd object-map check` are the independent oracles. It does not
establish RBD functional equivalence of a consumer implementation.

## Test shape

`TestCephRBDExclusiveLockIntegration` runs only with `CEPH_MSGR_TEST_RBD=1`
and `CEPH_MSGR_TEST_OSD=1` (single OSD, `legacy-v6`); the profile's selector is
now `^TestCephRBD(Base|Write|ExclusiveLock)Integration$` and `go test` runs the
three in file order (base, exclusive-lock, write). The Go client authenticates
as the base identity `client.rbd-base-<token>` (MON `profile rbd`, OSD
`profile rbd pool=<owned pool>`) with the selected key type and `EnableOSD`.
Every request uses caller-side placement from the MON `osd map` and the
primary's v2 address from `osd dump`; each reply must carry a nonzero
transaction equal to its submission, the client's GlobalID, the object name,
the raw-hash PG echo, a map epoch not older than the request's and retry 0,
and each successful mutation ONDISK and a user version. Every request,
notification, acknowledgement, lock-info output and object-map byte string is
recorded in `rbd-lock-results.json`.

The native image (plan in the fixture document): 320 KiB, order 16, five
objects, features 29, no snapshot. The native `setup` action writes object 0
and closes the image (librbd takes and releases the lock), so the object map
starts as `[1 0 0 0 0]`. Before the lock action Go reads the image identity as
librbd's open does (`rbd.get_id`, `get_size`, `get_object_prefix`,
`get_features` = 29/29 because all four features are
`RBD_FEATURES_RW_INCOMPATIBLE`, `get_snapcontext` = seq 0 and no snapshots),
`lock.get_info` of the header and of `rbd_object_map.<id>`, the raw object-map
object and `rbd.object_map_load`, each equal byte for byte to the native
librados `exec` or read of the setup observation.

The leaf table describes the current test. The compound class-call leaves
(`create_set_id_compound`, the two object-map update vectors in
`go_owner_object_map` and the native read of the Go id object in
`cleanup_state`) were added on 2026-10-16 with the client rule that admits a
CALL after read-mode CALLs and after a CREATE without FAILOK, and refuses a
CALL after a write-mode CALL; see "Compound class calls" below and
[OSD_CALL_COMPOUND_VALIDATION.md](OSD_CALL_COMPOUND_VALIDATION.md).

| Leaf | Checked behavior |
| --- | --- |
| `create_set_id_compound` | librbd's `CreateRequest::create_id_object` vector `[CREATE(EXCL), rbd.set_id(id)]` (write mode, current mtime) on the fresh `rbd_id.ceph-msgr-setid-go-<token>` with id `beef<8 token characters>`: aggregate 0, per-op 0 and 0, ONDISK, no output; Go reads `<le32 12><id>` raw and through `rbd.get_id`. The same vector again: aggregate and op 0 -17 (`EEXIST` from the exclusive create), op 1 0 with its union echoed and no output, the object unchanged. The native setup sent the same vector twice through python rados on `rbd_id.ceph-msgr-setid-native-<token>`: 0, then -17 with bytes and user version unchanged, and `rbd.get_id` returned the encoded id |
| `lock_owner_observation` | The lock action's first native write took the lock (librbd `is_exclusive_lock_owner()`). Go `lock.get_info(rbd_lock)` on the header equals the native exec of the same phase and decodes, as librbd's `GetLockerRequest` does, to one exclusive locker with tag `internal`, `client.<native gid>`, cookie `auto <handle>`, zero expiration, empty description and a non-blank LEGACY address. LIST_WATCHERS of the header holds exactly that client with cookie = handle and the same `ip:port/nonce` (librbd's `BreakRequest` liveness rule). Native `lock_get_owners()` reports that address in mode 0 (`RBD_LOCK_MODE_EXCLUSIVE`) and a fresh read-only `list_lockers()` the same locker. The object map is locked by the native owner with empty cookie and tag (`object_map::LockRequest`), and its raw bytes read by Go equal librados's: `[1 1 0 0 0]` (the owner write marked object 1); the native fast-diff lists objects 0 and 1 |
| `request_lock_release` | Go registers its header watch (timeout 30 s); LIST_WATCHERS holds the native owner and Go. A responder goroutine answers every notification on the Go watch as librbd's `ImageWatcher::handle_payload` would (AcquiredLock and ReleasedLock empty; RequestLock `ResponseMessage(0)` while Go owns the lock, empty otherwise and for its own request; HeaderUpdate and unknown ops are failures) and pings the watch every 5 s it holds. Go `NotifyObject` with `NotifyMessage(RequestLockPayload{ClientId{Go gid, Go watch cookie}, force=false})` and librbd's 5 s timeout: completion return code 0, no missed watcher, two acknowledgements, empty from Go's own watch and `ResponseMessage(0)` from the native owner (librbd's `NotifyLockOwner` takes the single non-empty acknowledgement as the owner's answer). Native librbd then releases: Go receives `ReleasedLock(ClientId{native gid, handle})`, and `lock.get_info` shows neither the header nor the object map locked. Phase `released`: native `is_exclusive_lock_owner()` false, no owners, no lockers, its exec outputs equal Go's, object map unchanged |
| `go_owner_object_map` | Go takes the lock as librbd's `AcquireRequest` and `PostAcquireRequest` do: `lock.lock` on the header (exclusive, `auto <Go watch cookie>`, `internal`), `lock.lock` on the object map (exclusive, empty cookie and tag), `rbd.object_map_load`, then `NotifyObject(AcquiredLock{Go})` (two empty acknowledgements). The Go locker's legacy address equals the Go watcher's. librbd's pre-write object-map update is the vector `[lock.assert_locked(rbd_lock, exclusive, cookie, ""), rbd.object_map_update(start, end, OBJECT_EXISTS, none)]`; Go declares `lock.assert_locked` (registered `CLS_METHOD_RD`) in read mode and the update in write mode, so the request carries READ and WRITE (librados's write op carries WRITE only; the OSD classifies CALLs by registration, not by these flags). Go first sends it with the cookie `ceph-msgr-wrong-cookie` and the range 4 to 5: aggregate -16, per-op -16 and 0, no output, the object map unchanged (`[1 1 0 0 0]` before and after); the native owner phase sent the same two inputs against its own object-map lock (empty cookie) and got -16 with an unchanged map. Go then sends librbd's form for object 2 with its own (empty) cookie as one vector: aggregate 0, per-op 0 and 0, ONDISK. The object map reads `[1 1 1 0 0]` with valid CRCs (raw and `object_map_load`). Go writes object 2 as librbd's `ObjectWriteRequest` does for an object the map called absent in an image without snapshots: `[SETALLOCHINT(65536, 65536, 0) FAILOK, WRITE(8192, 8 KiB)]` without SnapContext, and reads it back. Phase `go-owner`: native `is_exclusive_lock_owner()` false, `lock_get_owners()` = the Go address, `list_lockers()` = `client.<Go gid>`, `auto <cookie>`, that address; `watchers_list()` shows Go alive with the same address and cookie; the native header exec equals Go's locked `get_info`; the native librados read of the object map equals Go's; the native fast-diff (on-disk head map) lists objects 0, 1 and 2; librbd reads the Go extent |
| `native_requests_lock_back` | After the third proceed signal the native write to object 3 finds the lock held by a live owner and asks: Go receives `RequestLock(ClientId{native gid, handle}, force=false)` and answers `ResponseMessage(0)`. Go releases as librbd's `PreReleaseRequest` and `ReleaseRequest` do: `lock.unlock(rbd_lock, "")` on the object map, `lock.unlock(rbd_lock, auto <cookie>)` on the header, then `NotifyObject(ReleasedLock{Go})` (two empty acknowledgements). Go then receives `AcquiredLock(ClientId{native gid, handle})`. Exactly one native RequestLock was sent. Phase `reacquired`: the native write took far less than librbd's 5 s notify timeout and 10 s request retry; librbd owns the lock again with the same cookie (Go `get_info` of header and object map equal the native exec); the object map is `[1 1 1 1 0]` in Go's and librados's reads; fast-diff lists objects 0 to 3; librbd, which now reads through its loaded object map, reads the Go extent (an object the loaded map called absent would read as zeros) |
| `object_map_check` | After the fourth signal the driver closes the image and Go receives `ReleasedLock` from the native owner; `rbd object-map check` then runs as another client, and Go receives its `AcquiredLock` and `ReleasedLock` (all three acknowledged empty, nothing else). The check exits 0 with empty stdout and stderr (an object the map does not account for would be logged as an `object map error` and invalidate the map) and the image flags stay 0. The object map is unchanged by the check and equals Go's raw read and `object_map_load`; the final fast-diff lists objects 0 to 3; the librbd head read equals the setup, owner, Go and reacquire writes; librados and Go read objects 0 to 3 identically and object 4 is absent. The driver's stderr is empty for every action |
| `cleanup_state` | The responder stops with no failure and no unacknowledged notification left; Go `Unwatch`; LIST_WATCHERS of the header is empty; `lock.get_info` of the header and of the object map list no lockers. The read-only native `observe-released` action shows no lockers, no watchers, flags 0, the same exec outputs as Go and an unchanged object map and head image, and reads the Go id object as `set_id` wrote it (`rbd.get_id` and raw bytes) at the user version of Go's first set_id reply. A deferred compensation (object-map and header unlock, never a repeated lock, then ReleasedLock and Unwatch) runs only after a failure |

## Results

The results below up to "Earlier attempts" are those of the earlier change
set (exclusive-lock test 6/6); the compound class-call runs are recorded in
"Compound class calls".

Both selected configurations passed on 2026-10-16 (KST): the base test 5/5,
the exclusive-lock test 6/6 and the write test 4/4 subtests in each, no skips,
no retries. The tested source is HEAD
`5db8677e7d0b51d7594440c657c3ff8058b9f67c` plus only this change set, exported
to a separate directory with `git archive` (modified and new files copied
over). This record is the only file of the change set written after the runs.
The export holds 1197 files; the SHA-256 of its sorted per-file SHA-256 list
(`find . -type f -print0 | LC_ALL=C sort -z | xargs -0 shasum -a 256`, run in
the export root) is
`484cab68b2926717db260840df5a3e6a6f15bcdcca9e7b3c7598a158cfe0dbe5`, checked
again after the static checks, before the second run and after it. Source
SHA-256:

| File | SHA-256 |
| --- | --- |
| `integration/rbd_lock_test.go` | `189c7c6f6d64e3daf28d07970680be38fd1af31094d0c5564579c786b5969b22` |
| `integration/fixture/rbd_lock.go` | `e48e800b1c7c228db80dd0cee6092828c71481e25c0e39e79fb22b6b35872878` |
| `integration/fixture/rbd_lock_test.go` | `8a1cc2c85ee3a6616e871669b2355f5a4658fb62b740be531506c7e0dedc91e3` |
| `integration/fixture/rbd_base.go` | `b72ccc905ea2a986765dac6966c43da66c9476bd5e6d9922c955fcf3cb1f896e` |
| `integration/fixture/rbd_base_test.go` | `f12582de3562ffe2c33a6bc5ba3147d448af7eef046604c2540ca95db122bb90` |
| `integration/fixture/rbd_write.go` | `124ced0e5ffbc47eaf462126845ceb80ff598e061b6f3614e1e0741c8446ba14` |
| `integration/fixture/profile.go` | `70e347744b8dc394411372927ca82dfe979468ad66958cfe15c81885b1004048` |
| `integration/fixture/control.go` | `7933f6da3e533549af1e0687ddb35f4f7c0264b050a99182042f8617704985b6` |
| `integration/fixture/main.go` | `ad3c052dc9c7e7ccc73618a2ae9ab0be87b92ca2030ae531575ce106c8cd37b9` |
| `integration/fixture/diagnostics.go` | `7b93899ecb540d96dcd02beaeac2671f17a52d735de6d66318353800c3c00635` |
| `integration/fixture/RBD_BASE_FIXTURE.md` | `9a8a5c35fec7f83312711f16f06caabe1b41efa9e07e4f15eea32c3e37dbc58d` |
| `.github/workflows/ci.yml` | `332f00b9d7d2d3ae89969f2470a73689be8b5e2ba57326a7a638f224a66a6b08` |
| `tools/test_ci_osd_profiles.py` | `b90526c9524ec0955dc6e4cc2d6162dd260c3793cbba180be16dba706623abcf` |
| `README.md` | `1ecc47353f7f3a1d8dace7b36e4028a6ad227189374b2968b0b37849ac7defe2` |
| `SPEC.md` | `e27a91661d4cd274e51b5106fc93a69398eca025385b164756e8e7e80b6ab3be` |

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
CEPH_MSGR_TEST_RUN='^TestCephRBD(Base|Write|ExclusiveLock)Integration$' CEPH_MSGR_TEST_DIAGNOSTICS=<dir> \
sh integration/run.sh
```

| Configuration | Controller wall time (UTC, 2026-10-15) | Base test | Exclusive-lock test | Write test | Result |
| --- | --- | --- | --- | --- | --- |
| Darwin arm64 host runtime, IPv4 relay, CRC, AES client and service keys, single BlueStore OSD | 22:22:37 to 22:23:50 (73 s) | 5.28 s | 0.91 s | 2.19 s | 5/5, 6/6 and 4/4 passed |
| CGO=0 Linux arm64 container runtime, IPv6, secure, AES256K client and service keys (`KEY_TYPE`/`SERVICE_CIPHER` aes256k, `CONNECTION_MODE` secure, `IP_FAMILY` 6, `RUNTIME` container, `OSD_STORE` memstore), single MemStore OSD | 22:24:03 to 22:25:16 (73 s) | 5.34 s | 0.67 s | 2.26 s | 5/5, 6/6 and 4/4 passed |

Exclusive-lock subtest durations (CRC / secure, seconds): owner observation
0.01 / 0.00, RequestLock and release 0.03 / 0.02, Go owner and object map
0.04 / 0.02, native request back 0.04 / 0.04, object-map check 0.15 / 0.07,
cleanup 0.26 / 0.16. Native action durations (ms, CRC / secure): `setup` 109 /
117, `lock` 271 / 223 (including the four proceed waits of about 20 to 40 ms
and the check), `observe-released` 101 / 87; every native stderr was empty.
Each lock run recorded 49 Go requests (48 sent and one refused locally),
transactions 1 to 61 without duplicates (14 became the Go watch cookie, 17,
29 and 41 the notify cookies; the others are the responder's
acknowledgements). Both write runs again passed their unchanged checks with
the lock image already in the pool.

### Observed values

| Value | CRC/AES IPv4 BlueStore | secure/AES256K IPv6 MemStore |
| --- | --- | --- |
| Owned pool / lock image | ID 3; image ID `39146a611f1c`, prefix `rbd_data.39146a611f1c` | ID 3; `1299841eb7b8` |
| Go client | `client.rbd-base-e9fddf33…`, GlobalID 4755, watch cookie 14, lock cookie `auto 14`, legacy address `172.19.0.9:0/153088006` | `client.rbd-base-6a676231…`, GlobalID 4755, cookie 14, `auto 14`, `[fd62:93fe:2c9e::9]:0/3302424727` |
| Native owner (lock action) | GlobalID 14611, handle 281472896362048, cookie `auto 281472896362048`, `172.19.0.9:0/2408918305` | 14609, 281472762144016, `[fd62:93fe:2c9e::9]:0/1017981852` |
| `rbd object-map check` client | GlobalID 14624, handle 281472158141792 | 4773, 281473231946032 |
| Other native GlobalIDs (`setup`, `observe-released`) | 14612, 14623 | 4761, 14629 |
| Request map epochs | all requests and replies 24; notify IDs 0x18_00000008 to 0x18_00000010 | same |
| Reply flags | 0x4000025 for WATCH/UNWATCH, the four `lock.lock`/`lock.unlock` CALLs, `object_map_update` and the data write; 0x4000015 for reads, read CALLs (`lock.assert_locked` included), LIST_WATCHERS and NOTIFY (acknowledgement replies are not recorded); 0x4000014 for the ENOENT read of object 4 | same |
| Expected and native head image SHA-256 | `0e250e39fc9b96630c45f7d8a6c116756242fa24831e4f2be8cdb8b68a767cdc` | `8aa2a69362ab08886217096acf3a5f900eb1226c3e45fc187d037faa501a0326` |

### Native lock policy

Native librbd ran with its defaults: `rbd_auto_exclusive_lock_until_manual_request`
true selects `exclusive_lock::AutomaticPolicy`, whose `lock_requested` releases
the lock and returns 0 (`StandardPolicy`, used after a manual
`rbd_lock_acquire`, would answer -EROFS and keep it). In both runs librbd
answered Go's RequestLock with `01010400000000000000` (`ResponseMessage(0)`),
released the object map and the header lock and announced ReleasedLock 1.53 /
0.52 ms after Go sent its request. When Go owned the lock, librbd's write did
not break it: `BreakRequest` found Go's watch alive, librbd sent one
RequestLock and waited; Go's ReleasedLock notify completed 3.14 / 0.90 ms
after the request reached Go, and the native write that triggered it took 6.27 / 3.11 ms
in all (owner write that took a free lock: 2.41 / 1.21 ms).

### Notifications and acknowledgements

Raw payloads of the CRC run (the secure run differs only in GlobalIDs and
handles); every acknowledgement Go sent was empty except the answer as owner:

| # | Received by the Go watch | Payload | Go acknowledgement |
| --- | --- | --- | --- |
| 0 | Go RequestLock (own) | `0701 15000000 02000000 9312000000000000 0e00000000000000 00` | empty |
| 1 | native ReleasedLock | `0701 14000000 01000000 1339000000000000 40660084ffff0000` | empty |
| 2 | Go AcquiredLock (own) | `0701 14000000 00000000 9312000000000000 0e00000000000000` | empty |
| 3 | native RequestLock | `0701 15000000 02000000 1339000000000000 40660084ffff0000 00` | `01010400000000000000` |
| 4 | Go ReleasedLock (own) | `0701 14000000 01000000 9312000000000000 0e00000000000000` | empty |
| 5 | native AcquiredLock | `0701 14000000 00000000 1339000000000000 40660084ffff0000` | empty |
| 6 | native ReleasedLock (close) | `0701 14000000 01000000 1339000000000000 40660084ffff0000` | empty |
| 7 | check AcquiredLock | `0701 14000000 00000000 2039000000000000 600d0058ffff0000` | empty |
| 8 | check ReleasedLock | `0701 14000000 01000000 2039000000000000 600d0058ffff0000` | empty |

Each event front carries the Go cookie, the obsolete version field, the
notify ID, the payload, return code 0 and the notifier GlobalID (for example
`0101 0e00000000000000 1700000000000000 0900000018000000 1a000000 <payload>
00000000 1339000000000000` for #1). Go's three notifies completed after 0.90,
0.65 and 0.84 ms (CRC) and 0.29, 0.22 and 0.24 ms (secure), return code 0, no
missed watchers. The RequestLock completion of the CRC run: front
`0102 1100000000000000 1500000000000000 0800000018000000 00000000 00000000
9312000000000000` (opcode complete, cookie 17, version 21, notify ID, empty
payload, return code 0, notifier = the Go client), data `02000000` then
`9312000000000000 0e00000000000000 00000000` (Go's own, empty) and
`1339000000000000 40660084ffff0000 0a000000 01010400000000000000` (native
owner) and `00000000` missed. AcquiredLock and ReleasedLock completions carry
the same two identities with empty data.

### Lock info before and after (CRC run)

| Step | Header `lock.get_info` | Object map `lock.get_info` |
| --- | --- | --- |
| setup (closed) | `0101 11000000 00000000 01 08000000 internal` | `0101 09000000 00000000 01 00000000` |
| native owner | `0101 6d000000 01000000`, locker ID `0101 21000000 08 1339000000000000 14000000 "auto 281472896362048"`, info `0101 2f000000 0000000000000000` + address `01 0101 1c000000 01000000 212d958f 10000000 0200 0000 ac130009 0000000000000000` + `00000000`, then `01 08000000 internal` | one locker `client.14611`, cookie `00000000`, same address, `01 00000000` |
| native released | as setup | as setup |
| Go owner | `0101 60000000 01000000`, `0101 14000000 08 9312000000000000 07000000 "auto 14"`, address nonce `06f01f09`, `01 08000000 internal` | one locker `client.4755`, empty cookie, `01 00000000` |
| native reacquired | equal to native owner | equal to native owner |
| after cleanup | as setup | as setup |

The secure run's locker addresses are `01 0101 28000000 01000000 <nonce>
1c000000 0a00 0000 00000000 fd6293fe2c9e0000000000000000 0009 00000000`
(family 10, port 0, `fd62:93fe:2c9e::9`). Go's class inputs:
`lock.lock` header `0101 31000000 08000000 rbd_lock 01 07000000 "auto 14"
08000000 "internal" 00000000 0000000000000000 00`, object map `0101 22000000
08000000 rbd_lock 01 00000000 00000000 00000000 0000000000000000 00`;
`lock.assert_locked` `0101 15000000 08000000 rbd_lock 01 00000000 00000000`;
`rbd.object_map_update` `0200000000000000 0300000000000000 01 00`;
`lock.unlock` object map `0101 10000000 08000000 rbd_lock 00000000`, header
`0101 17000000 08000000 rbd_lock 07000000 "auto 14"`. All returned 0 with
empty output.

### Object map before and after

Identical in both runs (the bytes depend only on the states):

| Step | States | Raw `rbd_object_map.<id>` |
| --- | --- | --- |
| setup | 1 0 0 0 0 | `0e000000 0101 08000000 0500000000000000` `40 00` `0c000000 18459682 01000000 04c617ff` |
| native owner, native released | 1 1 0 0 0 | data `50 00`, data CRC `8537d2c0` |
| Go updated | 1 1 1 0 0 | data `54 00`, data CRC `5956588e` |
| native reacquired, final, after cleanup | 1 1 1 1 0 | data `55 00`, data CRC `2ecefa9d` |

The header CRC `18459682` and each data CRC equal the test-only
`ceph_crc32c(0, …)` of the header bufferlist and of the data block, so the
test-only `BitVector<2>` decoder agrees with the OSD class and with native
librbd. `rbd.object_map_load` returns the same header and data with an empty
footer (`00000000`, CRCs disabled).

### Cleanup verification

Both runs: the journal held `pool-create`, `pool-init`, `native-setup`,
`native-resize`, `native-lock-setup`, `native-lock-lock`,
`native-lock-observe-released` (`read_only: true`), `native-write-setup`, the
four `native-write-observe-*` actions and `native-write-watch`, all
`applied`; cleanup removed the pool (the three images, their object map,
metadata and lock attributes with it), found neither its name nor its ID in a
fresh native OSDMap and deleted both identities (`completed: true`, no error).
Before cleanup the lock image had no lockers on the header or the object map
and no watchers (Go and native). No Go compensation ran. The Go watch lived
under 1 s, so no ping was due (0 pings).

## Compound class calls

Both configurations passed from the export of the complete working tree with
the read-mode rule (HEAD `16faf1e` plus every modified and new file, 1200
files, tree SHA-256
`43662f91d260151ccff46412bfb83a2ec98e48edc5e581e1932d76d199128b51`, the same
export as [OSD_CALL_COMPOUND_VALIDATION.md](OSD_CALL_COMPOUND_VALIDATION.md),
where its static checks are listed): base 5/5, exclusive-lock 7/7, write
4/4, no skips, no retries.

| Configuration | Controller wall time (UTC, 2026-10-15) | Base | Exclusive-lock | Write |
| --- | --- | --- | --- | --- |
| CRC, AES, IPv4, Darwin arm64 host runtime, one BlueStore OSD | 23:42:57 to 23:44:15 (78 s) | 5.28 s | 1.01 s | 2.39 s |
| secure, AES256K, IPv6, CGO=0 container runtime, one MemStore OSD | 23:44:15 to 23:45:27 (72 s) | 5.33 s | 0.99 s | 2.24 s |

Each lock run records 55 Go requests (all sent, none refused locally),
transactions 1 to 68. Native actions: `setup` 123 / 137 ms, `lock` 297 /
320 ms, `observe-released` 124 / 99 ms (CRC / secure); every native stderr
was empty.

| Value | CRC/AES IPv4 BlueStore | secure/AES256K IPv6 MemStore |
| --- | --- | --- |
| Go set_id vector `[CREATE(EXCL), W rbd.set_id]` (tid 10) | 0, per-op 0 0, flags 0x4000025, user version 2 | same, user version 1 |
| Go id object | `0c000000` `beef40d629c0`; `rbd.get_id` the same 16 bytes | `0c000000` `beef06dd30d5` |
| Go repeat (tid 13) | -17, per-op -17 0, flags 0x4000025, bytes unchanged | same |
| Native set_id vector, twice | 0 then -17; `0c000000` `f00d40d629c0`, user version 13 after both | 0 then -17; `f00d06dd30d5`, version 5 after both |
| Native `observe-released` of the Go id object | same bytes and `get_id` output, user version 2 | same, version 1 |
| Non-matching-cookie vector `[R lock.assert_locked, W rbd.object_map_update]` (Go tid 38; native owner phase) | Go -16, per-op -16 0, flags 0x4000035; native -16 | same |
| Its inputs | `01012b000000 08000000 rbd_lock 01 16000000 ceph-msgr-wrong-cookie 00000000` and `0400000000000000 0500000000000000 01 00` | same |
| Object map around it (Go and native) | data `50 00` (`[1 1 0 0 0]`), data CRC `8537d2c0`, unchanged | same |
| librbd-form vector `[R lock.assert_locked, W rbd.object_map_update]` (tid 40) | 0, per-op 0 0, flags 0x4000035, user version 13; map then data `54 00` (`[1 1 1 0 0]`) | same, user version 14 |

The flags 0x4000035 carry READ and WRITE because Go declared one CALL in read
mode; a librados write op (librbd's and the native driver's) carries WRITE
only (`Objecter::prepare_mutate_op`). The OSD classifies a CALL by the
method's registration, and the results equal native. The rest of the
protocol, the object-map check (exit 0, empty output, flags 0) and the
cleanup matched the earlier record. python rados exposes only the aggregate
of a native write op, so the native per-op results of the two native vectors
are not observed; the positive object-map vector's native counterpart is
librbd's own `UpdateRequest`, whose effect is the object map the native
owner and reacquire writes leave behind.

Earlier runs of these leaves, before the client refused a CALL after a
write-mode CALL, sent `lock.assert_locked` in write mode (flags 0x4000025)
from the exports `3df837796b5ab6580a63b5acb907ada18a04709f08ff1e6fd98ad4aa9c65adfe`
and `fd6c929b6abc75b2831ce5529d2ce2c11e7525a8526cd95ddfa31c48b99fc333` and
passed with the same codes, per-op results, inputs and object-map bytes.

## Earlier attempts

No recorded run failed. The first failure of the change set was a unit
expectation: `TestRBDLockTestOnlyEncodings` expected a hand-computed
little-endian hex for a decimal watch handle that was wrong; the vector now
uses the handle `0x55d48bcef190`, whose bytes are evident. One development run
from the working tree (CRC/AES host IPv4 BlueStore) passed before the
recorded ones; after it the evidence encoding of decoded object-map states was
changed from `[]uint8` (marshalled as base64) to integers, an evidence-only
change.

## Findings and limits

Resolved product finding: librbd's object-map update is one write vector
`[CALL lock.assert_locked, CALL rbd.object_map_update]`
(`librbd/object_map/UpdateRequest.cc`, also `RefreshRequest::send_resize`).
In the earlier change set's runs the product refused any CALL after a CALL
before transmission (`ErrUnsupportedOSDOperation`, no transaction) and the
test sent the guard and the update as two requests, which loses the atomic
guard librbd relies on against a writer that lost its lock. The client now
admits a CALL after read-mode CALLs; the test sends `lock.assert_locked` in
read mode and requires the vector to be sent. See "Compound class calls" for
the positive and the non-matching-cookie forms and librbd's
`create(exclusive)` plus `rbd.set_id` form.

No other product defect was found: WATCH, NOTIFY with completions carrying
every acknowledgement, NOTIFY_ACK with empty and non-empty payloads from a
concurrent goroutine, LIST_WATCHERS, the lock and rbd class CALLs and the data
write carried what librbd's lock protocol needs, and native librbd accepted Go
as a live managed-lock owner. Observations:

- librbd's `ClientId` handle is the librados watch cookie, and its lock cookie
  is `auto <handle>`; Go's ClientId and cookie used the Go watch cookie (14)
  and librbd treated them the same way (`lock_get_owners`, liveness, no
  break, no blocklist).
- Every notifier receives its own notification on its own watch; librbd
  acknowledges its own lock notifications empty, and `NotifyLockOwner` takes
  the single non-empty acknowledgement as the owner's answer.
- `list_lockers()` and `flags()` of a read-write librbd instance are cached
  at its last refresh; the driver reads them from fresh read-only opens.
- After `lock.unlock` the header keeps type EXCLUSIVE and tag `internal`, the
  object map type EXCLUSIVE and an empty tag, with no lockers.
- librbd's whole-object diff from the beginning of time reports one 64 KiB
  extent per existing object from the on-disk head map.
- `rbd object-map check` acquires and releases the lock in its own client
  (one AcquiredLock and one ReleasedLock to every watcher).

Not verified here:

- the manual-lock `StandardPolicy` (-EROFS answers), forced RequestLock,
  `lock_break`, blocklisting, a dead or unresponsive owner, lock contention
  between more than two clients, shared locks and lock cookie updates
  (`set_cookie` after a rewatch);
- journaling, deep-flatten, snapshots with object maps (`object_map_snap_add`,
  EXISTS_CLEAN, fast-diff between snapshots), object-map resize or invalidation
  and `rebuild_object_map`, discard and other writes with post-write map
  updates, clones/parents/copy-up, larger orders and fancy striping;
- the Go watch's periodic ping (no ping was due), watch reconnect, notify
  timeouts, cancellation, uncertain outcomes and daemon failures during the
  lock exchange;
- the replicated-PG profiles, three replicas, EC pools, combinations other than
  the two recorded, the CI rows on GitHub runners (selector changed, not
  executed), Windows and long-running operation.
