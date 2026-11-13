# RBD exclusive-lock owner loss and blocklist handoff

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Issue #12, requirement 3, one cell: lock-owner loss and blocklist handoff.
The product is unchanged: no product code, no new primitive and no RBD or
lock API. The cell uses only the existing explicit OSD API (`OpenOSD`,
`Request` with `OSDCall`, `OSDSetAllocHint`, `OSDWrite`, `OSDStat`, `OSDRead`
and `OSDListWatchers`, `WatchObject`, `Next`, `Acknowledge`, `Ping`,
`Unwatch`, `NotifyObject`, `Close`) and `MonCommand`. The lock-class inputs,
the `cls_lock_break_op` encoding, librbd's watch-notify payloads, the lock
order and the object-map decoding are test-only consumer code in
`integration/rbd_lock_test.go` and `integration/rbd_lock_owner_loss_test.go`.
The native driver actions are development-only; see
[RBD_BASE_FIXTURE.md](RBD_BASE_FIXTURE.md) (section "Lock-owner-loss
sequence").

The [exclusive-lock acceptance](RBD_LOCK_VALIDATION.md) hands the lock over
between owners that are alive and release it when asked. Here the owner never
releases: the lock is taken away from it, once in each direction.

1. A Go client holds the lock and stops renewing its watch. Native librbd
   breaks the lock and blocklists the Go owner.
2. That native owner is killed. A Go client breaks its lock with the same
   steps librbd takes.

It does not establish that a consumer built on this client is a complete RBD
lock implementation.

## What the cell claims

Each claim is judged on its own evidence.

| Claim | Evidence | Not evidence for it |
| --- | --- | --- |
| Liveness rule: native librbd does not break the lock of an owner whose watch is listed | The Go observer's reads at the first native RequestLock: the lock still names the Go owner, its watch is listed, the MON blocklist is unchanged | The time the native write took |
| Native break: once the watch is gone, the lock is native librbd's and the old owner's address is fenced | Native `lock.get_info` of header and object map, librbd's owner and watcher lists and `osd blocklist ls` taken by the native owner; the same lock bytes, watchers and the MON `osd dump` blocklist read by the Go observer | librbd's AcquiredLock notification taken alone |
| Fencing: the old Go owner can change nothing on the connection that took the lock | The raw replies to three mutations it sent once each, and the next native observation of the object map, the data objects, the image and both locks | The blocklist entry taken alone; the Go client's own reads |
| Go break: a Go client takes a dead native owner's lock in librbd's order | The raw results of each step, the MON blocklist and the OSDMap epoch before and after its command, and a native observation of the result | That the steps appear in this order in the test source |
| Result: native librbd accepts the image afterwards | A fresh native open that writes and closes, `rbd object-map check`, the final object map, data objects and image read | Any Go read |

## Pinned source facts

Read at the reference tag for this change.

- [`src/librbd/managed_lock/AcquireRequest.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/librbd/managed_lock/AcquireRequest.cc):
  read the locker, try `lock.lock`, and on `-EBUSY` run a `BreakRequest`
  before locking again. A break that ends with `-EAGAIN` leaves the lock
  alone; the caller then asks the owner with a RequestLock notification.
- [`src/librbd/managed_lock/GetLockerRequest.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/librbd/managed_lock/GetLockerRequest.cc):
  the locker is the one entry of `lock.get_info` with tag `internal` and a
  cookie `auto <watch handle>`; its address is kept in legacy form
  (`ip:port/nonce`).
- [`src/librbd/managed_lock/BreakRequest.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/librbd/managed_lock/BreakRequest.cc):
  list the header's watchers. An owner is alive when one watcher has the
  locker's address and its handle as cookie; then the result is `-EAGAIN`.
  Otherwise read the locker again and stop if it changed, add the locker's
  address to the blocklist (`rbd_blocklist_on_break_lock`, default true),
  wait for the latest OSDMap, and send `lock.break_lock` with the locker's
  entity and cookie.
- [`src/librados/RadosClient.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/librados/RadosClient.cc)
  `blocklist_add`: the MON command `osd blocklist` with `blocklistop` `add`
  and `addr`, an `expire` only when one was asked for, then a wait for the
  latest OSDMap. Without `expire` the MON applies its configured default,
  3600 s in these runs.
- [`src/mon/MonCap.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/mon/MonCap.cc):
  `profile rbd` grants exactly that command form, with an address that has a
  nonce. It grants no removal.
- [`src/librbd/object_map/LockRequest.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/librbd/object_map/LockRequest.cc):
  lock the object map with an empty cookie and tag; on `-EBUSY` read its
  lockers, break all of them in one write and lock again. It has no
  blocklist step of its own.
- [`src/cls/lock/cls_lock.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/cls/lock/cls_lock.cc):
  `lock` stores the address of the connection that sent it. `break_lock`
  removes the named entity and cookie whoever sends it, and returns `-ENOENT`
  when there is none.
- [`src/osd/Watch.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/Watch.cc):
  a watch that pings is kept for its timeout after its last ping; the
  registration counts as one. A closed connection does not shorten or extend
  that.
- [`src/osd/PrimaryLogPG.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc):
  when a watch times out, the OSD removes it and sends DISCONNECT to a
  watcher that is still connected (`handle_watch_timeout`). `do_op` answers
  an operation whose source address is blocklisted with `-EBLOCKLISTED`
  before it looks at the object.
- [`src/include/rados.h`](https://github.com/ceph/ceph/blob/v20.2.4/src/include/rados.h):
  `EBLOCKLISTED` is 108.

One fact of this client matters for the fencing claim. A client that is not
a CephFS client uses a new address nonce for every OSD connection
(`internal/session/handshake.go`, `cephmsgr/client.go`). librados uses one
nonce for all connections of a client instance. A blocklist entry therefore
covers one Go connection, not the Go client. See "What the cell does not
establish".

## Executable boundary

Test: `TestCephRBDLockOwnerLossIntegration` in the `integration` suite, on
the RBD fixture (`CEPH_MSGR_TEST_RBD=1`, single OSD, `legacy-v6`) under its
exact selector. The fixture then serves the closed action sequence `setup`,
`observe-go-owner`, `takeover`, `observe-go-broke`, `final` on the lock image
and no other RBD test runs. Required rows:

- `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerloss`
  (CRC, AES client and service keys, IPv4, host test process, one BlueStore
  OSD);
- `rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerloss`
  (secure, AES256K client and service keys, IPv6, container test process, one
  MemStore OSD).

Reproduce the CRC row with:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=1 CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 CEPH_MSGR_TEST_RBD=1 \
CEPH_MSGR_TEST_RUN='^TestCephRBDLockOwnerLossIntegration$' \
sh integration/run.sh
```

The image is the lock image of the exclusive-lock acceptance: 320 KiB, order
16, five objects, features 29 (`layering`, `exclusive-lock`, `object-map`,
`fast-diff`). `setup` writes object 0. The native takeover writes object 1,
the Go breaker object 2 and the final native writer object 3. Object 4 is
the target of the fenced write and must never exist.

### Clients

All three Go clients authenticate as the same identity
(`client.rbd-base-<token>`, MON and OSD `profile rbd`). Each has its own
GlobalID and one OSD connection at a time.

- The **observer** watches `rbd_header.<id>` from before the first lock to
  the end. A responder acknowledges every notification empty, as librbd does
  for a client that does not own the lock, and pings the watch every 5 s. It
  supplies caller-side placement and every Go read that the claims use.
- The **zombie** takes the lock and then sends nothing until the first
  handoff is over.
- The **breaker** is dialed after the first handoff.
- The **native driver** uses its own identity
  (`client.rbd-base-native-<token>`) and one process per action.

Every Go header watch uses a 30 s timeout, which is what the OSD applies to
librbd's watch.

### Why the zombie stays connected

The zombie loses its liveness by silence: no ping, no acknowledgement, no
release. Its connection stays open. That is the case blocklisting exists
for. A client that only disconnected could not send anything later, and
nothing would test the fence. An explicit UNWATCH would be a release step of
a live owner.

Silence is also the only way to have a mutation arrive from the fenced
address afterwards: the lock record holds the address of the connection that
sent `lock.lock`, and a new connection of this client would have another
one.

### Sequence

| Step | What happens | Checked |
| --- | --- | --- |
| `setup` (native) | The image is created and object 0 written through librbd | No locker, no watcher, object map `[1 0 0 0 0]` |
| Zombie lock | The zombie watches the header, sends `lock.lock` on the header (exclusive, cookie `auto <handle>`, tag `internal`) and on the object map (empty cookie and tag), and announces AcquiredLock. It acknowledges its own notification | Its locker address equals its watcher address; the observer and the zombie acknowledged empty |
| `observe-go-owner` (native) | Read-only observation with `osd blocklist ls` | Stage `go-owner`, below |
| `takeover` starts (native) | A read-write open and a write of object 1, in the background. The write returns only when librbd owns the lock | |
| Request while listed | The observer receives the first native RequestLock and reads the lock, the watchers and the MON blocklist | The lock is the zombie's, its watch is listed, nothing is blocklisted |
| Native acquired | The observer receives the native AcquiredLock. The driver publishes its `acquired` phase: owner state, lockers, watchers, both `lock.get_info` outputs, the object map and `osd blocklist ls` before the open and after the write | |
| Fenced attempts | The zombie sends, on the connection that took the lock, each once: librbd's guarded object-map update for object 4, the data write to object 4 that would follow, and `lock.unlock` of the header | Raw outcomes kept; judged at stage `takeover` |
| Zombie watch | The events its watch had queued are read to the end | |
| New connection | The zombie closes that connection, opens a new one and sends one `lock.get_info` | Raw outcome kept |
| Fresh read | The breaker is dialed and reads the header lock, the object map and the five data objects | Byte-equal to the native reads of the next step |
| `takeover` ends (native) | After the proceed signal the driver observes once more, prints its output and ends itself with SIGKILL while it owns the lock | Stage `takeover`, below |
| Dead owner listed | The breaker watches the header, reads the locker and tries `lock.lock` | The locker is the killed owner's record, checked before the attempt; `-EBUSY` as a known OSD answer; the killed owner's watch is listed |
| Dead owner unlisted | The breaker reads the watcher list once a second until no watcher has the locker's address and handle, for at most 90 s, then reads the locker again | The refreshed locker equals the first read and the killed owner's record. Otherwise the run stops here, before the MON command |
| Blocklist | The breaker sends `{"prefix":"osd blocklist","blocklistop":"add","addr":"<locker address>"}` to the MON and reads the OSDMap epoch that holds the entry. Every later request names at least that epoch | The run stops here when the entry is missing |
| Break and lock | `lock.break_lock(rbd_lock, client.<gid>, auto <handle>)`, `lock.lock`; on the object map `lock.lock` (`-EBUSY`), `lock.get_info`, `break_lock` of the one locker, `lock.lock` | Each call must be answered with its one expected code as a known OSD answer carrying the reply identity of its submission; an unknown outcome or another code stops the run before the next mutation. The object-map `break_lock` is built only when the sole locker is the killed owner's record. Codes kept; judged again at stage `go-break` |
| Go write | `rbd.object_map_load`, AcquiredLock notification, `[lock.assert_locked, rbd.object_map_update]` for object 2, `[SETALLOCHINT, WRITE]` on object 2 | Each answered 0 with the breaker's identity |
| `observe-go-broke` (native) | Read-only observation with `osd blocklist ls` | Stage `go-break`, below |
| Go release | `lock.unlock` on the object map and the header, ReleasedLock notification, UNWATCH | |
| `final` (native) | A fresh read-write open writes object 3, reads the Go extent as the owner, and closes. `rbd object-map check` runs. Observation with `osd blocklist ls` | Stage `final`, below |
| End | The observer has received the final writer's and the check's AcquiredLock and ReleasedLock. It stops its responder, removes its watch and reads the state once more | |

The record is judged after `observe-go-owner`, after `takeover`, after
`observe-go-broke` and at the end. A contradiction at one stage stops the
run there, so a doubtful state is never followed by the next step of a
handoff. After a failure the deferred compensation lets whoever may still
hold a lock unlock it once. It never repeats a lock or a mutation.

### Claims as the test checks them

Native values come from the driver's output. Go values come from the
observer's requests and its MON `osd dump`. Lockers are decoded from the raw
`lock.get_info` output in both, and the native ones must equal librbd's
`list_lockers()`.

- **`go-owner`.** Natively and from Go: the header lock is the zombie's with
  its cookie and address, the object-map lock is the zombie's with an empty
  cookie, the watchers are the observer and the zombie, the blocklist holds
  neither address, the object map is `[1 0 0 0 0]`. The `lock.get_info`
  outputs and the object-map bytes are equal between native librados and Go.
  The observer has one notification, the zombie's AcquiredLock.
- **`takeover`, liveness.** At the first native RequestLock the Go observer
  still read the zombie as header locker with its watch listed and no new
  blocklist entry. The observer's notifications after the zombie's
  AcquiredLock are at least one native RequestLock, none forced, and then
  the native AcquiredLock, each with the native ClientId (GlobalID and watch
  handle).
- **`takeover`, break.** In the native `acquired` phase, in the Go
  observer's read after AcquiredLock and in the native observation before the
  kill: header and object-map locks are the native owner's (`auto <handle>`
  and an empty cookie, its address), the watchers are the native owner and
  the observer, the zombie's watch is not listed under any address, the
  object map is `[1 1 0 0 0]`, and the blocklist holds exactly one entry more
  than before the sequence, the zombie's address, with an expiry after the
  observation. `osd blocklist ls` taken before the native image was opened
  holds no new entry. librbd reports itself as the owner at its address.
- **`takeover`, fencing.** The zombie's watch had queued between one and as
  many native RequestLocks as the observer saw, each with a notify ID the
  observer also has, and then the OSD's DISCONNECT; the handle ended with
  `ErrOSDWatchDisconnected`. Three attempts exist, in order
  `object_map_update`, `data_write`, `lock_unlock`. Each was invoked once,
  carries the zombie's GlobalID and a transaction, and was answered for that
  transaction with code -108 as a known server error, not an unknown
  outcome. Their transactions follow the zombie's last request before its
  silence without a gap, so it sent nothing in between. The native
  observation taken afterwards has the same object-map bytes and lock
  outputs as the `acquired` phase, objects 0 and 1 only, flags 0 and the
  image of the setup and takeover writes.
- **`takeover`, other clients.** The zombie client's `lock.get_info` on a
  new connection is the next transaction, was answered 0 and names the
  native owner. The breaker has a GlobalID of its own and read the same
  header lock output, object-map bytes and data object digests as the native
  observation. The native driver ended with the supervisor's status for
  SIGKILL (-9) after printing a complete output.
- **`go-break`.** The breaker read the killed owner as locker, got -16 from
  its first `lock.lock`, and saw that owner's watch listed at least once.
  The Go observer saw it listed too, with the lock unchanged. With the watch
  gone the locker was read again unchanged, the lock was still the dead
  owner's and its address was not blocklisted. After the MON command
  (code 0) the blocklist holds exactly the two addresses, in a higher OSDMap
  epoch, and the lock is still the dead owner's. `break_lock` was sent and
  answered at an epoch not lower than that one, with 0; `lock.lock` then
  returned 0. The object-map `lock.lock` returned -16, its one locker was
  the dead owner with an empty cookie, its `break_lock` and the second
  `lock.lock` returned 0. Natively and from Go: both locks are the
  breaker's, the watchers are the observer and the breaker, the object map
  is `[1 1 1 0 0]` with equal bytes, and the native image is the model with
  the Go extent, objects 0 to 2, flags 0. The observer received the
  breaker's AcquiredLock next.
- **`final`.** After the release no lock is left and only the observer
  watches. After the final native action no lock is left, the object map is
  `[1 1 1 1 0]`, objects 0 to 3 exist and object 4 does not, flags are 0, the
  image read is the model with all four writers, the owner read the Go
  extent, `rbd object-map check` ended with 0 and no output, and both
  blocklist entries are still there. The observer's remaining notifications
  are exactly the breaker's ReleasedLock and two AcquiredLock and
  ReleasedLock pairs from two further clients, neither a Go client nor the
  killed owner. Every notification was acknowledged empty without an error,
  no ping failed, and the zombie has three attempts in all.

### What the cell does not establish

- **Fencing of a Go client as a whole.** The blocklist entry is the address
  in the lock record, which is the address of one OSD connection. The test
  shows that this connection is refused. It also records that another
  connection of the same client, opened afterwards, is served: its read
  returned 0. A consumer that needs librbd's fencing for a Go lock owner
  must send every write of that owner on the connection that sent
  `lock.lock`, or treat the loss of that connection as the loss of the lock.
  Nothing here tests a write on such a second connection. The data write
  carries no lock assertion, as in librbd, so only the blocklist stands
  between a stale owner and a data object.
- **That the fenced requests were never executed by the OSD.** The reply
  code says so and the native observation shows no effect. No daemon-side
  execution record is read.
- **librbd's reasons.** That librbd found the lock busy, got `-EAGAIN` from
  its break while the watch was listed and blocklisted before `break_lock`
  is read from the source. The test observes the notifications, the
  blocklist and the lock from outside. The order of librbd's blocklist entry
  and its `break_lock` is not observed.
- **Attribution on the secure row.** Nothing here depends on reading frames,
  so both rows check the same claims.

## Daemon-free checks

These establish no native result.

- `TestRBDLockOwnerLossClaims` (cluster-free, in the Basic CI list): a
  consistent record passes at each of the four stages, and a record of one
  stage is refused at every other. 140 contradictory records are refused,
  each with every other part of the record intact, at the stage they belong
  to and again in a complete record. Examples: a lock broken or an address
  blocklisted while the watch was listed; no native RequestLock; a zombie
  that is not blocklisted natively or in the MON; a second new blocklist
  entry; an expired entry; a zombie watch that is still listed; a fenced
  attempt that was applied, answered with another code, unknown, repeated,
  missing or sent after another request; an object or object-map change
  after the attempts; a refused read on the new connection; a native owner
  that exited by itself; a Go break while the watch was listed, without a
  blocklist entry, without a new map epoch or at an older epoch; a free
  object-map lock; a missing notification or one from the killed owner; a
  failed object-map check.
- `TestRBDLockOwnerLossBreakTargets` (cluster-free): the two checks the
  breaker runs before its mutations. The header check refuses a locker that
  changed to another single owner during the polling, another owner from the
  start, a new cookie, a new connection address, another client at the same
  address, a missing record and an incomplete killed-owner identity. The
  object-map check refuses a replacement owner, a second locker, no locker
  and a record with a header cookie.
- `TestRBDLockOwnerLossTestOnlyEncodings` (cluster-free): the
  `cls_lock_break_op` bytes for a managed-lock cookie and for the empty
  object-map cookie, refused locker entities, the blocklist expiry forms,
  the locker projection of a `lock.get_info` output a live OSD returned, and
  the refusal of a native view whose parts disagree or are missing.
- `integration/fixture/rbd_lock_test.go`: both action sequences are closed
  and ordered and each refuses the other's actions at every position; the
  selector is admitted only in its exact form and only with the RBD opt-in;
  only the takeover may end with the killed status and only observations are
  read-only; no output of one lock-owner-loss action is admitted as another,
  and the ordinary actions carry none of the new fields; the cleanup removes
  only the exact blocklist entries of the observed lock owners and reports
  one that remains.

## Execution evidence

Direct native execution, not hosted CI: Linux amd64 hosts (x86_64, kernel
6.8), Docker 29.9.1, Go 1.26.9, the image above (daemon version `ceph version
20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`),
provider `github.com/JSYoo5B/ceph-testcontainers-go` v0.5.0, three MONs, two
MGRs, one OSD and the native oracle container. Every run used the environment
that the native workflow passes for its catalog row and the same result gate
(`tools/native_test_results.py snapshot` before and `verify` after), one
`go test` process per Docker engine. The source of every run was uploaded as
`git archive` of the named commit, with the commit ID recorded on the host
before the run.

Source commit `85c9d22a7f5b648285cb676dd651aebf80a383af`, 2026-11-13 (UTC).
It holds the fixture sequence, the test, its claims and the catalog rows; the
commits after it in the same pull request change documentation only.

| Scenario ID | Gate | Test duration | Row wall time |
| --- | --- | --- | --- |
| `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerloss` | pass, 1 expected test, no problem | 62.64 s | 151 s |
| `rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerloss` | pass, 1 expected test, no problem | 57.71 s | 144 s |
| `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-37bcdb` | pass, 5 expected tests, no problem | base 5.34 s, clone 2.65 s, journal 2.63 s, exclusive-lock 11.19 s, write 1.68 s | 100 s |
| `rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-suite-37bcdb` | pass, 5 expected tests, no problem | base 5.32 s, clone 2.80 s, journal 2.41 s, exclusive-lock 10.87 s, write 2.13 s | 98 s |

The two existing RBD rows were run because the lock control's request
parser, driver, output decoder and profile admission changed. Each of the
four runs passed at its first attempt on that commit. The same four rows had
passed at their first attempt on the commit before it,
`c0701808c4d6fb5ad7771339ea8ef3545ba9c1ca` (62.78 s and 56.19 s), which
differs only in the failure path of the takeover driver. Earlier runs of this
change are listed below.

Almost all of the test's time is two waits for a watch to expire.

Source commit `4e0d7549f6e6cbbf99fb4f2ea6bdd9a90947d003`, 2026-11-13 (UTC),
the review follow-up: the breaker checks its target before the MON command
and before each `break_lock`, and requires every break and acquire call to be
a known OSD answer with its expected code. Same environment and gate.

| Scenario ID | Gate | Test duration | Row wall time |
| --- | --- | --- | --- |
| `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerloss` | pass, 1 expected test, no problem | 58.05 s | 146 s |
| `rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerloss` | pass, 1 expected test, no problem | 61.98 s | 148 s |

Both passed at their first attempt. The two broad RBD rows and the four
controls of the earlier commit were not repeated on this one; the commit
changes only this test, its daemon-free tests and the cluster-free list.

One control on this commit (temporary edit on the uploaded copy, never
committed, one run, CRC row): the locker read again after the watch expired
was replaced in the test by another single owner with the same address. Gate
fail: `before the MON blocklist command: the locker read again is not the
killed owner`. The record ends at `dead_owner_unlisted` with an empty
blocklist command, and the cleanup removed one blocklist entry, the
zombie's; the killed owner's address was never blocklisted and no
`break_lock` was sent. The replacement owner is simulated in the test's own
value: no second live owner took the lock.

### Observed values

Client GlobalIDs, addresses, the image ID and SHA-256 values differ per
fixture and are in the run's `rbd-lock-owner-loss-results.json`.

| Value | CRC/AES IPv4 host BlueStore | secure/AES256K IPv6 container MemStore |
| --- | --- | --- |
| Lock cookies | zombie `auto 1`, native owner `auto <15-digit handle>`, breaker `auto 8` | same |
| Addresses | four different nonces on one IPv4 address, port 0 | four different nonces on one IPv6 address, port 0 |
| Native RequestLocks seen by the observer | 6, about 5 s apart, from 0.4 s to 25.4 s after the zombie's AcquiredLock | 6, from 0.4 s to 25.4 s |
| Native AcquiredLock | 30.6 s after the zombie's AcquiredLock | 30.1 s |
| Zombie watch registration to native AcquiredLock | 30.58 s | 30.13 s |
| Native write that broke the lock | 30.21 s | 29.78 s |
| Events queued on the zombie's watch | the same 6 RequestLocks (same notify IDs), then DISCONNECT | same |
| `osd blocklist ls` before the native open / after its write | empty / the zombie's address | same |
| Zombie's blocklist entry | 3599 s left when the native owner was killed, about a second after the entry appeared | 3600 s |
| Go observer's MON `osd dump` epoch | 24 before the takeover, 25 at the first RequestLock, 26 with the zombie's entry, 27 with both entries | 25, 25, 27, 28 |
| Zombie requests | transactions 2 to 9 before the silence (watch registration, two locks, two reads, the notify and its cookie, the acknowledgement); 10, 11, 12 the three attempts; 13 the read on the new connection | same |
| Fenced `[R lock.assert_locked, W rbd.object_map_update]` | -108, reply flags 0x4000034, per-op results 0 and 0, request and reply map epoch 26 | same, epoch 27 |
| Fenced `[SETALLOCHINT, WRITE]` | -108, flags 0x4000024, per-op results 0 and 0 | same |
| Fenced `lock.unlock` | -108, flags 0x4000024 | same |
| Same client, new connection, `lock.get_info` | 0, flags 0x4000015, names the native owner | same |
| Native driver exit | -9, empty stderr, complete output | same |
| Killed owner's watch after the kill | listed in 30 polls, gone after 30.07 s | 26 polls, 26.10 s |
| Breaker's first `lock.lock` | -16 | -16 |
| MON reply to the breaker's command | code 0, `blocklisting <address> until <time> (3600 sec)` | same |
| OSDMap epoch before / with the breaker's entry | 26 / 27 | 27 / 28 |
| `lock.break_lock` (header), `lock.lock` | 0 at request and reply epoch 27, flags 0x4000025; 0 | same, epoch 28 |
| Object map: `lock.lock`, lockers, `break_lock`, `lock.lock` | -16; the killed owner with an empty cookie; 0; 0 | same |
| `cls_lock_break_op` input (header) | `0101 2d000000 08000000 rbd_lock 08 <s64 GlobalID> 14000000 "auto <handle>"` | same |
| Object map `[1 0 0 0 0]`, `[1 1 0 0 0]`, `[1 1 1 0 0]`, `[1 1 1 1 0]` | data bytes `40 00`, `50 00`, `54 00`, `55 00` with the CRCs of the exclusive-lock record | same |
| `rbd object-map check` | exit 0, empty stdout and stderr, 54 ms | 32 ms |
| Observer notifications | 14: 1, 6, 1, then the breaker's AcquiredLock and ReleasedLock and two native pairs | same |
| Observer pings | 12, no error | 11, no error |
| Native action durations (ms): setup, observe-go-owner, takeover, observe-go-broke, final | 165, 127, 30358, 105, 226 | 135, 133, 29930, 127, 169 |

The upper half of a notify ID is the OSDMap epoch at which the OSD issued
it. In both runs the native AcquiredLock carries the epoch at which the Go
observer then read the zombie's entry (26 and 27), and the breaker's two
notifications the epoch of its own entry (27 and 28).

Every native driver process ended with empty stderr, the killed one
included. librbd logged nothing at error level while it asked a silent owner
six times, blocklisted it and broke two locks.

### Cleanup

Both rows: `rbd-base-owned.json` holds `native-lock-setup`,
`-observe-go-owner` (`read_only`), `-takeover`, `-observe-go-broke`
(`read_only`) and `-final`, all `applied`. `rbd-base-cleanup.json` lists the
two lock-owner addresses the fixture observed and the two blocklist entries
it removed and verified absent, then reports the pool removed, natively
absent, both identities removed and `completed: true`. After each run no
container with the provider's label was left on the host. No independent
final network inventory is retained.

### Controls

The checks were shown to fail on wrong behavior by four runs with a temporary
edit, on the tree of the commit above. Each edit was applied only to the
uploaded copy and never committed. Each is one run in one profile.

| Control | Profile | Edit | Result |
| --- | --- | --- | --- |
| Renewed watch | CRC row | After it took the lock, the zombie pings its header watch every 5 s | Gate verdict fail. The test stopped at `observer: native AcquiredLock not received within 1m30s`. Until then the observer had received 18 native RequestLocks about 5 s apart and no AcquiredLock. The compensation then released both locks (0 and 0) |
| No blocklist on break | secure row | The native driver runs with `rbd_blocklist_on_break_lock=false` | Gate verdict fail. The test failed at stage `takeover` with seven lines: no new blocklist entry in the `acquired` phase, the Go observer's read and the last native observation; the object-map update refused with -16 and not -108 (per-op -16 and 0); the data write applied (0); the unlock refused with -2; the native image differs from the model, object 4 exists |
| Go break without a blocklist entry | CRC row | The breaker skips its MON command and the inline check of the entry | Gate verdict fail. The test failed at stage `go-break` with four lines: the killed owner's address is missing from the blocklist in the Go read before the break, the Go read after it and the native observation, and there is no new OSDMap epoch (26 and 26) |
| Go break of a listed owner | secure row | The breaker goes on after one watcher-list read that still shows the dead owner's watch | Gate verdict fail. The test failed at stage `go-break` with three lines: the Go observer's read before the blocklist command lists the dead owner's watch (three watchers, two expected), and the breaker never saw the watch gone |

The second control shows what the blocklist entry is for. librbd broke both
locks as before and wrote. The stale owner's guarded object-map update was
stopped by `lock.assert_locked`, because the object-map lock had a new
owner. Its data write carries no such guard and was applied: the native
observation showed object 4. Its unlock found no lock of its own.

In the last two controls the OSD answered the Go client's `break_lock` and
`lock.lock` with 0 on the header and on the object map. The OSD does not ask
for a blocklist entry or for the owner's absence: that order is the
breaker's duty, and only the claims hold the test to it.

After the first control the takeover action was journaled `uncertain`: its
driver wrote once the compensation had released the locks, waited 30 s for a
proceed signal that never came, closed the image and exited with 1. The
other three left `applied` actions. Each control's cleanup completed and
removed whatever blocklist entries existed (none, none, one and two).

### Earlier runs of this change

These were runs of the uncommitted working tree on the base commit
`17b71649e65b2049b8406049b84b80177b033dfc`, in the same environment and with
the same gate. They are development runs, not acceptance evidence.

- CRC row, first run: failed after the native AcquiredLock with `native RBD
  lock output exceeds its bound or is invalid`. Cause, in this change: the
  fixture's keyed runner strips the key receipt line only from a worker that
  exits with 0, so the killed takeover driver's output still began with that
  line and was not one JSON document. The fixture now strips and checks the
  receipt for the killed status. The first handoff of that run already
  showed six RequestLocks, the blocklist entry and three -108 replies.
- CRC row, second run: passed.
- Secure row, first run: failed at the breaker's release with `breaker
  header watch responder acknowledge header notification ...: ceph: request
  outcome unknown: context canceled`. Cause, in this change: the test
  stopped the breaker's responder as soon as its ReleasedLock notify had
  completed. The OSD completes a notify when it has the acknowledgements,
  and the reply to the breaker's own acknowledgement can arrive after that,
  so the stop canceled a wait that was still open. The test now waits for
  the responder's record of that notification before it stops it. The CRC
  run of the same tree had passed: the two replies race, and that pass
  explains nothing.
- Secure row, second run, and a third CRC run after the transaction check
  was added: passed.
- Before any commit, `TestRBDLockOwnerLossClaims` refused to accept two
  records the first version of the claims let through or could not read: a
  zombie that was already blocklisted before the sequence (the entry counted
  as part of the baseline), and a record without notifications (an index
  out of range). Both are fixed and are cases of the test.

- Commit `c0701808c4d6fb5ad7771339ea8ef3545ba9c1ca`: the four rows passed
  (above) and the four controls failed with the messages given above. In its
  renewed-watch control the failed takeover driver did not exit: it had left
  the image open while the interpreter shut the cluster handle down, and the
  supervisor killed it at the action's 150 s limit. The next commit closes
  the image on that path. On it the same control's driver exited with 1
  after its proceed wait.

On the commits of this change no run of the four rows failed or was repeated
for a different result.

## Product findings

- No wire gap and no product change. The lock class calls including
  `break_lock`, LIST_WATCHERS, the MON command and the watch and notify paths
  use existing primitives. The Go client could perform librbd's whole break
  sequence, and native librbd accepted the result.
- A request from a blocklisted connection ends as a known `OSDError` with
  code -108 and its reply fields, not as an unknown outcome. A watch that the
  OSD expired ends with a DISCONNECT event and `ErrOSDWatchDisconnected`;
  events queued before it stay readable.
- The address nonce is chosen per OSD connection for a client without
  `EnableCephFS`. A blocklist entry made from a lock record therefore fences
  one connection of a Go lock owner. This is recorded, not changed: no
  product API was added for it. See "What the cell does not establish".
- A request that names a map epoch the OSD does not have yet is the caller's
  way to wait for that map, as librados does after `blocklist_add`. The
  breaker's requests after its MON command name the epoch that holds its
  entry and were answered at that epoch.

## Not verified

- A write by a fenced Go owner on a second connection of the same client.
  The read above shows that such a connection is served.
- A primary change or any multi-OSD acting set. Both rows have one OSD. A
  live lock owner across a primary change on three replicated BlueStore
  OSDs is part of
  [RBD_JOURNAL_PRIMARY_CHANGE_VALIDATION.md](RBD_JOURNAL_PRIMARY_CHANGE_VALIDATION.md);
  the loss of a lock owner on more than one OSD is not run.
- A mutation of the old owner that is pending or uncertain when the lock is
  broken. The zombie has nothing in flight when it goes silent, and every
  fenced attempt got a reply. Two requests that the OSD holds unanswered
  through both handoffs are the subject of
  [RBD_LOCK_OWNER_LOSS_PENDING_VALIDATION.md](RBD_LOCK_OWNER_LOSS_PENDING_VALIDATION.md).
- The removal or expiry of a blocklist entry and what the fenced connection
  may do afterwards. The fixture removes both entries only at cleanup.
- A forced break (`RequestLock` with force, `rbd lock break`), a break of a
  lock whose owner is alive, `rbd_blocklist_on_break_lock=false` outside the
  control run, a blocklist expiry other than the default, and an owner that
  comes back between the liveness check and the break.
- A Go breaker without the MON capability for the command, a MON command
  with an uncertain outcome, and a `break_lock` whose reply is lost.
- A dead owner with journaling, snapshots, a clone parent or an invalid
  object map; an owner that died between its object-map update and its data
  write; more than one waiting client.
- The native owner's death by other means than SIGKILL of its process inside
  the oracle container (a paused process, a partitioned host).
- Hosted CI runs of the two rows, arm64, and other Ceph releases.
