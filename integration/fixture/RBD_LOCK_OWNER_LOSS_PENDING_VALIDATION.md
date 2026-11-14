# RBD exclusive-lock owner loss with requests still unanswered

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Issue #12, requirement 3, one cell: lock-owner loss while the owner has
requests the OSD has not answered. The product is unchanged: no product
code, no new primitive and no RBD or lock API. The cell uses the explicit OSD
API of the [lock-owner-loss cell](RBD_LOCK_OWNER_LOSS_VALIDATION.md) and
nothing else. The native driver actions and the fixture's administrative
action are development-only; see [RBD_BASE_FIXTURE.md](RBD_BASE_FIXTURE.md)
(section "Pending lock-owner-loss sequence").

The lock-owner-loss cell takes the lock away from an owner that has nothing
in flight: every request it sent before had been answered, and every request
it sent afterwards got a reply at once. Here the first owner has two
requests on the OSD, unanswered, when it loses the lock. The rest of that
sequence is unchanged and runs in full: the lock is handed over twice
without a release.

It does not establish that a consumer built on this client is a complete RBD
lock implementation.

## What the cell claims

Each claim is judged on its own evidence. The claims of the lock-owner-loss
cell are checked again on this run and are not repeated here.

| Claim | Evidence | Not evidence for it |
| --- | --- | --- |
| Held: the OSD had both requests before the first handoff began and had answered neither when the fixture started to raise the epoch | The OSD's own `dump_ops_in_flight`, read by the fixture: both transactions of the Go owner, each with a receipt time before the test's first observation of that owner | That the two calls had not returned to their callers |
| Unanswered through both handoffs | The two calls were still blocked after the first observation of the Go owner; the second one after the native owner was killed and after the Go breaker took the lock. The OSDMap epoch in every Go observation up to then is below the epoch the requests name | The fixture's later read taken alone |
| Canceled wait: the caller learned nothing and sent nothing in its place | The error and submission that `Request` returned when its context was canceled after the fence was observed; the transactions of everything the owner sent later | Any native observation |
| Answer: once the OSD had the named map it refused the request with the blocklisted code | The raw reply to the second request: -108 as a known server error, for the transaction of its one submission, at an epoch not below the named one | The time it took |
| Effect: neither request changed anything | The native observation taken after the answers: object map bytes, lock outputs and data objects equal to the observation before the epoch was raised; object 4 absent; `rbd object-map check` at the end | The reply codes taken alone; the Go client's own reads |
| Not resent | One caller invocation per request; the three later attempts of the same client take the next three transactions; the OSD reported no operation of that client in flight afterwards | Daemon-side execution counts, which no observer here has |

## Pinned source facts

Read at the reference tag for this change, in addition to those of the
lock-owner-loss record.

- [`src/osd/OSD.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc)
  `ms_fast_dispatch`: an MOSDOp from a connection without
  `CEPH_FEATURE_RESEND_ON_SPLIT` is appended to the session's
  `waiting_on_map` list, and `dispatch_session_waiting` hands the list's
  entries to their PGs in order, stopping at the first one whose map epoch is
  above the OSD's. A request that names an epoch the OSD does not have is
  therefore held, and so is everything that arrives after it on that
  connection. `consume_map` runs the same loop for every such session when a
  new map is installed. The `legacy-v6` Go client does not advertise that
  feature (`internal/session/handshake.go`).
- [`src/osd/PrimaryLogPG.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc)
  `do_request` and `do_op`: a client with that feature is queued on the PG's
  `waiting_for_map` list instead, by client entity. In both cases the
  blocklist is consulted only when the request reaches `do_op`, against the
  map the OSD has then.
- [`src/common/TrackedOp.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/common/TrackedOp.cc)
  and [`src/osd/OpRequest.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OpRequest.cc):
  `ms_fast_dispatch` registers the request with the OSD's operation tracker
  before it is queued, and `dump_ops_in_flight` lists it with its
  description, receipt time, client entity, client address and transaction.
- [`src/mon/OSDMonitor.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/mon/OSDMonitor.cc):
  `osd pool set <pool> nodeep-scrub <bool>` changes one flag of the pool and
  is committed in a new OSDMap epoch.

One fact of this client matters for the sequence. A client holds one
connection per OSD: `OpenOSD` for an OSD it is already connected to returns
that connection (`cephmsgr/osd.go`). While the two requests are held, nothing
the same client sends to that OSD overtakes them. The read "on a new
connection" of the lock-owner-loss cell is therefore possible only after the
lock-bearing connection is closed, and this sequence performs it last.

## Executable boundary

Test: `TestCephRBDLockOwnerLossPendingIntegration` in the `integration`
suite, on the RBD fixture (`CEPH_MSGR_TEST_RBD=1`, single OSD, `legacy-v6`)
under its exact selector. It is the body of
`TestCephRBDLockOwnerLossIntegration` with the pending steps switched on. The
fixture then serves the closed action sequence `setup`, `observe-go-owner`,
`takeover`, `observe-go-broke`, `advance-map`, `observe-answered`, `final`,
and no other RBD test runs. Required rows:

- `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerlosspending`
  (CRC, AES client and service keys, IPv4, host test process, one BlueStore
  OSD);
- `rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerlosspending`
  (secure, AES256K client and service keys, IPv6, container test process, one
  MemStore OSD).

Reproduce the CRC row with:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=1 CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 CEPH_MSGR_TEST_RBD=1 \
CEPH_MSGR_TEST_RUN='^TestCephRBDLockOwnerLossPendingIntegration$' \
sh integration/run.sh
```

Image, clients and the two handoffs are those of the lock-owner-loss record.
Object 4 is the target of both pending requests and of the fenced attempts
and must never exist.

### The two requests

Both are what a lock owner sends for an object nobody wrote, in librbd's
form and order:

- `pending_object_map_update`: `[lock.assert_locked, rbd.object_map_update]`
  on the object map, marking object 4 as existing;
- `pending_data_write`: `[SETALLOCHINT, WRITE]` of 4 KiB at offset 16 KiB of
  data object 4.

The observer reads the OSDMap epoch from the MON (`osd dump`). Both requests
name that epoch plus 8. Each is submitted once, from its own goroutine, on
the connection that took the lock, after the zombie acknowledged its own
AcquiredLock and before it goes silent.

A request that waits for a map is one of the ways a request stays
unanswered; a busy or recovering PG is another. This one is chosen because
the test controls when it ends: the OSD answers when the epoch arrives, and
the fixture raises the epoch on request.

### Sequence

Steps of the lock-owner-loss sequence are named only; the steps added or
moved here are described.

| Step | What happens | Checked |
| --- | --- | --- |
| `setup`, zombie lock | As in the lock-owner-loss sequence | |
| Pending submitted | The epoch is read, and the two requests are sent, naming that epoch plus 8 | |
| `observe-go-owner` (native), Go observation | As before | Stage `go-owner`; both calls still blocked, the Go observation's epoch below the named one |
| `takeover` starts, request while listed, native acquired | As before: native librbd asks six times, blocklists the zombie's address and breaks both locks | |
| Pending canceled | After the Go observer read the zombie's blocklist entry, the context of the first call is canceled | `OutcomeUnknownError` with cause `context.Canceled` and its submission; no reply fields |
| Zombie watch, fresh read, `takeover` ends | As before, without the fenced attempts and without the read on a new connection. The native owner observes and is killed | Stage `takeover`; the second call still blocked; nothing else sent by the zombie |
| Go break, Go write, `observe-go-broke` | As before: the breaker waits for the dead owner's watch, blocklists, breaks, locks, updates the object map and writes object 2 | Stage `go-break`; the second call still blocked; every Go observation's epoch below the named one |
| `advance-map` (fixture) | The fixture reads the OSD's operations in flight, raises the OSDMap epoch by at least 8 with `osd pool set <owned pool> nodeep-scrub true` and `false` in turn, and reads the operations in flight again until those it saw are gone | The receipt |
| Pending answered | The second call returns | Raw outcome kept |
| Fenced attempts | The zombie sends, on the same connection, each once: the guarded object-map update for object 4, a data write to object 4, and `lock.unlock` of the header | Raw outcomes kept |
| New connection | The zombie closes that connection, opens a new one and sends one `lock.get_info` | Raw outcome kept |
| `observe-answered` (native) | Read-only observation with `osd blocklist ls` | Stage `answered`, below |
| Go release, `final`, end | As before | Stage `final` |

The record is judged after `observe-go-owner`, after `takeover`, after
`observe-go-broke`, after `observe-answered` and at the end. A contradiction
at one stage stops the run there. After a failure the deferred compensation
cancels both waits, closes the lock-bearing connection and lets whoever may
still hold a lock unlock it once on another connection. It never repeats a
pending request.

### Claims as the test checks them

In addition to the claims of the lock-owner-loss record, with the fenced
attempts and the read on a new connection judged at stage `answered`:

- **`go-owner`.** The record names the epoch read before sending and an
  epoch 8 above it. Two requests exist, in the roles given above, neither
  with an outcome. Both calls were blocked after the native and Go
  observations of the Go owner. The Go observation's epoch is not below the
  epoch read before sending and is below the named one.
- **`takeover`.** The first request ended as one unknown outcome of a
  canceled wait: class `uncertain`, `OutcomeUnknownError`, canceled, no reply
  transaction or code, one invocation, a submission of the zombie's GlobalID
  at the named epoch whose transaction is one of the two directly after the
  zombie's last request before its silence. The cancellation is later than
  the Go observation that holds the zombie's blocklist entry. The second
  call was blocked after the native owner was killed and has no outcome. The
  epochs at the first native RequestLock and after the native acquisition
  are below the named one. No fenced attempt and no read on a new connection
  exist yet. The native observation before the kill shows object map
  `[1 1 0 0 0]` and objects 0 and 1 only, as in the lock-owner-loss cell.
- **`go-break`.** The second call was blocked after the Go break and has no
  outcome. The epochs of the four Go observations of that stage are below
  the named one.
- **`answered`, epoch.** The fixture started at an epoch not below the last
  Go observation's and below the named one, and ended at or above the named
  one and its own target, with the pool flag clear and at least one command.
- **`answered`, held by the OSD.** The operations in flight before the first
  command include, for each of the two requests, one operation of
  `client.<zombie GlobalID>` with the request's transaction, received by the
  OSD before the test's first Go observation of the Go owner. The operations
  in flight after the last command include none of that client.
- **`answered`, answer.** The second request has one invocation, class
  `refused`, code -108 and error code -108, is not an unknown outcome and
  was not canceled; the reply's transaction is its submission's, which is
  the other of the two transactions; it was submitted at the named epoch and
  answered at an epoch not below it.
- **`answered`, afterwards.** The three attempts are the next three
  transactions, each refused with -108 for its own transaction. The read on
  a new connection is the transaction after them, was answered 0 and names
  the breaker as lock owner. The native observation shows both locks held by
  the breaker, the watchers of the observer and the breaker, the object map
  `[1 1 1 0 0]`, the two blocklist entries, and the object map bytes, both
  `lock.get_info` outputs and every data object digest equal to
  `observe-go-broke`; objects 0 to 2 exist, object 4 does not, flags are 0
  and the image read is the model with the Go extent.
- **`final`.** As in the lock-owner-loss record: object 4 absent, the object
  map `[1 1 1 1 0]`, `rbd object-map check` with 0 and no output.

### What the cell does not establish

- **Other reasons for a request to stay unanswered.** The requests wait for
  a map. A request held by a PG that is peering, recovering or backing off,
  and a request the OSD has started to execute when the owner is fenced, are
  not produced.
- **That the canceled request was refused.** Its caller has no reply. The
  OSD no longer lists it afterwards and the native state did not change;
  what the OSD answered to it is not read.
- **Which of the two requests the OSD received first.** They are sent from
  two goroutines. The claims accept either order of their transactions.
- **A pending request that is applied.** With the blocklist in place both
  are refused. What a held write does when the owner was not blocklisted is
  shown only by the control below.
- **Fencing of a Go client as a whole.** As in the lock-owner-loss record:
  the blocklist entry is the address of the one connection that took the
  lock. Both pending requests were sent on that connection.

## Daemon-free checks

These establish no native result.

- `TestRBDLockOwnerLossPendingClaims` (cluster-free, in the Basic CI list):
  a consistent record passes at each of the five stages, and a record of one
  stage is refused at every other. A pending run without its pending record
  and an ordinary run with one are refused at every stage. 79 contradictory
  records are refused, each with every other part of the record intact, at
  the stage they belong to and again in a complete record. Examples:
  requests that name the current epoch; a cluster already at the named
  epoch; a call that was not seen blocked; a canceled request that was
  answered, applied, sent later, sent twice or canceled before the fence; a
  second request answered before the kill or during the break; an epoch
  that was already there or stopped short; a pool flag left set; an OSD that
  did not hold one of the requests, held it for another client or received
  it after the handoff began; an operation still in flight afterwards; an
  answer that is a success, unknown, another code, of another transaction or
  below the named epoch; attempts before the answers or a request between
  them; object 4, the object map, a lock or the image changed by the
  answers.
- `TestRBDLockOwnerLossClaims` (cluster-free): the claims shared with the
  ordinary sequence, 140 contradictory records.
- `integration/fixture/rbd_lock_test.go`: the pending sequence is the
  lock-owner-loss sequence with the two new actions before the final one,
  admitted only under its exact selector and refused by both other
  sequences; `advance-map` runs no driver; the epoch is raised until it is
  far enough and the flag is clear, within a bound; a set flag, an unread
  map, a refused command, an unchanged flag, an epoch that never moves and
  unread operations are errors; both samples are answers of the one OSD
  pinned at the start, and an inventory that is empty, larger or names
  another OSD at either sample is an error; operations left in flight at the
  bound are reported; `dump_ops_in_flight` decodes to client operations
  only, within a bound, and an absent or null `ops` array is refused beside
  the valid empty one.

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

Source commit `12a2a15e5756e51d9de8e3008efb8721ecc2ef9e`, 2026-11-13 (UTC):
the merge of the main branch into the last commit of this change that
touches code or the catalog, `29b110768efd592f8d227104a38964c54bb5d516`. The
commits after it in the same pull request change documentation only.

| Scenario ID | Gate | Test duration | Row wall time |
| --- | --- | --- | --- |
| `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerlosspending` | pass, 1 expected test, no problem | 73.02 s | 160 s |
| `rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerlosspending` | pass, 1 expected test, no problem | 70.73 s | 156 s |
| `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerloss` | pass, 1 expected test, no problem | 62.76 s | 142 s |
| `rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerloss` | pass, 1 expected test, no problem | 56.98 s | 135 s |
| `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-37bcdb` | pass, 5 expected tests, no problem | base 5.34 s, clone 3.14 s, journal 2.51 s, exclusive-lock 10.98 s, write 2.39 s | 100 s |
| `rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-suite-37bcdb` | pass, 5 expected tests, no problem | base 5.32 s, clone 2.69 s, journal 2.26 s, exclusive-lock 10.97 s, write 2.23 s | 98 s |

The two lock-owner-loss rows were run because the body of their test and its
claims changed. The two broad RBD rows were run because the lock control's
action list, output decoder and profile admission changed. Each of the six
runs passed at its first attempt on that commit. The same six rows had
passed at their first attempt on `29b1107` before the merge (73.14 s and
66.81 s for the two new rows), with the same values.

### Observed values

Client GlobalIDs, addresses, the image ID and SHA-256 values differ per
fixture and are in the run's `rbd-lock-owner-loss-pending-results.json`.
The values of the two handoffs are those of the lock-owner-loss record.

| Value | CRC/AES IPv4 host BlueStore | secure/AES256K IPv6 container MemStore |
| --- | --- | --- |
| Epoch read before sending / named by both requests | 24 / 32 | 25 / 33 |
| Zombie requests | transactions 2 to 9 before the two requests; 10 the object-map update, 11 the data write; 12, 13, 14 the three attempts; 15 the read on the new connection | same |
| Go observer's MON `osd dump` epoch | 24 with the Go owner and at the first RequestLock, 26 with the zombie's entry, 27 with both entries and after the Go write | 25, 25, 27, 28 |
| Canceled wait | 82 microseconds after the Go observation that holds the zombie's entry; `OutcomeUnknownError`, cause `context canceled`, submission transaction 10 at map epoch 32, no reply fields | 70 microseconds; same with epoch 33 |
| OSD operations in flight before the first command | two, both of the zombie: transactions 10 and 11, received 0.2 s before the first Go observation of the Go owner, age 62.4 s, description ending in `e32`, flag point `no flag points reached` | same; age 61.6 s, `e33` |
| Fixture commands | 8 (`nodeep-scrub` true and false four times), one epoch each: 27 to 35 in 10.1 s | 8, 28 to 36 in 8.7 s |
| OSD operations in flight after the last command | none | none |
| Answer to the data write | -108, transaction 11, request epoch 32, reply epoch 32, per-op results 0 and 0, 41 s after the cancellation | same with epoch 33, 39 s |
| Fenced `[R lock.assert_locked, W rbd.object_map_update]`, `[SETALLOCHINT, WRITE]`, `lock.unlock` | -108 each, reply epoch 35 | -108 each, reply epoch 36 |
| Same client, new connection, `lock.get_info` | 0, names the breaker | same |
| Native observation after the answers | both locks the breaker's, object map `[1 1 1 0 0]`, objects 0 to 2, object 4 absent, flags 0, two blocklist entries; bytes equal to `observe-go-broke` | same |
| Native RequestLocks, zombie watch events, kill to unlisted | 6; the same 6 and DISCONNECT; 30.11 s (30 polls) | 6; same; 30.06 s (30 polls) |
| `rbd object-map check` | exit 0, empty stdout and stderr, 50 ms | 34 ms |
| Observer notifications and pings | 14; 14, no error | 14; 14, no error |
| Native action durations (ms): setup, observe-go-owner, takeover, observe-go-broke, observe-answered, final | 202, 118, 30463, 133, 114, 212 | 127, 124, 30229, 120, 125, 164 |

The OSD answered the data write at the first epoch that satisfied it, the
fifth of the eight the fixture produced. Between the epoch read before
sending and the fixture's first command the map advanced by three epochs in
both runs: two by the time the zombie was blocklisted and one with the
breaker's entry. The distance of 8 left five.

Every native driver process ended with empty stderr.

### Cleanup

Both rows: `rbd-base-owned.json` holds `native-lock-setup`,
`-observe-go-owner` (`read_only`), `-takeover`, `-observe-go-broke`
(`read_only`), `-advance-map`, `-observe-answered` (`read_only`) and
`-final`, all `applied`. The advance-map receipt ends with the pool flag
clear. `rbd-base-cleanup.json` lists the two lock-owner addresses and the two
blocklist entries it removed and verified absent, then reports the pool
removed, natively absent, both identities removed and `completed: true`.
After each run no container with the provider's label was left on the host.
No independent final network inventory is retained.

### Controls

The checks were shown to fail on wrong behavior by three runs with a
temporary edit, on the tree of `29b1107`, before the merge. They were not
repeated on the merge commit, which changes no RBD file. Each edit was
applied only to the uploaded copy and never committed. Each is one run in one
profile.

| Control | Profile | Edit | Result |
| --- | --- | --- | --- |
| Requests that are not held | CRC row | Both requests name the epoch read before sending instead of that epoch plus 8 | Gate verdict fail after 0.45 s. The test failed at stage `go-owner` with three lines: the native and the Go observation show the object map `[1 0 0 0 1]`, and the two calls were not both blocked. The OSD had answered both at once, as it answers any request of a lock owner. The compensation released both locks (0 and 0) |
| Canceled request sent again | secure row | After the cancellation the zombie sends the canceled request once more on the same connection | Gate verdict fail. The test failed at stage `answered` with four lines: the three attempts have transactions 13, 14 and 15 where 12, 13 and 14 are required, and the read on the new connection 16 where 15 is required. The OSD's operations in flight before the first command were three: transactions 12, 10 and 11 |
| No blocklist on the native break | CRC row | The native driver runs with `rbd_blocklist_on_break_lock=false`, and the record is judged only from stage `answered` on, so that the run reaches the answers | Gate verdict fail. The test failed at stage `answered` with fourteen lines. Eight say that the zombie's address is missing from the blocklist in every observation from the native acquisition on. The held data write was answered 0 at the named epoch, not -108. The later attempts: object-map update -16 (per-op -16 and 0), data write 0, unlock -2. The native observation after the answers differs from `observe-go-broke`, and its image differs from the model: object 4 exists |

The third control shows what the blocklist entry is for when a request is
held. The stale owner's data write had been on the OSD since before the
first handoff. It carries no lock assertion. When the OSD reached it, two
handoffs later, the lock belonged to the breaker and the write was applied
all the same. With the entry in place the same request is refused. The
canceled object-map update of that run is guarded by `lock.assert_locked`;
the object map did not change.

In the second control the resent request was held behind the two others and
was listed by the OSD with them. Only the transaction numbers of the later
requests show the caller that sent it; no reply distinguishes the run.

After the first control the run ended before the takeover. The other two
left `applied` actions up to `observe-answered`; the deferred compensation
released the breaker's locks (0 and 0). Each control's cleanup completed and
removed whatever blocklist entries existed (none, two and one).

### Earlier runs of this change

These were runs of the uncommitted working tree on the base commit
`b5d08602a17365f03bd1e0b36f7b48fa401a8a75`, in the same environment and with
the same gate. They are development runs, not acceptance evidence.

- CRC row, first and second run: the test did not finish. The native
  takeover driver ended with `proceed signal 1 missing` after its 30 s wait,
  and the test process stayed blocked in the zombie's read "on a new
  connection". The first run was ended after seven minutes with a signal
  that prints the test's goroutines, the second by the row's time limit.
  Cause, in this change:
  the first version of the sequence performed that read while the two
  requests were held and the lock-bearing connection was open. `OpenOSD`
  returned that same connection, and the OSD queued the read behind the
  held requests. The second run was started to read the OSD's operations in
  flight during the hang: three operations of the zombie, the two requests
  and the read, all with the address of the lock-bearing connection. The
  sequence now performs that read last, after closing the connection. The
  second of these runs left its diagnostics directory behind on the host,
  because the fixture wrote to it after the row's time limit; it was removed
  by hand afterwards.
- CRC row, third run, and secure row, first run, with the reordered
  sequence: passed (72.16 s and 70.74 s) with the values above.

On the commits of this change no run of the six rows failed or was repeated
for a different result. The rows were run twice because the main branch was
merged in between.

## Product findings

- No wire gap and no product change.
- A request that names an OSDMap epoch the OSD does not have is held by the
  OSD, with everything the same client sends to that OSD after it, until the
  map arrives. The caller's wait ends only by its own context. A canceled
  wait returns `OutcomeUnknownError` with the submission; the connection
  stays usable, and the reply to the other held request arrived on it
  later.
- A held request of a blocklisted connection ends as a known `OSDError` with
  code -108 once the OSD reaches it, as a request sent after the fence does.
- A client holds one connection per OSD. While a request is held there, a
  consumer cannot reach that OSD with the same client on another
  connection without closing the first, which abandons the held requests'
  replies.
- The address nonce is chosen per OSD connection, as recorded in the
  lock-owner-loss record. Both held requests were sent on the connection
  that took the lock, which is the one the blocklist entry names.

## Not verified

- A request held for another reason than its map epoch, and a request the
  OSD is executing when its owner is fenced.
- What the OSD answered to the canceled request.
- A held request sent on another connection of the owner than the one that
  took the lock. With the per-connection nonce it would not be covered by
  the blocklist entry made from the lock record.
- A held request of the owner with journaling, with more than one OSD or
  across a primary change, and held requests of the killed native owner.
- An epoch that reaches the named one before the owner is fenced by itself.
  The distance of 8 was three more than the cluster used in these runs; a
  run in which the map advanced further would fail at an earlier stage and
  say so.
- A forced break, `rbd_blocklist_on_break_lock=false` outside the control
  run, and the removal or expiry of a blocklist entry while a request is
  held.
- Hosted CI runs of the two rows, arm64, and other Ceph releases.
