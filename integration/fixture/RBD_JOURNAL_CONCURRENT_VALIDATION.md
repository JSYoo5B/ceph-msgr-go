# RBD journal concurrent appenders acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Issue #12, work item 2. The
product is unchanged: no product code, no new primitive and no exported RBD,
image or journal API. The session uses only the existing explicit OSD API
(`OpenOSD`, `Request` with `OSDCall`, `OSDStat`, `OSDRead`, `OSDWrite`,
`OSDSetAllocHint`, `WatchObject`, `Next`, `Acknowledge`, `Ping`, `Unwatch`,
`NotifyObject`). The cls_journal and cls_lock encodings, the Entry framing,
librbd's `EventEntry`, `TagData` and watch-notify payloads, the commit
rule, the tag allocation and the lock handoff are test-only consumer code in
`integration/rbd_journal_test.go` (subtest `concurrent_appenders`), reusing
the journal and exclusive-lock acceptances' helpers. The native driver
action `concurrent` is development-only; see
[RBD_BASE_FIXTURE.md](RBD_BASE_FIXTURE.md) (section "Journaling image and
closed journal control").

This extends the [journal acceptance](RBD_JOURNAL_VALIDATION.md), whose Go
appends ran only while the image was closed natively, and the
[exclusive-lock acceptance](RBD_LOCK_VALIDATION.md), whose image had no
journal.

## What "concurrent" means here

librbd lets only the holder of the image's exclusive lock append to the
image journal. Two lock owners appending at the same time is not a valid
librbd configuration, so it was not attempted. In the tested session both
clients are live. Native librbd keeps the image open read-write for the
whole session. Its aio writes are issued and queued (their journal appends
waiting for the lock, as its RequestLock to Go shows) while Go owns the lock
and appends. The lock moves native
-> Go -> native only through librbd's own notifications. Go submits two of
its appends from two goroutines started together. The API exposes no
submission event, so whether both were on the wire at the same time was not
observed and is not claimed.

## Test shape

The subtest runs last in `TestCephRBDJournalIntegration`, after the replay
leaf, on the same journaling image (features 69, order 16, journal order 24,
splay width 4, data objects in the image's pool). The existing RBD catalog
rows already select it
(`^TestCephRBD(Base|Write|ExclusiveLock|Clone|Journal)Integration$`). The
catalog is unchanged. The native control is one `concurrent` action of the
closed journal sequence. It publishes phases in `rbd-journal-state.json` and
waits for proceed signals.

| Step | Checked behavior |
| --- | --- |
| Native owner | The driver opens the image read-write and writes 24 to 28 KiB and 152 to 156 KiB synchronously. Phase `owner`: `is_exclusive_lock_owner()` true, exactly one header watcher (the native instance, whose GlobalID and watch handle are its `ClientId`) |
| Handoff to Go | Go watches `rbd_header.<id>` with a responder that answers as librbd's `ImageWatcher` does. Go notifies `RequestLock(ClientId{Go}, force=false)` with a 5 s timeout: two acknowledgements, Go's own empty and native `ResponseMessage(0)`. Native librbd then sends `ReleasedLock(ClientId{native})` |
| Go acquires | `lock.lock` (exclusive, `auto <Go watch handle>`, `internal`); `lock.get_info` names Go; `AcquiredLock(ClientId{Go})` acknowledged empty by both watchers |
| Journal open by Go | The immutable and mutable metadata vectors, `get_client("")`, `tag_list` and STAT+READ of journal objects 0 to 7. Exactly one new tag (the native owner's) with mirror UUID `""` and its predecessor equal to the image client's commit front after the replay. The native owner's entries match its writes. Every other entry equals the replay observation, and object set 1 is absent. The image client's commit position equals the commit rule over the owner's entries, so nothing is left to replay. The test refuses to continue otherwise (Go never replays) |
| Go tag and entry 0 | `get_next_tag_tid`, then `tag_create(next, class 0, TagData{"" , {"" , true, front}})`. The same `tag_create` again is refused with -17 (`EEXIST`) and is recorded in the outcome ledger as `refused`. Then an empty journal-header notify (`JournalMetadata::C_NotifyUpdate`). Entry 0 (AioWrite 48 to 56 KiB) in librbd's `[journal.append(2^24, entry)]` form |
| Native writes queued | Proceed signal 1: the driver issues three `aio_write`s (52 to 56 KiB, overlapping the second half of Go's first extent, 200 to 204 KiB and 232 to 236 KiB) and publishes `issued` (not owner). Go's responder receives native `RequestLock(ClientId{native}, force=false)` and answers `ResponseMessage(0)` as owner |
| Go appends from two goroutines | Entries 1 (112 to 116 KiB) and 2 (176 to 180 KiB) are submitted from two goroutines started together, each `[journal.append]`. Each reply carries its own transaction, ONDISK and a user version. Every journal object Go wrote then reads as its bytes at lock acquisition followed by Go's entry. Wire overlap of the two requests is not observed |
| Go applies and commits | Each event is applied as `[SETALLOCHINT(65536, 65536) FAILOK, WRITE]` without SnapContext. `client_commit("", ...)` writes the commit rule over Go's entries, and `get_client` returns it. A second empty journal-header notify follows |
| Release to native | `lock.unlock`, then `ReleasedLock(ClientId{Go})` acknowledged empty by both watchers. Native librbd sends `AcquiredLock(ClientId{native})`. Phase `completed`: owner again, all three aio return values 0 |
| Close | Proceed signal 2: the driver closes the image; Go receives `ReleasedLock(ClientId{native})`, stops its responder and unwatches. Proceed signal 3: the observation runs |
| Native oracle | librados `exec` of the journal header list equals Go's per-op outputs byte for byte; librados reads of journal objects 0 to 7 equal Go's; `rbd journal info` and `status` equal Go's decoding. Tags: the owner's, Go's (data byte-equal to what Go created) and the native reacquisition tag, whose predecessor is Go's last entry. The three native entries each match one aio write (offset, length, data pattern, timestamp within a second of the native call). The image client's final position equals the commit rule over the native entries after Go's commit. Two clients remain (image and oracle, the oracle at an empty position). No lockers or watchers remain. `rbd journal inspect --verbose` replays every entry in tag and entry order with 0 errors. The librbd read of the whole image equals the model: native owner writes, then Go's writes, then the native aio writes (so 52 to 56 KiB holds native data) |

Every Go request is recorded with its inputs, transaction, submission,
GlobalID, PG, map epoch, flags and versions. Every Go mutation of the
session (lock, unlock, tag creation, appends, data writes, commit and the
compensating unlock) also enters an outcome ledger with its transaction,
submission, code and error. The entry is written right after the raw record
and before any assertion, so a failing mutation keeps its classified
outcome. The classes are `applied` (code 0), `refused` (an OSD error reply,
or the negative code the test asked for) and `uncertain`
(`OutcomeUnknownError`, or conservatively any other error). A mutation of
any class is never resent. The live session exercises `applied` and
`refused`. `uncertain` is covered only by the unit test
`TestRBDMutationOutcomeLedger`, because a lost reply cannot be induced
safely on the live journal. After a failure the
deferred compensation unlocks the header (never relocking or reappending),
notifies `ReleasedLock` and unwatches. The published state, Go's notifies,
the notifications Go received and the outcomes are written into
`rbd-journal-results.json`, which is on the diagnostics allowlist. The
fixture receipt `rbd-journal-concurrent.json` and the state file are not on
that allowlist.

## Results

Exact-SHA native runs on GitHub Actions (`ubuntu-24.04` runner image),
requested with `/ci run group:rbd` on PR #45. Image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Topology: three MON, two MGR, one OSD and the native oracle container, the
`legacy-v6` OSD profile.

| Run | Tested commit | Scenario | Result |
| --- | --- | --- | --- |
| [39811417103](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39811417103) | `101862d` | both RBD rows | failed: guarded appends refused (see "Development observation") |
| [39812345146](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39812345146) | `d9bc548` | both RBD rows | passed |
| [39813062712](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39813062712) | `5561555` | both RBD rows | passed |
| [39823842597](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39823842597) | `9da73a0` | both RBD rows | passed |

The first three commits were on the branch before it was rebased onto
`3de2098`. They had no outcome ledger, and their record described the
goroutine appends as overlapping. `9da73a0` is the rebased branch with the
ledger and the narrowed claim, and it is the tested source of this record.
Later commits change only documentation. The values below are from run
39823842597 (commit `9da73a09359d616f90b65013e691c4c3336c77e5`). The two
rows are
`rbd.aes.ip4.crc.host.osd-store-bluestore...` (CRC, AES client and service
keys, IPv4, host runtime, single BlueStore OSD) and
`rbd.aes256k.ip6.container.osd-store-memstore...` (secure, AES256K keys,
IPv6, container runtime, single MemStore OSD). In both rows the base, clone,
journal (6/6 subtests), exclusive-lock and write tests passed with no skips.
`concurrent_appenders` took 0.62 s and 0.51 s.

| Value | CRC/AES IPv4 BlueStore | secure/AES256K IPv6 MemStore |
| --- | --- | --- |
| Tags (tid: predecessor tag, entry) | owner 5: (3, 1); Go 6: (5, 1); native reacquisition 7: (6, 2); next tag tid 8 | same |
| Owner entries (tag/entry at object+offset) | 5/0 at 0+20789, 5/1 at 1+16615 | same |
| Go entries | 6/0 at 0+24962, 6/1 at 1+20788, 6/2 at 2+4251 | same |
| Native reacquisition entries | 7/0 at 0+33231, 7/1 at 1+24961, 7/2 at 2+8424 | same |
| Image client commit at Go's lock | `[(1,5,1), (0,5,0), (2,2,2), (3,1,3)]` (object, tag, entry) | same |
| After Go's commit | `[(2,6,2), (1,6,1), (0,6,0), (3,1,3)]` | same |
| Final | `[(2,7,2), (1,7,1), (0,7,0), (3,1,3)]` | same |
| `rbd journal inspect` | 18 entries, 0 errors | same |
| Go append transactions | entry 0: 136; entries 1 and 2 (goroutines): 139 and 138 | entry 0: 136; entries 1 and 2: 138 and 139 |
| Header notifications to Go's image watch | Go's own: RequestLock, AcquiredLock, ReleasedLock; native: ReleasedLock, RequestLock (exactly one), AcquiredLock, ReleasedLock | same |
| Go lock held | 59.8 ms | 40.5 ms |
| Native aio return values | 0, 0, 0 | same |
| Native close | 4.7 ms | 3.0 ms |

Run 39813062712 on `5561555` logged the same tags, entry positions, commit
positions and notification sequence. Run 39812345146 printed no summary
lines. Diagnostics are archived
only for failed scenarios, so the passing runs' `rbd-journal-results.json`
files were not retrieved. The table comes from the test's own log lines.

Local checks on the tested tree: `go build ./...`, `go vet ./...`,
`go test -race ./...` (unit tests only; integration tests skip without the
fixture), `go -C integration/fixture vet ./...` and `test -race ./...`, and
`python3 -m unittest discover -s tools -p 'test_*.py'` passed.

## Development observation

The first run (`101862d`) sent entries 1 and 2 as
`[lock.assert_locked(rbd_lock, exclusive, <Go cookie>, internal),
journal.append]` to their journal data objects. Both returned aggregate -16
with per-op results `[-16, 0]` even with Go's own cookie, and both were
recorded as server refusals and not resent. Go then unlocked and notified
`ReleasedLock` from its compensation. The image's lock lives on
`rbd_header.<id>`, and a class method acts only on its request's object, so
an assertion on a journal data object finds no lock. **A journal append
cannot carry an assertion of the image's exclusive lock in the same vector;
cls_journal's `append` checks no ownership.** Ownership of journal appends is
enforced by the client lock protocol alone. That run's control loop stopped
after the failed action, so the exclusive-lock and write tests reported a
cascading fixture error. The fixed test sends librbd's unguarded form.

## Product findings

- No wire gap. Every message of the session (lock class calls, journal class
  calls, appends, data writes with an allocation hint, watch, notify,
  acknowledgement and ping) uses existing primitives, so no product
  primitive was added.
- Two mutations submitted from concurrent goroutines on one OSD connection
  received distinct transactions and complete reply identity. Whether they
  overlapped on the wire was not observed.

## Not verified

- Two lock owners appending at once, or Go appending without holding the
  lock (not valid librbd behavior and not attempted).
- That the OSD accepted two Go appends that were simultaneously on the wire:
  the goroutine submissions are not observed to overlap.
- Lost notifications or acknowledgements, lock-owner loss, blocklisting
  during handoff, a reply lost after an append (an `uncertain` outcome was
  never observed live and is unit-tested only) and primary changes. These are issue #12 items 3 and 4.
- Go replaying uncommitted entries left by another owner: the test requires
  a fully committed journal at acquisition.
- Native librbd replaying a Go entry during a live reacquisition. Go
  committed its entries before releasing, so native replay found nothing.
  Native replay of Go entries is covered only for a closed image
  ([RBD_JOURNAL_VALIDATION.md](RBD_JOURNAL_VALIDATION.md)).
- More than one handoff cycle, object-set advance, journal trimming,
  events other than AioWrite, a separate journal pool, replicated or
  erasure-coded multi-OSD pools and other Ceph releases.
- RBD mirroring and an equivalent Go RBD client.
