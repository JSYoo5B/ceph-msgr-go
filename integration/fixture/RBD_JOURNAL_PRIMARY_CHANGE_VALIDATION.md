# RBD journal append across a change of the acting primary

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Issue #12, requirement 3, one cell: a primary change on replicated BlueStore
while a Go client holds the image's exclusive lock and the outcome of its
journal append is unknown. The product is unchanged: no product code, no new
primitive and no RBD, image or journal API. The cell uses the explicit OSD
API of the [lost-append cell](RBD_JOURNAL_LOST_APPEND_VALIDATION.md)
(`OpenOSD`, `Request` with `OSDCall`, `OSDStat` and `OSDRead`, `WatchObject`,
`ReconnectWatch`, `Unwatch`, `Close`), `OSDListWatchers` and one existing
field of `OSDRequest` that the lost-append cell did not use: `Resubmit` with
`RetryAttempt` ([RESUBMIT_CONTRACT.md](../../internal/osd/RESUBMIT_CONTRACT.md),
[OSD_RESUBMIT_VALIDATION.md](OSD_RESUBMIT_VALIDATION.md)). The journal and
lock encodings are the test-only consumer code of
`integration/rbd_journal_test.go`. The native driver actions and the
fixture's administrative action are development-only; see
[RBD_BASE_FIXTURE.md](RBD_BASE_FIXTURE.md) (section "Primary-change
sequence").

The lost-append cell ran on one OSD. Its caller did nothing with the
uncertain append, and its record listed two things as not verified: a new
primary, and an explicit resubmission of the append under its original
transaction. This cell does both, on three OSDs.

It does not establish that a consumer built on this client is a complete RBD
journal or lock implementation.

## What the cell claims

The four claims of the lost-append record (application, outcome, order,
transmission) are checked again on this run, with two transmissions instead
of one. Added here, each on its own evidence:

| Claim | Evidence | Not evidence for it |
| --- | --- | --- |
| Primary change: the acting primary of the PG that holds the image header, the journal header and the journal object moved to another member of its acting set | `ceph osd map` of the three objects, read by the fixture before and after its `osd out` command; the same placements resolved by the test through the Go client's MON commands | That the Go client's later requests were answered |
| State across the change: the entry, the lock and the commit position are where they were | A native observation through the new primary: journal object bytes, lock owner and cookie, watcher list, image read, `rbd journal status` | The Go client's reads on the new primary |
| Resubmission: the new primary answered the original transaction without running the append again | The raw reply to the one resubmission (0, the original transaction, retry attempt 1, ONDISK, the object's unchanged user version), the object's user version read before and after, and a native observation after it with every journal object byte-identical to the checkpoint | The reply taken alone: an OSD that ran the append again would also answer 0 |
| Lock continuity: the owner kept the lock and its watch across the change | Native lock and watcher lists before and after; the new primary's `lock.get_info` and LIST_WATCHERS as the subject read them after reattaching its watch | The reattachment's own reply |

The native replay at the end is the last observation: native librbd applied
the entry once and the journal bytes did not change.

## Pinned source facts

Read at the reference tag for this change, in addition to those of the
lost-append record.

- [`src/osd/OSDMap.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSDMap.cc)
  `_pg_to_raw_osds`, `_raw_to_up_osds`, `_get_temp_osds`: an OSD that is
  marked out has weight 0 and CRUSH no longer selects it, so it leaves the
  PG's up set; the first member of the up set is the up primary. A `pg_temp`
  entry replaces the acting set, and its first member is the acting primary.
- [`src/osd/PeeringState.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc)
  `choose_acting`, `calc_replicated_acting`: the new primary wants the up
  set and, while that is smaller than the pool size, further OSDs that hold
  the PG's data. The OSD that was marked out is still running and
  qualifies, so the primary asks the MON for a `pg_temp` entry that lists it
  last.
- [`src/osd/OSDMap.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSDMap.cc)
  `get_features`: an OSDMap in which any OSD has a non-default primary
  affinity requires `CEPH_FEATURE_OSD_PRIMARY_AFFINITY` of every client.
  This is why the primary is not moved with `osd primary-affinity`; see
  "Earlier runs of this change".
- [`src/osd/PrimaryLogPG.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc)
  `do_op`: a write whose request ID is found in the PG log is not executed;
  the OSD answers with the logged result and versions
  (`check_in_progress_op`, `already_complete`).
  [`src/osd/osd_types.h`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/osd_types.h)
  `osd_reqid_t`: the request ID is the client entity, its incarnation and the
  transaction. The PG log is replicated with each write, so every member of
  the acting set has it.
- [`src/osd/PG.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PG.cc)
  `can_discard_op`: a request whose map epoch is older than the PG's
  `same_primary_since` is dropped without an answer. Every request after the
  change therefore names a current epoch.
- [`src/osd/PrimaryLogPG.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc)
  `do_osd_ops` (`CEPH_OSD_OP_WATCH`, `CEPH_OSD_OP_LIST_WATCHERS`): the watch
  record in the object's metadata is written by a WATCH registration and is
  replicated with the object. A RECONNECT requires that record and does not
  rewrite it, and LIST_WATCHERS prints the records. A reattached watch is
  therefore listed under the address it was registered from.
  `populate_obc_watchers`: a new primary starts every recorded watch as
  disconnected, with the watch timeout counted from the PG's activation.
- [`src/osdc/Objecter.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osdc/Objecter.cc):
  a native client whose target changes resends its pending requests with
  their original transaction and a raised attempt. The Go client resends
  nothing on its own; the one resubmission here is the caller's explicit
  request.

## Executable boundary

Test: `TestCephRBDJournalPrimaryChangeIntegration` in the `integration`
suite. It is the body of `TestCephRBDJournalLostAppendReplyIntegration` with
the primary-change steps switched on. It runs on the RBD fixture with three
BlueStore OSDs (`CEPH_MSGR_TEST_RBD=1`, `CEPH_MSGR_TEST_OSD_REPLICAS=3`,
`CEPH_MSGR_TEST_OSD_STORE=bluestore`, `legacy-v6`) under its exact selector,
the only RBD selection that topology admits. The owned pool then has three
replicas, `min_size` 2 and one PG. The fixture serves the closed action
sequence `setup`, `observe-locked`, `checkpoint`, `observe-recorded`,
`change-primary`, `observe-resubmitted`, `replay`. Required rows:

- `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalprimarychange`
  (CRC, AES client and service keys, IPv4, host test process);
- `rbd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalprimarychange`
  (secure, AES256K client and service keys, IPv6, container test process).

Reproduce the CRC row with:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 CEPH_MSGR_TEST_RBD=1 \
CEPH_MSGR_TEST_RUN='^TestCephRBDJournalPrimaryChangeIntegration$' \
sh integration/run.sh
```

Clients, the owned entry and the reply gate are those of the lost-append
record. The subject keeps one connection per OSD it talks to; with one PG
that is one connection to the old primary and, after it is closed, one to
the new primary.

### Why one PG

The image header (lock and watch), the journal header (tags and commit
positions) and the journal object (the entry) are three objects. With
several PGs they can have three different primaries, and only one OSD can
leave a three-member acting set while the pool stays writable with
`min_size` 2. With one PG a single change moves the primary of all three, in
every run.

### How the primary is changed

The fixture marks the PG's primary out (`ceph osd out <id>`). The daemon is
not stopped: it leaves the up set, another member becomes the acting primary
in the next epoch, and the new primary keeps the old one in the acting set
as its last member. The subject's connection to the old primary stays open
until the test closes it. A primary that is killed is the case of
`TestCephOSDResubmitFailoverIntegration`
([OSD_RESUBMIT_VALIDATION.md](OSD_RESUBMIT_VALIDATION.md)), for a plain
APPEND without RBD.

### Sequence

The steps up to `observe-recorded` are those of the lost-append record: the
subject locks, creates its tag, appends once, the reply is withheld, a
native checkpoint finds the entry, the wait is canceled, the unknown outcome
is recorded and a second native observation finds nothing moved.

| Step | What happens | Checked |
| --- | --- | --- |
| Placements before | The test resolves the three objects through the observer's MON command `osd map` | |
| `change-primary` (fixture, then native) | The fixture reads `ceph osd map` of the three objects, records the primary, runs `ceph osd out <id>`, and reads the placements until each object has the two other OSDs as its up set and the old primary as the last member of its acting set. The native observation follows | Stage `change-primary`, below |
| Placements after | The test resolves the three objects again | Equal to the fixture's |
| Connections closed | The subject closes its connection to the old primary, which still carries the withheld reply | |
| Watch reattached | The subject opens a connection to the new primary and reattaches its watch with `ReconnectWatch`, naming the current epoch | The watch went less than its timeout without a ping; the cookie is unchanged |
| Lock and watch read | `lock.get_info` and LIST_WATCHERS on the new primary | The lock names the subject with its cookie; raw values kept |
| Version before | STAT of the journal object | User version kept |
| Resubmission | The append's operations and modification time, with `Resubmit` set to the submission of the unknown outcome and `RetryAttempt` 1, sent once to the new primary | Recorded before it is judged. Anything but a known 0 stops the run; nothing is sent again |
| Version after | STAT of the journal object | User version kept |
| `observe-resubmitted` (native) | Read-only observation | Stage `observe-resubmitted`, below |
| Unlock, unwatch, `replay` | As in the lost-append record | Stage `replay` |

The record is judged after `checkpoint`, after `observe-recorded`, after
`change-primary`, after `observe-resubmitted` and after `replay`. A
contradiction at one stage stops the run there, so the append is not
resubmitted when the primary change or the state across it is in doubt.
After any failure the deferred compensation resolves the header's current
primary and releases the lock there. It never sends the append.

### Claims as the test checks them

- **`change-primary`, placement.** The fixture's receipt names the three
  objects. Before: each is on three distinct OSDs, up and acting sets equal,
  led by one OSD, the one the fixture then marked out with exactly one
  command. After: each is in the same PG, in a newer epoch; its up set is
  the two other OSDs; its acting set is that up set followed by the OSD
  that was marked out; the up primary and the acting primary are the first
  member of the up set. The test's own placements name the same PG, primary
  and acting set, at an epoch not above the fixture's before and not below
  it after. The subject sent `lock.lock` and the append to the OSD that was
  marked out.
- **`change-primary`, state.** In the native observation every journal
  object has the size and SHA-256 of the checkpoint, the entry is at its
  offset with nothing after it, the lock names the subject with its cookie,
  the subject's watch is listed, and the image, the image client's commit
  position and the tags are unchanged. No resubmission exists yet.
- **`observe-resubmitted`, answer.** One caller invocation; class `applied`;
  not an unknown outcome; code 0; ONDISK; retry attempt 1 sent and echoed;
  the reply's transaction and the submission's transaction are the
  transaction of the unknown outcome, of the subject's GlobalID; sent to the
  new primary and not to the old one; answered at an epoch not below the
  fixture's after epoch; recorded after the unknown outcome.
- **`observe-resubmitted`, no second execution.** The journal object's user
  version before the resubmission, in its answer and after it are equal and
  nonzero. In the native observation every journal object has the size and
  SHA-256 of the checkpoint and the entry has nothing after it. The lock is
  still the subject's with its watch listed, and the image, commit position
  and tags are unchanged.
- **`observe-resubmitted`, lock continuity.** The new primary lists the
  reattached watch at the address in the lock record.
- **`replay`, transmission.** Two writes of at least 4096 bytes in all, on
  two different subject connections, one dialed before the silent
  connection was closed and one after; the gate bound on the first. CRC
  only: two written frames carry the marker, one on each of those
  connections; the bound transaction is the submission's and the held frame
  is the MOSDOpReply to it on the first connection. Secure only: no frame
  is named, as in the lost-append record.
- **`replay`.** As in the lost-append record: journal bytes equal to the
  checkpoint, six entries inspected with one of the subject's tag, the image
  with the Go extent, the commit position of one replayed entry, the native
  tag's predecessor, the lock free.

### What the cell does not establish

- **That the OSD did not execute the append a second time.** The object's
  version and bytes say so, natively and through the Go client. No
  daemon-side execution record is read.
- **A resubmission after the PG log no longer holds the request.** The
  request was the latest write to its object. A trimmed log is the case of
  `TestCephOSDResubmitAfterTrimIntegration` for a plain APPEND; for a
  `journal.append` it would append again, as the lost-append record's
  control shows for a new request.
- **Fencing or liveness of the reattached owner.** The lock record and the
  watch record keep the address of the connection that registered them, so
  librbd's liveness rule would find the owner. By this client's source the
  subject's new connection has another address (a new nonce per OSD
  connection), which a blocklist entry made from the lock record would not
  cover. Neither is tested: no native client asked for the lock and nothing
  was blocklisted here.
- **A primary that died.** The old primary keeps running and stays in the
  acting set. Its connection to the subject is closed by the test, not by a
  failure.
- **Attribution on the secure row.** As in the lost-append record, the
  secure row names no frame and no transaction on the wire.

## Daemon-free checks

These establish no native result.

- `TestRBDJournalPrimaryChangeClaims` (cluster-free, in the Basic CI list):
  a consistent record passes in both modes at each of the five stages, and a
  record of one stage is refused at every other. A primary-change run
  without its record, a single-OSD run with one and a single-OSD record at a
  primary-change stage are refused. 101 contradictory
  records in CRC mode and 98 in secure mode are refused, each with every other part of the record intact, at
  the stage they belong to and again in a complete record. Examples: a
  primary that did not move, still leads, stayed in the up set or left the
  acting set; an epoch that did not advance; another PG or another OSD
  afterwards; a degraded or `pg_temp` placement at the start; a journal
  header led by another OSD; no, another or a second `osd out`; a lock or an
  append sent to another OSD; test placements that differ from the
  fixture's; journal bytes, the lock, the watch, the commit position or the
  image changed across the move; a resubmission before the move was judged;
  a resubmission that is missing, unknown, refused, a new transaction, not
  retried, a second retry, not durable, sent to the old primary or to the
  other member, or answered under an older map; a version that moved; a
  watch listed at another address; a third or a missing frame with the
  entry; a resubmission on the silent connection.
- `TestRBDJournalLostAppendClaims` (cluster-free): the claims shared with
  the single-OSD sequence.
- `integration/fixture/rbd_journal_test.go`: the selector is admitted only
  in its exact form on three BlueStore OSDs with `legacy-v6`, and that
  topology admits no other RBD selection; the sequence is the lost-append
  sequence with the two new actions before the replay, refused by both other
  sequences; the primary is marked out once and only after the intent is
  recorded; objects with different primaries, a placement that is not three
  distinct OSDs, a primary that does not move, another acting set, an old
  primary that left the acting set, a refused command and an unread map are
  errors; the cleanup marks the OSD in and reports only what the OSDMap
  confirms.

## Execution evidence

Direct native execution, not hosted CI: Linux amd64 hosts (x86_64, kernel
6.8), Docker 29.9.1, Go 1.26.9, the image above (daemon version `ceph version
20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`),
provider `github.com/JSYoo5B/ceph-testcontainers-go` v0.5.0, three MONs, two
MGRs, three BlueStore OSDs and the native oracle container. Every run used
the environment that the native workflow passes for its catalog row and the
same result gate (`tools/native_test_results.py snapshot` before and `verify`
after), one `go test` process per Docker engine. The source of every run was
uploaded as `git archive` of the named commit, with the commit ID recorded on
the host before the run.

Source commit `12a2a15e5756e51d9de8e3008efb8721ecc2ef9e`, 2026-11-13 (UTC):
the merge of the main branch into the last commit of this change that
touches code or the catalog, `29b110768efd592f8d227104a38964c54bb5d516`. The
commits after it in the same pull request change documentation only.

| Scenario ID | Gate | Test duration | Row wall time |
| --- | --- | --- | --- |
| `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalprimarychange` | pass, 1 expected test, no problem | 5.69 s | 88 s |
| `rbd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalprimarychange` | pass, 1 expected test, no problem | 6.40 s | 92 s |
| `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournallostappendreply` | pass, 1 expected test, no problem | 1.85 s | 78 s |
| `rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournallostappendreply` | pass, 1 expected test, no problem | 1.55 s | 76 s |
| `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-37bcdb` | pass, 5 expected tests, no problem | base 5.34 s, clone 3.14 s, journal 2.51 s, exclusive-lock 10.98 s, write 2.39 s | 100 s |
| `rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-suite-37bcdb` | pass, 5 expected tests, no problem | base 5.32 s, clone 2.69 s, journal 2.26 s, exclusive-lock 10.97 s, write 2.23 s | 98 s |

The two single-OSD lost-append rows were run because the body of their test
changed (the subject's connections, the shared claims). The two broad RBD
rows were run because the fixture's RBD preparation, profile admission and
journal control changed. Each of the six runs passed at its first attempt on
that commit. The same six rows had passed at their first attempt on
`29b1107` before the merge (6.08 s and 5.99 s for the two new rows), with
the same values except the epochs noted below.

### Observed values

Client GlobalIDs, addresses, the image ID and SHA-256 values differ per
fixture and are in the run's `rbd-journal-primary-change-results.json`. The
journal values are those of the lost-append record: entry of 4173 bytes at
offset 8347 of object 0, tag 2, entry 0.

| Value | CRC/AES IPv4 host | secure/AES256K IPv6 container |
| --- | --- | --- |
| Placement before, all three objects | PG `3.0`, up and acting `[0,1,2]`, primary osd.0, epoch 28 | same, epoch 30 |
| Fixture command | `osd out 0`, one command | same |
| Placement after, all three objects | PG `3.0`, up `[1,2]`, acting `[1,2,0]`, primary osd.1, epoch 30 | same, epoch 32 |
| Fixture placement reads until that state | 2, 2.9 s for the administrative part | 2, 3.3 s |
| Test's placements through the Go client | before `[0,1,2]` led by osd.0 at epoch 28; after `[1,2,0]` led by osd.1 at epoch 31 | before at epoch 30; after at epoch 33 |
| `change-primary` action in all | 3.4 s | 3.8 s |
| Native observation through the new primary | journal objects 12520, 8269, 4173, 8269 bytes with the checkpoint's SHA-256 values; lock `client.<subject GlobalID>`, cookie `auto 1`, its watch listed; commit position `[(0,1,4), (3,1,3), (2,1,2), (1,1,1)]` | same |
| Subject requests before the change (osd.0) | transactions 2 to 8 answered, 9 the append: `OutcomeUnknownError`, cause `context canceled`, submission at map epoch 28 | same, epoch 30 |
| Watch without a ping (registration to reattachment) | 4.44 s | 5.06 s |
| Subject requests after the change (osd.1) | 10 the watch RECONNECT, 11 `lock.get_info`, 12 LIST_WATCHERS, 13 STAT, then transaction 9 again, 14 STAT, 15 `lock.unlock`, 16 UNWATCH, all at request and reply epoch 31 | same, epoch 33 |
| Lock record and reattached watch on the new primary | one address for both: the address of the subject's closed connection to osd.0 | same |
| Journal object user version before the resubmission | 52 | 52 |
| Resubmission answer | code 0, transaction 9, retry attempt 1, user version 52, reply flags 0x400002c (ONDISK, RETRY, WRITE; an ordinary write reply here is 0x4000025), map epoch 31 | same, epoch 33 |
| Journal object user version after the resubmission | 52 | 52 |
| Journal objects at `observe-resubmitted` and `replay` | 12520, 8269, 4173, 8269 bytes; identical SHA-256 per object from the checkpoint on; nothing after the entry | same |
| Frames with the entry | 2: connection 1 (4503 bytes, transaction 9, its reply held: MOSDOpReply, 310 bytes) and connection 2 | not observed; 2 writes of 4096 bytes or more, the first 4560 bytes; bytes withheld on connection 1 |
| OSD dials of the subject | 1 before the connection to osd.0 was closed, 2 in total | same |
| After `replay` | native librbd owned the lock; image read equals the model with the Go extent; commit `[(0,2,0), (3,1,3), (2,1,2), (1,1,1)]`; native tag 3 with predecessor (2, 0); six entries inspected, 0 errors; lock free; no watcher | same |
| Native action durations (ms): setup, observe-locked, checkpoint, observe-recorded, change-primary, observe-resubmitted, replay | 453, 172, 230, 181, 3396, 185, 293 | 504, 184, 272, 181, 3825, 173, 306 |

In the secure run before the merge the fixture read the image header at
epoch 29 and the two journal objects at epoch 30; the map advanced between
two of its reads without a change of the placement.

In both runs the fixture's second placement read showed the old primary as
the last member of the acting set. An earlier run listed below recorded the
placement before that one: the two other OSDs alone.

Every native driver process ended with empty stderr. Each of the three
`rbd journal inspect` runs per row printed the one line the journal
acceptance already tolerates and nothing else.

### Cleanup

Both rows: `rbd-base-owned.json` holds `osds_marked_out: [0]` and
`native-journal-setup`, `-observe-locked` (`read_only`), `-checkpoint`,
`-observe-recorded` (`read_only`), `-change-primary`, `-observe-resubmitted`
(`read_only`) and `-replay`, all `applied`. `rbd-base-cleanup.json` lists
`osds_marked_out: [0]` and `osds_marked_in: [0]`, then reports the pool
removed, natively absent, both identities removed and `completed: true`. Ten
container terminations are logged per run. After each run no container with
the provider's label was left on the host. No independent final network
inventory is retained.

### Controls

The checks were shown to fail on wrong behavior by two runs with a temporary
edit of the test, on the tree of `29b1107`, before the merge. They were not
repeated on the merge commit, which changes no RBD file. The edit was applied only
to the uploaded copy and never committed. Each is one run in one profile.

| Control | Profile | Edit | Result |
| --- | --- | --- | --- |
| New request instead of the resubmission | CRC row | After the primary change the subject sends the same `journal.append` without `Resubmit` and `RetryAttempt` | Gate verdict fail. The test failed at stage `observe-resubmitted` with four lines: the answer is for transaction 14, retry attempt 0, not for transaction 9; the object's user version is 52 before, 58 in the answer and 58 after; the journal objects changed; journal object 0 is 16693 bytes, the entry followed by 4173 more bytes |
| New request instead of the resubmission | secure row | The same edit | Gate verdict fail, the same four lines with the same values |

In both control runs the new primary answered the second append with code 0
and ONDISK, as it answered the resubmission in the passing runs. The reply
code does not tell the two apart. The transaction, the version and the
native read of the object's bytes do. The run stopped at that stage: the
deferred compensation released the lock (0), the journal was not replayed,
and the cleanup marked the OSD in and completed.

### Earlier runs of this change

These were runs of the uncommitted working tree on the base commit
`b5d08602a17365f03bd1e0b36f7b48fa401a8a75`, in the same environment and with
the same gate. They are development runs, not acceptance evidence.

- CRC row, first run. The fixture moved the primaries with `ceph osd
  primary-affinity osd.<id> 0` on a pool of eight PGs. The test failed at
  the subject's first connection after the change: `ceph messenger:
  unsupported required features: peer requires 0x0000020000000000`. Cause,
  in this change: bit 41 is `CEPH_FEATURE_OSD_PRIMARY_AFFINITY`, which every
  OSD requires of a client once any OSD has a non-default primary affinity
  (`OSDMap::get_features`). The client was right to refuse. The fixture now
  marks the primary out instead, and the pool has one PG so that one OSD
  leads all three objects.
- CRC row, second run. The test failed at stage `change-primary`: the
  fixture had accepted the first placement it read after `osd out`, up and
  acting `[1,2]` at epoch 31, and the test's own read a moment later showed
  acting `[1,2,0]` at epoch 33. Cause, in this change: the new primary asks
  for a `pg_temp` entry that keeps the old primary in the acting set, one
  epoch later. The fixture now waits for that placement, and both sides
  require it.
- CRC row, third run, and secure row, first run: passed (5.69 s and 5.89 s)
  with the values above. The claim that the reattached watch is listed at
  the lock record's address was added after these two runs, from what they
  recorded.

On the commits of this change no run of the six rows failed or was repeated
for a different result. The rows were run twice because the main branch was
merged in between.

## Product findings

- No wire gap and no product change. `Resubmit` with `RetryAttempt`, the
  watch reattachment and the requests at a new epoch use existing
  primitives.
- A `journal.append` that the old primary applied is answered from the PG
  log by the new primary when it is resubmitted under its original
  transaction: code 0, the original user version, the RETRY flag in the
  reply, and no second entry. The same bytes as a new request are appended
  again.
- After the canceled wait the client kept the submission, and
  `OSDRequest.Resubmit` accepted it on a connection to another OSD of the
  same client.
- A `legacy-v6` client cannot connect to an OSD of a cluster in which any
  OSD has a non-default primary affinity. The handshake fails with the
  feature the peer requires. This is recorded, not changed.
- The watch record and the lock record keep the address of the connection
  that created them: after the primary change and `ReconnectWatch` the new
  primary returned one address for both. By this client's source
  (`internal/session/handshake.go`) the owner's new connection has another
  address; no observation here reads the address of that connection.

## Not verified

- A primary that died or was partitioned, and a change of the acting set's
  members. The old primary stays up and in the acting set.
- Lock-owner loss, a native lock request, blocklisting and a forced break
  after the primary change. The subject releases the lock itself.
- A request that reached the old primary after it lost its role, or that
  the old primary had not applied. The withheld append was applied and
  replicated before the change.
- A resubmission after the PG log was trimmed, a second resubmission, and a
  resubmission that itself gets no answer.
- A pool with several PGs, in which the image header and the journal object
  have different primaries; more than one pending append; a lost reply of
  `tag_create`, `lock.lock`, `lock.unlock` or `client_commit` across the
  change.
- Watch pings and notifications across the change. The subject sends no
  ping, as in the lost-append record, and no client notified it.
- Frame-level attribution on the secure row.
- Hosted CI runs of the two rows, arm64, and other Ceph releases.
