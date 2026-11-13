# RBD journal append with a lost reply

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Issue #12, requirement 3, one cell: object-request uncertainty on a journal
append while the appender holds the image's exclusive lock. The product is
unchanged: no product code, no new primitive and no RBD, image or journal
API. The cell uses only the existing explicit OSD API (`OpenOSD`, `Request`
with `OSDCall`, `OSDStat` and `OSDRead`, `WatchObject`, `ReconnectWatch`,
`Unwatch`, `Close`). The cls_journal and cls_lock encodings, the Entry
framing, librbd's `EventEntry` and `TagData`, the commit rule and the tag
allocation are the test-only consumer code of
`integration/rbd_journal_test.go`. The native driver actions are
development-only; see [RBD_BASE_FIXTURE.md](RBD_BASE_FIXTURE.md) (section
"Lost-append-reply sequence").

The [journal acceptance](RBD_JOURNAL_VALIDATION.md) and the
[concurrent-appender acceptance](RBD_JOURNAL_CONCURRENT_VALIDATION.md)
exercised only answered appends. Their `uncertain` outcome class was covered
by a unit test alone. This cell produces that outcome live, once, and records
what native clients see around it.

## What the cell claims

One Go client, the subject, holds the lock and sends one journal append. Its
reply never reaches the session. The record keeps four claims apart, and each
is judged on its own evidence:

| Claim | Evidence | Not evidence for it |
| --- | --- | --- |
| Application: the entry is in the journal while the caller is still pending, and nothing else moved | Native librados reads of the journal objects, `rbd journal status`, `rbd journal inspect` and a librbd image read, taken while the call is pending and the lock is held | The gate, the Go observer client, the caller's result |
| Outcome: the caller learned nothing and asked once | The error and submission that `Request` returned after its local wait was canceled | Any native observation |
| Order: the unknown outcome was recorded before the commit position advanced or the lock was handed off | A second native observation after the outcome file was written and before the subject released the lock | The order of steps as the test lists them, taken alone |
| Transmission: the request left the client once | Frames or writes counted on every OSD connection of the subject, including the one opened afterwards | Daemon-side execution counts, which no observer here has |

The native replay at the end is a fifth observation: native librbd applied
the entry once, and the journal bytes did not change.

## Pinned source facts

Read at the reference tag for this change.

- [`src/cls/journal/cls_journal.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/cls/journal/cls_journal.cc)
  `journal_object_append`: the method stats the object and writes the entry
  at its current size. It keeps no record of earlier entries or of request
  identities, so a second `journal.append` of the same bytes appends them
  again.
- [`src/journal/ObjectRecorder.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/journal/ObjectRecorder.cc):
  with `require_osd_release` octopus or later the recorder sends one write
  vector `[journal.append(soft max size, entry)]`, the form used here.
- [`src/journal/JournalPlayer.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/journal/JournalPlayer.cc):
  the player expects the entry tids of a tag to be contiguous and reports an
  entry whose tid does not follow the last one.
- [`src/cls/lock/cls_lock.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/cls/lock/cls_lock.cc):
  a lock is released by the entity and cookie that hold it; the connection
  does not matter.
- [`src/osd/Watch.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/Watch.cc):
  a watch that pings is kept for its timeout after the last ping
  (`Watch::connect`, `Watch::got_ping`). Reattaching it on a new connection
  is the client's existing `ReconnectWatch`
  ([OSD_WATCH_RECONNECT_VALIDATION.md](OSD_WATCH_RECONNECT_VALIDATION.md)).
- [`src/librbd/Journal.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/librbd/Journal.cc):
  opening the journal on lock acquisition replays every entry after the image
  client's commit position.

A native client whose OSD session breaks resends its pending requests with
their original identity
([`src/osdc/Objecter.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osdc/Objecter.cc)).
The Go client never resends on its own, and this cell does not ask it to. The
journal append is therefore not repeated in any form: not as a new request and
not as an explicit resubmission of the original transaction.

## Executable boundary

Test: `TestCephRBDJournalLostAppendReplyIntegration` in the `integration`
suite, on the RBD fixture (`CEPH_MSGR_TEST_RBD=1`, single OSD, `legacy-v6`)
under its exact selector. The fixture then serves the closed action sequence
`setup`, `observe-locked`, `checkpoint`, `observe-recorded`, `replay` and no
other RBD test runs, so the journal holds only the native setup entries.
Required rows:

- `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournallostappendreply`
  (CRC, AES client and service keys, IPv4, host test process, one BlueStore
  OSD);
- `rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournallostappendreply`
  (secure, AES256K client and service keys, IPv6, container test process, one
  MemStore OSD).

Reproduce the CRC row with:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=1 CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 CEPH_MSGR_TEST_RBD=1 \
CEPH_MSGR_TEST_RUN='^TestCephRBDJournalLostAppendReplyIntegration$' \
sh integration/run.sh
```

### Clients

- The **subject** holds the lock and appends. Every OSD connection it dials
  is wrapped by the gate. Its keepalive timeout is raised to five minutes so
  that the silent connection ends by the test's cancellation and not by a
  keepalive failure.
- The **observer** is a second Go client with its own connections. It
  resolves placement through MON commands and, on the secure row, reads the
  journal object's size for the barrier described below. It is never used as
  evidence of the journal's state.
- The **native driver** uses its own identity and processes. No fault touches
  its connections: the gate exists only inside the subject's test process.

### The owned entry

The entry is the first Go write of the journal plan (40 to 44 KiB), built as
in the journal acceptance: an AioWrite `EventEntry` in a `journal::Entry` with
the subject's new tag and entry tid 0. Entry tid 0 belongs to splay offset 0
of the active object set, journal object 0, and the entry's offset in that
object is the object's size in the native `observe-locked` read. The entry is
4173 bytes.

### Reply gate

`internal/testcluster.OSDReplyGate` is test-only instrumentation on the
subject's OSD connections. It forwards every outgoing byte.

- **CRC row (decoded).** The gate is armed with the entry's bytes as a
  marker. When a complete MOSDOp frame that contains the marker is first
  written, the gate binds to that connection and to the transaction ID in the
  frame, before the bytes leave. On the read side it decodes the stream from
  the banner on and releases only complete frames. The MOSDOpReply frame with
  the bound transaction on the bound connection is held: no byte of it
  reaches the session. Frames before it are delivered first. Bytes after it
  on that connection stay withheld with it, because the stream is ordered; in
  this sequence nothing else is pending there. The gate counts every written
  MOSDOp frame that carries the marker, on every subject connection.
- **Secure row (undecoded).** Secure frames cannot be read by a connection
  wrapper. The gate binds to the first write of at least 4096 bytes and from
  then on withholds every byte received on that connection. Before the
  append, the test requires that the subject has written nothing of that
  size, so the bound write is the append.

Both rows count the subject's writes of at least 4096 bytes on every
connection. On the CRC row that count is 1 and the one such write is the
marked frame, which is the measured basis for using the write size to single
out the append on the secure row.

### Sequence

| Step | What happens | Checked |
| --- | --- | --- |
| `setup` (native) | The journaling image is created, the native oracle journal client is registered and the five setup events are written through librbd | The image read equals the model; five entries, 0 errors in `rbd journal inspect`; lock free, no watcher |
| Lock | The subject watches `rbd_header.<id>` (timeout 30 s), sends `lock.lock` (exclusive, cookie `auto <watch handle>`, tag `internal`) and reads `lock.get_info` | The reply names the subject |
| Tag | `get_client("")` (byte-equal to the native `exec` output of `setup`), `get_next_tag_tid`, `tag_create(next, class, TagData{"", {"", true, image client's commit front}})` | Each reply carries the subject's GlobalID, its own transaction, ONDISK and a user version |
| `observe-locked` (native) | Read-only observation | The lock is the subject's with its cookie and watch; the new tag exists with that predecessor; every journal object equals `setup`; image and image client commit position unchanged |
| Control read | The subject reads journal object 0 (STAT and READ) on the gated connection | The bytes equal the native read; the reply passed the gate; the subject has written nothing of 4096 bytes or more |
| Append | The gate is armed. `[journal.append(2^24, entry)]` is submitted once from a goroutine | The gate binds to a write on the subject's first connection |
| Barrier | CRC: the gate holds the reply frame of the bound transaction. Secure: the observer reads the object's size until it is the earlier size plus the entry; any other size, or the end of the caller, fails at once. Then the test waits, for at most 10 s, until the gate has intercepted received bytes on the bound connection | The caller is still pending |
| `checkpoint` (native) | Observation with `rbd journal inspect` | Application claim, below |
| Cancel and record | The local wait is canceled. The result and the entry's identity are written to `rbd-journal-lost-append-outcome.json` | `OutcomeUnknownError` with cause `context.Canceled`, the submission's transaction and GlobalID, no reply fields |
| `observe-recorded` (native) | Read-only observation | Order claim, below |
| Handoff | The subject closes the silent connection, opens a new one, reattaches its watch with `ReconnectWatch`, reads `lock.get_info`, sends `lock.unlock` and unwatches | The watch went less than its timeout without a ping; the lock was still the subject's; one more OSD dial |
| `replay` (native) | librbd acquires the exclusive lock, which opens the journal and replays | Replay observation and transmission claim, below |

The record is judged after `checkpoint`, after `observe-recorded` and after
`replay`. A contradiction at one stage stops the run there, so a doubtful
state is never followed by the next step of the handoff. After any failure
the deferred compensation releases the lock on a fresh connection and never
resends the append.

The subject sends no watch ping. A ping issued while replies are withheld
would be one more request without an answer. The OSD keeps a watch for its
timeout after the last ping, and the registration counts as one, so the
sequence between registration and reattachment must fit into 30 s. The test
records that interval and fails with that reason when it exceeds 25 s.

### Claims as the test checks them

Native values come from the driver's output: librados reads and `exec`,
librbd's locker and watcher lists, `rbd journal status` and
`rbd journal inspect`. The entry's own SHA-256 is computed from the bytes the
subject built.

- **Application** (`observe-locked`, `checkpoint`). Before the append,
  journal object 0 has the size that becomes the entry's offset and every
  object equals `setup`. At the checkpoint, object 0 is its earlier bytes
  (same SHA-256 as the whole object before) followed by exactly the entry
  (its SHA-256) and nothing after it; objects 1 to 7 are unchanged.
  `rbd journal inspect` replays the five setup entries and one entry of the
  subject's tag, an AioWrite with the entry's image offset, length and
  timestamp to the microsecond, with 0 errors. The image read equals the
  model without the Go extent. The image client's commit position, the tag
  list and the next tag tid are unchanged. The lock names the subject with
  its cookie and the subject's watch is listed. After the action returned the
  caller was still pending.
- **Outcome.** One caller invocation; `OutcomeUnknownError`; canceled; the
  submission's transaction is nonzero and belongs to the subject's GlobalID;
  no reply transaction or code.
- **Order** (`observe-recorded`). Every journal object has the size and
  SHA-256 of the checkpoint. The image, the image client's commit position,
  the tag list and the next tag tid are unchanged, and the lock is still the
  subject's with its watch.
- **Transmission.** The gate bound on the subject's first connection; one
  write of at least 4096 bytes across both connections; one OSD dial before
  the silent connection was closed and two in total; no undecodable stream.
  CRC only: one written frame carries the marker, on the first connection;
  its transaction equals the submission's; the held frame is a MOSDOpReply
  with that transaction on that connection. Secure only: the record must
  carry no transaction, no held frame and no marked-request count, and the
  gate's withheld-bytes flag must be true both as it was read directly before
  the wait was canceled (`WithheldBeforeCancel`) and in the final gate state.
  A flag that is present but false means that no read after the binding was
  intercepted, so the injection was not observed. A CRC record must not carry
  `WithheldBeforeCancel`.
- **Replay** (`replay`). Native librbd acquired the lock. Every journal
  object still has the size and SHA-256 of the checkpoint, so the replay
  appended nothing and the entry was not written twice. `rbd journal inspect`
  again shows six entries, one of the subject's tag, 0 errors. The image read
  equals the model with the Go extent. The image client's commit position is
  the commit rule over the one entry (front: object 0, the subject's tag,
  entry 0). The newest tag is native, with that entry as its predecessor. The
  lock is free and the subject's watch is gone.

### What the secure row does not establish

The transaction and sequence of the append, that the withheld bytes contain a
reply to it (they can be an acknowledgement, a keepalive or any other frame),
and the identity of any frame. The native checkpoint shows that the OSD
applied the append; the withheld stream shows that the caller could not have
been told. The barrier of that row is a state the observer client polls, not
a frame. Attribution at frame level in secure mode would need instrumentation
inside the session, after decryption, which no test has.

## Daemon-free checks

These establish no native result.

- `internal/testcluster/osd_reply_gate_test.go`: with three-byte read
  fragments and 1-byte, 7-byte and 64 KiB read buffers, the banner and
  earlier frames pass byte-identical; a request without the marker, another
  message type with the marker, a reply with another transaction and the
  bound transaction on another connection all pass; nothing binds or is
  counted before the gate is armed; the bound reply is held without releasing
  a byte and closing ends the read with `net.ErrClosed`; a repeated marked
  request is counted with its connection and never rebinds; an undecodable
  stream is forwarded unchanged with a recorded error. Undecoded mode: only
  the first write of the threshold size after arming binds, bytes received
  before it pass, every byte after it on that connection is withheld, other
  connections keep delivering, and the state never names a frame.
- `TestRBDJournalLostAppendClaims` (cluster-free, in the Basic CI list): a
  consistent record passes in both modes and at each stage. 59 contradictory
  records in CRC mode and 60 in secure mode are refused, each with every other part of the record
  intact: for example an entry that is absent, different or followed by more
  bytes; a changed prefix or neighbour object; an inspection that did not run
  or shows the entry twice; an image, commit position, tag or lock that moved
  before the record; a delivered reply; a cancellation before sending; a
  second caller invocation; a second marked request or large write; a redial
  while silent; a held reply of another transaction; a secure record that
  names a transaction or frame; a secure record whose withheld-bytes flag is
  false or absent before the cancellation or finally; a replay that appended, did not apply or did
  not commit the entry.
- `integration/fixture/rbd_journal_test.go`: both action sequences are closed
  and ordered and each refuses the other's actions at every position; the
  selector is admitted only in its exact form and only with the RBD opt-in;
  the checkpoint output must carry its inspection, the two observations must
  not carry one, native events or a lock acquisition.

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

Source commit `6d0593db5ebd4f254ece6907755973d1d64699df`, 2026-11-12 (UTC). It
holds the gate, the fixture sequence, the test and the catalog rows; the
commits after it in the same pull request change documentation only.

| Scenario ID | Gate | Test duration | Row wall time |
| --- | --- | --- | --- |
| `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournallostappendreply` | pass, 1 expected test, no problem | 1.85 s | 87 s |
| `rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournallostappendreply` | pass, 1 expected test, no problem | 1.44 s | 84 s |
| `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-37bcdb` | pass, 5 expected tests, no problem | base 5.34 s, clone 2.64 s, journal 2.74 s, exclusive-lock 11.20 s, write 1.48 s | 100 s |
| `rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-suite-37bcdb` | pass, 5 expected tests, no problem | base 5.32 s, clone 2.79 s, journal 2.26 s, exclusive-lock 10.97 s, write 2.23 s | 98 s |

The two existing RBD rows were run because the journal control's request
parser, action list and profile admission changed. Each of the four runs
passed at its first attempt on that commit. Earlier commits of the same
change were run before it (below).

Source commit `304f0cbd4bc2df493f62b5a52a22e642d44cea5e`, 2026-11-12 (UTC). It
adds the bounded wait for intercepted bytes on the secure row, the flag read
before the cancellation and the stricter claims. Both new rows ran once each
on that commit and passed at the first attempt. The existing RBD rows were not
rerun: the commit touches only the new test and its claims.

| Scenario ID | Gate | Test duration | Row wall time |
| --- | --- | --- | --- |
| `rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournallostappendreply` | pass, 1 expected test, no problem | 1.85 s | 86 s |
| `rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournallostappendreply` | pass, 1 expected test, no problem | 1.55 s | 84 s |

The secure run therefore passed the new requirement: the test fails before
the cancellation when the flag is not true at that point. No live run
produced a secure exchange in which nothing was intercepted; that case has
daemon-free coverage only. The controls below were not repeated on this
commit.

### Observed values

Both rows observed the same journal values. Client GlobalIDs, the image ID and
SHA-256 values differ per fixture and are in the run's
`rbd-journal-lost-append-results.json`.

| Value | CRC/AES IPv4 host BlueStore | secure/AES256K IPv6 container MemStore |
| --- | --- | --- |
| Journal objects 0 to 3 after `setup` and at `observe-locked` | 8347, 8269, 4173, 8269 bytes | same |
| Image client commit position before the replay | `[(0,1,4), (3,1,3), (2,1,2), (1,1,1)]` (object, tag, entry) at `setup`, `observe-locked`, `checkpoint` and `observe-recorded` | same |
| Subject's tag | tag 2, predecessor (1, 4) valid; next tag tid 3 from `observe-locked` to `observe-recorded` | same |
| Owned entry | tag 2, entry 0, object 0, offset 8347, 4173 bytes | same |
| Subject requests | transactions 2 to 8 answered (watch, `lock.lock`, `lock.get_info`, `get_client`, `get_next_tag_tid`, `tag_create`, control read), 9 the append, 10 to 13 on the second connection (watch reconnect, `lock.get_info`, `lock.unlock`, unwatch) | same |
| Append frame as written | 4503 bytes | 4560 bytes |
| Gate binding | connection 1, transaction 9, message sequence 8 | connection 1; transaction and sequence not observed |
| Marked request frames | 1, on connection 1 | not observed |
| Held frame | MOSDOpReply (type 43), transaction 9, sequence 8, 310 bytes, connection 1 | not observed; bytes arrived on the bound connection after the binding and were withheld |
| Writes of 4096 bytes or more, both connections | 1 | 1 |
| Barrier | held reply | observer size reads: 1 (the first read already showed 12520 bytes) |
| Journal objects at `checkpoint`, `observe-recorded` and `replay` | 12520, 8269, 4173, 8269 bytes; object 0 is the earlier 8347 bytes and the entry, nothing after it; identical SHA-256 per object at all three | same |
| `rbd journal inspect` | 5 entries at `setup`; 6 at `checkpoint` and at `replay`, one of tag 2 (AioWrite, offset 40960, length 4096, the entry's timestamp), 0 errors | same |
| Lock and watch at `observe-locked`, `checkpoint`, `observe-recorded` | the subject (`client.<subject GlobalID>`, cookie `auto 1`), its watch listed | same |
| Caller pending after `checkpoint` | yes | yes |
| Caller result | `OutcomeUnknownError`, cause `context canceled`; submission transaction 9, map epoch 24; no reply fields | same with map epoch 25 |
| Watch without a ping (registration to reattachment) | 1.02 s | 0.83 s |
| OSD dials of the subject | 1 before the silent connection was closed, 2 in total | same |
| After `replay` | native librbd owned the lock; image read equals the model with the Go extent; commit `[(0,2,0), (3,1,3), (2,1,2), (1,1,1)]`; native tag 3 with predecessor (2, 0); next tag tid 4; lock free; no watcher | same |
| Native action durations (ms): setup, observe-locked, checkpoint, observe-recorded, replay | 350, 177, 224, 181, 271 | 228, 175, 232, 175, 224 |

Every native driver process ended with empty stderr. Each of the three
`rbd journal inspect` runs per row printed the one line the journal acceptance
already tolerates (`failed to locate client: INSPECT`) and nothing else.

### Cleanup

Both rows: `rbd-base-owned.json` holds `native-journal-setup`,
`-observe-locked` (`read_only`), `-checkpoint`, `-observe-recorded`
(`read_only`) and `-replay`, all `applied`. `rbd-base-cleanup.json` reports
the pool removed, natively absent, both identities removed and
`completed: true`. Eight container terminations are logged per run. After
each run no container with the provider's label was left on the host. No
independent final network inventory is retained.

### Controls

The checks were shown to fail on wrong behavior by three runs with a temporary
edit of the test, on the tree of the commit above. Each edit was applied only
to the uploaded copy and never committed. Each is one run in one profile.

| Control | Profile | Edit | Result |
| --- | --- | --- | --- |
| Resent append | CRC row | After reattaching the watch, the subject sends the same `journal.append` once more on its new connection | Gate verdict fail. The test failed at stage `replay` with four lines: two transmission lines (2 marked request frames on connections 1 and 2, 2 writes of 4096 bytes or more) and two replay lines (journal object 0 is 16693 bytes: the entry is followed by 4173 more bytes) |
| Resent append | secure row | The same edit | Gate verdict fail. The test failed at stage `replay` with three lines: one transmission line (2 writes of 4096 bytes or more; no frame is named) and the same two replay lines |
| Early release | CRC row | Before canceling its wait, the subject sends `lock.unlock` on the silent connection with a 2 s wait | Gate verdict fail. The test failed at stage `observe-recorded`: the native observation shows the lock free. The run stopped there; the deferred compensation's own `lock.unlock` was answered with -2 |

In both resent-append runs the OSD answered the second append with code 0
and the object grew by the entry's size again, as the class method's source
says. The native tools did not report the duplicate: `rbd journal inspect`
still listed six entries with 0 errors, native librbd acquired the lock,
replayed and committed (front (0, 2, 0)) with empty stderr, and the image read
equalled the model with the Go extent. Only the native read of the journal
object's bytes and the client-side transmission count showed the second
append. A check that relied on the inspection or the image alone would have
accepted it.

### Earlier runs of this change

- Commit `6c2a00515d0d480925d03a1b4ccad3c5aafaa300` (first complete version):
  both new rows passed their gate at the first attempt (1.85 s and 1.55 s).
  That version pinged the watch every 5 s; no ping was ever due in a run of
  under 2 s, so the ping loop was removed instead of being claimed.
- Commits `11ad7f741e41a5f52536ca0e8eaf0f981a02dfc2` and
  `d3f7b2c7c87a0e6c8d1e8bd2ce0553258bc56643` (the explicit watch-interval
  check, then the claims judged in three stages): the two new rows and the
  two existing RBD rows passed their gates on each, with the values above.
  The three controls were first run on `d3f7b2c` with the same verdicts; its
  failure text printed pointer values for optional fields, which the final
  commit prints as JSON.
- No run of the new rows failed or was repeated for a different result. These
  commits were local revisions of the same branch and are not part of its
  published history.

## Product findings

- No wire gap and no product change. The lock and journal class calls, the
  append, the watch registration and its reattachment on a fresh connection
  use existing primitives.
- After the canceled wait the client kept the submission (transaction,
  GlobalID, map epoch) with the `OutcomeUnknownError`, on both rows. The
  later requests of the same client on its new connection took the next
  transactions.

## Not verified

- A primary change or any multi-OSD acting set: both rows have one OSD, so
  the lost reply is not combined with a new primary, a PG log dedup of a
  resent request, or peering. That part of issue #12 requirement 3 stays
  open, with a three-OSD replicated recipe as its precondition.
- An explicit resubmission of the uncertain append with its original
  transaction (`OSDRequest.Resubmit`). The caller here does nothing with the
  uncertain request. What the OSD answers to such a resubmission of a
  `journal.append` is not observed.
- Lock-owner loss, blocklisting and a forced lock break while the outcome is
  uncertain. The subject stays alive and releases the lock itself. Owner
  loss without a pending request, on an image without journaling, is
  recorded in
  [RBD_LOCK_OWNER_LOSS_VALIDATION.md](RBD_LOCK_OWNER_LOSS_VALIDATION.md).
- A reply lost on the `tag_create`, on `lock.lock` or `lock.unlock`, on the
  compat append form (`guard_append` then APPEND), on a data write or on a
  `client_commit`; more than one pending append; an append that the OSD did
  not apply.
- A journal header watcher or a second image watcher during the fault, and
  native librbd asking for the lock while the outcome is uncertain (the image
  was closed natively throughout).
- Frame-level attribution on the secure row, as stated above.
- Watch pings while replies are withheld, and a silent interval near or above
  the watch timeout.
- Hosted CI runs of the two rows, arm64, and other Ceph releases.
