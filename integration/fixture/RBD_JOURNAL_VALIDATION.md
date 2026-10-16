# RBD journaling Messenger I/O acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go,
without CGO, librados, librbd, go-ceph, Ceph CLI subprocesses or native
runtime dependencies, and it gains no RBD or journal API: this acceptance
uses only the existing explicit OSD API (`OpenOSD`, `Request` with `OSDCall`,
`OSDStat`, `OSDRead`, `OSDAppend`, `WatchObject`, `Next`, `Acknowledge`,
`Unwatch`). The cls_journal encodings and decodings, the Entry framing and
CRC, librbd's `EventEntry`, `ClientData` and `TagData`, the commit-position
rule, the tag allocation and the append protocol are test-only consumer code
in `integration/rbd_journal_test.go`. The disposable provider, the native
librbd/librados driver and the owned pool are development-only; see
[RBD_BASE_FIXTURE.md](RBD_BASE_FIXTURE.md) (section "Journaling image and
closed journal control").

This builds on the [RBD base](RBD_BASE_VALIDATION.md),
[write](RBD_WRITE_VALIDATION.md), [exclusive-lock](RBD_LOCK_VALIDATION.md)
and [clone](RBD_CLONE_VALIDATION.md) acceptances. Native librbd creates an
image with the `exclusive-lock` and `journaling` features in the owned pool
and writes and discards through it. Go reads the image journal the way
librbd's `JournalMetadata` and the `rbd journal` commands do, consumes it as a
mirror-like journal client, watches its header, and appends entries in
librbd's format under the image's exclusive lock. Native librados `exec`,
librados reads, `rbd journal info`, `status` and `inspect`, and a native
librbd journal replay are the independent oracles. It does not establish RBD
mirroring or journal replay by a Go consumer.

## Test shape

`TestCephRBDJournalIntegration` runs only with `CEPH_MSGR_TEST_RBD=1` and
`CEPH_MSGR_TEST_OSD=1` (single OSD, `legacy-v6`); the profile's selector is
now `^TestCephRBD(Base|Write|ExclusiveLock|Clone|Journal)Integration$` and
`go test` runs the five in file order (base, clone, journal, exclusive-lock,
write). The lock and write drivers' setup actions admit the journaling image
in the pool. The Go client authenticates as the base identity
`client.rbd-base-<token>` (MON `profile rbd`, OSD `profile rbd pool=<owned
pool>`) with the selected key type and `EnableOSD`. Every request uses
caller-side placement from the MON `osd map` and the primary's v2 address
from `osd dump`; each reply must carry a nonzero transaction equal to its
submission, the client's GlobalID, the object name, the raw-hash PG echo, a
map epoch not older than the request's and retry 0, and each successful
mutation ONDISK and a user version. Every request (inputs, modification time,
identity, epochs, flags, versions, per-operation results and outputs) is
recorded in `rbd-journal-results.json`, with the decoded entries, tags and
commit positions of every observation, the header notifications, the Go
appends and the parsed `rbd journal inspect` output.

The image (plan in the fixture document) is 256 KiB, order 16, features 69
and default journal options. Right after creating it the native driver
registers a native journal client `ceph-msgr-oracle-<token8>` with empty
data. `client_register` stores the minimum commit position of the connected
clients, which is empty here, so this client never commits and `rbd journal
inspect` (which registers its `INSPECT` client at that minimum) replays every
entry of the journal. The setup action writes four extents and discards
object 3; the write action writes two extents and discards object 2. The
test keeps its own model of the image (the drivers' SHA-256 patterns, zeros
for discards) and checks every native librbd read against it.

### Native forms restated by Go

From the pinned sources:

- Journal header (`journal.<image id>`, `cls_journal_client.cc`):
  `C_ImmutableMetadata` sends one read vector `[get_order, get_splay_width,
  get_pool_id]`, `C_MutableMetadata` one `[get_minimum_set, get_active_set]`
  followed by `client_list(start_after, JOURNAL_MAX_RETURN 256)`;
  `get_client(id)`, `get_next_tag_tid` and `tag_list(start_after_tag_tid,
  max_return, client_id, optional tag class)` go alone. Outputs: u8 order,
  u8 splay width, s64 pool id, u64 sets, `std::set<Client>` (each
  `ENCODE_START(1,1)` id, data bufferlist, `ObjectSetPosition`
  (`ENCODE_START(1,1)`, a list of `ObjectPosition` `ENCODE_START(1,1)`
  object number, tag tid, entry tid, most recent first) and u8 state), u64
  next tag tid, `std::set<Tag>` (`ENCODE_START(1,1)` tid, class, data).
- Journal data objects (`Journaler::object_oid_prefix`):
  `journal_data.<header pool id>.<journal id>.<object number>`; an entry
  with entry tid e goes to splay offset e mod splay width of the active set
  (`JournalRecorder`), and entry tids restart at 0 in each tag.
- `journal::Entry`: u64 preamble `0x3141592653589793`, u8 version 1, u64
  entry tid, u64 tag tid, the payload bufferlist, then u32
  `bufferlist::crc32c(0)` (seed 0, no inversion) of everything before it.
- librbd `EventEntry`: `ENCODE_START(5, 1)` with the u32 event type and the
  event (AioWrite: u64 offset, u64 length, data bufferlist; AioDiscard: u64
  offset, u64 length, bool skip partial discard, u32 discard granularity),
  then `ENCODE_START(1, 1)` metadata with the `utime_t` timestamp.
- Image client (`librbd/journal/CreateRequest.cc`): client id `""` with
  `ClientData{ImageClientMeta}` (`ENCODE_START(2, 1)`, type 0, u64 tag
  class, bool resync requested); tags carry `TagData` (mirror uuid, then the
  predecessor's mirror uuid, bool commit valid, tag tid and entry tid).
- Commit position (`JournalMetadata::committed`): each committed entry's
  `ObjectPosition` goes to the front and one position per splay offset is
  kept; `client_commit(id, ObjectSetPosition)` stores it.
- Tag allocation on lock acquisition (`Journal::allocate_local_tag`,
  `C_AllocateTag`): `get_next_tag_tid`, then `tag_create(tid, tag class,
  TagData{"" , predecessor {"" , true, image client's commit front}})`.
- Append (`ObjectRecorder`): with `require_osd_release` octopus or later
  (here tentacle) one write vector `[journal.append(soft max size 2^order,
  entry bytes)]`; before octopus `[journal.guard_append(soft max size),
  APPEND (FADVISE_DONTNEED)]`.
- Header updates (`JournalMetadata::C_NotifyUpdate`,
  `handle_watch_notify`): notifies with an empty payload and a 5 s timeout,
  acknowledged with an empty payload.

The product admits each of these Go vectors under the current client rule
(internal/osd/CLASS_CONTRACT.md "Ordering and flags"): the three and two
read-mode CALL vectors, the write-mode CALLs alone, and APPEND after the
write-mode `guard_append`. No Go request was refused locally.

| Leaf | Checked behavior |
| --- | --- |
| `journal_header` | Native `setup` created the image (journal ID = image ID), registered the oracle client and applied the setup events. Go sends the immutable vector, the mutable vector, `client_list("", 256)`, `get_client` of `""`, of the Go client (not yet registered: -ENOENT on both sides) and of the oracle, `get_next_tag_tid` and `tag_list(0, 256, "", Some(0))`; every per-op result and output equals the native librados `exec` of the same input byte for byte. Decoded: order 24, splay width 4, pool id -1 (no `rbd_journal_pool`, so data objects live in the header's pool), minimum and active set 0, two clients: `""` with `ImageClientMeta` (struct version 2, tag class 0, no resync) and a four-position commit, the oracle with empty data and position; tag 1 (class 0, mirror uuid `""`, predecessor not valid: librbd's first lock acquisition had no commit yet); next tag tid 2. `rbd journal info --format json` (journal ID, header OID, object prefix, order, splay width, no object pool) and `rbd journal status --format json` (sets; per client the ID, the data parsed from its hexdump, the state `connected` and every commit position) equal the Go decoding |
| `journal_entries` | Go reads `journal_data.<pool>.<id>.0` to `.7` with STAT+READ: objects 0 to 3 equal the native librados reads byte for byte, object set 1 is absent on both sides. Go walks every object: five entries of tag 1 with entry tids 0 to 4 at their splay offsets, each preamble, version and seed-0 CRC-32C valid, payloads `EventEntry` version 5: four AioWrite events whose offset, length and data equal the native writes (the test-only pattern) and one AioDiscard of 192 KiB+64 KiB with skip-partial-discard set and granularity 65536, each timestamp within a second of its native librbd call. `rbd journal inspect --verbose` replays the same five entries in the same order (tag, commit tid 1 to 5, event type, offset, length, granularity, timestamp to the microsecond) with 0 errors. The image client's stored commit position equals the commit rule applied to the decoded entries |
| `go_client_commit` | Go registers `ceph-msgr-mirror-<token8>` with `ClientData{MirrorPeerClientMeta{image id, REPLAYING}}` (`journal.client_register`, write mode). `get_client` shows the data and an empty commit position (the minimum of the connected clients). Go commits the position a consumer that replayed every setup entry would (`client_commit`), equal to the image client's. Native `observe-registered`: the exec outputs, `rbd journal status` (now three clients: the Go client's hexdump-decoded data, `connected`, the committed positions) and the image read all match |
| `header_watch_native_write` | Go watches `journal.<id>` (timeout 30 s). Native `write` opens the image, writes and discards, and closes it: Go receives two notifications from the native writer's GlobalID with empty payloads and acknowledges each with an empty payload. Afterwards: tag 2 with predecessor (tag 1, entry 4) valid, three new entries of tag 2 (entry tids 0 to 2, two AioWrite, one AioDiscard of object 2) equal to the native events, the tag 1 entries unchanged; the image client's commit position equals the commit rule over the new entries; the Go client did not move. Go commits the same rule over the new entries; native `observe-committed` shows the Go client's position equal to the image client's. Go unregisters its client (`client_unregister`), and `get_client` then returns -ENOENT |
| `go_append_under_lock` | Go watches `rbd_header.<id>` and takes the image's managed lock (`lock.lock`, cookie `auto <watch cookie>`, tag `internal`); `lock.get_info` names Go. As librbd does after acquiring the lock, Go reads the image client (unchanged), reads `get_next_tag_tid` (3) and sends `tag_create(3, class 0, TagData{"" , {"" , true, 2, 2}})`; a repeated `tag_create` returns -EEXIST and changes nothing. Go appends two AioWrite entries of tag 3 in librbd's format: entry 0 (40 KiB+4 KiB) to object 0 with `[journal.append(2^24, entry)]`, entry 1 (100 KiB+4 KiB) to object 1 with `[journal.guard_append(2^24), APPEND entry]`; each object is then exactly its previous bytes followed by the 4173-byte entry. Go releases the lock and its watch. Native `inspect`: the lock is free, no header watcher is left, the image is unchanged (Go wrote no data object), `rbd journal inspect` replays ten entries with 0 errors, the last two being Go's (tag 3, AioWrite, Go's offsets, lengths and timestamps); `rbd journal status` shows the image client unchanged and the oracle. Native `replay`: librbd acquires the exclusive lock (`is_exclusive_lock_owner` true), opens the journal and replays Go's entries; the image now holds Go's bytes at both extents and equals the model, the image client's commit position is the commit rule over Go's entries (front object 1, tag 3, entry 1), librbd allocated tag 4 with predecessor (3, 1), and `rbd journal inspect` still replays ten entries with 0 errors |

## Results

Both selected configurations passed on 2026-10-16 (UTC): base 5/5, clone
5/5, journal 5/5, exclusive-lock 7/7 and write 4/4 subtests in each, no
skips, no retries. The tested source is commit `2728bb9` (tree
`db222a9894ba5777d1a7058df56bda291fdd06dd`): the test, fixture, CI and tool
files of this change on `323ba55`, without these documents. Exported with
`git archive` to a separate directory: 1253 files, SHA-256 of the sorted
per-file SHA-256 list (`find . -type f -print0 | LC_ALL=C sort -z | xargs -0
shasum -a 256`, run in the export root)
`800ba7a96fc2559d4d9b69fda8a958ccecfdd23386b2304ee4387ef56e4f9c24`. In the
export: `gofmt -l .` empty; `go vet ./...` and `go test -count=1 ./...`
(product module, `GOWORK=off CGO_ENABLED=0`); `go -C integration/fixture vet
-mod=readonly ./...` and `test -count=1 -mod=readonly ./...`;
`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools -p
'test_*.py'` (363 tests) all passed.

Image `quay.io/ceph/ceph:v20.2.4`, daemon version `ceph version 20.2.4
(7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`, provider
`github.com/JSYoo5B/ceph-testcontainers-go` v0.2.0, Docker Desktop on Darwin
arm64 (4 CPUs, 4 GB, shared with another agent's cluster during the runs).
Topology: three MON, two MGR, one OSD, the native oracle container. Command
from the export root (the second configuration changes the variables shown
in the table):

```sh
GOWORK=off CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=1 CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 CEPH_MSGR_TEST_RBD=1 \
CEPH_MSGR_TEST_RUN='^TestCephRBD(Base|Write|ExclusiveLock|Clone|Journal)Integration$' \
CEPH_MSGR_TEST_DIAGNOSTICS=<dir> sh integration/run.sh
```

| Configuration | Controller wall time (UTC) | Base | Clone | Journal | Exclusive-lock | Write |
| --- | --- | --- | --- | --- | --- | --- |
| Darwin arm64 host runtime, IPv4, CRC, AES client and service keys, single BlueStore OSD | 04:34:11 to 04:35:30 (79 s) | 5.39 s | 2.91 s | 1.70 s | 0.79 s | 2.18 s |
| CGO=0 Linux arm64 container runtime, IPv6, secure, AES256K client and service keys, single MemStore OSD | 04:36:07 to 04:37:26 (79 s) | 5.33 s | 3.03 s | 1.36 s | 0.87 s | 2.24 s |

Journal subtests (seconds, CRC / secure): header 0.01 / 0.00, entries
0.01 / 0.00, Go client commit 0.21 / 0.21, header watch and native write
0.43 / 0.41, Go append under the lock 0.74 / 0.42 (Go held the lock 11.7 /
3.5 ms). Native action durations (ms, CRC / secure): setup 206 / 174,
observe-registered 122 / 130, write 158 / 137, observe-committed 141 / 126,
inspect 240 / 160, replay 160 / 167 (lock acquisition with the replay of the
two Go entries 5.4 / 3.5 ms). Each run recorded 103 Go requests, all transmitted
(none refused locally) at map epoch 26, within transactions 1 to 107; the
four transactions not in the record are the two notify acknowledgements and
one immediately before each `WatchObject` registration.

### Observed values

| Value | CRC/AES IPv4 BlueStore | secure/AES256K IPv6 MemStore |
| --- | --- | --- |
| Pool / image (journal) ID | pool 3, `12ccf0f655d4` | pool 3, `12b7b1156964` |
| Immutable metadata outputs | `18`, `04`, `ffffffffffffffff` | same |
| Image client data | `0201 0d000000 00000000 0000000000000000 00` (19 bytes) | same |
| Setup entries (object: size) | 0: 8347 (entry 0 AioWrite 8269 bytes, entry 4 AioDiscard 78 bytes), 1: 8269, 2: 4173, 3: 8269 | same |
| Image client commit after setup | `[(0,1,4), (3,1,3), (2,1,2), (1,1,1)]` (object, tag, entry) | same |
| After the native write | tag 2 predecessor (1, 4); commit `[(2,2,2), (1,2,1), (0,2,0), (3,1,3)]`, equal for the Go client | same |
| Header notifications | 2, from the native writer's GlobalID, empty payloads, acknowledged | same |
| Go appends | tag 3 (predecessor (2, 2)); entry 0 at offset 16616 of object 0 (`journal.append`, user version 16), entry 1 at offset 12442 of object 1 (`guard_append` + APPEND, per-op `[0, 0]`, user version 9); 4173 bytes each | entry 0 at 16616 (version 26), entry 1 at 12442 (version 22) |
| Repeated `tag_create` | -EEXIST, header user version unchanged (47), replay version advanced to 26'48 | -EEXIST, unchanged (27), 26'28 |
| After the native replay | objects 0: 20789, 1: 16615, 2: 4251, 3: 8269 (replay appended nothing); commit `[(1,3,1), (0,3,0), (2,2,2), (3,1,3)]`; tag 4 predecessor (3, 1); next tag tid 5 | same |
| `rbd journal inspect` | 5, 10 and 10 entries, 0 errors | same |
| Reply flags | 0x4000015 reads, 0x4000014 ENOENT reads, 0x4000025 class writes and the append vectors | same |

No padding appeared after any append: `journal.append` pads to the OSD's
minimum allocation size only when `require_min_compat_client` is octopus or
later, and the fixture's is luminous.

### Native stderr

Every native driver process ended with empty stderr. Each `rbd journal
inspect` run printed exactly one error-level line, `JournalMetadata: <ptr>
failed to locate client: INSPECT`, and exited 0 with 0 errors: the inspector
unregisters its `INSPECT` client while its `JournalMetadata` is still
refreshing after the unregister notification. The test tolerates exactly
this line for `inspect` (three per run, counted in the evidence) and refuses
any other stderr.

### Cleanup verification

Both runs: the journal held the base, clone, lock and write actions and
`native-journal-setup`, `-observe-registered` (`read_only`), `-write`,
`-observe-committed` (`read_only`), `-inspect` and `-replay`, all `applied`;
cleanup removed the pool with the six images (`completed: true`, no error).

## Product findings

- No refusal of a native vector. Every librbd and `rbd journal` form listed
  above was sent as librbd sends it, with one exception below.
- The client cannot express `CEPH_OSD_OP_FLAG_FADVISE_DONTNEED` (or any
  FADVISE operation flag): `OSDOperationFlags` admits only EXCL and FAILOK.
  librbd's compat append form sets FADVISE_DONTNEED on the APPEND, so Go sent
  the APPEND without it. The flag is an OSD cache hint and the bytes and
  results matched; Tentacle's librbd does not use that form here
  (`require_osd_release` tentacle).

## Development observation

In an early run Go kept its image header watch registered (without reading
or acknowledging events) through the native `inspect` and `replay` actions.
librbd's close after the replay then took 10.0 s instead of about 1 ms: its
lock-release notify waited for the unacknowledged Go watcher until its
timeout. The test now unwatches right after unlocking, as a closing librbd
client would, and the `inspect` observation requires no header watcher.

## Not verified

- A Go consumer applying journal entries to the image (replay) or mirroring
  them to another image; only native librbd replayed here.
- Journal object sets beyond set 0, `set_active_set`, `set_minimum_set`,
  journal trimming and object-set advance on `-EOVERFLOW`; client
  `client_update_data`, `client_update_state` (disconnected clients) and
  `rbd journal client disconnect`.
- Events other than AioWrite and AioDiscard (AioFlush, operation events such
  as snapshot or resize, write-same, compare-and-write), writes split across
  several events above the 16 KiB payload limit, and a separate
  `rbd_journal_pool`.
- The `journal.append` padding branch (`require_min_compat_client` octopus
  or later) and erasure-coded journal pools.
- Header-update notifies sent by Go (Go only received and acknowledged
  native ones), and AcquiredLock or ReleasedLock notifies around Go's lock
  (no other image watcher existed while Go held it).
- Concurrent native and Go appenders; Go held the lock only while the image
  was closed natively.
