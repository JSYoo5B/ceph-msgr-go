# RBD base Messenger I/O acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go,
without CGO, librados, librbd, go-ceph, Ceph CLI subprocesses or native
runtime dependencies, and it gains no RBD API: this acceptance uses only the
existing explicit OSD API (`OpenOSD`, `Request` with `OSDCall`, `OSDStat`,
`OSDRead`, `OSDSparseRead`, `OSDListSnapshots`, `OSDListWatchers` and
`ReadSnapshot`, `WatchObject`, `Ping`, `Acknowledge`, `Unwatch`). Image
layout, class input encodings, class output decoding and data-object naming
are test-only consumer code in `integration/rbd_base_test.go`. The disposable
provider, the native librbd/librados driver and the owned pool are
development-only; see [RBD_BASE_FIXTURE.md](RBD_BASE_FIXTURE.md).

This is a foundation check in the sense of SPEC.md: native librbd prepares
the data, Go reads and observes it through the Messenger OSD path, and native
librbd and librados are the independent oracles. It does not establish RBD
functional equivalence of a consumer implementation.

## Test shape

`TestCephRBDBaseIntegration` runs only with `CEPH_MSGR_TEST_RBD=1` and
`CEPH_MSGR_TEST_OSD=1` (single OSD, `legacy-v6`); the selector is
`^TestCephRBDBaseIntegration$`. One Go client authenticates as
`client.rbd-base-<token>` with RBD's capability profile (MON `profile rbd`,
OSD `profile rbd pool=<owned pool>`), the key type of the selected profile
and `EnableOSD` with the default `OSDProfileObjectHash`. Every object request
uses caller-side placement from the MON `osd map` of that object (pool, raw
object hash as `OSDObject.Hash`, epoch, acting primary) and the primary's v2
address from `osd dump`, the test-only helpers of the CephFS recovery and OSD
command acceptances. Each reply must carry a nonzero transaction equal to
its submission, the client's GlobalID, the object name, the raw-hash PG echo
`{pool, hash}`, a map epoch not older than the request's and retry 0. Every
request is recorded with its placement, submission, identity, epochs, flags,
user version and per-operation results (raw outputs up to 512 bytes, SHA-256
otherwise) in `rbd-base-results.json`.

| Leaf | Checked behavior |
| --- | --- |
| `header_class_calls` | CALL `rbd.get_id` on `rbd_id.<image>` gives the image ID; on `rbd_header.<id>` CALL `get_snapcontext` (snapshot ID taken from its output), `get_size` at CEPH_NOSNAP and at the snapshot, `get_object_prefix`, `get_features` (CEPH_NOSNAP, read_only false), `get_snapshot_name` and `snapshot_get`, each alone in its request with mode read. Per-op result 0 and the echoed class, method and input lengths. Each Go input and raw output equals the native librados `exec` of the same method on the same object byte for byte; the test-only decodings equal native librbd `id`, `stat` (order, size, block name prefix), `features` and `list_snaps` (ID, name, size, user namespace), with child count 0 and a nonzero timestamp |
| `data_objects_at_head` | Object names from the Go prefix and order equal the native layout. Per object (eight): STAT and READ of the whole object range. Existing objects: STAT size and READ bytes equal native librados `stat`/`read`, and the bytes padded with zeros to the object size equal native librbd's image extent. Absent objects: ENOENT (-2) for the vector and zeros in librbd's extent. Existence matches the written image extents and the native pool listing |
| `sparse_read` | SPARSE_READ of the whole object range for each existing object, and of each hole of the image extents alone. Extents are ordered, positive and within the range; their bytes expanded with zeros equal native librados bytes. BlueStore: the extents are exactly the written ranges (holes omitted). MemStore: one extent from the range start to EOF (`MemStore::fiemap`) |
| `snapshot_reads` | STAT and READ with `ReadSnapshot` set to the snapshot ID, compared as above with native librados reads under `set_read(snap)` and native librbd reads of the snapshot. LIST_SNAPS of each existing object: the head entry's size equals the native head size; an object written before and after the snapshot lists one clone with that ID, that snapshot, the native snapshot size and the overlap left by the post-snapshot writes, and sequence = snapshot ID; an object created after the snapshot lists only its head with sequence = snapshot ID (and reads ENOENT at the snapshot); an untouched object lists only its head with sequence 0 |
| `header_watch_notify` | LIST_WATCHERS of the header is empty. Go `WatchObject` (timeout 60 s) and LIST_WATCHERS shows only it. The fixture's `watch` action opens the image natively; LIST_WATCHERS then shows two client watchers, Go's and librbd's (global ID from the driver), and each equals an entry of librbd `watchers_list` (ID, cookie, `ip:port/nonce` suffix). After a ping and the first proceed signal, librbd resizes the image to 640 KiB: Go receives one notification from the librbd client whose payload is librbd's `NotifyMessage(HeaderUpdatePayload)` and acknowledges it with `ResponseMessage(0)`; the acknowledged resize completes in less than librbd's 5 s notify timeout. After the second signal, librbd resizes to 768 KiB; Go receives a second notification with a new notify ID and does not acknowledge it; that resize takes at least the timeout (minus 100 ms of clock slack), and the native stderr holds exactly one `update notification timed-out`. The native receipt is complete, its mutation applied and its final stat 768 KiB with the snapshot unchanged. LIST_WATCHERS after librbd's close shows only Go; Go CALL `get_size` at the head and at the snapshot again equals the native exec outputs; Go `Unwatch` leaves an empty listing |

## Results

The later [pending-notification checkpoint](RBD_NOTIFY_PENDING_VALIDATION.md)
adds raw head/snapshot size observations before the unacknowledged native
resize publishes completion. The historical runs below did not contain
that checkpoint and are not evidence for its intermediate-state assertion.

Both selected configurations passed on 2026-10-16 (KST), all five subtests,
no skips, no retries. The tested source is HEAD
`source-1` plus only this change set,
exported to a separate directory with `git archive` (modified and new files
copied over). This record is the only file of the change set written after
the runs. The export holds 1189 files; the SHA-256 of its sorted per-file
SHA-256 list (`find . -type f -print0 | LC_ALL=C sort -z | xargs -0 shasum
-a 256`, run in the export root) is
`209cb731f972e1d4f9808fd1cb5d97fed573c59cec2f688838f1f872880df0ab`, checked
again before the second run. Source SHA-256:

| File | SHA-256 |
| --- | --- |
| `integration/rbd_base_test.go` | `bfeb1a77ea112e2d56a81073046c8601d5e836fc8b561306b56087ae28c4de8c` |
| `integration/fixture/rbd_base.go` | `fc4ffaa1c588adc7b1f554501d4eaf9f94d45cee96c14470d4769c7808733ac9` |
| `integration/fixture/rbd_base_test.go` | `ba925e61eb9f1499fbdbae98864b5171525f06ecfcffe1b0e8ab73b10ba5f330` |
| `integration/fixture/profile.go` | `c65277344b0398528cedd7d60d5b1faca520e42fd79e4302de92b411c9dba2c1` |
| `integration/fixture/profile_test.go` | `d5e7ca937511dd3de92c80271edbcb8c5f0e504c7ef3f3ceddb3ccc3cfbf7fd7` |
| `integration/fixture/provider.go` | `7630393b0e8139d182a7e8406379960af5181dbd5763cac3526fbf4007d4aa78` |
| `integration/fixture/control.go` | `ffce60242348ace39918801897295fe904dd8b946afc58405949bde6844b621f` |
| `integration/fixture/main.go` | `9e1046a54e023e950c591e4f0bf89cbe06bd05480373c82ff607fbd6e74514c7` |
| `integration/fixture/diagnostics.go` | `cb185e0efb9943414d11229dfb53622a9494227eb646f58d79e5bcaa8ed9f185` |
| `integration/fixture/RBD_BASE_FIXTURE.md` | `6dd1ffdf1036f3f0c93d8afaf4638e4257fd58e83367e71001920bd6e388d390` |
| `.github/workflows/ci.yml` | `46ea9aa9d0691f1d9fd40e5fde8c97743a868f2a4f6f706a16cc925f5ab6960b` |
| `tools/test_ci_osd_profiles.py` | `54023a1dde2bdc87b66a8c861a94db7ed2f9900113efb39eadb8c1cbc0866a9e` |

In the export, before the live runs: `gofmt -l .` empty; `go vet ./...` and
`go test ./...` (product module, `GOWORK=off CGO_ENABLED=0`); `go -C
integration/fixture vet -mod=readonly ./...` and `test -mod=readonly ./...`;
`python3 -m unittest discover -s tools -p 'test_*.py'` (357 tests) all
passed. In the working tree, `go test -race` of `./integration` and of the
fixture's profile and RBD tests also passed.

Image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
(arm64, Docker Desktop), daemon version
`ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`,
provider `github.com/JSYoo5B/ceph-testcontainers-go` v0.2.0. Topology: three
MON, two MGR, one OSD, the native oracle container. Command (from the export
root; the second configuration changes the variables shown in the table):

```sh
GOWORK=off CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=1 CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 CEPH_MSGR_TEST_RBD=1 \
CEPH_MSGR_TEST_RUN='^TestCephRBDBaseIntegration$' CEPH_MSGR_TEST_DIAGNOSTICS=<dir> \
sh integration/run.sh
```

| Configuration | Controller wall time (UTC, 2026-10-15) | Native setup | Test | Result |
| --- | --- | --- | --- | --- |
| Darwin arm64 host runtime, IPv4 relay, CRC, AES client and service keys, single BlueStore OSD | 20:54:13 to 20:55:24 (71 s) | 1.11 s | 5.29 s | 5/5 passed |
| CGO=0 Linux arm64 container runtime, IPv6, secure, AES256K client and service keys (`KEY_TYPE`/`SERVICE_CIPHER` aes256k, `CONNECTION_MODE` secure, `IP_FAMILY` 6, `RUNTIME` container, `OSD_STORE` memstore), single MemStore OSD | 20:55:33 to 20:56:44 (71 s) | 1.08 s | 5.33 s | 5/5 passed |

Subtest durations (CRC / secure, seconds): class calls 0.02 / 0.00, head
reads 0.01 / 0.00, sparse reads 0.01 / 0.00, snapshot reads 0.01 / 0.00,
watch and notify 5.21 / 5.31 (the native watch action took 5.18 s in both,
of which about 5.0 s is the deliberately unacknowledged notify). Each run
made 48 recorded Go requests (transactions 1 to 49 without duplicates;
number 38 became the watch cookie) and one native control request.

### Observed values

| Value | CRC/AES IPv4 BlueStore | secure/AES256K IPv6 MemStore |
| --- | --- | --- |
| Owned pool | ID 3, flags 8192 (`selfmanaged_snaps`), no HASHPSPOOL; the other pools `.mgr` (1) and `ceph-msgr-osd-fixture` (2) also flags 0 | same |
| Go client | `client.rbd-base-7d386e1b…`, GlobalID 4743 | `client.rbd-base-1b2ebab7…`, GlobalID 4755 |
| Native setup / watch clients | GlobalID 14597 / 14608 | 14597 / 14602 |
| Image | ID `3905fd86b96d`, prefix `rbd_data.3905fd86b96d`, order 16, 512 KiB, 8 objects, features 1, op_features 0, flags 0, stripe 65536 x 1, data pool 3, no parent | ID `39058bfbe662`, otherwise the same |
| Snapshot | ID 3, `ceph-msgr-snap-7d386e1b`, 524288 bytes, user namespace | ID 3, `ceph-msgr-snap-1b2ebab7`, same |
| Request map epochs | all requests and replies 24 | 23 until the native watch action ended; the OSDMap then reached 24: the closing LIST_WATCHERS, UNWATCH and final LIST_WATCHERS kept the leaf's placement epoch 23 and were answered with 24, the two fresh `get_size` placements used 24 |
| Reply flags | 0x4000015 for reads, CALLs and LIST_*, 0x4000014 for the ENOENT vectors, 0x4000025 for WATCH | same |

Raw class outputs (CRC/AES run; the secure run differs only in the image
ID, snapshot name and timestamp bytes). Every Go output equaled the native
exec output of its own run:

| Method (input) | Output |
| --- | --- |
| `get_id` (empty) | `0c000000333930356664383662393664` (`3905fd86b96d`) |
| `get_size` (`feffffffffffffff`) | `10` `0000080000000000` (order 16, 524288) |
| `get_size` (`0300000000000000`) | same |
| `get_object_prefix` (empty) | `15000000` + `rbd_data.3905fd86b96d` |
| `get_features` (`feffffffffffffff00`) | `0100000000000000` `0100000000000000` (features 1, incompatible 1) |
| `get_snapcontext` (empty) | `0300000000000000` `01000000` `0300000000000000` (seq 3, snaps [3]) |
| `get_snapshot_name` (`0300000000000000`) | `17000000` + `ceph-msgr-snap-7d386e1b` |
| `snapshot_get` (`0300000000000000`) | `0101 41000000`, ID 3, namespace `0101 04000000 00000000`, name, size 524288, timestamp `aa3dd16a dd447118` (1792097706.410076381), child count `00000000` |
| after the resizes: `get_size` head / snapshot | `10 00000c0000000000` (786432) / `10 0000080000000000` (unchanged) |

Data objects (both runs; the bytes differ per run because the patterns are
token-bound, and each run's Go bytes equaled that run's native bytes):

| Object | Head | Snapshot | LIST_SNAPS (sequence; clones) |
| --- | --- | --- | --- |
| 0 | 65536 | 65536 (clone) | 3; clone 3 [3] size 65536 overlap 0+32768; head 65536 |
| 1 | 32768 | 32768 (clone) | 3; clone 3 [3] size 32768 overlap 8192+24576; head 32768 |
| 2 | 49152 | 49152 (head) | 0; head 49152 |
| 3 | 4096 | ENOENT | 3; head 4096 |
| 4 | 20480 | 20480 (head) | 0; head 20480 |
| 5 to 7 | ENOENT | ENOENT | not requested |

The native views: librbd image head and snapshot 524288 bytes each,
librados objects 172032 bytes at the head and 167936 bytes at the snapshot.

SPARSE_READ:

| Object, range | BlueStore extents (bytes) | MemStore extents (bytes) |
| --- | --- | --- |
| 0, 0+65536 | 0+65536 (65536) | 0+65536 (65536) |
| 1, 0+65536 | 0+32768 | 0+32768 |
| 2, 0+65536 | 0+8192, 40960+8192 (16384) | 0+49152 (49152) |
| 2, hole 8192+32768 | none (0) | 8192+32768 of zeros |
| 3, 0+65536 | 0+4096 | 0+4096 |
| 4, 0+65536 | 16384+4096 (4096) | 0+20480 (20480) |
| 4, hole 0+16384 | none (0) | 0+16384 of zeros |

Header watch and notify:

| Value | CRC/AES IPv4 | secure/AES256K IPv6 |
| --- | --- | --- |
| Go watch | cookie 38, timeout 60, address type 2 (MSGR2) `172.19.0.9:0/3974648288` | cookie 38, type 2, `[fd62:93fe:2c9e::9]:0/814179570` |
| librbd watch | GlobalID 14608, cookie 281473231959072, timeout 30, address type 3 (ANY) `172.19.0.9:0/412960194` | 14602, cookie 281473030631120, timeout 30, type 3 `[fd62:93fe:2c9e::9]:0/324757077` |
| librbd `watchers_list` | the same two (ID, cookie, address) | same |
| Notifications | notify IDs 103079215107 and 103079215108 (epoch 24 in the high word), notifier 14608, return code 0, session generation 1, payload `07010400000003000000`, no data segment | 98784247811 and 98784247812 (epoch 23), notifier 14602, same payload |
| Go acknowledgement | NOTIFY_ACK with `01010400000000000000`, transaction 43, result 0 | same |
| Acknowledged resize (librbd) | 1.723 ms | 0.905 ms |
| Unacknowledged resize (librbd) | 5.002 s | 5.002 s |
| Native stderr | one line `librbd::Operations: update notification timed-out` | same |

Raw MWatchNotify front of the first notification (CRC run),
`01012600000000000000070000000000000003000000180000000a00000007010400000003000000000000001039000000000000`:
message version 1, opcode 1 (notify), cookie 38, obsolete version field 7,
notify ID `0x0000001800000003`, a 10-byte payload (librbd's message), return
code 0 and notifier GlobalID 14608.

### Cleanup verification

Both runs: journal `pool-create`, `pool-init`, `native-setup` and
`native-resize` all `applied`; cleanup removed the pool, found neither its
name nor its ID in a fresh native OSDMap, and deleted both identities
(`completed: true`, no error). The fixture's other containers were removed
by the provider as in the other profiles.

## Earlier attempts

Four development runs failed before the recorded ones, all in the new
fixture or test code, none in the product:

1. Native setup: Python's `image_info_t.parent_name` is a fixed `char`
   array and reached the driver as `bytes`, which `json.dumps` refused. The
   driver now decodes such text fields.
2. The fixture refused the setup output: librbd reports "no parent" as an
   unsigned `parent_pool` (18446744073709551615), not -1.
3. The test-only `SnapshotInfo` decoder read an 8-byte child count; the Go
   and native outputs were already byte-identical, and the pinned
   `cls_rbd_types.h` declares `uint32_t child_count`.
4. The watch leaf missed the transient `resized-unacknowledged` phase
   because the driver had already published `closed`; the native side had
   completed correctly (1.6 ms and 5.003 s resizes, one timed-out line). The
   test now looks for the phase in the cumulative phase list.

## Findings and limits

No product defect and no missing wire capability was found for these
leaves: class CALL inputs and outputs, head and snapshot reads, SPARSE_READ,
LIST_SNAPS, LIST_WATCHERS, WATCH, ping, NOTIFY_ACK and UNWATCH carried what
librbd needs and matched the native oracles byte for byte. Observations:

- The Go client's watch address is type 2 (MSGR2) while librbd's is type 3
  (ANY). Both are accepted and listed by the OSD; nothing depends on it here.
- SPARSE_READ is a head-only operation in the product contract, so sparse
  reads at a snapshot were not attempted.
- The OSDMap advanced once during the secure run's watch leaf; requests kept
  their caller-supplied epoch and the replies reported the newer one. No
  epoch handling beyond that is claimed.

Not verified here:

- the replicated-PG profiles (explicit PG, HASHPSPOOL), three replicas, EC
  data pools and IPv6 host or IPv4 container combinations other than the two
  recorded;
- images with exclusive-lock, object-map, fast-diff, deep-flatten or
  journaling, larger orders, fancy striping, a separate data pool,
  namespaces, clones/parents/copy-up, group or mirror snapshots;
- Go as a writer (data writes with an image SnapContext, header mutations,
  `rbd` write methods, the `lock` class), Go as a notifier to librbd (for
  example lock requests answered by librbd), watch reconnect or watch
  timeout handling with librbd, renewal, cancellation, concurrency and
  daemon failures in this RBD context;
- the two new CI rows on GitHub runners (added, not executed), Windows, and
  long-running operation.

## ceph-testcontainers-go v0.3.0 key input

Local runs on 2026-10-16 from clean `git archive` exports, darwin/arm64,
Go 1.27.2, a compatible Docker engine, Ceph image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a4...`, ceph-testcontainers-go
v0.3.0. Configuration (a): aes/aes, CRC, IPv4, runtime `host`, BlueStore.
Configuration (b): aes256k/aes256k, secure, IPv6, runtime `container`,
MemStore. Both use the legacy-v6 single-OSD fixture.

The native-only keys reach their workers as one bounded
`NativeCommand.Input` record (`native_key_input.go`); no native keyring is
written to the host control directory. Every worker run checks
`InputWritten`, `InputDelivered`, `InputEOF` and the worker's byte-count
receipt separately from `Complete()`. The attach path ran from the host
fixture into the client container under both runtimes.

The lock and journal drivers' `rbd` CLI reads a 0600 keyring in a private
directory of the client container that the driver removes at exit.

| Export | Configuration | Result | Wall time |
| --- | --- | --- | --- |
| `ff5e7c8` | (a) | PASS: Base, Clone, Journal, ExclusiveLock, Write | 82 s |
| `ff5e7c8` | (b) | PASS: Base, Clone, Journal, ExclusiveLock, Write | 75 s |
