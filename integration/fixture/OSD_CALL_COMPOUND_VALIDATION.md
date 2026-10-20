# Compound object class CALL validation

Run window: 2026-10-16 Asia/Seoul. Subject: the client rule for CALL in a
compound OSD request vector (`internal/osd/operations.go`,
[CLASS_CONTRACT.md](../../internal/osd/CLASS_CONTRACT.md) "Ordering and
flags", `OSDClassInput` in `cephmsgr/osd_call.go`). A CALL is admitted after
guards and reads, after a CREATE without FAILOK (with or without EXCL) and
after read-mode CALLs. A CALL after a write-mode CALL or after any other
staged write is refused before transmission with
`ErrUnsupportedOSDOperation`. Source is Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`; image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
(stock `libcls_hello.so`, `libcls_lock.so`, `libcls_rbd.so`). The single-CALL
record is [OSD_CALL_VALIDATION.md](OSD_CALL_VALIDATION.md); the librbd forms
are recorded in [RBD_LOCK_VALIDATION.md](RBD_LOCK_VALIDATION.md) (section
"Compound class calls").

The refusal of a CALL after a write-mode CALL follows from the historical
run at the end of this record: under an earlier rule that admitted it, a
write method followed by a full-reading method made the OSD repair the
primary copy in a loop without replying. That vector is no longer sent by
any test.

## Oracle

Every admitted Go vector is compared with an independent native librados
client that sends the same operations in the same order, each on its own
disposable mirror seeded with the Go object's current bytes and xattrs (or
left absent):

- a C write op (`rados_write_op_create` exclusive or not,
  `rados_write_op_omap_set2`, `rados_write_op_exec` with a prval per CALL,
  `rados_write_op_operate2` with LIBRADOS_OPERATION_RETURNVEC and an explicit
  mtime). The C and C++ write builders expose no CALL output and no CREATE or
  OMAP result;
- for CALL-only vectors, a C read op (`rados_read_op_exec` with a per-op
  output buffer and prval, RETURNVEC). The OSD classifies the vector by the
  methods' registered flags, so a vector with a WR method executes as a write
  although the message carries only the READ flag.

`tools/osd_omap_write_native.py` provides the CREATE action and read ops of
up to four CALLs. Bytes, xattrs, OMAP, stat2 and user version of the Go
object and of each mirror are read natively before and after. `said_by` and
`amplified_by` values name the requesting client and are compared for
presence only. Refused vectors are checked without a native counterpart: the
`Request` error is `ErrUnsupportedOSDOperation` with a zero transaction and
submission and no operations, the object is unchanged natively, and the same
session then serves a STAT whose transaction is exactly the next TID after
the last transmitted request (the refused vector consumed none).

## Results

Tested tree: HEAD `source-1` plus the complete working tree (the caller's
product rule and README, and this test, fixture and tool work), exported with
`git archive` and every modified and new file copied over: 1200 files,
SHA-256 of the sorted per-file SHA-256 list (`find . -type f -print0 |
LC_ALL=C sort -z | xargs -0 shasum -a 256`, run in the export root)
`43662f91d260151ccff46412bfb83a2ec98e48edc5e581e1932d76d199128b51`, checked
again after all runs. This record, OSD_CALL_VALIDATION.md and
RBD_LOCK_VALIDATION.md are the only files written after the export. In the export: `gofmt -l .` empty, `go vet ./...` and `go
test -count=1 ./...` (`GOWORK=off CGO_ENABLED=0`, 11 packages), the fixture
module's `vet`/`test -mod=readonly`, and the 359 Python tool tests passed.

Selector `^TestCephOSDClassCall(Compound)?Integration$` (the compound test
and the existing CALL acceptance):

| Profile | Controller wall time (UTC, 2026-10-15) | Compound | Existing CALL |
| --- | --- | --- | --- |
| CRC, AES client and service keys, IPv4, Darwin arm64 host runtime, three BlueStore OSDs (size 3/min 2) | 23:38:53 to 23:40:13 | 16/16 in 13.50 s | 18/18 in 15.11 s |
| secure, AES256K client and service keys, IPv6, CGO=0 Linux arm64 container runtime, three BlueStore OSDs | 23:40:13 to 23:41:42 | 16/16 in 16.34 s | 18/18 in 16.52 s |
| secure, AES256K, IPv6, container runtime, one MemStore OSD | 23:41:42 to 23:42:57 | 16/16 in 14.74 s | 18/18 in 15.55 s |

The hello CALL acceptance was defined on the three-BlueStore profile; the
MemStore row is the standard secure/AES256K profile, where the whole legacy
OSD suite (and therefore the compound test) also runs in CI. No recorded run
failed. A first attempt from the previous export of this rule (tree
`13f8f80dee6ba7e2ef52c5f89ee17da53076e535ba2fed51f5e09acea5472ccb`) failed
in the CRC/AES three-BlueStore run at the read-mode `record_hello`, `replay`
leaf with `ceph osd: invalid object request`: the test set a modification
time on a vector of read-mode CALLs, which the client refuses for a read-only
vector. The 14 leaves before it had passed. The test now sets the time only
when the vector has a write-mode CALL or a write operation; the remaining
runs of that export were stopped and all five configurations were run again
from the export above.

### Admitted vectors

Identical in every profile, versions included (object names differ). Main object:
seeded `hello world data` (16 bytes, no xattrs). R and W mark the declared
call mode. Go aggregate, Go per-op results and outputs, the native write-op
aggregate and CALL prvals and (where present) the native read-op aggregate,
prvals and outputs were equal in every leaf:

| Object | Vector | Reply flags | Aggregate | Per-op results | Outputs | Object afterwards |
| --- | --- | --- | --- | --- | --- | --- |
| main | CREATE(EXCL), W `record_hello` | 0x4000025 | -17 | -17, 0 | none | unchanged (version 2) |
| main | R `say_hello`(101 bytes), W `write_return_data` | 0x4000035 | -22 | -22, 0 | none | unchanged, no `foo` |
| main | R `lock.assert_locked`(rbd_lock, exclusive, cookie `ceph-msgr-compound`), W `turn_it_to_11` | 0x4000035 | -16 | -16, 0 | none | unchanged, still lowercase |
| main | R `say_hello`(60 bytes), W `write_return_data` | 0x4000035 | -75 | -75, 42 | op 0 cleared (OutputLength 68), op 1 `you might see this` | data and xattrs unchanged, no `foo`; the primary's cached object_info advanced (version 2 to 27, the request mtime) |
| main | R `say_hello`(), W `write_return_data` | 0x4000035 | 42 | 0, 42 | `Hello, world!`, `you might see this` | `foo=bar` committed (version 33) |
| main | R `say_hello`(50 bytes), W `write_return_data` | 0x4000035 | 42 | 0, 42 | 58 and 18 bytes (76 in total) | committed (version 40) |
| main | W `write_return_data`, OMAPSETVALS `compound-key`=`compound-value` | 0x4000025 | 0 | 42, 0 | `you might see this`, none | `foo=bar` and the OMAP entry committed (version 44) |
| absent | CREATE(EXCL), W `record_hello` | 0x4000025 | -17 | 0, -17 | none | still absent |
| absent | CREATE, W `record_hello` | 0x4000025 | -17 | 0, -17 | none | still absent |
| absent | R `record_hello`, R `replay` | 0x4000015 | -2 | 0, -2 | none | still absent |
| absent | CREATE(EXCL), W `write_return_data` | 0x4000025 | 42 | 0, 42 | none, `you might see this` | created empty with `foo=bar` (version 56) |

The reply flags echo the request: 0x4000025 is RETURNVEC, WRITE, ONDISK and
ACK; 0x4000035 adds READ (a read-mode and a write-mode CALL; the native C
write op carries WRITE only and its read op READ only, and the OSD classifies
CALLs by registration, so the results still equal native); 0x4000015 is a
vector of read-mode CALLs that the OSD executed as a write (ONDISK, as in the
single-CALL acceptance). Every unexecuted operation echoed its union with
OutputLength 0 and no output. The CREATE and OMAPSETVALS results are Go-only
observations; librados exposes none.

What the values show:

- `cls_cxx_stat` inside a method sees the staged CREATE: `record_hello`
  returns -EEXIST after CREATE with or without EXCL, and nothing is created.
- A read-mode declaration does not change what the OSD runs: `record_hello`
  declared read-only still stages its write, and the following `replay`
  reads committed storage, where the object does not exist yet (-ENOENT).
  The client admits this vector only because of the declaration; it is safe
  here because the object is absent.
- The first failing operation aborts the vector; earlier operations report
  their own result (CREATE 0) and later ones 0 without output; nothing is
  committed.
- The aggregate is the last executed operation's return: `write_return_data`
  then OMAPSETVALS gives 0 with op 0 = 42 and both writes committed.
- `osd_max_write_op_reply_len` (64) bounds each operation: 58 + 18 = 76
  bytes succeed. One operation above it gets -EOVERFLOW with its output
  cleared while the other keeps 42 and its output in the refused reply.

### Refused vectors

| Object | Vector (refused before transmission) | Next STAT on the same session |
| --- | --- | --- |
| main | W `lock.assert_locked`, W `turn_it_to_11` | TID 2, code 0 |
| main | W `turn_it_to_11`, R `replay` (the historical repair-loop vector) | TID 6, code 0 |
| main | W `write_return_data`, R `say_hello`() | TID 7, code 0 |
| main | R `say_hello`(), W `write_return_data`, R `say_hello`() | TID 8, code 0 |
| absent | W `record_hello`, R `replay` | TID 14, code -2 |

Each `Request` returned `ceph osd: unsupported operation`
(`ErrUnsupportedOSDOperation`) with transaction 0, a zero submission and no
operations; the next STAT had exactly the TID after the last transmitted
request, so nothing was allocated or sent, and the object's native state was
unchanged. Identical in all three profiles. Read mode on
`lock.assert_locked` admits the same vector (third admitted row).

## Historical run under the earlier rule

These observations come from an earlier tree that admitted a CALL after any
CALL: HEAD `source-1` plus that day's change set, 1200 files, tree SHA-256
`fd6c929b6abc75b2831ce5529d2ce2c11e7525a8526cd95ddfa31c48b99fc333`, runs on
2026-10-15 UTC (CRC/AES IPv4 host runtime with three BlueStore OSDs,
23:09:37 to 23:11:11; secure/AES256K IPv6 container runtime with three
BlueStore OSDs, 23:11:49 to 23:13:22; secure/AES256K IPv6 container runtime
with one MemStore OSD, 23:13:44 to 23:14:49). They motivated the current
refusal. The test that produced the staged-read observation
(`TestCephOSDClassCallStagedReadIntegration`) and the oracle's
`op_timeout` option it used were removed afterwards; no current test sends
these vectors.

All method CALLs were then sent in write mode (reply flags 0x4000025
throughout). Vectors the current test still admits gave the values recorded
above. In addition, Go and native (write op and read op) agreed on two
vectors that are now refused:

| Vector (all write mode) | Object | Aggregate | Per-op results | Outputs | Object afterwards |
| --- | --- | --- | --- | --- | --- |
| `record_hello`, `replay` | absent | -2 | 0, -2 | none | still absent |
| `write_return_data`, `say_hello`() | main | 0 | 42, 0 | `you might see this`, `Hello, world!` | `foo=bar` committed |

The staged full-object read: `hello.turn_it_to_11` then `hello.replay` on an
object with committed `hello world data`, in the three-BlueStore runs only
(the test skipped itself on one replica). `replay`'s SYNC_READ was trimmed to
the staged size (16, equal to the committed size), read from committed
storage and then checked against the staged full-object digest that
`turn_it_to_11`'s WRITEFULL set (`PrimaryLogPG::do_read`). The crc differed,
`do_read` turned it into -EIO and `rep_repair_primary_object` marked the
primary copy missing, recovered it from a replica and parked the request
until the object was readable, where it ran again. No reply was sent while
the client stayed connected.

| Value | CRC/AES IPv4 BlueStore x3 | secure/AES256K IPv6 BlueStore x3 |
| --- | --- | --- |
| Go `Request` (4 s context) | `OutcomeUnknownError` wrapping `context.DeadlineExceeded` after 4002 ms, no reply | after 4003 ms, no reply |
| Native read op (`rados_osd_op_timeout` 4, separate connection) | aggregate -110 after 4000 ms; prvals left at the oracle's 999 sentinel; no outputs | same |
| Cluster log (`log last`, channel cluster) | `full-object read crc 0xb208a78f != expected 0xef0dcd0a` 1119 times for the Go object and 1117 for the mirror; the retained last 512 entries held 341 crc lines and 171 `missing primary copy of ..., will try copies on 0,2` lines | 1123 and 1121 times; 341 and 171 |
| Health afterwards | `OSD_TOO_MANY_REPAIRS`: `osd.1 had 1119 reads repaired` | `osd.1 had 1123 reads repaired` |
| Object afterwards | data, xattrs, mtime and version 99 unchanged (Go object and mirror) | same |

`0xb208a78f` is the crc of the committed `hello world data` and `0xef0dcd0a`
that of the staged `HELLO WORLD DATA`. Two mismatches went with each repair:
a mismatch on a clean PG marked the primary copy missing, and the next one,
while the PG was still recovering, waited for clean (`block_for_clean`)
without logging. Both objects were in the fixture pool's only PG (2.0,
primary osd.1), so about 2240 mismatches gave about 1120 repaired reads.
Closing the Go OSD session (and shutting down the native connection) let the
OSD discard the parked request (`osd_discard_disconnected_ops`); the fixture
was active+clean again within the test. With one OSD `primary_error` finds
no other copy and leaves the object unfound (source-derived, deliberately
not run).

## Contract check

Compared with internal/osd/CLASS_CONTRACT.md "Ordering and flags" and the
`OSDClassInput` comment in cephmsgr/osd_call.go of the tested tree. The
staged stat, the staged read length and digest against committed bytes, the
admitted forms, the local refusals, the first-failure abort with results 0,
echoed unions and no output, the last-executed aggregate and the per-operation
64-byte bound agree with the observations above and with the historical run.
Two sentences need care:

- "so a write method returning 42 followed by a successful read method
  yields aggregate 0 with per-op results 42 and 0": the example vector is one
  the client now refuses (third refused row); the observation comes from the
  historical run only. The admitted form `write_return_data` then OMAPSETVALS
  shows the same rule (0 with 42 and 0).
- "such a method cannot stage writes (it gets -EIO)": a method registered
  read-only that writes does stage the write in the transaction; the OSD
  turns the method's result into -EIO afterwards and, FAILOK on CALL being
  refused, the vector aborts, so no later method runs and nothing commits.
  The conclusion holds; the single-CALL acceptance observed -5 for
  `bad_writer`, and no compound with such a method was sent.

## Not verified

- xattr and OMAP reads inside a later method (no stock `hello` method reads
  them); a later method seeing an earlier operation's staged xattr or OMAP
  change is source-derived only.
- Guards before a CALL in a live compound (only the offline native encoder
  vector [CMPXATTR, CALL] in OSD_CALL_VALIDATION.md).
- The staged-read loop on one replica (unfound object), on MemStore and on
  EC pools; other classes whose methods combine a full write and a full read.
- A read-mode declaration of a data-writing method before a full-reading
  method on an existing object: the client admits it and it would reproduce
  the loop; it is not sent.
- FAILOK CREATE or other staged writes before a CALL (refused locally), FAILOK
  CALL, methods returning -EAGAIN/-EINPROGRESS inside a compound, response
  loss or an OSD failure during a compound.
- A native per-op result for CREATE and OMAP set (librados exposes none) and
  native CALL outputs of vectors containing CREATE or OMAP set.
- CI rows on GitHub runners, Windows and other profile combinations.

Logs and diagnostics are in the session scratchpad:
`diag-4-call-a`, `diag-4-call-b3`, `diag-4-call-bm`
(`osd-call-compound-results.json`, `osd-call-results.json`) for the current
runs, and `diag-call-crc-aes-1`, `diag-call-secure-aes256k-bluestore3-1`,
`diag-call-secure-aes256k-memstore-1` for the historical ones.

```sh
GOWORK=off CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 \
CEPH_MSGR_TEST_RUN='^TestCephOSDClassCall(Compound)?Integration$' \
CEPH_MSGR_TEST_DIAGNOSTICS=<dir> sh integration/run.sh
```
