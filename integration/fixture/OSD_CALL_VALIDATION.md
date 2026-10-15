# O2f object class CALL validation

Run window: 2026-10-15 Asia/Seoul. Product subset: CEPH_OSD_OP_CALL 0x1401 with
caller-supplied class, method and input bytes, raw output and signed per-op
results. [CLASS_CONTRACT.md](../../internal/osd/CLASS_CONTRACT.md) records the
pinned source semantics. Source is Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`; image is
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
which ships `libcls_hello.so` in `/usr/lib64/rados-classes`.

## Independent codec and local boundaries

The first `encode-osd-call` SDK run compiled a fresh wrapper against the same
generated headers and libceph-common as the earlier proofs and used the pinned
`cmpxattr` and three-argument `call` builders. Five triples were encoded and
separately decoded. Go request header/front/data matches all 41/249/27 bytes
for a write-mode [CMPXATTR guard, CALL hello.write_return_data with input
`54 00 ff`]: union `05 11 00 03000000` and payload_len 25. Replies keep the
echoed class/method/input lengths and the OSD output length: read output
(14 bytes, ACK|ONDISK), positive 42 with 18 bytes on the commit path
(ACK|ONDISK, versions 11'20/20), -75 with cleared output and OutputLength 520,
and a pre-execution -95 whose ONDISK-only flags follow `reply_op_error`.
Earlier OMAP success vectors were constructed with ONDISK alone; this vector
follows the pinned on-commit ACK|ONDISK, which the live replies also showed.
Provenance and the offline seq/ACK/priority caveat are in the
[native fixtures](../../internal/osd/testdata/call-native-README.md).

Internal and public synthetic tests cover the source-derived union and data
layout, 255-byte names, empty, overlong and NUL names, missing or unknown mode,
zero or oversized output budgets, FAILOK and EXCL refusal, field isolation in
both directions, a read-mode mtime, CALL after a staged write or DELETE, reads
and CALLs after a CALL, union echo and output-length validation, positive
results, EOVERFLOW-cleared output, pre-execution refusals, budget overflow and
a successful write without ONDISK as uncertain outcomes, and no resend.

## Live native and Go calls

The oracle seeds an owned object with known bytes and reserves absent names.
Each Go CALL is compared with a native C call through `rados_read_op_exec` with
LIBRADOS_OPERATION_RETURNVEC, which exposes the per-op prval and output; read
methods run on the owned object itself and write methods on a separate native
mirror seeded with the Go object's current bytes and xattrs. Complete native
state (bytes, stat2, version, xattrs) is observed before and after each Go
call.

| Actual profile | Observed result |
| --- | --- |
| CRC/AES entity/service, IPv4, Darwin arm64 host relay, three BlueStore/min2 | 18/18 subcases in 15.14s (second run) |
| secure/AES256K entity/service, IPv4, Linux arm64 container, three BlueStore/min2, CGO0 binaries | whole OSD suite 27/27 top-level, CALL 18/18; no FAIL/SKIP |

In every case the Go aggregate, the Go per-op result and the output bytes
equal the native C aggregate, prval and output:

| Case | Result |
| --- | --- |
| `hello.say_hello` with empty / `Tester` / 101-byte input | 0 `Hello, world!` / 0 `Hello, Tester!` / -22 |
| `hello.replay` on the seeded object | 0 with the object bytes |
| Unknown method / class outside the load list | -95 / -1 before execution, per-op 0, ONDISK without ACK |
| `bad_reader` (WR reading) / `bad_writer` (RD writing) | -5 / -5 |
| Read method on an absent object | -2, per-op 0 |
| `write_return_data` with empty input | aggregate and per-op 42, `you might see this`, xattr foo=bar committed like the native mirror |
| `write_return_data` with input | -22 with `too much input data!`, nothing committed |
| `write_too_much_return_data` | -75, output cleared, OutputLength 520, data and xattrs unchanged; the primary's cached object_info reported a newer version and the request mtime |
| `record_hello` on an absent object, then again | 0 creating `Hello, world!` with `said_by` naming the Go client's global ID, then -17 |
| `turn_it_to_11`, then `replay` | `HELLO, WORLD!` like the native mirror, `amplified_by` naming the Go client |
| Read-mode request to `record_hello` | executed as a write, creating `Hello, reader!` |
| client.readonly (`allow r`) `say_hello` | -1 for Go and native; the same session then served STAT |

The first run matched Go and native in every result, including the EOVERFLOW
case, but asserted that the cached object_info advance is exactly +1. Native
mirror writes in the same PG had already advanced the PG's last user version,
so the cached value was larger; the second run admits any newer version with
the request mtime and unchanged data and xattrs.

Native cleanup removed every owned name; owned containers and network were
independently absent afterwards. Logs and diagnostics:
`/tmp/ceph-osd-call-crc-aes-20261015-second` and
`/tmp/ceph-osd-whole-secure-aes256k-container-call-20261015-first`.

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDClassCallIntegration$' \
sh integration/run.sh
```

## Remaining gaps

Compound vectors (a CALL after read-mode CALLs or a CREATE, writes after a
CALL, the librbd forms, and the local refusal of a CALL after a write-mode
CALL with the historical run that motivated it) are recorded in
[OSD_CALL_COMPOUND_VALIDATION.md](OSD_CALL_COMPOUND_VALIDATION.md).
Vectors mixing CALL with explicit guards and later writes, FAILOK CALL,
methods that return -EAGAIN/-EINPROGRESS, positive read-path results (no
stock read method returns one), EC pools, response loss/SIGKILL, classes
beyond `hello`, `lock` and `rbd`, broader profiles and remote CI for this
subset need separate evidence.
