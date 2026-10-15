# O2e OMAP range removal validation

Run window: 2026-10-15 Asia/Seoul. Product subset: OMAPRMKEYRANGE 0x222c with
`RangeBegin` (inclusive) and `RangeEnd` (exclusive) byte strings, alongside the
existing OMAP reads, header, comparison and writes.
[OMAP_CONTRACT.md](../../internal/osd/OMAP_CONTRACT.md) records the pinned
source semantics. Source is Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`; image is
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Independent codec and local boundaries

The first `encode-osd-omap-range` SDK run compiled a fresh wrapper against the
same generated headers and libceph-common as the earlier OMAP proofs and used
the pinned `omap_rm_range` builder (Objecter.h:1395-1401). Five triples were
encoded and separately decoded. Go request header/front/data matches all
41/293/29 bytes for three removals: a binary forward range, an empty range with
FAILOK and an empty-begin range ending at `0xff`, with input lengths 12/8/9 and
WRITE|ONDISK|RETURNVEC. Replies keep the echoed lengths and no output for
success, ENOENT, FAILOK -2 before a committed SETVALS, and the logical EC-pool
EOPNOTSUPP shape. Provenance and the offline seq/ACK/priority caveat are in the
[native fixtures](../../internal/osd/testdata/omap-range-native-README.md).

Internal and public synthetic tests cover the source-derived literal
(`02000000 6200 02000000 63ff` with extent length 12), empty and forward
ranges, refusal of a reversed range (including a begin that extends the end by
a NUL byte) before TID allocation, field isolation in both directions, the
output budget, an OMAP read after a staged range removal, union echo and
no-output reply validation, known ENOENT, and a contradictory reply as an
uncertain outcome.

## Live native and Go range removal

The native C oracle seeds an owned object with nine keys including the empty
key, a NUL-prefixed key and a 0xff key, and reserves two absent names. Each Go
range removal is mirrored on a separate native object by
`rados_write_op_omap_rm_range2` with the same explicit mtime; aggregates and
final inventories are compared, and the header is observed through the pinned
rados CLI.

| Actual profile | Observed result |
| --- | --- |
| CRC/AES entity/service, IPv4, Darwin arm64 host relay, three BlueStore/min2 | 8/8 subcases in 6.65s on the first run; OMAP writes 17/17 and comparison 25/25 passed in the same run |
| secure/AES256K entity/service, IPv4, Linux arm64 container, three BlueStore/min2, CGO0 binaries | whole OSD suite 26/26 top-level, range removal 8/8; no FAIL/SKIP |

| Case | Observation |
| --- | --- |
| `[b, c)` on keys `b`, `b\x00x`, `b0`, `c` | the three keys below `c` removed, `c` kept, binary header retained; native mirror equal |
| `[c, c)` | executed: version and mtime advanced, inventory unchanged |
| `["", a)` | the empty key and `\x00k` removed |
| `[c, \xff\xff)` | `c`, `c0` and the `\xff` key removed |
| Absent object | aggregate and per-op -2, object stays absent; native mirror -2 |
| FAILOK range removal on an absent object, then SETVALS | per-op -2/0, aggregate 0, object created |
| Reversed `[z, a)` | native mirror aggregate 0 with unchanged inventory on a small object; Go refused before transmission |

Native cleanup removed every owned name; owned containers and network were
independently absent afterwards. Logs and diagnostics:
`/tmp/ceph-omap-range-crc-aes-20261015-first` and
`/tmp/ceph-osd-whole-secure-aes256k-container-range-20261015-first`.

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDOmapRangeRemovalIntegration$' \
sh integration/run.sh
```

## Remaining gaps

The DeleteRange path above `rocksdb_delete_range_threshold` keys, EC pool
EOPNOTSUPP refusal, response loss/SIGKILL, read-only credentials, broader
profiles and remote CI for this subset need separate evidence.
