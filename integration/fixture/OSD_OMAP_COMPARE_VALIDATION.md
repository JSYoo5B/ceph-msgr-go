# O2d OMAP comparison validation

Run window: 2026-10-15 Asia/Seoul. Product subset: OMAP_CMP 0x1219 with EQ=1,
GT=3 and LT=5 assertions, alongside the existing OMAP reads, header and writes.
RMKEYRANGE, staged reads, snapshots, EC and automatic pagination remain
unsupported. [OMAP_CONTRACT.md](../../internal/osd/OMAP_CONTRACT.md) records
the pinned source and wire semantics.

All daemon lifecycle uses public ceph-testcontainers-go v0.2.0. The native C
oracle (length-aware `rados_read_op_omap_cmp2`/`rados_write_op_omap_cmp2` with
per-op prval), the pinned rados CLI header reader, the C++ encoder and the
controller live outside product dependencies. The product still builds and runs
with CGO0. Source is Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`; image is
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The pool is replicated, legacy placement/HASHPSPOOL disabled, PG count 1, three
sparse-file BlueStore OSDs with min_size 2; the feature mask is unchanged.

## Independent codec and local boundaries

The first `encode-osd-omap-cmp` SDK run compiled a fresh wrapper against the
same generated headers and libceph-common as the earlier OMAP proofs. A named
`std::map<std::string, std::pair<bufferlist,int>>` selects the map builder.
Five triples were encoded and separately decoded. Go request header/front/data
matches all 41/253/76 bytes even though Go receives the four assertions in an
unsorted order; the native map order is `""`, `00`, `6280`, `7a`. Replies keep
union input lengths and no output for success, -125 cancel with the later
SETVALS result 0, FAILOK -125 then 0, and the handler's own ENOENT. Provenance
and the offline seq/ACK/priority caveat are in the
[native fixtures](../../internal/osd/testdata/omap-cmp-native-README.md).

Internal and public synthetic tests cover field exclusivity (including empty
slices on other opcodes), duplicate keys, identifiers 0/2/4/6/7/-1 and large
values refused before TID allocation, source literal encoding, a zero output
budget, comparison after a staged OMAP write or DELETE refused, compare-before-
write admitted, known -125/-2 refusals, FAILOK retention, contradictory replies
failing the session as OutcomeUnknownError, and no replay or reconnect. Python
oracle tests cover cmp admission, prval reporting and the RETURNVEC flag; they
are not daemon evidence.

## Live native and Go comparisons

The oracle exclusively creates one UUID-suffixed owned object with nine OMAP
values chosen for signed-char boundaries and reserves one absent name. Callers
obtain native MON map/dump identity and explicitly open the primary endpoint.
Read-only cases run the same single-assertion vector through the native C API
on the owned object, then Go, with complete native state observed before and
after. Write cases compare Go with a native C vector on a separate mirror seeded
with the current model and the same explicit mtime; native write vectors pass
LIBRADOS_OPERATION_RETURNVEC like the Go client.

| Actual profile | Observed result |
| --- | --- |
| CRC/AES entity/service, IPv4, Darwin arm64 host relay, three BlueStore/min2 (arm64 OSD containers) | 25/25 subcases in 17.73s; the OMAP write suite also passed 17/17 in the same run |
| secure/AES256K entity/service, IPv4, Linux arm64 container, three BlueStore/min2, CGO0 binaries | whole OSD suite 24/24 top-level, OMAP comparison 25/25, writes 17/17, reads 23/23, xattr 43/43; no FAIL/SKIP |

For all 17 single-assertion cases the Go per-op result, the native C prval and
the test-only signed-char model agree, and Go/native aggregates agree:

| Stored | Operand | Comparison | Result |
| --- | --- | --- | --- |
| 80 | 7f | LT / GT | 0 / -125 (unsigned order would invert both) |
| 7f | 80 | GT | 0 |
| ff | 00 | LT | 0 |
| 01 80 | 01 7f | LT | 0 |
| ff | 80 | GT | 0 (signed and unsigned agree) |
| `a\x80` | `a` | GT | 0 (length wins over a negative tail) |
| `ab` | `abc` | LT / EQ / GT | 0 / -125 / -125 |
| empty value | empty | EQ | 0 |
| missing key | empty | EQ / LT | 0 / -125 |
| empty value | 80 | LT | 0 |
| 00 | empty | GT | 0 |
| `a\x00b` | `a\x00c` / `a\x00b` | LT / EQ | 0 / 0 |

| Case | Observation |
| --- | --- |
| One Go operation with two passing assertions | Go 0; native single-assertion C operations in key order 0/0 |
| One Go operation whose later key fails | Go per-op -125, aggregate -125; native C 0/-125 (indirect: one C call is one assertion) |
| Two Go operations, the second failing | Go 0/-125 and native prvals 0/-125, aggregate -125 |
| Read-only comparison on an absent object | aggregate -2 for Go and native; Go per-op 0 and native prval 0 (request refused before the handler); flags ONDISK|READ|RETURNVEC without ACK |
| Passing guard, then SETVALS | committed; version and mtime advanced; native mirror aggregate 0, prval 0, equal OMAP |
| Failing guard, then SETVALS | aggregate -125, per-op -125/0, no state change; native mirror -125 |
| FAILOK failing guard, then SETVALS | aggregate 0, per-op -125/0, committed; native mirror 0 with prval -125 and equal OMAP |
| Identifiers 2, 4 and 6 | native server -22 aggregate and prval; Go refused each before transmission |

Read-only replies carried ACK|ONDISK|READ|RETURNVEC, and failed or successful
compare-before-write replies carried ACK|ONDISK|READ|WRITE|RETURNVEC. The
absent-object read reply matches the separate `reply_op_error` path rather
than the handler's own ENOENT, as the native encoder README notes.

The first attempt failed before any Go request because the Go test serialized
an absent seed attribute list as JSON null, which the strict oracle refused;
the second attempt, with that field omitted, passed. Native cleanup removed the
owned object and reported -2 for the never-created reserved name; owned
containers and network were independently absent afterwards. Logs and
diagnostics: `/tmp/ceph-omap-compare-crc-aes-20261015-second` and
`/tmp/ceph-osd-whole-secure-aes256k-container-omapcmp-20261015-first`.

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDOmapCompareIntegration$' \
sh integration/run.sh
```

## Remaining gaps

A single native operation with several assertions, and the server's
order-dependent EINVAL for an unsupported identifier after a passing or failing
assertion, need a C++ oracle because each C cmp2 call carries one assertion.
Read-only credential behavior, response loss/Close/SIGKILL for compare-before-
write vectors, x86_64 OSD images, broader profiles and remote CI for this
subset need separate evidence. No RMKEYRANGE, staged-read, EC, snapshot or
all-O2 completion is claimed.
