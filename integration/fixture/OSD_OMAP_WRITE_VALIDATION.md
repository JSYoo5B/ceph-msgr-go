# O2c OMAP header and bounded write validation

Run window: 2026-10-15 Asia/Seoul. Product subset: OMAPGETHEADER 0x1213,
OMAPSETVALS 0x2215, OMAPSETHEADER 0x2216, OMAPCLEAR 0x2217 and OMAPRMKEYS
0x2218, alongside the existing O2b reads. OMAP_CMP, RMKEYRANGE, OMAP reads after
staged OMAP writes, snapshots, EC and automatic pagination remain unsupported.
This is a partial O2 milestone. [OMAP_CONTRACT.md](../../internal/osd/OMAP_CONTRACT.md)
records the pinned source and wire semantics.

All daemon lifecycle uses public ceph-testcontainers-go v0.2.0. The native C
oracle, the pinned rados CLI header reader, the C++ encoder and the controller
live outside product dependencies. The product still builds and runs with
CGO0. Source is Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`;
image is
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The pool is replicated, legacy placement/HASHPSPOOL disabled, PG count 1, three
sparse-file BlueStore OSDs with min_size 2; the feature mask is unchanged.

## Independent codec and local boundaries

The first `encode-osd-omap-writes` SDK run compiled a fresh header/write
wrapper against the same generated headers and libceph-common as the mutation,
xattr and OMAP read proofs, encoded five message triples and decoded each in new
native objects. The prepared wrapper first needed typed null outputs to select
one of two pinned `omap_get_header` pointer overloads; no second build attempt
was needed. Go request header/front/data matches all 41/369/70 bytes, including
canonical byte-order sorting of unsorted entries and keys, the explicit mutation
mtime and READ|WRITE|ONDISK|RETURNVEC. Four replies keep echoed input lengths
(0/47/5/18/0), the raw five-byte header without a length prefix, FAILOK -2 and
ENOENT -2. Provenance and the offline seq/ACK/priority caveat are in the
[native fixtures](../../internal/osd/testdata/omap-write-native-README.md).

Internal tests bound every key, value, header and count before sorting or
allocation, reject duplicate SETVALS/RMKEYS keys, reject every field that does
not belong to the opcode (including nil versus empty Keys/Entries/Header for
non-OMAP operations), require zero output budgets for mutations and a positive
one for GETHEADER, and reject any OMAP read after a staged OMAP write, including
FAILOK. Public synthetic tests verify pre-TID/no-send rejection, exact sent
bytes, ONDISK enforcement for successful mutations, terminal handling of
contradictory replies, known refusals, FAILOK retention, local cancellation
as OutcomeUnknownError with retained submission identity, and no replay or
reconnect. Python oracle admission/argument/lifetime tests are separate and
are not daemon evidence.

## Live native and Go mutations

The oracle exclusively creates one UUID-suffixed owned object (binary bytes,
one xattr, four OMAP entries, no header) and reserves three absent UUID names
that Go mutations may create. Callers obtain native MON map/dump identity for
each name and explicitly open the primary endpoint; Go computes no placement.
Every observation records bytes, stat2, user version, xattrs, a bounded OMAP
page plus BYKEYS2 for the known key set (including the empty key) and the raw
header through `rados getomapheader OBJECT FILE`, rechecking stat2/version
around the separate CLI request. Two cases additionally execute the
C-expressible vector (SET2, RM_KEYS2, CLEAR with FAILOK) on a separate native
mirror with the same explicit mtime and compare aggregate and final state.

| Actual profile | Observed result |
| --- | --- |
| CRC/AES entity/service, IPv4, Darwin arm64 host relay, three BlueStore/min2 | final assertions passed 17/17 subcases in 9.25s (fifth run; earlier runs below); after the review fixes the committed product source passed 17/17 again |
| secure/AES256K entity/service, IPv4, Linux arm64 container, three BlueStore/min2, CGO0 binaries | whole OSD suite 23/23 top-level, OMAP writes 17/17, OMAP reads 23/23, xattr 43/43; no FAIL/SKIP, before and after the review fixes |

| Case | Native observation |
| --- | --- |
| SETVALS empty/NUL/0xff/empty-value keys and replacement | OMAP equals model; bytes/xattrs/header unchanged; version increased; mtime equals request; native mirror aggregate/state equal |
| SETHEADER binary, then GETHEADER | CLI header equals `00ff480a80`; Go raw header equals it; read changes nothing |
| Small reads before compound writes | GETHEADER/GETKEYS(2, more=true)/BYKEYS saw committed state; SETVALS/RMKEYS/SETHEADER applied atomically |
| RMKEYS existing and absent keys | key removed, absent key ignored, header retained |
| Empty SETVALS map, empty RMKEYS set, empty SETHEADER | each committed: version +1 and request mtime; OMAP unchanged; empty SETHEADER made the header empty but is not CLEAR |
| SETVALS + SETHEADER + failing AssertVersion | aggregate -75 (newer version), per-op 0/0/-75, no observable change |
| SETHEADER on absent object | object created with zero bytes, no xattrs, empty OMAP and the header |
| CLEAR(FAILOK) on absent object, then SETVALS | aggregate 0, per-op -2/0, object created; native mirror aggregate/state equal |
| RMKEYS or CLEAR on absent object | aggregate and per-op -2; object stays absent |
| SETHEADER then CLEAR, then GETHEADER | CLEAR removed all values and the header; Go and CLI header empty |

## Server observations retained without product workaround

Pinned PrimaryLogPG.cc:4287-4321 bounds each RETURNVEC output of a successful
mutation vector by `osd_max_write_op_reply_len` (default 64 bytes). A compound
of GETHEADER (5 bytes), an unbounded GETVALS page (more than 64 bytes), BYKEYS
(29 bytes) and three OMAP writes returned aggregate -75, page result -75 with
cleared output, retained the two small outputs and did not commit the OMAP or
header changes. The product returns this as a known `*OSDError` because the
limit is server configuration that the client cannot know in advance.

Pinned complete_read_ctx (PrimaryLogPG.cc:9222) adds ACK|ONDISK to read replies
and to refused vectors. The refused EOVERFLOW reply carried
ACK|ONDISK|READ|WRITE|RETURNVEC and user version 0; ENOENT write replies carried
ACK|ONDISK|WRITE|RETURNVEC. ONDISK on a negative aggregate is therefore not a
commit receipt; only successful mutation vectors require it.

After the EOVERFLOW refusal, native stat2 on the primary reported user version
+1 and the refused request's mtime, while OMAP, header, bytes and xattrs were
unchanged. Pinned finish_ctx (PrimaryLogPG.cc:9154) applies the new object_info
to the primary's cached object context inside prepare_transaction, before the
RETURNVEC bound refuses without submitting a transaction. Runs four and five
and both final profiles (including the secure/AES256K container) recorded the
exact advance; run three failed on the same unspecified state difference
before the dump existed. The test admits exactly that cached advance and nothing
else, and records it in the evidence. The failing-AssertVersion refusal returns
from do_osd_ops before finish_ctx and showed no advance. Whether the cached
value survives OSD restart, cache eviction or replica promotion was not tested;
the next successful write builds on it. This is a server behavior observation,
not a Go product guarantee, and it has not been reported upstream.

## Attempt history and cleanup

Native cleanup removed the owned object and the two Go-created reserved names
(0) and reported -2 for the reserved name that was never created. Each
fixture's owned containers and network were independently absent afterwards.
Final logs and diagnostics:

A review after the first passing runs only tightened local admission: the
SETVALS entry pre-bound became request-wide, DELETE now counts as a staged OMAP
and xattr write for later metadata reads, and public tests stopped passing
vacuously. No live vector used those combinations; both profiles were rerun on
the final source.

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDOmapWritesIntegration$' \
sh integration/run.sh
```

## Remaining gaps

OMAP mutation response loss, Close and actual primary SIGKILL recovery,
read-only credential EPERM, configured entry/byte/reply-length caps,
namespaces, broader store/pool/address/OS profiles and remote CI for this
subset need separate evidence. The pinned C API exposes no per-op results for
SET2/RM_KEYS2/CLEAR, so native per-op comparison is limited to aggregates and
final state. No CMP, RMKEYRANGE, staged-read, EC, snapshot or all-O2 completion
is claimed.

## Durability and lost replies (2026-10-17)

`TestCephOSDOmapDurabilityIntegration` (legacy-v6; single BlueStore with the
default secure CephX, and three BlueStore replicas with CRC/AES):

- One ONDISK vector of SETOMAPHEADER, SETOMAPVALS, RMKEYS and SETXATTR on a
  native-seeded object; native observation shows the header, keys, removed key,
  xattr and receipt version. The fixture OSD is SIGKILLed (the object's primary
  in the single-OSD run) and restarted; native observation then matches exactly.
- A relay drops the reply of an OMAP_CMP(k1 == "one") plus SETOMAPVALS(k1 =
  "two") vector after it executes; the caller gets an unknown outcome with its
  submission. An explicit resubmission (same transaction ID, retry attempt 1)
  on a fresh connection returns success with the native version and ONDISK,
  although re-executing the vector would fail its compare. Control: the same
  vector as a new request is refused with -125 and changes nothing.

Not claimed: OMAP writes under primary failover on three replicas (the killed
OSD there was the fixture object's primary), host power loss, and other OMAP
opcodes than those listed.
