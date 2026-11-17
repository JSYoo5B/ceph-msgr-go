# Go-originated manifest tier operation acceptance

This records live runs of TIER_PROMOTE, TIER_FLUSH and TIER_EVICT sent by this
repository's Go client (`OSDTierPromote`, `OSDTierFlush`, `OSDTierEvict`; see
[MANIFEST_CONTRACT.md](../../internal/osd/MANIFEST_CONTRACT.md#go-tier-operation-contract))
against Ceph Tentacle 20.2.4 source
`7f793731f1b39eb4f465e960113d2363c311b964`. Each Go operation is paired with
the same operation issued once by a native librados twin. It covers selected
profiles, not general dedup or tiering support.

## Fixture, roles and selection

`TestCephOSDTierOpGoIntegration` uses the ordinary OSD fixture and the two
owned plain replicated pools of `tools/osd_pool_oracle.py`. Slot 1 holds the
manifest objects. Slot 2 holds their chunk and redirect targets and, once slot
1 carries its dedup options, the fingerprint-named dedup chunks. The
owned-pool oracle seeds objects (WRITEFULL with its fixed mtime), places them
and observes the slot-2 objects (STAT2, READ, GETXATTRS with `chunk_refs`).

The closed tier observer `tools/osd_tier_op_go_oracle.py` (control
`verify-osd-tier-op`) never promotes what it observes. It reads slot-1
objects with one IGNORE_REDIRECT read operation (STAT2 and READ) and the C++
`is_dirty` read, plus, where the test names one, a proxied READ of one closed
extent without that flag. It sets the fixed dedup options of slot 1 once
(`dedup_tier` slot 2, `fingerprint_algorithm` sha256, `dedup_chunk_algorithm`
fixed, `dedup_cdc_chunk_size` 16) and reads them back. For every Go tier
operation it issues the same operation once on a separate twin object through
the C++ `tier_promote`, `tier_flush` and `tier_evict` entry points with the
request flags of `rados tier-promote`, `tier-flush` and `tier-evict`, and
returns the signed result. A repeated label, or any call after one left
without a result, is refused. The absent control is one name shared by both
sides.

Setup and probes use already admitted Go operations: SET_CHUNK, SET_REDIRECT,
UNDIRTY, UNSET_MANIFEST (as a refusal probe) and DELETE. Catalog row
`osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-osdtieropgo`
selects only this test without an opt-in parameter, so the test also belongs
to the broad legacy OSD selection.

## Sequence and expectations

Each Go reply must keep its transaction, GID, object, PG pool and seed, map
epoch, retry attempt, raw front and one operation without output. A negative
aggregate must surface as `OSDError` with the same code, and a success must
carry ONDISK. An `OutcomeUnknownError` stops the test and nothing is resent.
The native twin's code must equal the same expectation.

| Step | Operation (Go and twin) | Expected aggregate / per-op | Native observation after the step |
| --- | --- | --- | --- |
| Absent | promote, flush, evict | -2 / -2 each | the absent name stays absent |
| Plain, no dedup options | evict; flush; promote | -22; -22; 0 | bytes, user version, mtime and dirty=true unchanged |
| Chunked, dirty | Go SET_CHUNK 0..16 to its own target; flush | 0; -22 | source unchanged; target bytes and exact by_object `chunk_refs` |
| Chunked, clean | Go UNDIRTY; flush | 0; 0 / 0 | dirty=false; then nothing changes |
| Chunked evict | evict | 0 / 0 | range 0..16 zero, the rest kept; size, user version, mtime unchanged; dirty=false; proxied read of 0..16 returns the target's bytes; target unchanged |
| Chunked promote | promote | 0 / 0 | range 0..16 holds the target's bytes (which differ from the seed there); target and its reference unchanged; Go and twin change dirty, version and mtime alike |
| Redirect | Go SET_REDIRECT to its own target; evict; promote | 0; 0; 0 | after evict nothing changed; after promote the target's bytes and user version, and Go UNSET_MANIFEST is refused with -95 |
| Dedup flush | flush of a 40-byte dirty plain object | 0 / -11 (Go) | polls (20 x 0.5 s): three chunk objects named by the sha256 of each 16-, 16- and 8-byte chunk, holding those bytes and a by_object reference to the source; source bytes and user version kept, dirty=false |
| Dedup clean flush | flush | 0 / 0 | nothing changes |
| Dedup evict | evict | 0 / 0 | all 40 bytes zero; user version and mtime kept; proxied read of 0..16 returns chunk 0 |
| Dedup promote | promote | 0 / 0 | the original 40 bytes |
| Release | Go DELETE of each source | 0 / 0 | polls: every chunk object absent; source absent |

The expected per-op -11 of the dedup flush comes from the pinned source:
`finish_set_dedup` answers the retained request with `reply_op_error`, which
copies the request's operations, after the handler stored -EAGAIN in it. The
first local run received it and stopped as an unknown outcome, because the
reply validator then refused every negative per-op result in a success; the
validator now accepts exactly that shape (`TierFlushDeferredSuccess`). The
native C++ call exposes no per-op result, so only the aggregate is compared
there.

## Results

All runs used the pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`
on a Linux amd64 host with Docker 29.9.1, one row at a time under the CI
result gate (`tools/native_test_results.py`), each from a `git archive` of
commit `ed082e5758f2d54122e4d37f2c9be3420139da8c`. Later commits change
documents only. None is a hosted CI receipt.

| Environment | Result | Test time |
| --- | --- | --- |
| catalog row `...t-osdtieropgo` (secure, aes256k, IPv6, container, legacy-v6, one BlueStore OSD) | PASS, five subtests, no gate problem | 34.02 s |
| row `osd.aes256k.ip4.osd-store-bluestore.osd-replicas-3`, selector narrowed to this test | PASS, no gate problem | 34.10 s |
| row `osd.aes.ip4`, selector narrowed to this test | PASS, no gate problem | 34.14 s |
| row `osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-replicated-pg-request-v9-reply-v8.t-suite-385915`, selector narrowed to this test | PASS, no gate problem | 34.74 s |

The narrowed rows keep their row environment but replace its broad selector;
they show that this test passes on those profiles, not that the rows pass.

On the catalog row, at map epoch 20 (24 after the dedup options were set):

| Go request | Transaction | Reply flags | Aggregate | Per-op | Native twin |
| --- | --- | --- | --- | --- | --- |
| absent promote / flush / evict | 1 / 2 / 3 | `0x4000025` / `0x4028015` / `0x4028015` | -2 | -2 | -2 each |
| plain evict / flush | 4 / 5 | `0x4028015` | -22 | -22 | -22 each |
| plain promote | 6 | `0x4000025` | 0 | 0 | 0 |
| chunk dirty flush | 9 | `0x4028015` | -22 | -22 | -22 |
| chunk clean flush / evict | 12 / 13 | `0x4028015` | 0 | 0 | 0 each |
| chunk promote | 14 | `0x4000025` | 0 | 0 | 0 |
| redirect evict | 17 | `0x4028015` | 0 | 0 | 0 |
| redirect promote | 18 | `0x4000025` | 0 | 0 | 0 |
| dedup flush | 21 | `0x4028014` | 0 | -11 | 0 |
| dedup clean flush / evict | 22 / 23 | `0x4028015` | 0 | 0 | 0 each |
| dedup promote | 24 | `0x4000025` | 0 | 0 | 0 |

- The dedup flush reply is the only one without ACK: `reply_op_error` echoes
  the request's ACK/ONDISK bits, and this client's request carries ONDISK
  only. Its ONDISK therefore says nothing about the CLEAN transaction.
- Chunked source (Go side): user version 12 and the UNDIRTY request's mtime
  1700002000 stayed through evict. Promote kept the user version and dirty=false
  and set the mtime to the OSD's clock, on both twins.
- Redirect (Go side): the user version stayed the target's (10) through evict
  and promote; the promoted object took the target's mtime
  (1700000500.123456789) and stayed dirty.
- Dedup source (Go side): user version 38 throughout; dirty went from true to
  false and the mtime moved to the OSD's clock at the flush, stayed through
  evict and moved again at the promote. The first poll already showed the
  flushed state, and the first poll after each deletion found every chunk
  object absent.
- The owned-pool oracle confirmed both pools removed. Each run logged eight
  container terminations; afterwards the host had no testcontainers container
  and no workspace directory, and the engine lock was released.

Two mutation runs on the catalog row, from the same commit with one line of
the test changed on the host, failed as intended and were discarded. With
TIER_EVICT replaced by TIER_FLUSH in the chunk evict step (both sides
returned 0), the evicted-state check failed on the unchanged local bytes.
With the dedup flush replaced by TIER_PROMOTE, the flushed-state poll expired
after 20 polls with the source still dirty and no chunk object. The
cluster-free `TestTierGoObservationGates` refuses dirty, changed, short and
absent observations, changed versions and mtimes, failed `is_dirty` or
proxied reads, and checks the fingerprint names.

Five existing rows also passed from a working tree whose code equals that
commit: SET_CHUNK Go and native, cache-op Go, cache-tier ops and the secure
IGNORE_CACHE row. SET_CHUNK Go and cache-op Go passed again from the commit
archive.

## Lost reply

The test-peer control covers all three opcodes.
`TestOSDTierOperationPublicTransportAndUnknownOutcome` drops the connection
after each tier request is written and requires `OutcomeUnknownError` with
the original transaction, no redial and no resent request, on the
object-hash, replicated reply-v8 and request-v9 profiles.

`TestCephManifestTierEvictLostReplyIntegration` exercises TIER_EVICT
(opcode `0x822c`) on a clean chunked manifest across two owned plain
pools. On the dedicated legacy-v6, CRC/AES IPv4 host row, a single
BlueStore OSD serves both pools. Setup and cleanup use a separate client.
The subject sends a single TIER_EVICT with IGNORE_CACHE and IGNORE_OVERLAY.
Its dedicated CRC gate recognizes the owned object name in outgoing
MOSDOp frames across all subject OSD connections and withholds the entire
reply frame for the captured connection and TID.

On a Linux amd64 host with Docker 29.9.1 and the pinned image above,
the sole expected test passed in 12.25 s. The gate saw one marked frame,
on connection 1 at TID 1, and held MOSDOpReply type 43 for that pair.
The caller canceled its wait while the frame was held and received
`OutcomeUnknownError` wrapping context cancellation, with global ID 4734,
TID 1, incarnation 71, epoch 20 and attempt 0. The subject issued no
resubmission. Through a second native observation, about 0.90 s after
the held connection was closed, the marked-request count remained one.

Before evict, the local source contained its seed bytes and the target
held one exact by-object reference. Both observations after evict showed
zeros in the first 16 local bytes, the original suffix, clean state, and
unchanged version 4 and mtime. A proxied read still returned the target's
bytes. The target's bytes, xattrs including its reference, version and
mtime remained unchanged. Deleting the source from the setup client
released the final reference. The target was absent on the first native
poll, the source was absent, and owned-pool cleanup confirmed removal.
The fixture writes the gate, original submission and object observations to
`osd-tier-evict-lost-reply.json` and the pool cleanup to `osd-pool-result.json`.

The count covers subject transmissions in this observation window. The test
uses independent native reads to establish the state transition. It does
not instrument daemon execution counts or establish crash durability.


## Not verified

Other authentication, address and runtime profiles beyond those above; a
hosted exact-SHA CI receipt; live lost replies for TIER_PROMOTE and TIER_FLUSH; TIER_* on a pool in a
cache-tier relation (EOPNOTSUPP without the capability, EINVAL from a tier's
handler: source-derived only); scoped identities; erasure-coded pools
(refused locally); snapshot clones and their reference arithmetic; fastcdc
chunking, chunks shared between objects and a failed dedup reference; a
promote whose chunk or redirect target is absent; overwrites of a deduped
object; the tier agent; crash persistence.
