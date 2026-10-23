# Native SET_CHUNK manifest checkpoint

This is the native-only SET_CHUNK checkpoint for issue #8 (staged work item 3):
SET_CHUNK issued through native librados against Ceph Tentacle 20.2.4 source
`7f793731f1b39eb4f465e960113d2363c311b964`. It adds no Go product opcode,
operation union, feature bit or admission rule, and it makes no Go SET_CHUNK
support claim. The source audit is in
[MANIFEST_CONTRACT.md](../../internal/osd/MANIFEST_CONTRACT.md#set_chunk-and-manifest-tier-operations).

## Fixture and selection

SET_CHUNK needs no cache-tier or dedup pool configuration, so the checkpoint
uses the ordinary OSD fixture. `TestCephOSDSetChunkNativeIntegration` creates
the two owned plain replicated pools of `tools/osd_pool_oracle.py` (slot 1: the
manifest source, slot 2: the chunk target) and obtains the source object's raw
hash from that oracle's separate native placement read. It then sends one
`verify osd-set-chunk checkpoint` request. The fixture accepts it at most once
and runs `tools/osd_set_chunk_oracle.py` in the fixture container with a
three-minute bound; the oracle also refuses to overwrite an existing receipt.
The owned-pool oracle removes both pools when the test ends.

The catalog row
`osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-osdsetchunknative`
selects only this test. It uses no opt-in parameter. Broad legacy OSD rows
also select the test, but none of them has been run with it yet.

## Native construction

`ObjectReadOperation::set_chunk` exists only in the public C++ API. As in the
cache-tier operation checkpoint
([OSD_CACHE_TIER_OPS_NATIVE_VALIDATION.md](OSD_CACHE_TIER_OPS_NATIVE_VALIDATION.md)),
the oracle resolves the exported `librados.so.2` C++ entry points from the
fixture library's own dynamic symbol table and calls them through ctypes. The
signed return of `IoCtx::operate` is recorded unchanged and the resolved
symbols are kept in the receipt. The construction follows `rados set-chunk`:
no request flag, and the per-operation flag WITH_REFERENCE (0x80) except in the
missing-reference control. SET_CHUNK has no per-operation output.

Independent native observations after every phase:

- STAT2, READ and GETXATTRS of all four closed objects (source, absent source,
  target, absent target) and a direct GETXATTR of the target's `chunk_refs`
  attribute, which cls_cas maintains;
- `cas.references_chunk` executed on the source, which returns the number of
  the source's own chunk references to the target (-ENOLINK for none).

The test compares the raw `chunk_refs` bytes with the exact by_object
`chunk_refs_t` encoding that names the source head (`CEPH_NOSNAP`), the source
pool and the raw hash from the native placement read. The expected codes are
restated in the test independently of the oracle's own gates.

Every mutation records its intent before it is issued, is issued once and is
never reissued. An uncertain outcome stops the controller and skips object
cleanup. A stopped receipt still exits zero, so that the owned-pool cleanup
stays selectable; the test judges the receipt.

## Sequence

| Phase | Source-derived expectation |
| --- | --- |
| Seed source (32 bytes) and target (25 bytes) | target has no `chunk_refs` (-61); `references_chunk` -67 |
| Absent source; zero length; offset 28 length 8; no WITH_REFERENCE; absent target at offset 0 | -2, -22, -34, -95, -2; no object, version, mtime or reference change |
| Chunk A: source 8..16 to target offset 0 | 0; source bytes, user version and mtime unchanged; target bytes unchanged and user version advanced; one reference |
| Same and partially overlapping interval | -95, -95; nothing changes |
| Chunk B: source 16..24 to target offset 8 | 0; two references from the same source head |
| Delete the source; poll the target (20 x 0.5 s) | references released and the target removed; source absent |
| Cleanup | all four objects absent |

## Results

Run [39885934101](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39885934101),
attempt 1, actual scenario checkout `c24c0feddca28b2bafd0045ebae405d586e6db38`
(the job's first checkout is the trusted controller `e1d062b`), image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Profile: secure, aes256k client and service keys, IPv6, container runtime,
legacy-v6, one BlueStore OSD, two MGRs. The retained `result.json` and
`tests.json` name that commit and image and record the expected,
non-skipped test as PASS with no gate problem.

`TestCephOSDSetChunkNativeIntegration` passed in 9.01 s:

- Every SET_CHUNK result matched: absent source -2, zero length -22, out of
  range -34, missing WITH_REFERENCE -95, absent target -2, chunk A 0, same
  overlap -95, partial overlap -95, chunk B 0.
- The refusals left all four objects and both reference views unchanged.
- The source kept its bytes, user version (3) and seeded mtime
  (1700001100.000000000) through both chunks, so the manifest change was
  visible only through `references_chunk` (1, then 2). That run recorded the
  mtime; the test now also enforces it for both successful chunks
  (`setChunkSourceKept`, with the synthetic regression
  `TestSetChunkSourceKeptRequiresMTime`).
- The target kept its bytes. Its user version went from 3 to 5 after chunk A
  and to 6 after chunk B. The test asserts only that it advanced: user
  versions follow the PG's last user version, which the failed absent-target
  write in the same single-PG pool may also have advanced.
- The target's `chunk_refs` equalled the exact by_object encoding with one,
  then two, references to the source head in the source pool, hash
  `0xf0e758eb` from the native placement read (pool IDs and hashes are
  specific to this run).
- After the source was deleted, the first poll already found the target
  absent, and the source was absent. Object cleanup completed and the
  owned-pool oracle confirmed both pools removed.

## Not verified

Only the profile above ran. Not verified: native request vectors and any Go
request; other authentication, address, runtime, store, replica and request
profiles; scoped identities (the source audit finds that the cache-mode opcode
asks for neither read nor write capability); erasure-coded and tier source
pools and redirect-manifest sources (source-derived refusals only); a failed
reference increment at a non-zero source offset (a source-derived hang hazard,
deliberately not exercised); snapshot clones and their reference accounting;
UNSET_MANIFEST, chunk overwrite and partial reference release; native resend of
an uncertain SET_CHUNK; TIER_PROMOTE, TIER_FLUSH, TIER_EVICT and dedup pools.
