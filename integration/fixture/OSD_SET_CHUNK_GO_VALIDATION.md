# Go-originated SET_CHUNK acceptance

This records the live run of SET_CHUNK sent by this repository's Go client
(`OSDSetChunk`; see
[MANIFEST_CONTRACT.md](../../internal/osd/MANIFEST_CONTRACT.md#go-set_chunk-contract))
against Ceph Tentacle 20.2.4 source
`7f793731f1b39eb4f465e960113d2363c311b964`. It is one selected profile, not
general manifest or dedup support. The native-only predecessor is
[OSD_SET_CHUNK_NATIVE_VALIDATION.md](OSD_SET_CHUNK_NATIVE_VALIDATION.md).

## Fixture, roles and selection

`TestCephOSDSetChunkGoIntegration` uses the ordinary OSD fixture and the two
owned plain replicated pools of `tools/osd_pool_oracle.py` (slot 1: the
manifest sources, slot 2: the chunk target). The owned-pool oracle seeds the
objects natively (WRITEFULL with its fixed mtime), returns their native
placements and observes them: STAT2, READ and the GETXATTRS inventory, which
includes the target's cls_cas `chunk_refs` attribute. The Go client (the
fixture's admin identity) sends every SET_CHUNK and both source deletions
itself, once each, to the native placement's primary. No native SET_CHUNK is
issued and no new oracle is added.

A second sequence in the same test uses a fresh triple in the same pools: a
kept source and a peer source in slot 1 and their shared target in slot 2. It
adds one standalone Go UNSET_MANIFEST to the kept source, a second one that
must be refused, and one deletion of the peer. It uses no snapshot, no cache
tier and no new product opcode.

Catalog row
`osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-osdsetchunkgo`
selects only this test and uses no opt-in parameter, so the test also belongs
to the broad legacy OSD selection.

## Sequence and expectations

Each Go reply must keep its transaction, GID, object, PG pool and raw hash,
map epoch, retry attempt, raw front and one operation without output; a
negative aggregate must surface as `OSDError` with the same code. An
`OutcomeUnknownError` stops the test, and nothing is resent.

| Step | Go request | Expected aggregate / per-op | Native observation after the step |
| --- | --- | --- | --- |
| Local refusals | non-zero source offset, target equal to the source, missing WITH_REFERENCE | refused, no transaction | none needed |
| Server refusals | absent source; source A extent beyond its size; absent target at source offset 0 | -2 / 0 or -2; -34 / -34; -2 / -115 | source A and target unchanged (bytes, user version, mtime, xattrs); absent controls stay absent |
| Chunk | source A 0..8 to the target at offset 0 | 0 / 0 | source A unchanged; target bytes unchanged, user version advanced, `chunk_refs` equal to the exact by_object encoding naming source A's head |
| Overlaps | source A 0..8 again; 0..4 to target offset 16 | -95 / -95 each | source A and target unchanged |
| Second source | source B 0..16 to target offset 8 | 0 / 0 | sources unchanged; target version advanced and `chunk_refs` naming both heads in hobject order |
| Release | DELETE source A, then DELETE source B | 0 / 0 each | polls (20 x 0.5 s): `chunk_refs` naming only source B with a newer target version, then the target absent; both sources absent |
| Shared target | kept source 0..8 to the shared target at offset 0; peer source 0..16 to offset 8 | 0 / 0 each | both sources unchanged; shared target bytes unchanged, version advanced, `chunk_refs` equal to the exact two-head encoding |
| Unset | UNSET_MANIFEST to the kept source, once | 0 / 0 | both heads exist with their seeded bytes; the peer is unchanged; the kept source keeps its xattr inventory, and its new version and mtime are recorded, not predicted. Target-only polls (20 x 0.5 s): bytes unchanged, a newer version and `chunk_refs` naming the peer alone; an absent target ends the poll as a failure |
| Second unset | UNSET_MANIFEST to the kept source again | -95 / -95 or 0 | kept source and shared target unchanged |
| Peer release | DELETE the peer source | 0 / 0 | polls: the shared target absent; the peer absent; the kept source unchanged since the unset |

The expected `chunk_refs` bytes are built independently in the test from the
native placement hashes and the owned pool ID, ordered as `hobject_t`
compares within one pool (bit-reversed hash, then name). The absent-target
step uses source offset 0, so it stays off the documented non-zero-offset
hazard path.

### UNSET_MANIFEST on a chunked source

The pinned handler
([PrimaryLogPG.cc:7528-7558](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L7528-L7558))
checks the tier, existence, manifest and release guards, calculates the
reference drops, clears the manifest and marks the source modified. It does
not truncate or remove the source. For a chunked object the decrement is
scheduled on the source commit
([3755-3780](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L3755-L3780))
and submitted to the target without a completion back to the caller
([3607-3618](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L3607-L3618)).
A successful source reply therefore says nothing about when the target
reached its new state, so the target is polled.

The sequence separates three outcomes. A no-op leaves the kept source alive
but keeps both references, so the poll expires. An accidental deletion or an
emptied source fails the source check (`setChunkGoSourceKept`). A lost peer
reference shows as an absent target. The SET_CHUNK expectation of an
unchanged source version does not carry over: UNSET_MANIFEST is a
modification, and the kept source's version and mtime are recorded as
observed. If a poll expires, the receipt keeps the last target observation.
An unknown outcome stops the test and nothing is resent.

## Results

Run [39902724497](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39902724497),
attempt 1, scenario job 120247749633. The scenario checkout is
`19e162af43c980ccc613cdaf76365dbac7bb7e18` (the job's other checkout is the
trusted controller `a617c23`), image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Profile: secure, aes256k client and service keys, IPv6, container runtime,
legacy-v6, one BlueStore OSD, two MGRs. The retained `result.json` names that
commit and image with result success; `tests.json` records the selected
`TestCephOSDSetChunkGoIntegration` as PASS with no gate problem.

`TestCephOSDSetChunkGoIntegration` passed in 13.05 s with all six subtests:

| Go request | Transaction | Map epoch | Reply flags | Aggregate | Per-op |
| --- | --- | --- | --- | --- | --- |
| absent source | 1 | 19 | `0x4000014` | -2 | 0 |
| out of range | 2 | 19 | `0x4000015` | -34 | -34 |
| absent target | 3 | 19 | `0x4000014` | -2 | -115 |
| chunk A | 4 | 19 | `0x4000015` | 0 | 0 |
| same overlap | 5 | 19 | `0x4000015` | -95 | -95 |
| partial overlap | 6 | 19 | `0x4000015` | -95 | -95 |
| chunk B | 7 | 19 | `0x4000015` | 0 | 0 |
| delete source A | 8 | 19 | `0x4000025` | 0 | 0 |
| delete source B | 9 | 19 | `0x4000025` | 0 | 0 |

- Source A kept user version 4 and the seeded mtime
  (1700000500.123456789) through both chunks.
- The target's user version went 4 -> 6 (chunk A) -> 7 (chunk B), and
  `chunk_refs` equalled the exact one- and two-reference encodings.
- The first release poll already showed the single remaining reference, and
  the first poll after deleting source B found the target absent.
- The owned-pool oracle confirmed both pools removed.

Two outcomes are recorded as observed rather than inferred. The absent-source
refusal carries per-op result 0, consistent with an early object-context
refusal before the handler runs. The absent-target refusal carries aggregate
-2 with per-op -115: the pinned `RefCountCallback` answers a failed reference
increment through `reply_op_error`, while the operation keeps the
`-EINPROGRESS` stored before the source was blocked.

An earlier run, [39902330337](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39902330337)
at `bcb022f40611dc0ef5fd2d857cf0c67a7b4e5df1`, failed at the absent-target
step because its test required the per-op result to be 0 or the aggregate. It
received the same -2 / -115 pair, stopped there, and sent no later request;
owned pools were removed. The test and the synthetic transport test were then
updated to require and pass through that pair. The later pass does not depend
on any other change.

## UNSET_MANIFEST calibration

This local run preceded the pull request that added the sequence and is not
an exact-SHA CI receipt. It used this catalog row's environment and the
pinned image on a Linux amd64 host with Docker 29.9.1 and Go 1.26.9, at test
commit `525e6cd` (later commits of the same pull request change documents
only). The result gate recorded `TestCephOSDSetChunkGoIntegration` as PASS
with no problem: 16.91 s, all nine subtests, owned pools removed. The first
nine requests repeated the table above at map epoch 20.

| Go request | Transaction | Map epoch | Reply flags | Aggregate | Per-op |
| --- | --- | --- | --- | --- | --- |
| chunk kept source | 10 | 20 | `0x4000015` | 0 | 0 |
| chunk peer source | 11 | 20 | `0x4000015` | 0 | 0 |
| unset manifest | 12 | 20 | `0x4000025` | 0 | 0 |
| unset manifest again | 13 | 20 | `0x4000025` | -95 | -95 |
| delete peer source | 14 | 20 | `0x4000025` | 0 | 0 |

- The kept source went from user version 8 with the seeded mtime
  (1700000500.123456789) to user version 17 with the request's mtime
  (1700001400.000000000); the reply's user version was 17. Its bytes and
  xattr inventory were unchanged.
- The shared target went from user version 13 (two references) to 14 with
  the peer's reference alone. The first poll already showed it.
- The first poll after the peer's deletion found the shared target absent.

Two mutation checks ran against the same fixture and were then discarded.
With the UNSET_MANIFEST not sent, the kept source passed its check and the
target poll expired after 20 polls with both references still encoded. With
a DELETE sent in its place, the source check failed on the absent head. The
cluster-free `TestSetChunkGoRefsOrderAndTargetGate` also refuses a deleted,
emptied or changed source, an older version and a changed xattr inventory.

## Lost reply

The lost-reply boundary is synthetic:
`TestOSDSetChunkPublicTransportAndUnknownOutcome` drops the connection after
an `OSDSetChunk` request is written and requires `OutcomeUnknownError` with
the original transaction, no redial and no resent request, on the object-hash,
replicated reply-v8 and request-v9 profiles.

## Not verified

Only the profile above ran. Not verified: other authentication, address,
runtime, store, replica and request profiles; a non-zero source offset from
the Go client (refused locally because of the source-derived hang hazard);
scoped identities (the source audit finds that the cache-mode opcode asks for
neither read nor write capability); erasure-coded and tier source pools
(server refusals only); snapshot clones and their reference accounting;
overwrites of a chunked manifest; a live lost reply; the TIER_* operations
and dedup pools. For UNSET_MANIFEST, one chunked source with one offset-zero
extent and local bytes ran on this profile. Not verified: a source with
several chunks or without local bytes, the delay between the source reply
and the target's release (the first poll already showed it), replay after a
lost UNSET_MANIFEST reply, crash persistence, and an exact-SHA CI receipt
for this sequence.
