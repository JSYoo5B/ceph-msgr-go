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

The expected `chunk_refs` bytes are built independently in the test from the
native placement hashes and the owned pool ID, ordered as `hobject_t`
compares within one pool (bit-reversed hash, then name). The absent-target
step uses source offset 0, so it stays off the documented non-zero-offset
hazard path.

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
UNSET_MANIFEST or overwrites of a chunked manifest; a live lost reply; the
TIER_* operations and dedup pools.
