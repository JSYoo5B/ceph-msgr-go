# Explicit v8 OSD reply validation — 2026-10-15

The opt-in `replicated-pg-reply-v8` fixture selects
`Options.OSDProfile=OSDProfileReplicatedPGReplyV8` before MON admission. It
retains the explicit actual PG/raw hash/primary/epoch contract of
`replicated-pg-v8` and additionally requires negotiated
`NEW_OSDOPREPLY_ENCODING`. Successful public requests in this profile require
v8/compat2 replies; changing the profile does not add placement selection,
automatic routing, mutation replay, redirect following or EC support.

## Native wire and actual daemon evidence

Ceph source is pinned to Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The existing five independent native
reply vectors cover a limited success/error/mutation/redirect set, with native
message construction, segment hashes and separate native decoding. They do
not establish interoperability for every opcode family or every daemon
configuration. Synthetic malformed/version tests independently cover strict
profile selection; a successful native operation is not malformed-peer proof.

The first CRC and secure attempts stopped during native oracle preparation,
before either acceptance suite started. Bootstrap required
`fixture-manifest.json` before `prepareEvidence` could publish it. The bootstrap
observation now permits an absent manifest at that stage and strictly matches
it when present; later mutating ownership gates retain their requirements.
Both original attempts exited with setup deadlines, and all eight owned
containers per attempt were terminated. Their `*-first.log` files are retained.
These setup-only failures establish no Go v8 reply interoperability.

The next CRC and secure attempts reached actual suites. The thirteen existing
modern-profile leaves passed in both configurations, including split and
watch recovery. The first new leaf failed because its test incorrectly
required zero error-reply versions. The native READ bytes, signed aggregate
and per-op values were preserved; the reply held current user/legacy version
and a distinct next PG-log replay version. The other two new leaves did not
run after this failure. Both runtime failures are retained separately from
the earlier setup-only attempts. The source-backed expectation correction
below was then exercised by separate focused runs of the three new leaves.
The failed overall runs remain failed; passing leaves from separate runs are
not presented as one successful sixteen-case run.

| Configuration | Earlier complete-selector attempt | Corrected focused attempt |
| --- | --- | --- |
| Darwin arm64 host, IPv4 relay, CRC/AES/AES, one BlueStore OSD | Thirteen existing leaves passed; first new leaf failed its version assertion; two new leaves did not run; overall failed | All three new leaves passed in 2.78 seconds; owned-head cleanup, export and eight-container termination completed |
| CGO=0 Linux arm64 container, IPv6, secure/AES256K keys and service cipher, one MemStore OSD | Thirteen existing leaves passed; first new leaf failed the same assertion; two new leaves did not run; overall failed | All three new leaves passed in 3.45 seconds; owned-head cleanup, export and eight-container termination completed |

The product, fixture and existing thirteen-case source remained unchanged
between those runtime attempts and the corrected focused runs. The only
acceptance-code correction changed the failed-write version assertion and
bounded failure logging. Existing-case evidence is reused explicitly across
runs. A single final combined sixteen-case native pass is not claimed; the
combined selectors are configured for the subsequent exact-commit CI run.
Local compilation and opt-in skips remain separate from this daemon evidence.

The initial acceptance source had SHA-256
`28af3323d062b6b0358d19edf27090613fb4b17e1b778747a686f0c0848d7b66`
for `integration/osd_reply_v8_test.go`. The corrected acceptance source has
`e5136a1ad3461045e6f5f239d6ad90d3e2f17b80f97b6453eeecb58c405a8c4e`.
The correction changes the failed-write version expectation and bounds failure
log summaries; it changes no product decoder or error/payload expectations.
The profile dispatch source
`cephmsgr/osd_profile.go` has
`48c92c1e39b7fcfca86345f9899843c99c8c79211f2d7de801f420fe4f7af1f0`,
and the standalone Go codec foundation `internal/osd/reply_v8.go` has
`2d32a125af7388812f6c320aa6cfbddec89a66835db875702347b7b6c390c8c6`.
These identify the tested source snapshot used for the selected runs; they are not
a claim that a preceding base commit already contained the new profile.

## Independent raw-result verification

Both focused result sets were read independently of the test exit status.
They used the pinned Ceph 20.2.4 image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The host run selected actual PG `2.4`; the container run selected `2.0`.
Each kept its independent raw hash and explicit caller placement.

In both runs, refusal retained `{0,-125,0}`, four earlier READ bytes, own-head
user version 1, legacy `{epoch:12,version:1}` and PG-log replay
`{epoch:12,version:2}`. Native own-head and mirror state were each unchanged.
The subsequent FAILOK vector retained `{0,-125,0,0}` and aggregate zero,
preserved object bytes, and advanced its own-head native/user/legacy/replay
version to 3 with mtime `1700001201.987654321`. The distinct native write-part
mirror independently produced `-125` for CMP and the same committed OMAP and
hint values, without equating mirror and actual object versions.

The allocation hint had expected object size 1048576 and expected write size
4096; both hint flags matched the native object_info record. Raw GETXATTRS and
OMAP BYKEYS buffers were independently decoded and compared with the native
inventories, including NUL-containing OMAP keys and binary values. The three
per-op data segments joined exactly into the 91-byte metadata RawData. Read
versions were user 3, legacy `{epoch:0,version:3}`, replay `{0,0}`. The separate
empty watcher reply contained ten bytes and matched the native CLI inventory.
Both original fixture heads remained unchanged. All nine owned/reserved names
were cleaned, and each focused run terminated all eight owned containers.

## Three additional actual-daemon cases

`TestCephOSDReplyV8Integration` adds three leaves to the thirteen existing modern
profile cases. It exclusively creates a UUID head with binary bytes, an xattr
and an OMAP guard, reserves eight absent names, and cleans all nine names.
Caller placement comes from independent native metadata; the product does not
calculate a PG. The original fixture head must remain unchanged.

1. **Signed refusal preserves earlier output.** A READ precedes a failed
   OMAP_CMP and a later OMAP SET. The reply must retain aggregate `-125`, raw
   per-operation results `{0,-125,0}`, the earlier READ payload, the packed
   operation echoes and completion flags. This failed write is recorded in
   the PG log for duplicate detection: user/legacy version numbers retain the
   current object's version while replay version advances its PG-log sequence,
   with a common nonzero epoch. The final per-operation zero is not proof that the
   later SET executed. Independent observation must show unchanged own-head
   bytes, xattrs, OMAP, version and mtime.
2. **FAILOK signed result commits metadata and hint.** The same failed CMP is
   marked FAILOK, followed by OMAP SET and SETALLOCHINT. Aggregate success must
   retain the negative per-operation CMP result, the earlier READ bytes and
   allocation-hint union. Native own-head observations must show the new OMAP
   value and exact hint fields, unchanged object bytes/xattrs, a newer own-head
   user version and the explicit mtime. The actual reply's user/legacy/replay
   versions must agree with the pinned classic-pool non-watch mutation path.
3. **Bounded metadata outputs and standalone watchers.** GETXATTR, GETXATTRS
   and OMAP BYKEYS return bounded binary payloads in one vector. The test
   compares xattr length unions, the decoded inventories, each payload length
   and their ordered concatenation with RawData. A separate standalone
   LIST_WATCHERS request compares the empty listing with the pinned native
   `rados listwatchers` output. No compound watcher contract is added. Native
   metadata, bytes, version and mtime must remain unchanged after both reads.

Each compound case retains request identity, original caller placement and
map epoch, actual reply PG, TID, raw front/data, signed aggregate/per-op values
and ONDISK completion flags. ONDISK has the fixture storage's actual semantics;
a MemStore success does not establish restart durability.

## Independent comparison boundary

The first two cases construct distinct UUID mirrors through the existing
native C write-op helper. Those mirrors reconstruct only the CMP/metadata/hint
write part. They do not reconstruct the mixed READ plus write vector. The
READ payload is compared with the independently observed committed baseline,
and the write part is compared with the native C result and postcondition.
Native CMP exposes its own per-operation prval. Different objects' user
versions are never equated: each Go reply is checked immediately against that
same owned head's native state. A mirror is removed independently, including
on refusal.

The pinned source supports these expectations:

- `src/messages/MOSDOpReply.h:62–90` separates legacy, replay and user versions;
  classic-pool non-watch writes make their version numbers equal. Reads use
  the current user version with an empty replay version.
- `src/osd/PrimaryLogPG.cc:4265–4335` constructs a RETURNVEC reply, preserves
  per-operation output, and distinguishes completed writes from reads/errors.
- `src/osd/PrimaryLogPG.cc:9183–9224` preserves signed errors, sets read
  versions only for successful reads, and adds ACK/ONDISK flags to completed
  read/error replies. A failed ordinary operation stops the vector; FAILOK
  retains its error while allowing later operations.

The initial test incorrectly applied the zero-version read-error path to a
failed write. At `PrimaryLogPG.cc:8991–8998`, a failed `may_write` request sets
`update_log_only`. The branch at `4316` therefore does not call
`complete_read_ctx`; `4325` first calls
`set_reply_versions(ctx->at_version, ctx->user_at_version)`, with the current
object's user version initialized at `4247–4248`. The branch at `4352–4375`
then records the error in the PG log for duplicate detection without changing
the owned object. `MOSDOpReply.h:65–82` preserves replay version separately and
projects the current user version into the legacy version number. The native
CRC reply had user 6, legacy `{epoch:29,version:6}` and replay
`{epoch:29,version:7}`; secure had user 3, legacy `{29,3}` and replay `{29,4}`.
Those observations support the corrected strict relations, rather than a zero
or equal-version assumption for failed writes. Own-head native observations
must still remain unchanged, independently of the PG-log entry.

No new native C API builder, arbitrary subprocess, product dependency or
fixture policy change is introduced by this acceptance. It reuses the owned
OMAP/native metadata controls and the fixed new profile. Five vectors and
three additional cases remain bounded coverage, rather than an assertion that
all snapshot, class, sparse-read, checksum or other opcode families have been
verified with actual v8 replies.

## Reproduction and diagnostics

A focused development run selects one of the supported disposable profiles:

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_PROFILE=replicated-pg-reply-v8 \
CEPH_MSGR_TEST_OSD_REPLICAS=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_CONNECTION_MODE=crc \
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDReplyV8Integration$' \
sh integration/run.sh
```

The second profile selects container/Linux arm64, IPv6, secure transport,
AES256K keys and service cipher, and one MemStore OSD. Native profile runs and
full selected-case totals must be recorded separately from local compilation.

The first profile runs failed during setup. The next runs passed thirteen
existing leaves but failed the new-case expectation. Corrected focused runs
passed the three new leaves with completed cleanup/export in both profiles.

`osd-reply-v8-results.json` retains all three requests and replies, native
before/after inventories, separately labelled write-part mirrors, standalone
watcher evidence, original fixture state and final owned-head cleanup receipt.
All evidence publication uses the bounded public-diagnostic helper.
