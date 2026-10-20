# Owned actual PG split validation — 2026-10-15

This acceptance extends the fixed `OSDProfileReplicatedPG` profile. It exercises
caller-provided actual PG, raw hash, primary and epoch across a native split.
It adds no product map refresh, PG calculation, automatic routing, retry or
replay. The control recipe and its source audit are documented in
[OSD_SPLIT_RECIPE.md](OSD_SPLIT_RECIPE.md).

## Evidence and exact scope

Native evidence uses Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
and an exclusively owned single-OSD pool. The initial configuration has legacy
CRUSH, HASHPSPOOL, autoscaling off, and eight PGs/PGPs. The selected UUID head
moves to an actual child at sixteen PGs, then returns to its parent after MGR
merges the pool back to eight. Both transitions require active, clean PGs and
all actual, target and pending counts to agree. Native fixture tools are
restricted to development; product and container test binaries remain CGO=0.

| Run | Profile | Native result | Evidence boundary |
| --- | --- | --- | --- |
| First | Darwin arm64 host, IPv4 relay, CRC/AES/AES, one BlueStore | Passed, 66.02 seconds; restore, object cleanup and diagnostic export passed | Initial staged helper; does not cover the subsequent cross-scope input retirement correction |
| Final CRC | Same profile | Passed, 62.19 seconds; restore, cleanup and export passed | Final helper includes validated prior-scope input retirement; repeated live scopes remain untested |
| Secure build attempt | Linux arm64 container, IPv6, secure/AES256K, one MemStore | Build failed before the suite started | Unrelated concurrent MDS source referenced undefined `formatFSID`; no daemon split result |
| Final secure | Same secure profile | Passed, 64.98 seconds; restore, cleanup and export passed | Frozen final Go/oracle source, excluding unfinished concurrent MDS work |

The first CRC, final CRC and final secure artifacts were independently read and
checked, beyond the tests' exit status. The selected object moved from `2.2` at epoch 12 to `2.a` at epoch
16, then returned to `2.2` at epoch 58. All five PG/PGP count fields were
8 → 16 → 8. Both explicit test connections opened one OSD connection each.

The final CRC run separately moved `2.6`/epoch 12 → `2.e`/epoch 16 →
`2.6`/epoch 58, with counts 8 → 16 → 8 and versions 3 → 4 → 5. Its stale
receipt matched global ID 4863, transaction 1, incarnation 95, epoch 12 and
raw hash 863630462. Exact suffixes and both explicit mtimes matched native
state, both unknown submissions retained the old tuple, each connection dialed
one OSD, and all nine owned/reserved heads were cleaned after restoration.

The final secure run separately moved `2.5`/epoch 12 → `2.d`/epoch 16 →
`2.5`/epoch 57, with counts 8 → 16 → 8 and versions 2 → 3 → 4. Its receipt
matched global ID 4935, transaction 1, incarnation 95, parent `2.5`, epoch 12
and raw hash 3749596013. Exact byte suffixes, mtimes, original unknown tuples,
one dial per OSD connection, current-head preservation and nine-head cleanup
were independently checked. Both final profiles' full native pool records
preserved the original identity, type, flags, CRUSH rule, size/min_size,
autoscale mode and all five PG count fields after restoration.

## Frozen final source identity

| File | SHA-256 |
| --- | --- |
| `integration/osd_split_test.go` | `c8f50d03d13ab61198e5635726591b0c32518280203783dd173eecae17e5b340` |
| `tools/osd_split_oracle.py` | `569a2ff444c701ceaf869a59d7cea056139c29af670b6e817c77dec3d64cfadc` |
| `tools/test_osd_split_oracle.py` | `d10ec527f87b39c98a718b334a976dd437f57550f025cb6f48930e5a06221f86` |
| `integration/fixture/split_control_test.go` | `d9e969d11103395967151491d8b6dd2c0ee0d0f18ee38d1ace7bca33e4b6e2a4` |

A later change to the Go acceptance or Python oracle invalidates transferring
this native result to that changed code. The base commit alone is not a claim
that the pending split files had already been committed. Documentation edits
that record the completed results do not change the tested code identity.

## Requests and outcomes

The first APPEND was independently observed before the split: baseline version
1 advanced to version 2 with exactly one `before-split-once` suffix and explicit
mtime `1700001100.123456789`. Its Go reply was withheld, and cancellation across
the split retained `OutcomeUnknown`, the original epoch 12 and parent `2.2`.
The independent native observation establishes that append's postcondition;
the changed map alone establishes no request execution.

After the split completed, a separate connection submitted a distinct APPEND
with that old parent and epoch. Its short local deadline preserved an unknown
outcome and the original tuple. Native completed-operation history matched
client/global ID 4941, transaction 1, incarnation 95, parent `2.2`, epoch 12 and
raw hash 3497571130. The receipt says `reached_pg=true`, `body_decoded=false`,
and explicitly limits the evidence to stale-envelope reception. The native
snapshot at this point still equalled version 2 and its earlier bytes/mtime.
That snapshot is an observation at that time; it is not a definitive
nonexecution, rollback or remote cancellation guarantee.

A new request used independently obtained current child placement. Its READ
matched version 2 and native bytes. A different APPEND advanced native state to
version 3, added exactly one `fresh-child-once` suffix and set mtime
`1700001110.987654321`. The stale sentinel was absent at this observation.
Both earlier unknown submissions retained their original placement and epoch;
neither was replayed. Restoring eight PGs preserved this current version 3
head exactly, preserved the original fixture head, and preceded deletion of all
nine owned/reserved UUID heads. Cleanup reports all objects removed, including
already absent reserved names.

## Cross-scope correction and local checks

Independent review found that a completed scope's stale input could survive
into a later scope. Because the daemon retains historic operations, that old
input and history could otherwise be reattributed to a new token. The helper
now validates an existing stale input against the completed prior scope,
archives it under that prior token, and removes only that validated input
before any new observation or pool mutation. Orphan and mismatched inputs are
rejected and preserved. The Go test no longer blindly removes this file.

Seventeen Python mock tests pass, including old-history/new-token separation,
orphan-input rejection before observation/mutation/worker launch, exact native
history identity, ownership, uncertain first configuration changes, timeout
restoration, and PGP-before-PG cleanup ordering. Five fixture control cases
reject unsupported profiles, multiple OSDs and invalid actions before native
execution. The Go integration test compiles and skips without explicit opt-in.
These local checks do not themselves prove daemon interoperability or a second
live scope in the same fixture. Final profile results are tracked separately
above; preserve the initial staged run's narrower evidence boundary.

## Reproduction and diagnostics

From the repository root:


The secure configuration selects `container`, `memstore`, `secure`, `aes256k`
for both key and service cipher, and IP family `6`. No BlueStore durability
claim follows from a MemStore result. Native control changes use the ordinary
MON PG/PGP target commands; actual counts are advanced by MGR, including one-PG
merges. No daemon restart or provider repository changes are required.

This covers one controlled single-OSD 8→16→8 transition with explicit caller
placement. It does not establish concurrent multi-client split behavior,
three-replica split/recovery, arbitrary PG counts, automatic placement,
restart durability, EC, CRUSH variants or repeated live scopes with retained
historic entries. Those remain separate verification boundaries.
