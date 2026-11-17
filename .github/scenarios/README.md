# CI checks and selected interoperability scenarios

Automatic pull-request CI is Docker-free and has no scenario or OS/version
matrix. It checks Go formatting, builds, vet, workflow syntax, Python tools,
product dependencies and unit tests on Linux. Unit-test race detection is part
of these basic checks; `integration/` tests are excluded. Fixture-tool unit
checks run in their separate Go module without starting a cluster. A new PR
commit cancels its previous basic checks. Push events do not repeat PR CI.

See [Code quality](../QUALITY.md) for separate static analysis, vulnerability
checks and advisory structural PR reports. These do not select native scenarios.

This document owns scenario selection, the result gate and the inventories.
Which native path a scenario covers, and where its retained record is, is in
the [native path coverage map](../../integration/fixture/coverage/README.md).
The path from a feature's contract through its scenario to the observed
result is in [Tracing one feature](../../DOCUMENTATION.md#tracing-one-feature).

## Change-based advice and documentation-only checks

Basic CI and Code quality each call `change-plan.yml`. Its read-only planner
records the event base, requested PR head, checked-out commit, changed paths and
file modes. The summary contains a native-group recommendation table; the JSON
artifact is retained for seven days. It does not dispatch native jobs, change
`/ci run`, or filter periodic/platform/native matrices.

The initial recommendations use directory-level heuristics: `internal/mds`
changes recommend all MDS groups; `internal/osd` changes also include coupled
MDS and RBD groups. Shared transport/auth/session/maps, public API, fixture,
CI/tooling, contracts and unknown paths recommend every native group. Multiple
changes union the recommendations. A group outside a heuristic is not proven
unaffected. Declaration/call-graph analysis and automatic native-test reduction
remain separate future work.

Only same-repository PRs that modify ordinary non-executable files in the narrow
allowlist (`README.md`, `THIRD_PARTY_NOTICES.md`) may skip basic and
quality work. The planner additionally scans tracked Go/Python/shell/workflow
source for literal references to those documents and root Go embed directives;
a referenced document is treated as an input. This conservative regression guard
can reject harmless references and does not prove absence of dynamically
constructed paths. Expanding the allowlist requires source review. `SPEC.md`,
contracts, ledgers, package READMEs, fixture records and every other path run
checks even when they are Markdown.

Deletion, addition, rename, executable-mode change, symlink/submodule change,
empty diffs, forks, manual/scheduled runs and unproven Git comparisons never
qualify for a skip. Renames are compared as deletion plus addition. A checkout
must be the requested head containing the event base, or its two-parent test
merge with the requested head as the second parent and the event base in the
first parent's ancestry. A newer target's changes remain in the compared tree.
Missing history or planner failure runs downstream checks; only an explicit
`docs_only=true` output skips them.

Workflows still trigger on every PR. Basic/fixture/tooling jobs may be skipped;
quality matrix jobs retain their expanded check names and report that work was
not executed by policy. A policy skip is not test/scan success evidence. Weekly
vulnerability scans still execute. Branch protection settings are not modified.

## Catalog and planner

The catalog is the directory [`catalog.d`](catalog.d/). `_catalog.json` holds the
catalog version and the fixed check suites. Every row group has its own
`<group>.json`, `{"scenarios": [...]}`, holding that group's rows, and each row
keeps its `group` field. A new row goes into its group's file only; a new group
is a new file. `tools/scenario_catalog.py` composes the directory for every
reader (the planner, the change planner, the coverage map check and the
fixture's Go tests use it or its Go counterpart `ciLoadCatalog`). The composed
rows are in file-name order of the fragments and in file order within each
fragment. A duplicate identifier in any two fragments, a row whose `group` is
not its file's name, an empty fragment and any other file or directory are
refused.

### Fixture capabilities

A destructive or test-specific fixture mode (a MON SIGKILL, an injected OSD
fault, an exclusive owned fixture, the four-OSD topology and so on) is
admitted by a fixture capability, never by the fixture naming a test. The row
whose test needs the mode declares it in the parameter `capabilities`, a
sorted comma-separated list such as `"osd-erasure-width,osd-pg-interval,osd-shard-history"`.
The native workflow forwards it as `CEPH_MSGR_TEST_CAPABILITIES`, and the
fixture admits a mode only when its capability is declared, `test_run` is
exactly one anchored test (`^TestName$`) and the profile meets the
capability's requirements. A row with a capability therefore selects one
test; the tests that once shared a two-test row with a capability
(`TestCephMonMapVersion*KillIntegration`, `TestCephOSDErasureOptimizedK2M2`
with `...FailoverIntegration`, and `...InFlightMutationIntegration` with
`...SnapshotAndDegradedWriteIntegration`) now have a row each.

The vocabulary is the `defineCapability` calls in
[`integration/fixture`](../../integration/fixture/capability.go), each beside
the mode it admits, with its requirements: workflow-forwarded variables the
profile must have (`OSD_REPLICAS=4`), capabilities it extends (`+name`) and
capabilities it excludes (`!name`). Those terms are only part of what the
fixture checks: the topology a capability runs on has its own rules (the
four-OSD topology, for example, admits only BlueStore with the `legacy-v6` or
`replicated-pg-v8` profile). The planner therefore keeps no copy of either.
Every reading of the catalog refuses a capability on a `test_run` that is not
one anchored test, and `scenario_plan.py validate` runs the fixture's
`TestCatalogRowsAreAdmittedByTheFixture`, which passes every row, as the
native workflow forwards it, through the fixture's `loadProfile`. A row that
declares an undefined capability, or a capability on a topology the fixture
refuses for any reason, fails validation with the fixture's own error, the
same refusal the fixture gives before it starts a cluster. `validate`
therefore needs the fixture module's Go toolchain (1.26 or later). A new
control adds its `defineCapability` call beside its own code and the
declaration to its own row; it edits no shared selector list.

A documented reproduction runs a row (scenario identifier `ID`) with the
environment the catalog gives it, so the declaration and the topology cannot
drift from the row:

```sh
(
  env_lines=$(python3 tools/scenario_plan.py env ID) &&
  eval "$env_lines" &&
  sh integration/run.sh
)
```

For an unknown row `env` prints nothing on stdout and exits non-zero. The
assignment keeps that status (`eval "$(...)"` would replace it with the
success of evaluating an empty string), so the `&&` chain never starts the
runner with the environment the shell already had, and the subshell keeps one
row's environment out of the next. A record that reproduces two rows joins
their subshells with `&&`.

`validate` refuses a documented `env` of an unknown row, a documented `env`
lookup that is not this checked form, and a hand-written reproduction that
selects the exact test of a row with capabilities without declaring them in
the same code block or paragraph.

The fixture reads only the capability value and the exact selector. It does
not know that a catalog row supplied them, so a declaration placed beside the
test can supply the same two values later without a fixture change.

### Fixture sharing

Every native test declares beside its function whether a later test may use
the fixture it ran on: `//fixture:shareable <reason>` or
`//fixture:isolated <reason>` as the last line of its doc comment. A row has
no field of its own: it is shareable only when every test its effective
selector selects is. The fixture derives each row's value
(`TestCatalogRowsDeclareFixtureSharing`), and `scenario_plan.py validate`
runs it and refuses a native test without a valid declaration, a misplaced
declaration and a row without a value. `scenario_plan.py sharing` lists the
shareable and isolated rows of each fixture configuration (a row's
parameters without `test_run`) and the job count of the default coverage.
`plan` runs the selected shareable rows of one configuration that have an
explicit `test_run` as one fixture sequence job (at most eight rows, in
catalog order); every other row, and a request that selects one row of a
configuration, keeps a job of its own. The sequence runs each test alone and
requires the start-state check after every test. The rule, the
classification and the sequence are in
[FIXTURE_SHARING.md](../../integration/fixture/FIXTURE_SHARING.md).

The catalog preserves the original 63 native interoperability matrix rows and adds
the two opt-in `mgr-service` rows for Go service registration.
Fifteen further `osd` rows use the four-OSD erasure-capacity topology
(`osd_replicas` value `4`), each with one test. The first selects
`TestCephOSDErasureK2M2Integration`, the topology's default selection; every
other one declares `osd-erasure-width`. The second to the fifth select
`TestCephOSDErasureOptimizedK2M2Integration`,
`TestCephOSDErasureOptimizedK2M2FailoverIntegration`,
`TestCephOSDErasureOptimizedK2M2InFlightMutationIntegration` and
`TestCephOSDErasureOptimizedK2M2SnapshotAndDegradedWriteIntegration`. The sixth
selects only `TestCephOSDErasureOptimizedK2M2AppliedResubmitIntegration`: one
explicit caller resubmission of an independently observed applied APPEND after
primary replacement, with a fresh-transaction append as the byte/version control.
The seventh selects only
`TestCephOSDErasureOptimizedK2M2PrimaryHistoryCalibrationIntegration`: one
bracketed snapshot of the primary's native PG log that must contain two known
Go requests by their full request identity. It calibrates presence detection
only ([validation](../../integration/fixture/OSD_PG_HISTORY_CALIBRATION_VALIDATION.md)).
The eighth selects only
`TestCephOSDErasureOptimizedK2M2CopyFromHistoryIntegration`: one version-pinned
COPY_FROM2 from an owned replicated pool into the optimized pool, whose two
linked request identities are required on the live primary and then looked up
in a private store copy of every acting shard
([validation](../../integration/fixture/OSD_EC_COPY_FROM_SHARD_HISTORY_VALIDATION.md)).
The ninth selects only
`TestCephOSDErasureOptimizedK2M2PartialWriteShardHistoryIntegration`: one full
write and one overwrite confined to shard 0's chunk, whose request identities
are required on the live primary and then looked up in a private store copy
of every acting shard. The shards the overwrite names must hold its entry;
the nonprimary data shard it leaves out must not, and its own log must end at
the full write
([validation](../../integration/fixture/OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md)).
The tenth and the eleventh continue that sequence and capture every acting
shard again
([validation](../../integration/fixture/OSD_EC_PARTIAL_WRITE_LATER_STATE_VALIDATION.md)).
`TestCephOSDErasureOptimizedK2M2PartialWriteLaterWriteShardHistoryIntegration`
sends one more overwrite, inside the left-out shard's chunk, in the same
interval: every shard must hold its entry, and the left-out shard must hold
the full write and this write without the overwrite between them.
`TestCephOSDErasureOptimizedK2M2PartialWriteNewIntervalShardHistoryIntegration`
ends the PG's interval once with an owned, self-restoring `pg repeer` and
requires the PG clean on its original placement: the left-out shard's log
head must then be at the overwrite without its entry, and the first write of
the new interval must be held by every shard.
The twelfth and the thirteenth continue the same sequence with an outage of the
left-out shard's OSD, through the fixture's forced kill and start of an owned
OSD, and capture every acting shard once the PG is clean on its original
placement again
([validation](../../integration/fixture/OSD_EC_PARTIAL_WRITE_RECOVERY_VALIDATION.md)).
`TestCephOSDErasureOptimizedK2M2PartialWriteShardReturnShardHistoryIntegration`
writes nothing while the OSD is away: the returned shard's log head must be
at the overwrite without its entry.
`TestCephOSDErasureOptimizedK2M2PartialWriteDegradedWriteShardHistoryIntegration`
sends one overwrite inside the absent shard's chunk while the PG is active
without it: after the recovery the returned shard must hold all three
entries, the overwrite's included, with an empty missing set.
The fourteenth,
`TestCephOSDErasureOptimizedK2M2PartialWritePrimaryReplacementShardHistoryIntegration`,
continues it with an outage of the primary's own OSD
([validation](../../integration/fixture/OSD_EC_PARTIAL_WRITE_PRIMARY_REPLACEMENT_VALIDATION.md)):
the PG must become active under the first parity shard's OSD with a
pg_temp, one overwrite inside the absent shard 0's chunk goes through that
primary, and after the original primary's return and recovery the left-out
shard must hold the full write and that overwrite without the earlier one,
while every other shard holds all three entries.
These seven rows declare `osd-shard-history`, which admits the store-copy
step, and the last five also `osd-pg-interval`, which admits the owned-pool
oracle's `pginterval` mode that observes one owned PG and can end its
interval. The topology value travels in the already-forwarded `osd_replicas`
input; the fixture refuses it with any selector other than its default or
one test that declares `osd-erasure-width`.
The fifteenth selects only
`TestCephOSDErasureOptimizedK2M2RequestHeldAtDyingPrimaryIntegration`
([validation](../../integration/fixture/OSD_EC_HELD_REQUEST_VALIDATION.md)):
one Go APPEND+SETXATTR that names a map epoch the cluster has not published
must be held unapplied in the primary's op tracker when the primary's OSD is
killed. The call must end with an unknown outcome under its original
identity, the native object must be unchanged after the return, and the
returned primary's PG log must not hold that identity until the caller
resubmits it once.
Two further `osd-cache-pools` rows: one selects only
`TestCephOSDCacheTierOpsIntegration`, the native-only cache-tier operation
checkpoint, and another selects only `TestCephOSDCacheOpGoIntegration`, the
Go-originated cache-tier operations with a native observer, including its
lost-reply CACHE_FLUSH subtest
(`lost_reply_flush_applied_without_caller_replay`). Both reuse the
already-forwarded `osd_cache_pools` input and declare
`exclusive-osd-cache-pools`; the cache-pool fixture accepts only its default
`TestCephOSDIgnoreCacheIntegration` or one test with that capability.
Two `mds-cephfs-capsend` rows select only
`TestCephMDSCephFSCapSendRecoveryIntegration` in the two selected MDS
profiles. They reuse the already-forwarded `mds_cephfs_data` input and declare
`exclusive-mds-cephfs-data`; the data fixture accepts only its default
`TestCephMDSCephFSDataIntegration` or one test with that capability or the
writeback-fence capability below.
Two `mds-cephfs-writeback` rows select only
`TestCephMDSCephFSWritebackFenceIntegration` in the same two MDS profiles and
reuse the same input; they declare `mds-writeback-fence`, which alone admits
the OSD drop, background reader and eviction controls.
One further `osd` row selects only `TestCephOSDSetChunkNativeIntegration`, the
native-only SET_CHUNK manifest checkpoint, on the single-BlueStore legacy-v6
fixture. It needs no opt-in parameter: the checkpoint uses the ordinary OSD
fixture's owned plain pools, so the test also belongs to the broad legacy OSD
selection. A second such row selects only `TestCephOSDSetChunkGoIntegration`,
the Go-originated SET_CHUNK acceptance with native observation, on the same
fixture and likewise without an opt-in parameter.
Two `mds-cephfs-multi-reconnect` rows select only
`TestCephMDSCephFSMultiReconnectIntegration`, reusing the completion opt-in
for a fresh, isolated fixture in each selected MDS profile with
`exclusive-mds-cephfs-completion`. Existing completion rows keep their
original selector.
Two `mds-cephfs-heldflock-split-reconnect` rows select only
`TestCephMDSCephFSHeldFlockSplitReconnectIntegration` in the same two MDS
profiles, again on a fresh completion fixture. They declare
`mds-held-flock-split`, which alone admits the closed split-file lock probe,
level-15 debug window and per-capability reconnect log read.
Two `mds-cephfs-heldflock-split-fragments` rows select only
`TestCephMDSCephFSHeldFlockSplitFragmentsIntegration` in the same two MDS
profiles, each on a fresh completion fixture. It holds one lock in a nonfinal
and one in the final fragment of the split reconnect. They declare
`mds-held-flock-split` and `mds-held-flock-split-fragments`, which alone admits
the closed lock probe of the last split file.
Two `mds-cephfs-sharedflock-recovery` rows select only
`TestCephMDSCephFSSharedFlockRecoveryIntegration` in the same two MDS
profiles, each on a fresh metadata fixture (`mds_cephfs`). It runs the
recovery recipe of the `mds-cephfs` rows with two shared locks and declares
`exclusive-mds-cephfs`; the broad `mds-cephfs` selector does not reach it.
Two `mds-cephfs-fcntl-recovery` rows select only
`TestCephMDSCephFSFcntlRangeRecoveryIntegration` in the same two MDS
profiles, each on a fresh metadata fixture. It runs the same recovery recipe
with two FCNTL byte-range locks and one FLOCK lock and declares
`mds-held-fcntl`, which alone admits the closed FCNTL probe of the lock file;
the broad `mds-cephfs` selector does not reach it.
One further `osd` row selects only `TestCephOSDCopyFromLateErrorIntegration`
on the CRC/AES IPv4 host single-BlueStore legacy-v6 fixture. This owned daemon
fault requires `osd-copy-from-late-error` and the exact selector; broad suites
do not execute it.
A separate `osd` row on the same fixture selects only
`TestCephOSDCopyFromMultiChunkLateErrorIntegration`. It lowers the owned
daemon's copy chunk limit to force multi-chunk staging before the same
injected error, with its own capability `osd-copy-from-multi-chunk` and marker.
One further `osd` row,
`osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-osdcopyfromdestinationhistory`,
selects only `TestCephOSDCopyFromDestinationHistoryIntegration`: one Go source
write and one version-pinned COPY_FROM2 between two owned pools, with bracketed
primary PG-log snapshots that must show the source write's request identity
among the destination entry's extra request IDs. It injects no fault and needs
no opt-in parameter, so the test also belongs to the broad legacy OSD
selection ([validation](../../integration/fixture/OSD_COPY_FROM_HISTORY_VALIDATION.md)).
One dedicated `osd` row selects only `TestCephMsgrCompressionOfferIntegration`
on CRC/AES client and service cipher, IPv4 host runtime, one BlueStore OSD,
replica/min-size one and legacy-v6. Its ID is
`osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-msgrcompressionoffer`.
The existing `test_run` input overrides the broad OSD selector without
changing any other profile. Request that ID with `/ci run`; `all` includes it
in periodic main verification. The required-test gate binds it to
`api/TestCephMsgrCompressionOfferIntegration`, so a missing or skipped parent
cannot pass. The [selected native receipt](../../integration/fixture/MSGR_COMPRESSION_VALIDATION.md)
records no negotiated compression, successful MON/OSD commands and, since
2026-11-15, the enabled request and disabled response decoded on the serving
connections; it does not prove that compressed frames worked.
One dedicated `mon-mgr` row,
`mon-mgr.aes256k.ip4.t-monservicemaplifecyclefailedmonhold`, selects only the
client-suite `TestCephMonServiceMapLifecycleFailedMonHoldIntegration`. It
holds failed dials to a stopped MON for the setup timeout; see the
[lifecycle recipe](../../integration/fixture/SERVICE_MAP_LIFECYCLE_VALIDATION.md).
The planner checks `test_run` selectors against both the API (`integration`)
and client (`cephmsgr`) suites.
One further `mon-mgr` row,
`mon-mgr.aes.ip4.crc.host.t-appliednamedmanagertell`, narrows the broad AES/CRC
IPv4 host profile to the client-suite
`TestCephAppliedNamedManagerTellIsNotReplayedIntegration`: one applied
exact-name MGR Tell whose reply is withheld, observed on the named manager's
own admin socket. It needs no opt-in parameter, so the test also belongs to
the broad selection of ordinary `mon-mgr` rows; this row makes the result gate
require its PASS with decoded CRC frames. See the
[recipe](../../integration/fixture/MGR_TELL_OUTCOME_VALIDATION.md).
Four further rows require the remaining applied-command cells with a withheld
reply, all client-suite tests without an opt-in parameter of their own:

- `mon-mgr.aes.ip4.crc.host.t-appliedcurrentmanagertell` narrows the same
  AES/CRC IPv4 host profile to
  `TestCephAppliedCurrentManagerTellIsNotReplayedIntegration`, the cell for
  `MgrTell`, whose target comes from the client's MgrMap.
- `mon-mgr.aes256k.ip4.host.t-suite-e9be0b` runs both MGR Tell cells on one
  secure AES256K IPv4 host fixture. Secure frames are not decoded there; the
  cells use their padded-write mode
  ([recipe](../../integration/fixture/MGR_TELL_OUTCOME_VALIDATION.md)).
- `osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b6ed16`
  selects `TestCephAppliedOSDCommandIsNotReplayedIntegration` and
  `TestCephAppliedPGCommandIsNotReplayedIntegration` on one CRC
  single-BlueStore legacy-v6 fixture. The broad OSD selection does not match
  these names, so no other row runs them.
- `mds-commands.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-appliedmdscommand`
  selects `TestCephAppliedMDSCommandIsNotReplayedIntegration` on the owned MDS
  command fixture, which admits this one further test by its declared
  `mds-applied-command`.

Two further rows run those OSD, PG and MDS cells in secure mode, where frames
are not decoded and the cells use their padded-write mode, on the profile of
the existing secure rows of each fixture (AES256K, IPv6, container test
process):

- `osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b6ed16`
  selects the OSD and PG cells on one single-BlueStore legacy-v6 fixture.
- `mds-commands.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-appliedmdscommand`
  selects the MDS cell on the owned MDS command fixture.

The OSD, PG and MDS cells are described in the
[daemon command recipe](../../integration/fixture/DAEMON_COMMAND_OUTCOME_VALIDATION.md).
Two `mon-mgr` rows, `mon-mgr.aes.ip4.crc.host.t-senttellcancellation` and
`mon-mgr.aes256k.ip4.host.t-senttellcancellation`, select only the client-suite
`TestCephSentTellCancellationIntegration`: a sent `MonTell`, `MonTellTo`,
`MgrTell` and `MgrTellTo` whose reply is withheld, each ended once by the
caller's cancellation and once by `Client.Close`. The test ends MON and MGR
sessions on purpose and skips without this exact selector, so broad rows do
not run it. See the
[recipe](../../integration/fixture/TELL_CANCELLATION_VALIDATION.md).
One further `rbd` row,
`rbd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdmirrorpathrecovery`,
selects only `TestCephRBDMirrorPathRecoveryIntegration` on the single-BlueStore
IPv4 fixture. It needs no opt-in parameter and does not use the RBD base
opt-in: the fixture derives its owned destination cluster, rbd-mirror daemon
and directed source OSD fault from its declared `rbd-mirror` and the exact
selector. See the
[mirror recipe](../../integration/fixture/RBD_MIRROR_VALIDATION.md).
Four `osd` rows select `TestCephMonMapVersionConnectedKillIntegration` or
`TestCephMonMapVersionUnprocessedKillIntegration` alone, each test on CRC with
one BlueStore OSD and legacy-v6 in two profiles:
`osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-monmapversionkill`
and
`osd.aes256k.ip6.crc.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-monmapversionkill`
for the connected cell, and the same IDs ending in `t-monmapversionunprocessedkill`
for the unprocessed cell. They declare `mon-forced-kill`; the fixture admits
SIGKILL of a MON and uses 120 s tickets only with that capability for one
exact test. Broad OSD rows match the two names and report them as skipped. See the
[kill record](../../integration/fixture/MON_MAP_VERSION_KILL_VALIDATION.md).
Two `mon-mgr` rows, `mon-mgr.aes256k.ip4.t-monservicemaplargemap` and
`mon-mgr.aes.ip4.crc.host.t-monservicemaplargemap`, select only the
client-suite `TestCephMonServiceMapLargeMapIntegration`. It holds native
registrations above a watch budget, a frame limit and the collection cap and
runs only under its exact selector, so broad rows report it as skipped. See
the
[limits record](../../integration/fixture/SERVICE_MAP_LIMITS_VALIDATION.md).
Two further `rbd` rows,
`rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournallostappendreply`
and
`rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournallostappendreply`,
select only `TestCephRBDJournalLostAppendReplyIntegration` on the two RBD
profiles, each on a fresh fixture. They reuse the already-forwarded `rbd`
input and declare `rbd-journal-lost-append`; the RBD fixture admits this one
further exact test by that capability and then serves a different closed
journal action sequence. The broad RBD selector does not
match the test, so no other row runs it. The CRC row attributes the withheld
reply by transaction ID; the secure row cannot and says so in its record. See
the
[lost-append record](../../integration/fixture/RBD_JOURNAL_LOST_APPEND_VALIDATION.md).
Two further `rbd` rows,
`rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerloss`
and
`rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerloss`,
select only `TestCephRBDLockOwnerLossIntegration` on the same two profiles,
each on a fresh fixture. They reuse the `rbd` input too and declare
`rbd-lock-owner-loss`; the RBD fixture admits this exact test by that
capability and then serves a different closed lock action
sequence, in which one native driver process ends by SIGKILL and two client
addresses are blocklisted until the fixture's cleanup. No other RBD row
matches the test. Each row waits twice for a 30 s watch to expire, so its test
runs for about a minute. See the
[lock-owner-loss record](../../integration/fixture/RBD_LOCK_OWNER_LOSS_VALIDATION.md).
Two further `rbd` rows,
`rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerlosspending`
and
`rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdlockownerlosspending`,
select only `TestCephRBDLockOwnerLossPendingIntegration` on the same two
profiles, each on a fresh fixture, and declare `rbd-lock-owner-loss` and
`rbd-lock-pending-requests`. The test is the lock-owner-loss sequence
with two requests of the first owner held on the OSD through both handoffs.
Its closed lock action sequence has two more actions; in one of them the
fixture sets and clears a flag of the owned pool until the OSDMap epoch is
eight higher. No other RBD row matches the test. Each row runs for a little
more than a minute. See the
[pending lock-owner-loss record](../../integration/fixture/RBD_LOCK_OWNER_LOSS_PENDING_VALIDATION.md).
Two further `rbd` rows,
`rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalprimarychange`
and
`rbd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-legacy-v6.t-rbdjournalprimarychange`,
select only `TestCephRBDJournalPrimaryChangeIntegration`. They are the only
RBD rows on three OSDs: they declare `rbd-journal-primary-change`, and the RBD
fixture admits `osd_replicas` 3 with BlueStore for that capability alone, creates its owned pool with three replicas and
one PG, and serves the lost-append sequence with two more actions, in one of
which the fixture marks the PG's primary out. Both rows use BlueStore; they
differ in connection mode, key type, address family and runtime as the other
RBD rows do. See the
[primary-change record](../../integration/fixture/RBD_JOURNAL_PRIMARY_CHANGE_VALIDATION.md).
Four further rows narrow a broad selection to tests that need no opt-in
parameter and also run there, so that the result gate requires their PASS:

- `osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b300c0`
  and
  `osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b300c0`
  select `TestCephOSDCommandsIntegration` and
  `TestCephOSDCommandCrashIntegration`: an OSD and a PG command ended by the
  caller after their observed local write, and the two classifications of a
  command to a killed OSD. The in-flight crash test is not selected.
- `mon-mgr.aes.ip4.crc.host.t-suite-afb7b2` and
  `mon-mgr.aes256k.ip4.host.t-suite-afb7b2` select the client-suite
  `TestCephCompleteMonitorOutageIntegration` and the API-suite
  `TestCephNamedMonitorTellIntegration` and
  `TestCephNamedManagerTellIntegration`: every command channel while no MON
  is reachable, the dial cause of an exact MON Tell, and an exact MGR Tell
  across a manager failover.

See the
[record](../../integration/fixture/COMMAND_TARGET_LOSS_VALIDATION.md). The
sent cancellation rows above now also run `MonCommand` and `MgrCommand`
subtests, and the public MDS command rows a last subtest that stops the
fixture's MDS.
Each entry has a stable identifier, group and concrete fixture parameters.
Only Tentacle 20.2.x is accepted. Every catalog row is in the default coverage
(`all`, which the periodic run selects); there is no separate list of required
rows or row total to edit, and deleting a row is a visible catalog change.

The trusted planner refuses catalog parameters that its native workflow cannot
forward. Land new workflow opt-ins before requesting a feature PR's new profile;
checking out that PR does not replace the default branch's reusable workflow.
The MGR-service opt-in is forwarded in advance of its feature implementation.

Native runs use the calling revision's result gate. For an explicit `test_run`,
it lists the matching top-level tests that `go test -list` reports for the tested
checkout's `integration` and `cephmsgr` packages before execution
and requires a complete PASS result for every selected test. A skipped, missing
or failed selected test fails the job even if the process exits zero. An empty
secondary package is permitted; a selector matching no tests is refused.
Selectors address top-level tests only, without subtest paths. Broad fixture
defaults retain their optional skips and require at least one actual PASS;
success does not claim that those optional tests ran. Scenario artifacts include
the expected tests, observed results and verdict in `tests.json`.

The catalog also names five selectable check suites:

| Selector | Work |
| --- | --- |
| `unit` | Product unit checks on Linux, macOS and Windows, Go 1.25/1.27 |
| `fixture-tools` | Fixture-tool unit checks on those three operating systems |
| `race` | Race detection against actual Tentacle MON/MGR sessions |
| `fuzz` | Bounded fuzz runs of protocol decoders |
| `windows-ceph` | Windows binaries against actual MON/MGR through WSL |

The `race` suite is an integration test. The basic unit race check does not
start Ceph and is independent of this suite.

```
python3 tools/scenario_plan.py validate
python3 tools/scenario_plan.py list
python3 tools/scenario_plan.py plan --select group:rbd --select windows-ceph
```

The tools never read `func Test...` text to decide which top-level tests exist.
`validate`, the native result gate, the cluster-free gate, the row admission
tests, the coverage map check and the protocol evidence check all ask
`go test -json -list` through `tools/go_test_list.py`, per Go module with
`GOWORK=off`, `CGO_ENABLED=0` and `-mod=readonly`. Listing compiles the test
binaries and runs no test body. A missing `go`, a build failure or unexpected
listing output is an error, never an empty set. `go test -list` says which tests
exist, not where they are declared: a check that ties a test to a file (the
protocol evidence links, test-named links in the coverage map) also needs Go's
parser to find the declaration in that file (`go_test_list.declares`, through
`tools/quality/testdecls` under the go/build context). That file check counts
only the test-shaped declarations of the looked-up test, so other functions of
the file, such as repeated `init` functions, do not affect it.

The listing keeps how often Go lists each name. The internal and the external
test package of one directory may both declare a test of the same name, and Go
lists and runs both under that one name. The checks that select tests for a run
or read their result lines (the selector check of `validate`, the native result
gate's `snapshot` and the cluster-free gate) refuse a selected name that Go lists
more than once, before any result is read: `-run` cannot select one of the two
tests and a `--- PASS` line cannot say which one it reports. The checks that ask
only whether a name exists or where it is declared read the names.

The coverage map check also lists the fixture module, and `validate` runs
cluster-free tests of the fixture module, so the Python tool tests need Go 1.26 or later.
Basic CI and the platform unit matrix run them with Go 1.27 and install the
product's Go 1.25.0 (or the matrix Go) afterwards for the product steps;
`tools/test_workflow_toolchains.py` refuses a workflow step whose installed Go
is older than the modules its tools load.

The planner emits native rows in `include` and selected check suites in `fixed`.
`--catalog` reads the tested revision's vocabulary using the trusted planner.
It takes a fragment directory, or the single `catalog.json` that held the
catalog before it was split. For `/ci run`, the trusted
`scenario_catalog.py extract` copies the tested revision's catalog blobs from
Git objects, without checking the revision out, and refuses a revision with
neither layout, with both, or with a symlink, subdirectory or unknown file in
`catalog.d`. The single-file path exists only for pull requests opened before
the split and can be removed once none of them is open.
Unknown selectors fail rather than silently matching nothing. Validation rejects
duplicate identifiers, unsupported releases, missing parameters, bad or empty
test selectors, selectors matching no test and missing suite definitions.

## Test inventory and selection evidence

Basic CI verifies two inventories without starting a cluster:

- The native test inventory is every top-level `TestCeph...Integration` declared
  in `integration` and `cephmsgr`; there is no list to edit. Fixture tests parse
  Go declarations, reject duplicate names, and compare the source set with the
  compiled `go test -json -list` result. The current check requires these tests
  in the default build; adding platform-constrained tests needs an explicit
  inventory-policy change.
- `fuzz-targets.json` registers each package/target pair. The product job compares
  it with the root module's compiled fuzz list. Adding, renaming or removing a
  fuzz function without updating the inventory fails CI. This checks the current
  runner's default build, not every GOOS or optional tag combination.

`TestCICatalogInventory` reads the native workflow's actual matrix-to-environment
bindings (including fallback values) and passes each catalog row through the
fixture's real `loadProfile`. Unsupported expressions, unforwarded parameters,
invalid fixture combinations, rows whose effective selection includes no native
test and native tests that no row selects fail the check. It uses Go regular
expressions, matching the native runner's selector semantics. This complements
`scenario_plan.py validate`; it does not replace the catalog checks.

The `native-test-selection` artifact lists the source location, candidate scenarios
and explicit-selector required scenarios for every native test. `required_scenarios`
means that the existing native result gate requires that top-level test to PASS
when the scenario runs. It is not evidence of an actual PASS. Broad candidates
can still skip because of per-test environment requirements. The artifact's counts
describe selection, not verified interoperability. A new native test needs no
registration; some catalog row's selector must include it, and its intended
fixture and selector still need review. Selection alone cannot prove that all of
its skip conditions are satisfied.

Requested/periodic `fuzz` now runs all 26 registered targets, including wire,
MDS and additional CephX/OSD decoders omitted from the former ten-command list.
Every target gets a 20-second fuzz budget, two workers, a 90-second Go timeout
and a 120-second process bound. The job has a 20-minute bound. It verifies the
compiled inventory before executing anchored target names and requires target
RUN/PASS plus package PASS, without FAIL/SKIP events. JSON logs and a result
manifest are retained for seven days. A failed target does not hide later results;
the final verdict fails if any target failed, timed out or did not complete.
Basic PR CI only lists/validates fuzz targets; it does not run this fuzz campaign.

For a bounded Linux smoke check:

```sh
python3 tools/fuzz_targets.py run --fuzztime 1s --output OUTPUT_DIRECTORY
```

The output directory must not already exist. A one-second smoke run is not the scheduled 20-second
campaign. `python3 tools/fuzz_targets.py verify` only validates selection.

## Comment selection

```
/ci run <scenario-id | group:<group> | unit | fixture-tools | race | fuzz | windows-ceph | all> ...
```

For example, `/ci run windows-ceph race` runs those two suites without starting
the native scenario matrix. `/ci run group:rbd` runs just the RBD rows. Rows
that a request selects together and that may share a fixture run as one
fixture sequence job; a single named row always runs alone.
`group:checks` selects all five check suites. `all` selects everything.

One command per comment, on its own line, with at most 20 selectors. Quoted and
fenced lines are ignored, including longer Markdown fences. Malformed commands,
unauthorized callers, closed PRs, forks and unavailable vocabulary are refused
without running PR code. Only owners, members and collaborators are accepted.

Trusted default-branch control code records the PR head once and resolves names
against that revision. Test jobs check out that exact SHA with read-only access
and no persisted credentials. Separate trusted jobs publish a request-specific
`ci-command/run-<run-id>` status and summary. A failed, canceled, skipped or
missing selected suite cannot become a pass; unselected suites are skipped.
Later requests preserve earlier statuses and results. Each PR has its own
command concurrency group, separate from basic checks. GitHub permits one
running and one pending request per group; a newer pending request can replace
an older pending request.

Native jobs run with at most eight concurrent runners and keep per-scenario
commit, image, result and run artifacts. The selected platform suites retain
their existing test and time bounds. Module downloads fall back from the Go
proxy to the origin while `go.sum` verification remains in force. The native
runtime explicitly pre-pulls its pinned Ceph image before harness execution.

## Manual selection

`ci-command.yml` also accepts manual dispatch with required `selectors` and an
optional `pull_request` number. The selector grammar, catalog, refusal policy,
read-only test runners and request-specific statuses are shared with comments.
There is no implicit `all` selection. Dispatch the workflow on the default
branch (`main` here); another branch or a tag is refused before checkout or
test execution, without a write token in that refusal job. GitHub itself
requires repository write access to
[dispatch a workflow](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow).

```sh
# One native row on the default-branch revision captured by this dispatch:
gh workflow run ci-command.yml --ref main \
  -f selectors='mon-ping.aes.ip4.crc.host.t-monping'

# Select checks on an open same-repository PR (example PR number):
gh workflow run ci-command.yml --ref main \
  -f pull_request=123 -f selectors='unit fixture-tools'
```

With a PR number, trusted control reads its head once and applies the same
open/non-fork checks as a comment request. An empty number tests the exact
default-branch workflow commit, not a moving branch name in each job. Control
scripts and the native result gate use the calling workflow SHA so a newer
catalog cannot silently outrun that runner's capabilities. PR and default-branch
requests each have separate command concurrency groups; baseline CI remains
independent.

Every accepted request publishes its pending/final status on the tested SHA.
The Actions summary always records selection, SHA, result and run link; a PR
target also receives the existing summary comment. Main-only requests post no
PR comments. Invalid selectors, malformed PR numbers and unavailable PR/catalog
metadata are refusals, never permission to select all or execute an unpinned
revision. A rejected nonexistent PR is reported in the Actions summary without
attempting to comment on it.

## Periodic main verification

`periodic.yml` runs all native rows and all check suites every Sunday at
03:17 UTC, and on manual dispatch. It explicitly checks out `main` once and
passes that SHA to every test job, even if manually dispatched from another
branch. Native scenario results are retained for seven days. The final verdict
requires both native and platform suites to succeed; failures and skipped selected
suites are not a full-coverage pass. Periodic runs have a separate concurrency
group and do not cancel PR checks.

## Storage and image reuse

Artifact retention follows the purpose of each upload:

| Artifact category | Retention | Current uploads |
| --- | --- | --- |
| General results | 7 days | Scenario result/test JSON for requested and periodic runs; reviewed explicit cap-send and four-minute soak success bundles; successful Windows interoperability evidence |
| Failure diagnostics | 14 days | Failed native, race and unit-test logs/metadata; Windows evidence when the job fails |
| Deployment evidence | 30 days | Policy for future deployment workflows; no deployment workflow currently uploads evidence |

Successful explicit cap-send rows additionally request the closed
[schema-v1 native evidence bundle](NATIVE_EVIDENCE.md). Its manifest joins
tested/controller SHA, execution source, run ID/attempt, scenario/selector,
normalized requested profile, configured image, observed runtime facts and
included-file hashes. Only reviewed numeric/outcome/cleanup projections are
uploaded for seven days; raw metadata, logs, configuration and credentials
remain excluded. `result.json` distinguishes a native test PASS from incomplete
required evidence, and the pilot's final gate requires both collection and
upload. The single four-minute OSD soak row requests a second closed
family (`native-soak-success`, a fixed projection of its raw receipts with
hashes; see [the soak section](NATIVE_EVIDENCE.md#four-minute-osd-soak-family)).
Other rows keep their previous retention boundary. This new trusted
collector becomes available to PR-comment execution after its workflow lands
on the default branch; pre-merge native requests keep the older runner.

Diagnostic and Windows evidence uploads use compression level 9. Periodic runs
use the same retention policy as requested runs. These settings affect new
uploads; they do not shorten existing artifacts' retention or delete historical
evidence. Future deployment uploads must explicitly set `retention-days: 30`.

Native scenario jobs restore a shared Docker image archive from Actions cache.
The exact image reference, runner OS and architecture identify the cache; scenario,
commit, Go version and run ID do not create additional copies. Only digest-pinned
images are eligible, with no prefix fallback to another image. The first native
matrix job of a scheduled default-branch run may save a missing archive before
running its tests. Requested PR revisions only restore it. This avoids a writer
per scenario and branch-specific archives. Jobs starting before the archive is
saved still pull independently. Other image references remain cold until a
scheduled first row uses them. A saved archive may be at most 2 GiB before cache
compression.

After loading an archive, each job still pulls the original pinned reference:
Docker archive loading can omit `RepoDigests`, so skipping that pull would not
preserve fixture image identity. The pull reuses local layers and retains the
registry manifest/digest check. Cache restore, load and save have five-minute
bounds and are optional; a miss or cache failure falls back to the existing
bounded registry pulls. Registry failures still fail the job. No Docker images
are uploaded as workflow artifacts. Ryuk continues to use its small, retried
registry pull. Go dependency/build caches remain managed by `setup-go`.

According to
[GitHub's Actions billing documentation](https://docs.github.com/en/billing/concepts/product-billing/github-actions),
artifact/Packages storage and the included 10 GiB per-repository cache allowance
are separate. Reusing image layers reduces registry downloads; shorter artifact
retention reduces future artifact storage accrual. Neither change resets
already accrued account usage. Repository limits and account budgets are not
changed by these workflows.

Cache service behavior, eviction and default-branch visibility follow the
[Actions cache contract](https://github.com/actions/cache). Hosted-runner cache
hit rates and overall workflow timing require live CI measurement; an archive
restore is not evidence of native interoperability or a faster full matrix.

On a Linux amd64 Docker 29.9.1 runner, saving the pinned `v20.2.4` image by
digest produced a 542,868,480-byte archive, below the cache limit. Loading that
archive and pulling the same reference preserved the image ID. That check used
an already populated Docker store; it does not measure transfer savings on a
fresh hosted runner. The 500 Python tool tests, catalog validation and focused
fixture reaper workflow check, and actionlint validation of all workflows passed
on Linux. Live Actions cache upload/restore and full native coverage remain
separate verification.

## References and evidence

The separation of code checks, requested runtime checks, independent request
statuses, module-proxy fallback and bounded runtime concurrency follows the CI
architecture in `ceph-testcontainers-go` at
`e19e5218f8aab2a6befde5a0ee140bb1f5682850`. No source was copied. This project
retains its own Tentacle-only catalog, fixture inputs and coverage contracts.

Planner/parser and workflow-contract tests are synthetic checks. Native daemon
interoperability and a live PR-comment request are separate evidence. Comment
workflows become available only after their definition reaches the default
branch. The live request on PR #29 passed at
`523cfa254a7639a7574c34da7fe00f8a4d866bb4` in
[run 39568633356](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39568633356):
one expected MON-ping test actually passed, its artifact and final status named
the same SHA/image, and no platform matrix was selected. This is a bounded
request receipt, not full periodic coverage. After #30 merged, manual selection
on main passed at `da661e737bca69f3dd5695eed9ba26a13188ea55` in
[run 39574326673](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39574326673).
It selected only `mon-ping.aes.ip4.crc.host.t-monping` (Linux/amd64 host,
CRC, AES client/service, IPv4). The expected `TestCephMonPingIntegration` and
five subtests actually passed, `tests.json` reported no problems, and the final
request status and result artifact named that SHA and the pinned Ceph 20.2.4
image digest. All seven fixture containers were terminated. No platform matrix
was selected and the main-only request posted no PR comment. These are two
bounded request receipts, not a full native matrix pass.
Declaration-level changed-scenario selection, repeated campaigns and durable
historical classification remain outside this checkpoint.
