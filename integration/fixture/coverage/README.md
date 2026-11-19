# Native path coverage map

This map answers the first work item of issue #14: every mandatory native path
of the [final test-environment scope](../../../SPEC.md#final-test-environment-scope)
is joined to its reproducible setup, control, independently attributed
observation, fault and cleanup recipe, and classified by where the required
fixture capability actually lives. It is an inventory of recipes and retained
records. It adds no fixture API, runs no native test and claims no new native
result. A row's class is not an acceptance verdict: final acceptance still
needs successful native operation, fault and cleanup evidence on the tested
SHA, as SPEC.md requires.

The map is split by area: the rows of each path of the final scope are in
one area file of this directory, listed under [Area files](#area-files).
This index holds what applies to every area.

Reviewed on 2026-10-23 against this repository at
`ecbcfafce65e9be8cf69f71e1dff7e035cac8759`, Ceph Tentacle v20.2.4 source
`7f793731f1b39eb4f465e960113d2363c311b964`, the pinned provider
`github.com/JSYoo5B/ceph-testcontainers-go v0.3.0` (tag commit
`c899ebcc83d6122992613dcb1bdb5420016ccc82`, see `go.mod` in `integration/fixture`)
and that provider's unreleased `main` at
`caf5357fc335aa1602ed3c344b39e4f8d65f2bb8`, 83 commits ahead of v0.3.0.

Updated on 2026-11-01, when `go.mod` in `integration/fixture` moved to
`github.com/JSYoo5B/ceph-testcontainers-go v0.4.0` (tag commit
`67e1498ee9f5ebbdb1d983125ae332616aacdf33`). Only the provider-dependent
classes and upstream receipt revisions were re-reviewed: the provider
capabilities that rows D2D-5, D2D-15, SVC-3 and D2D-18 need are released in
v0.4.0, so these rows are now `primitive`, and every upstream receipt cited
from `main` exists in v0.4.0 and is cited as `ctg@v0.4.0:`. v0.4.0 keeps every
v0.3.0 capability under the names that its own API uses, for example
`override.SetConfig` for `TemporaryConfig`, `override.SetOSDFlag` for
`TemporaryOSDFlag`, `fault.InterruptNetwork` for `InterruptNetwork`, the daemon
handles' `AdminSocket` for the cluster's `AdminSocket`, and
`MonitorContainer.Remove` for `RemoveMonitor`; rows that name a v0.3.0 API
keep the name the review used. No local recipe, record or `released` row
changed.

Updated on 2026-11-08, when `go.mod` in `integration/fixture` moved to
`github.com/JSYoo5B/ceph-testcontainers-go v0.5.0` (tag commit
`649fc9a75ee78c385d33fa5de9c2254098f5eec0`). No class changed. Every
upstream receipt cited as `ctg@v0.4.0:` still exists under the same test name
in v0.5.0 and keeps the revision it was reviewed at. v0.5.0 moves several
v0.4.0 APIs: CephFS sessions and session timeouts to package
`cephfs/session`, diagnostics, crash metadata and redaction to
`ceph/diagnostics`, the debug-level types to `override`, and `WaitForClean`
and `ServiceContainers` to `ceph.Observer`. It also extends
`override.SetDebugLevel` to MGR and MDS daemons, which the MDS completion and
data fixtures now use for `debug_mds` and `debug_ms`; that changes how a
local recipe sets a runtime level, not what any row observes.

## How to use this map

This map owns the classification of native recipes and where each retained
record is. It does not own wire contracts, implementation status or the
attribution of a native result:

- the public and wire contract of a path is the contract linked from its row
  or from [PROTOCOL_EVIDENCE.md](../../../cephmsgr/PROTOCOL_EVIDENCE.md);
- the tested source, profile and observed result are in the linked validation
  record. Where a receipt joins that record to a product wire row, its
  attribution is summarized in
  [PROTOCOL_EVIDENCE.md](../../../cephmsgr/PROTOCOL_EVIDENCE.md#native-receipts);
- how a scenario ID is selected and gated is in
  [the scenario README](../../../.github/scenarios/README.md).

A native-only recipe and a Go-originated recipe for the same operation are
separate rows (for example OSD-24 and OSD-26 for SET_CHUNK, OSD-17 and
OSD-23 for the cache-tier operations). Only the Go-originated row is evidence
for the Go wire path. The full reader path is in
[Tracing one feature](../../../DOCUMENTATION.md#tracing-one-feature).

## Classes

Each row has exactly one class. The order below is also the precedence: a row
takes the first class that applies.

| Class | Meaning |
| --- | --- |
| `released` | This repository has a reproducible recipe on the pinned provider: a catalog scenario or a documented opt-in selector, with a validation record or a named test. |
| `main-only` | The fixture capability the path needs (not only a recipe) exists only on provider `main`, not in the pinned release. |
| `primitive` | The pinned provider's public APIs express the native condition, but this repository has no recipe or record for it. An upstream provider receipt may exist and is cited as a reference, not as local evidence. |
| `proposed` | Neither a local recipe nor a provider capability exists; an open issue or unmerged pull request proposes one. |
| `absent` | No capability, recipe or open proposal exists. |

SPEC.md also names `unverified`. Here that is a property of evidence, not a
class: it appears in the Visibility column (historical record, first failure
retained, skipped by default, outcome only) so that a `released` recipe is
never read as an accepted path.

## Reading the evidence column

- `group:<name>` and full scenario IDs refer to
  [catalog.d](../../../.github/scenarios/catalog.d/); `suite:<id>` refers to
  its check suites. Every native row of that catalog is in the default
  coverage.
- A plain `TestCeph...` name is a top-level Go test that `go test -list` reports
  in the product, fixture or quality module.
- `ctg@v0.3.0:`, `ctg@v0.4.0:` and `ctg@main:` name a native receipt test of
  the provider at that revision. These are fixture readiness receipts with native clients;
  they do not exercise the Go implementation of this repository.
- The Independent observation column names the evidence class of each
  observation. **Native oracle**: a native Ceph tool, library or daemon admin
  interface, independent of this repository's Go implementation. **Go
  observer**: a second client of this Go implementation (for example the
  tests' `fixtureClient`); it is not independent of the product. **Local
  count**: a Go-side wrapper that counts sends or connections; it shows what
  left the client, not what a daemon executed. **Server result**: a code or
  payload as received by Go. A row whose observation is not a native oracle
  keeps that gap explicit in its Visibility column.
- Linked Markdown files are the retained validation records in `integration/fixture`.
  Their own attribution (exact commit, file hashes, historical working tree)
  is summarized in [PROTOCOL_EVIDENCE.md](../../../cephmsgr/PROTOCOL_EVIDENCE.md#native-receipts)
  and is not upgraded by this map.

## Selection visibility

Catalog rows run through the native result gate described in
[the scenario README](../../../.github/scenarios/README.md). An explicit
`test_run` row requires a PASS of every selected test. A broad row (the
`mon-mgr` rows and the plain `osd` rows) tolerates optional skips and only
requires at least one PASS, so a broad row's verdict does not establish that
a particular test in it ran. The Visibility column states which applies:

- **dedicated row**: the scenario's selector names the test; a skip fails it.
- **broad row**: the test runs under a broad selector and may skip, for
  example a three-replica test on a single-OSD row.
- **opt-in, not in catalog**: an environment switch selects it; no catalog row
  runs it, so CI never reports it.
- **staged native oracle**: the native comparison needs a separately staged
  pinned C++ oracle and skips without it; catalog rows do not stage it.
- **outcome only** or **native state**: what the independent oracle observed,
  following the provider's daemon-to-daemon terminology.
- **upstream receipt only**: the only native recipe is in the provider.

## Area files

Each path of the [final test-environment scope](../../../SPEC.md#final-test-environment-scope)
has one area file in this directory. A row's ID starts with the prefix of
its area file, and the row is in that area only. The file's title is the
path's name in SPEC.md, as in the table below.

| Prefix | Path | Area file |
| --- | --- | --- |
| `MSG` | Messenger/CephX | [MSG.md](MSG.md) |
| `MM` | CLIENT to MON/MGR | [MM.md](MM.md) |
| `OSD` | CLIENT to OSD | [OSD.md](OSD.md) |
| `MDS` | CLIENT to MDS/CephFS | [MDS.md](MDS.md) |
| `RBD` | RBD underlying I/O | [RBD.md](RBD.md) |
| `D2D` | Daemon-to-daemon | [D2D.md](D2D.md) |
| `SVC` | Service-induced paths | [SVC.md](SVC.md) |

Each area file has a Rows section with one table of the columns of every
area; no coverage table is outside it. Where it
applies, a Product wire gaps section follows it, then an Updates section with
the dated notes of changes to that area's rows. The rows of an area file have
numbered IDs (`OSD-24`); row IDs are never renumbered or moved to another
prefix.

A new row is a row file of its own in the area's directory,
`<PREFIX>/<PREFIX>-<name>.md`, for example `RBD/RBD-journal-trim.md`. Its
ID is the file name without `.md`: the prefix and a lowercase name of the
row's subject (letters, digits and single hyphens, starting with a letter),
chosen by the author without looking at other pull requests. Its first line
is the title `# <ID>`; its Rows section holds that one row in the table of
the area files, and its Updates section holds the dated note of the change.
Its relative links start from the area directory, one level below the area
files. So two pull requests that add rows to one area add two files and
need not agree on a number:

- a new row adds its row file and edits no area file, and the area file
  keeps its rows and notes;
- a later change to an existing row edits the file that holds that row, with
  its dated note in that file's Updates section;
- nothing in this index counts or lists the rows.

A note on an update that changes rows of several areas at once is under
[Updates affecting several areas](#updates-affecting-several-areas). A new
path of the final scope is a new area file and a new line of the table above.

## Classification summary

`python3 tools/native_path_coverage.py` prints the number of rows in each
class; this map keeps no totals to update.

The `released` rows are recipes, not accepted paths: most have native evidence
only on selected profiles, several records are historical, and some selected
cases skip on broad rows. The daemon-to-daemon paths with a local recipe
(D2D-1, D2D-6, D2D-8, D2D-10) observe outcomes only; native state is recorded
locally only for MDS movement and takeover and for MGR registration. Several
MON/MGR rows rely partly on a Go observer or a local send count rather than a
native oracle (MSG-2, MSG-4, MSG-5, MM-1, MM-6, MM-8, MM-11, MM-12); their
independent native observation is still missing.

## Most significant absences and gaps

- **Almost no local daemon-to-daemon network faults.** The local fixture
  never separates the cluster network, so OSD heartbeat failure (D2D-4), held
  replication (D2D-7), quorum partitions (D2D-2) and the directional faults
  D2D-5, D2D-15 and SVC-5 have only upstream receipts. The only local recipe
  that uses the pinned v0.5.0 `fault.ApplyNetworkProfile` is the RBD mirror
  path cut (SVC-3), on one profile and between two clusters.
- **Map distribution is unobserved locally and has an open anomaly.** No
  local recipe reads each daemon's own map epoch (D2D-3), and
  [MON_FREEZE_FIXTURE_ISSUE.md](../MON_FREEZE_FIXTURE_ISSUE.md) records an OSD
  that stopped following the OSDMap after a MON freeze, with its cause open.
- **Service-induced paths are mostly outside the catalog.** RGW has a separate
  [small-object/native-CALL and gateway-restart checkpoint and multi-stripe
  manifest/GC checkpoint](../RGW_NATIVE_VALIDATION.md) (SVC-1) and a graceful
  OSD and manager restart checkpoint (SVC-rgw-backend-fault), all selectable
  through the trusted catalog; killed daemons, partitions and broader RGW
  profiles remain open. The rbd-mirror paths
  (SVC-2 and SVC-3) have one catalog recipe on one profile; the cephfs-mirror
  paths (SVC-4 and SVC-5) still have no local acceptance recipe.
- **Native Messenger state is unobserved.** No recipe reports native
  keepalive, ACK or reset state (MSG-8), and no recipe captures daemon frames
  (D2D-19).
- **Endurance is bounded to minutes.** The longest runs are the four-minute
  OSD soak and the three-minute MON/MGR stress (OSD-20, MM-12); hour-long
  campaigns are planned work (OSD-21).
- **Some recipes never run in CI.** The MON freeze recipe (MM-7) and the
  staged native oracles (OSD-2, OSD-4) are not selected by any catalog row.
  MSG-12 now has its dedicated CRC/single-OSD row and required parent gate;
  its live observation remains no negotiated compression and working commands.

## Product wire gaps (separate from environment completeness)

These are gaps in the Go implementation. A fixture recipe for the matching
native path does not close them, and closing them does not complete the
environment.

Each gap is listed in the Product wire gaps section of the area whose wire
path it limits, so a product change in one area edits that area file only:
[Messenger/CephX](MSG.md#product-wire-gaps) (compression and cookie resume),
[CLIENT to OSD](OSD.md#product-wire-gaps) (manifest and cache-tier opcodes),
[CLIENT to MDS/CephFS](MDS.md#product-wire-gaps) (withheld CephFS client
features) and [Daemon-to-daemon](D2D.md#product-wire-gaps). The other areas
list none.

## Updates affecting several areas

Updated on 2026-11-10 for issue #116. Two summary lines under "Product wire
gaps" and the MDS-10 visibility note were reconciled with their owning
contracts: the SET_CHUNK line now names both the native-only recipe (OSD-24)
and the Go-originated acceptance (OSD-26), and the CephFS client feature line
now states the two bits declared after the audit. No row's class, recipe,
record or evidence changed, and no native test ran for this update.

Updated on 2026-11-10 for issue #63. MM-11 gains the current-manager
MGR Tell cell and the secure mode of both MGR Tell cells with two more
required rows. OSD-13 and MDS-2 gain one applied-command cell each (OSD-13
for both the OSD and the PG command) with their required rows and the record
[DAEMON_COMMAND_OUTCOME_VALIDATION.md](../DAEMON_COMMAND_OUTCOME_VALIDATION.md).
No row's class changed.

Updated on 2026-11-14 for issue #63. OSD-13 and MDS-2 gain the secure mode
of their applied-command cells with one more required row each, and OSD-13
the PG command's canceled and locally refused cases in the existing command
test. MM-2 gains the sent Tell cancellation cell with two dedicated catalog
rows and the record
[TELL_CANCELLATION_VALIDATION.md](../TELL_CANCELLATION_VALIDATION.md). MM-11 and
MSG-11 point to these. No class changed.

Updated again on 2026-11-14 for issue #63 with the record
[COMMAND_TARGET_LOSS_VALIDATION.md](../COMMAND_TARGET_LOSS_VALIDATION.md). The
sent cancellation cell of MM-2 gains the ordinary MON and MGR commands
(MM-1 points to it). MM-2, MM-6 and MM-8 gain every Tell while no MON is
reachable or no manager is active, the dial cause of an exact MON Tell and
an exact MGR Tell across a failover, with two dedicated rows. OSD-13 gains
an OSD and a PG command ended by the caller after their observed local
write and the asserted classification of a command to a killed OSD, with
two dedicated rows; MDS-2 the same classification for a stopped MDS in its
existing rows. These add Go-side observation of the client's own
connections only. No class changed.

## Upper-layer applications (outside this repository)

RBD image, lock, clone and journal policy; CephFS filesystem state and POSIX
semantics; RGW HTTP and S3 administration; CRUSH placement, automatic routing
and retry policy. Test-only consumers in `integration/` exercise the
underlying Messenger paths and are not application completeness claims.

## Drift check

`python3 tools/native_path_coverage.py` checks this index, every area file
and every row file without running any test body. It reads every entry of
this directory and of its area directories and refuses an entry that is
neither this index, an area file listed under [Area files](#area-files) nor
a row file `<PREFIX>/<PREFIX>-<name>.md` of a listed area (including a
nested directory, a symbolic link or a file that is not UTF-8 text), a
listed area file that is missing, an area file whose title or prefix differs
from its line in the Area files table or that has no Rows section, a row
file whose title is not its ID or whose Rows section does not hold exactly
the row of that ID, a row in this index or outside a Rows section, a row
whose ID does not have the prefix of its area (a number in an area file, a
name in a row file) and a row ID used twice in any of the files. In every file, every scenario ID, `group:`
and `suite:` reference exists in the catalog, every plain test name is a
top-level Go test that `go test -list` reports in the product, fixture or
quality module, every linked record exists, and a link whose text is a test
name and whose target is a Go file points at the file that declares that
test (Go's parser under the build context). Each row has one known class,
every SPEC.md final-scope path has an area file with rows, and every
`released` row cites local evidence. It prints the number of rows in each
class. Pull-request state is not recorded here: whether a pull request has
merged is read from GitHub. Upstream `ctg@` receipts are checked for form
only. The unit test `tools/test_native_path_coverage.py` runs this check in
basic CI.
