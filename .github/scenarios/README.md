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

## Adding a native test

A pull request that adds a native test edits only the files below. It edits
no list, count or file of another area:

- `integration/<name>_test.go`: the native test `TestCeph<Name>Integration`
  with its rows and its sharing line at the end of its doc comment, and any
  cluster-free helper test. Each row is one `//scenario:row <id>
  <name>=<value> ...` line (see [Catalog and planner](#catalog-and-planner));
  the identifier of a row of one test ends in `.t-<name>`. The
  `//fixture:shareable` or `//fixture:isolated` line comes last (see
  [Fixture sharing](#fixture-sharing)). Neither test is registered anywhere
  else; the tools list them with `go test -list` and read the rows from the
  source.
- `integration/fixture/<name>.go`: the capability of a test-specific fixture
  mode, `defineCapability` beside the mode (see
  [Fixture capabilities](#fixture-capabilities)). The row declares it as
  `capabilities=<name>`. On a fixture with its own default selection the row also
  declares the one capability that admits its test there: an existing one
  (`osd-erasure-width` on the four-OSD topology), or a new
  `defineExclusiveCapability` when the new mode is the admission itself.
  The public receipts the control writes are declared in the same file with
  `collectDiagnosticFiles`; the fixture keeps only declared files on failure.
- `tools/osd_pool_mode_<name>_oracle.py`: a new mode of the owned-pool
  oracle, the handler `mode_<name>` marked `@oracle_mode` and no other mode.
  The oracle imports the module, and the fixture admits the mode from that
  declaration. The test reads the mode's result with `nativePoolResult` into
  a type of its own, so `poolReceipt` gains no field.
- `integration/fixture/coverage/<AREA>/<AREA>-<name>.md`: the row of the
  [coverage map](../../integration/fixture/coverage/README.md#area-files)
  in a row file of its own, named after the row's subject, with its dated
  note. The area file `<AREA>.md` is not edited, and no number is taken.
- `tools/test_ci_rows_<group>_<name>.py`: the row's own checks, when it needs
  more than `validate`, as a module of its own on
  `ci_row_catalog.RowsTestCase`.

A row's description belongs in its fixture record and its coverage row, not
in this document.

## Adding other registrations

Work that comes with a native test, or on its own, is registered the same
way: in files of its own, never in a shared list, count or sequence.

- `integration/fixture/<name>.go`: a fixture control, declared beside its
  function with `defineControl("<name>", controlAfterRanked, ...)` or, for one
  request and one acknowledgement, `defineRequestControl` (see
  `integration/fixture/control.go`). A new control takes no rank.
- `cephmsgr/receipts/<name>.md`: a native receipt of the
  [protocol evidence ledger](../../cephmsgr/PROTOCOL_EVIDENCE.md#native-receipts),
  named after its subject; numbered receipt IDs are frozen.
- `tools/scenario_checks/<name>.py`: a new validation of the catalog, a module
  with a `check(context)` function, which `scenario_plan.py validate` runs.
- `tools/test_scenario_check_<name>.py`: the tests of that check.
- `tools/native_evidence_<name>.py`: a successful-evidence family, a trusted
  module that says which rows it claims and what it projects; the workflow
  asks the collector for a job's family.
- `tools/test_native_<name>_evidence.py`: the tests of that family.
- `integration/fixture/rgw_native_<name>_test.go`: a routed RGW checkpoint,
  a `TestRGWNative<Name>Integration` test of the fixture module with its
  `//scenario:row ... rgw_native=1` declaration (see
  [Routed RGW checkpoint rows](#routed-rgw-checkpoint-rows)).
- `tools/rgw_native_gate_<name>.py`: that checkpoint's receipt check in the
  trusted result gate.
- `tools/test_rgw_native_gate_<name>.py`: its tests, with the accepted receipt
  line as `SAMPLE`.

`tools/test_merge_proof.py` makes pull requests that add a native test with
every file of the section above and every registration of this section, in a
temporary Git repository. It merges them once in two areas and once in one
area, including two RBD registrations and two four-OSD registrations, and
runs the validators on each merged tree. It then merges all of them into one
tree and runs the whole tools test suite there. The proof requires Git to
create each merge without repairs, requires that no two branches edit one
file, requires every registration of each branch to be found in the merged
tree, and requires the planner to refuse a catalog file brought back beside
the declarations.

## Catalog and planner

The catalog is not a file. Each row is declared beside the test it selects,
in that test's doc comment, before its `//fixture:` sharing line:

```go
// TestCephOSDCopyFromSnapTruncateIntegration checks ...
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-replicated-pg-reply-v8.t-osdcopyfromsnaptruncate key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=replicated-pg-reply-v8
//fixture:isolated ...
func TestCephOSDCopyFromSnapTruncateIntegration(t *testing.T) {
```

- `//scenario:row <id> <name>=<value> ...` selects the test it is declared
  beside: its `test_run` is `^<TestName>$`. A row of several tests gives an
  explicit `test_run=<selector>` that also selects that test, and is declared
  beside the first test the selector names.
- `//scenario:suite <id> <name>=<value> ...` has no `test_run`: it runs every
  test its fixture configuration enables. It is declared beside the test that
  configuration exists for (`managers=0` beside
  `TestCephManagerAvailabilityIntegration`, `osd_soak=4m` beside
  `TestCephOSDSoakIntegration`, and so on), or beside its group's base test
  (`TestCephIntegration` for `mon-mgr`, `TestCephOSDIOIntegration` for `osd`).
- The group is the identifier before its first dot. A value has no white
  space and may be empty (`stress=`). The row's parameters are the written
  ones in written order, then `test_run`, then `capabilities`.
- Declarations are read from the `*_test.go` files of `integration`,
  `cephmsgr` and, for the routed RGW checkpoints only (`rgw_native=1`),
  `integration/fixture`. The rows are in identifier order, so where a
  declaration sits changes no plan.

`tools/scenario_declarations.py` reads the declarations as text, and
`tools/scenario_catalog.py` composes them with the fixed check suites for
every reader (the planner, the change planner and the coverage map check).
That is the only reader of the declarations: the fixture never parses them.
Its catalog tests take the rows the planner read (`scenario_plan.py rows`, or
the file `validate` passes to `TestCatalogRowsAreAdmittedByTheFixture` in
`CEPH_MSGR_PLANNED_ROWS`), so a change to the declaration format changes one
reader. A malformed or indented
declaration, one outside the doc comment of a top-level native test of those
suites, one in any other Go file, a duplicate identifier or parameter, a suite
row with a `test_run`, a row whose explicit `test_run` does not select its
test (or repeats `^<TestName>$`), `rgw_native=1` outside the RGW checkpoints
and a checkpoint row without it are refused. So are the catalog files of the
earlier layout (`.github/scenarios/catalog.d`, `catalog.json`): a tree or a
tested revision that has one is refused, so rows are never read from two
places.

The fixed check suites are the jobs of
[`platform-checks.yml`](../workflows/platform-checks.yml) whose condition is
their own name in the plan's `fixed` list; the workflow that runs a check is
its only declaration.

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

A fixture with its own default selection (the MDS command, CephFS metadata,
completion and data fixtures, the RBD fixture, the cache-pool fixture and the
four-OSD erasure-capacity topology) runs one further exact test only when a
capability defined for it with `defineExclusiveCapability` admits that test.
`loadProfile` asks whether any declared capability of that fixture admits the
run, so a new one is defined beside its mode without editing `loadProfile`.
At most one such capability of a fixture may be declared for a run, so these
capabilities never exclude each other by name. A further mode on such a
fixture that only extends the test the exclusive capability admits (the
shard-history capture beside `osd-erasure-width`, or
`mds-held-flock-split-fragments` beside `mds-held-flock-split`) is an ordinary
`defineCapability` declared together with it.

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
selector selects is. [`tools/fixture_sharing.py`](../../tools/fixture_sharing.py)
is the only reader of these lines; the fixture never reads them and reports
only each row's effective selector (from
`TestCatalogRowsAreAdmittedByTheFixture`), from which the reader derives the
row's value. `scenario_plan.py validate` refuses a native test without a
valid declaration, a misplaced declaration, a native test that Go does not
list and a row without a value. `scenario_plan.py sharing` lists the
shareable and isolated rows of each fixture configuration (a row's
parameters without `test_run`) and the job count of the default coverage.
`plan` runs the selected shareable rows of one configuration that have an
explicit `test_run` as one fixture sequence job (at most eight rows, in
catalog order); every other row, and a request that selects one row of a
configuration, keeps a job of its own. The sequence runs each test alone and
requires the start-state check after every test. The rule, the
classification and the sequence are in
[FIXTURE_SHARING.md](../../integration/fixture/FIXTURE_SHARING.md).

What a row checks and the tests it selects are in the doc comment of the test
that declares it, and its validation record is linked from the
[native path coverage map](../../integration/fixture/coverage/README.md), not
from this file, so a new row adds no sentence here. `scenario_plan.py list` prints every row and `scenario_plan.py
rows` every row with its parameters. A row that needs a fixture input beyond
the fixture's default (for example the four-OSD erasure topology
`osd_replicas=4`, `osd_cache_pools`, `mds_cephfs_data` or a fresh MDS fixture)
declares the capability that admits its test, and the fixture refuses such an
input with any selector other than its default selection or one exact test
that declares the capability
([capability.go](../../integration/fixture/capability.go)).

Each entry has a stable identifier, group and concrete fixture parameters.
Only Tentacle 20.2.x is accepted. Every catalog row is in the default coverage
(`all`, which the periodic run selects); there is no separate list of required
rows or row total to edit, and deleting a row is a visible catalog change.

The native workflow forwards a row's parameters to the fixture runner by a
rule, with no line per parameter: the trusted helper `tools/native_env.py`,
copied from the default branch before the tested commit is checked out, turns
the job's matrix into `CEPH_MSGR_TEST_<P>` for a parameter `p` and execs
`integration/run.sh` with them; the result gate, the evidence collector and the
terminal recorder never receive them. The parameters the workflow bound before
this rule keep their variable names and fallbacks (a frozen table in the
helper that no parameter joins). A row cannot set a planner key, a variable
the workflow sets itself (`CEPH_MSGR_TEST_DIAGNOSTICS`) or the variable of
another parameter; the trusted planner and the helper both refuse it. Which
parameters exist is the fixture's decision: `loadEnvironmentProfile` refuses a
`CEPH_MSGR_TEST_` variable that `loadProfile` does not read, and `validate`
passes every row through it. A new fixture input therefore edits only the
fixture code that reads it and the rows that set it, and a pull request's new
input runs natively on that pull request.

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

The catalog also names five selectable check suites, the selectable jobs of
`platform-checks.yml`:

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
`--revision SHA` reads the tested revision's vocabulary using the trusted
planner: for `/ci run`, `plan --revision` reads the revision's declaring test
files (and their `//fixture:` sharing lines) from Git objects as text, without
checking the revision out or running any of its code, and takes the fixed
check suites from the trusted workflow that runs them. A revision that still
has `catalog.d` or `catalog.json` is refused with a request to merge the
default branch.
Unknown selectors fail rather than silently matching nothing. Validation rejects
duplicate identifiers, unsupported releases, missing parameters, bad or empty
test selectors, selectors matching no test and missing suite definitions.
A new validation of the catalog is a module of its own in
[`tools/scenario_checks`](../../tools/scenario_checks/__init__.py) with a
`check(context)` function and its tests in
`tools/test_scenario_check_<name>.py`; `validate` runs every module there in
name order. It edits no function, list or test class of `scenario_plan.py`
or `test_scenario_plan.py`.

### Routed RGW checkpoint rows

A routed row runs a dedicated RGW checkpoint of the nested fixture module
instead of the controller's own fixture. A checkpoint is a top-level
`TestRGWNative...Integration` test of the fixture module. It declares its
one row beside it (`//scenario:row ... rgw_native=1`), and its receipt check
lives in its own module `tools/rgw_native_gate_<name>.py`, which names the
test (`TEST`), the receipt line's `MARKER` and `check(line, gate)`. The
receipt tests sit beside it in `tools/test_rgw_native_gate_<name>.py`, whose
`SAMPLE` line the gate's common tests use. Adding a checkpoint adds these
files and edits no list. The recipes, receipts and limits of each checkpoint
are in [the RGW record](../../integration/fixture/RGW_NATIVE_VALIDATION.md).
A row routes with the parameter `rgw_native=1`, which the native workflow
forwards as `CEPH_MSGR_TEST_RGW_NATIVE`; every other row keeps the fallback
`0`. The routing is closed at each step:

- The trusted planner refuses a routed row whose `test_run` is not exactly
  `^Name$` of a checkpoint the trusted `tools/rgw_native_gate.py` verifies
  (the `TEST` of a `rgw_native_gate_<name>.py` module beside it, loaded from
  the gate's own directory only), a routed row with capabilities and any
  other `rgw_native` value, on every catalog it reads, a pull request's
  included. `validate` requires Go to list the routed test exactly once in the
  fixture module's root package and never resolves a routed row against the
  product suites, nor another row against the fixture module.
- The fixture's `loadProfile` admits a routed row only with a selector
  `^Name$` whose name has the checkpoint form `TestRGWNative\w*Integration`
  (word characters only, so the selector matches that one test), no
  capability or sequence, and every other input at the value
  the checkpoint itself uses (pinned image, aes256k, secure, IPv4, container
  runtime, no OSD, MDS, RBD, MON-ping, cache-pool, MGR-service,
  authentication, stress or soak mode). The controller
  (`integration/fixture/rgw_native_route.go`) then starts no fixture,
  refuses the name unless `go test -list ^Name$ .` lists exactly that test
  in the fixture module, and runs
  `go test -json -count=1 -timeout=20m -run ^Name$ .` in the fixture module
  with `CEPH_MSGR_TEST_RGW_NATIVE=1`. It writes the raw test2json stream to
  `rgw-native.jsonl` in `CEPH_MSGR_TEST_DIAGNOSTICS`, refusing an existing
  file, and prints the events' output after `Running Ceph test suite:
  fixture`.
- The native workflow's snapshot passes `--backend rgw-fixture`, the tested
  revision and the stream path. The trusted result gate refuses another
  selector, a test Go does not list once, a checkout whose HEAD is not the
  tested revision or whose fixture module has a modified or untracked file,
  and an existing stream. Its verdict requires the checkpoint's
  `--- PASS` line under the `fixture` suite and no other result, HEAD still
  at the tested revision, and the RGW gate's verdict on the stream: exactly
  one PASS of the test and a PASS of the fixture package, no skip or failure,
  the receipt as the checkpoint's own module checks it (it may recompute
  deterministic payload hashes and check the layout or fault relations the
  receipt claims) and the independent owned-resource removal record, every
  event attributed to the fixture package. A fixture result in a product-suite
  request is refused. A later step fails the job when any container other
  than the Testcontainers reaper remains.

Every routed row is in the default coverage, like every row: `all`,
`group:rgw` and the weekly periodic run select them, each as a job of its own (the checkpoints
declare `//fixture:isolated`, and the planner never groups a row outside the
product suites). The repository has no request-only rows; heavier rows (four
BlueStore OSDs, the four-minute soak, MDS multi-rank) are in `all` as well, and
one checkpoint takes about 35 to 50 seconds of test time on its own
one-MON, one-MGR, one-OSD, one-gateway cluster. The development workflow
[`rgw-native.yml`](../workflows/rgw-native.yml) still runs every checkpoint
its own revision's gate verifies automatically for pull requests that change the RGW fixture files, with its
source-archive manifest; a catalog row runs only on request or weekly. A
routed row's job has a name of its own: `RGW-native-fixture`, the row ID and
the checkpoint's fixed cluster (pinned 20.2.4 image, aes256k, secure, IPv4,
one MON, one MGR, one BlueStore OSD, one gateway). It omits the
controller-profile fields of the ordinary name (MGR count, OSD, store,
profile), which a checkpoint never uses.

## Test inventory and selection evidence

Basic CI verifies two inventories without starting a cluster:

- The native test inventory is every top-level `TestCeph...Integration` declared
  in `integration` and `cephmsgr`, and every `TestRGWNative...Integration`
  declared in the fixture module, which only a routed row selects; there is no
  list to edit. The fixture's route must admit every declared RGW
  checkpoint by the same name rule. Fixture tests parse
  Go declarations, reject duplicate names, and compare the source set with the
  compiled `go test -json -list` result. The current check requires these tests
  in the default build; adding platform-constrained tests needs an explicit
  inventory-policy change.
- `fuzz-targets.json` registers each package/target pair. The product job compares
  it with the root module's compiled fuzz list. Adding, renaming or removing a
  fuzz function without updating the inventory fails CI. This checks the current
  runner's default build, not every GOOS or optional tag combination.

`TestCICatalogInventory` asks the native workflow's forwarding helper
(`tools/native_env.py serve`) for each catalog row's environment, fallback
values included, and passes it through the fixture's real
`loadEnvironmentProfile`. Parameters the helper refuses or no fixture input reads,
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
`ci-command/run-<run-id>/attempt-<attempt>` status and summary. A failed, canceled, skipped or
missing selected suite cannot become a pass; unselected suites are skipped.
Later requests preserve earlier statuses and results. Each PR has its own
command concurrency group, separate from basic checks. It uses `queue: max`
and `cancel-in-progress: false`, allowing one running workflow plus up to
100 pending workflows. Pending workflows proceed in the order they started
waiting for the group. New requests beyond that capacity are canceled by
GitHub before controller code can publish a SHA-bound status. Inspect their
Actions run records and submit a new request after capacity becomes available.
Every event entering this workflow shares that capacity, including comments
that the parser will later ignore. The controller captures a PR head when its
request job starts, so a queued request can capture a later head than the one
present when the comment was posted. Reruns have distinct attempt statuses and
attempt-specific result links.
[GitHub concurrency policy](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency).

Basic CI runs `python3 tools/workflow_lint.py`. The pinned actionlint v1.7.7
rejects GitHub's `queue` key. This adapter accepts only the explicit
three-entry top-level queue policy in `ci-command.yml`, validates each key
and value, and blanks only its `queue: max` line before actionlint checks
the complete workflow. Other workflow text and line numbers are preserved.
Duplicate keys, aliases, anchors, inline values with comments, alternative
indentation, dynamic cancellation and unsupported queue modes fail this
repository's deliberately narrow policy. Other workflow files receive the
original actionlint validation.

The hosted acceptance run requires the updated controller on the default
branch. On a disposable same-repository PR, hold one requested job active,
post three authorized requests, and record their comment IDs, workflow run
IDs, attempts, captured SHAs and terminal statuses. Verify that all three
survive the wait, and that baseline PR checks keep their independent results.
Local policy tests and actionlint checks cannot establish GitHub scheduling
behavior. This hosted exercise remains pending until the updated controller
is available on the default branch.

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
| General results | 7 days | Scenario result/test JSON for requested and periodic runs; reviewed cap-send, shared-FLOCK and four-minute soak success bundles; successful Windows interoperability evidence |
| Failure diagnostics | 14 days | Failed native, race and unit-test logs/metadata; Windows evidence when the job fails |
| Deployment evidence | 30 days | Policy for future deployment workflows; no deployment workflow currently uploads evidence |

A row that an evidence family claims additionally requests the closed
[schema-v1 native evidence bundle](NATIVE_EVIDENCE.md). Each family is a
trusted module of its own, `tools/native_evidence_<name>.py`, which says
which rows it claims and what it projects; the workflow asks the trusted
collector for the job's family. Its manifest joins
tested/controller SHA, execution source, run ID/attempt, scenario/selector,
normalized requested profile, configured image, observed runtime facts and
included-file hashes. Only reviewed numeric/outcome/cleanup projections are
uploaded for seven days; raw metadata, logs, configuration and credentials
remain excluded. `result.json` distinguishes a native test PASS from incomplete
required evidence, and the pilot's final gate requires both collection and
upload. Other rows keep their previous retention boundary. This new trusted
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
