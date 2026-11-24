# Native scenarios and CI requests

Automatic pull-request CI starts no cluster. It checks formatting, builds,
vet, workflow syntax, the Python tools, product dependencies, unit and race
tests and the fixture module's unit checks (`.github/workflows/ci.yml`,
`code.yml`). Native scenarios against actual Ceph daemons run only when
requested or on the weekly periodic run. How each tool decides is in its own
docstring; this file says what an author declares and what an operator runs.

## Adding a native test

A pull request that adds a native test edits only the files below. It edits
no list, count or file of another area:

- `integration/<name>_test.go`: the native test `TestCeph<Name>Integration`
  and any cluster-free helper test. The doc comment of the native test ends
  with its declarations: one `//scenario:row <id> <name>=<value> ...` line per
  row (the identifier of a row of one test ends in `.t-<name>`), one
  `//scenario:covers <Path>` line per path of SPEC.md's environment scope
  table the test gives native evidence for, and last its
  `//fixture:shareable <reason>` or `//fixture:isolated <reason>` line.
- `integration/fixture/<name>.go`: the capability of a test-specific fixture
  mode, `defineCapability` beside the mode, which the row declares as
  `capabilities=<name>`. On a fixture with its own default selection the row
  also declares the one capability that admits its test there: an existing one
  (`osd-erasure-width` on the four-OSD topology), or a new
  `defineExclusiveCapability`. The public receipts the control writes are
  declared in the same file with `collectDiagnosticFiles`, and so is every
  receipt the test writes into the control directory, by its exact name.
- `tools/osd_pool_mode_<name>_oracle.py`: a new mode of the owned-pool
  oracle, the handler `mode_<name>` marked `@oracle_mode`. The test reads its
  result with `nativePoolResult` into a type of its own.
- `tools/test_ci_rows_<group>_<name>.py`: the row's own checks, when it needs
  more than `validate`, on `ci_row_catalog.RowsTestCase`.

What a row checks belongs in the doc comment of the test that declares it.

## Adding other registrations

Work that comes with a native test, or on its own, is registered the same
way, in files of its own:

- `integration/fixture/<name>.go`: a fixture control, declared beside its
  function with `defineControl("<name>", controlAfterRanked, ...)` or
  `defineRequestControl` (`integration/fixture/control.go`).
- `integration/fixture/<name>.go`: a fixture input, declared beside the code
  that reads it with `var <name>Input = defineInput("<name>", "<fallback>")`
  and read with `p.input(<name>Input)` (`integration/fixture/input.go`); a
  row sets it as `<name>=<value>`, and the native workflow forwards it as
  `CEPH_MSGR_TEST_<NAME>` by rule (`tools/native_env.py`).
- `tools/scenario_checks/<name>.py`: a validation of the catalog, a module
  with `check(context)`, which `scenario_plan.py validate` runs.
- `tools/test_scenario_check_<name>.py`: the tests of that check.
- `tools/native_evidence_<name>.py`: a successful-evidence family
  (`tools/native_evidence.py`).
- `tools/test_native_<name>_evidence.py`: the tests of that family.
- `integration/fixture/rgw_native_<name>_test.go`: a routed RGW checkpoint,
  a `TestRGWNative<Name>Integration` test of the fixture module with its
  `//scenario:row ... rgw_native=1` declaration.
- `tools/rgw_native_gate_<name>.py`: that checkpoint's receipt check
  (`tools/rgw_native_gate.py`).
- `tools/test_rgw_native_gate_<name>.py`: its tests, with the accepted receipt
  line as `SAMPLE`.

`tools/test_merge_proof.py` makes branches that each add every file of both
sections and requires them to merge without conflicts and pass the
validators. A local run of the tools suite includes it. Basic CI runs it in
the job `Fixture build and unit checks without a cluster` instead of the job
`Build, lint and unit tests`, which runs the rest of the suite, so the two
required jobs share its time.

## Declarations

```go
// TestCephOSDCopyFromSnapTruncateIntegration checks ...
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-replicated-pg-reply-v8.t-osdcopyfromsnaptruncate key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=replicated-pg-reply-v8
//scenario:covers CLIENT to OSD
//fixture:isolated ...
func TestCephOSDCopyFromSnapTruncateIntegration(t *testing.T) {
```

- Rows, suite rows and covered paths: `tools/scenario_declarations.py`, their
  one reader. A row selects the test it is declared beside unless it gives
  an explicit `test_run`; a `//scenario:suite` row runs its whole fixture
  configuration. Every row is in the default coverage (`all`).
- Suite parts: `integration/fixture/suite_part.go`. A suite row whose
  selection outgrows one native job is declared as rows that differ only in
  `suite_part=K/N` (identifier suffix `.suite-part-K-of-N`), one job each;
  part K runs the K-th contiguous slice of the selection Go lists, so the
  parts together run each test once. `TestSuitePartsCoverTheirSelectionOnce`
  (fixture module) requires the parts 1..N, no whole row beside them, and at
  most `suitePartMaxTests` tests per part; a part over that bound needs
  another part on every row of the configuration.
- Fixture sharing: `tools/fixture_sharing.py`. The planner runs selected
  shareable rows of one fixture configuration as one fixture sequence job.
- Capabilities: `integration/fixture/capability.go`.
- Fixed check suites: the jobs of `.github/workflows/platform-checks.yml`
  (`unit`, `fixture-tools`, `race`, `fuzz`, `windows-ceph`).
- Fuzz targets: `fuzz-targets.json` (`tools/fuzz_targets.py`).

## Local commands

```sh
python3 tools/scenario_plan.py validate
python3 tools/scenario_plan.py list
python3 tools/scenario_plan.py rows
python3 tools/scenario_plan.py sharing
python3 tools/scenario_plan.py plan --select group:rbd --select windows-ceph
python3 tools/fuzz_targets.py verify
```

`validate` also runs the fixture's admission tests, so the tools need the
fixture module's Go (1.26 or later). Docker integration runs on an
authorized Linux runner. A reproduction runs one row (identifier `ID`) with
the environment the catalog gives it, behind the native result gate:

```sh
(
  env_lines=$(python3 tools/scenario_plan.py env ID) &&
  eval "$env_lines" &&
  sh integration/run.sh
)
```

The assignment keeps a failed lookup's status, so the runner never starts
with the shell's own environment; `validate` refuses a documented `env`
lookup in any other form.

## Requesting native CI

```
/ci run <scenario-id | group:<group> | unit | fixture-tools | race | fuzz | windows-ceph | all> ...
```

One command per comment, on its own line, with at most 20 selectors. Only
owners, members and collaborators are accepted; quoted and fenced lines are
ignored. `group:checks` selects the five check suites. Manual dispatch, on
the default branch only:

```sh
gh workflow run ci-command.yml --ref main \
  -f selectors='mon-ping.aes.ip4.crc.host.t-monping'
gh workflow run ci-command.yml --ref main \
  -f pull_request=123 -f selectors='unit fixture-tools'
```

`periodic.yml` runs every row and check suite on `main` every Sunday.

## Trust split

The request controller runs trusted code from the default branch. It records
the pull request head once, plans that revision's rows from its Git objects
as text (`scenario_plan.py plan --revision`) without checking it out or
running its code, and refuses parameters its native workflow cannot forward.
Test jobs check out that exact SHA read-only, without persisted credentials,
and are judged by the calling revision's result gate
(`tools/native_test_results.py`): every test an explicit `test_run` selects
must PASS, and a skipped, missing or failed selected test fails the job. A
broad suite row requires at least one PASS and does not claim that its
optional tests passed; before its first suite runs, the fixture announces
the tests each suite run selects, and every announced test needs a PASS or
SKIP, so a run that was cut off, or whose first suite exited before the
next one, reports the tests it did not reach. Separate trusted jobs publish the request's status; a
failed, canceled, skipped or missing selected suite never becomes a pass.
A row's fixture inputs need no workflow change: the trusted
`tools/native_env.py` forwards each row parameter as `CEPH_MSGR_TEST_<NAME>`
to the runner step only, so a pull request's new input runs in that pull
request.

The controller and the native workflow it calls are always the default
branch's, also for a manual dispatch, so a change to either is first
exercised by the first request after it lands; no pull request can run its
own copy. A revision from before rows were declared beside their tests (it
still has `.github/scenarios/catalog.d` or `catalog.json`) still gets the
check suites, which come from the trusted platform workflow, but no row: a
row, group or `all` selector is refused with a request to merge the default
branch. Conversely, a default branch from before that change cannot plan the
rows of a revision that declares them.

Only a same-repository pull request that changes nothing but `README.md` or
`THIRD_PARTY_NOTICES.md` may skip basic and quality work
(`tools/ci_change_plan.py`); a skipped check is not success evidence.
