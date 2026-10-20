# CI checks and selected interoperability scenarios

Automatic pull-request CI is Docker-free and has no scenario or OS/version
matrix. It checks Go formatting, builds, vet, workflow syntax, Python tools,
product dependencies and unit tests on Linux. Unit-test race detection is part
of these basic checks; `integration/` tests are excluded. Fixture-tool unit
checks run in their separate Go module without starting a cluster. A new PR
commit cancels its previous basic checks. Push events do not repeat PR CI.

## Catalog and planner

`catalog.json` preserves the original 63 native interoperability matrix rows and adds
the two opt-in `mgr-service` rows for Go service registration.
Each entry has a stable identifier, group and concrete fixture parameters.
Only Tentacle 20.2.x is accepted. `mandatory.txt` preserves the required native
coverage; deleting a row requires an explicit coverage change.

The catalog also names five selectable check suites:

| Selector | Work |
| --- | --- |
| `unit` | Product unit checks on Linux, macOS and Windows, Go 1.24/1.27 |
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

The planner emits native rows in `include` and selected check suites in `fixed`.
`--catalog` reads the tested revision's vocabulary using the trusted planner.
Unknown selectors fail rather than silently matching nothing. Validation rejects
duplicate identifiers, unsupported releases, missing parameters, bad or empty
test selectors, selectors matching no test, missing mandatory native rows and
missing suite definitions.

## Comment selection

```
/ci run <scenario-id | group:<group> | unit | fixture-tools | race | fuzz | windows-ceph | all> ...
```

For example, `/ci run windows-ceph race` runs those two suites without starting
the native scenario matrix. `/ci run group:rbd` runs just the RBD rows.
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

## Periodic main verification

`periodic.yml` runs all native rows and all check suites every Sunday at
03:17 UTC, and on manual dispatch. It explicitly checks out `main` once and
passes that SHA to every test job, even if manually dispatched from another
branch. Native artifacts are retained for 30 days. The final verdict requires
both native and platform suites to succeed; failures and skipped selected
suites are not a full-coverage pass. Periodic runs have a separate concurrency
group and do not cancel PR checks.

## References and evidence

The separation of code checks, requested runtime checks, independent request
statuses, module-proxy fallback and bounded runtime concurrency follows the CI
architecture in `ceph-testcontainers-go` at
`e19e5218f8aab2a6befde5a0ee140bb1f5682850`. No source was copied. This project
retains its own Tentacle-only catalog, fixture inputs and coverage contracts.

Planner/parser and workflow-contract tests are synthetic checks. Native daemon
interoperability and a live PR-comment request are separate evidence. Comment
workflows become available only after their definition reaches the default
branch. Declaration-level changed-scenario selection, repeated campaigns and
durable historical classification remain outside this checkpoint; issue #1
stays open.
