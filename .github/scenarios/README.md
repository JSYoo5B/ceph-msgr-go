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
Three further `osd` rows use the four-OSD erasure-capacity topology
(`osd_replicas` value `4`): one selects only `TestCephOSDErasureK2M2Integration`,
another only `TestCephOSDErasureOptimizedK2M2Integration` and
`TestCephOSDErasureOptimizedK2M2FailoverIntegration`, and the third only
`TestCephOSDErasureOptimizedK2M2InFlightMutationIntegration` and
`TestCephOSDErasureOptimizedK2M2SnapshotAndDegradedWriteIntegration`. That value travels in the
already-forwarded `osd_replicas` input, so it needs no new workflow parameter;
the fixture refuses it with any other selector.
Two further `osd-cache-pools` rows: one selects only
`TestCephOSDCacheTierOpsIntegration`, the native-only cache-tier operation
checkpoint, and another selects only `TestCephOSDCacheOpGoIntegration`, the
Go-originated cache-tier operations with a native observer. Both reuse the
already-forwarded `osd_cache_pools` input; the cache-pool fixture accepts only
those selectors or `TestCephOSDIgnoreCacheIntegration`.
Two `mds-cephfs-capsend` rows select only
`TestCephMDSCephFSCapSendRecoveryIntegration` in the two selected MDS
profiles. They reuse the already-forwarded `mds_cephfs_data` input; the
data fixture accepts only that selector, the writeback-fence selector below or
`TestCephMDSCephFSDataIntegration`.
Two `mds-cephfs-writeback` rows select only
`TestCephMDSCephFSWritebackFenceIntegration` in the same two MDS profiles and
reuse the same input; the data fixture admits its OSD drop and eviction
controls only for that selector.
One further `osd` row selects only `TestCephOSDSetChunkNativeIntegration`, the
native-only SET_CHUNK manifest checkpoint, on the single-BlueStore legacy-v6
fixture. It needs no opt-in parameter: the checkpoint uses the ordinary OSD
fixture's owned plain pools, so the test also belongs to the broad legacy OSD
selection. A second such row selects only `TestCephOSDSetChunkGoIntegration`,
the Go-originated SET_CHUNK acceptance with native observation, on the same
fixture and likewise without an opt-in parameter.
The catalog has 76 native rows in total.
Each entry has a stable identifier, group and concrete fixture parameters.
Only Tentacle 20.2.x is accepted. `mandatory.txt` preserves the required native
coverage; deleting a row requires an explicit coverage change.

The trusted planner refuses catalog parameters that its native workflow cannot
forward. Land new workflow opt-ins before requesting a feature PR's new profile;
checking out that PR does not replace the default branch's reusable workflow.
The MGR-service opt-in is forwarded in advance of its feature implementation.

Native runs use the calling revision's result gate. For an explicit `test_run`,
it inventories matching top-level tests in the tested checkout before execution
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
| General results | 7 days | Scenario result JSON for requested and periodic runs; successful Windows interoperability evidence |
| Failure diagnostics | 14 days | Failed native, race and unit-test logs/metadata; Windows evidence when the job fails |
| Deployment evidence | 30 days | Policy for future deployment workflows; no deployment workflow currently uploads evidence |

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
