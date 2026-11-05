# Code quality checks and review reports

These Docker-free checks complement Basic CI and native scenario execution.
They do not establish protocol correctness or native interoperability, change
the scenario selection policy, or raise the product's minimum Go version.

| Workflow | Trigger | Policy |
| --- | --- | --- |
| Code quality: static analysis | PR, manual | Advisory diagnostics from errcheck, govet, ineffassign and staticcheck, separately for product/root, fixture and quality-tool modules; existing findings are not suppressed or mechanically rewritten |
| Code quality: tooling | PR, manual | Required successful Go analyzer tests and Python collector/publisher/vulnerability-policy tests |
| Code quality: vulnerabilities | PR, manual, weekly Monday 05:17 UTC | govulncheck v1.9.0 on Linux amd64 with tests; product Go 1.24.x/1.27.x and fixture Go 1.26.x/1.27.x; package/symbol findings fail, module-only findings are informational |
| Structural review evidence | PR | Advisory comparison of merge base and exact PR head; no metric threshold is a merge gate |
| Structural review comment | Evidence completion | Trusted default-branch publisher updates one bot-owned comment for the current head |

Workflow failure policy is distinct from repository branch-protection settings;
this change does not configure required status checks. Basic CI still validates
product builds on Go 1.24.0. Vulnerability scans use the latest patch available
in each selected series. The scanner is built using Go 1.27 before switching to
the scan's toolchain. A failed scanner process or invalid result fails the job;
its JSON exit status alone is not the vulnerability verdict. The weekly run
executes only vulnerability checks. It does not repeat Basic CI or native runs.
These scans do not cover other operating systems, architectures or optional build
tags, and do not claim that the dependency graph is free of all vulnerabilities.

Static analysis intentionally starts as advisory: the initial root-module scan
found existing diagnostics. The action still exposes its failure and annotations;
the job summary records the step outcome and the raw JSON is retained for seven
days. A tool failure is not a clean scan. A later gate should follow reviewed
findings and a separate policy change. Vulnerability JSON/text also lasts seven
days. No linter dependency is added to the product or fixture module.

## Structural report

Three bounded tables show changed packages (lines, exported declarations and
changed-line share), concrete owners (public/private methods and direct fields),
and changed complexity/length/duplication findings. Full inventories and paired
findings remain in seven-day artifacts. Metrics are navigation aids, not a design
score. Moves and unmatched identities are not automatic improvements; missing or
below-threshold measurements are not zero.

The collector measures production, non-generated Go in the root library module.
It excludes tests, `testdata`, `integration`, `internal/testcluster`, `tools`,
nested modules and hidden paths. Test/documentation/tool-only PRs are classified
before tool installation and produce no new report. Unknown paths are treated
conservatively. Dependency changes in the root `go.mod` or `go.sum` are eligible.
The exclusion test checks that library imports do not enter excluded directories.

The lexical structural inventory combines build profiles. Lint comparisons use
the root module on Linux amd64, CGO disabled and no extra build tags; other
platforms, fixture/tool modules and live runtime behavior are outside the report.
Both sides use the same analyzer binaries and linter version. The receipt records
base/head/target SHA, analysis SHA, run/attempt, environment, durations and hashes.
A failed collection is unavailable, never clean. Collected results do not certify
a newer base branch or a different merge candidate.

The PR workflow has read-only permissions and never publishes comments. The
publisher checks out only the trusted default branch, treats bounded artifacts
as data, checks source workflow/run/attempt/PR identities and hashes, and verifies
head freshness again before writing. It does not execute PR code or restore a PR
cache. It sanitizes report data, refuses stale reports, and removes its own old
comment when the current head was not measured. It cannot adopt human comments.
Automatic publication becomes available only after these workflows land on the
default branch; until then inspect the read-only job summary and artifact. PR
source branches named `main` are excluded by the publisher trigger.

For local investigation on an authorized Linux runner, with both commits present:

```sh
CGO_ENABLED=0 GOWORK=off python3 .github/scripts/structural_review.py \
  --base BASE_SHA --head HEAD_SHA --output REPORT_DIRECTORY \
  --golangci-lint GOLANGCI_LINT_BINARY
```

Use full commit SHAs and a new output directory outside the checkout. The
publisher's `--dry-run` can validate retained evidence without posting; personal
CLI publication is refused. No source baseline or suppression ledger is stored.

The imported tools and controls retain their MIT license and source attribution
in `tools/quality/LICENSE` and `THIRD_PARTY_NOTICES.md`.
