# Interoperability scenario catalog

`catalog.json` is the single list of the real-daemon scenarios the `ceph` job
runs, plus the fixed jobs (`unit`, `fixture-tools`, `race`, `fuzz`,
`windows-ceph`) that must exist in `ci.yml`. A scenario has a stable lowercase
identifier (its group plus the parameters that distinguish it, so a changed
parameter is a new scenario), a group, and the matrix parameters: `key`,
`service`, `family`, optional `release`, `mode`, `runtime`, OSD/MDS/RBD opt-ins
and the `test_run` selector. Only Tentacle 20.2.x releases are accepted.
`mandatory.txt` lists every scenario the default coverage must run; dropping
one needs an explicit edit of both files.

## Planner

`tools/scenario_plan.py` reads local files and runs no test:

```
python3 tools/scenario_plan.py validate
python3 tools/scenario_plan.py list
python3 tools/scenario_plan.py plan --select all --select group:rbd --select <id>
```

`validate` fails on duplicate or empty identifiers, missing parameters, an
unsupported release, an empty, malformed or non-matching `test_run`, a
catalog/mandatory-list mismatch in either direction, and a fixed job missing
from `ci.yml`. A selector is `all`, `group:<group>` or an identifier; an unknown
selector is an error, never an empty or conservative selection. `plan` prints
the matrix JSON on stdout and the explanation on stderr. `--catalog` plans
against another revision's catalog while running the trusted script.

## Where the catalog is used

| Workflow | Selection | Notes |
| --- | --- | --- |
| `ci.yml` (main push, pull request) | always `all` | Branch pushes use PR checks once; main retains full coverage. |
| `ci.yml` manual dispatch | `scenarios` input (space-separated selectors, default `all`) | The `plan` job validates, then plans. |
| `ci-command.yml` | the `/ci run` comment | See below. |
| `periodic.yml` | `all`, weekly (Sunday 03:17 UTC) and on dispatch | A `summary` job fails unless every scenario job succeeded. |

All of them call `native-scenarios.yml`, which checks out the requested commit
with `persist-credentials: false` and read-only permissions, runs each scenario,
and keeps `scenario-<index>-<id>` artifacts with the exact commit, image digest
and run link (7 days; 30 days for the periodic run). Pull-request check names
changed because the matrix now runs in a reusable workflow.

## PR comment command

```
/ci run <scenario-id | group:<group> | all> [...]
```

One command per comment, on its own line, at most 20 selectors. Lines in fenced
code blocks or block quotes are ignored; a comment without a `/ci` line is
ignored; a malformed, ambiguous (two commands) or unauthorized command gets a
refusal reply and executes nothing. Only owners, members and collaborators are
accepted, only on open pull requests whose head is in this repository (forks are
refused). The workflow runs from the default branch, reads the head commit once,
resolves selectors against that commit's catalog, runs exactly that commit, and
sets a request-specific `ci-command/run-<run-id>` status (pending, then success, failure or error for
canceled, skipped or missing) on it with a summary comment that links the run
and states the commit. A later request does not erase an earlier failure
comment or status. Requests for one pull request use a separate concurrency group;
baseline checks are unaffected. The status and comment permissions are held by
the job that never executes pull-request code.

Architecture adapted from the reference described in #1; no source was copied.

## Not covered yet

Declaration-level changed-scenario selection, repeated campaigns and durable
historical classification, and a live exercise of the comment command (it only
exists on the default branch once merged: `issue_comment` workflows are loaded
from there). Parser and planner behavior is covered by `tools/test_ci_command.py`
and `tools/test_scenario_plan.py`. Earlier CI runs exercised the reusable workflow;
verification of each new revision still requires its own CI result.
