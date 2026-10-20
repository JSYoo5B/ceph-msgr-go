# Interoperability scenario catalog

`catalog.json` lists every real-daemon scenario of the `ceph` job in
`.github/workflows/ci.yml`: a stable identifier, a group and the matrix
parameters (`key`, `service`, `family`, optional `release`, `mode`, `runtime`,
OSD/MDS/RBD opt-ins and `test_run`). Identifiers are lowercase and derive from
the group and the parameters that distinguish the row, so a changed parameter
is a new scenario. Releases other than Tentacle 20.2.x are rejected.

`tools/scenario_plan.py` reads local files only and runs no test:

```
python3 tools/scenario_plan.py validate
python3 tools/scenario_plan.py list
python3 tools/scenario_plan.py plan --select all --select group:rbd --select <id>
```

`validate` fails on duplicate or empty identifiers, missing parameters, an
unsupported release, a `test_run` that is empty, malformed or matches no test
in `integration/`, and any difference between the catalog and the static
workflow matrix in either direction. `plan` prints the selected matrix JSON on
stdout and the explanation on stderr; an unknown selector is an error, never an
empty or conservative selection.

Current state: the workflow still carries its static matrix, which remains the
mandatory coverage on every push and pull request. The catalog is verified to
equal it (`tools/test_scenario_plan.py`, run by the unit job). The workflow is
not yet driven by the planner, there is no PR-comment command, and no periodic
workflow exists; those remain open in #1.
