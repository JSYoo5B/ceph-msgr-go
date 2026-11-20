import io
import json
import os
import re
import subprocess
import sys
import unittest
from unittest import mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import native_env
import scenario_order
import scenario_plan as plan

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "native-scenarios.yml"

# Frozen: the run step's matrix bindings of the native workflow before
# forwarding became a rule (fabf96a), verbatim. It is the reference of the
# equivalence below and never gains a line: a new input is forwarded by the
# rule, and this literal and native_env.LEGACY stay as they are.
FROZEN_BINDINGS = """\
          CEPH_MSGR_TEST_IMAGE: ${{ matrix.image || 'quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9' }}
          CEPH_MSGR_TEST_KEY_TYPE: ${{ matrix.key }}
          CEPH_MSGR_TEST_SERVICE_CIPHER: ${{ matrix.service }}
          CEPH_MSGR_TEST_IP_FAMILY: ${{ matrix.family }}
          CEPH_MSGR_STRESS_DURATION: ${{ matrix.stress }}
          CEPH_MSGR_TEST_MGR_COUNT: ${{ matrix.managers || '2' }}
          CEPH_MSGR_TEST_OSD: ${{ matrix.osd || '0' }}
          CEPH_MSGR_TEST_OSD_PROFILE: ${{ matrix.osd_profile || 'legacy-v6' }}
          CEPH_MSGR_TEST_OSD_CACHE_POOLS: ${{ matrix.osd_cache_pools || '0' }}
          CEPH_MSGR_TEST_MGR_SERVICE: ${{ matrix.mgr_service || '0' }}
          CEPH_MSGR_TEST_MDS_FRESH: ${{ matrix.mds_fresh || '0' }}
          CEPH_MSGR_TEST_MDS_COMMANDS: ${{ matrix.mds_commands || '0' }}
          CEPH_MSGR_TEST_MDS_SESSIONS: ${{ matrix.mds_sessions || '0' }}
          CEPH_MSGR_TEST_MDS_CEPHFS: ${{ matrix.mds_cephfs || '0' }}
          CEPH_MSGR_TEST_MDS_CEPHFS_RANKS: ${{ matrix.mds_cephfs_ranks || '0' }}
          CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY: ${{ matrix.mds_cephfs_ranks_replay || '0' }}
          CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION: ${{ matrix.mds_cephfs_completion || '0' }}
          CEPH_MSGR_TEST_MDS_CEPHFS_DATA: ${{ matrix.mds_cephfs_data || '0' }}
          CEPH_MSGR_TEST_RBD: ${{ matrix.rbd || '0' }}
          CEPH_MSGR_TEST_MON_PING: ${{ matrix.mon_ping || '0' }}
          CEPH_MSGR_TEST_RGW_NATIVE: ${{ matrix.rgw_native || '0' }}
          CEPH_MSGR_TEST_RUN: ${{ matrix.test_run || '' }}
          CEPH_MSGR_TEST_SEQUENCE: ${{ matrix.test_sequence || '' }}
          CEPH_MSGR_TEST_CAPABILITIES: ${{ matrix.capabilities || '' }}
          CEPH_MSGR_TEST_OSD_STORE: ${{ matrix.osd_store || 'memstore' }}
          CEPH_MSGR_TEST_OSD_REPLICAS: ${{ matrix.osd_replicas || '1' }}
          CEPH_MSGR_TEST_OSD_SOAK: ${{ matrix.osd_soak || '' }}
          CEPH_MSGR_TEST_RUNTIME: ${{ matrix.runtime || 'container' }}
          CEPH_MSGR_TEST_RACE: ${{ matrix.race || '0' }}
          CEPH_MSGR_TEST_EXPIRE_TICKETS: ${{ matrix.expiry || '0' }}
          CEPH_MSGR_TEST_IDLE_SESSIONS: ${{ matrix.idle || '0' }}
          CEPH_MSGR_TEST_AUTH_EPOCH: ${{ matrix.auth_epoch || '0' }}
          CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION: ${{ matrix.native_mgr_preparation || '0' }}
          CEPH_MSGR_TEST_SHORT_TICKETS: ${{ matrix.short_tickets || '0' }}
          CEPH_MSGR_TEST_MODE_REJECTION: ${{ matrix.mode_rejection || '' }}
          CEPH_MSGR_TEST_CONNECTION_MODE: ${{ matrix.mode || 'secure' }}
          CEPH_MSGR_TEST_MAPPED_IPV6: ${{ matrix.mapped || '0' }}
"""
FROZEN_LINE = re.compile(r"(?m)^ +(CEPH_[A-Z0-9_]+): \$\{\{ matrix\.([a-z_]+)(?: \|\| '([^']*)')? \}\}$")
# The run step's one fixture input that is not a matrix binding, then and now.
WORKFLOW_SET = {"CEPH_MSGR_TEST_DIAGNOSTICS": "${{ runner.temp }}/ceph-diagnostics"}


def frozen_bindings():
    return [(name, param, fallback) for name, param, fallback in FROZEN_LINE.findall(FROZEN_BINDINGS)]


def old_environment(matrix):
    """The CEPH_ variables the run step of fabf96a gave the runner: GitHub's
    `matrix.p || 'f'` is the first non-empty string."""
    env = {name: matrix.get(param) or fallback for name, param, fallback in frozen_bindings()}
    return dict(env, **WORKFLOW_SET)


def new_environment(matrix):
    return native_env.runner_environment(dict(WORKFLOW_SET), matrix)


def planner_jobs():
    """Every matrix entry the planner makes: the default coverage (`plan
    --select all`, its fixture sequences included) and each row alone."""
    rows = plan.load_catalog()
    declared = plan.plan_sources()
    jobs = list(plan.matrix(rows, declared)["include"])
    for row in rows:
        jobs += plan.matrix([row], declared)["include"]
    return rows, jobs


def run_step():
    text = WORKFLOW.read_text(encoding="utf-8")
    start = text.index("      - name: Real MON, MGR and opt-in OSD protocol verification")
    return text[start:text.index("\n      - name:", start + 1)]


class ForwardingEquivalenceTests(unittest.TestCase):
    def test_frozen_literal_is_the_legacy_table(self):
        bindings = frozen_bindings()
        self.assertEqual(len(bindings), len(FROZEN_BINDINGS.splitlines()), "a frozen line the reader skipped")
        self.assertEqual([(param, name, fallback) for name, param, fallback in bindings],
                         [(param, name, fallback) for param, (name, fallback) in native_env.LEGACY.items()],
                         "the legacy table is frozen; a new input is forwarded by the rule")

    def test_every_planned_job_gets_the_old_environment(self):
        rows, jobs = planner_jobs()
        sequences = [job for job in jobs if "test_sequence" in job]
        self.assertGreater(len(jobs), len(rows))
        self.assertTrue(sequences, "the default coverage plans no fixture sequence")
        # The old bindings describe the rows that existed when the rule
        # replaced them (scenario_order.RECORDED). A row declared later may
        # set an input the old workflow never had, so only jobs of recorded
        # rows are compared; the rule's own tests cover new inputs.
        recorded = set(scenario_order.RECORDED)
        compared = 0
        for job in jobs:
            if not set((job.get("scenarios") or job["scenario"]).split()) <= recorded:
                continue
            compared += 1
            with self.subTest(job=job["scenario"], rows=job.get("scenarios")):
                self.assertEqual(new_environment(json.loads(json.dumps(job))), old_environment(job))
        self.assertGreater(compared, len(recorded))

    def test_no_row_parameter_is_refused_that_the_old_workflow_forwarded(self):
        rows, _ = planner_jobs()
        for row in rows:
            for name in row["params"]:
                self.assertIsNone(native_env.parameter_problem(name), (row["id"], name))


class ForwardingRuleTests(unittest.TestCase):
    base = {"key": "aes256k", "service": "aes256k", "family": "4"}

    def test_a_new_parameter_is_forwarded_by_name_and_only_when_set(self):
        env = native_env.environment(dict(self.base, osd_new_mode="1", scenario="x.1", scenarios="x.1 x.2",
                                          sequence_rows="2", release="20.2.4"))
        self.assertEqual(env["CEPH_MSGR_TEST_OSD_NEW_MODE"], "1")
        self.assertEqual(native_env.environment(dict(self.base, osd_new_mode=""))["CEPH_MSGR_TEST_OSD_NEW_MODE"], "")
        self.assertNotIn("CEPH_MSGR_TEST_OSD_NEW_MODE", native_env.environment(self.base))
        for name in ("SCENARIO", "SCENARIOS", "SEQUENCE_ROWS", "RELEASE"):
            self.assertNotIn("CEPH_MSGR_TEST_" + name, env)
        self.assertEqual(list(env)[:len(native_env.LEGACY)], [name for name, _ in native_env.LEGACY.values()])
        self.assertIsNone(native_env.parameter_problem("osd_new_mode"))

    def test_reserved_colliding_planner_and_invalid_names_are_refused(self):
        refused = {
            "diagnostics": "CEPH_MSGR_TEST_DIAGNOSTICS is set by the native workflow itself",
            "key_type": "CEPH_MSGR_TEST_KEY_TYPE is the variable of parameter key",
            "run": "CEPH_MSGR_TEST_RUN is the variable of parameter test_run",
            "mgr_count": "CEPH_MSGR_TEST_MGR_COUNT is the variable of parameter managers",
            "scenario": "the planner sets it",
            "check": "the planner sets it",
            "Upper": "lower-case name",
            "path": None,
        }
        for name, reason in refused.items():
            with self.subTest(name=name):
                problem = native_env.parameter_problem(name)
                if reason is None:
                    self.assertIsNone(problem)
                    continue
                self.assertIn(reason, problem)
                if name not in native_env.PLANNER_KEYS:
                    with self.assertRaises(native_env.ForwardingError):
                        native_env.environment(dict(self.base, **{name: "x"}))
                problems = plan.validate_catalog([{"id": "a.1", "group": "a", "params": dict(self.base, **{name: "x"})}])
                self.assertIn("a.1: trusted native runner does not support parameter %s: " % name, "\n".join(problems))
        # A name the rule may form never leaves the fixture's prefix.
        self.assertEqual(native_env.environment(dict(self.base, path="x"))["CEPH_MSGR_TEST_PATH"], "x")
        for matrix in ([], dict(self.base, osd=1), dict(self.base, **{"a-b": "1"})):
            with self.assertRaises(native_env.ForwardingError):
                native_env.environment(matrix)

    def test_the_step_keeps_its_own_variables(self):
        with self.assertRaisesRegex(native_env.ForwardingError, "already sets CEPH_MSGR_TEST_OSD"):
            native_env.runner_environment({"CEPH_MSGR_TEST_OSD": "0"}, self.base)
        env = native_env.runner_environment({"PATH": "/bin", "CEPH_MSGR_TEST_DIAGNOSTICS": "/d"}, self.base)
        self.assertEqual((env["PATH"], env["CEPH_MSGR_TEST_DIAGNOSTICS"]), ("/bin", "/d"))

    def test_serve_answers_one_line_per_matrix(self):
        output = io.StringIO()
        native_env.serve([json.dumps(self.base) + "\n", json.dumps(dict(self.base, run="x")) + "\n", "not json\n"], output)
        answers = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(answers[0]["environment"]["CEPH_MSGR_TEST_KEY_TYPE"], "aes256k")
        self.assertIn("CEPH_MSGR_TEST_RUN is the variable", answers[1]["error"])
        self.assertIn("error", answers[2])

    def test_exec_gives_the_command_the_forwarded_environment(self):
        script = Path(__file__).resolve().parent / "native_env.py"
        # SYSTEMROOT is kept where it exists: a Windows Python started
        # without it fails before running any code.
        env = {name: os.environ[name] for name in ("PATH", "SYSTEMROOT") if name in os.environ}
        env.update(MATRIX_JSON=json.dumps(dict(self.base, new_input="7")), CEPH_MSGR_TEST_DIAGNOSTICS="/d")
        out = subprocess.run([sys.executable, str(script), "--", sys.executable, "-c",
                              "import os, json; print(json.dumps({k: v for k, v in os.environ.items() if k.startswith('CEPH_')}))"],
                             env=env, capture_output=True, text=True, check=True)
        got = json.loads(out.stdout)
        self.assertEqual(got, dict(native_env.environment(dict(self.base, new_input="7")), CEPH_MSGR_TEST_DIAGNOSTICS="/d"))
        refused = subprocess.run([sys.executable, str(script), "--", "true"], capture_output=True, text=True,
                                 env=dict(env, MATRIX_JSON=json.dumps(dict(self.base, diagnostics="/tmp/x"))))
        self.assertEqual(refused.returncode, 2)
        self.assertIn("CEPH_MSGR_TEST_DIAGNOSTICS is set by the native workflow itself", refused.stderr)


class WindowsRunnerTests(unittest.TestCase):
    def test_windows_runs_the_command_and_returns_its_status(self):
        """Windows has no process-replacing exec; the helper runs the command
        with the forwarded environment and returns its status instead."""
        calls = []

        def call(args, env):
            calls.append((args, env))
            return 7

        matrix = json.dumps({"key": "aes256k", "service": "aes256k", "family": "4", "new_input": "1"})
        with mock.patch.object(native_env.os, "name", "nt"), mock.patch.object(native_env.subprocess, "call", call), \
                mock.patch.object(native_env.os, "execvpe", side_effect=AssertionError("exec on Windows")), \
                mock.patch.dict(native_env.os.environ, {"MATRIX_JSON": matrix}, clear=False):
            self.assertEqual(native_env.main(["--", "runner", "arg"]), 7)
        self.assertEqual(calls[0][0], ["runner", "arg"])
        self.assertEqual(calls[0][1]["CEPH_MSGR_TEST_NEW_INPUT"], "1")


class WorkflowTests(unittest.TestCase):
    def test_only_the_runner_receives_the_matrix_and_no_input_has_a_line(self):
        text = WORKFLOW.read_text(encoding="utf-8")
        # No step binds a fixture input to a matrix value; the helper does.
        self.assertEqual(re.findall(r"(?m)^ +CEPH_[A-Z0-9_]+: .*matrix\.", text), [])
        # The helper comes from the trusted checkout, before the tested one.
        self.assertLess(text.index('cp .trusted-native/tools/native_env.py "$RUNNER_TEMP/native_env.py"'),
                        text.index("ref: ${{ inputs.ref }}"))
        step = run_step()
        self.assertEqual(step.count('python3 "$RUNNER_TEMP/native_env.py" -- sh integration/run.sh'), 1)
        self.assertEqual(step.count("integration/run.sh"), 1)
        self.assertIn("MATRIX_JSON: ${{ toJSON(matrix) }}", step)
        self.assertNotIn("${{", step[:step.index("        env:")], "an expression inside the script")
        # The variables the step sets itself are the reserved names.
        own = set(re.findall(r"(?m)^          (CEPH_[A-Z0-9_]+): ", step))
        self.assertEqual(own, set(native_env.RESERVED))
        self.assertEqual(own, set(WORKFLOW_SET))


if __name__ == "__main__":
    unittest.main()
