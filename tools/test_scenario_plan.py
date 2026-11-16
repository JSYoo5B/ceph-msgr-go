import copy
import io
import json
import re
import shlex
import subprocess
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

import scenario_plan as plan


def entry(sid, group="mon-mgr", **params):
    base = {"key": "aes", "service": "aes", "family": "4"}
    base.update(params)
    return {"id": sid, "group": group, "params": base}


class CatalogTests(unittest.TestCase):
    def test_repository_catalog_is_valid_and_complete(self):
        document = plan.load_catalog_document()
        scenarios = document["scenarios"]
        self.assertEqual(plan.validate_catalog(scenarios), [])
        self.assertEqual(plan.mandatory_problems(scenarios, plan.load_mandatory()), [])
        self.assertEqual(plan.fixed_job_problems(document, plan.CHECKS_WORKFLOW.read_text(encoding="utf-8")), [])
        self.assertEqual(plan.selector_problems(scenarios, plan.integration_tests()), [])

    def test_workflows_use_the_planner_and_the_reusable_runtime(self):
        root = plan.ROOT / ".github" / "workflows"
        ci = (root / "ci.yml").read_text(encoding="utf-8")
        self.assertNotIn("matrix:", ci, "automatic CI must not expand scenario/platform matrices")
        self.assertNotIn("integration/run.sh", ci, "native sessions require an explicit request")
        self.assertNotIn("native-scenarios.yml", ci)
        for name in ("ci-command.yml", "periodic.yml"):
            text = (root / name).read_text(encoding="utf-8")
            self.assertIn("native-scenarios.yml", text)
            self.assertIn("platform-checks.yml", text)
            self.assertIn("scenario_plan.py plan", text)
        runtime = (root / "native-scenarios.yml").read_text(encoding="utf-8")
        self.assertIn("persist-credentials: false", runtime)
        self.assertIn("fromJSON(inputs.matrix).include", runtime)

    def test_rejects_duplicates_empty_selectors_and_unsupported_releases(self):
        scenarios = [entry("a"), entry("a"), entry("b", test_run=""), entry("c", release="19.2.3")]
        text = "\n".join(plan.validate_catalog(scenarios))
        self.assertIn("duplicate identifier a", text)
        self.assertIn("b has an empty test selector", text)
        self.assertIn("unsupported release 19.2.3", text)
        self.assertIn("no scenarios", "\n".join(plan.validate_catalog([])))

    def test_rejects_missing_required_parameters_and_bad_regex(self):
        bad = entry("a", test_run="(")
        del bad["params"]["service"]
        text = "\n".join(plan.validate_catalog([bad]))
        self.assertIn("lacks required parameter service", text)
        self.assertIn("invalid test selector", text)

    def test_workflow_bindings_cover_every_matrix_variable(self):
        text = plan.NATIVE_WORKFLOW.read_text(encoding="utf-8")
        bound = re.findall(r"(?m)^ +(CEPH_[A-Z0-9_]+): .*matrix\.", text)
        bindings = plan.workflow_bindings(text)
        self.assertEqual(sorted(bound), sorted(bindings), "a matrix binding the planner cannot read")
        self.assertEqual(bindings["CEPH_MSGR_TEST_CAPABILITIES"], ("capabilities", ""))

    # The planner's own refusal: a capability on a selector of more than one
    # test. Every other refusal (an undefined capability, a missing capability
    # that a declared one extends, a declared capability on a topology the
    # fixture does not admit) is the fixture's, through
    # fixture_admission_problems; integration/fixture/capability_test.go
    # covers those.
    def test_capability_requires_one_anchored_test(self):
        for selector in ("^TestCephMonMapVersion(ConnectedKill|UnprocessedKill)Integration$", "^TestCephOSD.*Integration$",
                         "TestCephOSDUpmapIntegration", "^TestCephOSDUpmapIntegration", "^TestA$|^TestB$", "^TestA$/sub", "^Test$"):
            text = "\n".join(plan.validate_catalog([entry("a", test_run=selector, capabilities="osd-long-tickets")]))
            self.assertIn("require a test_run of exactly one anchored test", text, selector)
        text = "\n".join(plan.validate_catalog([entry("a", capabilities="osd-long-tickets")]))
        self.assertIn("require a test_run of exactly one anchored test", text)
        self.assertEqual(plan.validate_catalog([entry("a", test_run="^TestCephOSDUpmapIntegration$", capabilities="osd-long-tickets")]), [])

    def test_capability_list_is_canonical(self):
        for value in ("", ",", "b,a", "a,a", "A", "a b", "a,,b", "-a"):
            text = "\n".join(plan.validate_catalog([entry("x", test_run="^TestX$", capabilities=value)]))
            self.assertTrue("invalid capability list" in text or "sorted and unique" in text, value)

    def test_fixture_admission_reports_the_fixtures_own_refusals(self):
        calls = []

        def fake(stdout="", returncode=0, stderr="", raises=None):
            def run(command, **kwargs):
                calls.append((command, kwargs))
                if raises:
                    raise raises
                return subprocess.CompletedProcess(command, returncode, stdout, stderr)
            return run

        def event(action, output=None, test=plan.ADMISSION_TEST):
            record = {"Action": action, "Package": "fixture", "Test": test}
            if output is not None:
                record["Output"] = output
            return json.dumps(record)

        passed = "\n".join([event("run"), event("pass")])
        self.assertEqual(plan.fixture_admission_problems(fake(passed)), [])
        command, kwargs = calls[0]
        self.assertEqual(command[:2], ["go", "test"])
        self.assertIn("^%s$" % plan.ADMISSION_TEST, command)
        self.assertEqual(kwargs["cwd"], str(plan.FIXTURE))
        self.assertEqual(kwargs["env"]["GOWORK"], "off")
        refused = "\n".join([
            event("run"),
            event("output", "    capability_test.go:9: catalog admission: osd.x: the fixture refuses the row: four-OSD topology\n"),
            event("output", "    capability_test.go:9: catalog admission: rbd.y: the fixture does not admit capability rbd-mirror\n"),
            event("output", "    capability_test.go:9: catalog admission: z: unrelated\n", test="TestOther"),
            event("fail")])
        self.assertEqual(plan.fixture_admission_problems(fake(refused, 1)),
                         ["osd.x: the fixture refuses the row: four-OSD topology",
                          "rbd.y: the fixture does not admit capability rbd-mirror"])
        # A check that does not run, is skipped, fails without naming a row or
        # cannot start is a problem, never a silent pass.
        for name, run in (("no test", fake('{"Action":"output","Output":"testing: warning: no tests to run\\n"}')),
                          ("skipped", fake("\n".join([event("run"), event("skip")]))),
                          ("build failure", fake("", 1, "go: go.mod requires go >= 1.26.0")),
                          ("failure without a row", fake("\n".join([event("run"), event("fail")]), 1)),
                          ("passed with a non-zero exit", fake(passed, 2)),
                          ("no go command", fake(raises=FileNotFoundError("go")))):
            with self.subTest(name):
                problems = plan.fixture_admission_problems(run)
                self.assertEqual(len(problems), 1)
                self.assertIn(plan.ADMISSION_TEST, problems[0])
        self.assertIn("go >= 1.26.0", plan.fixture_admission_problems(fake("", 1, "go: go.mod requires go >= 1.26.0"))[0])

    def test_fixture_admission_test_is_the_one_the_planner_runs(self):
        source = (plan.FIXTURE / "capability_test.go").read_text(encoding="utf-8")
        self.assertIn("func %s(t *testing.T) {" % plan.ADMISSION_TEST, source)
        self.assertIn('t.Error("%s" + problem)' % plan.ADMISSION_MARK, source)

    def test_validate_fails_on_a_row_the_fixture_refuses(self):
        refusal = "osd.x: the fixture refuses the row: four-OSD erasure-capacity topology requires ..."
        err = io.StringIO()
        with mock.patch.object(plan, "fixture_admission_problems", return_value=[refusal]), \
                redirect_stdout(io.StringIO()), redirect_stderr(err):
            self.assertEqual(plan.main(["validate"]), 1)
        self.assertIn(refusal, err.getvalue())

    def test_env_prints_the_workflow_environment_of_one_row(self):
        row = next(entry for entry in plan.load_catalog() if entry["params"].get("capabilities", "").count(",") == 2)
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(io.StringIO()):
            self.assertEqual(plan.main(["env", row["id"]]), 0)
        exported = {}
        for line in out.getvalue().splitlines():
            name, _, value = line.partition("=")
            self.assertTrue(name.startswith("export CEPH_"), line)
            exported[name[len("export "):]] = shlex.split(value)[0] if value else ""
        self.assertEqual(set(exported), set(plan.workflow_bindings()))
        self.assertEqual(exported["CEPH_MSGR_TEST_CAPABILITIES"], row["params"]["capabilities"])
        self.assertEqual(exported["CEPH_MSGR_TEST_RUN"], row["params"]["test_run"])
        self.assertEqual(exported["CEPH_MSGR_TEST_OSD_REPLICAS"], row["params"]["osd_replicas"])
        self.assertEqual(exported["CEPH_MSGR_TEST_MGR_COUNT"], "2", "the workflow fallback")
        self.assertTrue(exported["CEPH_MSGR_TEST_IMAGE"].startswith("quay.io/ceph/ceph:v20.2."), "the pinned image")
        self.assertEqual(exported["CEPH_MSGR_TEST_MODE_REJECTION"], "")
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(plan.main(["env", "no-such-scenario"]), 2)

    def test_documented_reproductions_declare_their_row(self):
        scenarios = [entry("osd.a", test_run="^TestCephA$", capabilities="osd-erasure-width,osd-shard-history"),
                     entry("osd.b", test_run="^TestCephB$")]
        good = ("Run it with:\n\n```sh\neval \"$(python3 tools/scenario_plan.py env osd.a)\"\nsh integration/run.sh\n```\n\n"
                "```sh\nCEPH_MSGR_TEST_RUN='^TestCephA$' \\\nCEPH_MSGR_TEST_CAPABILITIES=osd-erasure-width,osd-shard-history \\\n"
                "sh integration/run.sh\n```\n\nSelect `CEPH_MSGR_TEST_RUN=^TestCephB$` alone.\n")
        self.assertEqual(plan.reproduction_problems([("good.md", good)], scenarios), [])
        for text, wanted in (
                ("```sh\nCEPH_MSGR_TEST_RUN='^TestCephA$' \\\nsh integration/run.sh\n```\n", "lacks CEPH_MSGR_TEST_CAPABILITIES"),
                ("Select `CEPH_MSGR_TEST_RUN=^TestCephA$` and the row's key.\n", "lacks CEPH_MSGR_TEST_CAPABILITIES"),
                ("```sh\nCEPH_MSGR_TEST_RUN='^TestCephA$'\nCEPH_MSGR_TEST_CAPABILITIES=osd-erasure-width\n```\n",
                 "lacks CEPH_MSGR_TEST_CAPABILITIES=osd-erasure-width,osd-shard-history"),
                ("```sh\nCEPH_MSGR_TEST_CAPABILITIES=osd-erasure-width,osd-shard-history\n```\n\n"
                 "```sh\nCEPH_MSGR_TEST_RUN='^TestCephA$'\n```\n", "lacks CEPH_MSGR_TEST_CAPABILITIES"),
                ("```sh\neval \"$(python3 tools/scenario_plan.py env osd.gone)\"\n```\n", "unknown scenario osd.gone")):
            with self.subTest(text=text):
                self.assertIn(wanted, "\n".join(plan.reproduction_problems([("bad.md", text)], scenarios)))

    def test_repository_reproductions_declare_their_row(self):
        documents = [(str(path), path.read_text(encoding="utf-8")) for path in plan.documentation_files()]
        self.assertTrue(any("scenario_plan.py env " in text for _, text in documents))
        self.assertEqual(plan.reproduction_problems(documents, plan.load_catalog()), [])

    def test_mandatory_list_detects_silent_omission_in_both_directions(self):
        scenarios = [entry("a"), entry("b")]
        self.assertEqual(plan.mandatory_problems(scenarios, ["a", "b"]), [])
        text = "\n".join(plan.mandatory_problems(scenarios, ["a", "c", "c"]))
        self.assertIn("missing from catalog: c", text)
        self.assertIn("not in the mandatory list: b", text)
        self.assertIn("duplicates", text)

    def test_fixed_jobs_must_exist_in_the_workflow(self):
        document = {"fixed_jobs": [{"id": "unit", "job": "unit"}, {"id": "fuzz", "job": "fuzz"}]}
        self.assertEqual(plan.fixed_job_problems(document, "jobs:\n  unit:\n    x: 1\n  fuzz:\n"), [])
        self.assertIn("'fuzz'", "\n".join(plan.fixed_job_problems(document, "jobs:\n  unit:\n")))
        self.assertIn("no fixed jobs", "\n".join(plan.fixed_job_problems({}, "")))
        collision = {"scenarios": [entry("unit")], "fixed_jobs": [{"id": "unit", "job": "unit"}]}
        self.assertIn("duplicate fixed job unit", plan.fixed_job_problems(collision, "  unit:\n"))
        alias = {"fixed_jobs": [{"id": "unknown", "job": "unit"}]}
        self.assertIn("must equal its job name", "\n".join(plan.fixed_job_problems(alias, "  unit:\n")))

    def test_identifiers_must_be_safe_command_and_artifact_names(self):
        for sid in ("all", "UPPER", 'name"with-quote', "x\ny", "x" * 161):
            self.assertIn("invalid identifier", "\n".join(plan.validate_catalog([entry(sid)])))

    def test_selector_that_matches_no_test_is_reported(self):
        scenarios = [entry("a", test_run="^TestNothing$"), entry("b", test_run="^TestReal$"), entry("c")]
        problems = plan.selector_problems(scenarios, {"TestReal"})
        self.assertEqual(len(problems), 1)
        self.assertIn("a: selector", problems[0])

    def test_selectors_resolve_against_both_native_suites(self):
        tests = plan.integration_tests()
        self.assertIn("TestCephMonPingIntegration", tests)  # API suite
        self.assertIn("TestCephMonServiceMapLifecycleFailedMonHoldIntegration", tests)  # client suite

    def test_pr_catalog_cannot_silently_add_a_trusted_runner_parameter(self):
        from unittest.mock import patch
        future = entry("future", mgr_service="1")
        # Simulate main's old runner reading a PR's newer catalog.
        with patch("scenario_plan.NATIVE_WORKFLOW") as workflow:
            workflow.read_text.return_value = "\n".join(
                "  CEPH_TEST_%s: ${{ matrix.%s }}" % (name.upper(), name)
                for name in ("key", "service", "family")) + "\nname: matrix.mgr_service"
            self.assertIn("does not support parameter mgr_service", "\n".join(plan.validate_catalog([future])))
        self.assertEqual(plan.validate_catalog([future]), [])
        self.assertIn("CEPH_MSGR_TEST_MGR_SERVICE: ${{ matrix.mgr_service || '0' }}",
                      plan.NATIVE_WORKFLOW.read_text(encoding="utf-8"))


class PlannerTests(unittest.TestCase):
    scenarios = [entry("m1"), entry("m2", family="6"), entry("o1", group="osd", osd="1")]

    def test_all_group_and_identifier_selection_keeps_catalog_order(self):
        chosen, why = plan.select(self.scenarios, ["all"])
        self.assertEqual([e["id"] for e in chosen], ["m1", "m2", "o1"])
        self.assertIn("all: 3 scenarios", why)
        chosen, _ = plan.select(self.scenarios, ["o1", "group:mon-mgr"])
        self.assertEqual([e["id"] for e in chosen], ["m1", "m2", "o1"])
        chosen, _ = plan.select(self.scenarios, ["m2", "m2"])
        self.assertEqual([e["id"] for e in chosen], ["m2"])

    def test_unknown_or_empty_selection_fails_instead_of_matching_nothing(self):
        for selectors in ([], ["nope"], ["group:nope"], ["m1", "typo"]):
            with self.assertRaises(plan.PlanError):
                plan.select(self.scenarios, selectors)

    def test_platform_checks_can_be_requested_without_native_scenarios(self):
        document = {"fixed_jobs": [{"id": "race"}, {"id": "windows-ceph"}]}
        vocabulary = self.scenarios + plan.fixed_entries(document)
        selected, _ = plan.select(vocabulary, ["race", "windows-ceph"])
        self.assertEqual(plan.matrix(selected), {"include": [], "fixed": ["race", "windows-ceph"]})
        selected, _ = plan.select(vocabulary, ["m1", "group:checks"])
        result = plan.matrix(selected)
        self.assertEqual([r["scenario"] for r in result["include"]], ["m1"])
        self.assertEqual(result["fixed"], ["race", "windows-ceph"])

    def test_matrix_carries_parameters_and_scenario_identifier(self):
        chosen, _ = plan.select(self.scenarios, ["o1"])
        rows = plan.matrix(chosen)["include"]
        self.assertEqual(rows, [{"key": "aes", "service": "aes", "family": "4", "osd": "1", "scenario": "o1"}])
        self.assertEqual(self.scenarios[2]["params"].get("scenario"), None, "planning must not mutate the catalog")

    def test_plan_against_another_revisions_catalog(self):
        import io
        import os
        import tempfile
        from contextlib import redirect_stderr, redirect_stdout
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "catalog.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump({"version": 1, "scenarios": self.scenarios}, handle)
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                self.assertEqual(plan.main(["plan", "--catalog", path, "--select", "o1"]), 0)
            self.assertEqual([r["scenario"] for r in json.loads(out.getvalue())["include"]], ["o1"])
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                self.assertEqual(plan.main(["plan", "--catalog", path, "--select", "m9"]), 2)

    def test_cli_exit_codes(self):
        # The fixture's own admission runs in the validate command itself
        # (Basic CI); these unit tests also run with the product's Go 1.25.
        with mock.patch.object(plan, "fixture_admission_problems", return_value=[]), redirect_stdout(io.StringIO()):
            self.assertEqual(plan.main(["validate"]), 0)
        self.assertEqual(plan.main(["plan", "--select", "no-such-scenario"]), 2)


if __name__ == "__main__":
    unittest.main()
