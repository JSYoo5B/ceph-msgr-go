import copy
import json
import unittest

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

    def test_repository_capabilities_are_defined_and_met(self):
        scenarios = plan.load_catalog()
        vocabulary = plan.fixture_capabilities()
        self.assertEqual(plan.capability_problems(scenarios, vocabulary, plan.workflow_bindings()), [])
        names = [name for name, _ in vocabulary]
        self.assertEqual(len(names), len(set(names)))
        declared = {name for row in scenarios for name in row["params"].get("capabilities", "").split(",") if name}
        self.assertEqual(declared, set(names))
        self.assertEqual(plan.workflow_bindings()["CEPH_MSGR_TEST_CAPABILITIES"], ("capabilities", ""))

    # The four refusals of the acceptance, by the planner: a capability on a
    # selector of more than one test, a row that lacks a capability its
    # declared one requires, a capability the fixture does not define, and a
    # declared capability whose requirements the row's topology does not meet.
    # The fixture's refusals are in integration/fixture/capability_test.go.
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

    def test_capability_vocabulary_and_requirements_are_checked(self):
        vocabulary = [("mon-kill", ["OSD=1", "OSD_STORE=bluestore", "MDS_FRESH=0"]), ("pg-interval", ["+shard-history"]),
                      ("shard-history", ["OSD_REPLICAS=4"]), ("late", ["OSD=1"]), ("multi", ["OSD=1", "!late"]), ("local", ["LOCAL_ONLY="])]
        bindings = {"CEPH_MSGR_TEST_OSD": ("osd", "0"), "CEPH_MSGR_TEST_OSD_STORE": ("osd_store", "memstore"),
                    "CEPH_MSGR_TEST_MDS_FRESH": ("mds_fresh", "0"), "CEPH_MSGR_TEST_OSD_REPLICAS": ("osd_replicas", "1")}
        row = lambda sid, value, **params: entry(sid, test_run="^TestX$", capabilities=value, **params)
        good = [row("a", "mon-kill", osd="1", osd_store="bluestore"), row("b", "pg-interval,shard-history", osd_replicas="4"),
                row("c", "multi", osd="1")]
        self.assertEqual(plan.capability_problems(good, vocabulary, bindings), [])
        for scenario, wanted in (
                (row("u", "osd-kill"), "does not define"),
                (row("t", "mon-kill", osd="1"), "requires osd_store='bluestore', the row has 'memstore'"),
                (row("f", "mon-kill", osd="1", osd_store="bluestore", mds_fresh="1"), "requires mds_fresh='0'"),
                (row("n", "mon-kill"), "requires osd='1', the row has '0'"),
                (row("d", "pg-interval", osd_replicas="4"), "requires capability shard-history"),
                (row("x", "late,multi", osd="1"), "excludes capability late"),
                (row("l", "local"), "does not forward")):
            with self.subTest(scenario=scenario["id"]):
                self.assertIn(wanted, "\n".join(plan.capability_problems([scenario], vocabulary, bindings)))
        self.assertIn("defines a capability twice", "\n".join(plan.capability_problems([], vocabulary + vocabulary[:1], bindings)))

    def test_vocabulary_is_read_from_the_fixture_source(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "a.go").write_text('var x = defineCapability("one",\n\t"a \\"quoted\\" summary",\n\t"OSD=1 +two", nil)\n'
                                               'var y = defineCapability("two", "s", "!one", func(p profile) bool { return true })\n', encoding="utf-8")
            Path(directory, "a_test.go").write_text('var z = defineCapability("test-only", "s", "OSD=1", nil)\n', encoding="utf-8")
            self.assertEqual(plan.fixture_capabilities(directory), [("one", ["OSD=1", "+two"]), ("two", ["!one"])])

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
        self.assertEqual(plan.main(["validate"]), 0)
        self.assertEqual(plan.main(["plan", "--select", "no-such-scenario"]), 2)


if __name__ == "__main__":
    unittest.main()
