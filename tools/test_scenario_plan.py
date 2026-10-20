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
        self.assertEqual(plan.fixed_job_problems(document, plan.WORKFLOW.read_text(encoding="utf-8")), [])
        self.assertEqual(plan.selector_problems(scenarios, plan.integration_tests()), [])

    def test_workflows_use_the_planner_and_the_reusable_runtime(self):
        root = plan.ROOT / ".github" / "workflows"
        ci = (root / "ci.yml").read_text(encoding="utf-8")
        self.assertIn("scenario_plan.py plan", ci)
        self.assertIn("uses: ./.github/workflows/native-scenarios.yml", ci)
        self.assertNotIn("include:\n          - { key:", ci, "a static matrix would bypass the catalog")
        for name in ("ci-command.yml", "periodic.yml"):
            text = (root / name).read_text(encoding="utf-8")
            self.assertIn("native-scenarios.yml", text)
        runtime = (root / "native-scenarios.yml").read_text(encoding="utf-8")
        self.assertIn("persist-credentials: false", runtime)
        self.assertIn("fromJSON(inputs.matrix)", runtime)

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

    def test_selector_that_matches_no_test_is_reported(self):
        scenarios = [entry("a", test_run="^TestNothing$"), entry("b", test_run="^TestReal$"), entry("c")]
        problems = plan.selector_problems(scenarios, {"TestReal"})
        self.assertEqual(len(problems), 1)
        self.assertIn("a: selector", problems[0])


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
