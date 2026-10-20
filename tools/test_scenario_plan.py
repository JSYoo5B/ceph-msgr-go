import copy
import json
import unittest

import scenario_plan as plan


def entry(sid, group="mon-mgr", **params):
    base = {"key": "aes", "service": "aes", "family": "4"}
    base.update(params)
    return {"id": sid, "group": group, "params": base}


class CatalogTests(unittest.TestCase):
    def test_repository_catalog_is_valid_and_matches_workflow(self):
        scenarios = plan.load_catalog()
        self.assertEqual(plan.validate_catalog(scenarios), [])
        self.assertEqual(plan.parity_problems(scenarios, plan.workflow_matrix()), [])
        self.assertEqual(plan.selector_problems(scenarios, plan.integration_tests()), [])

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

    def test_parity_detects_both_directions(self):
        scenarios = [entry("a"), entry("b", family="6")]
        rows = [dict(scenarios[0]["params"]), {"key": "aes", "service": "aes", "family": "9"}]
        text = "\n".join(plan.parity_problems(scenarios, rows))
        self.assertIn("workflow row not in catalog", text)
        self.assertIn("catalog scenario not in workflow", text)
        self.assertEqual(plan.parity_problems(scenarios, [s["params"] for s in scenarios]), [])

    def test_selector_that_matches_no_test_is_reported(self):
        scenarios = [entry("a", test_run="^TestNothing$"), entry("b", test_run="^TestReal$"), entry("c")]
        problems = plan.selector_problems(scenarios, {"TestReal"})
        self.assertEqual(len(problems), 1)
        self.assertIn("a: selector", problems[0])

    def test_workflow_parser_handles_quoted_commas(self):
        self.assertEqual(plan._split_flow(" a: 'x,y', b: z"), [" a: 'x,y'", " b: z"])


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

    def test_cli_exit_codes(self):
        self.assertEqual(plan.main(["validate"]), 0)
        self.assertEqual(plan.main(["plan", "--select", "no-such-scenario"]), 2)


if __name__ == "__main__":
    unittest.main()
