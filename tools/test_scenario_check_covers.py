"""Tests of the covers check (tools/scenario_checks/covers.py)."""
import unittest
from pathlib import Path

import scenario_checks
from scenario_checks import covers

ROOT = Path(__file__).resolve().parent.parent
SPEC = """# Spec

## Final test-environment scope

Text.

| Path | Required native setup and oracle |
| --- | --- |
| Messenger/CephX | Authentication |
| CLIENT to OSD | Reads |

More text.

### Environment acceptance

| Not | A path |
| --- | --- |
| Other | Row |
"""


class CoversCheckTest(unittest.TestCase):
    def test_spec_paths_are_the_first_column_of_the_scope_table(self):
        self.assertEqual(covers.spec_paths(SPEC), ["Messenger/CephX", "CLIENT to OSD"])
        with self.assertRaises(ValueError):
            covers.spec_paths("# Spec\n")

    def test_an_unknown_path_is_refused(self):
        paths = covers.spec_paths(SPEC)
        self.assertEqual(covers.problems(paths, [("TestCephXIntegration", "CLIENT to OSD", "integration/x_test.go:7")]), [])
        found = covers.problems(paths, [("TestCephXIntegration", "CLIENT to MDS", "integration/x_test.go:7")])
        self.assertEqual(len(found), 1)
        self.assertIn("integration/x_test.go:7: TestCephXIntegration covers 'CLIENT to MDS'", found[0])

    def test_the_tree_declares_only_known_paths(self):
        context = scenario_checks.ScenarioContext(root=ROOT, document={}, scenarios=[])
        self.assertEqual(covers.check(context), [])

if __name__ == "__main__":
    unittest.main()
