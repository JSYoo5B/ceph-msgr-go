import tempfile
import unittest
from pathlib import Path

import scenario_checks

CONTEXT = scenario_checks.ScenarioContext(root=Path("."), document={}, scenarios=[{"id": "a"}, {"id": "b"}])


class ScenarioCheckModuleTests(unittest.TestCase):
    def run_checks(self, files):
        with tempfile.TemporaryDirectory() as directory:
            for name, text in files.items():
                (Path(directory) / name).write_text(text, encoding="utf-8")
            return scenario_checks.run(CONTEXT, Path(directory))

    def test_every_module_runs_in_name_order_with_the_shared_context(self):
        self.assertEqual(self.run_checks({
            "__init__.py": "raise SystemExit('the package file is not a check')\n",
            "b_rows.py": "def check(context):\n    return ['rows %d' % len(context.scenarios)]\n",
            "a_ids.py": "def check(context):\n    return [row['id'] for row in context.scenarios]\n",
            "notes.txt": "not a module\n",
        }), ["a_ids: a", "a_ids: b", "b_rows: rows 2"])
        self.assertEqual(self.run_checks({"clean.py": "def check(context):\n    return []\n"}), [])

    def test_a_broken_module_is_a_problem_not_a_skip(self):
        for name, files, wanted in (
                ("import error", {"broken.py": "import no_such_module\n"}, "scenario check broken does not import: ModuleNotFoundError"),
                ("no check", {"empty.py": "VALUE = 1\n"}, "scenario check empty defines no check(context)"),
                ("not callable", {"value.py": "check = 1\n"}, "scenario check value defines no check(context)"),
                ("wrong result", {"wrong.py": "def check(context):\n    return 'one problem'\n"}, "not a list of problem strings"),
                ("bad name", {"Bad-Name.py": "def check(context):\n    return []\n"}, "the name must match")):
            with self.subTest(name):
                problems = self.run_checks(files)
                self.assertEqual(len(problems), 1, problems)
                self.assertIn(wanted, problems[0])

    def test_the_repository_package_passes_on_the_repository(self):
        for path in scenario_checks.modules():
            self.assertRegex(path.stem, scenario_checks.NAME.pattern)


if __name__ == "__main__":
    unittest.main()
