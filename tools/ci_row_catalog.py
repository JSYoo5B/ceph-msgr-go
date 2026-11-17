"""Shared loading for the native CI row admission tests (tools/test_ci_rows_*.py).

The matrix rows come from the scenario catalog through scenario_plan's loader
(standard library only, for all three runner OSes); the checked workflow text
includes basic CI and the reusable platform/native workflows. Workflow syntax
is checked separately with the available workflow/YAML tooling.

Each area module checks only its own rows. This module holds no test: unittest
discovery collects test_*.py modules only, and the base class below defines no
test method.
"""
import importlib
from pathlib import Path
import re
import unittest

import scenario_plan


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ("ci.yml", "platform-checks.yml", "native-scenarios.yml")


def declared_tests(directory, root=ROOT):
    """The names of the top-level tests Go lists for one native suite package
    (integration or cephmsgr), each once.

    The row tests compare names only. A row that selects a name Go lists more
    than once is refused by scenario_plan.selector_problems (validate) and by
    the result gate's snapshot, so these lookups need no count."""
    return set(scenario_plan.suite_tests(root)[directory])


def dedicated_legacy_selectors():
    """The exact selectors of the dedicated legacy-v6 rows of every area.

    Each test_ci_rows_* module owns the dedicated legacy-v6 rows of its area
    in DEDICATED_LEGACY_SELECTORS, so a new row edits only its area module.
    """
    selectors = set()
    for path in sorted(Path(__file__).resolve().parent.glob("test_ci_rows_*.py")):
        selectors.update(getattr(importlib.import_module(path.stem), "DEDICATED_LEGACY_SELECTORS", ()))
    return selectors


class RowsTestCase(unittest.TestCase):
    """Loads the catalog entries, their rows, the workflow text and the API-suite tests."""

    @classmethod
    def setUpClass(cls):
        # The matrix lives in the catalog; its job runs in the reusable workflow.
        cls.workflow = "\n".join((ROOT / ".github/workflows" / name).read_text(encoding="utf-8") for name in WORKFLOWS)
        cls.entries = scenario_plan.load_catalog()
        cls.rows = [dict(entry["params"]) for entry in cls.entries]
        cls.declared = declared_tests("integration")

    def client_tests(self):
        return declared_tests("cephmsgr")

    def assert_client_row(self, selector, params, tests, *further):
        # One row with exactly these parameters, and one for each further
        # profile, whose selector matches exactly these client-suite tests and
        # no API-suite test, so the result gate requires each of them to PASS.
        rows = [row for row in self.rows if row.get("test_run") == selector]
        self.assertEqual(rows, [dict(profile, test_run=selector) for profile in (params,) + further])
        compiled = re.compile(selector)
        self.assertEqual({name for name in self.client_tests() | self.declared if compiled.search(name)}, tests)
        self.assertFalse(tests & self.declared)
