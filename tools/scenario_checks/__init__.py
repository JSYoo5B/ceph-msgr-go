"""Checks that `scenario_plan.py validate` runs, one module per check.

A new catalog check is a module of its own in this package, with its tests in
a test module of its own (tools/test_scenario_check_<name>.py), so two pull
requests that add different checks edit no shared file. validate imports
every module here in name order and calls its `check(context)`, which returns
a list of problem strings; `context` is a ScenarioContext. A module that does
not import or has no callable `check` is a problem itself, never skipped.
"""
import importlib.util
import re
from dataclasses import dataclass, field
from pathlib import Path

NAME = re.compile(r"[a-z][a-z0-9_]*")


@dataclass(frozen=True)
class ScenarioContext:
    """What validate has read once, shared by every check.

    root: the repository root; document: the composed catalog
    (scenario_catalog.load); scenarios: its rows; listing / routed_listing:
    the tests Go lists for the product suites and for the routed checkpoints
    (scenario_plan.suite_tests, routed_tests); sharing: the native tests'
    fixture sharing declarations (fixture_sharing.test_declarations);
    selectors: each admitted row's effective selector, as the fixture reports
    it; values: each row's fixture sharing value."""
    root: Path
    document: dict
    scenarios: list
    listing: dict = field(default_factory=dict)
    routed_listing: dict = field(default_factory=dict)
    sharing: dict = field(default_factory=dict)
    selectors: dict = field(default_factory=dict)
    values: dict = field(default_factory=dict)


DIRECTORY = Path(__file__).resolve().parent


def modules(directory=DIRECTORY):
    """The check modules of the package directory, in name order."""
    return sorted(path for path in Path(directory).glob("*.py") if path.name != "__init__.py")


def run(context, directory=DIRECTORY):
    """Every check module's problems, prefixed with its name."""
    problems = []
    for path in modules(directory):
        name = path.stem
        if not NAME.fullmatch(name):
            problems.append("scenario check module %r: the name must match %s" % (path.name, NAME.pattern))
            continue
        try:
            spec = importlib.util.spec_from_file_location("scenario_checks." + name, path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except Exception as error:  # a broken check must fail validation, not vanish
            problems.append("scenario check %s does not import: %s: %s" % (name, type(error).__name__, error))
            continue
        check = getattr(module, "check", None)
        if not callable(check):
            problems.append("scenario check %s defines no check(context)" % name)
            continue
        found = check(context)
        if not isinstance(found, list) or not all(isinstance(problem, str) for problem in found):
            problems.append("scenario check %s returned %r, not a list of problem strings" % (name, type(found).__name__))
            continue
        problems += ["%s: %s" % (name, problem) for problem in found]
    return problems
