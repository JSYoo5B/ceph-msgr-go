#!/usr/bin/env python3
"""Which catalog rows may run on one fixture, read from the test sources only.

The planner groups the shareable rows of one fixture configuration into one
job (scenario_plan.py plan). It may read another revision's catalog (a pull
request's), so it must not build or run that revision's code. This module
therefore reads the `//fixture:shareable` and `//fixture:isolated`
declarations (integration/fixture/FIXTURE_SHARING.md) from the Go test
sources as text: the working tree, or a Git revision's blobs without a
checkout.

The reading is conservative. A row joins a group only when it has an
explicit test_run and every top-level test function of the two native suite
packages (integration, cephmsgr) whose name the selector matches carries a
`//fixture:shareable` declaration as the last line of its doc comment, in
every file that declares that name. A test function this scan does not
recognize can only be missed if gofmt would not format it the canonical way;
`scenario_plan.py validate` requires this reading to agree with the fixture's
own (TestCatalogRowsDeclareFixtureSharing, go/parser) for every row with an
explicit test_run, so the two cannot drift apart on a validated tree.
"""
import re
from pathlib import Path

import scenario_catalog

SUITE_DIRECTORIES = ("integration", "cephmsgr")
TEST_FUNCTION = re.compile(r"^func (Test\w*)\(")
DECLARATION = re.compile(r"^//fixture:(shareable|isolated) \S")


def declarations(sources):
    """{test name: set of values} for every top-level test function.

    sources is {path: text}. A test without a valid declaration as the last
    line of its doc comment has the value None."""
    values = {}
    for text in sources.values():
        lines = text.splitlines()
        for index, line in enumerate(lines):
            match = TEST_FUNCTION.match(line)
            if not match:
                continue
            above = lines[index - 1] if index else ""
            declared = DECLARATION.match(above)
            values.setdefault(match[1], set()).add(declared[1] if declared else None)
    return values


def shareable_row(params, values):
    """A row with an explicit selector whose every selected test is shareable."""
    selector = params.get("test_run")
    if not selector:
        return False
    try:
        pattern = re.compile(selector)
    except re.error:
        return False
    selected = [name for name in values if pattern.search(name)]
    return bool(selected) and all(values[name] == {"shareable"} for name in selected)


def _suite_file(path):
    parts = Path(path).parts
    return len(parts) == 2 and parts[0] in SUITE_DIRECTORIES and parts[1].endswith("_test.go")


def tree_sources(root):
    """The suite packages' test files of a working tree."""
    root = Path(root)
    return {path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
            for directory in SUITE_DIRECTORIES for path in sorted((root / directory).glob("*_test.go"))}


# A revision whose fixture runs sequences (integration/fixture/sequence.go
# reads this variable). An older fixture would run a sequence job's union
# selector in one process without the start-state checks, so the planner
# groups nothing for it.
SEQUENCE_RUNNER = (Path("integration") / "fixture", "sequence.go", b"CEPH_MSGR_TEST_SEQUENCE")


def runs_sequences(revision, git_dir):
    directory, name, mark = SEQUENCE_RUNNER
    for mode, kind, oid, entry in scenario_catalog._tree(git_dir, revision, directory):
        if entry == name and kind == "blob":
            return mark in scenario_catalog._git(git_dir, "cat-file", "blob", oid)
    return False


def revision_sources(revision, git_dir):
    """The suite packages' test files of a Git revision, from its blobs, or
    None when that revision's fixture does not run sequences."""
    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", revision):
        raise scenario_catalog.CatalogError("revision must be a full commit SHA")
    if not runs_sequences(revision, git_dir):
        return None
    sources = {}
    for directory in SUITE_DIRECTORIES:
        for mode, kind, oid, name in scenario_catalog._tree(git_dir, revision, Path(directory)):
            path = directory + "/" + name
            if kind == "blob" and mode in ("100644", "100755") and _suite_file(path):
                sources[path] = scenario_catalog._git(git_dir, "cat-file", "blob", oid).decode("utf-8", "replace")
    return sources
