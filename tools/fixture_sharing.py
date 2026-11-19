#!/usr/bin/env python3
"""Fixture sharing: the one reader of the `//fixture:` declarations.

Every native test declares beside its function, as the last line of its doc
comment, whether a later test may use the fixture it ran on
(integration/fixture/FIXTURE_SHARING.md):

    //fixture:shareable <reason>
    //fixture:isolated <reason>

This module is the only code that reads those lines. It reads them as text,
because the trusted planner plans a pull request's revision from its Git
blobs and must not build or run that revision's code. The fixture module does
not read them: it supplies the one fact only it knows, each row's effective
selector (TestCatalogRowsAreAdmittedByTheFixture, loadProfile), and
row_values derives every row's value here from that selector and these
declarations. The planner groups rows with an explicit test_run by the same
rule (shareable_row), and validate requires the fixture's effective selector
of such a row to be its test_run, so the two uses cannot differ.
"""
import re
from pathlib import Path

import scenario_catalog

# (label, directory, native test name pattern, routed): the suites whose
# native tests declare fixture sharing. The routed suite holds the RGW
# checkpoints, which only rows with rgw_native=1 select.
SUITES = (
    ("integration", "integration", re.compile(r"TestCeph\w*Integration"), False),
    ("cephmsgr", "cephmsgr", re.compile(r"TestCeph\w*Integration"), False),
    ("fixture", "integration/fixture", re.compile(r"TestRGWNative\w*Integration"), True),
)
VALUES = ("shareable", "isolated")
PREFIX = "//fixture:"
DIRECTIVE = re.compile(r"//fixture:(shareable|isolated) (\S.*)")
REASON = re.compile(r"[ -~]+")
MAX_REASON = 120
TEST_FUNCTION = re.compile(r"func (Test\w*)\(")


def suite_of(path):
    """The SUITES entry whose test files include path, or None."""
    path = Path(path)
    if not path.name.endswith("_test.go"):
        return None
    for suite in SUITES:
        if suite[1] == path.parent.as_posix():
            return suite
    return None


def _doc_comment(lines, index):
    """The line indexes of the doc comment directly above line index."""
    start = index
    while start > 0 and lines[start - 1].startswith("//"):
        start -= 1
    return range(start, index)


def test_declarations(sources):
    """({"label/TestName": {"value", "reason", "source"}}, problems).

    sources is {path: text}; files outside SUITES are ignored. Every native
    test (a top-level function whose name matches its suite's pattern) is
    inventoried; its value is None unless it carries exactly one valid
    declaration as the last line of its doc comment. A missing, repeated,
    invalid, misplaced or not-last declaration and a native test declared
    twice in one suite are problems."""
    tests, problems = {}, []
    for path in sorted(sources):
        suite = suite_of(path)
        if suite is None:
            continue
        label, _, native, _ = suite
        lines = sources[path].splitlines()
        attached = set()
        for index, line in enumerate(lines):
            match = TEST_FUNCTION.match(line)
            if not match or not native.fullmatch(match[1]):
                continue
            key = "%s/%s" % (label, match[1])
            if key in tests:
                problems.append("duplicate native test %s (%s and %s)" % (key, tests[key]["source"], path))
                continue
            declared = {"value": None, "reason": "", "source": path}
            doc = [number for number in _doc_comment(lines, index) if lines[number].startswith(PREFIX)]
            attached.update(doc)
            if not doc:
                problems.append("%s (%s) declares no fixture sharing; add //fixture:shareable <reason> or"
                                " //fixture:isolated <reason> as the last line of its doc comment" % (key, path))
            elif len(doc) > 1:
                problems.append("%s (%s) declares fixture sharing %d times" % (key, path, len(doc)))
            elif doc[0] != index - 1:
                problems.append("%s (%s): the fixture sharing declaration %r is not the last line of its doc comment"
                                % (key, path, lines[doc[0]]))
            else:
                directive = DIRECTIVE.fullmatch(lines[doc[0]])
                if directive is None:
                    problems.append("%s (%s): invalid fixture sharing declaration %r; want //fixture:shareable <reason>"
                                    " or //fixture:isolated <reason>" % (key, path, lines[doc[0]]))
                elif (len(directive[2]) > MAX_REASON or not REASON.fullmatch(directive[2])
                      or directive[2].endswith(".")):
                    problems.append("%s (%s): fixture sharing reason must be printable ASCII of at most %d bytes"
                                    " without a final period: %r" % (key, path, MAX_REASON, directive[2]))
                else:
                    declared["value"], declared["reason"] = directive[1], directive[2]
            tests[key] = declared
        for index, line in enumerate(lines):
            if line.lstrip().startswith(PREFIX) and index not in attached:
                problems.append("%s:%d: fixture sharing declaration %r is not on a native test's doc comment"
                                % (path, index + 1, line.strip()))
    return tests, sorted(problems)


def selection_value(selector, routed, tests):
    """(value, selected, isolating, undeclared) of one effective selector.

    value is None when the selector is invalid, selects no native test of its
    suites (the routed suite or the product suites) or selects a test without
    a valid declaration."""
    try:
        pattern = re.compile(selector)
    except re.error:
        return None, [], [], []
    selected = sorted(key for key in tests
                      if (key.split("/", 1)[0] == "fixture") == routed and pattern.search(key.split("/", 1)[1]))
    isolating = [key for key in selected if tests[key]["value"] == "isolated"]
    undeclared = [key for key in selected if tests[key]["value"] not in VALUES]
    if not selected or undeclared:
        return None, selected, isolating, undeclared
    return ("isolated" if isolating else "shareable"), selected, isolating, undeclared


def shareable_row(params, tests):
    """Whether the planner may group a row: it has an explicit test_run, runs
    on the controller's fixture (not a routed checkpoint) and every product
    test its selector selects is shareable."""
    selector = params.get("test_run")
    if not selector or params.get("rgw_native") == "1":
        return False
    return selection_value(selector, False, tests)[0] == "shareable"


def row_values(scenarios, selectors, tests):
    """({row: value}, problems) for every row.

    selectors is {row: (routed, effective selector)} as the fixture reports
    them; a row the fixture does not report (it refuses it) has no value."""
    values, problems = {}, []
    for entry in scenarios:
        sid = entry["id"]
        if sid not in selectors:
            problems.append("%s: no fixture sharing value: the fixture reports no effective selector for it" % sid)
            continue
        routed, selector = selectors[sid]
        value, selected, isolating, undeclared = selection_value(selector, routed, tests)
        if value is not None:
            values[sid] = value
        elif undeclared:
            problems.append("%s: no fixture sharing value: it selects %s, which declares no valid fixture sharing"
                            % (sid, ", ".join(undeclared)))
        else:
            problems.append("%s: no fixture sharing value: its effective selector %r selects no native test"
                            % (sid, selector))
    return values, problems


def value_line(sid, selector, routed, tests):
    """A row's value as `sharing` prints it: isolated rows name how many
    selected tests isolate them and the first of them."""
    value, selected, isolating, _ = selection_value(selector, routed, tests)
    if value == "isolated":
        return "%s isolated by %d of %d selected tests, first %s" % (sid, len(isolating), len(selected), isolating[0])
    return "%s %s" % (sid, value)


def tree_sources(root):
    """The suites' test files of a working tree."""
    root = Path(root)
    return {path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
            for _, directory, _, _ in SUITES for path in sorted((root / directory).glob("*_test.go"))}


def revision_sources(revision, git_dir):
    """The suites' test files of a Git revision, from its blobs."""
    return {path: text for path, text in scenario_catalog.revision_sources(revision, git_dir).items()
            if suite_of(path) is not None}
