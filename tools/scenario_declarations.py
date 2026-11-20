#!/usr/bin/env python3
"""Read the native scenario rows declared beside the tests they select.

A row is one comment directive in the doc comment of a native test function:

  //scenario:row <id> [<parameter>=<value> ...]
  //scenario:suite <id> [<parameter>=<value> ...]
  //scenario:covers <path>
  //fixture:shareable <reason>
  func TestCeph<Name>Integration(t *testing.T) {

The directive's test is the owner. A `row` selects its owner: its test_run is
`^<owner>$`, or an explicit `test_run=` whose selector also selects the owner
(a row of several tests, declared beside one of them). A `suite` row has no
test_run: it runs every test its fixture configuration enables, and it is
declared beside the test that configuration exists for, or beside its group's
base test. The row's group is its identifier before the first dot. Its
parameters are the written ones in written order, followed by test_run and
then capabilities, the order the matrix entry keeps.

A `covers` line has no parameters: it names the path of SPEC.md's final
test-environment scope table (its first column, for example `CLIENT to OSD`)
that its owner gives native evidence for, one line per path. It claims no
result and no completeness; open gaps of a path are tracked by issues. Which
paths exist is checked by validate (scenario_checks/covers.py), the one
reader of that table.

Declarations are read from the `*_test.go` files of the native suites:
integration and cephmsgr (the product's API and client suites), and
integration/fixture, where only the routed RGW checkpoints are declared
(rgw_native=1; integration/fixture/rgw_native_route.go). Rows are returned in
planning order: the rows that predate the declarations in their recorded order
(scenario_order.py, which no new row joins), then every later row in
identifier order, so where a test or its file sits changes no plan.

The reading is text only and builds or runs nothing, because the trusted
planner reads a pull request's declarations from its Git blobs
(scenario_catalog.py). This is the only reader of the declarations: the
fixture's catalog tests take the rows it returns (`scenario_plan.py rows`,
integration/fixture/ci_rows_test.go) and never parse the directives. A
directive is refused when it is
malformed, indented, not in the doc comment of a top-level native test
function of its suite, duplicates an identifier, a parameter or a covered
path of its owner, or breaks the rules of its kind above.
"""
import re
from pathlib import Path

import scenario_order

# (directory, native test name pattern, routed): the suites whose test files
# hold declarations. Routed rows run one RGW checkpoint of the fixture module.
SUITES = (
    ("integration", re.compile(r"TestCeph\w*Integration"), False),
    ("cephmsgr", re.compile(r"TestCeph\w*Integration"), False),
    ("integration/fixture", re.compile(r"TestRGWNative\w*Integration"), True),
)
PREFIX = "//scenario:"
DIRECTIVE = re.compile(r"//scenario:(row|suite) (\S+)((?: \S+)*)")
COVERS = re.compile(r"//scenario:covers (\S(?:[ \S]*\S)?)")
IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._-]{0,159}")
GROUP = re.compile(r"[a-z0-9][a-z0-9-]*")
PARAMETER = re.compile(r"([a-z][a-z0-9_]*)=(\S*)")
TEST_FUNCTION = re.compile(r"func (Test\w*)\(")
ROUTING = "rgw_native"
# The parameters that follow the written ones, in this order.
TRAILING = ("test_run", "capabilities")
RECORDED_RANK = {sid: rank for rank, sid in enumerate(scenario_order.RECORDED)}


def suite_of(path):
    """The SUITES entry whose test files include path, or None."""
    path = Path(path)
    if not path.name.endswith("_test.go"):
        return None
    directory = path.parent.as_posix()
    for suite in SUITES:
        if suite[0] == directory:
            return suite
    return None


def declaration_lines(text):
    """[(line number, line)] of the lines that look like a declaration."""
    return [(number, line) for number, line in enumerate(text.splitlines(), 1) if line.lstrip().startswith(PREFIX)]


def _owner(lines, index):
    """The test function whose doc comment holds line index, or None."""
    for line in lines[index + 1:]:
        if line.startswith("//"):
            continue
        match = TEST_FUNCTION.match(line)
        return match[1] if match else None
    return None


def parse_file(path, text):
    """([(row, owner, location)], [(owner, covered path, location)], problems)
    of one test file."""
    suite = suite_of(path)
    lines = text.splitlines()
    rows, covers, problems = [], [], []
    for index, line in enumerate(lines):
        if not line.lstrip().startswith(PREFIX):
            continue
        location = "%s:%d" % (Path(path).as_posix(), index + 1)

        def refuse(message):
            problems.append("%s: %s" % (location, message))
        if suite is None:
            refuse("a scenario declaration outside the native suites' test files")
            continue
        directory, native, routed_suite = suite
        if line != line.lstrip():
            refuse("an indented scenario declaration")
            continue
        covered = COVERS.fullmatch(line)
        if covered:
            owner = _owner(lines, index)
            if owner is None or not native.fullmatch(owner):
                refuse("covers %s is not in the doc comment of a native test of %s" % (covered[1], directory))
            elif any(entry[:2] == (owner, covered[1]) for entry in covers):
                refuse("%s covers %s twice" % (owner, covered[1]))
            else:
                covers.append((owner, covered[1], location))
            continue
        match = DIRECTIVE.fullmatch(line)
        if not match:
            refuse("a malformed scenario declaration (//scenario:row|suite <id> [name=value ...]"
                   " or //scenario:covers <path>)")
            continue
        kind, sid, written = match[1], match[2], match[3].split()
        owner = _owner(lines, index)
        if owner is None:
            refuse("%s is not in the doc comment of a top-level test function" % sid)
            continue
        if not native.fullmatch(owner):
            refuse("%s is declared beside %s, which is not a native test of %s" % (sid, owner, directory))
            continue
        if not IDENTIFIER.fullmatch(sid) or sid == "all":
            refuse("invalid scenario identifier %r" % sid)
            continue
        if not GROUP.fullmatch(sid.split(".", 1)[0]):
            refuse("%s has no group before its first dot" % sid)
            continue
        params, broken = {}, False
        for word in written:
            parameter = PARAMETER.fullmatch(word)
            if not parameter:
                refuse("%s: malformed parameter %r (name=value)" % (sid, word))
                broken = True
            elif parameter[1] in params:
                refuse("%s: parameter %s is given twice" % (sid, parameter[1]))
                broken = True
            else:
                params[parameter[1]] = parameter[2]
        if broken:
            continue
        exact = "^%s$" % owner
        if kind == "suite":
            if "test_run" in params:
                refuse("%s: a suite row runs its whole fixture configuration and has no test_run" % sid)
                continue
            if routed_suite:
                refuse("%s: a routed RGW checkpoint is a row of its own test, not a suite row" % sid)
                continue
        elif "test_run" in params:
            selector = params["test_run"]
            if selector == exact:
                refuse("%s: test_run %s is the owner's own selector; leave it out" % (sid, selector))
                continue
            try:
                selects = re.search(selector, owner) is not None
            except re.error as error:
                refuse("%s: invalid test_run %r: %s" % (sid, selector, error))
                continue
            if not selects:
                refuse("%s: test_run %s does not select %s, beside which it is declared" % (sid, selector, owner))
                continue
        else:
            params["test_run"] = exact
        if routed_suite != (params.get(ROUTING) == "1"):
            refuse("%s: %s=1 is declared exactly on the RGW checkpoints of integration/fixture" % (sid, ROUTING))
            continue
        ordered = {name: value for name, value in params.items() if name not in TRAILING}
        ordered.update((name, params[name]) for name in TRAILING if name in params)
        rows.append(({"id": sid, "group": sid.split(".", 1)[0], "params": ordered}, owner, location))
    return rows, covers, problems


def discover(sources):
    """(rows in planning order, problems) of {path: text} test sources.

    A problem names the file and line of the declaration it refuses."""
    found, problems, where = [], [], {}
    for path in sorted(sources):
        rows, _, refused = parse_file(path, sources[path])
        problems += refused
        for row, _, location in rows:
            if row["id"] in where:
                problems.append("%s: duplicate scenario identifier %s, also declared at %s" % (location, row["id"], where[row["id"]]))
                continue
            where[row["id"]] = location
            found.append(row)
    return sorted(found, key=planning_key), problems


def coverage(sources):
    """[(owner, covered path, location)] of {path: text} test sources, in
    file and line order. Malformed lines are refused by discover()."""
    return [entry for path in sorted(sources) for entry in parse_file(path, sources[path])[1]]


def planning_key(row):
    """The rows that predate the declarations keep their recorded order
    (scenario_order.RECORDED); every later row follows in identifier order."""
    rank = RECORDED_RANK.get(row["id"])
    return (0, rank, "") if rank is not None else (1, 0, row["id"])


def tree_sources(root):
    """{path: text} of the declaring test files of a working tree."""
    root = Path(root)
    return {path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
            for directory, _, _ in SUITES for path in sorted((root / directory).glob("*_test.go"))}


def stray_declarations(root):
    """Problems for a declaration in a Go file outside the declaring test
    files of a working tree, which discovery would otherwise never read."""
    root = Path(root)
    problems = []
    for path in sorted(root.rglob("*.go")):
        relative = path.relative_to(root)
        if ".git" in relative.parts or suite_of(relative) is not None:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            continue
        for number, _ in declaration_lines(text):
            problems.append("%s:%d: a scenario declaration outside the native suites' test files"
                            % (relative.as_posix(), number))
    return problems
