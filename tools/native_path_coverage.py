#!/usr/bin/env python3
"""Check the native path coverage map against the catalog, tests and records.

The map is the directory integration/fixture/coverage: an index (README.md)
whose "Area files" section links one file per area, <PREFIX>.md, in the
order the rows compose, with each file's prefix and title (its SPEC.md path)
in one table line. Every row is in the Rows section of the area file whose
prefix its ID carries. The check refuses an entry of the directory that is
neither the index nor a listed area file (a directory, a symbolic link or
non-UTF-8 data included), a listed area file that is missing or disagrees
with its table line, a row in the index or outside a Rows section, a row in
the wrong area file and a row ID used in two places. Nothing counts or lists the rows, so a row added to one area edits
that area file only.

This validates references, not the truth of a native observation. Top-level
Go test names are the tests Go lists in the product, fixture and quality
modules (go_test_list.py): listing compiles the test binaries, which may fetch
uncached modules, and executes no test body. A link whose text is a test name
and whose target is a Go file must point at the file that declares it
(go_test_list.declares, Go's parser under the build context). It starts no fixture. Upstream fixture
receipts (``ctg@v0.3.0:``, ``ctg@v0.4.0:`` and ``ctg@main:`` tokens) are cited by name only and
are not resolved, because the provider repository is not part of this tree.
"""
import re
import sys
from pathlib import Path

import go_test_list
import scenario_catalog

ROOT = Path(__file__).resolve().parent.parent
COVERAGE = Path("integration") / "fixture" / "coverage"  # relative to a repository root
INDEX = "README.md"
AREA_FILE = re.compile(r"([A-Z][A-Z0-9]*)\.md")
ROW_ID = re.compile(r"([A-Z][A-Z0-9]*)-[1-9][0-9]*")
SPEC = Path("SPEC.md")

CLASSES = ("released", "main-only", "primitive", "proposed", "absent")
HEADER = ["ID", "Path", "Class", "Setup and control", "Independent observation",
          "Fault and cleanup", "Evidence", "Visibility"]
LINK = re.compile(r"\[([^\]]+)\]\(([^)\s]+)\)")
CODE = re.compile(r"`([^`]+)`")
UPSTREAM = re.compile(r"^ctg@(v0\.3\.0|v0\.4\.0|main):(Test\w+(?:/[\w./-]+)?)$")
SCENARIO = re.compile(r"^[a-z][a-z0-9-]*\.(?:aes|aes256k)\b[a-z0-9.-]*$")


# The Go modules whose tests the map may name: the product, the fixture and the
# quality tooling. The development-only capture modules under tools/msgrcap
# are not native evidence.
MODULES = (".", "integration/fixture", "tools/quality")


def go_tests(root, modules=MODULES):
    """Return the name of every top-level Go test that Go lists in the
    repository's modules. A coverage token names a test; how often Go lists
    the name plays no part."""
    names = set()
    for module in modules:
        for listed in go_test_list.list_tests(Path(root) / module, ("./...",)).values():
            names.update(listed)
    return names


def module_of(root, path):
    """The listed module directory (relative to root) that contains path."""
    best = None
    for module in MODULES:
        base = (Path(root) / module).resolve()
        if path.is_relative_to(base) and (best is None or len(base.parts) > len(best[1].parts)):
            best = (module, base)
    return None if best is None else best[1]


def test_link_problems(root, name, path):
    """A test-named link to a Go file must reach the file declaring the test."""
    module = module_of(root, path)
    if module is None or not go_test_list.declares(module, path, name):
        return ["link %s does not reach the file declaring top-level Go test %s"
                % (path.relative_to(Path(root).resolve()).as_posix(), name)]
    return []


def catalog_vocabulary(root):
    return catalog_from(scenario_catalog.load(root / scenario_catalog.CATALOG))


def spec_paths(root):
    """Read the first column of SPEC.md's final environment scope table."""
    text = (root / SPEC).read_text()
    match = re.search(r"(?m)^## Final test-environment scope\n", text)
    if not match:
        raise ValueError("SPEC.md: missing final test-environment scope")
    section = re.split(r"(?m)^#{2,3} ", text[match.end():], maxsplit=1)[0]
    rows = [line.strip().strip("|").split("|") for line in section.splitlines()
            if line.startswith("| ")]
    if len(rows) < 3:
        raise ValueError("SPEC.md: empty final test-environment scope table")
    return [row[0].strip() for row in rows[2:]]


def split_row(line):
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def table_rows(text):
    """Yield the cells of every row of every coverage table of one file."""
    in_table = False
    for line in text.splitlines():
        if not line.startswith("|"):
            in_table = False
            continue
        cells = split_row(line)
        if cells == HEADER:
            in_table = True
            continue
        if in_table and not set("".join(cells)) <= set("-: "):
            yield cells


def read_documents(root=ROOT):
    """Return {entry name: text} for every entry of the coverage directory.

    An entry that is not a regular UTF-8 text file (a directory, a symbolic
    link or binary data such as a desktop's folder metadata) maps to None, so
    the check refuses it instead of failing to read it."""
    documents = {}
    for path in sorted((Path(root) / COVERAGE).iterdir(), key=lambda p: p.name):
        documents[path.name] = None
        if not path.is_symlink() and path.is_file():
            try:
                documents[path.name] = path.read_text(encoding="utf-8")
            except UnicodeError:
                pass
    return documents


def section(text, heading):
    """The body of the ## section with this heading, or None."""
    match = re.search(r"(?m)^## " + re.escape(heading) + r"\n", text)
    if not match:
        return None
    return re.split(r"(?m)^## ", text[match.end():], maxsplit=1)[0]


def area_table(index):
    """{area file: (prefix, path)} from the Prefix | Path | Area file table
    of the index's Area files section."""
    table = {}
    for line in (section(index, "Area files") or "").splitlines():
        cells = split_row(line) if line.startswith("|") else []
        links = LINK.findall(cells[2]) if len(cells) == 3 else []
        if len(links) == 1:
            table[links[0][1]] = (cells[0].strip("`"), cells[1])
    return table


def area_files(index):
    """Return the area files linked from the index's Area files section, in
    order, or None when the section is missing. Only a plain link to a
    Markdown file of the same directory names an area file."""
    section_text = section(index, "Area files")
    if section_text is None:
        return None
    names = []
    for _, target in LINK.findall(section_text):
        if re.fullmatch(r"[^/\\#:]+\.md", target) and target not in names:
            names.append(target)
    return names


def title(text):
    first = text.splitlines()[0] if text else ""
    return first[2:].strip() if first.startswith("# ") else None


def where(name):
    return (COVERAGE / name).as_posix()


def compose(documents):
    """Return (areas, problems) for a coverage directory's documents.

    areas is a list of (file name, area title, rows) in the index's order;
    rows are the cells of the file's coverage rows in file order."""
    index = documents.get(INDEX)
    if index is None:
        return [], ["missing index " + where(INDEX)]
    listed = area_files(index)
    if listed is None:
        return [], [where(INDEX) + ": missing Area files section"]
    problems = []
    if not listed:
        problems.append(where(INDEX) + ": lists no area file")
    for name in sorted(documents):
        if documents[name] is None:
            problems.append("not a regular UTF-8 text file " + where(name))
        elif name != INDEX and name not in listed:
            problems.append("unlisted file " + where(name))
    for cells in table_rows(index):
        problems.append("row %s: in the index %s, not in an area file" % (cells[0], where(INDEX)))
    table = area_table(index)
    areas = []
    for name in listed:
        if not AREA_FILE.fullmatch(name):
            problems.append("area file %s: name is not <PREFIX>.md" % where(name))
            continue
        if name not in documents:
            problems.append("missing area file " + where(name))
            continue
        text = documents[name]
        if text is None:
            continue
        label = title(text)
        if label is None:
            problems.append("area file %s: first line is not a # title" % where(name))
            continue
        prefix, path = table.get(name, (None, None))
        if path is None:
            problems.append("area file %s: no Prefix | Path | Area file line in the index" % where(name))
        elif prefix != name[:-3] or path != label:
            problems.append("area file %s: title %s and prefix %s differ from the index line %s | %s"
                            % (where(name), label, name[:-3], prefix, path))
        rows = section(text, "Rows")
        if rows is None:
            problems.append("area file %s: missing Rows section" % where(name))
            rows = ""
        inside = list(table_rows(rows))
        for cells in table_rows(text.replace(rows, "", 1) if rows else text):
            problems.append("row %s: outside the Rows section of %s" % (cells[0], where(name)))
        areas.append((name, label, inside))
    return areas, problems


def class_counts(documents):
    """Count the coverage rows of each known class; the map keeps no totals."""
    counts = dict.fromkeys(CLASSES, 0)
    for _, _, rows in compose(documents)[0]:
        for cells in rows:
            klass = cells[2].strip("`") if len(cells) == len(HEADER) else None
            if klass in counts:
                counts[klass] += 1
    return counts


def placeholder(token):
    """True for prose that names a reference form rather than a reference."""
    prefix = re.match(r"^(?:ctg@(?:v0\.3\.0|v0\.4\.0|main)?:?|group:|suite:)", token)
    return "<" in token or (prefix is not None and prefix.end() == len(token))


def token_problems(token, tests, scenarios, groups, suites):
    if placeholder(token):
        return []
    if token.startswith("ctg@"):
        return [] if UPSTREAM.match(token) else ["malformed upstream receipt " + token]
    if token.startswith("group:"):
        return [] if token[6:] in groups else ["unknown catalog group " + token]
    if token.startswith("suite:"):
        return [] if token[6:] in suites else ["unknown check suite " + token]
    if token.startswith("^Test") and token.endswith("$"):
        try:
            pattern = re.compile(token)
        except re.error:
            return ["invalid test selector " + token]
        if not any(pattern.search(name) for name in tests):
            return ["selector matches no Go test " + token]
        return []
    if re.fullmatch(r"Test\w+(?:/[\w./-]+)?", token):
        name = token.split("/", 1)[0]
        return [] if name in tests else ["missing top-level Go test " + name]
    # Every scenario-shaped token is checked, so a mistyped or removed group
    # cannot hide a reference behind an unrecognized prefix.
    if SCENARIO.match(token):
        return [] if token in scenarios else ["unknown scenario " + token]
    return []


def catalog_from(data):
    """Return the (scenarios, groups, suites) vocabulary of catalog data."""
    scenarios = {row["id"] for row in data["scenarios"]}
    groups = {row["group"] for row in data["scenarios"]}
    suites = {row["id"] for row in data.get("fixed_jobs", [])}
    return scenarios, groups, suites


def check(root=ROOT, documents=None, tests=None, catalog=None):
    root = Path(root)
    if documents is None:
        documents = read_documents(root)
    if tests is None:
        tests = go_tests(root)
    if catalog is None:
        catalog = catalog_vocabulary(root)
    scenarios, groups, suites = catalog
    areas, problems = compose(documents)

    directory = root / COVERAGE
    for name, text in sorted(documents.items()):
        if text is None:
            continue
        for token in sorted(set(CODE.findall(text))):
            problems.extend(where(name) + ": " + problem
                            for problem in token_problems(token, tests, scenarios, groups, suites))
        for label, target in LINK.findall(text):
            if re.match(r"^[a-z]+:", target) or target.startswith("#"):
                continue
            path = (directory / target.split("#", 1)[0]).resolve()
            if not path.is_relative_to(root.resolve()) or not path.exists():
                problems.append(where(name) + ": missing or non-repository file " + target)
            elif path.suffix == ".go" and re.fullmatch(r"Test\w*", label.strip("`")):
                problems.extend(where(name) + ": " + problem
                                for problem in test_link_problems(root, label.strip("`"), path))

    labels = set()
    seen = {}
    for name, label, rows in areas:
        prefix = AREA_FILE.fullmatch(name).group(1)
        for cells in rows:
            if len(cells) != len(HEADER) or not all(cells):
                problems.append("row %s: expected %d nonempty columns" % (cells[0], len(HEADER)))
                continue
            key, klass = cells[0], cells[2].strip("`")
            match = ROW_ID.fullmatch(key)
            if match is None or match.group(1) != prefix:
                problems.append("row %s: in %s, whose rows are %s-<number>" % (key, where(name), prefix))
            if key in seen:
                problems.append("duplicate row %s: in %s and %s" % (key, where(seen[key]), where(name)))
            else:
                seen[key] = name
            labels.add(label)
            if klass not in CLASSES:
                problems.append("row " + key + ": unknown class " + cells[2])
                continue
            if klass == "released" and not LINK.search(cells[6]) and not CODE.search(cells[6]):
                problems.append("row " + key + ": released row needs local evidence")
    if not seen:
        problems.append("no coverage rows")

    for label in spec_paths(root):
        if label not in labels:
            problems.append("SPEC path without an area file with rows: " + label)
    return problems


def main():
    try:
        problems = check()
    except (OSError, ValueError, KeyError) as error:
        problems = [str(error)]
    if problems:
        for problem in problems:
            print("native_path_coverage: " + problem, file=sys.stderr)
        return 1
    counts = class_counts(read_documents())
    print("native path coverage references valid; native outcomes require record review")
    print("rows per class: " + ", ".join("%s %d" % (klass, counts[klass]) for klass in CLASSES))
    return 0


if __name__ == "__main__":
    sys.exit(main())
