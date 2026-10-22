#!/usr/bin/env python3
"""Check the native path coverage map against the catalog, tests and records.

This validates references, not the truth of a native observation. It runs no
tests, starts no fixture and makes no network requests. Upstream fixture
receipts (``ctg@v0.3.0:`` and ``ctg@main:`` tokens) are cited by name only and
are not resolved, because the provider repository is not part of this tree.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COVERAGE = Path("integration/fixture/NATIVE_PATH_COVERAGE.md")
CATALOG = Path(".github/scenarios/catalog.json")
SPEC = Path("SPEC.md")

CLASSES = ("released", "main-only", "primitive", "proposed", "absent")
HEADER = ["ID", "Path", "Class", "Setup and control", "Independent observation",
          "Fault and cleanup", "Evidence", "Visibility"]
LINK = re.compile(r"\[([^\]]+)\]\(([^)\s]+)\)")
CODE = re.compile(r"`([^`]+)`")
TEST = re.compile(r"(?m)^func (Test\w+)\(t \*testing\.T\)")
PR = re.compile(r"#(\d+)\b")
UPSTREAM = re.compile(r"^ctg@(v0\.3\.0|main):(Test\w+(?:/[\w./-]+)?)$")
SCENARIO = re.compile(r"^[a-z][a-z0-9-]*\.(?:aes|aes256k)\b[a-z0-9.-]*$")


def go_tests(root):
    """Return every top-level Go test function name in the repository."""
    names = set()
    for path in root.rglob("*_test.go"):
        if ".git" in path.parts:
            continue
        names.update(TEST.findall(path.read_text(errors="replace")))
    return names


def catalog_vocabulary(root):
    return catalog_from(json.loads((root / CATALOG).read_text()))


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


def coverage_rows(text):
    """Yield (area, cells) for every row of every coverage table."""
    area = None
    in_table = False
    for line in text.splitlines():
        if line.startswith("### "):
            area = line[4:].strip()
            in_table = False
            continue
        if line.startswith("## "):
            area = None
            in_table = False
            continue
        if not line.startswith("|"):
            in_table = False
            continue
        cells = split_row(line)
        if cells == HEADER:
            in_table = True
            continue
        if in_table and not set("".join(cells)) <= set("-: "):
            yield area, cells


def summary_counts(text):
    match = re.search(r"(?m)^## Classification summary\n", text)
    if not match:
        raise ValueError("missing classification summary")
    section = re.split(r"(?m)^## ", text[match.end():], maxsplit=1)[0]
    counts = {}
    for line in section.splitlines():
        cells = split_row(line) if line.startswith("| ") else []
        if len(cells) >= 2 and cells[0].strip("`") in CLASSES:
            counts[cells[0].strip("`")] = int(cells[1])
    return counts


def open_pull_requests(text):
    match = re.search(r"(?m)^## Open pull requests\n", text)
    if not match:
        raise ValueError("missing open pull request section")
    section = re.split(r"(?m)^## ", text[match.end():], maxsplit=1)[0]
    return {number for line in section.splitlines() if line.startswith("| ")
            for number in PR.findall(split_row(line)[0])}


def placeholder(token):
    """True for prose that names a reference form rather than a reference."""
    prefix = re.match(r"^(?:ctg@(?:v0\.3\.0|main)?:?|group:|suite:)", token)
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


def check(root=ROOT, text=None, tests=None, catalog=None):
    root = Path(root)
    if text is None:
        text = (root / COVERAGE).read_text()
    if tests is None:
        tests = go_tests(root)
    if catalog is None:
        catalog = catalog_vocabulary(root)
    scenarios, groups, suites = catalog
    problems = []

    for token in sorted(set(CODE.findall(text))):
        problems.extend(token_problems(token, tests, scenarios, groups, suites))

    document = (root / COVERAGE).parent
    for _, target in LINK.findall(text):
        if re.match(r"^[a-z]+:", target) or target.startswith("#"):
            continue
        path = (document / target.split("#", 1)[0]).resolve()
        if not path.is_relative_to(root.resolve()) or not path.exists():
            problems.append("missing or non-repository file " + target)

    areas = set()
    seen = set()
    counts = dict.fromkeys(CLASSES, 0)
    open_prs = open_pull_requests(text)
    for area, cells in coverage_rows(text):
        if len(cells) != len(HEADER) or not all(cells):
            problems.append("row %s: expected %d nonempty columns" % (cells[0], len(HEADER)))
            continue
        key, klass = cells[0], cells[2].strip("`")
        if area is None:
            problems.append("row " + key + ": outside an area heading")
        areas.add(area)
        if key in seen:
            problems.append("duplicate row " + key)
        seen.add(key)
        if klass not in CLASSES:
            problems.append("row " + key + ": unknown class " + cells[2])
            continue
        counts[klass] += 1
        if klass in ("released", "main-only", "primitive"):
            for number in sorted(set(PR.findall(" ".join(cells))) & open_prs):
                problems.append("row " + key + ": unmerged PR #" + number + " cited as " + klass)
        if klass == "released" and not LINK.search(cells[6]) and not CODE.search(cells[6]):
            problems.append("row " + key + ": released row needs local evidence")
    if not seen:
        problems.append("no coverage rows")

    for label in spec_paths(root):
        if label not in areas:
            problems.append("SPEC path without coverage section: " + label)

    try:
        summary = summary_counts(text)
    except ValueError as error:
        problems.append(str(error))
    else:
        for klass in CLASSES:
            if summary.get(klass) != counts[klass]:
                problems.append("summary %s: table says %s, rows give %d"
                                % (klass, summary.get(klass), counts[klass]))
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
    print("native path coverage references valid; native outcomes require record review")
    return 0


if __name__ == "__main__":
    sys.exit(main())
