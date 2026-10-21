#!/usr/bin/env python3
"""Check frame/message/role ledger links to tests and retained evidence.

This validates references, not the truth of a native observation. No tests,
producer tools or network requests are executed by this inventory check.
"""
import csv
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EVIDENCE = Path("cephmsgr/PROTOCOL_EVIDENCE.md")
LINK = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
TEST = re.compile(r"(?m)^func (Test\w+)\(t \*testing\.T\)")


def table(text, heading):
    """Read one simple Markdown table under an exact level-two heading."""
    match = re.search(r"(?m)^## " + re.escape(heading) + r"\n", text)
    if not match:
        raise ValueError("missing table section: " + heading)
    section = re.split(r"(?m)^## ", text[match.end():], maxsplit=1)[0]
    rows = [line.strip().strip("|").split("|")
            for line in section.splitlines() if line.startswith("| ")]
    if len(rows) < 3:
        raise ValueError("empty table section: " + heading)
    return [[cell.strip() for cell in row] for row in rows[2:]]


def indexed(rows, label, problems):
    result = {}
    for row in rows:
        if len(row) != 4 or not all(row):
            problems.append(label + ": expected four nonempty columns")
            continue
        key = row[0]
        if key in result:
            problems.append(label + ": duplicate row " + key)
        result[key] = row
    return result


def message_rows(root):
    result = {}
    for line in (root / "cephmsgr/MESSAGE_LEDGER.md").read_text().splitlines():
        cells = [cell.strip() for cell in line.split("|")]
        if len(cells) < 7:
            continue
        try:
            number = int(cells[1], 0)
        except ValueError:
            continue
        result[str(number)] = "Defined only" in cells[3]
    return result


def feature_rows(root):
    result = set()
    for line in (root / "internal/session/FEATURE_LEDGER.md").read_text().splitlines():
        cells = [cell.strip() for cell in line.split("|")]
        if len(cells) >= 7 and re.fullmatch(r"(?:adds )?`0x[0-9a-f]{16}`", cells[2]):
            result.add(cells[1])
    return result


def frame_rows(root):
    result = set()
    for line in (root / "internal/session/FRAME_TAG_LEDGER.md").read_text().splitlines():
        cells = [cell.strip() for cell in line.split("|")]
        if len(cells) >= 8 and cells[1].isdigit():
            result.add(str(int(cells[1])))
    return result


def opcode_rows(root):
    with (root / "internal/osd/opcodes-tentacle.csv").open() as source:
        reader = csv.DictReader(source)
        if not {"name", "opcode", "status"}.issubset(reader.fieldnames or []):
            raise ValueError("missing opcode catalog columns")
        rows = list(reader)
    if not rows or any(row["status"] not in {"implemented", "unsupported"} for row in rows):
        raise ValueError("missing or unknown opcode status")
    if len({row["name"] for row in rows}) != len(rows) or len({int(row["opcode"], 0) for row in rows}) != len(rows):
        raise ValueError("duplicate opcode name or value")
    return {row["name"]: row for row in rows}


def frame_receipt_problems(cell, receipts):
    problems = []
    for item in cell.split(", "):
        receipt, separator, observation = item.partition(":")
        if not separator or observation not in {"captured", "reported", "inferred", "unobserved"}:
            problems.append("invalid observation " + item)
            continue
        if receipt not in receipts:
            problems.append("unknown receipt " + receipt)
        elif (observation == "unobserved") != (receipts[receipt][1] == "unjoined"):
            problems.append("observation/attribution mismatch " + item)
    return problems


def local_file(root, document, target):
    # Only repository-local file links are evidence inputs. External run URLs
    # can occur in prose, but cannot replace a checked-in receipt or test.
    path = (root / document.parent / target.split("#", 1)[0]).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError("missing or non-repository file: " + target)
    return path


def check(root=ROOT, text=None):
    root = Path(root)
    if text is None:
        text = (root / EVIDENCE).read_text()
    problems = []
    messages = indexed(table(text, "Message evidence"), "messages", problems)
    frames = indexed(table(text, "Frame evidence"), "frames", problems)
    opcodes = indexed(table(text, "Implemented opcode evidence"), "opcodes", problems)
    features = indexed(table(text, "Feature evidence"), "features", problems)
    receipts = indexed(table(text, "Native receipts"), "receipts", problems)
    expected = message_rows(root)
    catalog = opcode_rows(root)
    for label, actual, wanted in (("messages", set(messages), set(expected)),
                                  ("frames", set(frames), frame_rows(root)),
                                  ("opcodes", set(opcodes), {name for name, row in catalog.items() if row["status"] == "implemented"}),
                                  ("features", set(features), feature_rows(root))):
        for key in sorted(wanted - actual):
            problems.append(label + ": missing row " + key)
        for key in sorted(actual - wanted):
            problems.append(label + ": undeclared row " + key)
    for name, row in receipts.items():
        if row[1] not in {"exact-commit", "file-attributed", "historical", "unjoined"}:
            problems.append("receipt " + name + ": unknown attribution category")
        links = LINK.findall(row[3])
        if row[1] != "unjoined" and not links:
            problems.append("receipt " + name + ": missing local record")
        for _, target in links:
            try:
                path = local_file(root, EVIDENCE, target)
                if path.suffix != ".md":
                    problems.append("receipt " + name + ": record must be Markdown")
            except ValueError as error:
                problems.append(str(error))
    for label, rows in (("message", messages), ("feature", features), ("frame", frames), ("opcode", opcodes)):
        for key, row in rows.items():
            prefix = label + " " + key
            if label == "opcode":
                if key in catalog and "`" + catalog[key]["opcode"] + "`" not in row[1]:
                    problems.append(prefix + ": wrong wire value")
                contracts = LINK.findall(row[1])
                if not contracts:
                    problems.append(prefix + ": missing contract link")
                for _, target in contracts:
                    try:
                        path = local_file(root, EVIDENCE, target)
                        if path.suffix != ".md":
                            problems.append(prefix + ": contract must be Markdown")
                    except ValueError as error:
                        problems.append(str(error))
            defined_only = label == "message" and expected.get(key, False)
            tests = LINK.findall(row[2])
            if defined_only:
                if row[2:] != ["none", "none"] or not row[1].startswith("Not exposed:"):
                    problems.append(prefix + ": defined-only row must refuse support/evidence")
                continue
            if not tests:
                problems.append(prefix + ": missing unit test link")
            for name, target in tests:
                try:
                    path = local_file(root, EVIDENCE, target)
                    if not path.name.endswith("_test.go") or name not in TEST.findall(path.read_text()):
                        problems.append(prefix + ": missing top-level Go test " + name)
                    elif "integration" in path.relative_to(root.resolve()).parts:
                        problems.append(prefix + ": integration selector is not unit evidence")
                except ValueError as error:
                    problems.append(str(error))
            if label == "frame":
                problems.extend(prefix + ": " + problem for problem in frame_receipt_problems(row[3], receipts))
            else:
                for receipt in row[3].split(", "):
                    if receipt not in receipts:
                        problems.append(prefix + ": unknown receipt " + receipt)
    return problems


def main():
    try:
        problems = check()
    except (OSError, ValueError) as error:
        problems = [str(error)]
    if problems:
        for problem in problems:
            print("protocol_evidence: " + problem, file=sys.stderr)
        return 1
    print("protocol evidence references valid; native outcomes require record review")
    return 0


if __name__ == "__main__":
    sys.exit(main())
