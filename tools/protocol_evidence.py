#!/usr/bin/env python3
"""Check frame/message/role ledger links to tests and retained evidence.

Each native receipt is one file in cephmsgr/receipts/ named by its ID; this
module is the only reader of those files. Table rows follow the order of their
source (wire value, opcode catalog, feature ledger), so a new row has a fixed
place.

This validates references, not the truth of a native observation. A linked
unit test must be declared as a top-level test in the linked file and listed by
Go for that file's package (go_test_list.declares: `go test -list` gives the
runnable set and Go's parser the declaring file; listing compiles the test
binaries and executes no test body). No producer tools are executed by this
inventory check.
"""
import csv
import re
import sys
from pathlib import Path

import go_test_list

ROOT = Path(__file__).resolve().parent.parent
EVIDENCE = Path("cephmsgr/PROTOCOL_EVIDENCE.md")
RECEIPTS = Path("cephmsgr/receipts")
LINK = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
CATEGORIES = {"exact-commit", "file-attributed", "historical", "unjoined"}
RECEIPT_FIELDS = ("Attribution", "Boundary", "Record")
# Receipt IDs that predate one-file-per-receipt. Other documents cite them, so
# they keep their names; no receipt joins this set. A later receipt takes a
# descriptive name (RECEIPT_NAME), never the next number.
NUMBERED_RECEIPTS = frozenset({"U"} | {"R%d" % number for number in range(1, 41) if number != 36})
RECEIPT_NAME = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")


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


def receipt_texts(root):
    """Text of every receipt file, by ID (the file name without .md)."""
    directory = Path(root) / RECEIPTS
    if not directory.is_dir():
        raise ValueError("missing receipt directory: " + str(RECEIPTS))
    return {path.stem: path.read_text() for path in sorted(directory.glob("*.md"))}


def parse_receipt(name, text, problems):
    """[ID, attribution, boundary, record] of one receipt file, or None.

    The file is a `# Native receipt <ID>` heading and one line per field in
    RECEIPT_FIELDS, each written once as `Field: value`."""
    prefix = "receipt " + name + ": "
    if name not in NUMBERED_RECEIPTS:
        if re.fullmatch(r"R[0-9]+", name):
            problems.append(prefix + "numbered IDs are frozen; name a new receipt instead")
            return None
        if not RECEIPT_NAME.fullmatch(name):
            problems.append(prefix + "ID must be lowercase words joined by hyphens")
            return None
    lines = text.splitlines()
    if not lines or lines[0] != "# Native receipt " + name:
        problems.append(prefix + "first line must be '# Native receipt " + name + "'")
        return None
    fields = {}
    for line in lines[1:]:
        field, separator, value = line.partition(": ")
        if separator and field in RECEIPT_FIELDS:
            if field in fields:
                problems.append(prefix + "duplicate " + field)
            fields[field] = value.strip()
        elif line.strip():
            problems.append(prefix + "unexpected line " + line[:40])
    missing = [field for field in RECEIPT_FIELDS if not fields.get(field)]
    if missing:
        problems.append(prefix + "missing " + ", ".join(missing))
        return None
    return [name] + [fields[field] for field in RECEIPT_FIELDS]


def receipt_rows(texts, problems):
    rows = {}
    for name, text in texts.items():
        row = parse_receipt(name, text, problems)
        if row is not None:
            rows[name] = row
    return rows


def order_problems(label, rows, rank, what):
    """Rows must appear in the order `rank` gives their keys, so a new row has
    one place and never goes at the end of the table by default."""
    keys = [key for key in rows if key in rank]
    for before, after in zip(keys, keys[1:]):
        if rank[before] > rank[after]:
            return [label + ": row " + after + " is out of " + what + " order (after " + before + ")"]
    return []


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
    """Feature ledger profile names in ledger order."""
    result = {}
    for line in (root / "internal/session/FEATURE_LEDGER.md").read_text().splitlines():
        cells = [cell.strip() for cell in line.split("|")]
        if len(cells) >= 7 and re.fullmatch(r"(?:adds )?`0x[0-9a-f]{16}`", cells[2]):
            result.setdefault(cells[1], len(result))
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


def module_dir(root, path):
    """The directory of the go.mod nearest to `path` inside root, or None.

    A nested module (integration/fixture, tools/quality) lists its own
    packages; the root module's listing does not contain them."""
    root, directory = Path(root).resolve(), Path(path).resolve().parent
    while directory.is_relative_to(root):
        if (directory / "go.mod").is_file():
            return directory
        if directory == root:
            break
        directory = directory.parent
    return None


def listed_test(root, path, name):
    """True when `path` declares `name` as a top-level test that Go lists for
    the package compiling `path` in the module containing it (see
    go_test_list.declares)."""
    module = module_dir(root, path)
    return module is not None and go_test_list.declares(module, path, name)


def linked_test_directories(root, rows):
    """Directories of every linked _test.go file, so that one parser run
    covers them all."""
    directories = set()
    for row in rows:
        for _, target in LINK.findall(row[2]):
            try:
                path = local_file(root, EVIDENCE, target)
            except ValueError:
                continue
            if path.name.endswith("_test.go"):
                directories.add(path.parent)
    return sorted(directories)


def local_file(root, document, target):
    # Only repository-local file links are evidence inputs. External run URLs
    # can occur in prose, but cannot replace a checked-in receipt or test.
    path = (root / document.parent / target.split("#", 1)[0]).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError("missing or non-repository file: " + target)
    return path


def numeric_rank(keys):
    return {key: int(key, 0) for key in keys}


def check(root=ROOT, text=None, receipts=None):
    """Problems in the ledger. `text` replaces the document and `receipts`
    (ID -> file text) the receipt directory, so tests can check variants."""
    root = Path(root)
    if text is None:
        text = (root / EVIDENCE).read_text()
    if receipts is None:
        receipts = receipt_texts(root)
    problems = []
    messages = indexed(table(text, "Message evidence"), "messages", problems)
    frames = indexed(table(text, "Frame evidence"), "frames", problems)
    opcodes = indexed(table(text, "Implemented opcode evidence"), "opcodes", problems)
    features = indexed(table(text, "Feature evidence"), "features", problems)
    for name in sorted(NUMBERED_RECEIPTS - set(receipts)):
        problems.append("receipt " + name + ": cited numbered ID has no file")
    receipts = receipt_rows(receipts, problems)
    expected = message_rows(root)
    catalog = opcode_rows(root)
    implemented = {name: index for index, name in enumerate(catalog) if catalog[name]["status"] == "implemented"}
    profiles = feature_rows(root)
    frame_keys = frame_rows(root)
    for label, rows, wanted, what in (("messages", messages, numeric_rank(expected), "message type"),
                                      ("frames", frames, numeric_rank(frame_keys), "frame tag"),
                                      ("opcodes", opcodes, implemented, "opcode catalog"),
                                      ("features", features, profiles, "feature ledger")):
        for key in sorted(set(wanted) - set(rows)):
            problems.append(label + ": missing row " + key)
        for key in sorted(set(rows) - set(wanted)):
            problems.append(label + ": undeclared row " + key)
        problems.extend(order_problems(label, rows, wanted, what))
    for name, row in receipts.items():
        if row[1] not in CATEGORIES:
            problems.append("receipt " + name + ": unknown attribution category")
        links = LINK.findall(row[3])
        if row[1] != "unjoined" and not links:
            problems.append("receipt " + name + ": missing local record")
        for _, target in links:
            try:
                path = local_file(root, RECEIPTS / (name + ".md"), target)
                if path.suffix != ".md":
                    problems.append("receipt " + name + ": record must be Markdown")
            except ValueError as error:
                problems.append(str(error))
    go_test_list.declarations(linked_test_directories(
        root, [row for rows in (messages, features, frames, opcodes) for row in rows.values() if len(row) > 2]))
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
                    if not path.name.endswith("_test.go") or not listed_test(root, path, name):
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
