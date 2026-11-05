#!/usr/bin/env python3
"""Conservative documentation skips and advisory native-test recommendations.

Only modifications of explicitly audited ordinary documentation files may skip
checks. Recommendations never filter the native matrix or dispatch tests.
"""
import argparse
import html
import json
import os
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parent.parent
UNREAD_DOCS = frozenset({"README.md", "THIRD_PARTY_NOTICES.md"})
SELF = {"tools/ci_change_plan.py", "tools/test_ci_change_plan.py"}
SHA = re.compile(r"[a-f0-9]{40}\Z")


def git(root, *args):
    return subprocess.check_output(["git", *args], cwd=root, stderr=subprocess.PIPE, timeout=30)


def changes(root, base, head):
    if not SHA.fullmatch(base) or not SHA.fullmatch(head):
        raise ValueError("full base and head commit IDs are required")
    checkout = git(root, "rev-parse", "HEAD").decode().strip()
    if checkout == head:
        # A direct head checkout must contain the event base. Otherwise its
        # diff could hide target-branch changes or come from unrelated history.
        git(root, "merge-base", "--is-ancestor", base, head)
    else:
        parents = git(root, "rev-list", "--parents", "-n", "1", checkout).decode().split()[1:]
        if len(parents) != 2 or parents[1] != head:
            raise ValueError("checkout is neither requested head nor its test merge")
        git(root, "merge-base", "--is-ancestor", base, parents[0])
    raw = git(root, "diff", "--raw", "--no-abbrev", "--no-renames", "-z", base, checkout).split(b"\0")
    if raw[-1] == b"":
        raw.pop()
    if len(raw) % 2:
        raise ValueError("invalid diff records")
    entries = []
    for meta, path in zip(raw[::2], raw[1::2]):
        fields = meta.decode().split()
        if len(fields) != 5 or not fields[0].startswith(":"):
            raise ValueError("invalid diff metadata")
        entries.append({"path": path.decode(), "old_mode": fields[0][1:], "new_mode": fields[1], "status": fields[4]})
    return checkout, entries


def documentation_consumers(root):
    """Conservative guard for new literal references/embeds in tracked tooling.

    This is a regression guard, not a proof against arbitrary dynamic path
    construction; expanding the allowlist still requires source review.
    """
    consumers = set()
    names = git(root, "ls-tree", "-r", "--name-only", "HEAD").decode().splitlines()
    for name in names:
        if name in SELF or Path(name).suffix not in {".go", ".py", ".sh", ".yml", ".yaml"}:
            continue
        data = git(root, "show", "HEAD:" + name).decode("utf-8")
        for doc in UNREAD_DOCS:
            if re.search(r"[\"'`]" + re.escape(doc) + r"[\"'`]", data):
                consumers.add(doc)
        # Root-level Go embed patterns can consume a top-level document.
        if "/" not in name and "//go:embed" in data:
            consumers.update(UNREAD_DOCS)
    return consumers


def docs_only(event, fork, entries, consumers):
    return (event == "pull_request" and not fork and bool(entries)
            and all(e["path"] in UNREAD_DOCS - consumers and e["status"] == "M"
                    and e["old_mode"] == e["new_mode"] == "100644" for e in entries))


def recommend(entries, groups, skip, fallback):
    reasons = {g: set() for g in groups}
    if skip:
        return reasons
    for entry in entries or [{"path": ""}]:
        path = entry["path"]
        if not fallback and path.startswith("internal/mds/"):
            selected = {g for g in groups if g.startswith("mds-")}
            reason = "MDS implementation or test changed"
        elif not fallback and path.startswith("internal/osd/"):
            selected = {g for g in groups if g.startswith(("osd", "mds-")) or g == "rbd"}
            reason = "OSD implementation or test changed; include coupled MDS/RBD I/O"
        else:
            selected = set(groups)
            reason = fallback or "Shared, fixture, contract, tooling or unclassified change: recommend all groups"
        for group in selected:
            reasons[group].add(reason)
    return reasons


def plan(root, event, fork, base, head):
    document = json.loads((root / ".github/scenarios/catalog.json").read_text())
    groups = sorted({row["group"] for row in document["scenarios"]})
    if not groups:
        raise ValueError("catalog has no native groups")
    checkout, entries, fallback = "", [], ""
    if event != "pull_request" or fork:
        fallback = "Non-PR or fork event: run checks and recommend all groups"
    else:
        try:
            checkout, entries = changes(root, base, head)
            if not entries:
                fallback = "Empty diff: run checks and recommend all groups"
        except (ValueError, OSError, subprocess.SubprocessError, UnicodeError):
            fallback = "Unproven comparison: run checks and recommend all groups"
    consumers = documentation_consumers(root) if not fallback else set()
    skip = not fallback and docs_only(event, fork, entries, consumers)
    reasons = recommend(entries, groups, skip, fallback)
    return {"schema": "ci-change-plan/v1", "event": event, "base": base, "head": head,
            "checkout": checkout, "docs_only": skip, "changed": entries,
            "reason": fallback or ("Only audited documentation content changed; checks are not executed" if skip else "Checks run; recommendations are advisory"),
            "documentation_inputs": sorted(consumers),
            "groups": [{"group": group, "recommended": bool(reasons[group]), "reasons": sorted(reasons[group])} for group in groups]}


def cell(value):
    return re.sub(r"[|`*\[\]@\\]", lambda m: "&#%d;" % ord(m[0]), html.escape(str(value))).replace("\n", " ")


def summary(result):
    lines = ["## Change-based CI plan", "", result["reason"], "",
             "This plan does not execute or filter native tests. Recommendations are path heuristics, not a dependency proof.", "",
             "Base `%s`; requested head `%s`; checkout `%s`." % tuple(cell(result[k]) for k in ("base", "head", "checkout")), "",
             "| Native group | Recommended | Reason |", "| --- | --- | --- |"]
    for row in result["groups"]:
        reason = "; ".join(row["reasons"]) or ("Documentation-only policy" if result["docs_only"] else "Outside the path heuristic; not proof of no impact")
        lines.append("| %s | %s | %s |" % (cell(row["group"]), "yes" if row["recommended"] else "no", cell(reason)))
    return "\n".join(lines) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", default="")
    parser.add_argument("--base", default="")
    parser.add_argument("--head", default="")
    parser.add_argument("--fork", choices=("true", "false"), default="true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    # A failure before publishing an explicit true output runs dependent checks.
    result = plan(ROOT, args.event, args.fork == "true", args.base, args.head)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
            stream.write(summary(result))
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
            stream.write("docs_only=%s\n" % str(result["docs_only"]).lower())
    print(result["reason"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
