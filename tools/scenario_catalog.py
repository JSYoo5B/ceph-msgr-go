#!/usr/bin/env python3
"""The native scenario catalog of a working tree or of a Git revision.

The catalog is not a file. Its rows are the //scenario: declarations beside
the native tests (scenario_declarations.py), in identifier order. Its fixed
checks are the selectable jobs of the reusable platform workflow
(.github/workflows/platform-checks.yml): a job whose `if:` is
`contains(fromJSON(inputs.matrix).fixed, '<job>')` is the check `<job>`.
The composed document is {"scenarios": [...], "fixed_jobs": [...]}.

Every reader of the catalog uses load() or load_revision(). load_revision
reads a revision's declaring test files from its Git objects without
checking it out and executes nothing from that revision, so the trusted
planner can plan a pull request's rows (`scenario_plan.py plan --revision`).
Its fixed checks always come from the calling tree's workflow, which is the
one that runs them.

A tree or revision that still has the catalog files of the earlier layout
(.github/scenarios/catalog.d or .github/scenarios/catalog.json) is refused:
its rows would be read from two places.
"""
import re
import subprocess
from pathlib import Path

import scenario_declarations

ROOT = Path(__file__).resolve().parent.parent
SCENARIOS = Path(".github") / "scenarios"
# The files of the earlier layout, which no tree may carry again.
RETIRED = (SCENARIOS / "catalog.d", SCENARIOS / "catalog.json")
PLATFORM_WORKFLOW = Path(".github") / "workflows" / "platform-checks.yml"
FIXED_JOB = re.compile(r"(?m)^  ([a-z0-9][a-z0-9-]*):\n    if: contains\(fromJSON\(inputs\.matrix\)\.fixed, '([^']*)'\)[ \t]*$")
REVISION = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")


class CatalogError(Exception):
    pass


def fixed_jobs(text):
    """[{"id", "job"}] of the selectable jobs of the platform workflow text,
    in workflow order. A job gated on another job's name is refused."""
    jobs = []
    for job, check in FIXED_JOB.findall(text):
        if check != job:
            raise CatalogError("platform job %s is selected as fixed check %r, not by its own name" % (job, check))
        jobs.append({"id": job, "job": job})
    return jobs


def rows_of(sources):
    rows, problems = scenario_declarations.discover(sources)
    if problems:
        raise CatalogError("\n".join(problems))
    return rows


def retired_problems(root):
    return ["%s: rows are declared beside their tests; remove this catalog file" % path.as_posix()
            for path in RETIRED if (Path(root) / path).exists()]


def load(root=ROOT):
    """The catalog of a working tree."""
    root = Path(root)
    retired = retired_problems(root)
    if retired:
        raise CatalogError("\n".join(retired))
    return {"scenarios": rows_of(scenario_declarations.tree_sources(root)),
            "fixed_jobs": fixed_jobs((root / PLATFORM_WORKFLOW).read_text(encoding="utf-8"))}


def _git(git_dir, *args):
    try:
        return subprocess.run(["git", *args], cwd=git_dir, check=True, capture_output=True).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        raise CatalogError("git %s failed" % args[0]) from error


def _tree(git_dir, revision, path):
    """Return [(mode, type, object, name)] of a tree, or [] if absent."""
    out = _git(git_dir, "ls-tree", "-z", revision, "--", path.as_posix() + "/")
    entries = []
    for record in out.split(b"\0"):
        if not record:
            continue
        meta, name = record.split(b"\t", 1)
        mode, kind, oid = meta.decode().split()
        entries.append((mode, kind, oid, Path(name.decode("utf-8", "replace")).name))
    return entries


def revision_sources(revision, git_dir):
    """{path: text} of the declaring test files of a Git revision, from its
    blobs. The revision must be a full commit SHA of git_dir."""
    if not REVISION.fullmatch(revision or ""):
        raise CatalogError("revision must be a full commit SHA")
    _git(git_dir, "cat-file", "-e", revision + "^{commit}")
    scenarios = {name for _, _, _, name in _tree(git_dir, revision, SCENARIOS)}
    retired = [path.as_posix() for path in RETIRED if path.name in scenarios]
    if retired:
        raise CatalogError("the tested revision has %s from before rows were declared beside their tests;"
                           " merge the default branch" % " and ".join(retired))
    sources = {}
    for directory, _, _ in scenario_declarations.SUITES:
        for mode, kind, oid, name in _tree(git_dir, revision, Path(directory)):
            path = directory + "/" + name
            if kind == "blob" and mode in ("100644", "100755") and scenario_declarations.suite_of(path):
                sources[path] = _git(git_dir, "cat-file", "blob", oid).decode("utf-8", "replace")
    return sources


def load_revision(revision, git_dir, root=ROOT):
    """The catalog of a Git revision of git_dir: its declared rows, and the
    fixed checks of the calling tree root."""
    return {"scenarios": rows_of(revision_sources(revision, git_dir)),
            "fixed_jobs": fixed_jobs((Path(root) / PLATFORM_WORKFLOW).read_text(encoding="utf-8"))}
