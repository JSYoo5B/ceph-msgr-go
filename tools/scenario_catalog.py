#!/usr/bin/env python3
"""Compose the native scenario catalog from its per-group fragments.

The catalog lives in .github/scenarios/catalog.d/:

  _catalog.json   {"version": 1, "fixed_jobs": [...]}
  <group>.json             {"scenarios": [...]}, existing rows of a group
  <group>.<subject>.json   {"scenarios": [...]}, independently added rows

The composed document is {"version", "scenarios", "fixed_jobs"}. Its rows are
the fragments' rows, fragment by fragment in file-name order and in file order
within a fragment. New tests add a subject-named fragment of their group.
A row whose "group" differs from the filename before the first dot, a
duplicate identifier in any two fragments and a file that is neither the
index nor a group fragment
are refused.

Every reader of the catalog uses load(). It also accepts the earlier single
file (.github/scenarios/catalog.json) so the trusted planner can still plan a
pull request opened before the split; that path goes once no such pull
request is open.

  scenario_catalog.py extract --revision SHA --output DIR

writes the catalog of a Git revision of the repository in the current
directory (fragments or the single file) below DIR from Git objects only, without checking the revision out, and prints the path
to pass to `scenario_plan.py --catalog`. It reads data and executes nothing
from that revision.
"""
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCENARIOS = Path(".github") / "scenarios"
CATALOG = SCENARIOS / "catalog.d"  # relative to a repository root
CATALOG_DIR = ROOT / CATALOG
INDEX = "_catalog.json"
LEGACY = "catalog.json"
FRAGMENT = re.compile(r"[a-z0-9][a-z0-9-]{0,63}(?:\.[a-z0-9][a-z0-9-]{0,63})?\.json")
INDEX_KEYS = {"version", "fixed_jobs"}
LEGACY_KEYS = {"version", "scenarios", "fixed_jobs"}


class CatalogError(Exception):
    pass


def _read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as error:
        raise CatalogError("%s: %s" % (Path(path).name, error)) from None


def _object(data, name, allowed):
    if not isinstance(data, dict):
        raise CatalogError("%s is not a JSON object" % name)
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise CatalogError("%s has unknown keys: %s" % (name, ", ".join(unknown)))
    return data


def _version(data, name):
    if data.get("version") != 1:
        raise CatalogError("%s: unsupported catalog version %r" % (name, data.get("version")))


def compose(directory):
    """Return the composed catalog document of a fragment directory."""
    directory = Path(directory)
    if not directory.is_dir():
        raise CatalogError("%s is not a catalog directory" % directory)
    fragments = []
    index = None
    for path in sorted(directory.iterdir(), key=lambda p: p.name):
        if path.is_symlink() or not path.is_file():
            raise CatalogError("unknown catalog entry %s (only regular files)" % path.name)
        if path.name == INDEX:
            index = path
        elif FRAGMENT.fullmatch(path.name):
            fragments.append(path)
        else:
            raise CatalogError("unknown catalog file %s" % path.name)
    if index is None:
        raise CatalogError("catalog directory has no %s" % INDEX)
    head = _object(_read_json(index), INDEX, INDEX_KEYS)
    _version(head, INDEX)
    scenarios, owner = [], {}
    for path in fragments:
        group = path.name.split(".", 1)[0]
        rows = _object(_read_json(path), path.name, {"scenarios"}).get("scenarios")
        if not isinstance(rows, list) or not rows:
            raise CatalogError("%s has no scenarios" % path.name)
        for position, row in enumerate(rows):
            if not isinstance(row, dict):
                raise CatalogError("%s: row %d is not a JSON object" % (path.name, position))
            sid = row.get("id")
            label = sid if isinstance(sid, str) else "row %d" % position
            if row.get("group") != group:
                raise CatalogError("%s: %s belongs to group %r, not %r" % (path.name, label, row.get("group"), group))
            if isinstance(sid, str):
                if sid in owner:
                    raise CatalogError("duplicate identifier %s in %s and %s" % (sid, owner[sid], path.name))
                owner[sid] = path.name
            scenarios.append(row)
    document = {"version": head["version"], "scenarios": scenarios}
    if "fixed_jobs" in head:
        document["fixed_jobs"] = head["fixed_jobs"]
    return document


def load(path=CATALOG_DIR):
    """Return the catalog document of a fragment directory or a single file."""
    path = Path(path)
    if path.is_dir():
        return compose(path)
    data = _object(_read_json(path), path.name, LEGACY_KEYS)
    _version(data, path.name)
    if not isinstance(data.get("scenarios"), list):
        raise CatalogError("%s has no scenario list" % path.name)
    return data


def dump(data):
    return json.dumps(data, indent=1) + "\n"


def split(document):
    """Return {file name: text} of the fragments that compose to document.

    Rows of one group need not be adjacent in document; the composed order is
    by group file name, keeping each group's row order."""
    files = {INDEX: dump({key: document[key] for key in ("version", "fixed_jobs") if key in document})}
    groups = {}
    for row in document["scenarios"]:
        groups.setdefault(row["group"], []).append(row)
    for group, rows in groups.items():
        name = group + ".json"
        if "." in group or not FRAGMENT.fullmatch(name):
            raise CatalogError("group %r cannot name a fragment file" % group)
        files[name] = dump({"scenarios": rows})
    return dict(sorted(files.items()))


def _git(git_dir, *args):
    try:
        return subprocess.run(["git", *args], cwd=git_dir, check=True, capture_output=True).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        raise CatalogError("git %s failed" % args[0]) from error


def _tree(git_dir, revision, path):
    """Return [(mode, type, object, name)] of a tree, or None if absent."""
    out = _git(git_dir, "ls-tree", "-z", revision, "--", path.as_posix() + "/")
    entries = []
    for record in out.split(b"\0"):
        if not record:
            continue
        meta, name = record.split(b"\t", 1)
        mode, kind, oid = meta.decode().split()
        entries.append((mode, kind, oid, Path(name.decode("utf-8", "replace")).name))
    return entries


def extract(revision, output, git_dir=ROOT):
    """Write the catalog of revision below output and return its path."""
    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", revision):
        raise CatalogError("revision must be a full commit SHA")
    _git(git_dir, "cat-file", "-e", revision + "^{commit}")
    entries = {name: (mode, kind, oid) for mode, kind, oid, name in _tree(git_dir, revision, SCENARIOS)}
    fragments, legacy = entries.get("catalog.d"), entries.get(LEGACY)
    if fragments and legacy:
        raise CatalogError("the tested revision has both catalog.d and catalog.json")
    output = Path(output)
    if legacy:
        mode, kind, oid = legacy
        if kind != "blob" or mode not in ("100644", "100755"):
            raise CatalogError("the tested revision's catalog.json is not a regular file")
        output.mkdir(parents=True, exist_ok=True)
        target = output / LEGACY
        target.write_bytes(_git(git_dir, "cat-file", "blob", oid))
        return target
    if not fragments:
        raise CatalogError("the tested revision has no scenario catalog")
    if fragments[1] != "tree":
        raise CatalogError("the tested revision's catalog.d is not a directory")
    target = output / "catalog.d"
    target.mkdir(parents=True, exist_ok=False)
    for mode, kind, oid, name in _tree(git_dir, revision, SCENARIOS / "catalog.d"):
        if name != INDEX and not FRAGMENT.fullmatch(name):
            raise CatalogError("unknown catalog file %s" % name)
        if kind != "blob" or mode not in ("100644", "100755"):
            raise CatalogError("catalog entry %s is not a regular file" % name)
        (target / name).write_bytes(_git(git_dir, "cat-file", "blob", oid))
    return target


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    pull = sub.add_parser("extract")
    pull.add_argument("--revision", required=True)
    pull.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        print(extract(args.revision, args.output, git_dir=Path.cwd()))
    except CatalogError as error:
        print("scenario_catalog: %s" % error, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
