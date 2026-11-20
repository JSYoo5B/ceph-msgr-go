"""Every path a native test covers is a path of SPEC.md's environment scope.

A native test names the paths it gives native evidence for with
`//scenario:covers <path>` lines beside it (scenario_declarations.py). This
check reads the first column of the table under SPEC.md's "Final
test-environment scope" heading, the one reader of that table, and refuses a
declared path that is not one of its keys. It does not require every path to
be covered and records no completeness: open gaps of a path are tracked by
issues.
"""
import re
from pathlib import Path

import scenario_declarations

SPEC = Path("SPEC.md")
HEADING = "## Final test-environment scope"


def spec_paths(text):
    """The keys of the environment scope table of SPEC.md text."""
    match = re.search(r"(?m)^%s\n" % re.escape(HEADING), text)
    if not match:
        raise ValueError("SPEC.md has no %r section" % HEADING)
    section = re.split(r"(?m)^#{2,3} ", text[match.end():], maxsplit=1)[0]
    rows = [line.strip().strip("|").split("|") for line in section.splitlines() if line.startswith("| ")]
    if len(rows) < 3:
        raise ValueError("SPEC.md: the environment scope table has no paths")
    return [row[0].strip() for row in rows[2:]]


def problems(paths, covers):
    known = set(paths)
    return ["%s: %s covers %r, which is not a path of SPEC.md's environment scope table (%s)"
            % (location, owner, path, ", ".join(paths))
            for owner, path, location in covers if path not in known]


def check(context):
    root = Path(context.root)
    try:
        paths = spec_paths((root / SPEC).read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        return [str(error)]
    return problems(paths, scenario_declarations.coverage(scenario_declarations.tree_sources(root)))
