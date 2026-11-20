#!/usr/bin/env python3
"""Validate the CI request queue before running the pinned actionlint.

GitHub supports concurrency.queue, while actionlint v1.7.7 rejects that key.
Only ci-command.yml's top-level concurrency mapping is adapted. Its three
scalar entries must be explicit, unique and match this repository's queue
policy. The remaining workflow is passed byte-for-byte to actionlint, with
the queue line replaced by an empty line to preserve diagnostic line numbers.
"""
import argparse
from pathlib import Path
import re
import subprocess
import sys

# The Go listings this tool runs: (subcommands that run Go, or None for every
# invocation; the module directories Go loads). tools/test_workflow_toolchains.py
# reads this declaration to require a Go that can list those modules.
# Here: the pinned Go tool runs under the product toolchain.
GO_LISTING = (None, (".",))

ACTIONLINT = "github.com/rhysd/actionlint/cmd/actionlint@v1.7.7"
GROUP = "ci-command-${{ github.event.issue.number || inputs.pull_request || 'main' }}"


def command_queue_source(source):
    lines = source.splitlines(keepends=True)
    starts = [i for i, line in enumerate(lines) if re.match(r"^concurrency\s*:", line)]
    if len(starts) != 1:
        raise ValueError("ci-command needs exactly one explicit top-level concurrency mapping")
    start = starts[0]
    if lines[start].rstrip() != "concurrency:":
        raise ValueError("ci-command concurrency must be a block mapping without anchors")
    entries = {}
    queue_line = None
    for i in range(start + 1, len(lines)):
        line = lines[i]
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if not line.startswith((" ", "\t")):
            break
        match = re.fullmatch(r"  (group|cancel-in-progress|queue): (.+)\n?", line)
        if match is None or match[1] in entries:
            raise ValueError("ci-command concurrency needs unique, two-space-indented scalar entries")
        entries[match[1]] = match[2]
        if match[1] == "queue":
            queue_line = i
    expected = {"group": GROUP, "cancel-in-progress": "false", "queue": "max"}
    if entries != expected:
        raise ValueError("ci-command requires its PR group, cancel-in-progress: false and queue: max")
    # The only unsupported key is independently checked above. Never remove
    # queue entries from jobs, reusable workflows or other workflow files.
    lines[queue_line] = "\n" if lines[queue_line].endswith("\n") else ""
    return "".join(lines)


def lint(root):
    command = ["go", "run", ACTIONLINT, "-shellcheck=", "-pyflakes="]
    result = 0
    for path in sorted((root / ".github/workflows").glob("*")):
        if path.suffix not in (".yml", ".yaml"):
            continue
        source = path.read_text(encoding="utf-8")
        if path.name == "ci-command.yml":
            try:
                source = command_queue_source(source)
            except ValueError as error:
                print(f"{path}: {error}", file=sys.stderr)
                result = 1
                continue
        checked = subprocess.run(command + ["-stdin-filename", str(path), "-"],
                                 input=source, text=True, cwd=root, check=False)
        if checked.returncode:
            result = 1
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    return lint(args.root)


if __name__ == "__main__":
    sys.exit(main())
