#!/usr/bin/env python3
"""Verify the compiled fuzz inventory and run every registered target, bounded.

Listing is Linux/default-build selection evidence, not fuzz execution. Inventory
changes require an explicit review; newly compiled targets may not silently miss
periodic and requested fuzz runs. Native fixtures are not started.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
INVENTORY = Path(".github/scenarios/fuzz-targets.json")
TEST = re.compile(r"Fuzz[A-Za-z0-9_]+\Z")
PACKAGE = re.compile(r"[A-Za-z0-9_]+(?:/[A-Za-z0-9_]+)*\Z")


def inventory(root):
    data = json.loads((root / INVENTORY).read_text())
    if data.get("version") != 1 or not data.get("targets"):
        raise ValueError("empty or unsupported fuzz inventory")
    targets = []
    for row in data["targets"]:
        package, test = row["package"], row["test"]
        if not PACKAGE.fullmatch(package) or not TEST.fullmatch(test):
            raise ValueError("invalid fuzz package or test")
        targets.append((package, test))
    if targets != sorted(set(targets)):
        raise ValueError("fuzz inventory must be sorted and unique")
    return targets


def compiled_events(text, module):
    targets = []
    for line in text.splitlines():
        event = json.loads(line)
        name = event.get("Output", "").strip()
        if event.get("Action") != "output" or not TEST.fullmatch(name):
            continue
        package = event.get("Package", "")
        if not package.startswith(module + "/"):
            raise ValueError("fuzz target outside root module")
        targets.append((package[len(module) + 1:], name))
    if not targets or len(set(targets)) != len(targets):
        raise ValueError("empty or duplicate compiled fuzz inventory")
    return sorted(targets)


def compare(expected, actual):
    problems = []
    for package, name in sorted(set(actual) - set(expected)):
        problems.append("unregistered compiled target: %s/%s" % (package, name))
    for package, name in sorted(set(expected) - set(actual)):
        problems.append("registered target missing from build: %s/%s" % (package, name))
    if problems:
        raise ValueError("; ".join(problems))


def verify(root):
    expected = inventory(root)
    env = dict(os.environ, CGO_ENABLED="0", GOWORK="off")
    module = subprocess.check_output(["go", "list", "-m"], cwd=root, env=env, text=True, timeout=60).strip()
    result = subprocess.run(["go", "test", "-mod=readonly", "-json", "-list", "^Fuzz", "./..."],
                            cwd=root, env=env, text=True, capture_output=True, timeout=300)
    if result.returncode:
        raise ValueError("compiled fuzz discovery failed: " + result.stderr[-2000:])
    compare(expected, compiled_events(result.stdout, module))
    return expected


def completed(text, name):
    """A successful process alone must not turn an empty/skip run into a pass."""
    events = [json.loads(line) for line in text.splitlines() if line.strip()]
    return (sum(e.get("Action") == "run" and e.get("Test") == name for e in events) == 1
            and sum(e.get("Action") == "pass" and e.get("Test") == name for e in events) == 1
            and any(e.get("Action") == "pass" and not e.get("Test") for e in events)
            and not any(e.get("Action") in ("fail", "skip") for e in events))


def run_targets(root, targets, output, fuzztime):
    output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, CGO_ENABLED="0", GOWORK="off")
    rows = []
    try:
        for index, (package, name) in enumerate(targets):
            log = output / ("%02d-%s.jsonl" % (index, name))
            outcome = "failed"
            with log.open("w") as stream:
                try:
                    result = subprocess.run(["go", "test", "-mod=readonly", "-json", "./" + package,
                                             "-run", "^$", "-fuzz", "^" + name + "$", "-fuzztime=" + fuzztime,
                                             "-parallel=2", "-timeout=90s"], cwd=root, env=env,
                                            stdout=stream, stderr=subprocess.STDOUT, timeout=120)
                except subprocess.TimeoutExpired:
                    outcome = "timeout"
                else:
                    stream.flush()
                    try:
                        if result.returncode == 0 and completed(log.read_text(), name):
                            outcome = "passed"
                    except (ValueError, TypeError):
                        pass
            rows.append({"package": package, "test": name, "outcome": outcome, "log": log.name})
            print("%s/%s: %s" % (package, name, outcome), flush=True)
    finally:
        (output / "results.json").write_text(json.dumps({"version": 1, "fuzztime": fuzztime,
                                                        "expected": len(targets), "results": rows}, indent=2) + "\n")
    return 0 if len(rows) == len(targets) and all(row["outcome"] == "passed" for row in rows) else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("verify", "run"))
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--fuzztime", choices=("1s", "20s"), default="20s",
                        help="1s is a smoke run; CI uses 20s per target")
    args = parser.parse_args(argv)
    try:
        targets = verify(args.root)
        print("Verified %d compiled fuzz targets" % len(targets), flush=True)
        if args.command == "run":
            if args.output is None:
                raise ValueError("run requires --output")
            return run_targets(args.root, targets, args.output, args.fuzztime)
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as error:
        print("fuzz inventory: %s" % error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
