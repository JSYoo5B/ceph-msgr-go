#!/usr/bin/env python3
"""Expose assertion and abrupt Ceph harness failures as public annotations."""
import argparse
from collections import deque
import re


def escape(value):
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def annotations(log, exit_code):
    if exit_code == 0:
        return []
    output = []
    diagnostics = deque(maxlen=10)
    last_started = None
    completed_failure = False

    def append_diagnostics():
        for path, number, message in diagnostics:
            output.append(f"::error file={escape(path)},line={number}::{escape(message)}")

    for line in log:
        started = re.match(r"^=== RUN\s+(\S+)", line)
        if started:
            diagnostics.clear()
            last_started = started.group(1)
        match = re.match(r"\s+(\S+_test\.go):(\d+): (.*)", line)
        if match:
            diagnostics.append(match.groups())
        if line.startswith("--- FAIL:"):
            completed_failure = True
            output.append("::error::" + escape(line.strip()))
            append_diagnostics()
    if not completed_failure:
        context = f"Last started Go test: {last_started}." if last_started else "No Go test start was observed."
        output.append(f"::error::Ceph harness exited with code {exit_code} without a completed Go test failure. " + escape(context))
        append_diagnostics()
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log")
    parser.add_argument("exit_code", type=int)
    args = parser.parse_args()
    if not 0 <= args.exit_code <= 255:
        parser.error("exit_code must be between 0 and 255")
    with open(args.log, encoding="utf-8", errors="replace") as log:
        for annotation in annotations(log, args.exit_code):
            print(annotation)


if __name__ == "__main__":
    main()
