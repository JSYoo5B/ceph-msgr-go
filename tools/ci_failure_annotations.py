#!/usr/bin/env python3
"""Expose failed Go test diagnostics as public GitHub check annotations."""
import re
import sys


def escape(value):
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


diagnostics = []
with open(sys.argv[1], encoding="utf-8", errors="replace") as log:
    for line in log:
        if line.startswith("=== RUN"):
            diagnostics = []
        match = re.match(r"\s+(\S+_test\.go):(\d+): (.*)", line)
        if match:
            diagnostics.append(match.groups())
        if line.startswith("--- FAIL:"):
            print("::error::" + escape(line.strip()))
            for path, number, message in diagnostics[-10:]:
                print(f"::error file={escape(path)},line={number}::{escape(message)}")
