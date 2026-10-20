#!/usr/bin/env python3
"""Publish bounded failed-test assertions from a go test -json event stream."""
import argparse
from collections import deque
from ipaddress import IPv6Address
import json
import ntpath
import os
from pathlib import Path, PureWindowsPath
import re
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
EVENT_LIMIT = 64 * 1024
LINE_LIMIT = 8192
MESSAGE_LIMIT = 4096
PACKAGE_LIMIT = 64
TEST_LIMIT = 256
DIAGNOSTIC_LIMIT = 10
ANNOTATION_LIMIT = 100
TRUNCATED = "\n[diagnostic truncated]"


def escape(value):
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def escape_property(value):
    return escape(value).replace(",", "%2C").replace(":", "%3A")


def bounded_message(value):
    # Bound the emitted UTF-8 bytes, including workflow-command escaping.
    chunks, size = [], 0
    marker = escape(TRUNCATED)
    for char in value:
        chunk = escape(char)
        width = len(chunk.encode("utf-8"))
        if size + width > MESSAGE_LIMIT - len(marker):
            return "".join(chunks) + marker
        chunks.append(chunk)
        size += width
    return "".join(chunks)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate event key")
        result[key] = value
    return result


def bounded_integer(value):
    if len(value.lstrip("-")) > 20:
        raise ValueError("oversized event integer")
    return int(value)


def reject_constant(_):
    raise ValueError("nonfinite event number")


def event_from_line(line):
    if not isinstance(line, str) or len(line) > EVENT_LIMIT:
        return None
    try:
        if len(line.encode("utf-8")) > EVENT_LIMIT:
            return None
        event = json.loads(line, object_pairs_hook=unique_object,
                           parse_int=bounded_integer, parse_constant=reject_constant)
    except (ValueError, UnicodeError, RecursionError):
        return None
    if not isinstance(event, dict):
        return None
    action = event.get("Action")
    if action not in ("start", "run", "pause", "cont", "output", "pass", "skip", "fail", "build-fail", "build-output"):
        return None
    package = event.get("ImportPath") if action.startswith("build-") else event.get("Package")
    if action.startswith("build-") and isinstance(package, str):
        # go test also emits build events for a package's test variant.
        match = re.fullmatch(r"(.+) \[(.+)\.test\]", package)
        if match and match.group(1) == match.group(2):
            package = match.group(1)
    if not isinstance(package, str) or len(package) > 512 or not re.fullmatch(r"[A-Za-z0-9_./+\-]+", package):
        return None
    if any(part in ("", ".", "..") for part in package.split("/")):
        return None
    test = event.get("Test", "")
    if not isinstance(test, str) or len(test) > 256 or any(not char.isprintable() for char in test):
        return None
    output = event.get("Output", "")
    if not isinstance(output, str):
        return None
    try:
        output.encode("utf-8")
        test.encode("utf-8")
    except UnicodeError:
        return None
    return action, package, test, output


def source_location(filename, repo_root):
    if any(not char.isprintable() for char in filename):
        return None
    # Package names cannot identify helpers imported from other packages.
    # Only a full Go -fullpath location within the supplied repository can
    # establish a file property; basename-only logs retain a text location.
    windows, root_windows = PureWindowsPath(filename), PureWindowsPath(str(repo_root))
    try:
        if windows.is_absolute():
            if not root_windows.is_absolute():
                return None
            if os.name == "nt":
                relative = Path(filename).resolve().relative_to(Path(repo_root).resolve()).as_posix()
            else:
                # Permit reading a Windows log off-host with its explicit
                # Windows root. Native Windows also resolves symlinks above.
                relative = PureWindowsPath(ntpath.normpath(filename)).relative_to(
                    PureWindowsPath(ntpath.normpath(str(repo_root)))).as_posix()
        elif Path(filename).is_absolute():
            if root_windows.is_absolute():
                return None
            relative = Path(filename).resolve().relative_to(Path(repo_root).resolve()).as_posix()
        elif "/" not in filename and "\\" not in filename:
            return None, filename
        else:
            return None
    except (ValueError, OSError, RuntimeError):
        return None
    if len(relative.encode("utf-8")) > 1024:
        return None
    return relative, relative


def continuation_line(value):
    if not value.startswith("        ") or not value[8:].strip():
        return False
    text = value[8:].lstrip()
    if text.startswith("{"):
        return False
    if text.startswith("["):
        # Keep errors.Join causes with IPv6 endpoints, but do not publish
        # arbitrary JSON arrays printed after an assertion.
        match = re.match(r"^\[([^\]]{1,128})\]:([0-9]{1,5})(?:/[0-9]{1,10})?(?::|\s|$)", text)
        if not match or int(match.group(2)) > 65535:
            return False
        try:
            IPv6Address(match.group(1))
        except ValueError:
            return False
    return True


class TestOutput:
    def __init__(self, repo_root):
        self.repo_root = repo_root
        self.diagnostics = deque(maxlen=DIAGNOSTIC_LIMIT)
        self.partial = ""
        self.line_truncated = False
        self.continuation = False
        self.omitted = False

    def line(self, value, truncated=False):
        value = value.rstrip("\n")
        match = re.match(r"^ {4,}(.{1,4096}?\.go):([0-9]{1,8}): (.*)$", value)
        if match and int(match.group(2)) > 0:
            filename, number, message = match.groups()
            location = source_location(filename, self.repo_root)
            if location is None:
                self.continuation = False
                return
            if len(self.diagnostics) == DIAGNOSTIC_LIMIT:
                self.omitted = True
            path, label = location
            self.diagnostics.append((path, label, number, message + (TRUNCATED if truncated else "")))
            self.continuation = not truncated
        elif self.continuation and continuation_line(value):
            path, label, number, message = self.diagnostics[-1]
            message += "\n" + value[8:]
            if len(message) > LINE_LIMIT or truncated:
                message = message[:LINE_LIMIT] + TRUNCATED
                self.continuation = False
            self.diagnostics[-1] = (path, label, number, message)
        else:
            # Raw stdout, JSON dumps, panic text and stack traces are not
            # assertion diagnostics and must not become continuations.
            self.continuation = False

    def feed(self, output):
        # test2json splits long output lines across events. Reassemble only
        # within the exact Package/Test key, with bounded partial-line storage.
        for piece in output.splitlines(keepends=True):
            remaining = LINE_LIMIT - len(self.partial)
            self.partial += piece[:remaining]
            self.line_truncated |= len(piece) > remaining
            if piece.endswith("\n"):
                self.line(self.partial, self.line_truncated)
                self.partial = ""
                self.line_truncated = False

    def finish(self):
        if self.partial:
            self.line(self.partial, self.line_truncated)
        self.partial = ""


def annotations(log, exit_code, repo_root=REPO_ROOT):
    if exit_code == 0:
        return []
    output, packages, tests = [], {}, {}
    omitted = False

    def emit(message, path=None, number=None):
        nonlocal omitted
        if len(output) == ANNOTATION_LIMIT:
            omitted = True
            return
        location = f" file={escape_property(path)},line={number}," if path is not None else " "
        output.append(f"::error{location}title=Go test failure::{bounded_message(message)}")

    def report_test(package, test, state, message):
        emit(message)
        state.finish()
        for path, label, number, diagnostic in state.diagnostics:
            emit(f"{package}: {test}\n{label}:{number}: {diagnostic}", path, number)
        if state.omitted:
            emit(f"{package}: {test}\n[earlier assertion diagnostics omitted]")

    def finish_package(package, failed):
        record = packages[package]
        for key in list(tests):
            if key[0] == package:
                state = tests.pop(key)
                if failed:
                    report_test(package, key[1], state,
                                f"{package}: {key[1]} ended without a terminal test event; go test exited with code {exit_code}.")
        if failed and not record["failed"]:
            emit(f"Go package failed: {package}; no completed failed test event was observed.")
        record["failed"] |= failed
        record["finished"] = True

    for line in log:
        event = event_from_line(line)
        if event is None:
            for state in tests.values():
                state.partial = ""
                state.line_truncated = state.continuation = False
            continue
        action, package, test, text = event
        if action == "build-output":
            # BuildEvents interleave with TestEvents from other packages.
            # Recognize them without publishing compiler/source output or
            # discarding another test's partially received assertion.
            continue
        if package not in packages:
            if len(packages) == PACKAGE_LIMIT:
                omitted = True
                continue
            packages[package] = {"failed": False, "finished": False}
        if not test:
            if action in ("fail", "build-fail", "pass", "skip"):
                finish_package(package, action in ("fail", "build-fail"))
            continue
        key = package, test
        if action in ("run", "output") and key not in tests:
            if len(tests) == TEST_LIMIT:
                omitted = True
                continue
            tests[key] = TestOutput(repo_root)
        if action == "output":
            tests[key].feed(text)
        elif action in ("pass", "skip"):
            tests.pop(key, None)
        elif action == "fail":
            packages[package]["failed"] = True
            report_test(package, test, tests.pop(key, TestOutput(repo_root)), f"Go test failed: {package}: {test}.")
    for (package, test), state in tests.items():
        report_test(package, test, state,
                    f"Go test stream ended with exit code {exit_code}; {package}: {test} had no terminal test event.")
    for package, record in packages.items():
        if not record["finished"] and not record["failed"] and not any(key[0] == package for key in tests):
            emit(f"Go test stream ended with exit code {exit_code} before package completion: {package}.")
    if not output:
        emit(f"go test exited with code {exit_code}; no failed test or package event was observed.")
    if omitted:
        output.append("::error title=Go test failure::[additional test diagnostics omitted]")
    return output


def bounded_lines(stream):
    while True:
        line = stream.readline(EVENT_LIMIT + 1)
        if not line:
            return
        if len(line) > EVENT_LIMIT:
            while line and not line.endswith("\n"):
                line = stream.readline(EVENT_LIMIT + 1)
            # Do not parse the tail of an oversized JSON record as a new event.
            yield ""
            continue
        yield line


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", help="go test -json log, or - for stdin")
    parser.add_argument("exit_code", type=int)
    parser.add_argument("--repo-root", default=str(REPO_ROOT), help="repository root used to validate Go -fullpath locations")
    args = parser.parse_args()
    if not 0 <= args.exit_code <= 255:
        parser.error("exit_code must be between 0 and 255")
    if args.log == "-":
        for annotation in annotations(bounded_lines(sys.stdin), args.exit_code, args.repo_root):
            print(annotation)
    else:
        with open(args.log, encoding="utf-8", errors="replace") as log:
            for annotation in annotations(bounded_lines(log), args.exit_code, args.repo_root):
                print(annotation)


if __name__ == "__main__":
    main()
