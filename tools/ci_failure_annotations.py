#!/usr/bin/env python3
"""Expose bounded Go failures and allowlisted Ceph crash metadata annotations."""
import argparse
from collections import deque
from datetime import datetime
from ipaddress import IPv6Address
import json
from pathlib import PurePosixPath
import re

DIAGNOSTIC_LIMIT = 8192
CRASH_METADATA_LIMIT = 1024 * 1024
CRASH_ANNOTATION_LIMIT = 4096
CRASH_RECORD_LIMIT = 5
CRASH_FRAME_LIMIT = 8
CRASH_FRAME_INPUT_LIMIT = 64
CRASH_SYMBOL_LIMIT = 240
FAILURE_DIAGNOSTICS_MARKER = "Ceph fixture failure diagnostics:"


def bounded_json_depth(value):
    depth, quoted, escaped = 0, False, False
    for char in value:
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in "[{":
            depth += 1
            if depth > 16:
                return False
        elif char in "]}":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0 and not quoted


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate metadata key")
        result[key] = value
    return result


def metadata_integer(value):
    if len(value.lstrip("-")) > 20:
        raise ValueError("oversized metadata integer")
    return int(value)


def reject_metadata_number(_):
    raise ValueError("unsupported metadata number")


def json_diagnostic_line(line):
    value = line.lstrip()
    if value.startswith("{"):
        return True
    if not value.startswith("["):
        return False
    # A joined error may begin with a bracketed IPv6 endpoint. It is not a
    # JSON array and must retain its cause and subsequent continuation lines.
    endpoint = re.match(r"^\[([^\]]{1,128})\]:([0-9]{1,5})(?:/[0-9]{1,10})?(?::|\s|$)", value)
    if endpoint and int(endpoint.group(2)) <= 65535:
        try:
            IPv6Address(endpoint.group(1))
            return False
        except ValueError:
            pass
    return True


def native_frame(frame):
    # Tentacle stores native and arbitrary Python traceback strings under the
    # same key. Publish recognizable native symbols, never Python source text.
    if len(frame) > 4096:
        return None, True
    if not all(32 <= ord(char) <= 126 for char in frame):
        return None, False
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_:]*\(\)", frame):
        symbol = frame
    else:
        match = re.fullmatch(r"\((.+)\+0x[0-9a-fA-F]{1,16}\) *\[0x[0-9a-fA-F]{1,16}\]", frame)
        if match:
            symbol = match.group(1)
            signature = re.sub(r"(?: (?:const|volatile|&{1,2}))+$", "", symbol)
            if not signature.endswith(")") or not re.fullmatch(r"[A-Za-z_(~][A-Za-z0-9_:<>, *&()~.\[\]{}+\-/%=!|^]*", symbol):
                return None, False
        else:
            match = re.fullmatch(r"([A-Za-z0-9_./+\-]+)\(\+0x([0-9a-fA-F]{1,16})\) *\[0x[0-9a-fA-F]{1,16}\]", frame)
            if not match:
                return None, False
            library = PurePosixPath(match.group(1)).name
            if not re.fullmatch(r"(?:lib[A-Za-z0-9_.+\-]+\.so(?:\.[0-9]+)*|ceph-(?:mon|mgr))", library):
                return None, False
            symbol = f"{library}(+0x{match.group(2)})"
    return symbol[:CRASH_SYMBOL_LIMIT], len(symbol) > CRASH_SYMBOL_LIMIT


def crash_summary(line):
    if len(line) > CRASH_METADATA_LIMIT or not bounded_json_depth(line):
        return None
    try:
        if len(line.encode("utf-8")) > CRASH_METADATA_LIMIT:
            return None
        metadata = json.loads(line, object_pairs_hook=unique_object,
                              parse_int=metadata_integer,
                              parse_float=reject_metadata_number,
                              parse_constant=reject_metadata_number)
    except (ValueError, UnicodeError, RecursionError):
        return None
    if not isinstance(metadata, dict) or metadata.get("process_name") not in ("ceph-mon", "ceph-mgr"):
        return None
    process = metadata["process_name"]
    timestamp, crash_id = metadata.get("timestamp"), metadata.get("crash_id")
    if not isinstance(timestamp, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z", timestamp):
        return None
    try:
        datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return None
    if not isinstance(crash_id, str) or not re.fullmatch(re.escape(timestamp) + r"_[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", crash_id):
        return None
    details = [f"Ceph daemon crash metadata recorded; process={process}",
               f"timestamp={timestamp}; crash_id={crash_id}"]
    if "entity_name" in metadata:
        entity = metadata["entity_name"]
        # These are the disposable fixture's daemon names, not host identity.
        pattern = r"mon\.[abc]" if process == "ceph-mon" else r"mgr\.[ab]"
        if not isinstance(entity, str) or not re.fullmatch(pattern, entity):
            return None
        details.append(f"entity={entity}")
    if "ceph_version" in metadata:
        version = metadata["ceph_version"]
        if not isinstance(version, str) or not re.fullmatch(r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}", version):
            return None
        details.append(f"ceph_version={version}")
    source, number = metadata.get("assert_file"), metadata.get("assert_line")
    if isinstance(source, str) and len(source) <= 512 and all(32 <= ord(char) <= 126 for char in source):
        filename = PurePosixPath(source).name
        if re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.\-]{0,79}\.(?:cc|c|h)", filename) and type(number) is int and 0 < number <= 10000000:
            details.append(f"assertion={filename}:{number}")
    if metadata.get("io_error") is True:
        code = metadata.get("io_error_code")
        if type(code) is int and 0 < abs(code) <= 4095:
            details.append(f"io_error=true; code={code}")
    frames = metadata.get("backtrace", [])
    if not isinstance(frames, list) or any(not isinstance(frame, str) for frame in frames):
        return None
    native, omitted, truncated = [], False, len(frames) > CRASH_FRAME_INPUT_LIMIT
    for frame in frames[:CRASH_FRAME_INPUT_LIMIT]:
        symbol, shortened = native_frame(frame)
        truncated |= shortened
        if symbol is None:
            omitted = True
        elif len(native) < CRASH_FRAME_LIMIT:
            native.append(symbol)
        else:
            truncated = True
    if native:
        details.append("native backtrace:\n" + "\n".join(f"  {i+1}. {symbol}" for i, symbol in enumerate(native)))
    if omitted:
        details.append("[unrecognized backtrace frames omitted]")
    if truncated:
        details.append("[native backtrace truncated]")
    return crash_id, "\n".join(details)


def escape(value):
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def crash_annotation(message):
    encoded = escape(message)
    if len(encoded) > CRASH_ANNOTATION_LIMIT:
        marker = escape("\n[crash annotation truncated]")
        chunks, size = [], 0
        for char in message:
            chunk = escape(char)
            if size + len(chunk) > CRASH_ANNOTATION_LIMIT - len(marker):
                break
            chunks.append(chunk)
            size += len(chunk)
        encoded = "".join(chunks) + marker
    return "::error title=Ceph daemon crash metadata::" + encoded


def annotations(log, exit_code):
    if exit_code == 0:
        return []
    output = []
    diagnostics = deque(maxlen=10)
    last_started = None
    completed_failure = False
    source_prefix = ""
    continuation = False
    seen_crashes = set()
    crashes_truncated = False
    failure_diagnostics = False

    def bounded(message):
        if len(message) > DIAGNOSTIC_LIMIT:
            return message[:DIAGNOSTIC_LIMIT] + "\n[diagnostic truncated]", False
        return message, True

    def append_diagnostics():
        for path, number, message in diagnostics:
            output.append(f"::error file={escape(path)},line={number}::{escape(message)}")

    for line in log:
        if line.rstrip("\r\n") == FAILURE_DIAGNOSTICS_MARKER:
            continuation = False
            failure_diagnostics = True
            continue
        if json_diagnostic_line(line):
            # failure_diagnostics emits compact JSONL after the test exits.
            # Even rejected metadata must not enter an abrupt Go assertion's
            # continuation or pull later arbitrary daemon logs into it.
            continuation = False
            if not failure_diagnostics:
                continue
            crash = crash_summary(line)
            if crash is not None and crash[0] not in seen_crashes:
                if len(seen_crashes) < CRASH_RECORD_LIMIT:
                    seen_crashes.add(crash[0])
                    output.append(crash_annotation(crash[1]))
                elif not crashes_truncated:
                    output.append(crash_annotation("[additional crash metadata omitted]"))
                    crashes_truncated = True
            continue
        suite = re.match(r"^Running Ceph test suite: (api|client)\s*$", line)
        started = re.match(r"^=== RUN\s+(\S+)", line)
        resumed = re.match(r"^=== (PAUSE|CONT|NAME)\s+\S+", line)
        if failure_diagnostics and not (suite or started or resumed):
            # Daemon log tails are untrusted text, not Go assertion records.
            continue
        if suite:
            failure_diagnostics = False
            source_prefix = "integration/" if suite.group(1) == "api" else "cephmsgr/"
            diagnostics.clear()
            last_started = None
            continuation = False
        if started:
            failure_diagnostics = False
            diagnostics.clear()
            last_started = started.group(1)
            continuation = False
        if re.match(r"^\s*(--- (PASS|FAIL|SKIP):|=== (RUN|PAUSE|CONT|NAME)\b|PASS\s*$|FAIL\s*$)", line):
            continuation = False
            failure_diagnostics = False
        match = re.match(r"\s+(\S+_test\.go):(\d+): (.*)", line)
        if match:
            path, number, message = match.groups()
            message, continuation = bounded(message)
            diagnostics.append((source_prefix + path, number, message))
        elif continuation and line.strip():
            # errors.Join and multiline assertions emit continuation lines
            # without another Go source prefix. Keep them with their cause.
            path, number, message = diagnostics[-1]
            message, continuation = bounded(message + "\n" + line.rstrip("\n"))
            diagnostics[-1] = (path, number, message)
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
