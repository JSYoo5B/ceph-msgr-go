#!/usr/bin/env python3
"""Independent test-only Tentacle OSD/PG command oracle, never product code.

Ceph 20.2.4 source 7f793731f1b39eb4f465e960113d2363c311b964:
src/include/rados/librados.h:4012-4024 and src/librados/librados_c.cc
(LGPL-2.1; header LGPL-2.1 or LGPL-3.0). This adapter copies no Ceph
implementation and emits no Go-generated wire data.

ROOT contains fixture ceph.conf/keyring. OUT contains pinned daemon-version and
bounded osd-command-input.json. The oracle connects with a fixed fixture
identity (client.test or client.readonly), sends ONE raw command through
rados_osd_command to an explicit OSD ID or rados_pg_command to a pgid, and
records the native return code, output bytes and status string. Native
librados routes PG commands itself and may resend commands (for example on
EAGAIN or reconnection); those native behaviors are oracle properties, not
Go product requirements. Unit adaptation tests are not daemon proof.
"""
import argparse
import base64
import ctypes as C
import json
import os
from pathlib import Path

SOURCE_COMMIT = "7f793731f1b39eb4f465e960113d2363c311b964"
MAX_INPUT = 4 << 20
MAX_OUTPUT = 6 << 20
MAX_DATA = 4 << 20
MAX_COMMAND = 64 << 10
ROLES = {"test": b"client.test", "readonly": b"client.readonly"}


def binary(text, limit):
    if not isinstance(text, str) or len(text) > 4 * ((limit + 2) // 3):
        raise ValueError("invalid or oversized base64 input")
    value = base64.b64decode(text, validate=True)
    if len(value) > limit:
        raise ValueError("binary input exceeds oracle bound")
    return value


def validate_request(raw):
    if not isinstance(raw, dict) or set(raw) - {"role", "osd", "pgid", "command", "input", "osd_op_timeout"}:
        raise ValueError("unknown OSD command oracle fields")
    if raw.get("role", "test") not in ROLES:
        raise ValueError("unsupported fixture identity")
    if ("osd" in raw) == ("pgid" in raw):
        raise ValueError("exactly one of osd or pgid is required")
    command = raw.get("command")
    if not isinstance(command, str) or not command or len(command.encode()) > MAX_COMMAND:
        raise ValueError("invalid raw command JSON string")
    parsed = json.loads(command)
    if not isinstance(parsed, dict) or not isinstance(parsed.get("prefix"), str):
        raise ValueError("raw command must be a JSON object with a string prefix")
    timeout = raw.get("osd_op_timeout", 0)
    if type(timeout) is not int or not 0 <= timeout <= 30:
        raise ValueError("invalid native OSD operation timeout")
    request = {"role": raw.get("role", "test"), "command": command.encode(), "input": binary(raw.get("input", ""), MAX_DATA),
               "osd_op_timeout": timeout}
    if "osd" in raw:
        if type(raw["osd"]) is not int or not 0 <= raw["osd"] < 1 << 31:
            raise ValueError("invalid OSD ID")
        request["osd"] = raw["osd"]
    else:
        pgid = raw["pgid"]
        if not isinstance(pgid, str) or not pgid or len(pgid) > 64 or "\0" in pgid:
            raise ValueError("invalid pgid")
        request["pgid"] = pgid.encode()
    return request


class Native:
    def __init__(self, library=None):
        self.lib = library if library is not None else C.CDLL("librados.so.2")
        p, s, i, char = C.c_void_p, C.c_size_t, C.c_int, C.c_char_p
        command = [C.POINTER(char), s, char, s, C.POINTER(p), C.POINTER(s), C.POINTER(p), C.POINTER(s)]
        for name, (arguments, result) in {
            "create2": ([C.POINTER(p), char, char, C.c_uint64], i),
            "conf_read_file": ([p, char], i), "conf_set": ([p, char, char], i),
            "connect": ([p], i), "shutdown": ([p], None), "buffer_free": ([p], None),
            "osd_command": ([p, i] + command, i), "pg_command": ([p, char] + command, i),
        }.items():
            function = getattr(self.lib, "rados_" + name)
            function.argtypes, function.restype = arguments, result

    def check(self, code):
        if code < 0:
            raise RuntimeError("native OSD command oracle setup failed: " + str(code))

    def connect(self, root, role, timeout=0):
        cluster = C.c_void_p()
        self.check(self.lib.rados_create2(C.byref(cluster), b"ceph", ROLES[role], 0))
        try:
            self.check(self.lib.rados_conf_read_file(cluster, os.fsencode(root / "ceph.conf")))
            self.check(self.lib.rados_conf_set(cluster, b"keyring", os.fsencode(root / "keyring")))
            if timeout:
                # Bounds native Objecter command resends (e.g. on EAGAIN).
                self.check(self.lib.rados_conf_set(cluster, b"rados_osd_op_timeout", str(timeout).encode()))
            self.check(self.lib.rados_connect(cluster))
        except BaseException:
            self.lib.rados_shutdown(cluster)
            raise
        return cluster

    def run(self, cluster, request):
        commands = (C.c_char_p * 1)(request["command"])
        inbuf = request["input"]
        out, outlen, status, statuslen = C.c_void_p(), C.c_size_t(), C.c_void_p(), C.c_size_t()
        if "osd" in request:
            code = self.lib.rados_osd_command(cluster, request["osd"], commands, 1, inbuf, len(inbuf),
                                              C.byref(out), C.byref(outlen), C.byref(status), C.byref(statuslen))
        else:
            code = self.lib.rados_pg_command(cluster, request["pgid"], commands, 1, inbuf, len(inbuf),
                                             C.byref(out), C.byref(outlen), C.byref(status), C.byref(statuslen))
        try:
            if outlen.value > MAX_DATA or statuslen.value > MAX_COMMAND:
                raise ValueError("native command output exceeds oracle bound")
            data = C.string_at(out.value, outlen.value) if out.value and outlen.value else b""
            message = C.string_at(status.value, statuslen.value) if status.value and statuslen.value else b""
        finally:
            for buffer in (out, status):
                if buffer.value:
                    self.lib.rados_buffer_free(buffer)
        return {"code": code, "data": base64.b64encode(data).decode(), "message": base64.b64encode(message).decode()}


def publish(out, result):
    data = json.dumps(result).encode()
    if len(data) + 1 > MAX_OUTPUT:
        raise ValueError("OSD command JSON output exceeds bound")
    temporary = out / "osd-command-result.tmp"
    temporary.write_bytes(data + b"\n")
    os.replace(temporary, out / "osd-command-result.json")


def collect(root, out):
    with (out / "osd-command-input.json").open("rb") as source:
        raw = source.read(MAX_INPUT + 1)
    if len(raw) > MAX_INPUT:
        raise ValueError("OSD command JSON input exceeds bound")
    request = validate_request(json.loads(raw))
    with (out / "daemon-version").open("rb") as source:
        version = source.read(513).decode().strip()
    if len(version) > 512 or "20.2.4" not in version or SOURCE_COMMIT not in version:
        raise ValueError("OSD command fixture is not pinned Tentacle")
    native = Native()
    cluster = native.connect(root, request["role"], request["osd_op_timeout"])
    try:
        result = native.run(cluster, request)
    finally:
        native.lib.rados_shutdown(cluster)
    result.update(schema_version=1, source_commit=SOURCE_COMMIT, fixture_daemon_version=version, role=request["role"],
                  osd_op_timeout=request["osd_op_timeout"],
                  scope={"independent_native_c_api": True, "native_client_may_resend": True})
    if "osd" in request:
        result["osd"] = request["osd"]
    else:
        result["pgid"] = request["pgid"].decode()
    publish(out, result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    collect(args.root, args.out)
