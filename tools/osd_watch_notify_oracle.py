#!/usr/bin/env python3
"""Test-only pinned native notifier, scoped to the fixture's owned object.

The native C librados notify2 path sends binary data and independently
decodes ACK identities/blobs, including partial timeout replies. It has no product dependency. Requests
are bounded and each invocation has a retained token receipt and diagnostic log.
The independent ABI adapter follows pinned include/rados/librados.h and
include/rados/rados_types.h; upstream headers carry LGPL notices. No native
implementation is copied.
"""
import argparse
import ctypes as C
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

import osd_omap_native as omap
import osd_watchers_oracle as watchers
import osd_xattr_native as xattr

NAME = "osd-watch-notify"


def check_input(value):
    if not isinstance(value, dict) or set(value) != {"payload_hex", "timeout_ms"}:
        raise ValueError("native notify input fields differ")
    payload = value["payload_hex"]
    timeout = value["timeout_ms"]
    if (not isinstance(payload, str) or len(payload) > 8192
            or not re.fullmatch("(?:[0-9a-f]{2})*", payload)):
        raise ValueError("native notify payload exceeds binary input bound")
    if type(timeout) is not int or not 100 <= timeout <= 10000:
        raise ValueError("native notify timeout out of bounds")
    return bytes.fromhex(payload), timeout


def token_name(token, suffix):
    if not isinstance(token, str) or not re.fullmatch("[0-9a-f]{32}", token):
        raise ValueError("invalid notify scope token")
    return NAME + "-" + token + "-" + suffix


def current(out, metadata):
    status = omap.read_json(out / (NAME + "-status.json"))
    if (not isinstance(status, dict) or status.get("schema_version") != 1
            or status.get("source_commit") != xattr.SOURCE_COMMIT
            or type(status.get("done")) is not bool):
        raise ValueError("invalid native notify scope receipt")
    scoped = omap.read_json(out / (token_name(status.get("token"), "status") + ".json"))
    if any(scoped.get(k) != status.get(k) for k in ("token", "input", "ownership", "object", "source_commit", "schema_version")):
        raise ValueError("native notify current/token receipts differ")
    if type(scoped.get("done")) is not bool or type(scoped.get("ready")) is not bool:
        raise ValueError("invalid native notify lifecycle receipt")
    names = watchers.writes.check_ownership(scoped.get("ownership"), metadata)
    if scoped.get("object") != names[0].decode():
        raise ValueError("native notify receipt object differs from ownership")
    check_input(scoped.get("input"))
    return scoped


def publish(out, status):
    held = omap.read_json(out / (NAME + "-status.json"))
    if held.get("token") != status["token"] or held.get("ownership") != status["ownership"]:
        raise ValueError("native notify worker lost its scope")
    omap.publish(out, NAME + "-status", status)
    omap.publish(out, token_name(status["token"], "status"), status)


class NativeAck(C.Structure):
    # Pinned src/include/rados/rados_types.h; native host ABI, not wire fields.
    _fields_ = [("notifier_id", C.c_uint64), ("cookie", C.c_uint64),
                ("payload", C.c_void_p), ("payload_len", C.c_uint64)]


class NativeMissed(C.Structure):
    _fields_ = [("notifier_id", C.c_uint64), ("cookie", C.c_uint64)]


def native_notify(native, io, name, payload, timeout):
    lib = native.lib
    p, sz, u64, char = C.c_void_p, C.c_size_t, C.c_uint64, C.c_char_p
    definitions = {
        "notify2": ([p, char, p, C.c_int, u64, C.POINTER(p), C.POINTER(sz)], C.c_int),
        "decode_notify_response": ([p, sz, C.POINTER(C.POINTER(NativeAck)), C.POINTER(sz),
                                    C.POINTER(C.POINTER(NativeMissed)), C.POINTER(sz)], C.c_int),
        "free_notify_response": ([C.POINTER(NativeAck), sz, C.POINTER(NativeMissed)], None),
        "buffer_free": ([p], None),
    }
    for name_, (arguments, result) in definitions.items():
        function = getattr(lib, "rados_" + name_)
        function.argtypes, function.restype = arguments, result
    reply, length = p(), sz()
    code = lib.rados_notify2(io, name.encode(), C.create_string_buffer(payload), len(payload),
                             timeout, C.byref(reply), C.byref(length))
    acks, missed = C.POINTER(NativeAck)(), C.POINTER(NativeMissed)()
    nr_acks, nr_missed = sz(), sz()
    decoded = False
    try:
        if not reply.value or length.value > (1 << 20):
            raise ValueError("native notify returned absent or oversized reply data")
        native.check(lib.rados_decode_notify_response(reply, length, C.byref(acks), C.byref(nr_acks),
                                                       C.byref(missed), C.byref(nr_missed)))
        decoded = True
        if nr_acks.value > 64 or nr_missed.value > 64:
            raise ValueError("native notify identities exceed oracle bounds")
        replies = []
        total = 0
        for item in acks[:nr_acks.value]:
            total += item.payload_len
            if total > (1 << 20) or item.payload_len and not item.payload:
                raise ValueError("native notify ACK blob exceeds bounds")
            replies.append(dict(global_id=item.notifier_id, cookie=item.cookie,
                                payload_hex=C.string_at(item.payload, item.payload_len).hex()))
        timeouts = [dict(global_id=item.notifier_id, cookie=item.cookie) for item in missed[:nr_missed.value]]
        return dict(return_code=code, replies=replies, missed=timeouts,
                    native_reply_hex=C.string_at(reply, length.value).hex())
    finally:
        if decoded:
            lib.rados_free_notify_response(acks, nr_acks, missed)
        if reply.value:
            lib.rados_buffer_free(reply)


def worker(root, out, token):
    metadata, name = watchers.owned(root, out)
    status = current(out, metadata)
    if status["token"] != token or status["object"] != name or status["done"]:
        raise ValueError("native notifier does not own its object scope")
    payload, timeout = check_input(status["input"])
    native, cluster, io = xattr.Native(), C.c_void_p(), C.c_void_p()
    lib = native.lib
    instance = lib.rados_get_instance_id
    instance.argtypes, instance.restype = [C.c_void_p], C.c_uint64
    native.check(lib.rados_create2(C.byref(cluster), b"ceph", b"client.test", 0))
    try:
        native.check(lib.rados_conf_read_file(cluster, os.fsencode(root / "ceph.conf")))
        native.check(lib.rados_conf_set(cluster, b"keyring", os.fsencode(root / "keyring")))
        native.check(lib.rados_connect(cluster))
        native.check(lib.rados_ioctx_create(cluster, metadata["pool"].encode(), C.byref(io)))
        try:
            status.update(global_id=instance(cluster), ready=True)
            publish(out, status)
            status.update(native_notify(native, io, name, payload, timeout), done=True)
        finally:
            lib.rados_ioctx_destroy(io)
    except BaseException as error:
        status.update(error=str(error)[:1024], done=True)
        raise
    finally:
        try:
            lib.rados_shutdown(cluster)
        except BaseException as error:
            status.update(error=str(error)[:1024], done=True)
            raise
        finally:
            publish(out, status)


def run(action, root, out):
    if action not in {"start", "observe", "finish"}:
        raise ValueError("unsupported native notify control action")
    metadata, name = watchers.owned(root, out)
    lock = out / (NAME + "-control.lock")
    with lock.open("x"):
        pass
    try:
        if action == "start":
            if (out / (NAME + "-status.json")).exists() and not current(out, metadata)["done"]:
                raise ValueError("native notify scope remains active")
            inputs = omap.read_json(out / (NAME + "-input.json"))
            check_input(inputs)
            token = uuid.uuid4().hex
            status = dict(schema_version=1, token=token, source_commit=xattr.SOURCE_COMMIT,
                          object=name, input=inputs, ready=False, done=False,
                          ownership=omap.read_json(out / (watchers.writes.OWNED + ".json")))
            with (out / token_name(token, "worker.log")).open("xb") as log:
                omap.publish(out, token_name(token, "status"), status)
                omap.publish(out, NAME + "-status", status)
                process = subprocess.Popen([sys.executable, __file__, "serve", str(root), str(out), "--token", token],
                                           stdout=log, stderr=log, start_new_session=True)
                deadline = time.monotonic() + 30
                while True:
                    status = current(out, metadata)
                    if status.get("error"):
                        raise RuntimeError("native notifier startup failed: " + str(status.get("error", "")))
                    if status.get("ready"):
                        break
                    if process.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError("native notifier startup deadline")
                    time.sleep(.05)
        else:
            status = current(out, metadata)
            if status["object"] != name:
                raise ValueError("native notify receipt belongs to a different owned object")
            deadline = time.monotonic() + 40
            while action == "finish" and not status["done"]:
                if time.monotonic() > deadline:
                    raise TimeoutError("native notify completion deadline")
                time.sleep(.05)
                status = current(out, metadata)
        if status.get("error"):
            raise RuntimeError("native notify scope failed: " + str(status.get("error", "")))
        omap.publish(out, NAME + "-result", status)
    finally:
        lock.unlink()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "observe", "finish", "serve"))
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument("--token")
    args = parser.parse_args()
    if args.action == "serve":
        token_name(args.token, "status")
        worker(args.root, args.out, args.token)
    else:
        run(args.action, args.root, args.out)
