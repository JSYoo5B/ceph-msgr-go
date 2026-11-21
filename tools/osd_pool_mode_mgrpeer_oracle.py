"""Owned-pool oracle mode mgrpeer: a native client talking to the active MGR.

Test-only; never product code. It drives the pinned Ceph 20.2.4 (commit
7f793731f1b39eb4f465e960113d2363c311b964) MgrClient of librados, on cluster
handles of its own, against whatever MGR the MgrMap names active, and
reports what that native client observed. Sources:
ceph/ceph/blob/v20.2.4/src/mgr/MgrClient.cc (service_daemon_register,
service_daemon_update_status, shutdown, start_command,
start_tell_command, handle_command_reply) and
ceph/ceph/blob/v20.2.4/src/librados/RadosClient.cc.

1. A service daemon registers (MMgrOpen with service_daemon true and the
   given metadata), sets a status (carried by its next MMgrReport, which it
   sends only after the MGR configured it), holds the registration, and
   shuts down: MgrClient sends MMgrClose and waits up to
   mgr_client_service_daemon_unregister_timeout. MgrClient::shutdown marks
   itself dead before it sends the close, so its dispatcher drops the MGR's
   echo and the wait always runs out: the shutdown time cannot show the
   echo. Instead the handle's own messenger log (debug_ms 1, a file of its
   own) records every message it sent and received; the counts of MMgrClose
   lines sent ("--> ") and received ("<== ") are reported.
2. A second service daemon does the same; the test's MGR leaves its close
   unanswered, so its log must show the close sent and none received
   (negative control).
3. One command handle sends rados_mgr_command (MMgrCommand when the MgrMap's
   active features carry SERVER_OCTOPUS), rados_mgr_command_target to the
   named active MGR (a tell, MCommand with the FSID), and a third command,
   each with bulk input. Its return code (signed), status string and bulk
   output are reported as the native client received them; a reply with
   another TID would never complete it (handle_command_reply drops it).
Nothing it sends changes cluster state; no command is resent by this
oracle (MgrClient itself resends a pending command on a new MGR session).
"""
import base64
import ctypes as C
import os
import pathlib
import time

from osd_pool_oracle import integer, oracle_mode

MAX_OUTPUT = 64 << 10


def native_dict(entries):
    out = b""
    for key, value in sorted(entries.items()):
        out += key.encode() + b"\x00" + value.encode() + b"\x00"
    return out + b"\x00"


def string_map(value, label):
    if type(value) is not dict or len(value) > 8 or not all(
            type(k) is str and type(v) is str and 0 < len(k) <= 64 and len(v) <= 128 for k, v in value.items()):
        raise ValueError("invalid " + label)
    return value


def connect(rados, options):
    lib, cluster = rados.lib, C.c_void_p()
    rados.native.check(lib.rados_create2(C.byref(cluster), b"ceph", b"client.test", 0))
    try:
        rados.native.check(lib.rados_conf_read_file(cluster, os.fsencode(rados.root / "ceph.conf")))
        rados.native.check(lib.rados_conf_set(cluster, b"keyring", os.fsencode(rados.root / "keyring")))
        for key, value in options.items():
            rados.native.check(lib.rados_conf_set(cluster, key.encode(), value.encode()))
        rados.native.check(lib.rados_connect(cluster))
    except BaseException:
        lib.rados_shutdown(cluster)
        raise
    return cluster


def timed_shutdown(lib, cluster):
    started = time.monotonic()
    lib.rados_shutdown(cluster)
    return round(time.monotonic() - started, 3)


MAX_LOG = 16 << 20


def close_lines(text):
    """The MMgrClose lines of a native debug_ms 1 log: sent and received."""
    sent = received = 0
    for line in text.splitlines():
        if " mgrclose(" not in line:
            continue
        if " --> " in line:
            sent += 1
        elif " <== " in line:
            received += 1
    return sent, received


def service(rados, daemon, metadata, status, hold, timeout):
    lib = rados.lib
    log = pathlib.Path("/tmp/ceph-msgr-mgrpeer-%s.log" % daemon)
    log.unlink(missing_ok=True)
    cluster = connect(rados, {"mgr_client_service_daemon_unregister_timeout": str(timeout), "log_file": str(log),
                              "log_to_file": "true", "debug_ms": "1/1", "log_to_stderr": "false", "err_to_stderr": "false"})
    try:
        rados.native.check(lib.rados_service_register(cluster, b"ceph-msgr", daemon.encode(), native_dict(metadata)))
        rados.native.check(lib.rados_service_update_status(cluster, native_dict(status)))
        time.sleep(hold)
    except BaseException:
        lib.rados_shutdown(cluster)
        log.unlink(missing_ok=True)
        raise
    seconds = timed_shutdown(lib, cluster)
    try:
        if log.stat().st_size > MAX_LOG:
            raise ValueError("native messenger log exceeds bound")
        sent, received = close_lines(log.read_text(errors="replace"))
    finally:
        log.unlink(missing_ok=True)
    return {"daemon": daemon, "shutdown_seconds": seconds, "unregister_timeout": timeout,
            "close_sent": sent, "close_received": received}


def command(lib, cluster, target, words, inbuf):
    commands = (C.c_char_p * len(words))(*[w.encode() for w in words])
    output, status = C.c_void_p(), C.c_void_p()
    output_length, status_length = C.c_size_t(), C.c_size_t()
    started = time.monotonic()
    try:
        if target is None:
            code = lib.rados_mgr_command(cluster, commands, len(words), inbuf, len(inbuf), C.byref(output), C.byref(output_length),
                                         C.byref(status), C.byref(status_length))
        else:
            code = lib.rados_mgr_command_target(cluster, target.encode(), commands, len(words), inbuf, len(inbuf), C.byref(output),
                                                C.byref(output_length), C.byref(status), C.byref(status_length))
        if output_length.value > MAX_OUTPUT or status_length.value > MAX_OUTPUT:
            raise ValueError("native command output exceeds bound")
        data = C.string_at(output, output_length.value) if output.value else b""
        text = C.string_at(status, status_length.value).decode(errors="replace") if status.value else ""
        return {"code": code, "status": text, "output": base64.b64encode(data).decode(), "seconds": round(time.monotonic() - started, 3)}
    finally:
        if output.value:
            lib.rados_buffer_free(output)
        if status.value:
            lib.rados_buffer_free(status)


@oracle_mode
def mode_mgrpeer(rados, cluster, request, out):
    """A native service daemon and native command families against the active MGR."""
    if set(request) != {"mgr_name", "daemon", "noecho_daemon", "metadata", "status", "commands"}:
        raise ValueError("mgrpeer takes mgr_name, daemon, noecho_daemon, metadata, status and commands")
    lib = rados.lib
    p, i, s = C.c_void_p, C.c_int, C.c_size_t
    lib.rados_service_register.argtypes, lib.rados_service_register.restype = [p, C.c_char_p, C.c_char_p, C.c_char_p], i
    lib.rados_service_update_status.argtypes, lib.rados_service_update_status.restype = [p, C.c_char_p], i
    lib.rados_mgr_command_target.argtypes = [p, C.c_char_p, C.POINTER(C.c_char_p), s, C.c_char_p, s, C.POINTER(p), C.POINTER(s), C.POINTER(p), C.POINTER(s)]
    lib.rados_mgr_command_target.restype = i
    metadata, status = string_map(request["metadata"], "metadata"), string_map(request["status"], "status")
    names = [request["mgr_name"], request["daemon"], request["noecho_daemon"]]
    if not all(type(n) is str and 0 < len(n) <= 64 and n.isascii() for n in names):
        raise ValueError("invalid mgrpeer names")
    steps = request["commands"]
    if type(steps) is not list or not 1 <= len(steps) <= 4:
        raise ValueError("invalid mgrpeer commands")
    result = {"echoed": service(rados, request["daemon"], metadata, status, 3, 2),
              "unanswered": service(rados, request["noecho_daemon"], metadata, status, 3, 2)}
    handle = connect(rados, {"rados_mon_op_timeout": "15"})
    try:
        replies = []
        for step in steps:
            if set(step) != {"tell", "command", "input"} or type(step["tell"]) is not bool or type(step["command"]) is not str:
                raise ValueError("invalid mgrpeer command step")
            inbuf = base64.b64decode(step["input"], validate=True)
            if len(inbuf) > 4096 or len(step["command"]) > 256:
                raise ValueError("mgrpeer command exceeds bound")
            replies.append(command(lib, handle, request["mgr_name"] if step["tell"] else None, [step["command"]], inbuf))
        result["commands"] = replies
    finally:
        lib.rados_shutdown(handle)
    integer(len(result["commands"]), 1, 4, "replies")
    return {"mgr_peer": result}
