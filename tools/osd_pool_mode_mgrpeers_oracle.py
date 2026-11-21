"""Owned-pool oracle mode mgrpeers: native peers held, answered and closed by the active MGR.

Test-only; never product code. It drives the pinned Ceph 20.2.4 (commit
7f793731f1b39eb4f465e960113d2363c311b964) MgrClient of librados, on cluster
handles of its own, against the MGR the MgrMap names active, while that MGR
holds one peer and answers another, and reports what each native peer
observed. Sources: ceph/ceph/blob/v20.2.4/src/mgr/MgrClient.cc (reconnect,
start_command, start_tell_command, shutdown) and
ceph/ceph/blob/v20.2.4/src/librados/RadosClient.cc (mgr_command).

1. A TCP socket to the MGR's address reads the server's whole msgr2 banner
   and sends only the first four bytes of its own, so that handshake stays
   unfinished; it is not a Ceph client. Its receipt is the banner prefix it
   read and whether the MGR later ended the connection (EOF) within
   close_wait seconds of the answered reply.
2. The stalled client, a cluster handle with rados_mon_op_timeout
   stall_timeout, sends one command on its own thread. RadosClient::mgr_command
   waits for the reply at most rados_mon_op_timeout and then returns
   ETIMEDOUT unnegated (positive 110, C_SaferCond::wait_for in
   ceph/ceph/blob/v20.2.4/src/common/Cond.h); MgrClient keeps no other
   timer. Its receipt is its instance
   (global) ID and the code, status and output it returned.
3. The answered client, a second cluster handle, sends one command and
   reports its instance ID and the code, status and output it received; a
   reply on another connection or with another TID never completes it
   (handle_command_reply looks the TID up in its own command table).
4. After the EOF, the ESTABLISHED TCP connections from this network
   namespace to the MGR's address are counted from /proc/net/tcp and tcp6
   until none remains (at most 10 s); the count is reported.
The answered handle shuts down first; then the stalled command's thread is
joined and its handle shut down (MgrClient::shutdown forgets the pending
command). Nothing it sends changes cluster state, and no command is resent
by this oracle.

This module imports the mgrpeer mode's helpers; the oracle loads mode
modules in sorted file order, which loads that module first.
"""
import base64
import ctypes as C
import ipaddress
import socket
import threading
import time

from osd_pool_mode_mgrpeer_oracle import command, connect, timed_shutdown
from osd_pool_oracle import integer, oracle_mode

BANNER = b"ceph v2\n"
ESTABLISHED = "01"


def endpoint(value):
    """The MGR address "a.b.c.d:port" as (ip, port); IPv4 only."""
    if type(value) is not str or value.count(":") != 1:
        raise ValueError("invalid MGR address")
    host, port = value.split(":")
    ip = ipaddress.IPv4Address(host)
    return str(ip), integer(int(port), 1, 65535, "MGR port")


def proc_endpoint(field):
    """Decode a /proc/net/tcp{,6} address field as (ip, port); IPv4 and
    IPv4-mapped IPv6 addresses only, None otherwise."""
    address, port = field.split(":")
    raw = bytes.fromhex(address)
    if len(raw) == 4:
        return str(ipaddress.IPv4Address(raw[::-1])), int(port, 16)
    if len(raw) == 16:
        # Four host-order 32-bit words.
        words = b"".join(raw[i:i + 4][::-1] for i in range(0, 16, 4))
        mapped = ipaddress.IPv6Address(words).ipv4_mapped
        if mapped is not None:
            return str(mapped), int(port, 16)
    return None


def established_to(tables, target):
    """Count ESTABLISHED entries whose remote endpoint is target in the
    given /proc/net/tcp{,6} texts."""
    count = 0
    for text in tables:
        for line in text.splitlines()[1:]:
            fields = line.split()
            if len(fields) < 4 or fields[3] != ESTABLISHED:
                continue
            if proc_endpoint(fields[2]) == target:
                count += 1
    return count


def read_tables():
    tables = []
    for name in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(name) as f:
                tables.append(f.read(1 << 20))
        except FileNotFoundError:
            pass
    return tables


def step(value, label):
    if type(value) is not dict or set(value) != {"tell", "command", "input"} or type(value["tell"]) is not bool \
            or type(value["command"]) is not str or len(value["command"]) > 256:
        raise ValueError("invalid " + label)
    inbuf = base64.b64decode(value["input"], validate=True)
    if len(inbuf) > 4096:
        raise ValueError(label + " input exceeds bound")
    return value["tell"], [value["command"]], inbuf


def read_exactly(s, n):
    data = b""
    while len(data) < n:
        chunk = s.recv(n - len(data))
        if not chunk:
            break
        data += chunk
    return data


def stalled_handshake(target):
    """Open the unfinished handshake: read the server's whole banner (its
    "ceph v2\\n" prefix, a little-endian 16-bit length and that many payload
    bytes), then send only four bytes of a banner. Return the socket and the
    banner read."""
    s = socket.create_connection(target, timeout=10)
    try:
        received = read_exactly(s, len(BANNER) + 2)
        if len(received) == len(BANNER) + 2 and received.startswith(BANNER):
            length = int.from_bytes(received[len(BANNER):], "little")
            if length > 64:
                raise ValueError("msgr2 banner payload exceeds bound")
            received += read_exactly(s, length)
        s.sendall(BANNER[:4])
    except BaseException:
        s.close()
        raise
    return s, received


def wait_eof(s, seconds):
    """Read until EOF or seconds pass; return (eof, bytes read)."""
    deadline, extra = time.monotonic() + seconds, 0
    while True:
        left = deadline - time.monotonic()
        if left <= 0:
            return False, extra
        s.settimeout(left)
        try:
            data = s.recv(4096)
        except socket.timeout:
            return False, extra
        except ConnectionResetError:
            return True, extra
        if not data:
            return True, extra
        extra += len(data)


@oracle_mode
def mode_mgrpeers(rados, cluster, request, out):
    """A stalled handshake, a held native command and an answered one against the active MGR."""
    if set(request) != {"mgr_name", "address", "stalled", "answered", "stall_timeout", "close_wait"}:
        raise ValueError("mgrpeers takes mgr_name, address, stalled, answered, stall_timeout and close_wait")
    name = request["mgr_name"]
    if type(name) is not str or not 0 < len(name) <= 64 or not name.isascii():
        raise ValueError("invalid MGR name")
    target = endpoint(request["address"])
    stalled, answered = step(request["stalled"], "stalled command"), step(request["answered"], "answered command")
    stall_timeout = integer(request["stall_timeout"], 5, 40, "stall timeout")
    close_wait = integer(request["close_wait"], 5, 40, "close wait")
    lib = rados.lib
    lib.rados_get_instance_id.argtypes, lib.rados_get_instance_id.restype = [C.c_void_p], C.c_uint64
    lib.rados_mgr_command_target.argtypes = [C.c_void_p, C.c_char_p, C.POINTER(C.c_char_p), C.c_size_t, C.c_char_p, C.c_size_t,
                                             C.POINTER(C.c_void_p), C.POINTER(C.c_size_t), C.POINTER(C.c_void_p), C.POINTER(C.c_size_t)]
    lib.rados_mgr_command_target.restype = C.c_int

    s, banner = stalled_handshake(target)
    result = {"handshake": {"banner": base64.b64encode(banner).decode()}}
    try:
        held = connect(rados, {"rados_mon_op_timeout": str(stall_timeout)})
        outcome = {}

        def run_held():
            try:
                outcome["reply"] = command(lib, held, name if stalled[0] else None, stalled[1], stalled[2])
            except BaseException as e:  # reported, never retried
                outcome["error"] = repr(e)

        thread = threading.Thread(target=run_held, daemon=True)
        try:
            thread.start()
            handle = connect(rados, {"rados_mon_op_timeout": "20"})
            try:
                result["answered"] = {"instance_id": lib.rados_get_instance_id(handle),
                                      "reply": command(lib, handle, name if answered[0] else None, answered[1], answered[2])}
                replied = time.monotonic()
                eof, extra = wait_eof(s, close_wait)
                result["handshake"].update(eof=eof, bytes_after_banner=extra, eof_seconds=round(time.monotonic() - replied, 3))
                remaining = established_to(read_tables(), target)
                for _ in range(20):
                    if remaining == 0:
                        break
                    time.sleep(0.5)
                    remaining = established_to(read_tables(), target)
                result["established_after_close"] = remaining
            finally:
                result["answered_shutdown_seconds"] = timed_shutdown(lib, handle)
            thread.join(stall_timeout + 15)
            if thread.is_alive():
                raise RuntimeError("the stalled native command did not return within its timeout")
        finally:
            if not thread.is_alive():
                result["stalled"] = {"instance_id": lib.rados_get_instance_id(held), **outcome}
                lib.rados_shutdown(held)
    finally:
        s.close()
    return {"mgr_peers": result}
