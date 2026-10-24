#!/usr/bin/env python3
"""Closed, test-only native holder of one ServiceMap registration.

A later Go ServiceMap lifecycle cell needs a daemon entry that stays registered
under independent control, plus a fresh native view of it. The existing service
oracle holds its registration inside one blocking request, so the fixture
control loop cannot run a MON fault while the entry is alive. This worker splits
that hold into closed phases:

  start    claim the request, record a fresh baseline (the owned daemon must be
           absent), start one detached holder process and return once it
           reports that rados_service_register returned and the instance GID.
  observe  open a fresh librados handle and report whether the MON's
           ``service dump`` lists the owned daemon. Repeatable read.
  release  ask the holder to shut its native handle down and wait for its exit
           marker. A returned shutdown is not a removal acknowledgement: absence
           is shown only by a later ``observe``.
  cleanup  report the holder state. If the holder is still alive, kill it once
           (SIGKILL after verifying the process is the owned holder) and compare
           a fresh view with the baseline taken by this worker.

The holder lifetime is capped (LIFETIME seconds) and it exits on its own with
the ``expired`` reason. Run every phase under an external timeout: pinned Python
mon_command ignores its timeout argument.

``start``, ``release`` and the kill in ``cleanup`` claim an exclusive started
marker before they cross the native or process boundary. A repeated request
after a lost acknowledgement is refused instead of replayed; the marker proves
the first attempt began even when its outcome is unknown. No arbitrary service,
daemon, command, signal, process or path is accepted. The service name and the
daemon name derive from the 128-bit token; the registration carries only two
synthetic metadata keys. The native host inventory that librados adds is never
persisted.
"""

import json
import os
import pathlib
import re
import signal
import subprocess
import sys
import time

from mon_command_outcome_oracle import atomic_write


PHASES = ("start", "observe", "release", "cleanup")
SERVICE = "ceph-msgr"
PREFIX = "native-service-holder-"
SCRIPT = "service_map_holder_oracle.py"
LIFETIME = 120
READY_WAIT = 40
EXIT_WAIT = 30
KILL_WAIT = 5
METADATA_LIMIT = 4 << 20
CLUSTER_ROLE = "holder"
RADOS_CONNECT_TIMEOUT = b"30"
TOKEN = re.compile(r"[0-9a-f]{32}")


def validate(raw_id, token, phase):
    if (not raw_id.isascii() or not raw_id.isdecimal()
            or not 0 < int(raw_id) < 1 << 63
            or not TOKEN.fullmatch(token) or phase not in PHASES):
        raise ValueError("Invalid owned ServiceMap holder request")
    return int(raw_id)


def daemon_name(token):
    if not TOKEN.fullmatch(token):
        raise ValueError("Invalid owned ServiceMap holder token")
    return "holder-" + token[:16]


def state_path(output, token, name):
    if not TOKEN.fullmatch(token) or not re.fullmatch(r"[a-z-]{1,24}\.(json|release|log)", name):
        raise ValueError("Invalid owned ServiceMap holder state file")
    return output / (PREFIX + token + "." + name)


def journal(output, value):
    # Only closed holder state reaches this metadata file: never a service dump,
    # host inventory or credentials.
    with (output / "service-holder-native.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")


def claim(path, receipt):
    """Create the exclusive started marker, or refuse a replayed request."""
    try:
        with path.open("x", encoding="utf-8") as handle:
            json.dump(receipt, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        raise ValueError("ServiceMap holder step already started; a lost acknowledgement is never replayed") from None


def read_state(path):
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return None
    if len(data) > 64 << 10:
        raise ValueError("ServiceMap holder state exceeds its bound")
    return json.loads(data)


def positive(value, label):
    if type(value) is not int or not 0 < value < 1 << 63:
        raise ValueError("Invalid ServiceMap holder " + label)
    return value


# ---- native view -----------------------------------------------------------

def project(code, payload, daemon):
    """Reduce a service dump to the owned entry; unrelated entries stay in memory."""
    view = {"code": code, "epoch": None, "present": False, "entry": None,
            "service_daemons": None}
    if code != 0 or not isinstance(payload, bytes) or len(payload) > METADATA_LIMIT:
        return view
    try:
        document = json.loads(payload)
    except (ValueError, UnicodeError):
        view["code"] = None
        return view
    # A dump with an unexpected structure is unreadable, never an absence: only
    # a missing owned service or daemon key inside a well-formed map is absent.
    def invalid():
        view["code"] = None
        view["epoch"] = None
        view["present"] = False
        view["entry"] = None
        view["service_daemons"] = None
        return view

    if not isinstance(document, dict):
        return invalid()
    epoch = document.get("epoch")
    services = document.get("services")
    if type(epoch) is not int or not 0 <= epoch < 1 << 32 or not isinstance(services, dict):
        return invalid()
    view["epoch"] = epoch
    service = services.get(SERVICE)
    if service is None:
        view["service_daemons"] = 0
        return view
    daemons = service.get("daemons") if isinstance(service, dict) else None
    if not isinstance(daemons, dict):
        return invalid()
    # The map may also hold a "summary" key; count daemon objects only.
    view["service_daemons"] = sum(1 for value in daemons.values() if isinstance(value, dict))
    if daemon in daemons:
        entry = daemons[daemon]
        if not isinstance(entry, dict):
            return invalid()
        metadata = entry.get("metadata")
        metadata = metadata if isinstance(metadata, dict) else {}
        view["present"] = True
        view["entry"] = {
            "gid": entry.get("gid") if type(entry.get("gid")) is int else None,
            "start_epoch": entry.get("start_epoch") if type(entry.get("start_epoch")) is int else None,
            "role": metadata.get("ceph_msgr_role") if isinstance(metadata.get("ceph_msgr_role"), str) else None,
            "daemon": metadata.get("ceph_msgr_daemon") if isinstance(metadata.get("ceph_msgr_daemon"), str) else None,
        }
    return view
    epoch = document.get("epoch")
    view["epoch"] = epoch if type(epoch) is int and 0 <= epoch < 1 << 32 else None
    services = document.get("services")
    service = services.get(SERVICE) if isinstance(services, dict) else None
    daemons = service.get("daemons") if isinstance(service, dict) else None
    if isinstance(daemons, dict):
        # The map may also hold a "summary" key; count daemon objects only.
        view["service_daemons"] = sum(1 for value in daemons.values() if isinstance(value, dict))
        entry = daemons.get(daemon)
        if isinstance(entry, dict):
            metadata = entry.get("metadata")
            metadata = metadata if isinstance(metadata, dict) else {}
            view["present"] = True
            view["entry"] = {
                "gid": entry.get("gid") if type(entry.get("gid")) is int else None,
                "start_epoch": entry.get("start_epoch") if type(entry.get("start_epoch")) is int else None,
                "role": metadata.get("ceph_msgr_role") if isinstance(metadata.get("ceph_msgr_role"), str) else None,
                "daemon": metadata.get("ceph_msgr_daemon") if isinstance(metadata.get("ceph_msgr_daemon"), str) else None,
            }
    return view


class Native:
    """Production boundary: fresh observation handle, process control."""

    def view(self, fixture_root, daemon):
        import rados

        # Every call owns a fresh independently authenticated handle.
        with rados.Rados(name="client.test", conffile=str(fixture_root / "ceph.conf"),
                         conf={"keyring": str(fixture_root / "keyring")}) as client:
            code, payload, _ = client.mon_command(json.dumps({"prefix": "service dump", "format": "json"}), b"")
        return project(code, payload, daemon)

    def spawn(self, fixture_root, output, token):
        log = state_path(output, token, "hold.log")
        with log.open("ab") as sink:
            process = subprocess.Popen(
                [sys.executable, str(pathlib.Path(__file__).resolve()), "hold", str(fixture_root), str(output), token],
                stdin=subprocess.DEVNULL, stdout=sink, stderr=sink, start_new_session=True, close_fds=True)
        return process.pid

    def alive(self, pid, token):
        """True only for a live process that is this token's holder."""
        try:
            with open("/proc/%d/cmdline" % pid, "rb") as source:
                argv = source.read(4096).split(b"\0")
            state = open("/proc/%d/stat" % pid).read().rsplit(")", 1)[1].split()[0]
        except (OSError, IndexError):
            return False
        return state != "Z" and SCRIPT.encode() in b" ".join(argv) and token.encode() in argv

    def kill(self, pid):
        os.kill(pid, signal.SIGKILL)

    def sleep(self, seconds):
        time.sleep(seconds)

    def monotonic(self):
        return time.monotonic()


# ---- holder process --------------------------------------------------------

def load_librados():
    import ctypes as C

    lib = C.CDLL("librados.so.2")
    p, char = C.c_void_p, C.c_char_p
    for name, (arguments, result) in {
        "create2": ([C.POINTER(p), char, char, C.c_uint64], C.c_int),
        "conf_read_file": ([p, char], C.c_int),
        "conf_set": ([p, char, char], C.c_int),
        "connect": ([p], C.c_int),
        "shutdown": ([p], None),
        "service_register": ([p, char, char, char], C.c_int),
        "get_instance_id": ([p], C.c_uint64),
    }.items():
        function = getattr(lib, "rados_" + name)
        function.argtypes, function.restype = arguments, result
    return lib


def hold(fixture_root, output, token, lib=None, clock=time.monotonic, sleep=time.sleep):
    """Register the owned daemon, publish readiness and wait for the release marker."""
    import ctypes as C

    daemon = daemon_name(token)
    lib = lib if lib is not None else load_librados()
    metadata = ("ceph_msgr_role\0" + CLUSTER_ROLE + "\0ceph_msgr_daemon\0" + daemon + "\0\0").encode()
    cluster = C.c_void_p()

    def check(code, step):
        if code < 0:
            raise RuntimeError("native %s failed: %d" % (step, code))

    deadline = clock() + LIFETIME
    created = False
    try:
        check(lib.rados_create2(C.byref(cluster), b"ceph", b"client.test", 0), "create")
        created = True
        check(lib.rados_conf_read_file(cluster, os.fsencode(fixture_root / "ceph.conf")), "configuration")
        check(lib.rados_conf_set(cluster, b"keyring", os.fsencode(fixture_root / "keyring")), "keyring")
        check(lib.rados_conf_set(cluster, b"client_mount_timeout", RADOS_CONNECT_TIMEOUT), "timeout")
        check(lib.rados_connect(cluster), "connect")
        check(lib.rados_service_register(cluster, SERVICE.encode(), daemon.encode(), metadata), "service registration")
        gid = lib.rados_get_instance_id(cluster)
        atomic_write(state_path(output, token, "ready.json"),
                     {"token": token, "pid": os.getpid(), "daemon": daemon, "service": SERVICE,
                      "gid": int(gid), "registered": True, "lifetime_seconds": LIFETIME})
        release = state_path(output, token, "go.release")
        reason = "expired"
        while clock() < deadline:
            if release.exists():
                reason = "released"
                break
            sleep(0.2)
    except BaseException as error:
        atomic_write(state_path(output, token, "failed.json"), {"token": token, "error": diagnostic(error)})
        if created:
            lib.rados_shutdown(cluster)
        raise
    # rados_shutdown returns no removal acknowledgement; only the marker records
    # that the call returned.
    lib.rados_shutdown(cluster)
    atomic_write(state_path(output, token, "exited.json"),
                 {"token": token, "pid": os.getpid(), "reason": reason, "shutdown_returned": True})


# ---- coordinator -----------------------------------------------------------

def holder_pid(output, token):
    for name in ("ready.json", "spawned.json"):
        value = read_state(state_path(output, token, name))
        if value is not None:
            return positive(value.get("pid"), "pid")
    return None


def holder_state(output, token, native):
    exited = read_state(state_path(output, token, "exited.json"))
    failed = read_state(state_path(output, token, "failed.json"))
    ready = read_state(state_path(output, token, "ready.json"))
    started = state_path(output, token, "start-started.json").exists()
    pid = holder_pid(output, token)
    alive = pid is not None and exited is None and native.alive(pid, token)
    return {"started": started, "ready": ready is not None, "failed": failed is not None,
            "exited": exited, "alive": alive}


def wait_for(native, condition, seconds):
    deadline = native.monotonic() + seconds
    while True:
        value = condition()
        if value:
            return value
        if native.monotonic() >= deadline:
            return None
        native.sleep(0.2)


def collect(fixture_root, output, raw_id, token, phase, native=None):
    native = native if native is not None else Native()
    request_id = validate(raw_id, token, phase)
    daemon = daemon_name(token)
    receipt = {"request_id": request_id, "token": token, "phase": phase, "daemon": daemon, "service": SERVICE}
    baseline_path = state_path(output, token, "baseline.json")

    if phase == "start":
        claim(state_path(output, token, "start-started.json"), receipt)
        journal(output, dict(receipt, state="start-claimed"))
        view = native.view(fixture_root, daemon)
        baseline = {"code": view["code"], "epoch": view["epoch"], "owned_present": view["present"],
                    "service_daemons": view["service_daemons"]}
        if view["code"] != 0 or view["present"] or view["epoch"] is None:
            raise ValueError("owned ServiceMap daemon is not absent in a readable baseline")
        # Recorded exclusively: a repeated start cannot replace the first baseline.
        with baseline_path.open("x", encoding="utf-8") as handle:
            json.dump({"token": token, "baseline": baseline}, handle, sort_keys=True)
        journal(output, dict(receipt, state="holder-spawn-outcome-unknown", baseline=baseline))
        pid = positive(native.spawn(fixture_root, output, token), "pid")
        # The spawned pid lets cleanup reach a holder that never became ready.
        atomic_write(state_path(output, token, "spawned.json"), {"token": token, "pid": pid})

        def ready():
            failed = read_state(state_path(output, token, "failed.json"))
            if failed is not None:
                raise RuntimeError("holder failed before readiness: " + str(failed.get("error")))
            return read_state(state_path(output, token, "ready.json"))

        report = wait_for(native, ready, READY_WAIT)
        if report is None:
            raise RuntimeError("holder did not become ready within %d seconds" % READY_WAIT)
        if (report.get("token") != token or report.get("daemon") != daemon or report.get("service") != SERVICE
                or report.get("registered") is not True):
            raise ValueError("holder readiness belongs to a different registration")
        positive(report["gid"], "GID")
        positive(report["pid"], "holder pid")
        receipt.update(baseline=baseline, ready={"pid": report["pid"], "gid": report["gid"],
                                                "lifetime_seconds": report["lifetime_seconds"]})
        receipt["state"] = "ready"
        journal(output, receipt)
    else:
        recorded = read_state(baseline_path)
        if recorded is not None and (recorded.get("token") != token or not isinstance(recorded.get("baseline"), dict)):
            raise ValueError("ServiceMap holder has a mismatched baseline receipt")
        if recorded is None and phase != "cleanup":
            raise ValueError("ServiceMap holder has no matching baseline receipt")
        receipt["baseline"] = recorded["baseline"] if recorded is not None else None
        ready = read_state(state_path(output, token, "ready.json"))
        if phase == "observe":
            view = native.view(fixture_root, daemon)
            receipt["view"] = view
            receipt["holder"] = holder_state(output, token, native)
            entry = view["entry"]
            receipt["gid_matches_holder"] = (entry is not None and ready is not None
                                             and entry["gid"] == ready["gid"])
        elif phase == "release":
            if ready is None:
                raise ValueError("ServiceMap holder was never ready; use cleanup")
            claim(state_path(output, token, "release-started.json"), receipt)
            journal(output, dict(receipt, state="release-claimed-outcome-unknown"))
            with state_path(output, token, "go.release").open("x", encoding="utf-8") as handle:
                handle.write("release\n")
            exited = wait_for(native, lambda: read_state(state_path(output, token, "exited.json")), EXIT_WAIT)
            if exited is None:
                raise RuntimeError("holder did not report exit within %d seconds" % EXIT_WAIT)
            receipt["exited"] = exited
            receipt["state"] = "released"
            journal(output, receipt)
        else:  # cleanup
            state = holder_state(output, token, native)
            receipt["holder"] = state
            killed = False
            if state["alive"]:
                pid = holder_pid(output, token)
                # Claim the one allowed kill before crossing the process boundary.
                claim(state_path(output, token, "cleanup-started.json"), receipt)
                journal(output, dict(receipt, state="kill-claimed-outcome-unknown"))
                native.kill(pid)
                killed = wait_for(native, lambda: not native.alive(pid, token), KILL_WAIT) is not None
                if not killed:
                    raise RuntimeError("owned holder survived the kill")
            receipt["killed"] = killed
            view = native.view(fixture_root, daemon)
            receipt["view"] = view
            # Restoration target is this worker's own baseline: the owned daemon
            # absent. A killed holder leaves its entry until the MGR times it out.
            wanted = False if recorded is None else recorded["baseline"]["owned_present"]
            receipt["restored"] = view["code"] == 0 and view["present"] == wanted
            journal(output, dict(receipt, state="cleanup-observed"))
    atomic_write(state_path(output, token, phase + ".json"), receipt)


def main():
    if len(sys.argv) == 5 and sys.argv[1] == "hold":
        root, output, token = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3]), sys.argv[4]
        hold(root, output, token)
        return
    if len(sys.argv) != 6:
        raise ValueError("Expected fixture root, output, request ID, token and phase")
    root, output, raw_id, token, phase = sys.argv[1:]
    collect(pathlib.Path(root), pathlib.Path(output), raw_id, token, phase)


def diagnostic(error):
    # A bounded class name and message with every path-like token masked, so a
    # failed owned step is attributable without private locations.
    text = re.sub(r"\S*/\S*", "<path>", str(error))
    text = re.sub(r"[^\x20-\x7e]", "?", text)[:200]
    return type(error).__name__ + (": " + text if text else "")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # The started markers distinguish an uncertain step from one that never began.
        raise SystemExit("Owned native ServiceMap holder step failed: " + diagnostic(error)) from None
