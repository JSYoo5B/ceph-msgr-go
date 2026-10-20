#!/usr/bin/env python3
"""Development-only fresh native MonClient hostname/config read oracle.

Each NODE_NAME requires a new native process because Ceph caches its hostname.
Only client_mount_timeout is published; no key or full config map is emitted.
Product Go packages must never invoke or depend on this disposable-fixture tool.
"""

import errno
import json
import os
import pathlib
import subprocess
import sys
import time


HOSTNAMES = ("fixture-host-a", "fixture-host-b", "", " raw-hostname ")
LABELS = {"initial", "subscriptions", "reopen", "changed", "renewal", "learned", "restored"}
TRANSIENT_CONNECT = {errno.ETIMEDOUT, errno.ECONNREFUSED, errno.ECONNRESET,
                     errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ENOTCONN}
RETRY_CONNECT = 75


def read_selected(fixture_root):
    import rados

    if os.environ.get("NODE_NAME") not in HOSTNAMES:
        return 1
    client = rados.Rados(name="client.test", conffile=str(fixture_root / "ceph.conf"),
                         conf={"keyring": str(fixture_root / "keyring")})
    try:
        try:
            client.connect()
        except rados.Error as error:
            if abs(getattr(error, "errno", 0)) in TRANSIENT_CONNECT:
                return RETRY_CONNECT
            return 1
        value = client.conf_get("client_mount_timeout")
        if not isinstance(value, str) or len(value.encode("utf-8")) > 256:
            return 1
        print(json.dumps(value, ensure_ascii=True), flush=True)
        return 0
    finally:
        client.shutdown()


def collect(fixture_root, output, request_id, label):
    deadline = time.monotonic() + 25
    processes, attempts, rows = {}, {}, {}

    def launch(hostname):
        environment = os.environ.copy()
        environment["NODE_NAME"] = hostname
        process = subprocess.Popen(
            [sys.executable, str(pathlib.Path(__file__).resolve()), "--read", str(fixture_root)],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env=environment)
        processes[hostname] = (process, min(time.monotonic() + 12, deadline))
        attempts[hostname] = attempts.get(hostname, 0) + 1

    try:
        for hostname in HOSTNAMES:
            launch(hostname)
        while processes:
            now = time.monotonic()
            if now >= deadline:
                raise ValueError("Native hostname reads exceeded their global bound")
            for hostname, (process, child_deadline) in list(processes.items()):
                code = process.poll()
                if code is None and now < child_deadline:
                    continue
                if code is None:
                    process.kill()
                    process.communicate(timeout=1)
                    code = RETRY_CONNECT
                    payload = b""
                else:
                    payload, _ = process.communicate(timeout=1)
                del processes[hostname]
                if code == RETRY_CONNECT and attempts[hostname] < 2 and time.monotonic() < deadline:
                    # Only independent connection/read probes retry. Fixture
                    # mutations belong to Go tests and are submitted once.
                    launch(hostname)
                    continue
                if code != 0 or len(payload) > 1024:
                    raise ValueError("Native hostname selected read failed")
                value = json.loads(payload)
                if not isinstance(value, str) or len(value.encode("utf-8")) > 256:
                    raise ValueError("Native hostname selected value is invalid")
                rows[hostname] = value
            if processes:
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))
    finally:
        for process, _ in processes.values():
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=1)

    metadata = {"request_id": request_id, "label": label, "rows": rows}
    temporary = output / ("hostname-config-oracle." + str(request_id) + ".tmp")
    temporary.write_text(json.dumps(metadata, ensure_ascii=True, separators=(",", ":")) + "\n",
                         encoding="utf-8")
    os.replace(temporary, output / "hostname-config-oracle.json")


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--read":
        return read_selected(pathlib.Path(sys.argv[2]))
    if len(sys.argv) != 5:
        raise ValueError("Expected fixture root, output directory, request ID, label")
    fixture_root, output, raw_id, label = sys.argv[1:]
    if not raw_id.isascii() or not raw_id.isdecimal() or int(raw_id) <= 0 or label not in LABELS:
        raise ValueError("Invalid native hostname oracle request")
    collect(pathlib.Path(fixture_root), pathlib.Path(output), int(raw_id), label)
    return 0


if __name__ == "__main__":
    try:
        code = main()
    except Exception:
        # Never print native initialization errors, config or credentials.
        raise SystemExit("Native hostname config oracle failed") from None
    raise SystemExit(code)
