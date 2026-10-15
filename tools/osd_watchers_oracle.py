#!/usr/bin/env python3
"""Test-only native watcher registration and listwatchers CLI oracle.

Uses pinned Tentacle's Python librados watch3 binding and CLI, independently
of Go. Targets only the native OMAP helper's exclusively created object.
A token-scoped stop file and a five-minute deadline bound the worker lifetime;
no product dependency or automatic registration recovery is implemented here.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

import osd_omap_native as omap
import osd_omap_write_native as writes
import osd_xattr_native as xattr

NAME = "osd-watchers"


def owned(root, out):
    metadata = omap.read_json(out / "osd-oracle.json")
    version = (out / "daemon-version").read_text().strip()
    if "20.2.4" not in version or xattr.SOURCE_COMMIT not in version:
        raise ValueError("watcher oracle requires pinned Tentacle")
    receipt = omap.read_json(out / (writes.OWNED + ".json"))
    objects = writes.check_ownership(receipt, metadata)
    return metadata, objects[0].decode()


def listing(root, out):
    metadata, name = owned(root, out)
    args = ["rados", "-c", str(root / "ceph.conf"), "-n", "client.test",
            "--keyring", str(root / "keyring"), "-p", metadata["pool"], "listwatchers", name]
    result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    if result.returncode or len(result.stdout) > 65536 or len(result.stderr) > 4096:
        raise RuntimeError("native listwatchers failed or exceeded output limit")
    entries = []
    for line in result.stdout.decode().splitlines():
        match = re.fullmatch(r"watcher=(\S+) client\.(\d+) cookie=(\d+)", line)
        if not match:
            raise ValueError("unexpected pinned listwatchers output")
        entries.append(dict(address=match[1], global_id=int(match[2]), cookie=int(match[3])))
    return dict(source_commit=xattr.SOURCE_COMMIT, object=name, entries=entries,
                cli_stdout=result.stdout.decode())


def worker(root, out, token):
    import rados
    metadata, name = owned(root, out)
    watches = []
    cluster = rados.Rados(name="client.test", conffile=str(root / "ceph.conf"),
                          conf={"keyring": str(root / "keyring")})
    status = dict(token=token, object=name, source_commit=xattr.SOURCE_COMMIT, stopped=False)
    try:
        cluster.connect()
        with cluster.open_ioctx(metadata["pool"]) as io:
            try:
                for timeout in (37, 61):
                    watches.append(io.watch(name, lambda *args: b"", timeout=timeout))
                status.update(global_id=cluster.get_instance_id(),
                              cookies=[w.get_id() for w in watches], timeouts=[37, 61])
                status["native"] = listing(root, out)
                omap.publish(out, NAME + "-status", status)
                deadline = time.monotonic() + 300
                while not (out / (NAME + "-stop-" + token)).exists():
                    if time.monotonic() > deadline:
                        raise TimeoutError("native watcher worker deadline")
                    for watch in watches:
                        watch.check()
                    time.sleep(.1)
            finally:
                for watch in reversed(watches):
                    watch.close()
                status["native_after_unwatch"] = listing(root, out)
                status["stopped"] = True
    except BaseException as error:
        status["error"] = str(error)[:1024]
        raise
    finally:
        cluster.shutdown()
        omap.publish(out, NAME + "-status", status)


def run(action, root, out):
    owned(root, out)
    if action == "start":
        if (out / (NAME + "-status.json")).exists():
            raise ValueError("native watcher scope already exists")
        token = uuid.uuid4().hex
        with (out / (NAME + "-worker.log")).open("xb") as log:
            process = subprocess.Popen([sys.executable, __file__, "serve", str(root), str(out), "--token", token],
                                       stdout=log, stderr=log, start_new_session=True)
        deadline = time.monotonic() + 30
        while not (out / (NAME + "-status.json")).exists():
            if process.poll() is not None or time.monotonic() > deadline:
                (out / (NAME + "-stop-" + token)).touch()
                raise RuntimeError("native watcher startup failed")
            time.sleep(.1)
        status = omap.read_json(out / (NAME + "-status.json"))
        if status.get("token") != token or status.get("error") or status.get("stopped"):
            raise RuntimeError("native watcher did not establish its owned registrations")
        omap.publish(out, NAME + "-result", status)
    elif action == "observe":
        omap.publish(out, NAME + "-result", listing(root, out))
    else:
        status = omap.read_json(out / (NAME + "-status.json"))
        token = status.get("token", "")
        if not re.fullmatch("[0-9a-f]{32}", token):
            raise ValueError("invalid watcher ownership token")
        (out / (NAME + "-stop-" + token)).touch()
        deadline = time.monotonic() + 30
        while not status.get("stopped"):
            if time.monotonic() > deadline:
                raise TimeoutError("native watcher cleanup deadline")
            time.sleep(.1)
            status = omap.read_json(out / (NAME + "-status.json"))
        if status.get("error") or status["native_after_unwatch"]["entries"]:
            raise RuntimeError("native watcher cleanup failed")
        omap.publish(out, NAME + "-result", status)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "observe", "stop", "serve"))
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument("--token")
    args = parser.parse_args()
    if args.action == "serve":
        if not re.fullmatch("[0-9a-f]{32}", args.token or ""):
            raise ValueError("invalid worker token")
        worker(args.root, args.out, args.token)
    else:
        run(args.action, args.root, args.out)
