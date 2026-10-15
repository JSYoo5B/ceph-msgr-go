#!/usr/bin/env python3
"""Test-only native watcher registration and listwatchers CLI oracle.

Uses pinned Tentacle's Python librados watch3 binding and CLI, independently
of Go. Targets only the native OMAP helper's exclusively created object.
Token-scoped receipts and logs retain each registration scope across reruns.
A token-scoped stop file and a five-minute observation deadline bound the worker;
no product dependency or automatic registration recovery is implemented here.
"""
import argparse
from contextlib import contextmanager
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


def scope_name(token, suffix):
    if not isinstance(token, str) or not re.fullmatch("[0-9a-f]{32}", token):
        raise ValueError("invalid watcher ownership token")
    return NAME + "-" + token + "-" + suffix


def validate_scope(status, metadata):
    if (not isinstance(status, dict) or status.get("schema_version") != 1
            or status.get("source_commit") != xattr.SOURCE_COMMIT
            or type(status.get("stopped")) is not bool):
        raise ValueError("invalid watcher scope provenance")
    scope_name(status.get("token"), "status")
    names = writes.check_ownership(status.get("ownership"), metadata)
    if status.get("object") != names[0].decode():
        raise ValueError("watcher object differs from its ownership receipt")
    return status


def current_scope(out, metadata):
    current = validate_scope(omap.read_json(out / (NAME + "-status.json")), metadata)
    status = validate_scope(omap.read_json(out / (scope_name(current["token"], "status") + ".json")), metadata)
    if any(status.get(k) != current.get(k) for k in ("token", "ownership", "object")):
        raise ValueError("watcher current scope differs from its token receipt")
    return status


def publish_status(out, status):
    # Token files retain every scope. The common file is only the current view.
    current = omap.read_json(out / (NAME + "-status.json"))
    if current.get("token") != status["token"] or current.get("ownership") != status["ownership"]:
        raise ValueError("watcher worker no longer owns the current scope")
    # Publish the token receipt last: completed token status is the handoff
    # boundary after which this worker performs no further common-file writes.
    omap.publish(out, NAME + "-status", status)
    omap.publish(out, scope_name(status["token"], "status"), status)


def completed(status):
    after = status.get("native_after_unwatch")
    return (status["stopped"] and not status.get("error") and isinstance(after, dict)
            and after.get("source_commit") == xattr.SOURCE_COMMIT
            and after.get("object") == status["object"] and after.get("entries") == [])


@contextmanager
def control_lock(out):
    # Never remove an existing lock: it may belong to a concurrent invocation.
    path = out / (NAME + "-control.lock")
    # The file's existence owns the lock; Windows requires its handle to be
    # closed before unlinking it when the protected operation finishes.
    with path.open("x"):
        pass
    try:
        yield
    finally:
        path.unlink()


def worker(root, out, token):
    import rados
    metadata, name = owned(root, out)
    status = current_scope(out, metadata)
    if status["token"] != token or status["object"] != name or status["stopped"] or status.get("error"):
        raise ValueError("worker does not own the current watcher object")
    watches = []
    cluster = rados.Rados(name="client.test", conffile=str(root / "ceph.conf"),
                          conf={"keyring": str(root / "keyring")})
    try:
        cluster.connect()
        with cluster.open_ioctx(metadata["pool"]) as io:
            try:
                for timeout in (37, 61):
                    watches.append(io.watch(name, lambda *args: b"", timeout=timeout))
                status.update(global_id=cluster.get_instance_id(),
                              cookies=[w.get_id() for w in watches], timeouts=[37, 61])
                status["native"] = listing(root, out)
                status["ready"] = True
                publish_status(out, status)
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
        try:
            cluster.shutdown()
        except BaseException as error:
            status["error"] = str(error)[:1024]
            raise
        finally:
            publish_status(out, status)


def run(action, root, out):
    metadata, name = owned(root, out)
    if action == "observe":
        omap.publish(out, NAME + "-result", listing(root, out))
        return
    with control_lock(out):
        if action == "start":
            previous = None
            if (out / (NAME + "-status.json")).exists():
                previous = current_scope(out, metadata)
                if not completed(previous):
                    raise ValueError("native watcher scope is active, failed or incomplete")
            # Keep the existing diagnostic log name as an owned relative view.
            # Never replace a regular file or a link outside the previous scope.
            log_view = out / (NAME + "-worker.log")
            if log_view.exists() or log_view.is_symlink():
                if (previous is None or not log_view.is_symlink()
                        or log_view.readlink() != Path(scope_name(previous["token"], "worker.log"))):
                    raise ValueError("watcher diagnostic log is not owned by the previous scope")
            token = uuid.uuid4().hex
            status = dict(schema_version=1, token=token, object=name,
                          source_commit=xattr.SOURCE_COMMIT, stopped=False, ready=False,
                          ownership=dict(schema_version=1, source_commit=xattr.SOURCE_COMMIT,
                                         pool=metadata["pool"], pool_id=metadata["pool_id"],
                                         fixture_object=metadata["object"], namespace="", objects=[name]))
            # Reserve before spawning. Startup failures stay visible and block overlap.
            log_name = scope_name(token, "worker.log")
            status_path = out / (scope_name(token, "status") + ".json")
            if status_path.exists():
                raise ValueError("watcher token scope already exists")
            with (out / log_name).open("xb") as log:
                omap.publish(out, scope_name(token, "status"), status)
                omap.publish(out, NAME + "-status", status)
                temporary_view = out / scope_name(token, "worker-view")
                temporary_view.symlink_to(log_name)
                temporary_view.replace(log_view)
                try:
                    process = subprocess.Popen([sys.executable, __file__, "serve", str(root), str(out), "--token", token],
                                               stdout=log, stderr=log, start_new_session=True)
                    deadline = time.monotonic() + 30
                    while True:
                        status = current_scope(out, metadata)
                        if status.get("error") or status["stopped"]:
                            raise RuntimeError("native watcher did not establish its owned registrations")
                        if status.get("ready"):
                            break
                        if process.poll() is not None or time.monotonic() > deadline:
                            raise RuntimeError("native watcher startup failed")
                        time.sleep(.1)
                except BaseException:
                    (out / (NAME + "-stop-" + token)).touch()
                    raise
            omap.publish(out, NAME + "-result", status)
        else:
            status = current_scope(out, metadata)
            # A previous completed scope can name an older owned object. Stopping
            # an active scope must still target the currently prepared object.
            if status["object"] != name:
                raise ValueError("watcher cleanup ownership differs from current object")
            token = status["token"]
            (out / (NAME + "-stop-" + token)).touch()
            deadline = time.monotonic() + 30
            while not status["stopped"]:
                if time.monotonic() > deadline:
                    raise TimeoutError("native watcher cleanup deadline")
                time.sleep(.1)
                status = current_scope(out, metadata)
            if not completed(status):
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
