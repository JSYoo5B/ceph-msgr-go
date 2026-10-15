#!/usr/bin/env python3
"""Disposable-only native PG state recipe for actual OSD backoff acceptance.

Pinned Tentacle 20.2.4, 7f793731f1b39eb4f465e960113d2363c311b964:
PrimaryLogPG.cc:1868-1901 sends PG-wide BLOCK for peered/inactive PGs;
PG.cc:570-657,1423-1429 sends BLOCK/UNBLOCK and releases on activation.
PeeringState.cc:4559-4606 exposes the actual OSD map epoch in `pg query`.
OSDMonitor.cc:8517-8580 resets min_size on size changes and requires an
explicit confirmation to restore size one. Objecter::_op_should_pause handles
pauserd client-side, so that flag is deliberately not used here.

This helper changes only its owned, single-OSD disposable fixture pool from
size/min_size 1/1 to 2/2, and restores the recorded values in finally. A detached
worker also restores after a bounded observation deadline if the Go test dies.
It does not create or decode backoff messages: actual BLOCK/ACK/UNBLOCK evidence
must come from the Go connection and daemon, separately from these state receipts.
Native Ceph CLI/librados dependencies are development-only. No upstream code is
copied; referenced Ceph implementation sources carry LGPL-2.1 notices.
"""
import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

import osd_omap_native as omap
import osd_omap_write_native as writes
import osd_xattr_native as xattr

NAME = "osd-backoff"
PROFILES = ("replicated-pg-v8", "replicated-pg-reply-v8")
MAX_OUTPUT = 1 << 20
HOLD_SECONDS = 180
STATE_SECONDS = 20
COMMAND_SECONDS = 5


def token_name(token, suffix):
    if not isinstance(token, str) or not re.fullmatch("[0-9a-f]{32}", token):
        raise ValueError("invalid backoff scope token")
    return NAME + "-" + token + "-" + suffix


def request(raw):
    if not isinstance(raw, dict) or set(raw) != {"object"}:
        raise ValueError("backoff oracle requires one explicit owned object")
    return xattr.identity(raw["object"], "owned object").decode()


def owned(root, out):
    metadata = omap.read_json(out / "osd-oracle.json")
    manifest = omap.read_json(out / "fixture-manifest.json")
    version = (out / "daemon-version").read_text().strip()
    if "20.2.4" not in version or xattr.SOURCE_COMMIT not in version:
        raise ValueError("backoff oracle requires pinned Tentacle")
    if (metadata.get("profile") not in PROFILES or metadata.get("replicas") != 1
            or metadata.get("pool_pg_num") != 8 or metadata.get("osd_id") != 0
            or metadata.get("pool") != "ceph-msgr-osd-fixture"
            or manifest.get("osd_profile") != metadata.get("profile") or manifest.get("osd_replicas") != "1"
            or manifest.get("osd") != "1" or not isinstance(manifest.get("fsid"), str)
            or not re.fullmatch("[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", manifest["fsid"])):
        raise ValueError("backoff recipe requires the opt-in single-OSD replicated-PG fixture")
    ownership = omap.read_json(out / (writes.OWNED + ".json"))
    names = writes.check_ownership(ownership, metadata)
    return metadata, manifest["fsid"], ownership, names[0].decode()


def ceph(root, arguments, json_output=True, timeout=COMMAND_SECONDS):
    args = ["ceph", "-c", str(root / "ceph.conf"), "-n", "client.test",
            "--keyring", str(root / "keyring")] + list(arguments)
    if json_output:
        args += ["--format", "json"]
    result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if result.returncode or len(result.stdout) > MAX_OUTPUT or len(result.stderr) > 4096:
        raise RuntimeError("native backoff fixture command failed or exceeded output bounds")
    return json.loads(result.stdout) if json_output else None


def checked_pool(topology, metadata, fsid):
    if topology.get("fsid") != fsid or type(topology.get("epoch")) is not int or topology["epoch"] <= 0:
        raise ValueError("backoff cluster identity or epoch differs from fixture ownership")
    osds = topology.get("osds", [])
    if len(osds) != 1 or osds[0].get("osd") != 0 or osds[0].get("up") != 1:
        raise ValueError("backoff recipe requires exactly its one live owned OSD")
    pools = [p for p in topology.get("pools", []) if p.get("pool") == metadata["pool_id"]]
    if len(pools) != 1:
        raise ValueError("owned backoff pool is missing or ambiguous")
    pool = pools[0]
    if (pool.get("pool_name") != metadata["pool"] or pool.get("type") != 1
            or pool.get("pg_num") != 8 or pool.get("pg_placement_num") != 8
            or type(pool.get("flags")) is not int or not pool["flags"] & 1
            or (pool.get("size"), pool.get("min_size")) not in {(1, 1), (2, 1), (2, 2)}):
        raise ValueError("backoff pool identity, profile or owned size transition differs")
    return pool


def observe(root, metadata, fsid, name):
    topology = ceph(root, ["osd", "dump"])
    pool = checked_pool(topology, metadata, fsid)
    placement = ceph(root, ["osd", "map", metadata["pool"], name])
    pgid = placement.get("pgid", "")
    matched = re.fullmatch(r"([0-9]+)\.([0-9a-f]+)", pgid)
    if (not matched or int(matched[1]) != metadata["pool_id"] or int(matched[2], 16) >= 8
            or placement.get("pool_id") != metadata["pool_id"] or placement.get("pool") != metadata["pool"]
            or placement.get("objname") != name or placement.get("acting_primary") != 0):
        raise ValueError("backoff object placement differs from owned pool or OSD")
    pg = ceph(root, ["pg", pgid, "query"])
    if (not isinstance(pg, dict) or pg.get("info", {}).get("pgid") != pgid
            or not isinstance(pg.get("state"), str) or type(pg.get("epoch")) is not int
            or pg["epoch"] < topology["epoch"] or not isinstance(pg.get("acting"), list)
            or not pg["acting"] or pg["acting"][0] != 0):
        raise ValueError("backoff PG query identity or applied epoch differs")
    return dict(pool=pool, placement=placement, pg=pg, epoch=topology["epoch"])


def ack_request(raw, status):
    if not isinstance(raw, dict) or set(raw) != {"global_id", "id", "map_epoch", "pgid"}:
        raise ValueError("native backoff ACK input fields differ")
    for field, minimum, maximum in (("global_id", 1, (1 << 63) - 1), ("id", 0, (1 << 64) - 1),
                                     ("map_epoch", 1, (1 << 32) - 1)):
        if type(raw[field]) is not int or not minimum <= raw[field] <= maximum:
            raise ValueError("native backoff ACK identity exceeds bounds")
    inactive = status.get("inactive", {})
    if (raw["pgid"] != inactive.get("placement", {}).get("pgid")
            or raw["map_epoch"] != inactive.get("pg", {}).get("epoch")):
        raise ValueError("native backoff ACK does not belong to current inactive PG/epoch")
    return dict(raw)


def parse_ack_history(history, expected, token):
    # MOSDBackoff::print includes its actual decoded wire PG/op/ID/epoch.
    # OpRequest::_dump independently attributes the original client entity;
    # reached_pg is recorded by OSD::dequeue_op before the PG handler runs.
    # Do not retain arbitrary descriptions, hobject bounds or client addresses.
    if not isinstance(history, dict) or not isinstance(history.get("ops"), list) or len(history["ops"]) > 4096:
        raise ValueError("native completed operation history exceeds schema or count bound")
    matches = []
    pattern = re.compile(r"osd_backoff\(([0-9]{1,19}\.[0-9a-f]{1,8}) ack-block id ([0-9]{1,20}) \[[^\r\n]{0,8192}\) e([0-9]{1,10})\)")
    for op in history["ops"]:
        if not isinstance(op, dict):
            raise ValueError("invalid native completed history entry")
        description = op.get("description")
        if not isinstance(description, str) or len(description) > 16384:
            continue
        match = pattern.fullmatch(description)
        if not match or (match[1], int(match[2]), int(match[3])) != (expected["pgid"], expected["id"], expected["map_epoch"]):
            continue
        data = op.get("type_data", {})
        if not isinstance(data, dict):
            continue
        client = data.get("client_info", {})
        if not isinstance(client, dict) or client.get("client") != "client." + str(expected["global_id"]):
            continue
        events = data.get("events", [])
        if not isinstance(events, list) or len(events) > 128:
            raise ValueError("native ACK event count exceeds bound")
        if not any(isinstance(event, dict) and event.get("event") == "reached_pg" for event in events):
            continue
        matches.append(dict(expected, token=token, source_type=8, source_number=expected["global_id"],
                            osd_id=0, op=2, reached_pg=True, source_commit=xattr.SOURCE_COMMIT,
                            evidence="native completed op history; reception only"))
    if len(matches) > 1:
        raise ValueError("native ACK history has ambiguous duplicate matching identities")
    return matches[0] if matches else None


def observe_ack(root, out, status):
    input_path = out / (NAME + "-ack-input.json")
    if not input_path.exists():
        return None
    expected = ack_request(omap.read_json(input_path), status)
    token = status["token"]
    omap.publish(out, token_name(token, "ack-input"), expected)
    cached_path = out / (token_name(token, "ack-receipt") + ".json")
    if cached_path.exists():
        cached = omap.read_json(cached_path)
        if (any(cached.get(k) != v for k, v in expected.items()) or cached.get("token") != token
                or cached.get("source_commit") != xattr.SOURCE_COMMIT or cached.get("op") != 2
                or cached.get("reached_pg") is not True or cached.get("source_type") != 8
                or cached.get("source_number") != expected["global_id"] or cached.get("osd_id") != 0
                or cached.get("evidence") != "native completed op history; reception only"):
            raise ValueError("native ACK cached receipt differs from current scope identity")
        return cached
    deadline = time.monotonic() + 10
    while True:
        # No daemon log/config mutation is needed. Discard the unfiltered native
        # JSON after extracting only the exact public ACK identity and event.
        history = ceph(root, ["tell", "osd.0", "dump_historic_ops"], timeout=2)
        receipt = parse_ack_history(history, expected, token)
        if receipt is not None:
            omap.publish(out, token_name(token, "ack-receipt"), receipt)
            omap.publish(out, NAME + "-ack-receipt", receipt)
            return receipt
        if time.monotonic() > deadline:
            raise TimeoutError("exact native original-epoch ACK reception was not observed")
        time.sleep(.1)


def archive_previous_ack(out, previous):
    path = out / (NAME + "-ack-input.json")
    if path.exists():
        if previous is None:
            raise ValueError("backoff ACK input has no previous owned scope")
        expected = ack_request(omap.read_json(path), previous)
        omap.publish(out, token_name(previous["token"], "ack-input"), expected)
        # Remove only this validated, previous-scope oracle input. The retained
        # token copy and matching ACK receipt remain available for diagnostics.
        path.unlink()


def native_object_state(root, metadata, name):
    native = xattr.Native()
    with xattr.client_connection(native, root, metadata, "test") as io:
        result = native.snapshot(io, name.encode(), [])
    if result["code"] != 0:
        raise ValueError("backoff recipe requires its existing owned head object")
    return result


def object_state(root, metadata, name, out):
    # Bound the independent librados observation at the process boundary too:
    # native read waits must not hold up automatic configuration restoration.
    observed = subprocess.run([sys.executable, __file__, "snapshot", str(root), str(out), "--object", name],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=COMMAND_SECONDS)
    if observed.returncode or len(observed.stdout) > MAX_OUTPUT or len(observed.stderr) > 4096:
        raise RuntimeError("native backoff object observation failed or exceeded bounds")
    result = json.loads(observed.stdout)
    if not isinstance(result, dict) or result.get("code") != 0:
        raise ValueError("backoff native object observation differs")
    return result


def wait_state(root, metadata, fsid, name, active):
    deadline = time.monotonic() + STATE_SECONDS
    while True:
        result = observe(root, metadata, fsid, name)
        flags = set(result["pg"]["state"].split("+"))
        ready = "active" in flags if active else "peered" in flags and "active" not in flags
        if ready:
            return result
        if time.monotonic() > deadline:
            raise TimeoutError("owned PG did not reach requested backoff fixture state")
        time.sleep(.1)


def set_pool(root, metadata, fsid, field, value):
    # Check the live pool ID/FSID/profile before every mutation, including cleanup.
    checked_pool(ceph(root, ["osd", "dump"]), metadata, fsid)
    args = ["osd", "pool", "set", metadata["pool"], field, str(value)]
    if field == "size" and value == 1:
        args.append("--yes-i-really-mean-it")
    ceph(root, args, json_output=False)


def current(out, metadata, fsid):
    view = omap.read_json(out / (NAME + "-status.json"))
    status = omap.read_json(out / (token_name(view.get("token"), "status") + ".json"))
    if (any(status.get(k) != view.get(k) for k in ("token", "object", "ownership", "fsid", "source_commit"))
            or status.get("schema_version") != 1 or status.get("source_commit") != xattr.SOURCE_COMMIT
            or status.get("fsid") != fsid or type(status.get("ready")) is not bool
            or type(status.get("done")) is not bool or type(status.get("restored")) is not bool):
        raise ValueError("backoff current/token scope identity differs")
    names = writes.check_ownership(status.get("ownership"), metadata)
    if status.get("object") != names[0].decode():
        raise ValueError("backoff scope object differs from ownership")
    original = status.get("before", {}).get("pool", {})
    if (original.get("size"), original.get("min_size")) != (1, 1):
        raise ValueError("backoff original pool policy differs from single-OSD ownership")
    return status


def publish(out, status):
    held = omap.read_json(out / (NAME + "-status.json"))
    if any(held.get(k) != status.get(k) for k in ("token", "ownership", "fsid", "object")):
        raise ValueError("backoff worker lost its current fixture scope")
    # The token file is authoritative and written last, after the common view.
    omap.publish(out, NAME + "-status", status)
    omap.publish(out, token_name(status["token"], "status"), status)


def restore(root, metadata, fsid, status, out):
    original = status["before"]["pool"]
    errors = []
    # Lower min_size first, then restore size. size resets min_size natively,
    # so the recorded original min_size must be explicitly restored last.
    for field, value in (("min_size", original["min_size"]), ("size", original["size"]),
                         ("min_size", original["min_size"])):
        try:
            set_pool(root, metadata, fsid, field, value)
        except BaseException as error:
            errors.append(str(error)[:512])
    try:
        status["after"] = wait_state(root, metadata, fsid, status["object"], active=True)
        pool = status["after"]["pool"]
        if any(pool.get(k) != original.get(k) for k in ("pool", "pool_name", "size", "min_size", "pg_num", "pg_placement_num", "flags")):
            raise ValueError("backoff restoration differs from recorded original pool policy")
        status["after"]["object_state"] = object_state(root, metadata, status["object"], out)
        if status["after"]["object_state"] != status["before"]["object_state"]:
            raise ValueError("backoff pool-state recipe changed owned object bytes or version")
        status["restored"] = True
    except BaseException as error:
        errors.append(str(error)[:512])
    if errors:
        raise RuntimeError("backoff cleanup failed: " + "; ".join(errors))


def worker(root, out, token):
    metadata, fsid, ownership, name = owned(root, out)
    status = current(out, metadata, fsid)
    if status["token"] != token or status["object"] != name or status["ownership"] != ownership or status["done"]:
        raise ValueError("backoff worker does not own current fixture object")
    entered = False
    try:
        before = observe(root, metadata, fsid, name)
        if (before["pool"]["size"], before["pool"]["min_size"]) != (1, 1):
            raise ValueError("backoff original pool policy changed before activation")
        entered = True  # Restore even if the first command's result is uncertain.
        set_pool(root, metadata, fsid, "size", 2)
        set_pool(root, metadata, fsid, "min_size", 2)
        status["inactive"] = wait_state(root, metadata, fsid, name, active=False)
        if (status["inactive"]["pool"]["size"], status["inactive"]["pool"]["min_size"]) != (2, 2):
            raise ValueError("backoff inactive policy differs from controlled transition")
        status["ready"] = True
        publish(out, status)
        deadline = time.monotonic() + HOLD_SECONDS
        while not (out / (NAME + "-finish-" + token)).exists():
            if time.monotonic() > deadline:
                status["automatic_restore"] = True
                break
            time.sleep(.1)
    except BaseException as error:
        status["error"] = str(error)[:1024]
        raise
    finally:
        try:
            if entered:
                restore(root, metadata, fsid, status, out)
        except BaseException as error:
            status["error"] = str(error)[:2048]
            raise
        finally:
            status["done"] = True
            publish(out, status)


@contextmanager
def control_lock(out):
    lock = out / (NAME + "-control.lock")
    with lock.open("x"):
        pass
    try:
        yield
    finally:
        lock.unlink()


def run(action, root, out):
    if action not in {"start", "observe", "finish"}:
        raise ValueError("unsupported backoff oracle action")
    started_deadline = time.monotonic() + 60
    metadata, fsid, ownership, name = owned(root, out)
    with control_lock(out):
        if action == "start":
            previous = None
            if (out / (NAME + "-status.json")).exists():
                previous = current(out, metadata, fsid)
                if not previous["done"] or not previous["restored"] or previous.get("error"):
                    raise ValueError("previous backoff scope remains active, failed or unrestored")
            archive_previous_ack(out, previous)
            if request(omap.read_json(out / (NAME + "-input.json"))) != name:
                raise ValueError("backoff input is outside the fixture's first owned object")
            before = observe(root, metadata, fsid, name)
            if ((before["pool"]["size"], before["pool"]["min_size"]) != (1, 1)
                    or "active" not in before["pg"]["state"].split("+")):
                raise ValueError("backoff recipe requires a previously active size/min_size 1/1 pool")
            before["object_state"] = object_state(root, metadata, name, out)
            log_view = out / (NAME + "-worker.log")
            if log_view.exists() or log_view.is_symlink():
                if (previous is None or not log_view.is_symlink()
                        or log_view.readlink() != Path(token_name(previous["token"], "worker.log"))):
                    raise ValueError("backoff diagnostic log is not owned by the previous scope")
            token = uuid.uuid4().hex
            if (out / (token_name(token, "status") + ".json")).exists():
                raise ValueError("backoff token receipt already exists")
            status = dict(schema_version=1, token=token, source_commit=xattr.SOURCE_COMMIT,
                          object=name, fsid=fsid, ownership=ownership, ready=False, done=False,
                          restored=False, before=before, hold_seconds=HOLD_SECONDS,
                          scope="owned single-OSD pool state only; Go must independently observe backoff messages")
            with (out / token_name(token, "worker.log")).open("xb") as log:
                omap.publish(out, token_name(token, "status"), status)
                omap.publish(out, NAME + "-status", status)
                view = out / token_name(token, "worker-view")
                view.symlink_to(token_name(token, "worker.log"))
                view.replace(log_view)
                try:
                    process = subprocess.Popen([sys.executable, __file__, "serve", str(root), str(out), "--token", token],
                                               stdout=log, stderr=log, start_new_session=True)
                    deadline = started_deadline
                    while True:
                        status = current(out, metadata, fsid)
                        if status.get("error") or status["done"]:
                            raise RuntimeError("backoff fixture worker failed during startup")
                        if status["ready"]:
                            break
                        if process.poll() is not None or time.monotonic() > deadline:
                            raise TimeoutError("backoff fixture startup deadline")
                        time.sleep(.1)
                except BaseException:
                    (out / (NAME + "-finish-" + token)).touch()
                    raise
        else:
            status = current(out, metadata, fsid)
            if status["object"] != name or status["ownership"] != ownership:
                raise ValueError("backoff completion object differs from current ownership")
            if action == "finish":
                (out / (NAME + "-finish-" + status["token"])).touch()
                deadline = time.monotonic() + 75
                while not status["done"]:
                    if time.monotonic() > deadline:
                        raise TimeoutError("backoff fixture restoration deadline")
                    time.sleep(.1)
                    status = current(out, metadata, fsid)
                if not status["restored"]:
                    raise RuntimeError("backoff fixture did not restore its original pool policy")
        if status.get("error"):
            raise RuntimeError("backoff fixture scope failed: " + str(status["error"]))
        if action in {"observe", "finish"}:
            receipt = observe_ack(root, out, status)
            if receipt is not None:
                status = dict(status, ack_receipt=receipt)
        omap.publish(out, NAME + "-result", status)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "observe", "finish", "serve", "snapshot"))
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument("--token")
    parser.add_argument("--object")
    args = parser.parse_args()
    if args.action == "snapshot":
        metadata, _, _, name = owned(args.root, args.out)
        if args.object != name:
            raise ValueError("native snapshot is outside the owned backoff object")
        print(json.dumps(native_object_state(args.root, metadata, name)), flush=True)
    elif args.action == "serve":
        token_name(args.token, "status")
        worker(args.root, args.out, args.token)
    else:
        run(args.action, args.root, args.out)
