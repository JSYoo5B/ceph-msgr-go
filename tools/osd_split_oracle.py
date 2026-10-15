#!/usr/bin/env python3
"""Owned, development-only Tentacle PG split/merge recipe; no product retry.

Pinned 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964 (LGPL-2.1):
OSDMonitor.cc:8593-8757 sets target counts; mgr/DaemonServer.cc:3340-3595
advances actual counts and merges one PG at a time; PG.cc:1926-1988 drops
pre-split-epoch ops without a reply; OSD.cc:7616-7745 dispatches explicit spg.
No upstream implementation is copied. The worker restores 8/8 actual/target/
pending counts after timeout or uncertain configuration results. No daemon
execution has been established merely by adding this recipe or its mock tests.
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

import osd_backoff_oracle as native
import osd_omap_native as omap
import osd_omap_write_native as writes
import osd_xattr_native as xattr

NAME = "osd-split"
PROFILE = "replicated-pg-v8"
COMMAND_SECONDS = 5
STATE_SECONDS = 60
HOLD_SECONDS = 120
RESTORE_SECONDS = 150
MAX_OUTPUT = 1 << 20
FIELDS = ("pg_num", "pg_num_target", "pg_num_pending", "pg_placement_num", "pg_placement_num_target")


class ObservationChanged(RuntimeError):
    """A read-only native observation crossed a map transition."""


def token_name(token, suffix):
    if not isinstance(token, str) or not re.fullmatch("[0-9a-f]{32}", token):
        raise ValueError("invalid split scope token")
    return NAME + "-" + token + "-" + suffix


def request(raw):
    if not isinstance(raw, dict) or set(raw) != {"object"}:
        raise ValueError("split recipe requires one explicit owned object")
    return xattr.identity(raw["object"], "owned object").decode()


def owned(root, out):
    metadata = omap.read_json(out / "osd-oracle.json")
    manifest = omap.read_json(out / "fixture-manifest.json")
    version = (out / "daemon-version").read_text().strip()
    if "20.2.4" not in version or xattr.SOURCE_COMMIT not in version:
        raise ValueError("split recipe requires pinned Tentacle")
    fsid = manifest.get("fsid")
    if (metadata.get("profile") != PROFILE or metadata.get("replicas") != 1
            or metadata.get("pool_pg_num") != 8 or metadata.get("osd_id") != 0
            or metadata.get("pool") != "ceph-msgr-osd-fixture"
            or manifest.get("osd_profile") != PROFILE or manifest.get("osd_replicas") != "1"
            or manifest.get("osd") != "1" or not isinstance(fsid, str)
            or not re.fullmatch("[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", fsid)):
        raise ValueError("split recipe requires its opt-in single-OSD eight-PG fixture")
    ownership = omap.read_json(out / (writes.OWNED + ".json"))
    names = writes.check_ownership(ownership, metadata)
    name = request(omap.read_json(out / (NAME + "-input.json")))
    if name.encode() not in names:
        raise ValueError("split object differs from its UUID ownership receipt")
    return metadata, fsid, ownership, name


def ceph(root, args, json_output=True, timeout=COMMAND_SECONDS):
    return native.ceph(root, args, json_output=json_output, timeout=timeout)


def checked_pool(topology, metadata, fsid):
    if topology.get("fsid") != fsid or type(topology.get("epoch")) is not int or topology["epoch"] <= 0:
        raise ValueError("split cluster FSID or epoch differs")
    osds = topology.get("osds", [])
    if len(osds) != 1 or osds[0].get("osd") != 0 or osds[0].get("up") != 1:
        raise ValueError("split recipe requires exactly its one live OSD")
    pools = [p for p in topology.get("pools", []) if p.get("pool") == metadata["pool_id"]]
    if len(pools) != 1:
        raise ValueError("split owned pool is absent or ambiguous")
    pool = pools[0]
    if (pool.get("pool_name") != metadata["pool"] or pool.get("type") != 1
            or pool.get("size") != 1 or pool.get("min_size") != 1
            or type(pool.get("flags")) is not int or not pool["flags"] & 1 or pool["flags"] & (1 << 5)
            or pool.get("pg_autoscale_mode") != "off"
            or any(type(pool.get(k)) is not int or not 8 <= pool[k] <= 16 for k in FIELDS)
            or pool["pg_num_target"] not in (8, 16) or pool["pg_placement_num_target"] not in (8, 16)
            or pool["pg_placement_num"] > pool["pg_num_pending"]
            or pool["pg_num_pending"] > pool["pg_num"]):
        raise ValueError("split live pool identity, policy or bounded transition differs")
    return pool


def stable(pool, pg_num, pgp_num=None):
    pgp_num = pg_num if pgp_num is None else pgp_num
    return (all(pool.get(k) == pg_num for k in ("pg_num", "pg_num_pending", "pg_num_target"))
            and all(pool.get(k) == pgp_num for k in ("pg_placement_num", "pg_placement_num_target")))


def parse_pg(value, pool_id):
    match = re.fullmatch(r"([0-9]+)\.([0-9a-f]{1,8})", value if isinstance(value, str) else "")
    if not match or int(match[1]) != pool_id:
        raise ValueError("split native PG identity differs from owned pool")
    return int(match[2], 16)


def pg_stats(raw, metadata, count):
    # Both public formatter envelopes wrap the same pinned pg_stats array.
    entries = raw.get("pg_stats") if isinstance(raw, dict) else raw
    if not isinstance(entries, list) or len(entries) > 16:
        raise ValueError("split PG stats schema or count exceeds bound")
    seeds = set()
    clean = True
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("invalid split PG stats entry")
        seed = parse_pg(entry.get("pgid"), metadata["pool_id"])
        if seed in seeds:
            raise ValueError("duplicate split PG stats identity")
        seeds.add(seed)
        clean &= {"active", "clean"} <= set(entry.get("state", "").split("+"))
    return len(entries) == count and seeds == set(range(count)) and clean


def observe(root, metadata, fsid, name, old_pg=None):
    topology = ceph(root, ["osd", "dump"])
    pool = checked_pool(topology, metadata, fsid)
    placement = ceph(root, ["osd", "map", metadata["pool"], name])
    if placement.get("epoch") != topology["epoch"]:
        raise ObservationChanged("split MON placement and dump epochs changed")
    seed = parse_pg(placement.get("pgid"), metadata["pool_id"])
    raw_hash = parse_pg(placement.get("raw_pgid"), metadata["pool_id"])
    if (seed >= pool["pg_num"] or placement.get("objname") != name
            or placement.get("pool_id") != metadata["pool_id"] or placement.get("pool") != metadata["pool"]
            or placement.get("acting_primary") != 0 or placement.get("acting") != [0]):
        raise ValueError("split native placement differs from its owned object/OSD")
    pg = ceph(root, ["pg", placement["pgid"], "query"])
    if (pg.get("info", {}).get("pgid") != placement["pgid"] or type(pg.get("epoch")) is not int
            or pg.get("acting") != [0]):
        raise ValueError("split actual PG query or applied epoch differs")
    if pg["epoch"] != topology["epoch"]:
        raise ObservationChanged("split OSD applied epoch changed during observation")
    all_pgs = ceph(root, ["pg", "ls-by-pool", metadata["pool"]])
    result = dict(pool=pool, placement=placement, pg=pg, epoch=topology["epoch"], raw_hash=raw_hash,
                  all_active_clean=pg_stats(all_pgs, metadata, pool["pg_num"]))
    if old_pg is not None and old_pg != placement["pgid"]:
        parent = ceph(root, ["pg", old_pg, "query"])
        if parent.get("info", {}).get("pgid") != old_pg or parent.get("acting") != [0]:
            raise ValueError("split parent PG query identity differs")
        result["parent_pg"] = parent
    return result


def stale_request(raw, status, metadata):
    fields = {"global_id", "transaction", "client_incarnation", "map_epoch", "pool", "pg_seed", "raw_hash"}
    if not isinstance(raw, dict) or set(raw) != fields:
        raise ValueError("split stale reception input fields differ")
    for key, maximum in (("global_id", (1 << 63) - 1), ("transaction", (1 << 64) - 1),
                         ("client_incarnation", (1 << 32) - 1), ("map_epoch", (1 << 32) - 1),
                         ("pool", (1 << 63) - 1), ("pg_seed", (1 << 32) - 1), ("raw_hash", (1 << 32) - 1)):
        if type(raw[key]) is not int or not 0 <= raw[key] <= maximum:
            raise ValueError("split stale identity exceeds bounds")
    before = status["before"]
    if (not raw["global_id"] or not raw["transaction"] or not raw["map_epoch"]
            or raw["map_epoch"] > before["pg"]["epoch"] or raw["pool"] != metadata["pool_id"]
            or raw["pg_seed"] != parse_pg(before["placement"]["pgid"], metadata["pool_id"])
            or raw["raw_hash"] != before["raw_hash"] or not status.get("ready")
            or status["split"].get("parent_pg", {}).get("info", {}).get("history", {}).get("last_epoch_split", 0) <= raw["map_epoch"]):
        raise ValueError("split stale identity is not the pre-split tuple of this scope")
    return dict(raw)


def parse_stale_history(history, expected, token):
    if not isinstance(history, dict) or not isinstance(history.get("ops"), list) or len(history["ops"]) > 4096:
        raise ValueError("split native history schema/count differs")
    pattern = re.compile(r"osd_op\(client\.([0-9]+)\.(-?[0-9]+):([0-9]+) ([0-9]+\.[0-9a-f]+) ([0-9]+\.[0-9a-f]+) \(undecoded\) [^\r\n]{0,8192} e([0-9]+)\)")
    matches = []
    for op in history["ops"]:
        if not isinstance(op, dict):
            raise ValueError("invalid split native history entry")
        description = op.get("description", "")
        if not isinstance(description, str) or len(description) > 16384:
            continue
        match = pattern.fullmatch(description)
        if not match or (int(match[1]), int(match[2]) & 0xffffffff, int(match[3]), match[4], match[5], int(match[6])) != (
                expected["global_id"], expected["client_incarnation"], expected["transaction"],
                f"{expected['pool']}.{expected['pg_seed']:x}", f"{expected['pool']}.{expected['raw_hash']:x}", expected["map_epoch"]):
            continue
        data = op.get("type_data", {})
        client = data.get("client_info", {}) if isinstance(data, dict) else {}
        events = data.get("events", []) if isinstance(data, dict) else []
        if (not isinstance(client, dict) or client.get("client") != "client." + str(expected["global_id"])
                or type(client.get("tid")) is not int or client["tid"] != expected["transaction"]
                or not isinstance(events, list) or len(events) > 128
                or not any(isinstance(e, dict) and e.get("event") == "reached_pg" for e in events)):
            continue
        matches.append(dict(expected, token=token, source_commit=xattr.SOURCE_COMMIT, source_type=8,
                            source_number=expected["global_id"], osd_id=0, reached_pg=True, body_decoded=False,
                            evidence="native completed history; stale envelope reception only; execution remains unknown"))
    if len(matches) > 1:
        raise ValueError("ambiguous duplicate native stale request identities")
    return matches[0] if matches else None


def observe_stale(root, out, metadata, status):
    path = out / (NAME + "-stale-input.json")
    if not path.exists():
        return None
    expected = stale_request(omap.read_json(path), status, metadata)
    token = status["token"]
    omap.publish(out, token_name(token, "stale-input"), expected)
    cached = out / (token_name(token, "stale-receipt") + ".json")
    if cached.exists():
        receipt = omap.read_json(cached)
        if (any(receipt.get(k) != v for k, v in expected.items()) or receipt.get("token") != token
                or receipt.get("source_commit") != xattr.SOURCE_COMMIT or receipt.get("reached_pg") is not True
                or receipt.get("body_decoded") is not False or receipt.get("source_type") != 8
                or receipt.get("source_number") != expected["global_id"] or receipt.get("osd_id") != 0
                or receipt.get("evidence") != "native completed history; stale envelope reception only; execution remains unknown"):
            raise ValueError("split stale cached receipt differs from scope")
        return receipt
    deadline = time.monotonic() + 10
    while True:
        receipt = parse_stale_history(ceph(root, ["tell", "osd.0", "dump_historic_ops"], timeout=2), expected, token)
        if receipt is not None:
            omap.publish(out, token_name(token, "stale-receipt"), receipt)
            omap.publish(out, NAME + "-stale-receipt", receipt)
            return receipt
        if time.monotonic() >= deadline:
            raise TimeoutError("exact native stale-parent reception was not observed")
        time.sleep(.1)


def archive_previous_stale(out, previous, metadata):
    path = out / (NAME + "-stale-input.json")
    if path.exists():
        if previous is None:
            raise ValueError("split stale input has no previous owned scope")
        expected = stale_request(omap.read_json(path), previous, metadata)
        omap.publish(out, token_name(previous["token"], "stale-input"), expected)
        # Only validated prior-scope input is retired. Native historic entries
        # retained by the daemon cannot be attributed to the next scope token.
        path.unlink()


def object_state(root, out, name):
    child = subprocess.run([sys.executable, __file__, "snapshot", str(root), str(out), "--object", name],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=COMMAND_SECONDS)
    if child.returncode or len(child.stdout) > MAX_OUTPUT or len(child.stderr) > 4096:
        raise RuntimeError("split native object observation failed or exceeded bound")
    result = json.loads(child.stdout)
    if not isinstance(result, dict) or result.get("code") != 0:
        raise ValueError("split native object observation lacks existing head")
    return result


def wait_state(root, metadata, fsid, name, pg_num, pgp_num=None, old_pg=None):
    deadline = time.monotonic() + STATE_SECONDS
    while True:
        try:
            result = observe(root, metadata, fsid, name, old_pg)
            if stable(result["pool"], pg_num, pgp_num) and result["all_active_clean"]:
                return result
        except (RuntimeError, subprocess.TimeoutExpired):
            # Repeat only bounded read observations, never a mutation command.
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError("split actual/target/pending PG transition did not settle active+clean")
        time.sleep(.2)


def wait_pgp_eight(root, metadata, fsid):
    deadline = time.monotonic() + STATE_SECONDS
    while True:
        pool = checked_pool(ceph(root, ["osd", "dump"]), metadata, fsid)
        if pool["pg_placement_num"] == pool["pg_placement_num_target"] == 8:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("split original actual PGP count did not restore")
        time.sleep(.2)


def set_pool(root, metadata, fsid, field, value):
    if field not in {"pg_num", "pgp_num"} or value not in {8, 16}:
        raise ValueError("split recipe admits only fixed eight/sixteen PG targets")
    checked_pool(ceph(root, ["osd", "dump"]), metadata, fsid)
    ceph(root, ["osd", "pool", "set", metadata["pool"], field, str(value)], json_output=False)


def current(out, metadata, fsid, ownership, name):
    view = omap.read_json(out / (NAME + "-status.json"))
    status = omap.read_json(out / (token_name(view.get("token"), "status") + ".json"))
    if (any(view.get(k) != status.get(k) for k in ("token", "object", "ownership", "fsid", "source_commit"))
            or status.get("schema_version") != 1 or status.get("source_commit") != xattr.SOURCE_COMMIT
            or status.get("fsid") != fsid or status.get("ownership") != ownership or status.get("object") != name
            or any(type(status.get(k)) is not bool for k in ("ready", "done", "restored"))
            or not stable(status.get("before", {}).get("pool", {}), 8)):
        raise ValueError("split token/current ownership or original policy differs")
    return status


def publish(out, status):
    view = omap.read_json(out / (NAME + "-status.json"))
    if any(view.get(k) != status.get(k) for k in ("token", "ownership", "fsid", "object")):
        raise ValueError("split worker no longer owns current scope")
    omap.publish(out, NAME + "-status", status)
    omap.publish(out, token_name(status["token"], "status"), status)


def restore(root, out, metadata, fsid, status):
    # Snapshot failures never prevent restoring the configuration. These are
    # observations of the caller's *current* state, not an execution receipt.
    errors = []
    try:
        status["pre_restore_object"] = object_state(root, out, status["object"])
    except BaseException as error:
        errors.append("pre-restore object: " + str(error))
    try:
        set_pool(root, metadata, fsid, "pgp_num", 8)
        wait_pgp_eight(root, metadata, fsid)
    except BaseException as error:
        errors.append("pgp target: " + str(error))
    # Always attempt the original pg_num target too: a failed first config
    # result can still have reached MON. MGR owns the bounded merge process.
    try:
        set_pool(root, metadata, fsid, "pg_num", 8)
        status["after"] = wait_state(root, metadata, fsid, status["object"], 8)
        status["after"]["object_state"] = object_state(root, out, status["object"])
        status["after"]["fixture_state"] = object_state(root, out, metadata["object"])
        if status["after"]["object_state"] != status.get("pre_restore_object"):
            raise ValueError("split merge changed the caller's pre-restore object state")
        if status["after"]["fixture_state"] != status["before"]["fixture_state"]:
            raise ValueError("split merge changed the original fixture object")
        original = status["before"]["pool"]
        for key in ("pool", "pool_name", "type", "flags", "crush_rule", "size", "min_size", "pg_autoscale_mode", *FIELDS):
            if status["after"]["pool"].get(key) != original.get(key):
                raise ValueError("split restore changed original pool policy: " + key)
        status["restored"] = True
    except BaseException as error:
        errors.append("PG restore: " + str(error))
    if errors:
        raise RuntimeError("split restoration was incomplete: " + "; ".join(errors)[:2048])


def worker(root, out, token):
    metadata, fsid, ownership, name = owned(root, out)
    status = current(out, metadata, fsid, ownership, name)
    if status["token"] != token or status["done"]:
        raise ValueError("split worker does not own live scope")
    entered = False
    try:
        before = observe(root, metadata, fsid, name)
        if not stable(before["pool"], 8) or not before["all_active_clean"]:
            raise ValueError("split original live policy changed before transition")
        entered = True  # Include uncertain first MON mutation in finally.
        set_pool(root, metadata, fsid, "pg_num", 16)
        set_pool(root, metadata, fsid, "pgp_num", 16)
        status["split"] = wait_state(root, metadata, fsid, name, 16, old_pg=status["before"]["placement"]["pgid"])
        old_seed = parse_pg(status["before"]["placement"]["pgid"], metadata["pool_id"])
        new_seed = parse_pg(status["split"]["placement"]["pgid"], metadata["pool_id"])
        if new_seed != old_seed + 8 or status["split"]["raw_hash"] != status["before"]["raw_hash"]:
            raise ValueError("native object did not move from its parent into its actual split child")
        status["split"]["object_state"] = object_state(root, out, name)
        status["split"]["fixture_state"] = object_state(root, out, metadata["object"])
        if (status["split"]["object_state"] != status["before"]["object_state"]
                or status["split"]["fixture_state"] != status["before"]["fixture_state"]):
            raise ValueError("split changed object bytes/version without a caller mutation")
        status["ready"] = True
        publish(out, status)
        deadline = time.monotonic() + HOLD_SECONDS
        while not (out / (NAME + "-finish-" + token)).exists():
            if time.monotonic() >= deadline:
                status["automatic_restore"] = True
                break
            time.sleep(.2)
    except BaseException as error:
        status["error"] = str(error)[:2048]
        raise
    finally:
        try:
            if entered:
                restore(root, out, metadata, fsid, status)
        except BaseException as error:
            status["error"] = str(error)[:2048]
            raise
        finally:
            status["done"] = True
            publish(out, status)


@contextmanager
def control_lock(out):
    path = out / (NAME + "-control.lock")
    with path.open("x"):
        pass
    try:
        yield
    finally:
        path.unlink()


def run(action, root, out):
    if action not in {"start", "observe", "finish"}:
        raise ValueError("invalid split recipe action")
    metadata, fsid, ownership, name = owned(root, out)
    with control_lock(out):
        if action == "start":
            prior = None
            if (out / (NAME + "-status.json")).exists():
                view = omap.read_json(out / (NAME + "-status.json"))
                prior = omap.read_json(out / (token_name(view.get("token"), "status") + ".json"))
                if (prior != view or not prior.get("done") or not prior.get("restored") or prior.get("error")):
                    raise ValueError("prior split scope is active, failed or unrestored")
            archive_previous_stale(out, prior, metadata)
            before = observe(root, metadata, fsid, name)
            if not stable(before["pool"], 8) or not before["all_active_clean"] or not before["raw_hash"] & 8:
                raise ValueError("split start requires a clean eight-PG owned head that will enter a child")
            before["object_state"] = object_state(root, out, name)
            before["fixture_state"] = object_state(root, out, metadata["object"])
            log_view = out / (NAME + "-worker.log")
            if log_view.exists() or log_view.is_symlink():
                if (prior is None or not log_view.is_symlink()
                        or log_view.readlink() != Path(token_name(prior["token"], "worker.log"))):
                    raise ValueError("split diagnostic log is not owned by the previous scope")
            token = uuid.uuid4().hex
            status = dict(schema_version=1, source_commit=xattr.SOURCE_COMMIT, token=token, fsid=fsid,
                          object=name, ownership=ownership, before=before, ready=False, done=False, restored=False,
                          scope="owned native PG split/merge state observations; no request execution conclusion")
            log_path = out / token_name(token, "worker.log")
            with log_path.open("xb") as log:
                omap.publish(out, token_name(token, "status"), status)
                omap.publish(out, NAME + "-status", status)
                view = out / token_name(token, "worker-view")
                view.symlink_to(token_name(token, "worker.log"))
                view.replace(log_view)
                process = subprocess.Popen([sys.executable, __file__, "serve", str(root), str(out), "--token", token],
                                           stdout=log, stderr=log, start_new_session=True)
                deadline = time.monotonic() + STATE_SECONDS + 15
                while True:
                    status = current(out, metadata, fsid, ownership, name)
                    if status.get("error") or status["done"]:
                        raise RuntimeError("split worker failed before readiness")
                    if status["ready"]:
                        break
                    if process.poll() is not None or time.monotonic() >= deadline:
                        (out / (NAME + "-finish-" + token)).touch()
                        raise TimeoutError("split worker readiness deadline")
                    time.sleep(.2)
        else:
            status = current(out, metadata, fsid, ownership, name)
            if action == "finish":
                (out / (NAME + "-finish-" + status["token"])).touch()
                deadline = time.monotonic() + RESTORE_SECONDS
                while not status["done"]:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("split worker restoration deadline")
                    time.sleep(.2)
                    status = current(out, metadata, fsid, ownership, name)
                if not status["restored"]:
                    raise RuntimeError("split original pool policy was not restored")
        if status.get("error"):
            raise RuntimeError("split scope failed: " + status["error"])
        if action == "observe":
            receipt = observe_stale(root, out, metadata, status)
            if receipt is not None:
                status = dict(status, stale_receipt=receipt)
        omap.publish(out, NAME + "-result", status)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "observe", "finish", "serve", "snapshot"))
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument("--token")
    parser.add_argument("--object")
    args = parser.parse_args()
    if args.action == "serve":
        token_name(args.token, "status")
        worker(args.root, args.out, args.token)
    elif args.action == "snapshot":
        metadata, _, ownership, name = owned(args.root, args.out)
        if args.object not in {name, metadata["object"]}:
            raise ValueError("split snapshot is outside its owned head or original fixture")
        print(json.dumps(native.native_object_state(args.root, metadata, args.object)), flush=True)
    else:
        run(args.action, args.root, args.out)
