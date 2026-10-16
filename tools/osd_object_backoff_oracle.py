#!/usr/bin/env python3
"""Disposable-only native recipe for object-scope OSD backoff and failover.

Pinned Tentacle 20.2.4, 7f793731f1b39eb4f465e960113d2363c311b964:

- PrimaryLogPG.cc:2206-2234 (do_op): a write to an object that is degraded on
  an acting replica calls add_backoff(session, head, head) only when the
  connection negotiated RADOS_BACKOFF and osd_backoff_on_degraded is true. The
  default (common/options/global.yaml.in:3237-3242, false) parks the op in
  waiting_for_degraded_object instead. Unfound objects back off by default.
  Both branches call maybe_kick_recovery, which recovers that one object even
  while the cluster-wide norecover flag is set (PrimaryLogPG.cc:618-643).
- PG.cc:570-594 sends BLOCK with begin == end == head at the PG's current map
  epoch; PrimaryLogPG.cc:505-523 (on_global_recover) calls release_backoffs(soid)
  and PG.cc:596-657 sends UNBLOCK with the original bounds and the current epoch.
- Objecter.cc:3835-3920 ACKs with the original epoch and resends ops contained
  in the released range; Objecter.cc:3434-3469 holds and logs blocked sends.

The recipe holds the recovery push by pausing (cgroup freezing) the stale
replica's container. That is performed by the Go test through the fixture's
own daemon control, not here. This helper only changes cluster-wide
noout/norecover/nobackfill flags, osd_heartbeat_grace and
osd_backoff_on_degraded configuration entries of the disposable fixture, and
records their original values so `restore` reinstates exactly those. It writes
only fixture-owned reserved object names. It neither constructs nor decodes
MOSDBackoff: the Go connection and native Objecter debug log supply that.
Native Ceph CLI/rados dependencies are development-only. No upstream code is
copied; referenced Ceph sources carry LGPL-2.1 notices.
"""
import argparse
import base64
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

import osd_omap_native as omap
import osd_omap_write_native as writes
import osd_xattr_native as xattr

NAME = "osd-object-backoff"
SOURCE_COMMIT = xattr.SOURCE_COMMIT
PROFILES = ("replicated-pg-v8", "replicated-pg-reply-v8")
FLAGS = ("noout", "norecover", "nobackfill")
# (who, option, value while the scope is active)
CONFIG = (("global", "osd_heartbeat_grace", "60"),)
DEGRADED = ("osd", "osd_backoff_on_degraded")
MAX_OUTPUT = 1 << 20
MAX_DATA = 4096
MAX_LOG = 4 << 20
COMMAND_SECONDS = 8
WAIT_SECONDS = 60
# The recipe's heartbeat grace is 60 seconds. A dead primary is reported
# only after that grace, then MON commits a map and the surviving PG peers.
# Keep a distinct DOWN phase budget rather than racing that first report.
DOWN_WAIT_SECONDS = 120
# Also cover the 20-second in-flight poll and 60-second local Go outcome
# wait before the native down observation, plus the completion join.
NATIVE_SECONDS = 360
ACTIONS = ("prepare", "config", "wait-osd", "put", "wait-degraded", "native-start", "native-finish",
           "native-stat", "ops", "ack-history", "restore")

BACKOFF_LINE = re.compile(
    r"^(\S+ \S+) .*handle_osd_backoff osd_backoff\(([0-9]+\.[0-9a-f]+(?:s[0-9]+)?) (block|unblock|ack-block) "
    r"id ([0-9]+) \[([^,\]]*),([^,\)]*)\) e([0-9]+)\)\s*$")
SEND_LINE = re.compile(r"^(\S+ \S+) .*_send_op ([0-9]+) to ([0-9]+\.[0-9a-f]+) on osd\.([0-9]+)\s*$")
HELD_LINE = re.compile(r"^(\S+ \S+) .*_send_op backoff ([0-9]+\.[0-9a-f]+) id ([0-9]+) on .*$")


def scope_path(out, suffix):
    return out / (NAME + "-" + suffix + ".json")


def owned(root, out):
    metadata = omap.read_json(out / "osd-oracle.json")
    manifest = omap.read_json(out / "fixture-manifest.json")
    version = (out / "daemon-version").read_text().strip()
    if "20.2.4" not in version or SOURCE_COMMIT not in version:
        raise ValueError("object backoff oracle requires pinned Tentacle")
    if (metadata.get("profile") not in PROFILES or metadata.get("replicas") != 3 or metadata.get("min_size") != 2
            or metadata.get("pool_pg_num") != 8 or metadata.get("pool") != "ceph-msgr-osd-fixture"
            or manifest.get("osd_profile") != metadata.get("profile") or manifest.get("osd_replicas") != "3"
            or manifest.get("osd_store") != "bluestore" or manifest.get("osd") != "1"
            or not isinstance(manifest.get("fsid"), str)
            or not re.fullmatch("[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", manifest["fsid"])):
        raise ValueError("object backoff recipe requires the opt-in three-BlueStore replicated-PG fixture")
    names = [n.decode() for n in writes.check_ownership(omap.read_json(out / (writes.OWNED + ".json")), metadata)]
    return metadata, manifest["fsid"], names


def owned_object(names, value):
    text = xattr.identity(value, "owned object").decode()
    if text not in names:
        raise ValueError("object is outside the fixture-owned names")
    return text


def osd_id(value):
    if type(value) is not int or not 0 <= value <= 2:
        raise ValueError("OSD identity is outside the three owned OSDs")
    return value


def payload(value):
    if not isinstance(value, str):
        raise ValueError("payload must be base64")
    data = base64.b64decode(value, validate=True)
    if not 1 <= len(data) <= MAX_DATA:
        raise ValueError("payload exceeds bound")
    return data


def base_args(root):
    return ["-c", str(root / "ceph.conf"), "-n", "client.test", "--keyring", str(root / "keyring")]


def ceph(root, arguments, json_output=True, timeout=COMMAND_SECONDS):
    args = ["ceph"] + base_args(root) + list(arguments)
    if json_output:
        args += ["--format", "json"]
    result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if result.returncode or len(result.stdout) > MAX_OUTPUT:
        raise RuntimeError("native ceph %s failed (%d): %s" % (" ".join(arguments[:3]), result.returncode,
                                                                result.stderr[-512:].decode(errors="replace")))
    if not json_output:
        return None
    return json.loads(result.stdout) if result.stdout.strip() else None


def rados(root, pool, arguments, timeout=COMMAND_SECONDS, extra=()):
    args = ["rados"] + base_args(root) + list(extra) + ["-p", pool] + list(arguments)
    return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)


def dump(root, fsid):
    topology = ceph(root, ["osd", "dump"])
    if topology.get("fsid") != fsid or type(topology.get("epoch")) is not int or topology["epoch"] <= 0:
        raise ValueError("cluster identity or epoch differs from fixture ownership")
    osds = {o.get("osd"): dict(up=o.get("up"), **{"in": o.get("in")}) for o in topology.get("osds", [])}
    if set(osds) != {0, 1, 2}:
        raise ValueError("object backoff recipe requires exactly three owned OSDs")
    flags = topology.get("flags", "")
    return dict(epoch=topology["epoch"], osds=osds, flags=sorted(f for f in flags.split(",") if f))


def placement(root, metadata, name):
    value = ceph(root, ["osd", "map", metadata["pool"], name])
    pgid = value.get("pgid", "")
    matched = re.fullmatch(r"([0-9]+)\.([0-9a-f]+)", pgid)
    if (not matched or int(matched[1]) != metadata["pool_id"] or int(matched[2], 16) >= 8
            or value.get("objname") != name or value.get("pool_id") != metadata["pool_id"]):
        raise ValueError("native placement differs from the owned pool")
    return dict(pgid=pgid, raw_pgid=value.get("raw_pgid"), acting=value.get("acting"), up=value.get("up"),
                primary=value.get("acting_primary"), epoch=value.get("epoch"))


def pg_summary(root, pgid):
    value = ceph(root, ["pg", pgid, "query"])
    if not isinstance(value, dict) or value.get("info", {}).get("pgid") != pgid or not isinstance(value.get("state"), str):
        raise ValueError("native PG query identity differs")
    stats = value.get("info", {}).get("stats", {}).get("stat_sum", {})
    return dict(pgid=pgid, state=value["state"], epoch=value.get("epoch"), acting=value.get("acting"),
                up=value.get("up"), degraded=stats.get("num_objects_degraded"),
                missing=stats.get("num_objects_missing_on_primary"), unfound=stats.get("num_objects_unfound"))


def config_entries(root):
    entries = ceph(root, ["config", "dump"])
    wanted = {(w, o) for w, o, _ in CONFIG} | {DEGRADED}
    return {w + "/" + o: e.get("value") for e in entries for w, o in wanted
            if e.get("section") == w and e.get("name") == o}


def load_state(out):
    path = scope_path(out, "state")
    return omap.read_json(path) if path.exists() else None


def save_state(out, state):
    omap.publish(out, NAME + "-state", state)


@contextmanager
def control_lock(out):
    lock = out / (NAME + "-control.lock")
    with lock.open("x"):
        pass
    try:
        yield
    finally:
        lock.unlink()


def wait(predicate, seconds=WAIT_SECONDS, label="native state"):
    deadline = time.monotonic() + seconds
    last = None
    while True:
        try:
            ok, last = predicate()
            if ok:
                return last
        except (RuntimeError, subprocess.TimeoutExpired, ValueError) as error:
            last = str(error)
        if time.monotonic() > deadline:
            raise TimeoutError(label + " not reached: " + json.dumps(last, default=str)[:2048])
        time.sleep(.5)


def active(state):
    parts = state.split("+")
    return "active" in parts and not {"peering", "activating", "down", "incomplete"} & set(parts)


def prepare(root, out, metadata, fsid, names, raw):
    state = load_state(out)
    if state is not None and not state.get("restored"):
        raise ValueError("previous object backoff scope remains unrestored")
    objects = [owned_object(names, n) for n in raw.get("objects", [])]
    if not 1 <= len(objects) <= len(names):
        raise ValueError("prepare requires owned objects")
    before = dump(root, fsid)
    if any(not o["up"] or not o["in"] for o in before["osds"].values()):
        raise ValueError("object backoff recipe requires all three OSDs up and in")
    state = dict(schema_version=1, source_commit=SOURCE_COMMIT, flags=before["flags"], config=config_entries(root),
                 restored=False, native=None)
    save_state(out, state)
    for flag in FLAGS:
        if flag not in state["flags"]:
            ceph(root, ["osd", "set", flag], json_output=False)
    for who, option, value in CONFIG:
        ceph(root, ["config", "set", who, option, value], json_output=False)
    return dict(before=before, after=dump(root, fsid), config=config_entries(root),
                placements={n: placement(root, metadata, n) for n in objects})


def set_degraded(root, raw):
    if type(raw.get("enabled")) is not bool:
        raise ValueError("config requires an explicit boolean")
    if raw["enabled"]:
        ceph(root, ["config", "set", DEGRADED[0], DEGRADED[1], "true"], json_output=False)
    else:
        ceph(root, ["config", "rm", DEGRADED[0], DEGRADED[1]], json_output=False)
    return dict(config=config_entries(root))


def wait_osd(root, metadata, fsid, names, raw):
    target, up = osd_id(raw.get("osd")), raw.get("up")
    name = owned_object(names, raw.get("object"))
    if type(up) is not bool:
        raise ValueError("wait-osd requires explicit up state")

    def reached():
        topology = dump(root, fsid)
        if bool(topology["osds"][target]["up"]) != up:
            return False, topology
        where = placement(root, metadata, name)
        if (target in where["acting"]) != up:
            return False, where
        pg = pg_summary(root, where["pgid"])
        return active(pg["state"]) and pg["epoch"] >= topology["epoch"], dict(topology=topology, placement=where, pg=pg)
    return wait(reached, seconds=WAIT_SECONDS if up else DOWN_WAIT_SECONDS,
                label="OSD %d up=%s and active PG" % (target, up))


def put(root, metadata, names, raw):
    name, data = owned_object(names, raw.get("object")), payload(raw.get("data"))
    source = Path("/tmp") / (NAME + "-put.bin")
    source.write_bytes(data)
    result = rados(root, metadata["pool"], ["put", name, str(source)], timeout=30)
    source.unlink()
    if result.returncode:
        raise RuntimeError("native put failed: " + result.stderr[-512:].decode(errors="replace"))
    return dict(object=name, length=len(data))


def wait_degraded(root, metadata, names, raw):
    name, target = owned_object(names, raw.get("object")), osd_id(raw.get("osd"))

    def reached():
        where = placement(root, metadata, name)
        pg = pg_summary(root, where["pgid"])
        ok = (active(pg["state"]) and "degraded" in pg["state"].split("+") and target in where["acting"]
              and where["primary"] != target and isinstance(pg["degraded"], int) and pg["degraded"] >= 1
              and not pg["unfound"])
        return ok, dict(placement=where, pg=pg)
    return wait(reached, label="degraded object on returned replica")


def native_start(root, out, metadata, names, raw):
    state = load_state(out)
    if state is None or state.get("restored") or state.get("native"):
        raise ValueError("native client requires one active prepared scope")
    name, data = owned_object(names, raw.get("object")), payload(raw.get("data"))
    label = raw.get("label")
    if not isinstance(label, str) or not re.fullmatch("[a-z0-9-]{1,32}", label):
        raise ValueError("invalid native client label")
    source = Path("/tmp") / (NAME + "-" + label + ".bin")
    source.write_bytes(data)
    log = Path("/tmp") / (NAME + "-" + label + ".log")
    status = out / (NAME + "-" + label + "-native.status")
    for path in (log, status):
        if path.exists():
            path.unlink()
    # The shell wrapper records the exit code; stdio never reaches the
    # control command, so the fixture's exec returns while rados waits.
    stderr = Path("/tmp") / (NAME + "-" + label + ".stderr")
    script = 'timeout %d rados "$@" > /dev/null 2> %s; echo $? > %s' % (NATIVE_SECONDS, stderr, status)
    args = base_args(root) + ["--log-to-file", "true", "--debug-objecter", "15", "--debug-ms", "0", "--log-file", str(log),
                              "-p", metadata["pool"], "append", name, str(source)]
    started = time.time()
    with open(os.devnull, "rb") as stdin, open(os.devnull, "wb") as sink:
        process = subprocess.Popen(["sh", "-c", script, "native-append"] + args, stdin=stdin, stdout=sink,
                                   stderr=sink, start_new_session=True)
    state["native"] = dict(label=label, object=name, pid=process.pid, started=started, length=len(data),
                           log=str(log), status=str(status), stderr=str(stderr))
    save_state(out, state)
    return dict(native=state["native"])


def parse_log(text, name):
    blocks, sends, held = [], [], []
    for line in text.splitlines():
        matched = BACKOFF_LINE.match(line)
        if matched:
            begin, end = matched[5], matched[6]
            blocks.append(dict(time=matched[1], pgid=matched[2], op=matched[3], id=int(matched[4]),
                               epoch=int(matched[7]), begin_has_object=(":" + name + ":") in begin,
                               end_has_object=(":" + name + ":") in end, single_object=begin == end))
            continue
        matched = SEND_LINE.match(line)
        if matched:
            sends.append(dict(time=matched[1], tid=int(matched[2]), pgid=matched[3], osd=int(matched[4])))
            continue
        matched = HELD_LINE.match(line)
        if matched:
            held.append(dict(time=matched[1], pgid=matched[2], id=int(matched[3])))
    return dict(backoffs=blocks, sends=sends, held=held)


def native_finish(out, raw):
    state = load_state(out)
    native = state and state.get("native")
    if not native:
        raise ValueError("no native client in this scope")
    seconds = raw.get("wait_seconds")
    if type(seconds) is not int or not 1 <= seconds <= WAIT_SECONDS:
        raise ValueError("invalid native wait")
    status = Path(native["status"])
    deadline = time.monotonic() + seconds
    while not status.exists() and time.monotonic() < deadline:
        time.sleep(.1)
    finished = status.exists()
    code = int(status.read_text().strip() or "-1") if finished else None
    completed = status.stat().st_mtime if finished else None
    log = Path(native["log"])
    text = log.read_bytes()[-MAX_LOG:].decode(errors="replace") if log.exists() else ""
    stderr = Path(native["stderr"])
    diagnostic = dict(log_bytes=log.stat().st_size if log.exists() else None,
                      stderr_tail=stderr.read_bytes()[-1024:].decode(errors="replace") if stderr.exists() else None,
                      objecter_tail=[line[:400] for line in text.splitlines() if "objecter" in line][-12:])
    result = dict(native, finished=finished, exit_code=code, completed=completed, diagnostic=diagnostic,
                  **parse_log(text, native["object"]))
    if finished:
        state["native"] = None
        state.setdefault("native_history", []).append(native["label"])
        save_state(out, state)
    return dict(native=result)


def native_stat(root, metadata, names, raw):
    name = owned_object(names, raw.get("object"))
    started = time.time()
    result = rados(root, metadata["pool"], ["stat", name], timeout=30)
    return dict(object=name, exit_code=result.returncode, started=started, completed=time.time(),
                stat=result.stdout[-512:].decode(errors="replace").strip())


def ops(root, names, raw):
    target, name = osd_id(raw.get("osd")), owned_object(names, raw.get("object"))
    value = ceph(root, ["tell", "osd.%d" % target, "dump_ops_in_flight"])
    found = []
    for op in value.get("ops", [])[:4096]:
        description = op.get("description", "")
        if not isinstance(description, str) or (":" + name + ":") not in description:
            continue
        events = op.get("type_data", {}).get("events", [])
        found.append(dict(description=description[:512], flag_point=op.get("type_data", {}).get("flag_point"),
                          client=op.get("type_data", {}).get("client_info", {}).get("client"),
                          tid=op.get("type_data", {}).get("client_info", {}).get("tid"),
                          events=[e.get("event") for e in events if isinstance(e, dict)][:32]))
    return dict(osd=target, object=name, ops=found)


ACK_DESCRIPTION = re.compile(
    r"osd_backoff\(([0-9]{1,19}\.[0-9a-f]{1,8}) ack-block id ([0-9]{1,20}) \[([^,\]]{0,4096}),([^,\)]{0,4096})\) e([0-9]{1,10})\)")


def parse_ack_history(history, pgid, name):
    # MOSDBackoff::print (messages/MOSDBackoff.h:75-81) supplies the decoded
    # wire PG/op/ID/bounds/epoch; OpRequest::_dump attributes the source
    # entity; reached_pg is recorded by OSD::dequeue_op (OSD.cc:9929-9936).
    if not isinstance(history, dict) or not isinstance(history.get("ops"), list) or len(history["ops"]) > 4096:
        raise ValueError("native completed operation history exceeds schema or count bound")
    found = []
    for op in history["ops"]:
        description = op.get("description") if isinstance(op, dict) else None
        if not isinstance(description, str) or len(description) > 16384:
            continue
        matched = ACK_DESCRIPTION.fullmatch(description)
        if not matched or matched[1] != pgid or (":" + name + ":") not in matched[3]:
            continue
        data = op.get("type_data", {}) if isinstance(op.get("type_data"), dict) else {}
        client = data.get("client_info", {}) if isinstance(data.get("client_info"), dict) else {}
        events = data.get("events", []) if isinstance(data.get("events"), list) else []
        found.append(dict(pgid=matched[1], id=int(matched[2]), map_epoch=int(matched[5]), op=2,
                          single_object=matched[3] == matched[4], client=client.get("client"),
                          reached_pg=any(isinstance(e, dict) and e.get("event") == "reached_pg" for e in events[:128]),
                          evidence="native completed op history; reception only"))
    return found


def ack_history(root, names, raw):
    target, name = osd_id(raw.get("osd")), owned_object(names, raw.get("object"))
    pgid = raw.get("pgid")
    if not isinstance(pgid, str) or not re.fullmatch(r"[0-9]{1,19}\.[0-9a-f]{1,8}", pgid):
        raise ValueError("invalid ACK history PG")
    return dict(osd=target, object=name, acks=parse_ack_history(
        ceph(root, ["tell", "osd.%d" % target, "dump_historic_ops"]), pgid, name))


def restore(root, out, fsid):
    state = load_state(out)
    if state is None:
        raise ValueError("no object backoff scope to restore")
    if not state.get("restored"):
        for flag in FLAGS:
            if flag not in state["flags"]:
                ceph(root, ["osd", "unset", flag], json_output=False)
        for key in [w + "/" + o for w, o, _ in CONFIG] + [DEGRADED[0] + "/" + DEGRADED[1]]:
            who, option = key.split("/")
            if key in state["config"]:
                ceph(root, ["config", "set", who, option, str(state["config"][key])], json_output=False)
            else:
                ceph(root, ["config", "rm", who, option], json_output=False)

        def clean():
            topology = dump(root, fsid)
            if any(not o["up"] or not o["in"] for o in topology["osds"].values()):
                return False, topology
            stat = ceph(root, ["pg", "stat"])
            states = stat.get("pg_summary", {}).get("num_pg_by_state", []) if isinstance(stat, dict) else []
            ok = bool(states) and all(s.get("name") == "active+clean" for s in states)
            return ok, dict(topology=topology, pg_stat=states)
        final = wait(clean, label="restored active+clean fixture")
        if sorted(final["topology"]["flags"]) != sorted(state["flags"]) or config_entries(root) != state["config"]:
            raise RuntimeError("restored flags/configuration differ from the recorded original")
        state["restored"] = True
        state["final"] = final
        save_state(out, state)
    return dict(restored=True, final=state.get("final"))


def run(action, root, out):
    if action not in ACTIONS:
        raise ValueError("unsupported object backoff oracle action")
    metadata, fsid, names = owned(root, out)
    raw = omap.read_json(scope_path(out, "input"))
    if not isinstance(raw, dict):
        raise ValueError("object backoff input must be an object")
    with control_lock(out):
        if action == "prepare":
            result = prepare(root, out, metadata, fsid, names, raw)
        elif action == "config":
            result = set_degraded(root, raw)
        elif action == "wait-osd":
            result = wait_osd(root, metadata, fsid, names, raw)
        elif action == "put":
            result = put(root, metadata, names, raw)
        elif action == "wait-degraded":
            result = wait_degraded(root, metadata, names, raw)
        elif action == "native-start":
            result = native_start(root, out, metadata, names, raw)
        elif action == "native-finish":
            result = native_finish(out, raw)
        elif action == "native-stat":
            result = native_stat(root, metadata, names, raw)
        elif action == "ops":
            result = ops(root, names, raw)
        elif action == "ack-history":
            result = ack_history(root, names, raw)
        else:
            result = restore(root, out, fsid)
    omap.publish(out, NAME + "-result", dict(schema_version=1, source_commit=SOURCE_COMMIT, action=action,
                                             observed=time.time(), result=result))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=ACTIONS)
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    run(args.action, args.root, args.out)
