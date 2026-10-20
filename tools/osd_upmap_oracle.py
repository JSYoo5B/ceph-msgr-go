#!/usr/bin/env python3
"""Closed, development-only actual upmap oracle; no product routing or retry.

Pinned Ceph 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964 (LGPL-2.1):
OSDMonitor.cc:12589-12868 checks/sets/removes explicit upmap vectors;
OSDMap.cc:2803-2847 applies full upmaps and membership replacements;
OSDMap.cc:1776-1778 requires the existing incarnation-2 bit21 mask.
No upstream implementation is copied. Native CLI/librados are test-only.
The ordinary three-replica fixture pool and all global settings stay unchanged.
"""
import argparse
import base64
import ctypes as C
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

import osd_omap_native as omap
import osd_xattr_native as xattr

NAME = "osd-upmap"
PROFILES = ("replicated-pg-v8", "replicated-pg-reply-v8")
ACTIONS = ("prepare", "items", "restore-items", "full", "restore-full", "observe", "finish")
SLOTS = ("items", "full")
SEEDS = {slot: bytes([0, 255, 128]) + b"owned-upmap-" + slot.encode() + b"\x00" for slot in SLOTS}
MAX_OUTPUT = 512 << 10
MAX_COMMANDS = 768
ACTION_SECONDS = 135
HOLD_SECONDS = 180
STATE_SECONDS = 60
COMMAND_SECONDS = 8


def check_input(raw):
    if not isinstance(raw, dict) or raw:
        raise ValueError("upmap control accepts only an empty input object")


def names(token):
    if not isinstance(token, str) or not re.fullmatch("[0-9a-f]{32}", token):
        raise ValueError("invalid upmap ownership token")
    return "ceph-msgr-upmap-" + token, {slot: "owned-upmap-" + slot for slot in SLOTS}


def fixture_identity(out):
    manifest = omap.read_json(out / "fixture-manifest.json")
    metadata = omap.read_json(out / "osd-oracle.json")
    version = (out / "daemon-version").read_text().strip()
    if "20.2.4" not in version or xattr.SOURCE_COMMIT not in version:
        raise ValueError("upmap oracle requires pinned Tentacle")
    if (manifest.get("osd") != "1" or manifest.get("osd_replicas") != "3"
            or manifest.get("osd_profile") not in PROFILES
            or metadata.get("profile") != manifest["osd_profile"]
            or metadata.get("replicas") != 3 or metadata.get("pool_pg_num") != 8
            or metadata.get("pool") != "ceph-msgr-osd-fixture"):
        raise ValueError("upmap oracle requires its existing three-OSD replicated-PG fixture")
    fsid = manifest.get("fsid")
    if not isinstance(fsid, str) or str(uuid.UUID(fsid)) != fsid:
        raise ValueError("invalid upmap fixture FSID")
    ownership_bytes = (out / "fixture-ownership.json").read_bytes()
    ownership = json.loads(ownership_bytes)
    containers = ownership.get("containers", {})
    if (not isinstance(containers, dict)
            or {k for k in containers if k.startswith("osd.")} != {"osd.0", "osd.1", "osd.2"}
            or any(not isinstance(v, str) or not re.fullmatch("[0-9a-f]{64}", v) for v in containers.values())):
        raise ValueError("upmap fixture daemon ownership differs")
    return fsid, hashlib.sha256(ownership_bytes).hexdigest(), metadata


def save(out, state):
    omap.publish(out, NAME + "-owned", state)


def load_owned(out):
    state = omap.read_json(out / (NAME + "-owned.json"))
    fsid, digest, _ = fixture_identity(out)
    pool, objects = names(state.get("token"))
    plan = state.get("plan", {})
    if (state.get("schema_version") != 1 or state.get("source_commit") != xattr.SOURCE_COMMIT
            or state.get("fsid") != fsid or state.get("fixture_ownership_sha256") != digest
            or plan.get("pool_name") != pool or plan.get("objects") != objects
            or plan.get("osd_ids") != [0, 1, 2] or type(state.get("finished")) is not bool
            or not isinstance(state.get("commands"), list) or len(state["commands"]) > MAX_COMMANDS):
        raise ValueError("upmap ownership journal differs from fixture")
    pool_id = plan.get("pool_id")
    if pool_id is not None and (type(pool_id) is not int or not 0 < pool_id < 1 << 63):
        raise ValueError("invalid owned upmap pool ID")
    return state


@contextmanager
def lock(out):
    # The fixture runs on Linux. Import lazily so Windows can import/model-test
    # this development helper without gaining a product platform dependency.
    import fcntl
    # Never unlink this inode: another live process may already have it open.
    # Closing/death (including SIGKILL) releases the kernel lock automatically.
    with (out / (NAME + ".lock")).open("a+b") as held:
        fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(held.fileno(), fcntl.LOCK_UN)


class Native:
    def __init__(self, root, out, state):
        self.root, self.out, self.state = root, out, state
        self.deadline = time.monotonic() + ACTION_SECONDS

    def remaining(self):
        left = self.deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError("upmap action hard deadline")
        return min(COMMAND_SECONDS, left)

    def command(self, words, *, tool="ceph", json_output=True):
        if len(self.state["commands"]) >= MAX_COMMANDS:
            raise ValueError("upmap native command budget exhausted")
        args = [tool, "-c", str(self.root / "ceph.conf"), "-n", "client.test", "--keyring", str(self.root / "keyring")]
        args += words
        if json_output:
            args += ["--format", "json"]
        timed_out = False
        try:
            process = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=self.remaining())
        except subprocess.TimeoutExpired as error:
            timed_out = True
            process = subprocess.CompletedProcess(args, -124, error.stdout or b"", error.stderr or b"")
        receipt = dict(tool=tool, command=words, returncode=process.returncode, timed_out=timed_out,
                       stdout_bytes=len(process.stdout), stdout_sha256=hashlib.sha256(process.stdout).hexdigest(),
                       stderr=process.stderr[:1024].decode("utf-8", errors="replace"),
                       stderr_truncated=len(process.stderr) > 1024)
        self.state["commands"].append(receipt)
        save(self.out, self.state)
        omap.publish(self.out, NAME + "-commands", dict(token=self.state["token"], commands=self.state["commands"]))
        if process.returncode or len(process.stdout) > MAX_OUTPUT or len(process.stderr) > 4096:
            raise RuntimeError("native upmap command failed: " + json.dumps(receipt))
        return json.loads(process.stdout) if json_output else process.stdout.decode().strip()

    def mutate(self, stage, words, *, tool="ceph"):
        if stage in self.state["attempted"]:
            raise ValueError("upmap native mutation phase cannot be replayed: " + stage)
        held = load_owned(self.out)
        if any(held.get(k) != self.state.get(k) for k in ("token", "plan", "pending", "finished", "attempted")):
            raise ValueError("upmap ownership changed before native mutation")
        dump = self.dump()
        if stage == "create-pool":
            if self.state["plan"]["pool_id"] is not None or any(p.get("pool_name") == self.state["plan"]["pool_name"] for p in dump["pools"]):
                raise ValueError("new owned pool name must be absent immediately before creation")
        else:
            if self.state["plan"]["pool_id"] is None:
                raise ValueError("mutation requires an independently confirmed owned pool ID")
            self.pool(dump, initializing_min_size=stage == "set-min-size")
        # CLI pool-name arguments cannot be made atomic with this observation.
        # The disposable single-owner UUID scope excludes concurrent operators;
        # observed replacement/rename fails closed, not an atomic remote lease.
        self.state["attempted"].append(stage)
        self.state["pending"] = stage
        save(self.out, self.state)  # intent is durable before spawning a mutation
        self.command(words, tool=tool, json_output=False)
        self.state["pending"] = None
        save(self.out, self.state)

    def dump(self):
        dump = self.command(["osd", "dump"])
        if dump.get("fsid") != self.state["fsid"] or type(dump.get("epoch")) is not int or dump["epoch"] <= 0:
            raise ValueError("upmap live cluster identity differs")
        if dump.get("require_min_compat_client") not in ("luminous", "mimic", "nautilus", "octopus", "pacific", "quincy", "reef", "squid", "tentacle"):
            raise ValueError("actual upmap requires native require_min_compat_client at least luminous; global settings are not changed")
        osds = dump.get("osds", [])
        if (len(osds) != 3 or {o.get("osd") for o in osds} != {0, 1, 2}
                or any(o.get("up") != 1 or o.get("in") != 1 for o in osds)):
            raise ValueError("upmap recipe requires exactly three live in OSDs")
        baseline = self.state["baseline_pool"]
        pools = [p for p in dump.get("pools", []) if p.get("pool") == baseline["pool"]]
        if len(pools) != 1 or any(pools[0].get(k) != baseline.get(k) for k in BASELINE_FIELDS):
            raise ValueError("original strict three-replica pool policy changed")
        allowed = {r["pgid"] for r in self.state["plan"].get("baseline", {}).values()}
        for field in ("pg_upmap", "pg_upmap_items"):
            entries = dump.get(field, [])
            if not isinstance(entries, list) or len(entries) > 1 or any(not isinstance(e, dict) or e.get("pgid") not in allowed for e in entries):
                raise ValueError("unexpected upmap outside this frozen owned plan")
            slot = "items" if field == "pg_upmap_items" else "full"
            for entry in entries:
                if entry.get("pgid") != self.state["plan"]["baseline"][slot]["pgid"]:
                    raise ValueError("upmap entry does not belong to the frozen phase PG")
                target = self.state["plan"]["targets"][slot]
                expected = [{"from": target[0], "to": target[1]}] if slot == "items" else target
                if entry.get("mappings" if slot == "items" else "osds") != expected:
                    raise ValueError("upmap entry vector differs from the exact owned target")
        if dump.get("pg_upmap_primaries"):
            raise ValueError("primary-upmap is outside this profile")
        return dump

    def pool(self, dump, *, initializing_min_size=False):
        p = self.state["plan"]
        matches = [v for v in dump["pools"] if v.get("pool_name") == p["pool_name"]]
        if len(matches) != 1 or p["pool_id"] is not None and matches[0].get("pool") != p["pool_id"]:
            raise ValueError("owned upmap pool name/ID differs")
        value = matches[0]
        if (value.get("type") != 1 or value.get("size") != 2 or value.get("min_size") not in ((1, 2) if initializing_min_size else (2,))
                or value.get("pg_autoscale_mode") != "off" or not value.get("flags", 0) & 1
                or any(value.get(k) != 8 for k in ("pg_num", "pg_num_target", "pg_num_pending", "pg_placement_num", "pg_placement_num_target"))):
            raise ValueError("owned pool must remain HASHPSPOOL replicated size/min_size2 and 8/8 PGs")
        return value

    def snapshot(self, slot, *, absent=False):
        # All native reads/writes have process deadlines, including librados.
        result = subprocess.run([sys.executable, __file__, "snapshot", str(self.root), str(self.out), "--slot", slot],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=self.remaining())
        if result.returncode or len(result.stdout) > MAX_OUTPUT or len(result.stderr) > 4096:
            raise RuntimeError("bounded native upmap object snapshot failed")
        value = json.loads(result.stdout)
        if value.get("code") != 0 and not (absent and value.get("code") == -2):
            raise ValueError("owned upmap object is absent")
        return value


BASELINE_FIELDS = ("pool", "pool_name", "type", "size", "min_size", "pg_num", "pg_num_target", "pg_num_pending",
                   "pg_placement_num", "pg_placement_num_target", "flags", "crush_rule", "pg_autoscale_mode")


def parse_pg(value, pool):
    match = re.fullmatch(r"([0-9]+)\.([0-9a-f]{1,8})", value if isinstance(value, str) else "")
    if not match or int(match[1]) != pool:
        raise ValueError("native PG identity differs from the owned pool")
    return int(match[2], 16)


def route(native, dump, pool, name, snapshot):
    mapped = native.command(["osd", "map", pool["pool_name"], name])
    pool_id = pool["pool"]
    pg = parse_pg(mapped.get("pgid"), pool_id)
    raw_hash = parse_pg(mapped.get("raw_pgid"), pool_id)
    up, acting = mapped.get("up"), mapped.get("acting")
    primary = mapped.get("acting_primary")
    if (mapped.get("epoch") != dump["epoch"] or mapped.get("pool_id") != pool_id
            or mapped.get("pool") != pool["pool_name"] or mapped.get("objname") != name or pg >= 8
            or not isinstance(up, list) or not isinstance(acting, list) or len(up) != pool["size"]
            or len(set(up)) != len(up) or set(up) - {0, 1, 2} or acting != up or primary != acting[0]):
        raise ObservationChanged("native map/dump placement is not a stable owned tuple")
    query = native.command(["pg", mapped["pgid"], "query"])
    if (query.get("epoch") != dump["epoch"] or query.get("info", {}).get("pgid") != mapped["pgid"]
            or query.get("acting") != acting or query.get("up") != up
            or not {"active", "clean"} <= set(query.get("state", "").split("+"))):
        raise ObservationChanged("native PG has not applied the same clean map epoch")
    endpoints = [o for o in dump["osds"] if o["osd"] == primary]
    addresses = endpoints[0].get("public_addrs", {}).get("addrvec", [])
    addresses = [a for a in addresses if a.get("type") == "v2"]
    if len(addresses) != 1 or type(addresses[0].get("nonce")) is not int or not isinstance(addresses[0].get("addr"), str):
        raise ValueError("native primary lacks a unique v2 endpoint/nonce")
    addr = addresses[0]
    return dict(pool_id=pool_id, object=name, namespace="", osd_id=primary, epoch=dump["epoch"],
                hash=raw_hash, pg_seed=pg, raw_pgid=mapped["raw_pgid"], pgid=mapped["pgid"],
                address="v2:" + addr["addr"] + "/" + str(addr["nonce"]), acting=acting, up=up,
                replicas=pool["size"], min_size=pool["min_size"], size=len(base64.b64decode(snapshot["data"], validate=True)),
                mtime_seconds=snapshot["seconds"], mtime_nanoseconds=snapshot["nanos"]), query


class ObservationChanged(RuntimeError):
    pass


def observe_once(native):
    dump = native.dump()
    pool = native.pool(dump)
    observations = {}
    for slot in SLOTS:
        snapshot = native.snapshot(slot)
        selected, query = route(native, dump, pool, native.state["plan"]["objects"][slot], snapshot)
        observations[slot] = dict(route=selected, pg_query=query, pool=pool, object_state=snapshot)
    baseline_pool = next(p for p in dump["pools"] if p["pool"] == native.state["baseline_pool"]["pool"])
    snapshot = native.snapshot("baseline")
    selected, query = route(native, dump, baseline_pool, native.state["baseline_object"], snapshot)
    if native.dump()["epoch"] != dump["epoch"]:
        raise ObservationChanged("native epoch advanced across the route observation")
    if native.state.get("baseline_route") is not None and without_epoch(selected) != without_epoch(native.state["baseline_route"]):
        raise ValueError("original fixture placement/address identity changed")
    if native.state.get("baseline_snapshot") is not None and snapshot != native.state["baseline_snapshot"]:
        raise ValueError("original fixture object bytes/version/mtime changed")
    return observations, dict(route=selected, object_state=snapshot, pg_query=query), dump


def wait_observe(native, expected=None):
    deadline = min(time.monotonic() + STATE_SECONDS, native.deadline)
    while True:
        try:
            observations, baseline, dump = observe_once(native)
            if expected is not None:
                for slot, vector in expected.items():
                    if observations[slot]["route"]["up"] != vector or observations[slot]["route"]["acting"] != vector:
                        raise ObservationChanged("owned remap has not converged to the exact planned vector")
            return observations, baseline, dump
        except ObservationChanged:
            if time.monotonic() >= deadline:
                raise TimeoutError("native upmap applied-map/active+clean observation deadline")
            time.sleep(.25)


def receipt(native, phase, expected=None):
    observations, baseline, dump = wait_observe(native, expected)
    value = dict(schema_version=1, source_commit=xattr.SOURCE_COMMIT, token=native.state["token"],
                 fsid=native.state["fsid"], phase=phase, plan=native.state["plan"], observations=observations,
                 baseline_fixture=baseline, finished=False, native_epoch=dump["epoch"])
    omap.publish(native.out, NAME + "-native", dict(osd_dump=dump, observations=observations, baseline_fixture=baseline))
    native.state["phase"] = phase
    native.state["latest"] = value
    save(native.out, native.state)
    return value


def without_epoch(route):
    return {k: v for k, v in route.items() if k != "epoch"}


def prepare(native):
    state, plan = native.state, native.state["plan"]
    if state["attempted"] or state["pending"]:
        raise ValueError("prepare cannot replay an existing upmap scope")
    dump = native.dump()
    if any(p.get("pool_name") == plan["pool_name"] for p in dump["pools"]):
        raise ValueError("owned UUID pool must be absent before creation")
    snapshot = native.snapshot("baseline")
    baseline_pool = next(p for p in dump["pools"] if p["pool"] == state["baseline_pool"]["pool"])
    state["baseline_route"], _ = route(native, dump, baseline_pool, state["baseline_object"], snapshot)
    state["baseline_snapshot"] = snapshot
    save(native.out, state)
    native.mutate("create-pool", ["osd", "pool", "create", plan["pool_name"], "8", "8", "replicated", "--size", "2", "--autoscale-mode", "off"])
    dump = native.dump()
    candidates = [p for p in dump["pools"] if p.get("pool_name") == plan["pool_name"]]
    if len(candidates) != 1:
        raise ValueError("owned pool create lacks independently confirmed name/ID")
    plan["pool_id"] = candidates[0]["pool"]
    save(native.out, state)
    if candidates[0].get("min_size") != 2:
        native.mutate("set-min-size", ["osd", "pool", "set", plan["pool_name"], "min_size", "2"])
    for slot in SLOTS:
        seed = native.out / (NAME + "-seed-" + slot + ".bin")
        seed.write_bytes(SEEDS[slot])
        native.mutate("seed-" + slot, ["-p", plan["pool_name"], "put", plan["objects"][slot], str(seed)], tool="rados")
    value = receipt(native, "prepare")
    plan["baseline"] = {slot: value["observations"][slot]["route"] for slot in SLOTS}
    plan["targets"] = {}
    for slot in SLOTS:
        before = plan["baseline"][slot]["acting"]
        spare = next(i for i in (0, 1, 2) if i not in before)
        plan["targets"][slot] = [before[1], spare] if slot == "items" else [spare, before[0]]
    state["baseline_snapshot"] = value["baseline_fixture"]["object_state"]
    state["prepared"] = True
    save(native.out, state)
    return receipt(native, "prepare")


def transition(native, action):
    state, plan = native.state, native.state["plan"]
    if not state.get("prepared") or state.get("pending"):
        raise ValueError("upmap transition requires confirmed preparation and no uncertain mutation")
    required = {"items": "prepare", "restore-items": "items", "full": "restore-items", "restore-full": "full"}
    if state["phase"] != required[action]:
        raise ValueError("upmap action does not follow its closed lifecycle")
    slot = "items" if "items" in action else "full"
    expected = {s: plan["baseline"][s]["acting"] for s in SLOTS}
    # Recheck frozen routes and policy before every mutation; caller cannot
    # supply PGs, IDs, pool names or arbitrary CLI words.
    wait_observe(native, expected if action in ("items", "full") else None)
    pg = plan["baseline"][slot]["pgid"]
    verb = "pg-upmap-items" if slot == "items" else "pg-upmap"
    if action.startswith("restore-"):
        words = ["osd", "rm-" + verb, pg]
    else:
        words = ["osd", verb, pg] + [str(i) for i in plan["targets"][slot]]
        before = plan["baseline"][slot]["acting"]
        target = [before[0], plan["targets"][slot][1]] if slot == "items" else plan["targets"][slot]
        # The two fixed object slots may share a PG. Both then have the same
        # effective vector, which is explicit evidence rather than a new route.
        expected = {s: target if plan["baseline"][s]["pgid"] == pg else expected[s] for s in SLOTS}
    native.mutate(action, words)
    state["active"] = None if action.startswith("restore-") else slot
    save(native.out, state)
    value = receipt(native, action, expected)
    dump = native.dump()
    entries = dump.get("pg_upmap_items" if slot == "items" else "pg_upmap", [])
    selected = [e for e in entries if e.get("pgid") == pg]
    if action.startswith("restore-") and selected or not action.startswith("restore-") and len(selected) != 1:
        raise ValueError("native upmap dump does not contain the exact expected owned entry")
    return value


def finish(native):
    state, plan = native.state, native.state["plan"]
    if state["finished"]:
        return state["finish_result"]
    unresolved = state.get("pending")
    dump = native.dump()
    # An unconfirmed create is never adopted for destructive cleanup merely by
    # matching its name. The fixture owns final teardown of such resources.
    if plan["pool_id"] is None:
        retained = [dict(kind="pool", name=plan["pool_name"], reason="unconfirmed pool creation; fixture teardown only")]
        return finish_receipt(native, False, False, retained, unresolved)
    native.pool(dump)
    for slot, field, verb in (("items", "pg_upmap_items", "rm-pg-upmap-items"), ("full", "pg_upmap", "rm-pg-upmap")):
        pg = plan.get("baseline", {}).get(slot, {}).get("pgid")
        if pg is None:
            continue
        entries = [e for e in dump.get(field, []) if e.get("pgid") == pg]
        if entries:
            stage = "cleanup-" + slot
            if stage in state["attempted"] or "restore-" + slot in state["attempted"]:
                return finish_receipt(native, False, False, [dict(kind="pool", name=plan["pool_name"], id=plan["pool_id"], reason="uncertain override removal")], unresolved or stage)
            native.mutate(stage, ["osd", verb, pg])
            dump = native.dump()
    if state.get("prepared"):
        expected = {s: plan["baseline"][s]["acting"] for s in SLOTS}
        value = receipt(native, "cleanup-restored", expected)
        state["pre_cleanup"] = value
    for slot in SLOTS:
        stage = "remove-" + slot
        if stage in state["attempted"]:
            continue
        # Observe absence/presence first; never reissue a previous removal.
        snap = native.snapshot(slot, absent=True)
        if snap.get("code") == -2:
            continue
        if snap.get("code") != 0:
            raise ValueError("owned cleanup object state is neither present nor absent")
        native.mutate(stage, ["-p", plan["pool_name"], "rm", plan["objects"][slot]], tool="rados")
    removed = all(native.snapshot(s, absent=True).get("code") == -2 for s in SLOTS)
    final = native.dump()
    restored = all(not e.get("pgid", "").startswith(str(plan["pool_id"]) + ".") for f in ("pg_upmap", "pg_upmap_items") for e in final.get(f, []))
    # Do not change mon_allow_pool_delete, the baseline pool or global policy.
    # Even where pool deletion is enabled, retaining this harmless empty pool
    # gives the same closed cleanup contract in both fixture configurations.
    retained = [dict(kind="pool", name=plan["pool_name"], id=plan["pool_id"], reason="empty owned pool retained for disposable fixture teardown")]
    return finish_receipt(native, restored, removed, retained, unresolved)


def finish_receipt(native, restored, removed, retained, unresolved):
    state = native.state
    result = dict(schema_version=1, source_commit=xattr.SOURCE_COMMIT, token=state["token"], fsid=state["fsid"],
                  phase="finish", plan=state["plan"], finished=True,
                  cleanup=dict(overrides_restored=restored, objects_removed=removed, pool_removed=False,
                               retained_for_fixture_teardown=bool(retained), baseline_unchanged=False, errors=[]),
                  retained_resources=retained,
                  unresolved_mutation=unresolved, pre_cleanup=state.get("pre_cleanup"),
                  automatic_cleanup=state.get("automatic_cleanup", False), first_error=state.get("first_error"))
    try:
        snapshot = native.snapshot("baseline")
        dump = native.dump()
        pool = next(p for p in dump["pools"] if p["pool"] == state["baseline_pool"]["pool"])
        selected, _ = route(native, dump, pool, state["baseline_object"], snapshot)
        result["cleanup"]["baseline_unchanged"] = (snapshot == state.get("baseline_snapshot") and without_epoch(selected) == without_epoch(state.get("baseline_route", {})))
    except Exception as error:
        result["cleanup"]["errors"].append(str(error))
    if not restored:
        result["cleanup"]["errors"].append("owned override restoration is unconfirmed")
    if not removed:
        result["cleanup"]["errors"].append("owned object cleanup is unconfirmed")
    if unresolved:
        result["cleanup"]["errors"].append("original uncertain mutation retained: " + unresolved)
    if state.get("automatic_cleanup"):
        result["cleanup"]["errors"].append("scope required automatic cleanup; original acceptance did not finish")
    state["finished"], state["finish_result"], state["phase"] = True, result, "finish"
    save(native.out, state)
    return result


@contextmanager
def pool_connection(native, root, pool_id):
    # Pinned librados.h:782/1009. Bind the observation to the confirmed numeric
    # pool ID, then independently verify the ioctx; a reused name cannot select
    # a replacement pool. Native library use is confined to this test process.
    lib = native.lib
    lib.rados_ioctx_create2.argtypes = [C.c_void_p, C.c_int64, C.POINTER(C.c_void_p)]
    lib.rados_ioctx_create2.restype = C.c_int
    lib.rados_ioctx_get_id.argtypes = [C.c_void_p]
    lib.rados_ioctx_get_id.restype = C.c_int64
    cluster, io = C.c_void_p(), C.c_void_p()
    native.check(lib.rados_create2(C.byref(cluster), b"ceph", xattr.CLIENT_ROLES["test"], 0))
    try:
        native.check(lib.rados_conf_read_file(cluster, os.fsencode(root / "ceph.conf")))
        native.check(lib.rados_conf_set(cluster, b"keyring", os.fsencode(root / "keyring")))
        native.check(lib.rados_connect(cluster))
        native.check(lib.rados_ioctx_create2(cluster, pool_id, C.byref(io)))
        try:
            if lib.rados_ioctx_get_id(io) != pool_id:
                raise ValueError("native observation ioctx pool ID differs from frozen ownership")
            yield io
        finally:
            lib.rados_ioctx_destroy(io)
    finally:
        lib.rados_shutdown(cluster)


def snapshot_native(root, out, slot):
    state = load_owned(out)
    _, _, baseline = fixture_identity(out)
    if slot == "baseline":
        pool_id, name = baseline["pool_id"], baseline["object"]
    else:
        pool_id, name = state["plan"]["pool_id"], state["plan"]["objects"][slot]
    if type(pool_id) is not int or not 0 < pool_id < 1 << 63:
        raise ValueError("snapshot requires independently confirmed numeric pool ownership")
    native = xattr.Native()
    with pool_connection(native, root, pool_id) as io:
        return native.snapshot(io, name.encode(), [])


def start_guard(root, out, state):
    state["last_action"] = time.time()
    state["guard_started"] = True
    save(out, state)
    with (out / (NAME + "-guard.log")).open("ab") as log:
        process = subprocess.Popen([sys.executable, __file__, "guard", str(root), str(out)],
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
    state["guard_pid"] = process.pid
    save(out, state)


def guard(root, out):
    # Independent of the Go caller/controller. Only the existing closed finish
    # operation may run, once, after a failure or an idle observation lease.
    while True:
        state = load_owned(out)
        if state["finished"]:
            return
        if state.get("first_error") or time.time() - state.get("last_action", 0) >= HOLD_SECONDS:
            try:
                result = run("finish", root, out, automatic=True)
                if result is not None:
                    omap.publish(out, NAME + "-guard-result", dict(token=state["token"], automatic_cleanup=True, result=result))
                    return
            except BlockingIOError:
                pass  # an admitted bounded action still owns the lock
            except Exception as error:
                omap.publish(out, NAME + "-guard-result", dict(error=str(error), token=state["token"]))
                return
        time.sleep(.5)


def run(action, root, out, automatic=False):
    check_input(omap.read_json(out / (NAME + "-input.json")))
    with lock(out):
        path = out / (NAME + "-owned.json")
        if action == "prepare" and not path.exists():
            fsid, digest, metadata = fixture_identity(out)
            token = uuid.uuid4().hex
            pool, objects = names(token)
            # Capture the unmodified baseline before any native mutation.
            initial = subprocess.run(["ceph", "-c", str(root / "ceph.conf"), "-n", "client.test", "--keyring", str(root / "keyring"), "osd", "dump", "--format", "json"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=COMMAND_SECONDS)
            if initial.returncode or len(initial.stdout) > MAX_OUTPUT or len(initial.stderr) > 4096:
                raise RuntimeError("initial native upmap cluster observation failed")
            dump = json.loads(initial.stdout)
            if dump.get("fsid") != fsid or any(dump.get(k) for k in ("pg_upmap", "pg_upmap_items", "pg_upmap_primaries")):
                raise ValueError("fresh upmap fixture must have no pre-existing overrides")
            pools = [p for p in dump.get("pools", []) if p.get("pool") == metadata["pool_id"]]
            if len(pools) != 1 or pools[0].get("pool_name") != metadata["pool"] or pools[0].get("size") != 3:
                raise ValueError("original three-replica pool identity differs")
            state = dict(schema_version=1, source_commit=xattr.SOURCE_COMMIT, fsid=fsid,
                         fixture_ownership_sha256=digest, token=token,
                         plan=dict(pool_name=pool, pool_id=None, osd_ids=[0, 1, 2], objects=objects),
                         baseline_pool={k: pools[0].get(k) for k in BASELINE_FIELDS}, baseline_object=metadata["object"],
                         commands=[], attempted=[], pending=None, phase="new", finished=False)
            save(out, state)
            start_guard(root, out, state)
        else:
            state = load_owned(out)
        if automatic:
            # Recheck the idle lease under the same lock as ordinary actions.
            # An action admitted after the guard's earlier read wins the lease.
            if state["finished"]:
                return state["finish_result"]
            if not state.get("first_error") and time.time() - state.get("last_action", 0) < HOLD_SECONDS:
                return None
            state["automatic_cleanup"] = True
            state.setdefault("first_error", "upmap observation lease expired before explicit finish")
        state["last_action"] = time.time()
        save(out, state)
        native = Native(root, out, state)
        try:
            if state.get("first_error") and action != "finish":
                raise ValueError("upmap scope with a first failure permits finish only")
            if state["finished"] and action != "finish":
                raise ValueError("finished upmap scope cannot be reused")
            if action == "prepare":
                result = prepare(native)
            elif action in ("items", "restore-items", "full", "restore-full"):
                result = transition(native, action)
            elif action == "observe":
                if not state.get("prepared") or state.get("pending"):
                    raise ValueError("observe requires a confirmed prepared upmap scope")
                result = receipt(native, state["phase"])
            else:
                result = finish(native)
            omap.publish(out, NAME + "-result", result)
            return result
        except Exception as error:
            state.setdefault("first_error", str(error))
            save(out, state)
            omap.publish(out, NAME + "-failure", dict(token=state["token"], phase=state["phase"], pending=state.get("pending"), first_error=state["first_error"]))
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=ACTIONS + ("snapshot", "guard"))
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument("--slot", choices=SLOTS + ("baseline",))
    args = parser.parse_args()
    if args.action == "guard":
        if args.slot is not None:
            raise ValueError("guard takes no slot")
        guard(args.root, args.out)
    elif args.action == "snapshot":
        if args.slot is None:
            raise ValueError("internal snapshot requires its closed slot")
        print(json.dumps(snapshot_native(args.root, args.out, args.slot)), flush=True)
    else:
        if args.slot is not None:
            raise ValueError("control actions accept no extra argument")
        run(args.action, args.root, args.out)
