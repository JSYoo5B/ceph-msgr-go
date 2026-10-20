#!/usr/bin/env python3
"""Disposable native MON map oracle; no MDS daemon, session, mount or CephFS I/O.

Independently uses the pinned Tentacle CLI, without Go wire encoders. Source
provenance: Ceph v20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964,
mon/{FSCommands,MDSMonitor}.cc and mds/FSMap.cc (LGPL-2.1; no code copied).
Only a controlled single-OSD fixture with no preexisting filesystem/standby is
admitted. UUID names and confirmed native IDs are journaled before mutations.
Uncertain mutations are never replayed or adopted automatically during cleanup.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid

import osd_omap_native as omap
import osd_xattr_native as xattr

NAME = "mds-maps"
MAX_OUTPUT = 256 << 10
MAX_COMMANDS = 128
PROFILES = ("replicated-pg-v8", "replicated-pg-reply-v8")


def integer(value, maximum, label):
    return omap.integer(value, maximum, label)


def check_input(raw):
    if not isinstance(raw, dict) or raw:
        raise ValueError("MDS map oracle accepts only an empty input object")


def names(token):
    if not isinstance(token, str) or not re.fullmatch("[0-9a-f]{32}", token):
        raise ValueError("invalid MDS map ownership token")
    base = "ceph-msgr-m0-" + token
    return dict(fs=base, metadata=base + "-meta", data=base + "-data")


def fixture_identity(out):
    version = (out / "daemon-version").read_text().strip()
    manifest = omap.read_json(out / "fixture-manifest.json")
    metadata = omap.read_json(out / "osd-oracle.json")
    if "20.2.4" not in version or xattr.SOURCE_COMMIT not in version:
        raise ValueError("MDS map oracle requires pinned Tentacle")
    if manifest.get("osd") != "1" or manifest.get("osd_replicas") != "1" or manifest.get("osd_profile") not in PROFILES or metadata.get("profile") != manifest.get("osd_profile") or metadata.get("replicas") != 1:
        raise ValueError("MDS map oracle requires the controlled single-OSD fixture")
    fsid = manifest.get("fsid")
    if not isinstance(fsid, str) or str(uuid.UUID(fsid)) != fsid:
        raise ValueError("MDS map oracle lacks a fixture FSID")
    ownership = omap.read_json(out / "fixture-ownership.json")
    containers = ownership.get("containers", {})
    if not isinstance(containers, dict) or "osd.0" not in containers or any(k.startswith("mds.") or k.startswith("osd.") and k != "osd.0" for k in containers):
        raise ValueError("MDS map oracle fixture ownership topology differs")
    for value in containers.values():
        if not isinstance(value, str) or not re.fullmatch("[0-9a-f]{64}", value):
            raise ValueError("invalid fixture container ownership identity")
    digest = hashlib.sha256((out / "fixture-ownership.json").read_bytes()).hexdigest()
    return fsid, digest


def load_owned(out):
    state = omap.read_json(out / (NAME + "-owned.json"))
    fsid, identity = fixture_identity(out)
    if (not isinstance(state, dict) or state.get("schema_version") != 1 or
            state.get("source_commit") != xattr.SOURCE_COMMIT or state.get("fsid") != fsid or
            state.get("fixture_ownership_sha256") != identity or state.get("names") != names(state.get("token"))):
        raise ValueError("MDS map ownership journal differs from the fixture")
    ids = state.get("pool_ids")
    if not isinstance(ids, dict) or set(ids) != {"metadata", "data"}:
        raise ValueError("invalid MDS map pool ownership IDs")
    for value in ids.values():
        if value is not None:
            integer(value, (1 << 63) - 1, "owned pool ID")
    if state.get("fscid") is not None:
        integer(state["fscid"], (1 << 31) - 1, "owned filesystem ID")
    if state.get("pending") not in (None, "create_metadata", "create_data", "create_fs", "remove_fs", "remove_metadata", "remove_data"):
        raise ValueError("invalid pending MDS map mutation stage")
    if type(state.get("finished")) is not bool or not isinstance(state.get("commands"), list) or len(state["commands"]) > MAX_COMMANDS:
        raise ValueError("invalid MDS map lifecycle journal")
    return state


def save(out, state):
    omap.publish(out, NAME + "-owned", state)


class Native:
    def __init__(self, root, out, state):
        self.root, self.out, self.state = root, out, state

    def command(self, words, *, json_output=True):
        if len(self.state["commands"]) >= MAX_COMMANDS:
            raise ValueError("MDS map native command budget exhausted")
        args = ["ceph", "-c", str(self.root / "ceph.conf"), "-n", "client.test", "--keyring", str(self.root / "keyring")]
        if json_output:
            args += ["--format", "json"]
        args += words
        timed_out = False
        try:
            process = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
        except subprocess.TimeoutExpired as error:
            timed_out = True
            process = subprocess.CompletedProcess(args, -124, error.stdout or b"", error.stderr or b"")
        receipt = dict(command=words, returncode=process.returncode, timed_out=timed_out,
                       stdout_bytes=len(process.stdout), stdout_sha256=hashlib.sha256(process.stdout).hexdigest(),
                       stderr=process.stderr[:1024].decode("utf-8", errors="replace"), stderr_truncated=len(process.stderr) > 1024)
        self.state["commands"].append(receipt)
        save(self.out, self.state)
        omap.publish(self.out, NAME + "-commands", dict(token=self.state["token"], commands=self.state["commands"]))
        if process.returncode or len(process.stdout) > MAX_OUTPUT or len(process.stderr) > 4096:
            raise RuntimeError("native MDS map command failed: " + json.dumps(receipt))
        if not json_output:
            return process.stdout.decode("utf-8").strip()
        return json.loads(process.stdout)

    def identity(self):
        held = load_owned(self.out)
        if any(held.get(k) != self.state.get(k) for k in ("token", "names", "pool_ids", "fscid", "pending", "finished")):
            raise ValueError("native MDS map mutation no longer owns its persisted journal")
        if fixture_identity(self.out) != (self.state["fsid"], self.state["fixture_ownership_sha256"]):
            raise ValueError("MDS map fixture ownership changed before native mutation")
        if self.command(["fsid"], json_output=False) != self.state["fsid"]:
            raise ValueError("native MON FSID differs from owned fixture")

    def snapshot(self):
        dump = self.command(["fs", "dump"])
        pools = self.command(["osd", "dump"])
        if not isinstance(dump, dict) or not isinstance(pools, dict):
            raise ValueError("invalid native map JSON shape")
        integer(dump.get("epoch"), (1 << 32) - 1, "native FSMap epoch")
        if pools.get("fsid") != self.state["fsid"]:
            raise ValueError("native OSDMap FSID differs from fixture")
        filesystems = dump.get("filesystems")
        if not isinstance(filesystems, list) or len(filesystems) > 1 or dump.get("standbys") != []:
            raise ValueError("MDS map fixture has foreign filesystems or standbys")
        for fs in filesystems:
            if not isinstance(fs, dict) or fs.get("mdsmap", {}).get("fs_name") != self.state["names"]["fs"]:
                raise ValueError("foreign filesystem present in MDS map fixture")
            if self.state["fscid"] is not None and fs.get("id") != self.state["fscid"]:
                raise ValueError("owned filesystem ID was replaced")
            empty_filesystem(fs, self.state)
        table = pools.get("pools")
        if not isinstance(table, list) or len(table) > 64:
            raise ValueError("native pool inventory exceeds fixture bound")
        ids, poolnames = set(), set()
        for p in table:
            if not isinstance(p, dict):
                raise ValueError("invalid native pool descriptor")
            identity = integer(p.get("pool"), (1 << 63) - 1, "native pool ID")
            name = p.get("pool_name")
            if not isinstance(name, str) or identity in ids or name in poolnames:
                raise ValueError("duplicate or invalid native pool identity")
            ids.add(identity); poolnames.add(name)
        for role, identity in self.state["pool_ids"].items():
            owned = [p for p in table if p["pool_name"] == self.state["names"][role]]
            if identity is not None and not owned and role not in self.state.get("removed_pools", []) and self.state.get("pending") != "remove_" + role:
                raise ValueError("confirmed owned pool disappeared before native mutation")
            if identity is not None and owned and owned[0]["pool"] != identity:
                raise ValueError("owned pool name was replaced by a different ID")
            if identity is not None and any(p["pool"] == identity and p["pool_name"] != self.state["names"][role] for p in table):
                raise ValueError("owned pool ID was renamed")
        return dump, table

    def guard(self):
        self.identity()
        return self.snapshot()

    def mutate(self, stage, words):
        dump, pools = self.guard()  # Recheck journal, live FSID and every confirmed ID.
        if stage in ("create_metadata", "create_data"):
            role = stage[len("create_"):]
            if any(p["pool_name"] == self.state["names"][role] for p in pools):
                raise ValueError("UUID pool name appeared before its creation")
        elif stage == "create_fs":
            if dump["filesystems"] or any(v is None for v in self.state["pool_ids"].values()):
                raise ValueError("filesystem creation requires absent name and confirmed pools")
        elif stage == "remove_fs":
            if self.state["fscid"] is None or len(dump["filesystems"]) != 1:
                raise ValueError("filesystem removal requires its confirmed present ID")
        elif stage in ("remove_metadata", "remove_data"):
            role = stage[len("remove_"):]
            if dump["filesystems"] or not any(p["pool_name"] == self.state["names"][role] and p["pool"] == self.state["pool_ids"][role] for p in pools):
                raise ValueError("pool removal requires unreferenced confirmed identity")
        else:
            raise ValueError("unsupported native MDS map mutation stage")
        if self.state["pending"] is not None:
            raise ValueError("uncertain native mutation must not be replayed")
        self.state["pending"] = stage
        save(self.out, self.state)
        self.command(words, json_output=False)


def empty_filesystem(fs, state):
    integer(fs.get("id"), (1 << 31) - 1, "native filesystem ID")
    m = fs.get("mdsmap")
    if not isinstance(m, dict) or m.get("fs_name") != state["names"]["fs"]:
        raise ValueError("native filesystem name differs")
    integer(m.get("epoch"), (1 << 32) - 1, "native MDSMap epoch")
    if m.get("info") != {} or m.get("up") != {} or any(m.get(k) != [] for k in ("in", "failed", "stopped", "damaged")):
        raise ValueError("owned filesystem acquired MDS/rank state")
    if m.get("enabled") is not True or m.get("metadata_pool") != state["pool_ids"]["metadata"] or m.get("data_pools") != [state["pool_ids"]["data"]]:
        raise ValueError("native filesystem enabled state or pool identity differs")
    return m


def observe(native):
    for _ in range(5):
        first, _ = native.guard()
        matches = first["filesystems"]
        if native.state["fscid"] is None or len(matches) != 1:
            raise ValueError("native owned filesystem is not confirmed/present")
        current = native.command(["fs", "get", native.state["names"]["fs"]])
        second, _ = native.snapshot()
        if first["epoch"] == second["epoch"] and current == matches[0] == second["filesystems"][0]:
            m = empty_filesystem(current, native.state)
            return dict(source_commit=xattr.SOURCE_COMMIT, token=native.state["token"], fsid=native.state["fsid"],
                        fs_name=native.state["names"]["fs"], fscid=native.state["fscid"], fsmap_epoch=first["epoch"],
                        before_fsmap_epoch=native.state.get("before_fsmap_epoch"),
                        default_fscid=first["default_fscid"], mdsmap_epoch=m["epoch"], metadata_pool=m["metadata_pool"],
                        data_pools=m["data_pools"], filesystem=current, fs_dump=first)
        time.sleep(.05)
    raise RuntimeError("native FSMap/MDSMap epochs did not converge within five read-only observations")


def start(native):
    if native.state["pending"] is not None or any(v is not None for v in native.state["pool_ids"].values()) or native.state["fscid"] is not None:
        raise ValueError("start cannot replay an existing native mutation scope")
    initial, pools = native.guard()
    if initial["filesystems"] or any(p["pool_name"] in native.state["names"].values() for p in pools):
        raise ValueError("UUID names must be absent before creating MDS map fixtures")
    native.state["before_fsmap_epoch"] = initial["epoch"]
    save(native.out, native.state)
    for role, pgs in (("metadata", 16), ("data", 1)):
        native.mutate("create_" + role, ["osd", "pool", "create", native.state["names"][role], str(pgs), str(pgs), "replicated", "--size", "1", "--autoscale-mode", "off"])
        _, pools = native.snapshot()
        created = [p for p in pools if p["pool_name"] == native.state["names"][role]]
        if len(created) != 1:
            raise ValueError("native pool creation has no independently observed ID")
        native.state["pool_ids"][role] = created[0]["pool"]
        native.state["pending"] = None
        save(native.out, native.state)
    native.mutate("create_fs", ["fs", "new", native.state["names"]["fs"], native.state["names"]["metadata"], native.state["names"]["data"]])
    dump, _ = native.snapshot()
    if len(dump["filesystems"]) != 1:
        raise ValueError("native filesystem creation has no independently observed ID")
    native.state["fscid"] = dump["filesystems"][0]["id"]
    native.state["pending"] = None
    save(native.out, native.state)
    return observe(native)


def complete_finish(native, deleted, retained, final_dump):
    state = native.state
    state["finished"] = True
    state["final_fs_dump"] = final_dump
    state["cleanup"] = dict(deleted=deleted, retained_for_fixture_teardown=retained)
    result = dict(source_commit=xattr.SOURCE_COMMIT, token=state["token"], fsid=state["fsid"],
                  finished=True, cleanup=state["cleanup"], unresolved_mutation=state.get("unresolved_mutation"),
                  fs_dump=final_dump, fsmap_epoch=final_dump["epoch"], default_fscid=final_dump["default_fscid"])
    if state.get("reconciled_absences"):
        result["reconciled_absences"] = state["reconciled_absences"]
    state["finish_result"] = result
    save(native.out, state)
    return result


def finish(native):
    if native.state["finished"]:
        # Return the identical retained outcome, including any unresolved
        # removal, without sending another native command.
        return native.state["finish_result"]
    dump, pools = native.guard()
    pending = native.state["pending"]
    retained, deleted = [], []
    if pending is not None:
        native.state["unresolved_mutation"] = pending
        if pending in ("remove_fs", "remove_data", "remove_metadata"):
            if pending == "remove_fs":
                present = bool(dump["filesystems"])
            else:
                role = pending[len("remove_"):]
                present = any(p["pool_name"] == native.state["names"][role] for p in pools)
            if present:
                # The previous request may still execute remotely. Do not
                # clear its pending marker or issue this removal again. The
                # disposable fixture owns final teardown of all retained IDs.
                if dump["filesystems"]:
                    retained.append(dict(kind="filesystem", name=native.state["names"]["fs"], id=native.state["fscid"],
                                         reason="unresolved native removal; no replay"))
                for role in ("data", "metadata"):
                    for p in pools:
                        if p["pool_name"] == native.state["names"][role]:
                            retained.append(dict(kind="pool", name=p["pool_name"], id=p["pool"],
                                                 reason="unresolved native removal; disposable fixture teardown required"))
                return complete_finish(native, deleted, retained, dump)
            # A read-only observation may reconcile confirmed absence. It
            # does not establish when the former request executed or replay it.
            native.state.setdefault("reconciled_absences", []).append(dict(mutation=pending, observed_absent=True))
            if pending != "remove_fs":
                role = pending[len("remove_"):]
                if role not in native.state.setdefault("removed_pools", []):
                    native.state["removed_pools"].append(role)
        # Pending creations are never replayed or adopted: their unconfirmed
        # IDs are retained below. Only independently confirmed pairs are eligible.
        native.state["pending"] = None
        save(native.out, native.state)
    if dump["filesystems"]:
        if native.state["fscid"] is None:
            retained.append(dict(kind="filesystem", name=native.state["names"]["fs"], reason="unconfirmed native mutation outcome"))
        else:
            native.mutate("remove_fs", ["fs", "rm", native.state["names"]["fs"], "--yes-i-really-mean-it"])
            remaining, _ = native.snapshot()
            if remaining["filesystems"]:
                raise RuntimeError("owned native filesystem remained after removal")
            native.state["pending"] = None
            save(native.out, native.state)
            deleted.append(dict(kind="filesystem", name=native.state["names"]["fs"], id=native.state["fscid"]))
    enabled = native.command(["config", "get", "mon", "mon_allow_pool_delete"], json_output=False)
    if enabled not in ("true", "false"):
        raise ValueError("native pool deletion policy is not an explicit boolean")
    for role in ("data", "metadata"):
        dump, pools = native.guard()
        current = [p for p in pools if p["pool_name"] == native.state["names"][role]]
        if not current:
            continue
        identity = native.state["pool_ids"][role]
        if dump["filesystems"] or identity is None or enabled != "true":
            reason = "pool deletion disabled; disposable fixture teardown owns final removal" if enabled != "true" else "unconfirmed native identity or filesystem remains"
            retained.append(dict(kind="pool", name=current[0]["pool_name"], id=current[0]["pool"], reason=reason))
            continue
        native.mutate("remove_" + role, ["osd", "pool", "delete", native.state["names"][role], native.state["names"][role], "--yes-i-really-really-mean-it"])
        _, after = native.snapshot()
        if any(p["pool"] == identity for p in after):
            raise RuntimeError("owned native pool remained after deletion")
        native.state["pending"] = None
        native.state.setdefault("removed_pools", []).append(role)
        save(native.out, native.state)
        deleted.append(dict(kind="pool", name=native.state["names"][role], id=identity))
    final_dump, _ = native.guard()
    return complete_finish(native, deleted, retained, final_dump)


def run(action, root, out):
    if action not in ("start", "observe", "finish"):
        raise ValueError("unsupported native MDS map action")
    check_input(omap.read_json(out / (NAME + "-input.json")))
    lock = out / (NAME + "-control.lock")
    with lock.open("x"):
        pass
    try:
        path = out / (NAME + "-owned.json")
        if action == "start":
            if path.exists():
                raise ValueError("MDS map start never replays an existing ownership journal")
            fsid, identity = fixture_identity(out)
            token = uuid.uuid4().hex
            state = dict(schema_version=1, source_commit=xattr.SOURCE_COMMIT, token=token, fsid=fsid,
                         fixture_ownership_sha256=identity, names=names(token), pool_ids=dict(metadata=None, data=None),
                         fscid=None, pending=None, finished=False, commands=[])
            save(out, state)
        state = load_owned(out)
        native = Native(root, out, state)
        if state["finished"]:
            if action != "finish":
                raise ValueError("MDS map scope has already finished")
            result = state["finish_result"]
        else:
            result = {"start": start, "observe": observe, "finish": finish}[action](native)
        omap.publish(out, NAME + "-result", result)
    finally:
        lock.unlink()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "observe", "finish"))
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    run(args.action, args.root, args.out)
