#!/usr/bin/env python3
"""Closed native counterpart for the Go IgnoreCache integration matrix.

Only sixteen owned heads in two fixture pools are accessible. Every C API call
has a published intent; a recorded uncertain call is never replayed. Ordinary
cache reads may change placement, so they receive the same once-only treatment.
The native-only oracle and its independent 66-call contract are unchanged.
"""
import base64
import ctypes as C
import json
import os
from pathlib import Path
import sys

import osd_ignore_cache_oracle as original

COMMIT, NAMESPACE = original.COMMIT, original.NAMESPACE
BASE, CHANGED, LIMIT = original.BASE, original.CHANGED, original.LIMIT
Timespec = original.Timespec
validate_plan = original.validate_plan
CASES = tuple(op + "-" + mode for op in ("stat", "read", "writefull", "delete") for mode in ("normal", "ignore"))
OBJECTS = tuple(case + "-" + actor for case in CASES for actor in ("go", "native"))
RECEIPT = "osd-cache-pools-native.json"


def publish(path, value):
    data = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    if len(data) > 1 << 20:
        raise ValueError("native integration receipt exceeds bound")
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(data)
    os.chmod(temporary, 0o644)
    os.replace(temporary, path)


def validate_topology(dump, plan):
    if not isinstance(dump, dict) or type(dump.get("epoch")) is not int or dump["epoch"] <= 0:
        raise ValueError("invalid native cache topology epoch")
    pools = dump.get("pools")
    if not isinstance(pools, list):
        raise ValueError("invalid native cache pools")
    selected = {}
    for role in ("base", "tier"):
        matches = [p for p in pools if isinstance(p, dict) and p.get("pool_name") == plan[role]]
        if len(matches) != 1:
            raise ValueError("owned native cache pool missing or ambiguous")
        p = matches[0]
        if (type(p.get("pool")) is not int or p["pool"] < 0 or p.get("type") != 1
                or p.get("size") != 1 or p.get("min_size") != 1
                or type(p.get("flags")) is not int or p["flags"] & 1
                or "hashpspool" in p.get("flags_names", "").split(",")
                or p.get("read_tier") != -1 or p.get("write_tier") != -1 or p.get("pool_snaps") != []):
            raise ValueError("owned native pool is not head replicated no-overlay/no-HASHPSPOOL")
        selected[role] = p
    base, tier = selected["base"], selected["tier"]
    if (base["pool"] == tier["pool"] or base.get("tier_of") != -1 or base.get("cache_mode") != "none"
            or base.get("tiers") != [tier["pool"]] or tier.get("tier_of") != base["pool"]
            or tier.get("tiers") != [] or tier.get("cache_mode") != "writeback"
            or tier.get("hit_set_params", {}).get("type") != "none"):
        raise ValueError("native cache relationship differs")
    return {"epoch": dump["epoch"], **selected}


class Native(original.Native):
    def topology(self):
        dump = original.mutation.native_mon_query(self.lib, self.cluster("observer"), "osd", "dump", timeout=10)
        return validate_topology(dump, self.plan)

    def foreign_placement(self):
        pool, name, namespace = self.plan["tier"], "writefull-ignore-go", "cache-foreign"
        cluster = self.cluster("observer")
        def command(*args, timeout):
            return original.mutation.native_mon_query(self.lib, cluster, *args, timeout=timeout)
        mapped = command("osd", "map", pool, name, namespace, timeout=10)
        result = original.mutation.collect_placement(command, pool, name, mapped["pool_id"], namespace=namespace)
        if result["pool_type"] != 1 or result["replicas"] != 1 or result["acting"] != [0] or result["primary_id"] != 0:
            raise ValueError("foreign namespace probe differs from the owned single replicated OSD")
        result["pool_name"] = pool
        return result

    def execute(self, role, pool, name, op, flags, namespace=NAMESPACE, data=CHANGED):
        if (role not in ("observer", "rw") or pool not in (self.plan["base"], self.plan["tier"])
                or name not in OBJECTS or op not in ("stat", "read", "writefull", "delete", "snapshot")
                or flags not in (0, 8) or namespace != NAMESPACE or data not in (BASE, CHANGED)):
            raise ValueError("operation outside closed integration matrix")
        case_op, mode, actor = name.split("-")
        if role == "rw":
            counterpart = actor == "native" and op == case_op and flags == (8 if mode == "ignore" else 0)
            followup = case_op == "delete" and op == "read" and flags == 0
            if pool != self.plan["tier"] or not (counterpart or followup) or data != CHANGED:
                raise ValueError("foreign native counterpart operation")
        if role == "observer" and (flags != 8 or op not in ("snapshot", "writefull", "delete")
                                   or op == "writefull" and data != BASE):
            raise ValueError("observer operation may not proxy or promote")
        io = C.c_void_p()
        self.check(self.lib.rados_ioctx_create(self.cluster(role), pool.encode(), C.byref(io)))
        self.lib.rados_ioctx_set_namespace(io, namespace.encode())
        try:
            if op in ("stat", "read", "snapshot"):
                operation = self.lib.rados_create_read_op()
                if not operation:
                    raise RuntimeError("native read allocation failed")
                buf, length, size, stamp = C.create_string_buffer(LIMIT), C.c_size_t(), C.c_uint64(), C.c_long()
                read_result, stat_result = C.c_int(-99999), C.c_int(-99999)
                try:
                    if op in ("stat", "snapshot"):
                        self.lib.rados_read_op_stat(operation, C.byref(size), C.byref(stamp), C.byref(stat_result))
                    if op in ("read", "snapshot"):
                        self.lib.rados_read_op_read(operation, 0, LIMIT, buf, C.byref(length), C.byref(read_result))
                    result = {"code": int(self.lib.rados_read_op_operate(operation, io, name.encode(), flags))}
                    if length.value > LIMIT:
                        raise ValueError("native output exceeds read budget")
                    if op in ("stat", "snapshot"):
                        result["stat_prval"] = stat_result.value
                        if result["code"] >= 0:
                            result.update(size=size.value, mtime=stamp.value)
                    if op in ("read", "snapshot"):
                        result["read_prval"] = read_result.value
                        if result["code"] >= 0:
                            result["bytes_b64"] = base64.b64encode(buf.raw[:length.value]).decode()
                finally:
                    self.lib.rados_release_read_op(operation)
            else:
                operation = self.lib.rados_create_write_op()
                if not operation:
                    raise RuntimeError("native write allocation failed")
                try:
                    if op == "writefull":
                        buf = C.create_string_buffer(data)
                        self.lib.rados_write_op_write_full(operation, buf, len(data))
                    else:
                        self.lib.rados_write_op_remove(operation)
                    stamp = Timespec(1700000800, 0)
                    result = {"code": int(self.lib.rados_write_op_operate2(operation, io, name.encode(), C.byref(stamp), flags))}
                finally:
                    self.lib.rados_release_write_op(operation)
            result["version"] = int(self.lib.rados_get_last_version(io))
            return result
        finally:
            self.lib.rados_ioctx_destroy(io)


class Controller:
    def __init__(self, native, plan, receipt, save):
        self.native, self.plan, self.receipt, self.save = native, validate_plan(plan), receipt, save

    def persist(self):
        identities = self.receipt.setdefault("native_identities", {})
        identities.update(self.native.identities)
        self.save()

    def capture_identity(self, step, role):
        identity = self.native.identities.get(role)
        if identity is not None:
            step["native_identity"] = dict(identity)

    def call(self, label, role, pool, name, operation="snapshot", flags=8, data=CHANGED):
        steps = self.receipt["steps"]
        prior = [step for step in steps if step["label"] == label]
        if prior:
            step = prior[0]
            if step["state"] != "completed":
                raise RuntimeError("native outcome unknown; will not repeat " + label)
            return step["result"]
        step = {"label": label, "identity_role": role, "pool": pool, "namespace": NAMESPACE,
                "object": name, "operation": operation, "capi_flags": flags, "state": "intended",
                "may_mutate": operation in ("writefull", "delete") or flags == 0}
        steps.append(step)
        self.persist()
        try:
            result = self.native.execute(role, pool, name, operation, flags, NAMESPACE, data)
        except BaseException as exc:
            step.update(state="unknown", error=str(exc)[:1024])
            self.capture_identity(step, role)
            self.persist()
            raise
        step["result"] = result
        step["state"] = "unknown" if result["code"] in original.UNKNOWN else "completed"
        self.capture_identity(step, role)
        self.persist()
        if step["state"] == "unknown":
            raise RuntimeError("native outcome unknown; will not repeat " + label)
        return result

    def snapshot(self, label, name):
        states = {}
        for pool in ("base", "tier"):
            state = self.call(label + "-" + pool, "observer", self.plan[pool], name)
            if state["code"] not in (0, -2):
                raise RuntimeError("native observation failed: " + label)
            if state["code"] == 0 and (state["stat_prval"] != 0 or state["read_prval"] != 0):
                raise RuntimeError("native snapshot per-operation result differs: " + label)
            states[pool] = state
        return states

    def prepare(self):
        if self.receipt.get("phase") in ("prepared", "observed", "native", "finished"):
            return
        if "pool_topology" not in self.receipt:
            self.receipt["pool_topology"] = self.native.topology()
            self.persist()
        if "foreign_placement" not in self.receipt:
            self.receipt["foreign_placement"] = self.native.foreign_placement()
            self.persist()
        placements = self.receipt["placements"]
        for pool in ("base", "tier"):
            for name in OBJECTS:
                if any(p.get("pool_name") == self.plan[pool] and p.get("object") == name for p in placements):
                    continue
                placed = self.native.placement(self.plan[pool], name)
                # collect_placement uses 'object'; retain the closed target even
                # if a fake/test placement omits that redundant field.
                placed.update(pool_name=self.plan[pool], object=name)
                placements.append(placed)
                self.persist()
        for name in OBJECTS:
            for pool in (("base", "tier") if name.startswith("delete-") else ("base",)):
                seeded = self.call("seed-" + pool + "-" + name, "observer", self.plan[pool], name, "writefull", data=BASE)
                if seeded["code"] != 0:
                    raise RuntimeError("native baseline seed failed")
            states = self.snapshot("baseline-" + name, name)
            for pool, state in states.items():
                present = pool == "base" or name.startswith("delete-")
                if state["code"] != (0 if present else -2):
                    raise RuntimeError("native baseline presence differs")
                if present and (state.get("bytes_b64") != base64.b64encode(BASE).decode()
                                or state.get("size") != len(BASE) or not state.get("version")
                                or state.get("mtime") != 1700000800):
                    raise RuntimeError("native baseline bytes/size/version/mtime differ")
            self.receipt["baseline"][name] = states
            self.persist()
        self.receipt["phase"] = "prepared"
        self.persist()

    def observe(self, case):
        self.require_case(case)
        entry = self.receipt["cases"].setdefault(case, {})
        observations = entry.setdefault("go_observations", [])
        observed = self.snapshot("go-observe-%s-%d" % (case, len(observations)), case + "-go")
        observations.append(observed)
        entry["go_after"] = observed
        self.receipt["phase"] = "observed"
        self.persist()

    def native_case(self, case):
        self.require_case(case)
        entry = self.receipt["cases"].get(case, {})
        if "go_after" not in entry:
            raise ValueError("observe Go result before native counterpart")
        entry.setdefault("go_after_native_comparison", entry["go_after"])
        operation, mode = case.split("-")
        entry["native_result"] = self.call("native-" + case, "rw", self.plan["tier"], case + "-native", operation, 8 if mode == "ignore" else 0)
        entry["native_after"] = self.snapshot("native-after-" + case, case + "-native")
        if operation == "delete":
            for actor in ("go", "native"):
                entry[actor + "_followup"] = self.call(actor + "-followup-" + case, "rw", self.plan["tier"], case + "-" + actor, "read", 0)
                entry[actor + "_after_followup"] = self.snapshot(actor + "-after-followup-" + case, case + "-" + actor)
        entry["complete"] = True
        self.receipt["phase"] = "native"
        self.persist()

    def require_case(self, case):
        if case not in CASES:
            raise ValueError("foreign integration case")
        if len(self.receipt["baseline"]) != len(OBJECTS) or self.receipt.get("phase") == "finished":
            raise ValueError("integration matrix is not prepared or is finished")

    def finish(self):
        if self.receipt.get("phase") == "finished":
            return
        for pool in ("tier", "base"):
            for name in OBJECTS:
                result = self.call("cleanup-" + pool + "-" + name, "observer", self.plan[pool], name, "delete")
                if result["code"] not in (0, -2):
                    raise RuntimeError("owned head cleanup failed")
                absent = self.call("cleanup-absent-" + pool + "-" + name, "observer", self.plan[pool], name)
                if absent["code"] != -2:
                    raise RuntimeError("owned head remains after cleanup")
        cases_complete = set(self.receipt["cases"]) == set(CASES) and all(e.get("complete") for e in self.receipt["cases"].values())
        calls_complete = all(step["state"] == "completed" for step in self.receipt["steps"])
        self.receipt.update(complete=cases_complete and calls_complete, cases_complete=cases_complete,
                            cleanup_complete=True, phase="finished",
                            whiteout_evidence="subsequent native read behavior only; raw FLAG_WHITEOUT was not observed")
        self.persist()


def main(args):
    if len(args) not in (4, 5):
        raise ValueError("usage: osd_ignore_cache_integration_oracle.py ROOT OUT ACTION [CASE]")
    root, out, action = Path(args[1]), Path(args[2]), args[3]
    if action not in ("prepare", "observe", "native", "finish") or (action in ("observe", "native")) != (len(args) == 5):
        raise ValueError("invalid closed integration action")
    if len(args) == 5 and args[4] not in CASES:
        raise ValueError("foreign integration case")
    raw = (out / "osd-ignore-cache-plan.json").read_bytes()
    if len(raw) > 16384:
        raise ValueError("plan exceeds bound")
    plan = validate_plan(json.loads(raw))
    path = out / RECEIPT
    if path.exists():
        raw = path.read_bytes()
        if len(raw) > 1 << 20:
            raise ValueError("receipt exceeds bound")
        receipt = json.loads(raw)
        if receipt.get("source_commit") != COMMIT or receipt.get("plan") != plan:
            raise ValueError("foreign integration receipt")
    elif action == "prepare":
        receipt = {"schema_version": 1, "source_commit": COMMIT, "plan": plan, "complete": False,
                   "phase": "preparing", "steps": [], "placements": [], "baseline": {}, "cases": {}, "native_identities": {}}
    else:
        raise ValueError("integration receipt is absent")
    save = lambda: publish(path, receipt)
    save()
    native = Native(root, out, plan)
    controller = Controller(native, plan, receipt, save)
    try:
        getattr(controller, "native_case" if action == "native" else action)(*args[4:])
    except BaseException as exc:
        receipt["error"] = str(exc)[:1024]
        controller.persist()
        raise
    finally:
        native.close()
    print(json.dumps({"complete": receipt["complete"], "phase": receipt["phase"], "steps": len(receipt["steps"])}))


if __name__ == "__main__":
    main(sys.argv)
