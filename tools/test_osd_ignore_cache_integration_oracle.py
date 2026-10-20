import base64
import copy
import ctypes as C
import json
import os
from pathlib import Path
import tempfile
import unittest

import osd_ignore_cache_integration_oracle as oracle
from test_osd_ignore_cache_oracle import plan


def receipt():
    return {"schema_version": 1, "source_commit": oracle.COMMIT, "plan": plan(), "complete": False,
            "phase": "preparing", "steps": [], "placements": [], "baseline": {}, "cases": {}, "native_identities": {}}


class ScriptedNative:
    """A controller fake, not a model or evidence of Ceph cache behavior."""
    def __init__(self):
        self.calls, self.placements, self.heads, self.identities = [], [], {}, {}
        self.before = lambda: None

    def topology(self):
        return {"epoch": 7}

    def foreign_placement(self):
        return {"pool_name": plan()["tier"], "object": "writefull-ignore-go", "namespace": "cache-foreign", "primary_id": 0, "raw_hash": 41}

    def placement(self, pool, name):
        self.placements.append((pool, name))
        return {"pool_name": pool, "object": name, "primary_id": 0}

    def execute(self, role, pool, name, operation, flags, namespace, data):
        self.before()
        self.calls.append((role, pool, name, operation, flags, namespace, data))
        self.identities[role] = {"name": "client.test" if role == "observer" else plan()["clients"][role], "global_id": 7}
        key = (pool, name)
        if operation == "writefull":
            self.heads[key] = data
            return {"code": 0, "version": 17}
        if operation == "delete":
            existed = key in self.heads
            self.heads.pop(key, None)
            return {"code": 0 if existed else -2, "version": 19}
        if key not in self.heads:
            return {"code": -2, "version": 0, "stat_prval": -2, "read_prval": -99999}
        result = {"code": 0, "version": 17}
        if operation in ("snapshot", "stat"):
            result.update(stat_prval=0, size=len(self.heads[key]), mtime=1700000800)
        if operation in ("snapshot", "read"):
            result.update(read_prval=0, bytes_b64=base64.b64encode(self.heads[key]).decode())
        return result


class ControllerTests(unittest.TestCase):
    def setup_controller(self):
        native, state, saved = ScriptedNative(), receipt(), []
        controller = oracle.Controller(native, plan(), state, lambda: saved.append(copy.deepcopy(state)))
        native.before = lambda: self.assertEqual(saved[-1]["steps"][-1]["state"], "intended")
        return controller, native, state, saved

    def test_closed_inventory_prepare_and_baselines(self):
        controller, native, state, saved = self.setup_controller()
        controller.prepare()
        self.assertEqual(len(set(oracle.OBJECTS)), 16)
        self.assertEqual(len(oracle.CASES), 8)
        self.assertEqual(len(native.placements), 32)
        self.assertEqual(state["foreign_placement"], {"pool_name": plan()["tier"], "object": "writefull-ignore-go", "namespace": "cache-foreign", "primary_id": 0, "raw_hash": 41})
        self.assertTrue(all(call[5] == oracle.NAMESPACE for call in native.calls))
        self.assertEqual(set(native.placements), {(pool, name) for pool in (plan()["base"], plan()["tier"]) for name in oracle.OBJECTS})
        self.assertEqual(len(native.calls), 52)  # 16 base seeds, 4 tier seeds, 32 snapshots.
        for name, states in state["baseline"].items():
            self.assertEqual(states["base"]["code"], 0)
            self.assertEqual(states["tier"]["code"], 0 if name.startswith("delete-") else -2)
            self.assertEqual(states["base"]["mtime"], 1700000800)
        self.assertFalse(state["complete"])
        self.assertEqual(state["phase"], "prepared")
        self.assertEqual(len(saved[-1]["placements"]), 32)
        count = len(native.calls)
        controller.prepare()
        self.assertEqual(len(native.calls), count)

    def test_observe_native_and_delete_followups_are_once_only(self):
        controller, native, state, _ = self.setup_controller()
        controller.prepare()
        for case in oracle.CASES:
            with self.subTest(case=case):
                with self.assertRaisesRegex(ValueError, "observe Go"):
                    controller.native_case(case)
                controller.observe(case)
                entry = state["cases"][case]
                self.assertEqual(set(entry["go_after"]), {"base", "tier"})
                controller.native_case(case)
                count = len(native.calls)
                result = copy.deepcopy(entry)
                controller.native_case(case)
                self.assertEqual(len(native.calls), count)
                self.assertEqual(entry, result)
                controller.observe(case)
                self.assertEqual(len(native.calls), count + 2)
                self.assertEqual(len(entry["go_observations"]), 2)
                self.assertEqual(entry["go_after_native_comparison"], result["go_after"])
                standalone = [call for call in native.calls if call[2] == case + "-native" and call[0] == "rw"]
                self.assertEqual(standalone[0][3:5], (case.split("-")[0], 8 if case.endswith("ignore") else 0))
                if case.startswith("delete-"):
                    self.assertEqual(standalone[1][3:5], ("read", 0))
                    self.assertTrue(all(k in entry for k in ("go_followup", "native_followup", "go_after_followup", "native_after_followup")))
        self.assertEqual(set(state["cases"]), set(oracle.CASES))
        self.assertEqual(set(state["native_identities"]), {"observer", "rw"})
        before = len(native.calls)
        controller.finish()
        cleanup = native.calls[before:]
        self.assertEqual(len(cleanup), 64)
        self.assertEqual([(c[1], c[2], c[3], c[4]) for c in cleanup],
                         [(pool, name, op, 8) for pool in (plan()["tier"], plan()["base"]) for name in oracle.OBJECTS for op in ("delete", "snapshot")])
        self.assertTrue(state["complete"])
        self.assertEqual(state["phase"], "finished")
        self.assertFalse(native.heads)
        controller.finish()
        self.assertEqual(len(native.calls), before + 64)
        with self.assertRaises(ValueError):
            controller.observe("read-normal")

    def test_pending_unknown_exception_and_errno_never_reexecute_after_reload(self):
        for failure in (RuntimeError("transport died"), {"code": -110, "version": 0}):
            controller, native, state, saved = self.setup_controller()
            original = native.execute
            def fail(*args):
                native.before()
                native.calls.append(args)
                native.identities["rw"] = {"name": plan()["clients"]["rw"], "global_id": 31}
                if isinstance(failure, Exception):
                    raise failure
                return failure
            native.execute = fail
            with self.assertRaises(RuntimeError):
                controller.call("one", "rw", plan()["tier"], "read-normal-native", "read", 0)
            self.assertEqual(state["steps"][0]["state"], "unknown")
            self.assertEqual(state["steps"][0]["native_identity"]["global_id"], 31)
            self.assertEqual(saved[0]["steps"][0]["state"], "intended")
            native.execute = original
            reloaded = copy.deepcopy(state)
            again = oracle.Controller(native, plan(), reloaded, lambda: None)
            with self.assertRaisesRegex(RuntimeError, "will not repeat"):
                again.call("one", "rw", plan()["tier"], "read-normal-native", "read", 0)
            self.assertEqual(len(native.calls), 1)
            reloaded["steps"][0]["state"] = "intended"  # Crash before outcome publication.
            with self.assertRaisesRegex(RuntimeError, "will not repeat"):
                again.call("one", "rw", plan()["tier"], "read-normal-native", "read", 0)
            self.assertEqual(len(native.calls), 1)

    def test_step_identity_is_copied_and_survives_a_later_action_global_id(self):
        first, native, state, _ = self.setup_controller()
        first.call("first-action", "rw", plan()["tier"], "stat-ignore-native", "stat", 8)
        self.assertEqual(state["steps"][0]["native_identity"]["global_id"], 7)
        native.identities["rw"]["global_id"] = 99
        self.assertEqual(state["steps"][0]["native_identity"]["global_id"], 7)
        later_native = ScriptedNative()
        execute = later_native.execute
        def later_execute(*args):
            result = execute(*args)
            later_native.identities["rw"]["global_id"] = 101
            return result
        later_native.execute = later_execute
        later = oracle.Controller(later_native, plan(), state, lambda: None)
        later.call("second-action", "rw", plan()["tier"], "stat-normal-native", "stat", 0)
        self.assertEqual([step["native_identity"]["global_id"] for step in state["steps"]], [7, 101])
        self.assertEqual(state["native_identities"]["rw"]["global_id"], 101)

    def test_case_and_phase_are_closed(self):
        controller, native, state, _ = self.setup_controller()
        for case in ("read", "read-normal-go", "read-normal;delete", "../read-normal"):
            with self.assertRaises(ValueError):
                controller.observe(case)
        with self.assertRaises(ValueError):
            controller.observe("read-normal")
        self.assertFalse(native.calls)

    def test_partial_finish_cleans_all_owned_heads_without_claiming_case_success(self):
        controller, native, state, _ = self.setup_controller()
        controller.prepare()
        state["steps"].append({"label": "earlier-unknown", "state": "unknown"})
        controller.finish()
        self.assertFalse(state["complete"])
        self.assertFalse(state["cases_complete"])
        self.assertTrue(state["cleanup_complete"])
        self.assertEqual(state["steps"][52]["state"], "unknown")
        self.assertFalse(native.heads)
        count = len(native.calls)
        controller.finish()
        self.assertEqual(len(native.calls), count)

    def test_publish_public_atomic_and_one_megabyte_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "native.json"
            oracle.publish(path, {"larger_than_old_limit": "x" * (270 << 10)})
            # Keep atomic/bounds/content coverage on every platform, while
            # checking the exact permission mask only where POSIX defines it.
            if os.name == "posix":
                self.assertEqual(os.stat(path).st_mode & 0o777, 0o644)
            self.assertFalse(path.with_suffix(".tmp").exists())
            with self.assertRaises(ValueError):
                oracle.publish(path, {"oversized": "x" * (1 << 20)})
            self.assertIn("larger_than_old_limit", json.loads(path.read_bytes()))

    def test_topology_rejects_overlay_hashpspool_foreign_relation_and_ambiguity(self):
        common = {"type": 1, "size": 1, "min_size": 1, "flags": 0, "flags_names": "",
                  "read_tier": -1, "write_tier": -1, "pool_snaps": [], "hit_set_params": {"type": "none"}}
        base = dict(common, pool=2, pool_name=plan()["base"], tier_of=-1, cache_mode="none", tiers=[3])
        tier = dict(common, pool=3, pool_name=plan()["tier"], tier_of=2, cache_mode="writeback", tiers=[])
        dump = {"epoch": 7, "pools": [base, tier]}
        self.assertEqual(oracle.validate_topology(dump, plan())["tier"]["pool"], 3)
        for index, field, value in ((0, "flags", 1), (1, "flags_names", "nodelete,hashpspool"),
                                    (0, "read_tier", 3), (1, "write_tier", 3),
                                    (1, "tier_of", 4), (1, "size", 2),
                                    (1, "hit_set_params", {"type": "bloom"}), (0, "pool_snaps", [{}])):
            bad = copy.deepcopy(dump)
            bad["pools"][index][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                oracle.validate_topology(bad, plan())
        with self.assertRaises(ValueError):
            oracle.validate_topology({"epoch": 7, "pools": [base, tier, tier]}, plan())

    def test_cli_invalid_action_is_rejected_before_native_construction(self):
        for argv in (("x", "root", "out"), ("x", "root", "out", "read"),
                     ("x", "root", "out", "prepare", "read-normal"),
                     ("x", "root", "out", "native"),
                     ("x", "root", "out", "observe", "foreign")):
            with self.assertRaises(ValueError):
                oracle.main(argv)


class Library:
    def __init__(self): self.ops, self.flags = [], []
    def rados_ioctx_create(self, cluster, pool, pointer): pointer._obj.value = 42; return 0
    def rados_ioctx_set_namespace(self, io, namespace): self.namespace = namespace
    def rados_ioctx_destroy(self, io): pass
    def rados_get_last_version(self, io): return 23
    def rados_create_read_op(self): return 11
    def rados_release_read_op(self, op): pass
    def rados_read_op_stat(self, op, size, stamp, result):
        self.ops.append("stat")
        size._obj.value, stamp._obj.value, result._obj.value = len(oracle.BASE), 1700000800, 0
    def rados_read_op_read(self, op, offset, limit, buf, length, result):
        self.ops.append("read")
        C.memmove(buf, oracle.BASE, len(oracle.BASE))
        length._obj.value, result._obj.value = len(oracle.BASE), len(oracle.BASE)
    def rados_read_op_operate(self, op, io, name, flags): self.flags.append(flags); return 0
    def rados_create_write_op(self): return 12
    def rados_release_write_op(self, op): pass
    def rados_write_op_write_full(self, op, data, length): self.ops.append("writefull"); self.data = C.string_at(data, length)
    def rados_write_op_remove(self, op): self.ops.append("delete")
    def rados_write_op_operate2(self, op, io, name, stamp, flags): self.flags.append(flags); self.stamp = stamp._obj.tv_sec; return 0


class CAPIEnvelopeTests(unittest.TestCase):
    def native(self):
        native = oracle.Native.__new__(oracle.Native)
        native.plan, native.lib = plan(), Library()
        native.cluster = lambda role: C.c_void_p(1)
        return native

    def test_standalone_operations_do_not_bundle_stat_and_read(self):
        for op in ("stat", "read", "writefull", "delete"):
            for mode, flag in (("normal", 0), ("ignore", 8)):
                with self.subTest(operation=op, mode=mode):
                    native = self.native()
                    result = native.execute("rw", plan()["tier"], op + "-" + mode + "-native", op, flag)
                    self.assertEqual(native.lib.ops, [op])
                    self.assertEqual(native.lib.flags, [flag])
                    self.assertEqual(result["version"], 23)
                    if op == "read":
                        self.assertEqual(result["read_prval"], len(oracle.BASE))
                        self.assertEqual(base64.b64decode(result["bytes_b64"]), oracle.BASE)
                        self.assertNotIn("size", result)
                    if op == "stat":
                        self.assertNotIn("read_prval", result)
                        self.assertEqual(result["size"], len(oracle.BASE))
                    if op == "writefull":
                        self.assertEqual(native.lib.data, oracle.CHANGED)
                        self.assertEqual(native.lib.stamp, 1700000800)

    def test_observer_snapshot_bundles_two_ops_only_with_ignore_cache(self):
        native = self.native()
        result = native.execute("observer", plan()["base"], "read-normal-go", "snapshot", 8)
        self.assertEqual(native.lib.ops, ["stat", "read"])
        self.assertEqual(native.lib.flags, [8])
        self.assertEqual(result["stat_prval"], 0)
        self.assertEqual(result["read_prval"], len(oracle.BASE))

    def test_foreign_placement_uses_only_fixed_readonly_native_queries(self):
        from unittest.mock import patch
        native = self.native()
        calls = []
        def query(lib, cluster, *args, timeout):
            calls.append((args, timeout))
            return {"pool_id": 3}
        def collect(command, pool, name, pool_id, *, namespace):
            self.assertEqual((pool, name, pool_id, namespace), (plan()["tier"], "writefull-ignore-go", 3, "cache-foreign"))
            command("osd", "map", pool, name, namespace, timeout=10)
            command("osd", "dump", timeout=10)
            return {"pool_type": 1, "replicas": 1, "acting": [0], "primary_id": 0, "namespace": namespace, "raw_hash": 81}
        with patch.object(oracle.original.mutation, "native_mon_query", query), patch.object(oracle.original.mutation, "collect_placement", collect):
            placed = native.foreign_placement()
        self.assertEqual(placed["pool_name"], plan()["tier"])
        self.assertEqual(placed["raw_hash"], 81)
        self.assertEqual(calls, [(("osd", "map", plan()["tier"], "writefull-ignore-go", "cache-foreign"), 10),
                                 (("osd", "map", plan()["tier"], "writefull-ignore-go", "cache-foreign"), 10),
                                 (("osd", "dump"), 10)])
        self.assertFalse(native.lib.ops)

    def test_closed_capi_scope_rejects_foreign_objects_pools_flags_and_roles(self):
        native = self.native()
        valid = ["rw", plan()["tier"], "writefull-ignore-native", "writefull", 8]
        for index, value in ((0, "admin"), (1, plan()["base"]), (2, "miss"), (2, "writefull-ignore-go"), (3, "stat"), (4, 0), (4, 0x8000)):
            args = list(valid)
            args[index] = value
            with self.assertRaises(ValueError):
                native.execute(*args)
        with self.assertRaises(ValueError):
            native.execute(*valid, namespace="foreign")
        with self.assertRaises(ValueError):
            native.execute(*valid, data=b"arbitrary")
        with self.assertRaises(ValueError):
            native.execute("observer", plan()["tier"], "read-normal-go", "snapshot", 0)
        self.assertFalse(native.lib.ops)


if __name__ == "__main__":
    unittest.main()
