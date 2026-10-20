import copy
import ctypes as C
import json
import os
from pathlib import Path
import tempfile
import unittest

import osd_ignore_cache_oracle as oracle


def plan():
    token = "012345abcdef"
    return {"source_commit": oracle.COMMIT, "fsid": "00000000-0000-0000-0000-000000000001", "token": token,
            "base": "ceph-msgr-cache-base-" + token, "tier": "ceph-msgr-cache-tier-" + token,
            "namespace": oracle.NAMESPACE, "clients": {r: "client.cache-" + r + "-" + token for r in ("rw", "ro", "base")}}


class IgnoreCacheOracleTests(unittest.TestCase):
    def test_closed_plan_rejects_foreign_target_identity_and_extra_fields(self):
        valid = plan()
        self.assertEqual(oracle.validate_plan(valid), valid)
        for field, value in (("source_commit", "unknown"), ("token", "../foreign"), ("base", valid["tier"]),
                             ("namespace", "other"), ("fsid", "not-a-fsid"), ("clients", {"rw": "client.admin"})):
            raw = copy.deepcopy(valid)
            raw[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                oracle.validate_plan(raw)
        raw = dict(valid, command="arbitrary")
        with self.assertRaises(ValueError):
            oracle.validate_plan(raw)

    def test_receipt_intent_precedes_native_call_and_unknown_is_never_repeated(self):
        receipt = {"steps": [], "complete": False}
        saved = []
        class UnknownNative:
            calls = 0
            def placement(self, pool, name): return {"pool": pool, "object": name}
            def execute(self, *args):
                self.calls += 1
                if saved[-1]["steps"][-1]["state"] != "intended":
                    raise AssertionError("no durable intent")
                raise RuntimeError("native outcome unknown")
        native = UnknownNative()
        with self.assertRaisesRegex(RuntimeError, "outcome unknown"):
            oracle.run(native, plan(), receipt, lambda: saved.append(copy.deepcopy(receipt)))
        self.assertEqual(native.calls, 1)
        self.assertFalse(receipt["complete"])
        self.assertEqual(receipt["steps"][0]["state"], "unknown")
        self.assertEqual(len(receipt["steps"]), 1)

    def test_publish_is_bounded_atomic_and_public_receipt_only(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "native.json"
            oracle.publish(path, {"steps": [], "complete": False})
            # Exact permission bits are a POSIX contract; Windows chmod
            # exposes the read-only attribute rather than this mode mask.
            if os.name == "posix":
                self.assertEqual(os.stat(path).st_mode & 0o777, 0o644)
            self.assertFalse(path.with_suffix(".tmp").exists())
            with self.assertRaises(ValueError):
                oracle.publish(path, {"oversize": "x" * (256 << 10)})
            self.assertEqual(json.loads(path.read_text())["steps"], [])

    def test_capi_flags_are_8_not_wire_0x8000_and_binary_buffers_are_live(self):
        class Library:
            def __init__(self): self.calls = []
            def rados_ioctx_create(self, cluster, pool, pointer): pointer._obj.value = 42; return 0
            def rados_ioctx_set_namespace(self, io, namespace): pass
            def rados_ioctx_destroy(self, io): pass
            def rados_get_last_version(self, io): return 7
            def rados_create_write_op(self): return 12
            def rados_release_write_op(self, op): pass
            def rados_write_op_write_full(self, op, buf, size): self.payload = C.string_at(buf, size)
            def rados_write_op_remove(self, op): self.remove = True
            def rados_write_op_operate2(self, op, io, name, stamp, flags): self.calls.append((name, flags)); return 0
        native = oracle.Native.__new__(oracle.Native)
        native.plan, native.lib = plan(), Library()
        native.cluster = lambda role: C.c_void_p(1)
        result = native.execute("rw", native.plan["tier"], "write-ignore", "write", 8)
        self.assertEqual(result["code"], 0)
        self.assertEqual(native.lib.calls, [(b"write-ignore", 8)])
        self.assertEqual(native.lib.payload, oracle.CHANGED)
        native.execute("rw", native.plan["tier"], "delete-ignore", "delete", 8)
        self.assertTrue(native.lib.remove)
        with self.assertRaises(ValueError):
            native.execute("rw", native.plan["tier"], "write-ignore", "write", 0x8000)
        with self.assertRaises(ValueError):
            native.execute("rw", "foreign-pool", "write-ignore", "write", 8)
        with self.assertRaises(ValueError):
            native.execute("rw", native.plan["tier"], "write-ignore", "write", 8, data=b"x" * (oracle.LIMIT + 1))

class IgnoreCacheReadEnvelopeTests(unittest.TestCase):
    def test_stat_and_read_are_two_ops_with_capi_ignore_cache_and_zero_prvals(self):
        class Library:
            def rados_ioctx_create(self, cluster, pool, pointer): pointer._obj.value = 42; return 0
            def rados_ioctx_set_namespace(self, io, namespace): pass
            def rados_ioctx_destroy(self, io): pass
            def rados_get_last_version(self, io): return 19
            def rados_create_read_op(self): return 11
            def rados_release_read_op(self, op): pass
            def rados_read_op_stat(self, op, size, stamp, result):
                self.stat_result = result
                size._obj.value, stamp._obj.value, result._obj.value = len(oracle.BASE), 7, 0
            def rados_read_op_read(self, op, offset, capacity, buf, count, result):
                self.read_result = result
                self.capacity = capacity
                C.memmove(buf, oracle.BASE, len(oracle.BASE))
                count._obj.value, result._obj.value = len(oracle.BASE), 0
            def rados_read_op_operate(self, op, io, name, flags): self.flags = flags; return 0
        native = oracle.Native.__new__(oracle.Native)
        native.plan, native.lib = plan(), Library()
        native.cluster = lambda role: C.c_void_p(1)
        result = native.execute("ro", native.plan["tier"], "miss", "read", 8)
        self.assertEqual(native.lib.flags, 8)
        self.assertEqual(native.lib.capacity, oracle.LIMIT)
        self.assertEqual((result["stat_prval"], result["read_prval"], result["size"], result["version"]), (0, 0, len(oracle.BASE), 19))
        self.assertEqual(result["bytes_b64"], oracle.base64.b64encode(oracle.BASE).decode())


class IgnoreCacheMatrixInventoryTests(unittest.TestCase):
    def test_complete_matrix_has_66_exact_native_calls(self):
        # A scripted C-API boundary verifies the controller's complete inventory,
        # ordering and flags. It is not a cache-server model or native proof.
        target = plan()
        script = []
        def add(role, pool, name, operation="read", flags=8, code=0, output=oracle.BASE, namespace=oracle.NAMESPACE, input_data=None):
            script.append((role, target[pool], name, operation, flags, namespace, code, output, input_data))
        for name in oracle.OBJECTS:
            add("observer", "base", name, "write", input_data=oracle.BASE)
            add("observer", "base", name)
            add("observer", "tier", name, code=-2)
        add("rw", "tier", "miss", code=-2)
        add("observer", "tier", "miss", code=-2)
        add("rw", "tier", "miss", flags=0)
        add("observer", "tier", "miss")
        add("observer", "base", "miss")
        for name, flags in (("write-normal", 0), ("write-ignore", 8)):
            add("rw", "tier", name, "write", flags, input_data=oracle.CHANGED)
            add("observer", "tier", name, output=oracle.CHANGED)
            add("observer", "base", name)
        for name, flags in (("delete-normal", 0), ("delete-ignore", 8)):
            add("rw", "tier", name, flags=0)
            add("rw", "tier", name, "delete", flags)
            add("observer", "tier", name, code=-2)
            add("rw", "tier", name, flags=0, code=-2 if flags == 0 else 0)
            add("observer", "base", name)
            add("observer", "tier", name, code=-2 if flags == 0 else 0)
        add("observer", "tier", "write-ignore", output=oracle.CHANGED)
        add("ro", "tier", "write-ignore", output=oracle.CHANGED)
        add("ro", "tier", "write-ignore", "write", code=-1, input_data=oracle.CHANGED)
        add("base", "tier", "write-ignore", code=-1)
        add("rw", "base", "write-ignore", code=-1)
        add("rw", "tier", "write-ignore", code=-1, namespace="cache-foreign")
        add("base", "base", "write-ignore")
        add("observer", "tier", "write-ignore", output=oracle.CHANGED)
        for pool in ("tier", "base"):
            for name in oracle.OBJECTS:
                add("observer", pool, name, "delete")
                add("observer", pool, name, code=-2)
        self.assertEqual(len(script), 66)
        test = self
        class ScriptedNative:
            identities = {}
            index = 0
            def placement(self, pool, name):
                return {"pool_name": pool, "object": name}
            def execute(self, role, pool, name, operation, flags, namespace, data):
                test.assertLess(self.index, len(script), "unexpected native operation")
                expected = script[self.index]
                self.index += 1
                test.assertEqual((role, pool, name, operation, flags, namespace), expected[:6])
                if expected[8] is not None:
                    test.assertEqual(data, expected[8])
                result = {"code": expected[6], "version": 19}
                if operation == "read" and expected[6] == 0:
                    result.update(read_prval=0, stat_prval=0, bytes_b64=oracle.base64.b64encode(expected[7]).decode(), size=len(expected[7]), mtime=7)
                return result
        native = ScriptedNative()
        receipt = {"steps": [], "complete": False}
        oracle.run(native, target, receipt, lambda: None)
        self.assertTrue(receipt["complete"])
        self.assertEqual(native.index, 66)
        self.assertEqual(len(receipt["steps"]), 66)
        self.assertEqual(len(receipt["placements"]), 10)
        self.assertTrue(all(step["state"] == "completed" for step in receipt["steps"]))
        self.assertEqual(len({step["label"] for step in receipt["steps"]}), 66)


if __name__ == "__main__":
    unittest.main()
