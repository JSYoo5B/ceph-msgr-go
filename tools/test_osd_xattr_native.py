#!/usr/bin/env python3
"""Tests for binary-safe C adaptation, not mocked Ceph interoperability."""
import ctypes as C
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import osd_xattr_native as xattr


class Function:
    def __init__(self, callback):
        self.callback = callback

    def __call__(self, *arguments):
        return self.callback(*arguments)


class Library:
    def __init__(self):
        self.functions = {}
        self.calls = []

    def __getattr__(self, name):
        if name not in self.functions:
            self.functions[name] = Function(lambda *args: self.calls.append((name, args)) or 1)
        return self.functions[name]

    def install(self, name, callback):
        self.functions[name] = Function(callback)


class IteratorLibrary(Library):
    def __init__(self, entries):
        super().__init__()
        self.remaining = iter(entries)
        self.buffers = []
        self.old_value = None
        self.install("rados_getxattrs_next", self.next)

    def next(self, iterator, name, value, length):
        if self.old_value is not None:
            C.memset(self.old_value, 0x78, C.sizeof(self.old_value))
        entry = next(self.remaining, None)
        if entry is None:
            name._obj.value = value._obj.value = None
            length._obj.value = 0
        else:
            raw_name, raw_value = entry
            name_buffer = C.create_string_buffer(raw_name)
            self.buffers.append(name_buffer)
            name._obj.value = C.addressof(name_buffer)
            if raw_value:
                self.old_value = C.create_string_buffer(raw_value)
                self.buffers.append(self.old_value)
                value._obj.value = C.addressof(self.old_value)
            else:
                self.old_value = None
                value._obj.value = None
            length._obj.value = len(raw_value)
        return 0


class XattrAdapterTests(unittest.TestCase):
    def comparison_request(self, **changes):
        value = {"object": "owned-go-object", "namespace": "tenant-A", "compare": True,
                 "kind": "write", "baseline": xattr.encode(b"\0before\xff"),
                 "attributes": [{"name": xattr.encode(b"binary"), "value": xattr.encode(b"\0old\xff")}],
                 "operations": [{"op": "cmp", "name": xattr.encode(b"binary"),
                                 "comparison": "eq", "value": xattr.encode(b"\0old\xff")},
                                {"op": "set", "name": xattr.encode(b"binary"), "value": xattr.encode(b"\0new\xff")}],
                 "get_names": [xattr.encode(b"binary")], "seconds": 1700000002, "nanos": 314159265}
        value.update(changes)
        return value

    def comparison_native(self, cleanup_code=0, vector_error=None):
        library = Library()
        events = []
        removals = 0

        def remove(io, name):
            nonlocal removals
            removals += 1
            events.append(("remove", name))
            return 0 if removals == 1 else cleanup_code

        def seed(operation, io, name, mtime, flags):
            events.append(("seed", name, mtime._obj.tv_sec, mtime._obj.tv_nsec))
            return 0

        def full(operation, value, length):
            events.append(("baseline", C.string_at(value, length)))

        def attribute(operation, name, value, length):
            events.append(("attribute", name, C.string_at(value, length)))

        library.install("rados_remove", remove)
        library.install("rados_ioctx_set_namespace", lambda io, ns: events.append(("namespace", ns)))
        library.install("rados_write_op_operate2", seed)
        library.install("rados_write_op_write_full", full)
        library.install("rados_write_op_setxattr", attribute)
        native = xattr.Native(library)

        def snapshot(io, name, names):
            events.append(("snapshot", name, names))
            return {"code": 0, "data": "", "version": 11}

        def vector(io, name, kind, operations, mtime=None):
            events.append(("vector", name, kind, operations,
                           None if mtime is None else (mtime.tv_sec, mtime.tv_nsec)))
            if vector_error:
                raise RuntimeError(vector_error)
            return {"code": -125, "lists": [], "raw_cmp_per_op_result_available": False}

        native.snapshot, native.vector = snapshot, vector
        return native, events

    def test_compare_mode_only_mutates_mirror_and_preserves_binary_namespace_mtime(self):
        native, events = self.comparison_native()
        request = xattr.validate_request(self.comparison_request())
        result = xattr.comparison_vector(native, C.c_void_p(1),
                                         {"pool": "owned", "pool_id": 2, "object": "fixture"}, request)
        mirror = b"owned-go-object-native-xattr-compare"
        self.assertEqual(events[0], ("namespace", b"tenant-A"))
        self.assertEqual([e[1] for e in events if e[0] == "remove"], [mirror, mirror])
        self.assertEqual([e[1:] for e in events if e[0] == "seed"], [(mirror, 1700000000, 123456789)])
        self.assertIn(("baseline", b"\0before\xff"), events)
        self.assertIn(("attribute", b"binary", b"\0old\xff"), events)
        vector = next(e for e in events if e[0] == "vector")
        self.assertEqual(vector[1], mirror)
        self.assertEqual(vector[3][1]["value"], b"\0new\xff")
        self.assertEqual(vector[4], (1700000002, 314159265))
        self.assertEqual([e[1] for e in events if e[0] == "snapshot"], [b"owned-go-object", mirror, mirror])
        self.assertEqual(result["vector"]["code"], -125)
        self.assertTrue(result["cleanup"]["object_removed"])
        self.assertFalse(result["scope"]["raw_cmp_per_op_result_available"])

    def test_readonly_observation_never_resets_or_seeds_mirror(self):
        native, events = self.comparison_native()
        request = xattr.validate_request({"observe_actual": True})
        result = xattr.comparison_vector(native, C.c_void_p(1),
                                         {"pool": "owned", "pool_id": 2, "object": "fixture"}, request)
        self.assertEqual(events, [("namespace", b""), ("snapshot", b"fixture", [])])
        self.assertFalse(result["cleanup"]["mirror_touched"])
        self.assertNotIn("vector", result)

    def test_restricted_vector_uses_separate_io_while_test_io_seeds_and_cleans(self):
        native, events = self.comparison_native()
        request = xattr.validate_request(self.comparison_request(client_role="readonly"))
        captured = []
        rights = []
        remove = native.lib.rados_remove
        seed = native.lib.rados_write_op_operate2
        native.lib.install("rados_remove", lambda io, name: rights.append(("remove", io.value)) or remove(io, name))
        native.lib.install("rados_write_op_operate2", lambda op, io, *args: rights.append(("seed", io.value)) or seed(op, io, *args))
        native.vector = lambda io, *args, **kwargs: captured.append(io.value) or {
            "code": -13, "lists": [], "raw_cmp_per_op_result_available": False}
        result = xattr.comparison_vector(native, C.c_void_p(11),
                                         {"pool": "owned", "pool_id": 2, "object": "fixture"},
                                         request, vector_io=C.c_void_p(22))
        self.assertEqual(captured, [22])
        self.assertEqual(rights, [("remove", 11), ("seed", 11), ("remove", 11)])
        self.assertEqual(result["vector"]["code"], -13)
        self.assertEqual(sum(e[0] == "seed" for e in events), 1)
        self.assertEqual([e[1] for e in events if e[0] == "remove"],
                         [b"owned-go-object-native-xattr-compare"] * 2)
        self.assertTrue(result["cleanup"]["object_removed"])

    def test_fixed_client_roles_select_only_fixture_entities_and_private_paths(self):
        library = Library()
        entities = []
        paths = []
        def create(cluster, ceph, entity, flags):
            entities.append(entity)
            cluster._obj.value = len(entities)
            return 0
        library.install("rados_create2", create)
        library.install("rados_conf_read_file", lambda cluster, path: paths.append(path) or 0)
        library.install("rados_conf_set", lambda cluster, key, path: paths.append((key, path)) or 0)
        native = xattr.Native(library)
        root = Path("/owned-private")
        for role in ("test", "readonly"):
            with xattr.client_connection(native, root, {"pool": "owned"}, role):
                pass
        self.assertEqual(entities, [b"client.test", b"client.readonly"])
        self.assertEqual(paths, [os.fsencode(root / "ceph.conf"),
                                 (b"keyring", os.fsencode(root / "keyring"))] * 2)
        self.assertEqual(sum(name == "rados_shutdown" for name, _ in library.calls), 2)
        self.assertEqual(sum(name == "rados_ioctx_destroy" for name, _ in library.calls), 2)

    def test_unknown_client_roles_rejected_before_loading_library_or_c_calls(self):
        for role in (None, False, 1, "", "client.readonly", "admin", "readonly\0", ["test"]):
            with self.subTest(role=role), self.assertRaisesRegex(ValueError, "client_role"):
                xattr.validate_request(self.comparison_request(client_role=role))
        library = Library()
        native = xattr.Native(library)
        with self.assertRaisesRegex(ValueError, "client_role"):
            with xattr.client_connection(native, Path("/owned-private"), {"pool": "owned"}, "admin"):
                self.fail("invalid role admitted")
        self.assertEqual(library.calls, [])

    def test_comparison_failure_cleans_only_mirror_without_masking_first_error(self):
        native, events = self.comparison_native(cleanup_code=-5, vector_error="first vector failure")
        with self.assertRaisesRegex(RuntimeError, "first vector failure"):
            xattr.comparison_vector(native, C.c_void_p(1),
                                     {"pool": "owned", "pool_id": 2, "object": "fixture"},
                                     xattr.validate_request(self.comparison_request()))
        self.assertEqual(events[-1], ("remove", b"owned-go-object-native-xattr-compare"))
        self.assertEqual(sum(e[0] == "vector" for e in events), 1)
        self.assertEqual(sum(e[0] == "seed" for e in events), 1)

    def test_collect_rejects_invalid_admission_before_loading_native_library(self):
        requests = [self.comparison_request(object="bad\0object"),
                    self.comparison_request(namespace="bad\0namespace"),
                    self.comparison_request(kind="read"),
                    self.comparison_request(nanos=10**9),
                    self.comparison_request(seconds=1 << 32),
                    self.comparison_request(client_name="client.admin"),
                    self.comparison_request(client_role="admin"),
                    self.comparison_request(operations=[{"op": "set", "name": xattr.encode(b"n\0alias"), "value": ""}]),
                    self.comparison_request(operations=[{"op": "list", "flags": 4}]),
                    self.comparison_request(operations=[{"op": "list"}] * 65),
                    self.comparison_request(attributes=[{"name": xattr.encode(str(i).encode()), "value": ""} for i in range(64)])]
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            (out / "osd-oracle.json").write_text(json.dumps({"pool": "owned", "pool_id": 2, "object": "fixture"}))
            for request in requests:
                (out / "osd-xattr-input.json").write_text(json.dumps(request))
                with self.subTest(request=request), patch.object(xattr, "Native") as constructor:
                    with self.assertRaises(ValueError):
                        xattr.collect_comparison(out, out)
                    constructor.assert_not_called()

    def test_input_and_output_json_bounds_do_not_publish_partial_results(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            source = out / "input.json"
            source.write_bytes(b" " * (xattr.MAX_INPUT + 1))
            with self.assertRaisesRegex(ValueError, "1 MiB"):
                xattr.read_json_bounded(source)
            result = out / "osd-xattr-result.json"
            result.write_text("previous valid receipt\n")
            with self.assertRaisesRegex(ValueError, "512 KiB"):
                xattr.publish_compare_result(out, {"data": "x" * xattr.MAX_COMPARE_OUTPUT})
            self.assertEqual(result.read_text(), "previous valid receipt\n")
            self.assertFalse((out / "osd-xattr-result.tmp").exists())
            xattr.publish_compare_result(out, {"value": xattr.encode(b"\0\xff"), "code": -125})
            self.assertEqual(json.loads(result.read_text()), {"value": xattr.encode(b"\0\xff"), "code": -125})

    def test_native_write_vector_explicit_mtime_and_fixed_matrix_default_are_distinct(self):
        library = Library()
        observed = []

        def operate(operation, io, name, mtime, flags):
            observed.append(None if mtime is None else (mtime._obj.tv_sec, mtime._obj.tv_nsec))
            return 0

        library.install("rados_write_op_operate2", operate)
        native = xattr.Native(library)
        actions = [{"op": "set", "name": b"safe", "value": b"\0", "flags": 0}]
        native.vector(C.c_void_p(1), b"owned", "write", actions)
        native.vector(C.c_void_p(1), b"owned", "write", actions, mtime=xattr.Timespec(0, 0))
        native.vector(C.c_void_p(1), b"owned", "write", actions,
                      mtime=xattr.Timespec(1700000002, 314159265))
        self.assertEqual(observed, [None, (0, 0), (1700000002, 314159265)])

    def test_native_entry_flags_and_kind_admission_precedes_any_c_builder(self):
        library = Library()
        native = xattr.Native(library)
        for kind, action in (("write", {"op": "list", "flags": 0}),
                             ("read", {"op": "set", "name": b"safe", "value": b"", "flags": 0}),
                             ("write", {"op": "set", "name": b"safe", "value": b"", "flags": False}),
                             ("write", {"op": "set", "name": b"safe", "value": b"", "flags": 1})):
            with self.subTest(kind=kind, action=action), self.assertRaises(ValueError):
                native.append(C.c_void_p(1), kind, [action])
        self.assertEqual(library.calls, [])

    def snapshot_native(self, final_state=(11, 4, 1700000000, 123456789)):
        library = Library()
        library.last_version = 0
        library.stats = 0
        library.gets = 0

        def stat(io, object_name, length, mtime):
            state = (11, 4, 1700000000, 123456789) if library.stats == 0 else final_state
            library.stats += 1
            library.last_version, length._obj.value, mtime._obj.tv_sec, mtime._obj.tv_nsec = state
            return 0

        def read(io, object_name, buffer, length, offset):
            C.memmove(buffer, b"\0A\xff\0", 4)
            library.last_version = 11
            return 4

        def absent_get(io, object_name, name, buffer, length):
            library.gets += 1
            library.last_version = 0  # Pinned native -ENODATA reply default.
            return -61

        library.install("rados_stat2", stat)
        library.install("rados_read", read)
        library.install("rados_getxattr", absent_get)
        library.install("rados_get_last_version", lambda io: library.last_version)
        native = xattr.Native(library)
        native.vector = lambda *args: {"code": 0, "lists": [{"code": 0, "entries": []}]}
        return native, library

    def test_negative_get_default_last_version_does_not_imply_object_change(self):
        native, library = self.snapshot_native()
        observed = native.snapshot(C.c_void_p(1), b"owned-object", [b"missing"])
        self.assertEqual(observed["version"], 11)
        self.assertEqual(observed["data"], xattr.encode(b"\0A\xff\0"))
        self.assertEqual(observed["gets"][0]["code"], -61)
        self.assertIsNone(observed["gets"][0]["value"])
        self.assertEqual(library.gets, 1)  # No repeated failed GET.
        self.assertEqual(library.stats, 2)
        self.assertEqual(library.last_version, 11)

    def test_final_stat_recheck_detects_version_size_and_nanosecond_changes(self):
        for final_state in ((12, 4, 1700000000, 123456789),
                            (11, 5, 1700000000, 123456789),
                            (11, 4, 1700000001, 123456789),
                            (11, 4, 1700000000, 123456790)):
            native, library = self.snapshot_native(final_state)
            with self.subTest(final_state=final_state), self.assertRaisesRegex(ValueError, "state changed"):
                native.snapshot(C.c_void_p(1), b"owned-object", [b"missing"])
            self.assertEqual(library.gets, 1)
            self.assertEqual(library.stats, 2)

    def test_names_reject_empty_and_every_nul_without_native_calls(self):
        library = Library()
        native = xattr.Native(library)
        for name in (b"", b"\0name", b"name\0suffix", b"name\0"):
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, "nonempty and NUL-free"):
                    xattr.validate_request({"kind": "write", "compare": True,
                                            "operations": [{"op": "set", "name": xattr.encode(name), "value": ""}]})
                with self.assertRaisesRegex(ValueError, "nonempty and NUL-free"):
                    native.append(C.c_void_p(1), "write", [{"op": "set", "name": name,
                                                          "value": b"", "flags": 0}])
        self.assertEqual(library.calls, [])
        self.assertEqual(xattr.name_bytes(xattr.encode(b"_prefixed")), b"_prefixed")
        self.assertEqual(xattr.name_bytes(xattr.encode(b"\xff-name")), b"\xff-name")

    def test_inventory_null_empty_value_is_not_end_and_values_copy_before_next(self):
        native = xattr.Native(IteratorLibrary([(b"empty", b""), (b"z-binary", b"\0A\xff\0")]))
        self.assertEqual(native.entries(C.c_void_p(1)), [
            {"name": xattr.encode(b"empty"), "value": ""},
            {"name": xattr.encode(b"z-binary"), "value": xattr.encode(b"\0A\xff\0")},
        ])
        self.assertEqual(native.lib.rados_getxattrs_next.argtypes[2], C.POINTER(C.c_void_p))

    def test_inventory_invalid_pointer_lengths_fail_before_dereference(self):
        for fault in ("null value", "oversized value", "bad end"):
            library = Library()
            name_buffer = C.create_string_buffer(b"safe")

            def next_entry(iterator, name, value, length):
                name._obj.value = C.addressof(name_buffer)
                value._obj.value = None
                length._obj.value = 1
                if fault == "oversized value":
                    length._obj.value = xattr.MAX_VALUE + 1
                elif fault == "bad end":
                    name._obj.value = None
                return 0

            library.install("rados_getxattrs_next", next_entry)
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                xattr.Native(library).entries(C.c_void_p(1))

    def test_native_input_pointers_preserve_binary_and_empty_lengths(self):
        library = Library()
        captured = []

        def capture(operation, name, value, length):
            captured.append((name, C.string_at(value, length), length))

        library.install("rados_write_op_setxattr", capture)
        native = xattr.Native(library)
        outputs, buffers = native.append(C.c_void_p(1), "write", [
            {"op": "set", "name": b"binary", "value": b"\0A\xff\0", "flags": 0},
            {"op": "set", "name": b"empty", "value": b"", "flags": 0},
        ])
        self.assertEqual(outputs, [])
        self.assertEqual(captured, [(b"binary", b"\0A\xff\0", 4), (b"empty", b"", 0)])
        self.assertEqual(len(buffers), 2)
        self.assertEqual(library.rados_write_op_setxattr.argtypes[2], C.c_void_p)

    def test_all_six_caller_left_comparisons_include_bytes_after_nul(self):
        expected = {"eq": [False, True, False], "ne": [True, False, True],
                    "gt": [False, False, True], "gte": [False, True, True],
                    "lt": [True, False, False], "lte": [True, True, False]}
        for comparison, results in expected.items():
            self.assertEqual([xattr.string_comparison(comparison, caller, b"m\0z")
                              for caller in (b"m\0y", b"m\0z", b"m\0{")], results)

    def test_read_write_c_api_comparison_arguments_and_flags_are_exact(self):
        library = Library()
        captured = []
        for kind in ("read", "write"):
            def capture(operation, name, comparison, value, length, kind=kind):
                captured.append((kind, name, comparison, C.string_at(value, length), length))
            library.install("rados_" + kind + "_op_cmpxattr", capture)
        native = xattr.Native(library)
        for kind in ("read", "write"):
            actions = [{"op": "cmp", "name": b"binary", "comparison": op,
                        "value": b"\0\xff", "flags": 2} for op in xattr.COMPARE]
            native.append(C.c_void_p(1), kind, actions)
        self.assertEqual(captured, [(kind, b"binary", code, b"\0\xff", 2)
                                   for kind in ("read", "write") for code in range(1, 7)])
        self.assertEqual(sum("set_flags" in name for name, _ in library.calls), 12)

    def test_read_write_split_and_unknown_fields_rejected_before_any_operation(self):
        for request in (
                {"kind": "write", "operations": [{"op": "list"}]},
                {"kind": "read", "operations": [{"op": "remove", "name": xattr.encode(b"safe")}]},
                {"operations": [{"op": "cmp", "name": xattr.encode(b"safe"), "value": "", "comparison": "u64"}]},
                {"operations": [{"op": "list", "value": ""}]},
                {"operations": [{"op": "list", "flags": 1}]},
                {"kind": "write", "exists": False, "attributes": [{"name": xattr.encode(b"safe"), "value": ""}]},
        ):
            with self.subTest(request=request), self.assertRaises(ValueError):
                xattr.validate_request(request)

    def test_inventory_preserves_raw_names_and_native_order(self):
        native = xattr.Native(IteratorLibrary([(b"_prefix", b""), (b"\xff-name", b"\xff")]))
        result = native.entries(C.c_void_p(1))
        self.assertEqual(result[1]["name"], xattr.encode(b"\xff-name"))
        for entries in ([(b"same", b"1"), (b"same", b"2")], [(b"z", b""), (b"a", b"")]):
            with self.assertRaisesRegex(ValueError, "inventory order"):
                xattr.Native(IteratorLibrary(entries)).entries(C.c_void_p(1))

    def test_native_selfcheck_has_no_connection_or_server_claim(self):
        library = Library()
        result = xattr.selfcheck(xattr.Native(library))
        self.assertFalse(result["daemon_interoperability_verified"])
        self.assertFalse(result["wire_payload_verified"])
        self.assertFalse(result["raw_cmp_per_op_result_available"])
        self.assertFalse(any(name in ("rados_connect", "rados_read_op_operate", "rados_write_op_operate2")
                             for name, _ in library.calls))
        self.assertEqual(sum(name.startswith("rados_release_") for name, _ in library.calls), 2)


if __name__ == "__main__":
    unittest.main()
