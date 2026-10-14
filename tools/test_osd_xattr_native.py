#!/usr/bin/env python3
"""Tests for binary-safe C adaptation, not mocked Ceph interoperability."""
import ctypes as C
import unittest

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
