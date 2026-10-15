#!/usr/bin/env python3
"""Admission/argument/lifetime tests; no mocked Ceph compatibility claim."""
import ctypes as C
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

import osd_omap_write_native as writes
from test_osd_xattr_native import Library

encode = writes.xattr.encode
METADATA = {"pool": "pool", "pool_id": 7, "object": "fixture"}


def owned(label):
    return (b"fixture-native-omapw-" + label + b"0123456789abcdef0123456789abcdef").decode()


class OmapWriteNativeTests(unittest.TestCase):
    def test_admits_bounded_binary_inputs_and_rejects_ambiguity(self):
        prepared = writes.validate_request({"seed": {"baseline": encode(b"\0\xff"),
                                                     "entries": [{"key": "", "value": ""},
                                                                 {"key": encode(b"\xff\0k"), "value": encode(b"\0")}]},
                                            "reserve": 3}, "prepare")
        self.assertEqual(prepared["seed"]["entries"], [(b"", b""), (b"\xff\0k", b"\0")])
        self.assertEqual(prepared["reserve"], 3)
        compared = writes.validate_request({"seed": None, "mtime": {"seconds": 1, "nanos": 2},
                                            "operations": [{"op": "clear", "flags": 2},
                                                           {"op": "set", "flags": 0, "entries": [{"key": encode(b"k"), "value": ""}]},
                                                           {"op": "remove", "flags": 0, "keys": [encode(b"b"), encode(b"a")]}]},
                                           "compare")
        self.assertIsNone(compared["seed"])
        self.assertEqual(compared["operations"][2]["keys"], [b"a", b"b"])
        for raw, mode in [
            ({"reserve": 9}, "prepare"),
            ({"unknown": 1}, "prepare"),
            ({"object": "x", "seed": {}}, "observe"),
            ({}, "observe"),
            ({"seed": None, "operations": [{"op": "clear"}]}, "compare"),
            ({"seed": None, "mtime": {"seconds": 1, "nanos": 10**9}, "operations": [{"op": "clear"}]}, "compare"),
            ({"seed": None, "mtime": {"seconds": 1, "nanos": 0}, "operations": []}, "compare"),
            ({"seed": None, "mtime": {"seconds": 1, "nanos": 0}, "operations": [{"op": "header"}]}, "compare"),
            ({"seed": None, "mtime": {"seconds": 1, "nanos": 0}, "operations": [{"op": "clear", "flags": 4}]}, "compare"),
            ({"seed": None, "mtime": {"seconds": 1, "nanos": 0}, "operations": [{"op": "clear", "keys": []}]}, "compare"),
            ({"seed": None, "mtime": {"seconds": 1, "nanos": 0}, "operations": [{"op": "remove", "keys": ["", ""]}]}, "compare"),
            ({"seed": {"entries": [{"key": "", "value": ""}, {"key": "", "value": encode(b"x")}]}}, "prepare"),
            ({"seed": {"attributes": [{"name": "", "value": ""}]}}, "prepare"),
            ({"inventory_keys": ["", ""]}, "observe"),
            ({}, "delete"),
        ]:
            with self.subTest(raw=raw, mode=mode), self.assertRaises((ValueError, KeyError, TypeError)):
                writes.validate_request(raw, mode)

    def test_comparison_inputs_keep_bytes_and_bounded_identifiers(self):
        request = writes.validate_request({"object": owned(b"main-"), "operations": [
            {"op": "cmp", "flags": 2, "key": encode(b"\0k"), "value": encode(b"\x80\0"), "comparison": 5},
            {"op": "cmp", "flags": 0, "key": "", "value": "", "comparison": 4}]}, "assert")
        self.assertEqual(request["operations"][0], {"op": "cmp", "flags": 2, "key": b"\0k", "value": b"\x80\0", "comparison": 5})
        for action in [{"op": "set", "flags": 0, "entries": []}, {"op": "cmp", "key": "", "value": "", "comparison": 256},
                       {"op": "cmp", "key": "", "value": "", "comparison": -1}, {"op": "cmp", "key": "", "comparison": 1, "keys": []}]:
            with self.subTest(action=action), self.assertRaises(ValueError):
                writes.validate_request({"object": owned(b"main-"), "operations": [action]}, "assert")

    def test_comparison_vectors_report_each_prval_and_returnvec(self):
        library = Library()
        library.install("rados_create_read_op", lambda: 41)
        library.install("rados_create_write_op", lambda: 42)
        seen = []

        def cmp2(operation, key, comparison, value, key_len, value_len, prval):
            seen.append((operation, key[:key_len], comparison, value[:value_len]))
            prval._obj.value = -125 if comparison == 3 else 0

        library.install("rados_read_op_omap_cmp2", cmp2)
        library.install("rados_write_op_omap_cmp2", cmp2)
        library.install("rados_read_op_operate", lambda operation, io, name, flags: -125)
        flags = {}
        library.install("rados_write_op_operate2", lambda operation, io, name, stamp, value: flags.setdefault("write", value) and 0)
        native = writes.Native.__new__(writes.Native)
        native.lib = library
        actions = [{"op": "cmp", "flags": 0, "key": b"\0k", "value": b"\x80", "comparison": 5},
                   {"op": "cmp", "flags": 2, "key": b"z", "value": b"", "comparison": 3}]
        result = native.assert_vector(1, b"object", actions)
        self.assertEqual(result, {"aggregate": -125, "prvals": [{"index": 0, "prval": 0}, {"index": 1, "prval": -125}]})
        self.assertEqual(seen[0], (41, b"\0k", 5, b"\x80"))
        self.assertIn(("rados_read_op_set_flags", (41, 2)), library.calls)
        aggregate, prvals = native.write_vector(1, b"object", actions + [{"op": "clear", "flags": 0}], {"seconds": 1, "nanos": 2})
        self.assertEqual((aggregate, [p["index"] for p in prvals]), (0, [0, 1]))
        self.assertEqual(flags["write"], writes.RETURNVEC)
        names = [name for name, _ in library.calls]
        self.assertEqual(names[-1], "rados_release_write_op")
        self.assertIn("rados_release_read_op", names)

    def test_range_removal_keeps_binary_endpoints(self):
        request = writes.validate_request({"seed": None, "mtime": {"seconds": 1, "nanos": 0}, "operations": [
            {"op": "rmrange", "flags": 2, "begin": encode(b"b\0"), "end": encode(b"c\xff")},
            {"op": "rmrange", "flags": 0, "begin": encode(b"z"), "end": encode(b"a")}]}, "compare")
        self.assertEqual(request["operations"][0], {"op": "rmrange", "flags": 2, "begin": b"b\0", "end": b"c\xff"})
        self.assertEqual(request["operations"][1]["begin"], b"z")  # reversed stays expressible natively
        library = Library()
        library.install("rados_create_write_op", lambda: 7)
        seen = []
        library.install("rados_write_op_omap_rm_range2", lambda op, b, bl, e, el: seen.append((op, b[:bl], e[:el])))
        library.install("rados_write_op_operate2", lambda op, io, name, stamp, flags: 0)
        native = writes.Native.__new__(writes.Native)
        native.lib = library
        self.assertEqual(native.write_vector(1, b"o", request["operations"], {"seconds": 1, "nanos": 0}), (0, []))
        self.assertEqual(seen, [(7, b"b\0", b"c\xff"), (7, b"z", b"a")])
        self.assertIn(("rados_write_op_set_flags", (7, 2)), library.calls)
        with self.assertRaises(ValueError):
            writes.validate_request({"seed": None, "mtime": {"seconds": 1, "nanos": 0}, "operations": [
                {"op": "rmrange", "begin": "", "end": "", "keys": []}]}, "compare")

    def test_call_admission_and_exec_output_lifetime(self):
        request = writes.validate_request({"seed": {}, "operations": [
            {"op": "call", "flags": 0, "class": "hello", "method": "say_hello", "input": encode(b"T\0\xff")}]}, "exec")
        self.assertEqual(request["operations"][0], {"op": "call", "flags": 0, "cls": b"hello", "method": b"say_hello", "input": b"T\0\xff"})
        self.assertEqual(writes.validate_request({"object": owned(b"main-"), "role": "readonly", "operations": [
            {"op": "call", "class": "hello", "method": "say_hello"}]}, "exec")["role"], "readonly")
        for raw in [{"seed": {}, "operations": [{"op": "call", "class": "", "method": "m"}]},
                    {"seed": {}, "operations": [{"op": "call", "class": "c" * 256, "method": "m"}]},
                    {"seed": {}, "operations": [{"op": "call", "class": "c\0", "method": "m"}]},
                    {"seed": {}, "role": "readonly", "operations": [{"op": "call", "class": "c", "method": "m"}]},
                    {"seed": {}, "object": owned(b"main-"), "operations": [{"op": "call", "class": "c", "method": "m"}]},
                    {"seed": {}, "operations": [{"op": "call", "class": "c", "method": "m"}] * 2},
                    {"seed": {}, "operations": [{"op": "set", "entries": []}]}]:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                writes.validate_request(raw, "exec")
        library = Library()
        library.install("rados_create_read_op", lambda: 5)
        payload = C.create_string_buffer(b"Hello\0!", 7)

        def exec_(op, cls, method, inbuf, inlen, out, outlen, prval):
            self.assertEqual((op, cls, method, inbuf[:inlen]), (5, b"hello", b"say_hello", b"T\0\xff"))
            out._obj.value, outlen._obj.value, prval._obj.value = C.addressof(payload), 7, 42

        library.install("rados_read_op_exec", exec_)
        flags = {}
        library.install("rados_read_op_operate", lambda op, io, name, value: flags.setdefault("read", value) and 42)
        native = writes.Native.__new__(writes.Native)
        native.lib = library
        result = native.exec_vector(1, b"o", request["operations"][0])
        self.assertEqual(result, {"aggregate": 42, "prvals": [{"index": 0, "prval": 42}], "output": encode(b"Hello\0!")})
        self.assertEqual(flags["read"], writes.RETURNVEC)
        names = [name for name, _ in library.calls]
        self.assertEqual(names[-2:], ["rados_buffer_free", "rados_release_read_op"])
        write = writes.validate_request({"seed": None, "mtime": {"seconds": 1, "nanos": 0}, "operations": [
            {"op": "call", "class": "hello", "method": "record_hello"}]}, "compare")
        library.install("rados_create_write_op", lambda: 6)
        library.install("rados_write_op_exec", lambda op, cls, method, inbuf, inlen, prval: setattr(prval._obj, "value", -17))
        library.install("rados_write_op_operate2", lambda op, io, name, stamp, value: -17)
        self.assertEqual(native.write_vector(1, b"o", write["operations"], {"seconds": 1, "nanos": 0}), (-17, [{"index": 0, "prval": -17}]))

    def test_allocation_hint_and_object_info_admission(self):
        request = writes.validate_request({"seed": None, "mtime": {"seconds": 1, "nanos": 0}, "object_info": True, "operations": [
            {"op": "allochint", "flags": 0, "expected_object_size": (1 << 64) - 1, "expected_write_size": 4096, "hint_flags": 2047}]}, "compare")
        self.assertTrue(request["object_info"])
        self.assertEqual(request["operations"][0]["expected_object_size"], (1 << 64) - 1)
        for raw in [{"seed": None, "mtime": {"seconds": 1, "nanos": 0}, "operations": [{"op": "allochint", "expected_object_size": -1}]},
                    {"seed": None, "mtime": {"seconds": 1, "nanos": 0}, "operations": [{"op": "allochint", "hint_flags": 1 << 32}]},
                    {"seed": None, "mtime": {"seconds": 1, "nanos": 0}, "object_info": 1, "operations": [{"op": "clear"}]}]:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                writes.validate_request(raw, "compare")
        library = Library()
        library.install("rados_create_write_op", lambda: 9)
        seen = []
        library.install("rados_write_op_set_alloc_hint2", lambda op, eos, ews, flags: seen.append((op, eos, ews, flags)))
        library.install("rados_write_op_operate2", lambda op, io, name, stamp, value: 0)
        native = writes.Native.__new__(writes.Native)
        native.lib = library
        self.assertEqual(native.write_vector(1, b"o", request["operations"], {"seconds": 1, "nanos": 0}), (0, []))
        self.assertEqual(seen, [(9, (1 << 64) - 1, 4096, 2047)])

    def test_ownership_is_limited_to_exact_fixture_uuid_names(self):
        receipt = {"schema_version": 1, "source_commit": writes.SOURCE_COMMIT, "pool": "pool", "pool_id": 7,
                   "fixture_object": "fixture", "namespace": "", "objects": [owned(b"main-"), owned(b"absent-")]}
        self.assertEqual(len(writes.check_ownership(receipt, METADATA)), 2)
        for edit in [
            lambda r: r.update(namespace="other"),
            lambda r: r.update(pool_id=8),
            lambda r: r.update(objects=[owned(b"absent-")]),
            lambda r: r.update(objects=[owned(b"main-"), owned(b"main-")]),
            lambda r: r.update(objects=[owned(b"main-"), owned(b"mirror-")]),
            lambda r: r.update(objects=[owned(b"main-")[:-1] + "g"]),
            lambda r: r.update(objects=[]),
        ]:
            changed = dict(receipt)
            edit(changed)
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                writes.check_ownership(changed, METADATA)

    def test_write_vector_preserves_lengths_flags_mtime_and_releases(self):
        library = Library()
        library.install("rados_create_write_op", lambda: 99)
        observed = {}

        def set2(operation, keys, values, key_lens, value_lens, count):
            observed["set"] = [(C.string_at(keys[i], key_lens[i]), C.string_at(values[i], value_lens[i])) for i in range(count)]

        def rm2(operation, keys, key_lens, count):
            observed["remove"] = [C.string_at(keys[i], key_lens[i]) for i in range(count)]

        def operate(operation, io, name, stamp, flags):
            observed["mtime"] = (stamp._obj.tv_sec, stamp._obj.tv_nsec)
            raise RuntimeError("submission failed after construction")

        library.install("rados_write_op_omap_set2", set2)
        library.install("rados_write_op_omap_rm_keys2", rm2)
        library.install("rados_write_op_operate2", operate)
        native = writes.Native.__new__(writes.Native)
        native.lib = library
        actions = [{"op": "clear", "flags": 2}, {"op": "set", "flags": 0, "entries": [(b"", b""), (b"n\0k", b"\0\xff")]},
                   {"op": "remove", "flags": 0, "keys": [b"\0", b"\xff"]}]
        with self.assertRaises(RuntimeError):
            native.write_vector(1, b"object", actions, {"seconds": 5, "nanos": 6})
        self.assertEqual(observed["set"], [(b"", b""), (b"n\0k", b"\0\xff")])
        self.assertEqual(observed["remove"], [b"\0", b"\xff"])
        self.assertEqual(observed["mtime"], (5, 6))
        names = [name for name, _ in library.calls]
        self.assertEqual(names.count("rados_write_op_omap_clear"), 1)
        self.assertIn(("rados_write_op_set_flags", (99, 2)), library.calls)
        self.assertEqual(names[-1], "rados_release_write_op")

    def test_header_cli_uses_fixed_identity_and_bounds_raw_bytes(self):
        cli = writes.HeaderCLI(Path("/root"), METADATA, "")

        def run(argv, **kwargs):
            # The oracle runs inside Linux; derive paths with the host's Path
            # so this argument test is independent of the checkout OS.
            self.assertEqual(argv[:10], ["rados", "--conf", str(Path("/root") / "ceph.conf"), "--keyring", str(Path("/root") / "keyring"),
                                         "--name", "client.test", "--pool", "pool", "getomapheader"])
            self.assertEqual(argv[10], "object")
            Path(argv[11]).write_bytes(payload)
            return subprocess.CompletedProcess(argv, code, b"", b"error text")

        for payload, code in [(b"", 0), (b"\0\xffH", 0)]:
            with patch.object(writes.subprocess, "run", run):
                self.assertEqual(cli.get(b"object"), payload)
        payload, code = b"x", 1
        with patch.object(writes.subprocess, "run", run), self.assertRaises(RuntimeError):
            cli.get(b"object")
        payload, code = b"x" * (writes.MAX_HEADER + 1), 0
        with patch.object(writes.subprocess, "run", run), self.assertRaises(ValueError):
            cli.get(b"object")
        self.assertEqual(writes.HeaderCLI(Path("/root"), METADATA, "ns").base[-2:], ["--namespace", "ns"])


if __name__ == "__main__":
    unittest.main()
