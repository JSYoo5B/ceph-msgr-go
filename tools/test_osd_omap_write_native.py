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
            self.assertEqual(argv[:10], ["rados", "--conf", "/root/ceph.conf", "--keyring", "/root/keyring",
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
