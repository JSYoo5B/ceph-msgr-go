#!/usr/bin/env python3
"""Admission/argument/lifetime tests; no mocked Ceph compatibility claim."""
import ctypes as C
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import osd_omap_write_native as writes
from test_osd_xattr_native import Library

encode = writes.xattr.encode
METADATA = {"pool": "pool", "pool_id": 7, "object": "fixture"}
MTIME = {"seconds": 1, "nanos": 2}


def owned(label):
    return (b"fixture-native-omapw-" + label + b"0123456789abcdef0123456789abcdef").decode()


def ownership_receipt(objects):
    return {"schema_version": 1, "source_commit": writes.SOURCE_COMMIT, "pool": "pool", "pool_id": 7,
            "fixture_object": "fixture", "namespace": "", "objects": objects}


class SnapLibrary(Library):
    """Scripted xattr/OMAP iterators for one compound snapshot read."""

    def __init__(self, xattrs, omap_pages, aggregate=0):
        super().__init__()
        self.keep, self.xattrs, self.omap = [], iter(xattrs), iter(omap_pages)
        self.install("rados_create_read_op", lambda: 77)
        self.install("rados_read_op_operate", lambda op, io, name, flags: aggregate)
        self.install("rados_get_last_version", lambda io: 9)
        self.install("rados_getxattrs_next", self.next_xattr)
        self.install("rados_omap_get_next2", self.next_omap)

    def pointer(self, data):
        buffer = C.create_string_buffer(data, len(data) + 1)
        self.keep.append(buffer)
        return C.addressof(buffer)

    def next_xattr(self, iterator, name, value, length):
        entry = next(self.xattrs, None)
        name._obj.value = value._obj.value = None
        length._obj.value = 0
        if entry is not None:
            name._obj.value, length._obj.value = self.pointer(entry[0]), len(entry[1])
            value._obj.value = self.pointer(entry[1]) if entry[1] else None
        return 0

    def next_omap(self, iterator, key, value, key_length, value_length):
        entry = next(self.omap, None)
        key._obj.value = value._obj.value = None
        key_length._obj.value = value_length._obj.value = 0
        if entry is not None:
            key._obj.value, key_length._obj.value = self.pointer(entry[0]), len(entry[0])
            value._obj.value, value_length._obj.value = (self.pointer(entry[1]) if entry[1] else None), len(entry[1])
        return 0


class FakeSnapNative:
    """Records the oracle's snapshot ordering without librados."""

    def __init__(self, snapc_code=0, fail=None):
        self.calls, self.snapc_code, self.fail = [], snapc_code, fail
        self.lib = Library()
        self.lib.install("rados_get_last_version", lambda io: 11)
        self.lib.install("rados_ioctx_selfmanaged_snap_rollback", lambda io, name, snap: self.calls.append(("rollback", snap)) or 0)

    def full_state(self, io, name, keys, cli):
        self.calls.append(("state", name))
        return {"code": 0}

    def set_write_context(self, io, snapc):
        self.calls.append(("snapc", snapc["seq"], tuple(snapc["snaps"])))
        return self.snapc_code

    def reset_write_context(self, io):
        self.calls.append(("reset",))

    def write_vector(self, io, name, actions, mtime, flags):
        self.calls.append(("write", flags))
        if self.fail:
            raise self.fail
        return -85, []

    def object_info(self, io, name):
        return {"user_version": 3}


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


class SnapshotOracleTests(unittest.TestCase):
    def test_snapshot_modes_admit_exact_fields_and_keep_snapc_order(self):
        self.assertEqual(writes.validate_request({}, "snapcreate")["count"], 1)
        self.assertEqual(writes.validate_request({"count": 4}, "snapcreate")["count"], 4)
        request = writes.validate_request({
            "object": owned(b"main-"), "snapc": {"seq": 9, "snaps": [3, 9]}, "ordersnap": True, "mtime": MTIME,
            "object_info": True, "inventory_keys": [encode(b"k")],
            "operations": [{"op": "writefull", "data": encode(b"\0v2")}, {"op": "write", "offset": (1 << 64) - 1, "data": ""},
                           {"op": "setxattr", "name": encode(b"user.a"), "value": encode(b"\xff\0")},
                           {"op": "rmxattr", "name": encode(b"gone"), "flags": 2}, {"op": "delete"},
                           {"op": "set", "entries": [{"key": "", "value": encode(b"v")}]}]}, "snapwrite")
        # Ascending snaps stay expressible: librados set_write_ctx refuses them.
        self.assertEqual(request["snapc"], {"seq": 9, "snaps": [3, 9]})
        self.assertTrue(request["ordersnap"])
        self.assertEqual(request["operations"][:5], [
            {"op": "writefull", "flags": 0, "data": b"\0v2"}, {"op": "write", "flags": 0, "data": b"", "offset": (1 << 64) - 1},
            {"op": "setxattr", "flags": 0, "name": b"user.a", "value": b"\xff\0"}, {"op": "rmxattr", "flags": 2, "name": b"gone"},
            {"op": "delete", "flags": 0}])
        self.assertEqual(writes.validate_request({"object": owned(b"main-"), "snap": writes.SNAP_HEAD}, "snapread")["snap"], writes.SNAP_HEAD)
        self.assertEqual(writes.validate_request({"object": owned(b"main-"), "snap": 0, "snapc": {"seq": 0, "snaps": []}},
                                                 "snaprollback")["snap"], 0)
        self.assertEqual(writes.validate_request({"raw": encode(b"\x02\x01")}, "listsnaps")["raw"], b"\x02\x01")
        self.assertNotIn("raw", writes.validate_request({"object": owned(b"main-")}, "listsnaps"))
        write = {"object": owned(b"main-"), "snapc": {"seq": 2, "snaps": [2]}, "mtime": MTIME, "operations": [{"op": "delete"}]}
        for raw, mode in [
            ({"count": 0}, "snapcreate"), ({"count": 5}, "snapcreate"), ({"count": True}, "snapcreate"),
            ({"object": owned(b"main-")}, "snapcreate"),
            (dict(write, snapc=None), "snapwrite"), (dict(write, snapc={"seq": 2}), "snapwrite"),
            (dict(write, snapc={"seq": 2, "snaps": [], "extra": 1}), "snapwrite"),
            (dict(write, snapc={"seq": -1, "snaps": []}), "snapwrite"), (dict(write, snapc={"seq": 1 << 64, "snaps": []}), "snapwrite"),
            (dict(write, snapc={"seq": True, "snaps": []}), "snapwrite"),
            (dict(write, snapc={"seq": 2, "snaps": [1] * (writes.MAX_SNAPC + 1)}), "snapwrite"),
            (dict(write, ordersnap=1), "snapwrite"), (dict(write, mtime=None), "snapwrite"), (dict(write, snap=2), "snapwrite"),
            (dict(write, seed={}), "snapwrite"), (dict(write, operations=[]), "snapwrite"),
            (dict(write, operations=[{"op": "header"}]), "snapwrite"), (dict(write, operations=[{"op": "delete", "data": ""}]), "snapwrite"),
            (dict(write, operations=[{"op": "setxattr", "name": ""}]), "snapwrite"),
            (dict(write, operations=[{"op": "setxattr", "name": encode(b"a\0b")}]), "snapwrite"),
            (dict(write, operations=[{"op": "rmxattr"}]), "snapwrite"),
            (dict(write, operations=[{"op": "write", "offset": -1, "data": ""}]), "snapwrite"),
            (dict(write, operations=[{"op": "writefull", "data": encode(b"x" * (writes.MAX_BINARY + 1))}]), "snapwrite"),
            ({"object": owned(b"main-"), "snap": 0}, "snapread"), ({"object": owned(b"main-"), "snap": 1 << 64}, "snapread"),
            ({"snap": 2}, "snapread"), ({"object": owned(b"main-"), "snap": 2, "snapc": {"seq": 2, "snaps": [2]}}, "snapread"),
            ({"object": owned(b"main-"), "snap": 2}, "snaprollback"),
            ({"object": owned(b"main-"), "snap": 2, "snapc": {"seq": 2, "snaps": [2]}, "operations": [{"op": "delete"}]}, "snaprollback"),
            ({}, "listsnaps"), ({"raw": "!"}, "listsnaps"), ({"object": ""}, "listsnaps"),
            ({"raw": encode(b"x" * (writes.MAX_BINARY + 1))}, "listsnaps"),
            ({"seed": None, "mtime": MTIME, "operations": [{"op": "writefull", "data": ""}]}, "compare"),
        ]:
            with self.subTest(raw=raw, mode=mode), self.assertRaises((ValueError, KeyError, TypeError)):
                writes.validate_request(raw, mode)

    def test_snapshot_write_builders_keep_lengths_names_offsets_and_flags(self):
        library = Library()
        library.install("rados_create_write_op", lambda: 13)
        seen = []
        library.install("rados_write_op_write_full", lambda op, buf, length: seen.append(("writefull", op, buf.raw[:length])))
        library.install("rados_write_op_write", lambda op, buf, length, offset: seen.append(("write", op, buf.raw[:length], offset)))
        library.install("rados_write_op_setxattr", lambda op, name, buf, length: seen.append(("setxattr", op, name, buf.raw[:length])))
        flags = {}
        library.install("rados_write_op_operate2", lambda op, io, name, stamp, value: flags.setdefault("value", value) and -85)
        native = writes.Native.__new__(writes.Native)
        native.lib = library
        actions = [{"op": "writefull", "flags": 0, "data": b"\0v2\xff"}, {"op": "write", "flags": 2, "data": b"", "offset": 7},
                   {"op": "setxattr", "flags": 0, "name": b"user.a", "value": b"\0"}, {"op": "rmxattr", "flags": 0, "name": b"gone"},
                   {"op": "delete", "flags": 0}]
        result = native.write_vector(1, b"o", actions, MTIME, writes.RETURNVEC | writes.ORDERSNAP)
        self.assertEqual(result, (-85, []))
        self.assertEqual(flags["value"], 1536)
        self.assertEqual(seen, [("writefull", 13, b"\0v2\xff"), ("write", 13, b"", 7), ("setxattr", 13, b"user.a", b"\0")])
        self.assertIn(("rados_write_op_rmxattr", (13, b"gone")), library.calls)
        self.assertIn(("rados_write_op_remove", (13,)), library.calls)
        self.assertEqual([c for c in library.calls if c[0] == "rados_write_op_set_flags"], [("rados_write_op_set_flags", (13, 2))])
        self.assertEqual(library.calls[-1], ("rados_release_write_op", (13,)))

    def test_set_write_context_passes_caller_order_and_returns_exact_code(self):
        library = Library()
        seen = []

        def set_ctx(io, seq, snaps, count):
            seen.append((seq, [snaps[i] for i in range(count)], count))
            return -22 if count and snaps[0] < snaps[count - 1] else 0

        library.install("rados_ioctx_selfmanaged_snap_set_write_ctx", set_ctx)
        native = writes.Native.__new__(writes.Native)
        native.lib = library
        self.assertEqual(native.set_write_context(1, {"seq": 9, "snaps": [3, 9]}), -22)
        self.assertEqual(native.set_write_context(1, {"seq": 9, "snaps": [9, 3]}), 0)
        native.reset_write_context(1)
        self.assertEqual(seen, [(9, [3, 9], 2), (9, [9, 3], 2), (0, [], 0)])

    def test_snapshot_read_reports_per_op_results_and_restores_head(self):
        library = SnapLibrary([(b"user.a", b"\0v1")], [(b"k", b"v1"), None, (b"", b""), (b"k", b"v1"), None])
        seen = {}

        def stat2(op, size, mtime, prval):
            size._obj.value, mtime._obj.tv_sec, mtime._obj.tv_nsec, prval._obj.value = 3, 1700000000, 123456789, 0

        def read(op, offset, length, buffer, count, prval):
            seen["read"] = (offset, length)
            C.memmove(buffer, b"\0v1", 3)
            count._obj.value, prval._obj.value = 3, 0

        def getxattrs(op, iterator, prval):
            iterator._obj.value, prval._obj.value = 51, 0

        def vals(op, start, prefix, maximum, iterator, more, prval):
            iterator._obj.value, more._obj.value, prval._obj.value = 52, 0, 0

        def bykeys(op, keys, count, lengths, iterator, prval):
            seen["keys"] = [C.string_at(keys[i], lengths[i]) for i in range(count)]
            iterator._obj.value, prval._obj.value = 53, 0

        for name, function in (("rados_read_op_stat2", stat2), ("rados_read_op_read", read), ("rados_read_op_getxattrs", getxattrs),
                               ("rados_read_op_omap_get_vals2", vals), ("rados_read_op_omap_get_vals_by_keys2", bykeys)):
            library.install(name, function)
        native = writes.Native.__new__(writes.Native)
        native.lib = library
        result = native.snap_read(1, b"o", 2, [b"k"], False)
        self.assertEqual(seen, {"read": (0, writes.MAX_BINARY + 1), "keys": [b"", b"k"]})
        self.assertEqual(result["aggregate"], 0)
        self.assertEqual(result["version"], 9)
        ops = result["operations"]
        self.assertEqual(ops[0], {"index": 0, "op": "stat", "prval": 0, "size": 3, "seconds": 1700000000, "nanos": 123456789})
        self.assertEqual(ops[1], {"index": 1, "op": "read", "prval": 0, "data": encode(b"\0v1")})
        self.assertEqual(ops[2]["entries"], [{"name": encode(b"user.a"), "value": encode(b"\0v1")}])
        self.assertEqual((ops[3]["op"], ops[3]["opcode"], ops[3]["more"], ops[3]["entries"]),
                         ("omap_vals", 0x1212, False, [{"key": encode(b"k"), "value": encode(b"v1")}]))
        self.assertEqual((ops[4]["index"], ops[4]["op"], ops[4]["more"], len(ops[4]["entries"])), (4, "omap_bykeys", None, 2))
        sets = [args for name, args in library.calls if name == "rados_ioctx_snap_set_read"]
        self.assertEqual(sets, [(1, 2), (1, writes.SNAP_HEAD)])
        names = [name for name, _ in library.calls]
        self.assertEqual(names.count("rados_omap_get_end"), 2)
        self.assertIn("rados_getxattrs_end", names)
        self.assertLess(names.index("rados_release_read_op"), len(names) - 1)

    def test_snapshot_read_failure_keeps_codes_and_restores_head_on_errors(self):
        library = SnapLibrary([], [], aggregate=-2)

        def stat2(op, size, mtime, prval):
            prval._obj.value = -2

        library.install("rados_read_op_stat2", stat2)
        library.install("rados_getxattr", lambda io, name, attr, buffer, length: -2)
        native = writes.Native.__new__(writes.Native)
        native.lib = library
        result = native.snap_read(1, b"o", writes.SNAP_HEAD, [], True)
        self.assertEqual((result["aggregate"], result["object_info_code"], result["object_info"]), (-2, -2, None))
        self.assertEqual([op.get("entries", "absent") for op in result["operations"][:3]], ["absent"] * 3)
        self.assertEqual(result["operations"][0], {"index": 0, "op": "stat", "prval": -2})
        self.assertIsNone(result["operations"][3]["entries"])
        library = SnapLibrary([], [])
        library.install("rados_read_op_operate", lambda op, io, name, flags: (_ for _ in ()).throw(RuntimeError("transport")))
        native.lib = library
        with self.assertRaises(RuntimeError):
            native.snap_read(1, b"o", 5, [], False)
        self.assertEqual(library.calls[-1], ("rados_ioctx_snap_set_read", (1, writes.SNAP_HEAD)))

    def test_snapwrite_and_rollback_run_only_admitted_contexts_and_always_reset(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            (out / (writes.OWNED + ".json")).write_text(json.dumps(ownership_receipt([owned(b"main-")])))
            base = {"object": owned(b"main-").encode(), "inventory_keys": [], "object_info": True}
            write = dict(base, snapc={"seq": 0, "snaps": []}, ordersnap=True, operations=[{"op": "delete", "flags": 0}], mtime=MTIME)
            native = FakeSnapNative()
            result = writes.execute_snapshot(native, 1, METADATA, write, "snapwrite", out, None, {})
            self.assertEqual((result["aggregate"], result["last_version"], result["operate_flags"], result["snapc"]["code"]),
                             (-85, 11, writes.RETURNVEC | writes.ORDERSNAP, 0))
            self.assertEqual([c[0] for c in native.calls], ["state", "snapc", "write", "reset", "state"])
            native = FakeSnapNative(snapc_code=-22)
            result = writes.execute_snapshot(native, 1, METADATA, write, "snapwrite", out, None, {})
            self.assertEqual((result["aggregate"], result["prvals"], result["snapc"]["code"]), (None, [], -22))
            self.assertNotIn("write", [c[0] for c in native.calls])
            native = FakeSnapNative(fail=RuntimeError("submission"))
            with self.assertRaises(RuntimeError):
                writes.execute_snapshot(native, 1, METADATA, write, "snapwrite", out, None, {})
            self.assertEqual(native.calls[-1], ("reset",))
            rollback = dict(base, snapc={"seq": 4, "snaps": [4, 2]}, snap=2)
            native = FakeSnapNative()
            result = writes.execute_snapshot(native, 1, METADATA, rollback, "snaprollback", out, None, {})
            self.assertEqual((result["rollback"], result["object_info"]), ({"snap": 2, "code": 0}, {"user_version": 3}))
            self.assertEqual(native.calls[1:4], [("snapc", 4, (4, 2)), ("rollback", 2), ("reset",)])
            with self.assertRaises(ValueError):
                writes.execute_snapshot(native, 1, METADATA, dict(rollback, object=owned(b"absent-").encode()), "snaprollback", out, None, {})

    def test_snap_allocation_receipts_accumulate_and_repair_unrecorded_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            ids, removed = iter([2, 3, 4]), []
            library = Library()

            def create(io, snap):
                snap._obj.value = next(ids)
                return 0

            library.install("rados_ioctx_selfmanaged_snap_create", create)
            library.install("rados_ioctx_selfmanaged_snap_remove", lambda io, snap: removed.append(snap) or 0)
            native = writes.Native.__new__(writes.Native)
            native.lib = library
            result = writes.execute_snapshot(native, 1, METADATA, {"count": 2}, "snapcreate", out, None, {})
            self.assertEqual(result, {"snaps": [{"code": 0, "snap": 2}, {"code": 0, "snap": 3}], "owned_snaps": [2, 3]})
            self.assertEqual(writes.owned_snaps(out, METADATA), [2, 3])
            with patch.object(writes, "publish", side_effect=OSError("disk")), self.assertRaises(OSError):
                writes.execute_snapshot(native, 1, METADATA, {"count": 1}, "snapcreate", out, None, {})
            self.assertEqual((removed, writes.owned_snaps(out, METADATA)), ([4], [2, 3]))
            library.install("rados_ioctx_selfmanaged_snap_create", lambda io, snap: -1)
            result = writes.execute_snapshot(native, 1, METADATA, {"count": 3}, "snapcreate", out, None, {})
            self.assertEqual(result["snaps"], [{"code": -1, "snap": None}])
            for receipt in [dict(writes.snap_receipt(METADATA, [2]), pool_id=8), writes.snap_receipt(METADATA, [2, 2]),
                            writes.snap_receipt(METADATA, [0]), writes.snap_receipt(METADATA, [writes.MAX_SNAP_ID + 1]),
                            writes.snap_receipt(METADATA, list(range(1, writes.MAX_OWNED_SNAPS + 2))),
                            dict(writes.snap_receipt(METADATA, [2]), extra=1)]:
                (out / (writes.SNAPS + ".json")).write_text(json.dumps(receipt))
                with self.subTest(receipt=receipt), self.assertRaises(ValueError):
                    writes.owned_snaps(out, METADATA)

    def test_cleanup_removes_owned_heads_before_snaps_and_clears_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            (out / (writes.OWNED + ".json")).write_text(json.dumps(ownership_receipt([owned(b"main-"), owned(b"absent-")])))
            writes.publish_snaps(out, METADATA, [2, 3])
            order = []
            library = Library()
            library.install("rados_remove", lambda io, name: order.append(("remove", name)) or -2)
            library.install("rados_ioctx_selfmanaged_snap_remove", lambda io, snap: order.append(("snap", snap)) or 0)
            native = writes.Native.__new__(writes.Native)
            native.lib = library
            result = writes.execute(native, 1, METADATA, {}, "cleanup", out, None)
            self.assertEqual([kind for kind, _ in order], ["remove", "remove", "snap", "snap"])
            self.assertTrue(result["cleanup"]["objects_removed"] and result["cleanup"]["snaps_removed"])
            self.assertFalse((out / (writes.SNAPS + ".json")).exists())
            writes.publish_snaps(out, METADATA, [5, 6])
            library.install("rados_ioctx_selfmanaged_snap_remove", lambda io, snap: -13 if snap == 6 else 0)
            with self.assertRaises(RuntimeError):
                writes.execute(native, 1, METADATA, {}, "cleanup", out, None)
            self.assertEqual(writes.owned_snaps(out, METADATA), [6])
            library.install("rados_remove", lambda io, name: -5)
            with self.assertRaises(RuntimeError):
                writes.execute(native, 1, METADATA, {}, "cleanup", out, None)
            self.assertEqual(writes.owned_snaps(out, METADATA), [6])  # snaps stay owned while heads remain

    def test_listsnaps_cli_and_dencoder_keep_exit_errno_and_bytes(self):
        cli = writes.HeaderCLI(Path("/root"), METADATA, "")
        outcomes = []

        def run(argv, **kwargs):
            outcomes.append(argv)
            if argv[0] == "ceph-dencoder":
                self.assertEqual(argv[:4] + argv[5:], ["ceph-dencoder", "type", "obj_list_snap_response_t", "import", "decode", "dump_json"])
                self.assertEqual(Path(argv[4]).read_bytes(), b"\x02\x01\0")
                return subprocess.CompletedProcess(argv, 0, b'{"clones":[]}', b"")
            self.assertEqual(argv[7:], ["--pool", "pool", "listsnaps", "object", "--format", "json"])
            return subprocess.CompletedProcess(argv, code, stdout, b"error listing snap shots pool/object: (2) No such file or directory\n")

        code, stdout = 0, b'{"name":"object","seq":2,"clones":[{"id":"head","snapshots":[],"size":3}]}'
        with patch.object(writes.subprocess, "run", run):
            self.assertEqual(cli.listsnaps(b"object"), {"exit": 0, "code": 0, "stderr": "", "json": json.loads(stdout)})
            code, stdout = 1, b""
            failed = cli.listsnaps(b"object")
            self.assertEqual((failed["exit"], failed["code"], failed["json"]), (1, -2, None))
            self.assertEqual(writes.decode_snap_list(b"\x02\x01\0")["json"], {"clones": []})
            code, stdout = 0, b"x" * (writes.MAX_SNAP_JSON + 1)
            with self.assertRaises(ValueError):
                cli.listsnaps(b"object")
        # The oracle runs inside Linux; host Path keeps this test OS-neutral.
        self.assertEqual(outcomes[0][:7], ["rados", "--conf", str(Path("/root") / "ceph.conf"), "--keyring",
                                           str(Path("/root") / "keyring"), "--name", "client.test"])


if __name__ == "__main__":
    unittest.main()
