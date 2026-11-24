import base64
import pathlib
import subprocess
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_nativeop_oracle as nativeop


def b(data):
    return base64.b64encode(data).decode()


class FakeLib:
    def __init__(self):
        self.destroyed = []

    def rados_ioctx_destroy(self, io):
        self.destroyed.append(io)


class FakeRados:
    def __init__(self):
        self.lib, self.opened, self.root = FakeLib(), [], pathlib.Path("/control")

    def ioctx(self, cluster, pool, namespace=""):
        self.opened.append((cluster, pool, namespace))
        return "io-%d" % len(self.opened)


class FakeNative:
    """Counts snapshots so before and after are told apart; no daemon is involved."""

    def __init__(self, aggregate=-95, error=None):
        self.aggregate, self.error, self.calls, self.snapshots = aggregate, error, [], 0

    def snapshot(self, io, name, names):
        self.snapshots += 1
        return {"code": 0, "version": 3, "snapshot": self.snapshots}

    def write(self, io, name, actions):
        self.calls.append(("write", io, name, actions))
        if self.error is not None:
            raise self.error
        return self.aggregate, [{"index": 1, "prval": -95}]

    def flagged_read(self, io, name, offset, length, flags):
        self.calls.append(("read", io, name, offset, length, flags))
        return {"aggregate": 0, "prval": 0, "data": b(b"x" * length)}


class Recorder:
    """A librados stand-in that records every builder call of one write op."""

    def __init__(self, aggregate=-95):
        self.aggregate, self.calls = aggregate, []

    def __getattr__(self, name):
        if not name.startswith("rados_"):
            raise AttributeError(name)

        def call(*arguments):
            self.calls.append(name[len("rados_"):])
            if name == "rados_create_write_op" or name == "rados_create_read_op":
                return 1
            if name in ("rados_write_op_operate2", "rados_read_op_operate"):
                self.flags = arguments[-1]
                return self.aggregate
            return None
        return call


EVERY = [
    {"op": "writefull", "data": b(b"full")},
    {"op": "write", "data": b(b"part"), "offset": 4096},
    {"op": "append", "data": b(b"tail")},
    {"op": "zero", "offset": 8, "length": 16},
    {"op": "truncate", "offset": 100},
    {"op": "setxattr", "name": b(b"a"), "value": b(b"alpha")},
    {"op": "create", "exclusive": True},
    {"op": "delete"},
]
OMAP = [
    {"op": "set", "entries": [{"key": b(b"k"), "value": b(b"v")}]},
    {"op": "remove", "keys": [b(b"k")]},
    {"op": "clear"},
    {"op": "rmrange", "begin": b(b"a"), "end": b(b"z")},
    {"op": "call", "class": "hello", "method": "turn_it_to_11", "input": b(b"")},
]


class Operations(unittest.TestCase):
    def test_every_operation_decodes_its_own_fields(self):
        decoded = nativeop.operations(EVERY) + nativeop.operations(OMAP)
        self.assertEqual([d["op"] for d in decoded], [a["op"] for a in EVERY + OMAP])
        self.assertEqual(decoded[1], {"op": "write", "data": b"part", "offset": 4096})
        self.assertEqual(decoded[3], {"op": "zero", "offset": 8, "length": 16})
        self.assertEqual(decoded[5], {"op": "setxattr", "name": b"a", "value": b"alpha"})
        self.assertEqual(decoded[8]["entries"], [(b"k", b"v")])
        self.assertEqual(decoded[11], {"op": "rmrange", "begin": b"a", "end": b"z"})
        self.assertEqual(decoded[12], {"op": "call", "cls": b"hello", "method": b"turn_it_to_11", "input": b""})

    def test_refuses_other_fields_values_and_counts(self):
        bad = [
            [], [{"op": "delete"}] * 9, "writefull", [{"op": "header", "value": b(b"h")}], [{"op": "zero", "offset": 0}],
            [{"op": "zero", "offset": 0, "length": -1}], [{"op": "truncate", "offset": 1 << 31}],
            [{"op": "write", "data": b(b"x"), "offset": True}], [{"op": "writefull", "data": b(b"x"), "flags": 2}],
            [{"op": "create", "exclusive": 1}], [{"op": "delete", "flags": 0}], [{"op": "append", "data": b(b"x" * ((64 << 10) + 1))}],
            [{"op": "call", "class": "", "method": "m", "input": b(b"")}],
        ]
        for raw in bad:
            with self.assertRaises(ValueError, msg=repr(raw)[:80]):
                nativeop.operations(raw)


class Builder(unittest.TestCase):
    def native(self, lib):
        native = nativeop.Native.__new__(nativeop.Native)
        native.lib = lib
        return native

    def test_one_write_op_holds_every_operation_in_order(self):
        lib = Recorder()
        aggregate, calls = self.native(lib).write("io", b"obj", nativeop.operations(EVERY) + nativeop.operations(OMAP))
        self.assertEqual(aggregate, -95)
        self.assertEqual(calls, [{"index": 12, "prval": 999}])
        self.assertEqual(lib.calls, ["create_write_op", "write_op_write_full", "write_op_write", "write_op_append", "write_op_zero",
                                     "write_op_truncate", "write_op_setxattr", "write_op_create", "write_op_remove", "write_op_omap_set2",
                                     "write_op_omap_rm_keys2", "write_op_omap_clear", "write_op_omap_rm_range2", "write_op_exec",
                                     "write_op_operate2", "release_write_op"])
        self.assertEqual(lib.flags, 0)

    def test_a_flagged_read_is_one_read_op_with_its_flag(self):
        lib = Recorder(aggregate=0)
        result = self.native(lib).flagged_read("io", b"obj", 0, 16, 1)
        self.assertEqual((result["aggregate"], lib.flags), (0, 1))
        self.assertEqual(lib.calls, ["create_read_op", "read_op_read", "read_op_operate", "release_read_op"])


class Tool(unittest.TestCase):
    def run_tool(self, tool, value, returncode, stderr):
        seen = []

        def run(command, **keywords):
            seen.append((command, keywords))
            return subprocess.CompletedProcess(command, returncode, b"", stderr)
        return nativeop.run_tool(pathlib.Path("/control"), "ceph-msgr-owned-pool-1", tool, b"obj", value, run), seen

    def test_a_refusal_keeps_its_exit_status_and_errno(self):
        result, seen = self.run_tool("setomapheader", "hdr", 1, b"error setting omap value pool/obj: (95) Operation not supported\n")
        self.assertEqual((result["exit"], result["code"], result["tool"]), (1, -95, "setomapheader"))
        self.assertEqual(seen[0][0][-3:], ["setomapheader", "obj", "hdr"])
        self.assertEqual(seen[0][1]["timeout"], 30)

    def test_success_and_an_unreadable_failure(self):
        result, seen = self.run_tool("mapext", "", 0, b"")
        self.assertEqual((result["exit"], result["code"]), (0, 0))
        self.assertEqual(seen[0][0][-2:], ["mapext", "obj"])
        result, _ = self.run_tool("mapext", "", 1, b"usage")
        self.assertEqual((result["exit"], result["code"]), (1, None))

    def test_oversized_output_is_refused(self):
        with self.assertRaises(ValueError):
            self.run_tool("mapext", "", 1, b"x" * ((16 << 10) + 1))


class Mode(unittest.TestCase):
    def test_write_brackets_one_native_write_op(self):
        rados, native = FakeRados(), FakeNative()
        result = nativeop.mode_nativeop(rados, "cluster", {"action": "write", "slot": 1, "object": "obj", "operations": OMAP[:1]}, None,
                                        native=native)["native_op"]
        self.assertEqual((result["aggregate"], result["calls"], result["operate_calls"]), (-95, [{"index": 1, "prval": -95}], 1))
        self.assertEqual((result["before"]["snapshot"], result["after"]["snapshot"]), (1, 2))
        self.assertEqual(native.calls, [("write", "io-1", b"obj", [{"op": "set", "entries": [(b"k", b"v")]}])])
        self.assertEqual(rados.opened, [("cluster", "ceph-msgr-owned-pool-1", "")])
        self.assertEqual(rados.lib.destroyed, ["io-1"])

    def test_read_passes_its_flag(self):
        for flag, bits in (("balance", 1), ("localize", 2)):
            rados, native = FakeRados(), FakeNative()
            result = nativeop.mode_nativeop(rados, "cluster", {"action": "read", "slot": 2, "object": "obj", "offset": 4, "length": 8, "flag": flag},
                                            None, native=native)["native_op"]
            self.assertEqual(native.calls, [("read", "io-1", b"obj", 4, 8, bits)])
            self.assertEqual((result["flag"], result["read"]["aggregate"]), (flag, 0))

    def test_tool_runs_the_cli_once(self):
        seen = []

        def run(command, **keywords):
            seen.append(command)
            return subprocess.CompletedProcess(command, 1, b"", b"mapext error on p/obj: (95) Operation not supported\n")
        result = nativeop.mode_nativeop(FakeRados(), "cluster", {"action": "tool", "slot": 1, "object": "obj", "tool": "mapext", "value": ""}, None,
                                        native=FakeNative(), run=run)["native_op"]
        self.assertEqual((len(seen), result["tool"]["code"]), (1, -95))

    def test_releases_the_context_when_the_operation_fails(self):
        rados = FakeRados()
        with self.assertRaises(RuntimeError):
            nativeop.mode_nativeop(rados, "cluster", {"action": "write", "slot": 1, "object": "obj", "operations": OMAP[:1]}, None,
                                   native=FakeNative(error=RuntimeError("native write-op allocation failed")))
        self.assertEqual(rados.lib.destroyed, ["io-1"])

    def test_refuses_other_requests(self):
        for request in (
                {"action": "exec", "slot": 1, "object": "obj"},
                {"action": "write", "slot": 1, "object": "obj"},
                {"action": "write", "slot": 3, "object": "obj", "operations": OMAP[:1]},
                {"action": "write", "slot": 1, "object": "obj", "operations": OMAP[:1], "flags": 0},
                {"action": "read", "slot": 1, "object": "obj", "offset": 0, "length": 8, "flag": "replica"},
                {"action": "read", "slot": 1, "object": "obj", "offset": 0, "length": 0, "flag": "balance"},
                {"action": "tool", "slot": 1, "object": "obj", "tool": "rm", "value": ""},
                {"action": "tool", "slot": 1, "object": "obj", "tool": "setomapheader", "value": ""},
                {"action": "tool", "slot": 1, "object": "obj", "tool": "mapext", "value": "x"},
                {"action": "tool", "slot": 1, "object": "obj", "tool": "setomapheader", "value": "--force"}):
            rados = FakeRados()
            with self.assertRaises(ValueError, msg=repr(request)):
                nativeop.mode_nativeop(rados, "cluster", request, None, native=FakeNative())
            self.assertEqual(rados.opened, [])

    def test_is_registered_once(self):
        self.assertIs(osd_pool_oracle.HANDLERS["nativeop"], nativeop.mode_nativeop)


if __name__ == "__main__":
    unittest.main()
