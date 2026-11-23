import base64
import ctypes as C
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_readcompound_oracle as readcompound


def b(data):
    return base64.b64encode(data).decode()


class FakeLib:
    def __init__(self):
        self.destroyed = []

    def rados_ioctx_destroy(self, io):
        self.destroyed.append(io)


class FakeRados:
    def __init__(self):
        self.lib, self.opened = FakeLib(), []

    def ioctx(self, cluster, pool, namespace=""):
        self.opened.append((cluster, pool, namespace))
        return "io-%d" % len(self.opened)


class FakeNative:
    def __init__(self, code=0):
        self.code, self.calls = code, []

    def seed(self, io, name, data, attributes, entries):
        self.calls.append(("seed", io, name, data, attributes, entries))
        return self.code

    def snapshot(self, io, name, names):
        return {"code": 0, "version": 3}

    def execute(self, io, name, actions):
        self.calls.append(("execute", io, name, actions))
        return {"aggregate": -125}


EVERY = [
    {"op": "read", "offset": 0, "length": 16},
    {"op": "stat"},
    {"op": "assert", "version": 7},
    {"op": "cmpext", "offset": 4, "data": b(b"abcd")},
    {"op": "getxattrs"},
    {"op": "cmpxattr", "name": b(b"a"), "comparison": "eq", "value": b(b"alpha")},
    {"op": "keys", "start_after": b(b""), "max_return": 8},
    {"op": "vals", "start_after": b(b"k1"), "prefix": b(b"k"), "max_return": 8},
]
MORE = [
    {"op": "bykeys", "keys": [b(b"k2"), b(b"")]},
    {"op": "omapcmp", "key": b(b"k1"), "comparison": 1, "value": b(b"v1")},
    {"op": "exec", "class": "hello", "method": "say_hello", "input": b(b"x")},
    {"op": "checksum", "type": 2, "offset": 0, "length": 16, "chunk_size": 8, "seed": 1},
]


class Validate(unittest.TestCase):
    def test_every_action_decodes_its_own_fields(self):
        decoded = [readcompound.validate(a) for a in EVERY + MORE]
        self.assertEqual([d["op"] for d in decoded], [a["op"] for a in EVERY + MORE])
        self.assertEqual(decoded[3]["data"], b"abcd")
        self.assertEqual(decoded[5], {"op": "cmpxattr", "name": b"a", "comparison": 1, "value": b"alpha"})
        self.assertEqual(decoded[7]["prefix"], b"k")
        self.assertEqual(decoded[8]["keys"], [b"k2", b""])
        self.assertEqual(decoded[10], {"op": "exec", "cls": b"hello", "method": b"say_hello", "input": b"x"})
        self.assertEqual(decoded[11]["chunk_size"], 8)

    def test_refuses_other_fields_and_values(self):
        bad = [
            {"op": "sparse_read", "offset": 0, "length": 8},
            {"op": "getxattr", "name": b(b"a")},
            {"op": "read", "offset": 0},
            {"op": "read", "offset": 0, "length": 8, "flags": 2},
            {"op": "stat", "offset": 0},
            {"op": "read", "offset": -1, "length": 8},
            {"op": "read", "offset": 0, "length": (64 << 10) + 1},
            {"op": "assert", "version": 1 << 64},
            {"op": "cmpxattr", "name": b(b"a"), "comparison": "approx", "value": b(b"")},
            {"op": "omapcmp", "key": b(b"k"), "comparison": 2, "value": b(b"")},
            {"op": "bykeys", "keys": []},
            {"op": "exec", "class": "", "method": "m", "input": b(b"")},
            {"op": "checksum", "type": 9, "offset": 0, "length": 8, "chunk_size": 0, "seed": 0},
            {"op": "checksum", "type": 2, "offset": 0, "length": 1 << 12, "chunk_size": 8, "seed": 0},
            {"op": "checksum", "type": 2, "offset": 0, "length": 0, "chunk_size": 0, "seed": 0},
            {"op": "keys", "start_after": "not base64!", "max_return": 1},
            {"op": "vals", "start_after": b(b""), "prefix": b(b""), "max_return": 33},
        ]
        for action in bad:
            with self.assertRaises(ValueError, msg=action):
                readcompound.validate(action)


class Mode(unittest.TestCase):
    def test_execute_runs_one_vector_and_releases_the_context(self):
        rados, native = FakeRados(), FakeNative()
        request = {"action": "execute", "slot": 1, "object": "o", "operations": EVERY}
        result = readcompound.mode_readcompound(rados, "cluster", request, None, native=native)["read_compound"]
        self.assertEqual(result["native"], {"aggregate": -125})
        self.assertEqual(len(native.calls), 1)
        kind, io, name, actions = native.calls[0]
        self.assertEqual((kind, io, name, len(actions)), ("execute", "io-1", b"o", len(EVERY)))
        self.assertEqual(rados.lib.destroyed, ["io-1"])

    def test_seed_sorts_its_input_and_reports_the_state(self):
        rados, native = FakeRados(), FakeNative()
        request = {"action": "seed", "slot": 2, "object": "o", "data": b(b"bytes"),
                   "attributes": [{"name": b(b"z"), "value": b(b"1")}, {"name": b(b"a"), "value": b(b"2")}],
                   "entries": [{"key": b(b"k2"), "value": b(b"")}, {"key": b(b""), "value": b(b"e")}]}
        result = readcompound.mode_readcompound(rados, "cluster", request, None, native=native)["read_compound"]
        self.assertEqual(result["code"], 0)
        self.assertEqual(result["state"], {"code": 0, "version": 3})
        self.assertEqual(native.calls, [("seed", "io-1", b"o", b"bytes", [(b"a", b"2"), (b"z", b"1")], [(b"", b"e"), (b"k2", b"")])])
        self.assertEqual(rados.opened, [("cluster", "ceph-msgr-owned-pool-2", "")])

    def test_failed_seed_reports_no_state(self):
        result = readcompound.mode_readcompound(FakeRados(), "cluster", {"action": "seed", "slot": 1, "object": "o", "data": "", "attributes": [], "entries": []},
                                                None, native=FakeNative(code=-95))["read_compound"]
        self.assertEqual((result["code"], result["state"]), (-95, None))

    def test_refuses_other_requests_before_opening_a_context(self):
        for request in (
            {"action": "execute", "slot": 1, "object": "o", "operations": []},
            {"action": "execute", "slot": 1, "object": "o", "operations": EVERY + MORE},
            {"action": "execute", "slot": 1, "object": "o", "operations": [{"op": "write", "data": ""}]},
            {"action": "execute", "slot": 1, "object": "o"},
            {"action": "execute", "slot": 3, "object": "o", "operations": EVERY[:1]},
            {"action": "execute", "slot": 1, "object": "", "operations": EVERY[:1]},
            {"action": "execute", "slot": 1, "object": "o", "namespace": "n", "operations": EVERY[:1]},
            {"action": "remove", "slot": 1, "object": "o"},
            {"action": "seed", "slot": 1, "object": "o", "data": "", "attributes": [], "entries": [{"key": b(b"k"), "value": ""}] * 2},
            {"action": "seed", "slot": 1, "object": "o", "data": "", "attributes": [{"name": b(b""), "value": ""}], "entries": []},
            {"action": "seed", "slot": 1, "object": "o", "data": "", "attributes": []},
        ):
            rados, native = FakeRados(), FakeNative()
            with self.assertRaises(ValueError, msg=request):
                readcompound.mode_readcompound(rados, "cluster", request, None, native=native)
            self.assertEqual((rados.opened, native.calls), ([], []), msg=request)


class Output(unittest.TestCase):
    """The per-action record of filled and unfilled native buffers."""

    def output(self, slot):
        return readcompound.Native.output(None, slot)

    def test_an_unfilled_read_reports_no_bytes(self):
        slot = {"action": {"op": "read", "offset": 0, "length": 4}, "prval": C.c_int(-5),
                "buffer": C.create_string_buffer(bytes([0xcc]) * 4, 4), "count": C.c_size_t(readcompound.UNSET)}
        self.assertEqual(self.output(slot), {"op": "read", "prval_available": True, "prval": -5, "count_unchanged": True, "data": ""})

    def test_a_filled_read_reports_its_bytes(self):
        slot = {"action": {"op": "read", "offset": 0, "length": 4}, "prval": C.c_int(0),
                "buffer": C.create_string_buffer(b"ab\xcc\xcc", 4), "count": C.c_size_t(2)}
        self.assertEqual(self.output(slot)["data"], b(b"ab"))

    def test_a_read_count_beyond_its_buffer_fails(self):
        slot = {"action": {"op": "read", "offset": 0, "length": 4}, "prval": C.c_int(0),
                "buffer": C.create_string_buffer(4), "count": C.c_size_t(5)}
        with self.assertRaises(ValueError):
            self.output(slot)

    def test_guards_without_a_callback_report_no_prval(self):
        for op in ("assert", "cmpxattr"):
            self.assertEqual(self.output({"action": {"op": op}, "prval": C.c_int(readcompound.SENTINEL)}),
                             {"op": op, "prval_available": False, "prval": None})

    def test_an_unfilled_stat_is_marked(self):
        from osd_xattr_native import Timespec
        slot = {"action": {"op": "stat"}, "prval": C.c_int(-5), "size": C.c_uint64(readcompound.UNSET), "stamp": Timespec(-7, -11)}
        self.assertTrue(self.output(slot)["unchanged"])
        slot["size"] = C.c_uint64(16)
        self.assertFalse(self.output(slot)["unchanged"])

    def test_checksum_values_decode_only_on_success(self):
        raw = (2).to_bytes(4, "little") + (7).to_bytes(4, "little") + (9).to_bytes(4, "little")
        slot = {"action": {"op": "checksum"}, "prval": C.c_int(0), "buffer": C.create_string_buffer(raw, len(raw)), "width": 4, "chunks": 2}
        self.assertEqual(self.output(slot)["values"], [7, 9])
        slot["prval"] = C.c_int(-22)
        self.assertEqual(self.output(slot)["values"], [])
        too_many = (3).to_bytes(4, "little") + bytes(8)
        slot = {"action": {"op": "checksum"}, "prval": C.c_int(0), "buffer": C.create_string_buffer(too_many, len(too_many)), "width": 4, "chunks": 2}
        with self.assertRaises(ValueError):
            self.output(slot)


if __name__ == "__main__":
    unittest.main()
