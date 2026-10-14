#!/usr/bin/env python3
"""C argument/lifetime/admission tests; no mocked Ceph compatibility claim."""
import ctypes as C
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import osd_omap_native as omap
from test_osd_xattr_native import Library


class IteratorLibrary(Library):
    def __init__(self, entries, fault=None):
        super().__init__()
        self.entries = iter(entries)
        self.buffers = []
        self.fault = fault
        self.install("rados_omap_get_next2", self.next)

    def next(self, iterator, key, value, kl, vl):
        # Invalidate the previous native buffer on every step to ensure the
        # adapter copied it before advancing, including bytes after NUL.
        for buf in self.buffers:
            C.memset(C.addressof(buf), 88, C.sizeof(buf))
        self.buffers = []
        item = next(self.entries, None)
        if item is None:
            key._obj.value, value._obj.value = None, None
            kl._obj.value, vl._obj.value = 0, 0
            if self.fault == "bad end":
                kl._obj.value = 1
            return 0
        k, v = item
        kb = C.create_string_buffer(k, len(k) + 1)
        self.buffers.append(kb)
        key._obj.value, kl._obj.value = C.addressof(kb), len(k)
        value._obj.value, vl._obj.value = None, len(v)
        if v:
            vb = C.create_string_buffer(v, len(v) + 1)
            self.buffers.append(vb)
            value._obj.value = C.addressof(vb)
        if self.fault == "null value":
            value._obj.value = None
            vl._obj.value = 1
        elif self.fault == "huge key":
            kl._obj.value = omap.MAX_KEY + 1
        elif self.fault == "huge value":
            vl._obj.value = omap.MAX_BINARY + 1
        return 0


class OmapNativeTests(unittest.TestCase):
    def request(self, **updates):
        raw = {"baseline": omap.xattr.encode(b"\0binary\xff"),
               "attributes": [{"name": omap.xattr.encode(b"kept"), "value": ""}],
               "entries": [{"key": "", "value": ""},
                           {"key": omap.xattr.encode(b"n\0after"), "value": omap.xattr.encode(b"\0\xff")},
                           {"key": omap.xattr.encode(b"\xff-key"), "value": ""}],
               "operations": [{"op": "keys", "max_return": 0},
                              {"op": "vals", "start_after": "", "prefix": "", "max_return": (1 << 64) - 1},
                              {"op": "bykeys", "keys": ["", omap.xattr.encode(b"n\0after")]}]}
        raw.update(updates)
        return raw

    def test_binary_empty_highbit_keys_and_zero_huge_count_are_admitted(self):
        request = omap.validate_request(self.request(), "compare")
        self.assertEqual(request["entries"], [(b"", b""), (b"n\0after", b"\0\xff"), (b"\xff-key", b"")])
        self.assertEqual(request["operations"][0]["max_return"], 0)
        self.assertEqual(request["operations"][1]["max_return"], (1 << 64) - 1)
        self.assertEqual(request["operations"][2]["keys"], [b"", b"n\0after"])

    def test_cursor_prefix_nul_is_only_c_oracle_restriction(self):
        for op, field in (("keys", "start_after"), ("vals", "start_after"), ("vals", "prefix")):
            with self.subTest(op=op, field=field), self.assertRaisesRegex(ValueError, "C oracle.*wire support"):
                omap.validate_request(self.request(operations=[{"op": op, field: omap.xattr.encode(b"a\0b")}]), "compare")

    def test_duplicate_shape_flags_counts_and_aggregate_budget_rejected(self):
        bad = [self.request(entries=[{"key": "", "value": ""}] * 2),
               self.request(operations=[{"op": "bykeys", "keys": ["", ""]}]),
               self.request(operations=[{"op": "header"}]),
               self.request(operations=[{"op": "keys", "max_return": True}]),
               self.request(operations=[{"op": "keys", "max_return": -1}]),
               self.request(operations=[{"op": "keys", "max_return": 1 << 64}]),
               self.request(operations=[{"op": "keys", "flags": 1}]),
               self.request(operations=[{"op": "keys", "flags": False}]),
               self.request(operations=[{"op": "keys", "value": ""}]),
               self.request(entries=[{"key": omap.xattr.encode(bytes([i])), "value": ""} for i in range(129)]),
               self.request(attributes=[{"name": omap.xattr.encode(str(i).encode()), "value": ""} for i in range(65)]),
               self.request(operations=[{"op": "keys"}] * 65),
               self.request(baseline=omap.xattr.encode(b"x" * omap.MAX_BINARY))]
        for raw in bad:
            with self.subTest(raw=str(raw)[:80]), self.assertRaises(ValueError):
                omap.validate_request(raw, "compare")

    def test_iterator_null_empty_value_and_empty_key_are_existing_entries(self):
        raw = [(b"", b""), (b"a\0z", b"\0A\xff"), (b"\xff", b"")]
        native = omap.Native(IteratorLibrary(raw))
        self.assertEqual(native.omap_entries(C.c_void_p(1)),
                         [{"key": omap.xattr.encode(k), "value": omap.xattr.encode(v)} for k, v in raw])
        self.assertEqual(native.lib.rados_omap_get_next2.argtypes[3], C.POINTER(C.c_size_t))

    def test_iterator_bad_lengths_order_terminator_fail_before_dereference(self):
        for fault in ("null value", "huge key", "huge value", "bad end"):
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                omap.Native(IteratorLibrary([(b"key", b"value")], fault)).omap_entries(C.c_void_p(1))
        for entries in ([(b"a", b""), (b"a", b"")], [(b"z", b""), (b"a", b"")]):
            with self.assertRaisesRegex(ValueError, "order/uniqueness"):
                omap.Native(IteratorLibrary(entries)).omap_entries(C.c_void_p(1))
        with self.assertRaisesRegex(ValueError, "GETKEYS"):
            omap.Native(IteratorLibrary([(b"key", b"nonempty")])).omap_entries(C.c_void_p(1), True)

    def test_seed_set2_keeps_binary_key_value_exact_lengths_and_exclusive_create(self):
        library = Library()
        captured = []
        def set2(op, keys, vals, kl, vl, count):
            captured.extend((C.string_at(keys[i], kl[i]), C.string_at(vals[i], vl[i])) for i in range(count))
        library.install("rados_write_op_omap_set2", set2)
        library.install("rados_write_op_operate2", lambda *args: 0)
        request = omap.validate_request(self.request(operations=[]), "prepare")
        native = omap.Native(library)
        self.assertEqual(native.seed(C.c_void_p(1), b"owned", request), 0)
        self.assertEqual(captured, request["entries"])
        creates = [args for name, args in library.calls if name == "rados_write_op_create"]
        self.assertEqual(creates[0][1:], (1, None))
        self.assertEqual(sum(name == "rados_release_write_op" for name, _ in library.calls), 1)

    def vector_native(self, aggregate=0, prvals=(0, 0, 0), invalid_more=False, iterator_error=False):
        library = Library()
        outputs, captured = [], []
        def page(op, *args):
            iterator, more, result = args[-3:]
            iterator._obj.value = len(outputs) + 10
            outputs.append((result, more))
            captured.append(args[:-3])
        def bykeys(op, keys, count, lengths, iterator, result):
            iterator._obj.value = len(outputs) + 10
            outputs.append((result, None))
            captured.append([C.string_at(keys[i], lengths[i]) for i in range(count)])
        def operate(*args):
            for index, (result, more) in enumerate(outputs):
                result._obj.value = prvals[index]
                if more is not None:
                    more._obj.value = 2 if invalid_more else index % 2
            return aggregate
        library.install("rados_read_op_omap_get_keys2", page)
        library.install("rados_read_op_omap_get_vals2", page)
        library.install("rados_read_op_omap_get_vals_by_keys2", bykeys)
        library.install("rados_read_op_operate", operate)
        native = omap.Native(library)
        def entries(*args):
            if iterator_error:
                raise ValueError("first iterator error")
            return []
        native.omap_entries = entries
        return native, library, captured

    def test_builders_exact_signatures_more_and_prval_independent_of_aggregate(self):
        native, library, captured = self.vector_native(prvals=(0, -125, 7))
        request = omap.validate_request(self.request(), "compare")
        result = native.read_vector(C.c_void_p(1), b"owned", request["operations"])
        self.assertEqual(captured, [(b"", 0), (b"", b"", (1 << 64) - 1), [b"", b"n\0after"]])
        self.assertEqual(result["aggregate"], 0)
        self.assertEqual([op["prval"] for op in result["operations"]], [0, -125, 7])
        self.assertEqual([op["more"] for op in result["operations"]], [False, None, None])
        self.assertEqual(result["operations"][1]["entries"], None)
        self.assertEqual(sum(name == "rados_omap_get_end" for name, _ in library.calls), 3)
        self.assertEqual(sum(name == "rados_release_read_op" for name, _ in library.calls), 1)

    def test_negative_aggregate_does_not_invent_successful_or_unexecuted_output(self):
        native, library, _ = self.vector_native(aggregate=-13, prvals=(-13, 999, 999))
        result = native.read_vector(C.c_void_p(1), b"owned", omap.validate_request(self.request(), "compare")["operations"])
        self.assertEqual([op["prval"] for op in result["operations"]], [-13, 999, 999])
        self.assertTrue(all(op["entries"] is None and op["more"] is None for op in result["operations"]))

    def test_iterator_and_bool_failure_release_all_iterators_and_op(self):
        for options in ({"invalid_more": True}, {"iterator_error": True}):
            native, library, _ = self.vector_native(**options)
            with self.subTest(options=options), self.assertRaises(ValueError):
                native.read_vector(C.c_void_p(1), b"owned", omap.validate_request(self.request(), "compare")["operations"])
            self.assertEqual(sum(name == "rados_omap_get_end" for name, _ in library.calls), 3)
            self.assertEqual(sum(name == "rados_release_read_op" for name, _ in library.calls), 1)

    def execute_native(self, seed_code=0, seed_error=None, cleanup=-2):
        library, events = Library(), []
        native = omap.Native(library)
        request = omap.validate_request(self.request(), "compare")
        def seed(io, name, req):
            events.append(("seed", name))
            if seed_error:
                raise RuntimeError(seed_error)
            return seed_code
        def remove(io, name):
            events.append(("remove", name))
            return cleanup
        def state(io, name, keys):
            events.append(("state", name))
            return {"code": 0, "data": omap.xattr.encode(request["baseline"]), "version": 7,
                    "seconds": 1700000000, "nanos": 123456789,
                    "attributes": [{"name": omap.xattr.encode(k), "value": omap.xattr.encode(v)} for k, v in request["attributes"]],
                    "omap": {"operations": [{"entries": []}, {"entries": [{"key": omap.xattr.encode(k), "value": omap.xattr.encode(v)} for k, v in request["entries"]]}]}}
        native.seed, native.state = seed, state
        native.read_vector = lambda io, name, actions: events.append(("read", name)) or {"aggregate": 0}
        library.install("rados_remove", remove)
        return native, events, request

    def test_one_seed_one_vector_cleanup_only_owned_new_object(self):
        native, events, request = self.execute_native()
        result = omap.execute(native, C.c_void_p(1), {"pool": "p", "pool_id": 7, "object": "original"}, request, "compare", Path("unused"))
        self.assertEqual([kind for kind, _ in events], ["seed", "state", "read", "state", "remove"])
        self.assertTrue(all(name.startswith(b"original-native-omap-") for _, name in events))
        self.assertTrue(result["cleanup"]["object_removed"])
        self.assertFalse(result["scope"]["raw_wire_payload_available"])

    def test_unknown_seed_is_not_replayed_but_owned_candidate_is_cleaned(self):
        for code, error in ((-5, None), (0, "first seed error")):
            native, events, request = self.execute_native(code, error, cleanup=-5)
            with self.subTest(code=code, error=error), self.assertRaisesRegex(RuntimeError, "first seed error" if error else "-5"):
                omap.execute(native, C.c_void_p(1), {"pool": "p", "pool_id": 7, "object": "original"}, request, "compare", Path("unused"))
            self.assertEqual([kind for kind, _ in events], ["seed", "remove"])
        native, events, request = self.execute_native(-17)
        with self.assertRaises(RuntimeError):
            omap.execute(native, C.c_void_p(1), {"pool": "p", "pool_id": 7, "object": "original"}, request, "compare", Path("unused"))
        self.assertEqual([kind for kind, _ in events], ["seed"])

    def test_prepare_transfers_receipt_and_observe_never_seeds_removes(self):
        metadata = {"pool": "p", "pool_id": 7, "object": "original"}
        native, events, _ = self.execute_native()
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            result = omap.execute(native, C.c_void_p(1), metadata,
                                  omap.validate_request(self.request(operations=[]), "prepare"), "prepare", out)
            receipt = omap.read_json(out / "osd-omap-owned.json")
            self.assertEqual(omap.check_ownership(receipt, metadata, ""), result["object"].encode())
            self.assertEqual([kind for kind, _ in events], ["seed", "state"])
        events.clear()
        request = omap.validate_request({"object": "caller-explicit", "operations": [{"op": "bykeys", "keys": [""]}]}, "observe")
        result = omap.execute(native, C.c_void_p(1), metadata, request, "observe", Path("unused"))
        self.assertEqual(events, [("state", b"caller-explicit"), ("read", b"caller-explicit"), ("state", b"caller-explicit")])
        self.assertFalse(result["cleanup"]["object_touched"])

    def test_ownership_rejects_original_object_namespace_pool_and_invalid_suffix(self):
        metadata = {"pool": "p", "pool_id": 7, "object": "original"}
        request = {"namespace": "tenant"}
        receipt = omap.ownership(metadata, request, b"original-native-omap-" + b"a" * 32)
        for update in ({"object": "original"}, {"namespace": ""}, {"pool_id": 8},
                       {"object": "original-native-omap-" + "z" * 32}, {"source_commit": "unverified"}):
            with self.subTest(update=update), self.assertRaises(ValueError):
                omap.check_ownership(dict(receipt, **update), metadata, "tenant")
        with self.assertRaisesRegex(ValueError, "owned object identity exceeds"):
            omap.owned_name(dict(metadata, object="x" * omap.xattr.MAX_NAME))

    def test_incomplete_inventory_rejected_before_state_comparison(self):
        native = omap.Native(Library())
        native.snapshot = lambda *args: {"code": 0}
        native.read_vector = lambda *args: {"aggregate": 0, "operations": [{"prval": 0, "more": True}, {"prval": 0}]}
        with self.assertRaisesRegex(ValueError, "inventory exceeds"):
            native.state(C.c_void_p(1), b"owned", [b"binary\0key"])

    def test_bounded_json_and_atomic_output_preserve_previous_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            (out / "input").write_bytes(b" " * (omap.MAX_INPUT + 1))
            with self.assertRaisesRegex(ValueError, "1 MiB"):
                omap.read_json(out / "input")
            (out / "result.json").write_text("old receipt")
            with self.assertRaisesRegex(ValueError, "512 KiB"):
                omap.publish(out, "result", {"data": "x" * omap.MAX_OUTPUT})
            self.assertEqual((out / "result.json").read_text(), "old receipt")
            self.assertFalse((out / "result.tmp").exists())

    def test_invalid_input_rejected_before_library_or_client_create(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            (out / "osd-omap-input.json").write_text(json.dumps({"object": "original", "operations": [{"op": "vals", "prefix": omap.xattr.encode(b"\0") }]}))
            with patch.object(omap, "Native") as constructor, self.assertRaises(ValueError):
                omap.collect(out, out, "observe")
            constructor.assert_not_called()


if __name__ == "__main__":
    unittest.main()
