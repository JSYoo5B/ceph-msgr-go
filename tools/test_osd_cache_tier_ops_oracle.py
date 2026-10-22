import base64
import copy
import os
from pathlib import Path
import struct
import tempfile
import unittest

import osd_cache_tier_ops_oracle as oracle
from test_osd_ignore_cache_oracle import plan

NS = "7v14_2_0"
STRING = "RKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE"
SYMBOLS = {
    "_ZN8librados" + NS + "5IoCtx7operateE" + STRING + "PNS0_19ObjectReadOperationEPN4ceph6buffer7v15_2_04listEi",
    "_ZN8librados" + NS + "5IoCtx7operateE" + STRING + "PNS0_19ObjectReadOperationEPN4ceph6buffer7v15_2_04listE",
    "_ZN8librados" + NS + "5IoCtx7operateE" + STRING + "PNS0_20ObjectWriteOperationEi",
    "_ZN8librados" + NS + "5IoCtxC1Ev", "_ZN8librados" + NS + "5IoCtxD1Ev",
    "_ZN8librados" + NS + "5IoCtx18from_rados_ioctx_tEPvRS1_",
    "_ZN8librados" + NS + "15ObjectOperationC1Ev", "_ZN8librados" + NS + "15ObjectOperationD1Ev",
    "_ZN8librados" + NS + "19ObjectReadOperation11cache_flushEv",
    "_ZN8librados" + NS + "19ObjectReadOperation15cache_try_flushEv",
    "_ZN8librados" + NS + "19ObjectReadOperation11cache_evictEv",
    "_ZN8librados" + NS + "19ObjectReadOperation8is_dirtyEPbPi",
    "_ZN8librados" + NS + "20ObjectWriteOperation9cache_pinEv",
    "_ZN8librados" + NS + "20ObjectWriteOperation11cache_unpinEv",
}


def elf(names, undefined=()):
    """A minimal ELF64 shared object containing only a dynamic symbol table."""
    strings, offsets = b"\0", {}
    for name in list(names) + list(undefined):
        offsets[name] = len(strings)
        strings += name.encode() + b"\0"
    symbols = bytes(24)
    for name in names:
        symbols += struct.pack("<IBBHQQ", offsets[name], (1 << 4) | 2, 0, 9, 0x1000, 8)
    for name in undefined:
        symbols += struct.pack("<IBBHQQ", offsets[name], (1 << 4) | 2, 0, 0, 0, 0)
    dynsym, dynstr = 64, 64 + len(symbols)
    shoff = dynstr + len(strings)
    header = bytearray(64)
    header[:6] = b"\x7fELF\x02\x01"
    struct.pack_into("<Q", header, 0x28, shoff)
    struct.pack_into("<HH", header, 0x3A, 64, 3)
    sections = bytes(64)
    sections += struct.pack("<IIQQQQIIQQ", 0, 11, 2, 0, dynsym, len(symbols), 2, 1, 8, 24)
    sections += struct.pack("<IIQQQQIIQQ", 0, 3, 2, 0, dynstr, len(strings), 0, 0, 1, 0)
    return bytes(header) + symbols + strings + sections


class ScriptedTier:
    """A controller fake that follows source-derived codes; not Ceph evidence."""
    def __init__(self, try_code=0, overrides=None):
        self.heads, self.dirty, self.pinned, self.calls, self.identities = {}, set(), set(), [], {}
        self.version, self.try_code, self.overrides = 10, try_code, overrides or {}
        self.before = lambda: None

    def placement(self, pool, name):
        return {"pool_name": pool, "object": name, "primary_id": 0}

    def state(self, pool, name):
        if (pool, name) not in self.heads:
            return {"code": -2, "version": 0, "stat_prval": -2, "read_prval": -99999}
        data, version, mtime = self.heads[(pool, name)]
        return {"code": 0, "version": version, "stat_prval": 0, "read_prval": 0, "size": len(data),
                "mtime": mtime, "bytes_b64": base64.b64encode(data).decode()}

    def execute(self, role, pool, name, action, flags, data):
        self.before()
        self.calls.append((role, pool, name, action, flags))
        self.identities[role] = {"name": role, "global_id": 5}
        base, tier = plan()["base"], plan()["tier"]
        key = (role, name, action, flags)
        if key in self.overrides:
            return dict(self.overrides[key])
        present = (pool, name) in self.heads
        if action == "writefull":
            self.version += 1
            self.heads[(pool, name)] = (data, self.version, 1700000800)
            if pool == tier:
                self.dirty.add(name)
            return {"code": 0, "version": self.version}
        if action == "delete":
            self.heads.pop((pool, name), None)
            return {"code": 0 if present else -2, "version": self.version}
        if action == "snapshot":
            if not present and pool == tier and flags == 0 and (base, name) in self.heads:
                self.heads[(tier, name)] = self.heads[(base, name)]
            return self.state(pool, name)
        if action == "is-dirty":
            return {"code": 0 if present else -2, "isdirty_prval": 0 if present else -2, "dirty": name in self.dirty, "version": 0}
        if role in ("ro", "base") and action in ("cache-pin", "cache-unpin"):
            return {"code": -1, "version": 0}
        if role == "base":
            return {"code": -1, "version": 0}
        if not present:
            return {"code": -2, "version": 0}
        if action in ("cache-pin", "cache-unpin"):
            (self.pinned.add if action == "cache-pin" else self.pinned.discard)(name)
            data, version, _ = self.heads[(pool, name)]
            self.heads[(pool, name)] = (data, version, 1700000900)
            return {"code": 0, "version": version}
        locked = not flags & oracle.SKIPRWLOCKS
        if action == "cache-flush" and not locked or action == "cache-try-flush" and locked:
            return {"code": -22, "version": 0}
        if name in self.pinned:
            return {"code": -1, "version": 0}
        if action == "cache-evict":
            if name in self.dirty:
                return {"code": -16, "version": 0}
            del self.heads[(pool, name)]
            return {"code": 0, "version": 0}
        if action == "cache-try-flush" and self.try_code:
            return {"code": self.try_code, "version": 0}
        if name in self.dirty:
            data, _, mtime = self.heads[(tier, name)]
            self.version += 1
            self.heads[(base, name)] = (data, self.version, mtime)
            self.dirty.discard(name)
        return {"code": 0, "version": 0}


def receipt():
    return oracle.new_receipt(plan())


class TierOpsOracleTests(unittest.TestCase):
    def controller(self, native):
        state, saved = receipt(), []
        controller = oracle.Controller(native, plan(), state, lambda: saved.append(copy.deepcopy(state)), sleep=lambda _: None)
        native.before = lambda: self.assertEqual(saved[-1]["steps"][-1]["state"], "intended")
        return controller, state, saved

    def test_flag_layers_follow_the_pinned_cli(self):
        self.assertEqual(oracle.FLUSH_FLAGS, 8 | 32)
        self.assertEqual(oracle.TRY_FLAGS, 8 | 16 | 32)
        self.assertEqual(oracle.EVICT_FLAGS, 8 | 16 | 32)
        self.assertEqual(oracle.wire_flags(oracle.FLUSH_FLAGS), 0x8000 | 0x20000)
        self.assertEqual(oracle.wire_flags(oracle.TRY_FLAGS), 0x8000 | 0x10000 | 0x20000)
        self.assertEqual(oracle.wire_flags(0), 0)

    def test_dynamic_symbols_select_defined_exports_and_exact_overloads(self):
        names = oracle.dynamic_symbols(elf(sorted(SYMBOLS), undefined=["_ZN8librados7v14_2_05IoCtx5closeEv"]))
        self.assertEqual(names, SYMBOLS)
        resolved = oracle.resolve_symbols(names)
        self.assertTrue(resolved["operate_read"].endswith("4listEi"))
        self.assertTrue(resolved["operate_write"].endswith("20ObjectWriteOperationEi"))
        self.assertEqual(len(resolved), 13)
        with self.assertRaises(ValueError):
            oracle.resolve_symbols(SYMBOLS - {"_ZN8librados" + NS + "19ObjectReadOperation11cache_evictEv"})
        with self.assertRaises(ValueError):
            oracle.resolve_symbols({n for n in SYMBOLS if not n.endswith("4listEi")})
        with self.assertRaises(ValueError):
            oracle.dynamic_symbols(b"not an elf" + bytes(64))

    def test_loaded_library_accepts_only_one_versioned_librados_mapping(self):
        with tempfile.TemporaryDirectory() as directory:
            maps = Path(directory) / "maps"
            maps.write_text("7f00-7f01 r-xp 0 08:01 1 /usr/lib64/librados.so.2.0.0\n7f02-7f03 r-xp 0 08:01 2 /usr/lib64/libradosstriper.so.1\n")
            self.assertEqual(oracle.loaded_library(str(maps)), "/usr/lib64/librados.so.2.0.0")
            maps.write_text("7f00-7f01 r-xp 0 08:01 1 /usr/lib64/libc.so.6\n")
            with self.assertRaises(ValueError):
                oracle.loaded_library(str(maps))

    def test_complete_sequence_publishes_intent_and_classifies_every_step(self):
        native = ScriptedTier()
        controller, state, saved = self.controller(native)
        controller.run()
        self.assertTrue(state["complete"], state.get("stopped"))
        self.assertTrue(state["cleanup"]["complete"])
        labels = [step["label"] for step in state["steps"]]
        self.assertEqual(len(labels), len(set(labels)))
        for label in ("evict-dirty", "pin", "pinned-flush", "pinned-try-flush", "pinned-evict", "unpin", "flush",
                      "try-flush", "evict-clean", "ordinary-read-after-evict", "absent-pin", "absent-unpin"):
            self.assertIn(label, labels)
        steps = {step["label"]: step for step in state["steps"]}
        self.assertEqual(steps["flush"]["api_flags"], oracle.FLUSH_FLAGS)
        self.assertEqual(steps["try-flush"]["api_flags"], oracle.TRY_FLAGS)
        self.assertEqual(steps["evict-clean"]["wire_flags_source_derived"], 0x38000)
        self.assertTrue(all(step["state"] == "completed" for step in state["steps"]))
        self.assertFalse(any(call[0] == "observer" and call[3].startswith("cache-") for call in native.calls))
        self.assertEqual(sum(1 for call in native.calls if call[3] == "cache-try-flush" and call[2] == oracle.TRY), 1)
        self.assertEqual(state["try_flush"]["dirty_observations"], [False])
        self.assertEqual(native.heads, {})
        self.assertTrue(state["native_only"])
        self.assertFalse(state["product_support_claimed"])

    def test_try_flush_eagain_is_retained_without_reissue(self):
        native = ScriptedTier(try_code=-11)
        controller, state, _ = self.controller(native)
        controller.run()
        self.assertEqual(state["try_flush"]["code"], -11)
        self.assertTrue(state["try_flush"]["definite"])
        self.assertEqual(len(state["try_flush"]["dirty_observations"]), oracle.TRY_POLLS)
        self.assertEqual(sum(1 for call in native.calls if call[3] == "cache-try-flush" and call[2] == oracle.TRY), 1)
        self.assertTrue(state["complete"])

    def test_uncertain_outcome_stops_and_skips_owned_cleanup(self):
        native = ScriptedTier(overrides={("rw", oracle.FLUSH, "cache-flush", oracle.FLUSH_FLAGS): {"code": -110, "version": 0}})
        controller, state, _ = self.controller(native)
        controller.run()
        self.assertFalse(state["complete"])
        self.assertIn("unknown", state["stopped"])
        self.assertEqual(state["steps"][-1]["state"], "unknown")
        self.assertFalse(state["cleanup"]["attempted"])
        self.assertFalse(any(call[3] == "delete" for call in native.calls))
        self.assertFalse(any(call[3] == "cache-evict" and call[4] == oracle.EVICT_FLAGS and call[2] == oracle.FLUSH
                             for call in native.calls[-3:]))

    def test_definite_gate_failure_stops_sequence_but_cleans_owned_heads(self):
        native = ScriptedTier(overrides={("rw", oracle.FLUSH, "cache-pin", oracle.PIN_FLAGS): {"code": -22, "version": 0}})
        controller, state, _ = self.controller(native)
        controller.run()
        self.assertFalse(state["complete"])
        self.assertIn("outside gate", state["stopped"])
        self.assertFalse(any(step["label"] == "pinned-flush" for step in state["steps"]))
        self.assertTrue(state["cleanup"]["complete"])
        self.assertEqual(native.heads, {})

    def test_native_exception_is_unknown_and_not_repeated(self):
        native = ScriptedTier()
        original = native.execute
        def broken(role, pool, name, action, flags, data):
            if action == "cache-evict":
                raise OSError("connection lost")
            return original(role, pool, name, action, flags, data)
        native.execute = broken
        controller, state, _ = self.controller(native)
        controller.run()
        self.assertEqual(state["steps"][-1]["label"], "evict-dirty")
        self.assertEqual(state["steps"][-1]["state"], "unknown")
        self.assertFalse(state["cleanup"]["attempted"])

    def test_main_refuses_rerun_over_existing_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            (out / "osd-ignore-cache-plan.json").write_text(__import__("json").dumps(plan()))
            (out / oracle.RECEIPT).write_text("{}")
            with self.assertRaises(ValueError):
                oracle.main(["oracle", directory, directory])
            self.assertEqual((out / oracle.RECEIPT).read_text(), "{}")


if __name__ == "__main__":
    unittest.main()
