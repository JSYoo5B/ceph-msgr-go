import base64
import struct
import tempfile
import unittest
from pathlib import Path

import osd_set_chunk_oracle as oracle
from test_osd_cache_tier_ops_oracle import NS, STRING, elf

REQUEST = {"source_pool_id": 7, "chunk_pool_id": 8}
SYMBOLS = {
    "_ZN8librados" + NS + "5IoCtx7operateE" + STRING + "PNS0_19ObjectReadOperationEPN4ceph6buffer7v15_2_04listEi",
    "_ZN8librados" + NS + "5IoCtxC1Ev", "_ZN8librados" + NS + "5IoCtxD1Ev",
    "_ZN8librados" + NS + "5IoCtx18from_rados_ioctx_tEPvRS1_",
    "_ZN8librados" + NS + "15ObjectOperationC1Ev", "_ZN8librados" + NS + "15ObjectOperationD1Ev",
    "_ZN8librados" + NS + "19ObjectReadOperation9set_chunkEmmRKNS0_5IoCtxENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEmi",
}


def envelope(version, compat, body):
    return struct.pack("<BBI", version, compat, len(body)) + body


def string(text):
    raw = text.encode()
    return struct.pack("<I", len(raw)) + raw


def hobject(name, pool, hash_=0x1234, snap=oracle.CEPH_NOSNAP, namespace=""):
    body = string("") + string(name) + struct.pack("<QIB", snap, hash_, 0) + string(namespace) + struct.pack("<q", pool)
    return envelope(4, 3, body)


def refs(*objects):
    inner = envelope(1, 1, struct.pack("<I", len(objects)) + b"".join(objects))
    return envelope(1, 1, b"\x01" + inner)


class ScriptedManifest:
    """Follows the pinned handler's source-derived branches; not Ceph evidence."""

    def __init__(self, overrides=None, removes_target=True):
        self.objects, self.chunks, self.target_refs = {}, {}, {}
        self.version, self.calls = 10, []
        self.overrides, self.removes_target = overrides or {}, removes_target

    def _state(self, role, name):
        key = (role, name)
        if key not in self.objects:
            return {"code": -2, "version": 0, "data": "", "attributes": None, "gets": []}
        data, version, xattrs = self.objects[key]
        state = {"code": 0, "version": version, "seconds": 1700001100, "nanos": 0,
                 "data": base64.b64encode(data).decode(), "attributes": sorted(xattrs), "gets": []}
        if role == "chunk":
            value = xattrs.get(oracle.REFS_ATTR)
            state["gets"] = [{"name": base64.b64encode(oracle.REFS_ATTR).decode(), "code": len(value) if value else -61,
                              "value": base64.b64encode(value).decode() if value else None}]
        return state

    def observe(self, role, name):
        return self._state(role, name)

    def references(self, name, target):
        if ("source", name) not in self.objects:
            return {"code": -2, "version": 0}
        count = sum(1 for _, (t, _) in self.chunks.items() if t == target)
        return {"code": count or -67, "version": self.objects[("source", name)][1]}

    def seed(self, role, name, data):
        self.calls.append(("seed", name))
        self.version += 1
        self.objects[(role, name)] = (data, self.version, {})
        return {"code": 0, "version": self.version}

    def remove(self, role, name):
        self.calls.append(("remove", name))
        if (role, name) not in self.objects:
            return {"code": -2, "version": 0}
        del self.objects[(role, name)]
        if role == "source" and name == oracle.SOURCE:
            for target in [t for t, _ in self.chunks.values()]:
                count = self.target_refs.get(target, 0) - 1
                self.target_refs[target] = count
                if count == 0 and self.removes_target:
                    self.objects.pop(("chunk", target), None)
            self.chunks.clear()
        return {"code": 0, "version": 0}

    def set_chunk(self, name, offset, length, target, target_offset, flag):
        self.calls.append(("set-chunk", name, offset, length, target, target_offset, flag))
        key = (name, offset, length, target, target_offset, flag)
        if key in self.overrides:
            return dict(self.overrides[key])
        source = self.objects.get(("source", name))
        if source is None:
            return {"code": -2, "version": 0}
        if length == 0:
            return {"code": -22, "version": source[1]}
        if offset + length > len(source[0]):
            return {"code": -34, "version": source[1]}
        if not flag & oracle.WITH_REFERENCE:
            return {"code": -95, "version": source[1]}
        for start, (_, size) in self.chunks.items():
            if start < offset + length and offset < start + size:
                return {"code": -95, "version": source[1]}
        chunk = self.objects.get(("chunk", target))
        if chunk is None:
            return {"code": -2, "version": source[1]}
        self.chunks[offset] = (target, length)
        self.target_refs[target] = self.target_refs.get(target, 0) + 1
        self.version += 1
        encoded = refs(*[hobject(oracle.SOURCE, REQUEST["source_pool_id"])] * self.target_refs[target])
        self.objects[("chunk", target)] = (chunk[0], self.version, {oracle.REFS_ATTR: encoded})
        return {"code": 0, "version": source[1]}


def run(native):
    receipt = oracle.new_receipt(REQUEST)
    controller = oracle.Controller(native, REQUEST, receipt, lambda: None, sleep=lambda _: None)
    controller.run()
    return receipt


class DecodeTest(unittest.TestCase):
    def test_by_object_references_decode_exactly(self):
        raw = refs(hobject("src", 7), hobject("src", 7, namespace="ns"))
        decoded = oracle.decode_refs(raw)
        self.assertEqual(decoded["count"], 2)
        self.assertEqual(decoded["refs"][0], {"key": "", "oid": "src", "snap": oracle.CEPH_NOSNAP, "hash": 0x1234,
                                              "max": False, "namespace": "", "pool": 7})
        self.assertEqual(decoded["refs"][1]["namespace"], "ns")

    def test_malformed_or_other_encodings_are_refused(self):
        raw = refs(hobject("src", 7))
        for bad in (raw[:-1], raw + b"\0", b"\x02" + raw[1:], raw[:6] + b"\x05" + raw[7:]):
            with self.assertRaises(ValueError):
                oracle.decode_refs(bad)


class SymbolTest(unittest.TestCase):
    def test_exact_set_chunk_entry_points_resolve(self):
        resolved = oracle.Cxx.resolve(oracle.cxxabi.dynamic_symbols(elf(SYMBOLS)))
        self.assertEqual(set(resolved.values()), SYMBOLS)
        self.assertIn("9set_chunkEmmRKNS0_5IoCtxE", resolved["set_chunk"])

    def test_missing_set_chunk_is_refused(self):
        names = {name for name in SYMBOLS if "set_chunk" not in name}
        with self.assertRaisesRegex(ValueError, "set_chunk"):
            oracle.Cxx.resolve(oracle.cxxabi.dynamic_symbols(elf(names)))


class InputTest(unittest.TestCase):
    def test_closed_input(self):
        self.assertEqual(oracle.validate_input(dict(REQUEST)), REQUEST)
        for bad in ({"source_pool_id": 7}, dict(REQUEST, extra=1), {"source_pool_id": 7, "chunk_pool_id": 7},
                    {"source_pool_id": "7", "chunk_pool_id": 8}, {"source_pool_id": 0, "chunk_pool_id": 8}):
            with self.assertRaises(ValueError):
                oracle.validate_input(bad)

    def test_pool_must_be_plain_replicated(self):
        plain = {"pool": 7, "type": 1, "tier_of": -1, "tiers": [], "read_tier": -1, "write_tier": -1, "cache_mode": "none", "options": {}}
        oracle.check_pool(plain, 7)
        for change in ({"type": 3}, {"tier_of": 8}, {"tiers": [8]}, {"cache_mode": "writeback"}, {"pool": 9},
                       {"options": {"dedup_tier": 8}}):
            with self.assertRaises(ValueError):
                oracle.check_pool(dict(plain, **change), 7)
        with self.assertRaises(ValueError):
            oracle.check_pool(None, 7)

    def test_receipt_is_never_rerun(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            (out / oracle.INPUT).write_text('{"source_pool_id": 7, "chunk_pool_id": 8}')
            (out / "daemon-version").write_text("ceph version 20.2.4 (%s) tentacle (stable)" % oracle.COMMIT)
            (out / oracle.RECEIPT).write_text("{}")
            with self.assertRaisesRegex(ValueError, "never rerun"):
                oracle.main(["oracle", directory, directory])


class ControllerTest(unittest.TestCase):
    def test_source_derived_sequence_completes_once(self):
        native = ScriptedManifest()
        receipt = run(native)
        self.assertTrue(receipt["complete"], receipt.get("stopped"))
        labels = [step["label"] for step in receipt["steps"]]
        self.assertEqual(len(labels), len(set(labels)))
        issued = [call for call in native.calls if call[0] == "set-chunk"]
        self.assertEqual(len(issued), len(oracle.SEQUENCE))
        codes = {step["label"]: step["result"]["code"] for step in receipt["steps"] if step["kind"] == "set-chunk"}
        self.assertEqual(codes, {entry[0]: entry[7] for entry in oracle.SEQUENCE})
        self.assertEqual(receipt["observations"]["after-chunk-b"]["chunk_refs"]["decoded"]["count"], 2)
        self.assertEqual(receipt["observations"]["after-chunk-b"]["references_chunk"]["code"], 2)
        self.assertTrue(receipt["release"]["target_removed_by_reference_release"])
        self.assertTrue(receipt["cleanup"]["complete"])

    def test_unexpected_result_stops_without_reissue(self):
        key = ("set-chunk-source", 0, 8, "set-chunk-target", 0, 0)
        native = ScriptedManifest(overrides={key: {"code": 0, "version": 12}})
        receipt = run(native)
        self.assertFalse(receipt["complete"])
        self.assertIn("without-reference", receipt["stopped"])
        self.assertEqual(sum(1 for call in native.calls if call[1:] == key), 1)
        self.assertTrue(receipt["cleanup"]["complete"])

    def test_uncertain_result_skips_cleanup(self):
        native = ScriptedManifest(overrides={("set-chunk-source", 8, 8, "set-chunk-target", 0, 0x80): {"code": -110, "version": 0}})
        receipt = run(native)
        self.assertFalse(receipt["complete"])
        self.assertEqual(receipt["cleanup"], {"attempted": False, "complete": False,
                                              "reason": "an uncertain native outcome stopped the controller"})
        self.assertEqual([call for call in native.calls if call[0] == "remove"], [])

    def test_target_left_after_release_is_reported(self):
        receipt = run(ScriptedManifest(removes_target=False))
        self.assertFalse(receipt["complete"])
        self.assertFalse(receipt["release"]["target_removed_by_reference_release"])
        self.assertEqual(len(receipt["release"]["polls"]), oracle.RELEASE_POLLS)
        self.assertTrue(receipt["cleanup"]["complete"])

    def test_reference_identity_is_checked(self):
        native = ScriptedManifest()
        original = native.set_chunk

        def foreign(*args):
            result = original(*args)
            if result["code"] == 0:
                data, version, _ = native.objects[("chunk", oracle.TARGET)]
                native.objects[("chunk", oracle.TARGET)] = (data, version, {oracle.REFS_ATTR: refs(hobject("other", 7))})
            return result
        native.set_chunk = foreign
        receipt = run(native)
        self.assertFalse(receipt["complete"])
        self.assertIn("first chunk reference differs", receipt["stopped"])


if __name__ == "__main__":
    unittest.main()
