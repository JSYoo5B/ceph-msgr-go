"""Collector rejection tests; temporary copies are not new native evidence."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

import osd_copy_from_native_encoder as oracle


class CopyFromCollectorTests(unittest.TestCase):
    def setUp(self):
        proof = Path(__file__).resolve().parents[1] / "internal/osd/testdata/copy-from-native.json"
        self.fixtures = json.loads(proof.read_bytes())["fixtures"]
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name)
        for name, fixture in self.fixtures.items():
            for part in ("front", "data", "header"):
                (self.output / (name + "." + part + ".bin")).write_bytes(bytes.fromhex(fixture[part + "_hex"]))
            for suffix, field in ((".native-header.json", "native_header"),
                                  (".decoded-ops.json", "decoded_operations"),
                                  (".decoded-source.json", "decoded_source")):
                (self.output / (name + suffix)).write_text(json.dumps(fixture[field]))

    def test_retained_artifacts_have_five_distinct_inputs(self):
        self.assertEqual(len(oracle.collect(self.output, copy.deepcopy(self.fixtures))), 5)

    def test_missing_extra_or_unknown_shape_is_rejected(self):
        unknown = copy.deepcopy(self.fixtures)
        unknown["unexpected"] = unknown.pop(next(iter(unknown)))
        with self.assertRaises(RuntimeError):
            oracle.collect(self.output, unknown)
        extra = self.output / "unexpected.front.bin"
        extra.write_bytes(b"")
        with self.assertRaises(RuntimeError):
            oracle.collect(self.output, copy.deepcopy(self.fixtures))
        extra.unlink()
        next(self.output.glob("*.decoded-source.json")).unlink()
        with self.assertRaises(RuntimeError):
            oracle.collect(self.output, copy.deepcopy(self.fixtures))

    def test_source_fields_and_scalar_types_are_checked(self):
        path = self.output / (oracle.PREFIX + "copy2-truncate.decoded-source.json")
        source = json.loads(path.read_bytes())
        for field, bad in (("pool", 7), ("hash", 0), ("object_hex", "00"),
                           ("truncate_sequence", 0), ("truncate_size", True)):
            with self.subTest(field=field):
                changed = dict(source, **{field: bad})
                path.write_text(json.dumps(changed))
                with self.assertRaises(RuntimeError):
                    oracle.collect(self.output, copy.deepcopy(self.fixtures))

    def test_opcode_union_payload_and_unused_tail_are_checked(self):
        name = oracle.PREFIX + "copy2-head"
        for field, bad in (("code", 0x221a), ("flags", 1), ("payload_len", 63),
                           ("union_hex", "00" * 28), ("indata_hex", ""), ("outdata_hex", "01")):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.fixtures)
                changed[name]["decoded_operations"][0][field] = bad
                with self.assertRaises(RuntimeError):
                    oracle.collect(self.output, changed)
        changed = copy.deepcopy(self.fixtures)
        changed[name]["data_hex"] = changed[name]["data_hex"][:-2] + "01"
        changed[name]["decoded_operations"][0]["indata_hex"] = changed[name]["data_hex"]
        with self.assertRaises(RuntimeError):
            oracle.collect(self.output, changed)

    def test_raw_segments_cannot_be_substituted(self):
        for part in ("front", "data", "header"):
            with self.subTest(part=part):
                changed = copy.deepcopy(self.fixtures)
                fixture = changed[oracle.PREFIX + "copy-head"]
                fixture[part + "_hex"] = "00" + fixture[part + "_hex"][2:]
                with self.assertRaises(RuntimeError):
                    oracle.collect(self.output, changed)


if __name__ == "__main__":
    unittest.main()
