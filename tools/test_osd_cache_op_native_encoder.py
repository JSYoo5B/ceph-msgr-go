import csv
from pathlib import Path
import unittest

import osd_cache_op_native_encoder as encoder

ROOT = Path(__file__).resolve().parent.parent


class CacheOpNativeEncoderTests(unittest.TestCase):
    def test_model_opcodes_match_the_pinned_catalog(self):
        catalog = {row[0]: int(row[1], 16) for row in csv.reader(
            (ROOT / "internal/osd/opcodes-tentacle.csv").read_text().splitlines()) if row and row[0].isupper()}
        names = {"flush": "CACHE_FLUSH", "try-flush": "CACHE_TRY_FLUSH", "evict": "CACHE_EVICT",
                 "pin": "CACHE_PIN", "pin-ignore-cache": "CACHE_PIN",
                 "unpin": "CACHE_UNPIN", "unpin-ignore-cache": "CACHE_UNPIN"}
        self.assertEqual(set(encoder.SHAPES), set(encoder.MODEL))
        for shape, (code, _, _) in encoder.MODEL.items():
            self.assertEqual(code, catalog[names[shape]], shape)

    def test_native_caller_flag_layers_stay_distinct(self):
        tier = encoder.IGNORE_CACHE | encoder.IGNORE_OVERLAY
        self.assertEqual(encoder.MODEL["flush"][1], encoder.READ | tier)
        for shape in ("try-flush", "evict"):
            self.assertEqual(encoder.MODEL[shape][1], encoder.READ | tier | encoder.SKIPRWLOCKS)
        for shape in ("pin", "unpin"):
            self.assertEqual(encoder.MODEL[shape][1], encoder.WRITE)
            self.assertEqual(encoder.MODEL[shape + "-ignore-cache"][1], encoder.WRITE | encoder.IGNORE_CACHE)
        for shape, (_, flags, write) in encoder.MODEL.items():
            self.assertEqual(bool(flags & encoder.WRITE), write, shape)
            self.assertNotEqual(bool(flags & encoder.READ), write, shape)

    def test_operations_carry_no_union_or_input(self):
        for shape in encoder.SHAPES:
            (op,) = encoder.expected_operations(shape)
            self.assertEqual((op["flags"], op["payload_len"], op["indata_hex"]), (0, 0, ""))
            self.assertEqual(bytes.fromhex(op["union_hex"]), bytes(28))


if __name__ == "__main__":
    unittest.main()
