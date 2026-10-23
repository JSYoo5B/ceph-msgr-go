import csv
from pathlib import Path
import struct
import unittest

import osd_set_chunk_native_encoder as encoder

ROOT = Path(__file__).resolve().parent.parent


class SetChunkNativeEncoderTests(unittest.TestCase):
    def test_model_opcode_matches_the_pinned_catalog(self):
        catalog = {row[0]: int(row[1], 16) for row in csv.reader(
            (ROOT / "internal/osd/opcodes-tentacle.csv").read_text().splitlines()) if row and row[0].isupper()}
        self.assertEqual(encoder.SET_CHUNK, catalog["SET_CHUNK"])
        self.assertEqual(set(encoder.SHAPES), set(encoder.MODEL))

    def test_request_flags_are_the_native_read_path_only(self):
        # rados set-chunk passes no request flag; READ comes from the native
        # read submission, ONDISK|RETURNVEC from this repository's encoder.
        self.assertEqual(encoder.FLAGS, encoder.ONDISK | encoder.RETURNVEC | encoder.READ)
        for version, features in encoder.FEATURES.items():
            self.assertFalse(features & (1 << 35), version)

    def test_with_reference_is_a_per_operation_flag(self):
        for shape in encoder.SHAPES:
            (op,) = encoder.expected_operations(shape)
            want = 0 if shape == "without-reference" else encoder.WITH_REFERENCE
            self.assertEqual(op["flags"], want, shape)
            self.assertEqual(bytes.fromhex(op["union_hex"]), bytes(28))
            self.assertEqual(op["payload_len"], len(bytes.fromhex(op["indata_hex"])))

    def test_indata_layout_follows_objecter_set_chunk(self):
        data = encoder.indata("chunk-located")
        self.assertEqual(struct.unpack_from("<QQ", data), (0, 4096))
        self.assertEqual(data[16:18], bytes([6, 3]))
        size = struct.unpack_from("<I", data, 18)[0]
        locator = data[22:22 + size]
        self.assertEqual(struct.unpack_from("<qi", locator), (7, -1))
        self.assertEqual(locator[12:12 + 4 + 9], struct.pack("<I", 9) + b"chunk-key")
        self.assertEqual(locator[-8:], struct.pack("<q", -1))
        rest = data[22 + size:]
        self.assertEqual(rest, struct.pack("<I", 18) + b"set-chunk-target-2" + struct.pack("<Q", 65536))

    def test_only_the_admitted_shapes_start_at_source_offset_zero(self):
        for shape, model in encoder.MODEL.items():
            self.assertEqual(model[0] == 0, shape != "nonzero-source", shape)


if __name__ == "__main__":
    unittest.main()
