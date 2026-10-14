import pathlib
import struct
import unittest

import osd_mutation_codec_oracle as oracle


class NativeMutationInputs(unittest.TestCase):
    def test_independent_inputs_match_native_accepted_static_bytes(self):
        root = pathlib.Path(__file__).resolve().parents[1] / "internal/osd/testdata"
        for name, (_, front, data) in oracle.vectors().items():
            with self.subTest(vector=name):
                self.assertEqual(front, bytes.fromhex((root / (name + ".front.hex")).read_text()))
                self.assertEqual(data, bytes.fromhex((root / (name + ".data.hex")).read_text()))

    def test_request_head_and_ordered_data(self):
        front, data = oracle.request(oracle.OPS)
        # These literal wire values independently bind head sentinel and
        # ordered aggregate input; they caught a SNAPDIR/NOSNAP confusion.
        self.assertEqual(data, b"\x00\xffA\nnew\x80!\x00\xff")
        self.assertEqual(front[-59:-51], b"\xfe\xff\xff\xff\xff\xff\xff\xff")
        self.assertEqual(struct.unpack_from("<I", front, 8)[0], 0x4000034)

    def test_exported_envelope_requires_exact_bounded_segments(self):
        front, data = oracle.request(oracle.OPS)
        raw = oracle.envelope(42, front, data)
        self.assertEqual(oracle.extract(raw), (front, data))
        bad = raw[:66] + struct.pack("<I", (1 << 20) + 1) + raw[70:]
        for invalid in (b"", raw[:-1], raw + b"x", bad):
            with self.subTest(bytes=len(invalid)):
                with self.assertRaises(ValueError):
                    oracle.extract(invalid)
        nonempty_middle = raw[:66] + oracle.blob(front) + oracle.blob(b"x") + oracle.blob(data)
        with self.assertRaises(ValueError):
            oracle.extract(nonempty_middle)


if __name__ == "__main__":
    unittest.main()
