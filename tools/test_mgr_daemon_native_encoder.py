"""Receipt rejection tests; synthetic artifacts never establish native parity."""
import json
from pathlib import Path
import tempfile
import unittest

import mgr_daemon_native_encoder as oracle


class MgrDaemonReceiptTests(unittest.TestCase):
    def model(self, root):
        for name, (typ, version, compat) in oracle.SHAPES.items():
            header = oracle.HEADER.pack(1, 0, typ, 127, version, 0, 0, 3, 1, compat, 0)
            for part, value in {"header": header, "front": b"synthetic-rejection-model",
                                "middle": b"", "data": b""}.items():
                (root / (name + "." + part + ".bin")).write_bytes(value)
            (root / (name + ".decoded.json")).write_text(json.dumps({"synthetic": True}))
        for name in oracle.REFUSED:
            (root / (name + ".front.bin")).write_bytes(b"refused")
            (root / (name + ".decoded.json")).write_text(json.dumps({"native_refused": True}))

    def test_closed_artifact_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.model(root)
            self.assertEqual(len(oracle.collect(root)), 14)
            extra = root / "foreign.front.bin"
            extra.write_bytes(b"foreign")
            with self.assertRaisesRegex(RuntimeError, "artifact set"):
                oracle.collect(root)
            extra.unlink()
            (root / "mgrmap-null.front.bin").unlink()
            with self.assertRaisesRegex(RuntimeError, "artifact set"):
                oracle.collect(root)

    def test_foreign_headers_and_segments_rejected(self):
        for part, offset in (("header", 16), ("header", 18), ("header", 20), ("header", 37),
                             ("middle", None), ("data", None), ("front", None)):
            with self.subTest(part=part, offset=offset), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.model(root)
                path = root / ("beacon-active." + part + ".bin")
                value = bytearray(path.read_bytes())
                if offset is not None:
                    value[offset] ^= 1
                else:
                    value = b"" if part == "front" else b"foreign"
                path.write_bytes(value)
                with self.assertRaisesRegex(RuntimeError, "envelope"):
                    oracle.collect(root)

    def test_refusal_receipt_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.model(root)
            (root / "beacon-count-mismatch.decoded.json").write_text(json.dumps({"native_refused": False}))
            with self.assertRaisesRegex(RuntimeError, "refusal"):
                oracle.collect(root)


if __name__ == "__main__":
    unittest.main()
