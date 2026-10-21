"""Receipt rejection tests; synthetic artifacts never establish native parity."""
import json
from pathlib import Path
import tempfile
import unittest

import mgr_service_native_encoder as oracle


class MgrServiceReceiptTests(unittest.TestCase):
    def model(self, root):
        for name, (typ, version) in oracle.SHAPES.items():
            header = oracle.HEADER.pack(1, 0, typ, 127, version, 0, 0, 3, 1, 1, 0)
            for part, value in {"header": header, "front": b"synthetic-rejection-model",
                                "middle": b"", "data": b""}.items():
                (root / (name + "." + part + ".bin")).write_bytes(value)
            (root / (name + ".decoded.json")).write_text(json.dumps(oracle.expected_fields(name)))

    def test_closed_artifact_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.model(root)
            self.assertEqual(len(oracle.collect(root)), 8)
            extra = root / "foreign.front.bin"
            extra.write_bytes(b"foreign")
            with self.assertRaisesRegex(RuntimeError, "artifact set"):
                oracle.collect(root)
            extra.unlink()
            (root / "close.front.bin").unlink()
            with self.assertRaisesRegex(RuntimeError, "artifact set"):
                oracle.collect(root)

    def test_foreign_headers_and_segments_rejected(self):
        for part, offset in (("header", 0), ("header", 8), ("header", 16),
                             ("header", 20), ("header", 22), ("header", 37),
                             ("middle", None), ("data", None), ("front", None)):
            with self.subTest(part=part, offset=offset), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.model(root)
                path = root / ("report-empty." + part + ".bin")
                value = bytearray(path.read_bytes())
                if offset is not None:
                    value[offset] ^= 1
                else:
                    value = b"" if part == "front" else b"foreign"
                path.write_bytes(value)
                with self.assertRaisesRegex(RuntimeError, "envelope"):
                    oracle.collect(root)

    def test_nil_empty_and_false_receipts_are_distinct(self):
        for name, key, value in (("report-empty", "status_hex", None),
                                 ("report-omitted", "task_hex", {}),
                                 ("report-populated", "status_hex", {}),
                                 ("report-empty", "packed_hex", ""),
                                 ("configure", "period", True),
                                 ("open-empty", "service_daemon", 1),
                                 ("close", "service_hex", "00")):
            with self.subTest(name=name, key=key), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.model(root)
                path = root / (name + ".decoded.json")
                fields = json.loads(path.read_text())
                fields[key] = value
                path.write_text(json.dumps(fields))
                with self.assertRaisesRegex(RuntimeError, "decoder fields"):
                    oracle.collect(root)


if __name__ == "__main__":
    unittest.main()
