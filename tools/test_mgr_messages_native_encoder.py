"""Receipt rejection tests; synthetic artifacts never establish native parity."""
import json
from pathlib import Path
import tempfile
import unittest

import mgr_messages_native_encoder as oracle


class MgrMessagesReceiptTests(unittest.TestCase):
    def model(self, root):
        for name, (typ, version, compat, tid, data) in oracle.SHAPES.items():
            header = oracle.HEADER.pack(1, tid, typ, 127, version, 0, 0, 3, 1, compat, 0)
            for part, value in {"header": header, "front": b"synthetic-rejection-model",
                                "middle": b"", "data": b"bulk" if data else b""}.items():
                (root / (name + "." + part + ".bin")).write_bytes(value)
            (root / (name + ".decoded.json")).write_text(json.dumps({"synthetic": True}))
        for name in oracle.REFUSED:
            (root / (name + ".front.bin")).write_bytes(b"refused")
            (root / (name + ".decoded.json")).write_text(json.dumps({"native_refused": True}))

    def test_closed_artifact_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.model(root)
            self.assertEqual(len(oracle.collect(root)), 21)
            extra = root / "foreign.front.bin"
            extra.write_bytes(b"foreign")
            with self.assertRaisesRegex(RuntimeError, "artifact set"):
                oracle.collect(root)
            extra.unlink()
            (root / "pgstats.front.bin").unlink()
            with self.assertRaisesRegex(RuntimeError, "artifact set"):
                oracle.collect(root)

    def test_foreign_headers_and_segments_rejected(self):
        for name, part, offset in (("report-declare", "header", 8), ("report-declare", "header", 16),
                                   ("report-declare", "header", 18), ("report-declare", "header", 20),
                                   ("report-declare", "header", 37), ("report-declare", "middle", None),
                                   ("report-declare", "data", None), ("report-declare", "front", None),
                                   ("mgr-command", "data", None), ("mgr-command", "header", 8)):
            with self.subTest(name=name, part=part, offset=offset), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.model(root)
                path = root / (name + "." + part + ".bin")
                value = bytearray(path.read_bytes())
                if offset is not None:
                    value[offset] ^= 1
                elif part == "front" or (part == "data" and value):
                    value = b""
                else:
                    value = b"foreign"
                path.write_bytes(value)
                with self.assertRaisesRegex(RuntimeError, "envelope"):
                    oracle.collect(root)

    def test_refusal_receipt_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.model(root)
            (root / "report-truncated.decoded.json").write_text(json.dumps({"native_refused": False}))
            with self.assertRaisesRegex(RuntimeError, "refusal"):
                oracle.collect(root)


if __name__ == "__main__":
    unittest.main()
