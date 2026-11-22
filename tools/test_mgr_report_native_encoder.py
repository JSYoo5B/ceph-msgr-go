"""Receipt rejection tests; synthetic artifacts never establish native parity."""
import json
from pathlib import Path
import tempfile
import unittest

import mgr_report_native_encoder as oracle


class MgrReportReceiptTests(unittest.TestCase):
    def model(self, root):
        data = {"report-populated": b"digest-a-bytes", "report-plain": b"digest-empty-bytes"}
        for name, (typ, version, compat) in oracle.MESSAGES.items():
            header = oracle.HEADER.pack(1, 0, typ, 127, version, 0, 0, 3, 1, compat, 0)
            for part, value in {"header": header, "front": b"synthetic-rejection-model",
                                "middle": b"", "data": data[name]}.items():
                (root / (name + "." + part + ".bin")).write_bytes(value)
            (root / (name + ".decoded.json")).write_text(json.dumps({"synthetic": True}))
        carried = {"digest-a": b"digest-a-bytes", "digest-empty": b"digest-empty-bytes"}
        for name, kind in oracle.STRUCTURES.items():
            (root / (name + "." + kind + ".bin")).write_bytes(carried.get(name, b"structure"))
            (root / (name + ".decoded.json")).write_text(json.dumps({"synthetic": True}))
        for name in oracle.REFUSED:
            (root / (name + ".digest.bin")).write_bytes(b"refused")
            (root / (name + ".decoded.json")).write_text(json.dumps({"native_refused": True}))

    def test_closed_artifact_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.model(root)
            self.assertEqual(len(oracle.collect(root)), 8)
            extra = root / "foreign.digest.bin"
            extra.write_bytes(b"foreign")
            with self.assertRaisesRegex(RuntimeError, "artifact set"):
                oracle.collect(root)
            extra.unlink()
            (root / "digest-b.digest.bin").unlink()
            with self.assertRaisesRegex(RuntimeError, "artifact set"):
                oracle.collect(root)

    def test_foreign_headers_and_segments_rejected(self):
        for part, offset in (("header", 8), ("header", 16), ("header", 18), ("header", 20), ("header", 37),
                             ("middle", None), ("data", None), ("front", None)):
            with self.subTest(part=part, offset=offset), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.model(root)
                path = root / ("report-populated." + part + ".bin")
                value = bytearray(path.read_bytes())
                if offset is not None:
                    value[offset] ^= 1
                elif part == "middle":
                    value = b"foreign"
                else:
                    value = b""
                path.write_bytes(value)
                with self.assertRaisesRegex(RuntimeError, "envelope"):
                    oracle.collect(root)

    def test_report_must_carry_its_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.model(root)
            (root / "report-plain.data.bin").write_bytes(b"another digest")
            with self.assertRaisesRegex(RuntimeError, "data segment"):
                oracle.collect(root)

    def test_refusal_receipt_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.model(root)
            (root / "digest-truncated.decoded.json").write_text(json.dumps({"native_refused": False}))
            with self.assertRaisesRegex(RuntimeError, "refusal"):
                oracle.collect(root)

    def test_refused_structure_is_not_a_vector(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.model(root)
            (root / "digest-b.decoded.json").write_text(json.dumps({"native_refused": True}))
            with self.assertRaisesRegex(RuntimeError, "structure"):
                oracle.collect(root)


if __name__ == "__main__":
    unittest.main()
