"""Receipt rejection tests; synthetic artifacts never establish native parity."""
import json
from pathlib import Path
import tempfile
import unittest

import mgr_acceptor_native_encoder as oracle


class MgrAcceptorReceiptTests(unittest.TestCase):
    def model(self, root):
        for name in oracle.EXCHANGES:
            exchange = {"refusals": {case: {"authorizer": "00", "accepted": False, "challenged": False}
                                     for case in oracle.REFUSALS}}
            for mode, length in (("secure", 64), ("crc", 0)):
                exchange[mode] = {"replay_accepted": False, "connection_secret": "ab" * length}
            (root / (name + ".json")).write_text(json.dumps(exchange))
        (root / "plaintexts.json").write_text(json.dumps({name: "00" for name in oracle.PLAINTEXTS}))
        (root / "frames.json").write_text(json.dumps({name: "00" for name in oracle.FRAMES}))

    def test_closed_artifact_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.model(root)
            self.assertEqual(len(oracle.collect(root)), 4)
            (root / "foreign.json").write_text("{}")
            with self.assertRaisesRegex(RuntimeError, "artifact set"):
                oracle.collect(root)
            (root / "foreign.json").unlink()
            (root / "frames.json").unlink()
            with self.assertRaisesRegex(RuntimeError, "artifact set"):
                oracle.collect(root)

    def test_native_admission_of_an_altered_authorizer_is_rejected(self):
        for field in ("accepted", "challenged"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.model(root)
                path = root / "exchange-aes.json"
                data = json.loads(path.read_text())
                data["refusals"]["unknown_secret"][field] = True
                path.write_text(json.dumps(data))
                with self.assertRaisesRegex(RuntimeError, "admitted an altered authorizer"):
                    oracle.collect(root)

    def test_replay_and_secret_length_are_checked(self):
        for mutate in (lambda d: d["secure"].update(replay_accepted=True),
                       lambda d: d["secure"].update(connection_secret="ab" * 40),
                       lambda d: d["crc"].update(connection_secret="ab")):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.model(root)
                path = root / "exchange-aes256k.json"
                data = json.loads(path.read_text())
                mutate(data)
                path.write_text(json.dumps(data))
                with self.assertRaisesRegex(RuntimeError, "exchange differs"):
                    oracle.collect(root)

    def test_missing_refusal_plaintext_or_frame_is_rejected(self):
        for name, key in (("exchange-aes.json", "refusals"), ("plaintexts.json", None), ("frames.json", None)):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.model(root)
                path = root / name
                data = json.loads(path.read_text())
                if key:
                    data[key].pop("global_id")
                else:
                    data.pop(sorted(data)[0])
                path.write_text(json.dumps(data))
                with self.assertRaisesRegex(RuntimeError, "set differs"):
                    oracle.collect(root)


if __name__ == "__main__":
    unittest.main()
