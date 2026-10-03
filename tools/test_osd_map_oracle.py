"""Native map evidence must reject mismatched identity or encoded bytes."""

import json
import pathlib
import tempfile
import unittest
from unittest import mock

import osd_map_oracle


class OSDMapOracleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = pathlib.Path(self.temporary.name)
        self.fsid = "80bbab73-69c1-4a0c-a746-4271357750b8"
        self.data = b"\x00\xffnative-map\x80"
        self.records = [{"kind": "full", "epoch": 12}, {"kind": "incremental", "epoch": 13}]
        for record in self.records:
            (self.output / f'osd-map-{record["kind"]}-{record["epoch"]}.bin').write_bytes(self.data)

    def collect(self, identity=None, encoded=None):
        (self.output / "osd-map-manifest.json").write_text(json.dumps({"fsid": self.fsid, "blobs": self.records}), encoding="utf-8")

        def native(*arguments):
            if arguments[-1] == "dump_json":
                epoch = int(pathlib.Path(arguments[4]).stem.rsplit("-", 1)[1])
                return json.dumps({"fsid": self.fsid if identity is None else identity, "epoch": epoch}).encode()
            self.assertIn("set_features", arguments)
            # Native set_features uses signed atoll and adds RESERVED itself.
            # Supplying unsigned bit 63 would overflow and encode other features.
            feature_mask = int(arguments[arguments.index("set_features") + 1])
            self.assertEqual(feature_mask, osd_map_oracle.FEATURES)
            self.assertLess(feature_mask, 2**63)
            pathlib.Path(arguments[-1]).write_bytes(self.data if encoded is None else encoded)
            return b""

        with mock.patch.object(osd_map_oracle, "native", side_effect=native) as calls:
            osd_map_oracle.collect(self.output)
            return calls

    def test_native_identity_and_exact_feature_encoding_are_required(self):
        calls = self.collect()
        evidence = json.loads((self.output / "osd-map-native-oracle.json").read_bytes())
        self.assertTrue(evidence["native_feature_encoding_match"])
        self.assertEqual([x["epoch"] for x in evidence["blobs"]], [12, 13])
        self.assertEqual(calls.call_count, 4)
        self.assertIn("OSDMap::Incremental", calls.call_args_list[2].args)

    def test_different_cluster_cannot_publish_evidence(self):
        with self.assertRaises(ValueError):
            self.collect(identity="foreign-fsid")
        self.assertFalse((self.output / "osd-map-native-oracle.json").exists())

    def test_native_encoder_mismatch_cannot_publish_evidence(self):
        with self.assertRaises(ValueError):
            self.collect(encoded=self.data + b"different")
        self.assertFalse((self.output / "osd-map-native-oracle.json").exists())

    def test_manifest_is_bounded_before_native_execution(self):
        self.records = [{"kind": "../../escape", "epoch": 12}]
        with self.assertRaises(ValueError):
            self.collect()
        self.records = [{"kind": "full", "epoch": 0}]
        with self.assertRaises(ValueError):
            self.collect()


if __name__ == "__main__":
    unittest.main()
