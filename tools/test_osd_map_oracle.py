"""Native map evidence must reject mismatched identity or encoded bytes."""

import json
import pathlib
import subprocess
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

    def collect(self, identity=None, encoded=None, source=..., decoded_epoch=None):
        manifest = {"fsid": self.fsid, "blobs": self.records}
        if source is not ...:
            manifest["source"] = source
        (self.output / "osd-map-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

        def native(*arguments):
            if arguments[-1] == "dump_json":
                epoch = int(pathlib.Path(arguments[4]).stem.rsplit("-", 1)[1])
                return json.dumps({"fsid": self.fsid if identity is None else identity,
                                   "epoch": epoch if decoded_epoch is None else decoded_epoch}).encode()
            self.assertIn("set_features", arguments)
            # Native set_features uses signed atoll and adds RESERVED itself.
            # Supplying unsigned bit 63 would overflow and encode other features.
            feature_mask = int(arguments[arguments.index("set_features") + 1])
            # Independent fixed peer-policy values, not a dynamic feature input.
            expected = 3314937398101836342 if source == "mon" else 3026423347916342070
            self.assertEqual(feature_mask, expected)
            self.assertLess(feature_mask, 2**63)
            self.assertEqual(feature_mask & ((1 << 62) | (1 << 63)), 0)
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
        self.assertEqual(set(evidence), {"fsid", "blobs", "native_feature_encoding_match"})

    def test_explicit_mon_mask_validates_both_native_classes(self):
        calls = self.collect(source="mon")
        evidence = json.loads((self.output / "osd-map-native-oracle.json").read_bytes())
        self.assertEqual(evidence["source"], "mon")
        self.assertTrue(evidence["native_feature_encoding_match"])
        self.assertEqual([call.args[2] for call in calls.call_args_list],
                         ["OSDMap", "OSDMap", "OSDMap::Incremental", "OSDMap::Incremental"])
        self.assertTrue(osd_map_oracle.MON_FEATURES & (1 << 9))
        self.assertEqual(osd_map_oracle.MON_FEATURES & sum(1 << b for b in (8, 10, 11, 39)), 0)

    def test_native_failure_exposes_only_bounded_textual_stderr(self):
        error = subprocess.CalledProcessError(1, ["private-argv"],
                                             output=b"private-output-map-bytes",
                                             stderr=b"buffer::end_of_buffer: decode failed\n" + b"x" * 8192)
        with mock.patch.object(osd_map_oracle.subprocess, "run", side_effect=error):
            with self.assertRaises(ValueError) as raised:
                osd_map_oracle.native("ceph-dencoder", "type", "OSDMap", "decode")
        text = str(raised.exception)
        self.assertIn("exited with code 1", text)
        self.assertIn("buffer::end_of_buffer", text)
        self.assertIn("[truncated]", text)
        self.assertNotIn("private-argv", text)
        self.assertNotIn("private-output-map-bytes", text)
        self.assertLess(len(text), 4200)

    def test_native_failure_omits_raw_binary_stderr(self):
        for stderr in [b"\x00raw map bytes", b"\xffraw map bytes"]:
            with self.subTest(stderr=stderr):
                error = subprocess.CalledProcessError(1, ["ceph-dencoder"], stderr=stderr)
                with mock.patch.object(osd_map_oracle.subprocess, "run", side_effect=error):
                    with self.assertRaises(ValueError) as raised:
                        osd_map_oracle.native("ceph-dencoder")
                self.assertIn("non-text native diagnostic omitted", str(raised.exception))
                self.assertNotIn("raw map bytes", str(raised.exception))

    def test_explicit_osd_source_keeps_existing_peer_mask(self):
        self.collect(source="osd")
        evidence = json.loads((self.output / "osd-map-native-oracle.json").read_bytes())
        self.assertEqual(evidence["source"], "osd")

    def test_source_cannot_supply_arbitrary_peer_features(self):
        for source in ["", "mgr", "MON", None, 0, [], {"features": 2**63 - 1}]:
            with self.subTest(source=source):
                manifest = {"fsid": self.fsid, "source": source, "blobs": self.records}
                (self.output / "osd-map-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
                with mock.patch.object(osd_map_oracle, "native") as calls:
                    with self.assertRaises(ValueError):
                        osd_map_oracle.collect(self.output)
                    calls.assert_not_called()
                self.assertFalse((self.output / "osd-map-native-oracle.json").exists())

    def test_different_cluster_cannot_publish_evidence(self):
        with self.assertRaises(ValueError):
            self.collect(identity="foreign-fsid")
        self.assertFalse((self.output / "osd-map-native-oracle.json").exists())

    def test_native_encoder_mismatch_cannot_publish_evidence(self):
        with self.assertRaises(ValueError):
            self.collect(encoded=self.data + b"different")
        self.assertFalse((self.output / "osd-map-native-oracle.json").exists())

    def test_native_epoch_mismatch_cannot_publish_evidence(self):
        with self.assertRaises(ValueError):
            self.collect(source="mon", decoded_epoch=999)
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
