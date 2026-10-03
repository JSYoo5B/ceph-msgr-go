"""Independent OSD fixture evidence must reject incompatible or incomplete data."""

import copy
import json
import pathlib
import tempfile
import unittest
from unittest import mock

import osd_oracle


class OSDOracleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name) / "root"
        self.output = pathlib.Path(self.temporary.name) / "output"
        self.root.mkdir()
        self.output.mkdir()
        self.data = bytes(range(256)) * 41 + b"\x00\xfftail"
        (self.output / "osd-object.bin").write_bytes(self.data)
        (self.output / "osd-native-read.bin").write_bytes(self.data)
        self.mapped = {"pool": "test-pool", "pool_id": 7, "objname": "binary-object", "epoch": 23,
                       "raw_pgid": "7.f123abcd", "pgid": "7.0", "acting": [0], "up": [0]}
        self.topology = {
            "epoch": 23,
            "pools": [{"pool": 7, "pool_name": "test-pool", "size": 1, "min_size": 1,
                       "pg_num": 1, "pg_placement_num": 1, "flags": 0, "flags_names": ""}],
            "osds": [{"osd": 0, "public_addrs": {"addrvec": [
                {"type": "v2", "addr": "127.0.0.1:36900", "nonce": 4294967295}]} }],
            "pg_upmap": [], "pg_upmap_items": [],
        }
        self.crush = {"buckets": [{"alg": "straw"}], "choose_args": {},
                      "tunables": {"choose_local_tries": 2, "choose_local_fallback_tries": 5,
                                   "choose_total_tries": 19, "chooseleaf_descend_once": 0,
                                   "chooseleaf_vary_r": 0, "chooseleaf_stable": 0,
                                   "straw_calc_version": 0}}

    def collect(self, topology=None, crush=None):
        for name, value in (("osd.object-map.json", self.mapped),
                            ("osd.dump.json", self.topology if topology is None else topology),
                            ("osd.crush.json", self.crush if crush is None else crush)):
            (self.root / name).write_text(json.dumps(value), encoding="utf-8")
        with mock.patch.object(osd_oracle, "native_stat", return_value=(len(self.data), 1720000000, 765432109)):
            osd_oracle.collect(self.root, self.output, "test-pool", "binary-object")

    def test_raw_hash_nonce_and_nanoseconds_preserved(self):
        self.collect()
        value = json.loads((self.output / "osd-oracle.json").read_bytes())
        self.assertEqual(value["hash"], 0xF123ABCD)
        self.assertEqual(value["pg_seed"], 0)
        self.assertEqual(value["raw_pgid"], "7.f123abcd")
        self.assertEqual(value["address"], "v2:127.0.0.1:36900/4294967295")
        self.assertEqual(value["mtime_nanoseconds"], 765432109)
        self.assertEqual(value["size"], len(self.data))
        self.assertFalse((self.output / "osd-oracle.tmp").exists())

    def test_modern_placement_requirements_cannot_claim_legacy_fixture(self):
        candidates = []
        value = copy.deepcopy(self.topology)
        value["pools"][0]["flags_names"] = "hashpspool"
        candidates.append((value, self.crush))
        value = copy.deepcopy(self.topology)
        value["pools"][0]["flags_names"] = "creating"
        candidates.append((value, self.crush))
        value = copy.deepcopy(self.topology)
        value["pg_upmap"] = [{"pgid": "7.0", "osds": [0]}]
        candidates.append((value, self.crush))
        value = copy.deepcopy(self.crush)
        value["buckets"][0]["alg"] = "straw2"
        candidates.append((self.topology, value))
        value = copy.deepcopy(self.crush)
        value["choose_args"] = {"-1": [{"bucket_id": -1}]}
        candidates.append((self.topology, value))
        value = copy.deepcopy(self.crush)
        value["tunables"]["chooseleaf_stable"] = 1
        candidates.append((self.topology, value))
        for topology, crush in candidates:
            with self.subTest(topology=topology, crush=crush):
                with self.assertRaises(ValueError):
                    self.collect(topology, crush)
                self.assertFalse((self.output / "osd-oracle.json").exists())

    def test_independent_read_corruption_cannot_publish_oracle(self):
        (self.output / "osd-native-read.bin").write_bytes(self.data[:-1])
        with self.assertRaises(ValueError):
            self.collect()
        self.assertFalse((self.output / "osd-oracle.json").exists())
        self.assertFalse((self.output / "osd-oracle.tmp").exists())

    def test_endpoint_and_actual_primary_must_match_controlled_fixture(self):
        for field, value in (("addr", "127.0.0.1:36901"), ("type", "v1"), ("nonce", -1)):
            topology = copy.deepcopy(self.topology)
            topology["osds"][0]["public_addrs"]["addrvec"][0][field] = value
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    self.collect(topology)
                self.assertFalse((self.output / "osd-oracle.json").exists())
        self.mapped["acting"] = [1]
        with self.assertRaises(ValueError):
            self.collect()

    def test_map_change_cannot_publish_a_mixed_epoch_snapshot(self):
        self.mapped["epoch"] = 22
        with self.assertRaises(ValueError):
            self.collect()
        self.assertFalse((self.output / "osd-oracle.json").exists())


if __name__ == "__main__":
    unittest.main()
