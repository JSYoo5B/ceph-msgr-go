"""Independent OSD fixture evidence must reject incompatible or incomplete data."""

import copy
import json
import os
import pathlib
import tempfile
import unittest
from unittest import mock

import osd_oracle


class OSDOracleTests(unittest.TestCase):
    def setUp(self):
        profile = mock.patch.dict(os.environ, {"CEPH_MSGR_TEST_OSD_REPLICAS": "1"})
        profile.start()
        self.addCleanup(profile.stop)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name) / "root"
        self.output = pathlib.Path(self.temporary.name) / "output"
        self.root.mkdir()
        self.output.mkdir()
        (self.output / "peers.json").write_text(json.dumps({"osd.0":"127.0.0.1:36900"}))
        self.data = bytes(range(256)) * 41 + b"\x00\xfftail"
        (self.output / "osd-object.bin").write_bytes(self.data)
        (self.output / "osd-native-read.bin").write_bytes(self.data)
        self.mapped = {"pool": "test-pool", "pool_id": 7, "objname": "binary-object", "epoch": 23,
                       "raw_pgid": "7.f123abcd", "pgid": "7.0", "acting": [0], "up": [0], "acting_primary": 0}
        self.topology = {
            "epoch": 23,
            "pools": [{"pool": 7, "pool_name": "test-pool", "size": 1, "min_size": 1,
                       "pg_num": 1, "pg_placement_num": 1, "flags": 0, "flags_names": ""}],
            "osds": [{"osd": 0, "public_addrs": {"addrvec": [
                {"type": "v2", "addr": "127.0.0.1:36900", "nonce": 4294967295}]} }],
            "pg_upmap": [], "pg_upmap_items": [],
        }
        self.crush = {"devices": [{"id": 0}], "buckets": [{"alg": "straw"}], "choose_args": {},
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
        self.assertEqual((value["osd_id"], value["acting"], value["up"], value["replicas"], value["min_size"]), (0, [0], [0], 1, 1))
        self.assertFalse((self.output / "osd-oracle.tmp").exists())

    def test_modern_placement_requirements_cannot_claim_legacy_fixture(self):
        candidates = []
        value = copy.deepcopy(self.topology)
        value["pools"].append({"pool": 1, "pool_name": ".mgr", "flags_names": "hashpspool"})
        candidates.append((value, self.crush))
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

    def replicated(self):
        os.environ["CEPH_MSGR_TEST_OSD_REPLICAS"] = "3"
        self.mapped.update(acting=[2, 0, 1], up=[2, 0, 1], acting_primary=2)
        self.topology["pools"][0].update(size=3, min_size=2)
        self.topology["osds"] = [
            {"osd": i, "public_addrs": {"addrvec": [
                {"type": "v2", "addr": "127.0.0.1:" + str(36900+i), "nonce": 100+i}]}}
            for i in range(3)]
        self.crush["devices"] = [{"id": i} for i in range(3)]
        (self.output / "peers.json").write_text(json.dumps({"osd." + str(i): "127.0.0.1:" + str(36900+i) for i in range(3)}))

    def test_three_replicas_use_actual_primary_endpoint(self):
        self.replicated()
        self.collect()
        value = json.loads((self.output / "osd-oracle.json").read_bytes())
        self.assertEqual((value["replicas"], value["min_size"]), (3, 2))
        self.assertEqual(value["osd_id"], 2)
        self.assertEqual(value["acting"], [2, 0, 1])
        self.assertEqual(value["up"], [2, 0, 1])
        self.assertEqual(value["address"], "v2:127.0.0.1:36902/102")

    def test_replica_topology_must_be_complete_and_unique(self):
        self.replicated()
        for field, bad in (("acting", [2, 0]), ("acting", [2, 0, 0]), ("acting", [2, 0, 3]),
                           ("up", [2, 0, True]), ("up", [2, 0]), ("acting_primary", 3),
                           ("acting_primary", True)):
            previous = self.mapped[field]
            self.mapped[field] = bad
            with self.subTest(field=field, bad=bad), self.assertRaises(ValueError):
                self.collect()
            self.mapped[field] = previous
            self.assertFalse((self.output / "osd-oracle.json").exists())
        for size, minimum in ((1, 1), (3, 1), (3, 3), (2, 2)):
            self.topology["pools"][0].update(size=size, min_size=minimum)
            with self.subTest(size=size, minimum=minimum), self.assertRaises(ValueError):
                self.collect()
        self.topology["pools"][0].update(size=3, min_size=2)
        self.crush["devices"][2]["id"] = 0
        with self.assertRaises(ValueError):
            self.collect()

    def test_replica_primary_endpoint_cannot_use_another_daemon(self):
        self.replicated()
        self.topology["osds"][2]["public_addrs"]["addrvec"][0]["addr"] = "127.0.0.1:36900"
        with self.assertRaises(ValueError):
            self.collect()

    def test_unknown_replica_profiles_are_rejected(self):
        for value in ("", "0", "2", "03", "4"):
            with self.subTest(value=value), mock.patch.dict(os.environ, {"CEPH_MSGR_TEST_OSD_REPLICAS": value}), self.assertRaises(ValueError):
                self.collect()


if __name__ == "__main__":
    unittest.main()
