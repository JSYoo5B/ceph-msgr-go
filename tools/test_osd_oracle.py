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
        profile = mock.patch.dict(os.environ, {"CEPH_MSGR_TEST_OSD_REPLICAS": "1", "CEPH_MSGR_TEST_IP_FAMILY": "4", "CEPH_MSGR_TEST_OSD_PROFILE": "legacy-v6"})
        profile.start()
        self.addCleanup(profile.stop)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name) / "root"
        self.output = pathlib.Path(self.temporary.name) / "output"
        self.root.mkdir()
        self.output.mkdir()
        self.manifest = dict(osd_profile="legacy-v6")
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

    def collect(self, topology=None, crush=None, write_manifest=True):
        if write_manifest:
            (self.output / "fixture-manifest.json").write_text(json.dumps(self.manifest))
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

    def pg_v8(self):
        os.environ["CEPH_MSGR_TEST_OSD_PROFILE"] = "replicated-pg-v8"
        self.manifest["osd_profile"] = "replicated-pg-v8"
        self.topology["pools"][0].update(pg_num=8, pg_placement_num=8, flags=1, flags_names="hashpspool")
        self.mapped["pgid"] = "7.5"

    def test_v8_profile_preserves_actual_pg_and_hashpspool_without_crush_expansion(self):
        self.pg_v8()
        self.topology["pg_upmap"] = [{"pgid": "7.5", "osds": [0]}]
        self.topology["pg_upmap_items"] = [{"pgid": "7.5", "mappings": []}]
        self.collect()
        value = json.loads((self.output / "osd-oracle.json").read_bytes())
        self.assertEqual((value["profile"], value["request_version"], value["pool_pg_num"]), ("replicated-pg-v8", 8, 8))
        self.assertEqual((value["hash"], value["pg_seed"], value["pgid"]), (0xF123ABCD, 5, "7.5"))
        self.assertEqual(value["pool_flags"], 1)
        self.assertEqual(value["pg_upmap"], self.topology["pg_upmap"])

    def test_reply_v8_profile_preserves_exact_name_and_same_native_topology(self):
        self.pg_v8()
        os.environ["CEPH_MSGR_TEST_OSD_PROFILE"] = "replicated-pg-reply-v8"
        self.manifest["osd_profile"] = "replicated-pg-reply-v8"
        self.collect()
        value = json.loads((self.output / "osd-oracle.json").read_bytes())
        self.assertEqual((value["profile"], value["request_version"], value["pool_pg_num"]), ("replicated-pg-reply-v8", 8, 8))
        self.assertEqual((value["hash"], value["pg_seed"], value["pool_flags"]), (0xF123ABCD, 5, 1))
        self.assertNotIn("reply_version", value)  # CLI metadata does not observe a reply wire header.

    def test_request_v9_profile_preserves_explicit_versions_and_fixed_topology(self):
        self.pg_v8()
        os.environ["CEPH_MSGR_TEST_OSD_PROFILE"] = "replicated-pg-request-v9-reply-v8"
        self.manifest["osd_profile"] = "replicated-pg-request-v9-reply-v8"
        self.collect()
        value = json.loads((self.output / "osd-oracle.json").read_bytes())
        self.assertEqual((value["profile"], value["request_version"], value["pool_pg_num"]), ("replicated-pg-request-v9-reply-v8", 9, 8))
        self.assertNotIn("reply_version", value) # CLI metadata is not observed wire evidence.

    def test_read_only_bootstrap_precedes_manifest_for_each_closed_profile(self):
        # Production order: provider native startup -> baseline oracle -> Go
        # prepareEvidence writes fixture-manifest.json -> mutating controls.
        for profile in ("legacy-v6", "replicated-pg-v8", "replicated-pg-reply-v8", "replicated-pg-request-v9-reply-v8"):
            with self.subTest(profile=profile):
                os.environ["CEPH_MSGR_TEST_OSD_PROFILE"] = profile
                modern = profile != "legacy-v6"
                self.topology["pools"][0].update(pg_num=8 if modern else 1, pg_placement_num=8 if modern else 1,
                                                flags=1 if modern else 0, flags_names="hashpspool" if modern else "")
                self.mapped["pgid"] = "7.5" if modern else "7.0"
                self.assertFalse((self.output / "fixture-manifest.json").exists())
                self.collect(write_manifest=False)
                value = json.loads((self.output / "osd-oracle.json").read_bytes())
                self.assertEqual(value["profile"], profile)
                self.assertEqual(value["pool_pg_num"], 8 if modern else 1)
                self.assertFalse((self.output / "fixture-manifest.json").exists())

    def test_manifest_absence_does_not_admit_unknown_profile_or_topology(self):
        os.environ["CEPH_MSGR_TEST_OSD_PROFILE"] = "reply-v8"
        with self.assertRaises(ValueError):
            self.collect(write_manifest=False)
        os.environ["CEPH_MSGR_TEST_OSD_PROFILE"] = "replicated-pg-reply-v8"
        # A modern selection still refuses the default one-PG legacy topology.
        with self.assertRaises(ValueError):
            self.collect(write_manifest=False)
        self.assertFalse((self.output / "osd-oracle.json").exists())

    def test_native_profile_cannot_differ_from_manifest_or_use_alias(self):
        self.pg_v8()
        for selected, manifest in (("replicated-pg-v8", "replicated-pg-reply-v8"),
                                   ("replicated-pg-reply-v8", "replicated-pg-v8"),
                                   ("replicated-pg-reply-v8", "reply-v8"),
                                   ("reply-v8", "reply-v8")):
            with self.subTest(selected=selected, manifest=manifest):
                os.environ["CEPH_MSGR_TEST_OSD_PROFILE"] = selected
                self.manifest["osd_profile"] = manifest
                with self.assertRaises(ValueError):
                    self.collect()
                self.assertFalse((self.output / "osd-oracle.json").exists())

    def test_v8_does_not_admit_new_crush_or_primary_requirements(self):
        self.pg_v8()
        cases = []
        topology = copy.deepcopy(self.topology)
        topology["pg_upmap_primaries"] = [{"pgid": "7.5", "primary_osd": 0}]
        cases.append((topology, self.crush))
        topology = copy.deepcopy(self.topology)
        topology["pools"][0]["flags"] = 0
        cases.append((topology, self.crush))
        topology = copy.deepcopy(self.topology)
        topology["pools"][0]["flags_names"] = ""
        cases.append((topology, self.crush))
        topology = copy.deepcopy(self.topology)
        topology["pools"][0]["pg_num"] = 1
        cases.append((topology, self.crush))
        crush = copy.deepcopy(self.crush)
        crush["buckets"][0]["alg"] = "straw2"
        cases.append((self.topology, crush))
        crush = copy.deepcopy(self.crush)
        crush["choose_args"] = {"-1": [{"bucket_id": -1}]}
        cases.append((self.topology, crush))
        for topology, crush in cases:
            with self.subTest(topology=topology, crush=crush), self.assertRaises(ValueError):
                self.collect(topology, crush)

    def test_v8_actual_pg_is_bounded_by_native_pool_count(self):
        self.pg_v8()
        self.mapped["pgid"] = "7.8"
        with self.assertRaises(ValueError):
            self.collect()

    def test_unknown_osd_profile_rejected(self):
        with mock.patch.dict(os.environ, {"CEPH_MSGR_TEST_OSD_PROFILE": "automatic"}), self.assertRaises(ValueError):
            self.collect()

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

    def ipv6(self):
        os.environ["CEPH_MSGR_TEST_IP_FAMILY"] = "6"
        peers = {}
        for daemon in self.topology["osds"]:
            endpoint = f"[fd00:ce01::{daemon['osd'] + 10}]:6800"
            daemon["public_addrs"]["addrvec"][0]["addr"] = endpoint
            peers["osd." + str(daemon["osd"])] = endpoint
        (self.output / "peers.json").write_text(json.dumps(peers))

    def test_ipv6_primary_nonce_and_replica_addresses_preserved(self):
        self.replicated()
        self.ipv6()
        self.collect()
        value = json.loads((self.output / "osd-oracle.json").read_bytes())
        self.assertEqual(value["ip_family"], 6)
        self.assertEqual(value["osd_id"], 2)
        self.assertEqual(value["address"], "v2:[fd00:ce01::12]:6800/102")
        self.assertEqual(value["endpoint"]["addr"], "[fd00:ce01::12]:6800")

    def test_ipv6_profile_rejects_ipv4_replica_even_when_primary_is_ipv6(self):
        self.replicated()
        self.ipv6()
        bad = "127.0.0.1:36900"
        self.topology["osds"][0]["public_addrs"]["addrvec"][0]["addr"] = bad
        peers = json.loads((self.output / "peers.json").read_bytes())
        peers["osd.0"] = bad
        (self.output / "peers.json").write_text(json.dumps(peers))
        with self.assertRaises(ValueError):
            self.collect()
        self.assertFalse((self.output / "osd-oracle.json").exists())

    def test_osd_endpoint_family_and_wire_syntax_are_strict(self):
        for family, endpoints in (
                (4, ("[::1]:6800", "[127.0.0.1]:6800", "fixture-osd:6800", "127.0.0.1:0", "0.0.0.0:6800")),
                (6, ("127.0.0.1:6800", "::1:6800", "[::ffff:127.0.0.1]:6800", "[::]:6800", "[ff02::1]:6800", "[fe80::1%eth0]:6800", "[::1]:65536"))):
            for endpoint in endpoints:
                with self.subTest(family=family, endpoint=endpoint), self.assertRaises(ValueError):
                    osd_oracle.validate_endpoint(endpoint, family)

    def test_unknown_ip_family_cannot_publish_oracle(self):
        with mock.patch.dict(os.environ, {"CEPH_MSGR_TEST_IP_FAMILY": "dual"}), self.assertRaises(ValueError):
            self.collect()


if __name__ == "__main__":
    unittest.main()
