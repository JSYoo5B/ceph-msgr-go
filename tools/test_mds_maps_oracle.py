import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import mds_maps_oracle as oracle


class FakeNative(oracle.Native):
    def __init__(self, root, out, state, world):
        super().__init__(root, out, state)
        self.world = world

    def command(self, words, *, json_output=True):
        world = self.world
        if words == ["fsid"]:
            return world["fsid"]
        if words == ["fs", "dump"]:
            return copy.deepcopy(world["dump"])
        if words == ["osd", "dump"]:
            return dict(fsid=world["fsid"], pools=copy.deepcopy(world["pools"]))
        if words[:2] == ["fs", "get"]:
            return copy.deepcopy(world["dump"]["filesystems"][0])
        if words == ["config", "get", "mon", "mon_allow_pool_delete"]:
            return world["allow_pool_delete"]
        world["mutations"].append(words)
        if world.get("raise_mutation"):
            raise RuntimeError("uncertain send result")
        fault = world.get("fault_words") == words
        if fault and not world.get("fault_after_effect"):
            raise subprocess.TimeoutExpired(words, 20)
        if words[:3] == ["osd", "pool", "create"]:
            world["pools"].append(dict(pool=world["next_id"], pool_name=words[3]))
            world["next_id"] += 1
        elif words[:2] == ["fs", "new"]:
            world["dump"]["epoch"] += 1
            world["dump"]["default_fscid"] = 7
            world["dump"]["filesystems"] = [dict(id=7, mdsmap=dict(fs_name=words[2], epoch=world["dump"]["epoch"],
                enabled=True, info={}, up={}, **{"in": []}, failed=[], stopped=[], damaged=[],
                metadata_pool=self.state["pool_ids"]["metadata"], data_pools=[self.state["pool_ids"]["data"]]))]
        elif words[:2] == ["fs", "rm"]:
            world["dump"]["filesystems"] = []
            world["dump"]["default_fscid"] = -1
            world["dump"]["epoch"] += 1
        elif words[:3] == ["osd", "pool", "delete"]:
            world["pools"] = [p for p in world["pools"] if p["pool_name"] != words[3]]
        else:
            raise AssertionError("unexpected native mutation " + repr(words))
        if fault and world.get("fault_after_effect"):
            raise subprocess.TimeoutExpired(words, 20)
        return ""


class MDSMapOracleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = Path(self.tmp.name)
        self.root = self.out / "root"
        self.root.mkdir()
        self.fsid = "00112233-4455-6677-8899-aabbccddeeff"
        self.write("fixture-manifest", dict(osd="1", osd_replicas="1", osd_profile="replicated-pg-v8", fsid=self.fsid))
        self.write("osd-oracle", dict(profile="replicated-pg-v8", replicas=1))
        self.write("fixture-ownership", dict(containers={"osd.0": "a" * 64, "mon.a": "b" * 64}))
        (self.out / "daemon-version").write_text("ceph version 20.2.4 " + oracle.xattr.SOURCE_COMMIT)
        _, digest = oracle.fixture_identity(self.out)
        self.state = dict(schema_version=1, source_commit=oracle.xattr.SOURCE_COMMIT, token="a" * 32,
                          fsid=self.fsid, fixture_ownership_sha256=digest, names=oracle.names("a" * 32),
                          pool_ids=dict(metadata=None, data=None), fscid=None, pending=None, finished=False, commands=[])
        oracle.save(self.out, self.state)
        self.world = dict(fsid=self.fsid, dump=dict(epoch=1, default_fscid=-1, filesystems=[], standbys=[]),
                          pools=[dict(pool=2, pool_name="existing-fixture")], next_id=3, allow_pool_delete="false", mutations=[])
        self.native = FakeNative(self.root, self.out, self.state, self.world)

    def write(self, name, data):
        (self.out / (name + ".json")).write_text(json.dumps(data))

    def test_closed_input_uuid_scope_and_fixture_gate(self):
        oracle.check_input({})
        for raw in ({"name": "foreign"}, [], None):
            with self.assertRaises(ValueError):
                oracle.check_input(raw)
        for token in ("", "../foreign", "g" * 32):
            with self.assertRaises(ValueError):
                oracle.names(token)
        self.write("fixture-manifest", dict(osd="1", osd_replicas="3", osd_profile="replicated-pg-v8", fsid=self.fsid))
        with self.assertRaises(ValueError):
            oracle.fixture_identity(self.out)

    def test_only_matching_explicit_native_profiles_admit_mutating_scope(self):
        for mode in oracle.PROFILES:
            self.write("fixture-manifest", dict(osd="1", osd_replicas="1", osd_profile=mode, fsid=self.fsid))
            self.write("osd-oracle", dict(profile=mode, replicas=1))
            self.assertEqual(oracle.fixture_identity(self.out)[0], self.fsid)
        for selected, actual in (("replicated-pg-v8", "replicated-pg-reply-v8"),
                                 ("replicated-pg-reply-v8", "replicated-pg-v8"),
                                 ("reply-v8", "reply-v8"), ("legacy-v6", "legacy-v6")):
            self.write("fixture-manifest", dict(osd="1", osd_replicas="1", osd_profile=selected, fsid=self.fsid))
            self.write("osd-oracle", dict(profile=actual, replicas=1))
            with self.subTest(selected=selected, actual=actual), self.assertRaises(ValueError):
                oracle.start(self.native)
            self.assertEqual(self.world["mutations"], [])

    def test_owned_start_empty_native_maps_and_readonly_observation(self):
        result = oracle.start(self.native)
        self.assertEqual((result["fsid"], result["fscid"], result["fsmap_epoch"], result["mdsmap_epoch"]), (self.fsid, 7, 2, 2))
        self.assertEqual((result["metadata_pool"], result["data_pools"]), (3, [4]))
        self.assertEqual(len(self.world["mutations"]), 3)
        before = copy.deepcopy(self.world)
        self.assertEqual(oracle.observe(self.native), result)
        self.assertEqual(self.world, before)
        with self.assertRaises(ValueError):
            oracle.start(self.native)

    def test_cleanup_removes_only_confirmed_fs_and_reports_disabled_pool_deletion(self):
        oracle.start(self.native)
        result = oracle.finish(self.native)
        self.assertTrue(result["finished"])
        self.assertEqual(len(result["cleanup"]["deleted"]), 1)
        self.assertEqual(len(result["cleanup"]["retained_for_fixture_teardown"]), 2)
        self.assertEqual(result["fs_dump"]["filesystems"], [])
        self.assertEqual(result["default_fscid"], -1)
        self.assertFalse(any(words[:2] == ["config", "set"] for words in self.world["mutations"]))

    def test_existing_enabled_pool_deletion_cleans_owned_ids_only(self):
        oracle.start(self.native)
        self.world["allow_pool_delete"] = "true"
        result = oracle.finish(self.native)
        self.assertEqual(len(result["cleanup"]["deleted"]), 3)
        self.assertEqual(result["cleanup"]["retained_for_fixture_teardown"], [])
        self.assertEqual(self.world["pools"], [dict(pool=2, pool_name="existing-fixture")])
        self.assertEqual(self.state["removed_pools"], ["data", "metadata"])

    def test_foreign_filesystem_or_standby_prevents_all_mutations(self):
        for field, value in (("standbys", [{"gid": 10}]), ("filesystems", [{"id": 1, "mdsmap": {"fs_name": "foreign"}}])):
            with self.subTest(field=field):
                self.world["dump"][field] = value
                with self.assertRaises(ValueError):
                    oracle.start(self.native)
                self.assertEqual(self.world["mutations"], [])
                self.world["dump"][field] = []

    def test_replaced_confirmed_pool_stops_cleanup_before_mutations(self):
        oracle.start(self.native)
        before = len(self.world["mutations"])
        self.world["pools"][-1]["pool"] = 99
        with self.assertRaises(ValueError):
            oracle.finish(self.native)
        self.assertEqual(len(self.world["mutations"]), before)

    def test_daemon_acquisition_prevents_fs_remove(self):
        oracle.start(self.native)
        self.world["dump"]["filesystems"][0]["mdsmap"]["up"] = {"mds_0": 50}
        before = len(self.world["mutations"])
        with self.assertRaises(ValueError):
            oracle.finish(self.native)
        self.assertEqual(len(self.world["mutations"]), before)

    def test_uncertain_create_is_journaled_and_not_replayed(self):
        self.world["raise_mutation"] = True
        with self.assertRaises(RuntimeError):
            oracle.start(self.native)
        self.assertEqual(oracle.load_owned(self.out)["pending"], "create_metadata")
        self.world["raise_mutation"] = False
        with self.assertRaises(ValueError):
            oracle.start(self.native)
        self.assertEqual(len(self.world["mutations"]), 1)
        result = oracle.finish(self.native)
        self.assertEqual(result["unresolved_mutation"], "create_metadata")
        self.assertEqual(len(self.world["mutations"]), 1)

    def test_journal_or_live_fsid_change_stops_mutations(self):
        before = len(self.world["mutations"])
        self.world["fsid"] = "ffffffff-ffff-ffff-ffff-ffffffffffff"
        with self.assertRaises(ValueError):
            oracle.start(self.native)
        self.assertEqual(len(self.world["mutations"]), before)
        self.world["fsid"] = self.fsid
        bad = copy.deepcopy(self.state); bad["token"] = "b" * 32
        oracle.save(self.out, bad)
        with self.assertRaises(ValueError):
            oracle.start(self.native)

    def test_native_timeout_receipt_preserves_bounded_error_and_pending(self):
        self.state["pending"] = "create_metadata"
        oracle.save(self.out, self.state)
        native = oracle.Native(self.root, self.out, self.state)
        error = subprocess.TimeoutExpired(["ceph"], 20, output=b"", stderr=b"timeout-details")
        with patch.object(oracle.subprocess, "run", side_effect=error), self.assertRaises(RuntimeError):
            native.command(["osd", "pool", "create", self.state["names"]["metadata"], "16"])
        self.assertEqual(oracle.load_owned(self.out)["pending"], "create_metadata")
        self.assertTrue(self.state["commands"][-1]["timed_out"])
        self.assertIn("timeout-details", self.state["commands"][-1]["stderr"])

    def test_control_rejects_existing_scope_and_cleans_lock(self):
        self.write("mds-maps-input", {})
        with self.assertRaises(ValueError):
            oracle.run("start", self.root, self.out)
        self.assertFalse((self.out / "mds-maps-control.lock").exists())

    def test_uncertain_fs_creation_never_adopts_observed_fscid_for_cleanup(self):
        oracle.start(self.native)
        self.state["fscid"] = None
        self.state["pending"] = "create_fs"
        oracle.save(self.out, self.state)
        self.world["allow_pool_delete"] = "true"
        before = len(self.world["mutations"])
        result = oracle.finish(self.native)
        self.assertEqual(len(self.world["mutations"]), before)
        self.assertEqual(len(result["cleanup"]["retained_for_fixture_teardown"]), 3)
        self.assertEqual(result["unresolved_mutation"], "create_fs")

    def test_owned_name_or_id_disappearance_cannot_be_cleaned_as_another_pool(self):
        oracle.start(self.native)
        self.world["pools"] = [p for p in self.world["pools"] if p["pool"] != 4]
        before = len(self.world["mutations"])
        with self.assertRaises(ValueError):
            oracle.finish(self.native)
        self.assertEqual(len(self.world["mutations"]), before)

    def test_uncertain_removals_are_never_replayed_before_or_after_effect(self):
        for role in ("fs", "data", "metadata"):
            for after_effect in (False, True):
                with self.subTest(role=role, after_effect=after_effect):
                    # Each scenario owns a fresh private lifecycle.
                    with tempfile.TemporaryDirectory() as temporary:
                        old_out, old_state, old_world, old_native = self.out, self.state, self.world, self.native
                        self.out = Path(temporary)
                        for name in ("fixture-manifest.json", "fixture-ownership.json", "daemon-version", "osd-oracle.json"):
                            (self.out / name).write_bytes((old_out / name).read_bytes())
                        self.state = copy.deepcopy(old_state)
                        self.world = copy.deepcopy(old_world)
                        oracle.save(self.out, self.state)
                        self.native = FakeNative(self.root, self.out, self.state, self.world)
                        oracle.start(self.native)
                        self.world["allow_pool_delete"] = "true"
                        name = self.state["names"][role]
                        words = ["fs", "rm", name, "--yes-i-really-mean-it"] if role == "fs" else ["osd", "pool", "delete", name, name, "--yes-i-really-really-mean-it"]
                        self.world.update(fault_words=words, fault_after_effect=after_effect)
                        with self.assertRaises(subprocess.TimeoutExpired):
                            oracle.finish(self.native)
                        stage = "remove_" + role
                        self.assertEqual(oracle.load_owned(self.out)["pending"], stage)
                        count = self.world["mutations"].count(words)
                        self.assertEqual(count, 1)
                        del self.world["fault_words"]
                        result = oracle.finish(self.native)
                        self.assertEqual(self.world["mutations"].count(words), count)
                        self.assertEqual(result["unresolved_mutation"], stage)
                        if after_effect:
                            self.assertIn(dict(mutation=stage, observed_absent=True), result["reconciled_absences"])
                        else:
                            self.assertEqual(self.state["pending"], stage)
                            self.assertTrue(result["cleanup"]["retained_for_fixture_teardown"])
                        for _ in range(3):
                            self.assertEqual(oracle.finish(self.native), result)
                        self.write("mds-maps-input", {})
                        with patch.object(oracle.subprocess, "run", side_effect=AssertionError("finished scope issued native command")):
                            oracle.run("finish", self.root, self.out)
                            oracle.run("finish", self.root, self.out)
                        self.assertEqual(oracle.omap.read_json(self.out / "mds-maps-result.json"), result)
                        self.assertEqual(self.world["mutations"].count(words), count)
                        self.out, self.state, self.world, self.native = old_out, old_state, old_world, old_native


if __name__ == "__main__":
    unittest.main()
