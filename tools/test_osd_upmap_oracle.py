#!/usr/bin/env python3
"""Fault-injected native-command model; this is not daemon interoperability."""
import base64
import copy
from contextlib import nullcontext
import os
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import osd_upmap_oracle as oracle

FSID = "12345678-1234-1234-1234-123456789abc"


def pool(number=7, name="ceph-msgr-osd-fixture", replicas=3):
    return dict(pool=number, pool_name=name, type=1, size=replicas, min_size=2,
                flags=1, crush_rule=0, pg_autoscale_mode="off", pg_num=8,
                pg_num_target=8, pg_num_pending=8, pg_placement_num=8, pg_placement_num_target=8)


def object_state(data=b"baseline", version=3):
    return dict(code=0, version=version, data=base64.b64encode(data).decode(),
                seconds=1, nanos=2, attributes=[], gets=[])


class Model:
    def __init__(self):
        self.dump = dict(fsid=FSID, epoch=11, require_min_compat_client="luminous", pools=[pool()],
                         osds=[dict(osd=i, up=1, **{"in": 1}, public_addrs=dict(addrvec=[dict(type="v2", addr=f"127.0.0.1:{3300+i}", nonce=40+i)])) for i in range(3)],
                         pg_upmap=[], pg_upmap_items=[], pg_upmap_primaries=[])
        self.objects = {"baseline": object_state()}
        self.mutations = []
        self.fail = None
        self.after_effect = False
        self.same_pg = True

    def vector(self, pg):
        if pg.startswith("7."):
            return [0, 1, 2]
        vector = [0, 1]
        for entry in self.dump["pg_upmap"]:
            if entry["pgid"] == pg:
                vector = list(entry["osds"])
        for entry in self.dump["pg_upmap_items"]:
            if entry["pgid"] == pg:
                for pair in entry["mappings"]:
                    if pair["from"] in vector and pair["to"] not in vector:
                        vector[vector.index(pair["from"])] = pair["to"]
        return vector

    def run(self, argv, **kwargs):
        if argv[0] == sys.executable:
            return self.result(argv, self.objects.get(argv[-1], dict(code=-2, version=0, data="", attributes=None, gets=[])))
        args = argv[argv.index("--keyring") + 2:]
        args = args[:-2] if args[-2:] == ["--format", "json"] else args
        mutation = (argv[0] == "rados" or args[:3] in (["osd", "pool", "create"], ["osd", "pool", "set"])
                    or len(args) > 1 and args[1] in ("pg-upmap", "pg-upmap-items", "rm-pg-upmap", "rm-pg-upmap-items"))
        fail = mutation and self.fail is not None and self.fail(args)
        if mutation:
            self.mutations.append(list(args))
        if fail and not self.after_effect:
            raise subprocess.TimeoutExpired(argv, kwargs.get("timeout", 8))
        value = {}
        if args == ["osd", "dump"]:
            value = self.dump
        elif args[:3] == ["osd", "pool", "create"]:
            self.dump["pools"].append(pool(8, args[3], 2)); self.dump["epoch"] += 1
        elif args[:3] == ["osd", "pool", "set"]:
            self.dump["pools"][1][args[4]] = int(args[5]); self.dump["epoch"] += 1
        elif argv[0] == "rados":
            slot = "items" if args[3].endswith("items") else "full"
            if args[2] == "put":
                self.objects[slot] = object_state(Path(args[4]).read_bytes(), 10 + len(self.objects))
            elif args[2] == "rm":
                self.objects.pop(slot, None)
            else:
                raise AssertionError(args)
        elif args[:2] == ["osd", "map"]:
            baseline = args[2] == "ceph-msgr-osd-fixture"
            pid, seed = (7, 0) if baseline else (8, 0 if self.same_pg or args[3].endswith("items") else 1)
            pg = f"{pid}.{seed:x}"
            v = self.vector(pg)
            value = dict(epoch=self.dump["epoch"], pool_id=pid, pool=args[2], objname=args[3],
                         pgid=pg, raw_pgid=f"{pid}.aabbcc00", up=v, acting=v, acting_primary=v[0])
        elif args[0] == "pg":
            v = self.vector(args[1])
            value = dict(epoch=self.dump["epoch"], info=dict(pgid=args[1]), up=v, acting=v, state="active+clean")
        elif args[:2] in (["osd", "pg-upmap"], ["osd", "pg-upmap-items"]):
            field = "pg_upmap_items" if args[1].endswith("items") else "pg_upmap"
            entry = dict(pgid=args[2])
            if field.endswith("items"):
                entry["mappings"] = [dict(**{"from": int(args[3]), "to": int(args[4])})]
            else:
                entry["osds"] = [int(i) for i in args[3:]]
            self.dump[field] = [entry]; self.dump["epoch"] += 1
        elif args[:2] in (["osd", "rm-pg-upmap"], ["osd", "rm-pg-upmap-items"]):
            field = "pg_upmap_items" if args[1].endswith("items") else "pg_upmap"
            self.dump[field] = []; self.dump["epoch"] += 1
        else:
            raise AssertionError(args)
        if fail:
            raise subprocess.TimeoutExpired(argv, kwargs.get("timeout", 8))
        return self.result(argv, value)

    @staticmethod
    def result(argv, value):
        return subprocess.CompletedProcess(argv, 0, json.dumps(value).encode(), b"")


class UpmapOracleTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.out = Path(temp.name); self.root = self.out / "root"
        self.model = Model()
        if os.name == "nt":
            # This models the Linux-only fixture; real lock/death coverage is
            # the separate POSIX-only test below, never silently emulated.
            mocked_lock = patch.object(oracle, "lock", side_effect=lambda out: nullcontext())
            mocked_lock.start(); self.addCleanup(mocked_lock.stop)
        oracle.omap.publish(self.out, "fixture-manifest", dict(fsid=FSID, osd="1", osd_replicas="3", osd_profile=oracle.PROFILES[0]))
        oracle.omap.publish(self.out, "osd-oracle", dict(pool="ceph-msgr-osd-fixture", pool_id=7, object="baseline", profile=oracle.PROFILES[0], replicas=3, pool_pg_num=8))
        oracle.omap.publish(self.out, "fixture-ownership", dict(containers={f"osd.{i}": str(i)*64 for i in range(3)}))
        oracle.omap.publish(self.out, oracle.NAME + "-input", {})
        (self.out / "daemon-version").write_text("20.2.4 " + oracle.xattr.SOURCE_COMMIT)
        self.run_patch = patch.object(oracle.subprocess, "run", side_effect=lambda *args, **kwargs: self.model.run(*args, **kwargs))
        self.run_patch.start(); self.addCleanup(self.run_patch.stop)
        self.spawn = patch.object(oracle.subprocess, "Popen", return_value=type("Process", (), {"pid": 12345})()).start(); self.addCleanup(patch.stopall)

    def action(self, action):
        oracle.run(action, self.root, self.out)
        return oracle.omap.read_json(self.out / (oracle.NAME + "-result.json"))

    def test_closed_input_and_exact_fixture_ownership(self):
        for value in (None, [], {"pool": "foreign"}, {"object": "foreign"}, {"command": []}):
            with self.assertRaises(ValueError):
                oracle.check_input(value)
        for token in (None, "x"*32, "a"*31, "../outside"):
            with self.assertRaises(ValueError):
                oracle.names(token)
        value = oracle.omap.read_json(self.out / "fixture-manifest.json")
        for changed in (dict(value, osd_replicas="1"), dict(value, osd_profile="legacy-v6")):
            oracle.omap.publish(self.out, "fixture-manifest", changed)
            with self.assertRaises(ValueError):
                oracle.fixture_identity(self.out)
        self.assertEqual(self.model.mutations, [])

    def test_full_lifecycle_same_or_different_pg_preserves_baseline_and_explicit_plan(self):
        for same in (True, False):
            with self.subTest(same_pg=same):
                # Reuse the model only once per isolated scope.
                if (self.out / (oracle.NAME + "-owned.json")).exists():
                    (self.out / (oracle.NAME + "-owned.json")).unlink()
                    self.model = Model()
                self.model.same_pg = same
                prepared = self.action("prepare")
                self.assertEqual(prepared["plan"]["targets"]["items"], [1, 2])
                self.assertEqual(prepared["plan"]["targets"]["full"], [2, 0])
                items = self.action("items")
                self.assertEqual(items["observations"]["items"]["route"]["acting"], [0, 2])
                self.action("restore-items")
                full = self.action("full")
                self.assertEqual(full["observations"]["full"]["route"]["acting"], [2, 0])
                self.assertEqual(full["observations"]["full"]["route"]["osd_id"], 2)
                self.assertEqual(full["baseline_fixture"]["object_state"], object_state())
                self.action("restore-full")
                final = self.action("finish")
                cleanup = final["cleanup"]
                self.assertTrue(cleanup["overrides_restored"] and cleanup["objects_removed"] and cleanup["baseline_unchanged"])
                self.assertFalse(cleanup["pool_removed"])
                self.assertTrue(cleanup["retained_for_fixture_teardown"])
                self.assertEqual(cleanup["errors"], [])
                mutations = copy.deepcopy(self.model.mutations)
                self.assertEqual(self.action("finish"), final)
                self.assertEqual(self.model.mutations, mutations)

    def test_second_prepare_and_out_of_order_actions_never_repeat_mutation(self):
        self.action("prepare")
        before = copy.deepcopy(self.model.mutations)
        for action in ("prepare", "full", "restore-items"):
            with self.assertRaises(ValueError):
                self.action(action)
        self.assertEqual(self.model.mutations, before)

    def test_unrelated_upmap_or_wrong_owned_vector_is_never_touched(self):
        self.action("prepare")
        for entry in (dict(pgid="7.0", osds=[2, 0]), dict(pgid="8.0", osds=[1, 2])):
            self.model.dump["pg_upmap"] = [entry]
            before = copy.deepcopy(self.model.mutations)
            with self.assertRaises(ValueError):
                self.action("items")
            self.assertEqual(self.model.mutations, before)

    def test_baseline_bytes_version_or_policy_change_refuses_before_remap(self):
        self.action("prepare")
        for field, value in (("version", 5), ("data", "Zm9yZWlnbg=="), ("nanos", 5)):
            self.model.objects["baseline"] = object_state(); self.model.objects["baseline"][field] = value
            before = len(self.model.mutations)
            with self.assertRaises(ValueError):
                self.action("items")
            self.assertEqual(len(self.model.mutations), before)
        self.model.objects["baseline"] = object_state(); self.model.dump["pools"][0]["size"] = 2
        with self.assertRaises(ValueError):
            self.action("items")

    def test_uncertain_install_is_not_retried_and_confirmed_override_can_be_restored(self):
        for after_effect in (False, True):
            with self.subTest(after_effect=after_effect):
                if (self.out / (oracle.NAME + "-owned.json")).exists():
                    (self.out / (oracle.NAME + "-owned.json")).unlink(); self.model = Model()
                self.action("prepare")
                self.model.fail = lambda words: words[:2] == ["osd", "pg-upmap-items"]
                self.model.after_effect = after_effect
                with self.assertRaises(RuntimeError):
                    self.action("items")
                failed = oracle.load_owned(self.out)
                self.assertEqual(failed["pending"], "items")
                self.model.fail = None
                final = self.action("finish")
                self.assertTrue(final["cleanup"]["overrides_restored"])
                self.assertEqual(final["unresolved_mutation"], "items")
                self.assertEqual(sum(v[:2] == ["osd", "pg-upmap-items"] for v in self.model.mutations), 1)

    def test_uncertain_cleanup_before_or_after_effect_is_not_reissued(self):
        for after_effect in (False, True):
            with self.subTest(after_effect=after_effect):
                if (self.out / (oracle.NAME + "-owned.json")).exists():
                    (self.out / (oracle.NAME + "-owned.json")).unlink(); self.model = Model()
                self.action("prepare"); self.action("items")
                self.model.fail = lambda words: words[:2] == ["osd", "rm-pg-upmap-items"]
                self.model.after_effect = after_effect
                with self.assertRaises(RuntimeError):
                    self.action("finish")
                self.model.fail = None
                final = self.action("finish")
                self.assertEqual(final["cleanup"]["overrides_restored"], after_effect)
                self.assertTrue(final["cleanup"]["errors"])
                before = copy.deepcopy(self.model.mutations)
                self.assertEqual(self.action("finish"), final)
                self.assertEqual(self.model.mutations, before)
                self.assertEqual(sum(v[:2] == ["osd", "rm-pg-upmap-items"] for v in self.model.mutations), 1)

    def test_uncertain_explicit_restore_is_not_replayed_as_cleanup(self):
        for after_effect in (False, True):
            with self.subTest(after_effect=after_effect):
                if (self.out / (oracle.NAME + "-owned.json")).exists():
                    (self.out / (oracle.NAME + "-owned.json")).unlink(); self.model = Model()
                self.action("prepare"); self.action("items")
                self.model.fail = lambda words: words[:2] == ["osd", "rm-pg-upmap-items"]
                self.model.after_effect = after_effect
                with self.assertRaises(RuntimeError):
                    self.action("restore-items")
                self.model.fail = None
                final = self.action("finish")
                self.assertEqual(final["cleanup"]["overrides_restored"], after_effect)
                self.assertEqual(sum(v[:2] == ["osd", "rm-pg-upmap-items"] for v in self.model.mutations), 1)

    def test_uncertain_create_is_not_adopted_or_reissued_during_cleanup(self):
        for after_effect in (False, True):
            with self.subTest(after_effect=after_effect):
                if (self.out / (oracle.NAME + "-owned.json")).exists():
                    (self.out / (oracle.NAME + "-owned.json")).unlink(); self.model = Model()
                self.model.fail = lambda words: words[:3] == ["osd", "pool", "create"]
                self.model.after_effect = after_effect
                with self.assertRaises(RuntimeError):
                    self.action("prepare")
                self.model.fail = None
                self.assertIsNone(oracle.load_owned(self.out)["plan"]["pool_id"])
                mutations = copy.deepcopy(self.model.mutations)
                final = self.action("finish")
                self.assertFalse(final["cleanup"]["objects_removed"])
                self.assertFalse(final["cleanup"]["pool_removed"])
                self.assertEqual(final["unresolved_mutation"], "create-pool")
                self.action("finish")
                self.assertEqual(self.model.mutations, mutations)

    def test_uncertain_object_removal_is_never_reissued(self):
        self.action("prepare")
        self.model.fail = lambda words: words[:3] == ["-p", self.model.dump["pools"][1]["pool_name"], "rm"] and words[3].endswith("items")
        with self.assertRaises(RuntimeError):
            self.action("finish")
        self.model.fail = None
        final = self.action("finish")
        self.assertFalse(final["cleanup"]["objects_removed"])
        self.assertEqual(sum(v[0] == "-p" and v[2] == "rm" and v[3].endswith("items") for v in self.model.mutations), 1)
        self.action("finish")
        self.assertEqual(sum(v[0] == "-p" and v[2] == "rm" and v[3].endswith("items") for v in self.model.mutations), 1)

    def test_idle_guard_is_failure_and_renewed_action_wins_lease_under_lock(self):
        self.action("prepare")
        state = oracle.load_owned(self.out); state["last_action"] = 0; oracle.save(self.out, state)
        final = oracle.run("finish", self.root, self.out, automatic=True)
        self.assertTrue(final["automatic_cleanup"])
        self.assertIn("lease expired", final["first_error"])
        self.assertTrue(final["cleanup"]["errors"])

    def test_guard_does_not_expire_a_newly_refreshed_action_lease(self):
        self.action("prepare")
        before = copy.deepcopy(self.model.mutations)
        self.assertIsNone(oracle.run("finish", self.root, self.out, automatic=True))
        self.assertEqual(before, self.model.mutations)
        self.assertFalse(oracle.load_owned(self.out)["finished"])

    def test_pool_name_replacement_after_cleanup_observation_cannot_remove_foreign_objects(self):
        self.action("prepare")
        before = copy.deepcopy(self.model.mutations)
        original = oracle.Native.snapshot
        seen = 0
        def replaced(native, slot, **kwargs):
            nonlocal seen
            if slot == "items":
                seen += 1
                if seen == 2:
                    self.model.dump["pools"][1]["pool"] = 9
                    self.model.dump["epoch"] += 1
            return original(native, slot, **kwargs)
        with patch.object(oracle.Native, "snapshot", replaced), self.assertRaisesRegex(ValueError, "name/ID"):
            self.action("finish")
        self.assertEqual(before, self.model.mutations)

    def test_successful_cleanup_remove_then_observation_failure_cannot_alias_restore(self):
        self.action("prepare"); self.action("items")
        original = oracle.receipt
        def failed(native, phase, expected=None):
            if phase == "cleanup-restored":
                raise RuntimeError("read-only post-cleanup observation failed")
            return original(native, phase, expected)
        with patch.object(oracle, "receipt", side_effect=failed), self.assertRaises(RuntimeError):
            self.action("finish")
        before = copy.deepcopy(self.model.mutations)
        with self.assertRaisesRegex(ValueError, "finish only"):
            self.action("restore-items")
        self.assertEqual(before, self.model.mutations)
        self.action("finish")
        self.assertEqual(sum(v[:2] == ["osd", "rm-pg-upmap-items"] for v in self.model.mutations), 1)

    def test_native_compatibility_prerequisite_is_read_only_and_not_silently_raised(self):
        for value in (None, "jewel", "unknown"):
            self.model.dump["require_min_compat_client"] = value
            before = copy.deepcopy(self.model.mutations)
            with self.assertRaisesRegex(ValueError, "at least luminous"):
                self.action("prepare")
            self.assertEqual(before, self.model.mutations)
            (self.out / (oracle.NAME + "-owned.json")).unlink()

    def test_numeric_snapshot_ioctx_verifies_the_frozen_pool_id_and_closes_on_failure(self):
        for actual_id in (8, 9):
            functions = ("rados_create2", "rados_conf_read_file", "rados_conf_set", "rados_connect",
                         "rados_ioctx_create2", "rados_ioctx_get_id", "rados_ioctx_destroy", "rados_shutdown")
            lib = SimpleNamespace(**{name: Mock(return_value=0) for name in functions})
            lib.rados_ioctx_get_id.return_value = actual_id
            native = SimpleNamespace(lib=lib, check=lambda code: self.assertEqual(code, 0))
            if actual_id == 8:
                with oracle.pool_connection(native, self.root, 8):
                    pass
            else:
                with self.assertRaisesRegex(ValueError, "ioctx pool ID"):
                    with oracle.pool_connection(native, self.root, 8):
                        self.fail("wrong-ID ioctx was exposed")
            self.assertEqual(lib.rados_ioctx_create2.call_args.args[1], 8)
            lib.rados_ioctx_destroy.assert_called_once()
            lib.rados_shutdown.assert_called_once()

    def test_action_deadline_and_guard_use_only_closed_finish(self):
        self.action("prepare")
        native = oracle.Native(self.root, self.out, oracle.load_owned(self.out))
        native.deadline = 0
        with self.assertRaises(TimeoutError):
            native.command(["osd", "dump"])
        with patch.object(oracle, "load_owned", return_value=dict(finished=False, first_error="original failure", token="a"*32)), patch.object(oracle, "run", return_value={"finished": True}) as run:
            oracle.guard(self.root, self.out)
        run.assert_called_once_with("finish", self.root, self.out, automatic=True)


@unittest.skipUnless(os.name == "posix", "real process-death flock coverage requires POSIX; model tests remain portable")
class PosixLockDeathTests(unittest.TestCase):
    def test_sigkill_releases_same_inode_for_guard_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            code = "import sys,time; from pathlib import Path; sys.path.insert(0,sys.argv[1]); import osd_upmap_oracle as o\nwith o.lock(Path(sys.argv[2])):\n print('held',flush=True)\n time.sleep(30)\n"
            child = subprocess.Popen([sys.executable, "-c", code, str(Path(oracle.__file__).parent), str(out)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                import selectors
                with selectors.DefaultSelector() as ready:
                    ready.register(child.stdout, selectors.EVENT_READ)
                    self.assertTrue(ready.select(5), "child did not acquire the lock within deadline")
                self.assertEqual(child.stdout.readline(), b"held\n")
                inode = (out / (oracle.NAME + ".lock")).stat().st_ino
                with self.assertRaises(BlockingIOError):
                    with oracle.lock(out):
                        self.fail("two live owners acquired one lock")
                child.kill(); child.wait(timeout=5)
                # This is the guard's same lock path after ungraceful owner death.
                with oracle.lock(out):
                    self.assertEqual((out / (oracle.NAME + ".lock")).stat().st_ino, inode)
                self.assertTrue((out / (oracle.NAME + ".lock")).is_file())
            finally:
                if child.poll() is None:
                    child.kill(); child.wait(timeout=5)
                child.stdout.close(); child.stderr.close()


if __name__ == "__main__":
    unittest.main()
