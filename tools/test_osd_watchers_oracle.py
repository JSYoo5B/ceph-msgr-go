#!/usr/bin/env python3
"""Watcher helper scope/lifetime checks; no mocked Ceph compatibility claim."""
import copy
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

import osd_watchers_oracle as oracle

METADATA = {"pool": "pool", "pool_id": 7, "object": "fixture"}
OLD = "1" * 32
NEW = "2" * 32
OBJECT = "fixture-native-omapw-main-" + "a" * 32
NEXT_OBJECT = "fixture-native-omapw-main-" + "b" * 32


def scope(token=OLD, name=OBJECT, stopped=True):
    return dict(schema_version=1, token=token, object=name,
                source_commit=oracle.xattr.SOURCE_COMMIT, stopped=stopped, ready=True,
                ownership=dict(schema_version=1, source_commit=oracle.xattr.SOURCE_COMMIT,
                               pool="pool", pool_id=7, fixture_object="fixture", namespace="", objects=[name]),
                native_after_unwatch=dict(source_commit=oracle.xattr.SOURCE_COMMIT, object=name, entries=[]))


class WatcherScopeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.out = Path(self.directory.name)
        self.root = self.out / "root"
        self.patch_owned = patch.object(oracle, "owned", return_value=(METADATA, OBJECT))
        self.owned = self.patch_owned.start()
        self.addCleanup(self.patch_owned.stop)
        self.patch_uuid = patch.object(oracle.uuid, "uuid4", return_value=SimpleNamespace(hex=NEW))
        self.patch_uuid.start()
        self.addCleanup(self.patch_uuid.stop)

    def seed(self, status):
        oracle.omap.publish(self.out, oracle.NAME + "-status", status)
        oracle.omap.publish(self.out, oracle.scope_name(status["token"], "status"), status)
        log = self.out / oracle.scope_name(status["token"], "worker.log")
        log.write_bytes(b"old scope evidence\n")
        return log

    def spawn_ready(self, args, stdout, **kwargs):
        token = args[-1]
        status = oracle.current_scope(self.out, METADATA)
        self.assertEqual(status["token"], token)
        self.assertFalse(status["ready"])
        stdout.write(b"new scope evidence\n")
        stdout.flush()
        status.update(ready=True, cookies=[1, 2], global_id=42)
        oracle.publish_status(self.out, status)
        return SimpleNamespace(poll=lambda: None)

    def test_completed_scope_allows_new_owned_object_and_preserves_evidence(self):
        old = scope()
        log = self.seed(old)
        unrelated = self.out / "unrelated.json"
        unrelated.write_text("leave me")
        self.owned.return_value = METADATA, NEXT_OBJECT
        with patch.object(oracle.subprocess, "Popen", side_effect=self.spawn_ready) as spawn:
            oracle.run("start", self.root, self.out)
        self.assertEqual(spawn.call_count, 1)
        self.assertEqual(oracle.omap.read_json(self.out / (oracle.scope_name(OLD, "status") + ".json")), old)
        self.assertEqual(log.read_bytes(), b"old scope evidence\n")
        self.assertEqual(unrelated.read_text(), "leave me")
        current = oracle.current_scope(self.out, METADATA)
        self.assertEqual((current["token"], current["object"]), (NEW, NEXT_OBJECT))
        self.assertEqual((self.out / oracle.scope_name(NEW, "worker.log")).read_bytes(), b"new scope evidence\n")
        self.assertFalse((self.out / (oracle.NAME + "-control.lock")).exists())
        self.assertEqual((self.out / (oracle.NAME + "-worker.log")).read_bytes(), b"new scope evidence\n")

    def test_active_failed_and_incomplete_scopes_refuse_overlap(self):
        cases = [scope(stopped=False), dict(scope(), error="unwatch failed"),
                 dict(scope(), native_after_unwatch={}),
                 dict(scope(), native_after_unwatch=dict(scope()["native_after_unwatch"], entries=[{"cookie": 1}]))]
        for status in cases:
            with self.subTest(status=status):
                self.seed(status)
                before = {p.name: p.read_bytes() for p in self.out.iterdir()}
                with patch.object(oracle.subprocess, "Popen") as spawn, self.assertRaises(ValueError):
                    oracle.run("start", self.root, self.out)
                spawn.assert_not_called()
                self.assertEqual(before, {p.name: p.read_bytes() for p in self.out.iterdir()})

    def test_foreign_fixture_or_token_receipt_refuses_overlap(self):
        for field, value in [("pool", "foreign"), ("pool_id", 8), ("fixture_object", "foreign")]:
            status = scope()
            status["ownership"][field] = value
            self.seed(status)
            with self.subTest(field=field), patch.object(oracle.subprocess, "Popen") as spawn, self.assertRaises(ValueError):
                oracle.run("start", self.root, self.out)
            spawn.assert_not_called()
        status = scope()
        self.seed(status)
        changed = copy.deepcopy(status)
        changed["object"] = NEXT_OBJECT
        changed["ownership"]["objects"] = [NEXT_OBJECT]
        oracle.omap.publish(self.out, oracle.scope_name(OLD, "status"), changed)
        with patch.object(oracle.subprocess, "Popen") as spawn, self.assertRaises(ValueError):
            oracle.run("start", self.root, self.out)
        spawn.assert_not_called()

    def test_start_stop_start_keeps_independent_token_receipts(self):
        with patch.object(oracle.subprocess, "Popen", side_effect=self.spawn_ready):
            oracle.run("start", self.root, self.out)
        def unwatch(_):
            status = oracle.current_scope(self.out, METADATA)
            self.assertTrue((self.out / (oracle.NAME + "-stop-" + NEW)).exists())
            status.update(stopped=True, native_after_unwatch=scope(NEW)["native_after_unwatch"])
            oracle.publish_status(self.out, status)
        with patch.object(oracle.time, "sleep", side_effect=unwatch):
            oracle.run("stop", self.root, self.out)
        completed = oracle.current_scope(self.out, METADATA)
        self.assertTrue(oracle.completed(completed))
        with patch.object(oracle.uuid, "uuid4", return_value=SimpleNamespace(hex=OLD)), \
                patch.object(oracle.subprocess, "Popen", side_effect=self.spawn_ready):
            oracle.run("start", self.root, self.out)
        self.assertEqual(oracle.omap.read_json(self.out / (oracle.scope_name(NEW, "status") + ".json")), completed)
        self.assertEqual(oracle.current_scope(self.out, METADATA)["token"], OLD)

    def test_spawn_failure_reserves_scope_and_signals_stop(self):
        with patch.object(oracle.subprocess, "Popen", side_effect=OSError("spawn failed")), self.assertRaises(OSError):
            oracle.run("start", self.root, self.out)
        self.assertFalse(oracle.current_scope(self.out, METADATA)["ready"])
        self.assertTrue((self.out / (oracle.NAME + "-stop-" + NEW)).exists())
        with patch.object(oracle.subprocess, "Popen") as spawn, self.assertRaises(ValueError):
            oracle.run("start", self.root, self.out)
        spawn.assert_not_called()

    def test_existing_control_lock_and_token_files_are_never_removed(self):
        lock = self.out / (oracle.NAME + "-control.lock")
        lock.write_text("another invocation")
        with self.assertRaises(FileExistsError):
            oracle.run("start", self.root, self.out)
        self.assertEqual(lock.read_text(), "another invocation")
        lock.unlink()
        log = self.out / oracle.scope_name(NEW, "worker.log")
        log.write_text("preexisting scope")
        with patch.object(oracle.subprocess, "Popen") as spawn, self.assertRaises(FileExistsError):
            oracle.run("start", self.root, self.out)
        spawn.assert_not_called()
        self.assertEqual(log.read_text(), "preexisting scope")
        self.assertFalse((self.out / (oracle.NAME + "-status.json")).exists())

    def test_control_lock_closes_handle_before_windows_unlink(self):
        lock = self.out / (oracle.NAME + "-control.lock")
        original_open, original_unlink = Path.open, Path.unlink
        handles = []

        def open_file(path, *args, **kwargs):
            handle = original_open(path, *args, **kwargs)
            if path == lock:
                handles.append(handle)
            return handle

        def windows_unlink(path, *args, **kwargs):
            if path == lock and any(not handle.closed for handle in handles):
                raise PermissionError("Windows refuses to unlink an open file")
            return original_unlink(path, *args, **kwargs)

        for fail_body in (False, True):
            with self.subTest(fail_body=fail_body), \
                    patch.object(Path, "open", autospec=True, side_effect=open_file), \
                    patch.object(Path, "unlink", autospec=True, side_effect=windows_unlink):
                def operation():
                    with oracle.control_lock(self.out):
                        self.assertTrue(lock.exists())
                        with self.assertRaises(FileExistsError):
                            with oracle.control_lock(self.out):
                                self.fail("overlapping lock admitted")
                        if fail_body:
                            raise ValueError("protected operation failed")

                if fail_body:
                    with self.assertRaisesRegex(ValueError, "protected operation failed"):
                        operation()
                else:
                    operation()
                self.assertFalse(lock.exists())

    def test_common_completion_waits_for_final_token_receipt(self):
        active = scope(stopped=False)
        self.seed(active)
        oracle.omap.publish(self.out, oracle.NAME + "-status", scope())
        with patch.object(oracle.subprocess, "Popen") as spawn, self.assertRaises(ValueError):
            oracle.run("start", self.root, self.out)
        spawn.assert_not_called()

    def test_stop_refuses_another_current_object_without_signaling_it(self):
        self.seed(scope(stopped=False))
        self.owned.return_value = METADATA, NEXT_OBJECT
        with self.assertRaises(ValueError):
            oracle.run("stop", self.root, self.out)
        self.assertFalse((self.out / (oracle.NAME + "-stop-" + OLD)).exists())

    def test_worker_unwatches_and_shuts_down_before_completion_receipt(self):
        for fail_shutdown in (False, True):
            with self.subTest(fail_shutdown=fail_shutdown):
                initial = scope(stopped=False)
                initial.pop("native_after_unwatch")
                initial["ready"] = False
                self.seed(initial)
                (self.out / (oracle.NAME + "-stop-" + OLD)).touch()
                cluster = MagicMock()
                watches = [MagicMock(), MagicMock()]
                cluster.open_ioctx.return_value.__enter__.return_value.watch.side_effect = watches
                cluster.get_instance_id.return_value = 42
                for index, watch in enumerate(watches):
                    watch.get_id.return_value = index + 1
                def shutdown():
                    for watch in watches:
                        watch.close.assert_called_once()
                    self.assertFalse(oracle.current_scope(self.out, METADATA)["stopped"])
                    if fail_shutdown:
                        raise RuntimeError("shutdown failed")
                cluster.shutdown.side_effect = shutdown
                observed = [dict(source_commit=oracle.xattr.SOURCE_COMMIT, object=OBJECT, entries=[{}, {}]),
                            scope()["native_after_unwatch"]]
                with patch.dict(oracle.sys.modules, {"rados": SimpleNamespace(Rados=lambda **kwargs: cluster)}), \
                        patch.object(oracle, "listing", side_effect=observed):
                    if fail_shutdown:
                        with self.assertRaisesRegex(RuntimeError, "shutdown failed"):
                            oracle.worker(self.root, self.out, OLD)
                    else:
                        oracle.worker(self.root, self.out, OLD)
                final = oracle.current_scope(self.out, METADATA)
                self.assertEqual(oracle.completed(final), not fail_shutdown)
                if fail_shutdown:
                    self.assertEqual(final["error"], "shutdown failed")

    def test_foreign_diagnostic_log_is_never_replaced(self):
        self.seed(scope())
        view = self.out / (oracle.NAME + "-worker.log")
        view.write_text("unowned diagnostic")
        with patch.object(oracle.subprocess, "Popen") as spawn, self.assertRaises(ValueError):
            oracle.run("start", self.root, self.out)
        spawn.assert_not_called()
        self.assertEqual(view.read_text(), "unowned diagnostic")
        view.unlink()
        view.symlink_to("unrelated.log")
        with patch.object(oracle.subprocess, "Popen") as spawn, self.assertRaises(ValueError):
            oracle.run("start", self.root, self.out)
        spawn.assert_not_called()
        self.assertEqual(view.readlink(), Path("unrelated.log"))

    def test_worker_cannot_publish_over_another_scope(self):
        self.seed(scope())
        with self.assertRaises(ValueError):
            oracle.publish_status(self.out, scope(NEW))
        self.assertEqual(oracle.current_scope(self.out, METADATA)["token"], OLD)


if __name__ == "__main__":
    unittest.main()
