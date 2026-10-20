#!/usr/bin/env python3
"""Owned PG-state recipe regressions; mocks do not establish daemon acceptance."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import osd_backoff_oracle as oracle

FSID = "12345678-1234-1234-1234-123456789abc"
TOKEN = "1" * 32
NEXT = "2" * 32
OBJECT = "fixture-native-omapw-main-" + "a" * 32
METADATA = dict(pool="ceph-msgr-osd-fixture", pool_id=7, object="fixture", profile=oracle.PROFILES[0],
                replicas=1, pool_pg_num=8, osd_id=0)
OWNERSHIP = dict(schema_version=1, source_commit=oracle.xattr.SOURCE_COMMIT, pool=METADATA["pool"],
                 pool_id=7, fixture_object="fixture", namespace="", objects=[OBJECT])
STATE = dict(code=0, data="AP9iYXNlbGluZQ==", version=3, seconds=1, nanos=2)


def pool(size=1, minimum=1):
    return dict(pool=7, pool_name=METADATA["pool"], type=1, pg_num=8, pg_placement_num=8,
                flags=1, size=size, min_size=minimum)


def observation(active=True):
    return dict(pool=pool() if active else pool(2, 2), epoch=12 if active else 11,
                placement=dict(pool=METADATA["pool"], pool_id=7, objname=OBJECT,
                               acting_primary=0, acting=[0], pgid="7.3", raw_pgid="7.aabbccdd"),
                pg=dict(state="active+clean" if active else "peered+undersized+degraded",
                        epoch=12 if active else 11, info=dict(pgid="7.3"), acting=[0]))


def topology(descriptor=None):
    return dict(fsid=FSID, epoch=10, osds=[dict(osd=0, up=1)], pools=[descriptor or pool()])


def scope():
    before = observation()
    before["object_state"] = copy.deepcopy(STATE)
    return dict(schema_version=1, token=TOKEN, source_commit=oracle.xattr.SOURCE_COMMIT,
                object=OBJECT, fsid=FSID, ownership=copy.deepcopy(OWNERSHIP), ready=False,
                done=False, restored=False, before=before)


def ack_input():
    return dict(global_id=42, id=1, map_epoch=11, pgid="7.3")


def ack_history():
    return {"ops": [{"description": "osd_backoff(7.3 ack-block id 1 [7:30000000::::head,7:50000000::::head) e11)",
                     "type_data": {"client_info": {"client": "client.42", "client_addr": "v2:private-address/nonce", "tid": 0},
                                   "events": [{"event": "queued_for_pg"}, {"event": "reached_pg"}]}}]}


class BackoffOracleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = Path(self.tmp.name)
        self.root = self.out / "root"
        self.owned_patch = patch.object(oracle, "owned", return_value=(METADATA, FSID, OWNERSHIP, OBJECT))
        self.owned = self.owned_patch.start()
        self.addCleanup(self.owned_patch.stop)

    def seed(self, status=None):
        status = scope() if status is None else status
        oracle.omap.publish(self.out, oracle.NAME + "-status", status)
        oracle.omap.publish(self.out, oracle.token_name(status["token"], "status"), status)
        return status

    def test_input_rejects_implicit_or_extra_scope(self):
        self.assertEqual(oracle.request({"object": OBJECT}), OBJECT)
        for raw in ({}, None, {"object": ""}, {"object": "bad\0name"}, {"object": OBJECT, "pool": "foreign"}):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                oracle.request(raw)
        for token in (None, "../outside", "g" * 32, "1" * 31):
            with self.assertRaises(ValueError):
                oracle.token_name(token, "status")

    def test_live_pool_ownership_rejects_other_clusters_and_replica_profiles(self):
        self.assertEqual(oracle.checked_pool(topology(), METADATA, FSID), pool())
        cases = [dict(topology(), fsid="another-cluster"), dict(topology(), osds=[dict(osd=1, up=1)]),
                 dict(topology(), osds=[dict(osd=0, up=1), dict(osd=1, up=1)]),
                 topology(dict(pool(), pool_name="foreign")), topology(dict(pool(), pg_num=1)),
                 topology(dict(pool(), flags=0)), topology(dict(pool(), size=3, min_size=2))]
        for state in cases:
            with self.subTest(state=state), self.assertRaises(ValueError):
                oracle.checked_pool(state, METADATA, FSID)

    def test_manifest_rejects_legacy_and_three_replica_profiles_before_commands(self):
        self.owned_patch.stop()
        metadata = copy.deepcopy(METADATA)
        manifest = dict(osd_profile=oracle.PROFILES[0], osd_replicas="1", osd="1", fsid=FSID)
        (self.out / "daemon-version").write_text("ceph version 20.2.4 " + oracle.xattr.SOURCE_COMMIT)
        for name, value in (("osd-oracle", metadata), ("fixture-manifest", manifest), (oracle.writes.OWNED, OWNERSHIP)):
            oracle.omap.publish(self.out, name, value)
        self.assertEqual(oracle.owned(self.root, self.out)[3], OBJECT)
        for change in (dict(metadata, profile="legacy-v6"), dict(metadata, replicas=3), dict(metadata, pool="foreign")):
            oracle.omap.publish(self.out, "osd-oracle", change)
            with patch.object(oracle, "ceph") as ceph, self.assertRaises(ValueError):
                oracle.owned(self.root, self.out)
            ceph.assert_not_called()

    def test_owned_scope_admits_exact_modern_profiles_and_rejects_mismatch(self):
        self.owned_patch.stop()
        (self.out / "daemon-version").write_text("20.2.4 " + oracle.xattr.SOURCE_COMMIT)
        oracle.omap.publish(self.out, oracle.writes.OWNED, OWNERSHIP)
        for profile in oracle.PROFILES:
            oracle.omap.publish(self.out, "osd-oracle", dict(METADATA, profile=profile))
            oracle.omap.publish(self.out, "fixture-manifest", dict(osd_profile=profile, osd_replicas="1", osd="1", fsid=FSID))
            self.assertEqual(oracle.owned(self.root, self.out)[0]["profile"], profile)
        for selected, actual in (("replicated-pg-v8", "replicated-pg-reply-v8"),
                                 ("replicated-pg-reply-v8", "replicated-pg-v8"),
                                 ("reply-v8", "reply-v8"), ("legacy-v6", "legacy-v6")):
            oracle.omap.publish(self.out, "osd-oracle", dict(METADATA, profile=actual))
            oracle.omap.publish(self.out, "fixture-manifest", dict(osd_profile=selected, osd_replicas="1", osd="1", fsid=FSID))
            with self.subTest(selected=selected, actual=actual), patch.object(oracle, "ceph") as ceph, self.assertRaises(ValueError):
                oracle.owned(self.root, self.out)
            ceph.assert_not_called()

    def test_observation_checks_applied_epoch_and_actual_pg(self):
        observed = observation()
        with patch.object(oracle, "ceph", side_effect=[topology(), observed["placement"], observed["pg"]]):
            result = oracle.observe(self.root, METADATA, FSID, OBJECT)
        self.assertEqual(result["pg"]["epoch"], 12)
        self.assertEqual(result["placement"]["pgid"], "7.3")
        for pg in (dict(observed["pg"], epoch=9), dict(observed["pg"], info=dict(pgid="8.3"))):
            with patch.object(oracle, "ceph", side_effect=[topology(), observed["placement"], pg]), self.assertRaises(ValueError):
                oracle.observe(self.root, METADATA, FSID, OBJECT)

    def test_inactive_wait_requires_peered_and_refuses_active_state(self):
        with patch.object(oracle, "observe", side_effect=[observation(), observation(False)]), \
                patch.object(oracle.time, "sleep") as sleep:
            got = oracle.wait_state(self.root, METADATA, FSID, OBJECT, active=False)
        self.assertEqual(got["pg"]["state"], "peered+undersized+degraded")
        sleep.assert_called_once()

    def test_native_calls_and_librados_snapshot_have_process_timeouts(self):
        observed = SimpleNamespace(returncode=0, stdout=b'{"code":0}', stderr=b"")
        with patch.object(oracle.subprocess, "run", return_value=observed) as run:
            self.assertEqual(oracle.object_state(self.root, METADATA, OBJECT, self.out), {"code": 0})
        self.assertEqual(run.call_args.kwargs["timeout"], oracle.COMMAND_SECONDS)
        self.assertIn("snapshot", run.call_args.args[0])
        self.assertEqual(run.call_args.args[0][-2:], ["--object", OBJECT])
        with patch.object(oracle.subprocess, "run", return_value=observed) as run:
            self.assertEqual(oracle.ceph(self.root, ["osd", "dump"]), {"code": 0})
        self.assertEqual(run.call_args.kwargs["timeout"], oracle.COMMAND_SECONDS)
        self.assertEqual(run.call_args.args[0][-2:], ["--format", "json"])
        bad = SimpleNamespace(returncode=0, stdout=b"x" * (oracle.MAX_OUTPUT + 1), stderr=b"")
        with patch.object(oracle.subprocess, "run", return_value=bad), self.assertRaises(RuntimeError):
            oracle.object_state(self.root, METADATA, OBJECT, self.out)

    def test_ack_identity_is_bound_to_inactive_scope_and_strict_input(self):
        status = dict(scope(), inactive=observation(False))
        self.assertEqual(oracle.ack_request(ack_input(), status), ack_input())
        cases = [dict(ack_input(), map_epoch=12), dict(ack_input(), pgid="7.2"),
                 dict(ack_input(), global_id=True), dict(ack_input(), id=-1),
                 dict(ack_input(), id=1 << 64), dict(ack_input(), extra="private")]
        for raw in cases:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                oracle.ack_request(raw, status)

    def test_ack_history_requires_exact_source_pg_id_epoch_opcode_and_reached_pg(self):
        got = oracle.parse_ack_history(ack_history(), ack_input(), TOKEN)
        self.assertEqual((got["source_type"], got["source_number"], got["op"], got["pgid"], got["map_epoch"]),
                         (8, 42, 2, "7.3", 11))
        self.assertTrue(got["reached_pg"])
        self.assertNotIn("description", got)
        self.assertNotIn("client_addr", got)
        for old, new in (("7.3 ", "7.2 "), ("id 1 ", "id 2 "), ("e11)", "e12)"),
                         ("ack-block", "block"), ("ack-block", "unblock")):
            history = ack_history()
            history["ops"][0]["description"] = history["ops"][0]["description"].replace(old, new)
            self.assertIsNone(oracle.parse_ack_history(history, ack_input(), TOKEN))
        history = ack_history()
        history["ops"][0]["type_data"]["client_info"]["client"] = "client.43"
        self.assertIsNone(oracle.parse_ack_history(history, ack_input(), TOKEN))
        history = ack_history()
        history["ops"][0]["type_data"]["events"] = [{"event": "queued_for_pg"}]
        self.assertIsNone(oracle.parse_ack_history(history, ack_input(), TOKEN))
        history = ack_history()
        history["ops"] *= 2
        with self.assertRaises(ValueError):
            oracle.parse_ack_history(history, ack_input(), TOKEN)

    def test_ack_receipt_waits_for_native_history_and_retains_only_public_identity(self):
        status = dict(scope(), inactive=observation(False))
        oracle.omap.publish(self.out, oracle.NAME + "-ack-input", ack_input())
        with patch.object(oracle, "osd_history", side_effect=[{"ops": []}, ack_history()]) as ceph, \
                patch.object(oracle.time, "sleep"):
            got = oracle.observe_ack(self.root, self.out, status)
        self.assertEqual(ceph.call_count, 2)
        self.assertEqual(ceph.call_args.args, (self.root, self.out))
        self.assertEqual(got["evidence"], "native completed op history; reception only")
        self.assertTrue(got["reached_pg"])
        with patch.object(oracle, "osd_history") as ceph:
            self.assertEqual(oracle.observe_ack(self.root, self.out, status), got)
        ceph.assert_not_called()
        receipt = oracle.omap.read_json(self.out / (oracle.NAME + "-ack-receipt.json"))
        self.assertEqual(receipt, got)
        self.assertFalse(any("private-address" in path.read_text() for path in self.out.glob("*.json")))

    def test_ack_history_timeout_cannot_become_a_success_receipt(self):
        status = dict(scope(), inactive=observation(False))
        oracle.omap.publish(self.out, oracle.NAME + "-ack-input", ack_input())
        with patch.object(oracle, "osd_history", return_value={"ops": []}), \
                patch.object(oracle.time, "monotonic", side_effect=[0, 11]), self.assertRaises(TimeoutError):
            oracle.observe_ack(self.root, self.out, status)
        self.assertFalse((self.out / (oracle.NAME + "-ack-receipt.json")).exists())

    def test_previous_ack_input_is_archived_only_after_matching_owned_scope(self):
        status = dict(scope(), inactive=observation(False), done=True, restored=True)
        oracle.omap.publish(self.out, oracle.NAME + "-ack-input", ack_input())
        with self.assertRaises(ValueError):
            oracle.archive_previous_ack(self.out, None)
        self.assertTrue((self.out / (oracle.NAME + "-ack-input.json")).exists())
        oracle.archive_previous_ack(self.out, status)
        self.assertFalse((self.out / (oracle.NAME + "-ack-input.json")).exists())
        self.assertEqual(oracle.omap.read_json(self.out / (oracle.token_name(TOKEN, "ack-input") + ".json")), ack_input())
        oracle.omap.publish(self.out, oracle.NAME + "-ack-input", dict(ack_input(), pgid="7.2"))
        with self.assertRaises(ValueError):
            oracle.archive_previous_ack(self.out, status)
        self.assertTrue((self.out / (oracle.NAME + "-ack-input.json")).exists())

    def test_worker_finishes_by_restoring_recorded_policy_and_unchanged_bytes(self):
        self.seed()
        (self.out / (oracle.NAME + "-finish-" + TOKEN)).touch()
        with patch.object(oracle, "observe", return_value=observation()), \
                patch.object(oracle, "wait_state", side_effect=[observation(False), observation()]), \
                patch.object(oracle, "object_state", return_value=STATE), patch.object(oracle, "set_pool") as change:
            oracle.worker(self.root, self.out, TOKEN)
        self.assertEqual([(call.args[-2], call.args[-1]) for call in change.call_args_list],
                         [("size", 2), ("min_size", 2), ("min_size", 1), ("size", 1), ("min_size", 1)])
        final = oracle.current(self.out, METADATA, FSID)
        self.assertTrue(final["done"] and final["restored"] and final["ready"])
        self.assertEqual(final["before"]["object_state"], final["after"]["object_state"])

    def test_uncertain_first_configuration_result_still_restores_once(self):
        self.seed()
        with patch.object(oracle, "observe", return_value=observation()), \
                patch.object(oracle, "wait_state", return_value=observation()), \
                patch.object(oracle, "object_state", return_value=STATE), \
                patch.object(oracle, "set_pool", side_effect=[TimeoutError("uncertain size result"), None, None, None]) as change, \
                self.assertRaisesRegex(TimeoutError, "uncertain size result"):
            oracle.worker(self.root, self.out, TOKEN)
        self.assertEqual([(call.args[-2], call.args[-1]) for call in change.call_args_list],
                         [("size", 2), ("min_size", 1), ("size", 1), ("min_size", 1)])
        final = oracle.current(self.out, METADATA, FSID)
        self.assertTrue(final["done"] and final["restored"])
        self.assertIn("uncertain", final["error"])

    def test_abandoned_scope_automatically_restores_at_deadline(self):
        self.seed()
        with patch.object(oracle, "observe", return_value=observation()), \
                patch.object(oracle, "wait_state", side_effect=[observation(False), observation()]), \
                patch.object(oracle, "object_state", return_value=STATE), patch.object(oracle, "set_pool"), \
                patch.object(oracle.time, "monotonic", side_effect=[10, 191]):
            oracle.worker(self.root, self.out, TOKEN)
        final = oracle.current(self.out, METADATA, FSID)
        self.assertTrue(final["automatic_restore"] and final["done"] and final["restored"])

    def test_restore_rejects_changed_object_state_and_attempts_all_cleanup_fields(self):
        status = scope()
        with patch.object(oracle, "set_pool", side_effect=[RuntimeError("first cleanup failed"), None, None]) as change, \
                patch.object(oracle, "wait_state", return_value=observation()), \
                patch.object(oracle, "object_state", return_value=dict(STATE, version=4)), self.assertRaises(RuntimeError):
            oracle.restore(self.root, METADATA, FSID, status, self.out)
        self.assertEqual(change.call_count, 3)
        self.assertFalse(status["restored"])

    def test_restore_size_one_uses_confirmation_only_after_live_identity_check(self):
        with patch.object(oracle, "ceph", side_effect=[topology(pool(2, 1)), None]) as ceph:
            oracle.set_pool(self.root, METADATA, FSID, "size", 1)
        self.assertEqual(ceph.call_args_list[1].args[1],
                         ["osd", "pool", "set", METADATA["pool"], "size", "1", "--yes-i-really-mean-it"])
        with patch.object(oracle, "ceph", return_value=dict(topology(), fsid="foreign")) as ceph, self.assertRaises(ValueError):
            oracle.set_pool(self.root, METADATA, FSID, "size", 1)
        self.assertEqual(ceph.call_count, 1)

    def test_active_failed_or_foreign_scope_never_spawns_overlap(self):
        for status in (scope(), dict(scope(), done=True, restored=False),
                       dict(scope(), done=True, restored=True, error="previous cleanup failed"),
                       dict(scope(), fsid="foreign")):
            self.seed(status)
            with patch.object(oracle.subprocess, "Popen") as spawn, self.assertRaises(ValueError):
                oracle.run("start", self.root, self.out)
            spawn.assert_not_called()

    def test_current_receipt_is_immutable_and_old_worker_cannot_publish(self):
        status = self.seed()
        view = dict(status, ready=True, done=True)
        oracle.omap.publish(self.out, oracle.NAME + "-status", view)
        self.assertEqual(oracle.current(self.out, METADATA, FSID), status)
        foreign = dict(status, token=NEXT)
        with self.assertRaises(ValueError):
            oracle.publish(self.out, foreign)
        self.assertEqual(oracle.current(self.out, METADATA, FSID), status)

    def test_start_preserves_completed_scope_evidence_and_reserves_new_worker(self):
        previous = dict(scope(), done=True, restored=True, ready=True)
        self.seed(previous)
        log = self.out / oracle.token_name(TOKEN, "worker.log")
        log.write_text("previous evidence")
        (self.out / (oracle.NAME + "-worker.log")).symlink_to(log.name)
        oracle.omap.publish(self.out, oracle.NAME + "-input", dict(object=OBJECT))
        def spawn_ready(*args, **kwargs):
            status = oracle.current(self.out, METADATA, FSID)
            self.assertFalse(status["ready"])
            status.update(ready=True, inactive=observation(False))
            oracle.publish(self.out, status)
            return SimpleNamespace(poll=lambda: None)
        with patch.object(oracle, "observe", return_value=observation()), \
                patch.object(oracle, "object_state", return_value=STATE), \
                patch.object(oracle.uuid, "uuid4", return_value=SimpleNamespace(hex=NEXT)), \
                patch.object(oracle.subprocess, "Popen", side_effect=spawn_ready):
            oracle.run("start", self.root, self.out)
        self.assertEqual(log.read_text(), "previous evidence")
        self.assertEqual(oracle.omap.read_json(self.out / (oracle.token_name(TOKEN, "status") + ".json")), previous)
        self.assertEqual(oracle.current(self.out, METADATA, FSID)["token"], NEXT)

    def test_failed_spawn_signals_cleanup_and_preserves_reservation(self):
        oracle.omap.publish(self.out, oracle.NAME + "-input", dict(object=OBJECT))
        with patch.object(oracle, "observe", return_value=observation()), \
                patch.object(oracle, "object_state", return_value=STATE), \
                patch.object(oracle.uuid, "uuid4", return_value=SimpleNamespace(hex=TOKEN)), \
                patch.object(oracle.subprocess, "Popen", side_effect=OSError("spawn failed")), self.assertRaises(OSError):
            oracle.run("start", self.root, self.out)
        self.assertTrue((self.out / (oracle.NAME + "-finish-" + TOKEN)).exists())
        self.assertFalse(oracle.current(self.out, METADATA, FSID)["done"])
        self.assertFalse((self.out / (oracle.NAME + "-control.lock")).exists())

    def test_foreign_control_lock_is_not_removed(self):
        lock = self.out / (oracle.NAME + "-control.lock")
        lock.write_text("other invocation")
        with self.assertRaises(FileExistsError):
            oracle.run("start", self.root, self.out)
        self.assertEqual(lock.read_text(), "other invocation")




class NativeOSDHistoryBudgetTests(unittest.TestCase):
    def test_large_retained_history_preserves_exact_ack_and_ordinary_budget(self):
        unrelated = [dict(description="unrelated-native-read-%d " % i + "x" * 1100, type_data=dict(events=[])) for i in range(1023)]
        history = dict(ops=unrelated + ack_history()["ops"])
        raw = json.dumps(history).encode()
        self.assertGreater(len(raw), oracle.MAX_OUTPUT)
        self.assertLess(len(raw), oracle.MAX_HISTORY_OUTPUT)
        complete = SimpleNamespace(returncode=0, stdout=raw, stderr=b"")
        root = Path("/fixture")
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            with patch.object(oracle.subprocess, "run", return_value=complete) as run:
                loaded = oracle.osd_history(root, out)
            self.assertEqual(run.call_args.args[0], ["ceph", "-c", str(root / "ceph.conf"), "-n", "client.test", "--keyring", str(root / "keyring"), "tell", "osd.0", "dump_historic_ops", "--format", "json"])
            self.assertEqual(run.call_args.kwargs["timeout"], 2)
            receipt = oracle.parse_ack_history(loaded, ack_input(), TOKEN)
            self.assertEqual(receipt["global_id"], 42)
            metrics = oracle.omap.read_json(out / "osd-history-query.json")
            self.assertEqual((metrics["returncode"], metrics["stdout_bytes"], metrics["operation_count"], metrics["classification"]), (0, len(raw), 1024, []))
            self.assertEqual(metrics["stdout_sha256"], hashlib.sha256(raw).hexdigest())
            self.assertNotIn("private-address", (out / "osd-history-query.json").read_text())
            self.assertNotIn("ops", metrics)
            with patch.object(oracle.subprocess, "run", return_value=complete), self.assertRaisesRegex(RuntimeError, "stdout_limit=1048576"):
                oracle.ceph(root, ["osd", "dump"])

    def test_history_failures_are_bounded_classified_and_do_not_copy_text(self):
        cases = [(7, b'{}', b'private-secret-text', ["returncode"]),
                 (0, b'x' * (oracle.MAX_HISTORY_OUTPUT + 1), b'', ["stdout_limit"]),
                 (0, b'{}', b'x' * 4097, ["stderr_limit"]),
                 (0, b'not JSON private-secret-text', b'', ["json"])]
        for rc, stdout, stderr, errors in cases:
            with self.subTest(errors=errors), tempfile.TemporaryDirectory() as directory:
                out=Path(directory)
                with patch.object(oracle.subprocess, "run", return_value=SimpleNamespace(returncode=rc, stdout=stdout, stderr=stderr)), self.assertRaises(RuntimeError):
                    oracle.osd_history(Path('/fixture'), out)
                metrics=oracle.omap.read_json(out / 'osd-history-query.json')
                self.assertEqual(metrics['classification'],errors)
                self.assertEqual((metrics['returncode'],metrics['stdout_bytes'],metrics['stderr_bytes']), (rc,len(stdout),len(stderr)))
                self.assertTrue(metrics['output_complete'])
                self.assertNotIn('private-secret-text',(out/'osd-history-query.json').read_text())

    def test_history_exact_bound_and_timeout_partial_capture(self):
        for stdout,stderr in ((None,None),(b'partial',b'warning'),('partial','warning')):
            with self.subTest(stdout=stdout),tempfile.TemporaryDirectory() as directory:
                out=Path(directory)
                expired=oracle.subprocess.TimeoutExpired(['fixed-native-history'],2,output=stdout,stderr=stderr)
                with patch.object(oracle.subprocess,'run',side_effect=expired),self.assertRaisesRegex(RuntimeError,'history timeout'):
                    oracle.osd_history(Path('/fixture'),out)
                metrics=oracle.omap.read_json(out/'osd-history-query.json')
                self.assertEqual(metrics['classification'],['timeout'])
                self.assertFalse(metrics['output_complete'])
                self.assertIsNone(metrics['returncode'])
                self.assertEqual(metrics['stdout_bytes'],0 if stdout is None else len(stdout))
        raw=b'{}'+b' '*(oracle.MAX_HISTORY_OUTPUT-2)
        with tempfile.TemporaryDirectory() as directory,patch.object(oracle.subprocess,'run',return_value=SimpleNamespace(returncode=0,stdout=raw,stderr=b'')):
            self.assertEqual(oracle.osd_history(Path('/fixture'),Path(directory)),{})


if __name__ == "__main__":
    unittest.main()
