import json
import pathlib
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle as oracle


class FakeServiceRados:
    """Answers service dump (MON) and service status (MGR) from scripted states."""

    def __init__(self, states):
        self.states, self.polls, self.last_status = list(states), 0, ""

    def current(self):
        return self.states[min(max(self.polls - 1, 0), len(self.states) - 1)]

    def mon(self, cluster, command):
        assert command == {"prefix": "service dump", "format": "json"}
        self.polls += 1
        daemons = {} if self.current() is None else {"d1": self.current()["dump"]}
        return 0, json.dumps({"epoch": 7, "services": {"svc": {"daemons": daemons}}}).encode()

    def mgr(self, cluster, command):
        assert command == {"prefix": "service status", "format": "json"}
        # The pinned formatter prints the named top-level section without its name.
        entry = {"status": self.current()["status"]}
        if "beacon" in self.current():
            entry["last_beacon"] = self.current()["beacon"]
        return 0, json.dumps({"svc": {"d1": entry}}).encode()


def watch(states, **request):
    base = {"service": "svc", "daemon": "d1", "expect": "present", "wait_seconds": 1}
    return oracle.mode_servicewatch(FakeServiceRados(states), None, dict(base, **request), None)


class ServiceWatchTest(unittest.TestCase):
    present = {"dump": {"gid": 5, "metadata": {"zone": "a"}, "task_status": {"t": "1"}}, "status": {"state": "up"}}

    def test_matches_a_registered_daemon_with_expected_fields(self):
        result = watch([self.present], metadata={"zone": "a"}, status={"state": "up"}, task_status={"t": "1"})
        self.assertTrue(result["matched"])
        self.assertEqual(result["entry"]["gid"], 5)
        self.assertEqual(result["dump_epoch"], 7)

    def test_a_mismatch_reports_the_last_observation_instead_of_matching(self):
        for request in ({"metadata": {"zone": "b"}}, {"status": {"state": "busy"}}, {"task_status": {"t": "2"}}):
            result = watch([self.present], **request)
            self.assertFalse(result["matched"], request)
            self.assertEqual(result["entry"]["gid"], 5)

    def test_exact_status_requires_old_keys_to_disappear_and_can_require_empty(self):
        self.assertTrue(watch([self.present], status={}, exact_status=False)["matched"])
        self.assertFalse(watch([self.present], status={}, exact_status=True)["matched"])
        cleared = dict(self.present, status={})
        self.assertTrue(watch([cleared], status={}, exact_status=True)["matched"])
        self.assertFalse(watch([dict(self.present, status={"state": "up", "old": "x"})],
                               status={"state": "up"}, exact_status=True)["matched"])
        with self.assertRaises(ValueError):
            watch([self.present], exact_status=1)

    def test_unchanged_or_missing_beacon_cannot_prove_receipt(self):
        for state in (self.present, dict(self.present, beacon="2026-01-01T00:00:01.000000+0000")):
            self.assertFalse(watch([state], after_beacon="2026-01-01T00:00:01.000000+0000")["matched"])
        self.assertTrue(watch([dict(self.present, beacon="2026-01-01T00:00:02.000000+0000")],
                              after_beacon="2026-01-01T00:00:01.000000+0000")["matched"])
        with self.assertRaises(ValueError):
            watch([self.present], after_beacon=1)

    def test_absent_and_present_expectations(self):
        self.assertTrue(watch([None], expect="absent")["matched"])
        self.assertFalse(watch([self.present], expect="absent")["matched"])
        self.assertFalse(watch([None])["matched"])

    def test_waits_for_a_late_registration(self):
        self.assertTrue(watch([None, self.present], wait_seconds=5, metadata={"zone": "a"})["matched"])

    def test_inputs_are_bounded_and_validated(self):
        for bad in ({"service": "a b"}, {"daemon": ""}, {"daemon": "x" * 65}, {"expect": "maybe"}, {"wait_seconds": 0},
                    {"wait_seconds": 91}, {"metadata": {"k": 1}}, {"status": {str(i): "v" for i in range(17)}},
                    {"task_status": {"k": "v" * 257}}, {"metadata": []},
                    {"stable_seconds": True}, {"stable_seconds": -1},
                    {"stable_seconds": 11}, {"stable_seconds": 1}):
            with self.assertRaises((ValueError, KeyError, TypeError), msg=str(bad)):
                watch([self.present], **bad)

    def test_stable_absence_resets_after_reappearance(self):
        with patch("time.monotonic", side_effect=range(20)), patch("time.sleep"):
            result = watch([None, None, self.present, None, None, None, None],
                           expect="absent", wait_seconds=12, stable_seconds=3)
        self.assertTrue(result["matched"])
        self.assertEqual(result["matched_for_seconds"], 3)
        self.assertEqual(result["observations"], 7)

    def test_deadline_does_not_accept_a_brief_absence(self):
        with patch("time.monotonic", side_effect=range(20)), patch("time.sleep"):
            result = watch([self.present, self.present, None, None],
                           expect="absent", wait_seconds=4, stable_seconds=3)
        self.assertFalse(result["matched"])
        self.assertEqual(result["matched_for_seconds"], 1)

    def test_actual_go_request_fixtures_omit_unset_maps_and_match_pinned_states(self):
        path = pathlib.Path(__file__).resolve().parents[1] / "integration/testdata/mgr-service-watch.json"
        fixtures = json.loads(path.read_text(encoding="utf-8"))
        reported = {"dump": {"gid": 5, "metadata": {"zone": "a"}, "task_status": {"t": "2"}},
                    "status": {"state": "busy"}}
        # MMgrUpdate changes daemon_state, not the initial ServiceMap metadata.
        # MMgrReport changes service status and the map's task status instead.
        for name, state in (("open", self.present), ("report", reported), ("absent", None)):
            request = fixtures[name]
            self.assertNotIn(None, request.values())
            self.assertTrue(oracle.mode_servicewatch(FakeServiceRados([state]), None, request, None)["matched"], name)


class ServiceComparisonTest(unittest.TestCase):
    request = {"service": "svc", "daemon": "native", "metadata": {"zone": "rack-λ"},
               "statuses": {"initial": {"state": "up", "old": "x"}, "replacement": {"state": "busy"}, "empty": {}}}

    def observations(self):
        result = []
        for phase in ("initial", "replacement", "empty"):
            result.append({"matched": True, "entry": {"gid": 8, "start_epoch": 3,
                "metadata": {"zone": "rack-λ", "hostname": "synthetic-host", "system_extra": "omit"}},
                "dump_epoch": 4, "status_entry": {"status": self.request["statuses"][phase]}})
        return result + [{"matched": True, "matched_for_seconds": 3}]

    def test_independent_handle_status_phases_redaction_and_cleanup(self):
        rados = Mock()
        rados.connect.return_value = "registration-handle"
        with patch.object(oracle, "mode_servicewatch", side_effect=self.observations()) as observe:
            result = oracle.mode_servicecompare(rados, "observer-handle", self.request, None)["comparison"]
        self.assertEqual(result["metadata"], self.request["metadata"])
        self.assertEqual(result["statuses"], self.request["statuses"])
        self.assertTrue(result["removed"])
        self.assertEqual(result["gid"], 8)
        self.assertEqual(rados.lib.rados_service_register.call_count, 1)
        self.assertEqual(rados.lib.rados_service_update_status.call_count, 3)
        self.assertEqual(rados.lib.rados_service_update_status.call_args.args, ("registration-handle", b"\0"))
        rados.lib.rados_shutdown.assert_called_once_with("registration-handle")
        self.assertTrue(all(call.args[1] == "observer-handle" for call in observe.call_args_list))
        self.assertTrue(all(call.args[2]["exact_status"] for call in observe.call_args_list[:3]))

    def test_failed_phase_and_identity_change_never_retry_mutations(self):
        for fault in ("refusal", "mismatch", "identity"):
            rados = Mock()
            rados.connect.return_value = "registration-handle"
            observed = self.observations()
            if fault == "refusal":
                rados.native.check.side_effect = [None, RuntimeError("refused")]
            elif fault == "mismatch":
                observed[0]["matched"] = False
            else:
                observed[1]["entry"]["gid"] = 9
            with patch.object(oracle, "mode_servicewatch", side_effect=observed):
                with self.assertRaises(RuntimeError):
                    oracle.mode_servicecompare(rados, "observer-handle", self.request, None)
            rados.lib.rados_service_register.assert_called_once()
            self.assertLessEqual(rados.lib.rados_service_update_status.call_count, 2)
            rados.lib.rados_shutdown.assert_called_once_with("registration-handle")

    def test_invalid_c_dictionaries_are_rejected_before_native_connection(self):
        for bad in ({"": "x"}, {"x\0y": "v"}, {"x": "v\0z"}, {"x": 1}):
            rados = Mock()
            with self.assertRaises(ValueError):
                oracle.mode_servicecompare(rados, None, dict(self.request, metadata=bad), None)
            rados.connect.assert_not_called()
        self.assertEqual(oracle.service_dictionary({"z": "", "a": "λ"}, "status"), b"a\0\xce\xbb\0z\0\0\0")

    def test_shutdown_return_without_observed_removal_is_failure(self):
        rados = Mock()
        observed = self.observations()
        observed[-1]["matched"] = False
        with patch.object(oracle, "mode_servicewatch", side_effect=observed):
            with self.assertRaises(RuntimeError):
                oracle.mode_servicecompare(rados, None, self.request, None)
        rados.lib.rados_shutdown.assert_called_once()


class ServiceMetadataTest(unittest.TestCase):
    def observe(self, responses, metadata, **fields):
        rados = Mock()
        rados.mgr.side_effect = responses
        request = dict(action="observe", service="svc", daemon="d1", metadata=metadata, wait_seconds=2)
        request.update(fields)
        with patch("time.monotonic", side_effect=range(20)), patch("time.sleep"):
            result = oracle.mode_servicemetadata(rados, None, request, None)
        return result, rados

    def test_replacement_requires_removing_old_keys(self):
        result, rados = self.observe([(0, b'{"zone":"b","old":"a"}'),
                                     (0, b'{"zone":"b","hostname":""}')], {"zone": "b"})
        self.assertTrue(result["matched"])
        self.assertEqual(result["observations"], 2)
        self.assertEqual(rados.mgr.call_args.args[1], {
            "prefix": "mgr self-test eval",
            "s": "print(__import__('json').dumps(mgr.get_metadata('svc', 'd1')))"})
        rados.mon.assert_not_called()

    def test_empty_metadata_is_distinct_from_a_missing_daemon(self):
        result, _ = self.observe([(0, b'null'), (0, b'{"hostname":""}')], {})
        self.assertTrue(result["matched"])
        self.assertEqual(result["observations"], 2)

    def test_refusal_and_old_metadata_cannot_pass(self):
        for response in ((-13, b''), (0, b'{"zone":"a"}'), (0, b'null')):
            result, _ = self.observe([response, response], {"zone": "b"})
            self.assertFalse(result["matched"])

    def test_invalid_inputs_cannot_form_an_expression(self):
        for fields in ({"service": "svc');print(1)#"}, {"daemon": "a b"},
                       {"wait_seconds": 46}, {"wait_seconds": True},
                       {"metadata": {"hostname": "x"}}, {"metadata": None}):
            rados = Mock()
            request = dict(action="observe", service="svc", daemon="d1", metadata={}, wait_seconds=2)
            request.update(fields)
            with self.assertRaises((ValueError, TypeError)):
                oracle.mode_servicemetadata(rados, None, request, None)
            rados.mgr.assert_not_called()
            rados.mon.assert_not_called()

    def test_enable_is_once_only_readiness_is_polled(self):
        rados = Mock()
        rados.mon.side_effect = [(0, b'{"enabled_modules":[]}'), (0, b'')]
        rados.mgr.side_effect = [(-22, b''), (0, b'3')]
        with patch("time.sleep"):
            result = oracle.mode_servicemetadata(rados, None, {"action": "prepare"}, None)
        self.assertTrue(result["module_enabled_by_test"])
        self.assertTrue(result["ready"])
        self.assertEqual(rados.mon.call_count, 2)
        self.assertEqual(rados.mgr.call_count, 2)

    def test_existing_module_is_preserved_and_failed_mutations_are_not_retried(self):
        rados = Mock()
        rados.mon.return_value = (0, b'{"enabled_modules":["selftest"]}')
        rados.mgr.return_value = (0, b'3')
        result = oracle.mode_servicemetadata(rados, None, {"action": "prepare"}, None)
        self.assertFalse(result["module_enabled_by_test"])
        self.assertEqual(rados.mon.call_count, 1)
        for action in ("prepare", "restore"):
            rados = Mock()
            rados.mon.side_effect = [(0, json.dumps({"enabled_modules": [] if action == "prepare" else ["selftest"]}).encode()),
                                     (-13, b'')]
            with self.assertRaises(RuntimeError):
                oracle.mode_servicemetadata(rados, None, {"action": action}, None)
            self.assertEqual(rados.mon.call_count, 2)

    def test_restoration_requires_observed_removal(self):
        for modules, passes in (([], True), (["selftest"], False)):
            rados = Mock()
            rados.mon.side_effect = [(0, b'{"enabled_modules":["selftest"]}'), (0, b''),
                                     (0, json.dumps({"enabled_modules": modules}).encode())]
            if passes:
                self.assertTrue(oracle.mode_servicemetadata(rados, None, {"action": "restore"}, None)["restored"])
            else:
                with self.assertRaises(RuntimeError):
                    oracle.mode_servicemetadata(rados, None, {"action": "restore"}, None)


class PoolOracleTest(unittest.TestCase):
    def test_only_two_owned_pool_names_exist(self):
        self.assertEqual(oracle.pool_name(1), "ceph-msgr-owned-pool-1")
        self.assertEqual(oracle.pool_name(2), "ceph-msgr-owned-pool-2")
        for bad in (0, 3, -1, True, "1", None, 1.0):
            with self.assertRaises(ValueError):
                oracle.pool_name(bad)

    def test_modes_are_closed_and_have_handlers(self):
        self.assertEqual(set(oracle.MODES), set(oracle.HANDLERS))
        with self.assertRaises(SystemExit):
            oracle.main(["x", "delete", "/root", "/out"])
        with self.assertRaises(SystemExit):
            oracle.main(["x", "create", "/root"])

    def test_integer_bounds(self):
        self.assertEqual(oracle.integer(3, 1, 3, "size"), 3)
        for bad in (0, 4, True, "3"):
            with self.assertRaises(ValueError):
                oracle.integer(bad, 1, 3, "size")

    def test_object_identity_rejects_nul_and_empty(self):
        with self.assertRaises(ValueError):
            oracle.object_request({"object": "a\0b"})
        with self.assertRaises(ValueError):
            oracle.object_request({"object": ""})
        self.assertEqual(oracle.object_request({"object": "o", "namespace": "ns"}), (b"o", "ns"))


if __name__ == "__main__":
    unittest.main()
