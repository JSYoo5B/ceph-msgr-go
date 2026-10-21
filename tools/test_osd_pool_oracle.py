import json
import pathlib
import sys
import unittest
from unittest.mock import patch

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
        return 0, json.dumps({"svc": {"d1": {"status": self.current()["status"]}}}).encode()


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
