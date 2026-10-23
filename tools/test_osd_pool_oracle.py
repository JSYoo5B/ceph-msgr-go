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


class ManagerStateTest(unittest.TestCase):
    state = {"epoch": 9, "flags": 1, "available": False, "active_name": "", "active_gid": 0}

    def test_down_and_active_projections_use_only_mon(self):
        for state in (self.state, dict(self.state, flags=0, available=True, active_name="a", active_gid=8)):
            rados = Mock()
            rados.mon.return_value = (0, json.dumps(dict(state, unrelated="omit")).encode())
            self.assertEqual(oracle.mode_mgrstate(rados, None, {}, None)["manager_map"], state)
            rados.mon.assert_called_once_with(None, {"prefix": "mgr dump", "format": "json"})
            rados.mgr.assert_not_called()

    def test_refusal_or_missing_fields_cannot_look_like_absence(self):
        rados = Mock()
        rados.mon.return_value = (-13, b'')
        with self.assertRaises(RuntimeError):
            oracle.mode_mgrstate(rados, None, {}, None)
        rados.mon.return_value = (0, b'{}')
        with self.assertRaises(KeyError):
            oracle.mode_mgrstate(rados, None, {}, None)

    def test_invalid_identity_types_and_bounds_fail(self):
        for fields in ({"epoch": 0}, {"epoch": True}, {"flags": -1}, {"flags": 1 << 64},
                       {"available": 0}, {"available": True}, {"active_gid": 5},
                       {"active_name": "a"}, {"active_name": "a\0"}, {"active_gid": True}):
            rados = Mock()
            rados.mon.return_value = (0, json.dumps(dict(self.state, **fields)).encode())
            with self.assertRaises(ValueError, msg=str(fields)):
                oracle.mode_mgrstate(rados, None, {}, None)


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
        rados.mon.return_value = (0, b'{"services":{"svc":{"daemons":{"native":{"gid":8}}}}}')
        with patch.object(oracle, "mode_servicewatch", side_effect=self.observations()) as observe:
            result = oracle.mode_servicecompare(rados, "observer-handle", self.request, None)["comparison"]
        self.assertEqual(result["metadata"], self.request["metadata"])
        self.assertEqual(result["statuses"], self.request["statuses"])
        self.assertTrue(result["removed"])
        self.assertEqual(result["gid"], 8)
        self.assertTrue(result["present_after_shutdown"])
        self.assertEqual(observe.call_args.args[2]["wait_seconds"], 90)
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
        rados.mon.return_value = (0, b'{"services":{}}')
        observed = self.observations()
        observed[-1]["matched"] = False
        with patch.object(oracle, "mode_servicewatch", side_effect=observed):
            with self.assertRaises(RuntimeError):
                oracle.mode_servicecompare(rados, None, self.request, None)
        rados.lib.rados_shutdown.assert_called_once()

    def test_failed_post_shutdown_inventory_cannot_prove_absence(self):
        rados = Mock()
        rados.mon.return_value = (-13, b'')
        with patch.object(oracle, "mode_servicewatch", side_effect=self.observations()[:3]):
            with self.assertRaisesRegex(RuntimeError, "post-shutdown observation failed"):
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


class FakeErasureRados:
    """A scripted MON for erasure-coded pool creation and cleanup.

    It applies profile/pool commands to an in-memory map the way the pinned
    MON reports them, so the oracle's independent read-backs see real shapes.
    """

    def __init__(self, existing_profiles=(), min_size=None, overwrites=True, rule_op="choose_indep", profile_override=None,
                 nonprimary_override=None):
        self.profiles = {name: {"k": "2", "m": "1", "plugin": "isa", "crush-failure-domain": "osd"} for name in existing_profiles}
        self.pools, self.commands, self.last_status = [], [], ""
        self.min_size, self.overwrites, self.rule_op, self.profile_override = min_size, overwrites, rule_op, profile_override
        self.removable, self.optimized, self.nonprimary_override = True, False, nonprimary_override
        self.native = Mock()
        self.lib = Mock()

    def dump(self, cluster):
        return {"epoch": 40, "pools": self.pools}

    def ioctx(self, cluster, name, namespace=""):
        return object()

    def mon(self, cluster, command):
        self.commands.append(command)
        prefix = command["prefix"]
        if prefix == "osd erasure-code-profile get":
            profile = self.profiles.get(command["name"])
            return (-2, b"") if profile is None else (0, json.dumps(profile).encode())
        if prefix == "osd erasure-code-profile set":
            if command["name"] in self.profiles and not command.get("force"):
                return -17, b""
            values = dict(item.split("=", 1) for item in command["profile"])
            values.setdefault("plugin", "isa")
            values.update(self.profile_override or {})
            self.profiles[command["name"]] = values
            return 0, b""
        if prefix == "osd erasure-code-profile rm":
            if self.removable:
                self.profiles.pop(command["name"], None)
            return 0, b""
        if prefix == "osd pool create":
            profile = self.profiles[command["erasure_code_profile"]]
            k, m = int(profile["k"]), int(profile["m"])
            self.pools.append({"pool": 5, "pool_name": command["pool"], "flags": 1, "flags_names": "hashpspool",
                               "type": 3, "size": k + m, "pg_num": command["pg_num"], "crush_rule": 2,
                               "min_size": self.min_size or k + min(1, m - 1),
                               "erasure_code_profile": command["erasure_code_profile"], "nonprimary_shards": "{}"})
            return 0, b""
        if prefix == "osd pool set":
            if command["var"] == "allow_ec_overwrites" and self.overwrites:
                self.pools[0]["flags_names"] += ",ec_overwrites"
            if command["var"] == "allow_ec_optimizations":
                # Pinned enable_pool_ec_optimizations: data raw shards 1..k-1.
                k = int(self.profiles[self.pools[0]["erasure_code_profile"]]["k"])
                self.optimized = True
                self.pools[0]["nonprimary_shards"] = self.nonprimary_override or "{" + ",".join(map(str, range(1, k))) + "}"
            return 0, b""
        if prefix == "osd pool get":
            if command["var"] == "allow_ec_overwrites":
                return 0, json.dumps({"allow_ec_overwrites": self.overwrites}).encode()
            return 0, json.dumps({"allow_ec_optimizations": self.optimized}).encode()
        if prefix == "osd crush rule dump":
            return 0, json.dumps([{"rule_id": 2, "type": 3, "steps": [{"op": "take", "item_name": "default"},
                                                                        {"op": self.rule_op, "num": 0, "type": "osd"}, {"op": "emit"}]}]).encode()
        if prefix in ("config set", "osd pool rm"):
            if prefix == "osd pool rm":
                self.pools = [p for p in self.pools if p["pool_name"] != command["pool"]]
            return 0, b""
        raise AssertionError("unexpected native command %r" % command)


def create(rados, **request):
    base = {"slot": 1, "pg_num": 8, "hashpspool": True, "erasure": True}
    return oracle.mode_create(rados, None, dict(base, **request), None)


class ErasureWidthTest(unittest.TestCase):
    def test_k2m2_uses_its_own_profile_and_verifies_the_native_shape(self):
        rados = FakeErasureRados(existing_profiles=[oracle.EC_PROFILE])
        before = dict(rados.profiles[oracle.EC_PROFILE])
        result = create(rados, width="k2m2", plugin="isa")
        self.assertEqual((result["width"], result["size"], result["min_size"], result["pool_type"]), ("k2m2", 4, 3, 3))
        self.assertEqual(result["erasure_code_profile"], "ceph-msgr-owned-ec-k2m2")
        self.assertEqual((result["ec_profile"]["k"], result["ec_profile"]["m"], result["ec_profile"]["plugin"]), ("2", "2", "isa"))
        self.assertIs(result["ec_overwrites"], True)
        self.assertFalse(result["ec_optimized"])
        setter = [c for c in rados.commands if c["prefix"] == "osd erasure-code-profile set"]
        self.assertEqual(len(setter), 1)
        self.assertNotIn("force", setter[0])
        self.assertEqual(setter[0]["profile"], ["k=2", "m=2", "crush-failure-domain=osd", "plugin=isa"])
        # The shared k=2,m=1 profile is neither read for writing nor changed.
        self.assertEqual(rados.profiles[oracle.EC_PROFILE], before)
        self.assertFalse(any(c.get("name") == oracle.EC_PROFILE for c in rados.commands))

    def test_k2m2_refuses_to_overwrite_an_existing_owned_profile(self):
        rados = FakeErasureRados(existing_profiles=["ceph-msgr-owned-ec-k2m2"])
        with self.assertRaises(RuntimeError):
            create(rados, width="k2m2", plugin="isa")
        self.assertFalse(any(c["prefix"] in ("osd erasure-code-profile set", "osd pool create") for c in rados.commands))

    def test_k2m2_inputs_are_closed(self):
        for request in (dict(width="k2m2"), dict(width="k2m2", plugin="jerasure"), dict(width="k2m2", plugin="jerasure", optimized=True),
                        dict(width="k2m2", plugin="isa", optimized="yes"),
                        dict(width="k4m2", plugin="isa"), dict(width="", plugin="isa"), dict(width="k2m2", plugin="isa", erasure=False, size=3, min_size=2)):
            with self.subTest(request=request), self.assertRaises(ValueError):
                create(FakeErasureRados(), **request)

    def test_k2m2_native_differences_fail_instead_of_being_repaired(self):
        cases = {
            "lowered min_size": dict(min_size=2),
            "missing overwrites": dict(overwrites=False),
            "replicated rule step": dict(rule_op="choose_firstn"),
            "different m": dict(profile_override={"m": "1"}),
            "different plugin": dict(profile_override={"plugin": "jerasure"}),
            "host failure domain": dict(profile_override={"crush-failure-domain": "host"}),
        }
        for name, fake in cases.items():
            rados = FakeErasureRados(**fake)
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                create(rados, width="k2m2", plugin="isa")
            # No step is retried after a failed check.
            self.assertLessEqual(sum(c["prefix"] == "osd erasure-code-profile set" for c in rados.commands), 1)

    def test_optimized_k2m2_reads_back_the_pinned_nonprimary_shards(self):
        rados = FakeErasureRados()
        result = create(rados, width="k2m2", plugin="isa", optimized=True)
        self.assertTrue(result["ec_optimized"])
        self.assertEqual((result["size"], result["min_size"], result["nonprimary_shards"]), (4, 3, "{1}"))
        steps = [c.get("var") for c in rados.commands if c["prefix"] == "osd pool set"]
        self.assertEqual(steps, ["allow_ec_overwrites", "allow_ec_optimizations"])
        self.assertEqual(oracle.ec_nonprimary_shards(2, True), {1})
        self.assertEqual(oracle.ec_nonprimary_shards(4, True), {1, 2, 3})
        self.assertEqual(oracle.ec_nonprimary_shards(2, False), set())

    def test_optimized_k2m2_nonprimary_differences_fail(self):
        for value in ("{}", "{2}", "{1,2}"):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                create(FakeErasureRados(nonprimary_override=value), width="k2m2", plugin="isa", optimized=True)
        # A nonoptimized pool must report no nonprimary shards either.
        rados = FakeErasureRados()
        self.assertEqual(create(rados, width="k2m2", plugin="isa")["nonprimary_shards"], "{}")

    def test_k2m1_keeps_its_profile_and_force_set(self):
        rados = FakeErasureRados()
        result = create(rados, plugin="jerasure")
        self.assertEqual((result["width"], result["size"], result["min_size"], result["erasure_code_profile"]), ("k2m1", 3, 2, oracle.EC_PROFILE))
        setter = [c for c in rados.commands if c["prefix"] == "osd erasure-code-profile set"]
        self.assertEqual(setter, [{"prefix": "osd erasure-code-profile set", "name": oracle.EC_PROFILE,
                                   "profile": ["k=2", "m=1", "crush-failure-domain=osd", "plugin=jerasure"], "force": True}])

    def test_cleanup_removes_every_owned_profile_and_reports_survivors(self):
        rados = FakeErasureRados(existing_profiles=list(oracle.EC_PROFILES))
        result = oracle.mode_cleanup(rados, None, {}, None)
        self.assertTrue(result["pools_removed"])
        self.assertEqual(result["ec_profiles_remaining"], [])
        self.assertEqual({c["name"] for c in rados.commands if c["prefix"] == "osd erasure-code-profile rm"}, set(oracle.EC_PROFILES))
        stuck = FakeErasureRados(existing_profiles=list(oracle.EC_PROFILES))
        stuck.removable = False
        self.assertEqual(oracle.mode_cleanup(stuck, None, {}, None)["ec_profiles_remaining"], list(oracle.EC_PROFILES))
        self.assertEqual(oracle.EC_PROFILES, ("ceph-msgr-owned-ec", "ceph-msgr-owned-ec-k2m2"))


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
