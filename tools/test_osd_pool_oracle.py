import json
import pathlib
import re
import sys
import tempfile
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


OUT_ROOT = None


def setUpModule():
    global OUT_ROOT
    OUT_ROOT = tempfile.TemporaryDirectory()


def tearDownModule():
    OUT_ROOT.cleanup()


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
        # The central configuration as `config dump` lists it, and the CRUSH
        # rules by name: the start state has the replicated default only.
        self.config, self.rules, self.rule_removable, self.config_removable = [], ["replicated_rule"], True, True
        self.native = Mock()
        self.lib = Mock()
        # The fixture output directory every oracle run of one fixture shares.
        self.out = pathlib.Path(tempfile.mkdtemp(dir=OUT_ROOT.name))

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
        if prefix == "osd pool create" and command["pool_type"] == "replicated":
            self.pools.append({"pool": 6, "pool_name": command["pool"], "flags": 1, "flags_names": "hashpspool",
                               "type": 1, "size": 3, "pg_num": command["pg_num"], "crush_rule": 0, "min_size": 2})
            return 0, b""
        if prefix == "osd pool create":
            profile = self.profiles[command["erasure_code_profile"]]
            k, m = int(profile["k"]), int(profile["m"])
            # Pinned OSDMonitor: an erasure pool created without a rule name
            # gets a rule named after the pool.
            if command["pool"] not in self.rules:
                self.rules.append(command["pool"])
            self.pools.append({"pool": 5, "pool_name": command["pool"], "flags": 1, "flags_names": "hashpspool",
                               "type": 3, "size": k + m, "pg_num": command["pg_num"], "crush_rule": 2,
                               "min_size": self.min_size or k + min(1, m - 1),
                               "erasure_code_profile": command["erasure_code_profile"], "nonprimary_shards": "{}"})
            return 0, b""
        if prefix == "osd pool set" and command["var"] in ("size", "min_size"):
            pool = next(p for p in self.pools if p["pool_name"] == command["pool"])
            pool[command["var"]] = int(command["val"])
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
        if prefix == "config dump":
            return 0, json.dumps(self.config).encode()
        if prefix == "config set":
            self.config = [e for e in self.config if (e["section"], e["name"]) != (command["who"], command["name"])]
            self.config.append({"section": command["who"], "name": command["name"], "value": command["value"], "mask": ""})
            return 0, b""
        if prefix == "config rm":
            if self.config_removable:
                self.config = [e for e in self.config if (e["section"], e["name"]) != (command["who"], command["name"])]
            return 0, b""
        if prefix == "osd crush rule ls":
            return 0, json.dumps(self.rules).encode()
        if prefix == "osd crush rule rm":
            if self.rule_removable and not any(p["pool_name"] == command["name"] for p in self.pools):
                self.rules.remove(command["name"])
            return 0, b""
        if prefix == "osd pool rm":
            if prefix == "osd pool rm":
                self.pools = [p for p in self.pools if p["pool_name"] != command["pool"]]
            return 0, b""
        raise AssertionError("unexpected native command %r" % command)


def create(rados, **request):
    base = {"slot": 1, "pg_num": 8, "hashpspool": True, "erasure": True}
    return oracle.mode_create(rados, None, dict(base, **request), rados.out)


def cleanup(rados):
    return oracle.mode_cleanup(rados, None, {}, rados.out)


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

    def test_cleanup_removes_every_profile_this_run_created_and_reports_survivors(self):
        rados = FakeErasureRados()
        create(rados, width="k2m2", plugin="isa")
        create(rados, slot=2, plugin="jerasure")
        result = cleanup(rados)
        self.assertTrue(result["pools_removed"])
        self.assertEqual(result["ec_profiles_remaining"], [])
        self.assertEqual({c["name"] for c in rados.commands if c["prefix"] == "osd erasure-code-profile rm"}, set(oracle.EC_PROFILES))
        stuck = FakeErasureRados()
        create(stuck, width="k2m2", plugin="isa")
        create(stuck, slot=2, plugin="jerasure")
        stuck.removable = False
        self.assertEqual(cleanup(stuck)["ec_profiles_remaining"], sorted(oracle.EC_PROFILES))
        self.assertEqual(oracle.EC_PROFILES, ("ceph-msgr-owned-ec", "ceph-msgr-owned-ec-k2m2"))

    def test_cleanup_restores_the_pool_delete_entry_it_found(self):
        unrelated = {"section": "osd", "name": "osd_recovery_sleep", "value": "0.25", "mask": ""}
        for prior in (None, "false", "true"):
            with self.subTest(prior=prior):
                rados = FakeErasureRados()
                create(rados, width="k2m2", plugin="isa")
                rados.config = [dict(unrelated)] + ([{"section": "mon", "name": "mon_allow_pool_delete", "value": prior, "mask": ""}]
                                                    if prior is not None else [])
                before = [dict(e) for e in rados.config]
                result = cleanup(rados)
                self.assertTrue(result["pools_removed"])
                self.assertTrue(result["pool_delete_config_restored"])
                self.assertEqual(result["pool_delete_config_prior"], prior)
                self.assertEqual(rados.config, before)
                # Deletion was enabled before the pool was removed, unless it already was.
                prefixes = [c["prefix"] for c in rados.commands]
                if prior != "true":
                    self.assertLess(prefixes.index("config set"), prefixes.index("osd pool rm"))
                else:
                    self.assertNotIn("config set", prefixes)

    def test_cleanup_reports_a_pool_delete_entry_it_could_not_restore(self):
        rados = FakeErasureRados()
        create(rados, width="k2m2", plugin="isa")
        rados.config_removable = False
        self.assertFalse(cleanup(rados)["pool_delete_config_restored"])

    def test_cleanup_restores_the_entry_when_pool_removal_fails(self):
        rados = FakeErasureRados()
        create(rados, width="k2m2", plugin="isa")
        original = rados.mon
        def refuse(cluster, command):
            if command["prefix"] == "osd pool rm":
                rados.commands.append(command)
                return -1, b""
            return original(cluster, command)
        rados.mon = refuse
        with self.assertRaises(RuntimeError):
            cleanup(rados)
        self.assertEqual(rados.config, [])
        self.assertEqual([c["prefix"] for c in rados.commands][-2:], ["config rm", "config dump"])

    def test_cleanup_removes_only_the_rules_of_owned_pools(self):
        rados = FakeErasureRados()
        create(rados, width="k2m2", plugin="isa")
        rados.rules.append("someone-elses-rule")
        self.assertIn(oracle.pool_name(1), rados.rules)
        result = cleanup(rados)
        self.assertEqual(result["crush_rules_remaining"], [])
        self.assertEqual(rados.rules, ["replicated_rule", "someone-elses-rule"])
        self.assertEqual([c["name"] for c in rados.commands if c["prefix"] == "osd crush rule rm"], [oracle.pool_name(1)])
        stuck = FakeErasureRados()
        create(stuck, width="k2m2", plugin="isa")
        stuck.rule_removable = False
        self.assertEqual(cleanup(stuck)["crush_rules_remaining"], [oracle.pool_name(1)])

    def test_create_refuses_an_existing_rule_named_after_the_owned_pool(self):
        rados = FakeErasureRados()
        rados.rules.append(oracle.pool_name(1))
        with self.assertRaises(RuntimeError):
            create(rados, width="k2m2", plugin="isa")
        self.assertFalse(any(c["prefix"] in ("osd erasure-code-profile set", "osd pool create") for c in rados.commands))


MUTATIONS = ("osd pool rm", "osd crush rule rm", "osd erasure-code-profile rm", "osd erasure-code-profile set", "config set", "config rm")


def mutations(rados):
    return [c for c in rados.commands if c["prefix"] in MUTATIONS]


def fail_once(rados, prefix, creates=False):
    """Make the first command with prefix report failure; with creates, the
    MON applied it anyway (a reply lost after the commit)."""
    original, state = rados.mon, {"failed": False}
    def mon(cluster, command):
        if command["prefix"] == prefix and not state["failed"]:
            state["failed"] = True
            if creates:
                original(cluster, command)
            else:
                rados.commands.append(command)
            return -110, b""
        return original(cluster, command)
    rados.mon = mon


def script_reads(rados, prefix, codes):
    """Answer the next reads with prefix by the given native codes, in
    order; None passes a read through to the fake MON."""
    original, queue = rados.mon, list(codes)
    def mon(cluster, command):
        if command["prefix"] == prefix and queue:
            code = queue.pop(0)
            if code is not None:
                rados.commands.append(command)
                return code, b""
        return original(cluster, command)
    rados.mon = mon


class OwnedClaimTest(unittest.TestCase):
    """Cleanup removes or changes only what this run's create made."""

    def assertNothingTouched(self, rados, before):
        # Only reads after the cleanup started: no removal, no config change.
        self.assertEqual(mutations(rados)[before:], [])

    def test_refused_create_then_cleanup_keeps_a_rule_that_predates_the_test(self):
        for width in ("k2m2", "k2m1"):
            with self.subTest(width=width):
                rados = FakeErasureRados()
                rados.rules.append(oracle.pool_name(1))
                with self.assertRaises(RuntimeError):
                    create(rados, width=width, plugin="isa")
                before = len(mutations(rados))
                result = cleanup(rados)
                self.assertNothingTouched(rados, before)
                self.assertEqual(rados.rules, ["replicated_rule", oracle.pool_name(1)])
                self.assertEqual(result["claimed"], {"pools": [], "crush_rules": [], "ec_profiles": []})
                self.assertEqual(result["crush_rules_remaining"], [])
                self.assertEqual(result["unclaimed_present"]["crush_rules"], [oracle.pool_name(1)])
                self.assertTrue(result["pool_delete_config_restored"])

    def test_a_rule_that_predates_the_test_for_the_other_slot_is_kept(self):
        rados = FakeErasureRados()
        rados.rules.append(oracle.pool_name(2))
        create(rados, width="k2m2", plugin="isa")
        result = cleanup(rados)
        self.assertEqual([c["name"] for c in rados.commands if c["prefix"] == "osd crush rule rm"], [oracle.pool_name(1)])
        self.assertEqual(rados.rules, ["replicated_rule", oracle.pool_name(2)])
        self.assertEqual(result["crush_rules_remaining"], [])
        self.assertEqual(result["unclaimed_present"]["crush_rules"], [oracle.pool_name(2)])

    def test_create_records_only_what_it_observed_created(self):
        rados = FakeErasureRados()
        create(rados, width="k2m2", plugin="isa")
        claims = json.loads((rados.out / oracle.CLAIMS_FILE).read_text())
        self.assertEqual(claims, {"pools": [oracle.pool_name(1)], "crush_rules": [oracle.pool_name(1)],
                                  "ec_profiles": ["ceph-msgr-owned-ec-k2m2"]})
        replicated = FakeErasureRados()
        oracle.mode_create(replicated, None, {"slot": 2, "pg_num": 8, "size": 3, "min_size": 2, "hashpspool": True}, replicated.out)
        claims = json.loads((replicated.out / oracle.CLAIMS_FILE).read_text())
        self.assertEqual(claims, {"pools": [oracle.pool_name(2)], "crush_rules": [], "ec_profiles": []})

    def test_partial_creation_failure_claims_exactly_what_exists(self):
        # (failing step, MON applied it anyway, claimed pools, rules, profiles)
        cases = [
            ("osd erasure-code-profile set", False, [], [], []),
            ("osd erasure-code-profile set", True, [], [], ["ceph-msgr-owned-ec-k2m2"]),
            ("osd pool create", False, [], [], ["ceph-msgr-owned-ec-k2m2"]),
            ("osd pool create", True, [oracle.pool_name(1)], [oracle.pool_name(1)], ["ceph-msgr-owned-ec-k2m2"]),
            ("osd pool set", False, [oracle.pool_name(1)], [oracle.pool_name(1)], ["ceph-msgr-owned-ec-k2m2"]),
        ]
        for prefix, applied, pools, rules, profiles in cases:
            with self.subTest(prefix=prefix, applied=applied):
                rados = FakeErasureRados()
                fail_once(rados, prefix, creates=applied)
                with self.assertRaises(RuntimeError):
                    create(rados, width="k2m2", plugin="isa")
                result = cleanup(rados)
                self.assertEqual(result["claimed"], {"pools": pools, "crush_rules": rules, "ec_profiles": profiles})
                # Everything the run created is gone again; nothing else was there.
                self.assertEqual((rados.pools, rados.rules, rados.profiles, rados.config), ([], ["replicated_rule"], {}, []))
                self.assertEqual([c["name"] for c in rados.commands if c["prefix"] == "osd crush rule rm"], rules)
                self.assertEqual([c["name"] for c in rados.commands if c["prefix"] == "osd erasure-code-profile rm"], profiles)
                self.assertEqual(bool(pools), any(c["prefix"] == "config set" for c in rados.commands))
                self.assertTrue(result["pools_removed"] and result["pool_delete_config_restored"])
                self.assertEqual(result["unclaimed_present"], {"pools": [], "crush_rules": [], "ec_profiles": []})
                self.assertEqual(result["ec_profiles_unread"], {})

    def test_a_failed_set_and_a_failed_read_claim_no_profile(self):
        # Absent before the set; the set times out without applying, and the
        # read after it fails. The name is unknown, not this run's.
        for code in (-5, -110, -1):
            with self.subTest(code=code):
                rados = FakeErasureRados()
                fail_once(rados, "osd erasure-code-profile set")
                script_reads(rados, "osd erasure-code-profile get", [None, code])
                with self.assertRaises(RuntimeError) as refused:
                    create(rados, width="k2m2", plugin="isa")
                result = cleanup(rados)
                self.assertFalse(any(c["prefix"] == "osd erasure-code-profile rm" for c in rados.commands))
                self.assertEqual(result["claimed"], {"pools": [], "crush_rules": [], "ec_profiles": []})
                # The create names the unread profile and its read error.
                self.assertRegex(str(refused.exception), "could not be read.*ec_profiles ceph-msgr-owned-ec-k2m2.*%d" % code)
                self.assertEqual(mutations(rados), [{"prefix": "osd erasure-code-profile set", "name": "ceph-msgr-owned-ec-k2m2",
                                                     "profile": ["k=2", "m=2", "crush-failure-domain=osd", "plugin=isa"]}])

    def test_an_applied_set_with_a_failed_read_stays_unclaimed_and_is_reported(self):
        rados = FakeErasureRados()
        fail_once(rados, "osd erasure-code-profile set", creates=True)
        script_reads(rados, "osd erasure-code-profile get", [None, -5])
        with self.assertRaises(RuntimeError):
            create(rados, width="k2m2", plugin="isa")
        result = cleanup(rados)
        self.assertFalse(any(c["prefix"] == "osd erasure-code-profile rm" for c in rados.commands))
        self.assertIn("ceph-msgr-owned-ec-k2m2", rados.profiles)
        self.assertEqual(result["unclaimed_present"]["ec_profiles"], ["ceph-msgr-owned-ec-k2m2"])

    def test_an_unreadable_profile_refuses_the_create(self):
        for width, plugin in (("k2m2", "isa"), ("k2m1", "jerasure")):
            with self.subTest(width=width):
                rados = FakeErasureRados()
                script_reads(rados, "osd erasure-code-profile get", [-5])
                with self.assertRaisesRegex(RuntimeError, "presence is unknown"):
                    create(rados, width=width, plugin=plugin)
                self.assertEqual(mutations(rados), [])
                self.assertFalse(any(c["prefix"] == "osd pool create" for c in rados.commands))

    def test_a_failed_read_back_after_the_set_names_the_read(self):
        rados = FakeErasureRados()
        # Before, the claim read after the set, then the read-back.
        script_reads(rados, "osd erasure-code-profile get", [None, None, -5])
        with self.assertRaisesRegex(RuntimeError, "read-back failed: -5"):
            create(rados, width="k2m2", plugin="isa")
        self.assertEqual(cleanup(rados)["claimed"]["ec_profiles"], ["ceph-msgr-owned-ec-k2m2"])

    def test_a_failed_pool_read_still_checks_the_rule_and_claims_neither_unread(self):
        rados = FakeErasureRados()
        original_dump, state = rados.dump, {"calls": 0}
        def dump(cluster):
            # The refusal check before the steps reads once; the claim read
            # after `osd pool create` fails.
            state["calls"] += 1
            if state["calls"] == 2:
                raise RuntimeError("native osd dump failed: -5")
            return original_dump(cluster)
        rados.dump = dump
        with self.assertRaisesRegex(RuntimeError, "osd pool create.*pools ceph-msgr-owned-pool-1"):
            create(rados, width="k2m2", plugin="isa")
        # The rule's own read succeeded, so the rule is claimed; the pool is not.
        self.assertEqual(json.loads((rados.out / oracle.CLAIMS_FILE).read_text()),
                         {"pools": [], "crush_rules": [oracle.pool_name(1)], "ec_profiles": ["ceph-msgr-owned-ec-k2m2"]})

    def test_a_failed_rule_read_claims_no_rule(self):
        rados = FakeErasureRados()
        # The refusal check before the steps reads once; the claim read fails.
        script_reads(rados, "osd crush rule ls", [None, -5])
        with self.assertRaisesRegex(RuntimeError, "crush_rules ceph-msgr-owned-pool-1"):
            create(rados, width="k2m2", plugin="isa")
        self.assertEqual(json.loads((rados.out / oracle.CLAIMS_FILE).read_text()),
                         {"pools": [oracle.pool_name(1)], "crush_rules": [], "ec_profiles": ["ceph-msgr-owned-ec-k2m2"]})

    def test_cleanup_reports_an_unreadable_profile_apart_from_survivors(self):
        rados = FakeErasureRados()
        create(rados, width="k2m2", plugin="isa")
        # The removal applies; the read after it fails for the claimed
        # profile and succeeds (absent) for the other.
        script_reads(rados, "osd erasure-code-profile get", [None, -5])
        result = cleanup(rados)
        self.assertEqual(result["ec_profiles_unread"], {"ceph-msgr-owned-ec-k2m2": -5})
        self.assertEqual(result["ec_profiles_remaining"], [])
        self.assertEqual(result["unclaimed_present"]["ec_profiles"], [])
        stuck = FakeErasureRados(existing_profiles=[oracle.EC_PROFILE])
        script_reads(stuck, "osd erasure-code-profile get", [-110])
        result = cleanup(stuck)
        self.assertEqual(result["ec_profiles_unread"], {oracle.EC_PROFILE: -110})
        self.assertEqual(result["unclaimed_present"]["ec_profiles"], [])
        self.assertEqual(mutations(stuck), [])

    def test_replicated_cleanup_never_touches_rules_or_profiles(self):
        rados = FakeErasureRados(existing_profiles=list(oracle.EC_PROFILES))
        rados.rules += [oracle.pool_name(1), oracle.pool_name(2)]
        profiles = {k: dict(v) for k, v in rados.profiles.items()}
        oracle.mode_create(rados, None, {"slot": 1, "pg_num": 8, "size": 3, "min_size": 2, "hashpspool": True}, rados.out)
        result = cleanup(rados)
        self.assertEqual([c["pool"] for c in rados.commands if c["prefix"] == "osd pool rm"], [oracle.pool_name(1)])
        self.assertFalse(any(c["prefix"] in ("osd crush rule rm", "osd erasure-code-profile rm") for c in rados.commands))
        self.assertEqual((rados.rules, rados.profiles), (["replicated_rule", oracle.pool_name(1), oracle.pool_name(2)], profiles))
        self.assertEqual((result["crush_rules_remaining"], result["ec_profiles_remaining"]), ([], []))
        self.assertEqual(result["unclaimed_present"]["ec_profiles"], sorted(oracle.EC_PROFILES))

    def test_k2m1_overwrites_only_a_profile_this_run_created(self):
        rados = FakeErasureRados(existing_profiles=[oracle.EC_PROFILE])
        before = dict(rados.profiles[oracle.EC_PROFILE])
        with self.assertRaises(RuntimeError):
            create(rados, plugin="jerasure")
        self.assertFalse(any(c["prefix"] in ("osd erasure-code-profile set", "osd pool create") for c in rados.commands))
        cleanup(rados)
        self.assertEqual(rados.profiles[oracle.EC_PROFILE], before)
        # A second k2m1 pool of the same run reuses the profile it created.
        rados = FakeErasureRados()
        create(rados, plugin="jerasure")
        create(rados, slot=2, plugin="jerasure")
        self.assertEqual(sum(c["prefix"] == "osd erasure-code-profile set" for c in rados.commands), 2)
        result = cleanup(rados)
        self.assertEqual(result["claimed"]["ec_profiles"], [oracle.EC_PROFILE])
        self.assertEqual((rados.pools, rados.rules, rados.profiles), ([], ["replicated_rule"], {}))

    def test_without_a_claimed_pool_the_pool_delete_entry_is_not_changed(self):
        for prior in (None, "false", "true"):
            with self.subTest(prior=prior):
                rados = FakeErasureRados()
                rados.pools.append({"pool": 9, "pool_name": oracle.pool_name(1), "flags": 1, "type": 1})
                rados.config = [{"section": "mon", "name": "mon_allow_pool_delete", "value": prior, "mask": ""}] if prior else []
                result = cleanup(rados)
                self.assertEqual(mutations(rados), [])
                self.assertEqual(len(rados.pools), 1)
                self.assertEqual(result["unclaimed_present"]["pools"], [oracle.pool_name(1)])
                self.assertFalse(result["pools_removed"])
                self.assertTrue(result["pool_delete_config_restored"])
                self.assertEqual(result["pool_delete_config_prior"], prior)

    def test_a_later_cleanup_never_removes_an_earlier_runs_leftover(self):
        rados = FakeErasureRados()
        create(rados, width="k2m2", plugin="isa")
        fail_once(rados, "osd pool rm")
        with self.assertRaises(RuntimeError):
            cleanup(rados)
        self.assertFalse((rados.out / oracle.CLAIMS_FILE).exists())
        before = len(mutations(rados))
        result = cleanup(rados)
        self.assertNothingTouched(rados, before)
        self.assertFalse(result["pools_removed"])
        self.assertEqual(result["unclaimed_present"], {"pools": [oracle.pool_name(1)], "crush_rules": [oracle.pool_name(1)],
                                                       "ec_profiles": ["ceph-msgr-owned-ec-k2m2"]})
        # The restore of the entry still ran on the failed path.
        self.assertEqual(rados.config, [])

    def test_an_invalid_claim_record_removes_nothing(self):
        for record in (b"[]", b'{"pools": ["default"], "crush_rules": [], "ec_profiles": []}',
                       b'{"pools": [], "crush_rules": ["replicated_rule"], "ec_profiles": []}',
                       b'{"pools": [], "crush_rules": [], "ec_profiles": ["default"]}', b'{"pools": []}', b"x" * 5000):
            with self.subTest(record=record[:40]):
                rados = FakeErasureRados()
                create(rados, width="k2m2", plugin="isa")
                (rados.out / oracle.CLAIMS_FILE).write_bytes(record)
                before = len(mutations(rados))
                with self.assertRaises(ValueError):
                    cleanup(rados)
                self.assertNothingTouched(rados, before)


# The rule the fixture reads from the staged sources to admit a mode
# (ownedPoolOracleModes in integration/fixture/control_native.go).
DECLARATION = re.compile(r"^@oracle_mode\ndef mode_([a-z][a-z0-9]{0,31})\(", re.M)


def declared_modes(directory):
    """The modes declared in the sources of directory by the textual rule."""
    modes = {}
    for path in [directory / "osd_pool_oracle.py"] + sorted(directory.glob("osd_pool_mode_*_oracle.py")):
        text = path.read_text(encoding="utf-8")
        found = DECLARATION.findall(text)
        if len(re.findall(r"(?m)^\s*@", text)) != len(found):
            raise AssertionError("%s marks a mode outside the declaration rule" % path.name)
        for mode in found:
            if mode in modes:
                raise AssertionError("mode %s declared twice" % mode)
            modes[mode] = path.name
    return modes


class ModeDeclarationTest(unittest.TestCase):
    """Modes are declared beside their handlers, not in a list."""

    def setUp(self):
        self.handlers = dict(oracle.HANDLERS)
        self.modules = set(sys.modules)
        self.path = list(sys.path)

    def tearDown(self):
        oracle.HANDLERS.clear()
        oracle.HANDLERS.update(self.handlers)
        for name in set(sys.modules) - self.modules:
            del sys.modules[name]
        sys.path[:] = self.path

    def test_registered_modes_are_exactly_the_declared_ones(self):
        tools = pathlib.Path(oracle.__file__).resolve().parent
        self.assertEqual(set(declared_modes(tools)), set(oracle.HANDLERS))
        for name, path in declared_modes(tools).items():
            module = sys.modules[pathlib.Path(path).stem] if path != "osd_pool_oracle.py" else oracle
            self.assertIs(oracle.HANDLERS[name], getattr(module, "mode_" + name))

    def test_no_central_mode_list_remains(self):
        source = pathlib.Path(oracle.__file__).read_text(encoding="utf-8")
        self.assertNotIn("MODES", source)
        self.assertEqual(re.findall(r"(?m)^HANDLERS = .*$", source), ["HANDLERS = {}"])
        # The module docstring enumerated the modes; two adjacent mode
        # names in an enumeration are an append point again.
        names = "|".join(sorted(oracle.HANDLERS))
        self.assertNotRegex(" ".join(oracle.__doc__.split()), r"\b(%s),? (and )?(%s)\b" % (names, names))

    def module(self, directory, file_name, body):
        (directory / file_name).write_text("import osd_pool_oracle as pool\nfrom osd_pool_oracle import oracle_mode\n\n\n" + body, encoding="utf-8")

    def test_mode_module_registers_its_own_mode(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = pathlib.Path(raw)
            sys.path.insert(0, raw)
            self.module(directory, "osd_pool_mode_zzown1_oracle.py",
                        "@oracle_mode\ndef mode_zzown1(rados, cluster, request, out):\n    return {\"slot\": pool.pool_name(1)}\n")
            oracle.load_mode_modules(directory)
            self.assertEqual(oracle.HANDLERS["zzown1"](None, None, {}, None), {"slot": "ceph-msgr-owned-pool-1"})
            self.assertEqual(set(oracle.HANDLERS) - set(self.handlers), {"zzown1"})

    def test_mode_module_is_refused_unless_it_declares_exactly_its_own_mode(self):
        cases = {
            "other": ("osd_pool_mode_zzwant_oracle.py", "@oracle_mode\ndef mode_zzother(rados, cluster, request, out):\n    return {}\n"),
            "none": ("osd_pool_mode_zznone_oracle.py", "def mode_zznone(rados, cluster, request, out):\n    return {}\n"),
            "two": ("osd_pool_mode_zztwo_oracle.py", "@oracle_mode\ndef mode_zztwo(rados, cluster, request, out):\n    return {}\n\n\n"
                                                    "@oracle_mode\ndef mode_zzthree(rados, cluster, request, out):\n    return {}\n"),
            "existing": ("osd_pool_mode_create_oracle.py", "@oracle_mode\ndef mode_create(rados, cluster, request, out):\n    return {}\n"),
            "name": ("osd_pool_mode_Bad_oracle.py", "@oracle_mode\ndef mode_Bad(rados, cluster, request, out):\n    return {}\n"),
        }
        for label, (file_name, body) in cases.items():
            with self.subTest(label), tempfile.TemporaryDirectory() as raw:
                directory = pathlib.Path(raw)
                sys.path.insert(0, raw)
                self.module(directory, file_name, body)
                with self.assertRaises(ValueError):
                    oracle.load_mode_modules(directory)
                oracle.HANDLERS.clear()
                oracle.HANDLERS.update(self.handlers)

    def test_handler_must_be_named_after_its_mode(self):
        for name in ("create_mode", "mode_", "mode_Upper", "mode_x-y"):
            def handler():
                return {}
            handler.__name__ = name
            with self.subTest(name), self.assertRaises(ValueError):
                oracle.oracle_mode(handler)
        self.assertEqual(oracle.HANDLERS, self.handlers)


class PoolOracleTest(unittest.TestCase):
    def test_only_two_owned_pool_names_exist(self):
        self.assertEqual(oracle.pool_name(1), "ceph-msgr-owned-pool-1")
        self.assertEqual(oracle.pool_name(2), "ceph-msgr-owned-pool-2")
        for bad in (0, 3, -1, True, "1", None, 1.0):
            with self.assertRaises(ValueError):
                oracle.pool_name(bad)

    def test_modes_are_closed_and_have_handlers(self):
        self.assertTrue(oracle.HANDLERS)
        for name, handler in oracle.HANDLERS.items():
            self.assertEqual(handler.__name__, "mode_" + name)
        with self.assertRaises(SystemExit):
            oracle.main(["x", "delete", "/root", "/out"])
        with self.assertRaises(SystemExit):
            oracle.main(["x", "create", "/root"])

    def test_objecter_requests_targets_the_bracketed_primary(self):
        placement = {"epoch": 31, "pgid": "4.0", "primary_id": 1, "osd_id": 1}
        sent = []

        class Rados:
            lib = None

            def dump(self, cluster):
                return {}

            def osd(self, cluster, osd, command, limit):
                sent.append((osd, command, limit))
                return 0, b'{"ops":[],"linger_ops":[],"pool_ops":[],"pool_stat_ops":[],"statfs_ops":[],"command_ops":[]}', "", None

        saved = oracle.find_pool, oracle.mutation.collect_placement
        oracle.find_pool = lambda dump, pool: {"pool": 4}
        oracle.mutation.collect_placement = lambda *arguments, **keywords: dict(placement)
        try:
            receipt = oracle.mode_objecterrequests(Rados(), None, {"slot": 2, "object": "dst", "source": {"pool": 3, "name": "src"}}, None)["objecter_requests"]
            former = oracle.mode_objecterrequests(Rados(), None, {"slot": 2, "object": "dst", "source": {"pool": 3, "name": "src"}, "osd": 2}, None)["objecter_requests"]
            for bad in ({"slot": 2, "object": "dst", "source": {"pool": 3, "name": "src"}, "entity": {}},
                        {"slot": 2, "object": "dst", "source": {"pool": 3, "name": "src"}, "osd": -1},
                        {"slot": 2, "object": "dst", "source": {"pool": 3, "name": "src"}, "osd": True},
                        {"slot": 2, "object": "dst", "source": {"pool": 3}}, {"slot": 2, "object": "dst"}):
                with self.subTest(bad):
                    with self.assertRaises(ValueError):
                        oracle.mode_objecterrequests(Rados(), None, bad, None)
        finally:
            oracle.find_pool, oracle.mutation.collect_placement = saved
        command = {"prefix": "objecter_requests", "format": "json"}
        self.assertEqual(sent, [(1, command, oracle.objecter_requests.MAX_REQUESTS_BYTES), (2, command, oracle.objecter_requests.MAX_REQUESTS_BYTES)])
        self.assertTrue(receipt["complete"], receipt["failures"])
        self.assertEqual((receipt["osd"], receipt["pgid"], receipt["ops"]), (1, "4.0", []))
        # An explicit former primary is asked instead; the brackets still name the current primary.
        self.assertEqual((former["osd"], former["before"]["primary_id"]), (2, 1))

    def test_ops_in_flight_can_ask_an_explicit_osd(self):
        sent = []

        class Rados:
            lib = None

            def dump(self, cluster):
                return {}

            def osd(self, cluster, osd, command, limit):
                sent.append(osd)
                return 0, b'{"ops":[],"num_ops":0}', "", None

        saved = oracle.find_pool, oracle.mutation.collect_placement
        oracle.find_pool = lambda dump, pool: {"pool": 4}
        oracle.mutation.collect_placement = lambda *arguments, **keywords: {"epoch": 31, "pgid": "4.0", "primary_id": 1}
        entity = {"type": "client", "num": 4171, "inc": 21}
        try:
            primary = oracle.mode_opsinflight(Rados(), None, {"slot": 2, "object": "dst", "entity": entity}, None)["ops_in_flight"]
            former = oracle.mode_opsinflight(Rados(), None, {"slot": 2, "object": "dst", "entity": entity, "osd": 0}, None)["ops_in_flight"]
            with self.assertRaises(ValueError):
                oracle.mode_opsinflight(Rados(), None, {"slot": 2, "object": "dst", "entity": entity, "osd": "0"}, None)
        finally:
            oracle.find_pool, oracle.mutation.collect_placement = saved
        self.assertEqual((sent, primary["osd"], former["osd"], primary["complete"], former["complete"]), ([1, 0], 1, 0, True, True))

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
