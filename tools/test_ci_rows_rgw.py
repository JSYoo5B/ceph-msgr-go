"""The routed RGW checkpoint rows (rgw_native=1) of the native CI matrix.

A routed row runs one RGW checkpoint of the fixture module
(integration/fixture/rgw_native_route.go) instead of the controller's
fixture. These tests check the two rows, their place in the default coverage
and the planner's refusal of every other routed row shape.
"""
import contextlib
import io
import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

import ci_row_catalog
import rgw_native_gate as gate
import scenario_plan as plan

CHECKPOINT = "rgw.aes256k.ip4.container.t-rgwcheckpoint"
MULTISTRIPE = "rgw.aes256k.ip4.container.t-rgwmultistripe"
BACKEND_FAULT = "rgw.aes256k.ip4.container.t-rgwbackendfault"
# Rows in the planner's order: identifier order.
ROUTED = [BACKEND_FAULT, CHECKPOINT, MULTISTRIPE]
PROFILE = {"key": "aes256k", "service": "aes256k", "family": "4", "stress": "", "rgw_native": "1"}


def row(sid="r", **params):
    return {"id": sid, "group": "rgw", "params": dict(PROFILE, **params)}


class RGWRowsTest(ci_row_catalog.RowsTestCase):
    def test_each_checkpoint_has_one_routed_row(self):
        routed = [entry for entry in self.entries if plan.routed(entry["params"])]
        self.assertEqual(routed, [
            row(BACKEND_FAULT, test_run="^%s$" % gate.BACKEND_FAULT),
            row(CHECKPOINT, test_run="^%s$" % gate.CHECKPOINT),
            row(MULTISTRIPE, test_run="^%s$" % gate.MULTISTRIPE)])
        # No other row names the routing parameter, so every existing row
        # keeps CEPH_MSGR_TEST_RGW_NATIVE at the workflow's fallback 0.
        self.assertEqual([entry["id"] for entry in self.entries if "rgw_native" in entry["params"]], ROUTED)
        self.assertIn("CEPH_MSGR_TEST_RGW_NATIVE: ${{ matrix.rgw_native || '0' }}", self.workflow)

    def test_routed_rows_are_default_coverage_and_each_runs_alone(self):
        chosen, _ = plan.select(self.entries, ["all"])
        for sid in ROUTED:
            self.assertIn(sid, [entry["id"] for entry in chosen])
        declared = plan.plan_sources()
        jobs = plan.matrix(chosen, declared)["include"]
        routed = [job for job in jobs if job.get("rgw_native") == "1"]
        self.assertEqual([job["scenario"] for job in routed], ROUTED)
        self.assertFalse([job for job in routed if "test_sequence" in job])
        selected, _ = plan.select(self.entries, ["group:rgw"])
        self.assertEqual([job["scenario"] for job in plan.matrix(selected, declared)["include"]], ROUTED)

    def test_routed_rows_resolve_in_the_fixture_module_only(self):
        listing = plan.routed_tests()
        self.assertEqual({name: listing["."][name] for name in gate.TESTS}, {name: 1 for name in gate.TESTS})
        routed = [entry for entry in self.entries if plan.routed(entry["params"])]
        self.assertEqual(plan.selector_problems(routed, plan.suite_tests(), listing), [])
        # Without the fixture listing a routed row has no test, and the
        # product suites never stand in for it.
        self.assertEqual(len(plan.selector_problems(routed, plan.suite_tests())), len(ROUTED))

    def test_routed_job_name_shows_the_checkpoint_cluster(self):
        # The controller-profile fields of the ordinary job name (MGR count,
        # OSD, store, profile) fall back to controller defaults that a routed
        # checkpoint never uses, so a routed job gets its own name: the row
        # and the fixed cluster rgwNativeStartFixture starts.
        match = re.search(r"^    name: \$\{\{ matrix\.rgw_native == '1' && format\('([^']*)', ([^)]*)\) \|\| format\('([^']*)', ", self.workflow, re.M)
        self.assertIsNotNone(match)
        routed, args, ordinary = match.groups()
        self.assertEqual(args.split(", "), ["matrix.scenario", "matrix.key", "matrix.service", "matrix.family"])
        self.assertIn("MON=1, MGR=1, OSD=1 BlueStore, gateway=1", routed)
        self.assertIn("secure", routed)
        for unused in ("MGR={", "OSD={", "store=", "profile=", "replicas="):
            self.assertNotIn(unused, routed)
        self.assertNotIn("RGW", ordinary)

    def test_env_exports_the_routing_flag(self):
        for sid, value in ((CHECKPOINT, "1"), (BACKEND_FAULT, "1"), ("mon-ping.aes.ip4.crc.host.t-monping", "0")):
            output = self.env(sid)
            self.assertIn("export CEPH_MSGR_TEST_RGW_NATIVE=%s" % value, output.splitlines())

    def env(self, sid):
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            self.assertEqual(plan.main(["env", sid]), 0)
        return buffer.getvalue()


class RoutedRowRefusalTest(unittest.TestCase):
    def problems(self, entry):
        return "\n".join(plan.validate_catalog([entry]))

    def test_accepts_exactly_one_known_checkpoint(self):
        for name in gate.TESTS:
            self.assertEqual(plan.validate_catalog([row(test_run="^%s$" % name)]), [])
        self.assertEqual(plan.validate_catalog([row(rgw_native="0")]), [])

    def test_refuses_other_routed_tests_and_shapes(self):
        refused = {
            "missing test_run": row(),
            "unknown test": row(test_run="^TestRGWNativeOtherIntegration$"),
            "fixture unit test": row(test_run="^TestRGWNativeManifestAcceptsOnlyCompleteMultiObjectLayouts$"),
            "product test": row(test_run="^TestCephMonPingIntegration$"),
            "unanchored": row(test_run=gate.CHECKPOINT),
            "prefix": row(test_run="^TestRGWNative"),
            "two checkpoints": row(test_run="^%s$|^%s$" % gate.TESTS[:2]),
            "every checkpoint": row(test_run="|".join("^%s$" % name for name in gate.TESTS)),
            "fault checkpoint prefix": row(test_run="^TestRGWNativeBackendFault"),
            "fault helper unit test": row(test_run="^TestRGWNativeFaultMapParsers$"),
            "fault capability": row(test_run="^%s$" % gate.BACKEND_FAULT, capabilities="osd-pg-interval"),
            "capability": row(test_run="^%s$" % gate.CHECKPOINT, capabilities="osd-erasure-width"),
            "other value": row(test_run="^%s$" % gate.CHECKPOINT, rgw_native="yes"),
            "value 2": row(test_run="^%s$" % gate.CHECKPOINT, rgw_native="2"),
        }
        for name, entry in refused.items():
            with self.subTest(name):
                self.assertIn("rgw_native", self.problems(entry))

    def test_plan_of_another_revision_refuses_a_bad_routed_row(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            git = lambda *args: subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", *args],
                                               cwd=root, check=True, capture_output=True).stdout.decode().strip()
            git("init", "-q")
            (root / "integration" / "fixture").mkdir(parents=True)
            (root / "integration" / "fixture" / "r_test.go").write_text(
                "package main\n\n//scenario:row rgw.bad key=aes256k service=aes256k family=4 rgw_native=1 test_run=^TestRGWNative.*$\n"
                "//fixture:isolated x\nfunc TestRGWNativeCheckpointIntegration(t *testing.T) {}\n", encoding="utf-8")
            git("add", ".")
            git("commit", "-qm", "x")
            errors, previous = io.StringIO(), os.getcwd()
            os.chdir(root)
            try:
                with contextlib.redirect_stderr(errors), contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(plan.main(["plan", "--revision", git("rev-parse", "HEAD"), "--select", "all"]), 2)
            finally:
                os.chdir(previous)
            self.assertIn("rgw_native=1 requires a test_run of exactly one of", errors.getvalue())

    def test_routed_listing_must_list_the_test_once(self):
        entry = row(test_run="^%s$" % gate.CHECKPOINT)
        product = {"integration": {gate.CHECKPOINT: 1}, "cephmsgr": {}}
        for count, refused in ((1, False), (0, True), (2, True)):
            with self.subTest(count=count):
                problems = plan.selector_problems([entry], product, {".": {gate.CHECKPOINT: count} if count else {}})
                self.assertEqual(bool(problems), refused)
        # An ordinary row is never resolved against the fixture module.
        ordinary = {"id": "o", "group": "mon-mgr", "params": {"key": "aes", "service": "aes", "family": "4",
                                                              "test_run": "^%s$" % gate.CHECKPOINT}}
        self.assertEqual(len(plan.selector_problems([ordinary], {"integration": {}, "cephmsgr": {}},
                                                    {".": {gate.CHECKPOINT: 1}})), 1)


if __name__ == "__main__":
    unittest.main()
