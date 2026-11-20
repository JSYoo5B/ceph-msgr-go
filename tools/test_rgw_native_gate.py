"""The checkpoint-independent rules of tools/rgw_native_gate.py.

Each checkpoint's receipt tests live in test_rgw_native_gate_<name>.py beside
its module rgw_native_gate_<name>.py; that test module's SAMPLE is a receipt
line the checkpoint's check accepts, which the streams here use.
"""
import importlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

import rgw_native_gate as gate

IMAGE = "quay.io/ceph/ceph:v20.2.4@sha256:" + gate.IMAGE_DIGEST
CLEANUP = "RGW_NATIVE_CLEANUP independent inspection confirms containers=4 volumes=1 network=removed"


def sample(test):
    """The accepted receipt line of test, from its checkpoint's own test module."""
    return importlib.import_module("test_" + gate.CHECKPOINTS[test].__name__).SAMPLE


def stream(test, *, passes=1, package=True, extra=(), cleanup=True, line=None, owner=gate.PACKAGE, terminal=gate.PACKAGE):
    """Build a test2json stream; owner names the package of every test event, terminal that of the package PASS."""
    rows = []
    if line is None:
        line = sample(test)
    if line:
        rows.append({"Action": "output", "Test": test, "Output": "    file_test.go:1: " + line + "\n"})
    if cleanup:
        rows.append({"Action": "output", "Test": test, "Output": "    file_test.go:2: " + CLEANUP + "\n"})
    rows += [{"Action": "pass", "Test": test}] * passes
    rows += list(extra)
    rows = [dict(row) if "Package" in row else dict(row, Package=owner) for row in rows]
    if package:
        rows.append({"Action": "pass", "Package": terminal})
    return "\n".join(json.dumps(row) for row in rows) + "\n"


class RGWNativeGateTest(unittest.TestCase):
    def test_accepts_actual_passes(self):
        self.assertTrue(gate.TESTS)
        for test in gate.TESTS:
            result = gate.verify(stream(test), test)
            self.assertTrue(result["passed"], result["problems"])
            self.assertEqual(result["cleanup"], {"containers": 4, "volumes": 1})

    def test_default_skip_is_not_acceptance(self):
        for test in gate.TESTS:
            with self.subTest(test=test):
                skipped = stream(test, passes=0, extra=[{"Action": "skip", "Test": test}], line="", cleanup=False)
                result = gate.verify(skipped, test)
                self.assertFalse(result["passed"])
                self.assertTrue(any("skip" in problem for problem in result["problems"]))

    def test_rejects_missing_duplicate_or_foreign_results(self):
        for test in gate.TESTS:
            other = next((name for name in gate.TESTS if name != test), "TestRGWNativeOtherIntegration")
            with self.subTest(test=test):
                self.assertFalse(gate.verify(stream(test, passes=0), test)["passed"])
                self.assertFalse(gate.verify(stream(test, passes=2), test)["passed"])
                self.assertFalse(gate.verify(stream(test, package=False), test)["passed"])
                self.assertFalse(gate.verify(stream(test, extra=[{"Action": "fail", "Test": test}]), test)["passed"])
                self.assertFalse(gate.verify(stream(test, extra=[{"Action": "pass", "Test": other}]), test)["passed"])
                self.assertFalse(gate.verify(stream(test, extra=[{"Action": "fail", "Test": test + "/sub"}]), test)["passed"])
                self.assertFalse(gate.verify(stream(test, cleanup=False), test)["passed"])
                self.assertFalse(gate.verify(stream(test, extra=[{"Action": "fail"}]), test)["passed"])

    def test_binds_every_result_to_the_fixture_package(self):
        other = "example.org/unrelated"
        for test in gate.TESTS:
            with self.subTest(test=test, case="foreign terminal package PASS"):
                result = gate.verify(stream(test, terminal=other), test)
                self.assertFalse(result["passed"])
                self.assertIn("event outside the fixture package: " + other, result["problems"])
                self.assertIn("no PASS of package " + gate.PACKAGE, result["problems"])
            with self.subTest(test=test, case="foreign test events"):
                result = gate.verify(stream(test, owner=other), test)
                self.assertFalse(result["passed"])
                self.assertTrue(any("must PASS exactly once, saw 0" in problem for problem in result["problems"]), result)
                self.assertTrue(any("receipt, saw 0" in problem for problem in result["problems"]), result)
                self.assertIsNone(result["cleanup"])
            with self.subTest(test=test, case="foreign package PASS beside the fixture's own"):
                result = gate.verify(stream(test, extra=[{"Action": "pass", "Package": other}]), test)
                self.assertEqual(result["problems"], ["event outside the fixture package: " + other])
            with self.subTest(test=test, case="event without a package"):
                text = stream(test) + json.dumps({"Action": "pass"}) + "\n"
                self.assertEqual(gate.verify(text, test)["problems"], ["event outside the fixture package: <none>"])
            with self.subTest(test=test, case="selected test PASS without a package"):
                text = stream(test, passes=0) + json.dumps({"Action": "pass", "Test": test}) + "\n"
                self.assertFalse(gate.verify(text, test)["passed"])
            with self.subTest(test=test, case="foreign failure"):
                result = gate.verify(stream(test, extra=[{"Action": "fail", "Package": other}]), test)
                self.assertFalse(result["passed"])
            self.assertEqual(gate.verify(stream(test), test)["package"], gate.PACKAGE)

    def test_rejects_missing_or_duplicate_receipt_and_garbage(self):
        for test in gate.TESTS:
            with self.subTest(test=test):
                self.assertFalse(gate.verify(stream(test, line=""), test)["passed"])
                self.assertFalse(gate.verify(stream(test) + stream(test), test)["passed"])
                # Another checkpoint's receipt never completes this one.
                for other in gate.TESTS:
                    if other != test:
                        self.assertFalse(gate.verify(stream(test, line=sample(other)), test)["passed"], other)
                with self.assertRaises(ValueError):
                    gate.verify("not json\n", test)
                with self.assertRaises(ValueError):
                    gate.verify("[]\n", test)
        with self.assertRaises(ValueError):
            gate.verify(stream(gate.TESTS[0]), "TestSomethingElse")


class CheckpointDiscoveryTest(unittest.TestCase):
    """The gate's checkpoints are the modules beside it, by their file name."""

    def test_every_checkpoint_module_has_its_own_tests(self):
        tools = Path(gate.__file__).resolve().parent
        modules = sorted(path.stem for path in tools.glob("rgw_native_gate_*.py"))
        self.assertEqual(sorted(module.__name__ for module in gate.CHECKPOINTS.values()), modules)
        for test, module in gate.CHECKPOINTS.items():
            with self.subTest(module=module.__name__):
                self.assertTrue((tools / ("test_%s.py" % module.__name__)).is_file())
                self.assertTrue(sample(test).startswith(module.MARKER))
        self.assertEqual(gate.TESTS, tuple(sorted(gate.CHECKPOINTS)))

    def load(self, files):
        with tempfile.TemporaryDirectory() as directory:
            for name, text in files.items():
                (Path(directory) / name).write_text(text, encoding="utf-8")
            before = set(sys.modules)
            try:
                return gate.load_checkpoints(Path(directory))
            finally:
                for name in set(sys.modules) - before:
                    del sys.modules[name]

    def test_loads_a_module_declared_beside_the_gate(self):
        good = 'TEST = "TestRGWNativeProbeIntegration"\nMARKER = "RGW_NATIVE_PROBE "\ndef check(line, gate):\n    return [], {}\n'
        checkpoints = self.load({"rgw_native_gate_probe.py": good, "unrelated.py": "raise SystemExit(1)\n"})
        self.assertEqual(list(checkpoints), ["TestRGWNativeProbeIntegration"])
        self.assertEqual(checkpoints["TestRGWNativeProbeIntegration"].MARKER, "RGW_NATIVE_PROBE ")

    def test_refuses_a_bad_declaration(self):
        check = "def check(line, gate):\n    return [], {}\n"
        good = 'TEST = "TestRGWNativeProbeIntegration"\nMARKER = "RGW_NATIVE_PROBE "\n' + check
        refused = {
            "module name": {"rgw_native_gate_Probe.py": good},
            "no test": {"rgw_native_gate_probe.py": 'MARKER = "RGW_NATIVE_PROBE "\n' + check},
            "unit test name": {"rgw_native_gate_probe.py": good.replace("ProbeIntegration", "Probe")},
            "product test name": {"rgw_native_gate_probe.py": good.replace("TestRGWNativeProbe", "TestCephProbe")},
            "selector, not a name": {"rgw_native_gate_probe.py": good.replace('"TestRGWNativeProbeIntegration"', '"TestRGWNative.*Integration"')},
            "no marker": {"rgw_native_gate_probe.py": 'TEST = "TestRGWNativeProbeIntegration"\n' + check},
            "marker without separator": {"rgw_native_gate_probe.py": good.replace('PROBE "', 'PROBE"')},
            "no check": {"rgw_native_gate_probe.py": good.replace(check, "")},
            "same test twice": {"rgw_native_gate_probe.py": good,
                                "rgw_native_gate_probe_two.py": good.replace("PROBE ", "PROBE_TWO ")},
            "same marker twice": {"rgw_native_gate_probe.py": good,
                                  "rgw_native_gate_probe_two.py": good.replace("ProbeIntegration", "ProbeTwoIntegration")},
        }
        for name, files in refused.items():
            with self.subTest(name):
                with self.assertRaises(ValueError):
                    self.load(files)


if __name__ == "__main__":
    unittest.main()
