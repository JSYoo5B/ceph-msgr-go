import tempfile
import unittest
from pathlib import Path

import scenario_declarations as declarations
import scenario_order


def source(*doc, test="TestCephXIntegration", package="integration_test"):
    lines = ["package %s" % package, "", "import \"testing\"", ""]
    lines += list(doc)
    lines += ["//fixture:shareable reason", "func %s(t *testing.T) {" % test, "}", ""]
    return "\n".join(lines)


ROW = "//scenario:row osd.aes.t-x key=aes service=aes family=4 stress= osd=1"


class DeclarationTest(unittest.TestCase):
    def discover(self, sources):
        return declarations.discover(sources)

    def assert_refused(self, sources, message):
        rows, problems = self.discover(sources)
        self.assertTrue(any(message in problem for problem in problems), problems)
        return rows, problems

    def test_row_selects_its_owner_with_the_parameters_in_matrix_order(self):
        rows, problems = self.discover({"integration/x_test.go": source(
            "// TestCephXIntegration checks x.", "//",
            "//scenario:row osd.aes.t-x key=aes capabilities=b-cap service=aes family=4 stress= osd=1")})
        self.assertEqual(problems, [])
        self.assertEqual(rows, [{"id": "osd.aes.t-x", "group": "osd", "params": {
            "key": "aes", "service": "aes", "family": "4", "stress": "", "osd": "1",
            "test_run": "^TestCephXIntegration$", "capabilities": "b-cap"}}])
        self.assertEqual(list(rows[0]["params"])[-2:], ["test_run", "capabilities"])

    def test_explicit_selector_must_select_its_owner(self):
        rows, problems = self.discover({"cephmsgr/x_test.go": source(
            "//scenario:row mon-mgr.aes.t-suite test_run=^TestCeph(X|Y)Integration$ key=aes", package="cephmsgr")})
        self.assertEqual(problems, [])
        self.assertEqual(rows[0]["params"], {"key": "aes", "test_run": "^TestCeph(X|Y)Integration$"})
        self.assert_refused({"integration/x_test.go": source("//scenario:row a.b test_run=^TestCephYIntegration$ key=aes")},
                            "does not select TestCephXIntegration")
        self.assert_refused({"integration/x_test.go": source("//scenario:row a.b test_run=^TestCephXIntegration$ key=aes")},
                            "is the owner's own selector")
        self.assert_refused({"integration/x_test.go": source("//scenario:row a.b test_run=^(TestCeph key=aes")},
                            "invalid test_run")

    def test_suite_row_has_no_selector(self):
        rows, problems = self.discover({"integration/x_test.go": source("//scenario:suite mon-mgr.aes.ip4 key=aes stress=")})
        self.assertEqual(problems, [])
        self.assertEqual(rows, [{"id": "mon-mgr.aes.ip4", "group": "mon-mgr", "params": {"key": "aes", "stress": ""}}])
        self.assert_refused({"integration/x_test.go": source("//scenario:suite a.b test_run=^TestCephXIntegration$")},
                            "has no test_run")

    def test_rows_are_in_identifier_order_wherever_they_are_declared(self):
        rows, problems = self.discover({
            "integration/b_test.go": source("//scenario:row osd.b key=aes", "//scenario:row osd.a key=aes"),
            "cephmsgr/a_test.go": source("//scenario:row mon.c key=aes", test="TestCephYIntegration", package="cephmsgr"),
        })
        self.assertEqual(problems, [])
        self.assertEqual([row["id"] for row in rows], ["mon.c", "osd.a", "osd.b"])

    def test_recorded_rows_keep_their_order_and_later_rows_follow_by_identifier(self):
        recorded = scenario_order.RECORDED
        self.assertEqual(len(recorded), len(set(recorded)), "a recorded row is listed twice")
        declared = {row["id"] for row in declarations.discover(declarations.tree_sources(Path(__file__).resolve().parent.parent))[0]}
        self.assertEqual(sorted(set(recorded) - declared), [], "a recorded row is no longer declared; remove its entry")
        later = ["zz.new-b", "aa.new-a"]
        rows = [{"id": sid} for sid in later + [recorded[2], recorded[0], recorded[1]]]
        self.assertEqual([row["id"] for row in sorted(rows, key=declarations.planning_key)],
                         [recorded[0], recorded[1], recorded[2], "aa.new-a", "zz.new-b"])

    def test_routed_rows_are_exactly_the_fixture_checkpoints(self):
        rows, problems = self.discover({"integration/fixture/r_test.go": source(
            "//scenario:row rgw.aes256k.t-r key=aes256k rgw_native=1", test="TestRGWNativeRIntegration", package="main")})
        self.assertEqual(problems, [])
        self.assertEqual(rows[0]["params"], {"key": "aes256k", "rgw_native": "1", "test_run": "^TestRGWNativeRIntegration$"})
        self.assert_refused({"integration/fixture/r_test.go": source(
            "//scenario:row rgw.a key=aes256k", test="TestRGWNativeRIntegration", package="main")}, "rgw_native=1 is declared exactly")
        self.assert_refused({"integration/x_test.go": source("//scenario:row rgw.a key=aes rgw_native=1")},
                            "rgw_native=1 is declared exactly")
        self.assert_refused({"integration/fixture/r_test.go": source(
            "//scenario:suite rgw.a key=aes256k rgw_native=1", test="TestRGWNativeRIntegration", package="main")},
            "not a suite row")
        self.assert_refused({"integration/fixture/r_test.go": source(
            "//scenario:row rgw.a key=aes256k rgw_native=1", test="TestCephXIntegration", package="main")},
            "not a native test of integration/fixture")

    def test_misplaced_and_malformed_declarations_are_refused(self):
        cases = {
            "not a native test": source(ROW, test="TestHelper"),
            "not in the doc comment": "package p\n\n%s\n\nfunc TestCephXIntegration(t *testing.T) {}\n" % ROW,
            "not in the doc comment of a top-level test function":
                "package p\n\n%s\nfunc (s suite) TestCephXIntegration(t *testing.T) {}\n" % ROW,
            "an indented scenario declaration": "package p\n\nfunc TestCephXIntegration(t *testing.T) {\n\t%s\n}\n" % ROW,
            "malformed scenario declaration": source("//scenario:test osd.a key=aes"),
            "malformed scenario declaration ": source("//scenario:row"),
            "malformed parameter": source("//scenario:row osd.a key"),
            "given twice": source("//scenario:row osd.a key=aes key=aes256k"),
            "invalid scenario identifier": source("//scenario:row Osd.A key=aes"),
            "invalid scenario identifier 'all'": source("//scenario:row all key=aes"),
            "has no group": source("//scenario:row os_d.a key=aes"),
        }
        for message, text in cases.items():
            with self.subTest(message=message):
                rows, _ = self.assert_refused({"integration/x_test.go": text}, message.strip())
                self.assertEqual(rows, [])

    def test_duplicate_identifiers_are_refused(self):
        _, problems = self.discover({"integration/a_test.go": source(ROW), "cephmsgr/b_test.go": source(ROW, package="cephmsgr")})
        self.assertEqual(problems, ["integration/a_test.go:5: duplicate scenario identifier osd.aes.t-x, also declared at "
                                    "cephmsgr/b_test.go:5"])

    def test_declarations_outside_the_suites_are_strays(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, text in {"integration/x_test.go": source(ROW), "internal/osd/x_test.go": source(ROW),
                               "integration/fixture/profile.go": "package main\n\n" + ROW + "\nfunc TestCephXIntegration() {}\n",
                               "integration/sub/x_test.go": source(ROW)}.items():
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                (root / name).write_text(text, encoding="utf-8")
            self.assertEqual(declarations.stray_declarations(root), [
                "integration/fixture/profile.go:3: a scenario declaration outside the native suites' test files",
                "integration/sub/x_test.go:5: a scenario declaration outside the native suites' test files",
                "internal/osd/x_test.go:5: a scenario declaration outside the native suites' test files",
            ])
            self.assertEqual(list(declarations.tree_sources(root)), ["integration/x_test.go"])


if __name__ == "__main__":
    unittest.main()
