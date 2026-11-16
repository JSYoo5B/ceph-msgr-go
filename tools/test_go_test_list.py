import json
import os
import tempfile
from pathlib import Path
import unittest
from unittest import mock

import go_test_list

MODULE = "example.com/listing"

# Shapes a text scanner gets wrong, in one package that `go test` compiles.
# Go lists exactly SHAPE_TESTS: the bare `Test`, the aliased-import test and
# the blank-parameter test. TestMain is the harness, the method and lowercase
# helper are not tests, and the build-constrained file is not compiled.
SHAPES = {
    "shapes_test.go": '''package integration

import (
	"os"
	"testing"
)

type suite struct{}

func Test(t *testing.T) {}

func TestX(_ *testing.T) {}

func (s suite) TestY(t *testing.T) {}

func Testable(n int) bool { return n > 0 }

func TestMain(m *testing.M) { os.Exit(m.Run()) }
''',
    "alias_test.go": '''package integration

import testpkg "testing"

func TestAlias(t *testpkg.T) {}
''',
    "ignored_test.go": '''//go:build ignore

package integration

import "testing"

func TestIgnored(t *testing.T) {}
''',
}
SHAPE_TESTS = {"Test", "TestAlias", "TestX"}

# The reviewer's wrong-file case: Go lists TestActual for the package, the
# declaration is in right_test.go, and wrong_test.go only holds a raw string
# whose line reads like the declaration.
WRONG_FILE = {
    "right_test.go": 'package integration\n\nimport "testing"\n\nfunc TestActual(t *testing.T) {}\n',
    "wrong_test.go": 'package integration\n\nconst doc = `\nfunc TestActual(t *testing.T) {}\nfunc TestActual()\n`\n',
}

# The reviewer's repeated-declaration case: Go permits several top-level init
# functions (and blank `_` functions) in one file, so right_test.go is a valid
# declaration site of TestActual; wrong_test.go still only mentions it.
REPEATED = {
    "right_test.go": 'package integration\n\nimport "testing"\n\nfunc init() {}\n\nfunc init() {}\n\n'
                     'func TestActual(t *testing.T) {}\n',
    "blank_test.go": 'package integration\n\nfunc _() {}\n\nfunc _() {}\n',
    "wrong_test.go": WRONG_FILE["wrong_test.go"],
}


def module(case, files, directory="integration"):
    """A temporary Go module whose package `directory` holds `files`."""
    holder = tempfile.TemporaryDirectory()
    case.addCleanup(holder.cleanup)
    root = Path(holder.name)
    (root / "go.mod").write_text("module %s\n\ngo 1.24\n" % MODULE, encoding="utf-8")
    package = root / directory
    package.mkdir(parents=True)
    (package / "doc.go").write_text("package %s\n" % package.name, encoding="utf-8")
    for name, text in files.items():
        (package / name).write_text(text, encoding="utf-8")
    return root


def tests_file(*bodies, package="integration"):
    return 'package %s\n\nimport "testing"\n\nvar _ testing.TB\n\n%s\n' % (package, "\n".join(bodies))


def event(action, package=MODULE + "/p", **fields):
    return json.dumps(dict(Action=action, Package=package, **fields))


class GoTestListTests(unittest.TestCase):
    def test_compiled_shapes_follow_go_discovery(self):
        root = module(self, SHAPES)
        self.assertEqual(go_test_list.package_tests(root, "integration"), SHAPE_TESTS)
        self.assertEqual(go_test_list.test_files(root, ["./integration"])["integration"],
                         {"shapes_test.go", "alias_test.go"})

    def test_listing_is_cached_per_process(self):
        root = module(self, {"a_test.go": tests_file("func TestA(t *testing.T) {}")})
        first = go_test_list.list_tests(root, ["./integration"])
        (root / "integration" / "b_test.go").write_text(tests_file("func TestB(t *testing.T) {}"), encoding="utf-8")
        self.assertIs(go_test_list.list_tests(root, ["./integration"]), first)

    def test_failures_are_errors_not_empty_sets(self):
        broken = module(self, {"a_test.go": tests_file("func TestA(t *testing.T) { undefined() }")})
        with self.assertRaisesRegex(go_test_list.ListError, "failed"):
            go_test_list.list_tests(broken, ["./integration"])
        duplicate = module(self, {"a_test.go": tests_file("func TestA(t *testing.T) {}"),
                                  "b_test.go": tests_file("func TestA(t *testing.T) {}")})
        with self.assertRaises(go_test_list.ListError):
            go_test_list.list_tests(duplicate, ["./integration"])
        empty = module(self, {})
        with self.assertRaisesRegex(go_test_list.ListError, "failed|did not list"):
            go_test_list.list_tests(empty, ["./absent"])
        with mock.patch.dict(os.environ, {"PATH": ""}):
            with self.assertRaisesRegex(go_test_list.ListError, "go is required"):
                go_test_list.list_tests(module(self, {}), ["./integration"])

    def test_declarations_come_from_the_parser_under_the_build_context(self):
        root = module(self, dict(SHAPES, **WRONG_FILE))
        package = root / "integration"
        files = go_test_list.declarations([package])[str(package.resolve())]
        # Literal expectation: the method, the raw string in wrong_test.go and
        # the build-ignored file declare nothing (a file without a top-level
        # function is absent); TestMain and the lowercase helper are not tests.
        self.assertEqual(files, {
            "shapes_test.go": (("Test", True), ("TestX", True), ("Testable", False), ("TestMain", False)),
            "alias_test.go": (("TestAlias", True),),
            "right_test.go": (("TestActual", True),),
        })

    def test_repeated_non_test_declarations_do_not_block_a_lookup(self):
        root = module(self, REPEATED)
        package = root / "integration"
        files = go_test_list.declarations([package])[str(package.resolve())]
        # Literal expectation: both init and both blank functions are kept in
        # source order; none is a test.
        self.assertEqual(files, {
            "right_test.go": (("init", False), ("init", False), ("TestActual", True)),
            "blank_test.go": (("_", False), ("_", False)),
        })
        self.assertEqual(go_test_list.package_tests(root, "integration"), {"TestActual"})
        self.assertTrue(go_test_list.declares(root, package / "right_test.go", "TestActual"))
        for file in ("wrong_test.go", "blank_test.go"):
            self.assertFalse(go_test_list.declares(root, package / file, "TestActual"), file)
        self.assertFalse(go_test_list.declares(root, package / "right_test.go", "init"))

    def test_internal_and_external_test_packages_may_share_a_name(self):
        root = module(self, {
            "internal_test.go": tests_file("func init() {}", "func TestShared(t *testing.T) {}"),
            "external_test.go": tests_file("func TestShared(t *testing.T) {}", package="integration_test"),
            "wrong_test.go": 'package integration\n\nconst doc = "func TestShared(t *testing.T) {}"\n',
        })
        package = root / "integration"
        self.assertEqual(go_test_list.package_tests(root, "integration"), {"TestShared"})
        for file in ("internal_test.go", "external_test.go"):
            self.assertTrue(go_test_list.declares(root, package / file, "TestShared"), file)
        self.assertFalse(go_test_list.declares(root, package / "wrong_test.go", "TestShared"))

    def test_only_the_looked_up_test_may_not_repeat(self):
        root = module(self, {"a_test.go": tests_file("func TestA(t *testing.T) {}")})
        path = (root / "integration" / "a_test.go").resolve()
        go_test_list.list_tests(root, ("./...",))
        go_test_list._CACHE[("decls", str(path.parent))] = {
            "a_test.go": (("init", False), ("init", False), ("TestA", True), ("TestA", True))}
        with self.assertRaisesRegex(go_test_list.ListError, "duplicate test declaration of TestA in a_test.go"):
            go_test_list.declares(root, path, "TestA")

    def test_declares_needs_the_listing_the_file_and_the_declaration(self):
        root = module(self, dict(SHAPES, **WRONG_FILE))
        package = root / "integration"
        for name, file in (("Test", "shapes_test.go"), ("TestX", "shapes_test.go"),
                           ("TestAlias", "alias_test.go"), ("TestActual", "right_test.go")):
            self.assertTrue(go_test_list.declares(root, package / file, name), (name, file))
        for name, file in (("TestActual", "wrong_test.go"), ("TestAlias", "shapes_test.go"),
                           ("TestY", "shapes_test.go"), ("TestMain", "shapes_test.go"),
                           ("Testable", "shapes_test.go"), ("TestIgnored", "ignored_test.go"),
                           ("TestActual", "doc.go")):
            self.assertFalse(go_test_list.declares(root, package / file, name), (name, file))

    def test_declaration_failures_are_errors(self):
        broken = module(self, {"a_test.go": "package integration\n\nfunc TestA(\n"})
        with self.assertRaisesRegex(go_test_list.ListError, "testdecls failed"):
            go_test_list.declarations([broken / "integration"])

    def test_parse_accepts_only_a_successful_listing(self):
        good = [event("start"), event("output", Output="Test\n"), event("output", Output="TestA\n"),
                event("output", Output="ok  \t%s/p\t0.01s\n" % MODULE), event("pass")]
        self.assertEqual(go_test_list.parse("\n".join(good), MODULE), {"p": {"Test", "TestA"}})
        # Literal expectation: the internal and the external test package may
        # both declare Test; Go lists it twice and the set holds it once.
        self.assertEqual(go_test_list.parse("\n".join(good[:2] + good[1:]), MODULE), {"p": {"Test", "TestA"}})
        none = [event("start"), event("output", Output="?   \t%s/p\t[no test files]\n" % MODULE), event("skip")]
        self.assertEqual(go_test_list.parse("\n".join(none), MODULE), {"p": frozenset()})
        bad = {
            "not JSON": good[:1] + ["Test"] + good[1:],
            "not an event": good + ["[]"],
            "stray output": good[:2] + [event("output", Output="init printed this\n")] + good[2:],
            "failed package": good[:-1] + [event("fail")],
            "no final event": good[:-1],
            "duplicate final": good + [event("pass")],
            "test event": good[:1] + [event("run", Test="TestA")] + good[1:],
            "unknown action": good[:1] + [event("pause")] + good[1:],
            "build failure": [json.dumps({"ImportPath": "p", "Action": "build-fail"})] + good,
            "skip with names": good[:-1] + [event("skip")],
            "outside module": [line.replace(MODULE, "other.example") for line in good],
        }
        for name, lines in bad.items():
            with self.subTest(name), self.assertRaises(go_test_list.ListError):
                go_test_list.parse("\n".join(lines), MODULE)


if __name__ == "__main__":
    unittest.main()
