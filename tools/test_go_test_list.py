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

    def test_parse_accepts_only_a_successful_listing(self):
        good = [event("start"), event("output", Output="Test\n"), event("output", Output="TestA\n"),
                event("output", Output="ok  \t%s/p\t0.01s\n" % MODULE), event("pass")]
        self.assertEqual(go_test_list.parse("\n".join(good), MODULE), {"p": {"Test", "TestA"}})
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
            "duplicate name": good[:2] + good[1:],
            "skip with names": good[:-1] + [event("skip")],
            "outside module": [line.replace(MODULE, "other.example") for line in good],
        }
        for name, lines in bad.items():
            with self.subTest(name), self.assertRaises(go_test_list.ListError):
                go_test_list.parse("\n".join(lines), MODULE)


if __name__ == "__main__":
    unittest.main()
