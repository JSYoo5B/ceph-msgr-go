import subprocess
import tempfile
import unittest
from pathlib import Path

import fixture_sharing as sharing
import scenario_catalog

SOURCE = '''package integration_test

// TestCephAIntegration reads.
//
//fixture:shareable reads only
func TestCephAIntegration(t *testing.T) {}

//fixture:isolated kills an OSD
func TestCephBIntegration(t *testing.T) {}

// TestCephCIntegration has no declaration.
func TestCephCIntegration(t *testing.T) {}

//fixture:shareable a declaration not directly above
// another comment line
func TestCephDIntegration(t *testing.T) {}

func (s suite) TestMethod(t *testing.T) {}
'''


class FixtureSharingSourceTests(unittest.TestCase):
    def test_declarations_are_the_last_doc_comment_line(self):
        values = sharing.declarations({"integration/a_test.go": SOURCE})
        self.assertEqual(values, {"TestCephAIntegration": {"shareable"}, "TestCephBIntegration": {"isolated"},
                                  "TestCephCIntegration": {None}, "TestCephDIntegration": {None}})

    def test_a_row_is_shareable_only_if_every_selected_test_is(self):
        values = sharing.declarations({"integration/a_test.go": SOURCE,
                                       "cephmsgr/b_test.go": "//fixture:isolated other package\nfunc TestCephEIntegration(t *testing.T) {}\n"
                                                             "//fixture:shareable x\nfunc TestCephAIntegration(t *testing.T) {}\n"})
        self.assertTrue(sharing.shareable_row({"test_run": "^TestCephAIntegration$"}, values))
        for selector in ("", "^TestCeph(A|B)Integration$", "^TestCephCIntegration$", "^TestCephDIntegration$",
                         "^TestCephNoneIntegration$", "(", "^TestCephEIntegration$"):
            with self.subTest(selector=selector):
                self.assertFalse(sharing.shareable_row({"test_run": selector} if selector else {}, values))
        # One name declared in two packages needs both to be shareable.
        values["TestCephAIntegration"].add("isolated")
        self.assertFalse(sharing.shareable_row({"test_run": "^TestCephAIntegration$"}, values))

    def test_revision_sources_read_only_the_suite_test_blobs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = lambda *args: subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)
            run("init", "-q")
            for path, text in {"integration/a_test.go": SOURCE, "integration/helper.go": "package integration\n",
                               "integration/fixture/x_test.go": "//fixture:shareable no\nfunc TestCephXIntegration(t *testing.T) {}\n",
                               "cephmsgr/b_test.go": "package cephmsgr_test\n"}.items():
                (root / path).parent.mkdir(parents=True, exist_ok=True)
                (root / path).write_text(text)
            (root / "integration/fixture/sequence.go").write_text('const sequenceEnv = "CEPH_MSGR_TEST_SEQUENCE"\n')
            run("add", ".")
            run("-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "x")
            sha = run("rev-parse", "HEAD").stdout.decode().strip()
            (root / "integration/a_test.go").write_text("changed after the commit\n")
            sources = sharing.revision_sources(sha, root)
            self.assertEqual(sorted(sources), ["cephmsgr/b_test.go", "integration/a_test.go"])
            self.assertEqual(sources["integration/a_test.go"], SOURCE)
            self.assertEqual(sorted(sharing.tree_sources(root)), ["cephmsgr/b_test.go", "integration/a_test.go"])
            with self.assertRaises(scenario_catalog.CatalogError):
                sharing.revision_sources("HEAD", root)
            # A revision whose fixture cannot run a sequence gets no sources.
            run("rm", "-q", "integration/fixture/sequence.go")
            run("-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "older fixture")
            older = run("rev-parse", "HEAD").stdout.decode().strip()
            self.assertIsNone(sharing.revision_sources(older, root))


if __name__ == "__main__":
    unittest.main()
