import unittest

import ci_row_catalog as rows
from test_go_test_list import SHAPES, module, tests_file


class DeclaredTestsTests(unittest.TestCase):
    def test_row_admission_reads_the_tests_go_lists_per_suite(self):
        root = module(self, SHAPES)
        client = root / "cephmsgr"
        client.mkdir()
        (client / "client_test.go").write_text(tests_file("func TestClient(t *testing.T) {}", package="cephmsgr"),
                                               encoding="utf-8")
        # Literal expectation: TestMain, the method, the lowercase helper and
        # the build-constrained test are not tests of the API suite.
        self.assertEqual(rows.declared_tests("integration", root), {"Test", "TestAlias", "TestX"})
        self.assertEqual(rows.declared_tests("cephmsgr", root), {"TestClient"})


if __name__ == "__main__":
    unittest.main()
