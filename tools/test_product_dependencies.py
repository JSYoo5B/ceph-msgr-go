import copy
import json
import subprocess
import unittest
from unittest import mock

import product_dependencies as policy


def product_graph():
    # Model the go list protocol independently of the checker's allowlist.
    compression = {"Path": "github.com/klauspost/compress", "Version": "v1.18.7"}
    return [
        {"ImportPath": "runtime", "Standard": True},
        {"ImportPath": "github.com/JSYoo5B/ceph-msgr-go/cephmsgr",
         "Module": {"Path": "github.com/JSYoo5B/ceph-msgr-go", "Main": True}},
        {"ImportPath": "github.com/klauspost/compress/snappy", "Module": compression},
        {"ImportPath": "github.com/klauspost/compress/zstd", "Module": compression},
    ]


def graph_output(graph):
    return "\n".join(json.dumps(package, indent=2) for package in graph) + "\n"


class ProductDependenciesTest(unittest.TestCase):
    def check_graph(self, graph):
        execute = mock.Mock(side_effect=[
            subprocess.CompletedProcess([], 0, stdout='{"CGO_ENABLED":"0","GOWORK":"off"}'),
            subprocess.CompletedProcess([], 0, stdout=graph_output(graph)),
        ])
        return policy.check(execute)

    def test_compression_packages_are_accepted_as_one_versioned_module(self):
        self.assertEqual(self.check_graph(product_graph()), {
            "CGO_ENABLED": "0", "GOWORK": "off", "standard_library": True,
            "external_modules": [{"path": "github.com/klauspost/compress", "version": "v1.18.7"}],
        })

    def test_only_modules_actually_used_are_reported(self):
        self.assertEqual(self.check_graph(product_graph()[:2])["external_modules"], [])

    def test_unexpected_and_native_ceph_modules_are_rejected(self):
        for path in ("example.com/unapproved", "github.com/ceph/go-ceph",
                     "github.com/klauspost/compress-extra"):
            graph = product_graph() + [{"ImportPath": path + "/client",
                                        "Module": {"Path": path, "Version": "v1.0.0"}}]
            with self.subTest(path=path), self.assertRaisesRegex(RuntimeError, "unexpected external module"):
                self.check_graph(graph)

    def test_import_path_cannot_disguise_an_unapproved_module(self):
        graph = product_graph()
        graph[-1] = {"ImportPath": "github.com/klauspost/compress/zstd",
                     "Module": {"Path": "example.com/unapproved", "Version": "v1.0.0"}}
        with self.assertRaisesRegex(RuntimeError, "unexpected external module"):
            self.check_graph(graph)

    def test_replacement_cannot_hide_native_or_unapproved_code(self):
        for replacement in ({"Path": "example.com/replacement", "Version": "v1.0.0"},
                            {"Path": "../replacement"}):
            graph = copy.deepcopy(product_graph())
            graph[-1]["Module"]["Replace"] = replacement
            with self.subTest(replacement=replacement), self.assertRaisesRegex(RuntimeError, "is replaced"):
                self.check_graph(graph)

    def test_native_sources_are_rejected_in_main_and_approved_modules(self):
        for index in (1, 3):
            for field in ("CgoFiles", "CFiles", "CXXFiles", "MFiles", "FFiles",
                          "SwigFiles", "SwigCXXFiles", "SysoFiles"):
                graph = product_graph()
                graph[index][field] = ["native-source"]
                with self.subTest(index=index, field=field):
                    with self.assertRaisesRegex(RuntimeError, "native source dependencies"):
                        self.check_graph(graph)

    def test_subprocess_dependency_is_rejected_even_though_it_is_standard(self):
        graph = product_graph() + [{"ImportPath": "os/exec", "Standard": True}]
        with self.assertRaisesRegex(RuntimeError, "subprocess dependency"):
            self.check_graph(graph)

    def test_empty_missing_and_incomplete_graphs_cannot_pass(self):
        for graph in ([], product_graph()[:1], [{}],
                      product_graph() + [{"ImportPath": "broken", "Incomplete": True}],
                      product_graph() + [{"ImportPath": "broken", "Error": {"Err": "failed"}}],
                      product_graph() + [{"ImportPath": "broken", "DepsErrors": [{"Err": "failed"}]}],
                      product_graph() + [{"ImportPath": "unidentified"}]):
            with self.subTest(graph=graph), self.assertRaises(RuntimeError):
                self.check_graph(graph)

    def test_external_evidence_requires_a_version_and_the_actual_main_module(self):
        for module in ({"Path": "github.com/klauspost/compress"},
                       {"Path": "github.com/klauspost/compress", "Main": True}):
            graph = product_graph() + [{"ImportPath": "dependency", "Module": module}]
            with self.subTest(module=module), self.assertRaises(RuntimeError):
                self.check_graph(graph)

    def test_wrong_build_environment_fails_before_listing_packages(self):
        for env in ({"CGO_ENABLED": "1", "GOWORK": "off"},
                    {"CGO_ENABLED": "0", "GOWORK": "workspace.go.work"}):
            execute = mock.Mock(return_value=subprocess.CompletedProcess([], 0, stdout=json.dumps(env)))
            with self.subTest(env=env), self.assertRaisesRegex(RuntimeError, "requires CGO_ENABLED=0"):
                policy.check(execute)
            self.assertEqual(execute.call_count, 1)

    def test_listing_failure_propagates_without_accepting_a_partial_graph(self):
        failure = subprocess.CalledProcessError(1, ["go", "list"])
        execute = mock.Mock(side_effect=[subprocess.CompletedProcess(
            [], 0, stdout='{"CGO_ENABLED":"0","GOWORK":"off"}'), failure])
        with self.assertRaises(subprocess.CalledProcessError) as raised:
            policy.check(execute)
        self.assertIs(raised.exception, failure)

    def test_malformed_or_trailing_output_is_not_ignored(self):
        for output in ("not JSON", "[]", graph_output(product_graph()) + "truncated"):
            with self.subTest(output=output), self.assertRaises((ValueError, RuntimeError)):
                list(policy.package_records(output))


if __name__ == "__main__":
    unittest.main()
