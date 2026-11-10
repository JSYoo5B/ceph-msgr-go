"""Merge-contract tests for the CHECKSUM native driver.

A fake shared builder stands in for the pinned native build. These tests check
only how the driver selects algorithms and merges returned proofs; they are not
native encoder evidence.
"""
import contextlib
import hashlib
import io
import json
import os
import pathlib
import unittest
from unittest import mock

import osd_checksum_native_encoder as driver

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIELDS = driver.VARIANT_FIELDS


class FakeBuilder:
    """Records each selection and returns a distinct five-vector proof."""

    def __init__(self, trailing=(), fail=None, names=None):
        self.calls, self.trailing, self.fail, self.names = [], trailing, fail, names

    def __call__(self, work, wrapper, build_target, output_name, status):
        algorithm = os.environ.get(driver.SELECTION)
        self.calls.append((algorithm, build_target, output_name))
        print("STEP native-direct-encoding", flush=True)
        if algorithm == self.fail:
            raise RuntimeError("native-direct-encoding exit=1")
        names = self.names(algorithm) if self.names else [algorithm + "-" + str(i) for i in range(5)]
        proof = {"ceph_commit": "commit", "image": "image", "status": status,
                 "fixtures": {name: {"front_hex": algorithm} for name in names}}
        proof.update({field: algorithm + "-" + field for field in FIELDS})
        print(json.dumps(proof), flush=True)
        for line in self.trailing:
            print(line, flush=True)
        return proof


def run(fake, prior=None):
    """Return (proof, stdout, error, selection left in the environment)."""
    stdout, proof, error = io.StringIO(), None, None
    with mock.patch.dict(os.environ), mock.patch.object(driver, "build", fake):
        os.environ.pop(driver.SELECTION, None)
        if prior is not None:
            os.environ[driver.SELECTION] = prior
        with contextlib.redirect_stdout(stdout):
            try:
                proof = driver.build_algorithms(pathlib.Path("work"), pathlib.Path("wrapper"))
            except RuntimeError as failure:
                error = failure
        return proof, stdout.getvalue(), error, os.environ.get(driver.SELECTION)


class ChecksumDriverTests(unittest.TestCase):
    def test_three_selections_merge_returned_proofs(self):
        fake = FakeBuilder()
        proof, stdout, error, after = run(fake)
        self.assertIsNone(error)
        self.assertEqual([call[0] for call in fake.calls], list(driver.ALGORITHMS))
        self.assertEqual({call[1] for call in fake.calls}, {"osd_checksum_native_encoder"})
        self.assertEqual([call[2] for call in fake.calls], ["encoded-checksum-" + a for a in driver.ALGORITHMS])
        self.assertEqual(len(proof["fixtures"]), 15)
        self.assertEqual(list(proof["algorithm_builds"]), list(driver.ALGORITHMS))
        for algorithm, variant in proof["algorithm_builds"].items():
            self.assertEqual(variant, {field: algorithm + "-" + field for field in FIELDS})
        self.assertTrue(proof["status"].startswith("fifteen fresh native CHECKSUM vectors"))
        launcher = hashlib.sha256((ROOT / "tools/osd_checksum_native_encoder.py").read_bytes()).hexdigest()
        self.assertEqual(proof["launcher_sha256"], launcher)
        # The CLI receipt is still the final stdout line, after every diagnostic.
        self.assertEqual(json.loads(stdout.splitlines()[-1]), proof)
        self.assertIsNone(after)

    def test_trailing_diagnostics_do_not_change_the_merged_proof(self):
        baseline, _, _, _ = run(FakeBuilder())
        # A trailing line that is itself a JSON object would have replaced the
        # proof when the driver parsed the last captured stdout line.
        trailing = ("native cleanup complete", json.dumps({"fixtures": {"stray": {}}}), "")
        proof, stdout, error, after = run(FakeBuilder(trailing=trailing))
        self.assertIsNone(error)
        self.assertEqual(proof, baseline)
        self.assertIn("native cleanup complete", stdout)
        self.assertEqual(stdout.count('{"fixtures": {"stray": {}}}'), 3)
        self.assertEqual(json.loads(stdout.splitlines()[-1]), proof)

    def test_algorithm_failure_stops_and_restores_selection(self):
        for prior in (None, "operator-choice"):
            with self.subTest(prior=prior):
                fake = FakeBuilder(fail="xxhash32")
                proof, stdout, error, after = run(fake, prior)
                self.assertIsNone(proof)
                self.assertEqual(str(error), "native-direct-encoding exit=1")
                self.assertEqual([call[0] for call in fake.calls], ["crc32c", "xxhash32"])
                self.assertEqual(after, prior)
                self.assertNotIn("fifteen fresh native CHECKSUM vectors", stdout)

    def test_duplicate_vector_name_is_rejected_and_selection_restored(self):
        fake = FakeBuilder(names=lambda algorithm: ["shared"] + [algorithm + str(i) for i in range(4)])
        proof, stdout, error, after = run(fake, "operator-choice")
        self.assertIsNone(proof)
        self.assertEqual(str(error), "duplicate checksum native vector name")
        self.assertEqual([call[0] for call in fake.calls], ["crc32c", "xxhash32"])
        self.assertEqual(after, "operator-choice")
        self.assertNotIn("fifteen fresh native CHECKSUM vectors", stdout)

    def test_unstructured_builder_result_is_rejected(self):
        def printing_only(work, wrapper, **_):
            print(json.dumps({"fixtures": {"printed": {}}}), flush=True)

        proof, _, error, after = run(printing_only)
        self.assertIsNone(proof)
        self.assertEqual(str(error), "shared builder returned no structured proof for crc32c")
        self.assertIsNone(after)

    def test_returned_proofs_are_not_aliased(self):
        returned = []

        def keeping(*args, **kwargs):
            proof = FakeBuilder()(*args, **kwargs)
            returned.append(proof)
            return proof

        proof, _, error, after = run(keeping)
        self.assertIsNone(error)
        self.assertEqual([len(item["fixtures"]) for item in returned], [5, 5, 5])
        self.assertNotIn("algorithm_builds", returned[-1])
        self.assertEqual(len(proof["fixtures"]), 15)


if __name__ == "__main__":
    unittest.main()
