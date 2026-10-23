import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import mon_command_outcome_oracle as oracle


class OwnedMonitorOutcomeTest(unittest.TestCase):
    token = "a" * 32

    def test_closed_inputs_and_result_bounds(self):
        for raw_id, token, phase in (("0", self.token, "initial"),
                                     ("-1", self.token, "initial"),
                                     ("1", "../key", "initial"),
                                     ("1", self.token, "set"),
                                     (str(1 << 63), self.token, "initial")):
            with self.assertRaises(ValueError):
                oracle.validate(raw_id, token, phase)
        self.assertEqual(oracle.result(-2, b"\x00\xff", "missing"),
                         {"code": -2, "data": "AP8=", "status": "missing"})
        for fields in ((True, b"", ""), (0, b"a" * (65536 + 1), ""),
                       (0, b"", "a" * 8193), (1 << 31, b"", "")):
            with self.assertRaises(ValueError):
                oracle.result(*fields)

    def test_existing_or_unowned_key_cannot_be_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            with patch.object(oracle, "native_command", return_value=oracle.result(0, b"other", "")) as native:
                oracle.collect(output, output, "1", self.token, "initial")
                with self.assertRaises(FileNotFoundError):
                    oracle.collect(output, output, "2", self.token, "cleanup")
            self.assertEqual(native.call_count, 1)

    def test_raw_signed_observation_and_one_fresh_cleanup_read(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            replies = [oracle.result(-2, b"", "initial absence"),
                       oracle.result(0, b"\x00\xffA", "observed"),
                       oracle.result(0, b"", "removed"),
                       oracle.result(-2, b"", "fresh absence")]
            with patch.object(oracle, "native_command", side_effect=replies) as native:
                oracle.collect(output, output, "1", self.token, "initial")
                oracle.collect(output, output, "2", self.token, "applied")
                oracle.collect(output, output, "3", self.token, "cleanup")
                with self.assertRaises(FileExistsError):
                    oracle.collect(output, output, "4", self.token, "cleanup")
            self.assertEqual([call.args[1] for call in native.call_args_list],
                             ["config-key get", "config-key get", "config-key rm", "config-key get"])
            receipt = json.loads((output / (oracle.PREFIX + self.token + ".cleanup.json")).read_text())
            self.assertEqual(receipt["result"]["code"], -2)
            self.assertEqual(receipt["removal"]["status"], "removed")
            observed = json.loads((output / (oracle.PREFIX + self.token + ".applied.json")).read_text())
            self.assertEqual(observed["result"]["data"], "AP9B")

    def test_failed_cleanup_keeps_its_signed_result_and_still_reads_absence(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            with patch.object(oracle, "native_command", side_effect=[oracle.result(-2, b"", ""),
                    oracle.result(-13, b"\xff", "denied"), oracle.result(0, b"still present", "")]):
                oracle.collect(output, output, "1", self.token, "initial")
                oracle.collect(output, output, "2", self.token, "cleanup")
            receipt = json.loads((output / (oracle.PREFIX + self.token + ".cleanup.json")).read_text())
            self.assertEqual(receipt["removal"]["code"], -13)
            self.assertEqual(receipt["result"]["code"], 0)

    def test_uncertain_cleanup_cannot_be_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            with patch.object(oracle, "native_command", side_effect=[oracle.result(-2, b"", ""), TimeoutError]) as native:
                oracle.collect(output, output, "1", self.token, "initial")
                with self.assertRaises(TimeoutError):
                    oracle.collect(output, output, "2", self.token, "cleanup")
                with self.assertRaises(FileExistsError):
                    oracle.collect(output, output, "3", self.token, "cleanup")
            self.assertEqual(native.call_count, 2)


if __name__ == "__main__":
    unittest.main()
