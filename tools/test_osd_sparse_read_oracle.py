import unittest
import osd_sparse_read_oracle as oracle


class SparseReadOracleTest(unittest.TestCase):
    def test_request_bounds(self):
        self.assertEqual(oracle.request(dict(object="owned", offset=3, length=97)), (b"owned", 3, 97, None))
        self.assertEqual(oracle.request(dict(object="owned", offset=3, length=97, snap=4)), (b"owned", 3, 97, 4))
        self.assertEqual(oracle.request(dict(object="owned", offset=0, length=1, snap=oracle.MAX_SNAP))[3], oracle.MAX_UINT64 - 2)
        for raw in [dict(object="owned", offset=0, length=0), dict(object="owned", offset=True, length=1),
                    dict(object="owned", offset=oracle.MAX_UINT64, length=1),
                    dict(object="owned", offset=0, length=oracle.MAX_LENGTH + 1),
                    dict(object="owned", offset=0, length=1, snap=0),
                    dict(object="owned", offset=0, length=1, snap=oracle.MAX_UINT64 - 1),
                    dict(object="owned", offset=0, length=1, snap=oracle.MAX_UINT64),
                    dict(object="owned", offset=0, length=1, snap=True),
                    dict(object="owned", offset=0, length=1, snap=None),
                    dict(object="owned", offset=0, length=1, snap=1, extra=1)]:
            with self.assertRaises(ValueError):
                oracle.request(raw)

    def test_native_range_and_payload_not_dense(self):
        result = dict(code=2, extents=[dict(offset=3, length=2), dict(offset=8, length=1)], data_hex="00ff41", version=1)
        self.assertEqual(oracle.validate_observation(result, 3, 7), b"\0\xffA")
        for field, value in [("code", 0), ("data_hex", "00"), ("data_hex", "GG"),
                             ("extents", [dict(offset=2, length=3)]),
                             ("extents", [dict(offset=3, length=0)])]:
            with self.assertRaises(ValueError):
                oracle.validate_observation(dict(result, **{field: value}), 3, 7)
        self.assertEqual(oracle.validate_observation(dict(code=-2, extents=[], data_hex="", version=0), 0, 1), b"")


class SparseReadExecutionDiagnosticTest(unittest.TestCase):
    def test_native_signal_and_stderr_are_preserved(self):
        from unittest.mock import patch
        import subprocess
        observed = subprocess.CompletedProcess(["oracle"], -5, b"", b"native sparse-read stage: init2\n")
        with patch.object(oracle.subprocess, "run", return_value=observed) as run:
            _, receipt, failed = oracle.native_execution(["oracle"])
        self.assertTrue(failed)
        self.assertEqual(receipt["returncode"], -5)
        self.assertIn("init2", receipt["stderr"])
        self.assertEqual(run.call_args.kwargs["env"]["CEPH_MSGR_NATIVE_SPARSE_READ_TRACE"], "1")

    def test_failure_diagnostics_remain_bounded(self):
        from unittest.mock import patch
        import subprocess
        observed = subprocess.CompletedProcess(["oracle"], 1, b"x" * 1024, b"e" * 5000)
        with patch.object(oracle.subprocess, "run", return_value=observed):
            _, receipt, failed = oracle.native_execution(["oracle"])
        self.assertTrue(failed)
        self.assertTrue(receipt["stderr_truncated"])
        self.assertEqual(len(receipt["stderr"]), 4096)
        self.assertEqual(len(receipt["stdout_prefix"]), 512)

    def test_native_timeout_retains_last_stage(self):
        from unittest.mock import patch
        import subprocess
        error = subprocess.TimeoutExpired(["oracle"], 30, output=b"", stderr=b"native sparse-read stage: connect\n")
        with patch.object(oracle.subprocess, "run", side_effect=error):
            _, receipt, failed = oracle.native_execution(["oracle"])
        self.assertTrue(failed)
        self.assertTrue(receipt["timed_out"])
        self.assertIn("connect", receipt["stderr"])


if __name__ == "__main__":
    unittest.main()
