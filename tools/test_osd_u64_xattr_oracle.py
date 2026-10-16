import copy
import json
from pathlib import Path
import subprocess
import unittest
from unittest import mock

import osd_u64_xattr_oracle as oracle


def valid():
    return dict(phase="execute", object="owned-a", mirror="owned-b", stored="MTlcAHRhaWw=",
                exists=True, action="read", comparison=3, operand=(1 << 64)-1, flags=0)


class U64OracleTests(unittest.TestCase):
    def test_high_operand_and_stored_bytes_are_not_normalized(self):
        value = valid()
        value["stored"] = oracle.xattr.encode(b"19\0tail")
        parsed = oracle.request(value)
        self.assertEqual(parsed["stored"], b"19\0tail")
        self.assertEqual(parsed["operand"], (1 << 64)-1)
        self.assertEqual(parsed["object"], b"owned-a")
        for stored in (None, ""):
            value["stored"] = stored
            self.assertEqual(oracle.request(value)["stored"], None if stored is None else b"")

    def test_closed_fields_types_lengths_and_modes(self):
        edits = [dict(extra=1), dict(phase="remove"), dict(action="exec"), dict(comparison=0),
                 dict(comparison=7), dict(operand=1 << 64), dict(operand=True), dict(flags=1),
                 dict(exists=1), dict(mirror="owned-a"), dict(object=""), dict(stored="not base64"),
                 dict(stored=oracle.xattr.encode(b"x"*129))]
        for edit in edits:
            value = valid();value.update(edit)
            with self.subTest(edit=edit), self.assertRaises((ValueError, TypeError)):
                oracle.request(value)

    def test_native_cpp_receives_unsigned_decimal_operand(self):
        observed = subprocess.CompletedProcess([], 0, json.dumps(dict(code=1, version=7, raw_cmp_per_op_result_available=False)).encode(), b"")
        with mock.patch.object(oracle.subprocess, "run", return_value=observed) as run:
            value = oracle.request(valid())
            result = oracle.execute(Path("/fixture"), Path("/out"), {"pool":"owned-pool"}, value)
            args = run.call_args.args[0]
            self.assertEqual(args[0], "/out/osd_u64_xattr_oracle")
            self.assertEqual(args[4:8], ["owned-b", "read", "3", str((1 << 64)-1)])
            self.assertEqual(result["code"], 1)
            self.assertFalse(result["raw_cmp_per_op_result_available"])

    def test_native_timeout_preserves_bounded_failure(self):
        with mock.patch.object(oracle.subprocess, "run", side_effect=subprocess.TimeoutExpired("oracle", 30, stderr=b"stage: compare")), mock.patch.object(oracle.omap, "publish") as publish:
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                oracle.execute(Path("/fixture"), Path("/out"), {"pool":"owned"}, oracle.request(valid()))
            self.assertTrue(publish.call_args.args[2]["native_execution"]["timed_out"])

    def test_invented_native_prval_or_invalid_unsigned_version_refused(self):
        for value in (dict(code=1,version=1,raw_cmp_per_op_result_available=True),
                      dict(code=1,version=-1,raw_cmp_per_op_result_available=False),
                      dict(code=True,version=1,raw_cmp_per_op_result_available=False)):
            observed = subprocess.CompletedProcess([],0,json.dumps(value).encode(),b"")
            with mock.patch.object(oracle.subprocess,"run",return_value=observed), self.assertRaises(ValueError):
                oracle.execute(Path("/fixture"),Path("/out"),{"pool":"owned"},oracle.request(valid()))


if __name__ == "__main__":
    unittest.main()
