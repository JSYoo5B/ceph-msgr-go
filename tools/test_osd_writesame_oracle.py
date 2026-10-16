import json
from pathlib import Path
import subprocess
import unittest
from unittest import mock
import osd_writesame_oracle as oracle


def valid():
    return dict(phase="execute",object="owned-a",mirror="owned-b",exists=True,
                action="plain",offset=17,length=12,pattern=oracle.xattr.encode(b"\0\xffx"),flags=0,snap=0)


class WriteSameOracleTests(unittest.TestCase):
    def test_pattern_is_not_expanded_or_normalized(self):
        value=oracle.request(valid())
        self.assertEqual(value["pattern"],b"\0\xffx")
        self.assertEqual(value["length"],12)
        zero=valid();zero["length"]=0
        self.assertEqual(oracle.request(zero)["length"],0)

    def test_closed_boundaries(self):
        for edit in [dict(extra=1),dict(phase="cleanup"),dict(action="exec"),dict(exists=1),dict(mirror="owned-a"),dict(offset=(1<<32)+1),dict(offset=True),dict(length=4097),dict(length=11),dict(pattern=""),dict(pattern=oracle.xattr.encode(b"x"*257)),dict(flags=1),dict(flags=2),dict(snap=1),dict(action="snapshot"),dict(action="failok")]:
            value=valid();value.update(edit)
            with self.subTest(edit=edit),self.assertRaises((ValueError,TypeError)):
                oracle.request(value)

    def test_native_receives_original_pattern_and_logical_length(self):
        observed=subprocess.CompletedProcess([],0,json.dumps(dict(code=0,version=7,raw_writesame_per_op_result_available=False)).encode(),b"")
        with mock.patch.object(oracle.subprocess,"run",return_value=observed) as run:
            result=oracle.execute(Path("/fixture"),Path("/out"),{"pool":"owned"},oracle.request(valid()))
            self.assertEqual(run.call_args.args[0][4:],["owned-b","plain","17","12","00ff78","0","0"])
            self.assertFalse(result["raw_writesame_per_op_result_available"])

    def test_timeout_preserves_first_failure(self):
        with mock.patch.object(oracle.subprocess,"run",side_effect=subprocess.TimeoutExpired("oracle",30,stderr=b"writesame")),mock.patch.object(oracle.omap,"publish") as publish:
            with self.assertRaisesRegex(RuntimeError,"timed out"):
                oracle.execute(Path("/fixture"),Path("/out"),{"pool":"owned"},oracle.request(valid()))
            self.assertTrue(publish.call_args.args[2]["native_execution"]["timed_out"])

    def test_invented_per_op_and_invalid_results_are_rejected(self):
        for value in [dict(code=0,version=7,raw_writesame_per_op_result_available=True),dict(code=True,version=7,raw_writesame_per_op_result_available=False),dict(code=0,version=-1,raw_writesame_per_op_result_available=False)]:
            observed=subprocess.CompletedProcess([],0,json.dumps(value).encode(),b"")
            with mock.patch.object(oracle.subprocess,"run",return_value=observed),self.assertRaises(ValueError):
                oracle.execute(Path("/fixture"),Path("/out"),{"pool":"owned"},oracle.request(valid()))

if __name__=="__main__":unittest.main()
