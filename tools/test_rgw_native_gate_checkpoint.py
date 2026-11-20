"""Receipt tests of TestRGWNativeCheckpointIntegration (tools/rgw_native_gate_checkpoint.py)."""
import unittest

import rgw_native_gate as gate
import rgw_native_gate_checkpoint as checkpoint
from test_rgw_native_gate import IMAGE, stream

SMALL = checkpoint.TEST
SAMPLE = checkpoint.MARKER + "image=%s source=%s object_index_user_cleanup=confirmed" % (IMAGE, gate.SOURCE)


class CheckpointReceiptTest(unittest.TestCase):
    def test_receipt_names_the_source_and_its_cleanup(self):
        self.assertTrue(gate.verify(stream(SMALL), SMALL)["passed"])
        for name, line in {
            "no cleanup": SAMPLE.replace(" object_index_user_cleanup=confirmed", ""),
            "unconfirmed cleanup": SAMPLE.replace("=confirmed", "=skipped"),
            "other source": SAMPLE.replace(gate.SOURCE, "0" * 40),
        }.items():
            with self.subTest(name):
                result = gate.verify(stream(SMALL, line=line), SMALL)
                self.assertEqual(result["problems"], ["checkpoint receipt lacks its confirmed cleanup or pinned source"])


if __name__ == "__main__":
    unittest.main()
