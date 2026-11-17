"""The dedicated COPY_FROM clone/head version-classification row."""
import re
from ci_row_catalog import RowsTestCase

class CopyClassificationRowsTest(RowsTestCase):
    def test_dedicated_row_selects_the_classification_parent(self):
        selector = "^TestCephOSDCopyFromClassificationIntegration$"
        rows = [row for row in self.rows if row.get("test_run") == selector]
        self.assertEqual(rows, [{"key": "aes", "service": "aes", "mode": "crc",
            "family": "4", "runtime": "host", "stress": "", "osd": "1",
            "osd_store": "bluestore", "osd_replicas": "1",
            "osd_profile": "replicated-pg-reply-v8", "test_run": selector}])
        self.assertEqual({test for test in self.declared if re.search(selector, test)},
                         {"TestCephOSDCopyFromClassificationIntegration"})
