import os
from pathlib import Path
import unittest
from unittest.mock import patch

import build_sparse_read_oracle as builder


class BuildSparseReadOracleTest(unittest.TestCase):
    def test_toolset_linker_and_image_loader_use_separate_environments(self):
        with patch.dict(os.environ, {"LD_LIBRARY_PATH": "/native-cache/work/build/lib", "KEEP": "value"}):
            compile_env, runtime = builder.build_environments(Path("/usr/lib64/librados.so.2"))
        self.assertEqual(compile_env["LD_LIBRARY_PATH"],
                         "/opt/rh/gcc-toolset-14/root/usr/lib64:/usr/lib64:/usr/lib64/ceph")
        self.assertEqual(runtime["LD_LIBRARY_PATH"], "/usr/lib64:/usr/lib64/ceph")
        self.assertEqual(runtime["KEEP"], "value")
        self.assertNotIn("native-cache", compile_env["LD_LIBRARY_PATH"])
        self.assertNotIn("gcc-toolset", runtime["LD_LIBRARY_PATH"])
        compile_env["KEEP"] = "changed"
        self.assertEqual(runtime["KEEP"], "value")


if __name__ == "__main__":
    unittest.main()
