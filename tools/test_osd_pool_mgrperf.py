"""Workload failure/cleanup checks; these substitutes are not native evidence."""
import ctypes
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from osd_pool_mode_mgrperf_oracle import mode_mgrperf


class MgrPerfWorkloadTests(unittest.TestCase):
    def run_workload(self, write_error=False, corrupt=False):
        data = {}
        def write(io, name, value, size):
            data[name] = value
            return -5 if write_error else 0
        def read(io, name, buffer, size, offset):
            value = data[name]
            if corrupt:
                value = b"x" * len(value)
            ctypes.memmove(buffer, value, len(value))
            return len(value)
        lib = SimpleNamespace(rados_get_instance_id=Mock(return_value=4242),
                              rados_write_full=Mock(side_effect=write), rados_read=Mock(side_effect=read),
                              rados_ioctx_destroy=Mock(), rados_shutdown=Mock())
        def check(code):
            if code < 0:
                raise RuntimeError("signed native failure")
        rados = SimpleNamespace(lib=lib, native=SimpleNamespace(check=check),
                                connect=Mock(return_value="handle"), ioctx=Mock(return_value="io"),
                                dump=Mock(return_value={"pools": [{"pool_name": "ceph-msgr-owned-1", "pool": 1}]}))
        # The pool naming helper reads the fixture's owned prefix; isolate that
        # selection without substituting any workload operation.
        from unittest.mock import patch
        with patch("osd_pool_mode_mgrperf_oracle.pool_name", return_value="ceph-msgr-owned-1"):
            if write_error or corrupt:
                with self.assertRaises(RuntimeError):
                    mode_mgrperf(rados, "outer", {"slot": 1}, None)
            else:
                result = mode_mgrperf(rados, "outer", {"slot": 1}, None)["workload"]
                self.assertEqual((result["global_id"], result["ops"], result["bytes"]), (4242, 6, 10758))
                self.assertEqual(lib.rados_write_full.call_count, 3)
                self.assertEqual(lib.rados_read.call_count, 3)
        lib.rados_ioctx_destroy.assert_called_once_with("io")
        lib.rados_shutdown.assert_called_once_with("handle")
        if write_error:
            self.assertEqual(lib.rados_write_full.call_count, 1)
            lib.rados_read.assert_not_called()
        if corrupt:
            self.assertEqual(lib.rados_write_full.call_count, 1)
            self.assertEqual(lib.rados_read.call_count, 1)

    def test_verified_workload(self):
        self.run_workload()

    def test_signed_failure_stops_without_replay_and_closes(self):
        self.run_workload(write_error=True)

    def test_readback_mismatch_stops_and_closes(self):
        self.run_workload(corrupt=True)
