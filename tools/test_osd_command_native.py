#!/usr/bin/env python3
"""Admission/argument/lifetime tests; no mocked Ceph compatibility claim."""
import base64
import ctypes as C
import unittest

import osd_command_native as command
from test_osd_xattr_native import Library


class OSDCommandNativeTests(unittest.TestCase):
    def test_admits_one_explicit_target_and_raw_json(self):
        request = command.validate_request({"osd": 2, "command": '{"prefix":"version"}', "input": base64.b64encode(b"\0\xff").decode()})
        self.assertEqual((request["osd"], request["command"], request["input"], request["role"]), (2, b'{"prefix":"version"}', b"\0\xff", "test"))
        request = command.validate_request({"pgid": "2.1f", "role": "readonly", "command": '{"prefix":"query","pgid":"2.1f"}'})
        self.assertEqual((request["pgid"], request["role"]), (b"2.1f", "readonly"))
        for raw in [{"command": '{"prefix":"version"}'}, {"osd": 1, "pgid": "1.0", "command": '{"prefix":"v"}'},
                    {"osd": -1, "command": '{"prefix":"v"}'}, {"osd": True, "command": '{"prefix":"v"}'},
                    {"osd": 1, "command": '["version"]'}, {"osd": 1, "command": '{"prefix":1}'},
                    {"osd": 1, "command": ""}, {"pgid": "", "command": '{"prefix":"v"}'},
                    {"osd": 1, "command": '{"prefix":"v"}', "role": "admin"}, {"osd": 1, "command": '{"prefix":"v"}', "extra": 1}]:
            with self.subTest(raw=raw), self.assertRaises((ValueError, TypeError)):
                command.validate_request(raw)

    def test_run_copies_outputs_and_frees_native_buffers(self):
        library = Library()
        keep = []

        def osd_command(cluster, osd, cmds, count, inbuf, inlen, out, outlen, status, statuslen):
            self.assertEqual((osd, cmds[0], count, inbuf[:inlen]), (3, b'{"prefix":"version"}', 1, b"in\0"))
            data, text = C.create_string_buffer(b"\0\xffout", 5), C.create_string_buffer(b"status", 6)
            keep.extend((data, text))
            out._obj.value, outlen._obj.value = C.addressof(data), 5
            status._obj.value, statuslen._obj.value = C.addressof(text), 6
            return -13

        library.install("rados_osd_command", osd_command)
        native = command.Native.__new__(command.Native)
        native.lib = library
        result = native.run(1, {"osd": 3, "command": b'{"prefix":"version"}', "input": b"in\0"})
        self.assertEqual(result, {"code": -13, "data": base64.b64encode(b"\0\xffout").decode(), "message": base64.b64encode(b"status").decode()})
        self.assertEqual([name for name, _ in library.calls].count("rados_buffer_free"), 2)


if __name__ == "__main__":
    unittest.main()
