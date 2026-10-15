#!/usr/bin/env python3
"""C adapter and receipt-bound tests; these do not claim daemon execution."""
import base64
import ctypes as C
import struct
import unittest

import osd_checksum_oracle as oracle


class Function:
    def __init__(self, call=lambda *args: 0):
        self.call = call

    def __call__(self, *args):
        return self.call(*args)


class Library:
    def __init__(self, checksum):
        self.checksum = checksum

    def __getattr__(self, name):
        function = Function(self.checksum if name == "rados_checksum" else lambda *args: 0)
        setattr(self, name, function)
        return function


class ChecksumOracleTests(unittest.TestCase):
    def request(self, **overrides):
        raw = dict(object="owned-object", offset=2, length=8, chunk_size=4, seed=0x12345678, type=2)
        raw.update(overrides)
        return oracle.validate_request(raw)

    def test_native_abi_preserves_seed_and_read_extent(self):
        def checksum(io, name, algorithm, seed, seed_len, length, offset, chunk, output, output_size):
            self.assertEqual((io, name, algorithm, seed, seed_len, length, offset, chunk, output_size),
                             (7, b"owned-object", 2, b"\x78\x56\x34\x12", 4, 8, 2, 4, 12))
            C.memmove(output, struct.pack("<III", 2, 0x12345678, 0xffffffff), 12)
            return 0
        result = oracle.Native(Library(checksum)).checksum(7, self.request())
        self.assertEqual(result["values"], [0x12345678, 0xffffffff])
        self.assertEqual(base64.b64decode(result["data"]), struct.pack("<III", 2, 0x12345678, 0xffffffff))

    def test_eof_output_remains_distinct_from_zero_count(self):
        result = oracle.Native(Library(lambda *args: 0)).checksum(7, self.request())
        self.assertEqual(result, dict(code=0, data="", values=[]))
        def empty(*args):
            C.memmove(args[-2], b"\0\0\0\0", 4)
            return 0
        result = oracle.Native(Library(empty)).checksum(7, self.request())
        self.assertEqual(base64.b64decode(result["data"]), b"\0\0\0\0")

    def test_signed_error_has_no_interpreted_output(self):
        result = oracle.Native(Library(lambda *args: -22)).checksum(7, self.request())
        self.assertEqual(result, dict(code=-22, data="", values=[]))

    def test_bad_native_count_does_not_escape_buffer(self):
        def bad(*args):
            C.memmove(args[-2], b"\xff\xff\xff\xff", 4)
            return 0
        with self.assertRaises(ValueError):
            oracle.Native(Library(bad)).checksum(7, self.request())

    def test_invalid_fields_refused_before_native_access(self):
        for fields in (dict(type=0), dict(type=1), dict(offset=(1 << 64)-2), dict(length=0),
                       dict(length=oracle.MAX_READ+1), dict(length=7), dict(seed=True), dict(chunk_size=-1)):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                self.request(**fields)


if __name__ == "__main__":
    unittest.main()
