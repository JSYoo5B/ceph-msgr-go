"""Native notify helper bounds/ownership tests; no simulated compatibility claim."""
import copy
import ctypes as C
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import osd_watch_notify_oracle as oracle

METADATA = {"pool": "pool", "pool_id": 7, "object": "fixture"}
OBJECT = "fixture-native-omapw-main-" + "a" * 32


def scope(done=False):
    return dict(schema_version=1, token="1" * 32, source_commit=oracle.xattr.SOURCE_COMMIT,
                object=OBJECT, input=dict(payload_hex="00ff", timeout_ms=1000), ready=True, done=done,
                ownership=dict(schema_version=1, source_commit=oracle.xattr.SOURCE_COMMIT,
                               pool="pool", pool_id=7, fixture_object="fixture", namespace="", objects=[OBJECT]))


class NotifyOracleTests(unittest.TestCase):
    def test_native_binary_notify_and_partial_response_release(self):
        blob = C.create_string_buffer(b"\0\xffack")
        reply_buffer = C.create_string_buffer(b"native reply bytes")
        acks = (oracle.NativeAck * 1)(oracle.NativeAck(51,81,C.addressof(blob),5))
        missed = (oracle.NativeMissed * 1)(oracle.NativeMissed(52,82))
        lib = SimpleNamespace(**{name: MagicMock() for name in (
            "rados_notify2", "rados_decode_notify_response", "rados_free_notify_response", "rados_buffer_free")})

        def notify(_io, name, payload, length, timeout, out, out_length):
            self.assertEqual((name,C.string_at(payload,length),timeout),(b"owned",b"\0\xffinput",1000))
            out._obj.value = C.addressof(reply_buffer)
            out_length._obj.value = len(b"native reply bytes")
            return -110

        def decode(_reply, _length, out, count, missed_out, missed_count):
            C.cast(out,C.POINTER(C.POINTER(oracle.NativeAck)))[0] = C.cast(acks,C.POINTER(oracle.NativeAck))
            C.cast(missed_out,C.POINTER(C.POINTER(oracle.NativeMissed)))[0] = C.cast(missed,C.POINTER(oracle.NativeMissed))
            count._obj.value = 1
            missed_count._obj.value = 1
            return 0

        lib.rados_notify2.side_effect = notify
        lib.rados_decode_notify_response.side_effect = decode
        native = SimpleNamespace(lib=lib,check=oracle.xattr.Native.check)
        got = oracle.native_notify(native,C.c_void_p(),"owned",b"\0\xffinput",1000)
        self.assertEqual(got["return_code"],-110)
        self.assertEqual(got["replies"],[dict(global_id=51,cookie=81,payload_hex="00ff61636b")])
        self.assertEqual(got["missed"],[dict(global_id=52,cookie=82)])
        lib.rados_free_notify_response.assert_called_once()
        lib.rados_buffer_free.assert_called_once()

    def test_binary_input_and_strict_bounds(self):
        self.assertEqual(oracle.check_input(dict(payload_hex="00ff", timeout_ms=1000)), (b"\0\xff", 1000))
        for bad in [None, {}, dict(payload_hex="00", timeout_ms=True), dict(payload_hex="00", timeout_ms=99),
                    dict(payload_hex="00", timeout_ms=10001), dict(payload_hex="0", timeout_ms=1000),
                    dict(payload_hex="FF", timeout_ms=1000), dict(payload_hex="00" * 4097, timeout_ms=1000),
                    dict(payload_hex="00", timeout_ms=1000, extra=1)]:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                oracle.check_input(bad)

    def test_token_rejects_paths(self):
        for token in [None, "../outside", "f" * 31, "g" * 32, "/" + "f" * 32]:
            with self.subTest(token=token), self.assertRaises(ValueError):
                oracle.token_name(token, "status")

    def test_receipt_ownership_and_immutable_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            status = scope()
            oracle.omap.publish(out, oracle.NAME + "-status", status)
            oracle.omap.publish(out, oracle.token_name(status["token"], "status"), status)
            self.assertEqual(oracle.current(out, METADATA), status)
            newer = dict(status, done=True)
            oracle.omap.publish(out, oracle.NAME + "-status", newer)
            # Readers use the token receipt until the publication completes.
            self.assertEqual(oracle.current(out, METADATA), status)
            for change in [dict(status, token="2" * 32), dict(status, object="other"),
                           dict(status, input=dict(payload_hex="ff", timeout_ms=1000))]:
                oracle.omap.publish(out, oracle.NAME + "-status", change)
                with self.assertRaises((ValueError, FileNotFoundError)):
                    oracle.current(out, METADATA)
            foreign = copy.deepcopy(status)
            foreign["ownership"]["pool_id"] = 8
            oracle.omap.publish(out, oracle.NAME + "-status", foreign)
            oracle.omap.publish(out, oracle.token_name(status["token"], "status"), foreign)
            with self.assertRaises(ValueError):
                oracle.current(out, METADATA)

    def test_active_scope_and_foreign_lock_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            status = scope()
            oracle.omap.publish(out, oracle.NAME + "-status", status)
            oracle.omap.publish(out, oracle.token_name(status["token"], "status"), status)
            with patch.object(oracle.watchers, "owned", return_value=(METADATA, OBJECT)), \
                    patch.object(oracle.subprocess, "Popen") as spawn:
                with self.assertRaises(ValueError):
                    oracle.run("start", out / "root", out)
                spawn.assert_not_called()
                self.assertEqual(oracle.current(out, METADATA), status)
                self.assertFalse((out / (oracle.NAME + "-control.lock")).exists())
                (out / (oracle.NAME + "-control.lock")).write_text("foreign lock")
                with self.assertRaises(FileExistsError):
                    oracle.run("start", out / "root", out)
                self.assertEqual((out / (oracle.NAME + "-control.lock")).read_text(), "foreign lock")


if __name__ == "__main__":
    unittest.main()
