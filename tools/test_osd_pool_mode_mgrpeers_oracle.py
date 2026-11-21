import base64
import pathlib
import socket
import sys
import threading
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import osd_pool_oracle  # noqa: F401  (registers the mode modules)
import osd_pool_mode_mgrpeers_oracle as mgrpeers

TCP = """  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:1A85 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 100 1 0 100 0 0 10 0
   1: 0A00000A:9C40 0200000A:1A85 01 00000000:00000000 00:00000000 00000000     0        0 101 1 0 20 4 30 10 -1
   2: 0A00000A:9C41 0200000A:1A85 06 00000000:00000000 03:00000000 00000000     0        0 0 3 0
   3: 0A00000A:9C42 0300000A:1A85 01 00000000:00000000 00:00000000 00000000     0        0 102 1 0 20 4 30 10 -1
"""
TCP6 = """  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0000000000000000FFFF00000A00000A:9C43 0000000000000000FFFF00000200000A:1A85 01 00000000:00000000 00:00000000 00000000     0        0 103 1 0 20 4 30 10 -1
   1: 00000000000000000000000001000000:9C44 00000000000000000000000001000000:1A85 01 00000000:00000000 00:00000000 00000000     0        0 104 1 0 20 4 30 10 -1
"""


class MgrPeersInputs(unittest.TestCase):
    def test_proc_endpoints_decode_ipv4_and_mapped(self):
        self.assertEqual(mgrpeers.proc_endpoint("0200000A:1A85"), ("10.0.0.2", 6789))
        self.assertEqual(mgrpeers.proc_endpoint("0000000000000000FFFF00000200000A:1A85"), ("10.0.0.2", 6789))
        self.assertIsNone(mgrpeers.proc_endpoint("00000000000000000000000001000000:1A85"))

    def test_only_established_connections_to_the_target_count(self):
        target = ("10.0.0.2", 6789)
        self.assertEqual(mgrpeers.established_to([TCP, TCP6], target), 2)
        self.assertEqual(mgrpeers.established_to([TCP], ("10.0.0.3", 6789)), 1)
        self.assertEqual(mgrpeers.established_to([TCP], ("10.0.0.2", 6790)), 0)
        self.assertEqual(mgrpeers.established_to([], target), 0)

    def test_endpoint_is_ipv4_with_a_port(self):
        self.assertEqual(mgrpeers.endpoint("10.0.0.2:6800"), ("10.0.0.2", 6800))
        for bad in ("10.0.0.2", "[::1]:6800", "10.0.0.2:0", "host:6800", 6800):
            with self.assertRaises(ValueError):
                mgrpeers.endpoint(bad)

    def test_steps_are_bounded(self):
        good = {"tell": True, "command": '{"prefix":"x"}', "input": base64.b64encode(b"ab").decode()}
        self.assertEqual(mgrpeers.step(good, "s"), (True, ['{"prefix":"x"}'], b"ab"))
        for bad in ({}, dict(good, tell=1), dict(good, command="x" * 257), dict(good, input="!"),
                    dict(good, input=base64.b64encode(b"x" * 4097).decode()), dict(good, extra=1)):
            with self.assertRaises(ValueError):
                mgrpeers.step(bad, "s")

    def test_request_shape_is_closed_before_any_native_call(self):
        good = {"tell": False, "command": "c", "input": ""}
        base = {"mgr_name": "x", "address": "10.0.0.2:6800", "stalled": good, "answered": good, "stall_timeout": 30, "close_wait": 30}
        for request in ({}, dict(base, extra=1), dict(base, stall_timeout=41), dict(base, close_wait=4), dict(base, address="x")):
            with self.assertRaises(ValueError):
                mgrpeers.mode_mgrpeers(None, None, request, None)

    def test_stalled_handshake_reads_the_banner_and_sees_the_close(self):
        server = socket.create_server(("127.0.0.1", 0))
        received = []

        def serve():
            conn, _ = server.accept()
            conn.sendall(mgrpeers.BANNER + b"\x03\x00abc")
            received.append(conn.recv(16))
            conn.sendall(b"xy")
            conn.close()

        thread = threading.Thread(target=serve)
        thread.start()
        s, banner = mgrpeers.stalled_handshake(server.getsockname())
        try:
            thread.join(5)
            self.assertEqual(banner, mgrpeers.BANNER + b"\x03\x00abc")
            self.assertEqual(received, [b"ceph"])
            self.assertEqual(mgrpeers.wait_eof(s, 5), (True, 2))
        finally:
            s.close()
            server.close()

    def test_wait_eof_times_out_on_an_open_connection(self):
        server = socket.create_server(("127.0.0.1", 0))
        client = socket.create_connection(server.getsockname())
        conn, _ = server.accept()
        try:
            self.assertEqual(mgrpeers.wait_eof(client, 0.2), (False, 0))
        finally:
            client.close()
            conn.close()
            server.close()

    def test_mode_is_registered(self):
        self.assertIs(osd_pool_oracle.HANDLERS["mgrpeers"], mgrpeers.mode_mgrpeers)


if __name__ == "__main__":
    unittest.main()
