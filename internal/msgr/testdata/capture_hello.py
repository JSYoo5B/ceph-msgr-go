"""Capture only the pre-authentication banner and HELLO from a disposable MON.

Usage: python3 capture_hello.py HOST PORT > ceph-mon-hello-v20.2.4.bin
Run inside the development fixture when its addresses are container-local.
Requires only Python's standard library. No authentication exchange is sent.
"""
import socket
import struct
import sys


def read(sock, count):
    out = bytearray()
    while len(out) < count:
        part = sock.recv(count - len(out))
        if not part:
            raise EOFError("Ceph closed before HELLO capture")
        out.extend(part)
    return bytes(out)


def crc(seed, data):
    for byte in data:
        seed ^= byte
        for _ in range(8):
            seed = (seed >> 1) ^ (0x82F63B78 if seed & 1 else 0)
    return seed


with socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=3) as sock:
    sock.sendall(b"ceph v2\n" + struct.pack("<HQQ", 16, 3, 1))
    banner = read(sock, 26)
    if banner[:10] != b"ceph v2\n\x10\x00":
        raise ValueError("unexpected Messenger banner")
    preamble = read(sock, 32)
    if preamble[0] != 1 or preamble[1] != 1:
        raise ValueError("expected one-segment HELLO")
    if crc(0, preamble[:28]) != struct.unpack_from("<I", preamble, 28)[0]:
        raise ValueError("preamble CRC32C mismatch")
    length = struct.unpack_from("<I", preamble, 2)[0]
    if not 0 < length < 256:
        raise ValueError("unexpected HELLO size")
    payload, checksum = read(sock, length), read(sock, 4)
    if crc(0xFFFFFFFF, payload) != struct.unpack("<I", checksum)[0]:
        raise ValueError("payload CRC32C mismatch")
    print(f"client endpoint from getsockname: {sock.getsockname()}", file=sys.stderr)
    sys.stdout.buffer.write(banner + preamble + payload + checksum)
