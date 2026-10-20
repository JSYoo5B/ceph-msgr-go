"""Generate independent msgr2.1 vectors; requires Python cryptography in dev only.

Wire definition: Ceph v20.2.4 src/msg/async/frames_v2.{h,cc} and crypto_onwire.cc.
The vectors are specification-derived, not traffic captured from a Ceph daemon.
"""
import json
import struct
from pathlib import Path
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def crc(seed, data):
    for b in data:
        seed ^= b
        for _ in range(8):
            seed = (seed >> 1) ^ (0x82F63B78 if seed & 1 else 0)
    return seed


def encode(tag, segments, secure):
    while len(segments) > 1 and not segments[-1]:
        segments = segments[:-1]
    pre = bytes([tag, len(segments)])
    for i in range(4):
        pre += struct.pack("<IH", len(segments[i]), 8) if i < len(segments) else bytes(6)
    pre += bytes(2)
    pre += struct.pack("<I", crc(0, pre))
    if not secure:
        out = pre + segments[0]
        if segments[0]:
            out += struct.pack("<I", crc(0xFFFFFFFF, segments[0]))
        if len(segments) > 1:
            out += b"".join(segments[1:]) + b"\x0e"
            for i in range(1, 4):
                out += struct.pack("<I", crc(0xFFFFFFFF, segments[i]) if i < len(segments) else 0)
        return out
    nonce = bytearray(range(16, 28))
    aes = AESGCM(bytes(range(16)))

    def seal(p):
        out = aes.encrypt(bytes(nonce), p, None)
        counter = struct.unpack("<Q", nonce[4:])[0]
        nonce[4:] = struct.pack("<Q", counter + 1)
        return out

    def pad(p):
        return p + bytes((-len(p)) % 16)

    out = seal(pre + segments[0][:48].ljust(48, b"\0"))
    if len(segments[0]) > 48:
        out += seal(pad(segments[0][48:]))
    if len(segments) > 1:
        out += seal(b"".join(pad(p) for p in segments[1:]) + b"\x0e" + bytes(15))
    return out


cases = [
    ("empty", 20, [b""]),
    ("control", 1, [bytes(range(20))]),
    ("empty_first", 17, [b"", b"mon"]),
    ("four_segments", 17, [bytes(range(41)), b"front", b"", b"binary\0data"]),
    ("partial_inline", 2, [bytes(range(105))]),
]
vectors = [{"name": name, "tag": tag, "segments": [p.hex() for p in segs],
            "crc": encode(tag, segs, False).hex(), "secure": encode(tag, segs, True).hex()}
           for name, tag, segs in cases]
Path(__file__).with_name("frames.json").write_text(json.dumps(vectors, indent=2) + "\n")
