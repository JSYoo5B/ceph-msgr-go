#!/usr/bin/env python3
"""Independent Tentacle mutation wire inputs checked by native ceph-dencoder.

Development only; uses no Go codec. Native re-export retains decoded payload
and proves decoder acceptance, not execution or native payload encoding.
Field definitions: pinned LGPL-2.1 Ceph MOSDOp{,Reply}.h, osd_types.{h,cc},
rados.h, msgr.h and Message.{h,cc}; no native implementation is incorporated.
"""
import hashlib
import json
import pathlib
import struct
import subprocess
import sys
import tempfile

COMMIT = "7f793731f1b39eb4f465e960113d2363c311b964"
FEATURES = 3026423347916342070
OBJECT = "mutation-codec"
OPS = [
    (0x220d, 1, 0, 0, b""),
    (0x2201, 0, 2, 4, b"\x00\xffA\n"),
    (0x2202, 0, 0, 3, b"new"),
    (0x2206, 0, 0, 2, b"\x80!"),
    (0x2203, 0, 7, 0, b""),
    (0x2204, 0, 1, 2, b""),
    (0x1208, 0, 0, 19, b""),
    (0x1220, 2, 1, 2, b"\x00\xff"),
    (0x2205, 0, 0, 0, b""),
]


def pack(fmt, *args):
    return struct.pack("<" + fmt, *args)


def blob(data):
    return pack("I", len(data)) + data


def text(value):
    return blob(value.encode())


def versioned(version, compat, body):
    return pack("BBI", version, compat, len(body)) + body


def locator(pool=7, key="route-key", namespace="tenant"):
    return versioned(6, 3, pack("qI", pool, 0xffffffff) + text(key) + text(namespace) + pack("q", -1))


def pg():
    return pack("BQII", 1, 7, 0x11223344, 0xffffffff)


def operation(op):
    code, flags, offset, length, data = op
    return pack("HIQQQII", code, flags, offset, length, 0, 0, len(data))


def request(ops):
    front = pack("IIIIIQI", 1, 11, 0x4000034, 1700000000, 123456789, 0, 0)
    front += locator() + pg() + text(OBJECT) + pack("H", len(ops))
    front += b"".join(operation(op) for op in ops)
    front += pack("QQIIQ", 0xfffffffffffffffe, 0, 0, 0, FEATURES)
    front += versioned(2, 2, pack("BQQI", 0, 0, 0, 0))
    return front, b"".join(op[4] for op in ops)


def reply(ops, result, perops, flags=4):
    front = text(OBJECT) + pg() + pack("QiQII", flags, result, 20, 11, 11)
    front += pack("I", len(ops))
    # Mutation input remains in the union; reply payload_len is output only.
    front += b"".join(operation((*op[:4], b"")) for op in ops)
    front += pack("i", 0) + b"".join(pack("i", code) for code in perops)
    front += pack("QIQ", 20, 11, 19)
    front += versioned(1, 1, locator(-1, "", "") + text("") + blob(b""))
    front += pack("QQQ", 0, 0, 0)
    return front, b""


def envelope(kind, front, data):
    compat = 3 if kind == 42 else 2
    header = pack("QQHHHIIIHBQHHI", 1, 77, kind, 127, 6, len(front), 0, len(data), 0, 8, 123, compat, 0, 0)
    footer = pack("IIIB", 0, 0, 0, 1)
    return header + footer + blob(front) + blob(b"") + blob(data)


def extract(raw):
    if len(raw) < 78:
        raise ValueError("truncated native message envelope")
    offset, fields = 66, []
    for _ in range(3):
        size, = struct.unpack_from("<I", raw, offset)
        offset += 4
        if size > 1 << 20 or offset + size > len(raw):
            raise ValueError("invalid native message segment length")
        fields.append(raw[offset:offset + size])
        offset += size
    if offset != len(raw) or fields[1]:
        raise ValueError("invalid native envelope trailing bytes/middle")
    return fields[0], fields[2]


def vectors():
    return {
        "mutation-request-v6-all": (42, *request(OPS)),
        "mutation-reply-v6-all-success": (43, *reply(OPS, 0, [0] * len(OPS))),
        "mutation-reply-v6-exclusive-eexist": (43, *reply([OPS[0]], -17, [-17])),
        "mutation-reply-v6-cmpext-mismatch": (43, *reply([OPS[7]], -4097, [-4097])),
        # PrimaryLogPG may normalize zero covering the tail into truncate.
        "mutation-reply-v6-zero-as-truncate": (43, *reply([(0x2203, 0, 1, 0, b"")], 0, [0])),
    }


def collect(directory):
    directory.mkdir(parents=True, exist_ok=True)
    version = subprocess.check_output(["ceph-dencoder", "version"], text=True).strip()
    if "20.2.4" not in version and COMMIT not in version:
        raise ValueError("native oracle is not the pinned Tentacle release: " + version)
    output = {"ceph_version": version, "ceph_commit": COMMIT, "features": FEATURES,
              "native_payload_encoder_verified": False,
              "validation": "Independent Python field encoding; native decoder acceptance and envelope re-export only. Message::encode retains nonempty decoded front and does not rerun encode_payload.",
              "fixtures": {}}
    for name, (kind, front, data) in vectors().items():
        original = envelope(kind, front, data)
        source, native = directory / (name + "-input.bin"), directory / (name + "-native.bin")
        source.write_bytes(original)
        argv = ["ceph-dencoder", "type", "MOSDOp" if kind == 42 else "MOSDOpReply", "import", str(source), "decode", "dump_json", "set_features", str(FEATURES), "encode", "export", str(native)]
        summary = json.loads(subprocess.check_output(argv, text=True))
        exported = native.read_bytes()
        native_front, native_data = extract(exported)
        if native_front != front or native_data != data:
            raise ValueError(name + " native re-export changed front/data")
        output["fixtures"][name] = {"type": kind, "version": 6, "compat": 3 if kind == 42 else 2,
                                    "front_hex": native_front.hex(), "data_hex": native_data.hex(),
                                    "front_bytes": len(front), "data_bytes": len(data),
                                    "input_envelope_sha256": hashlib.sha256(original).hexdigest(),
                                    "native_envelope_sha256": hashlib.sha256(exported).hexdigest(),
                                    "summary": summary, "native_argv": argv}
    return output


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="ceph-mutation-codec-") as directory:
        print(json.dumps(collect(pathlib.Path(directory)), sort_keys=True))
