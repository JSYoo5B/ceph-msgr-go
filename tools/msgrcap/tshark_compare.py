#!/usr/bin/env python3
"""Compare msgrcap TCP reassembly with tshark "follow,tcp,raw" output.

Development-only. tshark is an independent TCP reassembler; equal per-direction
SHA-256 digests show that both tools reconstructed the same byte streams. It
says nothing about Messenger interpretation.
"""
import hashlib
import json
import sys


def follow_streams(text):
    streams = []
    current = None
    for line in text.splitlines():
        if line.startswith("Follow: "):
            current = {"nodes": [], "data": [bytearray(), bytearray()]}
            streams.append(current)
        elif current is None:
            continue
        elif line.startswith("Node 0: ") or line.startswith("Node 1: "):
            current["nodes"].append(line.split(": ", 1)[1].strip())
        elif line.startswith("====") or line.startswith("Filter: "):
            continue
        elif line.strip():
            side = 1 if line.startswith("\t") else 0
            try:
                current["data"][side].extend(bytes.fromhex(line.strip()))
            except ValueError:
                continue
    return streams


def main():
    with open(sys.argv[1], encoding="utf-8", errors="replace") as f:
        streams = follow_streams(f.read())
    with open(sys.argv[2], encoding="utf-8") as f:
        report = json.load(f)
    by_pair = {(c["client"], c["server"]): c for c in report["connections"]}
    compared = mismatched = 0
    seen = set()
    for s in streams:
        if len(s["nodes"]) != 2:
            continue
        a, b = s["nodes"]
        digests = [hashlib.sha256(bytes(d)).hexdigest() for d in s["data"]]
        conn = by_pair.get((a, b))
        order = (0, 1)
        if conn is None:
            conn, order = by_pair.get((b, a)), (1, 0)
        if conn is None:
            print("tshark stream %s -> %s: no msgrcap connection" % (a, b))
            mismatched += 1
            continue
        c2s, s2c = digests[order[0]], digests[order[1]]
        ok = c2s == conn["client_to_server"]["reassembled_sha256"] and \
            s2c == conn["server_to_client"]["reassembled_sha256"]
        compared += 1
        mismatched += 0 if ok else 1
        print("conn %d %s -> %s: c->s %d bytes, s->c %d bytes, digests %s" % (
            conn["id"], conn["client"], conn["server"], len(s["data"][order[0]]),
            len(s["data"][order[1]]), "equal" if ok else "DIFFERENT"))
        seen.add(conn["id"])
    missing = sorted(c["id"] for c in report["connections"] if c["id"] not in seen)
    print("tshark reassembly comparison: %d streams compared, %d mismatched or unmatched, "
          "msgrcap connections without a tshark stream: %s" % (compared, mismatched, missing or "none"))
    return 1 if mismatched or missing else 0


if __name__ == "__main__":
    sys.exit(main())
