#!/usr/bin/env python3
"""Bounded shard-local PG-history receipt for the disposable fixture.

Test-only; never product code. The fixture (integration/fixture
shard_history.go) copies the data directory of every acting OSD of one PG,
one paused container at a time, and runs the offline reader of the pinned
Ceph 20.2.4 source (7f793731f1b39eb4f465e960113d2363c311b964) on each private
copy. The owned-pool oracle (osd_pool_oracle.py, mode ``shardhistory``) takes
the native placement before the first copy and after the last dump and passes
both, the fixture's capture record and the raw dumps here. This module copies
no Ceph code; it reads the JSON shape that source emits:

- src/tools/ceph_objectstore_tool.cc:4410-4448 selects the one collection of
  the exact ``spg_t`` given as ``--pgid``; :439-480 loads that collection's
  own log and missing set and prints them as ``pg_log_t`` and
  ``pg_missing_t`` (``pg_missing_set::dump``, src/osd/osd_types.h, emits
  ``missing`` and ``may_include_deletes``).
- The ``info`` operation prints that collection's ``pg_info_t``
  (src/osd/osd_types.cc ``pg_info_t::dump``): ``pgid`` is the ``spg_t``, with
  ``last_update``, ``last_complete`` and ``log_tail``.
- The ``pg_log_t`` section has the shape osd_pg_history_oracle.py parses.

One shard is complete only when the fixture attributed its copy (acting
index, OSD, ``whoami``, an isolated reader, a regular block file, the OSD
resumed, the reader and its copy removed), both offline operations exited 0
without diagnostics, both dumps are within the byte bound and parse as one
strict JSON document of exactly that shape, the info names that ``spg_t``,
the log runs from the info's ``log_tail`` to its ``last_update`` and the
missing set is empty. The receipt is complete only when every index of the
acting vector has a complete shard and the two placement records are
identical. Anything else fails closed.

A complete receipt reports where an exact identity occurs in each acting
shard's own log at that bracketed placement. The copies are sequential and
are not one atomic snapshot. A shard without a position is an observation of
that copy only; this module never states that an identity is absent from the
PG, was evicted or trimmed, or would not be replayed.
"""
import base64
import hashlib
import json

import osd_pg_history_oracle as history

MAX_SHARD_BYTES = 64 << 10
MAX_RECORD_BYTES = 64 << 10
MAX_SHARDS = 8
SOURCE_COMMIT = "7f793731f1b39eb4f465e960113d2363c311b964"
SHARD_KEYS = {"index", "osd", "spg", "reader_isolated", "paused", "resumed", "paused_ms", "store_archive_bytes", "whoami",
              "unsupported_layout", "info_exit_code", "info_bytes", "info_stderr", "info_file", "log_exit_code", "log_bytes",
              "log_stderr", "log_file", "reader_removed"}
HistoryError = history.HistoryError


def parse_offline_log(raw):
    """Parse one offline ``--op log`` dump; returns the log and the missing count."""
    document = history.strict_document(raw, MAX_SHARD_BYTES)
    if not isinstance(document, dict) or set(document) != {"pg_log_t", "pg_missing_t"}:
        raise HistoryError("offline output is not a pg_log_t and pg_missing_t dump")
    missing = document["pg_missing_t"]
    if (not isinstance(missing, dict) or set(missing) != {"missing", "may_include_deletes"}
            or not isinstance(missing["missing"], list) or type(missing["may_include_deletes"]) is not bool):
        raise HistoryError("pg_missing_t must have exactly a missing array and may_include_deletes")
    return history.parse_log_body(document["pg_log_t"]), len(missing["missing"])


def parse_offline_info(raw, spg):
    """The log bounds of one offline ``--op info`` dump for exactly ``spg``."""
    document = history.strict_document(raw, MAX_SHARD_BYTES)
    if not isinstance(document, dict) or not {"pgid", "last_update", "last_complete", "log_tail"} <= set(document):
        raise HistoryError("offline output is not a pg_info_t dump")
    if document["pgid"] != spg:
        raise HistoryError("offline info is for %r, not %s" % (str(document["pgid"])[:40], spg))
    return {name: history.parse_eversion(document[name], "info " + name) for name in ("last_update", "last_complete", "log_tail")}


def _shard(index, osd, pgid, record, dumps, identities):
    """One shard's receipt. ``record`` is the fixture's capture of that index."""
    spg = "%ss%d" % (pgid, index)
    receipt = {"index": index, "osd": osd, "spg": spg, "limit_bytes": MAX_SHARD_BYTES}
    failures = []
    if not isinstance(record, dict) or not SHARD_KEYS <= set(record) or set(record) - SHARD_KEYS - {"failure"}:
        return dict(receipt, complete=False, failures=["capture record has an unexpected shape"])
    receipt.update(whoami=record["whoami"], paused_ms=record["paused_ms"], store_archive_bytes=record["store_archive_bytes"])
    if record.get("failure"):
        failures.append("capture failed: " + str(record["failure"])[:200])
    if record["index"] != index or record["osd"] != osd or record["spg"] != spg:
        failures.append("capture is for index %r, osd %r, %r" % (record["index"], record["osd"], record["spg"]))
    if record["whoami"] != str(osd):
        failures.append("the copy's whoami is %r, not osd.%d" % (str(record["whoami"])[:20], osd))
    for flag, text in (("reader_isolated", "the reader was not isolated from every network"), ("paused", "the OSD was not paused for the copy"),
                       ("resumed", "the OSD was not resumed"), ("reader_removed", "the reader and its copy were not removed")):
        if record[flag] is not True:
            failures.append(text)
    if record["unsupported_layout"] != "":
        failures.append("unsupported block layout: " + str(record["unsupported_layout"])[:80])
    for op in ("info", "log"):
        if type(record[op + "_exit_code"]) is not int or record[op + "_exit_code"] != 0:
            failures.append("offline %s exited %r" % (op, record[op + "_exit_code"]))
        if record[op + "_stderr"] != "":
            failures.append("offline %s wrote diagnostics: %s" % (op, str(record[op + "_stderr"])[:200]))
    if failures:
        # An unattributed or refused copy is not read at all.
        return dict(receipt, complete=False, failures=failures)
    raw = {}
    for op in ("info", "log"):
        try:
            raw[op] = dumps(record[op + "_file"], MAX_SHARD_BYTES)
        except (OSError, ValueError) as error:
            failures.append("offline %s dump is unreadable: %s" % (op, str(error)[:120]))
            continue
        if len(raw[op]) != record[op + "_bytes"]:
            failures.append("offline %s dump has %d bytes; the capture recorded %r" % (op, len(raw[op]), record[op + "_bytes"]))
    if "log" in raw:
        receipt.update(bytes=len(raw["log"]), sha256=hashlib.sha256(raw["log"]).hexdigest(), raw=base64.b64encode(raw["log"]).decode())
    info = parsed = None
    try:
        if "info" in raw:
            info = parse_offline_info(raw["info"], spg)
    except HistoryError as error:
        failures.append("info: " + str(error))
    try:
        if "log" in raw:
            parsed, missing = parse_offline_log(raw["log"])
    except HistoryError as error:
        failures.append("log: " + str(error))
    if parsed is not None:
        receipt.update(head="%d'%d" % parsed["head"], tail="%d'%d" % parsed["tail"], log_entries=len(parsed["log"]),
                       dup_entries=len(parsed["dups"]), extra_reqid_entries=sum(len(e["extra_reqids"]) for e in parsed["log"]),
                       missing_entries=missing)
        receipt["targets"] = [{"reqid": history.reqid_text(identity), "locations": history.locate(parsed, identity)}
                              for identity in identities]
        if missing:
            failures.append("the shard's missing set has %d entries" % missing)
    if info is not None:
        receipt.update(last_update="%d'%d" % info["last_update"], last_complete="%d'%d" % info["last_complete"])
        if info["last_complete"] != info["last_update"]:
            failures.append("the shard's last_complete is behind its last_update")
        if parsed is not None and (info["last_update"] != parsed["head"] or info["log_tail"] != parsed["tail"]):
            failures.append("the log does not run from the info's log_tail to its last_update")
    receipt["failures"] = failures
    receipt["complete"] = not failures
    return receipt


def snapshot(before_raw, after, capture_raw, dumps, targets):
    """One fail-closed receipt over every acting shard.

    ``before_raw`` is the stored first placement record, ``after`` the
    placement read after the last dump, ``capture_raw`` the fixture's capture
    record and ``dumps(name, limit)`` returns one raw dump of at most limit
    bytes.
    """
    identities = [history.target_identity(t) for t in targets]
    if not 1 <= len(identities) <= history.MAX_TARGETS:
        raise ValueError("between 1 and %d history targets are required" % history.MAX_TARGETS)
    receipt = {"after": after, "all_acting_shards": True, "absence_established": False, "shards": []}
    failures = []

    def finish():
        receipt["failures"] = failures
        receipt["complete"] = not failures
        return receipt

    try:
        before = history.strict_document(before_raw, MAX_RECORD_BYTES)
        capture = history.strict_document(capture_raw, MAX_RECORD_BYTES)
    except HistoryError as error:
        failures.append("bracket or capture record: " + str(error))
        return finish()
    receipt["before"] = before
    if not isinstance(before, dict) or not isinstance(before.get("pgid"), str) or not isinstance(before.get("acting"), list):
        failures.append("the first bracket is not a placement record")
        return finish()
    pgid, acting = before["pgid"], before["acting"]
    receipt.update(pgid=pgid, acting=acting)
    if before != after:
        failures.append("placement or epoch changed between the before and after brackets")
    if (not isinstance(capture, dict) or capture.get("schema_version") != 1 or capture.get("source_commit") != SOURCE_COMMIT
            or not isinstance(capture.get("shards"), list)):
        failures.append("the capture record has an unexpected shape or provenance")
        return finish()
    if capture.get("before_sha256") != hashlib.sha256(before_raw).hexdigest():
        failures.append("the capture was not taken under this first bracket")
    if capture.get("complete") is not True:
        failures.append("the capture is incomplete")
    if capture.get("pgid") != pgid or capture.get("acting") != acting:
        failures.append("the capture is for %r acting %r" % (capture.get("pgid"), capture.get("acting")))
    if (not 2 <= len(acting) <= MAX_SHARDS or len(set(map(repr, acting))) != len(acting)
            or any(type(osd) is not int or not 0 <= osd < 1 << 31 for osd in acting)):
        failures.append("the acting vector is not between 2 and %d distinct OSDs" % MAX_SHARDS)
        return finish()
    if len(capture["shards"]) != len(acting):
        failures.append("the capture has %d shards for %d acting members" % (len(capture["shards"]), len(acting)))
    for index, osd in enumerate(acting):
        record = capture["shards"][index] if index < len(capture["shards"]) else None
        shard = _shard(index, osd, pgid, record, dumps, identities)
        receipt["shards"].append(shard)
        failures.extend("shard %d (osd.%d): %s" % (index, osd, text) for text in shard["failures"])
    return finish()
