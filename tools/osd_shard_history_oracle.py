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
  (src/osd/osd_types.cc ``pg_info_t::dump``): ``pgid`` is the ``spg_t`` and
  ``shard`` its shard ID, with ``last_update``, ``last_complete`` and
  ``log_tail``.
- The ``pg_log_t`` section has the shape osd_pg_history_oracle.py parses.
- Each log entry's ``written_shards`` (src/osd/osd_types.cc
  ``pg_log_entry_t::dump``) is the stream form of a shard set
  (src/common/bitset_set.h: ``{}`` or ``{0,2,3}``, ascending). The pinned
  writer stores the empty set for an entry that names every shard
  (src/osd/ECTransaction.cc:873-886), so an empty set never means "no shard".
- The info's ``partial_writes_last_complete`` array holds one
  ``{"id", "from", "to"}`` record per shard that a shard able to be primary
  saw left out of a partial write (src/osd/PGBackend.cc:414-496), and
  ``partial_writes_last_complete_epoch`` the map epoch of its last change.

One shard is complete only when the fixture attributed its copy (acting
index, OSD, ``whoami``, an isolated reader, a regular block file, the OSD
resumed, the reader and its copy removed, each dump under the one file name
of that index and operation), both offline operations exited 0
without diagnostics, both dumps are within the byte bound and parse as one
strict JSON document of exactly that shape, the info names that ``spg_t``
and the acting index as its shard ID, every entry's ``written_shards`` and
every partial-write record parses, the log runs from the info's ``log_tail``
to its ``last_update`` and the missing set is empty. The log's entries must
end at its head, with one exception that ``advanced_head_failure`` bounds: a
nonprimary shard whose own info records the partial-write range between its
last entry and its head. The receipt reports that shape per shard as
``head_past_entries`` with the ``last_entry``. The receipt is complete only
when every index of the acting vector has a complete shard and the two
placement records are identical. Anything else fails closed.

A degraded erasure-coded PG keeps a lost shard's position in its acting
vector as CRUSH_ITEM_NONE. The fixture copies nothing for it and records it
as absent; the receipt lists those positions in ``absent_shards`` and then
does not claim ``all_acting_shards``. Such a position has no store to read,
so nothing is reported for it.

A complete receipt reports where an exact identity occurs in each acting
shard's own log at that bracketed placement. The copies are sequential and
are not one atomic snapshot. A shard without a position is an observation of
that copy only; this module never states that an identity is absent from the
PG, was evicted or trimmed, or would not be replayed.
"""
import base64
import hashlib
import json
import re

import osd_pg_history_oracle as history

MAX_SHARD_BYTES = 64 << 10
MAX_RECORD_BYTES = 64 << 10
MAX_SHARDS = 8
SOURCE_COMMIT = "7f793731f1b39eb4f465e960113d2363c311b964"
SHARD_KEYS = {"index", "osd", "spg", "absent", "reader_isolated", "paused", "resumed", "paused_ms", "store_archive_bytes", "whoami",
              "unsupported_layout", "info_exit_code", "info_bytes", "info_stderr", "info_file", "log_exit_code", "log_bytes",
              "log_stderr", "log_file", "reader_removed"}
HistoryError = history.HistoryError
# shard_id_set is a 128-bit set printed in ascending order.
MAX_SHARD_ID = 128
# CRUSH_ITEM_NONE: an empty position of an erasure-coded acting vector.
CRUSH_ITEM_NONE = 0x7fffffff
# What the fixture records for an empty position: nothing copied or read.
ABSENT_RECORD = {"absent": True, "reader_isolated": False, "paused": False, "resumed": False, "paused_ms": 0,
                 "store_archive_bytes": 0, "whoami": "", "unsupported_layout": "", "info_exit_code": None, "info_bytes": 0,
                 "info_stderr": "", "info_file": "", "log_exit_code": None, "log_bytes": 0, "log_stderr": "", "log_file": "",
                 "reader_removed": False}
SHARD_SET = re.compile(r"\{(?:(?:0|[1-9][0-9]{0,2})(?:,(?:0|[1-9][0-9]{0,2}))*)?\}")


def dump_name(index, op):
    """The one file the fixture writes for an acting index and operation."""
    return "osd-shard-history-s%d-%s.json" % (index, op)


def parse_written_shards(text, index):
    """The shard IDs of one entry's ``written_shards``, ascending.

    An empty list is the dump's empty set: the entry names every shard.
    """
    if not isinstance(text, str) or not SHARD_SET.fullmatch(text):
        raise HistoryError("log entry %d has an unparsable written_shards %s" % (index, repr(text)[:80]))
    shards = [int(n) for n in text[1:-1].split(",")] if text != "{}" else []
    if any(b <= a for a, b in zip(shards, shards[1:])) or any(shard >= MAX_SHARD_ID for shard in shards):
        raise HistoryError("log entry %d written_shards is not an ascending shard set" % index)
    return shards


def parse_offline_log(raw, head_past_entries=False):
    """Parse one offline ``--op log`` dump; returns the log and the missing count.

    ``head_past_entries`` admits a head newer than the last entry; see
    ``advanced_head_failure`` for the only shape that is then accepted.
    """
    document = history.strict_document(raw, MAX_SHARD_BYTES)
    if not isinstance(document, dict) or set(document) != {"pg_log_t", "pg_missing_t"}:
        raise HistoryError("offline output is not a pg_log_t and pg_missing_t dump")
    missing = document["pg_missing_t"]
    if (not isinstance(missing, dict) or set(missing) != {"missing", "may_include_deletes"}
            or not isinstance(missing["missing"], list) or type(missing["may_include_deletes"]) is not bool):
        raise HistoryError("pg_missing_t must have exactly a missing array and may_include_deletes")
    parsed = history.parse_log_body(document["pg_log_t"], head_past_entries)
    for index, entry in enumerate(parsed["log"]):
        entry["written_shards"] = parse_written_shards(entry["written_shards"], index)
    return parsed, len(missing["missing"])


def parse_offline_info(raw, spg):
    """The shard, log bounds and partial-write records of one offline ``--op info`` dump for exactly ``spg``."""
    return info_fields(history.strict_document(raw, MAX_SHARD_BYTES), spg)


def info_fields(document, spg):
    """The same fields of one strictly loaded ``pg_info_t`` dump, which must name exactly ``spg``."""
    if not isinstance(document, dict) or not {"pgid", "last_update", "last_complete", "log_tail"} <= set(document):
        raise HistoryError("offline output is not a pg_info_t dump")
    if document["pgid"] != spg:
        raise HistoryError("offline info is for %r, not %s" % (str(document["pgid"])[:40], spg))
    shard = document.get("shard")
    if not isinstance(shard, str) or not re.fullmatch(r"0|[1-9][0-9]{0,2}", shard) or int(shard) >= MAX_SHARD_ID:
        raise HistoryError("offline info has no shard ID")
    info = {name: history.parse_eversion(document[name], "info " + name) for name in ("last_update", "last_complete", "log_tail")}
    records, epoch = document.get("partial_writes_last_complete"), document.get("partial_writes_last_complete_epoch")
    if not isinstance(records, list) or not isinstance(epoch, str) or not epoch.isdigit() or len(epoch) > 10:
        raise HistoryError("offline info has no partial_writes_last_complete array and epoch")
    info["partial_writes"], previous = [], -1
    for record in records:
        if (not isinstance(record, dict) or set(record) != {"id", "from", "to"} or type(record["id"]) is not int
                or not previous < record["id"] < MAX_SHARD_ID):
            raise HistoryError("offline info has an unexpected partial_writes_last_complete record")
        previous = record["id"]
        start, end = (history.parse_eversion(record[name], "partial write " + name) for name in ("from", "to"))
        if start > end:
            raise HistoryError("a partial_writes_last_complete range ends before it starts")
        info["partial_writes"].append({"shard": record["id"], "from": "%d'%d" % start, "to": "%d'%d" % end})
    info["partial_writes_epoch"], info["shard"] = int(epoch), int(shard)
    return info


def advanced_head_failure(index, parsed, info):
    """Why a log whose head is newer than its last entry is not the one accepted shape.

    A nonprimary shard gets no sub-write for a partial write that does not
    name it. At the next peering the pinned source advances that shard's
    ``last_update`` and log head to the end of the partial-write range that
    the primary sent for it, without any entry (src/osd/PeeringState.cc
    :324-364, applied to the shard's own info and log at :430-434), and
    keeps the ranges it merged in the shard's own info (:369-421). The
    offline reader takes the log head from the info's ``last_update``
    (src/osd/PGLog.h:1683-1689).

    The shape is therefore accepted only for a shard that the caller named
    as nonprimary, whose log holds at least one entry and whose own info
    holds exactly one record for its own shard ID that starts at or before
    the last entry and ends at the head. Returns None when it is that shape.
    """
    if info is None:
        return "the log's head is newer than its last entry and the shard's info is unusable"
    if not parsed["log"]:
        return "the log's head is newer than its tail and it holds no entry"
    own = [record for record in info["partial_writes"] if record["shard"] == index]
    if len(own) != 1:
        return "the log's head is newer than its last entry without a partial-write record of this shard"
    start, end = (history.parse_eversion(own[0][name], "partial write " + name) for name in ("from", "to"))
    if not start <= parsed["last_entry"] < end or end != parsed["head"]:
        return "the log's head is newer than its last entry and this shard's partial-write record does not span them"
    return None


def _shard(index, osd, pgid, record, dumps, identities, nonprimary=()):
    """One shard's receipt. ``record`` is the fixture's capture of that index.

    ``nonprimary`` are the pool's native nonprimary shard IDs; only such a
    shard may show a log head newer than its last entry.
    """
    spg = "%ss%d" % (pgid, index)
    receipt = {"index": index, "osd": osd, "spg": spg, "limit_bytes": MAX_SHARD_BYTES}
    failures = []
    if not isinstance(record, dict) or not SHARD_KEYS <= set(record) or set(record) - SHARD_KEYS - {"failure"}:
        return dict(receipt, complete=False, failures=["capture record has an unexpected shape"])
    if osd == CRUSH_ITEM_NONE:
        # An empty position: the fixture must have copied and read nothing.
        empty = (record["index"] == index and record["osd"] == osd and record["spg"] == spg and "failure" not in record
                 and all(record[key] == value and type(record[key]) is type(value) for key, value in ABSENT_RECORD.items()))
        return dict(receipt, absent=True, complete=empty, failures=[] if empty else ["an empty position has a capture"])
    receipt.update(whoami=record["whoami"], paused_ms=record["paused_ms"], store_archive_bytes=record["store_archive_bytes"])
    if record["absent"] is not False:
        failures.append("a present member is recorded absent")
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
    for op in ("info", "log"):
        # A dump carries no acting index and the log dump no shard at all, so
        # only the canonical name binds a file to this index and operation.
        if record[op + "_file"] != dump_name(index, op):
            failures.append("the capture names %s as this shard's %s dump" % (repr(record[op + "_file"])[:60], op))
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
    if "info" in raw:
        receipt.update(info_bytes=len(raw["info"]), info_sha256=hashlib.sha256(raw["info"]).hexdigest(),
                       info_raw=base64.b64encode(raw["info"]).decode())
    info = parsed = None
    try:
        if "info" in raw:
            info = parse_offline_info(raw["info"], spg)
    except HistoryError as error:
        failures.append("info: " + str(error))
    try:
        if "log" in raw:
            parsed, missing = parse_offline_log(raw["log"], index in nonprimary)
    except HistoryError as error:
        failures.append("log: " + str(error))
    if parsed is not None:
        receipt.update(head="%d'%d" % parsed["head"], tail="%d'%d" % parsed["tail"], log_entries=len(parsed["log"]),
                       dup_entries=len(parsed["dups"]), extra_reqid_entries=sum(len(e["extra_reqids"]) for e in parsed["log"]),
                       missing_entries=missing, last_entry="%d'%d" % parsed["last_entry"],
                       head_past_entries=parsed["last_entry"] != parsed["head"],
                       entries=[{"version": "%d'%d" % e["version"], "written_shards": e["written_shards"]} for e in parsed["log"]])
        receipt["targets"] = [{"reqid": history.reqid_text(identity), "locations": history.locate(parsed, identity)}
                              for identity in identities]
        if missing:
            failures.append("the shard's missing set has %d entries" % missing)
        if receipt["head_past_entries"]:
            reason = advanced_head_failure(index, parsed, info)
            if reason:
                failures.append(reason)
    if info is not None:
        receipt.update(shard=info["shard"], last_update="%d'%d" % info["last_update"], last_complete="%d'%d" % info["last_complete"],
                       partial_writes_last_complete=info["partial_writes"],
                       partial_writes_last_complete_epoch=info["partial_writes_epoch"])
        if info["shard"] != index:
            failures.append("the copied collection is shard %d, not acting index %d" % (info["shard"], index))
        if info["last_complete"] != info["last_update"]:
            failures.append("the shard's last_complete is behind its last_update")
        if parsed is not None and (info["last_update"] != parsed["head"] or info["log_tail"] != parsed["tail"]):
            failures.append("the log does not run from the info's log_tail to its last_update")
    receipt["failures"] = failures
    receipt["complete"] = not failures
    return receipt


def snapshot(before_raw, after, capture_raw, dumps, targets, nonprimary=()):
    """One fail-closed receipt over every acting shard.

    ``before_raw`` is the stored first placement record, ``after`` the
    placement read after the last dump, ``capture_raw`` the fixture's capture
    record and ``dumps(name, limit)`` returns one raw dump of at most limit
    bytes. ``nonprimary`` are the pool's native nonprimary shard IDs.
    """
    nonprimary = sorted(nonprimary)
    if any(type(shard) is not int or not 0 <= shard < MAX_SHARD_ID for shard in nonprimary):
        raise ValueError("invalid nonprimary shard IDs")
    identities = [history.target_identity(t) for t in targets]
    if not 1 <= len(identities) <= history.MAX_TARGETS:
        raise ValueError("between 1 and %d history targets are required" % history.MAX_TARGETS)
    receipt = {"after": after, "all_acting_shards": True, "absence_established": False, "shards": [], "nonprimary_shards": nonprimary}
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
    # Only an erasure-coded PG keeps an empty position (a lost shard) in its
    # acting vector; at least two members must be present.
    present = [osd for osd in acting if osd != CRUSH_ITEM_NONE]
    if (not 2 <= len(acting) <= MAX_SHARDS or len(present) < 2 or len(set(map(repr, present))) != len(present)
            or len(present) != len(acting) and before.get("pool_type") != 3
            or any(type(osd) is not int or not 0 <= osd < 1 << 31 for osd in acting)):
        failures.append("the acting vector is not between 2 and %d distinct OSDs" % MAX_SHARDS)
        return finish()
    receipt["absent_shards"] = [index for index, osd in enumerate(acting) if osd == CRUSH_ITEM_NONE]
    receipt["all_acting_shards"] = not receipt["absent_shards"]
    if len(capture["shards"]) != len(acting):
        failures.append("the capture has %d shards for %d acting members" % (len(capture["shards"]), len(acting)))
    for index, osd in enumerate(acting):
        record = capture["shards"][index] if index < len(capture["shards"]) else None
        shard = _shard(index, osd, pgid, record, dumps, identities, nonprimary)
        receipt["shards"].append(shard)
        failures.extend("shard %d (osd.%d): %s" % (index, osd, text) for text in shard["failures"])
    return finish()
