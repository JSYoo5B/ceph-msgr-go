#!/usr/bin/env python3
"""Bounded primary PG-history snapshot parser for the disposable fixture.

Test-only; never product code. The owned-pool oracle (osd_pool_oracle.py,
mode ``pghistory``) sends the PG ``log`` command to the bracketed primary OSD
and passes the raw reply here together with the placement observed before and
after the command. This module copies no Ceph code; it reads the JSON shape
that the pinned Ceph 20.2.4 source
(7f793731f1b39eb4f465e960113d2363c311b964) emits:

- src/osd/OSD.cc:2775-2825 resolves the PG's primary shard and answers only
  when that OSD is primary (``-EAGAIN`` otherwise). A replica cannot be
  observed this way.
- src/osd/PrimaryLogPG.cc:1076-1083 dumps ``pg_log_t`` inside an ``op_log``
  section; src/common/Formatter.cc:207-226 drops the top-level section name,
  so the document is ``{"pg_log_t": {...}}``.
- src/osd/osd_types.cc:5355-5373 (``pg_log_t::dump``) emits ``head``,
  ``tail``, the ordinary ``log`` array and the separate ``dups`` array.
- src/osd/osd_types.cc:5096-5118 (``pg_log_entry_t::dump``) emits each entry's
  ``reqid`` and its ``extra_reqids`` array; osd_types.cc:5234-5246
  (``pg_log_dup_t::dump``) emits each duplicate's ``reqid``.
- src/osd/osd_types.h:235-241 prints ``osd_reqid_t`` as
  ``<entity name>.<incarnation>:<tid>`` and keys equality by all three;
  src/msg/msg_types.h:122-128 prints a negative entity number as ``?``.

A snapshot is complete only when the native command returned 0, the reply is
within the byte bound, parses as one strict JSON document of exactly that
shape, every request identity in it parses, log versions increase within
(tail, head] and end at head, duplicates do not exceed tail, and the before and
after placement observations are identical. Anything else fails closed.

A complete snapshot reports where an exact identity occurs on the primary at
that bracketed epoch. Absence from one snapshot is not evidence that the
identity is absent from other shards, other epochs or the PG's history in
general; this module does not make that claim.
"""
import base64
import hashlib
import json
import re

MAX_LOG_BYTES = 256 << 10
MAX_TARGETS = 8
ENTITY_TYPES = ("client", "osd", "mds", "mon", "mgr", "auth", "unknown")
# entity name (num may print as "?"), incarnation (int32), tid (uint64).
REQID = re.compile(r"(client|osd|mds|mon|mgr|auth|unknown)\.(\?|0|[1-9][0-9]{0,18})\.(-?(?:0|[1-9][0-9]{0,9})):(0|[1-9][0-9]{0,19})")
EVERSION = re.compile(r"(0|[1-9][0-9]{0,9})'(0|[1-9][0-9]{0,19})")
ENTRY_KEYS = {"op", "object", "version", "prior_version", "reqid", "extra_reqids", "mtime", "return_code"}
ENTRY_OPTIONAL = {"op_returns", "snaps", "written_shards", "mod_desc", "clean_regions"}
DUP_KEYS = {"reqid", "version", "user_version", "return_code"}


class HistoryError(ValueError):
    """The native output cannot be trusted as one complete snapshot."""


def _strict_object(pairs):
    seen = {}
    for key, value in pairs:
        if key in seen:
            raise HistoryError("duplicate JSON key " + repr(key)[:80])
        seen[key] = value
    return seen


def parse_reqid(text):
    """(type, num or None, inc, tid) of one printed osd_reqid_t."""
    match = REQID.fullmatch(text) if isinstance(text, str) else None
    if not match:
        raise HistoryError("unparsable request identity " + repr(text)[:80])
    num = None if match[2] == "?" else int(match[2])
    inc, tid = int(match[3]), int(match[4])
    if not -(1 << 31) <= inc < 1 << 31 or tid >= 1 << 64 or (num is not None and num >= 1 << 63):
        raise HistoryError("request identity out of range " + repr(text)[:80])
    return match[1], num, inc, tid


def target_identity(raw):
    """Validate one caller-supplied identity: {"type", "num", "inc", "tid"}."""
    if not isinstance(raw, dict) or set(raw) != {"type", "num", "inc", "tid"}:
        raise ValueError("a history target needs exactly type, num, inc and tid")
    if raw["type"] not in ENTITY_TYPES[:5]:
        raise ValueError("invalid history target entity type")
    for field, low, high in (("num", 0, 1 << 63), ("inc", -(1 << 31), 1 << 31), ("tid", 0, 1 << 64)):
        if type(raw[field]) is not int or not low <= raw[field] < high:
            raise ValueError("invalid history target " + field)
    return raw["type"], raw["num"], raw["inc"], raw["tid"]


def reqid_text(identity):
    kind, num, inc, tid = identity
    return "%s.%s.%d:%d" % (kind, "?" if num is None else num, inc, tid)


def parse_eversion(text, label):
    match = EVERSION.fullmatch(text) if isinstance(text, str) else None
    if not match:
        raise HistoryError("unparsable " + label + " " + repr(text)[:80])
    epoch, version = int(match[1]), int(match[2])
    if epoch >= 1 << 32 or version >= 1 << 64:
        raise HistoryError(label + " out of range")
    return epoch, version


def parse_log(raw):
    """Parse one complete native ``pg log`` JSON reply, or raise HistoryError."""
    if not isinstance(raw, (bytes, bytearray)):
        raise HistoryError("native output is not bytes")
    if not raw:
        raise HistoryError("native output is empty")
    if len(raw) > MAX_LOG_BYTES:
        raise HistoryError("native output exceeds the snapshot bound")
    try:
        document = json.loads(bytes(raw).decode("utf-8"), object_pairs_hook=_strict_object)
    except HistoryError:
        raise
    except (UnicodeDecodeError, ValueError) as error:
        # A truncated reply ends inside the document and fails here.
        raise HistoryError("native output is not one complete JSON document: " + str(error)[:120]) from None
    if not isinstance(document, dict) or set(document) != {"pg_log_t"} or not isinstance(document["pg_log_t"], dict):
        raise HistoryError("native output is not a pg_log_t dump")
    body = document["pg_log_t"]
    if set(body) != {"head", "tail", "log", "dups"}:
        raise HistoryError("pg_log_t must have exactly head, tail, log and dups")
    head, tail = parse_eversion(body["head"], "head"), parse_eversion(body["tail"], "tail")
    if tail > head:
        raise HistoryError("log tail is after head")
    if not isinstance(body["log"], list) or not isinstance(body["dups"], list):
        raise HistoryError("log and dups must be arrays")
    entries, previous = [], tail
    for index, entry in enumerate(body["log"]):
        if not isinstance(entry, dict) or not ENTRY_KEYS <= set(entry) or set(entry) - ENTRY_KEYS - ENTRY_OPTIONAL:
            raise HistoryError("log entry %d has an unexpected shape" % index)
        if not isinstance(entry["op"], str) or not isinstance(entry["object"], str) or type(entry["return_code"]) is not int:
            raise HistoryError("log entry %d has invalid fields" % index)
        version = parse_eversion(entry["version"], "log entry version")
        parse_eversion(entry["prior_version"], "log entry prior_version")
        if not version > previous:
            raise HistoryError("log entry %d version does not increase within (tail, head]" % index)
        previous = version
        if not isinstance(entry["extra_reqids"], list):
            raise HistoryError("log entry %d extra_reqids must be an array" % index)
        extras = []
        for extra_index, extra in enumerate(entry["extra_reqids"]):
            if not isinstance(extra, dict) or not {"reqid", "user_version"} <= set(extra) or set(extra) - {"reqid", "user_version", "return_code"}:
                raise HistoryError("log entry %d extra_reqid %d has an unexpected shape" % (index, extra_index))
            if not isinstance(extra["user_version"], str) or not extra["user_version"].isdigit():
                raise HistoryError("log entry %d extra_reqid %d has an invalid user_version" % (index, extra_index))
            extras.append({"reqid": parse_reqid(extra["reqid"]), "user_version": int(extra["user_version"])})
        entries.append({"version": version, "reqid": parse_reqid(entry["reqid"]), "op": entry["op"],
                        "object": entry["object"], "return_code": entry["return_code"], "extra_reqids": extras})
    if previous != head:
        raise HistoryError("the last log entry is not head; the log array is incomplete")
    dups = []
    for index, dup in enumerate(body["dups"]):
        if not isinstance(dup, dict) or not DUP_KEYS <= set(dup) or set(dup) - DUP_KEYS - {"op_returns"}:
            raise HistoryError("dup %d has an unexpected shape" % index)
        version = parse_eversion(dup["version"], "dup version")
        if version > tail:
            raise HistoryError("dup %d is newer than the log tail" % index)
        dups.append({"version": version, "reqid": parse_reqid(dup["reqid"])})
    return {"head": head, "tail": tail, "log": entries, "dups": dups}


def locate(parsed, identity):
    """Every position of one exact (type, num, inc, tid) identity."""
    found = []
    for index, entry in enumerate(parsed["log"]):
        if entry["reqid"] == identity:
            found.append({"tier": "log", "index": index, "version": "%d'%d" % entry["version"],
                          "op": entry["op"], "object": entry["object"], "return_code": entry["return_code"]})
        for extra_index, extra in enumerate(entry["extra_reqids"]):
            if extra["reqid"] == identity:
                found.append({"tier": "extra_reqids", "index": index, "extra_index": extra_index,
                              "version": "%d'%d" % entry["version"], "op": entry["op"], "object": entry["object"],
                              "user_version": extra["user_version"]})
    for index, dup in enumerate(parsed["dups"]):
        if dup["reqid"] == identity:
            found.append({"tier": "dups", "index": index, "version": "%d'%d" % dup["version"]})
    return found


def snapshot(before, after, code, raw, status, targets, oversized_length=None):
    """One fail-closed receipt: completeness, brackets and target positions."""
    identities = [target_identity(t) for t in targets]
    if not 1 <= len(identities) <= MAX_TARGETS:
        raise ValueError("between 1 and %d history targets are required" % MAX_TARGETS)
    failures = []
    if type(code) is not int or code != 0:
        failures.append("native pg log command returned %s: %s" % (code, str(status)[:200]))
    receipt = {"code": code, "limit_bytes": MAX_LOG_BYTES, "before": before, "after": after,
               "primary_only": True, "absence_established": False}
    if oversized_length is not None:
        failures.append("native output of %d bytes exceeds the snapshot bound" % oversized_length)
        receipt["bytes"] = oversized_length
    else:
        receipt.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest(), raw=base64.b64encode(raw).decode())
    if before != after:
        failures.append("placement or epoch changed between the before and after brackets")
    parsed = None
    # A placement change is reported with the parsed reply; a failed or
    # oversized command has nothing to parse.
    if oversized_length is None and code == 0:
        try:
            parsed = parse_log(raw)
        except HistoryError as error:
            failures.append(str(error))
    if parsed is not None:
        receipt.update(head="%d'%d" % parsed["head"], tail="%d'%d" % parsed["tail"],
                       log_entries=len(parsed["log"]), dup_entries=len(parsed["dups"]),
                       extra_reqid_entries=sum(len(e["extra_reqids"]) for e in parsed["log"]))
        receipt["targets"] = [{"reqid": reqid_text(identity), "locations": locate(parsed, identity)} for identity in identities]
    receipt["failures"] = failures
    receipt["complete"] = not failures
    return receipt
