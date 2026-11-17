#!/usr/bin/env python3
"""Bounded parser of one OSD's internal Objecter request dump for the fixture.

Test-only; never product code. The owned-pool oracle (osd_pool_oracle.py,
mode ``objecterrequests``) sends ``objecter_requests`` to the bracketed
primary OSD of an owned destination object and passes the raw reply here
together with the placement observed before and after the command. This
module copies no Ceph code; it reads the JSON shape that the pinned Ceph
20.2.4 source (7f793731f1b39eb4f465e960113d2363c311b964) emits:

- src/osd/OSD.cc:7379-7393 hands every MCommand to the admin socket, and
  src/osdc/Objecter.cc:405-409 registers ``objecter_requests`` there for the
  OSD's own Objecter, the client the OSD uses to read a COPY_FROM source
  (src/osd/PrimaryLogPG.cc ``_copy_some``).
- src/osdc/Objecter.cc:4808-4819 (``dump_requests``) emits the arrays
  ``ops``, ``linger_ops``, ``pool_ops``, ``pool_stat_ops``, ``statfs_ops``
  and ``command_ops`` in one section whose name the JSON formatter drops at
  the top level (src/common/Formatter.cc:207-226).
- src/osdc/Objecter.cc:4821-4844 (``_dump_ops``) and 4763-4774
  (``op_target_t::dump``) emit each op's ``tid``, ``pg``, ``osd``,
  ``object_id``, ``object_locator``, ``target_object_id``,
  ``target_object_locator``, ``paused``, ``used_replica``, ``precalc_pgid``,
  ``last_sent``, ``age``, ``attempts``, ``snapid``, ``snap_context``,
  ``mtime`` and the printed ``osd_ops``.
- src/include/object.h:76-78 prints an object_t as its name;
  src/osd/osd_types.h:321-329 prints an object_locator_t as ``@<pool>``
  followed by ``;<namespace>`` and ``:<key>`` when they are not empty;
  src/osd/osd_types.cc:7323-7325 and 7388-7393 print COPY_GET as
  ``copy-get max <n>`` followed by `` in=<n>b`` for its encoded cursor
  (src/osdc/Objecter.h:1184-1204) and `` out=<n>b`` once it has output.
- src/osdc/Objecter.cc:2820-2830 (``target_should_be_paused``) pauses a READ
  op while the OSDMap carries ``pauserd``; such an op stays in the dump with
  ``paused`` 1 and is not sent (src/osdc/Objecter.cc:2550-2556).
  src/osdc/Objecter.cc:3081 also pauses an op that was already sent once a
  later map calls for it, and src/osdc/Objecter.cc:3385-3400 counts every
  prepared send in ``attempts``. ``paused`` alone therefore does not show
  that an op was never sent; ``attempts`` 0 does. This module reports both
  and judges neither.

A receipt is complete only when the native command returned 0, the reply is
within the byte bound, parses as one strict JSON document of exactly that
shape, every op has exactly the dumped fields, and the before and after
placement observations are identical. Every op whose object and locator name
the caller's source is reported with its TID, PG, OSD, pause flag, attempts
and printed operations, and the maximum of each ``copy-get``.

The receipt reports what the Objecter held at the moment of the command. An
op absent from it may have completed, been cancelled or never started; this
module establishes no absence.
"""
import base64
import hashlib
import re

import osd_pg_history_oracle as history

MAX_REQUESTS_BYTES = 256 << 10
MAX_OPS = 512
MAX_NAME = 4096
SECTIONS = ("ops", "linger_ops", "pool_ops", "pool_stat_ops", "statfs_ops", "command_ops")
OP_KEYS = {"tid", "pg", "osd", "object_id", "object_locator", "target_object_id", "target_object_locator", "paused", "used_replica",
           "precalc_pgid", "last_sent", "age", "attempts", "snapid", "snap_context", "mtime", "osd_ops"}
INTEGER_KEYS = ("tid", "osd", "paused", "used_replica", "precalc_pgid", "attempts")
TEXT_KEYS = ("pg", "object_id", "object_locator", "target_object_id", "target_object_locator", "last_sent", "snapid", "snap_context", "mtime")
NUMBER = r"(0|[1-9][0-9]{0,19})"
COPY_GET = re.compile(r"copy-get max %s(?: in=%sb)?(?: out=%sb)?" % (NUMBER, NUMBER, NUMBER))


class RequestsError(ValueError):
    """The native output cannot be trusted as one complete Objecter dump."""


def source_identity(raw):
    """Validate one caller-supplied source: {"pool", "name"} in the default namespace."""
    if not isinstance(raw, dict) or set(raw) != {"pool", "name"}:
        raise ValueError("an objecter source needs exactly pool and name")
    if type(raw["pool"]) is not int or not 0 <= raw["pool"] < 1 << 63:
        raise ValueError("invalid objecter source pool")
    name = raw["name"]
    if not isinstance(name, str) or not name or len(name.encode()) > MAX_NAME or "\0" in name:
        raise ValueError("invalid objecter source name")
    return raw["pool"], name


def parse_requests(raw):
    """The ops of one complete native dump, or RequestsError."""
    try:
        document = history.strict_document(raw, MAX_REQUESTS_BYTES)
    except history.HistoryError as error:
        raise RequestsError(str(error)) from None
    if not isinstance(document, dict) or set(document) != set(SECTIONS):
        raise RequestsError("native output is not an objecter_requests dump")
    for section in SECTIONS:
        if not isinstance(document[section], list):
            raise RequestsError("section %s is not an array" % section)
    ops = document["ops"]
    if len(ops) > MAX_OPS:
        raise RequestsError("the dump holds more ops than the bound")
    parsed = []
    for index, op in enumerate(ops):
        if not isinstance(op, dict) or set(op) != OP_KEYS:
            raise RequestsError("op %d has an unexpected shape" % index)
        if any(type(op[key]) is not int for key in INTEGER_KEYS) or any(not isinstance(op[key], str) for key in TEXT_KEYS):
            raise RequestsError("op %d has a field of an unexpected type" % index)
        if type(op["age"]) not in (int, float) or not isinstance(op["osd_ops"], list) or any(not isinstance(o, str) for o in op["osd_ops"]):
            raise RequestsError("op %d age or osd_ops are invalid" % index)
        if op["paused"] not in (0, 1) or op["tid"] < 0 or op["attempts"] < 0:
            raise RequestsError("op %d tid, paused flag or attempts are out of range" % index)
        parsed.append(op)
    return parsed, {section: len(document[section]) for section in SECTIONS}


def source_ops(parsed, pool, name):
    """Every op whose object is the source and whose locator is ``@<pool>``."""
    locator = "@%d" % pool
    found = []
    for index, op in enumerate(parsed):
        if op["object_id"] != name or op["object_locator"] != locator:
            continue
        copy_gets = []
        for printed in op["osd_ops"]:
            if printed.startswith("copy-get"):
                match = COPY_GET.fullmatch(printed)
                if not match:
                    raise RequestsError("op %d prints an unparsable copy-get" % index)
                copy_gets.append(int(match[1]))
        found.append({"index": index, "tid": op["tid"], "pg": op["pg"], "osd": op["osd"], "paused": op["paused"] == 1,
                      "attempts": op["attempts"], "target_object_id": op["target_object_id"],
                      "target_object_locator": op["target_object_locator"], "osd_ops": op["osd_ops"], "copy_get_max": copy_gets})
    return found


def snapshot(before, after, code, raw, status, source, oversized_length=None):
    """One fail-closed receipt: completeness, brackets and the source's ops."""
    pool, name = source_identity(source)
    failures = []
    if type(code) is not int or code != 0:
        failures.append("native objecter_requests returned %s: %s" % (code, str(status)[:200]))
    receipt = {"code": code, "limit_bytes": MAX_REQUESTS_BYTES, "before": before, "after": after, "absence_established": False}
    if oversized_length is not None:
        failures.append("native output of %d bytes exceeds the bound" % oversized_length)
        receipt["bytes"] = oversized_length
    else:
        receipt.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest(), raw=base64.b64encode(raw).decode())
    if before != after:
        failures.append("placement or epoch changed between the before and after brackets")
    if oversized_length is None and code == 0:
        try:
            parsed, sections = parse_requests(raw)
            receipt.update(num_ops=len(parsed), sections=sections, source={"pool": pool, "name": name}, ops=source_ops(parsed, pool, name))
        except RequestsError as error:
            failures.append(str(error))
    receipt["failures"] = failures
    receipt["complete"] = not failures
    return receipt
