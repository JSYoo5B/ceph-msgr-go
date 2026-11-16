#!/usr/bin/env python3
"""Bounded parser of one OSD's in-flight op dump for the disposable fixture.

Test-only; never product code. The owned-pool oracle (osd_pool_oracle.py,
mode ``opsinflight``) sends ``dump_ops_in_flight`` to the bracketed primary
OSD of an owned object and passes the raw reply here together with the
placement observed before and after the command. This module copies no Ceph
code; it reads the JSON shape that the pinned Ceph 20.2.4 source
(7f793731f1b39eb4f465e960113d2363c311b964) emits:

- src/osd/OSD.cc:2854-2878 answers ``dump_ops_in_flight`` from the OSD's op
  tracker; src/osd/OSD.cc:7692 registers every fast-dispatched message, a
  client MOSDOp included, before it is queued for its PG.
- src/common/TrackedOp.cc:236-282 (``OpTracker::dump_ops_in_flight``) emits an
  ``ops`` array and ``num_ops``; src/common/Formatter.cc:207-226 drops the
  top-level section name, so the document is ``{"ops": [...], "num_ops": N}``.
- src/common/TrackedOp.cc:505-520 (``TrackedOp::dump``) emits each op's
  ``description``, ``initiated_at``, ``age``, ``duration``, ``continuous`` and
  ``type_data``; src/osd/OpRequest.cc:53-89 (``OpRequest::_dump``) fills
  ``type_data`` with ``flag_point``, a ``client_info`` section for a client
  source and the ``events`` array.
- src/messages/MOSDOp.h:622-641 prints a client op as
  ``osd_op(<reqid> <spg_t> ... e<epoch>)``, with its raw PG and
  ``(undecoded)`` in place of the object and operations until the PG decodes
  them; src/osd/osd_types.h:235-241 prints the request identity as
  ``<entity name>.<incarnation>:<tid>``.

A receipt is complete only when the native command returned 0, the reply is
within the byte bound, parses as one strict JSON document of exactly that
shape, ``num_ops`` counts the array, every op has exactly the dumped fields,
and the before and after placement observations are identical. Every client
op whose request identity carries the caller's entity number and incarnation
is reported with its TID, PG, map epoch, flag point and event names; its
``client_info`` must name the same entity and TID. An op of that client whose
description does not parse fails the receipt instead of being skipped.

The receipt reports what the tracker held at the moment of the command. An
op absent from it may have completed, been discarded or never arrived; this
module establishes no absence from the PG or its history.
"""
import base64
import hashlib
import re

import osd_pg_history_oracle as history

MAX_OPS_BYTES = 256 << 10
MAX_OPS = 512
OP_KEYS = {"description", "initiated_at", "age", "duration", "continuous", "type_data"}
CLIENT_INFO_KEYS = {"client", "client_addr", "tid"}
EVENT_KEYS = {"event", "time", "duration"}
# A client op: identity, spg_t, the rest, and the epoch at the end.
CLIENT_OP = re.compile(r"osd_op\((\S+) ((?:0|[1-9][0-9]{0,18})\.(?:0|[1-9a-f][0-9a-f]{0,7})(?:s(?:0|[1-9][0-9]?))?) (.*) e(0|[1-9][0-9]{0,9})\)", re.S)


class OpsError(ValueError):
    """The native output cannot be trusted as one complete op dump."""


def client_entity(raw):
    """Validate one caller-supplied client entity: {"type", "num", "inc"}."""
    if not isinstance(raw, dict) or set(raw) != {"type", "num", "inc"} or raw["type"] != "client":
        raise ValueError("an in-flight target needs exactly type client, num and inc")
    for field, low, high in (("num", 1, 1 << 63), ("inc", -(1 << 31), 1 << 31)):
        if type(raw[field]) is not int or not low <= raw[field] < high:
            raise ValueError("invalid in-flight target " + field)
    return raw["num"], raw["inc"]


def parse_ops(raw):
    """The ops of one complete native dump, or OpsError."""
    try:
        document = history.strict_document(raw, MAX_OPS_BYTES)
    except history.HistoryError as error:
        raise OpsError(str(error)) from None
    if not isinstance(document, dict) or set(document) != {"ops", "num_ops"}:
        raise OpsError("native output is not an ops_in_flight dump")
    ops, count = document["ops"], document["num_ops"]
    if not isinstance(ops, list) or type(count) is not int or count != len(ops) or count > MAX_OPS:
        raise OpsError("num_ops does not count the ops array within the bound")
    parsed = []
    for index, op in enumerate(ops):
        if not isinstance(op, dict) or set(op) != OP_KEYS or not isinstance(op["description"], str):
            raise OpsError("op %d has an unexpected shape" % index)
        data = op["type_data"]
        if not isinstance(data, dict) or not {"flag_point", "events"} <= set(data) or set(data) - {"flag_point", "events", "client_info"}:
            raise OpsError("op %d type_data has an unexpected shape" % index)
        if not isinstance(data["flag_point"], str) or not isinstance(data["events"], list):
            raise OpsError("op %d flag_point or events are invalid" % index)
        events = []
        for event in data["events"]:
            if not isinstance(event, dict) or set(event) != EVENT_KEYS or not isinstance(event["event"], str):
                raise OpsError("op %d has an event of an unexpected shape" % index)
            events.append(event["event"])
        info = data.get("client_info")
        if info is not None and (not isinstance(info, dict) or set(info) != CLIENT_INFO_KEYS or not isinstance(info["client"], str) or type(info["tid"]) is not int):
            raise OpsError("op %d client_info has an unexpected shape" % index)
        parsed.append({"description": op["description"], "flag_point": data["flag_point"], "events": events, "client_info": info})
    return parsed


def client_ops(parsed, num, inc):
    """Every op whose request identity is client.<num>.<inc>:<tid>."""
    prefix = "osd_op(client.%d.%d:" % (num, inc)
    found = []
    for index, op in enumerate(parsed):
        if not op["description"].startswith(prefix):
            continue
        match = CLIENT_OP.fullmatch(op["description"])
        if not match:
            raise OpsError("op %d of the target client has an unparsable description" % index)
        try:
            kind, got_num, got_inc, tid = history.parse_reqid(match[1])
        except history.HistoryError as error:
            raise OpsError("op %d: %s" % (index, error)) from None
        if (kind, got_num, got_inc) != ("client", num, inc):
            raise OpsError("op %d request identity differs from its prefix" % index)
        info = op["client_info"]
        if info is None or info["client"] != "client.%d" % num or info["tid"] != tid:
            raise OpsError("op %d client_info does not name the identity of its description" % index)
        found.append({"index": index, "reqid": match[1], "tid": tid, "pgid": match[2], "epoch": int(match[4]),
                      "flag_point": op["flag_point"], "events": op["events"], "description": op["description"]})
    return found


def snapshot(before, after, code, raw, status, entity, oversized_length=None):
    """One fail-closed receipt: completeness, brackets and the client's ops."""
    num, inc = client_entity(entity)
    failures = []
    if type(code) is not int or code != 0:
        failures.append("native dump_ops_in_flight returned %s: %s" % (code, str(status)[:200]))
    receipt = {"code": code, "limit_bytes": MAX_OPS_BYTES, "before": before, "after": after, "absence_established": False}
    if oversized_length is not None:
        failures.append("native output of %d bytes exceeds the bound" % oversized_length)
        receipt["bytes"] = oversized_length
    else:
        receipt.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest(), raw=base64.b64encode(raw).decode())
    if before != after:
        failures.append("placement or epoch changed between the before and after brackets")
    if oversized_length is None and code == 0:
        try:
            parsed = parse_ops(raw)
            receipt.update(num_ops=len(parsed), client="client.%d.%d" % (num, inc), ops=client_ops(parsed, num, inc))
        except OpsError as error:
            failures.append(str(error))
    receipt["failures"] = failures
    receipt["complete"] = not failures
    return receipt
