#!/usr/bin/env python3
"""Bounded judgement of one owned interval change of a PG of the disposable fixture.

Test-only; never product code. The owned-pool oracle (osd_pool_oracle.py,
mode ``pginterval``) sends one ``pg repeer`` command for the PG of an owned
object and then reads the native placement and the primary's own ``pg query``
until this module judges the PG settled in a newer interval, or a bounded
number of observations is used up. This module copies no Ceph code; it reads
the JSON shape that the pinned Ceph 20.2.4 source
(7f793731f1b39eb4f465e960113d2363c311b964) emits:

- src/mon/OSDMonitor.cc:12551-12588 (``pg repeer``) stores a pg_temp of the
  acting primary alone, padded to the pool size, for a PG with more than one
  acting member. That ends the PG's interval. The primary of the shortened
  acting set wants its up set back and asks the MON to drop the pg_temp,
  which starts one more interval with the original sets. No daemon stops and
  nothing but that PG's own temporary mapping is changed.
- src/osd/PrimaryLogPG.cc:1055-1075 answers ``pg <pgid> query`` with
  ``PeeringState::dump_peering_state`` (src/osd/PeeringState.cc:4559-4606):
  ``state``, the OSD's map ``epoch``, ``up``, ``acting``, the primary's own
  ``info`` and one ``peer_info`` entry per peer.
- src/osd/osd_types.cc:3693-3738 (``pg_info_t::dump``) emits ``pgid``,
  ``shard``, ``last_update``, ``last_complete``, ``log_tail`` and the
  partial-write records; :3579-3600 (``pg_history_t::dump``) emits
  ``same_interval_since`` and ``last_epoch_started`` inside ``history``.

A PG is settled only when two identical placement records surround one
complete query of the same primary, the placement is the one recorded before
the command except for a newer epoch, it has no pg_temp, the primary reports
exactly ``active+clean`` with the same up and acting sets, and the interval
the primary reports started after the one recorded before the command and
not after the bracketing epoch, and the primary's own map epoch is not
behind the bracketing epoch. Anything else is reported as not settled with
its reasons; nothing is inferred from a later observation.

``absent_failures`` judges one further state for the controls that take the
OSD of one acting member out of the PG with the fixture's own daemon
control: the PG active on its original primary while that one acting
position is empty. It sends nothing and is as strict: the same two placement
records, the recorded placement except for a newer epoch and the one empty
position in the up and acting sets, no pg_temp, a primary that reports
exactly active and undersized, with or without degraded, in an interval that
started after the recorded one, in which the PG has started.

The receipt says that the PG entered a newer interval with its original
placement, or that it is active without one member. It says nothing about
what any shard holds; that is read from the shards' own stores by the
shard-history capture.
"""
import osd_pg_history_oracle as history
import osd_shard_history_oracle as shard_history

MAX_QUERY_BYTES = 256 << 10
SOURCE_COMMIT = "7f793731f1b39eb4f465e960113d2363c311b964"
CLEAN = "active+clean"
# CRUSH_ITEM_NONE, the map's value for an acting position without an OSD
# (src/osd/osd_types.h:1267).
NONE = (1 << 31) - 1
# What the primary of an active PG reports while one acting position is
# empty: src/osd/PeeringState.cc:3224-3228 sets undersized for an acting set
# smaller than the pool size, and :4354-4358 sets degraded once the PG's
# statistics count a degraded object.
ABSENT_STATES = (["active", "undersized"], ["active", "degraded", "undersized"])
PLACEMENT_KEYS = ("pgid", "pool_id", "object", "namespace", "up", "acting", "primary_id", "osd_id", "pg_shard", "address",
                  "replicas", "min_size", "pool_type", "nonprimary_shard_count", "hash", "raw_pgid", "pg_seed")
HistoryError = history.HistoryError


def _epoch(value, label):
    if type(value) is not int or not 0 < value < 1 << 32:
        raise HistoryError("pg query has no usable " + label)
    return value


def _vector(value, label):
    if (not isinstance(value, list) or not 1 <= len(value) <= 64
            or any(type(osd) is not int or not 0 <= osd < 1 << 31 for osd in value)):
        raise HistoryError("pg query has no usable %s set" % label)
    return value


def _info(document, label):
    """The projection of one ``pg_info_t`` dump that the receipt carries."""
    if not isinstance(document, dict) or not isinstance(document.get("pgid"), str) or len(document["pgid"]) > 40:
        raise HistoryError("pg query %s is not a pg_info_t dump" % label)
    info = shard_history.info_fields(document, document["pgid"])
    return {"pgid": document["pgid"], "shard": info["shard"],
            "last_update": "%d'%d" % info["last_update"], "last_complete": "%d'%d" % info["last_complete"],
            "log_tail": "%d'%d" % info["log_tail"], "partial_writes_last_complete": info["partial_writes"],
            "partial_writes_last_complete_epoch": info["partial_writes_epoch"]}


def parse_query(raw):
    """The state, sets, interval and shard infos of one complete ``pg query`` reply."""
    document = history.strict_document(raw, MAX_QUERY_BYTES)
    if not isinstance(document, dict) or not {"state", "epoch", "up", "acting", "info", "peer_info"} <= set(document):
        raise HistoryError("native output is not a pg query reply")
    if not isinstance(document["state"], str) or not 0 < len(document["state"]) <= 200:
        raise HistoryError("pg query has no usable state")
    info = document["info"]
    past = info.get("history") if isinstance(info, dict) else None
    if not isinstance(past, dict):
        raise HistoryError("pg query info has no history")
    peers = document["peer_info"]
    if not isinstance(peers, list) or len(peers) > 64:
        raise HistoryError("pg query has no usable peer_info array")
    parsed = {"state": document["state"], "epoch": _epoch(document["epoch"], "epoch"),
              "up": _vector(document["up"], "up"), "acting": _vector(document["acting"], "acting"),
              "same_interval_since": _epoch(past.get("same_interval_since"), "same_interval_since"),
              "last_epoch_started": _epoch(past.get("last_epoch_started"), "last_epoch_started"),
              "info": _info(info, "info"), "peers": []}
    for peer in peers:
        if not isinstance(peer, dict) or not isinstance(peer.get("peer"), str) or len(peer["peer"]) > 40:
            raise HistoryError("pg query has an unexpected peer_info entry")
        parsed["peers"].append(dict(_info(peer, "peer_info"), peer=peer["peer"]))
    return parsed


def observation(first, code, raw, status, oversized, second):
    """One observation: two placement records around one query of the primary.

    ``first`` and ``second`` are placement records, or a string saying why
    none could be read; the observation then carries no record in that
    position and the reason as ``placement_failure``. The query is parsed
    only when the command returned 0 within the byte bound.
    """
    seen = {"before": first if isinstance(first, dict) else None, "after": second if isinstance(second, dict) else None, "code": code}
    missing = [str(record)[:200] for record in (first, second) if not isinstance(record, dict)]
    if missing:
        seen["placement_failure"] = missing[0]
    if oversized is not None:
        seen["query_failure"] = "pg query reply of %d bytes exceeds the bound" % oversized
    elif type(code) is not int or code != 0:
        seen["query_failure"] = "pg query returned %s: %s" % (code, str(status)[:200])
    else:
        try:
            seen["query"] = parse_query(raw)
        except HistoryError as error:
            seen["query_failure"] = str(error)
    return seen


def unsettled(origin, origin_query, seen):
    """Why one observation is not the settled newer interval; empty when it is."""
    first, second = seen.get("before"), seen.get("after")
    if not isinstance(first, dict) or not isinstance(second, dict):
        return ["no placement record: %s" % seen.get("placement_failure", "")]
    reasons = []
    if first != second:
        reasons.append("placement or epoch changed during the observation")
    for key in PLACEMENT_KEYS:
        if first.get(key) != origin.get(key):
            reasons.append("placement %s is %r; it was %r before the command" % (key, first.get(key), origin.get(key)))
    if first.get("pg_temp") is not False:
        reasons.append("the PG has a pg_temp")
    if first.get("up") != first.get("acting"):
        reasons.append("up and acting sets differ")
    if type(first.get("epoch")) is not int or first["epoch"] <= origin["epoch"]:
        reasons.append("the map epoch %r is not newer than %d" % (first.get("epoch"), origin["epoch"]))
    query = seen.get("query")
    if query is None:
        return reasons + [seen.get("query_failure", "no pg query reply")]
    if query["state"] != CLEAN:
        reasons.append("the primary reports %s" % query["state"][:80])
    if query["up"] != first.get("acting") or query["acting"] != first.get("acting"):
        reasons.append("the primary's up %r and acting %r differ from the placement" % (query["up"], query["acting"]))
    if query["info"]["pgid"] != "%ss%d" % (origin["pgid"], origin["pg_shard"]) or query["info"]["shard"] != origin["pg_shard"]:
        reasons.append("the query was answered for %s" % query["info"]["pgid"])
    if query["same_interval_since"] <= origin_query["same_interval_since"]:
        reasons.append("the interval still started at epoch %d" % query["same_interval_since"])
    elif type(first.get("epoch")) is int and not query["same_interval_since"] <= first["epoch"]:
        reasons.append("the interval started at epoch %d, after the placement's epoch" % query["same_interval_since"])
    if query["last_epoch_started"] < query["same_interval_since"]:
        reasons.append("the PG has not started in its current interval")
    if type(first.get("epoch")) is int and query["epoch"] < first["epoch"]:
        reasons.append("the primary is at map epoch %d, behind the placement's epoch" % query["epoch"])
    return reasons


def origin_failures(origin, seen):
    """Why the PG is not a clean, unmapped starting point for the command."""
    if not isinstance(seen.get("after"), dict) or seen["after"] != origin:
        return ["placement or epoch changed while the starting point was read"]
    reasons = []
    if origin.get("pg_temp") is not False or origin.get("up") != origin.get("acting") or origin.get("pool_type") != 3:
        reasons.append("the PG is not an erasure-coded PG without a pg_temp and with equal up and acting sets")
    if type(origin.get("pg_shard")) is not int or not 0 <= origin["pg_shard"] < len(origin.get("acting", [])):
        reasons.append("the primary has no shard in the acting set")
    query = seen.get("query")
    if query is None:
        return reasons + [seen.get("query_failure", "no pg query reply")]
    if query["state"] != CLEAN or query["up"] != origin["acting"] or query["acting"] != origin["acting"]:
        reasons.append("the primary reports %s with up %r and acting %r" % (query["state"][:80], query["up"], query["acting"]))
    if not reasons and query["info"]["pgid"] != "%ss%d" % (origin["pgid"], origin["pg_shard"]):
        reasons.append("the query was answered for %s" % query["info"]["pgid"])
    if query["same_interval_since"] > origin["epoch"]:
        reasons.append("the interval started after the placement's epoch")
    if query["epoch"] < origin["epoch"]:
        # It may still have an interval change of a newer map ahead of it.
        reasons.append("the primary is at map epoch %d, behind the placement's epoch" % query["epoch"])
    return reasons


def absent_failures(origin, shard, since, seen):
    """Why one observation is not the PG active without one acting member.

    ``origin`` is the complete placement recorded before the member's OSD
    left, ``shard`` its acting position and ``since`` the epoch at which the
    interval of that placement started. The observation is accepted only if
    two identical placement records surround one complete query of the same
    primary; the placement is the recorded one in every field but a newer
    epoch and up and acting sets whose position ``shard`` alone is empty; it
    has no pg_temp; the primary reports active and undersized, optionally
    degraded and nothing else, with those sets, for its own shard, in an
    interval that started after ``since`` and not after the placement's
    epoch, in which the PG has started; and the primary's own map epoch is
    not behind the placement's.
    """
    vector = origin.get("acting") if isinstance(origin, dict) else None
    if (not isinstance(vector, list) or NONE in vector or origin.get("up") != vector or origin.get("pool_type") != 3
            or origin.get("pg_temp") is not False or type(origin.get("epoch")) is not int
            or type(origin.get("pg_shard")) is not int or not 0 <= origin["pg_shard"] < len(vector)):
        return ["the recorded placement is not a complete erasure-coded acting set without a pg_temp"]
    if type(shard) is not int or not 0 <= shard < len(vector) or shard == origin["pg_shard"]:
        return ["the absent position %r is not an acting position other than the primary's" % (shard,)]
    if type(since) is not int or not 0 < since <= origin["epoch"]:
        return ["the recorded interval start %r is not an epoch at or before the recorded placement's" % (since,)]
    first, second = seen.get("before"), seen.get("after")
    if not isinstance(first, dict) or not isinstance(second, dict):
        return ["no placement record: %s" % seen.get("placement_failure", "")]
    reasons = []
    if first != second:
        reasons.append("placement or epoch changed during the observation")
    holed = vector[:shard] + [NONE] + vector[shard + 1:]
    for key in PLACEMENT_KEYS:
        want = holed if key in ("up", "acting") else origin.get(key)
        if first.get(key) != want:
            reasons.append("placement %s is %r; want %r" % (key, first.get(key), want))
    if first.get("pg_temp") is not False:
        reasons.append("the PG has a pg_temp")
    if type(first.get("epoch")) is not int or first["epoch"] <= origin["epoch"]:
        reasons.append("the map epoch %r is not newer than %d" % (first.get("epoch"), origin["epoch"]))
    query = seen.get("query")
    if query is None:
        return reasons + [seen.get("query_failure", "no pg query reply")]
    if sorted(query["state"].split("+")) not in ABSENT_STATES:
        reasons.append("the primary reports %s" % query["state"][:80])
    if query["up"] != holed or query["acting"] != holed:
        reasons.append("the primary's up %r and acting %r are not %r" % (query["up"], query["acting"], holed))
    if query["info"]["pgid"] != "%ss%d" % (origin["pgid"], origin["pg_shard"]) or query["info"]["shard"] != origin["pg_shard"]:
        reasons.append("the query was answered for %s" % query["info"]["pgid"])
    if query["same_interval_since"] <= since:
        reasons.append("the interval still started at epoch %d" % query["same_interval_since"])
    elif type(first.get("epoch")) is int and not query["same_interval_since"] <= first["epoch"]:
        reasons.append("the interval started at epoch %d, after the placement's epoch" % query["same_interval_since"])
    if query["last_epoch_started"] < query["same_interval_since"]:
        reasons.append("the PG has not started in its current interval")
    if type(first.get("epoch")) is int and query["epoch"] < first["epoch"]:
        reasons.append("the primary is at map epoch %d, behind the placement's epoch" % query["epoch"])
    return reasons
