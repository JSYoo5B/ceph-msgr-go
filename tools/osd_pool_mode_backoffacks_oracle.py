"""Owned-pool oracle mode backoffacks: the backoff ACKs an OSD has completed.

Test-only; never product code. One read-only observation, sending nothing
that changes the cluster: a placement record of an owned object, then
`dump_historic_ops` of that object's primary, or of an explicit OSD, and the
MOSDBackoff ACK_BLOCK entries among those ops for the object's PG.

Pinned source (Ceph 20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964):
the OSD tracks a received MOSDBackoff as an op like any other message
(src/osd/OSD.cc ms_fast_dispatch, enqueue_op), and the op tracker keeps a
completed op in its history with the description MOSDBackoff::print gives,
"osd_backoff(<spg_t> ack-block id <id> [<begin>,<end>) e<epoch>)"
(src/messages/MOSDBackoff.h; spg_t prints "<pgid>s<shard>" for an
erasure-coded PG). OSD::dequeue_op records the event reached_pg when the PG
takes the op (src/osd/OSD.cc). A PG-wide ACK (begin != end) is handled at PG
level even while the PG is peered and not active; an object ACK waits for an
active PG (src/osd/PrimaryLogPG.cc do_request, PG::handle_backoff in
src/osd/PG.cc).

The history is bounded (osd_op_history_size, osd_op_history_duration), so an
ACK this mode does not report is not shown absent. A matched ACK is reported
only with its whole event list ending in "done"; an over-bound, malformed or
unterminated list fails the observation rather than being projected, so
reached_pg=false is read from a complete history. It judges nothing else.
"""
import re

import osd_mutation_oracle as mutation
import osd_pg_history_oracle as history
from osd_pool_oracle import explicit_osd, find_pool, object_request, oracle_mode, pool_name

MAX_HISTORY = 1 << 20
MAX_OPS = 4096
MAX_DESCRIPTION = 16384
MAX_EVENTS = 128
ACK = re.compile(r"osd_backoff\(([0-9]{1,19}\.[0-9a-f]{1,8})(?:s([0-9]{1,3}))? ack-block id ([0-9]{1,20}) "
                 r"\[([^,\]]{0,4096}),([^,\)]{0,4096})\) e([0-9]{1,10})\)")


def acks(document, pgid):
    """The ACK_BLOCK entries of one `dump_historic_ops` document for pgid."""
    if not isinstance(document, dict) or not isinstance(document.get("ops"), list) or len(document["ops"]) > MAX_OPS:
        raise ValueError("native op history exceeds its schema or count bound")
    found = []
    for op in document["ops"]:
        description = op.get("description") if isinstance(op, dict) else None
        if not isinstance(description, str) or len(description) > MAX_DESCRIPTION:
            continue
        match = ACK.fullmatch(description)
        if not match or match[1] != pgid:
            continue
        data = op.get("type_data") if isinstance(op.get("type_data"), dict) else {}
        client = data.get("client_info") if isinstance(data.get("client_info"), dict) else {}
        names = complete_events(data.get("events"))
        found.append({"pgid": match[1], "shard": -1 if match[2] is None else int(match[2]), "id": int(match[3]),
                      "begin": match[4], "end": match[5], "map_epoch": int(match[6]),
                      "client": client.get("client") if isinstance(client.get("client"), str) else "",
                      "reached_pg": "reached_pg" in names, "done": names[-1] == "done", "events": names})
    return found


def complete_events(events):
    """The event names of a matched ACK, only when the list is whole.

    A missing, over-bound or malformed event list raises instead of being
    truncated or filtered, so the absence of reached_pg is read only from a
    complete projection. The last event must be the terminal "done" that
    TrackedOp::put marks on a live op before unregister_inflight_op moves it
    into the history (src/common/TrackedOp.h); an entry without it is not
    evidence that the op completed.
    """
    if not isinstance(events, list) or not events or len(events) > MAX_EVENTS:
        raise ValueError("native op history of a matched ACK has no complete event list")
    names = []
    for event in events:
        name = event.get("event") if isinstance(event, dict) else None
        if not isinstance(name, str) or not name or len(name) > MAX_DESCRIPTION:
            raise ValueError("native op history of a matched ACK has a malformed event")
        names.append(name)
    if names[-1] != "done":
        raise ValueError("native op history of a matched ACK does not end with done")
    return names


@oracle_mode
def mode_backoffacks(rados, cluster, request, out):
    """The backoff ACKs of an owned object's PG in one OSD's op history.

    Read-only: one placement record of the object, then
    `dump_historic_ops` of its primary or of the explicit "osd".
    """
    if set(request) - {"slot", "object", "namespace", "osd"}:
        raise ValueError("backoffacks takes only slot, object, namespace and osd")
    pool = pool_name(request["slot"])
    _, namespace = object_request(request)
    target = explicit_osd(request)
    descriptor = find_pool(rados.dump(cluster), pool)
    if descriptor is None:
        raise RuntimeError("owned pool is missing")

    def command(*arguments, timeout):
        return mutation.native_mon_query(rados.lib, cluster, *arguments, timeout=timeout)

    placement = mutation.collect_placement(command, pool, request["object"], descriptor["pool"], namespace=namespace, timeout=10)
    osd = placement["primary_id"] if target is None else target
    code, raw, status, oversized = rados.osd(cluster, osd, {"prefix": "dump_historic_ops", "format": "json"}, MAX_HISTORY)
    if code != 0 or oversized is not None:
        raise RuntimeError("native dump_historic_ops failed (%d): %s" % (code, status))
    return {"backoff_acks": {"osd": osd, "pgid": placement["pgid"], "epoch": placement["epoch"],
                             "acks": acks(history.strict_document(raw, MAX_HISTORY), placement["pgid"])}}
