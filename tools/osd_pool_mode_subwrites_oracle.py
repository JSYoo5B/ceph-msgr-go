"""Owned-pool oracle mode subwrites: EC sub-writes an OSD has completed.

Test-only; never product code. One read-only observation, sending nothing
that changes the cluster: a placement record of an owned object, then
`dump_historic_ops` of one explicit OSD, and the MOSDECSubOpWrite entries
among those ops for the object's PG that carry one client request identity.

Pinned source (Ceph 20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964):
the primary of an erasure-coded PG sends each remote shard of a write one
MOSDECSubOpWrite (src/osd/ECCommon.cc RMWPipeline, src/osd/ECBackend.cc),
which the receiving OSD tracks as an op like any other fast-dispatched
message (src/osd/OSD.cc ms_fast_dispatch). Its description is
"MOSDECSubOpWrite(<spg_t> <map_epoch>/<min_epoch> ECSubWrite(tid=<tid>,
reqid=<reqid>, at_version=<epoch>'<version>, trim_to=..., pg_committed_to=...
[, has_updated_hit_set_history][, backfill_or_async_recovery]))"
(src/messages/MOSDECSubOpWrite.h print, src/osd/ECMsgTypes.cc operator<<),
where spg_t names the receiving shard. ECBackend::handle_sub_write marks
sub_op_started, and the SubWriteCommitted context marks sub_op_committed
once the shard's transaction is durable, before the reply to the primary is
sent (src/osd/ECBackend.cc:325-400). Once every shard committed a write and the
pipeline is idle, the primary sends each shard one more, transaction-empty
sub-write with the same request identity, at_version 0'0 and pg_committed_to
at the write's version, to roll the entry forward (src/osd/ECCommon.cc:955-995
finish_rmw, ECDummyOp). The tracker moves an op into its
history after its last reference is dropped, with the terminal event done
(src/common/TrackedOp.h).

The history is bounded (osd_op_history_size, osd_op_history_duration), so a
sub-write this mode does not report is not shown absent by this mode alone.
OpHistory::cleanup drops the ops that arrived more than the duration ago,
oldest first, and then, while more than size ops are kept, the op of the
shortest duration, whatever its arrival (src/common/TrackedOp.cc:101-124);
dump_ops runs that cleanup and then dumps size, duration and the ops in
arrival order (:126-153). A short roll-forward can therefore be dropped while
the slower write it follows is kept, and an op in flight at one read can
enter the history later and expire by its old initiation time, so two
snapshots do not bound what was inserted and dropped between them. The
receipt carries the history itself (its size and duration and one identity
per op, the SHA-256 of the op's initiated_at and description) so that the
caller can refuse inconsistent reads and report what it saw; it does not
make a missing entry evidence (integration/osd_erasure_subwrite_hold_test.go
opHistoryWitness). A
matched entry is reported only with its whole event list ending in "done";
an over-bound, malformed or unterminated list fails the observation. It
judges nothing else.
"""
import hashlib
import re

import osd_mutation_oracle as mutation
import osd_pg_history_oracle as history
from osd_pool_oracle import explicit_osd, find_pool, object_request, oracle_mode, pool_name

MAX_HISTORY = 1 << 20
MAX_OPS = 4096
MAX_DESCRIPTION = 16384
MAX_EVENTS = 128
EVERSION = r"(0|[1-9][0-9]{0,9})'(0|[1-9][0-9]{0,19})"
SUB_WRITE = re.compile(r"MOSDECSubOpWrite\(((?:0|[1-9][0-9]{0,18})\.(?:0|[1-9a-f][0-9a-f]{0,7}))s(0|[1-9][0-9]?) "
                       r"(0|[1-9][0-9]{0,9})/(0|[1-9][0-9]{0,9}) ECSubWrite\(tid=(0|[1-9][0-9]{0,19}), reqid=(\S{1,128}), "
                       r"at_version=" + EVERSION + r", trim_to=" + EVERSION + r", pg_committed_to=" + EVERSION +
                       r"(, has_updated_hit_set_history)?(, backfill_or_async_recovery)?\)\)")


def sub_writes(document, pgid, reqid):
    """The MOSDECSubOpWrite entries of one `dump_historic_ops` document for
    pgid that carry the request identity reqid (printed osd_reqid_t)."""
    if not isinstance(document, dict) or not isinstance(document.get("ops"), list) or len(document["ops"]) > MAX_OPS:
        raise ValueError("native op history exceeds its schema or count bound")
    found = []
    for op in document["ops"]:
        description = op.get("description") if isinstance(op, dict) else None
        if not isinstance(description, str) or len(description) > MAX_DESCRIPTION or not description.startswith("MOSDECSubOpWrite("):
            continue
        match = SUB_WRITE.fullmatch(description)
        if not match:
            # A sub-write of this request that does not parse is not skipped.
            if "reqid=" + reqid + "," in description:
                raise ValueError("native sub-write of the target request has an unparsable description")
            continue
        if match[1] != pgid or match[6] != reqid:
            continue
        data = op.get("type_data") if isinstance(op.get("type_data"), dict) else {}
        names = complete_events(data.get("events"))
        started = names.index("sub_op_started") if "sub_op_started" in names else -1
        found.append({"pgid": match[1], "shard": int(match[2]), "map_epoch": int(match[3]), "min_epoch": int(match[4]),
                      "tid": int(match[5]), "reqid": match[6], "at_version": "%s'%s" % (match[7], match[8]),
                      "trim_to": "%s'%s" % (match[9], match[10]), "pg_committed_to": "%s'%s" % (match[11], match[12]),
                      "backfill_or_async_recovery": match[14] is not None,
                      "committed": started >= 0 and "sub_op_committed" in names[started + 1:],
                      "done": names[-1] == "done", "events": names})
    return found


def history_retention(document):
    """The size, duration and op identities of one `dump_historic_ops`
    document: each identity is the SHA-256 of the op's initiated_at and
    description. A document without integer limits or with an op lacking
    either field raises."""
    if not isinstance(document, dict) or not isinstance(document.get("ops"), list) or len(document["ops"]) > MAX_OPS:
        raise ValueError("native op history exceeds its schema or count bound")
    size, duration = document.get("size"), document.get("duration")
    if type(size) is not int or type(duration) is not int or size < 0 or duration < 0:
        raise ValueError("native op history has no integer size and duration")
    ops = []
    for op in document["ops"]:
        initiated = op.get("initiated_at") if isinstance(op, dict) else None
        description = op.get("description") if isinstance(op, dict) else None
        if not isinstance(initiated, str) or not isinstance(description, str) or len(description) > MAX_DESCRIPTION:
            raise ValueError("native op history has an op without initiated_at and description")
        ops.append(hashlib.sha256((initiated + "\n" + description).encode()).hexdigest())
    return {"size": size, "duration": duration, "ops": ops}


def complete_events(events):
    """The event names of a matched sub-write, only when the list is whole
    and ends with the terminal "done"; anything else raises."""
    if not isinstance(events, list) or not events or len(events) > MAX_EVENTS:
        raise ValueError("native op history of a matched sub-write has no complete event list")
    names = []
    for event in events:
        name = event.get("event") if isinstance(event, dict) else None
        if not isinstance(name, str) or not name or len(name) > MAX_DESCRIPTION:
            raise ValueError("native op history of a matched sub-write has a malformed event")
        names.append(name)
    if names[-1] != "done":
        raise ValueError("native op history of a matched sub-write does not end with done")
    return names


@oracle_mode
def mode_subwrites(rados, cluster, request, out):
    """The completed EC sub-writes of one client request in one OSD's op history.

    Read-only: one placement record of the object, then `dump_historic_ops`
    of the explicit "osd" (never the primary implicitly: the caller names a
    shard's OSD).
    """
    if set(request) - {"slot", "object", "namespace", "osd", "reqid"} or "osd" not in request:
        raise ValueError("subwrites takes slot, object, namespace, osd and reqid, with an explicit osd")
    pool = pool_name(request["slot"])
    _, namespace = object_request(request)
    osd = explicit_osd(request)
    reqid = history.reqid_text(history.target_identity(request.get("reqid")))
    descriptor = find_pool(rados.dump(cluster), pool)
    if descriptor is None:
        raise RuntimeError("owned pool is missing")

    def command(*arguments, timeout):
        return mutation.native_mon_query(rados.lib, cluster, *arguments, timeout=timeout)

    placement = mutation.collect_placement(command, pool, request["object"], descriptor["pool"], namespace=namespace, timeout=10)
    code, raw, status, oversized = rados.osd(cluster, osd, {"prefix": "dump_historic_ops", "format": "json"}, MAX_HISTORY)
    if code != 0 or oversized is not None:
        raise RuntimeError("native dump_historic_ops failed (%d): %s" % (code, status))
    document = history.strict_document(raw, MAX_HISTORY)
    return {"sub_writes": {"osd": osd, "pgid": placement["pgid"], "epoch": placement["epoch"], "reqid": reqid,
                           "writes": sub_writes(document, placement["pgid"], reqid), "history": history_retention(document)}}
