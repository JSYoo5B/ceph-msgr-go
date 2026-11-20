"""Receipt check of TestRGWNativeBackendFaultIntegration for tools/rgw_native_gate.py.

It recomputes the deterministic object hashes and checks the retained fault
record: the OSD down and back up in later epochs with a new Messenger nonce,
every overlapping request classified as a success, gateway-attributed
reception at the restarted OSD in its new map epochs and after the gateway's
pre-fault TIDs, the gateway's watch reconnects, and a new manager the gateway
reconnected and reported to.
"""
import hashlib
import re

TEST = "TestRGWNativeBackendFaultIntegration"
MARKER = "RGW_NATIVE_BACKEND_FAULT "
# Match rgwNativeWatchReconnect and rgwNativeWrites in integration/fixture.
WATCH_RECONNECT = re.compile(r"(?:\[|,)watch reconnect cookie [0-9]+ gen [1-9][0-9]*(?:[ ,\]]|$)")
WRITE = re.compile(r"(?:\[|,)(?:write|writefull)(?:[ ,\]]|$)")
# Match rgwNativeHeadRead: a complete READ token at offset 0 whose length
# covers the object.
READ = re.compile(r"(?:\[|,)read 0~([0-9]+)(?:[ ,\]]|$)")
NOTIFY = re.compile(r"^notify\.[0-9]+$")
# The objects of the backend-fault checkpoint and the (multiplier, offset) of
# rgwNativeFaultPayload that builds each one's 1,031 bytes.
FAULT_OBJECTS = {"before-fault": (13, 7), "during-osd-fault": (29, 11), "during-mgr-fault": (31, 3)}
FAULT_SIZE = 1031


def check(line, gate):
    """(problems, summary fields) of the one receipt line."""
    receipt, problems = gate.json_receipt(line, MARKER)
    if receipt is None:
        return problems, {}
    return check_receipt(receipt, gate), {}


def fault_payload(multiplier, offset, size=FAULT_SIZE):
    """Mirror rgwNativeFaultPayload in integration/fixture."""
    return bytes((i * multiplier + offset) & 0xff for i in range(size))


def _ops(value):
    return [op for op in value if isinstance(op, dict)] if isinstance(value, list) else []


def _clients(value, generation):
    """Whether value is a nonempty list of the gateway's own client names."""
    return isinstance(value, list) and bool(value) and all(client in generation for client in value)


def _calls(op, method, gate):
    return method in gate.call_methods(op.get("description", ""))


def _head_read(op, key):
    """A complete READ of the whole head of key (rgwNativeHeadRead)."""
    return str(op.get("object", "")).endswith("_" + key) and any(
        int(length) >= FAULT_SIZE for length in READ.findall(op.get("description", ""))[:16])


def _put_problems(ops, key, phase, gate):
    """A complete gateway PUT of key: head WRITE/WRITEFULL and index
    prepare/complete, and no entry that is none of these."""
    head = [op for op in ops if str(op.get("object", "")).endswith("_" + key) and WRITE.search(op.get("description", ""))]
    index = [op for op in ops if str(op.get("object", "")).startswith(".dir.")]
    if not (head and any(_calls(op, "rgw.bucket_prepare_op", gate) for op in index)
            and any(_calls(op, "rgw.bucket_complete_op", gate) for op in index)):
        return ["%s has no complete gateway PUT reception of %s" % (phase, key)]
    if len(head) + len(index) != len(ops) or not all(
            _calls(op, "rgw.bucket_prepare_op", gate) or _calls(op, "rgw.bucket_complete_op", gate) for op in index):
        return ["%s PUT reception holds an entry that is not its head write or index call" % phase]
    return []


def check_receipt(receipt, gate):
    problems = list(gate.release_problems(receipt))
    hashes = receipt.get("sha256")
    expected = {key: hashlib.sha256(fault_payload(*args)).hexdigest() for key, args in FAULT_OBJECTS.items()}
    if receipt.get("bytes") != FAULT_SIZE or hashes != expected:
        problems.append("receipt object hashes differ from the deterministic inputs")
    generation = {entry.get("client") for entry in _ops(receipt.get("gateway_generation"))}
    if not generation:
        problems.append("receipt has no gateway generation")
    before_fault = receipt.get("before_fault") if isinstance(receipt.get("before_fault"), dict) else {}
    last = before_fault.get("last_tids") if isinstance(before_fault.get("last_tids"), dict) else {}
    if not last:
        problems.append("receipt has no gateway-attributed reception before the fault")
    problems.extend(_put_problems(_ops(before_fault.get("put_reception")), "before-fault", "pre-fault", gate))
    osd = receipt.get("osd") if isinstance(receipt.get("osd"), dict) else {}
    states = [osd.get(name) if isinstance(osd.get(name), dict) else {} for name in ("before", "down", "after")]
    before, down, after = states
    up_from = after.get("up_from")
    if not (before.get("up") is True and down.get("up") is False and after.get("up") is True
            and isinstance(before.get("epoch"), int) and isinstance(down.get("epoch"), int)
            and isinstance(up_from, int) and before["epoch"] < down["epoch"] < up_from):
        problems.append("OSD record does not show it up, then down, then up again in later epochs")
    if not before.get("nonce") or before.get("nonce") == after.get("nonce"):
        problems.append("restarted OSD kept its Messenger nonce")
    outcomes = _ops(osd.get("outcomes"))
    if len(outcomes) != 2 or any(o.get("class") != "succeeded" or o.get("status") != 200 for o in outcomes):
        problems.append("a request that overlapped the OSD outage is not classified as a success")
    if osd.get("pending_at_restart") is not True:
        problems.append("the overlapping requests were not outstanding when the OSD was started")
    put, get, watches = _ops(osd.get("put_reception")), _ops(osd.get("get_reception")), _ops(osd.get("watch_reconnect"))
    problems.extend(_put_problems(put, "during-osd-fault", "restarted OSD", gate))
    if not get or not all(_head_read(op, "before-fault") for op in get):
        problems.append("restarted OSD has no gateway GET reception that reads the whole before-fault head")
    if not watches or not all(WATCH_RECONNECT.search(op.get("description", "")) and NOTIFY.match(str(op.get("object", "")))
                              for op in watches):
        problems.append("restarted OSD has no gateway watch reconnect on a control object")
    for op in put + get + watches:
        client, tid, epoch = op.get("client"), op.get("tid"), op.get("map_epoch")
        if client not in generation or not isinstance(tid, int) or not isinstance(epoch, int):
            problems.append("reception entry is not attributed to the gateway")
        elif not isinstance(up_from, int) or epoch < up_from:
            problems.append("%s TID %d carries an epoch before the OSD returned" % (client, tid))
        elif client in last and (not isinstance(last[client], int) or tid <= last[client]):
            problems.append("%s TID %d is not after the gateway's pre-fault TIDs" % (client, tid))
    if not _clients(osd.get("gateway_connected"), generation):
        problems.append("no gateway Messenger connection to the restarted OSD")
    mgr = receipt.get("mgr") if isinstance(receipt.get("mgr"), dict) else {}
    old, new = [mgr.get(name) if isinstance(mgr.get(name), dict) else {} for name in ("before", "after")]
    if not (old.get("available") is True and new.get("available") is True and old.get("gid") and new.get("gid")
            and old["gid"] != new["gid"] and old.get("nonce") and new.get("nonce") and old["nonce"] != new["nonce"]):
        problems.append("manager record does not show a new active manager process")
    outcome = mgr.get("outcome") if isinstance(mgr.get("outcome"), dict) else {}
    if outcome.get("class") != "succeeded" or outcome.get("status") != 200:
        problems.append("the request during the manager outage is not classified as a success")
    if not _clients(mgr.get("gateway_connected"), generation) or mgr.get("status_reported") is not True:
        problems.append("the gateway did not reconnect and report to the restarted manager")
    gids = mgr.get("service_gids")
    if not isinstance(gids, list) or len(gids) != 1 or "client.%s" % gids[0] not in generation:
        problems.append("the service map rgw daemon is not the gateway")
    if receipt.get("native_readback") != "confirmed" or receipt.get("object_index_user_cleanup") != "confirmed":
        problems.append("receipt lacks its native readback or confirmed cleanup")
    return problems
