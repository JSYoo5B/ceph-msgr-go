"""Combined multistripe OSD interruption and existing-MGR-standby takeover receipt."""
import rgw_native_gate_multi_stripe as multi
from rgw_native_gate_backend_fault import WRITE

TEST = "TestRGWNativeMultiStripeFaultIntegration"
MARKER = "RGW_NATIVE_MULTISTRIPE_FAULT "


def check(line, gate):
    receipt, problems = gate.json_receipt(line, MARKER)
    if receipt is None:
        return problems, {}
    problems.extend(multi.check_receipt(receipt, gate, size=12 << 20))
    fault = receipt.get("osd_fault") or {}
    before, down, after = (fault.get(key, {}) for key in ("before", "down", "after"))
    if not (before.get("up") is True and down.get("up") is False and after.get("up") is True
            and 0 < before.get("epoch", 0) < down.get("epoch", 0) < after.get("up_from", 0)
            and before.get("nonce", 0) > 0 and after.get("nonce", 0) > 0
            and before.get("nonce") != after.get("nonce")):
        problems.append("OSD fault lacks ordered down/recovery maps and a new process nonce")
    if fault.get("prefix_bytes") != 11 << 20 or fault.get("pending_at_restart") is not True or fault.get("overlap_window_seconds", 0) < 5:
        problems.append("upload lacks a bounded outstanding-body OSD overlap")
    outcome = fault.get("outcome", {})
    if outcome.get("request") != "PUT multistripe" or outcome.get("class") != "succeeded" or outcome.get("status") != 200:
        problems.append("single upload did not return a complete successful outcome")
    writes = fault.get("pre_fault_writes")
    clients = {entry.get("client") for entry in receipt.get("gateway_generation", [])}
    objects = {entry.get("oid") for entry in receipt.get("stripes", [])}
    puts = {(entry.get("client"), entry.get("tid"), entry.get("object")) for entry in receipt.get("put_reception", [])}
    if not isinstance(writes, list) or not writes:
        problems.append("missing pre-fault gateway stripe reception")
    else:
        for op in writes:
            if (op.get("client") not in clients or op.get("object") not in objects
                    or (op.get("client"), op.get("tid"), op.get("object")) not in puts
                    or "reached_pg" not in op.get("events", [])
                    or not WRITE.search(op.get("description", ""))
                    or not 0 < op.get("map_epoch", 0) < down.get("epoch", 0)):
                problems.append("pre-fault WRITE does not join the gateway, stripe, PUT and old map")
    takeover = receipt.get("mgr_takeover") or {}
    old, new = (takeover.get(key, {}) for key in ("before", "after"))
    standby = old.get("standbys", [])
    if not (old.get("available") is True and new.get("available") is True
            and 0 < old.get("epoch", 0) < new.get("epoch", 0)
            and len(standby) == 1 and standby[0].get("name") == new.get("active_name")
            and standby[0].get("gid") == new.get("active_gid")
            and old.get("active_name") != new.get("active_name")
            and old.get("active_gid", 0) > 0 and new.get("active_gid", 0) > 0
            and old.get("active_gid") != new.get("active_gid")):
        problems.append("new active is not the pre-existing distinct standby")
    old_addrs = old.get("active_addrs", {}).get("addrvec", [])
    new_addrs = new.get("active_addrs", {}).get("addrvec", [])
    if (not old_addrs or not new_addrs or not new_addrs[0].get("addr")
            or old_addrs[0].get("addr") == new_addrs[0].get("addr")
            or not new_addrs[0].get("nonce")):
        problems.append("takeover lacks the new active endpoint")
    if not takeover.get("gateway_connected_before") or not set(takeover["gateway_connected_before"]).issubset(clients):
        problems.append("original active has no gateway connection")
    if takeover.get("former_active_stopped") is not True or not takeover.get("gateway_connected"):
        problems.append("takeover lacks the stopped former active and gateway reconnection")
    elif not set(takeover["gateway_connected"]).issubset(clients):
        problems.append("promoted-manager connection is not from the observed gateway")
    return problems, {"stripes": len(receipt.get("stripes", [])), "standby_takeover": not problems}
