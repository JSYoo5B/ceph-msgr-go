#!/bin/sh
# Test-only native oracles. Public v0.2.0 APIs own all daemons and keys.
set -eu
trap 'code=$?; if test "$code" -ne 0; then printf "native oracle exit=%s phase=%s\n" "$code" "${fixture_phase:-startup}" > /out/native-oracles.error; fi' EXIT
root=/tmp/ceph-msgr-test
mkdir -p "$root"
cp /etc/ceph/ceph.conf "$root/ceph.conf"
cat /out/*.keyring > "$root/keyring"
keyring="$root/keyring"
timeout 10 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" mon getmap -o "$root/monmap"
mgr_count=$CEPH_MSGR_TEST_MGR_COUNT
idle_sessions=$CEPH_MSGR_TEST_IDLE_SESSIONS
auth_epoch=$CEPH_MSGR_TEST_AUTH_EPOCH
short_tickets=$CEPH_MSGR_TEST_SHORT_TICKETS
mode_rejection=$CEPH_MSGR_TEST_MODE_REJECTION
mapped_ipv6=$CEPH_MSGR_TEST_MAPPED_IPV6
osd_fixture=$CEPH_MSGR_TEST_OSD
config_fixture=0
if test "$mgr_count" = 2 && test "$idle_sessions" = 0 && test "$auth_epoch" = 0 && test "$short_tickets" = 0 && test -z "$mode_rejection" && test "$mapped_ipv6" = 0; then
    config_fixture=1
fi
fixture_phase=native-oracles
if test "$mapped_ipv6" = 1; then
    # Fresh native Ceph clients independently prove the configured wire family
    # and MGR read path before the Go regression is permitted to run.
    mapped_monitors="$CEPH_MSGR_NATIVE_MONITORS"
    timeout 3 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" -m "$mapped_monitors" status --format json > /out/mapped-oracle-status.json
    timeout 3 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" -m "$mapped_monitors" mon dump --format json > /out/mapped-oracle-mon.json
    timeout 3 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" -m "$mapped_monitors" mgr dump --format json > /out/mapped-oracle-mgr.json
    timeout 3 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" -m "$mapped_monitors" pg stat --format json > /out/mapped-oracle-pg.json
    python3 /out/mapped_address_oracle.py /out
fi
if test "$mgr_count" = 2 && test "$idle_sessions" = 0 && test "$auth_epoch" = 0 && test "$short_tickets" = 0 && test -z "$mode_rejection" && test "$mapped_ipv6" = 0; then
    # Independent native clients exercise daemon-local MCommand tell routing
    # before the ordinary Go suite. Special fixtures keep their own selectors
    # and oracles. Host tests use this same owned oracle client container.
    tell_monitors="$CEPH_MSGR_NATIVE_MONITORS"
    timeout 5 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" -m "$tell_monitors" tell mon.a version --format json > /out/tell-oracle-mon.json
    timeout 5 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" -m "$tell_monitors" tell mon.b mon_status --format json > /out/named-mon-b.json
    timeout 3 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" -m "$tell_monitors" mgr dump --format json > /out/tell-oracle-mgr-map.json
    tell_mgr=$(python3 - /out/tell-oracle-mgr-map.json <<'PY'
import json
import sys

with open(sys.argv[1], "rb") as source:
    data = source.read(1024 * 1024 + 1)
if len(data) > 1024 * 1024:
    raise SystemExit("Tell oracle mgr map exceeds its metadata limit")
try:
    value = json.loads(data)
except (ValueError, UnicodeError):
    raise SystemExit("Tell oracle mgr map is invalid JSON") from None
if (not isinstance(value, dict) or value.get("available") is not True
        or value.get("active_name") not in ("a", "b")
        or type(value.get("active_gid")) is not int
        or not 0 < value["active_gid"] < 1 << 64):
    raise SystemExit("Tell oracle requires the current available fixture MGR")
print(value["active_name"])
PY
    )
    timeout 5 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" -m "$tell_monitors" tell "mgr.$tell_mgr" version --format json > /out/tell-oracle-mgr.json
    python3 - /out/tell-oracle-mon.json /out/tell-oracle-mgr.json <<'PY'
import json
import re
import sys

# Pinned admin_socket.cc VersionHook emits these three top-level strings;
# version.cc supplies the build version, release name, and release type.
# Keep each daemon's original JSON bytes unchanged for the Go comparison.
for path in sys.argv[1:]:
    with open(path, "rb") as source:
        data = source.read(4097)
    if len(data) > 4096:
        raise SystemExit("Tell oracle version exceeds its metadata limit")
    try:
        value = json.loads(data)
    except (ValueError, UnicodeError):
        raise SystemExit("Tell oracle version is invalid JSON") from None
    if not isinstance(value, dict) or set(value) != {"version", "release", "release_type"}:
        raise SystemExit("Tell oracle version has an unexpected JSON shape")
    if any(not isinstance(field, str) or not field or len(field) > 128
           or not field.isascii() or any(ord(char) < 32 or ord(char) == 127 for char in field)
           for field in value.values()):
        raise SystemExit("Tell oracle version fields must be bounded printable strings")
    if value["release"] != "tentacle" or not re.fullmatch(r"20\.2\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?", value["version"]):
        raise SystemExit("Tell oracle daemon must identify Tentacle 20.2")
PY
    log_sentinel=$(python3 -c 'import uuid; print("ceph-msgr-log-oracle-" + uuid.uuid4().hex)')
    printf '%s\n' "$log_sentinel" > /out/log-oracle-sentinel.txt
    timeout 5 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" -m "$tell_monitors" log "$log_sentinel"
    timeout 3 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" -m "$tell_monitors" log last 20 --format json > "$root/log-oracle-tail.json"
    python3 /out/log_oracle.py /out "$root/log-oracle-tail.json"
    timeout 15 python3 /out/command_oracle.py "$root" /out 0 initial
fi
if test "$osd_fixture" = 1; then
    fixture_phase=osd-bootstrap
    sh /out/osd-fixture.sh "$root"
fi
touch /out/ready
while true; do
    if test -f /out/verify-osd-maps; then
        fixture_phase=verify-osd-maps
        # Native blob validation also serves MON-only map acquisition; no OSD
        # daemon or object fixture is required to receive the MON's raw maps.
        read -r request_id label extra < /out/verify-osd-maps
        case "$request_id" in ''|*[!0-9]*) exit 2 ;; esac
        test "$request_id" -gt 0 && test "$label" = received && test -z "$extra" || exit 2
        rm /out/verify-osd-maps
        fixture_phase=verify-osd-map-oracle
        timeout 30 python3 /out/osd_map_oracle.py /out "$root" 2>> /out/osd-map-control.log
        sh /out/publish-ack.sh "/out/osd-maps-verify.$request_id"
        fixture_phase=control-idle
    fi
    if test -f /out/verify-digest; then
        fixture_phase=verify-digest
        test "$config_fixture" = 1 || exit 2
        read -r request_id name label extra < /out/verify-digest
        case "$request_id" in ''|*[!0-9]*) exit 2 ;; esac
        test "$request_id" -gt 0 || exit 2
        case "$name" in a|b|c) ;; *) exit 2 ;; esac
        case "$label" in initial|muted|unmuted|renewal|learned-mon|reopen) ;; *) exit 2 ;; esac
        test -z "$extra" || exit 2
        rm /out/verify-digest
        timeout 20 python3 /out/digest_oracle.py "$root" /out "$name" "$label"
        sh /out/publish-ack.sh "/out/digest-verify.$request_id"
    fi
    if test -f /out/verify-monitor-map; then
        fixture_phase=verify-monitor-map
        test "$config_fixture" = 1 || exit 2
        read -r request_id label extra < /out/verify-monitor-map
        case "$request_id" in ''|*[!0-9]*) exit 2 ;; esac
        test "$request_id" -gt 0 || exit 2
        case "$label" in initial|added|removed) ;; *) exit 2 ;; esac
        test -z "$extra" || exit 2
        rm /out/verify-monitor-map
        timeout 10 python3 /out/monitor_oracle.py "$root" /out
        sh /out/publish-ack.sh "/out/monitor-map-verify.$request_id"
    fi
    if test -f /out/verify-manager-map; then
        fixture_phase=verify-manager-map
        test "$config_fixture" = 1 || exit 2
        read -r request_id label extra < /out/verify-manager-map
        case "$request_id" in ''|*[!0-9]*) exit 2 ;; esac
        test "$request_id" -gt 0 || exit 2
        case "$label" in initial|replacement|disabled|restored) ;; *) exit 2 ;; esac
        test -z "$extra" || exit 2
        rm /out/verify-manager-map
        timeout 10 python3 /out/manager_oracle.py "$root" /out
        sh /out/publish-ack.sh "/out/manager-map-verify.$request_id"
    fi
    if test -f /out/verify-tell-descriptions; then
        fixture_phase=verify-tell-descriptions
        test "$config_fixture" = 1 || exit 2
        read -r request_id label extra < /out/verify-tell-descriptions
        case "$request_id" in ''|*[!0-9]*) exit 2 ;; esac
        test "$request_id" -gt 0 || exit 2
        test "$label" = initial && test -z "$extra" || exit 2
        rm /out/verify-tell-descriptions
        timeout 25 python3 /out/tell_command_oracle.py "$root" /out
        sh /out/publish-ack.sh "/out/tell-descriptions-verify.$request_id"
    fi
    if test -f /out/verify-command-descriptions; then
        fixture_phase=verify-command-descriptions
        test "$config_fixture" = 1 || exit 2
        read -r request_id label extra < /out/verify-command-descriptions
        case "$request_id" in ''|*[!0-9]*) exit 2 ;; esac
        test "$request_id" -gt 0 || exit 2
        case "$label" in initial|renewal|learned-mon|mgr-replacement) ;; *) exit 2 ;; esac
        test -z "$extra" || exit 2
        rm /out/verify-command-descriptions
        timeout 15 python3 /out/command_oracle.py "$root" /out "$request_id" "$label"
        sh /out/publish-ack.sh "/out/command-descriptions-verify.$request_id"
    fi
    if test -f /out/verify-hostname-config; then
        fixture_phase=verify-hostname-config
        test "$config_fixture" = 1 || exit 2
        read -r request_id label extra < /out/verify-hostname-config
        case "$request_id" in ''|*[!0-9]*) exit 2 ;; esac
        test "$request_id" -gt 0 || exit 2
        case "$label" in initial|subscriptions|reopen|changed|renewal|learned|restored) ;; *) exit 2 ;; esac
        test -z "$extra" || exit 2
        rm /out/verify-hostname-config
        timeout 30 python3 /out/hostname_config_oracle.py "$root" /out "$request_id" "$label"
        sh /out/publish-ack.sh "/out/hostname-config-verify.$request_id"
    fi
    if test -f /out/verify-config; then
        fixture_phase=verify-config
        fixture_phase=verify-config-read
        test "$config_fixture" = 1 || exit 2
        read -r request_id identity label extra < /out/verify-config
        case "$request_id" in ''|*[!0-9]*) exit 2 ;; esac
        test "$request_id" -gt 0 || exit 2
        case "$identity" in client.test|client.configwatch) ;; *) exit 2 ;; esac
        case "$label" in initial|reopen|reset|global|client|exact|no-read|no-read-update|fallback-client|fallback-global|removed|raw|replacement|after-overflow|renewal-1|renewal-2|learned|final-deletion) ;; *) exit 2 ;; esac
        test -z "$extra" || exit 2
        fixture_phase="verify-config-$label-remove"
        rm /out/verify-config
        fixture_phase="verify-config-$label-oracle"
        # A fresh native CLI is the independent read oracle. Full effective
        # configuration remains in memory; only these controlled keys may be
        # persisted. This path must never print credentials or raw responses.
        python3 - "$request_id" "$identity" "$label" "$root" /out <<'PY'
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import time

request_id, identity, label, fixture_root, output_dir = sys.argv[1:]
identities = {"client.test", "client.configwatch"}
labels = {
    "initial", "reopen", "reset", "global", "client", "exact", "no-read",
    "no-read-update", "fallback-client", "fallback-global", "removed", "raw",
    "replacement", "after-overflow", "renewal-1", "renewal-2", "learned",
    "final-deletion",
}
if not request_id.isascii() or not request_id.isdecimal() or int(request_id) <= 0 or identity not in identities or label not in labels:
    raise SystemExit("Invalid config oracle request")
root_path, output_path = pathlib.Path(fixture_root), pathlib.Path(output_dir)
command = [
    "ceph", "-c", str(root_path / "ceph.conf"), "-n", "client.test",
    "-k", str(root_path / "keyring"), "config", "get", identity,
    "--format", "json",
]
deadline = time.monotonic() + 12
while True:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise SystemExit("Fresh native config read did not complete within its bound")
    try:
        result = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, timeout=min(3, remaining), check=False)
    except subprocess.TimeoutExpired:
        continue
    if result.returncode == 0:
        break
    # Only independent read probes retry; Go mutations are submitted once.
    time.sleep(min(0.1, max(0, deadline - time.monotonic())))
if len(result.stdout) > 1024 * 1024:
    raise SystemExit("Native config read exceeds the response bound")
try:
    full = json.loads(result.stdout)
except (ValueError, UnicodeError):
    raise SystemExit("Native config read is invalid JSON") from None
if not isinstance(full, dict) or len(full) > 4096:
    raise SystemExit("Native config read has an invalid option map")
fingerprint = hashlib.sha256()
for key in sorted(full, key=lambda raw: raw.encode("utf-8")):
    option = full[key]
    if not isinstance(option, dict) or not isinstance(option.get("value"), str):
        raise SystemExit("Native config read has an invalid raw option")
    for raw in (key, option["value"]):
        encoded_raw = raw.encode("utf-8")
        fingerprint.update(len(encoded_raw).to_bytes(8, "big"))
        fingerprint.update(encoded_raw)
entries = len(full)
selected = {}
for key in ("client_mount_timeout", "ceph_msgr_config_fixture_raw"):
    if key not in full:
        continue
    option = full[key]
    if not isinstance(option, dict) or not isinstance(option.get("value"), str) or len(option["value"].encode("utf-8")) > 16 * 1024:
        raise SystemExit("Native config read has an invalid controlled value")
    selected[key] = option["value"]
del full, result
metadata = {"request_id": int(request_id), "identity": identity, "label": label, "values": selected,
            "map_sha256": fingerprint.hexdigest(), "map_entries": entries}
encoded = json.dumps(metadata, ensure_ascii=True, separators=(",", ":")) + "\n"
temporary = output_path / ("config-oracle." + request_id + ".tmp")
temporary.write_text(encoded, encoding="utf-8")
os.replace(temporary, output_path / "config-oracle.json")
with (output_path / "config-oracle-history.jsonl").open("a", encoding="utf-8") as history:
    history.write(encoded)
PY
        fixture_phase="verify-config-$label-ack"
        sh /out/publish-ack.sh "/out/config-verify.$request_id"
        fixture_phase=control-idle
    fi
    if test -f /out/verify-service-keys; then
        fixture_phase=verify-service-keys
        test "$auth_epoch" = 1 || exit 2
        read -r request_id name mgr_id map_epoch auth_key_epoch extra < /out/verify-service-keys
        case "$request_id" in ''|*[!0-9]*) exit 2 ;; esac
        case "$name" in a|b) ;; *) exit 2 ;; esac
        for value in "$mgr_id" "$map_epoch" "$auth_key_epoch"; do
            case "$value" in ''|*[!0-9]*) exit 2 ;; esac
            test "$value" -gt 0 || exit 2
        done
        test -z "$extra" || exit 2
        rm /out/verify-service-keys
        keys_ready=false
        key_probe_deadline=$(( $(date +%s) + 15 ))
        service_key_probe() {
            key_probe_remaining=$(( key_probe_deadline - $(date +%s) ))
            test "$key_probe_remaining" -gt 0 || return 1
            key_probe_timeout=3
            if test "$key_probe_remaining" -lt "$key_probe_timeout"; then
                key_probe_timeout=$key_probe_remaining
            fi
            # Native CLI bootstrap/reconnect follows Ceph's own policy. Only
            # final authenticated read replies satisfy this oracle; the Go
            # subject's first MGR authentication is still submitted once.
            timeout "$key_probe_timeout" ceph -c "$root/ceph.conf" -n client.test -k "$keyring" --monmap "$root/monmap" --mon-client-target-rank "$mon_rank" "$@"
        }
        service_key_check() {
            # Preserve every public native observation, including failed
            # readiness checks. A later successful CLI cannot prove which
            # rotating-key generation the Go subject already received.
            key_probe_phase=$1
            shift
            if service_key_probe "$@" > "$root/service-keys-probe.json" 2> "$root/service-keys-probe.log"; then
                key_probe_code=0
            else
                key_probe_code=$?
            fi
            python3 /out/service_key_oracle.py "$root/service-keys-probe.json" /out/service-keys-history.jsonl "$request_id" "$key_probe_phase" "$mon_name" "$mon_rank" "$map_epoch" "$auth_key_epoch" "$name" "$mgr_id" "$key_probe_code"
        }
        while test "$(date +%s)" -lt "$key_probe_deadline"; do
            keys_ready=true
            for mon_name in a b c; do
                case "$mon_name" in a) mon_rank=0 ;; b) mon_rank=1 ;; c) mon_rank=2 ;; esac
                # Use the fixture's complete initial map; a single -m seed
                # would have only rank 0 before bootstrap. Pin the native
                # MonClient's rank rather than allowing it to hunt any MON.
                # mon dump is forwarded to the leader, so it cannot prove a
                # particular MON's local map has learned this key wipe.
                if ! service_key_check mon-status tell "mon.$mon_name" mon_status --format json; then
                    keys_ready=false
                    break
                fi
                # This query checks active identity only; the following
                # named daemon Tell proves an actual fresh MGR connection.
                if ! service_key_check mgr-map mgr dump --format json; then
                    keys_ready=false
                    break
                fi
                if ! service_key_check mgr-version tell "mgr.$name" version --format json; then
                    keys_ready=false
                    break
                fi
            done
            test "$keys_ready" != true || break
            # Repeat only independent native read probes. The Go clients'
            # authentication rejection and the wipe mutation are not retried.
            sleep 1
        done
        if test "$keys_ready" != true; then
            cat "$root/service-keys-probe.log" "$root/service-keys-probe.json"
            exit 1
        fi
        sh /out/publish-ack.sh "/out/service-keys-verify.$request_id"
    fi
    sleep 0.1
done
