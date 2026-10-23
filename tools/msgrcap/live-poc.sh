#!/bin/bash
# Development-only live capture PoC for one disposable Tentacle MON.
# Usage: live-poc.sh crc|secure OUTPUT_DIR IMAGE
#
# Runs on a GitHub Actions Linux runner (Docker, sudo tcpdump, tshark). The
# capture uses runner-host tcpdump on the fixture bridge as a labelled
# substitute for the planned ceph-testcontainers capture helper; it does not
# validate that helper or its delivery contract. The disposable client key
# stays in a private work directory and is never written to OUTPUT_DIR.
set -euo pipefail
mode=$1
out=$2
image=$3
case "$mode" in crc) octet=10 ;; secure) octet=11 ;; *) echo "mode must be crc or secure" >&2; exit 2 ;; esac
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
mkdir -p "$out"
out=$(cd "$out" && pwd)
work=$(mktemp -d)
chmod 700 "$work"
subnet="10.233.$octet.0/24"
mon_ip="10.233.$octet.10"
net="msgrcap-$mode-$$"
mon="msgrcap-mon-$mode-$$"
tcpdump_pid=""
substitution="runner-host tcpdump on the fixture bridge (development substitute for the planned ceph-testcontainers capture helper)"

stop_capture() {
    if test -n "$tcpdump_pid"; then
        sudo kill -INT "$tcpdump_pid" 2>/dev/null || true
        wait "$tcpdump_pid" 2>/dev/null || true
        tcpdump_pid=""
    fi
}
cleanup() {
    code=$?
    stop_capture
    docker logs "$mon" > "$out/mon.log" 2>&1 || true
    result=ok
    docker rm -f "$mon" >/dev/null 2>&1 || result="container removal failed"
    docker network rm "$net" >/dev/null 2>&1 || result="$result; network removal failed"
    if docker ps -a --format '{{.Names}}' | grep -qx "$mon"; then result="container still present"; fi
    if docker network ls --format '{{.Name}}' | grep -qx "$net"; then result="$result; network still present"; fi
    rm -rf "$work"
    test -e "$work" && result="$result; key work directory still present"
    printf '{"exit_code":%d,"cleanup":"%s"}\n' "$code" "$result" > "$out/cleanup.json"
    if test -f "$out/capture.pcap"; then sudo chown "$(id -u):$(id -g)" "$out/capture.pcap"; fi
}
trap cleanup EXIT

echo "== build the development tool"
go -C "$here" build -mod=readonly -o "$work/msgrcap" .

echo "== start one disposable MON ($mode client sessions; server accepts crc and secure)"
docker network create --subnet "$subnet" "$net" >/dev/null
docker run -d --name "$mon" --network "$net" --ip "$mon_ip" \
    -e MSGRCAP_MON_IP="$mon_ip" -v "$here/mon-bootstrap.sh:/msgrcap-bootstrap.sh:ro" \
    --entrypoint /bin/bash "$image" /msgrcap-bootstrap.sh >/dev/null
ready() {
    for _ in $(seq 1 60); do
        if docker exec "$mon" timeout 5 ceph -n client.capture -k /etc/ceph/ceph.client.capture.keyring mon dump >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}
ready
# Native in-container readiness checks use the container's own address and do
# not cross the bridge, so they are absent from the capture.
docker exec "$mon" ceph-authtool -p -n client.capture /etc/ceph/ceph.client.capture.keyring > "$work/capture.key"

bridge="br-$(docker network inspect -f '{{.Id}}' "$net" | cut -c1-12)"
filter="host $mon_ip"
echo "== start capture on the fixture bridge"
sudo tcpdump -i "$bridge" -s 0 -U -n -w "$out/capture.pcap" "$filter" 2> "$out/tcpdump.stderr" &
tcpdump_pid=$!
for _ in $(seq 1 50); do
    grep -q "listening on" "$out/tcpdump.stderr" && break
    sleep 0.1
done
grep -q "listening on" "$out/tcpdump.stderr"
capture_started=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)

echo "== run the Go probe"
probe_rc=0
"$work/msgrcap" probe -mon "v2:$mon_ip:3300" -closed "$mon_ip:3301" -entity client.capture \
    -key-file "$work/capture.key" -mode "$mode" -trace "$out/probe-trace.json" \
    -restart-request "$work/restart.request" -restart-done "$work/restart.done" \
    -substitution "$substitution" > "$out/probe.stdout" 2>&1 &
probe_pid=$!
for _ in $(seq 1 600); do
    test -f "$work/restart.request" && break
    kill -0 "$probe_pid" 2>/dev/null || break
    sleep 0.1
done
restart_at=""
if test -f "$work/restart.request"; then
    restart_at=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)
    docker restart -t 5 "$mon" >/dev/null
    touch "$work/restart.done"
fi
wait "$probe_pid" || probe_rc=$?
sleep 1
stop_capture
capture_stopped=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)
sudo chown "$(id -u):$(id -g)" "$out/capture.pcap"
docker logs "$mon" > "$out/mon.log" 2>&1

echo "== analyze without any key"
"$work/msgrcap" analyze -pcap "$out/capture.pcap" -trace "$out/probe-trace.json" -json "$out/analysis.json" \
    -mon-log "$out/mon.log" -cross "$out/crosscheck.json" | tee "$out/summary.txt"
echo "== analyze with opt-in passive derivation from the disposable client key"
"$work/msgrcap" analyze -pcap "$out/capture.pcap" -trace "$out/probe-trace.json" -json "$out/analysis-derived.json" \
    -mon-log "$out/mon.log" -cross "$out/crosscheck-derived.json" \
    -entity client.capture -key-file "$work/capture.key" | tee "$out/summary-derived.txt"

echo "== evaluate the pinned tshark"
{
    tshark -v 2>&1 | head -n 1
    echo "-- registered protocols matching ceph:"
    tshark -G protocols 2>/dev/null | grep -i ceph || echo "(none)"
    echo "-- heuristic dissectors matching ceph:"
    tshark -G heuristic-decodes 2>/dev/null | grep -i ceph || echo "(none)"
} > "$out/tshark-version.txt"
tshark -r "$out/capture.pcap" -q -z conv,tcp > "$out/tshark-conv.txt" 2>&1 || true
tshark -r "$out/capture.pcap" -n -o tcp.desegment_tcp_streams:TRUE > "$out/tshark-default.txt" 2>&1 || true
# The Ceph dissector registers only a heuristic (ceph_tcp), enabled by default;
# it cannot be selected with decode-as on tcp.port.
tshark -r "$out/capture.pcap" -n -Y ceph -V > "$out/tshark-ceph-detail.txt" 2>&1 || true
tshark -r "$out/capture.pcap" -n --disable-heuristic ceph_tcp > "$out/tshark-no-ceph-heuristic.txt" 2>&1 || true
tshark -r "$out/capture.pcap" -n -q -z expert > "$out/tshark-expert.txt" 2>&1 || true
streams=$(tshark -r "$out/capture.pcap" -T fields -e tcp.stream 2>/dev/null | sort -n -u)
: > "$out/tshark-follow.txt"
for s in $streams; do
    tshark -r "$out/capture.pcap" -q -z "follow,tcp,raw,$s" >> "$out/tshark-follow.txt" 2>&1 || true
done
python3 "$here/tshark_compare.py" "$out/tshark-follow.txt" "$out/analysis.json" | tee "$out/tshark-compare.txt"

echo "== provenance"
image_digest=$(docker image inspect --format '{{join .RepoDigests ","}}' "$image")
ceph_version=$(docker run --rm --entrypoint ceph-mon "$image" --version 2>/dev/null | head -n 1)
key_type=$(base64 -d < "$work/capture.key" | od -An -tu2 -N2 | tr -d ' ')
python3 - "$out/provenance.json" <<PY
import json, sys
json.dump({
    "purpose": "development-only msgr2.1 capture PoC (issue #48)",
    "capture_substitution": "$substitution",
    "source_commit": "$(git -C "$root" rev-parse HEAD)",
    "workflow_run": "${GITHUB_SERVER_URL:-}/${GITHUB_REPOSITORY:-}/actions/runs/${GITHUB_RUN_ID:-}",
    "workflow_attempt": "${GITHUB_RUN_ATTEMPT:-}",
    "image": "$image",
    "image_repo_digests": "$image_digest",
    "ceph_version": "$ceph_version",
    "platform": "$(uname -srm)",
    "os": "$(. /etc/os-release && echo "$PRETTY_NAME")",
    "go": "$(go version)",
    "tcpdump": "$(tcpdump --version 2>&1 | head -n 1)",
    "libpcap": "$(tcpdump --version 2>&1 | sed -n 2p)",
    "capture_interface": "fixture bridge (runner host)",
    "capture_filter": "$filter",
    "snaplen": "0 (full packets)",
    "capture_started": "$capture_started",
    "capture_stopped": "$capture_stopped",
    "mon_restart_requested": "$restart_at",
    "connection_mode": "$mode",
    "server_modes": "crc secure (client offers only the selected mode)",
    "mon_endpoint": "v2:$mon_ip:3300",
    "closed_endpoint": "$mon_ip:3301",
    "client_entity": "client.capture (mon 'allow r')",
    "client_key_type": "$key_type",
    "probe_exit_code": $probe_rc,
    "tcpdump_stats": open("$out/tcpdump.stderr").read().strip().splitlines()[-3:],
}, open(sys.argv[1], "w"), indent=2)
PY
cat "$out/provenance.json"

echo "== check the PoC expectations"
# Every established MON session must pair with one native MON log connection
# and match its mode, received client tags and sent message sequences. CRC
# needs no key; secure needs the opt-in derived report.
python3 - "$mode" "$out" <<'PY' | tee "$out/expectations.txt"
import json, sys
mode, out = sys.argv[1], sys.argv[2]
problems = []
for name in (["crosscheck.json"] if mode == "crc" else []) + ["crosscheck-derived.json"]:
    rows = json.load(open(out + "/" + name))
    if len(rows) < 2:
        problems.append("%s: expected the command and after-restart sessions, got %d" % (name, len(rows)))
    for r in rows:
        if not (r["mode_equal"] and r["client_tags_equal"] and r["server_message_seqs_equal"]):
            problems.append("%s: connection %d differs from the MON log" % (name, r["capture_connection"]))
rep = json.load(open(out + "/analysis.json"))
phases = {c.get("trace_phase", "") for c in rep["connections"]}
for want in ("refused (failed dial)", "command", "after-restart"):
    if want not in phases:
        problems.append("analysis: no connection correlated to phase %r" % want)
if mode == "secure" and any(c.get("con_mode") == "secure" and
                            c["client_to_server"]["msgr2"].get("opaque_secure_bytes", 0) == 0
                            for c in rep["connections"] if c.get("con_mode")):
    problems.append("analysis without a key decoded secure frames")
print("\n".join(problems) or "all PoC expectations met")
sys.exit(1 if problems else 0)
PY
exit "$probe_rc"
