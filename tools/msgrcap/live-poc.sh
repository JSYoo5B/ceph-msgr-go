#!/bin/bash
# Development-only live capture PoC for one disposable Tentacle MON.
# Usage: live-poc.sh crc|secure OUTPUT_DIR CEPH_IMAGE CAPTURE_IMAGE
#
# Runs on a GitHub Actions Linux runner (Docker, sudo tcpdump, tshark). One run
# records the same traffic twice:
# - reference: runner-host tcpdump on the fixture bridge, one continuous file;
# - provider: ceph-testcontainers-go v0.5.0 capture.Start with CAPTURE_IMAGE
#   (pinned by digest) in the MON's network namespace. A session is bound to
#   one container generation, so the MON restart splits it into two segments
#   with one declared gap (see README.md, "Provider capture").
# Both are analyzed, and a parity step compares the analyses. PRIMARY below
# selects which capture the top-level reports and the expectation gate use.
# The disposable client key stays in a private work directory and is never
# written to OUTPUT_DIR.
set -euo pipefail
mode=$1
out=$2
image=$3
capture_image=$4
case "$mode" in crc) octet=10 ;; secure) octet=11 ;; *) echo "mode must be crc or secure" >&2; exit 2 ;; esac
case "$capture_image" in *@sha256:*) ;; *) echo "the capture helper image must be pinned by digest" >&2; exit 2 ;; esac
# The provider capture is the primary evidence: its analyses had full parity
# with the runner-host reference in both modes (CAPTURE_ANALYSIS.md, "Provider
# capture parity"). Every run must show full parity again (-require-full);
# the reference stays as the checked comparison.
primary=provider
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
mkdir -p "$out/reference" "$out/provider"
out=$(cd "$out" && pwd)
work=$(mktemp -d)
chmod 700 "$work"
ctl=$(mktemp -d)
chmod 755 "$ctl"
subnet="10.233.$octet.0/24"
mon_ip="10.233.$octet.10"
net="msgrcap-$mode-$$"
mon="msgrcap-mon-$mode-$$"
mon_id=""
tcpdump_pid=""
tcpdump_rc=""
provider_pid=""
provider_rc=""
segment=0
reference_label="runner-host tcpdump on the fixture bridge (one continuous file)"
provider_label="ceph-testcontainers-go capture.Start in the MON network namespace, one segment per MON container generation"
# Capture helpers are removed by Session.Stop and verified by cleanup.sh; the
# testcontainers reaper is not used.
export TESTCONTAINERS_RYUK_DISABLED=true

# shellcheck source=cleanup.sh
. "$here/cleanup.sh"
trap cleanup EXIT

now() { date -u +%Y-%m-%dT%H:%M:%S.%NZ; }

echo "== build the development tools"
go -C "$here" build -mod=readonly -o "$work/msgrcap" .
go -C "$here/providercap" build -mod=readonly -o "$work/providercap" .

echo "== start one disposable MON ($mode client sessions; server accepts crc and secure)"
touch "$ctl/gate"
docker network create --subnet "$subnet" "$net" >/dev/null
docker run -d --name "$mon" --network "$net" --ip "$mon_ip" \
    -e MSGRCAP_MON_IP="$mon_ip" -v "$here/mon-bootstrap.sh:/msgrcap-bootstrap.sh:ro" -v "$ctl:/msgrcap-ctl" \
    --entrypoint /bin/bash "$image" /msgrcap-bootstrap.sh >/dev/null
mon_id=$(docker inspect -f '{{.Id}}' "$mon")
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
# not cross the bridge, so they are absent from both captures.
docker exec "$mon" ceph-authtool -p -n client.capture /etc/ceph/ceph.client.capture.keyring > "$work/capture.key"

bridge="br-$(docker network inspect -f '{{.Id}}' "$net" | cut -c1-12)"
filter="host $mon_ip"

# start_segment starts the next provider capture segment on the MON's current
# generation and returns once capture.Start has returned (tcpdump listens).
start_segment() {
    segment=$((segment + 1))
    "$work/providercap" -container "$mon_id" -network "$net" -interface eth0 -filter "$filter" \
        -image "$capture_image" -output "$out/provider/segment-$segment.pcap" \
        -report "$out/provider/segment-$segment.json" -ready "$work/segment-$segment.ready" \
        > "$out/provider/segment-$segment.log" 2>&1 &
    provider_pid=$!
    for _ in $(seq 1 1800); do
        test -f "$work/segment-$segment.ready" && return 0
        kill -0 "$provider_pid" 2>/dev/null || break
        sleep 0.1
    done
    echo "provider capture segment $segment did not become ready" >&2
    cat "$out/provider/segment-$segment.log" >&2
    return 1
}

# end_segment stops the running segment; Session.Stop must succeed.
end_segment() {
    stop_provider
    if test "$provider_rc" != 0; then
        echo "provider capture segment $segment did not stop cleanly (status $provider_rc)" >&2
        cat "$out/provider/segment-$segment.log" >&2
        return 1
    fi
}

echo "== start the reference capture on the fixture bridge"
sudo tcpdump -i "$bridge" -s 0 -U -n -w "$out/capture.pcap" "$filter" 2> "$out/tcpdump.stderr" &
tcpdump_pid=$!
for _ in $(seq 1 50); do
    grep -q "listening on" "$out/tcpdump.stderr" && break
    sleep 0.1
done
grep -q "listening on" "$out/tcpdump.stderr"
capture_started=$(now)

echo "== start provider capture segment 1"
start_segment

echo "== run the Go probe"
probe_rc=0
"$work/msgrcap" probe -mon "v2:$mon_ip:3300" -closed "$mon_ip:3301" -entity client.capture \
    -key-file "$work/capture.key" -mode "$mode" -trace "$out/probe-trace.json" \
    -restart-request "$work/restart.request" -restart-done "$work/restart.done" -dial-hold "$work/dial-hold" \
    -substitution "reference: $reference_label; provider: $provider_label" > "$out/probe.stdout" 2>&1 &
probe_pid=$!
for _ in $(seq 1 600); do
    test -f "$work/restart.request" && break
    kill -0 "$probe_pid" 2>/dev/null || break
    sleep 0.1
done
restart_at=""
daemon_exited_at=""
gap_from=""
gap_to=""
if test -f "$work/restart.request"; then
    restart_at=$(now)
    # Stop the MON daemon; its container and network namespace stay up, so
    # the peer close and the refused redials are inside segment 1.
    docker kill -s TERM "$mon" >/dev/null
    for _ in $(seq 1 600); do
        test -f "$ctl/daemon-exited" && break
        sleep 0.1
    done
    test -f "$ctl/daemon-exited"
    daemon_exited_at=$(now)
    sleep 1
    # From here until segment 2 is ready, the probe holds new dials locally,
    # so no client packet is sent while no segment is capturing.
    touch "$work/dial-hold"
    sleep 0.5
    gap_from=$(now)
    end_segment
    rm -f "$ctl/gate" "$ctl/daemon-exited"
    docker restart -t 5 "$mon" >/dev/null
    for _ in $(seq 1 100); do
        test "$(docker inspect -f '{{.State.Running}}' "$mon")" = true && break
        sleep 0.1
    done
    echo "== start provider capture segment 2 on the restarted generation"
    start_segment
    gap_to=$(now)
    touch "$ctl/gate"
    rm -f "$work/dial-hold"
    touch "$work/restart.done"
fi
wait "$probe_pid" || probe_rc=$?
sleep 1
capture_stop_requested=$(now)
end_segment
stop_capture
capture_stopped=$(now)
sudo chown "$(id -u):$(id -g)" "$out/capture.pcap"
docker logs "$mon" > "$out/mon.log" 2>&1

echo "== write the capture manifests"
python3 - "$out" "$capture_started" "$capture_stop_requested" "$capture_stopped" "$tcpdump_rc" "$filter" \
    "$reference_label" "$provider_label" "$segment" <<'PY'
import hashlib, json, re, sys
out, ready, stop, ended, rc, filt, ref_label, prov_label, segments = sys.argv[1:]
stderr = open(out + "/tcpdump.stderr").read()
stats = {}
for n, what in re.findall(r"(?m)^(\d+) packets? (captured|received by filter|dropped by kernel|dropped by interface)\s*$", stderr):
    stats[what.replace(" ", "_")] = int(n)
digest = hashlib.sha256(open(out + "/capture.pcap", "rb").read()).hexdigest()
json.dump({
    "interface": "fixture bridge (runner host)", "filter": filt,
    "ready_at": ready, "stop_requested_at": stop, "ended_at": ended,
    "end": "stopped" if rc == "0" else "failed", "exit_code": int(rc or -1),
    "target": "not-bound", "statistics": stats, "retrieved": True, "retrieved_sha256": digest,
}, open(out + "/reference/segment.json", "w"), indent=2)
json.dump({"source": ref_label, "segments": [{"file": "../capture.pcap", "report": "segment.json"}], "gaps": []},
          open(out + "/reference/manifest.json", "w"), indent=2)
n = int(segments)
gaps = []
if n == 2:
    gaps.append({"after_segment": 1, "reason": "MON container restart: a capture.Start session is bound to one container "
                 "generation; the daemon had exited, and the probe held new dials until segment 2 was ready"})
json.dump({"source": prov_label,
           "segments": [{"file": "segment-%d.pcap" % i, "report": "segment-%d.json" % i} for i in range(1, n + 1)],
           "gaps": gaps}, open(out + "/provider/manifest.json", "w"), indent=2)
PY

analyze_set() { # NAME DIR: analyze one capture with and without the key
    local name=$1 dir=$2
    echo "== analyze the $name capture without any key"
    "$work/msgrcap" analyze -segments "$dir/manifest.json" -trace "$out/probe-trace.json" -json "$dir/analysis.json" \
        -mon-log "$out/mon.log" -cross "$dir/crosscheck.json" | tee "$dir/summary.txt"
    echo "== analyze the $name capture with opt-in passive derivation from the disposable client key"
    "$work/msgrcap" analyze -segments "$dir/manifest.json" -trace "$out/probe-trace.json" -json "$dir/analysis-derived.json" \
        -mon-log "$out/mon.log" -cross "$dir/crosscheck-derived.json" \
        -entity client.capture -key-file "$work/capture.key" | tee "$dir/summary-derived.txt"
}
analyze_set reference "$out/reference"
analyze_set provider "$out/provider"

echo "== compare the provider analyses with the reference"
require_full=()
if test "$primary" = provider; then require_full=(-require-full); fi
for kind in "" "-derived"; do
    "$work/msgrcap" parity -reference "$out/reference/analysis$kind.json" -candidate "$out/provider/analysis$kind.json" \
        -reference-cross "$out/reference/crosscheck$kind.json" -candidate-cross "$out/provider/crosscheck$kind.json" \
        -json "$out/parity$kind.json" "${require_full[@]}" | tee "$out/parity$kind.txt"
done
for f in analysis.json analysis-derived.json crosscheck.json crosscheck-derived.json summary.txt summary-derived.txt; do
    cp "$out/$primary/$f" "$out/$f"
done

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
follow() { # PCAP: append tshark's raw TCP follow output of every stream
    local s streams
    streams=$(tshark -r "$1" -Y tcp -T fields -e tcp.stream 2>/dev/null | sort -n | uniq)
    for s in $streams; do
        tshark -r "$1" -q -z "follow,tcp,raw,$s" 2>&1 || true
    done
}
follow "$out/capture.pcap" > "$out/tshark-follow.txt"
python3 "$here/tshark_compare.py" "$out/tshark-follow.txt" "$out/reference/analysis.json" | tee "$out/tshark-compare.txt"
# No connection spans the provider gap, so the segments' streams together
# must cover every connection of the provider analysis.
: > "$out/provider/tshark-follow.txt"
for i in $(seq 1 "$segment"); do
    follow "$out/provider/segment-$i.pcap" >> "$out/provider/tshark-follow.txt"
done
python3 "$here/tshark_compare.py" "$out/provider/tshark-follow.txt" "$out/provider/analysis.json" | tee "$out/provider/tshark-compare.txt"

echo "== provenance"
image_digest=$(docker image inspect --format '{{join .RepoDigests ","}}' "$image")
ceph_version=$(docker run --rm --entrypoint ceph-mon "$image" --version 2>/dev/null | head -n 1)
key_type=$(base64 -d < "$work/capture.key" | od -An -tu2 -N2 | tr -d ' ')
provider_module=$(go -C "$here/providercap" list -m -f '{{.Path}} {{.Version}}' github.com/JSYoo5B/ceph-testcontainers-go)
python3 - "$out/provenance.json" <<PY
import json, sys
segments = [json.load(open("$out/provider/segment-%d.json" % i)) for i in range(1, $segment + 1)]
json.dump({
    "purpose": "development-only msgr2.1 capture PoC (issue #48)",
    "primary_capture": "$primary",
    "reference_capture": "$reference_label",
    "provider_capture": "$provider_label",
    "capture_substitution": "runner-host tcpdump reference" if "$primary" == "reference" else "none: the primary capture is the provider capture",
    "provider_module": "$provider_module",
    "capture_helper_image": "$capture_image",
    "capture_helper_image_ids": sorted({s["helper_image_id"] for s in segments}),
    "capture_helper_platforms": sorted({s.get("helper_platform", "") for s in segments}),
    "capture_helper_tool_versions": sorted({s.get("tool_version", "") for s in segments}),
    "provider_segments": [{k: s.get(k) for k in ("container_id", "target_started_at", "sandbox_id", "interface", "filter",
                           "snaplen", "max_packets", "ready_at", "stop_requested_at", "ended_at", "end", "target",
                           "statistics", "retrieved_sha256", "file", "helper_removed")} for s in segments],
    "parity": json.load(open("$out/parity.json"))["verdict"],
    "parity_derived": json.load(open("$out/parity-derived.json"))["verdict"],
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
    "capture_interface": "fixture bridge (runner host); provider: MON eth0 in its network namespace",
    "capture_filter": "$filter",
    "snaplen": "0 (full packets, 262144)",
    "capture_started": "$capture_started",
    "capture_stopped": "$capture_stopped",
    "mon_restart_requested": "$restart_at",
    "mon_daemon_exited": "$daemon_exited_at",
    "provider_gap_observed": ["$gap_from", "$gap_to"],
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

echo "== check the PoC expectations on the $primary capture"
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
for want in ("refused (failed dial)", "command"):
    if want not in phases:
        problems.append("analysis: no connection correlated to phase %r" % want)
# The client's own recovery may dial before the workflow reports that the
# restart finished, so the reconnect is identified by the trace connection
# order, not by its phase label.
if not any(c.get("trace_phase") and c["trace_phase"] != "command" and "failed dial" not in c["trace_phase"]
           and c.get("con_mode") == mode for c in rep["connections"]):
    problems.append("analysis: no authenticated reconnect after the MON restart")
if not any(c["phase"] == "after-restart" and not c.get("error") for c in rep.get("trace_commands", [])):
    problems.append("trace: no successful command after the MON restart")
if mode == "secure" and any(c.get("con_mode") == "secure" and
                            c["client_to_server"]["msgr2"].get("opaque_secure_bytes", 0) == 0
                            for c in rep["connections"] if c.get("con_mode")):
    problems.append("analysis without a key decoded secure frames")
cov = rep["capture_coverage"]
if cov["lost_packets"]:
    problems.append("capture: %d packets dropped" % cov["lost_packets"])
in_gaps = (cov.get("trace") or {}).get("activity_in_gaps")
if in_gaps:
    problems.append("trace activity inside a capture gap: %s" % in_gaps)
if any(c.get("capture_gaps") for c in rep["connections"]):
    problems.append("analysis: a connection may have been interrupted by a capture gap")
print("\n".join(problems) or "all PoC expectations met")
sys.exit(1 if problems else 0)
PY
exit "$probe_rc"
