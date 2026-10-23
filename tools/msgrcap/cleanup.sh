# Owned-resource cleanup for live-poc.sh, sourced by it and by the fault-injected
# regression in cleanup_test.go. Expects mon, net, work and out to be set and
# tcpdump_pid to name a running capture or be empty.
#
# Cleanup failure, or an owned resource whose absence cannot be verified, fails
# the process. An earlier nonzero status is preserved.

stop_capture() {
    if test -n "$tcpdump_pid"; then
        sudo kill -INT "$tcpdump_pid" 2>/dev/null || true
        wait "$tcpdump_pid" 2>/dev/null || true
        tcpdump_pid=""
    fi
}

cleanup() {
    code=$?
    set +e
    stop_capture
    docker logs "$mon" > "$out/mon.log" 2>&1
    problems=""
    docker rm -f "$mon" >/dev/null 2>&1 || problems="$problems; container removal failed"
    docker network rm "$net" >/dev/null 2>&1 || problems="$problems; network removal failed"
    if names=$(docker ps -a --format '{{.Names}}' 2>/dev/null); then
        if printf '%s\n' "$names" | grep -qx "$mon"; then problems="$problems; container still present"; fi
    else
        problems="$problems; container absence unverifiable"
    fi
    if names=$(docker network ls --format '{{.Name}}' 2>/dev/null); then
        if printf '%s\n' "$names" | grep -qx "$net"; then problems="$problems; network still present"; fi
    else
        problems="$problems; network absence unverifiable"
    fi
    rm -rf "$work"
    if test -e "$work"; then problems="$problems; key work directory still present"; fi
    result=ok
    if test -n "$problems"; then result=${problems#; }; fi
    if test -f "$out/capture.pcap"; then sudo chown "$(id -u):$(id -g)" "$out/capture.pcap"; fi
    final=$code
    if test "$final" -eq 0 && test "$result" != ok; then final=1; fi
    printf '{"exit_code":%d,"cleanup":"%s","final_exit_code":%d}\n' "$code" "$result" "$final" > "$out/cleanup.json"
    if test "$result" != ok; then echo "owned-resource cleanup failed: $result" >&2; fi
    exit "$final"
}
