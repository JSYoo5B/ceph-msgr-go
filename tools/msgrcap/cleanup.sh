# Owned-resource cleanup for live-poc.sh, sourced by it and by the fault-injected
# regression in cleanup_test.go. Expects mon, net, work, ctl and out to be set,
# tcpdump_pid and provider_pid to name a running capture or be empty, and
# mon_id to hold the MON container ID once it exists.
#
# Cleanup failure, or an owned resource whose absence cannot be verified, fails
# the process. An earlier nonzero status is preserved.

stop_capture() {
    if test -n "$tcpdump_pid"; then
        sudo kill -INT "$tcpdump_pid" 2>/dev/null || true
        tcpdump_rc=0
        wait "$tcpdump_pid" 2>/dev/null || tcpdump_rc=$?
        tcpdump_pid=""
    fi
}

# stop_provider ends the running capture.Start session (one segment): its
# driver calls Session.Stop on SIGTERM and writes the segment report.
stop_provider() {
    if test -n "${provider_pid:-}"; then
        kill -TERM "$provider_pid" 2>/dev/null || true
        provider_rc=0
        wait "$provider_pid" 2>/dev/null || provider_rc=$?
        provider_pid=""
    fi
}

# owned_helpers lists capture helpers that joined the MON's namespace.
owned_helpers() {
    local ids id mode
    ids=$(docker ps -aq --filter label=org.testcontainers.ceph.role=capture-helper) || return 1
    for id in $ids; do
        mode=$(docker inspect -f '{{.HostConfig.NetworkMode}}' "$id" 2>/dev/null) || return 1
        if test "$mode" = "container:$mon_id"; then echo "$id"; fi
    done
}

cleanup() {
    code=$?
    set +e
    stop_provider
    stop_capture
    docker logs "$mon" > "$out/mon.log" 2>&1
    problems=""
    if test -n "${mon_id:-}"; then
        if helpers=$(owned_helpers); then
            for h in $helpers; do
                docker rm -f "$h" >/dev/null 2>&1 || problems="$problems; capture helper removal failed"
            done
            if helpers=$(owned_helpers); then
                if test -n "$helpers"; then problems="$problems; capture helper still present"; fi
            else
                problems="$problems; capture helper absence unverifiable"
            fi
        else
            problems="$problems; capture helper absence unverifiable"
        fi
    fi
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
    rm -rf "$work" "$ctl"
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
