#!/bin/sh
# All Ceph executables and faults stay in a disposable test container.
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
image=${CEPH_MSGR_TEST_IMAGE:-quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9}
key_type=${CEPH_MSGR_TEST_KEY_TYPE:-aes256k}
service_cipher=${CEPH_MSGR_TEST_SERVICE_CIPHER:-$key_type}
ip_family=${CEPH_MSGR_TEST_IP_FAMILY:-4}
mgr_count=${CEPH_MSGR_TEST_MGR_COUNT:-2}
runtime=${CEPH_MSGR_TEST_RUNTIME:-container}
case "$runtime" in container|host) ;; *) exit 2 ;; esac
race=${CEPH_MSGR_TEST_RACE:-0}
case "$race" in 0|1) ;; *) exit 2 ;; esac
if test "$race" = 1 && test "$runtime" != host; then
    echo 'Development race instrumentation requires CEPH_MSGR_TEST_RUNTIME=host.' >&2
    exit 2
fi
diagnostics=${CEPH_MSGR_TEST_DIAGNOSTICS:-}
expire_tickets=${CEPH_MSGR_TEST_EXPIRE_TICKETS:-0}
case "$expire_tickets" in 0|1) ;; *) exit 2 ;; esac
idle_sessions=${CEPH_MSGR_TEST_IDLE_SESSIONS:-0}
case "$idle_sessions" in 0|1) ;; *) exit 2 ;; esac
auth_epoch=${CEPH_MSGR_TEST_AUTH_EPOCH:-0}
case "$auth_epoch" in 0|1) ;; *) exit 2 ;; esac
short_tickets=${CEPH_MSGR_TEST_SHORT_TICKETS:-0}
case "$short_tickets" in 0|1) ;; *) exit 2 ;; esac
mode_rejection=${CEPH_MSGR_TEST_MODE_REJECTION:-}
case "$mode_rejection" in ''|mon|mgr) ;; *) exit 2 ;; esac
mapped_ipv6=${CEPH_MSGR_TEST_MAPPED_IPV6:-0}
case "$mapped_ipv6" in 0|1) ;; *) exit 2 ;; esac
case "$mgr_count" in
    0) test_run=${CEPH_MSGR_TEST_RUN:-^TestCephManagerAvailabilityIntegration$} ;;
    2) test_run=${CEPH_MSGR_TEST_RUN:-^TestCeph.*Integration$} ;;
    *) exit 2 ;;
esac
if test "$expire_tickets" = 1; then
    test "$mgr_count" = 2 || exit 2
    # Losing the rotating proof can also strand Ceph daemons. Give this
    # destructive authentication scenario its own short-lived fixture.
    test_run=${CEPH_MSGR_TEST_RUN:-'^TestCeph(ExpiredTicketRecovery|DiscardedTicketProof)Integration$'}
fi
if test "$idle_sessions" = 1; then
    test "$mgr_count" = 2 && test "$expire_tickets" = 0 || exit 2
    # Keep this long-ticket scenario separate from renewal/expiration tests
    # whose deadlines rely on the ordinary 12-second fixture lifetime.
    test_run=${CEPH_MSGR_TEST_RUN:-'^TestCephIdleSessionsIntegration$'}
fi
if test "$auth_epoch" = 1; then
    test "$mgr_count" = 2 && test "$expire_tickets" = 0 && test "$idle_sessions" = 0 || exit 2
    # Service-key disposal changes daemon authentication too. Verify it in a
    # separate long-ticket fixture, before scheduled client renewal is due.
    test_run=${CEPH_MSGR_TEST_RUN:-'^TestCephServiceKeyEpochIntegration$'}
fi
if test "$short_tickets" = 1; then
    test "$mgr_count" = 2 && test "$expire_tickets" = 0 && test "$idle_sessions" = 0 && test "$auth_epoch" = 0 || exit 2
    # Fractional lifetimes also affect daemon credentials. Keep the deadline
    # regression isolated from the ordinary recovery and mutation scenarios.
    test_run=${CEPH_MSGR_TEST_RUN:-'^TestCephFractionalTicketRenewalIntegration$'}
fi
if test -n "$mode_rejection"; then
    test "$mgr_count" = 2 && test "$expire_tickets" = 0 && test "$idle_sessions" = 0 && test "$auth_epoch" = 0 && test "$short_tickets" = 0 || exit 2
    # Only the independent fixture clients may use CRC. The native product
    # must reject these listeners rather than fall back from secure mode.
    test_run=${CEPH_MSGR_TEST_RUN:-'^TestCephConnectionModeRejectionIntegration$'}
fi
if test "$mapped_ipv6" = 1; then
    test "$ip_family" = 6 && test "$runtime" = container && test "$mgr_count" = 2 || exit 2
    test "$expire_tickets" = 0 && test "$idle_sessions" = 0 && test "$auth_epoch" = 0 && test "$short_tickets" = 0 && test -z "$mode_rejection" && test -z "${CEPH_MSGR_STRESS_DURATION:-}" || exit 2
    # Physical IPv4 routing and authoritative AF_INET6 identities differ in
    # this isolated fixture; ordinary DNS and recovery scenarios run separately.
    test_run=${CEPH_MSGR_TEST_RUN:-'^TestCephMappedAddressIntegration$'}
fi
case "$ip_family" in
    4) monitors=127.0.0.1:33300,127.0.0.1:33301,127.0.0.1:33302 ;;
    6) monitors='[::1]:33300,[::1]:33301,[::1]:33302' ;;
    *) exit 2 ;;
esac
if test "$mapped_ipv6" = 1; then
    # Ceph accepts mapped literals in hex input but renders dotted JSON output.
    monitors='[::ffff:7f00:1]:33300,[::ffff:7f00:1]:33301,[::ffff:7f00:1]:33302'
fi
out=$(mktemp -d "${TMPDIR:-/tmp}/ceph-msgr-integration.XXXXXX")
container="ceph-msgr-go-test-$$"
cleanup() {
    docker rm -f "$container" > /dev/null 2>&1 || true
    rm -rf "$out"
}
save_mapped_metadata() {
    test "$mapped_ipv6" = 1 && test -n "$diagnostics" || return 0
    mkdir -p "$diagnostics"
    for name in status mon mgr pg summary; do
        file="mapped-oracle-$name.json"
        if test -f "$out/$file"; then
            cp "$out/$file" "$diagnostics/$file"
        fi
    done
}
failure_diagnostics() {
    printf 'Ceph fixture failure diagnostics:\n'
    save_mapped_metadata
    if test -n "$diagnostics"; then
        mkdir -p "$diagnostics"
        # Copy daemon/probe text logs and crash metadata only, never keyrings or
        # process memory. The caller chooses the development output directory.
        for log in mon.a.log mon.b.log mon.c.log mgr.a.log mgr.b.log service-keys-probe.json service-keys-probe.log mode-probe.json mode-probe.log; do
            docker cp "$container:/tmp/ceph-msgr-test/$log" "$diagnostics/$log" > /dev/null 2>&1 || true
        done
        # docker cp also works after the container exits. Select metadata
        # from the archive stream without saving any other crash files.
        docker cp "$container:/var/lib/ceph/crash" - 2>/dev/null | python3 "$project_root/tools/collect_crash_metadata.py" > "$diagnostics/crash-metadata.jsonl" || true
        cat "$diagnostics/crash-metadata.jsonl"
        for log in mon.a.log mon.b.log mon.c.log mgr.a.log mgr.b.log service-keys-probe.json service-keys-probe.log mode-probe.json mode-probe.log; do
            if test -f "$diagnostics/$log"; then
                tail -n 80 "$diagnostics/$log"
            fi
        done
        return
    fi
    # The gate exits the fixture on failure; its stdout remains readable even
    # when docker exec can no longer collect logs from the stopped container.
    docker logs --tail 80 "$container" || true
    docker exec "$container" sh -c 'for file in /var/lib/ceph/crash/*/meta; do test -f "$file" || continue; cat "$file"; done; tail -n 80 /tmp/ceph-msgr-test/mon.*.log /tmp/ceph-msgr-test/mgr.*.log' || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if ! docker image inspect "$image" > /dev/null 2>&1; then
    docker pull "$image"
fi
arch=$(docker image inspect --format '{{.Architecture}}' "$image")
cd "$project_root"
if test "$runtime" = host; then
    test_cgo=0
    set --
    if test "$race" = 1; then
        # Race instrumentation is a development dependency. Default fixture
        # binaries and the product still build with CGO=0.
        test_cgo=1
        set -- -race
    fi
    CGO_ENABLED=$test_cgo GOOS=$(go env GOHOSTOS) GOARCH=$(go env GOHOSTARCH) go test "$@" -c -o "$out/client.test" ./cephmsgr
    CGO_ENABLED=$test_cgo GOOS=$(go env GOHOSTOS) GOARCH=$(go env GOHOSTARCH) go test "$@" -c -o "$out/api.test" ./integration
    CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -o "$out/relay" ./integration/relay
    cp "$project_root/integration/host-cluster.sh" "$out/host-cluster.sh"
    set -- -p 127.0.0.1::40000
    cluster_script=/out/host-cluster.sh
else
    CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go test -c -o "$out/client.test" ./cephmsgr
    CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go test -c -o "$out/api.test" ./integration
    set --
    cluster_script=/out/cluster.sh
fi
# Keep a running long test independent of edits in the shared checkout.
cp "$project_root/integration/cluster.sh" "$out/cluster.sh"
if test "$mapped_ipv6" = 1; then
    cp "$project_root/tools/mapped_address_oracle.py" "$out/mapped_address_oracle.py"
fi
docker run -d --name "$container" --label ceph-msgr-go.integration=true --entrypoint /bin/sh "$@" -e CEPH_MSGR_TEST_KEY_TYPE="$key_type" -e CEPH_MSGR_TEST_SERVICE_CIPHER="$service_cipher" -e CEPH_MSGR_TEST_IP_FAMILY="$ip_family" -e CEPH_MSGR_TEST_MGR_COUNT="$mgr_count" -e CEPH_MSGR_TEST_IDLE_SESSIONS="$idle_sessions" -e CEPH_MSGR_TEST_AUTH_EPOCH="$auth_epoch" -e CEPH_MSGR_TEST_SHORT_TICKETS="$short_tickets" -e CEPH_MSGR_TEST_MODE_REJECTION="$mode_rejection" -e CEPH_MSGR_TEST_MAPPED_IPV6="$mapped_ipv6" -v "$out:/out" "$image" "$cluster_script" > /dev/null
for attempt in $(seq 1 120); do
    if test -f "$out/ready"; then
        save_mapped_metadata
        # Keep fixture mutations serial. Run the public API suite before the
        # client package suite, whose final recovery test leaves MON a down.
        for suite in api client; do
            printf 'Running Ceph test suite: %s\n' "$suite"
            if test "$runtime" = host; then
                if ! test -f "$out/relay.ready"; then
                    cat "$out/relay.log"
                    failure_diagnostics
                    exit 1
                fi
                proxy=$(docker port "$container" 40000/tcp)
                if CEPH_MSGR_MONITORS="$monitors" CEPH_MSGR_KEY_FILE="$out/key" CEPH_MSGR_IDENTITY=client.test CEPH_MSGR_FSID=80bbab73-69c1-4a0c-a746-4271357750b8 CEPH_MSGR_CONTROL_DIR="$out" CEPH_MSGR_TEST_SERVICE_CIPHER="$service_cipher" CEPH_MSGR_TEST_MGR_COUNT="$mgr_count" CEPH_MSGR_TEST_EXPIRE_TICKETS="$expire_tickets" CEPH_MSGR_TEST_IDLE_SESSIONS="$idle_sessions" CEPH_MSGR_TEST_AUTH_EPOCH="$auth_epoch" CEPH_MSGR_TEST_SHORT_TICKETS="$short_tickets" CEPH_MSGR_TEST_MODE_REJECTION="$mode_rejection" CEPH_MSGR_TEST_MAPPED_IPV6="$mapped_ipv6" CEPH_MSGR_STRESS_DURATION="${CEPH_MSGR_STRESS_DURATION:-}" CEPH_MSGR_TEST_PROXY="$proxy" "$out/$suite.test" -test.run "$test_run" -test.v -test.timeout "${CEPH_MSGR_TEST_TIMEOUT:-10m}"; then
                    result=0
                else
                    result=$?
                fi
            elif docker exec -e CEPH_MSGR_MONITORS="$monitors" -e CEPH_MSGR_KEY_FILE=/out/key -e CEPH_MSGR_IDENTITY=client.test -e CEPH_MSGR_FSID=80bbab73-69c1-4a0c-a746-4271357750b8 -e CEPH_MSGR_CONTROL_DIR=/out -e CEPH_MSGR_TEST_SERVICE_CIPHER="$service_cipher" -e CEPH_MSGR_TEST_MGR_COUNT="$mgr_count" -e CEPH_MSGR_TEST_EXPIRE_TICKETS="$expire_tickets" -e CEPH_MSGR_TEST_IDLE_SESSIONS="$idle_sessions" -e CEPH_MSGR_TEST_AUTH_EPOCH="$auth_epoch" -e CEPH_MSGR_TEST_SHORT_TICKETS="$short_tickets" -e CEPH_MSGR_TEST_MODE_REJECTION="$mode_rejection" -e CEPH_MSGR_TEST_MAPPED_IPV6="$mapped_ipv6" -e CEPH_MSGR_STRESS_DURATION="${CEPH_MSGR_STRESS_DURATION:-}" "$container" "/out/$suite.test" -test.run "$test_run" -test.v -test.timeout "${CEPH_MSGR_TEST_TIMEOUT:-10m}"; then
                result=0
            else
                result=$?
            fi
            if test "$result" -ne 0; then
                failure_diagnostics
                exit "$result"
            fi
        done
        exit 0
    fi
    if test "$(docker inspect --format '{{.State.Running}}' "$container")" != true; then
        break
    fi
    sleep 1
done
docker logs --tail 50 "$container"
failure_diagnostics
exit 1
