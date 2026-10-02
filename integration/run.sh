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
case "$ip_family" in
    4) monitors=127.0.0.1:33300,127.0.0.1:33301,127.0.0.1:33302 ;;
    6) monitors='[::1]:33300,[::1]:33301,[::1]:33302' ;;
    *) exit 2 ;;
esac
out=$(mktemp -d "${TMPDIR:-/tmp}/ceph-msgr-integration.XXXXXX")
container="ceph-msgr-go-test-$$"
cleanup() {
    docker rm -f "$container" > /dev/null 2>&1 || true
    rm -rf "$out"
}
failure_diagnostics() {
    if test -n "$diagnostics"; then
        mkdir -p "$diagnostics"
        # Copy daemon text logs and crash metadata only, never keyrings or
        # process memory. The caller chooses the development output directory.
        for log in mon.a.log mon.b.log mon.c.log mgr.a.log mgr.b.log; do
            docker cp "$container:/tmp/ceph-msgr-test/$log" "$diagnostics/$log" > /dev/null 2>&1 || true
        done
        # docker cp also works after the container exits. Select metadata
        # from the archive stream without saving any other crash files.
        docker cp "$container:/var/lib/ceph/crash" - 2>/dev/null | python3 "$project_root/tools/collect_crash_metadata.py" > "$diagnostics/crash-metadata.jsonl" || true
        cat "$diagnostics/crash-metadata.jsonl"
        for log in mon.a.log mon.b.log mon.c.log mgr.a.log mgr.b.log; do
            if test -f "$diagnostics/$log"; then
                tail -n 80 "$diagnostics/$log"
            fi
        done
        return
    fi
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
docker run -d --name "$container" --label ceph-msgr-go.integration=true --entrypoint /bin/sh "$@" -e CEPH_MSGR_TEST_KEY_TYPE="$key_type" -e CEPH_MSGR_TEST_SERVICE_CIPHER="$service_cipher" -e CEPH_MSGR_TEST_IP_FAMILY="$ip_family" -e CEPH_MSGR_TEST_MGR_COUNT="$mgr_count" -e CEPH_MSGR_TEST_IDLE_SESSIONS="$idle_sessions" -v "$out:/out" "$image" "$cluster_script" > /dev/null
for attempt in $(seq 1 120); do
    if test -f "$out/ready"; then
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
                if CEPH_MSGR_MONITORS="$monitors" CEPH_MSGR_KEY_FILE="$out/key" CEPH_MSGR_IDENTITY=client.test CEPH_MSGR_FSID=80bbab73-69c1-4a0c-a746-4271357750b8 CEPH_MSGR_CONTROL_DIR="$out" CEPH_MSGR_TEST_SERVICE_CIPHER="$service_cipher" CEPH_MSGR_TEST_MGR_COUNT="$mgr_count" CEPH_MSGR_TEST_EXPIRE_TICKETS="$expire_tickets" CEPH_MSGR_TEST_IDLE_SESSIONS="$idle_sessions" CEPH_MSGR_STRESS_DURATION="${CEPH_MSGR_STRESS_DURATION:-}" CEPH_MSGR_TEST_PROXY="$proxy" "$out/$suite.test" -test.run "$test_run" -test.v -test.timeout "${CEPH_MSGR_TEST_TIMEOUT:-10m}"; then
                    result=0
                else
                    result=$?
                fi
            elif docker exec -e CEPH_MSGR_MONITORS="$monitors" -e CEPH_MSGR_KEY_FILE=/out/key -e CEPH_MSGR_IDENTITY=client.test -e CEPH_MSGR_FSID=80bbab73-69c1-4a0c-a746-4271357750b8 -e CEPH_MSGR_CONTROL_DIR=/out -e CEPH_MSGR_TEST_SERVICE_CIPHER="$service_cipher" -e CEPH_MSGR_TEST_MGR_COUNT="$mgr_count" -e CEPH_MSGR_TEST_EXPIRE_TICKETS="$expire_tickets" -e CEPH_MSGR_TEST_IDLE_SESSIONS="$idle_sessions" -e CEPH_MSGR_STRESS_DURATION="${CEPH_MSGR_STRESS_DURATION:-}" "$container" "/out/$suite.test" -test.run "$test_run" -test.v -test.timeout "${CEPH_MSGR_TEST_TIMEOUT:-10m}"; then
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
