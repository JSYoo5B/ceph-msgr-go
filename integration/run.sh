#!/bin/sh
# All Ceph executables and faults stay in a disposable test container.
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
image=${CEPH_MSGR_TEST_IMAGE:-quay.io/ceph/ceph:v20.2.4}
key_type=${CEPH_MSGR_TEST_KEY_TYPE:-aes256k}
service_cipher=${CEPH_MSGR_TEST_SERVICE_CIPHER:-$key_type}
ip_family=${CEPH_MSGR_TEST_IP_FAMILY:-4}
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
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if ! docker image inspect "$image" > /dev/null 2>&1; then
    docker pull "$image"
fi
arch=$(docker image inspect --format '{{.Architecture}}' "$image")
cd "$project_root"
CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go test -c -o "$out/client.test" .
# Keep a running long test independent of edits in the shared checkout.
cp "$project_root/integration/cluster.sh" "$out/cluster.sh"
docker run -d --name "$container" --label ceph-msgr-go.integration=true --entrypoint /bin/sh -e CEPH_MSGR_TEST_KEY_TYPE="$key_type" -e CEPH_MSGR_TEST_SERVICE_CIPHER="$service_cipher" -e CEPH_MSGR_TEST_IP_FAMILY="$ip_family" -v "$out:/out" "$image" /out/cluster.sh > /dev/null
for attempt in $(seq 1 120); do
    if test -f "$out/ready"; then
        if docker exec -e CEPH_MSGR_MONITORS="$monitors" -e CEPH_MSGR_KEY_FILE=/out/key -e CEPH_MSGR_IDENTITY=client.test -e CEPH_MSGR_FSID=80bbab73-69c1-4a0c-a746-4271357750b8 -e CEPH_MSGR_CONTROL_DIR=/out -e CEPH_MSGR_TEST_SERVICE_CIPHER="$service_cipher" -e CEPH_MSGR_STRESS_DURATION="${CEPH_MSGR_STRESS_DURATION:-}" "$container" /out/client.test -test.run "${CEPH_MSGR_TEST_RUN:-^TestCeph.*Integration$}" -test.v -test.timeout "${CEPH_MSGR_TEST_TIMEOUT:-10m}"; then
            exit 0
        else
            result=$?
            docker exec "$container" sh -c 'tail -n 25 /tmp/ceph-msgr-test/mon.*.log /tmp/ceph-msgr-test/mgr.*.log' || true
            exit "$result"
        fi
    fi
    if test "$(docker inspect --format '{{.State.Running}}' "$container")" != true; then
        break
    fi
    sleep 1
done
docker logs --tail 50 "$container"
exit 1
