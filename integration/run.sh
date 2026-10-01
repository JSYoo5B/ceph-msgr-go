#!/bin/sh
# All Ceph executables and faults stay in a disposable test container.
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
image=${CEPH_MSGR_TEST_IMAGE:-quay.io/ceph/ceph:v20.2.4}
key_type=${CEPH_MSGR_TEST_KEY_TYPE:-aes256k}
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
docker run -d --name "$container" --label ceph-msgr-go.integration=true --entrypoint /bin/sh -e CEPH_MSGR_TEST_KEY_TYPE="$key_type" -v "$project_root/integration:/test:ro" -v "$out:/out" "$image" /test/cluster.sh > /dev/null
for attempt in $(seq 1 120); do
    if test -f "$out/ready"; then
        docker exec -e CEPH_MSGR_MONITORS=127.0.0.1:33300,127.0.0.1:33301,127.0.0.1:33302 -e CEPH_MSGR_KEY_FILE=/out/key -e CEPH_MSGR_IDENTITY=client.test -e CEPH_MSGR_FSID=80bbab73-69c1-4a0c-a746-4271357750b8 -e CEPH_MSGR_CONTROL_DIR=/out "$container" /out/client.test -test.run '^TestCeph.*Integration$' -test.v -test.timeout 90s
        exit
    fi
    if test "$(docker inspect --format '{{.State.Running}}' "$container")" != true; then
        break
    fi
    sleep 1
done
docker logs --tail 50 "$container"
exit 1
