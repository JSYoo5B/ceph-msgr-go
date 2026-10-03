#!/bin/sh
# Development-only Linux fixture for Windows-native Go test executables.
# The Windows driver owns the output directory and the imported WSL distro.
set -eu

if test "$#" -lt 2 || test "$#" -gt 3; then
    echo 'usage: windows-fixture.sh start|hold|diagnostics|stop OUTPUT_PATH [DIAGNOSTICS_PATH]' >&2
    exit 2
fi
action=$1
case "$action" in start|hold|diagnostics|stop) ;; *) exit 2 ;; esac
test -d "$2" || { echo 'fixture output directory does not exist' >&2; exit 2; }
out=$(CDPATH= cd -- "$2" && pwd)
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
image=quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9

read_container() {
    test -f "$out/container-name" || return 1
    container=$(cat "$out/container-name")
    case "$container" in ceph-msgr-go-windows-?*) ;; *)
        echo 'invalid fixture container name' >&2
        exit 2
    esac
    case "$container" in *[!a-zA-Z0-9_.-]*)
        echo 'invalid fixture container name' >&2
        exit 2
    esac
}

require_owned_container() {
    owner=$(docker inspect --format '{{index .Config.Labels "ceph-msgr-go.windows-integration"}}' "$container")
    test "$owner" = true || { echo 'container is not owned by this Windows fixture' >&2; exit 2; }
}

case "$action" in
    hold)
        # A foreground WSL command owns this session while Windows tests run.
        # Docker's systemd service alone does not keep the distribution alive.
        touch "$out/holder.ready"
        deadline=$(($(date +%s) + 1800))
        while ! test -f "$out/holder.stop"; do
            test "$(date +%s)" -lt "$deadline" || { echo 'WSL session lifetime exceeded' >&2; exit 1; }
            sleep 1
        done
        exit 0
        ;;
    stop)
        if read_container; then
            # A failed Docker query is not proof that the owned container left.
            docker info > /dev/null
            present=$(docker container ls --all --filter "name=^/$container$" --format '{{.Names}}')
            test -n "$present" || exit 0
            test "$present" = "$container" || exit 2
            require_owned_container
            docker rm -f "$container" > /dev/null
            present=$(docker container ls --all --filter "name=^/$container$" --format '{{.Names}}')
            test -z "$present" || { echo 'owned fixture container is still present' >&2; exit 1; }
        fi
        exit 0
        ;;
    diagnostics)
        diagnostics=${3:-$out/diagnostics}
        mkdir -p "$diagnostics"
        if read_container && docker inspect "$container" > /dev/null 2>&1; then
            require_owned_container
            docker logs --tail 100 "$container" > "$diagnostics/fixture.log" 2>&1 || true
            docker inspect --format '{{json .State}}' "$container" > "$diagnostics/container-state.json"
            docker cp "$container:/tmp/ceph-msgr-test/fixture-exit" "$diagnostics/fixture-exit" > /dev/null 2>&1 || true
            for log in mon.a.log mon.b.log mon.c.log mgr.a.log mgr.b.log osd-map-control.log; do
                docker cp "$container:/tmp/ceph-msgr-test/$log" "$diagnostics/$log" > /dev/null 2>&1 || true
            done
        fi
        # Whitelist fixture metadata: never copy credentials or process memory.
        for file in relay.log daemon-version tell-oracle-mon.json tell-oracle-mgr.json tell-oracle-mgr-map.json named-mon-b.json log-oracle.json log-oracle-sentinel.txt command-oracle-mon.json command-oracle-mgr.json command-oracle-summary.json manager-oracle.json monitor-oracle.json digest-oracle.json config-oracle.json hostname-config-oracle.json osd-map-native-oracle.json osd-map-manifest.json; do
            if test -f "$out/$file"; then
                cp "$out/$file" "$diagnostics/$file"
            fi
        done
        exit 0
        ;;
esac

test "$(id -u)" = 0 || { echo 'start requires root in the disposable WSL distro' >&2; exit 2; }
test -f "$out/relay" || { echo 'Windows driver must build the Linux fixture relay first' >&2; exit 2; }
test ! -f "$out/container-name" || { echo 'fixture output directory has already been used' >&2; exit 2; }

if ! command -v docker > /dev/null 2>&1; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y --no-install-recommends docker.io python3
fi
if ! docker info > /dev/null 2>&1; then
    service docker start
fi
docker info > /dev/null
docker pull "$image"
test "$(docker image inspect --format '{{.Architecture}}' "$image")" = amd64 || {
    echo 'Windows fixture requires the pinned amd64 Ceph image' >&2
    exit 1
}

cp "$project_root/integration/cluster.sh" "$out/cluster.sh"
cp "$project_root/integration/osd-fixture.sh" "$out/osd-fixture.sh"
cp "$project_root/integration/publish-ack.sh" "$out/publish-ack.sh"
cp "$project_root/integration/host-cluster.sh" "$out/host-cluster.sh"
for oracle in "$project_root"/tools/*_oracle.py; do
    cp "$oracle" "$out/"
done
chmod +x "$out/relay"
container="ceph-msgr-go-windows-$(date +%s)-$$"
printf '%s\n' "$container" > "$out/container-name"
docker run -d --name "$container" \
    --label ceph-msgr-go.integration=true \
    --label ceph-msgr-go.windows-integration=true \
    --sysctl net.ipv4.ip_local_reserved_ports=33300,33301,33302,36800,36801 \
    --entrypoint /bin/sh -p 127.0.0.1::40000 \
    -e CEPH_MSGR_TEST_KEY_TYPE=aes256k \
    -e CEPH_MSGR_TEST_SERVICE_CIPHER=aes256k \
    -e CEPH_MSGR_TEST_IP_FAMILY=4 \
    -e CEPH_MSGR_TEST_MGR_COUNT=2 \
    -e CEPH_MSGR_TEST_CONTROL_DIAGNOSTICS=1 \
    -v "$out:/out" "$image" /out/host-cluster.sh > /dev/null

deadline=$(($(date +%s) + 120))
while test "$(date +%s)" -lt "$deadline"; do
    if test -f "$out/ready" && test -f "$out/relay.ready"; then
        docker exec "$container" ceph --version > "$out/daemon-version"
        version=$(cat "$out/daemon-version")
        test "$version" = 'ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)' || {
            echo 'fixture daemon version does not match the fixed Tentacle reference' >&2
            exit 1
        }
        proxy=$(docker port "$container" 40000/tcp)
        case "$proxy" in 127.0.0.1:*) ;; *)
            echo 'fixture relay was not published exclusively on IPv4 loopback' >&2
            exit 1
        esac
        port=${proxy#127.0.0.1:}
        case "$port" in ''|*[!0-9]*) exit 1 ;; esac
        test "$port" -gt 0 && test "$port" -le 65535 || exit 1
        printf '%s\n' "$proxy" > "$out/proxy"
        echo "Windows Ceph fixture ready through $proxy"
        exit 0
    fi
    if test "$(docker inspect --format '{{.State.Running}}' "$container")" != true; then
        echo 'Ceph fixture exited before readiness' >&2
        exit 1
    fi
    sleep 1
done
echo 'Ceph fixture did not become ready within 120 seconds' >&2
exit 1
