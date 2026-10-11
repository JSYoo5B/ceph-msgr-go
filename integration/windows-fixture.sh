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
out=$(CDPATH='' cd -- "$2" && pwd)
project_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
image=quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9

read_provider() {
    test -f "$out/provider.pid" || return 1
    provider_pid=$(cat "$out/provider.pid")
    case "$provider_pid" in ''|*[!0-9]*)
        echo 'invalid fixture controller process ID' >&2
        exit 2
    esac
    test "$provider_pid" -gt 0 || exit 2
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
        # Cancellation may arrive while preflight is still pulling the image.
        # Prevent that foreground start from launching a controller later.
        touch "$out/provider.stop"
        if ! read_provider; then
            test ! -f "$out/provider.cleanup.error" || { echo 'fixture controller cleanup failed; output retained' >&2; exit 1; }
            # Bootstrap may have failed before the controller was launched.
            exit 0
        fi
        deadline=$(($(date +%s) + 120))
        while ! test -f "$out/provider.stopped"; do
            test "$(date +%s)" -lt "$deadline" || { echo 'fixture controller cleanup timed out' >&2; exit 1; }
            if ! kill -0 "$provider_pid" 2>/dev/null; then
                # The controller may publish its final marker and exit between
                # the loop condition and this process check.
                test -f "$out/provider.stopped" || { echo 'fixture controller exited without confirming cleanup' >&2; exit 1; }
            fi
            sleep 1
        done
        test ! -f "$out/provider.cleanup.error" || { echo 'fixture controller cleanup failed; output retained' >&2; exit 1; }
        exit 0
        ;;
    diagnostics)
        read_provider || { echo 'fixture controller was not launched' >&2; exit 1; }
        rm -f "$out/provider.diagnostics.done"
        touch "$out/provider.diagnostics"
        deadline=$(($(date +%s) + 45))
        while ! test -f "$out/provider.diagnostics.done"; do
            test "$(date +%s)" -lt "$deadline" || { echo 'fixture controller diagnostics timed out' >&2; exit 1; }
            test ! -f "$out/provider.stopped" || { echo 'fixture controller stopped before collecting diagnostics' >&2; exit 1; }
            kill -0 "$provider_pid" 2>/dev/null || { echo 'fixture controller exited before collecting diagnostics' >&2; exit 1; }
            sleep 1
        done
        exit 0
        ;;
esac

test "$(id -u)" = 0 || { echo 'start requires root in the disposable WSL distro' >&2; exit 2; }
test -f "$out/relay" || { echo 'Windows driver must build the Linux fixture relay first' >&2; exit 2; }
test -f "$out/fixture-controller" || { echo 'Windows driver must build the Linux fixture controller first' >&2; exit 2; }
test ! -f "$out/provider.pid" || { echo 'fixture output directory has already been used' >&2; exit 2; }

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

# The controller owns the source snapshot, Ceph container and network. Keep the
# Windows profile independent of inherited settings for other test clusters.
for name in $(env | sed -n 's/^\(CEPH_MSGR_[A-Za-z0-9_]*\)=.*/\1/p'); do
    unset "$name"
done
export CEPH_MSGR_TEST_IMAGE="$image"
export CEPH_MSGR_TEST_KEY_TYPE=aes256k
export CEPH_MSGR_TEST_SERVICE_CIPHER=aes256k
export CEPH_MSGR_TEST_IP_FAMILY=4
export CEPH_MSGR_TEST_MGR_COUNT=2
export CEPH_MSGR_TEST_CONTROL_DIAGNOSTICS=1
export CEPH_MSGR_TEST_CONNECTION_MODE=secure
export CEPH_MSGR_TEST_RUNTIME=host
diagnostics=${3:-$out/diagnostics}
mkdir -p "$diagnostics"
chmod +x "$out/relay" "$out/fixture-controller"
test ! -f "$out/provider.stop" || exit 0
printf '%s\n' "$$" > "$out/provider.pid"
# Python owns this foreground WSL process until controller cleanup completes.
exec "$out/fixture-controller" serve -project "$project_root" -output "$out" -diagnostics "$diagnostics"
