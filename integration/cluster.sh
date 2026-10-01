#!/bin/sh
# Development fixture: runs only inside an ephemeral Ceph container.
set -eu
root=/tmp/ceph-msgr-test
key_type=${CEPH_MSGR_TEST_KEY_TYPE:-aes256k}
service_cipher=${CEPH_MSGR_TEST_SERVICE_CIPHER:-$key_type}
mgr_count=${CEPH_MSGR_TEST_MGR_COUNT:-2}
case "$mgr_count" in 0|2) ;; *) exit 2 ;; esac
case "$key_type" in aes|aes256k) ;; *) exit 2 ;; esac
case "$service_cipher" in aes|aes256k) ;; *) exit 2 ;; esac
allowed_ciphers=$key_type
if test "$service_cipher" != "$key_type"; then
    allowed_ciphers=aes,aes256k
fi
case "${CEPH_MSGR_TEST_IP_FAMILY:-4}" in
    4) address=127.0.0.1; bind_ipv4=true; bind_ipv6=false ;;
    6) address='[::1]'; bind_ipv4=false; bind_ipv6=true ;;
    *) exit 2 ;;
esac
mkdir -p "$root"
cat > "$root/ceph.conf" <<EOF
[global]
fsid = 80bbab73-69c1-4a0c-a746-4271357750b8
mon_host = [v2:$address:33300/0],[v2:$address:33301/0],[v2:$address:33302/0]
auth_cluster_required = cephx
auth_service_required = cephx
auth_client_required = cephx
auth_mon_ticket_ttl = 12
auth_service_ticket_ttl = 12
ms_cluster_mode = secure
ms_service_mode = secure
ms_client_mode = secure
ms_bind_msgr1 = false
ms_bind_msgr2 = true
ms_bind_ipv4 = $bind_ipv4
ms_bind_ipv6 = $bind_ipv6
log_to_stderr = true
err_to_stderr = true
[mon]
mon_data = /tmp/ceph-msgr-test/mon.\$id
[mon.a]
public_addr = $address:33300
[mon.b]
public_addr = $address:33301
[mon.c]
public_addr = $address:33302
[mgr]
mgr_data = /tmp/ceph-msgr-test/mgr.\$id
public_addr = $address
[mgr.a]
ms_bind_port_min = 36800
ms_bind_port_max = 36800
[mgr.b]
ms_bind_port_min = 36801
ms_bind_port_max = 36801
EOF
keyring="$root/keyring"
ceph-authtool "$keyring" --create-keyring --name mon. --gen-key --key-type "$key_type" --cap mon 'allow *'
ceph-authtool "$keyring" --name client.test --gen-key --key-type "$key_type" --cap mon 'allow *' --cap mgr 'allow *'
ceph-authtool "$keyring" --name client.readonly --gen-key --key-type "$key_type" --cap mon 'allow r' --cap mgr 'allow r'
ceph-authtool "$keyring" --name client.revocable --gen-key --key-type "$key_type" --cap mon 'allow *' --cap mgr 'allow *'
for name in a b; do
    ceph-authtool "$keyring" --name "mgr.$name" --gen-key --key-type "$key_type" --cap mon 'profile mgr' --cap mgr 'allow *'
done
monmaptool --create --fsid 80bbab73-69c1-4a0c-a746-4271357750b8 --addv a "[v2:$address:33300/0]" --addv b "[v2:$address:33301/0]" --addv c "[v2:$address:33302/0]" --set-min-mon-release 20 --enable-all-features --auth-service-cipher "$service_cipher" --auth-allowed-ciphers "$allowed_ciphers" --auth-preferred-cipher "$key_type" "$root/monmap"
cleanup() {
    for name in mon.a mon.b mon.c mgr.a mgr.b; do
        if test -f "$root/$name.pid"; then
            kill "$(cat "$root/$name.pid")" 2>/dev/null || true
        fi
    done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
start_mon() {
    ceph-mon -f -i "$1" -c "$root/ceph.conf" --keyring "$keyring" --setuser root --setgroup root >> "$root/mon.$1.log" 2>&1 &
    echo $! > "$root/mon.$1.pid"
}
for name in a b c; do
    mkdir -p "$root/mon.$name"
    ceph-mon --mkfs -i "$name" -c "$root/ceph.conf" --monmap "$root/monmap" --keyring "$keyring" --setuser root --setgroup root > "$root/mkfs.$name.log" 2>&1
    start_mon "$name"
done
mon_ready=false
for attempt in $(seq 1 60); do
    if timeout 3 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" status > /dev/null 2>&1; then
        mon_ready=true
        break
    fi
    sleep 1
done
test "$mon_ready" = true || exit 1
start_mgrs() {
    for name in a b; do
        mkdir -p "$root/mgr.$name"
        ceph-mgr -f -i "$name" -c "$root/ceph.conf" --keyring "$keyring" --setuser root --setgroup root > "$root/mgr.$name.log" 2>&1 &
        echo $! > "$root/mgr.$name.pid"
    done
    for attempt in $(seq 1 60); do
        if timeout 3 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" mgr dump --format json 2>/dev/null | python3 -c 'import json,sys; m=json.load(sys.stdin); sys.exit(not (m.get("available") and m.get("standbys")))'; then
            return 0
        fi
        sleep 1
    done
    return 1
}
if test "$mgr_count" = 2; then
    start_mgrs
fi
ceph-authtool "$keyring" -n client.test --print-key > /out/key
ceph-authtool "$keyring" -n client.readonly --print-key > /out/readonly.key
ceph-authtool "$keyring" -n client.revocable --print-key > /out/revocable.key
touch /out/ready
echo "Ceph test cluster ready: 3 MON, $mgr_count MGR, key=$key_type, service=$service_cipher, $address, secure."
while true; do
    if test -f /out/stop-mons; then
        read -r request_id extra < /out/stop-mons
        case "$request_id" in ''|*[!0-9]*) exit 2 ;; esac
        test -z "$extra" || exit 2
        rm /out/stop-mons
        for name in a b c; do
            kill "$(cat "$root/mon.$name.pid")"
        done
        for name in a b c; do
            wait "$(cat "$root/mon.$name.pid")" || true
        done
        touch "/out/mons-stopped.$request_id"
    fi
    if test -f /out/start-mons; then
        read -r request_id extra < /out/start-mons
        case "$request_id" in ''|*[!0-9]*) exit 2 ;; esac
        test -z "$extra" || exit 2
        rm /out/start-mons
        for name in a b c; do
            start_mon "$name"
        done
        joined=false
        for retry in $(seq 1 30); do
            if timeout 3 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" quorum_status --format json 2>/dev/null | python3 -c 'import json,sys; sys.exit(len(json.load(sys.stdin).get("quorum_names", [])) != 3)'; then
                joined=true
                break
            fi
            sleep 1
        done
        test "$joined" = true || exit 1
        touch "/out/mons-started.$request_id"
    fi
    if test -f /out/start-mgrs && ! test -f /out/mgrs-started; then
        test "$mgr_count" = 0 || exit 2
        start_mgrs
        mgr_count=2
        touch /out/mgrs-started
    fi
    # Fault controls affect only this container's daemons.
    if test -f /out/stop-mon-a && ! test -f /out/mon-a-stopped; then
        kill "$(cat "$root/mon.a.pid")"
        touch /out/mon-a-stopped
    fi
    if test -f /out/restart-mon; then
        read -r request_id name extra < /out/restart-mon
        case "$request_id" in ''|*[!0-9]*) exit 2 ;; esac
        case "$name" in a|b|c) ;; *) exit 2 ;; esac
        test -z "$extra" || exit 2
        rm /out/restart-mon
        pid=$(cat "$root/mon.$name.pid")
        kill "$pid"
        wait "$pid" || true
        start_mon "$name"
        joined=false
        for retry in $(seq 1 30); do
            if timeout 3 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" quorum_status --format json 2>/dev/null | python3 -c 'import json,sys; sys.exit(len(json.load(sys.stdin).get("quorum_names", [])) != 3)'; then
                joined=true
                break
            fi
            sleep 1
        done
        test "$joined" = true || exit 1
        touch "/out/mon-restarted.$request_id"
    fi
    sleep 1
done
