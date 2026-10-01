#!/bin/sh
# Development fixture: runs only inside an ephemeral Ceph container.
set -eu
root=/tmp/ceph-msgr-test
key_type=${CEPH_MSGR_TEST_KEY_TYPE:-aes256k}
case "$key_type" in aes|aes256k) ;; *) exit 2 ;; esac
mkdir -p "$root"
cat > "$root/ceph.conf" <<'EOF'
[global]
fsid = 80bbab73-69c1-4a0c-a746-4271357750b8
mon_host = [v2:127.0.0.1:33300/0],[v2:127.0.0.1:33301/0],[v2:127.0.0.1:33302/0]
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
ms_bind_ipv6 = false
log_to_stderr = true
err_to_stderr = true
[mon]
mon_data = /tmp/ceph-msgr-test/mon.$id
[mon.a]
public_addr = 127.0.0.1:33300
[mon.b]
public_addr = 127.0.0.1:33301
[mon.c]
public_addr = 127.0.0.1:33302
[mgr]
mgr_data = /tmp/ceph-msgr-test/mgr.$id
public_addr = 127.0.0.1
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
for name in a b; do
    ceph-authtool "$keyring" --name "mgr.$name" --gen-key --key-type "$key_type" --cap mon 'profile mgr' --cap mgr 'allow *'
done
monmaptool --create --fsid 80bbab73-69c1-4a0c-a746-4271357750b8 --addv a '[v2:127.0.0.1:33300/0]' --addv b '[v2:127.0.0.1:33301/0]' --addv c '[v2:127.0.0.1:33302/0]' --set-min-mon-release 20 --enable-all-features --auth-service-cipher "$key_type" --auth-allowed-ciphers "$key_type" --auth-preferred-cipher "$key_type" "$root/monmap"
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
for name in a b c; do
    mkdir -p "$root/mon.$name"
    ceph-mon --mkfs -i "$name" -c "$root/ceph.conf" --monmap "$root/monmap" --keyring "$keyring" --setuser root --setgroup root > "$root/mkfs.$name.log" 2>&1
    ceph-mon -f -i "$name" -c "$root/ceph.conf" --keyring "$keyring" --setuser root --setgroup root > "$root/mon.$name.log" 2>&1 &
    echo $! > "$root/mon.$name.pid"
done
for attempt in $(seq 1 60); do
    if timeout 3 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" status > /dev/null 2>&1; then
        break
    fi
    sleep 1
done
for name in a b; do
    mkdir -p "$root/mgr.$name"
    ceph-mgr -f -i "$name" -c "$root/ceph.conf" --keyring "$keyring" --setuser root --setgroup root > "$root/mgr.$name.log" 2>&1 &
    echo $! > "$root/mgr.$name.pid"
done
for attempt in $(seq 1 60); do
    if timeout 3 ceph -c "$root/ceph.conf" -n client.test -k "$keyring" mgr dump --format json 2>/dev/null | python3 -c 'import json,sys; m=json.load(sys.stdin); sys.exit(not (m.get("available") and m.get("standbys")))'; then
        ceph-authtool "$keyring" -n client.test --print-key > /out/key
        touch /out/ready
        echo "Ceph test cluster ready: 3 MON, 2 MGR, $key_type, secure."
        while true; do
            # The fault test controls only this container's MON a.
            if test -f /out/stop-mon-a && ! test -f /out/mon-a-stopped; then
                kill "$(cat "$root/mon.a.pid")"
                touch /out/mon-a-stopped
            fi
            sleep 1
        done
    fi
    sleep 1
done
tail -n 20 "$root"/mon.*.log "$root"/mgr.*.log
exit 1
