#!/bin/sh
# Test-only single-OSD topology. No host devices or product native dependencies.
set -eu
test "$#" = 1 || exit 2
root=$1
pool=ceph-msgr-osd-fixture
object=ceph-msgr-native-object
key_type=${CEPH_MSGR_TEST_KEY_TYPE:-aes256k}
connection_mode=${CEPH_MSGR_TEST_CONNECTION_MODE:-secure}
native_ceph() {
    timeout 10 ceph -c "$root/ceph.conf" -n client.test -k "$root/keyring" "$@"
}
mkdir -p "$root/osd.0"
cat >> "$root/ceph.conf" <<EOF
[osd]
osd_data = $root/osd.\$id
osd_objectstore = memstore
osd_crush_update_on_start = false
osd_beacon_report_interval = 1
ms_service_mode = $connection_mode
ms_mon_cluster_mode = secure crc
ms_bind_port_min = 36900
ms_bind_port_max = 36910
[osd.0]
admin_socket = $root/osd.0.asok
public_addr = 127.0.0.1:36900
cluster_addr = 127.0.0.1:36901
EOF
osd_uuid=$(python3 -c 'import uuid; print(uuid.uuid4())')
test "$(native_ceph osd create "$osd_uuid" 0)" = 0
ceph-authtool "$root/osd.0/keyring" --create-keyring --name osd.0 --gen-key --key-type "$key_type" --cap mon 'allow profile osd' --cap mgr 'allow profile osd' --cap osd 'allow *'
native_ceph auth import -i "$root/osd.0/keyring"
ceph-osd -i 0 --mkfs -c "$root/ceph.conf" --osd-uuid "$osd_uuid" --keyring "$root/osd.0/keyring" --setuser root --setgroup root > "$root/osd.0.mkfs.log" 2>&1
# This explicit legacy map uses a straw bucket and no chooseleaf/upmap/choose
# args. It exercises only the feature requirements implemented for OSD CLIENTs.
# Native crushtool, rather than product placement code, constructs the topology.
cat > "$root/osd.crush.txt" <<'EOF'
tunable choose_local_tries 2
tunable choose_local_fallback_tries 5
tunable choose_total_tries 19
tunable chooseleaf_descend_once 0
tunable chooseleaf_vary_r 0
tunable chooseleaf_stable 0
tunable straw_calc_version 0
device 0 osd.0
type 0 osd
type 10 root
root default {
    id -1
    alg straw
    hash 0
    item osd.0 weight 1.00000
}
rule replicated_rule {
    id 0
    type replicated
    step take default
    step choose firstn 0 type osd
    step emit
}
EOF
crushtool -c "$root/osd.crush.txt" -o "$root/osd.crush"
native_ceph osd setcrushmap -i "$root/osd.crush"
native_ceph balancer off
native_ceph config set mon mon_allow_pool_size_one true
native_ceph osd pool create "$pool" 1 1 replicated replicated_rule
native_ceph osd pool set "$pool" size 1 --yes-i-really-mean-it
native_ceph osd pool set "$pool" min_size 1
native_ceph osd pool set "$pool" hashpspool false --yes-i-really-mean-it
native_ceph osd pool set "$pool" pg_autoscale_mode off
ceph-osd -f -i 0 -c "$root/ceph.conf" --keyring "$root/osd.0/keyring" --setuser root --setgroup root >> "$root/osd.0.log" 2>&1 &
echo $! > "$root/osd.0.pid"
osd_ready=false
for attempt in $(seq 1 60); do
    if native_ceph osd dump --format json > "$root/osd.dump.json" 2> "$root/osd.probe.log" && python3 - "$root/osd.dump.json" <<'PY'
import json, sys
v = json.load(open(sys.argv[1]))
sys.exit(not any(o.get("osd") == 0 and o.get("up") == 1 and o.get("in") == 1 for o in v["osds"]))
PY
    then
        osd_ready=true
        break
    fi
    kill -0 "$(cat "$root/osd.0.pid")" 2>/dev/null || break
    sleep 1
done
test "$osd_ready" = true || exit 1
python3 - /out/osd-object.bin <<'PY'
import pathlib, sys
# Binary and >4 KiB: verify truncation, offset reads, NULs, and non-UTF8 bytes.
pathlib.Path(sys.argv[1]).write_bytes(bytes(range(256)) * 41 + b"\x00\xffceph-msgr-tail\x80")
PY
timeout 30 rados -c "$root/ceph.conf" -n client.test -k "$root/keyring" -p "$pool" put "$object" /out/osd-object.bin
timeout 10 rados -c "$root/ceph.conf" -n client.test -k "$root/keyring" -p "$pool" stat "$object" > /out/osd-native-stat.txt
timeout 10 rados -c "$root/ceph.conf" -n client.test -k "$root/keyring" -p "$pool" get "$object" /out/osd-native-read.bin
cmp /out/osd-object.bin /out/osd-native-read.bin
# Clearing the pool's creating flag advances OSDMap after the first successful
# put. Wait before publishing a caller-supplied epoch: these first codec tests
# intentionally do not implement unsolicited OSDMap recovery.
epoch_stable=false
previous_epoch=
for attempt in $(seq 1 30); do
    native_ceph osd dump --format json > "$root/osd.dump.json"
    current_epoch=$(python3 - "$root/osd.dump.json" "$pool" <<'PY'
import json, sys
value = json.load(open(sys.argv[1]))
pool = next(p for p in value["pools"] if p["pool_name"] == sys.argv[2])
if "creating" not in pool.get("flags_names", "").split(","):
    print(value["epoch"])
PY
    )
    if test -n "$current_epoch" && test "$current_epoch" = "$previous_epoch"; then
        epoch_stable=true
        break
    fi
    previous_epoch=$current_epoch
    sleep 2
done
test "$epoch_stable" = true || exit 1
native_ceph osd map "$pool" "$object" --format json > "$root/osd.object-map.json"
native_ceph osd dump --format json > "$root/osd.dump.json"
native_ceph osd crush dump --format json > "$root/osd.crush.json"
timeout 15 python3 /out/osd_oracle.py "$root" /out "$pool" "$object"
