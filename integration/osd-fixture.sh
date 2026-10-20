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
