#!/bin/bash
# Development-only single-MON bootstrap for the capture PoC. Runs inside the
# pinned Ceph image as the container command. A restart reuses the store.
set -euo pipefail
ip=$MSGRCAP_MON_IP
data=/var/lib/ceph/mon/ceph-a
if test ! -f "$data/msgrcap-ready"; then
    fsid=$(uuidgen)
    cat > /etc/ceph/ceph.conf <<EOF
[global]
fsid = $fsid
mon_host = [v2:$ip:3300]
mon_initial_members = a
auth_cluster_required = cephx
auth_service_required = cephx
auth_client_required = cephx
ms_bind_msgr1 = false
ms_mon_service_mode = crc secure
ms_mon_client_mode = crc secure
ms_mon_cluster_mode = crc secure
log_to_file = false
log_to_stderr = true
err_to_stderr = true
mon_cluster_log_to_stderr = false
[mon]
debug_ms = 10
EOF
    ceph-authtool --create-keyring /tmp/mon.keyring --gen-key -n mon. --cap mon 'allow *' >/dev/null
    # A read-only identity: the PoC issues only "mon dump".
    ceph-authtool --create-keyring /etc/ceph/ceph.client.capture.keyring --gen-key -n client.capture --cap mon 'allow r' >/dev/null
    ceph-authtool /tmp/mon.keyring --import-keyring /etc/ceph/ceph.client.capture.keyring >/dev/null
    monmaptool --create --addv a "[v2:$ip:3300]" --fsid "$fsid" /tmp/monmap >/dev/null
    mkdir -p "$data"
    ceph-mon --mkfs -i a --monmap /tmp/monmap --keyring /tmp/mon.keyring
    touch "$data/msgrcap-ready"
fi
exec ceph-mon -f -i a --public-addrv "[v2:$ip:3300]"
