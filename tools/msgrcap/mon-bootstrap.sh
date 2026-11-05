#!/bin/bash
# Development-only single-MON bootstrap for the capture PoC. Runs inside the
# pinned Ceph image as the container command. A restart reuses the store.
#
# The workflow controls each container generation through /msgrcap-ctl:
# - the MON daemon starts only once "gate" exists, so that a capture of a
#   restarted generation is ready before the MON can accept a connection;
# - SIGTERM to the container stops the daemon; after it exits, "daemon-exited"
#   holds its status and the container (and its network namespace) stays up
#   until the next SIGTERM, so that the peer close is captured before the
#   capture session of this generation is stopped.
set -euo pipefail
ip=$MSGRCAP_MON_IP
ctl=/msgrcap-ctl
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
trap 'exit 0' TERM
while test ! -f "$ctl/gate"; do sleep 0.1; done
ceph-mon -f -i a --public-addrv "[v2:$ip:3300]" &
pid=$!
trap 'kill -TERM "$pid" 2>/dev/null || true' TERM
status=0
# wait returns early when the trap runs; wait again until the daemon is gone.
while kill -0 "$pid" 2>/dev/null; do
    status=0
    wait "$pid" || status=$?
done
echo "$status" > "$ctl/daemon-exited"
trap 'exit 0' TERM
while true; do sleep 0.2; done
