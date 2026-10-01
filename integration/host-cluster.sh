#!/bin/sh
# Host-native tests use a loopback-published TCP relay into the fixture.
set -eu
case "${CEPH_MSGR_TEST_IP_FAMILY:-4}" in
    4) upstream=127.0.0.1 ;;
    6) upstream=::1 ;;
    *) exit 2 ;;
esac
/out/relay -upstream "$upstream" -ready /out/relay.ready > /out/relay.log 2>&1 &
exec /out/cluster.sh
