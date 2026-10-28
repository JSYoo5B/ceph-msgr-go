# MGR service map stream (MServiceMap)

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`messages/MServiceMap.h`, `mgr/ServiceMap.cc`, `msg/Message.h`
MSG_SERVICE_MAP 0x707, `mon/MgrStatMonitor.cc` check_sub; LGPL-2.1,
independently translated).

`Client.WatchServiceMap` adds a continuous `servicemap` subscription on the
admitted MON and delivers each full map: epoch, modification time, services by
name with their daemons by name (client GID, address, start epoch and time,
raw metadata and task status) and summary, plus the raw encoded map. This is
how service daemons such as RGW, rbd-mirror and cephfs-mirror are discovered;
this layer interprets no metadata. One unread map is retained and a later one
replaces it. Recovery, overflow, Close and the single active watch per client
follow the digest stream's rules (see `DigestStream`). An unset daemon address
(AF_UNSPEC, zero-filled) decodes as no endpoint.

Verification (`TestCephMonServiceMapIntegration`, 2026-10-17, Ceph 20.2.4
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`):
a native client registers a `ceph-msgr` service daemon with
`rados_service_register` and holds it for 25 seconds while the Go watch,
started before, reads maps concurrently. The Go map holding the daemon has the
GID and metadata of the MON's own `service dump`; the start epoch is compared
only when both are the same map epoch, because it was observed to move when
the daemon's MGR session re-registered. Passed twice in legacy-v6 IPv4 secure
and once in request-v9 IPv6 AES256K. Earlier attempts read the stream only
after the native client had exited and so sometimes saw the daemon already
gone; that was a test-ordering problem, not a decoding one. The first live run
also found that such daemons carry an AF_UNSPEC address, which the shared
address decoder now accepts as "no endpoint". Daemon removal timing, MON
failover during the watch and large maps are unverified.

A native holder keeps a synthetic registration alive under independent control
and is observed by a fresh native view
([native holder](../integration/fixture/SERVICE_MAP_HOLDER_VALIDATION.md)). It
is a fixture prerequisite for a later lifecycle cell and verifies no Go watch
behavior.

`TestCephMonServiceMapLifecycleIntegration` adds a distinct lifecycle recipe:
independently held native registration A, orderly Stop of the watch's actual
MON, the same watch registering on another admitted MON, and a new native
registration B that forces a post-fault map. Explicit release is followed by
fresh native absence and a newer Go map without either identity. Native cleanup
and full quorum restoration are checked separately. See the
[lifecycle recipe and evidence limits](../integration/fixture/SERVICE_MAP_LIFECYCLE_VALIDATION.md)
for selectors, phase bounds and the distinction from the holder-only test.
Execution acceptance is tied to the tested SHA/profile recorded on the pull
request; adding this recipe alone is not a native pass or large-map acceptance.
