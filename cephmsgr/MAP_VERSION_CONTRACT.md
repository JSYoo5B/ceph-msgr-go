# MON map versions (MMonGetVersion)

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`messages/MMonGetVersion.h`, `messages/MMonGetVersionReply.h`,
`mon/Monitor.cc` handle_get_version; LGPL-2.1, independently translated).

`Client.MapVersion(ctx, kind)` sends MMonGetVersion (message 19: a u64 handle
and the map name) for `osdmap`, `monmap`, `mdsmap` or `fsmap` (the last two
name the same MON service) and returns the MMonGetVersionReply (message 20,
header v2): the newest committed version, which is the newest map's epoch,
and the oldest version the MON keeps. It fetches no map; librados uses the same
request before waiting for the latest OSDMap. The MON copies the request's
header TID and handle into the reply; both are checked.

The MON answers only in quorum and once the service is readable, and silently
ignores other map names, so those are refused locally. ctx bounds the wait;
after transmission it ends as an `OutcomeUnknownError` (read-only). A reply
with another handle or an oldest version above the newest fails the session.

Verification: the newest version equals the epoch native `osd dump`,
`mon dump` and `fs dump` report, for all four names, in legacy-v6 single
BlueStore (IPv4, secure) and request-v9 single MemStore (IPv6, AES256K)
(`TestCephMonMapVersionIntegration`, 2026-10-17, Ceph 20.2.4 image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`).
The oldest version is only checked to lie between 1 and the newest; no native
client reports it. Clients seeded with each of the three monitors also match
the native OSDMap epoch, two of them answered by peons from their own
committed state. Leader stop (`TestCephMonMapVersionLeaderStopIntegration`):
a client pinned to a peon gets MMonGetVersion and MStatfs answers at once
while the peon's lease lasts, and after it (6 s later) the requests wait
about 8 s for the re-election and are then answered; after the leader
restarts and the election settles, the version equals the native epoch.
A frozen connected MON (`docker pause`) ends an in-flight request as an
unknown outcome at the keepalive timeout; the client moves to another member
of its map and answers fresh requests there (2026-10-18, two profiles; see
`integration/fixture/MON_POOL_OP_VALIDATION.md`). A MON that dies outright
during a request is unverified.
