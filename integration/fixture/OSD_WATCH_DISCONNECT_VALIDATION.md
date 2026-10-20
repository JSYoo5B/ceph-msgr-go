# Live watch DISCONNECT

Run 2026-10-17, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture. Test: `TestCephOSDWatchDisconnectIntegration`.
Contract: `internal/osd/WATCH_NOTIFY_CONTRACT.md`.

The OSD sends `CEPH_WATCH_EVENT_DISCONNECT` to a still-connected watcher in
two cases (`osd/PrimaryLogPG.cc` handle_watch_timeout and the watcher loop in
_delete_oid); both were produced live.

- Timeout: a Go WATCH with a 5-second timeout and no Ping. The independent
  native watcher list first shows exactly the Go GlobalID and cookie. About
  4.9 seconds later the handle delivers DISCONNECT for that cookie and then
  returns `ErrOSDWatchDisconnected`; the native list is empty, and the same
  OSD connection still answers a STAT.
- Delete: a Go WATCH (30-second timeout) on an object a second, independent
  Go client then deletes. The DISCONNECT arrives immediately after the
  delete's reply, the handle ends the same way, and native observation shows
  the object absent (-2).

Nothing re-registered automatically in either case.

Passed in legacy-v6 secure AES256K IPv4 with three BlueStore replicas and in
request-v9/reply-v8 secure AES256K IPv6 with one MemStore OSD.

## Blocklisting (2026-10-18)

`TestCephOSDWatchBlocklistIntegration`: a separate Go client registers a
watch (30-second timeout); the independent native watcher list shows its
GlobalID, cookie and address (for example `172.19.0.9:0/2856217396`). That
exact address is added through MON `osd blocklist add` (default expiry;
removed in cleanup). The OSD sends DISCONNECT for the cookie 4-5 ms later, the
handle then reports `ErrOSDWatchDisconnected`, the native list empties, and
a STAT on the same OSD connection gets a known `-108` (ESHUTDOWN) with no
retry. Passed in legacy-v6 single BlueStore (secure AES256K IPv4), legacy-v6
three BlueStore replicas and request-v9/reply-v8 secure AES256K IPv6 single
MemStore.

## Timer firing while the watcher is disconnected (2026-10-18)

`TestCephOSDWatchExpiredReconnectIntegration`: a watch with a 5-second
timeout whose OSD connection is closed without UNWATCH stays in the native
watcher list, then disappears 5.1-5.2 s after the close (the OSD has nowhere
to send DISCONNECT). A RECONNECT for that cookie on a new connection is then
refused with a known `-107` (ENOTCONN) and leaves no registration; the client
does not fall back to a new WATCH. Passed in legacy-v6 single BlueStore
(secure AES256K IPv4) and request-v9/reply-v8 secure AES256K IPv6 single
MemStore.

## PG interval change (2026-10-18)

`TestCephOSDWatchIntervalChangeIntegration` (legacy-v6, three BlueStore
replicas): a Go watch (30-second timeout) pings successfully at the object's
primary; a replica of the PG is then SIGKILLed and the acting set shrinks
(for example `[1 0 2]` -> `[1 2]`, epoch 19 -> 21) with the primary
unchanged. The OSD sends no DISCONNECT (none within 5 s); the watcher's next
Ping on its unchanged connection gets a known `-110` (ETIMEDOUT, the OSD's
signal to reconnect), while the native watcher list still shows the
registration, which is part of the object's state. An explicit RECONNECT on a
newer connection is accepted, the list shows the same cookie and Ping returns
0 again. Nothing re-registered automatically. Passed in secure AES256K IPv4
and CRC mode with AES key and service cipher over IPv6.
