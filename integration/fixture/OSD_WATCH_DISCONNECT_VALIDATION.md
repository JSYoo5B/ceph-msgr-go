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

Not claimed: DISCONNECT on blocklisting or PG interval changes, or a watcher
whose connection is down when the timer fires (the OSD then has nowhere to
send the event).
