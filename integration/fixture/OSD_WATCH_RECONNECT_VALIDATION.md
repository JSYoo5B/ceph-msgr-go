# Explicit WATCH reconnect native acceptance — 2026-10-15

`TestCephOSDWatchReconnectIntegration` passed once in each of the two profiles
below. These were targeted development runs of the working-tree reconnect API
and integration test. They are not a full CI result or native coverage for
subsequent malformed-input preflight changes.

Both fixtures used Ceph 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`, Linux arm64 daemons, fixture provider
v0.2.0, and image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The retained manifest's running image ID matches that digest in both cases.
The manifests identify the native configuration; they do not record an exact
Git commit for the development client working tree.

## Executed configurations and results

| Configuration | Native identity | WATCH / RECONNECT | Connection handoff | Result |
| --- | --- | --- | --- | --- |
| `legacy-v6`, AES key and service cipher, CRC, IPv4, host client, three BlueStore OSDs, size/min_size 3/2, one PG without hashpspool | client 4734, cookie 1 | generation 9 → 10 | session generation 1 → 2; 7.8065 ms | 1/1 PASS; native notify/ACK, UNWATCH and cleanup confirmed |
| `replicated-pg-v8`, AES256K key and service cipher, secure, IPv6, container client, one MemStore OSD, size/min_size 1/1, eight PGs with hashpspool | client 4623, cookie 1 | generation 9 → 10 | session generation 1 → 2; 755.333 µs | 1/1 PASS; native notify/ACK, UNWATCH and cleanup confirmed |

The profile names refer to MOSDOp request encoding: the legacy profile uses an
explicit object hash with v6 requests; the replicated-PG profile supplies the
actual caller-selected PG in v8 requests. They do not label MWatchNotify's
message version. The measured handoff starts immediately before old-connection
Close and ends after the successful explicit RECONNECT reply. Both completed
within the ten-second operation context and the registered 37-second grace
window. The JSON stores integer milliseconds, respectively 7 and 0; the log
retains the more precise durations shown above.

## Observed sequence

Each run created one exclusive native-fixture object, then performed:

1. Register a Go watch with timeout 37 seconds and registration generation 9.
   Independent native `rados listwatchers` reported exactly one registration
   with the Go client's GlobalID and cookie 1.
2. Deliver an independent native C librados binary NOTIFY to the old handle.
   Native observation remained incomplete until the Go handle explicitly
   acknowledged it. Its completion contained exactly one matching identity and
   binary ACK, return code 0, and no missed watcher. This established the old
   handle's event session generation 1.
3. Explicitly Ping, Close the old OSD connection, open a new OSD connection
   through the same Client, and call `ReconnectWatch` once with the old private
   registration handle and caller-owned routing. No native observer call occurs
   between Close and RECONNECT. The client GlobalID and cookie remained the same;
   the native reply echoed RECONNECT action 5 and generation 10.
4. Independently list exactly the same native watcher identity and cookie after
   reconnect. The Go LIST_WATCHERS result also retained the original stored
   timeout of 37 seconds. The old handle's Ping was rejected locally as
   superseded, and the new handle rejected the old-session notification as ACK
   authority, both without an operation submission.
5. Send another independent native binary NOTIFY. The new handle received the
   same cookie and client identity with session generation 2, a new NotifyID,
   and the native notifier's separate GlobalID. The native notifier again
   remained incomplete before the explicit binary ACK, then completed with
   exactly one correct reply, code 0, and no missed watcher.
6. Explicitly UNWATCH through the new handle. Native listing became empty,
   local delivery ended, and an independent native read retained the original
   object bytes `00ff807265636f6e6e656374`.

The initial notification payload was `00ff6f6c6400`, with explicit ACK
`00806f6c642d41434b`. The post-reconnect notification payload was
`00ff806e657700`, with ACK `00ff6e65772d41434b00`. The native C API completion
receipts independently preserve these ACK blobs and the watcher identity.
The two native notifications in each run are distinct cases; start/observe/
finish snapshots of a notifier are not counted as additional notifications.

## Independent artifact checks and cleanup

The retained raw JSON was checked separately from the integration test's PASS
line. This included decoding the stored MOSDOpReply `RawFront` bytes for WATCH,
RECONNECT and UNWATCH: opcode `0x220f`, cookie 1, actions 3/5/0, generations
9/10/0, and timeout fields 37/0/0, with successful signed results and ONDISK
completion flags. The zero timeout in the RECONNECT union does not replace the
existing registered timeout.

The old and new MWatchNotify raw fronts were separately decoded and compared
with their cookie, NotifyID, notifier GlobalID and exact binary payloads in the
JSON. Native list entries, both native notifier completions, generation values,
and local fencing results were also cross-checked. The list's stored address
is not evidence that the live connection changed; actual notification delivery
and the bound session generations establish the handoff exercised here.

For both runs, the final receipt records `Unwatched=true`, `Cleaned=true`, both
local fencing checks true, and no cleanup errors. The native OMAP-write cleanup
receipt independently reports removal code 0 for the exclusive object,
`objects_removed=true` and `snaps_removed=true`. The ownership manifests list
ten fixture containers for the three-OSD run and eight for the single-OSD run;
every one of those container IDs has a matching `Container terminated` line in
its retained run log. No cleanup omission was found. This is artifact-backed
container termination evidence; it is not a separate post-run Docker inventory
or network-removal inspection.

### Retained development evidence

CRC/AES case:

- Log: `/private/tmp/ceph-watch-reconnect-crc-aes-first.log`.
- Diagnostic directory: `/private/tmp/ceph-watch-reconnect-crc-aes-first`.
- `osd-watch-reconnect-results.json` SHA-256:
  `9da6fe57dd66a18712e90ebca0c8277a8c085465a0e0cd4573841f713f7908f8`.
- Log SHA-256:
  `10810ee5cfc563fa5de3bb51a9d996bc8433c16912ed3bb4b55984334fd0039e`.

Secure/AES256K/IPv6 case:

- Log: `/private/tmp/ceph-watch-reconnect-secure-aes256k-ipv6-first.log`.
- Diagnostic directory: `/private/tmp/ceph-watch-reconnect-secure-aes256k-ipv6-first`.
- `osd-watch-reconnect-results.json` SHA-256:
  `ec62badae88ecf41ec1bd08cfd4491e985c7a561d02dfedeced87dcdfac8fdd2`.
- Log SHA-256:
  `1c1065c86e5a3f0f0829def6bfa8cb149e8d331cf1c179ec3bfcd5011c376646`.

Each directory also retains `fixture-manifest.json`, `fixture-ownership.json`,
`osd-oracle.json`, native watcher/notifier receipts and the native owned-object
cleanup receipt. Raw fronts, event metadata and partial cleanup state are in
the public Go evidence file. No credentials are part of these receipts.

## Limits

The verified behavior is one caller-directed, successful reconnect to the same
OSD through the same Client and GlobalID, before the existing watch expired.
Neither run restarts or fails over an OSD, changes client identity, waits for
watch expiry, drops the RECONNECT reply, or reconnects with a pending native
notification. They do not verify native in-progress notification redelivery,
automatic recovery, renewal scheduling, registration fallback, or broader
profile/version coverage. Registration generation 9 → 10 is transmitted and
echoed wire metadata; it is not claimed as independently observed native
generation-based event fencing.

Old queued-event draining, uncertain submissions, ownership transfer races and
malformed-input preflight have separate synthetic/unit verification. These two
live successful paths do not substitute for those checks, whole-project CI or
native verification of later source edits. The protocol and explicit ownership
contract are described in
[WATCH_NOTIFY_CONTRACT.md](../../internal/osd/WATCH_NOTIFY_CONTRACT.md).

## Across a primary change — 2026-10-17

`TestCephOSDWatchFailoverIntegration` (three BlueStore replicas) registers a
Go WATCH (timeout 30 s, generation 3) on a native-seeded owned object; the
native watcher list shows it. The object's primary is SIGKILLed and the old
connection closed. Once native placement names a replacement primary at a
newer epoch, the same client opens it and calls `ReconnectWatch` with the
original handle and the new routing: the reply keeps cookie 1 and advances
the generation to 4, and the native list still shows exactly that one
registration (the watch is part of the replicated object info). A
notification from a second, independent Go client through the new primary
reaches the reconnected handle with its binary payload; the explicit ACK
completes the notify with one reply and no missed watcher; UNWATCH succeeds.
The killed OSD is restarted and the fixture returns to clean PGs.

| Profile | Primary | Epochs |
| --- | --- | --- |
| legacy-v6, secure AES256K IPv4 | osd.1 -> osd.0 | 19 -> 21 |
| replicated-pg-v8, secure AES256K IPv6 | osd.1 -> osd.2 | 18 -> 20 |

Not claimed: notifications sent while the watcher was between primaries
(redelivery of in-progress notifies), or a RECONNECT later than the watch
timeout.

## Pending-notify redelivery — 2026-10-17

`TestCephOSDWatchNotifyRedeliveryIntegration` registers a Go WATCH (timeout
30 s) and closes its connection. A second Go client then notifies (timeout
20 s); the notify does not complete within 2 seconds because its only watcher
is disconnected. The first client opens a new connection and calls
`ReconnectWatch`: the OSD redelivers the pending notification
(`Watch::connect`), the handle receives its binary payload, and the explicit
ACK completes the notify with one reply and no missed watcher. Passed in
legacy-v6 secure AES256K IPv4 single BlueStore and request-v9/reply-v8 secure
AES256K IPv6 single MemStore.
