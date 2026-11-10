# ServiceMap limits: large maps and over-budget input

This record covers requirement 2 of issue #64 for
[`WatchServiceMap`](../../cephmsgr/SERVICE_MAP_CONTRACT.md): a large native
map that the default limits admit, and the three ways a map can exceed a
limit. It adds one native cell, two closed steps of the
[native holder](SERVICE_MAP_HOLDER_VALIDATION.md) and daemon-free tests. It
changes no product code, limit or API.

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`; `mgr/ServiceMap.cc`,
`mgr/DaemonServer.cc` `handle_open`, `librados/RadosClient.cc`
`service_daemon_register`, `mon/MgrStatMonitor.cc` `check_sub` (LGPL-2.1, no
copied implementation). Image:
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Limits and their outcomes

| Limit | Value | A map above it | Scope of the failure |
| --- | --- | --- | --- |
| Watch byte budget | `ServiceMapOptions.MaxBufferedBytes` (default `MaxFrameSize`); counts the raw map plus 512 bytes | The watch ends with `ErrServiceMapOverflow` | That watch only. The MON session stays admitted |
| Frame size | `Options.MaxFrameSize` (default 16 MiB), front plus 41 header bytes | The MON session fails with `ErrLimitExceeded` | The MON session. The watch ends with that error; the client reconnects |
| Collection size | 65536 services, daemons per service, metadata entries and task-status entries per daemon (fixed) | The MON session fails with `ErrLimitExceeded` and `ErrMalformedMessage` | The MON session, as above |

In all three cases a map accepted earlier and not yet read is delivered
first, the cause is stable on later `Next` calls, the client's watch slot is
free again, and the ended watch is not resubscribed after recovery.

A command that was in flight on a failed session ends as an unknown outcome.
A command issued between the watch's end and the session's failure can be
admitted to the failing session. After a collection refusal the watch ends
first and the session fails immediately afterwards.

An ended or closed watch sends no remote unsubscribe (see the
[contract](../../cephmsgr/SERVICE_MAP_CONTRACT.md)). The MON keeps sending
each new map on that session until the session is replaced, and the client
drops those maps undecoded. A frame above `MaxFrameSize` still fails that
session, with or without a watch. This follows from the product source; the
cell does not assert it separately.

## What a native client can produce

Pinned `handle_open` copies a registering client's `daemon_metadata` into the
pending ServiceMap without a size or count check, and `ServiceMap::encode`
writes ordinary 32-bit collection counts. `service_daemon_register` refuses
only reserved service names and empty names. So:

- **Large metadata values** are producible by one native client. Covered
  natively below.
- **More than 65536 metadata entries in one daemon** are producible by one
  native client. Covered natively below. Such a map is valid for native
  clients and is refused by the Go decoder's fixed cap.
- **More than 65536 daemons in a service, or services,** need that many
  simultaneously registered native clients, one librados handle each. This
  is not produced. Those two caps and the task-status cap have daemon-free
  evidence only.

## Native cell

`TestCephMonServiceMapLargeMapIntegration` (client suite, ordinary
three-MON/two-MGR fixture). It runs only when its exact selector is the test
binary's `-test.run` value, because its maps reach every ServiceMap watch of
the fixture; broad rows report it as skipped.

The holder gains two start steps beside `start`. Each registers the two
identity keys plus padding keys `ceph_msgr_pad_<index>` whose values derive
from the token:

| Step | Padding | Raw metadata |
| --- | --- | --- |
| `start-large` | 4096 keys, 360 bytes each | about 1.55 MiB |
| `start-wide` | 65537 keys, 8 bytes each | about 1.77 MiB |

Nothing else is selectable, and a token is started once with one step. The
holder, the native view and the Go test each compute the padding summary
(key count, key and value bytes, SHA-256 over the sorted pairs) on their own
side: the holder from what it passed to `rados_service_register`, the view
from the MON's `service dump`, the test by recomputing the padding from the
token and by summarizing the Go-decoded metadata. Receipts carry only the
summary.

Sequence:

1. Three Go clients open one watch each: default limits; a 256 KiB watch
   budget; `MaxFrameSize` 1 MiB. Each receives an ordinary map first. The
   budgeted watch leaves its map unread.
2. `start-large`. The native view must show the holder's GID and exactly
   the registered padding.
3. Default limits: the watch delivers the map. Required: the holder's GID;
   every one of the 4096 values equal to the recomputed padding; the Go and
   native summaries equal; the raw map above 1 MiB; and the retained raw
   map decoding on its own to the same daemon. The start epoch is compared
   only at the same map epoch, as in the lifecycle cell.
4. Watch budget: `Next` returns the unread ordinary map (at least one), then
   `ErrServiceMapOverflow` twice. The source session has not failed (ticket
   renewal may have retired it). The client keeps its GID and completes a
   `mon dump`. A new default-budget watch on the same client reads the same
   large map with the same checks.
5. Frame limit: `Next` returns `ErrLimitExceeded`, not the overflow error,
   twice. The client keeps its GID and completes a `mon dump` after
   reconnecting.
6. Release. Fresh native views must show the daemon absent on observations
   at least two seconds apart; the default watch delivers a newer map
   without it; the frame-limited client opens a new watch and reads that
   small map. Holder cleanup reports the baseline restored without a kill.
7. `start-wide` with a second token. The native view must show 65537
   padding keys. The default-limit watch ends with an error that is both
   `ErrLimitExceeded` and `ErrMalformedMessage`, twice, without delivering
   that daemon. The client keeps its GID and completes a `mon dump`.
8. Release, stable native absence, a new default-limit watch reading a map
   without either daemon, and restored cleanup as in step 6.

The usability command is `mon dump`, not `status`: the `status` reply embeds
the whole ServiceMap, so with the large registration present it is itself
larger than the frame-limited client's `MaxFrameSize`.

## First failure and correction

The first run of the cell passed steps 2 to 4 and failed in step 5. The
frame-limited watch had ended with the limit error; the following `status`
command then returned `request outcome unknown: ceph wire: length limit
exceeded`. The reply to `status` contains the ServiceMap with all 4096
metadata values and exceeded the client's 1 MiB frame limit on the recovered
session. That is the documented reply-limit behavior, reached by the test's
choice of command. The command became `mon dump`. No product code changed.
The later passing runs verify the corrected test; they do not explain that
failure.

## Native receipts

On 2026-11-10 UTC the cell passed without skips at
`d03baf3c89473e6c42ac27ed8d0ebb4ca379ab92`, run from an archive of exactly
that commit on a Linux/amd64 host with Docker 29.9.1 and Go 1.26.9, through
`integration/run.sh` with the catalog row's parameters and the result gate
(`tools/native_test_results.py`, verdict passed, one expected test, no
problems). Each row used a fresh fixture: three MONs, two MGRs, no OSD.

| Observation | secure/aes256k IPv4 container | CRC/AES IPv4 host |
| --- | --- | --- |
| Row elapsed / cell elapsed | 176 s / 134.65 s | 113 s / 78.69 s |
| Large registration: holder GID | 4482 | 14353 |
| Padding keys / bytes (holder, native dump and Go decode equal) | 4096 / 1,548,288 | 4096 / 1,548,288 |
| Go map epoch / native dump epoch | 2 / 2 | 2 / 2 |
| Raw map delivered with default limits | 1,582,295 bytes | 1,582,295 bytes |
| 256 KiB budget: maps drained, then cause | 1, `ErrServiceMapOverflow` | 1, `ErrServiceMapOverflow` |
| Budgeted client GID kept / new default watch | 4467 / epoch 2, 1,582,295 bytes | 4464 / epoch 2, 1,582,295 bytes |
| 1 MiB frame limit: watch cause after baseline epoch 1 | length limit exceeded | length limit exceeded |
| Frame-limited client GID kept, `mon dump` after recovery | 4470, yes | 4467, yes |
| After release: Go epoch / native epoch / raw | 6 / 6 / 22 bytes | 3 / 3 / 494 bytes |
| Frame-limited client's new watch | epoch 6, 22 bytes | epoch 3, 494 bytes |
| Wide registration: holder GID / native epoch | 14713 / 7 | 4539 / 4 |
| Native metadata keys / bytes | 65537 / 1,769,499 | 65537 / 1,769,499 |
| Default-limit watch cause | malformed frame, invalid service map, length limit exceeded | malformed frame, invalid service map, length limit exceeded |
| Default client GID kept, `mon dump` after recovery | 4464, yes | 4461, yes |
| After wide release: native epoch / new watch | 9 / epoch 9, 22 bytes | 8 / epoch 8, 22 bytes |
| Holder cleanup (both) | restored, not killed | restored, not killed |
| Owned containers terminated by the fixture | 7 | 7 |

After the rows, an independent Docker listing on the host showed no
remaining test container and no custom network.

The SHA-256 of the padding differs per run because the token is random; in
each run the three sides agreed on it. The 494-byte map after the first
release on the host row is a map without the owned daemon, which is all the
cell requires there; its other content was not inspected.

## Reproduction

Catalog rows:

- `mon-mgr.aes256k.ip4.t-monservicemaplargemap` (secure, container)
- `mon-mgr.aes.ip4.crc.host.t-monservicemaplargemap` (CRC, host)

The result gate requires
`client/TestCephMonServiceMapLargeMapIntegration` to PASS. On an authorized
Linux runner:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes256k CEPH_MSGR_TEST_SERVICE_CIPHER=aes256k \
CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephMonServiceMapLargeMapIntegration$' \
sh integration/run.sh
```

## Daemon-free evidence

These tests use an independently assembled encoding and, for the stream, a
synthetic MON peer. They are not native evidence.

- `TestDecodeServiceMapCollectionCaps`: each of the four collections admits
  exactly 65536 entries and refuses 65537 as a limit error with an empty
  result.
- `TestDecodeServiceMapFrameBound`: a map at front plus 41 bytes is admitted
  whole; one byte less of limit refuses it.
- `TestDecodeServiceMapPreservesRawMetadataAndOwnsItsBytes`: values that are
  not text (NUL, 0xff, 0x80) and of 256 KiB each, the GID and the raw map
  are preserved, and the result shares no memory with the received frame.
- `TestServiceMapWatchBudgetOverflowIsIsolated`: the budget boundary to the
  byte. A map exactly at the budget is retained and delivered; one byte
  more ends the watch with the overflow error; the MON session is the same
  object without an error and without a new dial; a new watch with one more
  byte of budget receives the same map; a caller's changes to a delivered
  snapshot reach no later map.
- `TestServiceMapSessionLimitEndsWatchAndRecoversClient`: a metadata count
  above the cap and a frame above `MaxFrameSize` each fail the session; the
  watch drains, then reports the limit error; the client recovers on
  another MON, which receives no ServiceMap subscription for the ended
  watch; a new watch registers there.
- `TestServiceMapOverflowKeepsAcceptedMapForLiveNext`: a canceled `Next` and
  `Close` discard neither the accepted map nor the earlier overflow cause.
- Holder and receipt tests (`tools/test_service_map_holder_oracle.py`,
  `TestServiceHolderPaddingMatchesTheHoldersPinnedSummary`,
  `TestServiceHolderReceiptsBindTheRegistrationSize`): the Python and Go
  padding agree on pinned summaries; a start receipt must report the size
  of its own step; a native view is the holder's only with exactly the
  registered padding.

Each stream and decoder check was run once against a temporary product
mutation and failed: budget check removed or off by one, overflow failing
the session, overflow dropping the pending map, decode error not ending the
session, caps doubled, raw map aliased to the frame, frame bound off by one.

## Remaining limits

- The daemon and service count caps and the task-status cap have no native
  evidence, for the reason above.
- A map near the 16 MiB default frame limit was not produced natively. The
  admitted native map is about 1.58 MiB.
- The cell does not cross a MON fault. Lifecycle across a MON Stop is the
  separate [lifecycle cell](SERVICE_MAP_LIFECYCLE_VALIDATION.md).
- A holder killed without release, whose entry stays until the MGR times it
  out, is not covered.
- Two profiles ran. Other key, address-family and runtime combinations are
  not covered.
