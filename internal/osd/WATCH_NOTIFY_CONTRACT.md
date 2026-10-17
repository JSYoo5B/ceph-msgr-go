# Tentacle WATCH / NOTIFY transport

Reference: Ceph 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Independently translated from
`include/rados.h`, `osdc/Objecter.h`, `messages/MWatchNotify.h`,
`osd/PrimaryLogPG.cc` and `osd/Watch.cc`. Those source files carry LGPL-2.1
notices; no upstream implementation is copied. Native tools remain development
dependencies. Product builds and runtime remain native Go without CGO.

## Wire operations and scope

The bounded subset accepts a single head-object operation, explicit caller
pool/object/namespace/key/hash, target OSD and map epoch. It does not calculate
placement, change epochs, redirect or retry. Reconnect is an explicit caller
operation described below. Compound vectors,
snapshot inputs and operation flags are rejected. WATCH is write-mode but
does not imply a user-visible object version change: the pinned OSD explicitly
excludes it from `user_modify`. Successful WATCH replies must satisfy the
existing requested OSD ONDISK completion contract. NOTIFY and NOTIFY_ACK are
read-mode operations: their ordinary success does not establish object durability
or replace asynchronous notify completion. Messenger ACK is unrelated.

| Opcode | Packed 28-byte union | Input data | Successful ordinary output |
| --- | --- | --- | --- |
| WATCH `0x220f` | cookie u64, unused version u64 (zero), action u8, registration generation u32, timeout u32, three zero bytes | empty | empty |
| NOTIFY `0x1206` | cookie u64, twenty zero bytes | protocol u32 (1), timeout seconds u32, length-prefixed binary blob | assigned notify ID u64 |
| NOTIFY_ACK `0x1207` | twenty-eight zero bytes | notify ID u64, watch cookie u64, length-prefixed binary reply | empty |

Current action values are UNWATCH 0, WATCH 3, RECONNECT 5 and PING 7. The codec
preserves registration generation separately from local connection generation.
Legacy WATCH action 1 is rejected. The public `WatchObject` path registers;
`Ping`, `Unwatch` and `Acknowledge` are explicit requests. `ReconnectWatch`
reattaches a previously confirmed registration to a caller-selected newer
connection without a fresh WATCH fallback.
Public `Request` rejects these three opcodes before admission. Only a stream's
private submission path can transmit them, after matching its connection,
reserved cookie, object and client incarnation; arbitrary caller cookies cannot
collide with a stream's event association.

## Public lifetime

`WatchObject` and `NotifyObject` take an `OSDRequest` containing routing fields
and no Operations. They reserve a fresh client-wide cookie and install delivery
before submitting the request, so a fast event cannot outrun registration.
The request context controls the operation wait, not the returned stream.
Cancellation never proves remote cancellation, unwatch or rollback. A reserved
handle is returned together with submission errors, including unknown outcomes,
so its cookie and explicit cleanup remain available. There is no automatic
registration, notify replay, ping scheduling or retry after ticket renewal or
connection loss.

Each stream belongs to one authenticated OSD connection and local session
generation. Events expose OSD target, authenticated client global ID, local
session generation, watch/notify cookie and wire notify ID. A connection's
local generation is independent of the WATCH registration generation. Delivery
checks the current authenticated source both before decoding and before queue
publication. A stream remains bound to that connection; an explicit reconnect
creates a new stream with the same logical watch cookie.

For example, after the caller has prepared `route` with no Operations:

```go
watch, registration, err := connection.WatchObject(ctx, route,
    cephmsgr.OSDWatchOptions{TimeoutSeconds: 37})
if err != nil {
    // Inspect registration.Submission. A non-nil watch retains its cookie
    // for an explicit cleanup decision after an uncertain sent outcome.
    return fmt.Errorf("watch registration %d: %w", registration.Submission.Transaction, err)
}
defer watch.Close()
event, err := watch.Next(ctx)
if err != nil {
    return err
}
if event.Opcode == cephmsgr.OSDWatchNotification {
    // Handle event.Payload, then elect to acknowledge it explicitly.
    if _, err := watch.Acknowledge(ctx, callerSelectedEpoch, event, replyBytes); err != nil {
        return err
    }
}
_, err = watch.Unwatch(ctx, callerSelectedEpoch)
return err
```

The consumer schedules `Ping` when it wants registration renewal. A deferred
local Close does not replace an explicit Unwatch decision.

`Close` only stops local delivery and releases a stream slot; it sends no
operation. `Unwatch` submits removal to the same explicit OSD, with the caller's
selected map epoch, then closes local delivery after a successful server reply.
An uncertain unwatch result does not establish removal. Explicit cleanup may
still be submitted using a locally closed handle. `Ping` renews only the existing
remote registration; it does not revive local delivery or reconnect.

At most 64 active streams exist per connection. Each queue holds at most 64
events and a caller-selected byte estimate; its default is MaxFrameSize, and
its allowed range is 64 bytes through MaxFrameSize. The estimate includes raw
front/data and conservative event/slice metadata. It is not an exact Go heap
limit. Input frames remain bounded independently by the session frame limit.

An overflow terminates only that stream with `ErrOSDWatchOverflow`; accepted
events drain before the first terminal cause. It does not implicitly unwatch,
acknowledge or fail unrelated requests. After terminal delivery, later valid
events for the retired cookie are discarded. Locally generated cookies are
never reassigned to another logical registration. A `(cookie, local session
generation)` pair is never reused, including by explicit reconnect.
Concurrent `Next` calls each consume an event at most once;
an already canceled Next context consumes nothing. Client/connection shutdown
wakes all waiters and preserves queued events and the first terminal error.

## Explicit watch reconnect

`newConnection.ReconnectWatch(ctx, previous, route)` accepts an `OSDWatch`
with a private receipt installed only after an initial ordinary WATCH success.
An initial WATCH with an unknown outcome does not supply that proof. The target
must belong to the same Client and current authenticated GlobalID, and its
local session generation must be newer than every connection previously used
by this logical registration. The object identity (including supplied hash)
and client incarnation must match; the caller can update map epoch and
placement. The route contains no Operations. Arbitrary cookies and manually
constructed handles do not confer reconnect authority.

The new stream inherits the normalized queue limits and retains the cookie.
Its WATCH registration generation increments without wraparound. Admission
installs the new consumer before one RECONNECT request, fences the prior
handle's Ping/Unwatch/ACK locally, and terminates the old stream with
`ErrOSDWatchSuperseded` unless it already has a terminal cause. Accepted old
events drain before that cause; accepted new events remain available even if
the ordinary reconnect reply is lost. These queues retain their independent
bounds. The OSD's original watch timeout remains in force; no timer schedules
PINGs in Go.

Pre-admission errors return no new handle or submission. After local ownership
transfer, the returned handle owns the receipt even if its operation fails or
has an unknown outcome. Caller wire inputs, including epoch, modification time
and encoder bounds, are validated with the negotiated encoder before waiting
for the control gate or changing ownership, generation or stream slots. There
is no rollback to the prior stream. Another
explicit attempt must use the latest returned handle and a newer connection.
Concurrent registration controls are serialized with a context-aware gate;
cancellation while waiting for that gate leaves ownership unchanged. A
successful UNWATCH retires the receipt. An uncertain sent UNWATCH also prevents
reconnect because remote removal is unresolved, while explicit cleanup remains
available on the current handle. No path automatically registers, reconnects,
replays NOTIFY or discovers a target.

This follows pinned `Objecter.cc` `_send_linger` (RECONNECT and incremented
`register_gen`) and `PrimaryLogPG.cc` WATCH handling (lookup by cookie and
authenticated entity; missing registration yields signed `-ENOTCONN`). The
server can also return `-ENOENT` for a missing object; these errors are
preserved without a WATCH fallback. `Watch.cc` `connect` can redeliver pending
notifications. Consumers therefore decide deduplication across generations;
an old-generation event cannot be acknowledged through the new handle.
`MWatchNotify` has no registration-generation field, which is why a reconnect
cannot return its cookie to an already used physical session.

## Separate asynchronous events

`MWatchNotify` is message type 44, current header version 3/compatibility 1.
The product accepts that current envelope and payload version 1, not historical
header branches. Fields are payload version u8, opcode u8, cookie u64, obsolete
version u64, notify ID u64, front blob, signed return code i32 and notifier global
ID u64. Front notification bytes remain separate from message data. Unknown
opcodes, wrong versions, malformed lengths/counts or inconsistent stream kind
fail the OSD session as protocol errors, preserving uncertain sent requests.

| Event | Handling |
| --- | --- |
| NOTIFY = 1 | Deliver binary front payload and full identity. Neither receipt nor `Next` sends an ACK. |
| NOTIFY_COMPLETE = 2 | Deliver signed status and message-data map `(watcher global ID,cookie) -> binary reply`, followed by missed identities. The front blob must be empty. Terminate the notifier stream after that event with `ErrOSDNotifyComplete`. |
| DISCONNECT = 3 | Deliver the event, then terminate that watch stream with `ErrOSDWatchDisconnected`. This does not prove remote unwatch or notify success. |

A NOTIFY operation reply and asynchronous completion are separate receipts.
Either may arrive first. When the ordinary operation reply succeeds, the
completion ID is checked against its returned notify ID before the public
notifier handle is returned. If that reply is lost or its local wait is
canceled, a previously accepted event can still be retained with the unknown
operation outcome; no reply ID is available to corroborate it. A completion
event whose message-header TID collides with a pending call cannot complete
that call. `-ETIMEDOUT` completion retains partial replies and all missed
identities; the event is delivered successfully so the caller can inspect its
signed status and raw bytes. The terminal `ErrOSDNotifyComplete` does not erase
or reinterpret that status.

`Acknowledge` requires a notification from the handle's cookie, authenticated
OSD/client and session generation, then submits user-selected bytes explicitly.
The ordinary ACK reply preserves server codes and submission uncertainty.
Local callback policy, deduplication across caller decisions, lock schemas and
re-registration decisions belong to the consumer. An ACK after a notify timeout
does not retroactively change the completion already delivered.

## Verification boundaries

The independent native encoder constructs six current actions/operations,
six corresponding replies and four current events. It invokes actual native
payload encoders on fresh objects, separately decodes the results, and emits
native message/header evidence. Binary NOTIFY/ACK, empty replies, complete and
partial-timeout reply maps are distinct vectors. Native event fields are
checked in the wrapper; an empty decoded-operation list is expected for events,
which are not MOSDOp operation vectors. Header sequence/TID/ACK inputs are
explicit offline test inputs, not evidence of a live Messenger session.

Synthetic tests cover event/request TID collision, completion-before-reply,
explicit ACK, authenticated identity changes between decode and publication,
queue overflow, cancellation, local close and source metadata. These tests
alone do not establish daemon interoperability.

Reconnect synthetic race tests additionally cover receipt admission, current
GlobalID, generation exhaustion, handoff during a pending reply, immediate old
control fencing, accepted events on both streams, reply loss, explicit retry
after an unknown outcome, signed `-ENOTCONN` without registration fallback,
context cancellation while waiting for a control and uncertain UNWATCH.
`TestCephOSDWatchReconnectIntegration` is the independent native acceptance
recipe; its two successful executions are recorded below and separately from
these synthetic checks.

`TestCephOSDWatchNotifyIntegration` supplies the live acceptance recipe:
independent native registrations receive Go NOTIFY; a Go registration explicitly
PINGs; a Go notification timeout preserves native replies and the unacknowledged
Go watcher; an independent native notifier exchanges binary data and an explicit
Go ACK; explicit Go UNWATCH leaves the native registrations intact. Its retained
Go operation/event record and native token receipts keep notifier/watcher
identities separate.

The final 2026-10-15 executions passed all five watch/notify cases in each of
these pinned Ceph 20.2.4 profiles, with evidence export and fixture cleanup:

| Verified profile | Watch/notify result |
| --- | --- |
| Darwin arm64 host, IPv4 relay, CRC, AES client/service tickets, three BlueStore replicas | 5/5 passed, no skips |
| CGO=0 Linux arm64 container, IPv6, secure, AES256K client/service tickets, one MemStore | 5/5 passed, no skips |

Each run retained five ordinary operation receipts and four separate events.
The combined PGNLS/SPARSE_READ/watch suite passed 19/19 cases per profile; this
is not a count of watch cases. The independent native notifier used
`rados_notify2` and the native response decoder with explicit binary lengths.
Earlier Cython notifier failures, their correction, native wire provenance and
the reproduction commands are recorded in
[the O3 acceptance checkpoint](../../integration/fixture/OSD_O3_VALIDATION.md).
These local runs do not establish remote CI results or support for other daemon
configurations or platforms.

The separate 2026-10-15 reconnect acceptance passed one test without skips in
each of these pinned Ceph 20.2.4 profiles, with evidence export and fixture
cleanup:

| Verified reconnect profile | Result |
| --- | --- |
| Darwin arm64 host, IPv4 relay, CRC, AES client/service tickets, three BlueStore OSDs, object-hash v6 requests | 1/1 passed |
| CGO=0 Linux arm64 container, IPv6, secure, AES256K client/service tickets, one MemStore OSD, replicated-PG v8 requests | 1/1 passed |

Both preserve the native-listed cookie and authenticated entity across an
explicit connection close and reconnect, with WATCH registration generation
9 to 10 and local session generation 1 to 2. Each independently completes two
native binary notifications (one per connection) with one explicit Go ACK and
no missed watcher, confirms local old-handle/old-event fencing, then verifies
new-handle UNWATCH, an empty native watch list and unchanged object bytes.
The retained ordinary replies, separate events, native receipts and
reproduction commands are described in
[the reconnect acceptance checkpoint](../../integration/fixture/OSD_WATCH_RECONNECT_VALIDATION.md).
These successful daemon paths do not directly exercise the later malformed
route preflight correction; its ownership-preservation coverage is synthetic.
Unknown reconnect replies, uncertain UNWATCH, overflow and concurrent handoff
also remain synthetic coverage, rather than native results from these runs.

Server DISCONNECT is live-verified for its two connected-watcher causes, a
missed ping timeout and deletion by another client
([record](../../integration/fixture/OSD_WATCH_DISCONNECT_VALIDATION.md)).
Explicit RECONNECT to a replacement primary after SIGKILL is live-verified
([record](../../integration/fixture/OSD_WATCH_RECONNECT_VALIDATION.md)).
Remaining live acceptance includes scoped watch permissions and
pending-notify redelivery across a reconnect. The local generation fence does not establish
server-side generation rejection. Backoff/send gating has its
[own contract](BACKOFF_CONTRACT.md); daemon-to-daemon event paths remain a
separate milestone.
