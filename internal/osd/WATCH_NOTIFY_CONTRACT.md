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
placement, change epochs, redirect, retry or reconnect. Compound vectors,
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
`Ping`, `Unwatch` and `Acknowledge` are explicit requests. Caller-directed
cross-connection reattachment is not yet a public lifecycle operation, even
though the current RECONNECT wire union is encoded and independently tested.
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
publication. Local handles cannot be transferred between connections.

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
never reused. Concurrent `Next` calls each consume an event at most once;
an already canceled Next context consumes nothing. Client/connection shutdown
wakes all waiters and preserves queued events and the first terminal error.

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

Remaining live acceptance includes forced server DISCONNECT, timeout-driven
watch loss, scoped watch permissions, failover and cross-connection
caller-directed reconnect. Backoff/send gating
and daemon-to-daemon event paths are separate milestones. This implementation
does not advertise the combined RADOS_BACKOFF/CRUSH feature mask.
