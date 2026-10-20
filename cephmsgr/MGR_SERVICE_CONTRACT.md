# Opt-in MGR service registration

Reference: Ceph Tentacle v20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`src/mgr/MgrClient.cc`, `src/mgr/DaemonServer.cc`, `src/messages/MMgrOpen.h`,
`MMgrUpdate.h`, `MMgrReport.h`, `MMgrConfigure.h`, `MMgrClose.h`; LGPL-2.1,
independently translated, no code copied). Product code stays native Go with
CGO disabled. This is a communication primitive, not an RGW or daemon
implementation.

## What this adds

`Options.EnableMgrService` permits `Client.OpenMgrService`, which registers one
service daemon (`MgrServiceOptions.Service` and `.Daemon`, with raw `Metadata`
and `Status`) on a **dedicated** connection to the active MGR. The ordinary MGR
command session, MON sessions, authentication and every Messenger feature mask
are unchanged: the option adds no feature bit, and an ordinary command-only
client still sends no open (pinned `MgrClient::reconnect` only opens for a
service daemon or a non-CLIENT entity).

| Message | Type | Header | Direction | Use |
| --- | --- | --- | --- | --- |
| MMgrOpen | 0x700 | 3/1 | sent | `service_daemon` true, caller metadata and status, empty config buffers |
| MMgrConfigure | 0x701 | 1-4 | received | statistics period/threshold; a CLIENT peer gets no OSD/MDS extension (`_send_configure`) |
| MMgrReport | 0x702 | 9/1 | sent | empty counter envelope plus optional daemon/task status; doubles as the liveness beacon |
| MMgrClose | 0x708 | 1/1 | sent and received | unregister; the MGR echoes the same message back |
| MMgrUpdate | 0x70b | 2/1 | sent | replacement metadata (the MGR ignores the status map of an update) |

The packed counter buffer of a report must be the empty versioned envelope
(version 1, compat 1, zero length): `DaemonPerfCounters::update` runs
`DECODE_START` on it even without counters. `daemon_status`/`task_status` are
`boost::optional` maps: a nil Go map omits the field (the MGR keeps its previous
value), a non-nil map replaces it.

## Lifecycle

1. `OpenMgrService` validates names (1-255 bytes, no NUL), map size (1024
   entries, 64 KiB per string, 1 MiB per message) and rejects a duplicate
   identity or more than `MaxMgrServices` (16) before sending anything.
2. It dials the current active MGR, binds the handle to that daemon's name, GID,
   address vector and the client's authenticated identity, queues MMgrOpen and
   returns when the MGR's MMgrConfigure arrives. `handle_open` sends that push
   first, so it proves dispatch, not ServiceMap visibility; use a ServiceMap
   stream for visibility. A cancellation, timeout or connection loss after the
   open was queued returns `ErrMgrServiceOutcomeUnknown`.
3. While open, the client sends an empty report at the period the MGR configured
   (the pinned `mgr_service_beacon_grace` drops a silent service daemon; period 0
   means no reports), unless `DisableBeacon` leaves liveness to `Report`.
   `UpdateMetadata` and `Report` return when the message is queued: the MGR sends
   no reply, so applied-versus-lost stays unknown if the session later fails.
4. The handle fails, and never reconnects by itself, when the connection ends,
   a protocol error occurs, or the active MGR or authenticated identity changes
   (`ErrManagerChanged`). Registering again is an explicit new `OpenMgrService`
   call and is never combined with uncertain-command replay.
5. `Close` queues MMgrClose and waits for the echo (bounded by its context). With
   no echo it still tears the connection down and returns
   `ErrMgrServiceCloseUnconfirmed`: the MGR may keep the daemon until the beacon
   grace expires. Closing the client ends all handles.

The pinned `handle_open` performs no capability check on the opener (session
capabilities are consulted for commands only), so no permission-denial result is
defined for registration itself.

## Evidence

Source-derived byte vectors for every message (`internal/msgr/mgr_service_test.go`),
and unit and race tests against a synthetic MGR that shares the Go frame codec
(`cephmsgr/mgr_service_test.go`): open/update/report/close bytes, configure
handling, duplicate and bound refusals, unconfirmed open and close, connection
loss, failover, a service beside ordinary MGR commands, and client close.
These are **not** native encoder vectors and **not** live daemon evidence.

## Not verified

- Fresh native request vectors for each message.
- A Go-originated registration observed natively: ServiceMap visibility, status
  and metadata, update, removal after Close.
- Service-ticket renewal, MGR failover and bounded close against real daemons,
  and the comparison with a native librados service registration.
- A registration made while the MGR is not yet active waits for the map; no live
  test covers that.

These need a disposable Linux fixture and remain open in #6.
