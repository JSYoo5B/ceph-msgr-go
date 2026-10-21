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

`handle_update` replaces metadata in the MGR's daemon-state index. It does not
replace the initial registration metadata in the MON's ServiceMap. Observing
that map or `service status` therefore cannot establish an applied metadata
update. A separate [native daemon-state oracle](../integration/fixture/MGR_SERVICE_METADATA_VALIDATION.md)
observes replacement and removal of omitted keys on selected profiles. Empty
updates are refused locally because the native handler ignores an empty map.

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
handling, duplicate and bound refusals, unconfirmed open and close, concurrent
`Close` callers, connection loss, failover, a service beside ordinary MGR
commands, and client close. These source-derived and synthetic checks are
**not** native encoder vectors and **not** live daemon evidence. Separately,
[TestMgrServiceNativeVectors](../internal/msgr/mgr_service_native_test.go) compares
eight retained fresh native vectors for all five messages, including omitted
versus empty report maps, binary values, configure and close decoding. The
[codec record](../internal/msgr/testdata/mgr-service-native-README.md) and
[hashed receipt](../internal/msgr/testdata/mgr-service-native-provenance.json)
define the source/build and offline-header scope; no daemon lifecycle follows
from these vectors.

### Native observation on selected profiles

`TestCephMgrServiceRegistrationIntegration` (`integration/mgr_service_test.go`)
registers a Go service daemon and has the native owned-pool oracle's
`servicewatch` mode (`tools/osd_pool_oracle.py`, public librados MON and MGR
commands) observe it independently of the Go client:

1. after `OpenMgrService`: the native MON `service dump` lists the daemon with
   the sent metadata and the client's authenticated GID, and the MGR's
   `service status` shows the sent status; the Go service map watch must show
   the same GID and metadata;
2. after queuing `UpdateMetadata` and `Report`: native status and task status
   equal the new report values, while the ServiceMap still carries the initial
   metadata. The separate native daemon-state query must observe the exact
   replacement, preserve it after a locally refused empty update, and remove
   omitted keys on a second nonempty replacement;
3. after an acknowledged `Close`: the daemon is absent from the native dump, and
   a report on the closed handle is refused locally.

It runs only in the opt-in profile `CEPH_MSGR_TEST_MGR_SERVICE=1` (single-OSD
legacy-v6 fixture, two MGRs), selected by the catalog group `mgr-service`:
`/ci run group:mgr-service`. Direct Linux runs passed CRC/aes/IPv4/host and
secure/aes256k/IPv6/container profiles; exact tested commits, execution gates,
cleanup and limits are in
[MGR_SERVICE_VALIDATION.md](../integration/fixture/MGR_SERVICE_VALIDATION.md).
These are direct native results, not a claim that the PR-comment workflow ran.

The parent test now also requires `renewal_and_active_manager_change`: it
observes a report after the original ticket expiry, a native active-MGR
transition, terminal old-handle refusals, sampled stable native absence and
caller-initiated registration on a new connection with the current ticket.
The [recovery record](../integration/fixture/MGR_SERVICE_RECOVERY_VALIDATION.md)
separates these observations from the earlier lifecycle and codec checkpoints.

The same parent now requires `unconfirmed_close` with two native-observed faults:
withheld receive bytes leave the caller uncertain even after native removal;
discarded writes leave the daemon visible after local teardown until native
beacon grace removes it. Both preserve cancellation, terminal state and the
repeated Close result. The [close record](../integration/fixture/MGR_SERVICE_CLOSE_VALIDATION.md)
keeps local write/dial counts separate from native receipt and sampled absence.

## Not verified

- Native vectors for nonempty daemon extensions and older configure versions
  (only the current v4 empty-extension service profile has a fresh vector).
- Profiles beyond the two key/mode/address/runtime combinations recorded above.
- Comparison with a native librados registration of the same fields.
- Failure configurations beyond the selected receive-withholding and write-
  discarding faults. A terminal handle after MGR replacement is a separate
  failure boundary.
- A registration made while the MGR is not yet active waits for the map; no live
  test covers that.

These remain open in #6.
