# MON ping

`Client.PingMonitor(ctx, name)` sends one `MPing` to an exact bare MON name
in the authenticated MonMap and returns the raw Ceph string as `[]byte`.
Callers interpret its contents. It is the Messenger primitive used by
go-ceph `rados.Conn.PingMonitor`, not an ordinary MON command or a Messenger
keepalive. No keyring, configuration file or native client is needed by the
product.

Each invocation opens a private CephX-authenticated MON connection and checks
the release, FSID and name/address association before sending. The existing
connection mode applies without downgrade. A native diagnostic's ability to
answer before command capability checks does not bypass this admission.
Only the selected member's first valid v2 address is attempted. No automatic
endpoint fallback or replay occurs. Primary authentication, subscriptions,
command TIDs and the lazily opened MGR connection are preserved.

Both directions use message type 2 and TID zero. The native empty request is
version 1, compat 1, with no segments. The MON constructs a fresh reply, sets
an already encoded length-prefixed string as its front, then sends it; this
version 1 reply retains compat 0. Strict decoding accepts that producer
envelope, no middle/data, one complete bounded Ceph string and no trailing
bytes. Empty or binary strings remain raw wire values; the product does not
validate JSON or infer daemon health from them.

One connection per request isolates uncorrelated replies. Concurrent calls
share `MaxInFlight` with command and private MON calls. The operation context
covers setup, queue admission and the wait; `Client.Close` joins connection
cleanup. A failure before queue admission is known unsent. Loss or cancellation
after admission without an accepted reply returns `OutcomeUnknownError`.
Local cancellation implies no remote cancellation. A valid accepted reply is
preserved when completion races local cancellation or connection teardown.

Independently translated from pinned Ceph Tentacle 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`: `include/ceph_fs.h`,
`messages/MPing.h`, `msg/Message.cc`, `mon/Monitor.cc::handle_ping`, and
`mon/MonClient.{h,cc}`. Ceph source is LGPL-2.1; no native implementation is
copied into the product. The development-only C++ encoder invokes those native
APIs and records source/configuration/library and wrapper hashes. Offline
MessageFrame header projection is distinct from live ProtocolV2 validation.

Synthetic tests cover raw binary/copy bounds, malformed envelopes, isolated
concurrent replies, command TID continuity, private identities, shared slots,
cancellation, Close and admission refusal. Known command-reply types consumed
by the shared session dispatcher cannot become a ping reply; without a valid
ping the wait remains bounded by the caller's context. Native verification
and its configuration limits are recorded separately in
`integration/fixture/MON_PING_VALIDATION.md`.
