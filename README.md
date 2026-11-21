# ceph-msgr-go

A native Go communication layer for Ceph Tentacle: msgr2.1, CephX, raw
MON/MGR commands and subscriptions, explicitly targeted OSD requests, and
bounded MDS/CephFS wire primitives.

The product requires Go 1.25 or later and no CGO, go-ceph, librados, Ceph CLI,
or operating-system Ceph installation. Snappy and zstd codecs use the pure Go
`github.com/klauspost/compress` dependency. Native development tools belong to
the separate `integration/fixture` module.

The protocol reference is Ceph `v20.2.4`, commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Tentacle (20.2) is the minimum
release family. A successful connection does not certify every patch,
configuration, platform or higher-level storage operation.

## Which side of the connection Go implements

`Dial` makes the product the connecting CLIENT entity (`client.<id>`, CephX
entity type CLIENT). It opens outbound msgr2.1 connections to MON, MGR, OSD
and MDS daemons that already exist. The one daemon-role protocol so far
(stage 3 of [SPEC.md](SPEC.md) "Protocol stages") is opt-in and separate:
`DialMgrDaemon` authenticates a `mgr.<id>` principal to MON as the MGR entity
and carries that daemon's MON control channel (beacons, MgrMap role, ticket
renewal, rotating MGR secrets); `MgrDaemon.Listen` then accepts stock OSD
and client sessions as that MGR on a listener the consumer bound, verifying
each peer's CephX MGR service authorizer, decoding the MGR daemon messages
it sends (open, update, reports, PG statistics, close, commands) and
sending only what the consumer asks for (configure, the close
acknowledgement, command replies). No other role accepts connections, and
none of the other daemon-to-daemon messages (quorum, heartbeat, peering,
replication, recovery, metadata migration, MGR reports to MON) is sent. The
consumer that uses `DialMgrDaemon` supplies the daemon's decisions, beacon
content, peer admission, configuration and command results.

| Peer | What the Go client does | What it never does |
| --- | --- | --- |
| MON | Authenticates, runs raw commands and Tell, receives maps and subscriptions; with `DialMgrDaemon`, acts as a MGR daemon's MON channel | Join quorum or serve maps |
| MGR | Runs raw commands and Tell on the active MGR; optionally registers a service daemon as a CLIENT | Run modules, accept client sessions, aggregate reports |
| OSD and clients (as a MGR) | With `MgrDaemon.Listen`, accepts authenticated MGR sessions, decodes their reports, PG statistics and commands, and sends the consumer's configure, close acknowledgements and command replies | Aggregate statistics, execute commands or decide configuration |
| OSD | Sends object requests and raw commands to a target the caller names | Peer, replicate, recover, store objects or choose placement |
| MDS | Runs raw commands and the opt-in CephFS session, metadata and capability messages | Hold a rank, journal or migrate metadata |

MGR service registration is the one place where the client presents a service
name to a daemon. It is still a CLIENT session: the registration announces
caller-supplied metadata and status to the active MGR and makes nothing in
this repository a Ceph daemon.

The native test environment supplies every daemon. Tests start disposable
clusters from the pinned official Ceph image: MON, MGR, OSD and MDS daemons,
and in selected recipes an RGW gateway or an `rbd-mirror` daemon, together
with native clients (librados, librbd, libcephfs) that act as independent
oracles. Daemon-to-daemon paths are therefore exercised between real Ceph
daemons, never by Go. The fixture and its dependencies live in the separate `integration/fixture`
module and are not product dependencies.

## Repository scope

| Communication layer | Application responsibility |
| --- | --- |
| Framing, CephX, ticket renewal, sessions and recovery | Credential selection, keyring/configuration loading and operational retry policy |
| Raw command JSON, bulk input, original output and server codes | Command construction, schemas, response interpretation and typed management APIs |
| Wire maps, logs, config, digest and raw OSDMap delivery | Applying configuration, interpreting maps and using advertised service URIs |
| Explicit OSD connections, operation vectors and control events | CRUSH, placement, target selection, object APIs and explicit resubmission |
| MDS sessions, metadata, capabilities and recovery messages | Filesystem routing, inode/path/cache state, layout, writeback and consistency |

This is not a complete RADOS, RBD or CephFS client. Test-only consumers exercise
those underlying paths to verify communication. Requirements and completion
criteria are in [SPEC.md](SPEC.md). APIs follow the current specification
without compatibility wrappers for superseded APIs.

## Installation

```sh
go get github.com/JSYoo5B/ceph-msgr-go/cephmsgr
```

The import path is `github.com/JSYoo5B/ceph-msgr-go/cephmsgr`; the package name
is `cephmsgr`. There are no published module version tags, so Go resolves a
pseudo-version of the default branch; record the commit you verified. The
module needs Go 1.25 or later and builds with `CGO_ENABLED=0`.

## Quick start

The first connection needs MON addresses, an exact CephX identity and its
key. `Dial` authenticates to a MON and returns a client that can already run
MON and MGR commands.

```go
key, err := cephmsgr.ParseKey(encodedKey)
if err != nil {
    return err
}
bootstrap, stop := context.WithTimeout(context.Background(), 10*time.Second)
client, err := cephmsgr.Dial(bootstrap, cephmsgr.Options{
    Monitors: []string{"v2:192.0.2.10:3300/0", "v2:192.0.2.11:3300/0"},
    Identity: "client.management",
    Key: key,
    ExpectedFSID: clusterFSID,
})
stop()
if err != nil {
    return err
}
defer client.Close()
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
result, err := client.MonCommand(ctx, cephmsgr.Command{
    JSON: []byte(`{"prefix":"status","format":"json"}`),
})
if err != nil {
    return err
}
fmt.Printf("%s\n", result.Data)
```

`ParseKey` accepts the selected base64 CephX key, not a whole keyring.
The caller supplies the exact identity. A successful client's lifetime is
independent of the bootstrap context passed to `Dial`.

`Options.ConnectionMode` defaults to `SecureMode`. Select `CRCMode` explicitly
for CRC framing after authentication. Both modes use CephX and verify the
authentication transcript. CRC does not encrypt subsequent traffic; CRC32C
detects corruption. Connections offer only the selected mode, without fallback.

## Representative operations

Every operation below is a communication primitive with raw input and output.
The Opt-in column names the `Options` fields each row needs. A row marked
`none` works on any client returned by `Dial`; the OSD, MDS, MDS map and MGR
service rows are refused unless their option was set at `Dial`.

| Task | Entry point | Opt-in |
| --- | --- | --- |
| Run a MON or MGR command | `MonCommand`, `MgrCommand`, `MonTell`, `MgrTell`, `MonTellTo`, `MgrTellTo` | none |
| Follow cluster state | `Snapshot`, `WaitMonMap`, `WaitMgrMap`, `WatchLogs`, `WatchConfig`, `WatchDigest` | none |
| Receive raw OSDMaps from MON | `RequestOSDMaps`, `WatchOSDMaps` | none |
| Send an object request to one OSD | `OpenOSD`, then `OSDConnection.Request` | `EnableOSD`, optionally `OSDProfile` and `EnableOSDCachePools` |
| Send a raw OSD or PG command | `OpenOSD`, then `OSDConnection.Command` or `OSDConnection.PGCommand` | `EnableOSD` |
| Receive MDS maps | `WatchFSMapUser`, `WatchMDSMaps` | `EnableMDSMaps` |
| Send a raw MDS command | `OpenMDS`, then `MDSConnection.Command` | `EnableMDS` |
| Open a CephFS session and send metadata requests | `MDSConnection.OpenSession`, `MDSSession.Submit` | `EnableMDS`, `EnableCephFS` for metadata and capabilities |
| Register a service daemon with the active MGR | `OpenMgrService` | `EnableMgrService` |
| Act as a MGR daemon toward MON | `DialMgrDaemon`, then `SendBeacon`, `NextMgrMap`, `WaitRotatingKeys` | its own `MgrDaemonOptions` |
| Accept OSD and client sessions as that MGR | `MgrDaemon.Listen`, then `MgrListener.Accept`, `MgrPeer.Next`, `MgrPeer.Configure`, `MgrPeerCommand.Reply`, `MgrPeerClose.Acknowledge` | its own `MgrListenerOptions` with an `Authorize` hook |

The Go documentation of each name states its exact inputs, bounds, outcomes
and wire subset (`go doc github.com/JSYoo5B/ceph-msgr-go/cephmsgr`).

## Limitations

- The client never becomes a daemon and never accepts connections.
- It is not a RADOS, RBD or CephFS client. Callers supply OSD targets,
  placement and map epochs; there is no CRUSH computation, automatic routing,
  redirect following or automatic resend of an object request.
- Commands are raw. The product builds no command JSON, parses no command
  output and loads no keyring or configuration file.
- Outcome rules differ by API; the package documentation owns them.
  - A daemon command or an OSD request that fails after its transmission
    started returns an `OutcomeUnknownError`: the daemon may have executed it,
    and the client never sends it again on its own.
  - A CephFS request is different. `MDSSession.Submit` returns once the
    request is queued. A `Wait` or `WaitSafe` context that expires returns the
    bare context error while the call stays pending, so the caller can wait on
    the same call again instead of discarding it. On a resumable MDS
    connection, Messenger resume retransmits unacknowledged frames with their
    original sequence and transaction ID; that is delivery of the same
    request, not a new one. Only a session that ends before the reply resolves
    the call with an `OutcomeUnknownError`. Sending a request again
    (`MDSSession.ResubmitForwarded`) is always an explicit caller action.
  - Methods with other outcome rules, such as `MDSSession.SendCaps` and
    `OpenMgrService`, document them.
  - Canceling a local wait never establishes remote rollback.
- Admission requires an authenticated MonMap with `min_mon_release >= 20`
  (Tentacle, 20.2). A cluster that retains an older minimum, including one
  that is being upgraded, is rejected, and earlier release families have no
  compatibility path.
- Only implemented features are advertised. OSD opcodes outside the supported
  set are refused, and CephFS client feature bits that are not implemented
  are withheld.
- CRC connections negotiate snappy or zstd compression when the peer offers
  it; live compressed frames are unverified, and secure-mode compression and
  zlib are not implemented.
- Native verification covers selected profiles only; a result on one profile
  is not a claim about another. Completion criteria are in
  [SPEC.md](SPEC.md#verification-and-completion).

## Behaviour by area

- **Raw commands and Tell.** `MonCommand` and `MgrCommand` transport
  caller-built JSON and optional binary input and return the original output,
  status string and server code; negative codes return `*CommandError` with
  the result kept. `MonTell` and `MgrTell` address the current MON or active
  MGR; `MonTellTo` and `MgrTellTo` bind one exact daemon and never substitute
  another.
- **Maps and streams.** `Snapshot` copies MonMap, MgrMap and service state
  without network I/O, and `WaitMonMap`/`WaitMgrMap` wait for a strictly newer
  authenticated map. `RequestOSDMaps` and `WatchOSDMaps` deliver raw OSDMap
  batches from MON. `WatchLogs`, `WatchFSMapUser`, `WatchMDSMaps` and
  `WatchServiceMap` are bounded watches whose overflow ends only that watch;
  `WatchConfig` and `WatchDigest` keep only the newest unread value. Map
  reception is not placement, routing or mount success.
- **OSD.** Callers supply the address and ID, pool, locator, raw hash, map
  epoch and PG/shard placement. The product preserves request identity,
  epochs, per-op results, versions and raw data; it computes no CRUSH and
  never moves or resends a request on its own. Raw OSD and PG commands bind
  an explicit target.
- **MDS and CephFS.** Raw MDS commands go to an explicitly selected daemon.
  The opt-in CephFS profile provides sessions, Messenger resume, reconnect and
  bounded metadata, capability and push messages; it is not a mount, and the
  consumer owns path, inode, cache and writeback state.
- **RBD.** RBD has no wire role of its own: its class calls, watches,
  notifications and object I/O are OSD requests, and image layout, locking
  and journal policy belong to the consumer.
- **MGR service registration.** `OpenMgrService` registers caller-supplied
  service metadata and status with the active MGR on a dedicated CLIENT
  connection; the handle fails on connection loss or an active-MGR change and
  never re-registers by itself.
- **MGR daemon role.** `DialMgrDaemon` covers the MON control channel and
  `MgrDaemon.Listen` the OSD and client side. MON decides which daemon is
  active; the consumer decides availability, the advertised addresses, which
  authenticated peer it admits (no `Authorize` hook admits none) and what an
  active daemon does, and sends every beacon itself. A peer's messages are
  decoded but never interpreted; its report counters are read against that
  connection's own declarations. A close or command is answered at most
  once, through a handle bound to the connection that received it; an
  answer after that connection ended is refused, and nothing is resent on a
  replacement connection. MON and MDS peers are refused, and no report is
  sent to MON yet.

## Lifetime and recovery

- `MaxFrameSize` defaults to 16 MiB and `MaxInFlight` to 64.
  `ErrLimitExceeded` covers byte and collection bounds, and
  `ErrMalformedMessage` a complete invalid message.
- A context ends a local wait, not remote execution. A watch context owns the
  watch; a `Next` context owns only that wait.
- `Close` stops owned sessions and workers and releases pending callers; it
  does not turn an uncertain execution into a failure or a rollback.
- Ticket renewal re-authenticates to the MON that currently admits the
  client, keeping its GlobalID; other seeds and learned MonMap addresses are
  fallbacks. MON, MGR and OSD sessions have no cookie resume.
- Seeds accept `host:port` or `v2:host:port/nonce`, with bracketed IPv6;
  local IPv6 zones are refused. `Options.Hostname` is caller-supplied.
  `ExpectedFSID` pins the cluster; otherwise the first authenticated MonMap
  does.

## Development

```sh
CGO_ENABLED=0 go build ./...
CGO_ENABLED=0 go test ./...
CGO_ENABLED=0 go vet ./...
CGO_ENABLED=1 go test -race ./...
go -C integration/fixture test -mod=readonly ./...
python3 -m unittest discover -s tools -p 'test_*.py'
CGO_ENABLED=0 python3 tools/product_dependencies.py
```

`tools/product_dependencies.py` rejects any product dependency beyond the
compression module, native sources, module replacements and subprocess
dependencies. Native tests against disposable Ceph clusters live in
`integration/` and `cephmsgr/` and run on an authorized Linux runner; how
rows are declared and requested is in
[.github/scenarios/README.md](.github/scenarios/README.md), and the
repository rules for contributors and agents are in [AGENTS.md](AGENTS.md).
Unit, native codec parity, live interoperability and CI results are separate
evidence.

## Sources and licensing

Wire semantics are independently implemented in Go from the pinned Ceph source
and [Tentacle Messenger documentation](https://docs.ceph.com/en/tentacle/dev/msgr2/).
Ceph native sources/libraries are development-oracle inputs, not product dependencies.
See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md), [NOTICE](NOTICE) and
[LICENSE](LICENSE).
