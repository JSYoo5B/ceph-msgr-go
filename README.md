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

## Where to start

| Reader | Start here |
| --- | --- |
| Package user | [Which side Go implements](#which-side-of-the-connection-go-implements), [Installation](#installation), [Quick start](#quick-start), [Representative operations](#representative-operations) and [Limitations](#limitations) |
| Protocol implementer | [Tracing one feature](DOCUMENTATION.md#tracing-one-feature): public contract, pinned wire reference, implementation status, scenario, observed native result and remaining limits |
| Fixture or CI contributor | [Fixture and CI entry points](DOCUMENTATION.md#fixture-and-ci-contributors), the [scenario catalog guide](.github/scenarios/README.md) and the [native path coverage map](integration/fixture/NATIVE_PATH_COVERAGE.md) |

[DOCUMENTATION.md](DOCUMENTATION.md) names the authoritative document for each
kind of information. This README summarizes usage and feature boundaries; when
a summary here and an owning contract differ, the contract is correct.

## Which side of the connection Go implements

The product is always the connecting CLIENT entity (`client.<id>`, CephX
entity type CLIENT). It opens outbound msgr2.1 connections to MON, MGR, OSD
and MDS daemons that already exist. It has no listening Messenger, accepts no
connection, and implements no daemon role: it cannot act as a MON, MGR, OSD or
MDS, and it sends none of the daemon-to-daemon messages (quorum, heartbeat,
peering, replication, recovery, metadata migration).

| Peer | What the Go client does | What it never does |
| --- | --- | --- |
| MON | Authenticates, runs raw commands and Tell, receives maps and subscriptions | Join quorum or serve maps |
| MGR | Runs raw commands and Tell on the active MGR; optionally [registers a service daemon](#mgr-service-registration) as a CLIENT | Become an MGR or a module |
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
daemons, never by Go; see the daemon-to-daemon rows of the
[coverage map](integration/fixture/NATIVE_PATH_COVERAGE.md#daemon-to-daemon).
The fixture and its dependencies live in the separate `integration/fixture`
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
Paths beyond MON and MGR commands are opt-in through `Options`.

| Task | Entry point | Opt-in | Section |
| --- | --- | --- | --- |
| Run a MON or MGR command | `MonCommand`, `MgrCommand`, `MonTell`, `MgrTell`, `MonTellTo`, `MgrTellTo` | none | [Raw commands](#raw-commands-and-targeted-tell) |
| Follow cluster state | `Snapshot`, `WaitMonMap`, `WaitMgrMap`, `WatchLogs`, `WatchConfig`, `WatchDigest` | none | [State snapshots](#state-snapshots-and-map-waits), [logs](#mon-logs), [configuration](#mon-configuration), [digest](#mon-health-and-digest) |
| Receive raw OSDMaps from MON | `RequestOSDMaps`, `WatchOSDMaps` | none | [MON OSDMap requests](#mon-osdmap-requests-and-subscriptions) |
| Send an object request to one OSD | `OpenOSD`, then `OSDConnection.Request` | `EnableOSD`, optionally `OSDProfile` and `EnableOSDCachePools` | [OSD object requests](#osd-object-requests) |
| Send a raw OSD or PG command | `OSDConnection.Command`, `PGCommand` | `EnableOSD` | [OSD and PG commands](#osd-and-pg-commands) |
| Receive MDS maps | `WatchFSMapUser`, `WatchMDSMaps` | `EnableMDSMaps` | [MDS maps](#mon-fsmapuser-and-mdsmap-subscriptions) |
| Send a raw MDS command | `OpenMDS`, then `MDSConnection.Command` | `EnableMDS` | [MDS commands](#mds-commands) |
| Open a CephFS session and send metadata requests | `MDSConnection.OpenSession`, `MDSSession.Submit` | `EnableMDS`, `EnableCephFS` for metadata and capabilities | [CephFS sessions](#cephfs-sessions), [metadata](#cephfs-metadata-and-push-messages) |
| Register a service daemon with the active MGR | `OpenMgrService` | `EnableMgrService` | [MGR service registration](#mgr-service-registration) |

The Go documentation of each name states its exact inputs, bounds and
outcomes. The contracts linked from each section define the wire subset.

## Limitations

- The client never becomes a daemon and never accepts connections; see
  [Which side of the connection Go implements](#which-side-of-the-connection-go-implements).
- It is not a RADOS, RBD or CephFS client. Callers supply OSD targets,
  placement and map epochs; there is no CRUSH computation, automatic routing,
  redirect following or automatic retry.
- Commands are raw. The product builds no command JSON, parses no command
  output and loads no keyring or configuration file.
- A request that fails after transmission started has an unknown outcome and
  is never sent again automatically. Local cancellation is not remote
  rollback.
- Only Tentacle (20.2) and later clusters are admitted
  (`min_mon_release >= 20`). Earlier release families have no compatibility
  path.
- Only implemented features are advertised. OSD opcodes outside the supported
  set are refused; see [unsupported opcodes](internal/osd/UNSUPPORTED_OPCODES.md).
  Withheld CephFS client feature bits are listed in the
  [client extension decisions](internal/mds/CLIENT_EXTENSION_DECISIONS.md).
- Live compressed frames, secure-mode compression and zlib are unverified or
  deferred; see [compression](internal/msgr/COMPRESSION_PLAN.md).
- Native verification covers selected profiles only. Each contract and
  validation record names the profile and source revision it observed; a
  result on one profile is not a claim about another. Completion criteria are
  in [SPEC.md](SPEC.md#verification-and-completion).

## Raw commands and targeted Tell

`MonCommand` and `MgrCommand` transport caller-built JSON and optional
`Command.Input`. The product checks structure and bounds without interpreting
command-specific schemas. `MgrCommand` authenticates to the active MGR.

`Result.Data` is binary-capable original output, `Message` is the status string,
and `Code` is the original server code. Negative codes return `*CommandError`
while preserving the result. Codes are not translated into host OS errno values.
Explicit server authentication refusal returns `*AuthenticationError`.

`MonTell` and `MgrTell` address the current MON or active MGR. `MonTellTo`
binds an exact bare MON name through a separate authenticated connection and
checks FSID, minimum release, name and original address. It preserves the main
connection, identity, subscriptions and tickets. Another MON is never substituted.

`MgrTellTo` binds the exact available active MGR name, GID, original address
vector and client identity. It shares the existing session and command slots.
Unknown, standby, unavailable and changed targets are rejected explicitly,
without waiting for promotion. See [the contract](cephmsgr/MGR_TELL_CONTRACT.md).

A context ends the local wait, not remote execution or rollback. Failures after
transmission starts may return `OutcomeUnknownError`. The client never
re-executes an uncertain command automatically. Messenger ACK is not completion.

## MON OSDMap requests and subscriptions

`RequestOSDMaps(ctx, OSDMapRequest{})` requests the latest full map. Inclusive
full/incremental epoch ranges are also available. A range is disabled by two
zeros or satisfies `0 < First <= Last`. No OSD ticket or `EnableOSD` is required.

Batches retain FSID, trim/latest epochs, ordered blobs and original front bytes.
Embedded maps remain opaque. Native limits can produce partial or empty batches;
`NewestMap` is not a completion cursor. Independent authenticated connections
provide request isolation because the protocol has no correlation ID. They
share `MaxInFlight` without replacing the main MON session.

`WatchOSDMaps` allows one continuous watch per client. Zero requests a current
full baseline; positive start epochs request inclusive history. Its FIFO holds
at most 64 batches and a conservative retained-byte budget. The default budget
is `MaxFrameSize`, with a range of 1 KiB to 1 GiB. Overflow terminates the watch
rather than silently dropping a map. Local registration is not a capability ACK.

Consumers own baseline application, continuity, trim handling, placement and
additional requests. See [range requests](cephmsgr/mon_osd_request.go),
[MON watches](cephmsgr/mon_osd_maps.go) and [OSD delivery](cephmsgr/osd_maps.go).

## MON FSMapUser and MDSMap subscriptions

The opt-in MDS path receives filesystem, rank and daemon identities, epochs,
addresses and raw wire metadata through MON. Reception is not filesystem
selection, mount success or routing. See [the map contract](cephmsgr/MDS_MAPS_CONTRACT.md)
and [native observations](integration/fixture/MDS_MAPS_VALIDATION.md).

## MDS commands

The MDS service path authenticates to an explicitly selected daemon and transports
raw commands with bounded input/output, server errors and uncertain outcomes.
It does not expand wildcards or silently select another rank. Identity and
map-generation changes stay visible. See [the contract](cephmsgr/MDS_COMMAND_CONTRACT.md)
and [validation](integration/fixture/MDS_COMMAND_VALIDATION.md).

## CephFS sessions

The opt-in profile provides OPEN, renewal, CLOSE, selected reclaim messages,
Messenger resume and explicit new-incarnation reconnect state. It is not a mount.
Consumers supply capabilities, realms and pending request/flush state. Same-TID
resubmission is explicit; unsafe requests retain REPLAY/target-inode semantics,
and unanswered requests retain retry identity. Recovery must not repeat completed
OSD writes or infer success from a new session alone.

See [session semantics](cephmsgr/MDS_SESSION_CONTRACT.md),
[fresh connections](internal/session/MDS_FRESH_CONTRACT.md),
[session validation](integration/fixture/MDS_SESSIONS_VALIDATION.md) and
[recovery](integration/fixture/MDS_CEPHFS_RECOVERY_VALIDATION.md).

## CephFS metadata and push messages

Bounded codecs expose capabilities, leases, forwarding, unsafe/safe completion,
snapshots and reconnect messages. Consumers own path/inode/cache state, writeback
and explicit acknowledgements. A transport ACK, metadata reply and completed
writeback have different meanings.

Test-only consumers compare selected metadata and striped object I/O paths with
libcephfs, including failover, forwarding, capability handoff and cap-snapshot
recovery. These profiles do not establish complete POSIX/CephFS, arbitrary dirty
state or all-rank recovery. See [the public contract](cephmsgr/MDS_CEPHFS_CONTRACT.md),
[wire details](internal/mds/CONTRACT.md), [file data](integration/fixture/MDS_CEPHFS_DATA_VALIDATION.md),
[explicit resubmission](integration/fixture/MDS_CEPHFS_CAPSNAP_RESUBMIT_VALIDATION.md)
and [flush deduplication](integration/fixture/MDS_CEPHFS_CAPSNAP_DEDUPE_VALIDATION.md).

## RBD transport primitives

Test-only consumers exercise class payloads, snapshots, sparse reads, watches,
notifications, image metadata, writes, locks, object maps, clones and journal
replay against librbd. The product transports bytes; image layout, striping,
parent/copy-up, lock policy, groups, mirroring and migration belong to the consumer.
See [base primitives](integration/fixture/RBD_BASE_VALIDATION.md),
[writes](integration/fixture/RBD_WRITE_VALIDATION.md), [locks](integration/fixture/RBD_LOCK_VALIDATION.md),
[clones](integration/fixture/RBD_CLONE_VALIDATION.md) and [journaling](integration/fixture/RBD_JOURNAL_VALIDATION.md).

## OSD object requests

Callers supply the address/ID, pool, locator, raw hash, map epoch and required
PG/shard placement. The product does not compute CRUSH, apply maps, choose targets
or move a request after a redirect. Requests preserve identity, incarnation,
attempt, epochs, per-op results, object versions, flags and raw data.

Support depends on request/reply version, negotiated features and configuration:

| Area | Contracts |
| --- | --- |
| Core operations and versions | [core](internal/osd/CONTRACT.md), [request v8](internal/osd/REQUEST_V8_CONTRACT.md), [request v9](internal/osd/REQUEST_V9_CONTRACT.md), [reply v8](internal/osd/OSD_REPLY_V8_CONTRACT.md) |
| Completion and recovery | [compound operations](internal/osd/O4_CONTRACT.md), [resubmission](internal/osd/RESUBMIT_CONTRACT.md), [fullness](internal/osd/FULL_CONTRACT.md) |
| Metadata and conditions | [metadata](internal/osd/METADATA_CONTRACT.md), [OMAP](internal/osd/OMAP_CONTRACT.md), [allocation hints](internal/osd/ALLOC_HINT_CONTRACT.md) |
| Object data | [checksums](internal/osd/CHECKSUM_CONTRACT.md), [sparse reads](internal/osd/SPARSE_READ_CONTRACT.md), [MAPEXT](internal/osd/MAPEXT_CONTRACT.md), [CALL](internal/osd/CLASS_CONTRACT.md), [COPY_FROM](internal/osd/COPY_FROM_CONTRACT.md), [WRITESAME](internal/osd/WRITESAME_CONTRACT.md) |
| Snapshots and cache flags | [snapshots](internal/osd/SNAPSHOT_CONTRACT.md), [manifest](internal/osd/MANIFEST_CONTRACT.md), [dirty state](internal/osd/DIRTY_CONTRACT.md), [cache pools](internal/osd/CACHE_POOL_CONTRACT.md) |
| Enumeration and events | [PG listing](internal/osd/PG_LIST_CONTRACT.md), [v8 listing](internal/osd/PG_LIST_V8_CONTRACT.md), [watchers](internal/osd/WATCHERS_CONTRACT.md), [watch/notify](internal/osd/WATCH_NOTIFY_CONTRACT.md), [backoff](internal/osd/BACKOFF_CONTRACT.md), [events](internal/osd/EVENT_CONTRACT.md) |
| Placement profiles | [replica reads](internal/osd/REPLICA_READ_CONTRACT.md), [erasure coding](internal/osd/ERASURE_CONTRACT.md) |

Native recipes compare bytes, versions, metadata and completion flags, including
selected replicated-primary failures and EC configurations. They do not prove
power-loss durability, every CRUSH rule, all EC profiles or every platform.
Compound vectors have only the selected Ceph contract's ordering/completion semantics.

EC requests require explicit shard information. The narrow CRUSH_V2 admission
bit does not claim CRUSH computation. MSR and unsupported required features are
rejected. Replica reads and optimized EC shard numbering have separate contracts.

## OSD and PG commands

Raw commands bind an explicit target and preserve binary input/output, server
codes and uncertainty. PG interpretation, discovery and typed administration
belong to the consumer. See [command validation](integration/fixture/OSD_COMMAND_VALIDATION.md).

## MGR service registration

`Options.EnableMgrService` permits `OpenMgrService`, an explicit service-daemon
registration (MMgrOpen/Update/Report/Close) on a dedicated connection to the
active MGR, with caller-supplied raw service/daemon names, metadata and status.
It changes no ordinary MON/MGR command behavior or feature mask. The handle
fails on connection loss or active-MGR change and never re-registers by itself;
`Close` reports an unconfirmed acknowledgement explicitly. The registration is
a CLIENT session and does not make the caller a Ceph daemon.

Evidence is owned by [the contract](cephmsgr/MGR_SERVICE_CONTRACT.md#evidence):
source-derived vectors and synthetic unit/race tests, retained fresh native
codec vectors for the five messages, and native observation on selected
profiles of registration, status, task status, applied metadata, close,
ticket renewal and an active-MGR change. Each linked validation record names
its own tested source and profile. Nonempty daemon extensions, older
configure versions, further profiles and further failure configurations are
[not verified](cephmsgr/MGR_SERVICE_CONTRACT.md#not-verified).

## Command descriptions

Command catalogs remain raw management/daemon responses. The product does not
construct typed methods or interpret schemas. Native test oracles may normalize
known formatter differences without changing the original product output.

## MON logs

`WatchLogs` registers one local watch per client without consuming a command
slot. `LogStream.Next(ctx)` returns owned batches with cursor metadata and the
first terminal cause. Registration/SubscribeAck does not prove authorization
or lossless history. The queue holds up to 64 batches and a conservative byte
budget: default `MaxFrameSize`, range 1 KiB to 1 GiB, not an exact heap limit.
Overflow ends only the watch. A live `Next` receives accepted batches before the
terminal cause; canceled waits do not consume queued data. `Close` releases the
slot and waits for its worker without replacing an earlier recorded cause.

## MON configuration

Configuration watches expose owned raw `map[string]string` snapshots without
parsing or applying settings. A newer unread map replaces the older one. Keys,
newlines, Unicode, NUL and whitespace remain raw. Local subscription success is
not an authorization result.

## MON health and digest

Digest watches preserve health-detail and MON-status bytes together, replacing
an older unread pair. They do not interpret health policy or guarantee a full
history. The other MON streams' lifetime, ownership and first-cause rules apply.

## State snapshots and map waits

`Snapshot()` copies state without network I/O. MonMap members retain names/ranks;
MgrMap retains active/standby identities, module metadata and schemas, activation
policies and raw service URIs. Metadata is not readiness, permission or an
instruction to contact a service.

`WaitMonReady`/`WaitMgrReady` wait for existing preparation without commands or
command slots. `WaitMonMap(ctx, afterEpoch)`/`WaitMgrMap` wait for strictly newer
authenticated maps; zero accepts an existing nonzero map. They promise neither
intermediate epochs nor subsequent command success.

## Lifetime and recovery

- `MaxFrameSize` defaults to 16 MiB; `MaxInFlight` defaults to 64. Bulk input is
  copied after command admission. Fixed codec/transcript limits also apply.
- `ErrLimitExceeded` covers byte/collection bounds. A valid oversized response
  is not automatically malformed; structural errors may preserve both categories.
- `ErrMalformedMessage` distinguishes complete invalid messages from interrupted
  network reads while preserving underlying decoder causes.
- Per-operation cancellation must not set a deadline on a shared connection.
  Late replies are consumed safely; partial writes must not corrupt live framing.
- A watch context owns the watch; a `Next` context owns only that wait. Accepted
  data and a recorded first cause survive later cancellation or Close.
- `Close` stops owned sessions/workers and releases pending callers. It does not
  convert uncertain execution into confirmed failure or rollback.
- Ticket renewal first re-authenticates to the MON that currently admits the
  client, keeping the GlobalID. Other seeds and learned MonMap addresses are
  fallbacks. Initial setup and recovery after a lost MON session try seeds
  first. See the [CephX ledger](internal/cephx/EXCHANGE_LEDGER.md).
- MON/MGR/OSD have no cookie-based resume here. The opt-in MDS profile has an
  explicit resume/reconnect contract.

Seeds accept `host:port` or `v2:host:port/nonce`, with bracketed IPv6. Local IPv6
routing zones are rejected rather than guessed as remote wire `ScopeID`.
Authenticated family, nonce, scope and flow remain intact. `Options.Hostname`
is caller-supplied and immutable; the product does not infer the execution host's name.

`ExpectedFSID` pins identity; otherwise the first authenticated MonMap does.
Admission requires `min_mon_release >= 20`, not a particular daemon patch.
Unsupported features are rejected explicitly.

## Verification

```sh
CGO_ENABLED=0 go build ./...
CGO_ENABLED=0 go test ./...
CGO_ENABLED=0 go vet ./...
CGO_ENABLED=1 go test -race ./...
go -C integration/fixture test -mod=readonly ./...
go -C integration/fixture vet -mod=readonly ./...
python3 -m unittest discover -s tools -p 'test_*.py'
CGO_ENABLED=0 python3 tools/product_dependencies.py
```

CI and the Windows interoperability verifier share this dependency check. It
checks the root module's build graph, permits only the documented compression
module externally, and rejects native sources, module replacements and subprocess
dependencies. Fixture-only dependencies remain in `integration/fixture/`.

Basic CI also runs, with the race detector, exactly the cluster-free
integration-package tests listed in `integration/cluster_free_tests.txt`, and
`tools/cluster_free_tests.py` requires each of them to report PASS. Every other
top-level test there must be a `TestCeph...Integration` native test, which
stays opt-in.

`integration/fixture/` contains bounded native setup/control/oracle recipes.
Run Docker profiles on Linux with their documented selectors. Native vectors,
unit/race/fuzz tests, live interoperability and full CI are separate evidence.
Skipped/canceled jobs and other-source results do not prove the current commit.
Selected profiles do not establish full RADOS/RBD/CephFS or long-duration coverage.

Snappy/zstd codec and frame round trips are unit-tested. The live CRC test shows
only that the pinned MON and OSD report no negotiated compression and commands
still succeed. The test does not capture the peer compression bit or negotiation
frames, so it cannot distinguish a declined offer from skipped negotiation.
Live compressed frames remain unverified; secure-mode compression and zlib are deferred.
See [compression](internal/msgr/COMPRESSION_PLAN.md),
[fixture validation](integration/fixture/VALIDATION.md) and
[completion criteria](SPEC.md#verification-and-completion).

## Sources and licensing

Wire semantics are independently implemented in Go from the pinned Ceph source
and [Tentacle Messenger documentation](https://docs.ceph.com/en/tentacle/dev/msgr2/).
Ceph native sources/libraries are development-oracle inputs, not product dependencies.
See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md), [NOTICE](NOTICE),
[LICENSE](LICENSE) and native-vector provenance records. Those records distinguish
native encoding from live ProtocolV2 sessions and independent server execution.
