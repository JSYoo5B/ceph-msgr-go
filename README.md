# ceph-msgr-go

A native Go communication layer for Ceph Tentacle: msgr2.1, CephX, raw
MON/MGR commands and subscriptions, explicitly targeted OSD requests, and
bounded MDS/CephFS wire primitives.

The product requires Go 1.24 or later and no CGO, go-ceph, librados, Ceph CLI,
or operating-system Ceph installation. Snappy and zstd codecs use the pure Go
`github.com/klauspost/compress` dependency. Native development tools belong to
the separate `integration/fixture` module.

The protocol reference is Ceph `v20.2.4`, commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Tentacle (20.2) is the minimum
release family. A successful connection does not certify every patch,
configuration, platform or higher-level storage operation.

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

## Quick start

The import path is `github.com/JSYoo5B/ceph-msgr-go/cephmsgr`; the package name
is `cephmsgr`. There are no published module version tags.

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
```

`integration/fixture/` contains bounded native setup/control/oracle recipes.
Run Docker profiles on Linux with their documented selectors. Native vectors,
unit/race/fuzz tests, live interoperability and full CI are separate evidence.
Skipped/canceled jobs and other-source results do not prove the current commit.
Selected profiles do not establish full RADOS/RBD/CephFS or long-duration coverage.

Snappy/zstd codec and frame round trips are unit-tested. The live CRC test shows
only that the pinned MON and OSD decline the offer and commands still succeed;
daemons apply onwire compression policy only toward OSD peers, so live compressed
frames are unreachable from a client. Secure-mode compression and zlib are deferred.
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
