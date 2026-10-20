# Specification and verification requirements

This document defines the communication-layer contract, implementation priorities
and acceptance requirements. [README.md](README.md) describes usage; contracts
beside the implementation define individual wire/API boundaries. A requirement
or test fixture is not a claim that the corresponding product feature is complete.

## Product policy

- Implement Ceph Messenger directly in Go, without a product dependency on CGO,
  go-ceph, librados, Ceph CLI subprocesses or an OS-specific Ceph installation.
- Support Tentacle (20.2) and later verified configurations. Do not introduce
  compatibility code for earlier Ceph families or wrappers for superseded APIs.
- Accept programmatic credentials and configuration. Identity selection, text
  keyrings, configuration files and environment policy belong to the consumer.
- Provide communication primitives: framing, authentication, sessions, raw commands,
  maps, subscriptions, targeted object operations and bounded MDS communication.
- Keep command schemas, typed management APIs, operational workflows, CRUSH,
  automatic placement/routing, filesystem state and storage policy in another layer.
- Preserve raw command output and wire metadata for that layer. Do not silently
  reinterpret server errors, drop unknown outcomes or replay uncertain mutations.
- Expand product and fixture coverage through explicit contracts and independent
  native validation, rather than treating an initial successful profile as the
  final boundary.

## Implementation priorities

Security, data correctness, cancellation and shutdown regressions take priority
within every stage. Prerequisites apply even when a dependent feature has higher
priority. Lower-priority requirements remain part of the final goal.

| Priority | Work | Completion boundary |
| --- | --- | --- |
| Foundation | Messenger, CephX, MON/MGR, raw maps, contexts and targeted OSD I/O | Preserve existing role-specific contracts and verification |
| P0 | O0 operation inventory and O1 mutation/compound completion semantics | Independent native execution, readback, uncertainty, cancellation and failure recovery |
| P1 | Metadata, snapshots, class calls, enumeration, events, backoff and targeted commands | Opcode/version/feature coverage with explicit limitations |
| P2 | MDS authentication, maps, sessions, metadata, capabilities and recovery | Required events and state submission must accompany each usable path |
| P3 | Additional EC/platform/feature profiles, long runs and complete fixture coverage | Do not generalize selected-profile success to all configurations |

MDS metadata cannot be called supported without the capabilities, leases and
recovery required by that path. Fixture preparation for a P0–P2 feature happens
before its validation; P3 does not defer those prerequisites.

## Implementation and evidence

Track implementation and verification separately:

- Implementation: absent, partial, or present within a stated contract.
- Verification: unverified, synthetic/unit-tested, verified on selected native
  profiles, or accepted against every requirement of the specified stage.

Current code includes secure/CRC framing, CephX aes/aes256k, MON/MGR commands,
subscriptions and recovery; targeted OSD operations and control messages; and
opt-in MDS sessions, metadata and explicit recovery primitives. These do not
constitute complete RADOS, RBD or CephFS clients.

| Area | Contract and evidence | Remaining boundary |
| --- | --- | --- |
| Messenger/CephX | `internal/msgr/`, `internal/cephx/`, `internal/session/`; native role/key/mode profiles | A wire feature does not prove a release family or every security/platform combination; declared masks per role are in [FEATURE_LEDGER.md](internal/session/FEATURE_LEDGER.md) |
| MON/MGR commands | `cephmsgr/api.go`, `cephmsgr/named_mon.go`, [MGR Tell](cephmsgr/MGR_TELL_CONTRACT.md) | No typed management layer, standby command routing or automatic mutation replay |
| MON subscriptions | Log/config/digest and raw map APIs in `cephmsgr/` | No authorization ACK, complete event history or autonomous map application |
| OSD operations | [core](internal/osd/CONTRACT.md), [completion](internal/osd/O4_CONTRACT.md), [EC](internal/osd/ERASURE_CONTRACT.md) | Explicit target/profile/version/feature requirements; no automatic placement or universal durability claim |
| MDS/CephFS transport | [MDS wire](internal/mds/CONTRACT.md), [CephFS](cephmsgr/MDS_CEPHFS_CONTRACT.md) | Consumer-owned state, selected recovery profiles and incomplete high-level filesystem semantics |
| Compression | [compression contract and plan](internal/msgr/COMPRESSION_PLAN.md) | Snappy/zstd codec/frame unit coverage; live CRC declined-offer test only (pinned daemons decline compression toward clients, so live compressed frames are unreachable from a client); zlib and secure compression deferred |
| Native development tools | `integration/fixture/`, `tools/`, native-vector provenance | Product interoperability, fixture capability and higher-level consumer behaviour are separate results |

A later passing retry does not explain an earlier failure. Long-duration failures
need an established cause and a matching regression before being called fixed.
Shorter successful runs do not establish hour-long operation. Report skipped,
canceled, missing and failed cases explicitly.

## Final test-environment scope

The environment must reproduce and independently observe every required
Messenger-based communication and I/O path of the selected Ceph release,
including paths not yet implemented in Go.

| Path | Required native setup and oracle |
| --- | --- |
| Messenger/CephX | Role-specific authentication, capabilities, key rotation, ticket expiry/renewal, features, addresses/nonces, ACK/keepalive, reset/resume, cancellation and shutdown |
| CLIENT to MON/MGR | Commands, bulk/binary responses, Tell, discovery, subscriptions, map epochs, permissions and failover |
| CLIENT to OSD | Reads, writes, compound operations, conditions, metadata, class calls, enumeration, notifications, snapshots and completion semantics |
| CLIENT to MDS/CephFS | Sessions, namespace/inodes, capabilities/leases, snapshots, multi-rank/standby transitions and coupled object I/O |
| RBD underlying I/O | Native image/snapshot/clone preparation, object/class metadata, reads/writes, notifications and independent data comparison |
| Daemon-to-daemon | Quorum, map distribution, heartbeat, peering, replication, recovery/backfill and metadata movement on public/cluster networks |
| Service-induced paths | Messenger activity caused by native RGW or mirror services, with HTTP/S3 distinguished as input/observation transports |

RBD is not a separate Messenger server role. CephFS requires MDS metadata and
OSD data paths together. Native service success does not establish the matching
Go wire implementation. Daemon-to-daemon fixtures use actual Ceph daemons;
this does not require the product to implement Ceph servers.

### Environment acceptance

For every required path, record:

1. Reference release/source, messages/opcodes/features, implementation stage and
   prerequisites; daemon roles/counts, addresses, networks, pools, PGs, replicas,
   EC and data; credentials/capabilities, security mode and ticket settings.
2. Bounded setup, control input, independent native oracle, fault/recovery steps
   and cleanup conditions. Generic `Exec`, an installed executable, a connection
   or an adapter alone is not a completed fixture feature.
3. Bytes, metadata, versions, map epochs, request identities and server results.
   Distinguish transport receipt, execution and the native durability semantics.
4. Required daemon stop/restart/replacement, directional/peer-specific network
   faults, partial sends, late replies, permission failures, epoch/fullness/
   blocklist changes and cancellation/shutdown races.
5. Actual release/image digest, architecture, mode/key/address/runtime profile,
   controls and cleanup. Preflight does not replace operation/oracle success.
6. Ownership of containers, networks, volumes, processes and changed native state;
   bounded commands/output/diagnostics and confirmed restoration after failure.

The final environment is complete only when all mandatory paths have reproducible
setup/control/oracle/fault/cleanup recipes and successful native evidence.
Unimplemented Go features remain separate. Unsupported platforms and skipped
required cases cannot be omitted to declare completion.

### Fixture requirements

Distinguish capability available in a released dependency, present only on main,
proposed, expressible with an existing public primitive but lacking a recipe,
absent, and unverified. Reuse an existing primitive when it expresses the actual
native condition. A consumer-specific oracle or missing convenience wrapper is
not automatically a missing fixture capability.

Image requirements need a concrete native operation, reproducible image/runtime
failure and acceptance test. Compare official and role images on equivalent
release, architecture, configuration and input. Product, image, fixture and
higher-level consumer completion must be recorded independently.

## Layer responsibilities

The transport provides raw inputs/outputs and identities. Consumers own:
command construction/schema/result interpretation; credentials and text config;
map application, object hashes and CRUSH; pool/namespace selection and routing;
filesystem/inode/cache/layout state; RBD image/lock/clone/journal policy;
operational retries, reconciliation and service workflows.

Test-only consumers may construct commands, interpret native output and configure
disposable clusters to test communication. They must not become exported product
APIs or runtime dependencies.

## Communication requirements

Use the pinned Ceph source as the wire authority. go-ceph feature groups may help
find omissions, but C ABI, function names, native handles and older-release
compatibility are not product targets. Each feature needs a trace from user-visible
behaviour to message/opcode/encoding/feature, public input/output, consumer-owned
state, independent oracle and failure test.

### OSD requests and completion

Preserve locator, namespace, raw hash, placement, map epoch, client incarnation,
TID/attempt, operation order, raw payloads, per-op results, versions and completion
flags. Bound all inputs, outputs, lengths and collections. Reject unsupported
opcodes, encodings and required features explicitly.

Required operation groups include read/stat, writes/partial writes/append/truncate/
delete, conditions and compound vectors, xattrs/OMAP, class calls, allocation hints,
sparse reads/checksums, snapshots/clones and relevant cache/control operations.
The opcode catalog is [opcodes-tentacle.csv](internal/osd/opcodes-tentacle.csv).
Detailed supported subsets are the contracts in `internal/osd/`.

Do not equate Messenger ACK with execution, or an operation reply with a stronger
durability guarantee than the selected server flags. Sent failures preserve an
unknown outcome. Caller-directed resubmission must preserve identity and expose
map/redirect/control information without automatically replaying a mutation.

Enumeration returns bounded PG pages and cursors, not pool-wide iteration policy.
Watch/notify exposes registration, delivery, explicit ACK, errors and connection
control information; consumers decide handling and re-registration. Backoff needs
bounded ranges, original epochs, ACK identity and correct writer gating. Do not
infer object execution from ACK history.

### MDS and filesystem communication

Provide role-specific authentication/renewal, filesystem/rank/daemon map metadata,
explicit target binding, session OPEN/renewal/CLOSE and bounded metadata requests,
replies and push events. Required paths include capabilities/leases, revocation,
flush/ACK, unsafe/safe completion, forwarding, snapshots, reconnect and replay.
Keep path/inode/cache/handle state, permissions, routing and consistency policy
with the consumer.

Messenger cookie/sequence resume and new-incarnation MClientReconnect are different
operations. Consumers explicitly supply outstanding requests, capabilities, realms
and pending flushes. Preserve same-TID, retry, REPLAY, target-inode and uncertain
outcome semantics. Do not repeat completed object writes to recover metadata.
Shared-nonce eviction/blocklist fencing must remain visible to associated OSD I/O.

Multi-rank forwarding and standby/standby-replay takeover require identity, epoch,
request and native postcondition checks. A tested rank/profile does not prove
all-rank recovery, arbitrary dirty state or complete POSIX behaviour.

### RBD and coupled file data

Transport RBD's class payloads, object operations, snapshots, watches and
notifications without embedding image/striping/parent/lock/journal policy.
Compare test-only consumers with librbd for underlying reads/writes, metadata,
lock handoff, object-map changes, clone/copy-up and journal replay as applicable.

For CephFS, verify layout-based OSD I/O alongside MDS capabilities, truncation,
writeback, snapshots, flush/ACK and recovery. Native-prepared data read by Go is
only a foundation; full higher-level equivalence needs separate consumer
execution, concurrency, failure and recovery evidence.

### Targeted raw commands

OSD, PG and MDS commands require explicit target identity, service authentication,
bounded raw JSON/bulk/binary input and original result/error delivery. Do not
expand wildcards, silently substitute targets, build typed administrative APIs or
re-execute uncertain commands. Discovery, routing and operational policy stay
with the consumer.

### Milestones

| ID | Contract | Prerequisites and acceptance |
| --- | --- | --- |
| O0 | Source-backed opcode/message/feature inventory | Reference encoding, limits, completion and native-oracle mapping |
| O1 | Mutation and compound request public contract | Independent execution/readback, uncertainty, concurrent use and daemon failure |
| O2 | Metadata, class calls and snapshots | O0/O1, raw payloads/versions, native state and condition checks |
| O3 | Enumeration, watch/notify and control events | Identity, bounded queues/cursors, delivery/ACK and recovery semantics |
| O4 | Completion, backoff and explicit resubmission | Required OSD features and independent failure/postcondition observations |
| D1 | OSD/PG raw commands | Target binding, role authentication, native command and error comparison |
| D2 | MDS raw commands | MDS discovery/authentication and exact target identity |
| M0 | MDS authentication, maps and sessions | Native service identity, permissions, renewal and closure |
| M1 | Metadata, capabilities, leases and required pushes | M0, independent namespace/inode/state observations |
| M2 | State submission, flush/ACK, reconnect/replay, ranks and object I/O | M1 and required OSD paths; explicit pending state and native data/metadata recovery |
| V1 | EC, extra feature/platform profiles and long runs | Relevant wire contracts, native fixture/oracle and failure analysis |
| E1 | Complete native environment | Every required setup/control/oracle/fault/cleanup path, separately from Go completion |

These are requirement groups, not claims that every case is complete. Existing
implementations may satisfy subsets. No stage is complete from opcode presence,
a synthetic round trip, setup success or a skipped required case.

## Reference and release admission

Reference Ceph `v20.2.4` at `7f793731f1b39eb4f465e960113d2363c311b964`.
Use later source changes for explicit review; do not describe that pinned version
as the newest release. Admit only authenticated MonMaps with `min_mon_release >= 20`.
This checks cluster minimum-family policy, not every daemon patch. Upgrade clusters
retaining an older minimum are rejected.

Never advertise unimplemented features or downgrade security. MON requires a narrow
set of CRUSH-generation bits even for command-only clients; those are admission
exceptions, not CRUSH support. Do not apply the entire exception to OSD sessions.
CRUSH_V2 (bit 36), required by MON and OSD when an EC rule exists, is a narrow
admission-only exception for both roles. Reject other unsupported requirements.
Selected optimized EC profiles may require explicit primary-first shard numbering;
MSR and unverified EC profiles do not follow from those results.

## Messenger and CephX

Implement msgr2.1 without msgr1/msgr2.0 fallback. Secure is the default; CRC is
explicit. Each connection offers only its selected mode, including reconnect,
renewal and independent Tell connections. Both modes use CRC framing before
authentication and verify CephX/AUTH_SIGNATURE. CRC after authentication is
unencrypted corruption detection, not confidentiality.

Support the selected aes and Tentacle aes256k key paths. In CRC mode, handle the
native encrypted empty connection secret and nonce-only authorizer response while
still checking structure, encryption and nonce. Secure requires its versioned
response and at least a 40-byte connection secret. Publish no authenticated
session, partial credential state or tickets before AUTH_DONE and transcript checks.

CRC connections can negotiate implemented snappy/zstd compression. Secure-mode
compression and zlib are deferred. Bound decompression and each logical segment;
small uncompressed frames can coexist with compressed frames. Native codec/frame
round trips are not live compression interoperability, and the declined-offer
live test shows only that the offer does not break MON/OSD connections. See
[compression details](internal/msgr/COMPRESSION_PLAN.md).

Preserve feature negotiation, addresses, nonces, request/session identity,
sequence/ACK, keepalive, reconnect/reset and loss semantics. Apply bounded parser
and cryptographic tests, independent native vectors, fuzzing, race checks and
actual authentication/renewal/failover tests. MON/MGR/OSD do not implement cookie
resume here; the opt-in MDS resume contract is separate.

## Raw maps and subscriptions

OSD-supplied MOSDMap envelopes preserve FSID, trim/latest epochs, ordered full/
incremental opaque blobs and original front bytes. Consumers apply maps and choose
targets. OSD FIFO overflow fails that session to avoid silently breaking a chain;
accepted maps precede the first terminal cause. Map reception is not request
completion or permission to replay an operation.

MON range requests and watches are separate from OSD delivery. Zero requests the
latest full map, positive epoch ranges are inclusive, and native limits may give
partial/empty batches. No automatic continuation, placement or replay occurs.
Continuous watches preserve explicit queue/byte bounds and continuity failures.

Log streams retain owned batches/cursors. Configuration streams retain raw maps;
digest streams retain raw health/MON-status pairs. New unread configuration/digest
snapshots may replace older ones. Local registration and SubscribeAck do not
establish permission or a lossless event history.

## Go API requirements

- `ParseKey` validates a selected base64 credential; consumers select identities
  and load text keyrings/configuration.
- `Dial` permits bootstrap cancellation without tying a successful client's entire
  lifetime to that context. `Options.Hostname` is immutable caller input, not an
  execution-host lookup.
- Raw commands preserve JSON/bulk input and original binary output/status/codes.
  Separate server errors, local context/network/protocol errors and unknown outcomes.
- Exact-name Tell preserves target binding and main-session ownership. Reject an
  absent/changed target before sending when possible, and retain post-send uncertainty.
- Ready/map waits use existing state/notifications without command slots or extra
  sessions. Map waits require a strictly newer authenticated epoch; zero accepts an
  existing nonzero map. They do not promise intermediate epochs or future readiness.
- `Snapshot` performs no network I/O and returns independent copies, including
  nested module option metadata, activation policies and raw service URIs. These
  fields do not establish permissions, a running module or service readiness.
- Permit concurrent operations without putting a request's deadline on the shared
  TCP connection. Bound `MaxFrameSize` (default 16 MiB) and `MaxInFlight` (default 64).
  Copy bulk inputs only after admission.
- A watch context owns its lifetime/recovery; `Next` owns only that local wait.
  Canceled waits must not consume data. A live wait receives accepted items before
  the first terminal cause. Close must not overwrite an already recorded failure.
- Log/raw-map FIFOs are bounded to 64 batches and a conservative byte estimate.
  `MaxBufferedBytes` defaults to `MaxFrameSize` and ranges from 1 KiB to 1 GiB;
  it is not an exact heap bound. Preserve each stream/session's overflow contract.
- `ErrLimitExceeded` covers frame/codec/transcript bounds, including fixed limits;
  keep it separate from admission waits and watch overflow. A complete structural
  error may retain both malformed-message and limit causes.
- Clean pending requests safely, consume late replies and never leave partly written
  framing usable. Local cancellation does not roll back the remote operation.
- `Close` terminates owned connections/workers, releases waiters and is tested
  against cancellation, reconnect and concurrent shutdown.

## Verification and completion

For each accepted feature, require source-backed encoding/features, public inputs/
outputs, consumer responsibilities, unsupported cases, independent native oracle
and failure/recovery tests. Compare bytes, metadata, versions and server results,
including role/mode/key/address/runtime/image identity.

Exercise ticket renewal, permissions, session/target changes, concurrent calls,
late replies, output bounds, cancellation and Close. Mutations need independent
readback and the appropriate native execution/durability semantics after uncertain
responses, daemon restarts and required faults. Preserve the original failure;
a successful retry alone is not a fix.

Use unit/synthetic, race and bounded fuzz tests, native codec parity, real-daemon
profiles and target-OS builds/execution as distinct checks. Native encoding does
not establish live sessions; live service success does not establish the matching
Go path. A required skip or preflight-only result leaves acceptance incomplete.

Check product and fixture modules separately. The product remains CGO-free and
independent of native Ceph. Fixture-only libraries, containers, native clients,
source builders and command interpreters do not become product dependencies.
Use the actual module versions in `go.mod` and `integration/fixture/go.mod`.

CI claims must identify the exact commit and attempt. Earlier-source, canceled,
skipped or missing jobs are not passes of the current source. Maintain feature
scope and verified profile tables alongside the contracts rather than inferring
unverified upper-layer progress.

## Maintenance and sources

Review upstream changes by wire/features, CephX/crypto, MonClient/MgrClient and
used message/map encodings. A command-schema or C++ refactor change matters only
when it affects the communication contract or test oracle. Keep transport changes
separate from application policy.

For a supported upgrade, compare pinned source, regenerate independent vectors
where required and repeat affected native profiles. Distinguish a protocol change
from an implementation defect. Connection success alone is not compatibility.

Preserve upstream source attribution and licenses without importing native code
or dependencies into the product. Native oracles may normalize known formatter
representations in comparison copies only; original product data stays intact.
See [third-party notices](THIRD_PARTY_NOTICES.md) and native-vector provenance.

References: [Messenger](https://docs.ceph.com/en/tentacle/dev/msgr2/),
[architecture](https://docs.ceph.com/en/tentacle/architecture/),
[librados](https://docs.ceph.com/en/tentacle/rados/api/librados/),
[CephFS I/O](https://docs.ceph.com/en/tentacle/cephfs/cephfs-io-path/),
[capabilities](https://docs.ceph.com/en/tentacle/cephfs/capabilities/), and
[the pinned source](https://github.com/ceph/ceph/tree/7f793731f1b39eb4f465e960113d2363c311b964).
