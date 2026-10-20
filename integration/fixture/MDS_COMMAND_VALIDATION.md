# Explicit raw MDS command acceptance

Reference: Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The product remains native Go,
without CGO, librados, go-ceph, Ceph CLI subprocesses or native runtime
dependencies. The disposable provider and independent Python LibCephFS client
are development-only oracles. This stage adds raw command transport, not a
CephFS session OPEN, mount, metadata workflow or automatic recovery.

## Closed fixture and caller-owned target

`TestCephMDSCommandsIntegration` skips before extra actions unless
`CEPH_MSGR_TEST_MDS_COMMANDS=1`. Its fixture also requires MDS_FRESH=1 and the
`legacy-v6` OSD recipe, with legacy CRUSH and HASHPSPOOL disabled. The single
OSD is development infrastructure for the active MDS's 16-PG metadata pool and
one-PG data pool. This does not establish an OSD product requirement for MDS
communication. Go clients enable only `Options.EnableMDS`, with neither
EnableOSD nor EnableMDSMaps, and assert zero OSD dial attempts.

The native owned map supplies filesystem ID/name, active rank zero, daemon
GID and exact v2 address/nonce. The test constructs an explicit `MDSTarget` and
calls `OpenMDS`; it does not ask the product to discover or route to an MDS.
FilesystemID is local cache/submission metadata, not an authenticated wire
FSCID assertion. ServerIdent checks active rank zero, not daemon GID. Native
owned-map evidence attributes the endpoint to the daemon; shared MDS service
keys are not cryptographic proof of an individual daemon GID.

`CEPH_MSGR_MDS_COMMANDS_IDENTITY`/`KEY_FILE` select the dedicated command
identity, with MON read and MDS `allow *` caps. Existing MDS_FRESH identity/key
variables select a separate read-only identity with filesystem-scoped MDS
read caps. The product opens no MGR connection for these commands. Client keys
are privately parsed and checked against the selected AES/AES256K fixture
profile; private bytes and opaque tickets are not public evidence.

The command identity's `allow *` is deliberately unrestricted. The pinned
`MDSDaemon::handle_command` uses `MDSAuthCaps::allow_all`; an unrestricted
UID/path grant can satisfy that check even with fsname text. Neither a local
FSCID nor an apparent fsname cap restriction establishes Tell-command
filesystem isolation. These tests make no such permission claim.

## Native byte oracle and cases

The provider publishes `mds-commands-oracle.json` and exposes only fixed
`version`, `invalid` and `readonly-version` native actions. Each initializes a
fresh LibCephFS instance, invokes native `mds_command` on the exact owned
decimal daemon GID and shuts down without mounting or sending CephFS OPEN.
The native binding returns signed code, binary output and text directly;
there is no CLI decoration to trim or reinterpret. Hex output/status bytes,
native GlobalID, command JSON, input hex and process receipt are retained.
The Go test verifies provenance and compares raw output, status bytes and
signed code exactly. Native GlobalIDs must differ from both Go clients.

Both sides use unchanged fixed JSON and binary input `00ff6d647300`.
`version` need not echo this input; output equality alone is not proof that an
unused input reached the daemon. Independent current type97/type98 wire tests
cover binary data and canonical header v1/compat1. No command schema or result
interpretation is added to the product.

The native acceptance has eleven sequential leaves:

| Leaf | Checked behavior |
| --- | --- |
| Native version | Raw result bytes/status match independent native code zero; submission retains TID, client GlobalID, connection generation and original target |
| Unknown prefix | Native signed negative result and status remain a CommandError, with no authentication-error or unknown-outcome substitution |
| Read-only identity | Authenticated command returns native `-13`, retaining raw result and separate client identity |
| Concurrent requests | Eight alternating version/invalid commands retain distinct TIDs, one generation, and each corresponding native byte result |
| Local target admission | Equivalent exact address shares a handle; changed nonce, limit overflow, malformed target and disabled MDS refuse before MDS dialing |
| Pre-canceled wait | Zero submission, no outgoing relay write, and the next successful TID advances only once |
| MDS ticket renewal | MDSTicket expiry advances before prior expiry under a bounded wait with stable GlobalID; existing command works, then explicit Close/new Open uses newer generation/TID and a renewed ticket |
| Sent cancellation | Withheld receive bytes plus independent outgoing-write progress produce OutcomeUnknown with original submission, no automatic redial |
| Sent Close | A live caller receives OutcomeUnknown plus ErrClosed; Done closes, and relay workers are joined |
| Explicit later Open | Closed handle refuses locally; separately chosen Open/command obtains newer generation/TID and native version bytes |
| Native session absence | Bound full/read-only GlobalIDs remain absent from native CephFS sessions after commands; OSD dials stay zero |

The renewal wait observes actual scheduling metadata rather than inferring
renewal from a sleep. The previous deadline caps the 25-second wait; no key
wipe, configuration mutation or uncertain-request retry is used. A later
fresh service connection authenticates with the newly published MDS ticket.
The public state exposes scheduling times, not service key bytes or cipher
proofs; this stage's scheduling evidence is separate from the direct
[fresh-MDS key-type acceptance](MDS_FRESH_VALIDATION.md).

The stream relay fragments opaque transport bytes and can withhold incoming
bytes after setup. The receive gate can catch a transport ACK or keepalive;
the test separately requires outgoing progress before cancellation/Close.
Neither observation identifies an encrypted reply's TID or proves remote
execution of the uncertain call. The preserved submission is local attempt
identity, and a default zero result code with OutcomeUnknown is not success.
Fault cases use a stable read-only version command, close the owned handle
and relay, and join relay workers. Explicit subsequent requests are separate
caller decisions; no failed command is automatically replayed.

The test uses one full client GlobalID across four explicitly opened MDS
generations, plus one read-only client/connection. Native before/after session
observations bind that exact pair and use distinct increasing controller
receipt IDs. The final observation runs while the latest full and read-only
handles remain locally open. This does not promise that a native daemon keeps
every command transport open: command dispatch can mark it disposable, and
immediate native closure is not assumed.

## Public evidence and current status

`mds-commands-results.json` retains native receipts, Go command JSON/input and raw results,
signed errors, original submissions, renewal scheduling snapshots, write
progress, dial counts and both scoped session observations. The provider
separately retains command/fresh oracles, native process result, owned-resource
journal and cleanup receipts. Keys and keyrings remain private. The provider
owns daemon, filesystem, pool and identity cleanup; the Go test only closes
its transports and joins fault relays.

The two native controller executions completed successfully on 2026-10-15:

| Selected configuration | Native result | Scheduled renewal leaf |
| --- | --- | --- |
| Darwin arm64 host, IPv4 relay, CRC/AES client and service profile, single BlueStore OSD | 11/11 leaves passed, no skips, 10.53s | 4.88s |
| CGO=0 Linux arm64 container, IPv6, secure/AES256K client and service profile, single MemStore OSD | 11/11 leaves passed, no skips, 10.32s | 4.79s |

Both used pinned Ceph 20.2.4 on the owned legacy/no-HASHPSPOOL fixture and
acceptance source SHA-256
`581675961b122b0215af641289c12540c7ab86829af1efc3053a2b686e09ea32`.
Earlier controller invocations exited on missing command-line arguments,
before creating Ceph containers or compiling/running the suite; those usage
failures are not native MDS interoperability results.

An independent raw-evidence audit confirmed native process completion and
exact output/status comparisons: version returned code 0 and the JSON
`{"version":"20.2.4","release":"tentacle","release_type":"stable"}`;
the unknown prefix returned `-22` with its unchanged native text; the read-only
identity returned `-13` and `permission denied`. Every recorded binary input
is `00ff6d647300`. JSON evidence serializes Command.JSON as a JSON object;
it is not a byte capture of the request's original JSON formatting.

Each run retained 19 command records, including two uncertain submissions.
Full-client TIDs increased from 1 through 18 across four explicitly selected
connection generations. The read-only client's TID 1 has its own GlobalID.
Canceled TID 15/generation 2 and closed TID 17/generation 3 retained original
target/identity with outgoing progress and OutcomeUnknown. Separate explicit
requests used TID 16/generation 3 and TID 18/generation 4. Native before/after
receipts had increasing IDs and excluded both Go GlobalIDs (CRC 4854/4857;
secure 4866/4869). Each full client dialed MDS exactly four times, each
read-only client once, and Go OSD dials remained zero.

MDS ticket Expires and RenewAfter both advanced with stable GlobalID before
the old expiry; existing-handle and explicit new-connection commands then
matched native version output. Both final cleanup receipts confirm removal
of the filesystem, both owned pools and both identities, with completed=true
and remove_pending=false. Final native maps confirm absence of the filesystem
and owned pool IDs. All nine owned containers per run reached termination;
an additional Ryuk container is not counted as an owned Ceph resource.

| SHA-256 | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| `mds-commands-results.json` | `2610b097729074eac73fda83b4d008c573bbb46b7581cf316ef0e9e6aa43a008` | `067485c37a7cb8d35b38a54b2c9adc2c0f5b9ee08c4ac1e4749973cc919d186f` |
| `mds-fresh-cleanup.json` | `55139685acfc7f5fb4ce07dfab3d2df607cafa638b29b4faf5dc84fcee6b57b5` | `c7f5e5e70549eb0a50da3f773e4b84fdfde247353c6a75c07b12214bcd726152` |

These are local selected-configuration results, separate from completed remote
CI. General MDS compatibility, filesystem permission isolation, CephFS I/O and
stateful recovery remain outside this acceptance. See the
[product command contract](../../cephmsgr/MDS_COMMAND_CONTRACT.md) and
[owned fixture recipe](MDS_COMMAND_FIXTURE.md) for their separate boundaries.
