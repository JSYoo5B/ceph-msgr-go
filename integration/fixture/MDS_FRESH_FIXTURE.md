# Disposable active-MDS fresh transport fixture

This opt-in development fixture prepares one owned active rank-0 MDS for
internal CephX fresh-handshake and Messenger keepalive tests. It establishes
neither a public Go MDS API nor a CephFS OPEN/session or metadata-I/O contract.
Native libraries and CLI commands remain fixture dependencies only.

Set `CEPH_MSGR_TEST_MDS_FRESH=1` with `CEPH_MSGR_TEST_OSD=1`, one OSD, and
`CEPH_MSGR_TEST_OSD_PROFILE=replicated-pg-v8` or `replicated-pg-reply-v8`.
The only admitted test selector is `^TestCephMDSFreshIntegration$`; a different
explicit selector is rejected. This excludes the separate MDS maps oracle,
which requires an initially empty filesystem directory and owns its own names.
Unknown boolean/profile values and combinations with authentication/stress/
mapped-address fixtures are rejected before cluster startup.

The existing OSD preparation installs the disposable CRUSH hierarchy first.
Then public `cephfs.Start` from installed `ceph-testcontainers-go@v0.2.0` creates
a new `ceph-msgr-fresh-<128-bit token>` filesystem, dedicated `-meta` and `-data`
pools (16 and one PG, replication/min_size 1/1), one active MDS and no standby.
`WaitReady` and `MDSStatus` must report one provider-owned active rank 0. No
existing filesystem is adopted. `ScaleMDS` and filesystem mounts are not used.
The parent provider owns partial resources even when startup fails.

Only this closed fixture sets bootstrap `mon_allow_pool_delete=true`, allowing
public `cephfs.Remove` to remove its owned pools at teardown. The provider's
`MessengerV2Only` policy removes v1 listeners while leaving MDS service modes at
the native defaults. The current `MessengerPolicy` selects MON/MGR/OSD modes,
without an MDS-specific field. The Go test offers exactly its configured CRC or
secure mode and checks the selected AuthDone mode; it makes no claim that the
MDS server rejects other modes. Existing profiles retain their original policy
and daemon modes.

## Dedicated credentials

The fixture creates `client.mds-fresh-<token>` with exactly `mon: allow r` and
`mds: allow r fsname=<owned filesystem>`. It grants no OSD or MGR capability.
The current internal Go CephX client requests AUTH/MGR plus MDS, but pinned
`CephxKeyServer.cc`, `KeyServerData::get_caps`, returns true for an existing
identity even when a requested service has no capability entry. The MGR ticket
can therefore carry empty capabilities; receiving it does not require granting
MGR permissions. The test establishes no MGR connection.

`CEPH_MSGR_MDS_FRESH_IDENTITY` and `CEPH_MSGR_MDS_FRESH_KEY_FILE` identify this
dedicated identity and `<controlDir>/mds-fresh.key`. Container tests receive
`/out/mds-fresh.key`; host tests receive the private host directory path. The
key and `mds-fresh.keyring` are mode 0600 and excluded from public diagnostics.
The existing `client.test`, primary key and native-oracle keyring are unchanged.

## Native target evidence

Startup writes `mds-fresh-owned.json` before mutation and then publishes
`mds-fresh-oracle.json` with these fields:

```json
{
  "source_commit": "7f793731f1b39eb4f465e960113d2363c311b964",
  "token": "<32 lowercase hex digits>",
  "fsid": "<native cluster UUID>",
  "fscid": 1,
  "fs_name": "ceph-msgr-fresh-<token>",
  "fsmap_epoch": 4,
  "mdsmap_epoch": 4,
  "metadata_pool": 3,
  "data_pools": [4],
  "client_identity": "client.mds-fresh-<token>",
  "mds": {
    "name": "<provider-owned daemon name>",
    "gid": 51,
    "rank": 0,
    "state": "up:active",
    "incarnation": 1,
    "addresses": [{"type": "v2", "addr": "172.18.0.9:6800", "nonce": 17}],
    "endpoint": "172.18.0.9:6800",
    "nonce": 17
  },
  "before_sessions": [],
  "filesystem": {},
  "fs_dump": {}
}
```

Numbers above illustrate the schema; actual IDs, addresses, epochs and nonce
come from the native fixture. `filesystem` and `fs_dump` retain the full native
`fs get` and `fs dump` JSON, rather than the empty placeholders shown above.
The native OSDMap independently confirms the FSID and exact UUID pool-name/ID
pairs. The per-filesystem entry must equal the full FSMap entry and agree with
the provider's owned rank/GID/status. Foreign filesystems, standbys, ranks,
daemon identities, pool identities or changing targets are rejected.

`mds.endpoint` is the single advertised v2 numeric host:port of the selected
address family, with brackets around IPv6. It has no `v2:` prefix or nonce
suffix. Nonce is separate and nonzero. Addresses retain native string types
`v1`/`v2`. Unspecified, multicast, mapped-IPv6, invalid, zero-port or ambiguous
selected addresses are refused. No listener port is guessed from ContainerIP.
The selected address is also added to `peers.json` under `mds.<daemon name>`
for the existing host relay. Ownership records include every actual MDS
container, including partially initialized daemons tracked by the provider.

## Fixed read-only observation control

There is no start, general-exec or mutation control for this fixture. Before
and after the fresh MDS handshake/keepalive, use:

```go
testcluster.RequestControl(ctx, dir,
    "mds-fresh-observe", "mds-fresh-observed",
    oracle.Token, strconv.FormatUint(globalID, 10))
```

The request is `<positive requestID> <ownership token> <positive GlobalID>`.
IDs must be canonical decimal values; request IDs must increase, and after the
first observation the client GlobalID binding cannot change. Unknown scope,
extra fields, overflow, replayed IDs and observations after cleanup begins are
refused. Successful processing atomically publishes
`mds-fresh-session-result.json` and acknowledges
`mds-fresh-observed.<requestID>`. The result contains `source_commit`, `token`,
`request_id`, `fsid`, `fscid`, `fs_name`, `mds_gid`, `rank`,
`expected_global_id`, `expected_global_id_present`, `sessions`, `filesystem`
and `fs_dump`.

Observation revalidates the same owned target and obtains native sessions
through public `fs.Sessions(ctx)`. Session records retain the public native
helper's exported JSON field names, including `ID`, `Rank`, `EntityID` and
`Address`. A matching GlobalID is recorded as present and fails the fixture.
Other native probe identities may be present; they do not establish that the
test client opened a session. The entire observer, including serialized lock
acquisition, has a 15-second context; only read-only map snapshots may repeat,
at most three times. Input native JSON is limited to 256 KiB per map and the
public evidence to 1 MiB.

Pinned `MDSDaemon.cc` explains the boundary: the accepted Messenger connection
has a closed Session object that is not inserted into SessionMap until an
explicit OPEN request. Absence from native `session ls` therefore supports
absence of an open MDS session; it does not imply absence of authentication or
a Messenger connection. Native GID/name provenance is also distinct from the
rank in ServerIdent and from proof of an individual daemon's service key.

## Cleanup and failure handling

After transports close, the harness calls `finishMDSFresh` before archiving
metadata. `Terminate` provides a fallback and always continues parent cluster
termination even when explicit removal fails. Cleanup and observation serialize
on the same scope lock. Cleanup has a 45-second context including lock wait.
Before its single `cephfs.Remove` attempt, it records attempted/pending state;
the result is cached, so fallback never retries an uncertain mutation. On
confirmed success it independently checks absence of the filesystem and its
two UUID pools, then revokes the dedicated identity once.

`mds-fresh-cleanup.json` records source/token/FSID/name/FSCID/pools,
`attempted`, `remove_pending`, `filesystem_removed`, `pools_removed`,
`identity_removed`, `completed`, any error, and full final native FSMap/OSDMap
JSON. Pending or failed cleanup remains visible for parent teardown; the
fixture does not claim explicit cleanup success from container termination.
Private credentials are never placed in these receipts.

Mock/unit tests cover closed profile selection and paths, scoped bootstrap,
native identity/address parsing and negative cases, observation request scope,
bounded serialization, cached uncertain cleanup, and host-readable evidence.
Those tests and successful compilation do not establish live MDS interoperability.

## Actual fresh transport checks — 2026-10-15

| Executed configuration | Selected wire mode | Result |
| --- | --- | --- |
| AES client/service keys, CRC, IPv4 host client, one BlueStore OSD | CRC 1 | 1/1 PASS, 2.42 seconds |
| AES256K client/service keys, secure, IPv6 container client, one MemStore OSD | secure 2 | 1/1 PASS, 2.21 seconds |

Both actual fixtures retained the provider's v2-only/default MDS service-mode
policy described above. Each client offered exactly one mode, and the successful
native handshake and keepalive ACK verified that selected mode. Neither result
establishes server mode exclusivity or permits a security downgrade.

Independent artifact checks confirmed native FSID/FSCID/name, rank-0 daemon
GID/address/nonce, epochs and pool IDs, matching 8-byte keepalive/ACK payloads,
one MON and one MDS dial, and zero post-handshake application messages. Native
before/after observations excluded the client's GlobalID from open sessions
while its authenticated MDS transport remained live. Both successful runs
confirmed explicit filesystem, two-pool and dedicated-identity removal, then
termination of all nine owned containers. The failed secure attempt also
completed explicit resource cleanup and all nine container terminations.
No cleanup omission was found. No additional native command or Docker inventory
was issued during this independent artifact audit.

Detailed source hashes, retained failures, artifact identities and validation
limits are in [MDS_FRESH_VALIDATION.md](MDS_FRESH_VALIDATION.md). These targeted
internal fresh-transport checks are separate from whole-project CI, public MDS
connections, OPEN/session semantics, metadata requests and CephFS I/O.
