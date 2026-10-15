# Disposable explicit MDS command fixture

`CEPH_MSGR_TEST_MDS_COMMANDS=1` is a closed development-only extension of
`CEPH_MSGR_TEST_MDS_FRESH=1`. It selects only
`^TestCephMDSCommandsIntegration$` and requires one OSD with `legacy-v6`.
The metadata/data pools explicitly disable hashpspool, with 16/1 PGs and
replication/min_size 1/1. This lets the public Go client enable MDS without
enabling OSD or advertising the modern OSD profile's extra MON admission bits.
It does not add a MON feature exception. Ordinary MDS fresh tests retain their
existing modern profiles and original exclusive selector.

The existing fresh fixture creates a UUID filesystem and one active rank-0
MDS. It supplies exact native FSID/FSCID/name, daemon GID, advertised v2
address/nonce and pool metadata. The same parent owns all partial resources.
No filesystem is mounted and no CephFS OPEN is sent by the command oracle.
MDS service modes retain the provider's v2-only/default policy; the public Go
client independently selects its exact CRC or secure connection mode.

## Two separate identities

The existing `client.mds-fresh-<token>` identity retains MON read and MDS
filesystem-name read permissions. A new `client.mds-commands-<token>` has MON
read and `mds allow *`, with no MGR or OSD capabilities. The latter is an
explicit full-command identity: pinned `MDSDaemon::handle_command` requires
`auth_caps.allow_all()` and does not grant filesystem-bound command authority
from an `fsname` restriction. This fixture makes no such restricted-command
permission claim.

`CEPH_MSGR_MDS_COMMANDS_IDENTITY` and `CEPH_MSGR_MDS_COMMANDS_KEY_FILE` name the
new identity and `<controlDir>/mds-commands.key`. Existing MDS fresh identity
variables still select the read-only client. Host and container paths follow
the existing private control directory rules; keys and keyrings are mode 0600
and excluded from diagnostics. Existing native-oracle `client.test` credentials
are unchanged.

Startup publishes `mds-commands-oracle.json`:

```json
{
  "source_commit": "7f793731f1b39eb4f465e960113d2363c311b964",
  "token": "<owned token>",
  "fsid": "<native UUID>",
  "fscid": 1,
  "fs_name": "ceph-msgr-fresh-<token>",
  "mds_gid": 51,
  "client_identity": "client.mds-commands-<token>",
  "readonly_identity": "client.mds-fresh-<token>",
  "command_json": {
    "version": "{\"prefix\":\"version\",\"format\":\"json\"}",
    "invalid": "{\"prefix\":\"ceph_msgr_unknown_mds_command\",\"format\":\"json\"}"
  },
  "input_hex": "00ff6d647300"
}
```

Illustrative numeric IDs above are replaced with actual native values. The
exact advertised address remains in `mds-fresh-oracle.json`.

## Independent native command oracle

The pinned CLI's MDS route in `pybind/ceph_argparse.py` initializes LibCephFS,
calls `mds_command` and shuts it down without mounting. The Python binding in
`pybind/cephfs/cephfs.pyx` returns `(signed_code, raw_output_bytes, status)`.
`client/Client.cc::mds_command` accepts an initialized unmounted client, resolves
the explicit daemon GID, and constructs MCommand. The daemon accepts commands
on a closed connection without placing that client in its OPEN session map.
These LGPL-2.1 sources at the pinned commit are references; the fixture's
independent driver incorporates no copied native implementation.

`mds_commands.go` embeds a fixed Python native-API driver and uses the public
provider's supervised `RunNativeCommand` API. It does not add a general exec
interface, mount, filesystem access or caller-selected native prefix. The
only admitted actions are:

| Action | Identity | Exact command |
| --- | --- | --- |
| `version` | full command identity | the version JSON above |
| `invalid` | full command identity | the fixed unknown-prefix JSON above |
| `readonly-version` | read-only identity | the version JSON above |

Each action sends the fixed six-byte input `00ff6d647300` to the exact recorded
decimal daemon GID. It preserves the native signed result, raw data hex and
status with matching UTF-8 hex, including negative native results. These fixed
commands return ASCII status; this is not a general native oracle for arbitrary
invalid-UTF-8 status. Native TIDs are allocated inside libcephfs and are not
exposed or equated with Go request TIDs.

Use this fixed control:

```go
testcluster.RequestControl(ctx, dir,
    "mds-commands-native", "mds-commands-native-completed",
    token, action)
```

The request is `<requestID> <ownership token> <closed action>`. IDs must be
positive canonical decimal and strictly increase. The target map is revalidated
against the recorded FSID/FSCID/GID/address/nonce and pools before submission.
The entire controller, including scope-lock acquisition, is limited to 20
seconds; the supervised native process has a ten-second timeout, 64 KiB stdout
and 4 KiB stderr limits. It performs only fixed read-only commands. Failed or
incomplete native processes remain explicit and never become successful
command receipts.

`mds-commands-native-result.json` contains `source_commit`, `token`,
`request_id`, `fsid`, `fscid`, `fs_name`, `mds_gid`, `identity`, `command_json`,
`input_hex`, signed `code`, `data_hex`, `status`, `status_utf8_hex`,
`native_global_id` and `complete`. Its `native_process` preserves bounded
stdout/stderr, exit status and timeout/truncation state. Errors are retained
before a failed control returns. Complete success acknowledges
`mds-commands-native-completed.<requestID>`. The driver does not parse and
reformat the command's output bytes as a CLI might.

## Native OPEN-session absence and cleanup

Only in command mode, the fixed MDS fresh observer accepts both Go identities:

```go
testcluster.RequestControl(ctx, dir,
    "mds-fresh-observe", "mds-fresh-observed",
    token, fullGlobalID, readonlyGlobalID)
```

The two GlobalIDs must be positive, distinct and retain their order/binding
after the first observation. The existing result adds `expected_global_ids`
and any `present_global_ids`; `expected_global_id` remains the primary full
identity. The test must observe both absent while the relevant Go command
transports remain live. Request IDs still increase and the full native session
listing remains available. In ordinary fresh mode, the old single-GlobalID
protocol remains unchanged and rejects this extra argument.

Session absence establishes no OPEN entry at those observations. It does not
deny the existence of an authenticated closed Session object, prove absence
throughout an unobserved interval, or turn command transport into CephFS session
semantics. Native map GID provenance remains distinct from the rank in
ServerIdent and from cryptographic proof of an individual daemon's service key.

The shared single-attempt cleanup removes the UUID filesystem and its two owned
pools and revokes both identities. `mds-fresh-cleanup.json` additionally records
`command_identity_removed`. Attempted/unknown cleanup remains cached, so a
Terminate fallback never reissues uncertain mutations. The parent always
continues cluster cleanup. There are still nine owned containers; adding an
identity does not add a daemon or test container.

Mock/unit checks cover the exclusive profile, legacy/no-hash pool configuration,
separate credentials and caps, closed native actions, request replay protection,
the two-client observer binding and exact signed/binary native-result decoding.
They do not establish actual MDS command interoperability, native code/status
values, Go correlation/concurrency, or native cleanup. Those require the
separate actual acceptance run and its retained evidence.
