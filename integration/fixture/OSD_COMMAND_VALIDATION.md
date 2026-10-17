# D1 OSD and PG command contract and validation

Run window: 2026-10-15 Asia/Seoul. Product subset: raw daemon-local commands
to an explicitly targeted OSD (`OSDConnection.Command`) and raw PG commands to
the caller-selected primary (`OSDConnection.PGCommand`). MDS-targeted commands
(D2), wildcard expansion, target selection and retry policy remain outside the
product.

## Wire and contract

Pinned Ceph 20.2.4 (`7f793731f1b39eb4f465e960113d2363c311b964`) sends both
`tell osd.N` and PG commands as MCommand (97) with the cluster FSID and one
JSON string, and the OSD replies with MCommandReply (98): signed result,
status string and raw data (messages/MCommand.h, MCommandReply.h). A PG
command is the same message with `pgid` inside the JSON; there is no separate
PG message and MCommand carries no map epoch. OSD.cc:7379-7394 queues commands
to the admin-socket thread and refuses a session without the allow-all OSD
capability with -EACCES. For PG commands OSD.cc:2789-2826 parses the pgid
against its own current map: a local PG on a non-primary returns -EAGAIN "not
primary for pgid", and a missing PG returns -ENOENT; neither executes the
command. These sources carry LGPL-2.1 notices; no implementation is copied.

The Go client reuses its pinned MCommand/MCommandReply codec on the existing
lossy OSD session and the client-wide OSD TID sequence. Before transmission it
requires a JSON object with a non-empty string prefix, rejects a plain Command
whose JSON contains `pgid`, and requires a PGCommand's JSON `pgid` to equal the
canonical `<pool>.<hex seed>` of the caller's explicit PG and its Primary to
equal the connection's OSD ID (`ErrOSDCommandTarget`). It never injects or
rewrites JSON, never resends on EAGAIN, reconnection or map change, and keeps
cancellation, Close, connection loss and invalid replies as
OutcomeUnknownError with the local submission (TID, global ID, OSD ID, PG
metadata). Result codes are Linux wire errno values. A command may block later
commands on the same OSD because the server runs them one at a time.

## Independent evidence

Synthetic peers verify pre-TID/no-send rejection, the exact FSID/JSON vector
and copied bulk input, TID matching independent of reply order with interleaved
object requests, known -13/-22/-11/-2 refusals, invalid reply fronts and
compatibility as terminal uncertain outcomes, late replies after cancellation
being dropped without resend, and an MCommandReply arriving for an object TID
failing the session.

A test-only native oracle (`tools/osd_command_native.py`) calls
`rados_osd_command`/`rados_pg_command` through the pinned C API in the fixture
container with fixed fixture identities and records the native code, output
and status bytes.

| Actual profile | Observed result |
| --- | --- |
| CRC/AES entity/service, IPv4, Darwin arm64 host relay, three BlueStore/min2 | 11/11 subcases in 9.42s (second run) |
| secure/AES256K entity/service, IPv4, Linux arm64 container, three BlueStore/min2, CGO0 binaries | whole OSD suite 25/25 top-level, OSD commands 11/11; no FAIL/SKIP |

| Case | Observation |
| --- | --- |
| `version`, also with 1 MiB ignored bulk input | code 0, 85 bytes, byte-identical to native |
| `config get` (osd_max_write_op_reply_len) | code 0, 35 bytes, byte-identical |
| `get_command_descriptions` | code 0, 33,895 bytes, byte-identical |
| Unknown prefix / missing required argument | -22 with the same status string as native |
| client.readonly (osd `allow r`) | OpenOSD and object STAT succeed; Command -13 with empty status, same as native |
| PG `query` on the primary | code 0; pgid, state `active+clean` and acting set match native pg_command |
| PG `query` on an acting replica | Go -11 "not primary for pgid" once, then the session served `version`; native librados resent until its 5 s operation timeout (-110) |
| PG `query` for absent pool 4095 | -2 with the same status string as native |
| 8 commands and 8 object STATs concurrently on one session | all succeeded with unique TIDs |
| Primary container paused | a 1.5 s cancelled command and a command pending during Close returned OutcomeUnknownError (Close cause retained); after resume a fresh handle served `version`; no resend or reconnect |

The first run passed the first eight cases. The replica case then waited for
the native oracle, because native Objecter resends EAGAIN commands
(Objecter.cc:5058-5067) and the test had no native timeout; the test context
was also derived from the fixture's 45 s setup context, so the remaining cases
timed out. The second run bounded the native call with `rados_osd_op_timeout`
and used its own test context.

Native and Go cleanup left no owned containers or network. Logs and
diagnostics: `/tmp/ceph-osd-commands-crc-aes-20261015-second` and
`/tmp/ceph-osd-whole-secure-aes256k-container-d1-20261015-first`.

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDCommandsIntegration$' \
sh integration/run.sh
```

## Remaining gaps

SIGKILL or restart of the OSD during a long command, a MON capability change
on an existing connection, an OSD outside the acting set, larger outputs near
MaxFrameSize, counting server-side executions through admin-socket debug logs,
broader profiles and remote CI for this subset need separate evidence.

## OSD SIGKILL (2026-10-17)

`TestCephOSDCommandCrashIntegration` (legacy-v6, single BlueStore and three
BlueStore replicas): `version` succeeds, the target OSD is SIGKILLed, and the
next command on the same connection fails with the session's own cause (`EOF`:
the session had already ended, so the command was never sent and is not an
unknown outcome). The old connection never recovers by itself. After restart
the OSD has a new address nonce; a new connection to it answers `version` with
the same bytes and a later transaction ID. A command in flight at the moment
of the kill was not reproduced.
