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

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDCommandsIntegration$' \
sh integration/run.sh
```

## Remaining gaps

Counting server-side executions through admin-socket debug logs, broader
profiles and remote CI for this subset need separate evidence.

## OSD outside the acting set (2026-10-18)

`TestCephOSDCommandOutsideActingSetIntegration` creates a single-replica
owned pool on the three-OSD fixture, so two OSDs hold none of its PG, and
sends `query` for that PG to one of them, naming it as primary. The OSD
answers a known `-2` with `i don't have pgid <pg>`, no output and no retry;
the native client's command to the same OSD gets the same code and message,
and the Go session then answers `version`. Passed in legacy-v6 three BlueStore
(secure AES256K IPv4; PG 3.0 on osd.0, asked at osd.1) and replicated-pg-v8
three BlueStore (secure AES256K IPv6; PG 3.0 on osd.1, asked at osd.0).

## Reply frame limit (2026-10-18)

`TestCephOSDCommandFrameLimitIntegration` measures the fixture OSD's
`config help` JSON (every option's schema, 671,682 bytes, stable between
calls) and dials two more clients. With `MaxFrameSize` 1 KiB above the output
the command returns the same bytes. With 1 KiB below it the reply frame is
refused: the call is an unknown outcome wrapping `ErrLimitExceeded`, with its
transaction ID and no code or output, and the connection does not recover by
itself; a new connection from the same client answers `version`. (A limit
near the much smaller `config show` output, about 77 KB, could not be tested:
the MON's bootstrap frames are larger, so admission itself fails.) Passed in
legacy-v6 single BlueStore (IPv4) and request-v9/reply-v8 secure AES256K IPv6
single MemStore.

## OSD SIGKILL (2026-10-17)

`TestCephOSDCommandCrashIntegration` (legacy-v6, single BlueStore and three
BlueStore replicas): `version` succeeds, the target OSD is SIGKILLed, and the
next command on the same connection fails with the session's own cause (`EOF`:
the session had already ended, so the command was never sent and is not an
unknown outcome). The old connection never recovers by itself. After restart
the OSD has a new address nonce; a new connection to it answers `version` with
the same bytes and a later transaction ID.

In flight (2026-10-18, `TestCephOSDCommandCrashInFlightIntegration`): the
target OSD's runtime `osd_bench_small_size_max_iops` is raised to 100000 with
an OSD `config set` command, then a `bench` command (1 GiB of 4 KiB overwrites
inside one 4 MiB object, so the device does not grow) is sent and the OSD is
SIGKILLed 0.5 s later. The call ends as an unknown outcome (connection EOF)
with its transaction ID and no result code or output; nothing is resent. After
the restart (which also drops the raised limit) a new connection answers
`version`. Passed in legacy-v6 single BlueStore (IPv4, 0.7 s in flight) and
three BlueStore replicas (secure AES256K IPv6, 0.8 s in flight).

A second round of the same test (2026-10-18) stops the restarted OSD with
`docker stop` (SIGTERM) instead, 0.5 s into another bench. The daemon sent no
reply while shutting down: the call ended as an unknown outcome (connection
EOF, 1.3 s and 1.9 s after the send) with its transaction and no output, and
a new connection after the restart answered. Same two profiles.

## Capability change propagation (2026-10-17)

`TestCephOSDCommandCapsChangeIntegration` creates a test-only identity with
`mon 'allow r'` and `osd 'allow r'` through MON `auth get-or-create` (removed
with `auth rm` in cleanup). Its `{"prefix":"version"}` OSD command is refused
with -13. MON `auth caps` then widens the OSD grant to `allow *`. Two seconds
later the same OSD session still gets -13, and so does a new OSD session of
the same client: that client reuses its cached OSD service ticket, which
carries the capabilities the OSD evaluates. A newly dialed client with fresh
tickets gets 0. The client never refreshes tickets because of a capability
change; a caller that needs the new grant sooner dials anew (or waits for the
normal ticket renewal). Passed in legacy-v6 single BlueStore (secure AES256K
IPv4) and request-v9/reply-v8 secure AES256K IPv6 single MemStore.

Ticket renewal boundary (2026-10-20,
`TestCephOSDCommandCapsRenewalIntegration`): the fixture's service tickets
live 12 s (auth_service_ticket_ttl in each daemon's ceph.conf; a MON config
database value does not override it). An `osd 'allow r'` identity's held OSD
session is refused `version` with -13; `auth caps` widens the grant, and the
held session stays -13. About 9 s after issue the client renews its OSD ticket
on its own schedule (three quarters of the lifetime); the held session still
gets -13, while an OSD session opened after the renewal gets 0. Passed in
legacy-v6 single BlueStore (secure AES256K IPv4) and request-v9/reply-v8
secure AES256K IPv6 single MemStore.

MON commands behave the same way (2026-10-18,
`TestCephMonCommandCapsChangeIntegration`, base MON/MGR fixture): an identity
with `mon 'allow r'` is refused a `config set` on its own section with -13
while it can read `fsid`; after `auth caps` widens it to `mon 'allow rw'`,
the existing MON session still gets -13 two seconds later (the MON evaluates
the capabilities it recorded when the session authenticated), and a newly
dialed client gets 0. The option is removed afterwards. Passed in secure
AES256K IPv4 and IPv6 container profiles.
