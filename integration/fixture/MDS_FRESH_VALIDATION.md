# Internal fresh MDS authentication and keepalive acceptance

The recipe is pinned to Tentacle Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. It exercises the internal
`session.HandshakeFreshMDS` foundation, with no public MDS connection API,
CephFS session OPEN, metadata operation, capabilities, reconnect or replay.
Native Ceph tools and the owned MDS are development-only fixture dependencies;
the product and this Go client build remain independent of CGO and librados.

## Recipe and ownership

`TestCephMDSFreshIntegration` skips before credentials, controller actions or
network activity unless `CEPH_MSGR_TEST_MDS_FRESH=1`. The disposable provider
starts its owned active filesystem/MDS before the test and publishes
`mds-fresh-oracle.json`. That receipt supplies source pin, ownership token,
cluster FSID, filesystem ID/name, native daemon GID, active rank zero and the
exact v2 numeric endpoint/nonce. The dedicated identity is
`client.mds-fresh-<token>`; `CEPH_MSGR_MDS_FRESH_IDENTITY` and
`CEPH_MSGR_MDS_FRESH_KEY_FILE` identify its private credential. Credential bytes
are never copied into public evidence. The provider owns filesystem, pool,
daemon and credential cleanup; there is no cleanup mutation or retry in this
test.

The test parses the dedicated key using internal CephX and requests precisely
AUTH, MGR and MDS tickets. The dedicated fixture identity has MON read and
filesystem-scoped MDS read caps, with no MGR caps. A parsed MGR ticket with
empty service caps grants no MGR operation here; the test opens no MGR
connection. It dials the fixture's numeric `mon.a` once and uses
the ordinary role-1 handshake with MON nonce zero. Modern OSD fixture admission
selects only the existing MON HASHPSPOOL opt-in; MDSENC is not selected. The
authenticated MON connection stays open throughout MDS acceptance. Its ticket
parser supplies one MDS service authorizer, and its GlobalID remains the client
identity for the subsequent connection. `CEPH_MSGR_TEST_KEY_TYPE` and
`CEPH_MSGR_TEST_SERVICE_CIPHER` must each explicitly select `aes` (type 1) or
`aes256k` (type 2); unknown or missing selections fail. The actual parsed
client key and MDS service-ticket key types must match those selections before
the MDS exchange is attempted. Recording a requested AES256K profile alone is
not accepted as evidence of an AES256K ticket.

Before opening the MDS transport, the test obtains an independent native
session listing through the fixed `mds-fresh-observe` controller. The request
contains the receipt token and this authenticated GlobalID. The result must
match source, token, cluster/filesystem, active rank, native daemon GID and
GlobalID, and must report that GlobalID absent from the session list. The
result is `mds-fresh-session-result.json`; its full public JSON is retained.

The test then dials that exact owned MDS endpoint once and calls
`HandshakeFreshMDS` with rank zero and ordinary MDS ServiceAuth. The foundation
validates the target address including nonce, rank-zero ServerIdent, fixed
features, non-lossy fresh cookies and authentication/security transcript.
It does not copy MON CRUSH admission, MDSENC or OSD profile features into MDS.
The handshake's context controls setup only. A separate established-I/O
deadline (five seconds, bounded by the outer context) and cancellation close
govern the following transport exchange.

After authentication the only test-initiated frame is one Messenger Keepalive
with an eight-byte seconds/nanoseconds timestamp. The matching KeepaliveAck
must echo those bytes exactly. An incoming peer Keepalive can be acknowledged
explicitly; at most eight incoming control frames are examined. Other frames,
including Message, recovery, session-open and metadata messages, fail the test.
No `Session` worker, automatic reconnect or redial is created. The test records
the post-handshake sent/received frame tags and public timestamp bytes, not an
unencrypted capture of handshake credentials.

While this authenticated Messenger transport remains open, a second native
observation must still show the client's GlobalID absent from CephFS sessions,
with the same ownership/active-rank binding and a distinct controller receipt.
The test closes MDS and MON transports, then publishes
`mds-fresh-results.json`. This records GlobalID, key type numbers (not keys),
selected key/cipher labels, requested services, connection mode, negotiated feature masks, exact target,
expected rank, native GID, single-attempt dial counters, keepalive ACK and both
native observations. The provider separately exports its teardown evidence.

## Identity and verification limits

The native owned MDS map identifies a daemon by GID and supplies its active
rank/address. Ordinary active ServerIdent uses the rank, so rank zero is the
expected peer ID; daemon GID is not substituted for it. CephX's MDS service key
is shared service authentication. Address/rank validation and the native owned
map do not establish cryptographic authentication of an individual daemon GID.
The authenticated client's GlobalID is a third, separate identity.

A keepalive ACK establishes this authenticated Messenger control exchange. It
does not establish CephFS OPEN, a filesystem session, capability processing,
metadata I/O, mount support, failover, recovery or uncertain-operation replay.
Native session absence is observed before the connection and while it remains
open after ACK; it is not evidence about every later daemon event. Synthetic
negative tests for malformed/authentication/recovery transcripts are separate
from this positive native acceptance recipe; see
[the internal fresh-MDS contract](../../internal/session/MDS_FRESH_CONTRACT.md).

## Observed local native results — 2026-10-15

The acceptance now maps closed `crc`/`secure` labels explicitly to wire modes
1/2 and rejects missing or unknown labels. A focused mapping regression covers
both values. Only secure was rerun after this test-only correction; the CRC
mode-1 path and product/fixture implementation were unchanged. CRC evidence
is deliberately reused from its successful earlier source. This does not
claim both profiles ran against the final acceptance-file hash together.

| Successful configuration | Native test result | Client/service key types | Wire mode |
| --- | --- | --- | --- |
| Darwin arm64 host, IPv4 relay, CRC, one BlueStore OSD, owned active MDS rank 0 | 1/1 passed, no skip, 2.42s | AES 1 / AES 1 | 1 |
| CGO=0 Linux arm64 container, IPv6, secure, one MemStore OSD, owned active MDS rank 0 | 1/1 passed, no skip, 2.21s | AES256K 2 / AES256K 2 | 2 |

Both fixtures used `replicated-pg-reply-v8` solely for their required MON/pool
admission and bootstrap recipe. The MDS transport used neither that OSD
encoding profile nor MDSENC. The native daemon version was
`ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)`
on the pinned arm64 image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Independent evidence audit

The successful receipts were read independently of the Go test exit status.
The native filesystem JSON equals its corresponding complete `fs dump`
entry at startup and in both observations. Each retained the same active
rank-zero daemon GID, address/nonce, filesystem ID 1, MDSMap epoch 7 and owned
metadata/data pool IDs 3/4. The two controller receipt IDs differ in each run.

| Public identity/control | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| Authenticated client GlobalID | 4797 | 4854 |
| Native active daemon GID / expected ServerIdent rank | 14446 / 0 | 4635 / 0 |
| Native v2 endpoint / nonce | `172.19.0.10:6800` / 1429692454 | `[fd62:93fe:2c9e::a]:6800` / 4247797332 |
| Keepalive and exact ACK hex | `20d5d06a0843f820` | `c4d5d06a52971300` |
| Post-handshake sent / received tags | `[18]` / `[19]` | `[18]` / `[19]` |

Both records have requested service mask 50 (AUTH/MGR/MDS), one MON dial and
one MDS dial, zero application Messages, an ACK verified before the live
native observation and a closed MDS transport. Negotiated MDS features are
exactly `3026423347916341302`, the implemented common feature set, with no
MON-only CRUSH admission or MDSENC bits. MON features are
`3314937399175578166`; these are a separate service's negotiation, not MDS
permission. Before and after, the native session listing has no client entry
and `expected_global_id_present=false` for the authenticated GlobalID.

Both successful cleanup receipts have `filesystem_removed`, `pools_removed`,
`identity_removed` and `completed` true, with `remove_pending=false`. The
final native FSMap is empty and the recorded owned pool IDs are absent from
the final native OSDMap. The successful run logs each match all nine owned
container IDs to completed termination. These are actual native removal and
fixture teardown receipts, separate from closing Go connections.

| Evidence SHA-256 | CRC/AES | secure/AES256K IPv6 |
| --- | --- | --- |
| `mds-fresh-results.json` | `5c17ce481632191d5f03b7aa5036d96d8d16a0b8c3aca6294b9ec1d0e1c2939f` | `f84bd568957b9bf5b181eb0ae86117b5df37d03d0be9ad4122edf063db417791` |
| `mds-fresh-cleanup.json` | `a311bb18c1cf1d36b75661827b2ff5f98d49c3c82f059b64cdaa2af143f8d9d3` | `09aabce5669c71718bd446a1308ae8dbd72c3d697adbcb0f9c16e635eba808e3` |
| Adjacent successful run log | `84fae71b704486f5325c985f7fe293f52e56656dcafaf196a360a924f00e7a3c` | `5059ad2f539908e8617161ff31f99f76085300cac0ce06a81cc7e4fa0d91d48e` |

The initial runtime acceptance source SHA-256 was
`b3403078f4929fe92e5c3a6e75eddc89fa11a6d411e188f4423ba71a2ae36728`.
The corrected acceptance source is
`4b1c7ddf57e15193cb71bf54fef368a22ab57ce7d95e21c19e0236267fe4e9c3`.
The tested internal handshake source hash is
`39c65b5d696d8f7b0e01a211e59c4f4e35200cc2c2a0d23b9cae64f3b5765446`;
the CephX authentication source hash is
`37adecbd532342f78dc2f34066d477e7d591251c6f9864a87617caffafbf5caf`.
After native execution, a documentation comment in the handshake source was
updated to point to the now-recorded acceptance. Its file hash became
`99bd477f3d871190efb7811988633d7b2ad6345fcb2328648121cb3f650631c1`;
the implementation did not change. Restoring the old comment reproduces the
tested handshake hash above.
These identify the tested source files; they do not imply these changes
already existed in a preceding base commit.

## Reproduction and remaining boundary

Run from the repository root with the disposable provider available. The
provider creates the dedicated identity and supplies private key-file paths.
For the CRC variant:


For secure, use `RUNTIME=container`, `IP_FAMILY=6`, `OSD_STORE=memstore`,
`CONNECTION_MODE=secure`, `KEY_TYPE=aes256k` and `SERVICE_CIPHER=aes256k`, each
with the same `CEPH_MSGR_TEST_` prefix, and a distinct diagnostics directory.
Keep the MDS opt-in and exact test selector. Native MDS service-mode defaults
remain separate from the client's single offered mode.

These local observations establish only the selected internal fresh
authentication and keepalive path. They do not establish a public MDS API,
CephFS OPEN, metadata/capability I/O, mounts, failover, recovery, replay,
other daemon configurations, or completed remote CI. Compilation, opt-in skip
and synthetic malformed-peer tests retain their separate evidence boundaries.
