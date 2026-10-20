# Service-key epoch first failure, 2026-10-15

## Status and evidence boundary

Authentication epoch handling and the limits of native readiness probes are
described in the failure observations below.

## Exact execution

The failed job's completed log was obtained through its individual job-log REST
endpoint; its uploaded artifact was downloaded without restarting or cancelling
CI. The checked-out source in that log is the full SHA above. The uncommitted
MDS fresh-session development was not part of this execution. The relevant
auth-epoch test, native readiness shell recipe, and service-key oracle were
unchanged from that SHA during this investigation.

| Extracted evidence | SHA-256 |
| --- | --- |
| `ceph-tests.log` | `9ac5dd09d913517c83c32c3ca1f061cc73b2f6cec3f0b6b97c60d03e1240e104` |
| `ceph-diagnostics/provider-diagnostics.json` | `34762a301da3c1f16fc0c0dcc428ecd7665d73ee45fe4dfbb1289c2bdc646512` |
| `ceph-diagnostics/service-keys-history.jsonl` | `1184f51d8b7919b2ed77c94c14079803efadfc9d45599d1440448b10d2707fba` |
| `ceph-diagnostics/fixture-manifest.json` | `0ca41795cf37aca888f7e3aa5ec1cbd183f1d023ae11f8f54f679480df981ada` |

## Observed sequence

`TestCephServiceKeyEpochIntegration` performs two actual rotating-key wipes. Its
first cycle passed, including the cold Go client's first MGR handshake using its
replacement ticket. The second cycle failed immediately at that first cold
handshake; no subsequent successful authentication replaced the failure.

In cycle 2, MON state advanced from map epoch 4/auth epoch 10 to map epoch 5/auth
epoch 11. The held client's identity remained 4434 and its established MGR
connection remained at one dial. The target remained mgr.a, global ID 4191,
MGR-map epoch 4, endpoint `[fd7e:1a8d:99c::6]:6800`.

The cold client was 4464. The selected MGR ticket's local parse/install receipt
was cycle 2, install sequence 2, service 16, secret ID 2, with expiry
`2026-10-15T13:13:59.21351321Z` and scheduled renewal
`2026-10-15T13:13:29.21351321Z`. Its authenticated issuer connection was mon.a at
`[fd7e:1a8d:99c::2]:3300`; the issuer's cached map/auth epochs were 5/11. These
epochs are observations on that authenticated connection, not key-generation
identifiers carried by the opaque service ticket.

The run-local HMAC labels link the install receipt, selected ticket, and blob
encoded into the authorizer:

| Label | Value |
| --- | --- |
| Installed/selected/encoded ticket blob | `80c2aeafe59a9bda4b5b8f4ce5b90b074e3a77843e217c855a5a68511f6aa8a0` |
| Authenticated issuer session | `a0a169c9309bad6802cde10c5eeb4dae31a318e7a28dcfbc021fc735b192c63c` |

These are keyed, run-scoped equality labels. They disclose neither the ticket
bytes nor the service key and cannot be compared across independent runs.

The first cycle-2 cold attempt, trace attempt 3, occurred around
`2026-10-15T13:12:01.630Z`, before scheduled renewal. The trace recorded exactly
one encoded round and one complete local `AUTH_REQUEST` (tag 2) write of 309
bytes. It recorded no `AUTH_REQUEST_MORE` write, no verified `AUTH_DONE`, and no
completed handshake. The server explicitly refused authentication method 2
with code `-13`. Local write completion alone is not remote acceptance; the
matching server refusal and daemon log provide the remote failure evidence.

`ceph-tests.log` line 80 contains this bounded trace. The job log reports the
failure at `integration_auth_epoch_test.go:192` and
`--- FAIL: TestCephServiceKeyEpochIntegration (9.76s)`.

The native readiness history contains nine accepted observations in each cycle.
In cycle 2, all three MONs reported local map/auth epochs 5/11, each MGR-map
query reported the same active mgr.a/4191, and each fresh native named-MGR
`version` command succeeded. The last accepted native response was recorded at
`2026-10-15T13:12:01.545925Z`. These probes establish current MON observations,
target identity and successful native authentication with the CLI's own ticket.
They do not prove that the Go client's earlier ticket and the MGR verifier used
the same actual rotating-key generation. Native CLI bootstrap/reconnection may
follow its own policy; the Go subject's first handshake was submitted once.

## Server evidence and pinned-source interpretation

The mgr.a container log, `provider-diagnostics.json` artifact index 5, ends with:

```text
2026-10-15T13:12:01.629+0000 ... cephx: cephx_verify_authorizer: could not decrypt ticket info: decryption failed
```

This corresponds to the failed cold handshake. In pinned Ceph source
`src/auth/cephx/CephxProtocol.cc:489–507`, this diagnostic occurs after
`get_service_secret(service_id, ticket.secret_id, service_secret)` succeeds and
the ticket-info decryption fails. Global-ID validation and authorizer challenge
verification occur afterward. With the Go trace's secret ID 2, the combined
evidence establishes a numeric-ID lookup followed by failure to decrypt the
selected opaque ticket. It is not evidence of a missing secret ID, challenge
failure, or successful authorizer verification.

Numeric secret ID 2 does not identify one generation across wipes.
`src/auth/cephx/CephxKeyServer.cc:501–536` constructs fresh pending rotating
state for a wipe, retains the AUTH service's previous keys, and generates the
other services' keys. `src/auth/Auth.h:196–209` initializes a fresh
`RotatingSecrets` counter at zero and increments it when adding keys. An MGR
service ID can therefore be reused for different key material after another
wipe. `src/auth/RotatingKeyRing.cc` looks up the verifier's key by numeric ID;
the captured log does not fingerprint the returned key.

The native MonClient invalidates tickets and wipes rotating secrets when it
observes an increased auth epoch (`src/mon/MonClient.cc:473–475,658–665`), and
refreshes rotating secrets separately (`:1157–1203`). This source behavior
explains why map/auth observations, ticket receipt identity, verifier cache
contents and their timing must be distinguished. It does not identify which
side was stale in this execution.

The captured data cannot establish equality between the actual MON issuance key
and the actual MGR verifier key. It therefore cannot assign the root cause to a
particular MON snapshot, MGR cache update, service-key generation, or Go codec
defect. It also does not establish that the failure is a harmless flake. The
exact source/image failure remains an unresolved interoperability result.

## Bounded next diagnostic proposal

No additional daemon was started for this investigation. A future disposable,
explicit diagnostic fixture could instrument only the pinned native ticket
issuance and MGR verifier boundaries:

1. Generate one ephemeral random HMAC key for that fixture, kept outside logs
   and result artifacts. Use domain-separated keyed labels for the exact opaque
   ticket blob and for the copied service key's type/material. Never print the
   original bytes or enable broad Ceph authentication debug levels that dump
   secrets.
2. At MON issuance and MGR verification, record only the labels, numeric service
   and secret IDs, daemon identity, local monotonic sequence and result. At the
   verifier, label the actual key returned by the lookup, rather than a later
   cache inspection. Correlate these events to the Go authorizer's ticket-blob
   label using a shared diagnostic-only label domain/key.
3. Cap collection to this test's two wipes and first cold handshake per cycle,
   with a fixed small record/byte budget. Preserve the first refusal and stop;
   do not retry it, replay the wipe, or change readiness delays/assertions to
   obtain a pass.
4. Record the instrumented source, build and image identities separately from
   this unmodified-image CI failure. A new reproduction may resolve an equality
   question but cannot retroactively establish the missing keys in this run.

This proposal requires new native diagnostic work and independent validation of
its secrecy and correlation boundaries. It is not implemented or a fix claim.

## Root cause (CI triage, 2026-10-16)

The four auth-epoch failures (source-3, source-4, source-5, source-6) share one
mechanism. In each, three fresh native `tell mgr.a version` probes made after
the second wipe succeeded shortly before the Go attempt, so `mgr.a` held the
post-wipe key 2; the Go blob failed with `could not decrypt ticket info`,
meaning it was encrypted with the pre-wipe key 2. The Go cold ticket was
minted 2.6 to 5.4 ms after the wipe command on a new MON connection.

In the pinned source the wipe and the auth epoch bump commit together
(AuthMonitor.cc:2107-2141) and the commit is applied under `mon.lock`
(Paxos.cc:831-842, 896-947), refreshing MONMAP before AUTH
(Monitor.cc:1037-1042). MonmapMonitor pushes the new MonMap to subscribers
during its refresh (MonmapMonitor.cc:85-114, 1535-1563), before AuthMonitor
applies the keys (AuthMonitor.cc:333-398), while new msgr2 handshakes take
only `auth_lock` (Monitor.cc:6522-6530) and can mint tickets from the
KeyServer mid-refresh. The client renewed on a fresh connection right after
the push and walked into that window. A native MonClient renews in-session
with MAuth, dispatched under `mon.lock` (Monitor.h:980-984), so it always
lands after the refresh. This is a client race, not a provider issue.

The product now records, per MON endpoint, the auth epoch of the first
MonMap a session receives (the reply to its subscription, sent under
`mon.lock` after the refresh). Renewal tickets from a handshake that started
before that MON proved the current auth epoch are discarded and the client
authenticates to the same MON once more. The synthetic auth-epoch tests
cover the extra handshake and that the raced tickets are never published;
the live cold-cycle rate (about 2.3% per cycle before the change) has not yet
been remeasured.

## Local soak after the fix (2026-10-16)

With `CEPH_MSGR_TEST_AUTH_EPOCH_CYCLES`, product `source-1` ran 300 cold cycles
on each CI auth-epoch row profile (aes/aes IPv4, aes256k IPv6, container
runtime, arm64 Docker Desktop) from a clean export, plus an 80-cycle run with
MON debug logging: 680 of 680 first cold MGR authentications succeeded and no
-13 occurred. The barrier handshake runs on every renewal after a wipe (two
dials to the same MON, about 3 to 5 ms). With MON debug logging, two renewal
handshakes of other clients were minted from the pre-wipe key between the
MonMap push and the AuthMonitor refresh, and in both the barrier handshake
replaced them with new-key tickets. Zero failures in 600 cycles bounds the
post-fix rate below about 0.5% (95%), but no local pre-fix baseline was run,
only the leader issued tickets, and CI amd64 runners were not used. MON
`debug_auth` 10 logs plaintext session keys and service secrets in this
image, so diagnostics must not raise it in collected artifacts.
