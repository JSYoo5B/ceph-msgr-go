# MGR service close uncertainty and native expiry

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`.
Image: `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Each owned disposable fixture has three MONs, two MGRs, one MemStore OSD,
legacy-v6 profile, ordinary 12-second tickets and native 60-second beacon grace.

Native tested source: `bbb84c8590222dd70bf8fe13967995ae2e84de9a`.
The existing opt-in selector `^TestCephMgrServiceRegistrationIntegration$`
now requires `unconfirmed_close/receive-withheld` and
`unconfirmed_close/send-discarded`, after its earlier lifecycle/recovery cases.
No scenario, default CI trigger or product feature mask is added.

| Keys / mode / address / runtime | Actual parent / receive / send result |
| --- | --- |
| AES client/service, CRC, IPv4, host | PASS 156.55 s / PASS 7.61 s / PASS 65.91 s |
| aes256k client/service, secure, IPv6, container | PASS 152.05 s / PASS 6.61 s / PASS 63.91 s |

Both complete parent tests and both close-fault children actually passed. The
earlier renewal/failover child also passed (77.43 s CRC, 75.93 s secure). The
empty secondary `client` package selection adds no native evidence. Each run
logged termination of eight owned fixture containers; client containers and
owned reapers also exited. No unrelated resources were removed. Linux/amd64,
Go 1.26.9, Docker 29.9.1 and Testcontainers-Go v0.44.0 were used.

| Fault observation | CRC/AES/IPv4 | Secure/aes256k/IPv6 |
| --- | --- | --- |
| Receive withheld: native absent before cancellation | true | true |
| Receive withheld: final absence / total watch polls | 3.008 s / 4 | 3.007 s / 4 |
| Send discarded: native present after local Close | true | true |
| Send discarded: local discarded write calls | 1 | 1 |
| Send discarded: final absence / total watch polls | 3.008 s / 64 | 3.008 s / 63 |
| Each fault: MGR dial count | 1 | 1 |
| Each fault: unconfirmed, canceled, terminal, repeated result preserved | all true | all true |

The source above contains every native-tested implementation/helper change.
Later documentation-only commits do not represent another native execution.

## What each fault establishes

Both cases use a new Go client and a dedicated MGR service connection, with
automatic reports disabled. An explicit report establishes an independently
observed native metadata/status and authenticated GID baseline. Only after
authentication/configure and native visibility does the test arm the transport
fault. MON traffic is unaffected. No plaintext/ciphertext, credentials, session
secrets or runner endpoints are retained by the fault helper.

### Withheld receive bytes

The helper consumes incoming bytes from the established connection but withholds
them from the Messenger reader until local teardown. It does not identify or
decode the suppressed frame and therefore makes no captured-close-echo claim.
Outgoing bytes still reach the native MGR. The independent librados observer
must see the daemon removed while the caller's Close remains pending. Only then
does the test cancel that local wait.

Close must return ErrMgrServiceCloseUnconfirmed wrapping context.Canceled,
terminate the handle, and preserve that result on a repeated Close. Native
removal does not turn the unacknowledged caller result into success. The test
also requires three seconds of consecutive native absence afterward.

### Discarded outgoing bytes

After native visibility and the explicit beacon, the helper discards subsequent
writes while reporting local write success. This intentionally separates local
I/O admission from native receipt. It does not prove a transmitted close message
or count server executions. No automatic reports are enabled, and the client
does not perform ordinary MGR commands during this fault.

After canceling Close and checking the same uncertainty/terminal/idempotency
conditions, a native observation must still find the daemon registered. A later
read-only watch must observe its removal after native beacon grace, followed by
three seconds of consecutive absence. No close mutation is retried to obtain
that result. Each case permits only one MGR dial across its entire lifetime;
this local dial count is separate from sampled native absence.

## Bounds, records and limits

Close has a 15-second context deadline. After explicit cancellation, a separate
five-second local completion bound detects a stuck caller. Native presence/
absence queries use the parent operation context, with at most a 90-second
watch and the existing 120-second fixture command budget. Local teardown must
release the blocked reader; independent net.Pipe/race tests cover that helper
property and prove discarded writes never reach their peer.

The sanitized `mgr-service-close.json` receipt records fault type, client GID,
activated fault, uncertainty/cancellation, terminal/idempotent result, native
presence/removal, stable absence duration, total watch polls and MGR dial count.
Total polls include observations before absence; they are not all absent polls.
Session error text and addresses are deliberately excluded. The fixture's
public diagnostic allowlist includes this receipt.

Use the existing [MGR service recipe](MGR_SERVICE_VALIDATION.md) and catalog
group `mgr-service` on an authorized Linux runner. Require complete parent and
both child PASS; zero tests or skipped children do not establish acceptance.
Basic CI remains cluster-free. Earlier codec and recovery records retain their
original scopes and source identities. Separate Linux checks passed the new
helper tests, `-race -count=5`, shared request serialization, integration vet,
fixture unit/vet, all 546 Python tool tests and the 65-entry scenario catalog.

Applied metadata changes inside the native MGR daemon-state index, same-field
native service registration comparison, and other profiles remain separate
requirements in #6. These injected transport faults do not prove every possible
connection failure or server shutdown behavior.
