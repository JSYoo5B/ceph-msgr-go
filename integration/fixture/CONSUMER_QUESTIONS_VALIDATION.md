# Consumer follow-up for fixture questions

Reference: published ceph-testcontainers-go v0.2.0 and Ceph 20.2.4,
commit `7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
This record concerns consumer test preparation and diagnostics. Product
dependencies and the public Messenger API retain their existing scope.

## MON membership changes: issue 65

After each once-only raw MON add/remove, the Go map-wait assertion runs first.
Before a fresh native MonMap oracle starts, the fixture then observes native
`quorum_status` with public `RunNativeCommand`. Two complete observations must
match the expected FSID, map epoch, member names/ranks and owned quorum, and
have the same election epoch at least two seconds apart. A changed election
epoch, incomplete quorum or timed-out read clears the stability window.
Schema/identity errors, truncated output and unconfirmed native worker stop
fail preparation. Each read is bounded at five seconds; the total barrier is
bounded at thirty seconds. No MON mutation is repeated. The fresh native
oracle has a thirty-second bound. Bootstrap addresses are not refreshed to
include the unreachable MON.

Public bounded history and separate added/removed receipts preserve both
barriers. This proves observed stability between samples, not the absence of
all later elections. The evidence-bound native MGR recovery is recorded
separately in [NATIVE_MGR_PREPARATION_VALIDATION.md](NATIVE_MGR_PREPARATION_VALIDATION.md).

## Pool admission: issue 68

The OSD profile passes `osd_pool_default_flag_hashpspool = false` in the global
bootstrap configuration through `WithConfigFile` before any daemon starts.
There is no post-bootstrap pool-flag repair. Explicit data-pool `NoHashPSPool`
is retained. Bounded read-only native observations wait for the data pool and,
with two MGRs, the automatic `.mgr` pool. Every pool is checked for HASHPSPOOL.
Actual FSID, positive epoch, pool IDs, unique names and present numeric flags
are required. Other flag bits are retained, not reported as zero.
`pool-policy.json` records the bootstrap mechanism and the actual native map.

## Go authentication provenance: issue 71

The opt-in integration observer joins a successful local ticket parse to its
authenticated MON session and actual publication sequence, then to the frozen
MGR ticket selection and fresh authorizer envelope. Retained tickets keep their
original issuer. Run-local keyed labels compare blob identity without emitting
ticket, proof or key bytes. Local protocol-frame writes, authorizer AUTH_DONE
verification and completed handshakes are distinct observations. A write alone
does not prove server acceptance. Installation callbacks can arrive after
publication; the observer joins them by opaque receipt identity and context.

Cached map/auth epochs and a local parse identity do not establish native
rotating-key generations. Stock Ceph and v0.2.0 do not expose the MGR verifier's
selected key at the failed attempt. That native observer capability and the
intermittent failure's cause remain unresolved; no wipe/authentication retry,
native key dump or automatic mutation replay is introduced.

## Validation

Whole-product and fixture-module `CGO_ENABLED=0` tests and vet passed. Product
authentication/session/integration race tests and changed fixture race tests
passed. The existing Python suite passed all 230 tests. Receipt tests include
retained issuers, reused IDs, late callback delivery across cycle changes,
canceled joins, callback reentry/Close races, zero/partial/full protocol writes
and explicit refusal after a completed write. Public product dependencies
remain Go standard library and this repository's packages.

Both first native runs used one immutable 450-file source snapshot based on
`source-1` plus the consumer changes, Darwin arm64 host clients built with
`CGO_ENABLED=0`, and Linux arm64 daemon containers. No test failed or skipped.

| Actual profile | First-run result |
| --- | --- |
| secure/AES256K entity/service, IPv4 host, one MemStore OSD, two MGRs, ordinary 12s tickets | Five top-level tests passed: MapWait 18.62s, mutations 4.72s, OMAP 9.48s, OSD I/O 0.01s, unsupported features 1.75s |
| secure/AES entity/service, IPv4 host, no OSDs, two MGRs, 120s tickets | ServiceKeyEpoch passed in 7.71s with two once-only wipe cycles |

The first profile preserved both settled-election receipts: added map epoch 4,
election epoch 14, four members and owned quorum a/b/c, stable for 2.135275s;
removed map epoch 5, election epoch 20, three members and the same quorum,
stable for 2.115375s. Each pair matched its request/expected identities and
bounded history. Native OSDMap epoch 12 reported `.mgr` ID 1 and the data pool
ID 2, both with actual flags 0. The fixture's global bootstrap setting was
false before startup. MON admission and explicit OSD bytes/native comparisons
passed; the mutation and OMAP suites included 25 and 23 cases respectively.

Each authentication cycle recorded exactly one cold MGR attempt, two encoded
authorizer rounds and two completed local protocol writes (226/214 bytes),
matching selected/encoded run labels, verified AUTH_DONE and a completed
handshake. Both issuing fixture routes were mon.a. Local installation sequence
2 remained associated with cycles 1/2 and selected client IDs 4404/4431;
SecretID 2 repeated without being treated as a generation identity.
The native verifier-generation result is explicitly unavailable, not inferred
from these successful handshakes. Prior intermittent failures remain evidence.