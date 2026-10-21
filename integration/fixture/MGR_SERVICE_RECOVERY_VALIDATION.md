# MGR service ticket expiry and active-manager change

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`.
Image: `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Linux/amd64, Go 1.26.9, Docker 29.9.1, Testcontainers-Go v0.44.0.
Each owned disposable fixture has three MONs, two MGRs and one MemStore OSD,
legacy-v6 profile, ordinary 12-second AUTH/service tickets and the native
60-second service beacon grace. The configured report period was 1 second.

Tested source: `97ad9d7a245f952d038e1574a5903bd93729ea1a`.
Later documentation-only commits do not constitute another native execution.
The selector remains `^TestCephMgrServiceRegistrationIntegration$`, with
`CEPH_MSGR_TEST_MGR_SERVICE=1`. Its new required subtest is
`renewal_and_active_manager_change`; the existing catalog group `mgr-service`
selects it through the parent test. Basic CI does not start these fixtures.

| Keys / mode / address / runtime | Actual parent / recovery subtest |
| --- | --- |
| AES client/service, CRC, IPv4, host | PASS 83.83 s / PASS 77.13 s |
| aes256k client/service, secure, IPv6, container | PASS 81.53 s / PASS 75.93 s |

Both complete parent tests and the recovery subtest actually passed. The empty
secondary `client` package selection contributes no additional native evidence.
Each run logged termination of eight owned fixture containers. The secure
client container and owned reapers also exited; no unrelated resources were
removed.

| Native observation | CRC/AES/IPv4 | Secure/aes256k/IPv6 |
| --- | --- | --- |
| Active name/GID before | a / 4167 | a / 4167 |
| Active name/GID after | b / 4245 | b / 14126 |
| MGR map epoch | 5 to 7 | 5 to 7 |
| Original/reopened client GID | 4635 / 4635 | 4635 / 4635 |
| Stable absence / observation count | 3.008 s / 65 | 3.008 s / 65 |
| Explicit registration start epoch | 7 | 7 |
| Terminal cause classified as ErrManagerChanged | yes | yes |
| Report/update refusal, close-cause preservation, final native removal | all true | all true |

These GIDs and epochs identify disposable native observations, not fixed inputs
or values required by the test.

## Assertions and independent observations

1. Native fixture preparation establishes active and standby MGRs before the
   tested transition. The existing native named-MGR oracle brackets exact
   active names, GIDs and map epochs before/after, using native CLI commands.
2. An explicitly opened Go service is visible through independent librados
   `service dump` and `service status` commands. The test waits until the
   initially observed MGR ticket has expired and the client exposes a later
   expiry with the same authenticated GID. A report on the existing service
   is then independently observed with its replacement status.
3. One `mgr fail` mutation targets the observed active name. An uncertain
   mutation is never retried. The new active name/GID and increased map epoch
   are confirmed natively and matched by the Go snapshot.
4. The old service's Done closes with a non-nil terminal cause. Report and
   metadata update are refused as closed with that cause, and Close preserves
   it. Transport termination may precede a map change; the contract does not
   require every real failure to return ErrManagerChanged specifically.
5. The ordinary MGR command connection is prepared on the replacement without
   another service-open invocation. The read-only native service oracle waits
   through expiry and requires three seconds of consecutive absence, resetting
   its interval if the daemon reappears. This is sampled native absence, not a
   wire-transmission or server-execution counter.
6. Only then does the caller explicitly open the same identity again, with new
   metadata/status. Native observation checks its original client GID and a
   registration start epoch newer than the absent map. The new connection is
   opened after the initial ticket expiry using the current authorizer. An
   acknowledged close and native map removal complete cleanup.

An established authenticated session remaining usable after ticket expiry is
distinct from authenticating a new connection with a renewed ticket. This test
observes both stages; it does not claim in-session reauthentication.

The public `mgr-service-recovery.json` receipt retains client/manager GIDs,
epochs, ticket expiries and bounded observation/refusal outcomes. It deliberately
omits session error strings and addresses. The native oracle still keeps its
separate command receipts in the private fixture output. Public diagnostic
selection includes the new sanitized receipt.

## Execution and limits

Use the existing recipe in [MGR_SERVICE_VALIDATION.md](MGR_SERVICE_VALIDATION.md)
on an authorized Linux runner. Both profiles use the same explicit selector and
require complete parent/subtest PASS. Fixture command execution permits 120 s
for the bounded native service watch (request wait at most 90 s); other owned-
pool modes retain their existing 80 s command budget.

Synthetic clock tests check reappearance resets, deadline refusal, input bounds
and the existing optional-map serialization contract. Full Linux Python tools
passed 546 tests, scenario validation passed 65 entries, and integration vet plus
fixture unit/vet checks passed. These are separate from live daemon evidence.

A later [close-fault checkpoint](MGR_SERVICE_CLOSE_VALIDATION.md) separately
observes unconfirmed Close with native removal/expiry. Applied metadata updates
inside the MGR daemon-state index, comparison against a native registration
with identical fields, and other profiles remain unverified. The
failed old handle here follows MGR replacement, not a deliberately lost close
acknowledgement. This checkpoint does not complete #6.
