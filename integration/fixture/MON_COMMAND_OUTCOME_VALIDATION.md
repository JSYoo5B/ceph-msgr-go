# Independently observed MON applied mutation and no replay

This is the first bounded implementation checkpoint of #63, not complete
command-channel acceptance. The product API and replay behavior are unchanged.
MGR, current/exact Tell and targeted OSD/PG/MDS native applied-mutation cells
remain open in [the channel matrix](../../cephmsgr/COMMAND_CHANNEL_MATRIX.md).

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Pinned `src/mon/KVMonitor.cc:280–329` consumes the bulk config-key set input and
arranges the command acknowledgement after the update commits. Python librados
is a development-only independent observer inside the disposable native image.

## Executable boundary

Selector: `TestCephAppliedMonitorMutationIsNotReplayedIntegration` in the
`cephmsgr` suite. It uses the existing ordinary three-MON/two-MGR fixture; no
new selector opt-in or catalog row is introduced. Initial target profile:
`mon-mgr.aes.ip4.crc.host` (Linux/amd64, CRC, AES client and service, IPv4,
host test process and owned relay). Reproduce with:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_RUN='^TestCephAppliedMonitorMutationIsNotReplayedIntegration$' \
sh integration/run.sh
```

1. Generate a 128-bit random token and closed `native-mon-no-replay-` key.
   Fresh native config-key get must return signed `-2` before the native worker
   exclusively records ownership; a pre-existing key is never claimed/removed.
2. Submit binary mutation A once through the Go MON command API, arm the
   connection's receive withholding, and require both independent native A
   and the blocked-receive signal before proceeding.
3. In CRC mode a test-only net.Conn wrapper decodes complete validated CRC
   command frames. Bind the original TID, sequence, authenticated global ID
   and fixture MON role to the exact request front/data digest and a local
   connection ordinal. Preserve partial local write facts separately.
4. A separate Go caller submits a distinct binary overwrite B once. Fresh
   native get must confirm B before restarting the original MON. Recovery
   rejects that role's endpoint and must authenticate another MON connection.
5. The original caller must return `OutcomeUnknownError` without an acknowledged
   result. A new caller-directed read on the replacement connection and fresh
   native get must both retain B. The original identified mutation still has
   exactly one complete local write. A later observation never acknowledges A
   retroactively, and these local writes are not server execution counts.
6. Cleanup has its own fresh bounded context. The closed worker exclusively
   marks cleanup started, submits one native `config-key rm`, publishes its
   original signed result and binary/status output, then uses another fresh
   native handle to require signed `-2`. No mutation retries are allowed. A
   timeout retains the started marker and uncertainty; the test fails instead
   of treating a later absence as successful acknowledged cleanup.

The controller validates request IDs and closed phases, places each request
atomically, runs the Python process under `timeout 25`, and publishes its
request-specific acknowledgement only after atomic result publication. The
external timeout is necessary because the pinned Python `mon_command` ignores
its timeout argument. Results are bounded at 64 KiB data and 8 KiB status,
with base64 preserving binary bytes. Only the owned key can be read/removed;
this worker has no arbitrary command or set operation. Receipts retain native
signed code, raw binary bytes, status, request ID, token and phase privately.
The fixture's metadata allowlist includes `mon-command-outcome-native.jsonl`
and `mon-command-outcome-identity.json`; failure diagnostics can retain the
original observations, partial identity progress and cleanup uncertainty.

## Execution evidence

Initial native receipt: source
`fa71fb23a132297a07209d1f8e6564bdec24c492`,
[CI command run 39926008773, attempt 1](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39926008773),
existing `mon-mgr.aes.ip4.crc.host` row, Linux/amd64. The scenario's
`result.json` names that exact source, the image above and `success`;
`tests.json` reports this client test as actual `PASS`, not skipped. The broad
row has 56 top-level passes and 161 optional skips, with no result-gate problems;
those skips do not establish their dedicated-profile acceptance.

The applied MON test passed in 2.72 seconds:

| Observation | Retained result |
| --- | --- |
| Initial fresh native get | Signed `-2`, empty data/status; ownership claimed only after this result |
| Independently applied binary A | Code `0`, 49,152 bytes, SHA-256 `94e28ae25574044f433b5c0f0b09b531ae5bf6c22ccb083c9cbfc494082820a5`, empty status |
| Original CRC submission | `mon.a`, local connection 1, authenticated global ID 24318, TID 1, sequence 2; complete local write and blocked receive |
| Distinct binary B before MON replacement | Code `0`, 48 bytes, SHA-256 `982143c94c39e7fd7847e5d58c6e06389b0ca7f7b7116add0acccf91e61d1112`, empty status |
| Original caller after MON replacement | `OutcomeUnknownError`, no acknowledged server result; no caller reinvocation of A |
| Recovery read and fresh native B | `mon.b`, local connection 2, global ID 24318, TID 1, sequence 2; native code `0`, same 48 bytes/hash/status as B, original mutation local writes still 1 |
| Native cleanup | One removal, signed `0`, empty data and status `key deleted`; another fresh native handle returns signed `-2`, empty data/status |

TIDs are scoped to their sessions: the two TID values both being 1 is expected
and is not a replay identity by itself. The distinct authenticated connections,
MON roles and exact request front/data digests preserve the original binding.
The CI log reports termination of all seven fixture-owned containers. This
record does not assert an independently queried final Docker/network inventory.

[Basic CI run 39925955623, attempt 1](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39925955623)
passed on the same source: both jobs, 640 Python tool tests, scenario validation,
product build/vet/dependency checks, unit/race checks, the cluster-free integration
gate and fixture build/vet/unit checks. This is separate from native execution.
The first local Linux check had a new regression incorrectly corrupt an unused
CRC epilogue slot; changing the injection to an actual protected front byte
made that regression meaningful. It was a test error, not a native replay defect.

The later non-secret identity receipt permission is 0644 inside the private
fixture directory, with a focused regression for host/container UID readability.
That source change is not retroactively attributed to the initial receipt.
Final PR-head checks and any later native run are recorded on the PR itself.
No exactly-once server execution, secure-frame transaction capture, other profile,
independently native-observed MGR mutation, or completion of #63 is claimed.
