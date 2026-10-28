# ServiceMap lifecycle across a connected MON stop

`TestCephMonServiceMapLifecycleIntegration` uses the independent
[native holder](SERVICE_MAP_HOLDER_VALIDATION.md) with the Go
[`WatchServiceMap` stream](../../cephmsgr/SERVICE_MAP_CONTRACT.md). It changes no
product API, wire feature or automatic replay policy. The holder-only selector
remains a separate prerequisite check and makes no Go-watch claim.

Reference: Tentacle v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`, pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Native holders call `rados_service_register`; independent observations open a
fresh native handle and read `service dump`. These tools are development-only
and are not product dependencies.

## Executable sequence

1. Open one Go watch before starting native holder A. Require a readable native
   registration with the holder's own GID and the two synthetic metadata fields,
   and a Go full map with the same daemon/GID/metadata and owned raw bytes.
2. Resolve the watch's actual registered MON source to its owned fixture role.
   Request orderly Stop of that MON, wait for its connection to end and require
   the same watch to register on another admitted MON. No second watch is opened.
3. Independently verify A is still alive and registered after recovery. Start
   holder B only then. Require that same Go watch to deliver both identities in
   an epoch newer than the pre-stop map. B cannot appear in a buffered pre-fault
   map; the test drains older snapshots rather than demanding every epoch.
4. Explicitly release each holder once and require each released shutdown to
   return. That is not removal evidence. Poll fresh native observations until
   each owned daemon is absent in readable maps on observations at least two
   seconds apart; require a newer Go full map without either daemon, within a
   120-second absence budget.
5. Require native cleanup to show each holder dead and its owned baseline
   restored without a kill. Restart the stopped MON and use the existing native
   settled-election barrier: original monmap and full owned quorum, unchanged
   election epoch across observations at least two seconds apart.

The test has a six-minute context. Holder lifetimes remain capped at 120 seconds;
A must remain alive through the actual fault/recovery before B is created. Each
native phase retains its existing external command bound. Failure cleanup uses
fresh contexts and restores a possibly stopped MON even when the Stop request
had an uncertain result. Registration, release and MON Start each consume their
mutation attempt before sending; a failed acknowledgement or a later quorum-read
failure cannot replay them. Read-only native restoration observations may repeat.

The native and Go snapshots need not have identical map epochs. The daemon start
epoch is compared only at the same map epoch because native re-registration can
change it. GID and synthetic identity metadata must agree at either epoch. Logs
record only owned roles/identities and actual epochs, not complete service dumps.

## Reproduction and evidence boundaries

Run on an authorized Linux runner with the ordinary three-MON/two-MGR fixture:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_RUN='^TestCephMonServiceMapLifecycleIntegration$' \
sh integration/run.sh
```

The selector is also discovered by the existing broad MON/MGR rows. It needs no
new catalog row or opt-in. Require the actual client-suite parent to PASS without
a skip, independently of the runner exit code. The API suite has no matching
parent and its empty selection is not a second acceptance result. Exact tested
SHA, attempt, image/profile, native/Go epochs, target result and owned-resource
cleanup are recorded on the pull request. The recipe itself is not evidence that
any particular revision or configuration has passed.

Daemon-free tests in `cephmsgr/service_map_lifecycle_receipt_test.go` reject stale
absence, partial removal, missing raw bytes, unreadable native views, mismatched
GIDs and same-epoch start-epoch disagreement. They preserve the different-epoch
comparison rule. These tests are not live lifecycle evidence.

## Remaining limits

The fault is orderly Stop of the connected MON, not SIGKILL or a request-specific
unprocessed-request barrier. This does not require a lossless event history,
acknowledge subscription authorization, change service registration policy, or
prove a high-level service-discovery consumer. Large-but-admitted maps,
over-budget isolation, killed-holder grace timing, broader profiles and all
remaining MapVersion fault variants remain separate acceptance work.
