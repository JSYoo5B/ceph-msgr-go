# Independently controlled native ServiceMap holder

This is a development fixture prerequisite of #64. It is not a ServiceMap
lifecycle acceptance, adds no product API and starts no Go ServiceMap watch.
The product behavior of `Client.WatchServiceMap` is unchanged
([SERVICE_MAP_CONTRACT.md](../../cephmsgr/SERVICE_MAP_CONTRACT.md)).

## Why a holder

`TestCephMonServiceMapIntegration` keeps a native registration alive only
inside one blocking oracle request (`mode_service` of
[osd_pool_oracle.py](../../tools/osd_pool_oracle.py)). The fixture control loop
runs controls synchronously and publishes an acknowledgement only after the
request returns, so a daemon control queued during that hold does not run while
the entry is registered. A MON fault in a later Go lifecycle cell therefore
needs a registration that outlives its request.

Reference: Tentacle v20.2.4, `7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The holder uses the public `rados_service_register` C API through `ctypes`, and
the fresh view uses the Python `rados` module `mon_command`, both only inside the
disposable native image.

## Executable boundary

Selector: `TestCephServiceMapNativeHolderIntegration` in the `cephmsgr` suite,
on the ordinary three-MON/two-MGR fixture (`config_fixture`). It adds no
catalog row or opt-in; the first target is the existing broad
`mon-mgr.aes.ip4.crc.host` row. Reproduce on an authorized Linux runner:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephServiceMapNativeHolderIntegration$' \
sh integration/run.sh
```

The controller request is `observe-service-holder` with `<id> <token> <phase>`,
handled by `integration/cluster.sh` and
[service_map_holder_oracle.py](../../tools/service_map_holder_oracle.py). The
token is 128 random bits; the service is `ceph-msgr` and the daemon is
`holder-` plus the first 16 hex digits of the token. The registration carries
only `ceph_msgr_role=holder` and `ceph_msgr_daemon=<daemon>`. The phases are
closed, and each runs under an external `timeout` because pinned Python
`mon_command` ignores its timeout argument:

| Phase | Bound | Effect |
| --- | --- | --- |
| `start` | 60 s | Claims `start-started`, reads a fresh `service dump` baseline (owned daemon must be absent in a readable dump), records it exclusively, starts one detached holder process and returns once the holder reports that `rados_service_register` returned, with its pid and `rados_get_instance_id` GID. |
| `observe` | 30 s | Opens a fresh librados handle and reports whether the dump lists the owned daemon, with its GID, start epoch and the two synthetic metadata values, the dump epoch, and the holder process state. Repeatable read. |
| `release` | 45 s | Claims `release-started`, creates the release marker and waits for the holder's exit marker (native handle shut down). |
| `cleanup` | 30 s | Reports the holder state. If the holder process is still alive it is killed once after claiming `cleanup-started`; the process must be this token's holder. A fresh view is compared with the worker's own baseline. |

The holder stays alive after `start` returns, so the control loop is free to
execute other controls (for example a daemon stop) while the entry is
registered. The holder has a capped lifetime of 120 seconds and exits on its own
with the `expired` reason. A fixture teardown destroys it in any case.

## Closed behavior

- A repeated `start`, `release` or kill is refused, never replayed. The started
  markers prove that the first attempt began even if its acknowledgement was
  lost; a timeout keeps the uncertain state, and the journal
  (`service-holder-native.jsonl`) records `holder-spawn-outcome-unknown`,
  `release-claimed-outcome-unknown` and `kill-claimed-outcome-unknown` before
  each boundary is crossed.
- Restoration comes from the worker's own baseline receipt (owned daemon
  absent), not from a caller value. Only the owned registration is touched.
- A returned `rados_shutdown` is not a removal acknowledgement. The exit marker
  records only that the call returned; absence is shown only by a later fresh
  `observe` that finds a readable dump without the daemon. A killed holder
  sends no close, so its entry may remain until the MGR times it out; `cleanup`
  reports that as `restored: false` instead of waiting.
- A dump is readable only when it is a JSON object with an integer epoch in
  range and a `services` object, and the owned service (when present) holds a
  `daemons` object and the owned entry (when present) is an object. Any other
  shape yields no code or epoch. Only a missing owned service or daemon key in a
  well-formed map is absence; the Go acceptance side separately requires a
  success code, epoch and service count for both absence and restoration.
- Only the owned entry is persisted: the dump epoch, the owned daemon's GID,
  start epoch and the two synthetic metadata values, and the count of daemons
  in the `ceph-msgr` service. Other entries and the host inventory that
  librados adds to the registration never reach a receipt. Request IDs, tokens,
  phases and state file names are validated, and there is no arbitrary service,
  daemon, command, signal or path input. The Go helper rejects an invalid token
  or phase before publishing a request.

## What the selector asserts

1. `start` returns a ready holder over an absent, readable baseline, with a
   nonzero GID.
2. Repeated fresh native views show the owned daemon with the holder's own GID
   and synthetic metadata while the holder process is alive, then again after a
   pause, so presence is retained rather than momentary.
3. `release` reports a released shutdown that returned.
4. A fresh native view shows readable absence for at least two seconds.
5. `cleanup` kills nothing and reports the baseline restored.

The test's own cleanup runs the `cleanup` phase even after a failure, so a
holder that is still alive is killed.

## Daemon-free checks

- [tools/test_service_map_holder_oracle.py](../../tools/test_service_map_holder_oracle.py):
  closed request fields and state file names, nothing written for a rejected
  request, projection of only the owned entry, baseline refusal (present,
  unreadable or no epoch) without spawning, no replay of start, release or kill,
  holder failure, readiness timeout and foreign readiness, exit-marker timeout,
  a surviving process, cleanup before start and for a holder that never became
  ready, the holder's native call order, lifetime cap and failure recording.
- [cephmsgr/service_map_holder_receipt_test.go](../../cephmsgr/service_map_holder_receipt_test.go):
  open requests rejected before anything is published, mismatched receipt
  identities, the receipt bound, and acceptance rules for readiness, presence
  (the holder's own GID and identity) and absence (a readable dump only).

## Not established

This does not verify the Go ServiceMap lifecycle: no Go watch is started, no
MON is stopped or recovered, no map epoch is compared with Go, and no large map
or collection cap is exercised. It does not verify SIGKILL of a MON, an
unprocessed MON request or a secure request-specific observation. It does not
show how the pinned MGR times out a killed holder. Execution evidence for a
tested head is recorded on the pull request, not in this file.
