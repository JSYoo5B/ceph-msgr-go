# MON MPing validation

Run date: 2026-10-18. Pinned Ceph Tentacle 20.2.4 source:
`7f793731f1b39eb4f465e960113d2363c311b964`; image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Product contract: `cephmsgr/MON_PING_CONTRACT.md`. Machine-readable live
receipt: `MON_PING_LINUX_20261018.json`.

## Native operation and scope

The independent oracle is `ceph ping mon.a`, `.b`, `.c`. Pinned `ceph.in`
1072–1081 selects its dedicated ping branch before the ordinary command
connection. It calls Python `Rados.ping_monitor`, `rados_ping_monitor`,
`RadosClient::ping_monitor`, then `MonClient::ping_monitor`, which sends MPing
on a temporary connection. `ceph tell` and ordinary `mon_command` are not
used as substitutes for this oracle.

Native process success alone is insufficient: MonClientPinger can wake on
reset without a payload. Each oracle must return nonempty JSON with the exact
MON name/rank and owned FSID, a nonzero MonMap epoch and the fixture's three
members. The Go implementation preserves the raw string rather than parsing
JSON; these schema and health fields are interpreted only in the tests.

The fixture opt-in `CEPH_MSGR_TEST_MON_PING=1` selects only
`^TestCephMonPingIntegration$` with three MONs and two MGRs, no OSD/MDS/RBD,
freeze, authentication fault, mapped-address or stress profile. Its three
native pings do not run on unrelated fixtures. CI adds two dedicated rows and
retains all previous rows; static admission checks verify both selectors,
profile combinations and the opt-in's environment forwarding.

Run the acceptance on a Linux Docker host from the repository root:

```sh
CEPH_MSGR_TEST_MON_PING=1 CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_IP_FAMILY=4 sh integration/run.sh
CEPH_MSGR_TEST_MON_PING=1 CEPH_MSGR_TEST_RUNTIME=container CEPH_MSGR_TEST_CONNECTION_MODE=secure CEPH_MSGR_TEST_KEY_TYPE=aes256k CEPH_MSGR_TEST_IP_FAMILY=6 sh integration/run.sh
```

The native-only builder is `tools/mon_ping_native_encoder.py --build-native
--work PATH --wrapper tools/mon_ping_native_encoder.cc`, invoked inside an
owned disposable container of the pinned image. It refuses host package
installation and an SDK cache from a different architecture. Native package
installation, source extraction and compilation are development-only steps.

## Live results

Both runs use `CGO_ENABLED=0` clients on Linux amd64. Each passed all five API
leaves, with no API leaf skipped. The separate internal-client binary has no
test matching this selector and is not additional live coverage.

| Profile | UTC interval | API duration | Outcome |
| --- | --- | --- | --- |
| CRC/AES, IPv4, host | 05:37:22–05:38:07 | 0.32 s | 5/5 passed; controller exit 0 |
| secure/AES256K, IPv6, container | 05:37:19–05:38:02 | 0.52 s | 5/5 passed; controller exit 0 |

- All three named MONs return their own identity and the owned cluster's map,
  agreeing with their independent native pings. The map epoch may advance;
  uptime, health details, session counts and other dynamic fields are not
  required to match bytes between separately timed calls.
- Two simultaneous pings coexist with ordinary MON and MGR commands. The
  primary GlobalID, held MON/MGR endpoints and MGR identity remain unchanged;
  a pre-existing log subscription receives a later test-owned sentinel.
- A deliberately failed dial to mon.b is known unsent and attempts only b.
  The test then explicitly pings c successfully. Unknown names and an already
  cancelled context are refused. This is an injected connection failure,
  not a stopped or frozen native MON.
- An independently authenticated `client.configwatch` identity, whose only
  MON command grant is `fsid`, successfully pings b. Admission is still
  required; this does not add an unauthenticated Go connection mode.
- Each profile records seven successful Go responses and three native
  responses. Go raw response bytes are base64-preserved with independently
  checked SHA-256 values; parsed JSON is retained separately for inspection.
- Each owned fixture's seven containers and its network are absent after
  teardown. There is no object, filesystem or daemon configuration mutation
  in this acceptance. The existing diagnostic log sentinel is test-owned.

An initial secure IPv6 run at 05:28:46–05:29:28 passed all five API leaves but
its controller exited 1: the container wrote its public result receipt with
mode 0600, preventing the host collector from reading it. The receipt now uses
0644. The initial failure log and cleanup audit are retained separately, and
subsequent complete runs passed in both profiles. That initial execution is
not counted as a successful controller run.

## Additional checks and limits

The independent fresh amd64 native encoder subsequently produced four
source-shaped vectors: empty request (v1/c1, front 0), JSON reply (v1/c0,
front 40), empty-string reply (v1/c0, front 4), binary reply (v1/c0, front 11).
`internal/msgr/testdata/mon-ping-native-provenance.json` preserves native
header/front/middle/data, decoded raw strings, and source, wrapper, driver,
configuration, ELF architecture and matching ceph-common hashes. Canonical
Message::encode runs once on each fresh producer, followed by a separate
fresh decoder. The native 41-byte MessageFrame header uses explicit offline
sequence/ACK/priority inputs; actual ProtocolV2 coverage comes from the live
runs above. Product dependencies remain Go-only.

Linux `CGO_ENABLED=0 go test ./...` then passed without skips, including all
four native-vector byte comparisons; vet and the stdlib-only dependency check
also passed. Earlier related `cephmsgr`, `internal/msgr`, `internal/session`
race tests, all fixture tests/vet, 423 Python checks and 20 seconds of
PingReply fuzzing (1,883,853 executions) passed. The live receipt preserves
the earlier vector-test deferral and appends its completed native proof and
complete root check separately. The verified live snapshot changes only
receipt byte retention, the fuzz CI line and MDS contract prose from those
earlier checks; product Go bytes are unchanged. Final native driver/wrapper
source hashes are separately recorded in the native proof.

The final comparison detected that `internal/msgr/ping_test.go` had gained
stricter pinned archive/image/architecture/artifact provenance checks after
the original Linux snapshot. The push proceeded despite that comparison
failure, an execution-order error. Linux was then updated with that exact
committed test, and complete CGO=0 root tests/vet passed again for `source-1`.
All 687 committed Go files match that checked snapshot; product Go source
remained unchanged. The receipt appends this exact-head verification without
replacing the original run history.

The native builder's initial attempts exposed a read-only wrapper mtime
issue, a misplaced/cross-architecture SDK cache, missing cached wrapper
sources and an unqualified C++ string decoder. They are retained as builder
failures/cancellations, not product failures or successful encodings. The
successful run uses a fresh x86-64 SDK; the driver now rejects incompatible
cache architectures before setup and checks both encoder and loaded library
ELF machines. It changes only its owned disposable container and cache.

Synthetic peers separately cover binary/empty strings, bounds, malformed
headers/segments/lengths, primary command TID continuity, opposite-order
concurrent replies, shared slots, cancellation and Close during setup or reply
wait, admission FSID refusal and source loss without replay. These do not
establish native MON freeze/restart, interrupted authentication, membership
change during a ping, native malformed replies, older releases, Windows live
MPing or arbitrary address/security combinations. JSON status is caller-owned
interpretation; an accepted raw reply is not a product health verdict.
