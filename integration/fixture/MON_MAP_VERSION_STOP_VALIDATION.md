# MapVersion after a connected MON stops

This is a bounded checkpoint for issue #64. It adds a deterministic,
request-specific lost-reply barrier to
`TestCephMonMapVersionConnectedStopIntegration`. It changes no product API,
feature mask, authentication policy or automatic retry behavior.

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`;
`messages/MMonGetVersion.h`, `messages/MMonGetVersionReply.h` and
`mon/Monitor.cc` `handle_get_version` (LGPL-2.1, no copied implementation).
Image:
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The fixture uses the pinned development-only
`github.com/JSYoo5B/ceph-testcontainers-go v0.3.0`.

## Fault boundary and ownership

1. Start an isolated three-MON/two-MGR/single-BlueStore fixture and read
   native quorum identity. Seed the subject client only with a peon. It may
   learn replacement MONs from its authenticated MonMap.
2. Arm a test-only CRC connection wrapper. It forwards the original banner
   and frame bytes, including the authentication transcript. It records a
   complete transmitted message 19 with connection number, TID, sequence,
   map name and payload handle. Neither a write beginning nor elapsed time
   establishes the barrier.
3. Observe the native message 20 with the same connection, TID and handle,
   and discard only that reply. Keep reading the real peer. Unrelated
   frames remain transferable; the wrapper does not wait for local Close.
   The original Go operation must still be pending at this barrier.
4. Stop the actual connected MON with the existing provider Stop control.
   Require the original call to return a zero result and
   `OutcomeUnknownError` wrapping the actual peer `io.EOF`. The operation
   context must remain live. Its keepalive interval/timeout are 30/60 seconds;
   a keepalive failure or local cancellation cannot pass as the stop result.
5. Make one new explicit `MapVersion(osdmap)` call. Require another admitted
   connection/MON, the same client GID/FSID, the next client-wide handle and
   the matching native reply. TIDs are session-local and may be reused.
   Exactly two complete version requests and two corresponding replies are
   observed; the first query is not retransmitted. Compare the new newest
   epoch with the independent native pool oracle's `versions` result.
6. Restart the stopped MON and require the existing native election barrier:
   all original MON members in quorum, equal election epochs in two
   observations at least two seconds apart, and the original MonMap identity.
   Fresh cleanup contexts restore this state after an assertion failure.
   Close the client and join the original operation worker. The fixture
   removes only its owned containers and network.

The wrapper is bounded to 1 MiB logical frames and 16 request/reply receipts.
This test configures the same `MaxFrameSize`; it is not a large-map test.
It validates CRC frames and forwards their original bytes, rather than
re-encoding authentication traffic. Secure and compressed frames are outside
its contract. No additional forwarding goroutine or registration is created.

The native server has already answered before the fault. This proves loss of
a server-answered reply with a locally pending query. It does not prove a
request that remains unprocessed in the daemon. Provider Stop is an orderly
container stop, not an accepted forced SIGKILL. The old leader-pause/one-second
sleep case remains separate and is not upgraded by this record.

## First failure and correction

The first CRC/AES IPv4 host run at
`74bcf7f923e6ebb434cf1ce4357461834458dafc` reached the request-specific
barrier, stopped MON `b` and returned unknown/EOF. The original exchange was
connection 1, TID 1, sequence 2, handle 1, newest 14 and oldest 5. A fresh
exchange on connection 2 had TID 1, sequence 2, handle 2 and the same version
range; client GID 14554 and FSID were preserved and the MON changed.

The test then failed because it incorrectly required a different numeric
TID after reconnect. A MON session owns its TID allocator; connection identity
and handle distinguish this valid reuse. The native epoch comparison after
that assertion was not reached, so this failed run does not establish it.
All eight owned containers and the network were independently absent after
cleanup.

`a92b87ebb50b3e55348821393b40095a09cd7a57` corrects the assertion to compare
the connection/TID/handle tuple, adds a cluster-free regression with the same
TID on two distinct connections, and replaces the full state dump with bounded
identity diagnostics. The original captured identities explain the failure;
the later passing run is verification of the corrected assertion.

## Native receipts

On 2026-10-23 UTC, the selected test actually passed without skips at
`a92b87ebb50b3e55348821393b40095a09cd7a57` on Linux/amd64 in each profile
below. Both use three MONs, two MGRs, one BlueStore OSD and the legacy-v6
fixture; CRC applies to the whole selected connection profile. A fresh
fixture is created for each row.

| Observation | CRC/AES IPv4 host | CRC/aes256k IPv6 container |
| --- | --- | --- |
| Selected test elapsed / whole fixture elapsed | 3.61 s / 59.60 s | 3.81 s / 56.49 s |
| Connected MON stopped | `b` | `b` |
| Original connection / TID / sequence / handle | 1 / 1 / 2 / 1 | 1 / 1 / 2 / 1 |
| Original server newest / oldest | 14 / 5 | 14 / 5 |
| Original terminal result | unknown / actual EOF | unknown / actual EOF |
| Fresh connection / TID / sequence / handle | 2 / 1 / 2 / 2 | 2 / 1 / 2 / 2 |
| Client GID before / after | 14557 / 14557 | 14557 / 14557 |
| Fresh Go newest / native newest / oldest | 14 / 14 / 5 | 14 / 14 / 5 |
| Restored MonMap / election epoch | 3 / 16 | 3 / 16 |
| Native full-quorum observation separation | 2.215353901 s | 2.264208394 s |
| Restored quorum | `a`, `b`, `c` | `a`, `b`, `c` |
| Owned containers / network independently absent | 8 / 1 | 8 / 1 |

The request-specific CRC observations are direct live frame/message-header
receipts, not fresh native-encoder vectors. The independent native oracle
establishes the current newest OSDMap epoch; it does not report the oldest
retained epoch. The test checks the latter's range and reply identity.
Ephemeral fixture endpoints remain in the private execution receipt rather
than being treated as reusable deployment addresses.

## Reproduction

Run on an authorized Linux/amd64 Docker runner while holding its shared
execution lock. From the repository root, use a fresh owned output directory:

```sh
CGO_ENABLED=0 CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 CEPH_MSGR_TEST_OSD_REPLICAS=1 \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_RUN='^TestCephMonMapVersionConnectedStopIntegration$' \
CEPH_MSGR_TEST_TIMEOUT=8m \
go -C integration/fixture run . run -project ../.. \
  -output ../../.artifacts/mon-map-version-stop-aes4/ipc \
  -diagnostics ../../.artifacts/mon-map-version-stop-aes4/diagnostics
```

For the second profile use `aes256k` for both key/cipher, IPv6, container
runtime, and another owned output directory. The frame mode stays CRC.
The selected test must actually report PASS; a secure-profile skip is not
acceptance. Keep fixture IPC/key material and unsanitized diagnostics private.

## Separate cluster-free evidence and remaining work

`TestMonMapVersionRelayFragmentationCorrelationAndEOF` exercises fragmented
banner/frame writes and reads, byte-preserving request transport, unrelated
frames, wrong TID/handle replies, the selected reply barrier, and real peer
EOF without local Close.
`TestMonMapVersionRelayFreshConnectionPassesNewIdentity` preserves a fresh
reply even when a new connection reuses the old TID. Both are in Basic CI's
cluster-free inventory. Linux race instrumentation repeated them 50 times.
These synthetic peers are not native daemon evidence.

Whole-product CGO-disabled tests/vet, 635 Python tool tests and the 76-scenario
catalog passed on Linux at the first implementation commit above. The corrected
relay's two cluster-free tests passed another 50 race repetitions and integration
vet at `a92b87ebb50b3e55348821393b40095a09cd7a57`. Product tests with no fixture
do not execute native acceptance. CI and native executions on the exact final
PR head are reported separately on the PR; this record identifies its own
tested commit rather than claiming that a documentation update was executed.

Issue #64 remains open for forced connected-MON death and other fault phases,
secure-profile request correlation, ServiceMap owned registration/removal and
same-stream MON recovery, bounded removal visibility, admitted large maps and
the separate session/watch collection-budget boundaries. No native encoder
parity, daemon execution count, lossless subscription history or large-map
claim follows from this checkpoint.
