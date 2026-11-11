# MapVersion when the connected MON is killed

This record covers requirement 3 of issue #64: an in-flight `MapVersion`
request whose actual connected MON is killed with SIGKILL. It adds two native
cells, a closed fixture admission for the kill, and two observations to the
test-only CRC relay. It changes no product API, feature mask, authentication
policy or retry behavior.

Reference: Tentacle v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`; `mon/Monitor.cc`
`handle_get_version`, `handle_tell_command`, `dispatch_op` and `tick`,
`messages/MMonGetVersion.h`, `messages/MMonGetVersionReply.h` (LGPL-2.1, no
copied implementation). Image:
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The fixture uses the pinned development-only
`github.com/JSYoo5B/ceph-testcontainers-go v0.5.0`.

The orderly-Stop predecessor is
[MON_MAP_VERSION_STOP_VALIDATION.md](MON_MAP_VERSION_STOP_VALIDATION.md). Its
relay, correlation rules and TID note apply here unchanged.

## Fixture admission

The daemon `kill` control sends SIGKILL through the Docker API and accepts
only a stopped container with exit code 137 that was not OOM-killed. It was
restricted to OSDs. It now also accepts a MON, only when the fixture runs the
exact selector

```
^TestCephMonMapVersion(ConnectedKill|UnprocessedKill)Integration$
```

on the single-BlueStore legacy-v6 OSD fixture. Any other selection, including
the broad OSD selection that matches both names, keeps the OSD-only rule; the
two tests then skip. The MON receipt is written to `mon-crash-state.json`.

That selection also runs with 120 s AUTH and service tickets instead of the
ordinary 12 s. The second cell keeps every MON out of quorum for tens of
seconds. With 12 s tickets the Go client's routine renewal would replace the
subject session inside that window, and native daemons would return with
reclaim proofs past their validity. This is a fixture setting of the two
cells. It is not evidence for the 12 s profile.

## Cell 1: server-answered request, SIGKILL

`TestCephMonMapVersionConnectedKillIntegration` runs the connected-Stop
sequence with one difference. The relay observes the complete request and the
matching native reply, discards only that reply, and the MON is then killed
instead of stopped. The fixture's exit 137 receipt for that MON is required.

Required outcome:

- the original call returns a zero result and `OutcomeUnknownError`; its
  cause is the peer loss that the relay saw on the same connection. A killed
  peer's kernel closes the socket with FIN, or with RST when unread bytes
  remain, so EOF and `ECONNRESET` are both admitted; the test logs which one
  occurred. The operation context is still live, and the keepalive timeout
  is 60 s, so neither can supply the result;
- exactly one request existed before the fresh call (no retransmission);
- one new explicit `MapVersion(osdmap)` uses another connection and MON, the
  next client-wide handle, the same client GID and FSID, and its newest
  epoch equals the independent native `versions` oracle;
- the killed MON is started again and the native settled-election barrier
  reports the original MonMap and full quorum.

## Cell 2: request read by the MON but not answered

`TestCephMonMapVersionUnprocessedKillIntegration` withholds nothing. It makes
the connected MON unable to answer, proves on the wire that the MON read the
request and did not answer it, and only then kills that MON.

1. The subject client is seeded with one peon. Its own `mon_status` (a
   daemon-local tell on the subject session) must report `peon`.
2. One of the other two MONs is stopped with the provider's orderly Stop,
   and the test waits until the subject reports `leader` or `peon` with a
   nonempty quorum again. The last peer is then stopped while the test polls
   the subject MON's `mon_status` until its state is neither `leader` nor
   `peon` and its quorum is empty. The stop is acknowledged before the cell
   continues.
3. Pinned `handle_get_version` answers only when `is_leader() || is_peon()`.
   Otherwise it queues the operation on `waitfor_quorum` and sends nothing.
   `dispatch_op` passes `MSG_COMMAND` to `handle_tell_command` without a
   quorum check, so a tell is still answered.
4. The relay is armed and the call starts. The relay reports the request
   once its whole frame was written: connection, TID, Messenger sequence,
   handle and map name.
5. A second `mon_status` tell is sent after the request. The barrier holds
   when the relay shows a tell on the same connection, with a later
   Messenger sequence, answered by a `MCommandReply` with its TID; that
   answer still reports the MON out of quorum; the relay holds no version
   reply; and the call is still pending. The MON reads one connection in
   order, so an answer to the later message shows that it read past the
   request.
6. The MON is killed. The exit 137 receipt is required.

Two rules keep the kill from being confused with the MON's own behavior. A
MON that has been out of quorum for two lease intervals (10 s) drops every
client session on each tick (pinned `tick`, "out of quorum too long"). The
test therefore keeps one origin: the start of the latest `mon_status`
request that the MON answered as `leader` or `peon` with a nonempty quorum,
beginning with the check in step 1. The MON was in quorum after that request
started, so it cannot drop sessions earlier than 10 s after it. Stopping the
peers and a status that already shows the MON out of quorum do not move the
origin. Three bounds are required: the kill is requested at most 7 s after
the origin, the relay saw the request's connection end after the kill was
requested, and that observed end is itself at most 7 s after the origin. The
last bound covers a controller that accepts the request in time and then
stalls. A run that misses a bound fails. It does not pass with a different
cause.

Required outcome:

- unknown outcome, zero result and peer-loss cause as in cell 1, with one
  request, no version reply and no retransmission;
- the two stopped MONs are started again. One new explicit call uses another
  connection and MON, the next handle and the same GID and FSID. The relay
  shows exactly two requests and one reply, the fresh one;
- the killed MON is started, the native settled-election barrier reports the
  original MonMap and full quorum, and the Go newest epoch equals the native
  `versions` oracle with one native read bracketed by two equal Go reads.
  Daemons re-register after the outage and can still move the OSDMap, so a
  single unbracketed comparison is not used here.

MONs restart in descending name order. The provider's readiness check of the
bootstrap MON `a` needs a quorum; started first with no other MON running, it
times out. Each stop, kill and start is attempted once. A failed restoration
marks the fixture unusable.

What the cell shows is a request that the MON's messenger read and its
dispatcher did not answer. The relay cannot see inside the daemon: that the
operation sat on `waitfor_quorum` follows from the pinned source and the
reported state, not from a daemon-side observation.

## First failures and corrections

All three were test or fixture-usage defects found while developing the
cells. None showed a product defect, and no product code changed.

1. The first complete run of cell 2 passed its barrier and kill, then failed
   in restoration: `start` of MON `a` returned "wait until ready: context
   deadline exceeded" after 60 s. MON `a` was started first while no other
   MON ran, and the provider's readiness check for it executes a cluster
   command. Corrected by the descending restart order above.
2. An intermediate version waited for the MON's first session drop before
   starting the cell. It polled every 200 ms and hit the relay's bound of 64
   tell receipts after 12.8 s, before any drop. The sequence was replaced by
   the fixed interval described above, and the bound was raised to 1024.
3. Both of those runs passed cell 1.

## Native receipts

On 2026-11-10 UTC both cells passed without skips at
`d03baf3c89473e6c42ac27ed8d0ebb4ca379ab92`, run from an archive of exactly
that commit on a Linux/amd64 host with Docker 29.9.1 and Go 1.26.9, through
`integration/run.sh` with the catalog row's parameters and the result gate
(`tools/native_test_results.py`, verdict passed, two expected tests, no
problems). Each row used a fresh fixture: three MONs, two MGRs, one
BlueStore OSD, legacy-v6, CRC, 120 s tickets.

| Observation | CRC/AES IPv4 host | CRC/aes256k IPv6 container |
| --- | --- | --- |
| Row elapsed | 90 s | 92 s |
| **Cell 1** elapsed | 17.61 s | 17.71 s |
| Killed MON / exit receipt | `b` / 137, not OOM | `b` / 137, not OOM |
| Original connection / TID / sequence / handle | 1 / 1 / 2 / 1 | 1 / 1 / 2 / 1 |
| Withheld server reply newest / oldest | 14 / 5 | 14 / 5 |
| Original terminal result / relay transport end | unknown, EOF / EOF | unknown, EOF / EOF |
| Fresh connection / TID / sequence / handle | 2 / 1 / 2 / 2 | 2 / 1 / 2 / 2 |
| Client GID before and after | 14548 | 14548 |
| Fresh Go newest / native newest / oldest | 14 / 14 / 5 | 14 / 14 / 5 |
| Restored MonMap / election epoch / quorum | 3 / 22 / `a`, `b`, `c` | 3 / 22 / `a`, `b`, `c` |
| **Cell 2** elapsed | 23.18 s | 24.57 s |
| Connected MON state at the barrier | `probing`, election epoch 22, quorum empty | `probing`, election epoch 22, quorum empty |
| Request connection / TID / sequence / handle | 5 / 38 / 39 / 1 | 5 / 40 / 41 / 1 |
| Later tell answered on that connection / version replies | yes / 0 | yes / 0 |
| Killed MON / exit receipt | `b` / 137, not OOM | `b` / 137, not OOM |
| Original terminal result / relay transport end | unknown, EOF / EOF | unknown, EOF / EOF |
| Connection end after the kill request | 57 ms | 122 ms |
| Fresh connection / TID / sequence / handle | 6 / 1 / 2 / 2 | 6 / 1 / 2 / 2 |
| Client GID before and after | 24100 | 24100 |
| Restored MonMap / election epoch / quorum | 3 / 28 / `a`, `b`, `c` | 3 / 28 / `a`, `b`, `c` |
| Go newest / native newest / oldest (bracketed, first attempt) | 14 / 14 / 5 | 14 / 14 / 5 |
| Native full-quorum observation separation (cell 1 / cell 2) | 2.23 s / 2.21 s | 2.28 s / 2.22 s |
| Owned containers terminated by the fixture | 8 | 8 |

After the rows, an independent Docker listing on the host showed no
remaining test container and no custom network.

In cell 2 the relay had wrapped five connections, all dialed to the subject
MON, before the request. The fifth carried about forty status tells, so the
four earlier ones ended before the status polling. They belong to the
client's setup against a MON that the preceding cell had just killed and
restarted; their cause was not investigated. The barrier does not depend on
them: it binds the request, the later tell and the connection end to one
connection.

The same two cells also passed at `10ad0e57ed0becf00917446256d899f8cf38b4f1`
on the IPv6 container row (cell 1 3.81 s, cell 2 23.62 s, connection end
76 ms after the kill request). The commits between differ in the ServiceMap
holder and its cell and in the relay's diagnostic log only.

The request-specific observations are live CRC frame and message-header
receipts. The native oracle supplies the newest OSDMap epoch only.

## Reproduction

Catalog rows (both CRC; secure frames are outside the relay's contract):

- `osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-monmapversionkill`
- `osd.aes256k.ip6.crc.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-monmapversionkill`

The result gate requires both `api/` tests to PASS. On an authorized
Linux/amd64 Docker runner, holding its execution lock:

```sh
CGO_ENABLED=0 CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 CEPH_MSGR_TEST_OSD_REPLICAS=1 \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_RUN='^TestCephMonMapVersion(ConnectedKill|UnprocessedKill)Integration$' \
sh integration/run.sh
```

## Daemon-free evidence

- `TestMonMapVersionRelayOrdersAnsweredTellAfterUnansweredRequest`
  (cluster-free inventory): an armed request that the peer never answers is
  reported as sent; of five tells, only the one on the same connection, with
  a later sequence and an answer, qualifies; no reply receipt and no reply
  barrier appear.
- `TestMonSessionTrimWindowIsAnchoredToAnInQuorumStatus` (cluster-free
  inventory): a window with no in-quorum observation admits nothing; a first
  poll that is already out of quorum leaves the origin at the earlier
  in-quorum status, so slow stops are counted; a kill requested in time
  followed by a connection lost 8 s later is refused.
- `TestMonForcedKillIsAdmittedOnlyForItsExclusiveSelection` and
  `TestMonForcedKillMarkerAndTicketValidityFollowSelection` (fixture module):
  the MON kill, its marker and the 120 s tickets exist only for the exact
  selector on the admitted fixture; an inherited marker variable does not
  enable them.

These use synthetic peers or no peer. They are not native daemon evidence.

## Remaining limits

- Secure-profile request-specific observation is not covered. The relay
  decodes CRC frames only.
- Cell 2's "not answered" state is the out-of-quorum wait. A request delayed
  inside a MON that is in quorum (for example behind a slow Paxos read) is a
  different state and is not covered.
- Both cells ran with 120 s tickets and one BlueStore OSD. Other ticket
  lifetimes, topologies and platforms are not covered.
- In every run the peer loss was EOF. `ECONNRESET` is admitted by the tests
  but was not observed.
