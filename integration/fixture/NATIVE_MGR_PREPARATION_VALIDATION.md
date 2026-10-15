# Native MGR preparation validation

The retained `20e4487` CI failure exposed stale registered MGR slots: both
native MGRs received MON AUTH reclaim refusals for unavailable secret ID 13,
while MgrMap epoch 44 still advertised active `b/5280` and standby `a/15056`.
The separate Go map-wait failover subsequently had no available replacement.
The original failure remains evidence; later passes do not establish its fix.

Preparation now reads the latest exact MonClient `-13` line from each owned
daemon's latest process before accepting membership. It preserves the first
observed refusal, map epoch and registered GID in `manager-preparation.jsonl`.
A registered but refused daemon cannot satisfy readiness. Only actual map
absence plus an explicit latest-process refusal permits one public Stop/Start
per daemon during that preparation. Native map observations determine absence;
the helper does not sleep for a presumed beacon grace. Clean process logs and
registration with a GID distinct from every GID observed with a refusal are
required afterwards.
Same-process refusal with a new GID remains blocked, without restarting a
present daemon. The child preparation is bounded at 45 seconds.

Eleven focused fixture tests passed with `CGO_ENABLED=0` and with the race
detector; fixture-module `go vet` also passed. Fake-backend coverage includes
stale two-slot membership, culling, once-only restarts, rejection of stale GIDs
after clean respawn, healthy preparation without controls, conservative
self-registration, invalid identities, cancellation, deadlines and failed
Start with the original receipt preserved.

One local native trial passed on public provider v0.2.0, pinned Ceph 20.2.4
commit `7f793731f1b39eb4f465e960113d2363c311b964`, image digest
`sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Profile: arm64, CRC, AES entity/service ciphers, IPv4, host runtime, three MONs,
two MGRs, no OSDs, ordinary 12-second tickets, native preparation opt-in enabled.
`TestCephMapWaitIntegration` passed in 15.02 seconds.
`TestCephNativeManagerPreparationIntegration` passed in 49.24 seconds: its
existing 45-second standby pause produced an explicit refusal and actual
missing `mgr.a`, preserved one restart-intent receipt at epoch 7, restored both
owned slots at epoch 8 (`b/4239`, standby `a/4551`, replacing `a/14405`), and
performed no further restart during healthy preparation.
This proves the missing-first restoration branch. The trial did not observe a
present-but-refused slot, so that transition remains synthetic coverage.

Evidence is preserved locally:

- `/tmp/ceph-mgr-preparation-first.log` and
  `/tmp/ceph-mgr-preparation-first-out`.
- `/tmp/ceph-mgr-preparation-final-source.json`: exact 441-file snapshot;
  aggregate SHA256 `25639f5eff44ef125b1fd0c4972111c2d09f3a988b6d3c3fef5f3571d38ced9e`.
- `/tmp/ceph-mgr-preparation-first-cleanup.json`: all seven owned containers
  absent and the owned network absent, checked independently after exit 0.

This changes disposable fixture preparation. It does not prove a product
CephX causal fix, resolve issue 71's verifier-generation question, retry a Go
authentication or replay the tested mutation. No additional profile or remote
CI verification was performed for this change.

## Registered GID attribution

CI job 116165091788 (commit `ec31913`; Ceph 20.2.4, AES/AES, secure, IPv4,
container runtime, two MGRs, no OSD) failed with `native MGR preparation never
restored unrefused active and standby: context deadline exceeded`. After
back-to-back MON elections under 12-second tickets, MONs b and c logged
`cephx server mgr.b:  attempt to reclaim global_id 15059 using bad ticket` and
the latest mgr.b process logged MonClient `-13` lines. MgrMap epoch 44 still
listed active `b/5280` and standby `a/15058`. A ceph-mgr process runs several
MonClients: the beacon client, whose global_id is the MgrMap GID, and module
clients. The refused client was not the registered one, but the previous rule
blocked on any latest-process `-13` line and never restarts a present daemon,
so preparation waited until its deadline.

Preparation now ties a daemon-side refusal to the registered GID with MON
evidence. For a daemon whose latest process logged `-13`, the fixture parses
the timestamp of that process's latest ` ceph version ` line and streams each
owned MON container's log through public provider v0.2.0 (`Monitors()` and the
container `Logs` API). Each MON log is capped at 32 MiB; exceeding the cap is an
explicit error. Entries at or after the process start with the prefix
`cephx server mgr.<name>:  attempt to reclaim global_id <N>` supply the refused
global IDs. The prefix covers the three refusals in pinned
`src/auth/cephx/CephxServiceHandler.cc:111-113`, `126-128` and `141-143`
(dout prefix at line 30, commit `7f793731f1b39eb4f465e960113d2363c311b964`).
N must be a canonical non-zero decimal `uint64` followed by one of the three
pinned suffixes. A malformed matching entry is an error, not absence. Ceph
timestamps follow `src/log/LogClock.h:139-164`; all daemons share the Docker
host's kernel clock.

For a present registered daemon with a latest-process `-13` line:

- Refused IDs include the registered GID: the slot is stale. The existing
  handling applies: block, wait for actual map absence, then one public
  Stop/Start. Staleness persists for the preparation even if the daemon
  re-registers with a new GID.
- Refused IDs are non-empty and none equals the registered GID: the slot is
  retained. Preparation does not block on it and appends a receipt with action
  `observe native refusal of a non-registered MonClient; registered slot
  retained`.
- The MONs attribute nothing: preparation stays blocked and fails at its
  45-second bound. Because Ceph logs asynchronously, attribution that arrives
  later may still release the block, unless the slot was already found stale
  or the daemon was already restarted.

A missing daemon with a latest-process `-13` line keeps the existing once-only
restart. Every receipt now carries `refused_global_ids` (null when the MONs
attributed nothing). No Go authentication is retried and no mutation is
replayed.

Replaying the retained CI logs through the new parser gives mgr.b refused
`[15059]` against registered `5280` for `ec31913` (retained slot), and mgr.b
`[5280, 15053]` and mgr.a `[15056]` against registered `b/5280` and `a/15056`
for `20e4487` (both stale).

The fixture module passed `gofmt -l`, `go vet` and `go test` (also with
`CGO_ENABLED=0`). The 21 tests in `manager_preparation_test.go` also passed
under the race detector. New fake-backend cases cover the retained slot, the
unattributed failure, late attribution, and a retained slot that later becomes
stale and is restarted once. Parser tests cover the three suffixes, other
entity names, entries before the process start, malformed global IDs and the
byte cap.

One local native run of `TestCephMapWaitIntegration` passed in 17.80 seconds
with the failing profile (Ceph 20.2.4 image digest above, AES/AES, secure,
IPv4, container runtime, two MGRs, no OSD, arm64 Docker Desktop 29.8.2). No
refusal occurred and no preparation receipt was written, so this run did not
exercise the new branch. The trigger is intermittent, and a pass does not show
that the CI race is fixed. Evidence: `/tmp/ceph-mgr-prep-fix-mapwait-20261015/`.
