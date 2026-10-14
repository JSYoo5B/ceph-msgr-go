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
