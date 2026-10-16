# OSD primary-failure observation budgets — 2026-10-16

This change concerns the disposable native fixture, not product retry,
routing or failure-detection policy. Ceph remains pinned to Tentacle 20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`. Earlier local acceptance in
[OSD_BACKOFF_SCOPE_VALIDATION.md](OSD_BACKOFF_SCOPE_VALIDATION.md) does not
replace the failing CI evidence recorded here.

## Preserved CI failures

The exact `6a8fe8808cb430f6f01a97f72be28f641995f8ee` workflow completed
with 64 successful and two failed jobs. At the saved `4cbb5bb` snapshot,
30 jobs succeeded, one failed, 26 were running and nine queued; that
snapshot establishes no final workflow conclusion.

All three observed failures were
`TestCephOSDObjectBackoffFailoverIntegration`, three BlueStore OSDs,
`replicated-pg-v8`, after the initial object BLOCK/UNBLOCK and unrelated
object STAT/READ checks had passed. The later primary kill preserved the
Go mutation's unknown outcome; replacement-primary resubmission had not
been reached.

| Source / profile | Failed job | Artifact | Kill to failed down wait |
| --- | --- | --- | --- |
| `6a8fe88`, secure/AES256K, container IPv6 | [116890226716](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38875735025/job/116890226716) | [12036528807](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38875735025/artifacts/12036528807) | 60.646081 s |
| `6a8fe88`, CRC/AES, host IPv4 | [116890226813](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38875735025/job/116890226813) | [12036713648](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38875735025/artifacts/12036713648) | 60.609253 s |
| `4cbb5bb`, secure/AES256K, container IPv6 | [116902161744](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38879391666/job/116902161744) | [12037640303](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38879391666/artifacts/12037640303) | 61.183463 s |

Each original `osd-object-backoff-command.json` retains an untruncated
Python `TimeoutError: OSD <id> up=False and active PG not reached`, exit 1,
`TimedOut=false`, with the last native OSDMap still marking every OSD up.
Docker crash and inspect receipts show the intended primary exited 137,
without OOM, automatic restart or a running process.

In artifact 12037640303, a surviving peer's first retained no-reply
heartbeat diagnostic occurs 61.133 s after the kill, only 49.97 ms before
the failed wait. The bounded daemon log is truncated; no later MON down
decision is available in it. The fixture configured heartbeat grace to
60 s and used a 60 s down/active-PG observation budget. Native heartbeat
detection, failure reporting, MON map commit and PG activation are separate
steps: pinned `OSD.cc:6171–6215,6247,6427–6442,7290–7311` and
`OSDMonitor.cc:3200–3285`.

Cleanup start/restore errors repeated the same stored originating
`provider.error` after the controller loop exited. They are not separate
OSD startup failures. Native receipts remain `restored=false` and
`cleaned=false`; later fixture teardown stopped the owned containers.
No successful restoration or object cleanup is inferred from that teardown.

Retained ZIP SHA-256, in table order:

- `38263170f8d444688577aa1acabaecc1f023542ea718b694cb7c6454e606638a`
- `31ab081ebc3fea8da0e52e2a8e0ce1a09ff11712120ce31f2d2b78bbd901326d`
- `6b7e794022cf8f4de3675067ce7ae9247ae96f89234d088cd52bb50bbc687750`

Local originals remain at `/private/tmp/ceph-ci-artifact-<id>-first-failure.zip`
and the corresponding extracted directory. Detailed independent audit:
`/private/tmp/ceph-osd-object-backoff-ci-audit-20261016.md`.

## Change and verification boundary

The down wait has its own 120 s bound. Up waits remain 60 s. The
object-backoff native wrapper allows 150 s, including the predicate's
bounded CLI calls; all other oracle wrappers retain 80 s. The concurrent
native append allows 360 s, covering the 20 s pre-kill in-flight poll,
60 s Go outcome wait, 150 s native wrapper, 45 s completion join and
30 s daemon-control margin. The overall test retains its 9 min bound;
these per-phase budgets do not guarantee completion if every phase stalls.
Heartbeat grace, reporter requirements and actual down/acting/active/epoch
predicates are unchanged. There is no manual MON down marking or automatic
Go mutation replay.

Fake-clock regressions require successful down/active-PG observation after
90 s, refusal while peering, and bounded failure for a permanently wrong
map state. A cross-language fixture test checks nested command/process
budgets. These checks establish the timeout behavior, not interoperability.

Final native validation and exact final source identity are pending.
