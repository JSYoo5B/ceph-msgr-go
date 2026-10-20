# OSD primary-failure observation budgets — 2026-10-16

This change concerns the disposable native fixture, not product retry,
routing or failure-detection policy. Ceph remains pinned to Tentacle 20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`. Earlier local acceptance in
[OSD_BACKOFF_SCOPE_VALIDATION.md](OSD_BACKOFF_SCOPE_VALIDATION.md) does not
replace the failing CI evidence recorded here.

## Preserved CI failures

The exact `source-1` workflow completed
with 64 successful and two failed jobs. At the saved `source-2` snapshot,
30 jobs succeeded, one failed, 26 were running and nine queued; that
snapshot establishes no final workflow conclusion.

All three observed failures were
`TestCephOSDObjectBackoffFailoverIntegration`, three BlueStore OSDs,
`replicated-pg-v8`, after the initial object BLOCK/UNBLOCK and unrelated
object STAT/READ checks had passed. The later primary kill preserved the
Go mutation's unknown outcome; replacement-primary resubmission had not
been reached.

| Source / profile | Kill to failed down wait |
| --- | --- |
| `source-3`, secure/AES256K, container IPv6 | 60.646081 s |
| `source-3`, CRC/AES, host IPv4 | 60.609253 s |
| `source-2`, secure/AES256K, container IPv6 | 61.183463 s |

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

## Final local native results

Both final runs used `source-4`, before
this results update and later U64 integration. Image reference:
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
These arm64 native regressions are distinct from the earlier amd64 CI failures.
Selector: `^TestCephOSDObjectBackoffFailoverIntegration$`.

| Configuration | Whole test | Go unknown after kill | Native completion after kill | Result |
| --- | --- | --- | --- | --- |
| CRC/AES client/service, IPv4 host, 3 BlueStore | 41.90 s | 305.5 ms | 1381.6 ms | PASS, runner exit 0 |
| secure/AES256K client/service, IPv6 container, 3 BlueStore | 44.65 s | 504.1 ms | 2062.3 ms | PASS, runner exit 0 |

Both restored flags/configuration and removed all nine owned object names;
the final readiness oracle required all three OSDs up/in and all nine PGs
active+clean. All ten recorded owned containers terminated in each run.
The range admission and unrelated-object reads passed before the failover
phase. The original held Go mutation retained its unknown outcome and
submission tuple; native retry reached the new primary, then an explicit
fresh Go request appended once. Final native bytes/version/mtime agreed
after the former primary returned.

The exported wait projection retains acting/up sets, active PG state and
map epoch, but not the complete OSDMap's per-OSD up bits. The down-bit
claim relies on the executed native predicate and passing test, not an
archived full OSDMap. Native ACK history proves reception at the PG; it
does not by itself prove mutation execution or durability.

Both complete tests finished within 60 s, so these runs do not reproduce
the longer amd64 detection boundary. The fake-clock tests cover that
boundary. An exact final-main amd64 CI pass remains a separate requirement;
no such result is claimed in this record.

Before the final runs, an initial CRC run at `source-5` passed in 43.10 s
with cleanup. Independent review then found the initial 240 s native
process limit omitted pre-observation waits. The final source raises that
limit to 360 s and tests the complete phase sum. The initial receipt is
retained separately and does not replace either final-source result.

Local artifact directories, each with an adjacent `.log`:

Final `osd-object-backoff-results.json` SHA-256:

- CRC: `775c6827d2d49f022f33d6394fb19c6f369fb14a33e9cb640d684748a3e8ec5a`
- Secure: `822b773d13268bfd7e177f1fe376356e6b17d2233ff61403e0d690f87490ad3c`

Tested source SHA-256:

- `tools/osd_object_backoff_oracle.py`: `61c315e201913cc96993fda4743277a692c79d1e9eb62e97d8adb7b5be4f81ca`
- `integration/fixture/control.go`: `d7fb6776abd4a821782b4d36d7ec95c983678687f556ec77becd99cf2f7cd3dd`
- `integration/osd_object_backoff_test.go`: `89a6fca02e138431544f99ef1b43e4ba12d7d2c639391b62732c20ba1ab473c4`

Python's eight oracle regressions and the fixture's focused nested-budget
tests passed before native execution. After U64 integration at `source-6`,
whole-product and whole-fixture CGO-disabled tests and vet also passed.
Those combined source checks are separate from the native runs above.
