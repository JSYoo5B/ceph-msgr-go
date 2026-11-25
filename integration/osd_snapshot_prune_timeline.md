# Snapshot prune observation

`TestCephOSDSnapshotLifecycleReadIntegration` writes its diagnostic timeline in
`osd-snapshot-lifecycle-read-results.json`, on success and failure. The prune
budget is 180 seconds including MGR resume acknowledgements. Each resume has a
15-second wait within that budget and the fixture lease deadline. The receipt
retains the lease's actual resume time and the caller's acknowledgement interval.

A separate sampler reads `mgr dump` through the MON command session, starting
before resume and continuing through prune or failure. Each read is limited to
two seconds, with one second between reads; a PG oracle that stalls cannot stop
this sampler. An additional read follows each acknowledged resume. PG/OSDMap
observations have ten-second waits and 250 milliseconds between reads, all
bounded by the same prune deadline. A local timeout does not cancel fixture work;
lease cleanup and pool cleanup still run with independent bounded contexts.

Each completed observation retains start, end, effective deadline, error and
timeout status. The timeline retains every observation, including first, last
and state changes, plus the first observed error without replacing it with a
later cleanup or retry error. The manager epoch, active name, GID and explicit
availability are separate from the PG epoch and OSDMap epoch. A failed read has
no manager state: it must not be interpreted as `available=false`. Records are
appended in completion order; use their intervals to identify overlap.

The prune verdict is separate from the owned-pool cleanup receipt and a fresh
MON observation after lease restoration. Fixture teardown and a following test's
readiness remain separate evidence. `available=true` is an advertised MgrMap
state, not proof that MgrStat committed a digest or that the digest contained
`purged_snaps`. Neither a pass nor temporal overlap establishes the cause of an
earlier timeout. If pruning stalls with advertised availability, digest commit
and the MON's newly purged snapshot state need a subsequent, narrower observation.

The pinned source is Ceph v20.2.4: `src/mon/MgrMonitor.cc` (`mgr dump` reads the
MgrMap), `src/mon/MgrStatMonitor.cc` (digest handling) and
`src/mon/OSDMonitor.cc` (`try_prune_purged_snaps`). This test does not change
those daemons or their pruning policy. Preserve every attempt's diagnostics and
result gate, including the first failure, when comparing a retry or control.
