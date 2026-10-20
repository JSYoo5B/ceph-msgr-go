# OSD fixture readiness diagnostics

Context: issue #3. After `TestCephOSDFullFailoverIntegration` restored its
full ratios and restarted the killed OSD, the controller's 30 s clean wait
ended with `context deadline exceeded` roughly 0.6 s after the MGR first
reported all PGs `active+clean`. The controller stored that error and every
later fixture request repeated it.

## What changed

- `verify-osd-ready` uses `waitOSDClean` (`osd_ready.go`) instead of
  `WaitForClean`. The predicate is unchanged: manager available, every owned OSD
  up and in, at least one PG, and every PG exactly `active+clean`.
- The 30 s budget is unchanged. Each `ceph status` invocation has its own 10 s
  deadline, so a stuck CLI/Docker exec is reported as an invocation failure and
  not as unclean daemon state.
- A failure reports the budget, poll count, slowest poll, invocation failure
  count, the last unclean observation (OSDMap epoch, OSD total/up/in, manager
  availability, PG states, health check names) and the last invocation error.
  Cancellation is distinguished from a state timeout.
- `RequestControl` refuses to publish a request when `provider.error` or
  `native-oracles.error` already exists, and names the error as a cascading
  failure of an earlier controller failure. A request already waiting still
  receives the worker error unchanged.

## Not established

The cause of the delayed `recovery_toofull` clearing and the share of the
budget used by status invocations are not established; the timeout was not
changed. The next occurrence will carry the per-wait data above. Reproduction
on a Linux runner with the original preceding test sequence, and final-SHA
native evidence for restored ratios, owned-pool cleanup and teardown, remain
open. These changes are covered by unit tests only.
