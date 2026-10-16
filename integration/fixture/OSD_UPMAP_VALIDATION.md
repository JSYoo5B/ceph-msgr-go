# Explicit caller-routed upmap verification

Prepared 2026-10-16 against Ceph 20.2.4 source
`7f793731f1b39eb4f465e960113d2363c311b964`.
This is a verification-only extension of the existing replicated-PG OSD
protocol profile. It adds no placement algorithm, automatic routing or retry,
inner OSDMap decoder, new Messenger feature bit, or typed management API.
Actual daemon execution has not yet been completed for this recipe.

## Closed scope and observations

The existing three-BlueStore fixture and its original three-replica pool
remain unchanged. The recipe creates one UUID-owned, HASHPSPOOL replicated
pool with size/min_size 2, eight PGs, autoscaling off, and two fixed head
objects. Native pool compatibility must already permit upmap; the recipe
reads that setting and never raises it or changes global deletion policy.

The seven control actions accept only an empty JSON object:
`prepare`, `items`, `restore-items`, `full`, `restore-full`, `observe`, `finish`.
The items transition replaces a baseline secondary with the spare OSD while
preserving the primary. After restoration, the full transition changes both
membership and primary. Both transitions must restore exact baseline vectors.
The two objects may share a PG; the recipe accounts for that explicitly.

Each native observation retains a coherent map epoch, raw hash, actual PG,
primary address/nonce, up/acting vectors and active+clean PG query, along with
independent librados bytes, stat2 and own-object version. Go supplies those
observed targets explicitly and issues STAT/READ/APPEND without automatic
reselection. The append is submitted once; an unknown result is retained and
fails acceptance. Native readback must contain the sentinel exactly once and
match the returned version. Own-object version must increase, without assuming
an increment of exactly one. The original fixture object and pool policy are
checked throughout.

## Bounds, ownership and cleanup

Native children have eight-second deadlines within a 135-second action;
the fixture control has 150 seconds and the Go caller 160 seconds.
Results and command counts are bounded. A kernel file lock is automatically
released on process death; its inode is not unlinked. A scoped watchdog can
attempt the closed cleanup after an abandoned lease, preserving the original
failure instead of reporting a successful acceptance.

Mutation intents are journaled before execution. Observed pool name/ID or
policy replacement refuses further mutation. Native object observations bind
the ioctx to the frozen numeric pool ID and independently check that ID.
CLI name-based mutations have a preflight check, not an atomic remote lease;
the UUID scope assumes a single owner in the disposable fixture. Confirmed
or uncertain commands are never replayed through another action alias.
After the first action failure only cleanup is permitted.

Cleanup distinguishes override restoration, both object removals, baseline
preservation and retained resources. The owned pool is deliberately retained
for disposable fixture teardown even when deletion is enabled. Therefore
`finished=true` means the control finished, not that every resource was deleted.
Container/network teardown is verified separately from in-cluster cleanup.

## Verification status

The new mock-native regressions cover same/different PGs, deadlines,
uncertain install/removal outcomes, stale process locks, pool-ID replacement,
and confirmed removal followed by a failed observation and an alias attempt.
These establish local control behavior only. Actual CRC/AES IPv4 and
secure/AES256K IPv6 three-BlueStore runs, raw receipt audits, and final cleanup
inventory are still pending. No successful live upmap coverage is claimed.
