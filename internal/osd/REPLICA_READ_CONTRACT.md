# Replica reads (BALANCE_READS / LOCALIZE_READS)

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`include/rados.h:462-466`, `osd/PrimaryLogPG.cc:2058-2073,2195-2215,
2335-2358`, `osd/PG.cc:1940-1954`, `osd/PeeringState.cc:1555-1566`; LGPL-2.1,
independently translated).

`OSDRequest.BalanceReads` / `LocalizeReads` set the two request flags. The OSD
then serves a vector on a non-primary acting member when the vector may read
and neither writes nor uses the cache; otherwise it treats the request as
misdirected and drops it, so the caller receives no reply. The caller owns the
choice of replica (any acting-set member) and the replicated profiles' `Primary`
field keeps naming the PG primary, which may differ from the connection target
only when one of these flags is set.

Admitted vectors: Read, Stat, GetXattr(s), the OMAP reads, MapExt, Checksum and
SparseRead. Writes, class calls, snapshot/watcher listing, PG listing and watch
vectors are refused before submission. A replica answers `-EAGAIN` when the
object is unreadable there or has a write newer than its `pg_committed_to`;
that is a known refusal preserved raw. `pg_committed_to` advances on a replica
only with the next write in the same PG, so an object written and then left
idle can be bounced to the primary until another write lands in its PG.
Both causes were observed live: an object whose write was the PG's last, and an
object a replica still lacked after rejoining with recovery held
(`norecover`/`nobackfill`), each answered `-11` with no effect and no local
resend. A replica just recovered may bounce once more after the next write.
The client never redirects or repeats a bounced read; the caller may resend
it (to the primary, or later), since a read has no effect.

Verification: [OSD_REPLICA_READ_VALIDATION.md](../../integration/fixture/OSD_REPLICA_READ_VALIDATION.md).
