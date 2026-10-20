# Full-pool request flags

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`include/rados.h:479-480`, `osd/PrimaryLogPG.cc:2138-2159,9014-9030`;
LGPL-2.1, independently translated).

A writing vector sent to a pool the map marks full is not answered by default:
the OSD discards an op sent before the full transition and otherwise drops it
(`-EAGAIN` internally), expecting the client to wait for the flag to clear.
This client has no such wait, so a default write simply never completes and its
outcome becomes unknown when the caller's context ends.

`OSDRequest.FullTry` sets `CEPH_OSD_FLAG_FULL_TRY`: the OSD replies `-EDQUOT`
for a quota-full pool or `-ENOSPC`, as a known server refusal with no effect.
`FullForce` sets `CEPH_OSD_FLAG_FULL_FORCE`: the write proceeds despite the
flag (native clients use it for deletes). The flags are mutually exclusive and
valid only on writing vectors; watch/notify vectors and read vectors refuse
them before submission. ENFORCE_SNAPC, FLUSH and the other rados.h request
flags remain unsupported.

Status: unit-tested for v6/v8 encoding and verified live on four profiles with
a quota-full pool, and on two BlueStore profiles with OSD fullness (lowered
cluster full ratios: `-ENOSPC` for FULL_TRY, flag cleared after restoration) ([record](../../integration/fixture/OSD_FULL_VALIDATION.md)).
