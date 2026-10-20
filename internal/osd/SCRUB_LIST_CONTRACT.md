# Scrub error listing (SCRUBLS)

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`include/rados.h` SCRUBLS 0x1507, `common/scrub_types.{h,cc}`
scrub_ls_arg_t/scrub_ls_result_t, `osdc/Objecter.h` scrub_ls,
`osd/PrimaryLogPG.cc` do_scrub_ls; LGPL-2.1, independently translated).

`OSDConnection.ListInconsistent` sends one PG operation SCRUBLS to the PG's
primary, as librados get_inconsistent_objects/get_inconsistent_snapsets do:
the PG-op envelope of PGNLS (empty head object, PGOP flag, the profile's
v6/v8/v9 encoding) with a zero operation union and an encoded
`scrub_ls_arg_t` v1 as indata (interval, get_snapsets, start-after name,
namespace and snap, max_return). The reply's `scrub_ls_result_t` v1 carries
the interval and one buffer per inconsistent object or snapset; each is
returned raw (`Values`), bounded by MaxReturn and MaxOutputBytes, and its
interpretation belongs to the caller.

Server semantics: the PG id in the request must equal the PG's own id, so the
seed is the actual folded seed in every profile (a raw hash is -EINVAL);
interval zero accepts the current interval and a different nonzero interval
is -EAGAIN; a primary without scrub results (no scrub since it became
primary) answers -ENOENT. The call only reads: it never starts a scrub or a
repair, and nothing is retried.

Verification: [OSD_SCRUB_LIST_VALIDATION.md](../../integration/fixture/OSD_SCRUB_LIST_VALIDATION.md).
