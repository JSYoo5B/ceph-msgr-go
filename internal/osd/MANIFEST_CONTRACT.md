# Manifest redirect (SET_REDIRECT, UNSET_MANIFEST)

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`include/rados.h` SET_REDIRECT 0x2227 and UNSET_MANIFEST 0x222a,
`osdc/Objecter.h` set_redirect/unset_manifest, `osd/PrimaryLogPG.cc`
do_osd_ops and the manifest read proxy; LGPL-2.1, independently translated).

`OSDSetRedirect` carries the COPY_FROM union (target snap, always the head,
and the target version) and indata (target object name and locator), exactly
as Objecter's set_redirect without CEPH_OSD_OP_FLAG_WITH_REFERENCE. The OSD
makes the targeted object a manifest of type redirect, truncates its data,
sets its user version to the given target version and from then on serves
its reads from the target (in any pool). `OSDUnsetManifest` has no input and
clears the manifest; the object stays, now plain and empty. Both are
standalone and flagless here; reference-counted redirects, SET_CHUNK and the
tier operations are not implemented.

A read the OSD proxies to the redirect target answers with the requested
extent length echoed rather than the number of bytes returned (observed
live); the read validator therefore accepts an extent length equal to either
the payload or the request, the payload never exceeding the request.

Server refusals observed live: the redirected object must exist (-ENOENT),
must not target itself (-EINVAL), and UNSET_MANIFEST on a plain object is
-EOPNOTSUPP. A redirect inside a cache-tier pool would be -EINVAL.

Verification: [OSD_REDIRECT_VALIDATION.md](../../integration/fixture/OSD_REDIRECT_VALIDATION.md).
