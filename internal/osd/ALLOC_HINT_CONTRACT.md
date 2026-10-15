# Tentacle O2g allocation hint contract

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`.
Implementation and source audit: 2026-10-15. Native evidence is recorded in
[OSD_ALLOC_HINT_VALIDATION.md](../../integration/fixture/OSD_ALLOC_HINT_VALIDATION.md).
[CONTRACT.md](CONTRACT.md) supplies the shared v6 request rules.

CEPH_OSD_OP_SETALLOCHINT (0x2223) is an advisory write. Its packed
`alloc_hint` union (include/rados.h:651-655) is expected_object_size le64,
expected_write_size le64 and flags le32, followed by eight zero union bytes;
there is no payload. The pinned builder (osdc/Objecter.h:303-310 and
1631-1641) always adds per-op FAILOK so that older OSDs do not fail the vector;
this client sends only the flags the caller sets.

The handler (osd/PrimaryLogPG.cc:6717-6730) creates a missing object, stores
the three values in object_info and passes them to the object store. It does
not validate the flag bits. The client admits only the pinned
CEPH_OSD_ALLOC_HINT_FLAG_* bits (include/rados.h:558-568) and refuses others
before transmission, because a value the server records without meaning
should not be presented as a supported hint. Like any write, a successful
vector requires ONDISK, advances the object version and applies a nonzero
mutation mtime. The reply echoes the union and returns no output. A hint is
a staged write for the ordering rules: a following CALL is refused.

The values are not visible through ordinary librados reads. Native acceptance
reads the committed object_info through the empty xattr name, which the OSD
maps to OI_ATTR `_` (PrimaryLogPG.cc:6429), and decodes it with the pinned
`ceph-dencoder` (object_info_t dump, osd_types.cc:6621-6638). This is a test
oracle only; the product does not decode object_info.

This contract paraphrases pinned LGPL-2.1 sources (rados.h permits LGPL-2.1
or LGPL-3.0) and copies no implementation.
