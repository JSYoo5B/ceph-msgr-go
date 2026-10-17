# Filtered PG listing (PGNLS_FILTER)

Run 2026-10-17, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture. Test: `TestCephOSDPGListFilterIntegration`. Contract:
`internal/osd/PG_LIST_CONTRACT.md`.

Nine objects are seeded natively in an owned pool: three with xattr
`tag=keep`, three with `tag=keeper` and three without it. The expected set is
taken from native xattr observation of each object. Every PG that holds them
is listed to its end in count-two pages, once with
`OSDPlainPGListFilter("_tag", "keep")` and once unfiltered:

- the filtered listing returns exactly the three `keep` objects (not the
  `keeper` prefix matches, not the untagged ones);
- the object-class filter `hello.hello` (cls_hello's registered PGLS filter,
  same input and semantics as plain) returns the same three;
- the unfiltered listing returns all nine;
- `nosuchclass.filter` (a class outside `osd_class_load_list`) is a known
  -1 (EPERM); `hello.nosuchfilter` and plain input whose length runs past the
  end are a known -22 (EINVAL).

An earlier attempt used plain input with a short declared length; on the
8-PG profile the OSD consumed cursor bytes as the value and answered
successfully, because the filter input and the cursor share one stream. That
case is not detectable by the client and is no longer used.

Passed in legacy-v6 single BlueStore (one PG, secure AES256K IPv4),
replicated-pg-v8 three BlueStore (five PGs, secure AES256K IPv6) and
request-v9/reply-v8 single MemStore (five PGs, secure AES256K IPv6); the
`hello.hello` case was added afterwards and passed in the first two.

Not claimed: other object-class filters (for example cephfs's), PGLS_FILTER
(the legacy unordered listing) and listing across PG splits while filtered.
