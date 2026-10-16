# U64 CMPXATTR draft verification

This development branch adds wire mode 2 to existing raw xattr comparisons.
It is **not yet native-verified**. No daemon compatibility, new native vector,
C++ build success or atomicity claim follows from the local Go tests.

Reference: Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Audited sources:
`include/rados.h` (mode 2 and packed union), `osdc/Objecter.h` (raw name and
operand builder), `librados/librados_cxx.cc:130-136` (uint64 overload),
`osd/PrimaryLogPG.cc:5027-5043,6462-6530` (decimal stored-value parsing,
caller-left operators, true +1, false -ECANCELED and malformed stored EINVAL).
These LGPL-2.1 sources were inspected; no implementation was copied.

`OSDXattrInput.Mode` is explicit: STRING=1 or U64=2. Zero/unknown CMP modes
are rejected. U64 operands remain exactly eight caller-supplied little-endian
bytes in `Data`; there is no numeric storage workflow or stored-value parser.
Non-CMP mode fields remain zero. Existing transaction ordering restrictions,
FAILOK rules, output/result metadata and unknown-outcome handling remain.

Local verification completed:

- Full product module `CGO_ENABLED=0 go test ./...` passed.
- Full fixture module `CGO_ENABLED=0 go test ./...` passed after adding the
  new optional environment marker to its explicit environment contract.
- Product codec/public API `go vet` and focused race checks passed.
- Focused packed-field, input bound, ownership, reply width and public
  known-result tests passed for STRING and U64.
- Five Python tests passed for closed oracle requests, unsigned high operands,
  raw stored bytes, preserved timeout evidence and unavailable native prvals.
- Fixture staging tests passed for opt-in OSD requirements, host-path privacy,
  pinned image/source/wrapper/binary hashes and non-ELF rejection.

Pending independent evidence:

1. Run `encode-osd-u64-xattrs` in an owned pinned native build container. Its
   separate five-vector suite has two fresh requests and three logical native
   replies. Import only its own new proof/files; historical vectors are unchanged.
2. The same builder compiles `osd_u64_xattr_oracle.cc` using the public C++
   `cmpxattr(name,op,uint64_t)` overload. The C ABI is STRING-only and is used
   solely for fixture seeding and independent before/after state observations.
3. Stage that binary with `CEPH_MSGR_TEST_U64_XATTR_ORACLE`, then run
   `TestCephOSDU64XattrIntegration` in the selected replicated fixture profiles.
   Its 52 cases cover all six caller-left operators above 2^63, UINT64_MAX,
   empty/missing attr, missing object, decimal prefixes/NUL suffix/leading zeros,
   invalid signs/whitespace/NUL prefix/overflow, write guards, data rollback
   and FAILOK continuation. Compare raw Go metadata, native aggregate results,
   bytes/attrs/mtime and per-object version changes. Native C++ cmp per-op
   output is unavailable and is explicitly labeled as such.

No Docker build or live daemon test has been run for this draft yet.
