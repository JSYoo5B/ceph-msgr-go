# Native xattr oracle preparation

This is an O2 prerequisite, not implemented Go xattr support. The product still
rejects these opcodes. The independent test-only adapter invokes Tentacle's
public librados C API; no Go wire encoder generates its native requests.
Wire payload/header vectors, raw per-op replies and Go/native interoperability
remain separate acceptance work in [METADATA_CONTRACT.md](../../internal/osd/METADATA_CONTRACT.md).

Reference source: Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The first successful native matrix on 2026-10-15 used released
ceph-testcontainers-go v0.2.0, CRC, AES entity/service keys, IPv4, Darwin arm64
host relay and three BlueStore OSDs with replica size 3/min_size 2. The fixture
manifest records its actual architecture/image/FSID and daemon release; librados
ABI version `[3,0,0]` is recorded separately and does not identify Ceph's release.

## Observed native behavior

`TestCephOSDXattrNativeOracleIntegration` passed in 0.31s. Its bounded receipt
and independent Go assertions checked these facts on a dedicated disposable
object, removed afterward:

- Four attributes retained binary NUL/0xff bytes, an empty value, a leading
  underscore name and the comparison operand. Direct GET byte counts and
  GETXATTRS iterator output matched the original values; absent attributes
  returned ENODATA. Object bytes remained unchanged.
- One compound SET/RM replaced binary data and removed the empty attribute,
  increasing the version without changing unrelated object content.
- All six STRING comparison operators ran in both read and write vectors,
  with caller operands less than/equal to/greater than the stored binary
  operand: 36 cases. The caller operand is on the left. Ordinary false guards
  returned ECANCELED and preserved prior content, attributes, version and mtime.
  Successful write guards committed their distinct marker; read guards did not
  change object state.
- A false guard after an earlier staged SET rolled back the whole vector.
  FAILOK allowed the later marker and earlier SET to commit. Final native
  stat2 version/size/nanosecond mtime bracketed each set of observation reads.

The C API has no compound single-GETXATTR builder in the pinned header. Direct
GET and compound LIST are separately labeled. CMPXATTR/SET/RM callbacks also
do not expose raw per-op results; positive CMP true result 1 is pinned server
source evidence, not an observed C callback result. This oracle does not prove
wire union encoding, Go output budgets, Go sent-outcome semantics, staged xattr
read visibility, non-UTF-8 names on the server, configured size/name refusal,
permission refusal, U64 mode, restart persistence or crash recovery.

Eleven adapter regression tests passed. They cover binary-safe pointer/length
bindings, NULL value with length zero versus NULL-name iterator end, copying
before native buffers are freed, name validation and observation-version rules.
A daemonless pinned container also constructed/released all six read/write
comparison builders plus binary/empty SET/RM; that result alone was not treated
as server interoperability.

## First failures and correction

The first live fixture failed; the generic controller discarded the traceback,
so that run has no conclusive cause receipt. Its original log remains at
`/tmp/ceph-xattr-native-first.log` and its ownership files remain separate.
The second fresh fixture retained a bounded native command result: exit 1,
complete, no timeout/truncation. It failed the oracle's post-GET version check,
not a Go xattr request.

Pinned [PrimaryLogPG's read completion](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L9201)
does not set user_version for ENODATA.
[Objecter](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.cc#L3680)
copies that default reply version and
[IoCtxImpl](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/librados/IoCtxImpl.cc#L723)
updates last_objver even for a negative read. Thus absent-attribute GET can
replace the ioctx's last version with zero; it does not establish object change.
The oracle now performs one final successful stat2 and compares its version,
size and mtime to the initial stat2. Regressions accept a negative GET followed
by unchanged stat and reject independently changed final version/size/mtime.
No mutation or failed GET is retried.

The third fresh fixture passed with adapter SHA256
`a8a3c725a68d281de22920efee347ea5f7be72a2e7f38b107ca96f22fbe25b53`.
Evidence is `/tmp/ceph-xattr-native-third.log`,
`/tmp/ceph-xattr-native-third-diag/osd-xattr-native.json` and its bounded command
receipt. Every attempt's ten owned containers and network were independently
absent after cleanup. Native command stdout/stderr is now retained within
explicit limits; keys, configuration and private auth state are not inputs to
this receipt.

## Reproduce

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc \
CEPH_MSGR_TEST_RUN='^TestCephOSDXattrNativeOracleIntegration$' \
go -C integration/fixture run . run -project ../.. \
  -diagnostics /tmp/ceph-xattr-native-diagnostics
python3 -m unittest discover -s tools -p 'test_osd_xattr_native.py'
```

All daemon lifecycle stays in the public fixture APIs. The C adapter and Ceph
tools are development-only; the Go product's build/runtime/dependency graph
remains CGO-free. The wrapper is independently written against public headers
with LGPL-2.1 provenance and copies no native implementation.
