# Selected native COPY_FROM compound acceptance

Test/oracle checkpoint: `16c3bfaf07ef54408ddfe655ae7eb77216c14f51`.
Reference: Ceph v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`. Both profiles use
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

This is live daemon evidence, separate from the
[fresh v6 request vectors](../../internal/osd/testdata/copy-from-native-README.md).
It expands the historical COPY_FROM2/xattr test to both admitted opcodes and
absent/populated destinations. It changes no product encoder or runtime policy.

## Setup, operations and independent observations

`TestCephOSDCopyFromCompoundIntegration` uses three MONs, two MGRs and one or
three BlueStore OSDs. A native C API write exclusively creates a UUID-named
source and records ownership of five initially absent destination names.
The Go source-header setup is then independently observed through librados C
reads and the pinned rados OMAP-header CLI. The source has binary bytes/xattrs,
three OMAP keys (`""`, `a`, `z`), an empty value and a binary OMAP header.

One destination is reserved for a single independent `rados cp`. Its source and
destination must be distinct members of the oracle's ownership receipt, and
the target must still be absent. Fixed argv selects the fixture's pool and
identity; no shell is involved. The CLI is invoked once with a 30-second bound.
Refusal or timeout never triggers a second mutation. Its output is a baseline
copy, not a twin execution of the Go three-operation compound.

For each of COPY_FROM and flagless COPY_FROM2, Go copies to an absent destination
and to a destination prefilled with different bytes, an extra xattr, an extra
OMAP entry and a different OMAP header. Each vector is exactly:

1. Copy the caller-selected source pool/object, exact native source version and
   SEQUENTIAL source-read hint.
2. SETXATTR `added` to binary `00 61`.
3. RMXATTR `drop`, which came from the source.

Require signed aggregate/per-op success, ONDISK, matching opcodes/source union,
nonzero request identity, unchanged retry attempt, nonstale reply epoch and
matching local submission metadata. Native observations require the source's
bytes, exact edited xattrs, OMAP/header behavior matching the native baseline,
the reply's user version and the source's mtime. Stale destination metadata
must be absent. The independently observed source must remain unchanged.

After each successful copy, reject SETXATTR before the copy, and READ, WRITEFULL,
STAT or another copy after it. Every refusal must be ErrUnsupportedOSDOperation,
known-unsent, with zero submission/TID and no reply bytes. Independent native
state remains identical afterward. A subsequent STAT on the same connection
must have exactly the copy TID plus one and match native size/version/mtime.
This is bounded admission/connection evidence, not a server execution counter.

## First failures and the empty OMAP key boundary

The first native executions at `67c19ca05cefcd12889e393061785a92ab9cc22b`
failed all four destination comparisons on both profiles. Their tests lasted
3.87 seconds (CRC) and 4.61 seconds (secure); full runner times were 61.65 and
75.29 seconds. The test expected complete preservation of an empty OMAP key. First-run stdout
retained matching data, edited xattrs and mtime, but the bundled comparison
and pointer-bearing state did not identify its exact mismatching field.

The initial failure log printed pointer-bearing Go state. Diagnostic-only
checkpoint `2e42bdc914d277c5d32a5bffd3e33f67976e9843` moved native before/after and reply-version receipt capture
before the assertion. Its CRC rerun failed the same four comparisons and
retained the exact missing-key observation: source had three keys and each
destination had only `a`/`z`, while data, edited xattrs, header, mtime and
reply/native versions matched. This identifies the overbroad expectation in
the unchanged comparison; it did not fix a product failure.
All first/diagnostic runs confirmed owned-object and container/network cleanup.

The pinned `PrimaryLogPG::do_copy_get` source (PrimaryLogPG.cc:9397-9413)
starts OMAP enumeration at cursor `""` with `ObjectStore::UPPER_BOUND`. The final
checkpoint separately executes native `rados cp` (`rados.cc:541-551`,
`ObjectWriteOperation::copy_from`), which also omits that key on both profiles.
The final test requires that exact native baseline and compares Go's result
against it; the original raw source inventory remains in the receipt. This is
an independently reproduced pinned server-copy boundary, not a Go repair,
complete OMAP preservation claim or normalization of observed state.

`finish_copyfrom` (PrimaryLogPG.cc:10132-10133) adopts the source mtime and
clears `ctx->mtime`. Both native and Go copies retain
1700009000 seconds plus 123456789 nanoseconds despite the distinct Go compound
request stamp of 1700009002 seconds plus 987654321 nanoseconds.

## Final native results

Both runners exited 0; all four required children passed without skips:

| Profile | Full runner / test duration | Source version / native-copy version | Reply epochs |
| --- | --- | --- | --- |
| AES client/service, CRC, IPv4, host; legacy-v6, one BlueStore OSD | 61.63 s / 5.99 s | 3 / 3 | 13 |
| aes256k client/service, secure, IPv6, container; replicated-pg-v8, three BlueStore OSDs | 74.59 s / 6.64 s | 2 / 2 | 18 |

The replicated profile negotiates request v8/reply v6; this is not native request
v9 or reply-v8 acceptance. Native source has three keys, and native/Go copies
have two. In all eight Go cases, every per-op result is 0, all five refused
vectors remain known-unsent, source/destination comparisons remain unchanged
and next TID equals copy TID plus one.

| Case | CRC copy/next TID; destination version | Secure copy/next TID; destination version |
| --- | --- | --- |
| COPY_FROM absent | 2/3; 5 | 2/3; 3 |
| COPY_FROM populated | 5/6; 7 | 5/6; 3 |
| COPY_FROM2 absent | 7/8; 8 | 7/8; 4 |
| COPY_FROM2 populated | 10/11; 10 | 10/11; 4 |

Each native cleanup removed all six owned objects. Direct inspection after
runner termination confirmed removal of every recorded container and owned
network: eight fixture containers for the single-OSD profile and ten for the
three-OSD profile. Client containers/reapers also exited. No host settings or
unrelated resources were changed. The test has a 240-second operation context,
a separate 40-second object cleanup context and an eight-minute test timeout.

`osd-copyfrom-compound-results.json` retains source, native baseline, per-case
before/after observations, epochs, TIDs, versions, refusals and cleanup. It
omits endpoints, credentials and execution paths, and is in the diagnostic
allowlist. Failed comparisons retain partial observations before asserting.
The fixture's private ownership file is used only for teardown verification.

Separate Linux checks passed all 571 Python tests, 65 scenario declarations,
product/integration vet, fixture unit/vet and COPY_FROM-focused unit race tests
with count 3. Native CLI argument/ownership/absence/timeout unit tests are
adapter checks, not daemon evidence. No extra CI scenario was requested.

## Reproduction and remaining scope

On an authorized Linux runner, select the pinned image, set
`CEPH_MSGR_TEST_OSD=1`, `CEPH_MSGR_TEST_OSD_STORE=bluestore`, and
`CEPH_MSGR_TEST_RUN=^TestCephOSDCopyFromCompoundIntegration$`.
Select each profile's key/service cipher, connection mode, IP family, runtime,
OSD profile and replica count from the table. Run the existing fixture command
from the repository, respecting the runner's run lock:

```sh
go -C integration/fixture run . run -project ../..
```

The nested fixture module runs in its own module context. The receipt is
retained when an output/diagnostic directory is supplied to that command.
The secondary client-package selector has no matching tests and adds no evidence.

The test exercises actual same-OSD and cross-PG copies incidentally; it does
not prove every placement. No native compound twin, snapshot compound,
TRUNCATE_SEQ compound, destination loss after source-read completion/staging,
every source-version error, broader read-only vector admission, EC or additional
platform/topology is established. Keep issue #7 open for remaining work. The
product exposes the raw native operation result; it does not inspect source
OMAP, emulate missing keys or automatically re-execute mutations.
