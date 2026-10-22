# Fresh Tentacle COPY_FROM request vectors

Reference: Ceph v20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Image:
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

[copy-from-native.json](copy-from-native.json) retains five raw front/data/header
vectors, native decoded operations/source inputs, segment hashes, source archive
and submodule hashes, generated configuration, producer/driver and binary/library
hashes. The Linux/amd64 run uses the existing official Ceph CMake SDK bootstrap;
the common library is reused from the matching pinned build. Only the new wrapper
is compiled and linked. Development packages are installed inside an owned
disposable container, never on the runner or in the product module.

## Inputs and results

The independent wrapper calls native `ObjectOperation::copy_from/copy_from2`
and directly encodes fresh MOSDOp objects. A separate MOSDOp decodes each result
and checks the packed operation and input bytes. Native object/locator decoders
also check the source inputs and complete payload consumption. No Go bytes,
decoded fronts or network captures are native encoder inputs. No reply or actual
copy execution is synthesized as daemon evidence.

All requests target pool 7, object `copy-codec`, namespace `tenant`, key
`route-key`, raw hash `0x11223344`, epoch 11, incarnation 1 and TID 77. Source
pool is 8, object UTF-8 `src\x01é`, namespace `source-ns` and key `source-key`,
with no explicit locator hash. Mtime is 1700000000 seconds plus 5 nanoseconds;
there is no destination snapshot context. Native opcode mode selection gives
WRITE alone, plus explicit ONDISK and RETURNVEC (`0x4000024`). Sequence 1 and ACK
sequence 3 are offline transport inputs, not live session observations.

| Shape | Native front/data bytes | Source selection | Go comparison |
| --- | --- | --- | --- |
| copy-head | 211/63 | COPY_FROM, head, version 0, hints 0 | Complete header/front/data equality |
| copy-snapshot | 211/63 | COPY_FROM, snap 6, version `0x0102030405060708`, SEQUENTIAL hint | Complete header/front/data equality |
| copy2-head | 211/75 | COPY_FROM2, head, version 0, flags/hints 0 | Go data 63 bytes; explicit difference below |
| copy2-snapshot | 211/75 | COPY_FROM2, snap 6, version above, flags 0, SEQUENTIAL hint | Go data 63 bytes; explicit difference below |
| copy2-truncate | 211/75 | COPY_FROM2, head, TRUNCATE_SEQ, sequence 9, size `0xfedcba98` | Complete header/front/data equality |

`TestCopyFromFreshNativeRequests` verifies all segment hashes before comparing.
For the two flagless COPY_FROM2 cases it requires exactly twelve trailing native
zero bytes, the corresponding payload-length increase in the operation header,
and equality of every other front/header byte. The raw native vectors remain
unchanged: this assertion does not normalize them and claim byte parity.

Pinned `osdc/Objecter.h:1520-1545` always appends the truncate pair for COPY_FROM2.
`osd/PrimaryLogPG.cc:8126-8135` reads it only with TRUNCATE_SEQ set. Go deliberately
keeps its existing minimal flagless representation, already accepted in the
[historical live profiles](../../../integration/fixture/OSD_COPY_FROM_VALIDATION.md).
This checkpoint changes no product encoder, API, feature mask or dependencies.
Native v6 encoding is proven here; v8/v9 native copy vectors remain ungenerated.

The expanded `TestCopyFromCompoundAdmission` checks 17 shapes across v6/v8/v9,
including both copy opcodes with SETXATTR/RMXATTR, prefix/read/write/STAT/second
copy refusals, SET_REDIRECT and four adjacent standalone guards. Every rejected
encoder must return ErrOperation with no sendable message. These are synthetic
admission checks, not native compound acceptance or public TID allocation proof.

## Reproduction and boundaries

Run on an authorized Linux Docker runner, with the run lock required by that
runner. From this repository, using a fresh work directory:

```sh
mkdir -p .artifacts/copy-native
docker run --rm --cpus=2 \
  -v "$PWD:/repo" -v "$PWD/.artifacts/copy-native:/work" \
  quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9 \
  python3 /repo/tools/osd_copy_from_native_encoder.py \
  --work /work --wrapper /repo/tools/osd_copy_from_native_encoder.cc \
  > .artifacts/copy-native.json
go test ./internal/osd -run 'TestCopyFromFreshNativeRequests|TestCopyFromCompoundAdmission' -count=1
```

The driver refuses stale output, an unexpected artifact set, inconsistent native
source/operation receipts, substituted raw segments or a foreign header. Private
build/loader logs stay in the work directory; the printed receipt omits paths.
Collector mutation tests do not generate new native evidence.

The first preparation attempts stopped before compilation because the transfer
destination was missing and Docker created the tools mount as root. Only that
run's directory ownership was corrected; those containers exited with no remaining
owned containers. The first completed producer passed all five native decoders
and Go comparisons. A later producer removes an unused-function warning and
clarifies operation receipt names; keep that provenance separate from the first
pass. Final owned-container cleanup is checked independently of codec success.

This closes only the native request-vector and admission-regression checkpoint
of issue #7. A later [selected live compound record](../../../integration/fixture/OSD_COPY_FROM_COMPOUND_VALIDATION.md)
separately covers both copy opcodes, independent native baseline and the
empty-OMAP-key boundary. Further read-only combinations and destination loss
after source-read completion/staging remain separate work. Constructed reply
vectors and native execution are not established by this request-only checkpoint.

The wrapper translates inputs and calls unchanged LGPL-2.1 Ceph headers/library;
it copies no native implementation into the product. The offline header follows
this repository's MAPEXT wrapper: native Message encoding and MessageFrame header2
projection, not execution of ProtocolV2's live writer. See
[THIRD_PARTY_NOTICES.md](../../../THIRD_PARTY_NOTICES.md).
