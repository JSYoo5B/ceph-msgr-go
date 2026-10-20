# PGNLS, SPARSE_READ and watch/notify acceptance — 2026-10-15

Tested product checkpoint: `source-1`.

This checkpoint adds explicitly targeted, bounded PG listing and sparse reads,
and owner-bound watch/notify streams. Callers still own placement, map changes,
ping scheduling, acknowledgement decisions and recovery. No new OSD feature is
advertised, and no uncertain operation is automatically replayed.

## Independent wire and daemon evidence

Fresh native message construction and separate native decoders use Ceph 20.2.4
commit `7f793731f1b39eb4f465e960113d2363c311b964`:

- PGNLS: one request and populated, empty-end, EINVAL and EACCES replies.
- SPARSE_READ: one request and populated, empty, ENOENT and EACCES replies.
- Watch/notify: six requests, six replies, and NOTIFY, COMPLETE, timeout and
  DISCONNECT events (16 vectors).

The message header projection has explicit offline sequence/ACK inputs. Native
logical replies are wire evidence, not live operation results. Stored provenance
binds source, archives, configuration, wrapper/driver, library, binary and segment
hashes. Watch/notify has its own native build driver so earlier generator proofs
are not silently relabeled. Go request/reply/event parity and provenance checks
passed. EACCES vectors do not establish live capability enforcement.

Both selected daemon profiles use the same pinned commit and image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Daemons are Linux arm64; the fixture module uses provider v0.2.0. Native helpers
are development-only and product compilation remains CGO=0.

| Profile | PGNLS | SPARSE_READ | Watch/notify | Outcome |
| --- | --- | --- | --- | --- |
| Darwin arm64 host, IPv4 relay, CRC, AES client/service, three BlueStore replicas | 4 | 10 | 5 | 19/19 passed, no skips, cleanup completed |
| CGO=0 Linux arm64 container, IPv6, secure, AES256K client/service, one MemStore | 4 | 10 | 5 | 19/19 passed, no skips, evidence collection and cleanup completed |

PGNLS compares the union of count-one Go pages with an independent native
complete inventory of the same explicit PG. Default, isolated and empty
namespaces are covered, plus local rejection of a different primary. Empty
intermediate pages remain possible; aggregate result one, rather than the cursor
sentinel or per-operation result, indicates completion. Native and Go page
boundaries are not compared. There is no atomic inventory or automatic pool scan.

SPARSE_READ compares the exact submitted offset/length with public native C++
`IoCtx::sparse_read`. Its success result is an extent count, whereas the Go wire
operation result is zero. Ten cases cover dense/full/partial reads, clipped and
exact EOF, past EOF, sparse ranges and holes, ENOENT, and an isolated output-budget
failure. The observer checks object bytes, nanosecond mtime, xattrs and version
before and after. BlueStore and MemStore allocation behavior is retained rather
than normalized into one layout. The C++ helper links the selected image's
librados/ceph-common; build and loader receipts are retained separately. Ordinary
CI explicitly skips this comparison unless the optional helper is staged.

Since 2026-10-16 the helper also takes an optional read snap id
(`IoCtx::snap_set_read`), and SPARSE_READ at a read snapshot is compared on
both profiles above: a clone with holes, the head through a later or higher
snap id, ENOENT where no clone covers the snap id, reads past the clone's end
and the clone of a deleted head. The rebuilt helper, its hashes and the
observed maps are in
[OSD_SNAPSHOT_VALIDATION.md](OSD_SNAPSHOT_VALIDATION.md) ("Snapshot
SPARSE_READ"); the ten head cases above passed again with it.

Watch/notify covers a Go notifier with two independent native watchers, explicit
Go watch registration and ping, partial timeout with native replies and one
unacknowledged Go watch, a native binary notifier receiving an explicit binary
Go ACK, and explicit unwatch. Ordinary operation replies and asynchronous
completion remain distinct. Each run retains five operation receipts and four
events. Native notification uses `rados_notify2` and the native response decoder,
with explicit binary lengths and separate watcher/notifier identities.

## First failures and corrections

The first PG run passed its cases but closed a shared connection before a
namespace cleanup hook. Cleanup ordering was corrected; subsequent PG runs and
final profiles passed. The first two native notifier startups failed. The retained second status
confirms a TypeError from bytes in the Cython `msg: str` argument; the first
run retained only the aggregate startup failure. Switching the test oracle to the native C API resolved it.

The first sparse helper build failed because the compiler linker found an
incompatible libctf. Compiler and runtime library search paths were separated.
The compiled helper then trapped during native initialization when linked with
a static C++ runtime; an isolated fake-config smoke run reproduced exit 133.
Using the image's dynamic C++ runtime made that smoke test reach the expected
configuration error and both real profiles pass. This is fixture tool evidence,
not a diagnosed product protocol defect. Final receipts preserve stage traces,
exit codes and bounded native output instead of concealing execution failures.

Independent review also identified a stale-authentication publication race and
an early notify-completion ID mismatch. Publication now rechecks the live
identity while admitting the event; provisional completion is correlated with a
successful ordinary reply before delivery through the returned notifier. If that
reply is lost, retained events are evidence rather than a corroborated result.
Raw Request cannot bypass stream cookie ownership. Deterministic race regressions
cover these paths. Public evidence files are atomically published as 0644 so a
non-root host can collect container-generated JSON; see
[CI evidence permissions](CI_PUBLIC_EVIDENCE_VALIDATION.md).

## Reproduction and evidence

Build the optional helper and fresh wire vectors from the fixture module:


Run the selected tests from the product checkout:


Selected profiles do not establish live DISCONNECT/reconnect/failover, scoped
watch permissions, mutation races during enumeration/reads, EC or broad platform
compatibility. Backoff/split feature activation and MDS remain separate milestones.
Remote CI success is not established by these local runs.

Local verification of the exact staged source passed: whole-product CGO=0
tests/vet, cephmsgr/internal-osd race tests, nested fixture CGO=0 tests, and
284 Python tool tests. Sparse decoder fuzz ran 1,574,385 executions over 20
seconds with two workers. These checks do not replace native profile evidence.
