# Native IGNORE_CACHE probe

Run on Linux on 2026-10-18 with Ceph Tentacle 20.2.4 source
`7f793731f1b39eb4f465e960113d2363c311b964`. The independent receipt audit,
full native/controller receipts, attached/detached OSDMap metadata, source
hashes and original public-file hashes are recorded in
[OSD_IGNORE_CACHE_NATIVE_PROBE_LINUX_20261018.json](OSD_IGNORE_CACHE_NATIVE_PROBE_LINUX_20261018.json).

This is a native librados experiment. It creates no product Go client and
makes no Go IGNORE_CACHE or cache-pool support claim. The explicit
`probe-osd-ignore-cache` fixture command isolates cache pools from ordinary
integration runs: attaching a tier makes MON require OSD_CACHEPOOL.

## Execution recipe

Use a Linux Docker host and a fresh output directory for each run. Start
with other `CEPH_MSGR_TEST_*` selectors and fault profiles unset. From the
repository's `integration/fixture` directory, the CRC invocation is:

```sh
CGO_ENABLED=0 GOWORK=off \
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6 \
CEPH_MSGR_TEST_OSD_REPLICAS=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_RUNTIME=container CEPH_MSGR_TEST_CONNECTION_MODE=crc \
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_IP_FAMILY=4 \
go run . probe-osd-ignore-cache -project ../.. \
  -output .artifacts/ignore-cache-crc/private \
  -diagnostics .artifacts/ignore-cache-crc/diagnostics
```

For the second run, use `CONNECTION_MODE=secure`, `KEY_TYPE=aes256k`,
`SERVICE_CIPHER=aes256k` and `IP_FAMILY=6` with the same
`CEPH_MSGR_TEST_` prefixes and different output/diagnostics directories.
Both recorded runs use the container runtime and one BlueStore OSD.
The fixture enforces its pinned image and rejects mixed MDS/RBD/test/fault
profiles. The public evidence copier selects six probe receipts; it does
not copy configuration, keyrings or private ownership files.

## Observed native behavior

Each run uses two freshly owned, single-replica/min-size-1 pools: a base
pool and its writeback tier, with no overlay, hit set or snapshots. Five
fixed object names live only in the `cache-probe` namespace. Four native
clients have distinct positive GlobalIDs: the fixture observer and three
owned identities with tier read/write, tier read-only and base read/write
caps. The oracle checks the fixture FSID on each native connection.

| Profile | UTC interval | Controller steps | C API calls | Placement records |
| --- | --- | --- | --- | --- |
| CRC/AES, IPv4 container, BlueStore | 06:43:23–06:44:57 | 18 completed | 66 completed | 10 |
| secure/AES256K, IPv6 container, BlueStore | 06:47:10–06:48:43 | 18 completed | 66 completed | 10 |

Both controllers exit 0 and record `native_only=true` and
`product_support_claimed=false`. Every placement matches its owned pool ID,
namespace and object at attached epoch 27, with acting/up `[0]`, primary 0
and replicated shard -1. The FSID agrees across the plan, controller and
attached/detached maps; native identity names agree with the plan.

The native C API operation flag is **8**, which librados translates to wire
IGNORE_CACHE **0x8000**. Successful reads are two-op STAT+READ vectors, with
both per-operation results 0. WRITEFULL and DELETE are single-op vectors.
Raw binary read bytes, size, native user version and second-resolution
mtime are retained. The original return fields are also retained on errors;
an overall failure does not imply successful per-operation results.

- An IGNORE_CACHE read of a tier miss returns `-ENOENT` (-2) and leaves the
  tier absent. A subsequent ordinary read returns the base bytes and a
  direct tier read then finds those bytes.
- Ordinary and IGNORE_CACHE WRITEFULL both leave the replacement bytes in
  the tier. The base retains its original binary bytes, size, version and
  mtime.
- Both DELETE calls return 0. After ordinary DELETE, a subsequent ordinary
  read returns -2. After IGNORE_CACHE DELETE, the subsequent ordinary read
  returns 0 with the base bytes, which are again observable in the tier.
  The base stays unchanged in both cases.
- The tier read-only identity reads successfully but its write returns
  `-EPERM` (-1). Base-only access to the tier, tier-only access to the base
  and tier access in a foreign namespace each return -1. The tier target's
  complete bytes/size/version/mtime snapshot is unchanged across these
  authorization refusals.

The DELETE/read difference is consistent with the pinned cache whiteout
paths. The raw object_info `FLAG_WHITEOUT` bit was not observed; this record
claims subsequent native read behavior only.

## Completion, cleanup and limits

Before each selected controller mutation and C API operation, the oracle
writes an intended receipt. It records completion or uncertainty and stops
on an unexpected result; it never reselects an uncertain mutation. This
is the probe controller's policy, not a claim about librados internals.

All ten owned heads are removed with IGNORE_CACHE and absence is checked.
The controller then detaches the tier. Full detached maps at epoch 28 show
both original pool IDs, no tier relationships/overlays and cache mode none.
Completed controller steps cover removal of both pools and their owned
CRUSH rules, restoration of `mon_allow_pool_delete`, and removal of the
three owned identities. Its final gates check effective pool deletion is
false, each identity is absent and neither pool name remains. The temporary
central deletion override is removed, preserving the previously absent
central override. Those final auth/pool/config inventories are checked by
the frozen controller but are not separately archived. Each run's eight
owned container IDs and network are independently recorded absent after
teardown; no private ownership file is included.

The source audit starts from `source-1` plus exactly six fixture/oracle
files. Both native runs and the Linux checks use that frozen source. The
later MDS CLOSE change and other product work are outside this native-only
snapshot. Fixture tests, fixture vet and 429 Python tests pass with
`CGO_ENABLED=0`; their commands, UTC intervals and log hashes are retained.
These initial checks do not establish complete root tests or CI for a later
commit.

The separate `subsequent_checkpoint_checks` receipt covers the later
`source-2` MDS CLOSE checkpoint plus the same six fixture/oracle files, from
06:54:08.840642 to 06:55:03.744266 UTC. Their hashes are identical to the
native snapshot, and all 695 Go files match that later frozen inventory.
Complete CGO-disabled root tests, integration/cephmsgr race tests, root and
fixture vet, fixture tests and 429 Python tests each exit 0. The race step
uses `CGO_ENABLED=1`; the Go test/vet steps use `CGO_ENABLED=0`. This is a
subsequent source-combination check, not another native experiment. Other
product IGNORE_CACHE work is excluded. The record makes no completed CI
claim for `source-2` or a resulting commit.

Only these two profiles, one replicated BlueStore OSD, head objects and
this closed matrix are verified. Product Go transport, overlays, snapshots,
EC pools, multi-OSD routing, recovery, uncertain-outcome fault injection and
raw whiteout-bit decoding are outside this experiment.
