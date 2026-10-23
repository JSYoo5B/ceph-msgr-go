# Cache-operation terminal completion

Native fixed checkpoint: `2700127d24df7d6ff693f2eb34ad680b2a3b2346`.
Pinned Ceph v20.2.4 source `7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
The [bounded sanitized receipt](testdata/cache-completion-secure-v6.json)
retains both the first native expectation failure and the corrected run's raw
reply fronts, signed results/flags, request/route identity, independent states
and cleanup. Endpoints, keys, filesystem paths and incidental host metadata
are omitted. Inactive input unions are omitted from the JSON projection;
active operation codes/flags and raw terminal reply bytes remain unchanged.

## Completion rule derived from native branches

Native READ encoding of FLUSH, TRY_FLUSH and EVICT remains unchanged. A
successful reply to any of the five admitted CACHE operations requires the
requested ONDISK bit; PIN/UNPIN already satisfy the ordinary WRITE-mode rule.
ACK-only, ONNVRAM-only and absent-completion success preserve the sent
submission/raw reply and return OutcomeUnknownError. A signed negative
aggregate remains OSDError even without completion bits. Nothing is
resubmitted automatically. Request flags, packed per-op flags and reply flags
are different fields; this change neither replaces READ with WRITE nor adds
per-op flags.

This requirement follows the pinned source's terminal paths, rather than an
analogy with SET_CHUNK:

- [CACHE handlers](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L6307-L6430): dirty FLUSH/TRY_FLUSH starts asynchronous base work; clean flush is a no-op. Clean EVICT calls `_delete_oid`; dirty, watched or pinned heads refuse it.
- [execute_ctx](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L4268-L4284) consumes internal EAGAIN without producing a terminal reply. It is not a Go-visible retry result merely because a flush handler returns it.
- The [base-pool mutation](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L11089-L11097) precedes the [CLEAN transaction and retained-request requeue](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L11259-L11321). One Go submission is distinct from OSD reprocessing or its internal Objecter transactions.
- Both [complete_read_ctx](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L9219-L9222) and the [commit callback](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L4379-L4391) add ACK+ONDISK. Equal bits do not establish equal state transitions or successful mutation.
- An [early error reply](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/OSD.cc#L1635-L1650) instead echoes the request's ACK/ONDISK bits. These requests ask for ONDISK only, so absent-head errors need not carry ACK. Their untouched zero per-op result does not show that a handler executed.

| Operation / independently observed branch | Signed terminal result | Minimum success evidence | Independent base/tier postcondition |
| --- | --- | --- | --- |
| FLUSH absent | ENOENT, per-op 0 on this profile | Not a successful completion | Both heads remain absent |
| TRY_FLUSH absent | ENOENT, per-op 0 | Not a successful completion | Both heads remain absent |
| EVICT absent | ENOENT, per-op 0 | Not a successful completion | Both heads remain absent |
| FLUSH clean | 0 / 0 | ONDISK | Both copies' bytes/size/version/mtime unchanged, tier clean |
| TRY_FLUSH clean | 0 / 0 | ONDISK | Same no-op state, tier clean |
| FLUSH dirty | 0 / 0 | ONDISK | Base A becomes B, tier keeps B, dirty clears |
| TRY_FLUSH dirty, issued once | 0 / 0 in this run | ONDISK | B in both pools, final native dirty read/poll false |
| EVICT dirty | EBUSY / EBUSY | Not a successful completion | Both copies and dirty=true unchanged |
| EVICT clean/unpinned | 0 / 0 | ONDISK | Tier head absent; flushed base bytes/size/version/mtime unchanged |
| PIN / UNPIN and pinned refusals | 0; EPERM refusals; absent ENOENT | ONDISK on success | Existing behavioral pin boundary retained; no raw pin-bit observation claimed |

The READ wire request cannot tell the client whether a successful flush was a
no-op or wrote data. ONDISK is therefore the common positive terminal minimum,
not proof that a mutation happened. Independent state supplies that distinction.
This profile does not prove crash, power-loss or device durability.

## Reproduced completion counterexample and regressions

At `373742a138c9244d16e98c269590cdee75a31e1f`, unchanged product code actually
accepted a synthetic successful ACK-only CACHE_FLUSH with nil error. The
focused Linux selector was:

```sh
go test ./cephmsgr \
  -run '^TestOSDCacheOperationCompletion$/^v6$/^flush$/^ack-only$' -count=1 -v
```

The new assertion failed because success completed with flags 1. This is a
public-transport counterexample, not a live daemon returning an ACK-only cache
success. Earlier synthetic helper attempts failed feature admission before
sending a request; they are not completion evidence. The final helper
negotiates the cache capability with the correct MON/OSD profile subset.

The fix extends RequiresOnDisk for successful cache READ operations without
changing encoding, signed negatives or automatic replay policy.
`TestOSDCacheOperationCompletion` covers five opcodes, three wire profiles
(v6, request-v8/reply-v8, request-v9/reply-v8), and eight completion/refusal
shapes: no bits, ACK, ONNVRAM, ACK+ONNVRAM, ONDISK, ACK+ONDISK and two signed
ENOENT forms. All 120 cases retain original submission/raw metadata and reject
resending. Linux Cache/SetChunk unit race selections passed with count 3;
fixture units/vet, product/integration vet, four closed-observer Python tests
and all 635 Python tool tests passed. These are distinct from live evidence.

## First native failure and corrected native result

At `c10b7079b761ee1127432bcf5950f6cc08b91651`, the first extended native test
incorrectly required both ACK and ONDISK on every reply. All six earlier child
steps passed, including dirty write-back and the two clean no-ops. The absent
FLUSH returned aggregate -2, per-op 0 and flags `0x4028014`, containing ONDISK
without ACK. The test failed at that overbroad flag assertion (2.14 seconds),
after 13 Go requests; later absent cases were not executed. The independent
native observer finished cleanup, and all eight owned containers/network were
removed. This was a test expectation failure, not a reproduced product or
persistence failure. The pinned early-error path above explains the missing
ACK; a later passing run is not its explanation.

At fixed checkpoint `2700127d24df7d6ff693f2eb34ad680b2a3b2346`, the test requires
ONDISK on successful replies and keeps negative flags raw. All seven children
actually passed without skips in **2.94 seconds**, with 17 Go submissions:

| TID / operation | Aggregate / per-op | Raw reply flags |
| --- | --- | --- |
| 1 dirty EVICT | -16 / -16 | 0x4038015 |
| 2 PIN | 0 / 0 | 0x4008025 |
| 3 pinned FLUSH | -1 / -1 | 0x4028015 |
| 4 pinned TRY_FLUSH | -1 / -1 | 0x4038015 |
| 5 pinned EVICT | -1 / -1 | 0x4038015 |
| 6 UNPIN | 0 / 0 | 0x4008025 |
| 7 dirty EVICT after UNPIN | -16 / -16 | 0x4038015 |
| 8 dirty FLUSH | 0 / 0 | 0x4028015 |
| 9 clean FLUSH | 0 / 0 | 0x4028015 |
| 10 clean TRY_FLUSH | 0 / 0 | 0x4038015 |
| 11 dirty TRY_FLUSH | 0 / 0 | 0x4038015 |
| 12 clean EVICT | 0 / 0 | 0x4038015 |
| 13 absent FLUSH | -2 / 0 | 0x4028014 |
| 14 absent TRY_FLUSH | -2 / 0 | 0x4038014 |
| 15 absent EVICT | -2 / 0 | 0x4038014 |
| 16 absent PIN | -2 / -2 | 0x4008025 |
| 17 absent UNPIN | -2 / -2 | 0x4008025 |

All replies carry epoch 27. The selected fixture is Linux/amd64,
secure/aes256k client and service, IPv6 container runtime, legacy-v6, one
BlueStore OSD with two owned one-replica pools in writeback mode without an
overlay. The closed native observer never issues CACHE operations. It seeds,
reads and removes only its three owned names through IGNORE_CACHE STAT+READ
and C++ is_dirty; it records each new clean and absent branch separately.
TRY_FLUSH is sent once; internal reprocessing is not counted as another client
submission. No live EAGAIN was observed or manufactured.

Native finish removed/confirmed absence of all three names in both pools.
Fixture teardown completed its pool/client/config restoration checks and
exited zero. Direct post-run inspection found all eight recorded containers
and their network absent. The bounded receipts retain the original signed
metadata and all independent native seed/branch observations.

## Reproduction and remaining cells

Run the existing fixture command on an authorized Linux runner under its run
lock with `CEPH_MSGR_TEST_OSD_CACHE_POOLS=1`, `CEPH_MSGR_TEST_OSD=1`,
`CEPH_MSGR_TEST_OSD_STORE=bluestore`, `CEPH_MSGR_TEST_OSD_REPLICAS=1`,
`CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6` and
`CEPH_MSGR_TEST_RUN=^TestCephOSDCacheOpGoIntegration$`. Select secure,
aes256k client/service, IPv6 and container runtime, with the pinned image.
The existing exact scenario row is
`osd-cache-pools.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-osdcacheopgo`;
no duplicate row or workflow trigger is added.

This establishes the selected terminal contract and state distinctions,
not every cache configuration, live lost-response behavior, crash durability,
all concurrency/EAGAIN paths, raw pin bits, snapshots, EC or autonomous cache
policy. The public negative errno remains raw if another server sends one.
Keep #8 open for its remaining acceptance cells.
