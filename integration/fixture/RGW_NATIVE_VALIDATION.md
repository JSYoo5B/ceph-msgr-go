# Bounded native RGW Messenger checkpoints

`TestRGWNativeCheckpointIntegration` (small object) and
`TestRGWNativeMultiStripeIntegration` (multi-stripe object, manifest and
garbage collection, described [below](#multi-stripe-manifest-and-gc-checkpoint))
are dedicated development-fixture tests using released
ceph-testcontainers-go v0.3.0. They add no product API, S3 SDK, native runtime
dependency, fixture profile, or comment-selected CI command.
Issue [#13](https://github.com/JSYoo5B/ceph-msgr-go/issues/13) remains open for
the remaining service-induced paths and failure campaigns.

The retained historical Linux/amd64 run at source
`bdbd655e9f3c1373ed7a08ab98641f08d80a129a` actually passed in 38.02 seconds.
[The sanitized native receipt](RGW_NATIVE_LINUX_20261023.json) retains the
gateway generations, request identities, PG/pool IDs, map epochs, object
names, byte hash and independent cleanup checks. This is native fixture
evidence; the normal cluster-free CI checks are a separate result. The receipt
does not establish native acceptance for later code changes. Review follow-up
results for the exact final code-and-contract commit are recorded in the PR
body, after that commit is fixed, without a subsequent documentation commit.

## Reproduction and mandatory gate

Run on an authorized Linux Docker runner with Go 1.26 or later, under the
runner's exclusive Docker-test lock. Do not run against an existing Ceph
cluster. The test creates a separate one-MON, one-MGR, one-BlueStore-OSD,
one-gateway fixture, uses the fixture's generated CephX credentials and creates
one ordinary S3 user and one uniquely named bucket.

```sh
CEPH_MSGR_TEST_RGW_NATIVE=1 go -C integration/fixture test -json \
  -run '^TestRGWNativeCheckpointIntegration$' -count=1 -timeout=10m . \
  > rgw-native.jsonl
python3 - <<'PY'
import json
from pathlib import Path
rows = [json.loads(line) for line in Path("rgw-native.jsonl").read_text().splitlines()]
assert sum(row.get("Action") == "pass" and
           row.get("Test") == "TestRGWNativeCheckpointIntegration"
           for row in rows) == 1, "the native checkpoint must actually PASS"
assert any(row.get("Action") == "pass" and not row.get("Test") for row in rows)
PY
```

Without the explicit environment variable the native test skips. The normal
fixture unit checks therefore provide no native acceptance evidence. Keep raw
Docker/native logs private and review any selected excerpts before publishing.
The test omits S3 credentials, HTTP error bodies, admin-socket addresses and
private runner paths from its checkpoint receipt.

## Recipe and assertion boundaries

1. Start only the owned fixture, with Messenger v2 secure-only configuration
   and aes256k keys/service tickets. Require the native `ceph version` response
   to identify v20.2.4 and source
   `7f793731f1b39eb4f465e960113d2363c311b964`. The immutable image reference is
   `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
2. Create the owned bucket through test-only signed HTTP and read its native
   bucket ID/marker. RGW and the independent oracle both authenticate as
   `client.admin`; that entity alone cannot attribute requests to the gateway.
3. Issue exactly one PUT of a deterministic 1,031-byte binary object. Before
   any backend data/index oracle, read actual pool names/numeric IDs from
   MON metadata and discover the gateway PID 1's owned UNIX
   admin sockets and sample its local Messenger catalog. Query each actual
   `radosclient` / `radosclient-<nonce>` name for its authenticated numeric
   identity and nonce, including handles connected after the input. Retain
   OSD `dump_historic_ops` output once the required request vector appears.
   Completed history is published asynchronously; a ten-second read-only
   polling bound handles that visibility delay without resubmitting PUT. The
   fixture retains at most 4,096 operations for 600 seconds; parsing bounds
   the history to 32 MiB, descriptions to 16 KiB and events to 128 per entry.
4. Discover every actual shard of this bucket in its identified index pool.
   Require gateway-GlobalID-attributed, TID-consistent, decoded `osd_op`
   descriptions with a `reached_pg` event for the head write and native
   `rgw.bucket_prepare_op` / `rgw.bucket_complete_op` class calls. The
   corresponding head/index object names must match exactly. Both the PG
   pool ID and hobject pool ID must equal the discovered pool's numeric ID;
   prepare/complete CALLs count only on an owned index shard, not on the
   head or a same-named object in another pool. Complete WRITE/WRITEFULL
   operation tokens and class-method tokens are required. A head class call
   such as `call rgw.not_write in=4b` or an xattr operation such as
   `setxattr user.not_write (1)` cannot substitute for a WRITE opcode;
   regression checks pair each with valid index prepare/complete calls.
   A class method with an extra suffix cannot satisfy the gate either.
5. Compare every S3 GET byte with the input, independently read the native
   head bytes with `rados get`, and locate the exact key in exactly one
   actual bucket-index OMAP. These oracle requests happen after the retained
   reception evidence and use different native client instances.
6. Stop the owned gateway once, observe it stopped, start the same container
   once, require a different process start time and Messenger GlobalID/nonce,
   then resolve that same container's current published S3 endpoint and
   repeat only read observations. Docker can change an ephemeral published
   port on Stop/Start. All previous identities/nonces must be absent from
   the new process generation. The retained bytes and index shard must be
   unchanged. No PUT is replayed to manufacture recovery.
7. Delete the object once through S3, retain gateway-attributed native head
   `CALL rgw.obj_remove` reception, and independently require HTTP 404, head
   absence and index-key absence. S3 DELETE is not a RADOS DELETE opcode:
   pinned RGW uses its native class method. Delete the empty bucket and
   remove the owned S3 user.
8. Terminate the owned cluster with a fresh 90-second cleanup context.
   Independently inspect each recorded owned container, mounted named volume
   and network through Docker and require NotFound. No unrelated container,
   network, volume or host setting is modified.

The S3 input has a 20-second HTTP bound, no redirect following, no automatic
mutation retry and at most a 1 MiB response. Native command reads are bounded
to 20 seconds and 32 MiB. A timeout preserves an unknown remote mutation
outcome by failing the run; cluster teardown removes its disposable state.

## Evidence classes and remaining work

The HTTP status/body, OSD request reception and backend postconditions are
separate evidence. A `reached_pg` history entry establishes reception of the
printed native request; it does not decode its entire payload, assert its
return code, prove cross-object atomicity or establish stronger durability.
Matching object/index state supplies a separate postcondition.

In particular, bucket-index OMAP contents do not imply that RGW sent
OMAPSETVALS over Messenger. The test requires received native CALL methods;
the independent oracle's `listomapkeys` is its own OMAP traffic. Existing raw
Go OSD call/write primitives remain separate from this native-only checkpoint;
no direct Go RGW operation or server-role implementation is introduced.

This small-object checkpoint does not cover large objects, multipart,
multisite, service-registration messages, MGR/OSD faults, compression or
arbitrary security/address profiles. Manifest-ordered tail reconstruction and
deferred GC belong to the multi-stripe checkpoint below. Neither adds
catalog/workflow propagation or a trusted comment selector. Those require
their own actual-PASS gates and native evidence before SVC-1 or issue #13 can
be described as complete.

## Multi-stripe manifest and GC checkpoint

`TestRGWNativeMultiStripeIntegration` starts its own fresh one-MON, one-MGR,
one-BlueStore-OSD, one-gateway fixture with the same secure-only aes256k
configuration and the same release/source identity check, and reuses the
small checkpoint's gateway Messenger identity discovery, bounded read-only
history polling, one-shot S3 input and cleanup gate. It is selected only by
its exact name; it needs `CEPH_MSGR_TEST_RGW_NATIVE=1` and otherwise skips.

1. Issue exactly one PUT of a deterministic 6 MiB (6,291,456 byte) object whose
   every 32-byte block differs (`rgwNativeLargePayload`). The size is an input,
   not an assumption about the layout.
2. Before any manifest or backend oracle, poll the OSD operation history until
   gateway-GlobalID-attributed, TID/PG/pool-consistent `reached_pg` requests
   show a head WRITE/WRITEFULL, at least one tail
   (`<bucket marker>__shadow_...`) WRITE/WRITEFULL and the index
   `bucket_prepare_op` / `bucket_complete_op` calls on an owned index shard.
   The retained snapshot is evaluated again after the manifest is known.
3. Read the pinned `radosgw-admin object manifest` output before DELETE and
   retain logical offsets, stripe sizes and full raw identities. The parser
   requires a bounded, nonoverlapping layout that covers exactly the input
   size with at least two byte-bearing RADOS objects, sequential indexes, the
   head first (or a metadata-only head sentinel with index -1 and size 0,
   retained for cleanup and never used for bytes) and every other object a
   tail of this bucket marker. Any pool other than the discovered data pool,
   namespace, locator, repeated identity, gap, overlap or truncation is
   rejected rather than inferred.
4. Each byte-bearing manifest object must have gateway-attributed WRITE
   reception whose summed extents equal its manifest stripe size. S3 GET
   uses an explicit 8 MiB response allowance for this case only; the default
   1 MiB bound and an overflow/cancellation check remain. Independent
   `rados get` reads of each raw object, concatenated in manifest order, must
   equal the input in bytes and SHA-256, and the key must be in exactly one
   actual index shard. A size sum alone is not accepted.
5. Before DELETE every manifest object is present and no GC chain carries any
   of them. Delete the object once through S3. Require gateway-attributed
   head `CALL rgw.obj_remove` and a gateway-attributed `call` on a `gc.<N>`
   shard object in the discovered log pool (class and method are recorded,
   not assumed), then HTTP 404, head absence and index-key absence while
   every tail still exists.
6. Require each tail to be carried by exactly one chain of `gc list
   --include-all`, with the data pool and no locator or instance, and no chain
   object of this bucket outside the manifest. Run `gc process --include-all`
   exactly once (it processes the whole store queue, which is acceptable only
   because the fixture is wholly disposable), then, with read-only polling for
   30 seconds and no second process action, require those exact raw objects
   absent from the pool and absent from the queue.
7. Remove the empty bucket and owned user, terminate the cluster and require
   independent NotFound results for every owned container, volume and network.

The observer's own `rados` and `radosgw-admin` requests come from different
client instances and are not counted as gateway reception. `radosgw-admin gc
process` removes the tails as its own client, so tail removal is established by
independent absence, not by gateway attribution. A `reached_pg` entry
establishes reception only, not a result code or durability. No restart is
repeated in this checkpoint, and MGR/OSD faults, multipart and the trusted
selector remain open.

### Running and retaining the evidence

The `RGW native fixture run` workflow (`.github/workflows/rgw-native.yml`) is a
development runner, not the trusted `/ci run` catalog. It runs on manual
dispatch and for pull requests that touch the RGW fixture files, the gate tool
or the workflow. It records the tested commit, proves that the checkout's file
hash manifest equals the immutable `git archive` of the same commit, checks
that every Go file of the fixture module is tracked, runs the selected test
under an `flock` and `go test -json`, applies `tools/rgw_native_gate.py` to the
test2json stream and checks that no container or tree change remains. The gate
requires exactly one PASS of the selected test, a package PASS, no failed or
skipped test, the independent cleanup record and the test's own receipt, and it
recomputes the deterministic payload hash and the layout arithmetic. The same
command shape works on an authorized Linux runner:

```sh
CEPH_MSGR_TEST_RGW_NATIVE=1 go -C integration/fixture test -json \
  -run '^TestRGWNativeMultiStripeIntegration$' -count=1 -timeout=20m . \
  > rgw-multistripe.jsonl
python3 tools/rgw_native_gate.py --jsonl rgw-multistripe.jsonl \
  --test TestRGWNativeMultiStripeIntegration --output gate.json
```

Native results for a specific commit are recorded in the pull request, not in
this file, so the recorded commit is the tested one.

## Selected run and first failures

The final run used Go 1.26.9 and the exact image/source above. S3 and native
head readback both matched all 1,031 input bytes before and after restart:
SHA-256 `f5ae8589d38de5ff5e7d3214338b062b60e477067a16e3efdf26ae28cbfa3eab`.
Gateway `client.4164` sent index prepare TID 799, head WRITEFULL TID 800
and index complete TID 801 at epoch 20 (index pool 6 / data pool 7).
After restart, `client.4233` sent head `CALL rgw.obj_remove` TID 774 at
epoch 21. The before/after nonce sets were disjoint. The published HTTP
endpoint changed and was re-resolved from the same owned container.

The selected key was in exactly one actual index shard before/after restart.
After S3 deletion, HTTP 404, native head absence and index-key absence passed;
the empty bucket and owned user were removed. Independent Docker inspection
returned NotFound for all four owned containers, the named run volume and
the fixture network. A final container inventory was empty.

| Native attempt | Tested source | Result / elapsed | Observed boundary and correction |
| --- | --- | --- | --- |
| 1 | `93905d5555d984c901c166e42b7bd9b95b5da4de` | FAIL / 24.17 s | Gateway identity assertion failed before object PUT; fixture termination completed. |
| 2 | `a6754baceaca220305261f7cb9d14e70ef827e43` | FAIL / 25.10 s | Diagnostic retained a non-JSON CLI text reply. Fixed-name `radosclient` selection was replaced with PID-1-owned socket/catalog discovery; pinned RGW names can include nonce suffixes. |
| 3 | `6c71686f71fed0df7d67694d1f0e0d0bfb18fc19` | FAIL / 31.05 s | Immediate history contained index initialization only. This did not prove no PUT request was sent. Process identities are sampled after input; the final test polls delayed history publication with a deadline. The available first-failure record does not distinguish those visibility causes. |
| 4 | `5faa52e8a06fa8fdd2c34c04a81fba9f450802bb` | FAIL / 35.13 s | PUT reception and native byte/index checks passed; the first post-restart GET had a transport error. The test retained the old published endpoint. It now queries the owned container's current endpoint; no errno was retained for this earlier generic error, so a later pass is not a complete root-cause explanation. |
| 5 | `985bb9c3b8f544b9361694295cd60879934c57b3` | FAIL / 35.89 s | Restart readback passed; a mistaken direct-DELETE-opcode expectation failed. Pinned RGW uses `CALL rgw.obj_remove`; the test now requires that actual received method. |
| 6 | `54f991d1dac37da156d5792d7c8d534c2b12467e` | PASS / 36.17 s | Complete initial small-object/restart/delete checkpoint and independent resource removal. |
| 7 | `de413709b3ac898b1c7af2edc4697e159f9e4288` | PASS / 38.08 s | Adds bounded read-only history observation and a cancellation/publication regression. |
| 8 | `bdbd655e9f3c1373ed7a08ab98641f08d80a129a` | PASS / 38.02 s | Independent review correction binds PG/hobject pools, head/index roles and complete class-method tokens; selected public receipt. |

Every native attempt used a new disposable fixture; uncertain S3 mutations
were not resubmitted to an existing bucket/session. All attempts reported
fixture termination without cleanup errors. The final selected run additionally
records independent NotFound counts. Skipped native tests are not passes.

At the final tested source, Linux fixture `vet`, all fixture unit tests
(2.745 seconds) and the focused RGW history/S3/catalog/PUT-admission race
tests (1.178 seconds) passed. The regression checks reject mismatched
client/TID/PG/pool/OID, index CALLs on a head object, wrong method suffixes,
missing PG reception, malformed histories and redirected mutation replay.
The final PR's ordinary CI is not a substitute for this native receipt.

## Provenance

The released provider is MIT-licensed. The upstream native RGW receipt
(`ceph-testcontainers-go`'s `TestRGWRadosReceipt`) informed the small-head and
actual-index-shard recipe. This checkpoint independently adds native request
attribution and gateway restart; it does not import upstream fixture source.
Pinned Ceph sources are source-analysis references under their LGPL-2.1
notices; no Ceph implementation source or binary fixture is copied:

- `src/librados/RadosClient.cc`: authenticated GlobalID becomes the
  `radosclient` Messenger's client identity.
- `src/msg/async/AsyncMessenger.cc`: local Messenger identity/nonce dump.
- `src/osd/OpRequest.cc` and `src/messages/MOSDOp.h`: source client, TID,
  request-vector description and PG event observation.
- `src/rgw/driver/rados/rgw_rados.cc`, `src/cls/rgw/cls_rgw_client.cc` and
  `src/cls/rgw/cls_rgw_const.h`: head write, index prepare/complete and native
  class/method naming. The received history, rather than these source
  references alone, gates the native CALL claim.
