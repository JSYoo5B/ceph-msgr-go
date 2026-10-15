# LIST_WATCHERS native acceptance — 2026-10-15

Product source: `226e02f64930235c73a3f0aabea7efdc705b91a8`. This is the bounded
standalone head-listing subset of O3; watch registration, notification delivery,
PG enumeration and backoff are still separate implementation milestones.

## Implementation and independent wire evidence

`OSDListWatchers` takes a local output budget of at least ten bytes. Results
preserve entity type/number, cookie, registered timeout, address metadata and
raw output. The client transmits no new feature bit and performs no routing,
registration or retry. Current/native compatible envelope versions and local
bounds are described in [WATCHERS_CONTRACT.md](../../internal/osd/WATCHERS_CONTRACT.md).

Five fresh pinned native encoder/decoder vectors cover the request, populated
IPv4/IPv6 list (146 bytes), empty list (10 bytes), ENOENT and EACCES. Native
classes independently encode and decode the watcher-list output itself; Go
request front/data and 41-byte header match the fresh native message. Public
request/result tests also use the native bytes. The header's explicit offline
seq/ACK inputs are not live ProtocolV2 evidence. Refusal vectors are explicit
logical inputs, not an actual daemon permission test.

The shared native builder now supports a selected target. Its first invocation
failed because the new target parameter was shadowed by an existing archive
symlink variable; renaming it fixed the build. Existing allocation-hint fixture
checks correctly rejected their old build-driver hash. Re-executing its native
encoder produced all fifteen header/front/data segments byte-for-byte equal
to the stored vectors; the provenance now records that actual regeneration
while retaining the previous source commentary. Neither failure was a product
wire failure.

Native wire receipts are in `internal/osd/testdata/*watchers-native*`. Their
provenance records source/archive/submodule/config/wrapper/library/binary and
segment hashes, exact image, independent decoder receipts and builder cleanup.

## Actual daemon runs

All runs used Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
Linux arm64 daemons and the fixture provider v0.2.0. This task did not change
or review the provider repository. The product module remains native Go with
no provider, librados, CLI or CGO dependency.

| Profile | Executed cases | Result |
| --- | --- | --- |
| Darwin arm64 host/IPv4 relay, CRC, AES client/service, three BlueStore replicas | Empty head; absent object ENOENT; two live native registrations; native unwatch then empty list; populated output above a 10-byte budget on an independent client while the original connection remains usable | 5/5 pass, no case skipped |
| CGO=0 Linux arm64 container, IPv6, secure, AES256K client/service, single MemStore | Empty head; absent object ENOENT; two live native registrations; native unwatch then empty list | 4/4 pass, no case skipped; the later independent-client budget case was not run on this profile |

The test-only Python librados worker registers two watches with timeouts 37
and 61 on an exclusively created UUID object. The live Go list matches their
cookies, native global ID and timeout; a separate `rados listwatchers` request
matches cookie/global ID and address/nonce. The Go observer's identity is
separate (final IPv4 run: observer 4734, watcher 14618; IPv6: observer 4629,
watcher 14506). Populated live outputs are respectively 134 and 158 bytes.
The difference from the 146-byte offline vector follows from the actual
address encodings. The CLI does not expose timeout; those values are checked
against the native watch3 registration inputs.

One intermediate budget test failed because it called OpenOSD twice with the
same client/OSD ID and assumed a new connection. OpenOSD deliberately returns
the existing handle, so failing its budget also failed the supposedly separate
handle. The corrected test creates an independent client. Its sent listing
fails with the uncertain-call error and preserved raw reply/submission identity,
and the original client immediately reads both watchers successfully. The
product's connection ownership behavior did not change.

Native unwatch completes before owned-object cleanup. Worker status confirms
both registrations disappeared through the native CLI. The fixture runner
then removed its disposable containers. A token-scoped stop file and a five-
minute worker deadline bound cleanup even after a failed consumer test.

Logs/whitelisted diagnostics:

- `/private/tmp/ceph-watchers-crc-aes-first.log` and directory without `.log`: initial four-case IPv4 success.
- `/private/tmp/ceph-watchers-secure-aes256k-ipv6-first.log` and matching directory: four-case IPv6 success.
- `/private/tmp/ceph-watchers-crc-aes-budget.log` and matching directory: intermediate handle-reuse test failure.
- `/private/tmp/ceph-watchers-crc-aes-budget2.log` and matching directory: final five-case IPv4 success.
- `/private/tmp/ceph-watchers-native-proof2.json` and `.log`: native wire proof after fixing the builder variable.
- `/private/tmp/ceph-watchers-alloc-regression-proof.json` and `.log`: unchanged allocation-hint segments from the shared builder.

## Local checks and remaining gaps

CGO=0 product unit tests and vet, nested fixture unit tests and vet,
cephmsgr/internal-osd race tests and all 250 Python tool tests passed. A ten-
second watcher decoder fuzz run completed 3,341,610 executions without failure.
The initial sandboxed unit run failed on forbidden loopback listeners; the
subsequent authorized local run passed those tests. Synthetic tests cover
malformed counts/lengths/versions, trailers, input admission, local budgets,
raw/decoded ownership and cancel/Close without replay.

This is local acceptance, not a claim that the pushed commit passed remote CI.
Live read-only credentials, removal/snapshot races, registration loss/expiry,
watch renewal/reconnect, notifications/ACK/completion, EC and other profiles
remain unverified or unimplemented. O3 as a whole remains incomplete.

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDListWatchersIntegration$' sh integration/run.sh
```
