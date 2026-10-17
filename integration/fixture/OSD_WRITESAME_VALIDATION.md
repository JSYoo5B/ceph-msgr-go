# Bounded WRITESAME verification

Prepared 2026-10-16. Product/source checkpoint before actual daemon execution:
`8d95553e1ecb604c2ab61dd1ce8339aef0d32013`, based on main
`b64714d25edc94b2dd7a6c71ff4608ebd7b2f839`. Native builder proof predates this
rebase but its C++ wrapper, launcher, shared driver and oracle binary sources
are unchanged. No historical native proof was regenerated or rewritten.

Reference Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964` and
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

Five independent native v6 vectors were directly encoded from fresh native
objects and decoded by separate native objects. The new suite contains one
ordered request and logical success/refusal/FAILOK/zero-length replies. The
logical success/FAILOK/zero inputs explicitly carry ACK only: public Go
consumers correctly retain those receipts as unknown outcomes because ONDISK
is absent. They establish wire encoding, not live daemon completion. The first
consumer test expected those ACK-only vectors to complete and failed; its
observed stdout is retained at
`/private/tmp/writesame-public-native-first-ack-only-expectation-failure-20261016.log`.
The expectation was corrected without changing a native byte or proof.

Native build first attempt passed; first builder
`031699aa3d49b001e1268d681613e15377d3db41ee54e8b244962b6223313c88`
was terminated. Build receipts:
`/private/tmp/writesame-native-proof-20261016-first.json` and its adjacent log;
retained five-vector proof `internal/osd/testdata/writesame-native-provenance.json`.
Separate C++ oracle `writesame-oracle` SHA-256:
`d119b00f3f9b27bc736db0416954479c5c1c934f4ee6feb9999fe21da4603440`.
Its pinned source/header compile and matching image librados loader audit
passed; the C++ compound public API supplies the native aggregate and state
observations. It exposes no individual WRITESAME callback; every live input
retains raw_writesame_per_op_result_available=false. Native client routing or
retries are not Go routing or replay support.

Product semantics and safety are in `internal/osd/WRITESAME_CONTRACT.md`.
Both actual wire input and cumulative logical WRITE/WRITEFULL/APPEND/WRITESAME
work are bounded independently. WRITESAME's offset-zero positive mutation
makes every following positive offset-zero READ/CMPEXT unsafe in the same
vector: truncate_seq can clip its nested WRITE below the requested logical
length before setting a full-object digest. The local boundary is one byte,
preserved across FAILOK and staged TRUNCATE. The echoed original Length is
never labeled actual bytes written. Product runs remain pure Go/CGO-disabled.

Fresh native v8 WRITESAME combination vectors are not claimed: v8 composition
is covered by unit/public tests against the independently proven existing v8
message base. Actual daemon profiles and ONDISK receipts are recorded below. No EC,
automatic placement/retry, hardware power
loss, or broad release/CI completion is claimed.


## First daemon attempt and corrected fixture preparation

The first CRC/AES IPv4 host, 3-BlueStore, explicitly legacy-v6 run at source
8d95553 failed: 18/19 subcases passed, then the last clipping-preparation
assertion failed (actual observed size 26, test expected 6). Its original
stdout, all 19 raw records, before/after state and owned cleanup remain in
`/private/tmp/writesame-crc-3blue-legacy-v6-20261016-first` and adjacent log.
The last record's Go and native WRITESAME returned 0 with identical full
bytes, attributes and explicit mtime. This does not make that run a 19-case
PASS: its fixture state was wrong for the intended clipping observation.

Both clipping preparations reused truncate_seq=2. The server applies a
sequenced TRUNCATE only when its sequence is newer; the second preparation
returned code 0 while leaving the 26-byte seeded object. The test had inferred
six bytes without checking the observed state. The corrected test uses a
strictly increasing sequence (case index+2) and confirms six bytes on both
objects before native or Go WRITESAME executes. This changes only consumer
fixture preparation; product codec, wire C++ wrapper, launcher and independent
native client are unchanged. Corrected source checkpoint:
`667094cc977d8ffde742816f43c4886c2dd2f130`.
Acceptance SHA-256:
`31a1d355536b2b5334d4fb5636b71a39e38b6a40730388ab6fac4f0bc8d8bb4a`.
Focused CGO-disabled and race-instrumented integration compilation passed;
without the opt-in marker those runs skip daemon execution.

The first profile observed zero-length behavior directly: for both offset
zero and past-end offsets, existing objects kept their bytes, attributes,
mtime and own version; missing objects stayed absent with version zero.
Those are recorded server observations, not assumptions used to validate
another profile. Actual successful mutation receipts require ONDISK even
when the zero-length operation has no storage effect.


## Final live results and limits

Both successful runs used the corrected frozen source
`667094cc977d8ffde742816f43c4886c2dd2f130` and actual provider v0.3.0. Both
explicitly selected legacy-v6, whose fixture creates the target pool with one
PG and no HASHPSPOOL (fixture/control.go:1302-1306); subject/mirror native
placement was rechecked before the two clipping preparations. v8/8-PG live
acceptance is not claimed. The product's existing explicit v8 codec and public
transport composition tests passed separately.

| Run | Profile | Cases / test time | Result |
| --- | --- | --- | --- |
| CRC first | CRC/AES key and service, IPv4, host, 3 BlueStore replicas | 18/19 passed, 13.88 s | Failed fixture-preparation expectation; preserved |
| CRC second | Same profile, corrected preparation | 19/19, 13.97 s | PASS, exit 0 |
| Secure first | Secure/AES256K key and service, IPv6, container, single MemStore | 19/19, 12.53 s | PASS, exit 0 |

Every successful Go mutation vector returned ONDISK. This is a server
completion report, not hardware power-loss proof; MemStore has no restart
persistence. The native C++ helper reports aggregate results and the C API
independently observes full bytes, attributes, stat2 and each object's own
version. Native per-op WRITESAME callbacks remain unavailable and were not
invented. Go preserves signed aggregate/per-op values, TID/global identity,
request/received epochs, retry attempt, raw front/data and pattern-length union.

Examples observed in both successful profiles:

- Binary pattern replacement, partial range, hole extension, missing-object
  creation and ordered append/metadata effects matched the native twin.
- Zero-length at offset zero and past end left existing bytes, attributes,
  mtime and own version unchanged; missing objects stayed absent/version zero.
- WRITESAME followed by a false xattr guard returned aggregate -125 and Go
  results `[0,-125,0,0]`, preserving all original state and own version.
- An offset of 2^32 with a tiny pattern returned -27 with Go per-op -27 and
  no state change. With WRITESAME FAILOK the aggregate became 0, Go results
  `[-27,0,0,0]`, and later append/SET/RM committed identically to the native twin.
- A real self-managed write SnapContext preserved the original clone while
  changing the head. This new combined test did not set ORDERSNAP; existing
  independent ORDERSNAP evidence is not relabeled as a new WRITESAME run.
- Sequenced TRUNCATE prepared both objects at six bytes using Go's already
  independently encoded truncate fields. The public native C++ WRITESAME and
  Go WRITESAME then kept six actual bytes but echoed logical Length 12 and
  pattern length 3, both for offset-zero and offset-three cases. Preparatory
  Go raw replies are separate `go_truncate_preparation` records, not invented
  independent native TRUNCATE executions.

Retained local artifacts (each directory has an adjacent original `.log`):

- `/private/tmp/writesame-crc-3blue-legacy-v6-20261016-first`
- `/private/tmp/writesame-crc-3blue-legacy-v6-20261016-second`
- `/private/tmp/writesame-secure-aes256k-ipv6-memstore-legacy-v6-20261016-first`

Each contains `osd-writesame-results.json` (19 raw case records), native oracle
input/result/provenance, actual fixture manifest, pool policy and ownership.
Every run removed both owned objects and one self-managed snapshot (ID 2).
CRC runs terminated 10 owned containers each; secure terminated eight. A
separate Docker inventory audit confirmed all 29 owned CIDs (builder plus
all three fixtures) and all three fixture networks absent:
`/private/tmp/writesame-owned-cleanup-audit-20261016.json`.

Live source SHA-256 (unchanged during the two successful runs):

- `internal/osd/writesame.go`: `9f8ae47579c0f997fed6992c1817ee2f7c6ed836eff86c598ef8605b0b132cc0`
- `internal/osd/operations.go`: `4258d5f8e2c9068d4af65e45eb06ed77c5601e835f5e169930ae844fcf4e5e90`
- `cephmsgr/osd_request.go`: `18d1f0f477d28a7932e37b9a34de5c4af5f343f1330d0c80ec44b56873e906e0`
- `integration/osd_writesame_test.go`: `31a1d355536b2b5334d4fb5636b71a39e38b6a40730388ab6fac4f0bc8d8bb4a`
- `tools/osd_writesame_oracle.py`: `88ee88ae22c39beef3ec6f6cadb200acbe69eb2cdcd579fca051eeb71060555b`
- `tools/osd_writesame_oracle.cc`: `74e9b5661ba5de6780454035fae46394a43cff9249e4871fbe630c73d15731e4`

After live execution only documentation/catalog and the public input comment
were clarified; no runtime behavior or native proof changed. Final local
validation includes whole CGO-disabled product/fixture tests and vet, focused
normal/race WRITESAME tests, and five closed Python oracle tests. These local
runs do not claim an exact-final-main CI pass or a new release.

## Reproduction

Build the fixture controller from its separate module and the native encoder
with its explicit pinned source/image proof. Set the WRITESAME oracle path to
the resulting `writesame-oracle` binary with its adjacent `.proof.json`; the
fixture rejects mismatched image, source, binary or wrapper hashes. For actual
acceptance select `CEPH_MSGR_TEST_RUN=^TestCephOSDWriteSameIntegration$`,
`CEPH_MSGR_TEST_OSD=1`, `CEPH_MSGR_TEST_OSD_PROFILE=legacy-v6` and
`CEPH_MSGR_TEST_WRITESAME_ORACLE=/path/to/writesame-oracle`.
The CRC profile additionally selects key/service `aes`, connection `crc`, IP
family `4`, runtime `host`, store `bluestore`, replicas `3`. The secure profile
selects key/service `aes256k`, connection `secure`, IP family `6`, runtime
`container`, store `memstore`, replicas `1`. Each selected value is explicit
`CEPH_MSGR_TEST_*` fixture configuration and is retained in the manifest.
Run the controller's `run -project PATH -diagnostics UNIQUE_PATH` sequentially;
keep a distinct diagnostic path and stdout log for every attempt.

## Primary SIGKILL

2026-10-17, `TestCephOSDWriteSameFailoverIntegration` (legacy-v6, container,
three BlueStore replicas, min_size 2, default secure CephX/IPv4; one run). A
native-seeded owned object receives an ONDISK Go WRITESAME (offset 2, length 12,
three-byte binary pattern, explicit mtime); native observation matches the
expected bytes, the receipt user version and the mtime. The object's primary is
then SIGKILLed (exit 137, no OOM). Native observation through the surviving
two replicas shows identical bytes, xattr, version and mtime; the refreshed
placement has a newer epoch, a different primary and a two-OSD acting set; a
caller-targeted Go read on the new primary returns the same bytes and version
under a later transaction; a final native observation shows no replay. This is
OSD process termination with surviving replicas, not host power loss, and no
WRITESAME was in flight when the primary died. Other profiles are not claimed:
the shared restoration helper is legacy-v6 only (a replicated-pg-v8 attempt
passed the test body but failed in that helper's fixture restore, so it is not
recorded as a result).
