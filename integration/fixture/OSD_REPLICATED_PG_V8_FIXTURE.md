# Explicit replicated PG v8 fixture

`CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_PROFILE=replicated-pg-v8` selects an
opt-in fixture for the explicit replicated-PG request profile. The default is
`legacy-v6`; it retains its one-PG, no-HASHPSPOOL bootstrap and legacy CRUSH map.

The v8 fixture creates an eight-PG replicated data pool with HASHPSPOOL enabled.
It sets the same global pool default before daemon startup, so automatically
created `.mgr` pools use HASHPSPOOL too. It keeps the controlled legacy CRUSH map,
disables automatic CRUSH updates and turns the balancer off. It supports the
existing one-OSD MemStore/BlueStore and three-OSD BlueStore fixture selections;
this does not create an erasure-coded fixture or compute object placement in Go.

The selected profile is passed to both native preparation and the Go test
runtime. Native `osd-oracle.json` records `profile`, `request_version`,
`pool_pg_num`, the original raw object hash, the actual PG seed and PG string,
acting/up OSDs, primary identity and endpoint. Native observations must show all
pools' numeric and named HASHPSPOOL flags and exactly eight data-pool PGs. The
caller uses the observed PG and target; no hash-to-PG or CRUSH calculation is
part of the product.

The native observer permits `pg_upmap` and `pg_upmap_items` only under this opt-in
profile and preserves their native metadata. It still rejects
`pg_upmap_primaries` (which needs a separate SERVER_REEF feature contract), newer
CRUSH buckets, choose args and nonlegacy tunables. In particular, Tentacle's
ordinary default CRUSH configuration is outside this fixture: CRUSH_TUNABLES5
collides with the NEW_OSDOPREPLY_ENCODING bit and would require a separately
implemented and verified reply profile.

The intended narrow negotiated additions are OSDHASHPSPOOL (bit 30),
NEW_OSDOP_ENCODING (bit 56), and the incarnation-2 bit-21 mask used for
RESEND_ON_SPLIT / OSDMAP_PG_UPMAP. Fixture preparation alone does not establish
that a Go session negotiated those bits or passed native communication. Actual
profile runs must retain their request/reply and oracle evidence separately.


## Separate reply-v8 fixture selection

`CEPH_MSGR_TEST_OSD_PROFILE=replicated-pg-reply-v8` is a second explicit
selection with the same eight-PG, HASHPSPOOL, controlled legacy CRUSH and native
placement recipe. The original `replicated-pg-v8` and default `legacy-v6`
selections retain their behavior. Native preparation and tests preserve the
selected string, and the native oracle refuses a manifest/profile mismatch or
an unrecognized alias. The mutating backoff, split and filesystem-map oracles
admit only those two matching modern profiles.

Native JSON continues to record the selected request-v8 profile and raw/actual
PG metadata. It does not add an observed reply-version field: that requires the
product's separate negotiated decoder contract and actual message evidence.
Fixture admission alone does not establish reply-v8 daemon compatibility.

## Reply-v8 first bootstrap attempts: retained setup failure

The first `replicated-pg-reply-v8` CRC/AES IPv4 BlueStore and secure/AES256K
IPv6 MemStore attempts stopped during `osd-bootstrap`. Their native clients
started, but `osd_oracle.py` tried to read `/out/fixture-manifest.json` before
provider startup returned and the Go runner's `prepareEvidence` created that
file. Both native oracle logs retain the resulting `FileNotFoundError`;
neither run reached a Go test suite. These are fixture preparation failures,
not reply-v8 request/reply interoperability results.

The read-only baseline collector now accepts manifest absence during that
bootstrap phase. It still validates the exact closed environment profile and
controlled native topology; when a manifest already exists, a mismatched or
invalid manifest is rejected. The later mutating backoff, split and MDS-map
controls continue to require an existing manifest and matching actual oracle
metadata under one of their two explicit modern profiles. Missing-manifest
bootstrap tests cover all three profile selections, and mismatched present
manifests, unknown selections and unsupported topology remain rejection cases.
The correction subsequently completed native bootstrap in both separately
retained `*-final` runs. Their thirteen existing modern-profile leaves passed,
but both overall runs failed on an incorrect version expectation in the first
new reply-v8 leaf. Separate corrected `*-focused` runs then passed the three new
leaves; these results are not a single combined sixteen-case pass. The
[reply-v8 validation record](OSD_REPLY_V8_VALIDATION.md) distinguishes the first
setup failures, the failed overall runs with passing existing leaves, and the
separate focused successes.

Original evidence is unchanged:

| Attempt | Runner log SHA-256 |
| --- | --- |
| CRC/AES, IPv4, single BlueStore | `f481b9f34bb32fcded54aa69981cbbf76837f0b2ca21f431cf87f7fa275a006b` |
| Secure/AES256K, IPv6, single MemStore | `c83d810128796606f0d5ce2ac1081357f6131386a2a44ffc085e6fad0e7ae228` |

Each directory's `native-oracles.log` has SHA-256
`ab4440e5cd8c83d31449df7ca4efe4763af123330de7f9c070042dd9844cab49`, and
`native-oracles.error` has SHA-256
`e1bc1d01a79fc129a5e856b825c0d25bc45c1180d02e68fcca24e535d25a307c`.
The runner logs are sibling files with the same directory names and `.log`
suffix. Each log records termination of all eight distinct containers in its
own ownership receipt. Later successful attempts must retain separate evidence.
