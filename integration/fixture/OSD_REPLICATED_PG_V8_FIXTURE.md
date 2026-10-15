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
