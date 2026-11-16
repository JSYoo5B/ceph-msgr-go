# Fixture sharing classification

Context: issue #178, part of #171. Before several catalog rows can run on one
fixture, each test must be known not to leave state that a later test could
depend on or be hurt by. This document defines the classification and where
it is declared. It runs nothing on a shared fixture: the default and periodic
coverage still start one fixture per row.

## Declaration

Every native test (`TestCeph...Integration` in `integration` and `cephmsgr`)
declares its value beside its function, as the last line of its doc comment:

```go
// TestCephExampleIntegration reads the OSDMap.
//
//fixture:shareable reads MON maps and command descriptions only
func TestCephExampleIntegration(t *testing.T) {
```

The value is `shareable` or `isolated`. The reason is one line of printable
ASCII, at most 120 bytes and without a final period. It names what the test
changes (daemons, cluster flags, configuration, owned pools, MDS state) and
what its cleanup proves.

A test is `shareable` only if, when it runs, it changes nothing outside the
pools, objects, images or filesystem entries it creates and removes again
with a checked cleanup, or its cleanup already proves the start state: it
restores the change with a checked command and, for a daemon it stopped,
waits for the state the change disturbed (a settled full MON quorum, every
OSD up and in with every PG `active+clean` and no pg_temp). Everything else is
`isolated`, including a test that kills, stops or pauses a daemon without
such a proof, runs `mgr fail`, leaves a configuration entry, a CRUSH rule, a
blocklist entry, a pool flag or a self-managed snapshot behind, or rewrites
the fixture's own object without verifying it. A test is classified by what
it does when it runs; its skip conditions do not matter. The
[start-state check](FIXTURE_START_STATE.md) is the gate between tests; the
classification decides which tests are offered to it.

## Rows

A row's fixture configuration is its parameters without `test_run`. A row is
`shareable` only when every test its effective selector selects in either
suite is `shareable`. The effective selector is the row's `test_run`, or the
fixture's own default selection when the row has none (`loadProfile`). A
broad row, whose default selects the whole suite, is therefore `isolated` as
long as any native test is.

The fixture derives the row values: `TestCatalogRowsDeclareFixtureSharing`
in [`sharing_test.go`](sharing_test.go) reads the declarations with
`go/parser`, passes every row through the fixture's `loadProfile` as the
native workflow forwards it, and reports

- a native test without a declaration, with two, or with an invalid value or
  reason;
- a `//fixture:` comment that is not on a native test's doc comment;
- a row the fixture refuses, a row whose selector selects no native test and
  a row that selects a test without a valid declaration (that row has no
  value).

`python3 tools/scenario_plan.py validate` runs it and fails on every problem
and on every row without a value. `python3 tools/scenario_plan.py sharing`
prints each configuration with its shareable and isolated rows and the job
count of the default coverage if each configuration's shareable rows shared
one fixture. Both need the fixture module's Go toolchain.

Adding a native test adds its declaration in its own file. No shared list
changes.

## Findings at classification

The first classification covered 255 native tests and 130 rows: 103 tests
and 24 rows are `shareable`. The 130 rows have 101 configurations; six
configurations have more than one shareable row, so the default coverage
would need 122 jobs instead of 130.

Two leftovers of the shared owned-pool cleanup (`mode_cleanup` in
[`tools/osd_pool_oracle.py`](../../tools/osd_pool_oracle.py)) make every test
that removes its owned pools through it `isolated`, including every
four-OSD erasure-capacity control:

- it sets `mon_allow_pool_delete=true` in the MON section of the central
  configuration and never removes it. The fixture starts without that entry
  ([`osd_cache_pools.go`](osd_cache_pools.go) restores its own window), so
  the start-state `config` condition fails afterwards;
- an erasure-coded owned pool is created without a rule name, so the MON
  creates a CRUSH rule named after the pool, and nothing removes it; the
  `crush-rule` condition fails afterwards.

The outage controls' own proofs are recorded in their reasons. The
`osd-ready clean` wait covers OSDs up and in and every PG `active+clean`, but
not pg_temp or the original placement.
