# Actual linked fixture provider identity — 2026-10-16

The fixture previously hardcoded provider_version as v0.2.0 although its
module dependency had advanced to v0.3.0. Historical manifests remain intact;
that label is not retroactively corrected or treated as linked-module proof.

Future manifests derive provider metadata from runtime/debug.ReadBuildInfo
of the running controller. provider_original_module preserves the selected
module path/version/sum; provider_effective_module describes the actual
replacement, when present. provider_version describes only effective code.
A local replacement, development module, unavailable build info or missing
dependency emits an empty version and an explicit provider_identity_status,
rather than presenting the selected release as the running code. Image,
architecture, daemon version and module identity remain distinct evidence.

Eight injected build-info cases cover ordinary dependencies, versioned and
local replacements, development/missing versions, missing dependency and
missing build info. The fixture's complete CGO-disabled tests and vet pass.
An actual CGO-disabled controller build was inspected with go version -m:
it links github.com/JSYoo5B/ceph-testcontainers-go v0.3.0, checksum
h1:LRXc5mVpqw+/xDRWsv+m8ouLzkVDtB385RoQZ/7ygOY=.

The secure snapshot-checksum run independently verified the running
controller's manifest: provider_version v0.3.0, provider_identity_status
module, and matching original/effective module paths, versions and sums.
It passed 39 snapshot and 24 head cases and terminated all eight owned
containers. This is runtime evidence separate from the injected test cases
and standalone build-info audit. Its original manifest is retained at
`/private/tmp/ceph-snapshot-checksum-secure-aes256k-ipv6-20261016-first/fixture-manifest.json`,
SHA-256 `052cfcbcdbfef17d5f1ddc3ebc7bde874b8b31155919c1da0ad6458080a1d732`. This change adds no product
module dependency or native runtime requirement.

Frozen controller source SHA-256:
493546b23e68f0f07022a46711cd03fe929a0ba6aa02c3ddc19ad469b2f9eabb.
Frozen identity helper SHA-256:
6161871d3a5d421d1ccf7552be4d01c5ed0af7253c22ef23f51edd60fa4cad5f.
