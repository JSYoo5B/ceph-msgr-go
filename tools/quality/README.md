# Development quality analyzers

This independent, standard-library-only Go module keeps source analysis and
subprocess execution out of the product module and dependency graph. It requires
Go 1.24. `structmetrics` measures concrete Go owners and package size;
`lintcalibration` records and pairs complexity, function length and duplication
findings from golangci-lint v2.15.0.

The analyzers and their tests are adapted from ceph-testcontainers-go commit
`2d4243d1e04167ff570f582a899b91a58871b61f`, originally in
`internal/tools/structmetrics` and `internal/tools/lintcalibration`, under MIT.
See `LICENSE` and the repository's `THIRD_PARTY_NOTICES.md`.

Run the tests with `go -C tools/quality test ./...` from the repository root.
The collector builds both executables once and uses the same tools and embedded
lint configuration for both snapshots. See `.github/QUALITY.md` for report
scope, trust boundaries and commands. The module has no third-party imports;
the external linter is installed separately in CI.
