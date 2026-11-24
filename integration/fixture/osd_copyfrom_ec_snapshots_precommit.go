package main

// saveMetadata (diagnostics.go) collects the public receipts of the
// erasure-coded snapshot-source and EC-to-EC copy tests
// (integration/osd_erasure_copyfrom_snapshots_test.go), the destination
// losses on the k=3,m=1 and k=4,m=2 widths
// (integration/osd_erasure_copyfrom_sources_test.go) and the replicated
// pre-commit loss (integration/osd_copyfrom_precommit_test.go) when they
// exist.
var _ = collectDiagnosticFiles(
	"osd-copyfrom-precommit-loss-results.json",
	"osd-erasure-copyfrom-chunk-loss-k3m1-results.json",
	"osd-erasure-copyfrom-chunk-loss-k4m2-results.json",
	"osd-erasure-copyfrom-erasure-to-erasure-results.json",
	"osd-erasure-copyfrom-snapshot-source-results.json",
)
