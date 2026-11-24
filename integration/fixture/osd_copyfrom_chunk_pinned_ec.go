package main

// saveMetadata (diagnostics.go) collects the public receipts of the pinned
// chunk-mutation tests (integration/osd_copyfrom_multichunk_mutations_test.go),
// the erasure-coded destination widths
// (integration/osd_erasure_copyfrom_chunks_test.go) and the erasure-coded
// source and destination-loss tests
// (integration/osd_erasure_copyfrom_sources_test.go) when they exist.
var _ = collectDiagnosticFiles(
	"osd-copyfrom-chunk-pinned-omap-edit-results.json",
	"osd-copyfrom-chunk-pinned-source-removal-results.json",
	"osd-erasure-copyfrom-chunk-loss-results.json",
	"osd-erasure-copyfrom-chunks-k3m1-results.json",
	"osd-erasure-copyfrom-chunks-k4m2-results.json",
	"osd-erasure-copyfrom-source-chunks-results.json",
)
