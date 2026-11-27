package main

// The EC-to-EC copy tests of other width pairs, of snapshot sources lost
// between chunks or in the held final transaction on every pair, and of the
// offline per-shard histories after a lost copy use
// the existing owned-pool, daemon and shard-history controls. Keep their
// post-cleanup public cell receipts in native diagnostics.
var _ = collectDiagnosticFiles(
	"osd-erasure-copyfrom-k3m1-to-k2m2-results.json",
	"osd-erasure-copyfrom-k3m1-to-k2m2-loss-results.json",
	"osd-erasure-copyfrom-k4m2-to-k3m1-results.json",
	"osd-erasure-copyfrom-k4m2-to-k3m1-loss-results.json",
	"osd-erasure-copyfrom-snapshot-loss-results.json",
	"osd-erasure-copyfrom-k3m1-to-k2m2-snapshot-loss-results.json",
	"osd-erasure-copyfrom-k4m2-to-k3m1-snapshot-loss-results.json",
	"osd-erasure-copyfrom-loss-history-results.json",
)
