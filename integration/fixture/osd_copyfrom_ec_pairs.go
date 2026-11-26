package main

// The EC-to-EC copy tests of other width pairs and of snapshot sources lost
// between chunks use the existing owned-pool and daemon controls. Keep their
// post-cleanup public cell receipts in native diagnostics.
var _ = collectDiagnosticFiles(
	"osd-erasure-copyfrom-k3m1-to-k2m2-results.json",
	"osd-erasure-copyfrom-k3m1-to-k2m2-loss-results.json",
	"osd-erasure-copyfrom-k4m2-to-k3m1-results.json",
	"osd-erasure-copyfrom-k4m2-to-k3m1-loss-results.json",
	"osd-erasure-copyfrom-snapshot-loss-results.json",
)
