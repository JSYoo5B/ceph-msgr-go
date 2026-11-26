package main

// Preserve native workload identities and the exact opaque metric reports.
var _ = collectDiagnosticFiles("mgr-perf-false.json", "mgr-perf-true.json")
