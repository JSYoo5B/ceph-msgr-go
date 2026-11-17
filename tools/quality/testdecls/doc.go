// Command testdecls prints the top-level function declarations of the test
// files that the Go build context selects for each given package directory.
//
// The repository's Python inventory checks use it to tie a test name to the
// file that declares it; they take the set of runnable tests from
// `go test -list`, never from this command. See tools/quality/README.md.
package main
