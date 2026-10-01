# ceph-msgr-go

Native Go Ceph Messenger client targeting Tentacle (20.2) and later verified
releases. Initial scope is MON/MGR management commands. Object I/O is deferred.
The product uses no CGO, Ceph libraries, or Ceph CLI subprocesses.

The wire reference is Ceph `v20.2.4`, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.
See [SPEC.md](SPEC.md) for scope and compatibility policy.

## Development

Go 1.24 or later. Run `go test -race ./...` and `go vet ./...`.
Tests and fixtures do not constitute real-cluster interoperability certification.

## Source provenance

Protocol definitions are checked against Ceph's pinned source and
[Messenger documentation](https://docs.ceph.com/en/tentacle/dev/msgr2/).
Ceph source files carry their own license notices, including LGPL-2.1.
Reference sources are not vendored or linked into the Go runtime.
Fixture provenance and generation instructions are recorded with each fixture.
