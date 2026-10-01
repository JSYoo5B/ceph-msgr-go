# Ceph-generated map fixture

`monmap-v20.2.4.bin` is the unmodified output of `ceph mon getmap -o FILE`
from an ephemeral cluster created by `integration/cluster.sh` on 2026-10-01.
The image was `quay.io/ceph/ceph:v20.2.4`, digest
`sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Ceph reported commit `7f793731f1b39eb4f465e960113d2363c311b964`.

Fixture SHA-256:
`76ba70850239a649e2938dc18aa0a3984b55ea4dac97bb621aea4a4b20e61b79`.

The test cluster had one MON (`a`), epoch 1, minimum monitor release 20,
msgr2 address `127.0.0.1:33300/0`, and aes256k authentication policy.
The fixed FSID is test data. This binary map contains addresses and public
cluster metadata, no authentication keys or tickets. It was generated for
this project, not copied from a Ceph source-code fixture.

This is an independent daemon output, unlike the synthetic prefix fixtures
in the Go tests. Later harness changes do not rewrite this captured fixture.
