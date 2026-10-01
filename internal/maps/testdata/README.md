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

## IPv6 quorum and manager maps

`monmap-3-ipv6-v20.2.4.bin` and `mgrmap-ipv6-v20.2.4.bin` were generated on
2026-10-01 by the same pinned Ceph image and commit above. The development
cluster used 3 MON and 2 MGR, IPv6 loopback, aes256k, secure Messenger, and
`auth_allow_insecure_global_id_reclaim=false`. Its MGR module path was an
empty directory, so the map contains no upstream module description catalog.
The normal interoperability fixture still tests Ceph's bundled modules.

The MonMap is unmodified `ceph mon getmap -o FILE` output: epoch 1, minimum
release 20, with `a`, `b`, and `c` at `[::1]:33300`, `33301`, and `33302`.
After stopping the MON processes, the manager map was extracted without
re-encoding using:

```sh
ceph-monstore-tool /tmp/ceph-msgr-test/mon.a get mgr -- --out FILE
ceph-monstore-tool /tmp/ceph-msgr-test/mon.a get mgr -- --readable --out JSON
```

The independent readable output gave epoch 4, active name `b`, global ID
4111, available=true, address `[::1]:36801`, nonce 3962530023, and standby
`a` with global ID 4114. The binary is a complete version 14, compatibility
version 6 MgrMap envelope; no source encoder or Go encoder produced it.

| Fixture | Bytes | SHA-256 |
| --- | ---: | --- |
| `monmap-3-ipv6-v20.2.4.bin` | 384 | `febf0460156f42d875335c1349f270d684d33d162b29c3a1b8074cb323eb34d2` |
| `mgrmap-ipv6-v20.2.4.bin` | 1109 | `aabc11827f6ff039104b47cf0305961a4aba916922f92fbc2f09eb0c741a5b2b` |

These fixtures contain public map metadata and no keys or tickets. They are
generated development data, not copies of upstream test fixtures. The
[MgrMap definition](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/MgrMap.h)
declares LGPL-2.1; its C++ implementation is not incorporated into this project.
