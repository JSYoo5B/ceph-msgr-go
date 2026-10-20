# Messenger frame test data

`frames.json` contains specification-derived CRC and secure vectors generated
by `generate.py` using independent Python CRC32C and AES-GCM implementations.
These vectors are not captured Ceph traffic.

`ceph-mon-hello-v20.2.4.bin` is an unmodified server banner followed by one
pre-authentication CRC HELLO frame captured from a disposable MON on
2026-10-01. Ceph reported commit
`7f793731f1b39eb4f465e960113d2363c311b964`. The image was
`quay.io/ceph/ceph:v20.2.4`, digest
`sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

The capture is 98 bytes, SHA-256
`54b8b7f18a7ea701787094eb1e9a34b2b5d1c729e6c81dd754afe326681afe8a`.
Python's socket `getsockname()` independently reported the client endpoint
`127.0.0.1:34716`. The MON's HELLO identifies that endpoint with address type
2 and nonce 0. Its banner supports revision 1 and compression, with no
required banner bits. The Go test requires revision 1 regardless.

`capture_hello.py` records the same exchange using Python's standard library
and independently verifies both CRC32C values. The script sends only a
banner. No authentication keys, tickets, connection secrets, or command
output appear in this fixture. It verifies pre-authentication framing;
secure mode is covered by the separate vectors and live interoperability.

The captured bytes are generated development data, not an upstream code
fixture. Ceph's [address definitions](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/msg/msg_types.h)
declare LGPL-2.1; their C++ implementation is not incorporated into this
project. The wire reference is the [Tentacle Messenger specification](https://docs.ceph.com/en/tentacle/dev/msgr2/).
