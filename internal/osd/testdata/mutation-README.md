# Tentacle mutation native decoder fixtures

`mutation-*` front/data bytes are independent Python field encodings accepted
by pinned Tentacle `ceph-dencoder`. They are **native decoder fixtures**, not
native payload encoder output or live OSD captures. Neither a Go encoder nor
a Go decoder produced the reference bytes. These vectors do not prove object
execution, atomicity, persistence, or placement; live native mutation tests
provide separate execution evidence.

The reference generator is `tools/osd_mutation_codec_oracle.py`. A disposable
cluster was created with the public `ceph.Run` API of
`ceph-testcontainers-go v0.2.0`, one MON and no MGR/OSD. The generator was copied
to that owned container and invoked with public `RunNativeCommand`. Each input
was imported by `ceph-dencoder`, decoded, summarized, and re-exported with
feature mask `3026423347916342070`. Native exported front/data bytes had to
match every independently constructed input byte. Container cleanup completed.
`mutation-provenance.json` records image identity, native summaries, commands,
input/export envelope digests, and pinned source hashes/license provenance.

The all-operations request has client incarnation 1, map epoch 11, pool 7,
key `route-key`, namespace `tenant`, raw hash `0x11223344`, object
`mutation-codec`, mtime 1700000000 seconds + 123456789 nanoseconds,
ONDISK/READ/WRITE/RETURNVEC request flags, head object (NOSNAP = -2), empty snap
context, attempt 0, default reqid, and Messenger TID 77/client global ID 123.
Its ordered vector covers create with EXCL, write 2~4 (`00 ff 41 0a`),
write-full (`6e 65 77`), append (`80 21`), truncate to 7, zero 1~2,
assert-version 19, compare-extent 1~2 (`00 ff`) with FAILOK, and delete.
Input data is concatenated in vector order. The assert-version union stores
version in its second 64-bit slot; it is not an extent length.

Replies preserve the packed operation union and flags, contain no output data,
and have the v6 redirect/24-byte trace suffix. They cover synthetic all-op
success, exclusive-create EEXIST (-17), compare-extent mismatch at byte 2
(-4095 - 2 = -4097), and zero-tail normalization into truncate offset 1.
Each reply uses ONDISK, map/version epoch 11, version 20, user version 19,
attempt 0, and the same object name. They intentionally isolate wire decoding;
the all-op vector is not claimed to be executable against a live object.

`Message::encode` only calls `encode_payload` when its decoded payload is empty.
`ceph-dencoder` has no CLI operation to clear a decoded message payload.
Therefore re-export preserves the decoded front and does not independently
run `MOSDOp::encode_payload` or `MOSDOpReply::encode_payload`. True independent
payload-encoder evidence remains an explicit O0 gap. Native librados ordinarily
negotiates newer request/reply encoding branches, so an unqualified native
packet capture would not prove this negotiated v6 branch either.

The cited source files carry LGPL-2.1 notices. The generator independently
translates their serialized field definitions and includes data only; no Ceph
native implementation is copied into this repository. The script is strictly
development tooling and adds no native product dependency.

A native invocation for each vector is:

```sh
ceph-dencoder type MOSDOp import mutation-request-v6-all-input.bin \
  decode dump_json set_features 3026423347916342070 encode \
  export mutation-request-v6-all-native.bin
```

Reply vectors use `type MOSDOpReply`. Run `python3 tools/osd_mutation_codec_oracle.py`
inside the pinned disposable native fixture; it emits bounded JSON containing
validated front/data bytes and provenance, and fails on a different release or
changed native front/data. Offline generator/parser checks run with
`python3 -m unittest discover -s tools -p 'test_osd_mutation_codec_oracle.py'`.
