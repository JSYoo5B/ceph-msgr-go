# Fresh Tentacle OMAP read codec fixtures

Ceph 20.2.4 source `7f793731f1b39eb4f465e960113d2363c311b964`; pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

The first public `encode-osd-omap` SDK run compiled and linked the independently
written C++ wrapper against the official generated headers and matching
libceph-common. Fresh ObjectOperation/MOSDOp/MOSDOpReply objects directly run
`encode_payload`; different native objects decode every emitted payload.
Decoded objects are never encoder inputs. No Go byte buffers, retained decoded
fronts, server traffic or authentication material supply the native encoding.

Five triples have 41-byte native MessageFrame header, front and data:

| Name | Front/data bytes | Logical case |
| --- | --- | --- |
| request-v6-read-all | 325/75 | zero-count keys, binary cursor keys, distinct binary prefix vals, raw BYKEYS set |
| reply-v6-read-mixed | 327/76 | four separate outputs; more=true pages and BYKEYS map without suffix |
| reply-v6-keys-final | 201/18 | final binary-key page, more=false |
| reply-v6-vals-empty | 201/5 | empty final map plus more=false |
| reply-v6-bykeys-empty-value | 201/15 | explicit binary key with empty existing value, no more suffix |

Files use the `omap-native-` prefix. Request incarnation is 1, TID77, epoch11,
pool7/route-key/tenant, raw hash0x11223344 and object `omap-codec`; read mtime is
zero. Ordinary std::string GETVALS overload selection is explicit. Native packed
extent lengths derive from encoded input bytes. Separate native decoders
verify operation unions, flags, input/output bytes and results. Go compares
complete request header/front/data and retains each reply segment/metadata.

Message::encode supplies the native envelope/footer after direct payload
encoding. Native MessageFrame uses explicit offline sequence1/ACK3/priority127;
these are not live ProtocolV2 session/ACK observations. Replies have logical
constructed output, not claims of server filtering or durable storage.
The exact library/binary/wrapper/driver hashes and decoded operations are in
`omap-native-provenance.json`. The owned builder was independently absent
after public termination. Runtime proof/log:
`/tmp/ceph-omap-native-encoder-attempt1-proof.json` and `.json.log`.

```sh
cd integration/fixture
go run . encode-osd-omap -project ../.. \
  -cache /private/tmp/ceph-native-encoder-cache \
  -output /tmp/ceph-omap-native-proof.json
```

The runner supplies all three original wrapper sources when sharing the SDK
cache; the new CMake target is guarded by source existence. Ceph implementations
remain unchanged. Native build libraries are test tooling only. Source headers
and library carry LGPL-2.1 notices; no native dependency enters the Go product.
Live daemon evidence is separately recorded in
[OSD_OMAP_VALIDATION.md](../../../integration/fixture/OSD_OMAP_VALIDATION.md).
