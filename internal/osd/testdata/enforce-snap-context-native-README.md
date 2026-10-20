# Fresh native ENFORCE_SNAPC request vectors

[The retained proof](enforce-snap-context-native-provenance.json) contains
12 fresh MOSDOp requests from pinned Ceph 20.2.4 commit
`7f793731f1b39eb4f465e960113d2363c311b964`: v6, v8 and untraced v9,
each with an empty context or `seq=7, snaps=[7,5]`, and ENFORCE_SNAPC
disabled or enabled. Every request is a head WRITEFULL with ORDERSNAP and
four data bytes `00ff410a`. Matched pairs differ only by request flag
`0x100000`; the default feature inputs do not include cache-pool bit 35.

The development-only wrapper constructs a fresh ObjectOperation and MOSDOp
for each vector, calls canonical native `Message::encode` once, and decodes
into a separate fresh object without re-encoding it. The Go comparison checks
the complete header, front, middle and data segments, request identity, map
epoch, locator, snapshot context, retry count, mtime and packed operation
union. It also checks modern backoff admission identity and empty tracing.

The 41-byte Messenger v2 header uses explicit offline sequence 1, TID 77,
ACK sequence 3 and priority 127 through native `MessageFrame::Encode`.
This establishes serialization for those inputs. It does not execute a live
ProtocolV2 session or establish server behavior, placement or durability.
The SDK uses WITH_JAEGER=OFF; v9 carries the invalid empty OTEL envelope.

The builder reuses unchanged MPing SDK helpers and IgnoreCache proof helpers.
The new wrapper and driver, the shared wrapper and both imported drivers are
bound by hashes. Provenance includes the official archive, pinned submodules,
eight unmodified codec sources, generated CMake configuration, compiler,
matching library and executable. The compile database must bind the exact
wrapper path; ELF and loader checks require the owned Linux amd64 library.
An existing output directory is refused to preserve earlier evidence.

The actual owned daemonless builder ran on 2026-10-18 from 07:52:42 to
07:53:52 UTC, exited successfully, and its named container was absent after
cleanup. The proof retains that execution receipt and the complete build
log's SHA-256. All 12 native requests match the Go encoder.

To regenerate inside an owned Linux amd64 development container using the
pinned image recorded in the proof, mount the five tools as siblings and
use a fresh SDK work directory, or a cache without encoded-enforce-snap-context:

```sh
python3 tools/osd_enforce_snap_context_native_encoder.py --build-native \
  --work .artifacts/native-sdk \
  --wrapper tools/osd_enforce_snap_context_native_encoder.cc
```

The last structured JSON line is this proof; the preceding one belongs to
the shared MPing build. Preserve the build log, attach the actual execution
receipt and log hash, and independently verify owned container cleanup.
The product comparison is:

```sh
CGO_ENABLED=0 go test ./internal/osd -run '^TestEnforceSnapContext' -count=1
```

Ceph development headers and libraries carry LGPL-2.1 notices. Only wire
facts and provenance enter the repository; native dependencies do not enter
the Go product's build or runtime.
