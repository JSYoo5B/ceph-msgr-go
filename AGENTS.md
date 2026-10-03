# Project instructions

## Product scope

- Implement the Ceph Messenger client in native Go, independently of OS-specific Ceph installations.
- Product builds, runtime, and transitive dependencies must not require CGO, go-ceph, librados, Ceph CLI subprocesses, or other native libraries.
- The minimum supported Ceph release family is Tentacle (20.2). Do not add support for earlier release families.
- Use the current Tentacle specification. Reject requests to preserve older public APIs when APIs change. Do not add compatibility wrappers, deprecated aliases, or legacy behavior for that purpose unless the user explicitly changes this policy.
- This repository implements the communication layer. Initial work targets raw MON/MGR command transport, authentication, wire message/map codecs, subscriptions, session handling, and recovery.
- Actual use belongs to a separate layer and repository: command construction and schema/result interpretation, text keyring/configuration loading, typed management operations, module/configuration policy, and operational workflows are outside the product scope. Keep raw command input/output and decoded wire metadata available to that layer.
- Object I/O, OSD data connections, CRUSH placement, RBD, and CephFS are later work. Do not introduce them speculatively.

## Design and verification

- Read `SPEC.md` for the reference Ceph tag, technical scope, and verification requirements. Distinguish user decisions from implementation proposals and unverified support.
- Use Go-friendly APIs with per-operation contexts. Cancellation of a local wait does not imply remote cancellation or rollback.
- Preserve raw command output, server error codes, and unknown execution outcomes. Do not automatically re-execute uncertain mutation commands in a new session.
- Never advertise unimplemented protocol features or silently downgrade authentication or connection security.
- MON admission is an explicit narrow exception for CRUSH-generation feature bits: Tentacle MON requires them even from command-only clients. Keep those bits confined to MON sessions, document their admission meaning, and never treat them as permission to subscribe to OSDMap or claim CRUSH/object I/O support.
- Distinguish Messenger framing, CephX, message encoding, and command schema changes. Translate wire semantics rather than C++ implementation details.
- Current Tentacle authentication includes the new `aes256k` key type. Legacy-key-only testing does not prove current authentication support.
- Claim compatibility only for configurations actually verified. A Messenger feature bit does not establish the Ceph release family.
- Ceph fixtures, CLI tools, and containers may be used for development and independent interoperability tests; they must not become product dependencies.
- Development tests may construct commands, interpret responses, and change disposable cluster configuration to verify communication. Keep these consumer helpers in test-only files, without exporting them as product APIs.
- Test actual Ceph interoperability, authentication renewal, MON/MGR failover, concurrency, cancellation, and shutdown. Do not rely solely on self-generated encoder/decoder round trips.
- Keep source provenance and check the licenses of any code or fixtures incorporated into this project.
