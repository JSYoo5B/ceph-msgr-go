# Project instructions

## Product scope

- Implement the Ceph Messenger client in native Go, independently of OS-specific Ceph installations.
- Product builds, runtime, and transitive dependencies must not require CGO, go-ceph, librados, Ceph CLI subprocesses, or other native libraries.
- The minimum supported Ceph release family is Tentacle (20.2). Do not add support for earlier release families.
- Use the current Tentacle specification. Reject requests to preserve older public APIs when APIs change. Do not add compatibility wrappers, deprecated aliases, or legacy behavior for that purpose unless the user explicitly changes this policy.
- This repository implements the communication layer: authentication, Messenger framing, raw commands, wire messages/maps, subscriptions, session handling and bounded OSD/MDS requests. Keep supported configurations and incomplete paths explicit in `SPEC.md` and the relevant contracts.
- Actual use belongs to a separate layer and repository: command construction and schema/result interpretation, text keyring/configuration loading, typed management operations, module/configuration policy, and operational workflows are outside the product scope. Keep raw command input/output and decoded wire metadata available to that layer.
- The final test-environment goal is to reproduce all Messenger-based communication and I/O paths of the selected Ceph release, as defined in SPEC.md: MON/MGR/OSD/MDS, object mutations, CephFS/RBD underlying I/O, and real daemon-to-daemon paths. Track future fixture requirements separately from current Go implementation support; a limited successful profile is not the final environment boundary.
- Callers supply OSD placement and routing metadata. Do not turn this repository into an autonomous RADOS client.
- CRUSH placement, automatic object routing and retry policy, and typed storage workflows remain outside the product scope. Wire support does not establish a complete RBD or CephFS client.

## Design and verification

- Read `SPEC.md` for the reference Ceph tag, technical scope, and verification requirements. Distinguish product requirements from implementation proposals and unverified support.
- Use Go-friendly APIs with per-operation contexts. Cancellation of a local wait does not imply remote cancellation or rollback.
- Preserve raw command output, server error codes, and unknown execution outcomes. Do not automatically re-execute uncertain mutation commands in a new session.
- Never advertise unimplemented protocol features or silently downgrade authentication or connection security.
- MON admission is an explicit narrow exception for CRUSH-generation feature bits: Tentacle MON requires them even from command-only clients. Keep those bits confined to MON sessions, document their admission meaning, and never treat them as permission to subscribe to OSDMap or claim CRUSH/object I/O support.
- OSD sessions must independently negotiate only implemented features. CRUSH_V2 (feature bit 36) is the narrow admission exception: MON and OSDs require it from every client once any erasure-code rule exists, so both roles advertise it for admission only. It never claims CRUSH computation or placement. Reject other unsupported OSDMap/CRUSH/pool feature requirements explicitly and state the configurations actually verified.
- Distinguish Messenger framing, CephX, message encoding, and command schema changes. Translate wire semantics rather than C++ implementation details.
- Current Tentacle authentication includes the new `aes256k` key type. Legacy-key-only testing does not prove current authentication support.
- Claim compatibility only for configurations actually verified. A Messenger feature bit does not establish the Ceph release family.
- Ceph fixtures, CLI tools, and containers may be used for development and independent interoperability tests; they must not become product dependencies.
- Development tests may construct commands, interpret responses, and change disposable cluster configuration to verify communication. Keep these consumer helpers in test-only files, without exporting them as product APIs.
- Test actual Ceph interoperability, authentication renewal, MON/MGR failover, concurrency, cancellation, and shutdown. For OSD work, compare replies and object bytes with an independent native client on a disposable fixture, and preserve map epochs, request identity and uncertain outcomes. Do not rely solely on self-generated encoder/decoder round trips.
- Keep source provenance and check the licenses of any code or fixtures incorporated into this project.

## Privacy review

Before publishing source, documentation, fixtures, commit messages, release notes,
pull requests, issues or GitHub discussions, search for private
execution-environment details. Review matches in context, including hidden CI
files and generated reports. These rules apply to internal code as well as
pull request and issue descriptions, reviews and comments.

- Omit actual personal hostnames, private domains and addresses, account-specific
  filesystem paths, remote-access helpers, checkout locations and private log names.
- When describing paths, use only file paths within this repository or related
  repositories. Do not expose unrelated filesystem locations or unnecessary
  arbitrary paths.
- Software versions, architecture and hardware specifications may remain when they
  explain reproducibility, compatibility or performance. Remove their association
  with an identifiable private host or access path.
- Preserve functional platform strings, synthetic test inputs, public CI runner
  information, general product requirements and public author attribution or image
  namespaces. Do not globally replace operating-system or architecture strings.
- When logs or other diagnostic information must be shown as examples, mask
  information unrelated to the point being demonstrated. Retain only the details
  needed to explain or reproduce that point.

## Documentation

- Write documentation, code comments, commit messages, release notes and issue/PR descriptions in English. Preserve intentional non-ASCII test inputs.
- Describe current behavior, supported configurations and explicit limitations. Keep planned work separate from implemented and independently verified behavior.
- Keep `README.md` focused on usage and feature boundaries; put detailed wire contracts beside their implementations and reproducible test recipes beside the fixtures.
- Record protocol references, licensing, test selectors, image versions and relevant observations. Keep private execution details and incidental diagnostic metadata out of examples.
- Match verification claims to the tested source and configuration. Unit tests, native codec parity, live daemon interoperability and CI are distinct evidence. Do not treat a later pass as the explanation of an earlier failure.
- Run substantial checks and Docker integration on an authorized Linux runner.
