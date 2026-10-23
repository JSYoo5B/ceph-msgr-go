# Packet-capture analysis for msgr2.1: research report (issue #48)

This report records a bounded, development-only evaluation of packet-capture
analysis for this project's Messenger implementation. It answers the questions
of issue #48 with evidence from [msgrcap](README.md) and its
[live workflow](../../.github/workflows/msgr-capture-poc.yml). Nothing here
changes product behavior, product dependencies or supported configurations.

## Evidence boundary

| Item | Value |
| --- | --- |
| Reference | Ceph `v20.2.4` (`7f793731f1b39eb4f465e960113d2363c311b964`), image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`; `ceph-mon --version` reported the same commit |
| Accepted live run | Messenger capture PoC run 39887173761 (attempt 1) on tool source `bb52fff` (pull-request merge ref `7aadffc`); both `crc` and `secure` jobs passed their expectation gate. Artifacts `msgrcap-crc` (capture SHA-256 `29488ce2...a6cb30`, 15935 bytes) and `msgrcap-secure` (`b4f64894...0ffba8`, 17155 bytes) are retained for 30 days. Later commits change only this report. |
| Earlier live runs | 39886145519 (first complete capture set, includes the unanswered-SYN observation below) and 39886602524 (secure expectation gate rejected a phase label; see "Unclassified and limits") |
| Platform | GitHub-hosted `ubuntu-24.04` runner, Linux x86_64, Go 1.24.0 |
| Capture tools | tcpdump 4.99.4, libpcap 1.10.4, TShark 4.2.2 (Ubuntu package 4.2.2-1.1build3) |
| Capture settings | Runner-host `tcpdump -i <fixture bridge> -s 0 -U -w`, filter `host <MON>`; snaplen 262144; tcpdump reported 0 packets dropped by kernel in every run |
| Topology | One MON on a private Docker bridge, `ms_bind_msgr1 = false`, MON service modes `crc secure`, `debug_ms = 10`; host-native Go probe as `client.capture` (`mon 'allow r'`, aes256k key, type 2) |
| Substitution | The capture is runner-host tcpdump, a labelled development substitute for the planned ceph-testcontainers capture helper. It does not validate that helper, its image checks or the Go provider's cleanup and delivery contract. |

Evidence types are kept separate. Unit tests in `analysis_test.go` use
synthetic captures whose frames come from the product's own `internal/msgr`
writer, so they are not interoperability evidence. Live evidence is the native
MON's own ProtocolV2 log compared with the capture, and tshark's independent
TCP reassembly compared with msgrcap's.

## Demonstrated scope

| Layer | msgrcap (this PoC) | TShark 4.2.2 | Independent reference used |
| --- | --- | --- | --- |
| pcap open | Classic pcap (Ethernet, raw IP, Linux SLL/SLL2); pcapng rejected explicitly | Opens the file | |
| TCP reassembly | Per-direction streams, retransmission, conflict, gap, snaplen and peer-ACK accounting | `follow,tcp,raw` | Per-direction SHA-256 equal for all 8 streams in each mode (`tshark-compare.txt`); runs before `63bbf68` skipped tshark stream 0 in this comparison |
| msgr2.1 banner | Decoded (features) | Misread as msgr1 | |
| Pre-auth frames (both modes) | HELLO, AUTH_REQUEST(_MORE), AUTH_REPLY_MORE, AUTH_DONE with CRC checks, con_mode, global_id | Misread: "Connect", "Connect Reply", "UNKNOWN xN" | MON log tag sequence |
| CRC post-auth frames | AUTH_SIGNATURE, COMPRESSION_REQUEST/DONE, CLIENT/SERVER_IDENT, MESSAGE header (type, seq, tid, ack_seq), ACK | Misread: invented "KEEPALIVE", "ACK, Seq: ...", "Unknown tag" expert errors | Client-to-server tags and server message seqs equal to the MON log for every session |
| Secure post-auth frames, no key | Byte counts and TCP timing only | Misread as msgr1 | |
| Secure post-auth frames, opt-in key | Same as CRC after passive derivation (MON connections only) | Not supported | Equal to the MON log for every session |
| Message bodies | Not decoded; payload bytes never reported | | |

A Ceph dissector exists in Wireshark, but it is msgr1-only. Its heuristic
(`ceph_tcp`) matches the 6-byte prefix `ceph v` and accepts the msgr2 banner
because `strnlen` over `ceph v2\n` followed by the banner length byte returns
the msgr1 banner size. Every msgr2.1 packet it labels is therefore a false
interpretation. The decoder of the Wireshark `master` branch (blob at commit
`964bf0822abb033695694b3a298c25587533c327`) has the same banner logic and no
msgr2 preamble, frame tag or AES-GCM support. `decode-as` cannot select the
dissector on `tcp.port`. Disabling the heuristic (`--disable-heuristic
ceph_tcp`) leaves correct TCP-only output.

## 1. Capture consumption and correlation

Raw capture alone established, in the accepted run:

- **No TCP connection**: the probe's dial to a closed MON-host port produced
  four connections answered by RST at +0, +250 ms, +750 ms and +1.75 s. One
  public `Dial` call therefore appears as four TCP attempts, and the capture
  shows the client's own retry schedule.
- **Peer close**: during the command session the MON sent FIN first
  (`server FIN first`) when the workflow restarted it, about 45 to 50 ms after the
  command reply.
- **Reconnect timing**: refused SYNs while the MON was down, then an
  authenticated session about 0.75 s after the close in both modes (refused
  SYNs at +0 and +0.25 s). Run 39886602524 reconnected after 0.25 s, so the
  recovery time is a per-run observation, not a constant. In run 39886145519 (secure) one reconnect SYN went unanswered and was
  completed by the kernel's SYN retransmission about 1.06 s later. msgrcap
  treats a same-ISN SYN as one attempt (`syn_count = 2`), so "unanswered" and
  "refused" stay distinct.
- **Request identity in CRC**: MON_COMMAND (type 50, tid 1) and
  MON_COMMAND_ACK (type 51, tid 1) with sequence and ack_seq values, per
  connection.

Correlation with this project's requests needs data the capture does not
contain:

- **Client trace** (`probe-trace.json`): per dial the target, local and remote
  address, dial start/end, error and close time, plus phase and command timing.
  Exact local/remote address matching assigned every established connection;
  failed dials are matched by target and time window. Local cancellation,
  admission waits and unsent requests are visible only here.
- **Negotiated mode and identity**: the capture shows con_mode and global_id in
  AUTH_DONE in both modes. Feature bits after the banner are inside
  CLIENT/SERVER_IDENT, encrypted in secure mode.
- **Native daemon log**: the MON logs `accept ... peer_addr v2:IP:PORT/0`
  with the TCP source port, then logs the client as `IP:0/nonce` after
  CLIENT_IDENT. The accept line is the only native join key to a TCP stream.
- **Request outcome**: the capture shows the reply frame, not whether the
  caller observed it, and not execution semantics. Uncertain outcomes keep
  their existing contract.

Reassembly rules. Packet boundaries are not frame boundaries: in the CRC run
one 761-byte server segment carried several frames and small frames were split
across segments. msgrcap decodes frames only from the contiguous prefix of each
direction. It reports retransmitted and conflicting overlaps, gaps (data after
the first gap is counted, not decoded), snaplen-truncated bytes and bytes the
peer acknowledged that are absent from the capture. The last one separates
capture loss from network loss, because the peer received those bytes. It is
the acknowledged payload range minus the union of every captured range, so
payload captured after a gap, retransmitted overlaps and the FIN sequence
number are not counted as lost.
Captured bytes beyond the per-direction analysis bound (`-max-stream-bytes`)
are reported as analysis-limit truncation (`analysis_limit_excluded_bytes`)
and are never counted as capture loss; acknowledged offsets past an exhausted
bound are not attributed to the capture. A file
that ends inside a record is reported as a partial capture. A capture started
after the handshake is reported as midstream and not decoded. A new SYN with a
different ISN on the same four-tuple starts a new connection instance.

## 2. Current tool suitability

TShark 4.2.2 is suitable for opening captures, TCP conversation tables, stream
reassembly and expert TCP events (RST, FIN). It is not suitable for msgr2.1
interpretation, and its Ceph heuristic must be disabled or ignored when reading
msgr2.1 captures because it produces confident, wrong labels. No released or
`master` Wireshark dissector was found that handles msgr2.1.

The smallest useful helper is what msgrcap demonstrates: TCP reassembly with
loss accounting, a frame walker for the 32-byte rev1 preamble with CRC checks,
optional AES-GCM opening of secure frames, extraction of identity fields only
(tags, segment lengths, con_mode, global_id, message type/seq/tid/ack_seq) and
a correlation join. Its analysis code is about 1,500 lines of standard-library Go and does not
need a message decoder. A complete dissector is not required for the debugging
questions above.

## 3. CRC and secure visibility

CRC mode exposes every post-authentication frame header and the message header
without any secret. The capture is authenticated by CephX but not
confidential, which matches the existing CRC contract.

Secure mode without a key exposes the banner, the plaintext pre-auth frames
(including con_mode and global_id), then only byte counts and TCP timing. That
is enough for connection lifecycle questions (refused, unanswered, peer close,
reconnect timing, per-direction byte volume) and nothing more.

Session keys need no export from the Go client for MON connections. The MON
AUTH_DONE payload carries the AUTH ticket encrypted with the client's
long-term key and the connection secret encrypted with that ticket's session
key. A holder of the disposable client key can therefore derive the 40-byte
connection secret from the plaintext AUTH_DONE in the capture
(`cephx.Client.Finish`), and decrypt both directions with the AES-GCM nonce
sequence. Live, this decrypted every secure MON frame in the accepted run, and
the decoded client tags and server message sequences equalled the native MON
log. AES-GCM authentication of daemon-produced ciphertext under the derived
key is itself strong evidence that the derivation is correct; a wrong key fails
closed (unit test).

A test-only key export from the Go client was evaluated and is not
recommended:

- It is unnecessary for MON sessions, as shown above.
- Any export seam inside `internal/session` would be a product code path that
  can leak connection secrets, even behind a build tag, and would contradict the
  project rule against automatic key export.
- The passive method uses only a key the fixture already owns and is opt-in per
  analysis run (`-entity`/`-key-file`). It never changes the configured mode.

Secret handling. The key file stays in a mode-0700 work directory removed
during cleanup; it is never written to the artifact directory, the reports or
logs. Reports contain no payload bytes, tickets or secrets. The retained
secure capture is undecryptable without the destroyed key. Do not apply passive
derivation to captures from non-disposable identities.

Not demonstrated: OSD, MDS and MGR connections use a CephX authorizer, whose
reply is encrypted with a service ticket session key obtained from the MON. A
passive analyzer would have to follow the MON stream (initial AUTH_DONE extra
tickets and in-session MAuth renewals) to derive those keys. msgrcap reports
`not implemented for service authorizers`. Ticket renewal inside a long secure
MON session is likewise not followed. Secure-mode compression is not offered by
this project and is not handled.

## 4. PoC boundary and debugging conclusions

The PoC runs one disposable Tentacle MON, a host-native public-API probe and
read-only `mon dump` commands. Capture readiness is established before any
client traffic (tcpdump "listening on" line), native readiness checks stay
inside the container and do not cross the captured bridge, and the capture is
bound to the configured mode, MON address and client identity in
`provenance.json`. Cleanup (`cleanup.sh`) removes the container, network and
key directory and records the result in `cleanup.json`. A failed removal, or an
owned resource whose absence cannot be verified, fails the job while an
earlier nonzero status is preserved; `TestLivePoCCleanupFailsTheJob` replays
the cleanup with injected Docker failures. The live job fails unless every
established session pairs with one MON log connection and matches its mode,
received tags and sent message sequences, and unless the refused phase, the
command session and an authenticated reconnect are present.

Debugging conclusions supported by retained evidence:

1. A failing bootstrap to a closed endpoint is "no TCP connection", with four
   RST-answered attempts and the client's backoff schedule visible.
2. A lost session during a MON restart is a peer close (server FIN), not a
   client close, followed by refused attempts and an authenticated reconnect;
   the recovery time is measurable from capture timestamps alone.
3. In CRC (and secure with the opt-in key), the command and its reply are
   visible with tid and sequence numbers, and the MON's own log confirms the
   same frame and message sequence.

The first live attempt also showed a non-transport fact: Tentacle answers
`mon_status` sent as a MON command with `-22 unrecognized command`. The MON log
recorded it; the probe now uses `mon dump`.

## 5. What belongs here and what belongs to the capture providers

This repository (or a separate analysis helper): Messenger interpretation,
TCP-to-frame reassembly policy, passive derivation, correlation with client
traces and native logs, and the decision of what to retain from a capture.

Requests to the capture providers (ceph-testcontainers images #8 and
testcontainers-go #52) concern collection and delivery only:

- Capture readiness signal before the caller starts traffic, and capture start
  and stop timestamps in a stated clock.
- Interface or network namespace, BPF filter, snaplen, link type, and the
  capture tool and libpcap versions.
- Kernel and filter drop counters at stop (`packets dropped by kernel`) and
  whether the file was closed cleanly (partial final record).
- Container identities on the captured network: name, ID, IP addresses and
  MAC per daemon generation. A restart can change the MAC on the same IP
  (TShark reported duplicate IP use after the restart).
- Daemon generation markers (restart and replacement times) so connections can
  be assigned to the correct incarnation.
- Classic pcap output (or a documented pcapng option), file size and SHA-256
  at retrieval.
- Optional retrieval of daemon logs with their configured debug levels next to
  the pcap.

They should not be asked to decode messages, decrypt traffic or handle client
keys.

## Recommendation

Reuse tshark only for TCP-level work (conversations, reassembly cross-checks,
TCP expert events) with the `ceph_tcp` heuristic disabled. Keep a small
separate analysis helper of the msgrcap kind for msgr2.1 framing, opt-in
passive secure decryption of MON sessions and correlation; keep it out of the
product module. Defer integration into the regular native scenarios until the
capture providers deliver the collection fields above. Further research is
needed before claiming service-connection (OSD/MDS/MGR) secure visibility or
long-session ticket renewal. A full interpreter or dissector is not
recommended at this point.

## Unclassified and limits

- Phase labels come from the probe's local view. The client's recovery dial can
  start before the workflow reports that the restart finished, so in run
  39886602524 the secure reconnect carried the `peer-restart` label and the
  original gate rejected it. The gate now identifies the reconnect by
  correlation, not by label.
- Only one MON, one client, IPv4, aes256k and short sessions were exercised.
  Multi-MON, MGR, OSD, IPv6, aes keys, compression, renewal and long runs are
  unverified for capture analysis.
- Capture on the runner bridge sees host-to-container traffic. It is not the
  container-namespace view the planned helper may provide.
- No capture fixture is committed. Live captures exist only as workflow
  artifacts and contain disposable fixture addresses, FSIDs and CRC-mode
  plaintext `mon dump` output from a throwaway cluster.

## Provenance and licensing

No external code or capture was incorporated. msgrcap is original code under
this repository's license and uses only the Go standard library and this
module's packages. Wireshark (GPL) and tcpdump/libpcap (BSD) were executed on
the CI runner only and are not linked or redistributed. The pinned Ceph
ProtocolV2 source was read to identify its log format; no Ceph code was copied.
