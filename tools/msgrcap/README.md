# msgrcap: development-only capture analysis PoC

`msgrcap` is a bounded proof of concept for issue #48. It is not part of the
product. It lives in its own Go module, so the product module never imports
it, and it reads classic pcap files with the Go standard library. It needs no
libpcap, CGO, tshark or subprocess. `tcpdump` and `tshark` run only on the
GitHub Actions runner in [the PoC workflow](../../.github/workflows/msgr-capture-poc.yml).

Findings and the recommendation are in [CAPTURE_ANALYSIS.md](CAPTURE_ANALYSIS.md).

## Contents

| File | Purpose |
| --- | --- |
| `pcap.go` | Classic pcap reader (Ethernet, raw IP, Linux SLL/SLL2; IPv4/IPv6). pcapng is rejected explicitly. |
| `cleanup.sh` | Owned-resource cleanup sourced by `live-poc.sh`, including capture helpers that joined the MON's namespace; a failed or unverifiable cleanup fails the job. |
| `tcp.go` | Connection tracking (SYN-defined instances, four-tuple reuse) and per-direction reassembly with retransmission, conflict, gap, snaplen, "acknowledged but not captured" and separate analysis-limit accounting. |
| `msgr2.go` | A small msgr2.1 rev1 frame walker, written separately from `internal/msgr`: banner, CRC frames, opt-in secure frame decryption, and identity metadata only. |
| `analysis.go` | Per-connection TCP and Messenger outcome, opt-in passive CephX derivation for MON connections, and trace correlation. |
| `coverage.go` | Capture manifests: consecutive segment files, their session reports, declared gaps and loss accounting. |
| `parity.go` | Machine comparison of two analyses of the same run (`msgrcap parity`). |
| `crosscheck.go` | Comparison with the native MON's ProtocolV2 log (`debug_ms = 10`). |
| `probe.go`, `trace.go` | A public-API probe that records client-side connection identities, phases and command timing, with an optional local dial hold. |
| `live-poc.sh`, `mon-bootstrap.sh` | The disposable single-MON live recipe run by the workflow. |
| `providercap/` | A separate module: one ceph-testcontainers-go `capture.Start` session per run (one capture segment). |
| `verify-image.sh` | Checks the pinned capture helper image digest at the registry before it is pulled. |
| `tshark_compare.py` | Compares TCP reassembly digests with `tshark -z follow,tcp,raw`. |

## Usage

```sh
go -C tools/msgrcap build -o msgrcap .
./msgrcap analyze -pcap capture.pcap -trace probe-trace.json -json analysis.json
# Opt-in: decrypt secure MON frames from a disposable test key.
./msgrcap analyze -pcap capture.pcap -entity client.capture -key-file capture.key
# Opt-in: compare with a native MON log captured at debug_ms = 10.
./msgrcap analyze -pcap capture.pcap -mon-log mon.log -cross crosscheck.json
# Segmented capture: a manifest instead of one file (see "Capture manifests").
./msgrcap analyze -segments manifest.json -trace probe-trace.json -json analysis.json
# Compare two analyses of the same run; -require-full also rejects gaps.
./msgrcap parity -reference ref.json -candidate cand.json -json parity.json
```

Reports contain identities, lengths, tags, message header fields
(seq/tid/type/ack_seq), TCP events and digests. They never contain segment
payloads, keys, tickets or connection secrets. The key file is read only when
`-key-file` is given. Supply only a disposable fixture key, and never commit
or upload it.

## Capture manifests

`-segments` reads a JSON manifest that lists consecutive capture files, each
with the report of the session that recorded it, and the gaps between them:

```json
{"source": "...", "segments": [{"file": "segment-1.pcap", "report": "segment-1.json"}],
 "gaps": [{"after_segment": 1, "reason": "..."}]}
```

Segment reports use the JSON fields of the ceph-testcontainers-go v0.4.0
`capture.Report`. A runner-host tcpdump file carries the same fields, written
by `live-poc.sh`, with `target` set to `not-bound`. The covered interval of a
segment runs from `ready_at` to `stop_requested_at`; a gap runs from one
segment's stop request to the next segment's readiness.

The analysis fails, rather than reporting a continuous capture, when:

- two segments have no declared gap between them, a gap has no reason, or a
  declared gap has no following segment;
- segments overlap, or a segment has no ready-to-stop interval;
- a segment did not end `stopped` (`packet_limit`, `premature`, `killed`,
  `failed` or `unknown`), or its target is not `unchanged` (or `not-bound`);
- a file was not retrieved, is partial, is not `valid` and complete, or its
  SHA-256 or packet count differs from its report;
- tcpdump printed no captured or kernel drop counter (the loss is unknown);
- the client trace starts before the first segment is ready or finishes after
  the last one was asked to stop.

Counted drops are reported as `capture_coverage.lost_packets`, not hidden.
Trace activity inside a gap is listed in `capture_coverage.trace`. A
connection that a gap may have interrupted (not closed by RST or both FINs
before the gap, or with packets on both sides of it) carries `capture_gaps`,
and its outcome reads `unobserved after a declared capture gap` instead of
`open at capture end`.

`msgrcap parity` compares two such analyses of the same run: connections by
client and server address in order, with TCP outcome and events, per-direction
byte counts, digests and loss accounting, frames, connection mode, MON
identity, auth and derivation results, trace phases, unmatched trace
connections and the MON log cross-check rows. Timestamps, pcap record numbers
and tracker IDs are excluded. Reference connections that overlap a candidate
gap are listed instead of compared. The verdict is `full parity` only when
nothing differs, neither capture dropped a packet, no reference connection
falls in a candidate gap and no candidate connection is marked by a gap.

## Live recipe

`live-poc.sh crc|secure OUTPUT_DIR CEPH_IMAGE CAPTURE_IMAGE` runs on a Linux
runner with Docker, `sudo tcpdump` and `tshark`. The workflow first runs
`verify-image.sh`: it fetches the helper image index by digest, checks that
its raw bytes hash to the pinned digest and list linux/amd64 and linux/arm64,
records what the tag resolves to today for information only, pulls by digest
and checks the local repository digest.

1. Bootstrap one MON from the pinned image on a private bridge with a fixed
   address, `ms_bind_msgr1 = false`, MON service modes `crc secure` and
   `debug_ms = 10`. Create a read-only `client.capture` identity.
2. Wait for native in-container readiness. Those checks do not cross the
   bridge, so they are not captured.
3. Start the reference capture, `tcpdump -i <bridge> -s 0 -U -w capture.pcap
   host <mon>`, and wait for its "listening on" line. Then start provider
   segment 1 with `capture.Start` on the MON container (`eth0`, the same
   filter, snaplen 262144), which returns once its tcpdump listens.
4. Run the probe: a dial to a closed port on the MON host (`refused`), one
   `mon dump` session (`command`), a workflow-driven MON restart
   (`peer-restart`), explicit read-only retries (`after-restart`) and `close`.
5. The restart: the workflow stops the MON daemon with SIGTERM while its
   container and network namespace stay up, waits until the daemon has exited
   and one more second (the peer close and refused redials fall in segment 1),
   creates the probe's dial-hold file, stops segment 1, restarts the
   container, starts segment 2 on the new generation, and only then lets the
   daemon start and releases the dial hold. The interval between the two
   segments is declared in `provider/manifest.json`.
6. Stop both captures, collect the MON log, analyze each capture with and
   without the key, run `msgrcap parity` (`parity.json`,
   `parity-derived.json`), run the tshark evaluation on the reference file and
   on the provider segments, and write `provenance.json` and `cleanup.json`.

`reference/` and `provider/` hold each capture's manifest and analyses. The
top-level `analysis*.json`, `crosscheck*.json` and `summary*.txt` are copies
of the primary capture's, named by `primary_capture` in `provenance.json`;
the expectation gate reads them. The primary capture is the provider capture,
because its analyses had full parity with the reference in both modes (see
CAPTURE_ANALYSIS.md, "Provider capture parity"). Every run must show full
parity again, with and without the key, so a provider capture that diverges
from the reference fails the job. The provider tshark comparison must cover
every provider connection. The top-level `capture.pcap`, `tcpdump.stderr` and
`tshark-*` files belong to the reference.

The dial hold is a harness control: while its file exists, a new dial waits
in the probe before any packet is sent, and the trace records
`dial_held_from`/`dial_held_until`. The daemon gate and the hold keep client
traffic out of the provider gap without changing what the client sees: the
MON still closes the session, refuses redials while it is down and accepts
the reconnect after the restart.

`capture.Start` binds a session to one container generation and never follows
a restart, so the recipe records the restart as two segments with an explicit
gap instead of one continuous provider file. Continuity across a restart and
gap/loss verification are tracked upstream in
[JSYoo5B/ceph-testcontainers-go#537](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/537);
this recipe is a consumer-side composition of the public v0.4.0 API for that
question. The helper image is
`ghcr.io/jsyoo5b/ceph-testcontainers-images:extension-capture-v1` (amd64 and
arm64), pinned by index digest in the workflow.
