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
| `tcp.go` | Connection tracking (SYN-defined instances, four-tuple reuse) and per-direction reassembly with retransmission, conflict, gap, snaplen and "acknowledged but not captured" accounting. |
| `msgr2.go` | A small msgr2.1 rev1 frame walker, written separately from `internal/msgr`: banner, CRC frames, opt-in secure frame decryption, and identity metadata only. |
| `analysis.go` | Per-connection TCP and Messenger outcome, opt-in passive CephX derivation for MON connections, and trace correlation. |
| `crosscheck.go` | Comparison with the native MON's ProtocolV2 log (`debug_ms = 10`). |
| `probe.go`, `trace.go` | A public-API probe that records client-side connection identities, phases and command timing. |
| `live-poc.sh`, `mon-bootstrap.sh` | The disposable single-MON live recipe run by the workflow. |
| `tshark_compare.py` | Compares TCP reassembly digests with `tshark -z follow,tcp,raw`. |

## Usage

```sh
go -C tools/msgrcap build -o msgrcap .
./msgrcap analyze -pcap capture.pcap -trace probe-trace.json -json analysis.json
# Opt-in: decrypt secure MON frames from a disposable test key.
./msgrcap analyze -pcap capture.pcap -entity client.capture -key-file capture.key
# Opt-in: compare with a native MON log captured at debug_ms = 10.
./msgrcap analyze -pcap capture.pcap -mon-log mon.log -cross crosscheck.json
```

Reports contain identities, lengths, tags, message header fields
(seq/tid/type/ack_seq), TCP events and digests. They never contain segment
payloads, keys, tickets or connection secrets. The key file is read only when
`-key-file` is given. Supply only a disposable fixture key, and never commit
or upload it.

## Live recipe

`live-poc.sh crc|secure OUTPUT_DIR IMAGE` runs on a Linux runner with Docker,
`sudo tcpdump` and `tshark`:

1. Bootstrap one MON from the pinned image on a private bridge with a fixed
   address, `ms_bind_msgr1 = false`, MON service modes `crc secure` and
   `debug_ms = 10`. Create a read-only `client.capture` identity.
2. Wait for native in-container readiness. Those checks do not cross the
   bridge, so they are not captured.
3. Start `tcpdump -i <bridge> -s 0 -U -w capture.pcap host <mon>` and wait for
   its "listening on" line before any client traffic (capture readiness).
4. Run the probe: a dial to a closed port on the MON host (`refused`), one
   `mon dump` session (`command`), a workflow-driven MON restart
   (`peer-restart`), explicit read-only retries (`after-restart`) and `close`.
5. Stop the capture, collect the MON log, analyze with and without the key,
   run the tshark evaluation and write `provenance.json` and `cleanup.json`.

The capture is a labelled development substitute for the planned
ceph-testcontainers capture helper. It does not validate that helper or its
delivery contract.
