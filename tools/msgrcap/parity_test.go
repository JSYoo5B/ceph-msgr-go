package main

import (
	"bytes"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Cluster-free checks of the parity comparison. Both sides are analyses of
// synthetic captures; a pass here says only that the comparison detects the
// differences it is meant to detect.

// parityPair returns two independent analyses of the same synthetic traffic:
// a refused dial, a closed CRC session and a reconnect after a pause.
func parityPair(t *testing.T) (*report, *report) {
	t.Helper()
	_, payload, _ := fixture(t)
	c2s, s2c := monStreams(t, modeCRC, payload, nil)
	build := func(skew time.Duration) *report {
		b := newPcap(65535)
		b.t = b.t.Add(skew)
		closed := netip.MustParseAddrPort("10.0.0.2:3301")
		b.tcp(cliAddr, closed, 77, 0, tcpSYN, nil)
		b.tcp(closed, cliAddr, 0, 78, tcpRST|tcpACK, nil)
		conversation(b, cliAddr, monAddr, c2s, s2c, 64, true)
		b.t = b.t.Add(10 * time.Second)
		conversation(b, netip.MustParseAddrPort("10.0.0.1:40002"), monAddr, c2s, s2c, 64, false)
		rep, err := analyze("p", bytes.NewReader(b.buf.Bytes()), analysisOptions{ServerPorts: []uint16{3300}, MaxBytes: 1 << 20, FrameLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		rep.Capture = &coverage{Source: "synthetic"}
		return rep
	}
	// The candidate's clock differs slightly, as two observation points do.
	return build(0), build(3 * time.Microsecond)
}

func TestParityHoldsForEqualAnalyses(t *testing.T) {
	ref, cand := parityPair(t)
	cand.Pcap.Records++ // a non-TCP record seen only by one observation point
	res, err := parity(ref, cand, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Full || !res.OutsideGaps || res.Compared != 3 || len(res.Mismatches) != 0 || res.Verdict != "full parity" {
		t.Fatalf("parity %+v", res)
	}
}

func TestParityDetectsDifferences(t *testing.T) {
	for _, c := range []struct {
		name, want string
		mutate     func(ref, cand *report)
	}{
		{"message sequence", "server_to_client.msgr2.frames", func(_, cand *report) {
			for i := range cand.Connections[1].S2C.Msgr.Frames {
				if m := cand.Connections[1].S2C.Msgr.Frames[i].Message; m != nil {
					m2 := *m
					m2.Seq++
					cand.Connections[1].S2C.Msgr.Frames[i].Message = &m2
				}
			}
		}},
		{"frame dropped", "client_to_server.msgr2.frames: ", func(_, cand *report) {
			f := cand.Connections[1].C2S.Msgr.Frames
			cand.Connections[1].C2S.Msgr.Frames = f[:len(f)-1]
		}},
		{"connection mode", "con_mode", func(_, cand *report) { cand.Connections[2].ConMode = "secure" }},
		{"auth phase", "msgr_outcome", func(_, cand *report) { cand.Connections[1].MsgrOutcome = "closed or stopped before AUTH_DONE" }},
		{"secret derivation", "secret_derivation", func(_, cand *report) { cand.Connections[1].SecretState = "derivation failed" }},
		{"tcp outcome", "tcp_outcome", func(_, cand *report) { cand.Connections[0].TCPOutcome = "unanswered SYN" }},
		{"tcp events", "tcp_events", func(_, cand *report) { cand.Connections[1].Events = cand.Connections[1].Events[1:] }},
		{"stream digest", "reassembled_sha256", func(_, cand *report) { cand.Connections[2].C2S.SHA256 = "00" }},
		{"retransmissions", "retransmitted_segments", func(_, cand *report) { cand.Connections[1].C2S.Retransmitted++ }},
		{"acked but not captured", "peer_acked_bytes_missing_from_capture", func(_, cand *report) { cand.Connections[1].S2C.AckedNotCaptured = 20 }},
		{"trace phase", "trace_phase", func(_, cand *report) { cand.Connections[2].Phase = "after-restart" }},
		{"missing connection", "absent from the candidate", func(_, cand *report) { cand.Connections = cand.Connections[:2] }},
		{"extra connection", "absent from the reference", func(ref, _ *report) { ref.Connections = ref.Connections[:2] }},
		{"candidate loss", "capture loss", func(_, cand *report) { cand.Capture.Lost = 1 }},
		{"reference loss", "capture loss", func(ref, _ *report) { ref.Capture.Lost = 2 }},
		{"decryption", "secure decryption", func(_, cand *report) { cand.Decryption = "enabled" }},
		{"snaplen truncation", "pcap link/snaplen/truncation", func(_, cand *report) { cand.Pcap.Truncated = 1 }},
		{"unmatched trace", "unmatched trace connections", func(_, cand *report) {
			cand.Unmatched = []traceConn{{Phase: "command", Target: "10.0.0.2:3300", DialStart: t0, DialEnd: t0}}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			ref, cand := parityPair(t)
			c.mutate(ref, cand)
			res, err := parity(ref, cand, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if res.OutsideGaps || res.Full || !strings.Contains(strings.Join(res.Mismatches, "\n"), c.want) {
				t.Fatalf("mismatches %q (outside=%v full=%v), want %q", res.Mismatches, res.OutsideGaps, res.Full, c.want)
			}
		})
	}
}

func TestParityAccountsCandidateGaps(t *testing.T) {
	ref, cand := parityPair(t)
	// The candidate never saw the closed session: it fell in its gap.
	g := coverageGap{AfterSegment: 1, From: ref.Connections[1].First.Add(-time.Millisecond), To: ref.Connections[1].Last.Add(time.Millisecond), Reason: "restart"}
	cand.Capture.Gaps = []coverageGap{g}
	cand.Connections = append(cand.Connections[:1], cand.Connections[2:]...)
	res, err := parity(ref, cand, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !res.OutsideGaps || res.Full || len(res.ReferenceInGaps) != 1 || res.Compared != 2 || res.CandidateGaps != 1 {
		t.Fatalf("parity %+v", res)
	}
	// A candidate connection marked by a gap prevents full parity even when
	// the reference saw nothing in the gap.
	ref, cand = parityPair(t)
	cand.Capture.Gaps = []coverageGap{{AfterSegment: 1, From: t0.Add(time.Hour), To: t0.Add(2 * time.Hour), Reason: "restart"}}
	cand.Connections[2].CaptureGaps = []int{1}
	if res, err = parity(ref, cand, nil, nil); err != nil || !res.OutsideGaps || res.Full || len(res.CandidateMarked) != 1 {
		t.Fatalf("marked candidate %+v %v", res, err)
	}
	// MON log rows of a reference connection in a gap are not compared;
	// every other row is.
	ref, cand = parityPair(t)
	cand.Capture.Gaps = []coverageGap{g}
	cand.Connections = append(cand.Connections[:1], cand.Connections[2:]...)
	rows := func(r *report) []crossResult {
		var out []crossResult
		for _, c := range r.Connections {
			if c.ConMode != "" {
				out = append(out, crossResult{Connection: c.ID, Client: c.Client, Phase: c.Phase, ModeEqual: true})
			}
		}
		return out
	}
	rc, cc := rows(ref), rows(cand)
	if res, err = parity(ref, cand, rc, cc); err != nil || !res.OutsideGaps || res.CrossCompared != 1 {
		t.Fatalf("cross rows %+v %v", res, err)
	}
	cc[0].SeqsEqual = true
	if res, err = parity(ref, cand, rc, cc); err != nil || res.OutsideGaps || !strings.Contains(strings.Join(res.Mismatches, "\n"), "server_message_seqs_equal") {
		t.Fatalf("cross mismatch %+v %v", res, err)
	}
}

func TestParityNeedsCoverage(t *testing.T) {
	ref, cand := parityPair(t)
	cand.Capture = nil
	if _, err := parity(ref, cand, nil, nil); err == nil {
		t.Fatal("parity without candidate coverage was accepted")
	}
}

// The command fails on any mismatch, and with -require-full also on parity
// that holds only outside the gaps.
func TestParityCommandExit(t *testing.T) {
	ref, cand := parityPair(t)
	dir := t.TempDir()
	write := func(name string, v any) string {
		p := filepath.Join(dir, name)
		if err := writeJSON(p, v); err != nil {
			t.Fatal(err)
		}
		return p
	}
	r, c := write("ref.json", ref), write("cand.json", cand)
	var out bytes.Buffer
	if err := parityMain([]string{"-reference", r, "-candidate", c, "-require-full", "-json", filepath.Join(dir, "p.json")}, &out); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "p.json")); err != nil {
		t.Fatal(err)
	}
	cand.Capture.Gaps = []coverageGap{{AfterSegment: 1, From: ref.Connections[1].First, To: ref.Connections[1].Last}}
	cand.Connections = append(cand.Connections[:1], cand.Connections[2:]...)
	c = write("cand.json", cand)
	if err := parityMain([]string{"-reference", r, "-candidate", c}, &out); err != nil {
		t.Fatalf("parity outside gaps failed: %v", err)
	}
	if err := parityMain([]string{"-reference", r, "-candidate", c, "-require-full"}, &out); err == nil {
		t.Fatal("-require-full accepted a candidate gap with reference traffic")
	}
	cand.Connections[0].SYNs++
	c = write("cand.json", cand)
	if err := parityMain([]string{"-reference", r, "-candidate", c}, &out); err == nil || !strings.Contains(out.String(), "MISMATCH") {
		t.Fatalf("mismatch accepted: %v", err)
	}
}
