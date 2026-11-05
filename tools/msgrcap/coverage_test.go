package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Cluster-free checks of capture coverage: segment manifests, declared gaps,
// loss accounting and gap marking. Segment reports are synthetic stand-ins
// for capture.Report JSON; they are not evidence of the provider's behavior.

func u64(v uint64) *uint64 { return &v }

var t0 = time.Unix(1_700_000_000, 0).UTC()

func goodReport(sha string, records int, ready, stop time.Time) segmentReport {
	return segmentReport{
		ContainerID: "mon", ReadyAt: ready, StopRequestedAt: stop, EndedAt: stop.Add(time.Millisecond),
		End: "stopped", Target: "unchanged", Retrieved: true, RetrievedSHA256: sha,
		Statistics: segmentStatistics{Captured: u64(uint64(records)), DroppedByKernel: u64(0)},
		File:       segmentFile{Status: "valid", Packets: records, Complete: true},
	}
}

func twoSegments() (*captureManifest, []segmentReport, []segmentInput) {
	m := &captureManifest{Source: "test", Segments: []manifestSegment{{"a.pcap", "a.json"}, {"b.pcap", "b.json"}},
		Gaps: []declaredGap{{AfterSegment: 1, Reason: "restart"}}}
	reports := []segmentReport{
		goodReport("aa", 3, t0, t0.Add(10*time.Second)),
		goodReport("bb", 4, t0.Add(15*time.Second), t0.Add(30*time.Second)),
	}
	return m, reports, []segmentInput{{SHA256: "aa", Records: 3}, {SHA256: "bb", Records: 4}}
}

func TestCoverageAcceptsDeclaredGap(t *testing.T) {
	m, reports, inputs := twoSegments()
	tr := &traceFile{Started: t0.Add(time.Second), Finished: t0.Add(29 * time.Second),
		Connections: []traceConn{
			{Phase: "command", Target: "m", DialStart: t0.Add(2 * time.Second), DialEnd: t0.Add(2 * time.Second), Closed: t0.Add(9 * time.Second)},
			{Phase: "after-restart", Target: "m", DialStart: t0.Add(16 * time.Second), DialEnd: t0.Add(16 * time.Second), Closed: t0.Add(20 * time.Second)},
		}}
	cov, err := checkCoverage(m, reports, inputs, tr)
	if err != nil {
		t.Fatal(err)
	}
	if len(cov.Gaps) != 1 || !cov.Gaps[0].From.Equal(t0.Add(10*time.Second)) || !cov.Gaps[0].To.Equal(t0.Add(15*time.Second)) ||
		cov.Gaps[0].Reason != "restart" || cov.Lost != 0 || len(cov.Trace.InGaps) != 0 {
		t.Fatalf("coverage %+v", cov)
	}
	// Activity inside the gap is listed, not hidden; an unclosed connection
	// is taken to last until the trace finished.
	tr.Connections[0].Closed = time.Time{}
	tr.Commands = []traceCommand{{Phase: "peer-restart", Prefix: "mon dump", Attempt: 1, Started: t0.Add(11 * time.Second), Ended: t0.Add(12 * time.Second)}}
	cov, err = checkCoverage(m, reports, inputs, tr)
	if err != nil {
		t.Fatal(err)
	}
	if len(cov.Trace.InGaps) != 2 || !strings.Contains(cov.Trace.InGaps[0], "command connection") ||
		!strings.Contains(cov.Trace.InGaps[1], "peer-restart command") {
		t.Fatalf("activity in gaps %q", cov.Trace.InGaps)
	}
	// Loss is accounted, not an error.
	reports[1].Statistics.DroppedByKernel = u64(2)
	reports[1].Statistics.DroppedByInterface = u64(1)
	if cov, err = checkCoverage(m, reports, inputs, nil); err != nil || cov.Lost != 3 || len(cov.Notes) != 1 {
		t.Fatalf("loss %+v %v", cov, err)
	}
}

// Everything that would let a gap, a lost end or an unknown loss pass as
// continuous capture fails the analysis.
func TestCoverageFailsClosed(t *testing.T) {
	for _, c := range []struct {
		name, want string
		mutate     func(*captureManifest, []segmentReport, []segmentInput, *traceFile)
	}{
		{"undeclared gap", "unaccounted capture gap between segment 1 and 2", func(m *captureManifest, _ []segmentReport, _ []segmentInput, _ *traceFile) {
			m.Gaps = nil
		}},
		{"gap without reason", "without a reason", func(m *captureManifest, _ []segmentReport, _ []segmentInput, _ *traceFile) {
			m.Gaps[0].Reason = ""
		}},
		{"gap after the last segment", "no following segment", func(m *captureManifest, _ []segmentReport, _ []segmentInput, _ *traceFile) {
			m.Gaps[0].AfterSegment = 2
		}},
		{"gap declared twice", "declared twice", func(m *captureManifest, _ []segmentReport, _ []segmentInput, _ *traceFile) {
			m.Gaps = append(m.Gaps, m.Gaps[0])
		}},
		{"overlapping segments", "segments overlap", func(_ *captureManifest, r []segmentReport, _ []segmentInput, _ *traceFile) {
			r[1].ReadyAt = r[0].StopRequestedAt.Add(-time.Millisecond)
		}},
		{"packet limit", `ended "packet_limit"`, func(_ *captureManifest, r []segmentReport, _ []segmentInput, _ *traceFile) {
			r[0].End = "packet_limit"
		}},
		{"premature end", `ended "premature"`, func(_ *captureManifest, r []segmentReport, _ []segmentInput, _ *traceFile) {
			r[1].End = "premature"
		}},
		{"target restarted", `target is "changed"`, func(_ *captureManifest, r []segmentReport, _ []segmentInput, _ *traceFile) {
			r[0].Target = "changed"
		}},
		{"not retrieved", "not retrieved", func(_ *captureManifest, r []segmentReport, _ []segmentInput, _ *traceFile) {
			r[0].Retrieved = false
		}},
		{"partial file", "partial", func(_ *captureManifest, _ []segmentReport, in []segmentInput, _ *traceFile) {
			in[1].ReadError = "record 4: truncated"
		}},
		{"other file", "differs from the report", func(_ *captureManifest, _ []segmentReport, in []segmentInput, _ *traceFile) {
			in[0].SHA256 = "cc"
		}},
		{"invalid file", `file is "malformed"`, func(_ *captureManifest, r []segmentReport, _ []segmentInput, _ *traceFile) {
			r[0].File.Status = "malformed"
		}},
		{"incomplete check", "complete=false", func(_ *captureManifest, r []segmentReport, _ []segmentInput, _ *traceFile) {
			r[0].File.Complete = false
		}},
		{"report packet count", "report counts 9 packets", func(_ *captureManifest, r []segmentReport, _ []segmentInput, _ *traceFile) {
			r[1].File.Packets = 9
		}},
		{"no captured counter", "no captured counter", func(_ *captureManifest, r []segmentReport, _ []segmentInput, _ *traceFile) {
			r[0].Statistics.Captured = nil
		}},
		{"captured counter differs", "captured counter 5", func(_ *captureManifest, r []segmentReport, _ []segmentInput, _ *traceFile) {
			r[0].Statistics.Captured = u64(5)
		}},
		{"unknown kernel drops", "loss is unknown", func(_ *captureManifest, r []segmentReport, _ []segmentInput, _ *traceFile) {
			r[1].Statistics.DroppedByKernel = nil
		}},
		{"no ready time", "ready-to-stop", func(_ *captureManifest, r []segmentReport, _ []segmentInput, _ *traceFile) {
			r[0].ReadyAt = time.Time{}
		}},
		{"trace before coverage", "outside the capture coverage", func(_ *captureManifest, _ []segmentReport, _ []segmentInput, tr *traceFile) {
			tr.Started = t0.Add(-time.Second)
		}},
		{"trace after coverage", "outside the capture coverage", func(_ *captureManifest, _ []segmentReport, _ []segmentInput, tr *traceFile) {
			tr.Finished = t0.Add(31 * time.Second)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			m, reports, inputs := twoSegments()
			tr := &traceFile{Started: t0.Add(time.Second), Finished: t0.Add(29 * time.Second)}
			c.mutate(m, reports, inputs, tr)
			_, err := checkCoverage(m, reports, inputs, tr)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v, want %q", err, c.want)
			}
		})
	}
	if _, err := checkCoverage(&captureManifest{}, nil, nil, nil); err == nil {
		t.Fatal("an empty manifest was accepted")
	}
}

// writeSegments writes pcap segments, their reports and a manifest, and
// returns the manifest path.
func writeSegments(t *testing.T, segs [][]byte, windows [][2]time.Time, gaps []declaredGap) string {
	t.Helper()
	dir := t.TempDir()
	m := captureManifest{Source: "synthetic segments", Gaps: gaps}
	for i, data := range segs {
		rep, err := analyze("seg", strings.NewReader(string(data)), analysisOptions{ServerPorts: []uint16{3300}, MaxBytes: 1 << 20, FrameLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		r := goodReport(hex.EncodeToString(sum[:]), rep.Pcap.Records, windows[i][0], windows[i][1])
		name := filepath.Join(dir, "segment-"+string(rune('1'+i)))
		raw, _ := json.Marshal(r)
		if err := os.WriteFile(name+".pcap", data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name+".json", raw, 0o600); err != nil {
			t.Fatal(err)
		}
		m.Segments = append(m.Segments, manifestSegment{File: filepath.Base(name) + ".pcap", Report: filepath.Base(name) + ".json"})
	}
	raw, _ := json.Marshal(m)
	path := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Two segments around a restart: the first session closes inside segment 1,
// the reconnect lies in segment 2. Packet numbers continue across files.
func TestSegmentedCaptureAnalysis(t *testing.T) {
	_, payload, _ := fixture(t)
	c2s, s2c := monStreams(t, modeCRC, payload, nil)
	first := newPcap(65535)
	conversation(first, cliAddr, monAddr, c2s, s2c, 64, true)
	end1 := first.t
	second := newPcap(65535)
	second.t = end1.Add(10 * time.Second)
	start2 := second.t
	reconnect := netip.MustParseAddrPort("10.0.0.1:40002")
	conversation(second, reconnect, monAddr, c2s, s2c, 64, false)
	end2 := second.t
	windows := [][2]time.Time{{t0.Add(-time.Second), end1.Add(time.Second)}, {start2.Add(-time.Second), end2.Add(time.Second)}}
	gaps := []declaredGap{{AfterSegment: 1, Reason: "restart"}}
	opts := analysisOptions{ServerPorts: []uint16{3300}, MaxBytes: 1 << 20, FrameLimit: 1 << 20}

	rep, err := analyzePaths("", writeSegments(t, [][]byte{first.buf.Bytes(), second.buf.Bytes()}, windows, gaps), opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Inputs) != 2 || rep.Pcap.Records != rep.Inputs[0].Records+rep.Inputs[1].Records || len(rep.Connections) != 2 {
		t.Fatalf("inputs %+v pcap %+v connections %d", rep.Inputs, rep.Pcap, len(rep.Connections))
	}
	if got := rep.Connections[1].Events[0].Packet; got != rep.Inputs[0].Records+1 {
		t.Fatalf("segment 2 starts at packet %d, want %d", got, rep.Inputs[0].Records+1)
	}
	for _, c := range rep.Connections {
		if len(c.CaptureGaps) != 0 || c.ConMode != "crc" {
			t.Fatalf("connection %d: gaps %v mode %q outcome %q", c.ID, c.CaptureGaps, c.ConMode, c.TCPOutcome)
		}
	}
	if rep.Capture == nil || len(rep.Capture.Gaps) != 1 || rep.Capture.Source != "synthetic segments" {
		t.Fatalf("coverage %+v", rep.Capture)
	}

	// A session still open when segment 1 stopped is not continuous: it is
	// marked, and its outcome does not read as a capture end.
	open := newPcap(65535)
	conversation(open, cliAddr, monAddr, c2s, s2c, 64, false)
	rep, err = analyzePaths("", writeSegments(t, [][]byte{open.buf.Bytes(), second.buf.Bytes()}, windows, gaps), opts)
	if err != nil {
		t.Fatal(err)
	}
	if c := rep.Connections[0]; len(c.CaptureGaps) != 1 || c.CaptureGaps[0] != 1 || c.TCPOutcome != "unobserved after a declared capture gap" {
		t.Fatalf("open session across the gap: gaps %v outcome %q", c.CaptureGaps, c.TCPOutcome)
	}
	if c := rep.Connections[1]; len(c.CaptureGaps) != 0 || c.TCPOutcome != "open at capture end" {
		t.Fatalf("last session: gaps %v outcome %q", c.CaptureGaps, c.TCPOutcome)
	}

	// The same files without the declaration fail closed.
	if _, err := analyzePaths("", writeSegments(t, [][]byte{first.buf.Bytes(), second.buf.Bytes()}, windows, nil), opts); err == nil ||
		!strings.Contains(err.Error(), "unaccounted capture gap") {
		t.Fatalf("undeclared gap: %v", err)
	}
}

// A connection whose packets lie on both sides of a gap is marked even when
// it was closed after the gap.
func TestMarkGapsAcrossSegments(t *testing.T) {
	g := []coverageGap{{AfterSegment: 1, From: t0.Add(10 * time.Second), To: t0.Add(15 * time.Second)}}
	rep := &report{Connections: []connReport{
		{ID: 1, First: t0, Last: t0.Add(9 * time.Second), TCPOutcome: "server FIN first",
			Events: []tcpEvent{{From: "server", Kind: "FIN"}, {From: "client", Kind: "FIN"}}},
		{ID: 2, First: t0, Last: t0.Add(20 * time.Second), TCPOutcome: "client RST first", Events: []tcpEvent{{From: "client", Kind: "RST"}}},
		{ID: 3, First: t0, Last: t0.Add(9 * time.Second), TCPOutcome: "server FIN first", Events: []tcpEvent{{From: "server", Kind: "FIN"}}},
		{ID: 4, First: t0.Add(16 * time.Second), Last: t0.Add(20 * time.Second), TCPOutcome: "open at capture end"},
	}}
	markGaps(rep, g)
	want := map[int]bool{1: false, 2: true, 3: true, 4: false}
	for _, c := range rep.Connections {
		if (len(c.CaptureGaps) > 0) != want[c.ID] {
			t.Fatalf("connection %d gaps %v", c.ID, c.CaptureGaps)
		}
	}
}
