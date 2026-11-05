// Command msgrcap is a development-only packet-capture analysis PoC for this
// project's msgr2.1 traffic. It is a separate module: the product module never
// imports it, and it uses no libpcap, CGO, tshark or subprocess.
//
//	msgrcap analyze -pcap capture.pcap | -segments manifest.json [-trace probe.json]
//	        [-json report.json] [-entity client.x -key-file key] [-mon-log mon.log -cross cross.json]
//	msgrcap parity -reference ref.json -candidate cand.json [-reference-cross a.json
//	        -candidate-cross b.json] [-json parity.json] [-require-full]
//	msgrcap probe -mon v2:IP:3300 -entity client.x -key-file key -mode crc|secure -trace probe.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: msgrcap analyze|probe|parity [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "analyze":
		err = analyzeMain(os.Args[2:], os.Stdout)
	case "probe":
		err = probeMain(os.Args[2:])
	case "parity":
		err = parityMain(os.Args[2:], os.Stdout)
	default:
		err = fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "msgrcap:", err)
		os.Exit(1)
	}
}

func probeMain(args []string) error {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	var o probeOptions
	fs.StringVar(&o.Monitor, "mon", "", "MON endpoint, e.g. v2:IP:3300")
	fs.StringVar(&o.Closed, "closed", "", "endpoint without a listener for the no-TCP phase")
	fs.StringVar(&o.Entity, "entity", "client.admin", "CephX client identity")
	fs.StringVar(&o.KeyFile, "key-file", "", "file holding the disposable base64 CephX key")
	fs.StringVar(&o.Mode, "mode", "secure", "crc or secure; never downgraded")
	fs.StringVar(&o.TracePath, "trace", "probe-trace.json", "client correlation trace output")
	fs.StringVar(&o.RestartSignal, "restart-request", "", "file created when the MON may be restarted")
	fs.StringVar(&o.RestartDone, "restart-done", "", "file the workflow creates after the restart")
	fs.StringVar(&o.Substitute, "substitution", "", "label for the development capture substitution")
	fs.StringVar(&o.DialHold, "dial-hold", "", "while this file exists, new dials wait for its removal")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if o.Monitor == "" || o.KeyFile == "" {
		return fmt.Errorf("-mon and -key-file are required")
	}
	return runProbe(o)
}

func analyzeMain(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("analyze", flag.ContinueOnError)
	pcapPath := fs.String("pcap", "", "classic pcap capture")
	manifestPath := fs.String("segments", "", "capture manifest of consecutive segment files (instead of -pcap)")
	tracePath := fs.String("trace", "", "optional probe correlation trace")
	jsonPath := fs.String("json", "", "optional JSON report output")
	ports := fs.String("server-port", "3300", "comma-separated Messenger server ports for midstream orientation")
	maxBytes := fs.Int64("max-stream-bytes", 64<<20, "per-direction reassembly bound")
	entity := fs.String("entity", "", "client identity for opt-in passive secure decryption")
	keyFile := fs.String("key-file", "", "disposable client key for opt-in passive secure decryption")
	monLog := fs.String("mon-log", "", "optional native MON log (debug_ms>=10) for cross-checking")
	crossPath := fs.String("cross", "", "optional cross-check JSON output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if (*pcapPath == "") == (*manifestPath == "") {
		return fmt.Errorf("exactly one of -pcap and -segments is required")
	}
	opts := analysisOptions{MaxBytes: *maxBytes, FrameLimit: 64 << 20}
	for _, p := range strings.Split(*ports, ",") {
		v, err := strconv.ParseUint(strings.TrimSpace(p), 10, 16)
		if err != nil {
			return fmt.Errorf("invalid server port %q", p)
		}
		opts.ServerPorts = append(opts.ServerPorts, uint16(v))
	}
	if (*entity == "") != (*keyFile == "") {
		return fmt.Errorf("-entity and -key-file must be supplied together")
	}
	if *keyFile != "" {
		raw, err := os.ReadFile(*keyFile)
		if err != nil {
			return err
		}
		k, err := cephx.ParseKey(strings.TrimSpace(string(raw)))
		if err != nil {
			return err
		}
		opts.Entity, opts.Key = *entity, &k
	}
	if *tracePath != "" {
		raw, err := os.ReadFile(*tracePath)
		if err != nil {
			return err
		}
		var tr traceFile
		if err := json.Unmarshal(raw, &tr); err != nil {
			return fmt.Errorf("trace: %w", err)
		}
		opts.Trace = &tr
	}
	rep, err := analyzePaths(*pcapPath, *manifestPath, opts)
	if err != nil {
		return err
	}
	if *jsonPath != "" {
		if err := writeJSON(*jsonPath, rep); err != nil {
			return err
		}
	}
	printSummary(out, rep)
	if *monLog != "" {
		lf, err := os.Open(*monLog)
		if err != nil {
			return err
		}
		defer lf.Close()
		logs, err := parseMonLog(lf)
		if err != nil {
			return err
		}
		cross := crosscheck(rep, logs, opts.ServerPorts[0])
		printCross(out, cross)
		if *crossPath != "" {
			if err := writeJSON(*crossPath, cross); err != nil {
				return err
			}
		}
	}
	return nil
}

// analyzePaths analyzes one pcap file, or the segments of a capture manifest
// with their coverage checked; a coverage error fails the analysis.
func analyzePaths(pcapPath, manifestPath string, opts analysisOptions) (*report, error) {
	if manifestPath == "" {
		f, err := os.Open(pcapPath)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		return analyze(pcapPath, f, opts)
	}
	m, reports, err := readManifest(manifestPath)
	if err != nil {
		return nil, err
	}
	var inputs []pcapInput
	for _, s := range m.Segments {
		f, err := os.Open(filepath.Join(filepath.Dir(manifestPath), s.File))
		if err != nil {
			return nil, err
		}
		defer f.Close()
		inputs = append(inputs, pcapInput{Name: s.File, R: f})
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("capture manifest lists no segment")
	}
	rep, err := analyzeInputs(inputs, opts)
	if err != nil {
		return nil, err
	}
	if err := applyCoverage(rep, m, reports, opts.Trace); err != nil {
		return nil, err
	}
	return rep, nil
}

// applyCoverage checks the manifest against the files the report read and
// marks the connections that a declared gap may have interrupted.
func applyCoverage(rep *report, m *captureManifest, reports []segmentReport, tr *traceFile) error {
	files := rep.Inputs
	if len(files) == 0 {
		files = []inputInfo{rep.Input}
	}
	var seen []segmentInput
	for _, f := range files {
		seen = append(seen, segmentInput{SHA256: f.SHA256, Records: f.Records, ReadError: f.ReadError})
	}
	cov, err := checkCoverage(m, reports, seen, tr)
	if err != nil {
		return fmt.Errorf("capture coverage: %w", err)
	}
	rep.Capture = cov
	markGaps(rep, cov.Gaps)
	if len(cov.Gaps) > 0 {
		rep.Limits = append(rep.Limits, "The capture has declared gaps (capture_coverage.gaps): traffic inside them is not observed, and connections listed with capture_gaps are not shown as continuous.")
	}
	return nil
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func tagList(frames []frameInfo) string {
	var parts []string
	for _, f := range frames {
		s := f.Tag
		if f.Message != nil {
			s += fmt.Sprintf("(type=%d seq=%d tid=%d)", f.Message.Type, f.Message.Seq, f.Message.Tid)
		}
		parts = append(parts, s)
	}
	if len(parts) > 24 {
		parts = append(parts[:24], fmt.Sprintf("... %d more", len(parts)-24))
	}
	return strings.Join(parts, " ")
}

func printSummary(w io.Writer, rep *report) {
	fmt.Fprintf(w, "input %s: %d bytes sha256=%s\n", rep.Input.Name, rep.Input.Bytes, rep.Input.SHA256)
	fmt.Fprintf(w, "pcap: link=%d snaplen=%d records=%d truncated=%d non-tcp=%d\n",
		rep.Pcap.LinkType, rep.Pcap.Snaplen, rep.Pcap.Records, rep.Pcap.Truncated, rep.Pcap.Skipped)
	fmt.Fprintf(w, "secure decryption: %s\n", rep.Decryption)
	if cov := rep.Capture; cov != nil {
		fmt.Fprintf(w, "capture coverage: %s; %d segment(s), %d declared gap(s), %d packets dropped\n", cov.Source, len(cov.Segments), len(cov.Gaps), cov.Lost)
		for _, s := range cov.Segments {
			fmt.Fprintf(w, "  segment %d %s: ready %s stop %s end=%s target=%s captured=%d dropped-by-kernel=%d\n", s.Index, s.File,
				s.ReadyAt.Format(time.RFC3339Nano), s.StopRequestedAt.Format(time.RFC3339Nano), s.End, s.Target, s.Captured, s.DroppedKernel)
		}
		for _, g := range cov.Gaps {
			fmt.Fprintf(w, "  gap after segment %d: %s (%s) %s\n", g.AfterSegment, g.From.Format(time.RFC3339Nano), g.To.Sub(g.From).Round(time.Millisecond), g.Reason)
		}
		if cov.Trace != nil {
			for _, a := range cov.Trace.InGaps {
				fmt.Fprintf(w, "  trace activity in a gap: %s\n", a)
			}
		}
	}
	var origin time.Time
	if len(rep.Connections) > 0 {
		origin = rep.Connections[0].First
	}
	for _, c := range rep.Connections {
		fmt.Fprintf(w, "\nconn %d %s -> %s phase=%q t=+%s..+%s\n", c.ID, c.Client, c.Server, c.Phase,
			c.First.Sub(origin).Round(time.Millisecond), c.Last.Sub(origin).Round(time.Millisecond))
		fmt.Fprintf(w, "  tcp: %s; syn=%d", c.TCPOutcome, c.SYNs)
		if len(c.CaptureGaps) > 0 {
			fmt.Fprintf(w, " capture-gaps=%v", c.CaptureGaps)
		}
		for _, e := range c.Events {
			fmt.Fprintf(w, " %s:%s@+%s", e.From, e.Kind, e.Time.Sub(origin).Round(time.Millisecond))
		}
		fmt.Fprintln(w)
		for _, d := range []struct {
			name string
			r    dirReport
		}{{"c->s", c.C2S}, {"s->c", c.S2C}} {
			fmt.Fprintf(w, "  %s: packets=%d bytes=%d retrans=%d conflicts=%d gaps=%d acked-not-captured=%d snaplen-missing=%d\n",
				d.name, d.r.Packets, d.r.Reassembled, d.r.Retransmitted, d.r.Conflicts, len(d.r.Gaps), d.r.AckedNotCaptured, d.r.SnaplenMissing)
			if d.r.LimitExcluded > 0 {
				fmt.Fprintf(w, "    analysis limit %d bytes reached: %d captured bytes excluded (not capture loss)\n", d.r.Limit, d.r.LimitExcluded)
			}
			if m := d.r.Msgr; m != nil {
				fmt.Fprintf(w, "    frames=%d %s\n", len(m.Frames), tagList(m.Frames))
				if m.OpaqueBytes > 0 || m.Stop != "" {
					fmt.Fprintf(w, "    opaque=%d undecoded=%d stop=%q\n", m.OpaqueBytes, m.Remainder, m.Stop)
				}
			}
		}
		if c.MsgrOutcome != "" {
			fmt.Fprintf(w, "  msgr2: entity=%s mode=%s outcome=%q secret=%q\n", c.PeerEntity, c.ConMode, c.MsgrOutcome, c.SecretState)
		}
	}
	for _, u := range rep.Unmatched {
		fmt.Fprintf(w, "\nunmatched trace connection: phase=%s target=%s local=%s error=%q\n", u.Phase, u.Target, u.Local, u.DialError)
	}
	for _, c := range rep.Commands {
		fmt.Fprintf(w, "trace command: phase=%s %s attempt=%d +%s..+%s code=%d bytes=%d err=%q\n", c.Phase, c.Prefix, c.Attempt,
			c.Started.Sub(origin).Round(time.Millisecond), c.Ended.Sub(origin).Round(time.Millisecond), c.Code, c.Bytes, c.Error)
	}
}

func printCross(w io.Writer, cross []crossResult) {
	fmt.Fprintln(w, "\nnative MON log cross-check:")
	for _, c := range cross {
		logMode, logTags, logSeqs := "", 0, 0
		if c.Log != nil {
			logMode, logTags, logSeqs = c.Log.Mode, len(c.Log.RxTags), len(c.Log.TxMsgSeqs)
		}
		fmt.Fprintf(w, "  conn %d phase=%q match=%q mode capture=%s log=%s equal=%v; client tags capture=%d log=%d equal=%v prefix-equal=%v; server msg seqs capture=%d log=%d equal=%v %s\n",
			c.Connection, c.Phase, c.Match, c.CaptureMode, logMode, c.ModeEqual, len(c.CaptureTags), logTags, c.TagsEqual, c.PrefixEqual,
			len(c.CaptureSeqs), logSeqs, c.SeqsEqual, c.Note)
	}
}
