package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Capture coverage. A capture manifest lists the consecutive segment files of
// one capture, each with the report of the session that recorded it, and
// declares every interval between two segments. The analyzer never treats
// segments as one uninterrupted capture: an interval that the manifest does
// not declare, a segment whose end, target or loss is not accounted, or
// client activity outside the covered time fails the analysis.
//
// Segment reports use the JSON field names of the ceph-testcontainers-go
// v0.5.0 capture.Report. A runner-host tcpdump segment carries the same
// fields, written by live-poc.sh, with target "not-bound": it is not bound to
// a container generation.

type captureManifest struct {
	// Source labels how the segments were recorded.
	Source   string            `json:"source"`
	Segments []manifestSegment `json:"segments"`
	Gaps     []declaredGap     `json:"gaps"`
}

type manifestSegment struct {
	// File and Report are relative to the manifest's directory.
	File   string `json:"file"`
	Report string `json:"report"`
}

// declaredGap states why the capture did not cover the interval after
// segment AfterSegment (1-based) and before the next one.
type declaredGap struct {
	AfterSegment int    `json:"after_segment"`
	Reason       string `json:"reason"`
}

type segmentStatistics struct {
	Captured           *uint64 `json:"captured,omitempty"`
	ReceivedByFilter   *uint64 `json:"received_by_filter,omitempty"`
	DroppedByKernel    *uint64 `json:"dropped_by_kernel,omitempty"`
	DroppedByInterface *uint64 `json:"dropped_by_interface,omitempty"`
}

type segmentFile struct {
	Status   string `json:"status"`
	Packets  int    `json:"packets"`
	Complete bool   `json:"complete"`
}

// segmentReport is the subset of capture.Report that coverage checks.
type segmentReport struct {
	ContainerID     string            `json:"container_id"`
	SandboxID       string            `json:"sandbox_id"`
	TargetStartedAt string            `json:"target_started_at"`
	Interface       string            `json:"interface"`
	Filter          string            `json:"filter"`
	HelperImage     string            `json:"helper_image"`
	HelperImageID   string            `json:"helper_image_id"`
	ToolVersion     string            `json:"tool_version"`
	ReadyAt         time.Time         `json:"ready_at"`
	StopRequestedAt time.Time         `json:"stop_requested_at"`
	EndedAt         time.Time         `json:"ended_at"`
	End             string            `json:"end"`
	Target          string            `json:"target"`
	Statistics      segmentStatistics `json:"statistics"`
	Retrieved       bool              `json:"retrieved"`
	RetrievedSHA256 string            `json:"retrieved_sha256"`
	File            segmentFile       `json:"file"`
}

// coverage is what the analysis report states about capture continuity.
type coverage struct {
	Source   string           `json:"source"`
	Segments []coveredSegment `json:"segments"`
	Gaps     []coverageGap    `json:"gaps"`
	Lost     uint64           `json:"lost_packets"`
	Trace    *traceCoverage   `json:"trace,omitempty"`
	Notes    []string         `json:"notes,omitempty"`
}

type coveredSegment struct {
	Index           int       `json:"index"`
	File            string    `json:"file"`
	SHA256          string    `json:"sha256"`
	Records         int       `json:"records"`
	ContainerID     string    `json:"container_id,omitempty"`
	TargetStartedAt string    `json:"target_started_at,omitempty"`
	SandboxID       string    `json:"sandbox_id,omitempty"`
	HelperImageID   string    `json:"helper_image_id,omitempty"`
	ReadyAt         time.Time `json:"ready_at"`
	StopRequestedAt time.Time `json:"stop_requested_at"`
	EndedAt         time.Time `json:"ended_at"`
	End             string    `json:"end"`
	Target          string    `json:"target"`
	Captured        uint64    `json:"captured"`
	DroppedKernel   uint64    `json:"dropped_by_kernel"`
	// DroppedIface is nil when tcpdump printed no interface counter, which
	// it does only for a nonzero count.
	DroppedIface *uint64 `json:"dropped_by_interface,omitempty"`
}

// coverageGap is an interval in which no segment was guaranteed to capture:
// from the stop request of one segment to the readiness of the next.
type coverageGap struct {
	AfterSegment int       `json:"after_segment"`
	From         time.Time `json:"from"`
	To           time.Time `json:"to"`
	Reason       string    `json:"reason"`
}

type traceCoverage struct {
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	// InGaps lists trace connections and commands active inside a gap.
	InGaps []string `json:"activity_in_gaps,omitempty"`
}

// segmentInput is one segment file as read by the analyzer.
type segmentInput struct {
	SHA256    string
	Records   int
	ReadError string
}

func readManifest(path string) (*captureManifest, []segmentReport, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var m captureManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, nil, fmt.Errorf("capture manifest: %w", err)
	}
	dir := filepath.Dir(path)
	reports := make([]segmentReport, len(m.Segments))
	for i, s := range m.Segments {
		raw, err := os.ReadFile(filepath.Join(dir, s.Report))
		if err != nil {
			return nil, nil, fmt.Errorf("segment %d report: %w", i+1, err)
		}
		if err := json.Unmarshal(raw, &reports[i]); err != nil {
			return nil, nil, fmt.Errorf("segment %d report: %w", i+1, err)
		}
	}
	return &m, reports, nil
}

// checkCoverage validates the manifest against the segment files that were
// read and the client trace, and returns the coverage, or an error for
// anything that would let a gap or a loss pass as continuous capture.
func checkCoverage(m *captureManifest, reports []segmentReport, inputs []segmentInput, tr *traceFile) (*coverage, error) {
	if len(m.Segments) == 0 {
		return nil, errors.New("capture manifest lists no segment")
	}
	if len(reports) != len(m.Segments) || len(inputs) != len(m.Segments) {
		return nil, errors.New("capture manifest, segment reports and files disagree in number")
	}
	cov := &coverage{Source: m.Source}
	for i, r := range reports {
		n := i + 1
		switch {
		case r.End != "stopped":
			return nil, fmt.Errorf("segment %d ended %q, not stopped: traffic after its end is not accounted", n, r.End)
		case r.Target != "unchanged" && r.Target != "not-bound":
			return nil, fmt.Errorf("segment %d target is %q: the file covers only the generation it joined", n, r.Target)
		case !r.Retrieved:
			return nil, fmt.Errorf("segment %d file was not retrieved", n)
		case inputs[i].ReadError != "":
			return nil, fmt.Errorf("segment %d file is partial: %s", n, inputs[i].ReadError)
		case r.RetrievedSHA256 != inputs[i].SHA256:
			return nil, fmt.Errorf("segment %d file SHA-256 %s differs from the report's %s", n, inputs[i].SHA256, r.RetrievedSHA256)
		case r.File.Status != "" && (r.File.Status != "valid" || !r.File.Complete):
			return nil, fmt.Errorf("segment %d file is %q (complete=%v)", n, r.File.Status, r.File.Complete)
		case r.File.Status != "" && r.File.Packets != inputs[i].Records:
			return nil, fmt.Errorf("segment %d report counts %d packets, file holds %d", n, r.File.Packets, inputs[i].Records)
		case r.Statistics.Captured == nil:
			return nil, fmt.Errorf("segment %d has no captured counter", n)
		case *r.Statistics.Captured != uint64(inputs[i].Records):
			return nil, fmt.Errorf("segment %d captured counter %d differs from the %d records in its file", n, *r.Statistics.Captured, inputs[i].Records)
		case r.Statistics.DroppedByKernel == nil:
			return nil, fmt.Errorf("segment %d has no kernel drop counter: its loss is unknown", n)
		case r.ReadyAt.IsZero() || r.StopRequestedAt.IsZero() || r.StopRequestedAt.Before(r.ReadyAt):
			return nil, fmt.Errorf("segment %d has no valid ready-to-stop interval", n)
		}
		if i > 0 && r.ReadyAt.Before(reports[i-1].StopRequestedAt) {
			return nil, fmt.Errorf("segment %d became ready before segment %d was stopped: segments overlap", n, i)
		}
		seg := coveredSegment{
			Index: n, File: m.Segments[i].File, SHA256: inputs[i].SHA256, Records: inputs[i].Records,
			ContainerID: r.ContainerID, TargetStartedAt: r.TargetStartedAt, SandboxID: r.SandboxID, HelperImageID: r.HelperImageID,
			ReadyAt: r.ReadyAt, StopRequestedAt: r.StopRequestedAt, EndedAt: r.EndedAt, End: r.End, Target: r.Target,
			Captured: *r.Statistics.Captured, DroppedKernel: *r.Statistics.DroppedByKernel, DroppedIface: r.Statistics.DroppedByInterface,
		}
		cov.Lost += seg.DroppedKernel
		if seg.DroppedIface != nil {
			cov.Lost += *seg.DroppedIface
		}
		cov.Segments = append(cov.Segments, seg)
	}
	declared := map[int]string{}
	for _, g := range m.Gaps {
		if g.AfterSegment < 1 || g.AfterSegment >= len(m.Segments) {
			return nil, fmt.Errorf("declared gap after segment %d has no following segment", g.AfterSegment)
		}
		if _, dup := declared[g.AfterSegment]; dup {
			return nil, fmt.Errorf("gap after segment %d is declared twice", g.AfterSegment)
		}
		if g.Reason == "" {
			return nil, fmt.Errorf("gap after segment %d is declared without a reason", g.AfterSegment)
		}
		declared[g.AfterSegment] = g.Reason
	}
	for i := 1; i < len(reports); i++ {
		reason, ok := declared[i]
		if !ok {
			return nil, fmt.Errorf("unaccounted capture gap between segment %d and %d (%s to %s)", i, i+1,
				reports[i-1].StopRequestedAt.Format(time.RFC3339Nano), reports[i].ReadyAt.Format(time.RFC3339Nano))
		}
		cov.Gaps = append(cov.Gaps, coverageGap{AfterSegment: i, From: reports[i-1].StopRequestedAt, To: reports[i].ReadyAt, Reason: reason})
	}
	if tr != nil {
		first, last := reports[0].ReadyAt, reports[len(reports)-1].StopRequestedAt
		if tr.Started.Before(first) || tr.Finished.After(last) {
			return nil, fmt.Errorf("client trace %s to %s lies outside the capture coverage %s to %s",
				tr.Started.Format(time.RFC3339Nano), tr.Finished.Format(time.RFC3339Nano), first.Format(time.RFC3339Nano), last.Format(time.RFC3339Nano))
		}
		cov.Trace = &traceCoverage{Started: tr.Started, Finished: tr.Finished, InGaps: traceInGaps(tr, cov.Gaps)}
	}
	if cov.Lost > 0 {
		cov.Notes = append(cov.Notes, fmt.Sprintf("tcpdump reported %d dropped packets: the segments are not loss-free", cov.Lost))
	}
	return cov, nil
}

func overlaps(from, to time.Time, g coverageGap) bool {
	return from.Before(g.To) && to.After(g.From)
}

// traceInGaps lists trace activity that overlaps a gap. A connection without
// a recorded close is taken to last until the trace finished.
func traceInGaps(tr *traceFile, gaps []coverageGap) []string {
	var out []string
	for _, g := range gaps {
		for _, c := range tr.Connections {
			end := c.Closed
			if end.IsZero() {
				end = tr.Finished
			}
			if c.DialError != "" {
				end = c.DialEnd
			}
			if overlaps(c.DialStart, end, g) {
				out = append(out, fmt.Sprintf("gap %d: %s connection to %s dialed %s", g.AfterSegment, c.Phase, c.Target, c.DialStart.Format(time.RFC3339Nano)))
			}
		}
		for _, c := range tr.Commands {
			if overlaps(c.Started, c.Ended, g) {
				out = append(out, fmt.Sprintf("gap %d: %s command %q attempt %d", g.AfterSegment, c.Phase, c.Prefix, c.Attempt))
			}
		}
	}
	return out
}

// markGaps flags every connection that a gap may have interrupted: one whose
// packets lie on both sides of a gap, or one that was not closed in the
// capture before a gap began. Such a connection's continuation is not
// observed, so its outcome never reads as a capture end.
func markGaps(rep *report, gaps []coverageGap) {
	for i := range rep.Connections {
		c := &rep.Connections[i]
		end := c.Last
		if !closedInCapture(c) {
			end = time.Unix(1<<62, 0)
		}
		for _, g := range gaps {
			if overlaps(c.First, end, g) {
				c.CaptureGaps = append(c.CaptureGaps, g.AfterSegment)
			}
		}
		if len(c.CaptureGaps) > 0 && c.TCPOutcome == "open at capture end" {
			c.TCPOutcome = "unobserved after a declared capture gap"
		}
	}
}

// closedInCapture reports a connection that ended inside the capture: an
// RST (including a refused dial) or FINs from both sides. An unanswered SYN
// is not closed: its retransmissions may continue into a gap.
func closedInCapture(c *connReport) bool {
	fin := map[string]bool{}
	for _, e := range c.Events {
		switch e.Kind {
		case "RST":
			return true
		case "FIN":
			fin[e.From] = true
		}
	}
	return fin["client"] && fin["server"]
}
