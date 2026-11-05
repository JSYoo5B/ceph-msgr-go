package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"
)

// Parity compares the analysis of a candidate capture with that of a
// reference capture of the same run: sessions, TCP outcomes, frames, auth and
// derivation results, loss accounting, trace correlation and the MON log
// cross-check. Timestamps and pcap record numbers are excluded, since two
// observation points stamp the same packet differently and may record
// different non-TCP traffic.
//
// Reference connections that overlap a gap of the candidate's coverage
// cannot be in the candidate's files and are listed, not compared. Full
// parity requires that there are none, so a candidate with a gap in which
// the reference saw traffic never has full parity.

type parityResult struct {
	ReferenceSource string   `json:"reference_source"`
	CandidateSource string   `json:"candidate_source"`
	ReferenceLost   uint64   `json:"reference_lost_packets"`
	CandidateLost   uint64   `json:"candidate_lost_packets"`
	CandidateGaps   int      `json:"candidate_gaps"`
	Compared        int      `json:"compared_connections"`
	CrossCompared   int      `json:"compared_crosscheck_rows"`
	ReferenceInGaps []string `json:"reference_connections_in_candidate_gaps,omitempty"`
	CandidateMarked []string `json:"candidate_connections_marked_by_gaps,omitempty"`
	Mismatches      []string `json:"mismatches,omitempty"`
	// OutsideGaps holds when every compared item is equal and both captures
	// are loss-free; Full also requires that no traffic fell in a gap.
	OutsideGaps bool   `json:"parity_outside_gaps"`
	Full        bool   `json:"full_parity"`
	Verdict     string `json:"verdict"`
}

func parityMain(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("parity", flag.ContinueOnError)
	refPath := fs.String("reference", "", "analysis JSON of the reference capture")
	candPath := fs.String("candidate", "", "analysis JSON of the candidate capture")
	refCross := fs.String("reference-cross", "", "optional MON log cross-check JSON of the reference")
	candCross := fs.String("candidate-cross", "", "optional MON log cross-check JSON of the candidate")
	jsonPath := fs.String("json", "", "optional parity JSON output")
	requireFull := fs.Bool("require-full", false, "fail unless full parity holds")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *refPath == "" || *candPath == "" || (*refCross == "") != (*candCross == "") {
		return errors.New("-reference and -candidate are required; cross-check files go together")
	}
	var ref, cand report
	if err := readJSON(*refPath, &ref); err != nil {
		return err
	}
	if err := readJSON(*candPath, &cand); err != nil {
		return err
	}
	var rc, cc []crossResult
	if *refCross != "" {
		if err := readJSON(*refCross, &rc); err != nil {
			return err
		}
		if err := readJSON(*candCross, &cc); err != nil {
			return err
		}
	}
	res, err := parity(&ref, &cand, rc, cc)
	if err != nil {
		return err
	}
	if *jsonPath != "" {
		if err := writeJSON(*jsonPath, res); err != nil {
			return err
		}
	}
	fmt.Fprintf(out, "parity %s vs %s: %s\n", res.CandidateSource, res.ReferenceSource, res.Verdict)
	fmt.Fprintf(out, "  compared connections=%d cross-check rows=%d lost reference=%d candidate=%d candidate gaps=%d\n",
		res.Compared, res.CrossCompared, res.ReferenceLost, res.CandidateLost, res.CandidateGaps)
	for _, s := range res.ReferenceInGaps {
		fmt.Fprintf(out, "  reference traffic in a candidate gap: %s\n", s)
	}
	for _, s := range res.CandidateMarked {
		fmt.Fprintf(out, "  candidate connection marked by a gap: %s\n", s)
	}
	for _, s := range res.Mismatches {
		fmt.Fprintf(out, "  MISMATCH %s\n", s)
	}
	switch {
	case !res.OutsideGaps:
		return errors.New("capture parity failed outside the declared gaps")
	case *requireFull && !res.Full:
		return errors.New("full capture parity is required")
	}
	return nil
}

func readJSON(path string, v any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// parity compares two analyses. Both must carry capture coverage, so that
// their loss is accounted.
func parity(ref, cand *report, refCross, candCross []crossResult) (*parityResult, error) {
	if ref.Capture == nil || cand.Capture == nil {
		return nil, errors.New("parity needs the capture coverage of both analyses (analyze with -segments)")
	}
	res := &parityResult{
		ReferenceSource: ref.Capture.Source, CandidateSource: cand.Capture.Source,
		ReferenceLost: ref.Capture.Lost, CandidateLost: cand.Capture.Lost, CandidateGaps: len(cand.Capture.Gaps),
	}
	mismatch := func(format string, args ...any) {
		res.Mismatches = append(res.Mismatches, fmt.Sprintf(format, args...))
	}
	if res.ReferenceLost != 0 || res.CandidateLost != 0 {
		mismatch("capture loss: reference dropped %d packets, candidate %d", res.ReferenceLost, res.CandidateLost)
	}
	if ref.Decryption != cand.Decryption {
		mismatch("secure decryption: %q != %q", ref.Decryption, cand.Decryption)
	}
	if ref.Pcap.LinkType != cand.Pcap.LinkType || ref.Pcap.Snaplen != cand.Pcap.Snaplen || ref.Pcap.Truncated != cand.Pcap.Truncated {
		mismatch("pcap link/snaplen/truncation: %d/%d/%d != %d/%d/%d", ref.Pcap.LinkType, ref.Pcap.Snaplen, ref.Pcap.Truncated,
			cand.Pcap.LinkType, cand.Pcap.Snaplen, cand.Pcap.Truncated)
	}
	inGap := func(c connReport) bool {
		for _, g := range cand.Capture.Gaps {
			if overlaps(c.First, c.Last, g) || (c.First.Equal(c.Last) && !c.First.Before(g.From) && !c.First.After(g.To)) {
				return true
			}
		}
		return false
	}
	byKey := map[string][]connReport{}
	for _, c := range cand.Connections {
		k := c.Client + " -> " + c.Server
		byKey[k] = append(byKey[k], c)
		if len(c.CaptureGaps) > 0 {
			res.CandidateMarked = append(res.CandidateMarked, fmt.Sprintf("%s (gaps %v, %s)", k, c.CaptureGaps, c.TCPOutcome))
		}
	}
	matched := map[string]int{}
	gapClients := map[string]bool{}
	for _, r := range ref.Connections {
		k := r.Client + " -> " + r.Server
		if inGap(r) {
			res.ReferenceInGaps = append(res.ReferenceInGaps, fmt.Sprintf("%s phase=%q %s", k, r.Phase, r.TCPOutcome))
			gapClients[r.Client] = true
			continue
		}
		i := matched[k]
		if i >= len(byKey[k]) {
			mismatch("%s phase=%q: absent from the candidate", k, r.Phase)
			continue
		}
		matched[k] = i + 1
		res.Compared++
		for _, d := range diffJSON(normalizeConn(r), normalizeConn(byKey[k][i])) {
			mismatch("%s #%d: %s", k, i+1, d)
		}
	}
	for k, cs := range byKey {
		for i := matched[k]; i < len(cs); i++ {
			mismatch("%s phase=%q: absent from the reference", k, cs[i].Phase)
		}
	}
	// A trace connection whose dial overlaps a candidate gap cannot match a
	// candidate connection; the reference traffic it caused is listed above.
	for _, d := range diffJSON(traceUnmatched(ref.Unmatched, cand.Capture.Gaps), traceUnmatched(cand.Unmatched, cand.Capture.Gaps)) {
		mismatch("unmatched trace connections: %s", d)
	}
	refCross, candCross = crossOutsideGaps(refCross, gapClients), crossOutsideGaps(candCross, gapClients)
	if len(refCross) != len(candCross) {
		mismatch("MON log cross-check rows: %d != %d", len(refCross), len(candCross))
	} else {
		for i := range refCross {
			res.CrossCompared++
			for _, d := range diffJSON(normalizeCross(refCross[i]), normalizeCross(candCross[i])) {
				mismatch("cross-check row %d: %s", i+1, d)
			}
		}
	}
	sort.Strings(res.Mismatches)
	res.OutsideGaps = len(res.Mismatches) == 0
	res.Full = res.OutsideGaps && len(res.ReferenceInGaps) == 0 && len(res.CandidateMarked) == 0
	switch {
	case res.Full:
		res.Verdict = "full parity"
	case res.OutsideGaps:
		res.Verdict = fmt.Sprintf("parity outside declared gaps only: %d reference connection(s) in candidate gaps, %d candidate connection(s) marked by gaps",
			len(res.ReferenceInGaps), len(res.CandidateMarked))
	default:
		res.Verdict = fmt.Sprintf("%d mismatch(es)", len(res.Mismatches))
	}
	return res, nil
}

// normalizeConn drops what two observation points legitimately disagree on:
// timestamps, pcap record numbers and tracker IDs. Gap marks are compared
// separately.
func normalizeConn(c connReport) connReport {
	c.ID, c.First, c.Last, c.CaptureGaps = 0, time.Time{}, time.Time{}, nil
	c.Events = append([]tcpEvent(nil), c.Events...)
	for i := range c.Events {
		c.Events[i].Time, c.Events[i].Packet = time.Time{}, 0
	}
	for _, d := range []*dirReport{&c.C2S, &c.S2C} {
		if d.Msgr == nil {
			continue
		}
		m := *d.Msgr
		m.Frames = append([]frameInfo(nil), m.Frames...)
		for i := range m.Frames {
			m.Frames[i].Time = time.Time{}
		}
		d.Msgr = &m
	}
	return c
}

func traceUnmatched(list []traceConn, gaps []coverageGap) []string {
	var out []string
	for _, t := range list {
		if slices.ContainsFunc(gaps, func(g coverageGap) bool { return overlaps(t.DialStart, t.DialEnd, g) }) {
			continue
		}
		out = append(out, fmt.Sprintf("%s %s local=%s error=%q", t.Phase, t.Target, t.Local, t.DialError))
	}
	sort.Strings(out)
	return out
}

func crossOutsideGaps(rows []crossResult, gapClients map[string]bool) []crossResult {
	var out []crossResult
	for _, r := range rows {
		if !gapClients[r.Client] {
			out = append(out, r)
		}
	}
	return out
}

func normalizeCross(c crossResult) crossResult {
	c.Connection = 0
	return c
}

// diffJSON compares the JSON forms of two values and lists differing paths.
func diffJSON(a, b any) []string {
	var x, y any
	for _, p := range []struct {
		v   any
		dst *any
	}{{a, &x}, {b, &y}} {
		raw, err := json.Marshal(p.v)
		if err != nil {
			return []string{"marshal: " + err.Error()}
		}
		if err := json.Unmarshal(raw, p.dst); err != nil {
			return []string{"unmarshal: " + err.Error()}
		}
	}
	var out []string
	diffValue("", x, y, &out)
	return out
}

func diffValue(path string, a, b any, out *[]string) {
	if len(*out) >= 50 {
		return
	}
	am, aok := a.(map[string]any)
	bm, bok := b.(map[string]any)
	if aok && bok {
		keys := map[string]bool{}
		for k := range am {
			keys[k] = true
		}
		for k := range bm {
			keys[k] = true
		}
		var sorted []string
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		for _, k := range sorted {
			diffValue(strings.TrimPrefix(path+"."+k, "."), am[k], bm[k], out)
		}
		return
	}
	as, aok := a.([]any)
	bs, bok := b.([]any)
	if aok && bok {
		if len(as) != len(bs) {
			*out = append(*out, fmt.Sprintf("%s: %d items != %d items", orRoot(path), len(as), len(bs)))
		}
		for i := 0; i < min(len(as), len(bs)); i++ {
			diffValue(fmt.Sprintf("%s[%d]", path, i), as[i], bs[i], out)
		}
		return
	}
	if !reflect.DeepEqual(a, b) {
		*out = append(*out, fmt.Sprintf("%s: %v != %v", orRoot(path), short(a), short(b)))
	}
}

func orRoot(path string) string {
	if path == "" {
		return "value"
	}
	return path
}

func short(v any) string {
	s := fmt.Sprint(v)
	if v == nil {
		s = "absent"
	}
	if len(s) > 80 {
		s = s[:77] + "..."
	}
	return s
}
