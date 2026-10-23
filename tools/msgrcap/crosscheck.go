package main

import (
	"bufio"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Cross-check capture interpretation against the native MON's own ProtocolV2
// log (debug_ms >= 10). The MON log is an independent decoder: it records
// every frame tag the daemon received and each message sequence it sent, in
// both CRC and secure modes, on the daemon side of the same TCP connection.

var (
	logConn    = regexp.MustCompile(`--2- .* >> (\S+) conn\((0x[0-9a-f]+) 0x[0-9a-f]+ (\S+) :`)
	logTag     = regexp.MustCompile(`handle_read_frame_dispatch tag=(\d+)`)
	logSent    = regexp.MustCompile(`write_message sending message m=0x[0-9a-f]+ seq=(\d+) (\S+?)\(`)
	logRecvMsg = regexp.MustCompile(`handle_message received message m=0x[0-9a-f]+ seq=(\d+) from=(\S+) type=(\d+)`)
)

type logConnRecord struct {
	Pointer   string   `json:"conn"`
	Peers     []string `json:"peer_addresses"`
	Mode      string   `json:"mode"`
	RxTags    []uint8  `json:"received_tags"`
	RxMsgSeqs []uint64 `json:"received_message_seqs"`
	RxTypes   []uint16 `json:"received_message_types"`
	TxMsgSeqs []uint64 `json:"sent_message_seqs"`
	TxNames   []string `json:"sent_message_names"`
	order     int
}

func parseMonLog(r io.Reader) ([]*logConnRecord, error) {
	byPtr := map[string]*logConnRecord{}
	var all []*logConnRecord
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 1<<20), 1<<20)
	for s.Scan() {
		line := s.Text()
		m := logConn.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		peer, ptr, mode := m[1], m[2], m[3]
		rec := byPtr[ptr]
		if rec == nil {
			rec = &logConnRecord{Pointer: ptr, order: len(all)}
			byPtr[ptr] = rec
			all = append(all, rec)
		}
		if len(rec.Peers) == 0 || rec.Peers[len(rec.Peers)-1] != peer {
			rec.Peers = append(rec.Peers, peer)
		}
		if mode != "unknown" {
			rec.Mode = mode
		}
		if t := logTag.FindStringSubmatch(line); t != nil {
			v, _ := strconv.Atoi(t[1])
			rec.RxTags = append(rec.RxTags, uint8(v))
		}
		if t := logSent.FindStringSubmatch(line); t != nil {
			v, _ := strconv.ParseUint(t[1], 10, 64)
			rec.TxMsgSeqs = append(rec.TxMsgSeqs, v)
			rec.TxNames = append(rec.TxNames, t[2])
		}
		if t := logRecvMsg.FindStringSubmatch(line); t != nil {
			v, _ := strconv.ParseUint(t[1], 10, 64)
			ty, _ := strconv.Atoi(t[3])
			rec.RxMsgSeqs = append(rec.RxMsgSeqs, v)
			rec.RxTypes = append(rec.RxTypes, uint16(ty))
		}
	}
	return all, s.Err()
}

type crossResult struct {
	Connection   int            `json:"capture_connection"`
	Client       string         `json:"client"`
	Phase        string         `json:"trace_phase,omitempty"`
	Match        string         `json:"log_match"`
	Log          *logConnRecord `json:"mon_log,omitempty"`
	CaptureMode  string         `json:"capture_con_mode,omitempty"`
	CaptureTags  []uint8        `json:"capture_client_tags"`
	CaptureSeqs  []uint64       `json:"capture_server_message_seqs"`
	TagsEqual    bool           `json:"client_tags_equal"`
	SeqsEqual    bool           `json:"server_message_seqs_equal"`
	ModeEqual    bool           `json:"mode_equal"`
	TagsCompared int            `json:"tags_compared"`
	Note         string         `json:"note,omitempty"`
}

// crosscheck pairs each established capture connection to the MON with one
// MON-log connection: by exact client ip:port when the log shows it, else by
// accept order among log connections that received a HELLO.
func crosscheck(rep *report, logs []*logConnRecord, monPort uint16) []crossResult {
	var candidates []*logConnRecord
	for _, l := range logs {
		if len(l.RxTags) > 0 && l.RxTags[0] == 1 {
			candidates = append(candidates, l)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].order < candidates[j].order })
	used := map[*logConnRecord]bool{}
	var out []crossResult
	conns := append([]connReport(nil), rep.Connections...)
	sort.SliceStable(conns, func(i, j int) bool { return conns[i].First.Before(conns[j].First) })
	next := 0
	for _, c := range conns {
		if c.C2S.Msgr == nil || c.C2S.Msgr.Banner == nil || !strings.HasSuffix(c.Server, ":"+strconv.Itoa(int(monPort))) {
			continue
		}
		res := crossResult{Connection: c.ID, Client: c.Client, Phase: c.Phase, CaptureMode: c.ConMode}
		for _, f := range c.C2S.Msgr.Frames {
			res.CaptureTags = append(res.CaptureTags, f.TagNumber)
		}
		for _, f := range c.S2C.Msgr.Frames {
			if f.Message != nil {
				res.CaptureSeqs = append(res.CaptureSeqs, f.Message.Seq)
			}
		}
		var match *logConnRecord
		for _, l := range candidates {
			for _, p := range l.Peers {
				if !used[l] && strings.Contains(p, c.Client+"/") {
					match, res.Match = l, "client ip:port in MON log peer address"
				}
			}
		}
		for match == nil && next < len(candidates) {
			if l := candidates[next]; !used[l] {
				match, res.Match = l, "accept order (MON log shows no client port)"
			}
			next++
		}
		if match == nil {
			res.Match = "none"
			out = append(out, res)
			continue
		}
		used[match] = true
		res.Log = match
		res.ModeEqual = match.Mode == c.ConMode
		// Opaque secure frames are absent from the capture tag list; compare
		// the decoded prefix and report whether it covers the whole log.
		n := min(len(res.CaptureTags), len(match.RxTags))
		res.TagsCompared = n
		res.TagsEqual = n > 0 && equalU8(res.CaptureTags[:n], match.RxTags[:n]) && len(res.CaptureTags) == len(match.RxTags)
		res.SeqsEqual = equalU64(res.CaptureSeqs, match.TxMsgSeqs)
		if len(res.CaptureTags) != len(match.RxTags) {
			res.Note = "capture decoded " + strconv.Itoa(len(res.CaptureTags)) + " client frames; MON logged " + strconv.Itoa(len(match.RxTags))
		}
		out = append(out, res)
	}
	return out
}

func equalU8(a, b []uint8) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
