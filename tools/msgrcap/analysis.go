package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"path/filepath"
	"sort"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
)

type analysisOptions struct {
	ServerPorts []uint16
	MaxBytes    int64
	FrameLimit  uint32
	// Keys enables passive secure-frame decryption for MON connections. It is
	// an explicit development option; derived secrets are never reported.
	Entity string
	Key    *cephx.Key
	Trace  *traceFile
}

type report struct {
	Tool        string         `json:"tool"`
	Input       inputInfo      `json:"input"`
	Pcap        pcapInfo       `json:"pcap"`
	Decryption  string         `json:"secure_decryption"`
	Connections []connReport   `json:"connections"`
	Unmatched   []traceConn    `json:"unmatched_trace_connections,omitempty"`
	Commands    []traceCommand `json:"trace_commands,omitempty"`
	Limits      []string       `json:"limits"`
}

type inputInfo struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type dirReport struct {
	Packets          int           `json:"packets"`
	Reassembled      int64         `json:"reassembled_bytes"`
	Retransmitted    int           `json:"retransmitted_segments"`
	Conflicts        int           `json:"conflicting_overlaps"`
	Gaps             []gap         `json:"gaps,omitempty"`
	BytesAfterGap    int64         `json:"bytes_after_first_gap,omitempty"`
	SnaplenMissing   int           `json:"snaplen_missing_bytes,omitempty"`
	AckedNotCaptured int64         `json:"peer_acked_bytes_missing_from_capture,omitempty"`
	BeforeBase       int           `json:"segments_before_stream_base,omitempty"`
	SHA256           string        `json:"reassembled_sha256"`
	Msgr             *streamResult `json:"msgr2,omitempty"`
}

type connReport struct {
	ID          int        `json:"id"`
	Client      string     `json:"client"`
	Server      string     `json:"server"`
	Phase       string     `json:"trace_phase,omitempty"`
	First       time.Time  `json:"first_packet"`
	Last        time.Time  `json:"last_packet"`
	SYNs        int        `json:"syn_count"`
	Midstream   bool       `json:"capture_started_midstream,omitempty"`
	TCPOutcome  string     `json:"tcp_outcome"`
	Events      []tcpEvent `json:"tcp_events"`
	ConMode     string     `json:"con_mode,omitempty"`
	PeerEntity  string     `json:"server_entity,omitempty"`
	MsgrOutcome string     `json:"msgr_outcome,omitempty"`
	SecretState string     `json:"secret_derivation,omitempty"`
	C2S         dirReport  `json:"client_to_server"`
	S2C         dirReport  `json:"server_to_client"`
}

var entityNames = map[uint8]string{1: "mon", 2: "mds", 4: "osd", 8: "client", 16: "mgr"}

func analyze(name string, r io.Reader, opts analysisOptions) (*report, error) {
	h := sha256.New()
	counter := &countingReader{r: io.TeeReader(r, h)}
	pr, err := newPcapReader(counter)
	if err != nil {
		return nil, err
	}
	t := newTracker(opts.ServerPorts, opts.MaxBytes)
	var readErr error
	for {
		p, err := pr.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			readErr = err // keep what was read; report the partial file
			break
		}
		t.add(p)
	}
	_, _ = io.Copy(io.Discard, counter)
	rep := &report{
		Tool:  "msgrcap analyze (development-only)",
		Input: inputInfo{Name: filepath.Base(name), Bytes: counter.n, SHA256: hex.EncodeToString(h.Sum(nil))},
		Pcap:  pr.info,
		Limits: []string{
			"Frames are decoded only from the contiguous reassembled prefix of each direction; data after the first gap is counted, not decoded.",
			"Segment payloads, keys, tickets and connection secrets are never written to this report.",
			"A capture does not show local client state, cancellations or unsent requests; use the client trace for those.",
		},
	}
	if readErr != nil {
		rep.Limits = append(rep.Limits, "Capture file ended early: "+readErr.Error())
	}
	var auth *cephx.Client
	switch {
	case opts.Key == nil:
		rep.Decryption = "disabled: secure frames after AUTH_DONE are reported as opaque bytes"
	default:
		auth, err = cephx.NewClient(opts.Entity, *opts.Key, 0)
		if err != nil {
			return nil, err
		}
		rep.Decryption = "enabled for MON connections by passive CephX derivation from an explicitly supplied disposable client key"
	}
	for _, c := range t.conns {
		rep.Connections = append(rep.Connections, analyzeConn(c, opts, auth))
	}
	if opts.Trace != nil {
		correlate(rep, opts.Trace)
	}
	return rep, nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func tcpOutcome(c *tcpConn) string {
	if !c.Established {
		for _, e := range c.Events {
			if e.Kind == "RST" && e.From == "server" {
				return "refused (SYN answered by RST)"
			}
		}
		if c.SYNs > 0 {
			return "unanswered SYN"
		}
	}
	for _, e := range c.Events {
		switch e.Kind {
		case "FIN", "RST":
			return e.From + " " + e.Kind + " first"
		}
	}
	if c.Midstream {
		return "partial: no handshake or close in capture"
	}
	return "open at capture end"
}

func dirSummary(d *direction, peer *direction) dirReport {
	sum := sha256.Sum256(d.stream)
	return dirReport{
		Packets: d.packets, Reassembled: int64(len(d.stream)), Retransmitted: d.retransmitted,
		Conflicts: d.conflicts, Gaps: d.gaps, BytesAfterGap: d.afterGap, SnaplenMissing: d.missing,
		AckedNotCaptured: d.ackedMissing(peer.maxAck, peer.haveAck), BeforeBase: d.beforeBase,
		SHA256: hex.EncodeToString(sum[:]),
	}
}

func analyzeConn(c *tcpConn, opts analysisOptions, auth *cephx.Client) connReport {
	c.c2s.reassemble()
	c.s2c.reassemble()
	cr := connReport{
		ID: c.ID, Client: c.Client.String(), Server: c.Server.String(), First: c.First, Last: c.Last,
		SYNs: c.SYNs, Midstream: c.Midstream, TCPOutcome: tcpOutcome(c), Events: c.Events,
		C2S: dirSummary(&c.c2s, &c.s2c), S2C: dirSummary(&c.s2c, &c.c2s),
	}
	if len(c.c2s.stream) == 0 && len(c.s2c.stream) == 0 {
		return cr
	}
	if c.Midstream {
		cr.MsgrOutcome = "not decoded: capture started after the TCP handshake"
		return cr
	}
	limit := opts.FrameLimit
	srv := &walker{stream: c.s2c.stream, dir: &c.s2c, res: &streamResult{Bytes: int64(len(c.s2c.stream))}, limit: limit}
	cli := &walker{stream: c.c2s.stream, dir: &c.c2s, res: &streamResult{Bytes: int64(len(c.c2s.stream))}, limit: limit}
	cr.S2C.Msgr, cr.C2S.Msgr = srv.res, cli.res
	srvOK, cliOK := srv.banner(), cli.banner()

	// Server: plaintext CRC framing until AUTH_DONE.
	var done *frameInfo
	replyMore := 0
	for srvOK && done == nil {
		f, segs, err := srv.crcFrame()
		if err != nil {
			srv.stopped(err)
			srvOK = false
			break
		}
		annotate(&f, segs)
		srv.res.Frames = append(srv.res.Frames, f)
		switch f.TagNumber {
		case 1:
			if f.EntityType != nil {
				cr.PeerEntity = entityNames[*f.EntityType]
			}
		case 3:
			cr.MsgrOutcome = "authentication refused (AUTH_BAD_METHOD)"
			srvOK = false
		case 4:
			replyMore++
		case tagAuthDone:
			done = &srv.res.Frames[len(srv.res.Frames)-1]
		}
	}
	mode := uint32(0)
	if done != nil && done.ConMode != nil {
		mode = *done.ConMode
		cr.ConMode = map[uint32]string{modeCRC: "crc", modeSecure: "secure"}[mode]
		if cr.ConMode == "" {
			cr.ConMode = fmt.Sprintf("unknown(%d)", mode)
		}
	}
	var rx, tx *gcm // server->client, client->server
	if mode == modeSecure || (mode == modeCRC && auth != nil) {
		cr.SecretState = "not attempted"
		if auth == nil {
			cr.SecretState = "no key supplied: secure frames remain opaque"
		} else if cr.PeerEntity != "mon" {
			cr.SecretState = "not implemented for service authorizers (only MON CephX exchanges are derived)"
		} else if payload, ok := authDonePayload(*done); ok {
			_, secret, err := auth.Finish(*done.GlobalID, payload, mode == modeSecure)
			switch {
			case err != nil:
				cr.SecretState = "derivation failed: " + err.Error()
			case mode == modeCRC:
				cr.SecretState = "CRC mode: CephX ticket state followed; no connection secret is used"
			case len(secret) < 40:
				cr.SecretState = "derivation failed: short connection secret"
			default:
				rx, err = newGCMState(secret[:16], secret[16:28])
				if err == nil {
					tx, err = newGCMState(secret[:16], secret[28:40])
				}
				cr.SecretState = "derived from captured AUTH_DONE"
				if err != nil {
					cr.SecretState = "derivation failed: " + err.Error()
					rx, tx = nil, nil
				}
			}
		}
	}
	// Server: post-auth frames.
	if srvOK && done != nil {
		srv.post(mode, rx)
	}
	// Client: HELLO, AUTH_REQUEST and one AUTH_REQUEST_MORE per server
	// AUTH_REPLY_MORE are plaintext; the next frame uses the selected mode.
	preAuth := 2 + replyMore
	for cliOK && len(cli.res.Frames) < preAuth {
		f, segs, err := cli.crcFrame()
		if err != nil {
			cli.stopped(err)
			cliOK = false
			break
		}
		annotate(&f, segs)
		cli.res.Frames = append(cli.res.Frames, f)
	}
	if cliOK {
		if done != nil {
			cli.post(mode, tx)
		} else if cli.pos < int64(len(cli.stream)) {
			cli.res.Remainder = int64(len(cli.stream)) - cli.pos
			cli.res.Stop = "client data after authentication frames without a captured AUTH_DONE"
		}
	}
	if cr.MsgrOutcome == "" {
		cr.MsgrOutcome = msgrOutcome(cr, srv.res, cli.res, done != nil)
	}
	return cr
}

func (w *walker) stopped(err error) {
	w.res.Remainder = int64(len(w.stream)) - w.pos
	if errors.Is(err, errIncomplete) {
		w.res.Stop = err.Error()
		return
	}
	w.res.Stop = "decode error: " + err.Error()
}

func (w *walker) post(mode uint32, g *gcm) {
	for w.pos < int64(len(w.stream)) {
		var f frameInfo
		var segs [][]byte
		var err error
		switch {
		case mode == modeCRC:
			f, segs, err = w.crcFrame()
		case mode == modeSecure && g != nil:
			f, segs, err = w.secureFrame(g)
		case mode == modeSecure:
			w.res.OpaqueBytes = int64(len(w.stream)) - w.pos
			w.res.Stop = "secure frames are opaque without a connection secret"
			return
		default:
			w.res.Remainder = int64(len(w.stream)) - w.pos
			w.res.Stop = "unknown connection mode"
			return
		}
		if err != nil {
			w.stopped(err)
			return
		}
		annotate(&f, segs)
		f.payload0 = nil
		w.res.Frames = append(w.res.Frames, f)
	}
}

func msgrOutcome(cr connReport, srv, cli *streamResult, authDone bool) string {
	has := func(r *streamResult, tag uint8) bool {
		for _, f := range r.Frames {
			if f.TagNumber == tag {
				return true
			}
		}
		return false
	}
	switch {
	case srv.Banner == nil && cli.Banner == nil:
		return "no msgr2 banner exchanged"
	case !authDone:
		return "closed or stopped before AUTH_DONE"
	case has(srv, 9):
		if has(srv, tagMessage) || has(cli, tagMessage) {
			return "session established; messages exchanged"
		}
		return "session established (SERVER_IDENT)"
	case srv.OpaqueBytes > 0 || cli.OpaqueBytes > 0:
		return "authenticated; post-auth secure traffic opaque"
	default:
		return "authenticated; no SERVER_IDENT decoded"
	}
}

// correlate joins the client trace to TCP connections by exact local/remote
// address, and failed dials by target and time window.
func correlate(rep *report, tr *traceFile) {
	rep.Commands = tr.Commands
	used := make(map[int]bool)
	for _, tc := range tr.Connections {
		matched := false
		for i := range rep.Connections {
			c := &rep.Connections[i]
			if used[c.ID] {
				continue
			}
			if tc.Local != "" && sameAddr(tc.Local, c.Client) && sameAddr(tc.Remote, c.Server) {
				c.Phase, used[c.ID], matched = tc.Phase, true, true
				break
			}
			if tc.Local == "" && tc.DialError != "" && sameAddr(tc.Target, c.Server) &&
				!c.First.Before(tc.DialStart.Add(-time.Second)) && !c.First.After(tc.DialEnd.Add(time.Second)) {
				c.Phase, used[c.ID], matched = tc.Phase+" (failed dial)", true, true
				break
			}
		}
		if !matched {
			rep.Unmatched = append(rep.Unmatched, tc)
		}
	}
	sort.SliceStable(rep.Connections, func(i, j int) bool { return rep.Connections[i].First.Before(rep.Connections[j].First) })
}

func sameAddr(a, b string) bool {
	x, err1 := netip.ParseAddrPort(a)
	y, err2 := netip.ParseAddrPort(b)
	if err1 != nil || err2 != nil {
		return a == b
	}
	return x.Addr().Unmap() == y.Addr().Unmap() && x.Port() == y.Port()
}
