package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// mgrHandshakeLogLimit bounds the complete container log one
// mgr-handshake-log request scans, and mgrHandshakeEventLimit the events
// it retains. The MGR tests raise debug_ms to 5 only for their own run.
const (
	mgrHandshakeLogLimit   = 256 << 20
	mgrHandshakeEventLimit = 8192
)

// errHandshakeEventsExceedBound reports more matching events than the
// receipt retains. A truncated event list is never returned as complete.
var errHandshakeEventsExceedBound = errors.New("handshake log has more events than its bound")

// Pinned ceph/ceph/blob/v20.2.4/src/msg/async/ProtocolV2.cc prefixes every
// line with _conn_prefix: "--2- <own addrs> >> <peer addrs> conn(<connection>
// <protocol> <mode> :<port> s=<state> pgs=.. gs=.. cs=.. l=<lossy>
// c_cookie=<decimal> s_cookie=<decimal> reconnecting=.. rev1=.. crypto rx=.. tx=..
// comp rx=.. tx=..).<function>", and AsyncConnection.cc its own lines with
// "-- <own addrs> >> <peer addrs> conn(<connection> msgr2=<protocol> <mode>
// :<port> s=<state> l=<lossy>).<function>"; an accepted connection's peer
// address vector prints empty until its CLIENT_IDENT is handled. A Ceph log
// line starts with its
// timestamp, after an optional stderr prefix word (log_stderr_prefix).
var (
	handshakeV2Prefix   = regexp.MustCompile(`--2- (\S+) >> (\S*) conn\((0x[0-9a-f]+) 0x[0-9a-f]+ \S+ :-?\d+ s=(\S+) pgs=\d+ gs=\d+ cs=\d+ l=([01]) c_cookie=(\d+) s_cookie=(\d+) reconnecting=[01] rev1=[01] crypto rx=\S+ tx=\S+ comp rx=\S+ tx=\S+\)\.(\w+)(.*)$`)
	handshakeConnPrefix = regexp.MustCompile(`-- (\S+) >> (\S*) conn\((0x[0-9a-f]+) msgr2=0x[0-9a-f]+ \S+ :-?\d+ s=(\S+) l=([01])\)\.(\w+) injecting socket failure`)
	handshakeStamp      = regexp.MustCompile(`^(?:\S+ )?(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+[+-]\d{4}) `)
	handshakeBanner     = regexp.MustCompile(`, process (\S+), pid (\d+)`)

	// Identification frames (debug_ms 5): send_client_ident and
	// handle_client_ident carry a target, send_server_ident and
	// handle_server_ident do not. Masks, flags and cookies are hex.
	handshakeIdent     = regexp.MustCompile(`^ (?:sending|received (?:client|server)) identification:? addrs=(\S+)(?: target=(\S+))? gid=(\d+) global_seq=(\d+) features_supported=([0-9a-f]+) features_required=([0-9a-f]+) flags=([0-9a-f]+) cookie=([0-9a-f]+)$`)
	handshakeReconnect = regexp.MustCompile(`client_cookie=([0-9a-f]+) server_cookie=([0-9a-f]+) gs=(\d+) cs=(\d+) ms=(\d+)`)
	handshakeReady     = regexp.MustCompile(`^ entity=(\S+) client_cookie=([0-9a-f]+) server_cookie=([0-9a-f]+) in_seq=(\d+) out_seq=(\d+)$`)
	handshakeFull      = regexp.MustCompile(`^ received session reset full=([01])$`)
	handshakeSent      = regexp.MustCompile(`^ sending message m=0x[0-9a-f]+ seq=(\d+) ([a-z_]+)\(`)
	handshakeReceived  = regexp.MustCompile(`^ received message m=0x[0-9a-f]+ seq=(\d+) from=\S+ type=\d+ ([a-z_]+)\(`)
)

// mgrHandshakeEvent is one messenger event of a stock daemon's log on a
// connection to one requested endpoint. Time is the log's timestamp,
// Process the number of daemon start banners seen up to this line (one for
// the first process in the container log), Conn the AsyncConnection pointer
// that identifies the connection within that process, Endpoint the index of
// the requested endpoint it names. Kind is one of:
//
//   - connect, accept, stop, reset_session: the function ran
//   - client_ident_sent, client_ident_received, server_ident_sent,
//     server_ident_received: an identification frame, with its fields
//   - reconnect_sent, reconnect_received: SESSION_RECONNECT
//   - reset_received: a decoded SESSION_RESET, Full its bit
//   - reset_sent: the accepting side's decision to write SESSION_RESET;
//     Full is "1" or "0" for the fixed branches and empty for the cookie
//     mismatch branch, whose bit is the policy's resetcheck
//   - retry_sent, retry_global_sent, retry_received,
//     retry_global_received, reconnect_ok_sent, reconnect_existing:
//     the other reconnect outcomes
//   - ready: the session became READY, with its entity
//   - fault_lossy, fault_reconnect, fault_standby, fault_backoff: the
//     _fault branch taken
//   - ident_mismatch: the server identified with addresses that do not
//     include the target
//   - socket_failure_injected: ms_inject_socket_failures shut the socket
//   - message_write_attempt: write_message logged a message before
//     _try_send, so it was not necessarily transmitted; Message is its type
//     name and Seq its sequence
//   - message_received: handle_message decoded a message from the peer,
//     the delivery evidence for the sender's attempt
//
// Every *_sent kind and message_write_attempt is logged before its write
// (WRITE or _try_send) and is an attempt; only the peer's matching
// *_received kind shows that the frame arrived. reset_sent and the retry
// and reconnect_ok decisions are likewise decisions to write.
//
// Cookies are never published: ClientCookie, ServerCookie (the connection
// prefix's c_cookie and s_cookie) and Cookie (an identification or
// reconnect frame's own cookie, PeerCookie its second cookie) are "0" or a
// label "k<N>" that is equal exactly when the raw values are equal within
// one receipt.
type mgrHandshakeEvent struct {
	Line         int    `json:"line"`
	Time         string `json:"time"`
	Process      int    `json:"process"`
	Endpoint     int    `json:"endpoint"` // -1 when unattributed
	Conn         string `json:"conn"`
	Kind         string `json:"kind"`
	State        string `json:"state"`
	Lossy        string `json:"lossy"`
	Peer         string `json:"peer"`
	ClientCookie string `json:"client_cookie,omitempty"`
	ServerCookie string `json:"server_cookie,omitempty"`
	Addrs        string `json:"addrs,omitempty"`
	Target       string `json:"target,omitempty"`
	GID          string `json:"gid,omitempty"`
	GlobalSeq    string `json:"global_seq,omitempty"`
	ConnectSeq   string `json:"connect_seq,omitempty"`
	MessageSeq   string `json:"msg_seq,omitempty"`
	Supported    string `json:"features_supported,omitempty"`
	Required     string `json:"features_required,omitempty"`
	Flags        string `json:"flags,omitempty"`
	Cookie       string `json:"cookie,omitempty"`
	PeerCookie   string `json:"peer_cookie,omitempty"`
	Full         string `json:"full,omitempty"`
	Entity       string `json:"entity,omitempty"`
	Message      string `json:"message,omitempty"`
	Seq          string `json:"seq,omitempty"`
}

// mgrHandshakeProcess is one daemon start banner of the container log.
type mgrHandshakeProcess struct {
	Process int    `json:"process"`
	Time    string `json:"time"`
	Name    string `json:"name"`
	PID     string `json:"pid"`
}

// mgrHandshakeLogReceipt is the public receipt of one request. Admitted
// reports whether this run declared capMgrDaemonControl. It holds
// addresses, numeric names, sequences, masks and cookie labels, no key
// material and no raw cookie.
type mgrHandshakeLogReceipt struct {
	Label     string                `json:"label"`
	Admitted  bool                  `json:"admitted"`
	Daemon    string                `json:"daemon"`
	Endpoints []string              `json:"endpoints"`
	Processes []mgrHandshakeProcess `json:"processes"`
	Events    []mgrHandshakeEvent   `json:"events"`
	// Unattributed holds the events of connections that never named a
	// peer (Endpoint -1). An absence verdict that finds one of the kinds it
	// looks for here is inconclusive, not an absence.
	Unattributed []mgrHandshakeEvent `json:"unattributed"`
}

// cookieLabels replaces raw cookies with labels that keep only zero,
// nonzero and equality.
type cookieLabels map[string]string

func (l cookieLabels) label(raw string) string {
	value, err := strconv.ParseUint(raw, 16, 64)
	if err != nil {
		return "invalid"
	}
	if value == 0 {
		return "0"
	}
	key := strconv.FormatUint(value, 16)
	if name, ok := l[key]; ok {
		return name
	}
	name := "k" + strconv.Itoa(len(l)+1)
	l[key] = name
	return name
}

// decimalCookie rewrites a decimal cookie in hex, or "x" when it is not a
// 64-bit value (labelled "invalid").
func decimalCookie(raw string) string {
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return "x"
	}
	return strconv.FormatUint(value, 16)
}

// addressNames reports whether an address vector as Ceph prints it (one
// entry bare, several in brackets) contains the msgr2 entry exactly.
func addressNames(vector, entry string) bool {
	for _, a := range strings.Split(strings.Trim(vector, "[]"), ",") {
		if a == entry {
			return true
		}
	}
	return false
}

// classifyHandshake turns one function's message into an event, or reports
// false for a line the receipt does not keep.
// Cookies are left raw here; scanMgrHandshakes labels those of kept events.
func classifyHandshake(e *mgrHandshakeEvent, function, message string) bool {
	ident := func(kind string) bool {
		m := handshakeIdent.FindStringSubmatch(message)
		if m == nil {
			return false
		}
		e.Kind, e.Addrs, e.Target, e.GID, e.GlobalSeq = kind, m[1], m[2], m[3], m[4]
		e.Supported, e.Required, e.Flags, e.Cookie = m[5], m[6], m[7], m[8]
		return true
	}
	reconnect := func(kind string) bool {
		m := handshakeReconnect.FindStringSubmatch(message)
		if m == nil {
			return false
		}
		e.Kind, e.Cookie, e.PeerCookie = kind, m[1], m[2]
		e.GlobalSeq, e.ConnectSeq, e.MessageSeq = m[3], m[4], m[5]
		return true
	}
	switch function {
	case "connect", "accept", "stop", "reset_session":
		e.Kind = function
		return message == ""
	case "send_client_ident":
		return ident("client_ident_sent")
	case "handle_client_ident":
		return ident("client_ident_received")
	case "send_server_ident":
		return ident("server_ident_sent")
	case "handle_server_ident":
		if strings.Contains(message, " does not include ") {
			e.Kind = "ident_mismatch"
			return true
		}
		return ident("server_ident_received")
	case "send_reconnect":
		return reconnect("reconnect_sent")
	case "handle_reconnect":
		switch {
		case strings.HasPrefix(message, " received reconnect:"):
			return reconnect("reconnect_received")
		case strings.Contains(message, "no existing connection exists, reseting client"),
			strings.Contains(message, "already closed. Reseting client"):
			e.Kind, e.Full = "reset_sent", "1"
		case strings.Contains(message, "client cookie mismatch"):
			e.Kind = "reset_sent"
		case strings.Contains(message, "Asking peer to resume session"):
			e.Kind, e.Full = "reset_sent", "0"
		case strings.Contains(message, "stale global_seq"),
			strings.Contains(message, "racing replace happened"):
			e.Kind = "retry_global_sent"
		case strings.Contains(message, "stale connect_seq"):
			e.Kind = "retry_sent"
		case strings.Contains(message, "reconnect to existing"):
			e.Kind = "reconnect_existing"
		default:
			return false
		}
		return true
	case "handle_session_reset":
		m := handshakeFull.FindStringSubmatch(message)
		if m == nil {
			return false
		}
		e.Kind, e.Full = "reset_received", m[1]
		return true
	case "handle_session_retry":
		e.Kind = "retry_received"
		return strings.HasPrefix(message, " received session retry")
	case "handle_session_retry_global":
		e.Kind = "retry_global_received"
		return strings.HasPrefix(message, " received session retry global")
	case "send_reconnect_ok":
		e.Kind = "reconnect_ok_sent"
		return strings.HasPrefix(message, " sending reconnect_ok")
	case "ready":
		m := handshakeReady.FindStringSubmatch(message)
		if m == nil {
			return false
		}
		e.Kind, e.Entity, e.Cookie, e.PeerCookie = "ready", m[1], m[2], m[3]
		return true
	case "_fault":
		switch {
		case message == " on lossy channel, failing":
			e.Kind = "fault_lossy"
		case message == " initiating reconnect":
			e.Kind = "fault_reconnect"
		case strings.Contains(message, "going to standby"):
			e.Kind = "fault_standby"
		case strings.HasPrefix(message, " waiting "):
			e.Kind = "fault_backoff"
		default:
			return false
		}
		return true
	case "write_message":
		m := handshakeSent.FindStringSubmatch(message)
		if m == nil {
			return false
		}
		e.Kind, e.Seq, e.Message = "message_write_attempt", m[1], m[2]
		return true
	case "handle_message":
		m := handshakeReceived.FindStringSubmatch(message)
		if m == nil {
			return false
		}
		e.Kind, e.Seq, e.Message = "message_received", m[1], m[2]
		return true
	}
	return false
}

// scanMgrHandshakes reads one complete log within limit bytes and keeps
// the events of connections to the requested msgr2 endpoints
// ("v2:<ip>:<port>/<nonce>"). A line names its peer by its connection's
// peer address vector, the target of a sent CLIENT_IDENT or the addresses
// of a received identification. An accepted connection prints an empty
// peer vector until handle_client_ident or handle_reconnect sets it, and
// handle_reconnect logs the received SESSION_RECONNECT before that
// (pinned ProtocolV2.cc), as it logs accept and any fault before
// identification. Such a line is held with its connection (process and
// pointer) until a later line of that connection names a peer: it is kept
// when that peer is requested and dropped when it is another, and its stop
// or a new accept or connect of the pointer ends the lifetime. Lines whose
// connection never names a peer are returned as unattributed, so that an
// absence verdict can see what it could not attribute. Kept events are in
// log order (Line).
func scanMgrHandshakes(r io.Reader, endpoints []string, limit, maxEvents int) ([]mgrHandshakeProcess, []mgrHandshakeEvent, []mgrHandshakeEvent, error) {
	counted := &countedReader{r: io.LimitReader(r, int64(limit)+1)}
	scanner := bufio.NewScanner(counted)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	var processes []mgrHandshakeProcess
	var events, unattributed []mgrHandshakeEvent
	type lifetime struct {
		process int
		conn    string
	}
	pending := map[lifetime][]mgrHandshakeEvent{}
	held := 0
	match := func(vectors ...string) int {
		for i, endpoint := range endpoints {
			for _, v := range vectors {
				if v != "" && addressNames(v, endpoint) {
					return i
				}
			}
		}
		return -1
	}
	keep := func(list *[]mgrHandshakeEvent, e ...mgrHandshakeEvent) error {
		if len(events)+len(unattributed)+held+len(e) > maxEvents {
			return errHandshakeEventsExceedBound
		}
		*list = append(*list, e...)
		return nil
	}
	end := func(key lifetime) error {
		held -= len(pending[key])
		err := keep(&unattributed, pending[key]...)
		delete(pending, key)
		return err
	}
	for number := 1; scanner.Scan(); number++ {
		line := scanner.Text()
		stamp := handshakeStamp.FindStringSubmatch(line)
		if stamp == nil {
			continue
		}
		if m := handshakeBanner.FindStringSubmatch(line); m != nil && strings.Contains(line, "ceph version ") {
			processes = append(processes, mgrHandshakeProcess{Process: len(processes) + 1, Time: stamp[1], Name: m[1], PID: m[2]})
			continue
		}
		e := mgrHandshakeEvent{Line: number, Time: stamp[1], Process: len(processes), Endpoint: -1}
		if m := handshakeV2Prefix.FindStringSubmatch(line); m != nil {
			e.Peer, e.Conn, e.State, e.Lossy = m[2], m[3], m[4], m[5]
			if !classifyHandshake(&e, m[8], m[9]) {
				continue
			}
			// The prefix prints its cookies in decimal, the frames in
			// hex; labels compare the values.
			e.ClientCookie, e.ServerCookie = decimalCookie(m[6]), decimalCookie(m[7])
		} else if m := handshakeConnPrefix.FindStringSubmatch(line); m != nil {
			e.Peer, e.Conn, e.State, e.Lossy, e.Kind = m[2], m[3], m[4], m[5], "socket_failure_injected"
		} else {
			continue
		}
		key := lifetime{e.Process, e.Conn}
		if e.Kind == "accept" || e.Kind == "connect" {
			if err := end(key); err != nil {
				return nil, nil, nil, err
			}
		}
		// The addresses a daemon sends in its own identification are its
		// own, never the peer's.
		peer := []string{e.Peer}
		switch e.Kind {
		case "client_ident_sent":
			peer = append(peer, e.Target)
		case "client_ident_received", "server_ident_received":
			peer = append(peer, e.Addrs)
		}
		named := slices.ContainsFunc(peer, func(v string) bool { return v != "" })
		if !named {
			pending[key] = append(pending[key], e)
			if held++; len(events)+len(unattributed)+held > maxEvents {
				return nil, nil, nil, errHandshakeEventsExceedBound
			}
			if e.Kind == "stop" {
				// The lifetime ended without naming a peer; the pointer may
				// be reused by another connection.
				if err := end(key); err != nil {
					return nil, nil, nil, err
				}
			}
			continue
		}
		endpoint := match(peer...)
		earlier := pending[key]
		held -= len(earlier)
		delete(pending, key)
		if endpoint < 0 {
			continue // the connection's peer is another endpoint
		}
		for i := range earlier {
			earlier[i].Endpoint = endpoint
		}
		e.Endpoint = endpoint
		if err := keep(&events, append(earlier, e)...); err != nil {
			return nil, nil, nil, err
		}
	}
	if counted.n > int64(limit) {
		return nil, nil, nil, errContainerLogExceedsBound
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, nil, err
	}
	for key := range pending {
		if err := end(key); err != nil {
			return nil, nil, nil, err
		}
	}
	byLine := func(a, b mgrHandshakeEvent) int { return a.Line - b.Line }
	slices.SortFunc(events, byLine)
	slices.SortFunc(unattributed, byLine)
	// Labels follow the log order of kept events, then of unattributed
	// ones, so two receipts of one log label a cookie alike when their
	// earlier events match.
	cookies := cookieLabels{}
	for _, list := range [][]mgrHandshakeEvent{events, unattributed} {
		for i := range list {
			for _, c := range []*string{&list[i].ClientCookie, &list[i].ServerCookie, &list[i].Cookie, &list[i].PeerCookie} {
				if *c != "" {
					*c = cookies.label(*c)
				}
			}
		}
	}
	return processes, events, unattributed, nil
}

// parseHandshakeEndpoint accepts "<ip>:<port>/<nonce>" and returns its
// msgr2 entry as Ceph prints it.
func parseHandshakeEndpoint(s string) (string, error) {
	address, nonce, ok := strings.Cut(s, "/")
	if !ok {
		return "", fmt.Errorf("endpoint %q has no nonce", s)
	}
	endpoint, err := netip.ParseAddrPort(address)
	if err != nil {
		return "", fmt.Errorf("endpoint %q: %w", s, err)
	}
	if _, err := strconv.ParseUint(nonce, 10, 32); err != nil {
		return "", fmt.Errorf("endpoint %q nonce: %w", s, err)
	}
	return "v2:" + endpoint.String() + "/" + nonce, nil
}

// mgrHandshakeLog answers "<id> <label> <osd|mgr> <daemon> <endpoint>...":
// it scans that daemon container's complete log for connections to the
// given endpoints (one to eight) and replaces mgr-handshake-log.json
// atomically.
func (c *fixtureCluster) mgrHandshakeLog(ctx context.Context, out string, fields []string) error {
	if len(fields) < 5 || len(fields) > 12 || (fields[2] != "osd" && fields[2] != "mgr") {
		return errors.New("invalid MGR handshake log request")
	}
	receipt := mgrHandshakeLogReceipt{Label: fields[1], Admitted: c.profile.admits(capMgrDaemonControl), Daemon: fields[2] + "." + fields[3], Processes: []mgrHandshakeProcess{}, Events: []mgrHandshakeEvent{}, Unattributed: []mgrHandshakeEvent{}}
	for _, field := range fields[4:] {
		endpoint, err := parseHandshakeEndpoint(field)
		if err != nil {
			return err
		}
		receipt.Endpoints = append(receipt.Endpoints, endpoint)
	}
	if receipt.Admitted {
		daemon, err := c.daemon(ctx, fields[2], fields[3])
		if err != nil {
			return err
		}
		call, cancel := context.WithTimeout(ctx, time.Minute)
		err = withContainerLog(call, daemon.Logs, func(r io.Reader) error {
			processes, events, unattributed, scanErr := scanMgrHandshakes(r, receipt.Endpoints, mgrHandshakeLogLimit, mgrHandshakeEventLimit)
			if scanErr == nil {
				receipt.Processes = append(receipt.Processes, processes...)
				receipt.Events = append(receipt.Events, events...)
				receipt.Unattributed = append(receipt.Unattributed, unattributed...)
			}
			return scanErr
		})
		cancel()
		if err != nil {
			return fmt.Errorf("%s log for %s: %w", receipt.Daemon, fields[1], err)
		}
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	temporary := filepath.Join(out, "mgr-handshake-log.tmp")
	if err := os.WriteFile(temporary, data, 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, filepath.Join(out, "mgr-handshake-log.json"))
}

var _ = defineRequestControl("mgr-handshake-log", controlAfterRanked, (*fixtureCluster).mgrHandshakeLog)

var _ = collectDiagnosticFiles("mgr-handshake-log.json")
