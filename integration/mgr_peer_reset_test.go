package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// statefulIdentListener is a test-only fault injection for a CRC-mode Go
// MGR listener: the first SERVER_IDENT written on any accepted connection
// is rewritten to claim a stateful session (lossy flag cleared, a nonzero
// cookie), with its CRC recomputed. A stock connector adopts the lossy bit
// of the SERVER_IDENT it receives (ProtocolV2::handle_server_ident), so its
// next reconnection resumes with SESSION_RECONNECT, which the lossy Go
// acceptor answers with SESSION_RESET. SERVER_IDENT follows AUTH_SIGNATURE,
// so the rewrite touches no signed transcript. Secure frames are encrypted
// and cannot be rewritten; a secure stream fails here instead.
type statefulIdentListener struct {
	net.Listener
	cookie    uint64
	claimed   atomic.Bool
	rewritten atomic.Bool
}

func (l *statefulIdentListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &identRewriteConn{Conn: conn, listener: l}, nil
}

type identRewriteConn struct {
	net.Conn
	listener    *statefulIdentListener
	mu          sync.Mutex
	pending     []byte
	banner      bool
	passthrough bool
	err         error
}

func (c *identRewriteConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.passthrough {
		return c.Conn.Write(p)
	}
	if c.err != nil {
		return 0, c.err
	}
	if len(c.pending)+len(p) > 1<<20 {
		c.err = errors.New("SERVER_IDENT rewrite exceeds its pending-byte bound")
		return 0, c.err
	}
	c.pending = append(c.pending, p...)
	if c.err = c.forward(); c.err != nil {
		return 0, c.err
	}
	return len(p), nil
}

// forward writes every complete banner or frame pending, rewriting the
// listener's first SERVER_IDENT, and passes everything after a
// SERVER_IDENT through unparsed.
func (c *identRewriteConn) forward() error {
	if !c.banner {
		if len(c.pending) < 10 {
			return nil
		}
		n := 10 + int(binary.LittleEndian.Uint16(c.pending[8:10]))
		if len(c.pending) < n {
			return nil
		}
		if err := msgr.WriteFull(c.Conn, c.pending[:n]); err != nil {
			return err
		}
		c.pending, c.banner = c.pending[n:], true
	}
	for len(c.pending) > 0 && !c.passthrough {
		input := bytes.NewReader(c.pending)
		frame, err := msgr.NewReader(input, 1<<20).Read()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("SERVER_IDENT rewrite: %w", err)
		}
		consumed := len(c.pending) - input.Len()
		out := c.pending[:consumed]
		if frame.Tag == msgr.ServerIdent {
			c.passthrough = true
			if seg := frame.Segments[0]; len(frame.Segments) == 1 && len(seg) >= 16 && c.listener.claimed.CompareAndSwap(false, true) {
				seg = bytes.Clone(seg)
				binary.LittleEndian.PutUint64(seg[len(seg)-16:], 0) // flags: not CEPH_MSG_CONNECT_LOSSY
				binary.LittleEndian.PutUint64(seg[len(seg)-8:], c.listener.cookie)
				var rewritten bytes.Buffer
				if err := msgr.NewWriter(&rewritten, 1<<20).Write(msgr.Frame{Tag: frame.Tag, Segments: [][]byte{seg}, Alignment: frame.Alignment}); err != nil {
					return err
				}
				out = rewritten.Bytes()
				c.listener.rewritten.Store(true)
			}
		}
		if err := msgr.WriteFull(c.Conn, out); err != nil {
			return err
		}
		c.pending = c.pending[consumed:]
	}
	if c.passthrough && len(c.pending) > 0 {
		if err := msgr.WriteFull(c.Conn, c.pending); err != nil {
			return err
		}
		c.pending = nil
	}
	return nil
}

// v2Endpoint is the "<ip>:<port>/<nonce>" of an address vector's msgr2
// entry.
func v2Endpoint(t *testing.T, addrvec []nativeMgrAddress) string {
	t.Helper()
	for _, a := range addrvec {
		if a.Type == "v2" {
			return fmt.Sprintf("%s/%d", a.Addr, a.Nonce)
		}
	}
	t.Fatal("no msgr2 entry in", addrvec)
	return ""
}

// resumeKinds are the events of a resumed (stateful) session on either
// side: their absence on a connection is what "no SESSION_RESET" means
// there, together with the reset events themselves.
var resumeKinds = []string{"reconnect_sent", "reconnect_received", "reset_received", "reset_sent", "retry_sent", "retry_received", "retry_global_sent", "retry_global_received", "reconnect_ok_sent", "reconnect_existing"}

// pairSession is one healthy post-fault session of the stock pair: the
// osd.0 connection, the MGR connection that served it, and their events.
type pairSession struct {
	OSDConn, MGRConn     string
	OSDEvents, MGREvents []handshakeEvent
}

// healthyPair finds, from since on, a fresh lossy session of osd.0 with
// the MGR (gid) and its delivery evidence, each side within one session of
// one connection lifetime in that daemon's latest process (lifetimes,
// sessions): pointers are reused, message sequences restart with every
// lossy session and a global sequence restarts with every process. On
// osd.0's session (endpoint 0 is the MGR): CLIENT_IDENT (flags 1, cookie
// 0), SERVER_IDENT from gid (flags 1, cookie 0), READY with no server
// cookie, then write attempts of MMgrOpen and MMgrReport. Those are
// attempts only (write_message logs before _try_send), so one MGR session
// must show the same exchange, a received CLIENT_IDENT with the OSD's
// global_seq and a sent SERVER_IDENT with the global_seq the OSD received,
// and the receipt of that MMgrOpen and MMgrReport by sequence. The error
// names how far the best candidate got.
func healthyPair(osd, mgr handshakeLog, since time.Time, gid uint64) (pairSession, error) {
	latest := func(l handshakeLog) int {
		if len(l.Processes) == 0 {
			return 0
		}
		return l.Processes[len(l.Processes)-1].Process
	}
	recent := func(process int, endpoint bool) func(handshakeEvent) bool {
		return func(e handshakeEvent) bool {
			at, err := e.stamp()
			return err == nil && !at.Before(since) && e.Process == process && (!endpoint || e.Endpoint == 0)
		}
	}
	if len(mgr.Processes) == 0 {
		return pairSession{}, fmt.Errorf("%s log has no daemon start banner", mgr.Daemon)
	}
	current := latest(mgr)
	osdLifetimes := osd.lifetimes(recent(latest(osd), true))
	mgrLifetimes := mgr.lifetimes(recent(current, false))
	want := fmt.Sprint(gid)
	best := fmt.Errorf("osd.0 logged no fresh lossy CLIENT_IDENT to the MGR since %s", since.UTC().Format(time.RFC3339Nano))
	for _, lifetime := range osdLifetimes {
		for _, session := range lifetime.sessions() {
			var clientSeq, serverSeq, openSeq, reportSeq string
			step := 0
			for _, e := range session {
				switch {
				case step == 0 && e.Kind == "client_ident_sent" && e.Flags == "1" && e.Cookie == "0":
					step, clientSeq = 1, e.GlobalSeq
				case step == 1 && e.Kind == "server_ident_received" && e.GID == want && e.Flags == "1" && e.Cookie == "0":
					step, serverSeq = 2, e.GlobalSeq
				case step == 2 && e.Kind == "ready" && e.Lossy == "1" && e.ServerCookie == "0":
					step = 3
				case step == 3 && e.Kind == "message_write_attempt" && e.Message == "mgropen":
					step, openSeq = 4, e.Seq
				case step == 4 && e.Kind == "message_write_attempt" && e.Message == "mgrreport":
					step, reportSeq = 5, e.Seq
				}
				if step == 5 {
					break
				}
			}
			if step < 5 {
				if step > 0 {
					best = fmt.Errorf("osd.0 connection %s stopped at step %d of 5 (ident, server ident, ready, open and report attempts)", lifetime.Conn, step)
				}
				continue
			}
			best = fmt.Errorf("osd.0 connection %s attempted MMgrOpen and MMgrReport, but %s process %d logged no matching session", lifetime.Conn, mgr.Daemon, current)
			for _, served := range mgrLifetimes {
				for _, receipt := range served.sessions() {
					got := 0
					for _, e := range receipt {
						switch {
						case got == 0 && e.Kind == "client_ident_received" && e.GlobalSeq == clientSeq && e.Flags == "1" && e.Cookie == "0":
							got = 1
						case got == 1 && e.Kind == "server_ident_sent" && e.GID == want && e.GlobalSeq == serverSeq && e.Flags == "1" && e.Cookie == "0":
							got = 2
						case got == 2 && e.Kind == "message_received" && e.Message == "mgropen" && e.Seq == openSeq:
							got = 3
						case got == 3 && e.Kind == "message_received" && e.Message == "mgrreport" && e.Seq == reportSeq:
							return pairSession{OSDConn: lifetime.Conn, MGRConn: served.Conn, OSDEvents: session, MGREvents: receipt}, nil
						}
					}
					if got > 0 {
						best = fmt.Errorf("osd.0 connection %s attempted MMgrOpen seq %s and MMgrReport seq %s, but %s connection %s stopped at receipt step %d of 4 (ident, server ident, open, report) within its session", lifetime.Conn, openSeq, reportSeq, mgr.Daemon, served.Conn, got)
					}
				}
			}
		}
	}
	return pairSession{}, best
}

// injectedCut returns the first socket failure injected at or after
// activated on a connection to endpoint 0 whose fault followed it within
// the same connection lifetime, with that connection. A lossy fault
// without an injected failure, an injection before activation, and a fault
// of a later lifetime of a reused pointer do not count.
func injectedCut(l handshakeLog, activated time.Time) (time.Time, string, bool) {
	for _, lifetime := range l.lifetimes(func(e handshakeEvent) bool { return e.Endpoint == 0 }) {
		for i, e := range lifetime.Events {
			at, err := e.stamp()
			if e.Kind != "socket_failure_injected" || err != nil || at.Before(activated) {
				continue
			}
			for _, f := range lifetime.Events[i+1:] {
				if f.Kind == "fault_lossy" || f.Kind == "fault_backoff" {
					return at, e.Conn, true
				}
			}
		}
	}
	return time.Time{}, "", false
}

// TestCephMgrPeerResetIntegration determines when a stock OSD receives
// SESSION_RESET while its MgrClient reconnects, on a disposable one-OSD,
// two-MGR fixture in CRC mode (#141).
//
// Source prediction (pinned ProtocolV2.cc): a connector sends
// SESSION_RECONNECT only when it holds a nonzero server cookie
// (start_session_connect), and SESSION_RESET is accepted only in answer to
// it (handle_session_reset requires SESSION_RECONNECTING). A server sets a
// cookie only when its policy is not lossy (send_server_ident), and the
// connector adopts the SERVER_IDENT lossy bit. The stock MGR gives OSD
// peers Policy::stateless_server (src/mgr/DaemonServer.cc), so an OSD's
// MGR sessions never carry a cookie, and every reconnection is a fresh
// CLIENT_IDENT exchange on a new or retried connection.
//
// Observation: osd.0's and the stock MGRs' own debug_ms 5 container logs,
// read by the fixture control mgr-handshake-log, which keeps the events of
// connections to the exact msgr2 endpoint and nonce in log order, with the
// decoded reset bit, timestamps, process generations, connection pointers
// and cookie labels (zero, nonzero, equality). Three faults are bracketed
// by native `mgr dump` samples (epoch, active name, GID, address vector):
//
//   - transport cut, unchanged listener: osd.0's ms_inject_socket_failures,
//     set and cleared through its admin socket, shuts one of its sockets;
//     the cell requires the injection marker on a connection to the active
//     MGR, logged after the injection was requested and followed by a fault
//     of that connection (injectedCut; a lossy fault alone does not
//     qualify), and an unchanged MgrMap
//   - active-GID change: `mgr fail` of the active MGR promotes the standby
//   - restarted listener: with the standby stopped and failed out, the
//     active MGR container restarts; MON drops its old GID and promotes the
//     new process (new nonce)
//
// For each, a healthy post-fault session is required (healthyPair), each
// side matched within one session of one connection lifetime of its
// daemon's latest process (pointers are reused and sequences restart):
// on osd.0, CLIENT_IDENT (lossy, cookie 0), SERVER_IDENT from the
// expected GID (lossy, cookie 0), READY and write attempts of MMgrOpen and
// MMgrReport; on the MGR's log, the same session by both global sequences
// and the receipt of that MMgrOpen and MMgrReport by sequence. The OSD's
// write marker precedes _try_send, so the MGR's receipt is the delivery
// evidence. Neither log may show any resume event (resumeKinds) for the
// pair across the whole run, also among events of connections whose peer
// the log never named: one of those makes the verdict inconclusive.
//
// Reset-observer positive control: the stock MGRs stop, a Go MgrDaemon
// becomes active, and statefulIdentListener rewrites its first SERVER_IDENT
// so that osd.0 holds a stateful session with a nonzero server cookie.
// Closing that session must make the same OSD connection lifetime log
// fault_reconnect, a SESSION_RECONNECT carrying that cookie, a decoded
// SESSION_RESET full=1 (written by the Go acceptor, internal/session
// Accept), reset_session, and a fresh CLIENT_IDENT and SERVER_IDENT, after
// which the Go listener admits the session. This is a native reset
// specimen decoded by a stock receiver; it does not claim that a stock MGR
// ever sends one.
//
// Not covered: MON or MDS peers of a MGR, secure mode (the rewrite needs
// CRC framing), IPv6 and the stock MGR's own MON sessions.
//
// Pinned sources: ceph/ceph/blob/v20.2.4/src/msg/async/ProtocolV2.cc,
// ceph/ceph/blob/v20.2.4/src/msg/async/AsyncConnection.cc (socket failure
// injection), ceph/ceph/blob/v20.2.4/src/mgr/DaemonServer.cc,
// ceph/ceph/blob/v20.2.4/src/mgr/MgrClient.cc,
// ceph/ceph/blob/v20.2.4/src/mon/MgrMonitor.cc (active restart).
//
//scenario:row mgr-peer-reset.aes.ip4.crc.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mgrpeerreset key=aes service=aes mode=crc family=4 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 capabilities=mgr-daemon-control
//scenario:covers Messenger/CephX
//scenario:covers Daemon-to-daemon
//fixture:isolated fails over, restarts and stops the stock MGRs and injects osd.0 socket failures; restarts the MGRs
func TestCephMgrPeerResetIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" || os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "2" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" {
		t.Skip("requires the disposable one-OSD two-MGR fixture")
	}
	requireUsableFixture(t, control)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	if _, admitted := mgrDaemonDump(t, ctx, control, "reset-admission"); !admitted {
		t.Skip("failing over the stock MGRs requires this test alone with fixture capability mgr-daemon-control")
	}
	base := integrationOptions(t)
	if base.ConnectionMode != cephmsgr.CRCMode {
		t.Fatal("the reset specimen rewrites a CRC frame; this test needs a CRC row")
	}
	admin, err := cephmsgr.Dial(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	var osdMap struct {
		OSDs []nativeOSDEntry `json:"osds"`
	}
	if err := json.Unmarshal(adminCommand(t, ctx, admin, map[string]any{"prefix": "osd dump", "format": "json"}), &osdMap); err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(osdMap.OSDs, func(o nativeOSDEntry) bool { return o.OSD == 0 })
	if index < 0 {
		t.Fatal("osd.0 is not in the native OSDMap")
	}
	osdEndpoint := v2Endpoint(t, osdMap.OSDs[index].Addrs.Addrvec)
	t.Logf("osd.0 public msgr2 endpoint %s", osdEndpoint)

	set := func(who, option, value string) {
		adminCommand(t, ctx, admin, map[string]any{"prefix": "config set", "who": who, "name": option, "value": value})
	}
	remove := func(ctx context.Context, who, option string) {
		adminCommand(t, ctx, admin, map[string]any{"prefix": "config rm", "who": who, "name": option})
	}
	set("osd.0", "debug_ms", "5/5")
	set("mgr", "debug_ms", "5/5")
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		osdInject(t, cleanup, control, "cleanup", 0)
		remove(cleanup, "osd.0", "debug_ms")
		remove(cleanup, "mgr", "debug_ms")
	}()
	// The MON pushes the options to the running daemons.
	time.Sleep(3 * time.Second)

	stopped := map[string]bool{}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stop()
		for name, down := range stopped {
			if down {
				if err := testcluster.ControlDaemon(cleanup, control, "start", "mgr", name); err != nil {
					t.Error("restart stock MGR", name, err)
				}
			}
		}
	}()

	pairEndpoints := []string{}
	mgrNames := []string{}
	// cell runs one fault and requires the fresh post-fault handshake on both
	// logs. fault returns the native map the OSD must reconnect to and the
	// time the fault began, from which the fresh handshake is required.
	cell := func(label string, fault func(before nativeMgrDump) (nativeMgrDump, time.Time)) {
		t.Helper()
		before := waitMgrDump(t, ctx, control, label+"-before", func(d nativeMgrDump) bool { return d.Available && d.ActiveGID != 0 })
		after, since := fault(before)
		endpoint := v2Endpoint(t, after.ActiveAddrs.Addrvec)
		if !slices.Contains(pairEndpoints, endpoint) {
			pairEndpoints = append(pairEndpoints, endpoint)
		}
		if !slices.Contains(mgrNames, after.ActiveName) {
			mgrNames = append(mgrNames, after.ActiveName)
		}
		t.Logf("%s: MgrMap epoch %d active %s/%d at %s -> epoch %d active %s/%d at %s", label,
			before.Epoch, before.ActiveName, before.ActiveGID, v2Endpoint(t, before.ActiveAddrs.Addrvec), after.Epoch, after.ActiveName, after.ActiveGID, endpoint)
		deadline := time.Now().Add(3 * time.Minute)
		var pair pairSession
		for attempt := 1; ; attempt++ {
			osdLog := mgrHandshakeLog(t, ctx, control, fmt.Sprintf("%s-osd-%d", label, attempt), "osd", "0", endpoint)
			mgrLog := mgrHandshakeLog(t, ctx, control, fmt.Sprintf("%s-mgr-%d", label, attempt), "mgr", after.ActiveName, osdEndpoint)
			var err error
			if pair, err = healthyPair(osdLog, mgrLog, since, after.ActiveGID); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: no healthy osd.0 session with %s/%d after the fault: %v; osd.0 %v; mgr.%s %v", label, endpoint, after.ActiveGID, err, osdLog.Events, after.ActiveName, mgrLog.Events)
			}
			time.Sleep(3 * time.Second)
		}
		for _, e := range pair.OSDEvents {
			t.Logf("%s osd.0 %v", label, e)
		}
		for _, e := range pair.MGREvents {
			if e.Kind != "message_received" || e.Message == "mgropen" || e.Message == "mgrreport" {
				t.Logf("%s mgr.%s %v", label, after.ActiveName, e)
			}
		}
		t.Logf("%s: osd.0 connection %s and mgr.%s connection %s completed a fresh lossy exchange (both cookies 0) and the MGR received the OSD's MMgrOpen and MMgrReport", label, pair.OSDConn, after.ActiveName, pair.MGRConn)
	}

	// 1. Transport cut with an unchanged listener.
	cell("transport-cut", func(before nativeMgrDump) (nativeMgrDump, time.Time) {
		endpoint := v2Endpoint(t, before.ActiveAddrs.Addrvec)
		// The injection cannot act before the admin-socket request is
		// sent; the host and container clocks are the same clock.
		activated := time.Now().Truncate(time.Millisecond)
		osdInject(t, ctx, control, "transport-cut-on", 10)
		var cut time.Time
		for deadline := time.Now().Add(3 * time.Minute); ; {
			log := mgrHandshakeLog(t, ctx, control, "transport-cut-inject", "osd", "0", endpoint)
			if at, conn, ok := injectedCut(log, activated); ok {
				cut = at
				t.Logf("transport-cut: socket failure injected on osd.0 connection %s to %s at %s, then a fault on that connection", conn, endpoint, at.UTC().Format(time.RFC3339Nano))
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("transport-cut: no injected socket failure followed by a fault on the osd.0 connection to", endpoint, log.Events)
			}
			time.Sleep(2 * time.Second)
		}
		osdInject(t, ctx, control, "transport-cut-off", 0)
		after, _ := mgrDaemonDump(t, ctx, control, "transport-cut-after")
		if after.ActiveGID != before.ActiveGID || !sameAddrvec(after.ActiveAddrs.Addrvec, before.ActiveAddrs.Addrvec) {
			t.Fatalf("transport-cut changed the MgrMap: %+v -> %+v", before, after)
		}
		return after, cut
	})

	// 2. Active-GID change: the standby takes over.
	cell("active-change", func(before nativeMgrDump) (nativeMgrDump, time.Time) {
		if len(before.Standbys) == 0 {
			before = waitMgrDump(t, ctx, control, "active-change-standby", func(d nativeMgrDump) bool { return d.Available && len(d.Standbys) > 0 })
		}
		since := time.Now().Truncate(time.Millisecond)
		adminCommand(t, ctx, admin, map[string]any{"prefix": "mgr fail", "who": before.ActiveName})
		return waitMgrDump(t, ctx, control, "active-change-after", func(d nativeMgrDump) bool {
			return d.Available && d.ActiveGID != before.ActiveGID && d.ActiveName != before.ActiveName
		}), since
	})

	// 3. Restarted listener: the active MGR container restarts while the
	// other MGR is stopped and failed out, so MON promotes the restarted
	// process.
	cell("listener-restart", func(before nativeMgrDump) (nativeMgrDump, time.Time) {
		other := waitMgrDump(t, ctx, control, "listener-restart-standby", func(d nativeMgrDump) bool {
			return d.Available && d.ActiveGID == before.ActiveGID && len(d.Standbys) > 0
		}).Standbys[0].Name
		if err := testcluster.ControlDaemon(ctx, control, "stop", "mgr", other); err != nil {
			t.Fatal("stop standby MGR", other, err)
		}
		stopped[other] = true
		adminCommand(t, ctx, admin, map[string]any{"prefix": "mgr fail", "who": other})
		waitMgrDump(t, ctx, control, "listener-restart-alone", func(d nativeMgrDump) bool { return len(d.Standbys) == 0 })
		if err := testcluster.ControlDaemon(ctx, control, "stop", "mgr", before.ActiveName); err != nil {
			t.Fatal("stop active MGR", before.ActiveName, err)
		}
		stopped[before.ActiveName] = true
		since := time.Now().Truncate(time.Millisecond)
		if err := testcluster.ControlDaemon(ctx, control, "start", "mgr", before.ActiveName); err != nil {
			t.Fatal("start active MGR", before.ActiveName, err)
		}
		stopped[before.ActiveName] = false
		after := waitMgrDump(t, ctx, control, "listener-restart-after", func(d nativeMgrDump) bool {
			return d.Available && d.ActiveName == before.ActiveName && d.ActiveGID != before.ActiveGID
		})
		if err := testcluster.ControlDaemon(ctx, control, "start", "mgr", other); err != nil {
			t.Fatal("restart standby MGR", other, err)
		}
		stopped[other] = false
		return after, since
	})

	// No resume event anywhere in either log of the stock pair, and none
	// on a connection whose peer the log never named.
	osdLog := mgrHandshakeLog(t, ctx, control, "stock-osd-all", "osd", "0", pairEndpoints...)
	unnamed := osdLog.absent(t, "stock pair", resumeKinds...)
	idents := len(osdLog.kinds("client_ident_sent"))
	for _, name := range mgrNames {
		mgrLog := mgrHandshakeLog(t, ctx, control, "stock-mgr-all-"+name, "mgr", name, osdEndpoint)
		n := mgrLog.absent(t, "stock pair", resumeKinds...)
		t.Logf("mgr.%s: %d processes, %d identification exchanges with osd.0, no resume event; %d events on connections that never named a peer, none of a resume kind", name, len(mgrLog.Processes), len(mgrLog.kinds("client_ident_received")), n)
	}
	t.Logf("stock pair: osd.0 attempted %d CLIENT_IDENTs to MGR endpoints %v and logged no SESSION_RECONNECT, SESSION_RESET or retry; %d unattributed osd.0 events, none of a resume kind", idents, pairEndpoints, unnamed)

	resetSpecimen(t, ctx, control, admin, base)
}

func sameAddrvec(a, b []nativeMgrAddress) bool { return slices.Equal(a, b) }

// osdInject sets osd.0's ms_inject_socket_failures through its admin socket
// (control mgr-osd-inject). A MON config change is not used: the OSD would
// learn it over its MON session, which the injection itself keeps cutting,
// and a first run left the injection on for the rest of the test that way.
func osdInject(t *testing.T, ctx context.Context, control, label string, period int) {
	t.Helper()
	if err := testcluster.RequestControl(ctx, control, "mgr-osd-inject", "mgr-osd-inject-completed", label, strconv.Itoa(period)); err != nil {
		t.Fatal("osd.0 socket failure injection", label, err)
	}
	data, err := os.ReadFile(filepath.Join(control, "mgr-osd-inject.json"))
	var receipt struct {
		Label    string `json:"label"`
		Admitted bool   `json:"admitted"`
		Code     int    `json:"code"`
		Output   string `json:"output"`
	}
	if err != nil || json.Unmarshal(data, &receipt) != nil || receipt.Label != label || !receipt.Admitted || receipt.Code != 0 {
		t.Fatalf("osd.0 socket failure injection %s: %+v %v", label, receipt, err)
	}
}

// specimenStepCount is the number of steps specimenSteps requires.
const specimenStepCount = 7

// specimenSteps follows the reset specimen on osd.0's log within the one
// connection lifetime that holds claimed, the stateful READY (matched by
// process, pointer and log line, since a pointer is reused and labels are
// per receipt), from cut on: fault_reconnect, SESSION_RECONNECT carrying
// the claimed server cookie with no client cookie, a decoded SESSION_RESET
// full=1, reset_session, the fresh CLIENT_IDENT with clientSeq (the Go
// listener's record of the session it admitted), SERVER_IDENT from gid
// with serverSeq (that session's generation), lossy and without a cookie,
// and READY lossy. It returns the steps reached and the lifetime's events
// from cut on, write attempts left out.
func specimenSteps(l handshakeLog, claimed handshakeEvent, cut time.Time, clientSeq, serverSeq string, gid uint64) (int, []handshakeEvent) {
	var lifetime []handshakeEvent
	for _, t := range l.lifetimes(func(e handshakeEvent) bool { return e.Process == claimed.Process && e.Conn == claimed.Conn }) {
		if slices.ContainsFunc(t.Events, func(e handshakeEvent) bool { return e.Line == claimed.Line && e.Kind == "ready" }) {
			lifetime = t.Events
		}
	}
	at := slices.IndexFunc(lifetime, func(e handshakeEvent) bool { return e.Line == claimed.Line })
	if at < 0 || lifetime[at].Lossy != "0" || lifetime[at].ServerCookie == "0" {
		return 0, nil
	}
	serverCookie := lifetime[at].ServerCookie
	var sequence []handshakeEvent
	for _, e := range lifetime[at:] {
		if when, err := e.stamp(); err == nil && !when.Before(cut) && e.Kind != "message_write_attempt" {
			sequence = append(sequence, e)
		}
	}
	want := []func(handshakeEvent) bool{
		func(e handshakeEvent) bool { return e.Kind == "fault_reconnect" },
		func(e handshakeEvent) bool {
			return e.Kind == "reconnect_sent" && e.PeerCookie == serverCookie && e.Cookie == "0"
		},
		func(e handshakeEvent) bool { return e.Kind == "reset_received" && e.Full == "1" },
		func(e handshakeEvent) bool { return e.Kind == "reset_session" },
		func(e handshakeEvent) bool { return e.Kind == "client_ident_sent" && e.GlobalSeq == clientSeq },
		func(e handshakeEvent) bool {
			return e.Kind == "server_ident_received" && e.GID == fmt.Sprint(gid) && e.Flags == "1" && e.Cookie == "0" && e.GlobalSeq == serverSeq
		},
		func(e handshakeEvent) bool { return e.Kind == "ready" && e.Lossy == "1" },
	}
	step := 0
	for _, e := range sequence {
		if step < len(want) && want[step](e) {
			step++
		}
	}
	return step, sequence
}

// resetSpecimen is the reset observer's native positive control: a stock
// OSD decoding a SESSION_RESET on its connection to a Go MGR acceptor.
func resetSpecimen(t *testing.T, ctx context.Context, control string, admin *cephmsgr.Client, base cephmsgr.Options) {
	ttl := runningServiceTicketTTL(t, ctx, admin)
	name := fmt.Sprintf("gorst%d", time.Now().UnixNano()%1_000_000)
	entity := "mgr." + name
	var created []struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(adminCommand(t, ctx, admin, map[string]any{"prefix": "auth get-or-create", "entity": entity, "caps": []string{"mon", "allow profile mgr", "osd", "allow *", "mds", "allow *"}, "format": "json"}), &created); err != nil || len(created) != 1 {
		t.Fatal("scoped MGR credential", err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		adminCommand(t, cleanup, admin, map[string]any{"prefix": "auth rm", "entity": entity})
	}()
	key, err := cephmsgr.ParseKey(created[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	var localMu sync.Mutex
	var local netip.Addr
	daemon, err := cephmsgr.DialMgrDaemon(ctx, cephmsgr.MgrDaemonOptions{
		Monitors: base.Monitors, Identity: entity, Key: key, ConnectionMode: base.ConnectionMode,
		ExpectedFSID: base.ExpectedFSID, ServiceTicketTTL: ttl,
		DialContext: func(ctx context.Context, network, endpoint string) (net.Conn, error) {
			conn, err := base.DialContext(ctx, network, endpoint)
			if err == nil {
				if tcp, ok := conn.LocalAddr().(*net.TCPAddr); ok {
					localMu.Lock()
					local = tcp.AddrPort().Addr().Unmap()
					localMu.Unlock()
				}
			}
			return conn, err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	gid := daemon.Identity().GlobalID
	if _, err := daemon.WaitRotatingKeys(ctx, 0); err != nil {
		t.Fatal("rotating MGR secrets", err)
	}
	bound, err := net.Listen("tcp4", ":0")
	if err != nil {
		t.Fatal(err)
	}
	var cookie [8]byte
	if _, err := rand.Read(cookie[:]); err != nil {
		t.Fatal(err)
	}
	rewrite := &statefulIdentListener{Listener: bound, cookie: binary.LittleEndian.Uint64(cookie[:]) | 1}
	localMu.Lock()
	host := local
	localMu.Unlock()
	var nonce [4]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	advertised := []cephmsgr.ManagerAddress{{Type: 2, Nonce: binary.LittleEndian.Uint32(nonce[:])&0x7fffffff | 1, Endpoint: netip.AddrPortFrom(host, uint16(bound.Addr().(*net.TCPAddr).Port))}}
	listener, err := daemon.Listen(cephmsgr.MgrListenerOptions{
		Listener: rewrite, Addresses: advertised, ConnectionMode: base.ConnectionMode, HandshakeTimeout: 5 * time.Second,
		Authorize: func(_ context.Context, id cephmsgr.MgrPeerIdentity) error {
			if id.Entity != "osd.0" {
				return errors.New("only osd.0 is expected")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	beacons := startBeaconLoop(daemon, cephmsgr.MgrBeacon{Metadata: map[string]string{"gotest": "standby"}})
	defer func() {
		if err := beacons.close(); err != nil {
			t.Error("beacon loop", err)
		}
	}()
	standby := nextMgrEvent(t, ctx, daemon, "standby", func(e cephmsgr.MgrMapEvent) bool { return e.Role == cephmsgr.MgrRoleStandby })
	// Every stock MGR (MGR_COUNT=2) must be in the map, so that stopping
	// them leaves only this daemon for MON to select.
	stock := waitMgrDump(t, ctx, control, "specimen-standby", func(d nativeMgrDump) bool {
		return d.Epoch >= standby.Map.Epoch && d.standby(gid, name) && d.Available && len(d.Standbys) == 2
	})
	stockNames := []string{stock.ActiveName}
	for _, s := range stock.Standbys {
		if s.GID != gid {
			stockNames = append(stockNames, s.Name)
		}
	}
	restored := false
	defer func() {
		if restored {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stop()
		for _, n := range stockNames {
			if err := testcluster.ControlDaemon(cleanup, control, "start", "mgr", n); err != nil {
				t.Error("restart stock MGR", n, err)
			}
		}
	}()
	for _, n := range stockNames {
		if err := testcluster.ControlDaemon(ctx, control, "stop", "mgr", n); err != nil {
			t.Fatal("stop stock MGR", n, err)
		}
	}
	for _, n := range append(stockNames[1:], stockNames[0]) {
		adminCommand(t, ctx, admin, map[string]any{"prefix": "mgr fail", "who": n})
	}
	nextMgrEvent(t, ctx, daemon, "selection", func(e cephmsgr.MgrMapEvent) bool { return e.Role == cephmsgr.MgrRoleActive })
	beacons.set(cephmsgr.MgrBeacon{Available: true, Addresses: advertised, Metadata: map[string]string{"gotest": "reset-specimen"}})
	waitMgrDump(t, ctx, control, "specimen-available", func(d nativeMgrDump) bool {
		return d.ActiveGID == gid && d.Available && sameNativeAddresses(d.ActiveAddrs.Addrvec, advertised)
	})
	var osdMap struct {
		OSDs []nativeOSDEntry `json:"osds"`
	}
	if err := json.Unmarshal(adminCommand(t, ctx, admin, map[string]any{"prefix": "osd dump", "format": "json"}), &osdMap); err != nil {
		t.Fatal(err)
	}
	osdAddrs := osdMap.OSDs[slices.IndexFunc(osdMap.OSDs, func(o nativeOSDEntry) bool { return o.OSD == 0 })].Addrs.Addrvec

	// The session whose SERVER_IDENT claimed a stateful session.
	peer, _ := acceptOSD(t, ctx, listener, "stateful-claim", gid, advertised[0], base.ConnectionMode, osdAddrs)
	if !rewrite.rewritten.Load() {
		t.Fatal("the first admitted session's SERVER_IDENT was not rewritten")
	}
	endpoint := fmt.Sprintf("%s/%d", advertised[0].Endpoint, advertised[0].Nonce)
	var claimed handshakeEvent
	for deadline := time.Now().Add(time.Minute); ; {
		log := mgrHandshakeLog(t, ctx, control, "specimen-claim", "osd", "0", endpoint)
		if i := slices.IndexFunc(log.Events, func(e handshakeEvent) bool { return e.Kind == "ready" && e.Lossy == "0" && e.ServerCookie != "0" }); i >= 0 {
			claimed = log.Events[i]
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("osd.0 did not log a stateful READY session with the rewritten SERVER_IDENT:", log.Events)
		}
		time.Sleep(time.Second)
	}
	t.Logf("osd.0 connection %s holds a stateful session: %v", claimed.Conn, claimed)
	select {
	case <-peer.Done():
		t.Logf("the stateful session ended before the test closed it: %v", peer.Err())
	default:
	}
	cut := time.Now().Add(-time.Second)
	peer.Close()

	// The OSD resumes; the Go acceptor answers SESSION_RESET; the OSD then
	// identifies afresh, with its own cookie since it is no longer lossy.
	wait, stop := context.WithTimeout(ctx, 90*time.Second)
	resumed, err := listener.Accept(wait)
	stop()
	if err != nil {
		t.Fatalf("no session after the reset: %v (listener %+v)", err, listener.State())
	}
	id := resumed.Identity()
	if id.Entity != "osd.0" || id.Generation <= peer.Identity().Generation {
		t.Fatalf("session after the reset: %+v", id)
	}
	t.Logf("Go listener admitted session %d after the reset: CLIENT_IDENT flags %d, cookie nonzero %v", id.Generation, id.PeerFlags, id.PeerCookie != 0)
	log := mgrHandshakeLog(t, ctx, control, "specimen-reset", "osd", "0", endpoint)
	step, sequence := specimenSteps(log, claimed, cut, fmt.Sprint(id.PeerGlobalSeq), fmt.Sprint(id.Generation), gid)
	for _, e := range sequence {
		t.Logf("specimen osd.0 %v (prefix cookies %s/%s)", e, e.ClientCookie, e.ServerCookie)
	}
	if step != specimenStepCount {
		t.Fatalf("osd.0 connection %s: reset specimen stopped at step %d of %d within the claimed lifetime; no decoded SESSION_RESET full=1 in that order: %v", claimed.Conn, step, specimenStepCount, sequence)
	}
	t.Logf("reset specimen: osd.0 connection %s resumed with the claimed server cookie, decoded SESSION_RESET full=1 from the Go acceptor, reset its session and identified afresh", claimed.Conn)

	resumed.Close()
	listener.Close()
	if err := beacons.close(); err != nil {
		t.Fatal("beacon loop", err)
	}
	daemon.Close()
	for _, n := range stockNames {
		if err := testcluster.ControlDaemon(ctx, control, "start", "mgr", n); err != nil {
			t.Fatal("restart stock MGR", n, err)
		}
	}
	restored = true
	adminCommand(t, ctx, admin, map[string]any{"prefix": "mgr fail", "who": name})
	back := waitMgrDump(t, ctx, control, "specimen-restored", func(d nativeMgrDump) bool {
		return d.Available && d.ActiveGID != gid && slices.Contains(stockNames, d.ActiveName)
	})
	t.Logf("restored: stock %s/%d active at native epoch %d", back.ActiveName, back.ActiveGID, back.Epoch)
}

// TestStatefulIdentRewriteChangesOnlyTheFirstServerIdent drives
// statefulIdentListener's connection wrapper with fragmented writes: the
// banner and every other frame pass byte for byte, the first SERVER_IDENT
// of the listener loses its lossy flag and gains the cookie, and a later
// connection's SERVER_IDENT is unchanged.
func TestStatefulIdentRewriteChangesOnlyTheFirstServerIdent(t *testing.T) {
	ident := make([]byte, 40)
	binary.LittleEndian.PutUint64(ident[24:], 1) // lossy
	var stream bytes.Buffer
	stream.Write(msgr.Banner{Supported: msgr.Revision1, Required: msgr.Revision1}.Encode())
	w := msgr.NewWriter(&stream, 1<<20)
	for _, f := range []msgr.Frame{{Tag: msgr.Hello, Segments: [][]byte{{1, 2, 3}}}, {Tag: msgr.ServerIdent, Segments: [][]byte{ident}}, {Tag: msgr.Message, Segments: [][]byte{{9}, nil, {7, 7}}}} {
		if err := w.Write(f); err != nil {
			t.Fatal(err)
		}
	}
	original := stream.Bytes()
	listener := &statefulIdentListener{cookie: 0x1122334455667789}
	run := func() []byte {
		var out bytes.Buffer
		conn := &identRewriteConn{Conn: writeOnlyConn{&out}, listener: listener}
		for rest := original; len(rest) > 0; {
			n := min(7, len(rest))
			if _, err := conn.Write(rest[:n]); err != nil {
				t.Fatal(err)
			}
			rest = rest[n:]
		}
		return out.Bytes()
	}
	first := run()
	if !listener.rewritten.Load() || len(first) != len(original) {
		t.Fatal("first SERVER_IDENT not rewritten", len(first), len(original))
	}
	r := bytes.NewReader(first[26:])
	reader := msgr.NewReader(r, 1<<20)
	hello, _ := reader.Read()
	server, err := reader.Read()
	message, _ := reader.Read()
	if err != nil || hello.Tag != msgr.Hello || server.Tag != msgr.ServerIdent || message.Tag != msgr.Message || len(message.Segments) != 3 {
		t.Fatal("rewritten stream", hello.Tag, server.Tag, message.Tag, err)
	}
	if flags, cookie := binary.LittleEndian.Uint64(server.Segments[0][24:]), binary.LittleEndian.Uint64(server.Segments[0][32:]); flags != 0 || cookie != listener.cookie {
		t.Fatalf("flags %d cookie %#x", flags, cookie)
	}
	if !bytes.Equal(first[:26], original[:26]) {
		t.Fatal("banner changed")
	}
	if second := run(); !bytes.Equal(second, original) {
		t.Fatal("a later connection's SERVER_IDENT was rewritten")
	}
}

type writeOnlyConn struct{ io.Writer }

func (c writeOnlyConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c writeOnlyConn) Close() error                     { return nil }
func (c writeOnlyConn) LocalAddr() net.Addr              { return nil }
func (c writeOnlyConn) RemoteAddr() net.Addr             { return nil }
func (c writeOnlyConn) SetDeadline(time.Time) error      { return nil }
func (c writeOnlyConn) SetReadDeadline(time.Time) error  { return nil }
func (c writeOnlyConn) SetWriteDeadline(time.Time) error { return nil }

// pairLogs are synthetic receipts shaped like a fresh lossy osd.0 to MGR
// session: the OSD's ident exchange, READY and write attempts of MMgrOpen
// seq 1 and MMgrReport seq 2, and on the MGR the matching session. extra
// events follow on each side.
func pairLogs(osdExtra, mgrExtra []handshakeEvent, mgrReceives bool) (handshakeLog, handshakeLog) {
	osd := handshakeLog{Daemon: "osd.0", Events: []handshakeEvent{
		{Time: "2026-11-20T00:00:01.000+0000", Conn: "0xa", Kind: "client_ident_sent", Flags: "1", Cookie: "0", GlobalSeq: "12"},
		{Time: "2026-11-20T00:00:01.001+0000", Conn: "0xa", Kind: "server_ident_received", GID: "4167", Flags: "1", Cookie: "0", GlobalSeq: "40"},
		{Time: "2026-11-20T00:00:01.002+0000", Conn: "0xa", Kind: "ready", Lossy: "1", ServerCookie: "0"},
		{Time: "2026-11-20T00:00:01.003+0000", Conn: "0xa", Kind: "message_write_attempt", Message: "mgropen", Seq: "1"},
		{Time: "2026-11-20T00:00:01.004+0000", Conn: "0xa", Kind: "message_write_attempt", Message: "mgrreport", Seq: "2"},
	}}
	mgr := handshakeLog{Daemon: "mgr.a", Processes: []handshakeProcess{{Process: 1}}, Events: []handshakeEvent{
		{Time: "2026-11-20T00:00:01.000+0000", Process: 1, Conn: "0xb", Kind: "client_ident_received", Flags: "1", Cookie: "0", GlobalSeq: "12"},
		{Time: "2026-11-20T00:00:01.001+0000", Process: 1, Conn: "0xb", Kind: "server_ident_sent", GID: "4167", Flags: "1", Cookie: "0", GlobalSeq: "40"},
	}}
	if mgrReceives {
		mgr.Events = append(mgr.Events,
			handshakeEvent{Time: "2026-11-20T00:00:01.005+0000", Process: 1, Conn: "0xb", Kind: "message_received", Message: "mgropen", Seq: "1"},
			handshakeEvent{Time: "2026-11-20T00:00:01.006+0000", Process: 1, Conn: "0xb", Kind: "message_received", Message: "mgrreport", Seq: "2"})
	}
	osd.Events = append(osd.Events, osdExtra...)
	mgr.Events = append(mgr.Events, mgrExtra...)
	return osd, mgr
}

// TestHealthyPairNeedsTheMgrReceiptOfOpenAndReport: OSD write attempts
// followed by an injected failure and a lossy fault, with no MGR receipt,
// are not a healthy pair; neither is a receipt of another sequence or on
// another MGR session. Only the MGR's receipt of the attempted MMgrOpen and
// MMgrReport makes the session healthy.
func TestHealthyPairNeedsTheMgrReceiptOfOpenAndReport(t *testing.T) {
	since := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	lost := []handshakeEvent{
		{Time: "2026-11-20T00:00:01.005+0000", Conn: "0xa", Kind: "socket_failure_injected"},
		{Time: "2026-11-20T00:00:01.005+0000", Conn: "0xa", Kind: "fault_lossy"},
	}
	osd, mgr := pairLogs(lost, nil, false)
	if _, err := healthyPair(osd, mgr, since, 4167); err == nil || !strings.Contains(err.Error(), "receipt step 2 of 4") {
		t.Fatal("a report lost after its write attempt was accepted as healthy:", err)
	}
	osd, mgr = pairLogs(nil, []handshakeEvent{
		{Time: "2026-11-20T00:00:01.005+0000", Process: 1, Conn: "0xb", Kind: "message_received", Message: "mgropen", Seq: "1"},
		{Time: "2026-11-20T00:00:01.006+0000", Process: 1, Conn: "0xb", Kind: "message_received", Message: "mgrreport", Seq: "7"},
	}, false)
	if _, err := healthyPair(osd, mgr, since, 4167); err == nil {
		t.Fatal("a report of another sequence was accepted")
	}
	osd, mgr = pairLogs(nil, nil, true)
	mgr.Events[0].GlobalSeq = "13" // another session's CLIENT_IDENT
	if _, err := healthyPair(osd, mgr, since, 4167); err == nil {
		t.Fatal("a receipt on another MGR session was accepted")
	}
	osd, mgr = pairLogs(nil, nil, true)
	pair, err := healthyPair(osd, mgr, since, 4167)
	if err != nil || pair.OSDConn != "0xa" || pair.MGRConn != "0xb" {
		t.Fatal("received open and report", pair, err)
	}
	if _, err := healthyPair(osd, mgr, since.Add(time.Hour), 4167); err == nil {
		t.Fatal("a session before since was accepted")
	}
}

// TestInjectedCutNeedsTheInjectionMarker: a lossy fault without an injected
// socket failure, an injection logged before the injection was requested,
// and an injection on a connection that then logs no fault do not set the
// transport cut.
func TestInjectedCutNeedsTheInjectionMarker(t *testing.T) {
	activated := time.Date(2026, 11, 20, 0, 0, 1, 0, time.UTC)
	log := func(events ...handshakeEvent) handshakeLog { return handshakeLog{Daemon: "osd.0", Events: events} }
	fault := handshakeEvent{Time: "2026-11-20T00:00:02.000+0000", Conn: "0xa", Kind: "fault_lossy"}
	if _, _, ok := injectedCut(log(fault), activated); ok {
		t.Fatal("a generic lossy fault set the cut")
	}
	early := handshakeEvent{Time: "2026-11-20T00:00:00.500+0000", Conn: "0xa", Kind: "socket_failure_injected"}
	if _, _, ok := injectedCut(log(early, fault), activated); ok {
		t.Fatal("an injection before activation set the cut")
	}
	injected := handshakeEvent{Time: "2026-11-20T00:00:01.500+0000", Conn: "0xa", Kind: "socket_failure_injected"}
	other := handshakeEvent{Time: "2026-11-20T00:00:02.000+0000", Conn: "0xb", Kind: "fault_lossy"}
	if _, _, ok := injectedCut(log(injected, other), activated); ok {
		t.Fatal("a fault of another connection was correlated with the injection")
	}
	at, conn, ok := injectedCut(log(early, injected, fault), activated)
	if !ok || conn != "0xa" || !at.Equal(time.Date(2026, 11, 20, 0, 0, 1, 500_000_000, time.UTC)) {
		t.Fatal("injected cut", at, conn, ok)
	}
}

// ev is a synthetic handshake event at second.millis of 2026-11-20T00:00.
func ev(at string, process int, conn, kind string, set func(*handshakeEvent)) handshakeEvent {
	e := handshakeEvent{Time: "2026-11-20T00:00:" + at + "+0000", Process: process, Conn: conn, Kind: kind}
	if set != nil {
		set(&e)
	}
	return e
}

// TestHealthyPairKeepsEachMatchWithinOneSession covers the reused-pointer
// shapes on both sides (round-2 review of #281): a MGR pointer that stops,
// is accepted again for another session and receives a new open seq 1 and
// report seq 2; an OSD pointer whose report attempt belongs to a later
// lifetime or to a retried identification; and a receipt only in an
// earlier MGR process. Each must be rejected; the single-session receipt
// is still accepted.
func TestHealthyPairKeepsEachMatchWithinOneSession(t *testing.T) {
	since := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	received := func(at, conn, msg, seq string) handshakeEvent {
		return ev(at, 1, conn, "message_received", func(e *handshakeEvent) { e.Message, e.Seq = msg, seq })
	}
	// Reviewer's case: old open seq 1 on 0xb, stop, accept, another
	// session's idents (13/41), then that session's open and report.
	osd, mgr := pairLogs(nil, []handshakeEvent{
		received("01.005", "0xb", "mgropen", "1"),
		ev("01.006", 1, "0xb", "stop", nil),
		ev("01.007", 1, "0xb", "accept", nil),
		ev("01.008", 1, "0xb", "client_ident_received", func(e *handshakeEvent) { e.Flags, e.Cookie, e.GlobalSeq = "1", "0", "13" }),
		ev("01.009", 1, "0xb", "server_ident_sent", func(e *handshakeEvent) { e.GID, e.Flags, e.Cookie, e.GlobalSeq = "4167", "1", "0", "41" }),
		received("01.010", "0xb", "mgropen", "1"),
		received("01.011", "0xb", "mgrreport", "2"),
	}, false)
	if _, err := healthyPair(osd, mgr, since, 4167); err == nil {
		t.Error("accepted a report from a new MGR connection lifetime with different global sequences")
	}
	// The same, with the second session on one accepted lifetime (a new
	// identification exchange, no stop in between).
	osd, mgr = pairLogs(nil, []handshakeEvent{
		received("01.005", "0xb", "mgropen", "1"),
		ev("01.008", 1, "0xb", "client_ident_received", func(e *handshakeEvent) { e.Flags, e.Cookie, e.GlobalSeq = "1", "0", "13" }),
		received("01.011", "0xb", "mgrreport", "2"),
	}, false)
	if _, err := healthyPair(osd, mgr, since, 4167); err == nil {
		t.Error("accepted a report from a later identification exchange of the MGR connection")
	}
	// OSD side: the report attempt belongs to a later lifetime of 0xa.
	osd, mgr = pairLogs(nil, nil, true)
	osd.Events = append(osd.Events[:4],
		ev("01.004", 0, "0xa", "stop", nil),
		ev("01.005", 0, "0xa", "connect", nil),
		ev("01.006", 0, "0xa", "message_write_attempt", func(e *handshakeEvent) { e.Message, e.Seq = "mgrreport", "2" }))
	for i := range osd.Events {
		osd.Events[i].Endpoint = 0
	}
	if _, err := healthyPair(osd, mgr, since, 4167); err == nil {
		t.Error("accepted an OSD report attempt from a later lifetime of the pointer")
	}
	// OSD side: a lossy retry identifies again before the report.
	osd, mgr = pairLogs(nil, nil, true)
	osd.Events = append(osd.Events[:4],
		ev("01.005", 0, "0xa", "client_ident_sent", func(e *handshakeEvent) { e.Flags, e.Cookie, e.GlobalSeq = "1", "0", "14" }),
		ev("01.006", 0, "0xa", "message_write_attempt", func(e *handshakeEvent) { e.Message, e.Seq = "mgrreport", "2" }))
	if _, err := healthyPair(osd, mgr, since, 4167); err == nil {
		t.Error("accepted an OSD report attempt from a retried identification")
	}
	// MGR side: the receipt is only in an earlier process; the latest
	// process restarted global sequences and has no receipt.
	osd, mgr = pairLogs(nil, nil, true)
	mgr.Processes = append(mgr.Processes, handshakeProcess{Process: 2})
	if _, err := healthyPair(osd, mgr, since, 4167); err == nil {
		t.Error("accepted a receipt from an earlier MGR process")
	}
	osd, mgr = pairLogs(nil, nil, true)
	if _, err := healthyPair(osd, mgr, since, 4167); err != nil {
		t.Error("single-session receipt rejected:", err)
	}
}

// TestInjectedCutStaysWithinTheInjectedLifetime: an injection on a lifetime
// that stops without a fault does not borrow the fault of a reused
// pointer's next lifetime.
func TestInjectedCutStaysWithinTheInjectedLifetime(t *testing.T) {
	activated := time.Date(2026, 11, 20, 0, 0, 1, 0, time.UTC)
	mgrEndpoint := func(e *handshakeEvent) { e.Endpoint = 0 }
	l := handshakeLog{Events: []handshakeEvent{
		ev("01.500", 0, "0xa", "socket_failure_injected", mgrEndpoint),
		ev("01.501", 0, "0xa", "stop", mgrEndpoint),
		ev("01.600", 0, "0xa", "connect", mgrEndpoint),
		ev("01.700", 0, "0xa", "fault_lossy", mgrEndpoint),
	}}
	if _, _, ok := injectedCut(l, activated); ok {
		t.Error("an injection borrowed the fault of a reused pointer's next lifetime")
	}
	l.Events = []handshakeEvent{l.Events[0], ev("01.501", 0, "0xa", "fault_lossy", mgrEndpoint), ev("01.502", 0, "0xa", "stop", mgrEndpoint)}
	if _, conn, ok := injectedCut(l, activated); !ok || conn != "0xa" {
		t.Error("native order (injection, lossy fault, stop) rejected")
	}
}

// TestSpecimenStepsStayWithinTheClaimedLifetime: the specimen's reconnect,
// reset and fresh identification must follow the stateful READY on the
// same lifetime; a reused pointer's later events do not complete it.
func TestSpecimenStepsStayWithinTheClaimedLifetime(t *testing.T) {
	cut := time.Date(2026, 11, 20, 0, 0, 1, 0, time.UTC)
	claimed := ev("01.000", 2, "0xa", "ready", func(e *handshakeEvent) { e.Line, e.Lossy, e.ServerCookie = 10, "0", "k1" })
	rest := []handshakeEvent{
		ev("01.100", 2, "0xa", "fault_reconnect", nil),
		ev("01.101", 2, "0xa", "reconnect_sent", func(e *handshakeEvent) { e.Cookie, e.PeerCookie = "0", "k1" }),
		ev("01.102", 2, "0xa", "reset_received", func(e *handshakeEvent) { e.Full = "1" }),
		ev("01.103", 2, "0xa", "reset_session", nil),
		ev("01.104", 2, "0xa", "client_ident_sent", func(e *handshakeEvent) { e.GlobalSeq = "63" }),
		ev("01.105", 2, "0xa", "server_ident_received", func(e *handshakeEvent) { e.GID, e.Flags, e.Cookie, e.GlobalSeq = "4788", "1", "0", "2" }),
		ev("01.106", 2, "0xa", "ready", func(e *handshakeEvent) { e.Lossy = "1" }),
	}
	one := handshakeLog{Events: append([]handshakeEvent{claimed}, rest...)}
	if step, _ := specimenSteps(one, claimed, cut, "63", "2", 4788); step != specimenStepCount {
		t.Error("specimen in one lifetime stopped at step", step)
	}
	reused := handshakeLog{Events: append([]handshakeEvent{claimed, ev("01.050", 2, "0xa", "stop", nil), ev("01.060", 2, "0xa", "connect", nil)}, rest...)}
	if step, _ := specimenSteps(reused, claimed, cut, "63", "2", 4788); step == specimenStepCount {
		t.Error("specimen completed by a reused pointer's next lifetime")
	}
}

// TestIdentExchangeStaysWithinOneSession: the acceptor test's CLIENT_IDENT
// and SERVER_IDENT must be one session of osd.0's latest process.
func TestIdentExchangeStaysWithinOneSession(t *testing.T) {
	client := ev("01.000", 2, "0xa", "client_ident_sent", func(e *handshakeEvent) { e.GlobalSeq = "12" })
	server := ev("01.001", 2, "0xa", "server_ident_received", func(e *handshakeEvent) { e.GlobalSeq = "3" })
	l := handshakeLog{Processes: []handshakeProcess{{Process: 1}, {Process: 2}}, Events: []handshakeEvent{client, server}}
	if _, _, ok := identExchange(l, "12", "3"); !ok {
		t.Error("one session rejected")
	}
	l.Events = []handshakeEvent{client, ev("01.000", 2, "0xa", "stop", nil), ev("01.000", 2, "0xa", "connect", nil), server}
	if _, _, ok := identExchange(l, "12", "3"); ok {
		t.Error("paired across a reused pointer's lifetimes")
	}
	l.Events = []handshakeEvent{client, ev("01.000", 2, "0xa", "client_ident_sent", func(e *handshakeEvent) { e.GlobalSeq = "13" }), server}
	if _, _, ok := identExchange(l, "12", "3"); ok {
		t.Error("paired across a retried identification")
	}
	old := client
	old.Process = 1
	oldServer := server
	oldServer.Process = 1
	l.Events = []handshakeEvent{old, oldServer}
	if _, _, ok := identExchange(l, "12", "3"); ok {
		t.Error("paired in an earlier OSD process whose global sequences restarted")
	}
}
