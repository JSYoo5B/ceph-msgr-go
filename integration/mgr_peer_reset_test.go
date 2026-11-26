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
	"slices"
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

// freshOSDSession returns the first connection of osd.0's log, from since
// on, that completed a fresh lossy handshake with the MGR at endpoint (gid)
// and then sent MMgrOpen and MMgrReport, with the events of that
// connection from since on. It reports false when the log has none yet.
func freshOSDSession(t *testing.T, log handshakeLog, since time.Time, endpoint int, gid uint64) (string, []handshakeEvent, bool) {
	t.Helper()
	conns := map[string][]handshakeEvent{}
	var order []string
	for _, e := range log.Events {
		if e.Endpoint != endpoint || e.at(t).Before(since) {
			continue
		}
		if _, ok := conns[e.Conn]; !ok {
			order = append(order, e.Conn)
		}
		conns[e.Conn] = append(conns[e.Conn], e)
	}
	want := fmt.Sprint(gid)
	for _, conn := range order {
		step := 0
		for _, e := range conns[conn] {
			switch {
			case step == 0 && e.Kind == "client_ident_sent" && e.Flags == "1" && e.Cookie == "0":
				step = 1
			case step == 1 && e.Kind == "server_ident_received" && e.GID == want && e.Flags == "1" && e.Cookie == "0":
				step = 2
			case step == 2 && e.Kind == "ready" && e.Lossy == "1" && e.ServerCookie == "0":
				step = 3
			case step == 3 && e.Kind == "message_sent" && e.Message == "mgropen":
				step = 4
			case step == 4 && e.Kind == "message_sent" && e.Message == "mgrreport":
				return conn, conns[conn], true
			}
		}
	}
	return "", nil, false
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
//   - transport cut, unchanged listener: osd.0's ms_inject_socket_failures
//     shuts one of its sockets; the cell requires the injection on the
//     connection to the active MGR and an unchanged MgrMap
//   - active-GID change: `mgr fail` of the active MGR promotes the standby
//   - restarted listener: with the standby stopped and failed out, the
//     active MGR container restarts; MON drops its old GID and promotes the
//     new process (new nonce)
//
// For each, a complete post-fault handshake is required: on one OSD
// connection, CLIENT_IDENT (lossy, cookie 0), SERVER_IDENT from the
// expected GID (lossy, cookie 0), READY, MMgrOpen and MMgrReport (the
// healthy positive control for the pair), and on the MGR's log the matching
// received CLIENT_IDENT and sent SERVER_IDENT. Neither log may show any
// resume event (resumeKinds) for the pair across the whole run.
//
// Reset-observer positive control: the stock MGRs stop, a Go MgrDaemon
// becomes active, and statefulIdentListener rewrites its first SERVER_IDENT
// so that osd.0 holds a stateful session with a nonzero server cookie.
// Closing that session must make the same OSD connection log
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
		remove(cleanup, "osd.0", "ms_inject_socket_failures")
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
		var conn string
		var events []handshakeEvent
		for attempt := 1; ; attempt++ {
			log := mgrHandshakeLog(t, ctx, control, fmt.Sprintf("%s-osd-%d", label, attempt), "osd", "0", endpoint)
			var ok bool
			if conn, events, ok = freshOSDSession(t, log, since, 0, after.ActiveGID); ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: no fresh osd.0 session to %s/%d after the fault: %v", label, endpoint, after.ActiveGID, log.Events)
			}
			time.Sleep(3 * time.Second)
		}
		for _, e := range events {
			t.Logf("%s osd.0 %v", label, e)
		}
		mgrLog := mgrHandshakeLog(t, ctx, control, label+"-mgr", "mgr", after.ActiveName, osdEndpoint)
		current := mgrLog.Processes[len(mgrLog.Processes)-1].Process
		received := slices.IndexFunc(mgrLog.Events, func(e handshakeEvent) bool {
			return e.Process == current && e.Kind == "client_ident_received" && !e.at(t).Before(since) && e.Flags == "1" && e.Cookie == "0"
		})
		if received < 0 || !slices.ContainsFunc(mgrLog.Events[received:], func(e handshakeEvent) bool {
			return e.Conn == mgrLog.Events[received].Conn && e.Kind == "server_ident_sent" && e.GID == fmt.Sprint(after.ActiveGID) && e.Flags == "1" && e.Cookie == "0"
		}) {
			t.Fatalf("%s: mgr.%s (process %d) logged no fresh identification exchange with osd.0: %v", label, after.ActiveName, current, mgrLog.Events)
		}
		t.Logf("%s: osd.0 connection %s and mgr.%s process %d connection %s completed a fresh lossy exchange (both cookies 0); no resume event", label, conn, after.ActiveName, current, mgrLog.Events[received].Conn)
	}

	// 1. Transport cut with an unchanged listener.
	cell("transport-cut", func(before nativeMgrDump) (nativeMgrDump, time.Time) {
		endpoint := v2Endpoint(t, before.ActiveAddrs.Addrvec)
		start := time.Now().Add(-time.Second)
		set("osd.0", "ms_inject_socket_failures", "10")
		var cut time.Time
		for deadline := time.Now().Add(3 * time.Minute); cut.IsZero(); {
			log := mgrHandshakeLog(t, ctx, control, "transport-cut-inject", "osd", "0", endpoint)
			for _, e := range log.kinds("socket_failure_injected", "fault_lossy") {
				if at := e.at(t); !at.Before(start) {
					t.Logf("transport-cut: %v", e)
					if cut.IsZero() {
						cut = at
					}
				}
			}
			if time.Now().After(deadline) {
				t.Fatal("transport-cut: no socket failure on the osd.0 connection to", endpoint)
			}
			time.Sleep(2 * time.Second)
		}
		remove(ctx, "osd.0", "ms_inject_socket_failures")
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
		since := time.Now().Add(-time.Second)
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
		since := time.Now().Add(-time.Second)
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

	// No resume event anywhere in either log of the stock pair.
	osdLog := mgrHandshakeLog(t, ctx, control, "stock-osd-all", "osd", "0", pairEndpoints...)
	if resumed := osdLog.kinds(resumeKinds...); len(resumed) != 0 {
		t.Fatalf("osd.0 resumed a session with a stock MGR: %v", resumed)
	}
	idents := len(osdLog.kinds("client_ident_sent"))
	for _, name := range mgrNames {
		mgrLog := mgrHandshakeLog(t, ctx, control, "stock-mgr-all-"+name, "mgr", name, osdEndpoint)
		if resumed := mgrLog.kinds(resumeKinds...); len(resumed) != 0 {
			t.Fatalf("mgr.%s resumed a session with osd.0: %v", name, resumed)
		}
		t.Logf("mgr.%s: %d processes, %d identification exchanges with osd.0, no resume event", name, len(mgrLog.Processes), len(mgrLog.kinds("client_ident_received")))
	}
	t.Logf("stock pair: osd.0 sent %d CLIENT_IDENTs to MGR endpoints %v and logged no SESSION_RECONNECT, SESSION_RESET or retry", idents, pairEndpoints)

	resetSpecimen(t, ctx, control, admin, base)
}

func sameAddrvec(a, b []nativeMgrAddress) bool { return slices.Equal(a, b) }

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
	var sequence []handshakeEvent
	for _, e := range log.Events {
		if e.Conn == claimed.Conn && !e.at(t).Before(cut) && e.Kind != "message_sent" {
			sequence = append(sequence, e)
		}
	}
	for _, e := range sequence {
		t.Logf("specimen osd.0 %v", e)
	}
	want := []func(handshakeEvent) bool{
		func(e handshakeEvent) bool { return e.Kind == "fault_reconnect" },
		func(e handshakeEvent) bool {
			return e.Kind == "reconnect_sent" && e.PeerCookie == claimed.ServerCookie && e.Cookie == "0"
		},
		func(e handshakeEvent) bool { return e.Kind == "reset_received" && e.Full == "1" },
		func(e handshakeEvent) bool { return e.Kind == "reset_session" },
		func(e handshakeEvent) bool {
			return e.Kind == "client_ident_sent" && e.GlobalSeq == fmt.Sprint(id.PeerGlobalSeq)
		},
		func(e handshakeEvent) bool {
			return e.Kind == "server_ident_received" && e.GID == fmt.Sprint(gid) && e.Flags == "1" && e.Cookie == "0" && e.GlobalSeq == fmt.Sprint(id.Generation)
		},
		func(e handshakeEvent) bool { return e.Kind == "ready" && e.Lossy == "1" },
	}
	step := 0
	for _, e := range sequence {
		if step < len(want) && want[step](e) {
			step++
		}
	}
	if step != len(want) {
		t.Fatalf("osd.0 connection %s: reset specimen stopped at step %d of %d; no decoded SESSION_RESET full=1 in that order: %v", claimed.Conn, step, len(want), sequence)
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
