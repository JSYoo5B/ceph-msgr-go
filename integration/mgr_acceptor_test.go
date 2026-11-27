package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// nativeOSDEntry is one OSD of the native `osd dump --format json`.
type nativeOSDEntry struct {
	OSD   int `json:"osd"`
	Addrs struct {
		Addrvec []nativeMgrAddress `json:"addrvec"`
	} `json:"public_addrs"`
}

func formatAddress(a cephmsgr.ManagerAddress) string {
	return fmt.Sprintf("v2:%s/%d", a.Endpoint, a.Nonce)
}

// peerLog drains one peer's inbound messages so its bounded queue never
// overflows, keeping the first message and a count per type.
type peerLog struct {
	mu     sync.Mutex
	first  *cephmsgr.MgrInboundMessage
	counts map[uint16]int
	done   chan struct{}
}

func drainPeer(peer *cephmsgr.MgrPeer) *peerLog {
	l := &peerLog{counts: map[uint16]int{}, done: make(chan struct{})}
	go func() {
		defer close(l.done)
		for {
			m, err := peer.Next(context.Background())
			if err != nil {
				return
			}
			l.mu.Lock()
			if l.first == nil {
				l.first = &m
			}
			l.counts[m.Type]++
			l.mu.Unlock()
		}
	}()
	return l
}

func (l *peerLog) firstMessage(t *testing.T, ctx context.Context) cephmsgr.MgrInboundMessage {
	t.Helper()
	for {
		l.mu.Lock()
		first := l.first
		l.mu.Unlock()
		if first != nil {
			return *first
		}
		select {
		case <-ctx.Done():
			t.Fatal("no message from the stock OSD")
		case <-l.done:
			t.Fatal("peer ended before its first message")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// acceptOSD waits for the next published session and checks what does not
// change between sessions of the stock osd.0.
func acceptOSD(t *testing.T, ctx context.Context, listener *cephmsgr.MgrListener, label string, gid uint64, advertised cephmsgr.ManagerAddress, mode cephmsgr.ConnectionMode, native []nativeMgrAddress) (*cephmsgr.MgrPeer, *peerLog) {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	peer, err := listener.Accept(wait)
	if err != nil {
		t.Fatalf("%s: no stock OSD session: %v (listener %+v)", label, err, listener.State())
	}
	id := peer.Identity()
	if id.Entity != "osd.0" || id.PeerID != 0 || id.MgrGlobalID != gid || id.Target != advertised || id.ConnectionMode != mode ||
		id.Features != id.PeerSupported&session.MgrOSDPeerFeatures || id.PeerFlags != 1 || id.GlobalID == 0 || id.Generation == 0 {
		t.Fatalf("%s: identity %+v", label, id)
	}
	if len(id.Addresses) != len(native) {
		t.Fatalf("%s: CLIENT_IDENT addresses %v, native OSD public addrs %v", label, id.Addresses, native)
	}
	for i, a := range id.Addresses {
		kind := map[uint32]string{1: "v1", 2: "v2"}[a.Type]
		if native[i].Type != kind || native[i].Addr != a.Endpoint.String() || native[i].Nonce != a.Nonce {
			t.Fatalf("%s: CLIENT_IDENT addresses %v, native OSD public addrs %v", label, id.Addresses, native)
		}
	}
	t.Logf("%s: session generation %d: %s global ID %d (ticket secret ID %d, caps allow_all=%v %q), CLIENT_IDENT gid %d global_seq %d supported %#x required %#x, negotiated %#x, mode %d, from %v",
		label, id.Generation, id.Entity, id.GlobalID, id.SecretID, id.Caps.AllowAll, id.Caps.Text, id.PeerID, id.PeerGlobalSeq, id.PeerSupported, id.PeerRequired, id.Features, id.ConnectionMode, id.Remote)
	return peer, drainPeer(peer)
}

// identExchange returns the CLIENT_IDENT osd.0 sent with clientSeq and
// the SERVER_IDENT with serverSeq it received in answer, both within one
// session of one connection lifetime of its latest process: a pointer is
// reused and a global sequence restarts with the OSD process, so neither
// alone binds the two lines.
func identExchange(l handshakeLog, clientSeq, serverSeq string) (handshakeEvent, handshakeEvent, bool) {
	process := 0
	if len(l.Processes) > 0 {
		process = l.Processes[len(l.Processes)-1].Process
	}
	for _, lifetime := range l.lifetimes(func(e handshakeEvent) bool { return e.Process == process }) {
		for _, session := range lifetime.sessions() {
			if session[0].Kind != "client_ident_sent" || session[0].GlobalSeq != clientSeq {
				continue
			}
			for _, e := range session[1:] {
				if e.Kind == "server_ident_received" && e.GlobalSeq == serverSeq {
					return session[0], e, true
				}
			}
		}
	}
	return handshakeEvent{}, handshakeEvent{}, false
}

// TestCephMgrAcceptorIntegration accepts the stock osd.0's own MGR
// connections on a Go MGR endpoint (MgrDaemon.Listen) on a disposable
// one-OSD fixture, with the row's key type and connection mode.
//
// As in TestCephMgrDaemonControlIntegration, a scoped mgr.<id> daemon
// becomes the MON-selected active MGR with the stock MGRs stopped, and
// advertises the address of a listener this test bound. The stock OSD
// discovers that address from the MgrMap and opens a real authenticated
// session: its MGR service authorizer is verified against the daemon's
// rotating MGR secrets, the challenge exchange and both transcript
// signatures complete in the row's mode, and the session is published only
// after the Authorize hook admits osd.0. Correlated on both sides: the
// accepted identity (osd.0, its ticket global ID and secret ID, its
// CLIENT_IDENT numeric ID 0 and address vector, which must equal its
// public addrs in the native OSDMap), the MGR global ID and the connection
// generation. The OSD's own debug_ms 5 log (control mgr-handshake-log, for
// the exact advertised endpoint and nonce) must show a
// CLIENT_IDENT to the advertised target with the global sequence and
// supported mask the acceptor received, and a SERVER_IDENT naming the MGR's
// global ID, the generation the acceptor assigned, its supported mask, the
// native required mask 0x800000000200000, the lossy flag and a zero cookie,
// both within one session of one connection lifetime (identExchange);
// it must log no SESSION_RECONNECT, SESSION_RESET or retry for that
// endpoint, since a lossy server's peer never resumes. The first
// message the OSD sends is its MMgrOpen naming daemon "0".
//
// Failure and recovery on the stock peer: closing the session makes the
// OSD reconnect as a new session with a higher generation and global
// sequence; repeated reconnections run until the OSD presents a ticket
// sealed with a newer rotating MGR secret (the fixture's service tickets
// last 12 s), each secret ID held by the daemon when accepted (key
// rollover). The daemon is given the service ticket TTL every MON runs with
// (runningServiceTicketTTL), so it refreshes on the native rule's ttl/4
// schedule and holds the key the MON currently issues tickets under. A raw connection that sends nothing holds a handshake slot
// while the OSD's next session is admitted, and ends at the handshake
// timeout as a failure (slow-peer isolation, timeout). With osd.0's
// ms_mon_cluster_mode set to the other mode only, its reconnection is
// refused with AUTH_BAD_METHOD in the authentication stage, and once the
// option is removed the OSD's retry is admitted (refused mode,
// reconnection). Closing the listener ends the live session; a new
// listener on the same port admits the OSD's reconnection with a higher
// generation (shutdown).
//
// Not verified here, because a stock peer never produces them: a wrong
// service or secret ID, a replayed challenge response, a tampered
// authorizer or signature, a refused feature mask and a partial write.
// Those are synthetic tests (internal/cephx service tests,
// internal/session accept tests, cephmsgr listener tests), the authorizer
// cases also against the native verifier's own refusals
// (internal/cephx/testdata/mgr-acceptor-native-provenance.json). MDS and
// CLIENT peers, messages sent to the peer, IPv6 and VIP addresses are not
// covered.
//
// Pinned sources: ceph/ceph/blob/v20.2.4/src/mgr/DaemonServer.cc,
// ceph/ceph/blob/v20.2.4/src/mgr/MgrClient.cc (reconnect on reset and map),
// ceph/ceph/blob/v20.2.4/src/msg/async/ProtocolV2.cc (identification logs),
// ceph/ceph/blob/v20.2.4/src/auth/AuthRegistry.cc (a daemon's modes toward
// a MGR follow ms_mon_cluster_mode), ceph/ceph/blob/v20.2.4/src/messages/MMgrOpen.h.
//
//scenario:row mgr-acceptor.aes256k.ip4.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mgracceptor key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 capabilities=mgr-daemon-control
//scenario:row mgr-acceptor.aes.ip4.crc.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mgracceptor key=aes service=aes mode=crc family=4 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 capabilities=mgr-daemon-control
//scenario:covers Messenger/CephX
//scenario:covers Daemon-to-daemon
//fixture:isolated stops both stock MGRs, makes a test MGR active and changes osd.0 messenger options; restarts the MGRs
func TestCephMgrAcceptorIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" || os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "2" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" {
		t.Skip("requires the disposable one-OSD two-MGR fixture")
	}
	requireUsableFixture(t, control)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	if _, admitted := mgrDaemonDump(t, ctx, control, "acceptor-admission"); !admitted {
		t.Skip("stopping the stock MGRs requires this test alone with fixture capability mgr-daemon-control")
	}
	base := integrationOptions(t)
	admin, err := cephmsgr.Dial(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ttl := runningServiceTicketTTL(t, ctx, admin)

	name := fmt.Sprintf("goacc%d", time.Now().UnixNano()%1_000_000)
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

	// The OSD's identity in the native OSDMap.
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
	osdAddrs := osdMap.OSDs[index].Addrs.Addrvec
	t.Logf("native OSDMap: osd.0 public addrs %v", osdAddrs)

	// osd.0 logs its identification frames for the rest of this test.
	setOSD := func(option, value string) {
		adminCommand(t, ctx, admin, map[string]any{"prefix": "config set", "who": "osd.0", "name": option, "value": value})
	}
	removeOSD := func(ctx context.Context, option string) {
		adminCommand(t, ctx, admin, map[string]any{"prefix": "config rm", "who": "osd.0", "name": option})
	}
	setOSD("debug_ms", "5/5")
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		removeOSD(cleanup, "debug_ms")
		removeOSD(cleanup, "ms_mon_cluster_mode")
	}()

	// The listener: the endpoint the beacon advertises.
	bound, err := net.Listen("tcp4", ":0")
	if err != nil {
		t.Fatal(err)
	}
	port := bound.Addr().(*net.TCPAddr).Port
	localMu.Lock()
	host := local
	localMu.Unlock()
	var nonce [4]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	advertised := []cephmsgr.ManagerAddress{{Type: 2, Nonce: binary.LittleEndian.Uint32(nonce[:])&0x7fffffff | 1, Endpoint: netip.AddrPortFrom(host, uint16(port))}}
	var authorizedMu sync.Mutex
	var authorized []cephmsgr.MgrPeerIdentity
	listenerOptions := cephmsgr.MgrListenerOptions{
		Addresses: advertised, ConnectionMode: base.ConnectionMode, HandshakeTimeout: 5 * time.Second,
		Authorize: func(_ context.Context, id cephmsgr.MgrPeerIdentity) error {
			authorizedMu.Lock()
			defer authorizedMu.Unlock()
			authorized = append(authorized, id)
			if id.Entity != "osd.0" {
				return errors.New("only osd.0 is expected")
			}
			return nil
		},
	}
	listenerOptions.Listener = bound
	listener, err := daemon.Listen(listenerOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { listener.Close() }()

	// Become the active MGR (as in TestCephMgrDaemonControlIntegration).
	beacons := startBeaconLoop(daemon, cephmsgr.MgrBeacon{Metadata: map[string]string{"gotest": "standby"}})
	defer func() {
		if err := beacons.close(); err != nil {
			t.Error("beacon loop", err)
		}
	}()
	standby := nextMgrEvent(t, ctx, daemon, "standby", func(e cephmsgr.MgrMapEvent) bool { return e.Role == cephmsgr.MgrRoleStandby })
	stock := waitMgrDump(t, ctx, control, "acceptor-standby", func(d nativeMgrDump) bool {
		return d.Epoch >= standby.Map.Epoch && d.standby(gid, name)
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
	beacons.set(cephmsgr.MgrBeacon{Available: true, Addresses: advertised, Metadata: map[string]string{"gotest": "acceptor"}})
	published := waitMgrDump(t, ctx, control, "acceptor-available", func(d nativeMgrDump) bool {
		return d.ActiveGID == gid && d.Available && sameNativeAddresses(d.ActiveAddrs.Addrvec, advertised)
	})
	t.Logf("native mgr dump epoch %d: %s/%d active at %v", published.Epoch, name, gid, published.ActiveAddrs.Addrvec)

	mode := base.ConnectionMode
	var sessions []cephmsgr.MgrPeerIdentity
	accept := func(l *cephmsgr.MgrListener, label string) (*cephmsgr.MgrPeer, *peerLog) {
		peer, log := acceptOSD(t, ctx, l, label, gid, advertised[0], mode, osdAddrs)
		id := peer.Identity()
		if len(sessions) > 0 {
			last := sessions[len(sessions)-1]
			if id.Generation <= last.Generation || id.PeerGlobalSeq <= last.PeerGlobalSeq {
				t.Fatalf("%s: generation %d / OSD global_seq %d did not increase past %d / %d", label, id.Generation, id.PeerGlobalSeq, last.Generation, last.PeerGlobalSeq)
			}
		}
		held := rotatingIDs(daemon.State().RotatingKeys)
		if !slices.Contains(held, id.SecretID) {
			t.Fatalf("%s: ticket secret ID %d not among the held rotating MGR secrets %v", label, id.SecretID, held)
		}
		sessions = append(sessions, id)
		return peer, log
	}

	// 1. The stock OSD's session.
	peer, log := accept(listener, "first")
	first := log.firstMessage(t, ctx)
	d := wire.NewDecoder(first.Front)
	if first.Type != 0x700 || d.String() != "0" {
		t.Fatalf("first OSD message: type %#x, MMgrOpen daemon name not \"0\"", first.Type)
	}
	t.Logf("first OSD message: MMgrOpen (type %#x, header %d/%d, front %d bytes) naming daemon \"0\"", first.Type, first.Version, first.CompatVersion, len(first.Front))

	// 2. Reconnection after the MGR closes the session, until a ticket
	// sealed with a newer rotating secret is presented (key rollover).
	deadline := time.Now().Add(3 * time.Minute)
	for reconnects := 0; ; reconnects++ {
		peer.Close()
		peer, log = accept(listener, fmt.Sprintf("reconnect-%d", reconnects+1))
		var ids []uint64
		for _, s := range sessions {
			if !slices.Contains(ids, s.SecretID) {
				ids = append(ids, s.SecretID)
			}
		}
		if len(ids) >= 2 && reconnects >= 1 {
			t.Logf("key rollover: ticket secret IDs %v over %d sessions; held rotating IDs now %v", ids, len(sessions), rotatingIDs(daemon.State().RotatingKeys))
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no newer rotating secret presented by the OSD", ids)
		}
		time.Sleep(4 * time.Second)
	}

	// 3. A stalled connection does not delay the OSD, and times out.
	before := listener.State()
	stall, err := net.Dial("tcp4", bound.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer stall.Close()
	waitFor(t, ctx, "stalled handshake", func() bool { return listener.State().Handshakes == 1 })
	stallStart := time.Now()
	peer.Close()
	peer, log = accept(listener, "beside-a-stalled-peer")
	admitted := time.Now()
	stallAddress := stall.LocalAddr().(*net.TCPAddr).AddrPort()
	stalled := waitRefusalFrom(t, ctx, listener, before.Refused+before.Failed, func(r cephmsgr.MgrListenerRefusal) bool {
		return r.Remote == netip.AddrPortFrom(stallAddress.Addr().Unmap(), stallAddress.Port())
	})
	if stalled.Refused || stalled.Stage != "banner" || !stalled.Time.After(admitted) {
		t.Fatal("stalled connection", stalled, admitted)
	}
	t.Logf("OSD admitted %v after the stall began; the stalled connection ended %v after it began, at stage %s: %s",
		admitted.Sub(stallStart).Round(time.Millisecond), stalled.Time.Sub(stallStart).Round(time.Millisecond), stalled.Stage, stalled.Reason)

	// 4. A refused connection mode, then a retry in the accepted mode.
	other := "crc"
	if mode == cephmsgr.CRCMode {
		other = "secure"
	}
	setOSD("ms_mon_cluster_mode", other)
	osdHost := peer.Identity().Remote.Addr()
	after := listener.State().Refused + listener.State().Failed
	// AuthRegistry observes the option at runtime; give the MON's config
	// push a moment before the OSD reconnects.
	time.Sleep(3 * time.Second)
	peer.Close()
	refusalDeadline := time.Now().Add(90 * time.Second)
	for refused := false; !refused; {
		if time.Now().After(refusalDeadline) {
			t.Fatalf("osd.0 never offered only %s: %+v", other, listener.State())
		}
		wait, stop := context.WithTimeout(ctx, 2*time.Second)
		early, err := listener.Accept(wait)
		stop()
		if err == nil {
			// Admitted with the option not yet applied: not the refused
			// attempt, so end it and let the OSD reconnect again.
			t.Logf("%s session %d admitted before the option applied; closing it", time.Now().Format("15:04:05.000"), early.Identity().Generation)
			early.Close()
			continue
		}
		for _, r := range listener.State().Recent {
			if r.Sequence <= after || r.Remote.Addr() != osdHost {
				continue // records of stock MON peers are refused at HELLO
			}
			after = r.Sequence
			if r.Refused && r.Stage == "authentication" && errors.Is(r.Err, session.ErrModeRefused) {
				t.Logf("%s osd.0 offering only %s refused at %s: %s", r.Time.Format("15:04:05.000"), other, r.Stage, r.Reason)
				refused = true
				break
			}
			t.Logf("other osd.0 record while waiting for the mode refusal: %+v", r)
		}
	}
	removeOSD(ctx, "ms_mon_cluster_mode")
	peer, _ = accept(listener, "after-mode-refusal")

	// 5. Listener shutdown and a new listener on the same address. Stock
	// MONs also dial the active MGR; count their refusals first.
	monRefusals, recentRecords := 0, 0
	for _, r := range listener.State().Recent {
		recentRecords++
		if r.Stage == "hello" && r.Refused && errors.Is(r.Err, session.ErrPeerIdentity) && r.Remote.Addr() != osdHost {
			monRefusals++
		}
	}
	listener.Close()
	select {
	case <-peer.Done():
	case <-ctx.Done():
		t.Fatal("session survived its listener")
	}
	if !errors.Is(peer.Err(), cephmsgr.ErrClosed) {
		t.Fatal("session cause after listener Close", peer.Err())
	}
	var again net.Listener
	waitFor(t, ctx, "rebind", func() bool {
		again, err = net.Listen("tcp4", fmt.Sprintf(":%d", port))
		return err == nil
	})
	listenerOptions.Listener = again
	listener, err = daemon.Listen(listenerOptions)
	if err != nil {
		t.Fatal(err)
	}
	peer, _ = accept(listener, "after-listener-restart")

	// The OSD's own log, read by the fixture, for every published session:
	// connections to the exact advertised endpoint and nonce.
	osdLog := mgrHandshakeLog(t, ctx, control, "acceptor-sessions", "osd", "0", fmt.Sprintf("%s/%d", advertised[0].Endpoint, advertised[0].Nonce))
	idents := osdLog.kinds("client_ident_sent", "server_ident_received")
	for _, s := range sessions {
		// The CLIENT_IDENT line is the OSD's write attempt; its delivery is
		// the acceptor's own record of the same global_seq (s). The
		// SERVER_IDENT line is the OSD's receipt.
		c, v, ok := identExchange(osdLog, strconv.FormatUint(s.PeerGlobalSeq, 10), strconv.FormatUint(s.Generation, 10))
		if !ok || c.Target != formatAddress(advertised[0]) {
			t.Fatalf("generation %d: osd.0 log lacks the CLIENT_IDENT (global_seq %d) and SERVER_IDENT pair within one session: %v", s.Generation, s.PeerGlobalSeq, idents)
		}
		// A one-entry address vector prints without brackets
		// (entity_addrvec_t's operator<<).
		if c.GID != "0" || c.Supported != strconv.FormatUint(s.PeerSupported, 16) || c.Required != strconv.FormatUint(s.PeerRequired, 16) ||
			v.GID != strconv.FormatUint(gid, 10) || v.Addrs != formatAddress(advertised[0]) ||
			v.Supported != strconv.FormatUint(session.MgrOSDPeerFeatures, 16) || v.Required != "800000000200000" || v.Flags != "1" || v.Cookie != "0" {
			t.Fatalf("generation %d: osd.0 logged CLIENT_IDENT %v and SERVER_IDENT %v", s.Generation, c, v)
		}
	}
	// The Go acceptor is a lossy server: the stock OSD never resumes, so it
	// never sends SESSION_RECONNECT here and never receives SESSION_RESET,
	// also on no connection whose peer its log did not name.
	osdLog.absent(t, "lossy Go acceptor", "reconnect_sent", "reset_received", "retry_received", "retry_global_received")
	t.Logf("osd.0 log: %d identification lines and no SESSION_RECONNECT or SESSION_RESET for the MGR endpoint; every one of the %d accepted sessions matches its CLIENT_IDENT global_seq and its SERVER_IDENT MGR global ID %d and generation within one session", len(idents), len(sessions), gid)
	authorizedMu.Lock()
	t.Logf("Authorize hook calls: %d, every one for osd.0", len(authorized))
	authorizedMu.Unlock()
	t.Logf("first listener: %d of its latest %d records are other entity types (stock MON MgrClients) refused at HELLO", monRefusals, recentRecords)

	// Restore a stock active MGR.
	peer.Close()
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
	back := waitMgrDump(t, ctx, control, "acceptor-restored", func(d nativeMgrDump) bool {
		return d.Available && d.ActiveGID != gid && slices.Contains(stockNames, d.ActiveName)
	})
	t.Logf("restored: stock %s/%d active at native epoch %d", back.ActiveName, back.ActiveGID, back.Epoch)
}

// waitRefusalFrom returns the first refusal record after after that ok
// selects. Stock MONs also open MGR sessions (their MgrClient) and are
// refused at HELLO; their records are skipped here and counted at the end.
func waitRefusalFrom(t *testing.T, ctx context.Context, listener *cephmsgr.MgrListener, after uint64, ok func(cephmsgr.MgrListenerRefusal) bool) cephmsgr.MgrListenerRefusal {
	t.Helper()
	for {
		r, err := listener.WaitRefusal(ctx, after)
		if err != nil {
			t.Fatal("refusal record", err, listener.State())
		}
		if ok(r) {
			return r
		}
		after = r.Sequence
	}
}

func waitFor(t *testing.T, ctx context.Context, label string, ok func() bool) {
	t.Helper()
	for !ok() {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for", label)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
