package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// mgrNativeSample is the owned-pool oracle's mgrsample result
// (tools/osd_pool_mode_mgrsample_oracle.py): the MON's OSD map before and
// after, one object's placement, its primary's own pg query and the
// primary's perf counters, none of it read through a MGR.
type mgrNativeSample struct {
	Before, After struct {
		Epoch  uint32            `json:"epoch"`
		UpFrom map[string]uint32 `json:"up_from"`
	}
	Placement osdFixtureOracle `json:"placement"`
	PG        struct {
		PGID          string           `json:"pgid"`
		State         string           `json:"state"`
		ReportedEpoch uint32           `json:"reported_epoch"`
		ReportedSeq   uint64           `json:"reported_seq"`
		Up            []int32          `json:"up"`
		Acting        []int32          `json:"acting"`
		UpPrimary     int32            `json:"up_primary"`
		ActingPrimary int32            `json:"acting_primary"`
		Sum           map[string]int64 `json:"stat_sum"`
	} `json:"pg"`
	Counters map[string]json.RawMessage `json:"counters"`
}

// mgrPeerReceipt is the owned-pool oracle's mgrpeer result
// (tools/osd_pool_mode_mgrpeer_oracle.py): what native librados clients
// observed from the active MGR.
type mgrPeerReceipt struct {
	Echoed, Unanswered struct {
		Daemon   string  `json:"daemon"`
		Shutdown float64 `json:"shutdown_seconds"`
		Timeout  float64 `json:"unregister_timeout"`
		Sent     int     `json:"close_sent"`
		Received int     `json:"close_received"`
	}
	Commands []struct {
		Code    int32   `json:"code"`
		Status  string  `json:"status"`
		Output  []byte  `json:"output"`
		Seconds float64 `json:"seconds"`
	} `json:"commands"`
}

// mgrClientLog is what the test's MGR consumer saw from client peers.
type mgrClientLog struct {
	mu       sync.Mutex
	opens    map[string]cephmsgr.MgrDaemonOpen
	statuses map[string]map[string]string
	closes   map[string]error // acknowledgement outcome per daemon; nil = sent
	commands []*cephmsgr.MgrPeerCommand
	expired  error // the reply to the command held over its connection's end
	held     *cephmsgr.MgrPeerCommand
	failures []string
	iso      *mgrIsolation
}

func (l *mgrClientLog) fail(format string, args ...any) {
	l.mu.Lock()
	l.failures = append(l.failures, fmt.Sprintf(format, args...))
	l.mu.Unlock()
}

func reversed(b []byte) []byte {
	out := slices.Clone(b)
	slices.Reverse(out)
	return out
}

// serveClient is the test's consumer for one client peer: it configures a
// registered service daemon (and no other client, which then sends no
// report), acknowledges every close except noecho's, and answers the three
// test commands. The replace command is held, its connection closed, and
// the reply to that handle recorded; the client's resend on its new
// connection is answered. The stall and answer commands hand their peer to
// the isolation step (mgrIsolation).
func (l *mgrClientLog) serveClient(ctx context.Context, p *cephmsgr.MgrPeer, noecho string) {
	for {
		m, err := p.Next(ctx)
		if err != nil {
			return
		}
		switch {
		case m.Open != nil:
			if !m.Open.ServiceDaemon {
				continue
			}
			l.mu.Lock()
			l.opens[m.Open.DaemonName] = *m.Open
			l.mu.Unlock()
			if err := p.Configure(ctx, cephmsgr.MgrConfigure{StatsPeriod: 1}); err != nil {
				l.fail("configure %s: %v", m.Open.DaemonName, err)
			}
		case m.Report != nil:
			if m.Report.Status != nil {
				l.mu.Lock()
				l.statuses[m.Report.DaemonName] = m.Report.Status
				l.mu.Unlock()
			}
		case m.Close != nil:
			var err error = errors.New("not acknowledged")
			if m.Close.DaemonName != noecho {
				err = m.Close.Acknowledge(ctx)
			}
			l.mu.Lock()
			l.closes[m.Close.DaemonName] = err
			l.mu.Unlock()
		case m.Command != nil && strings.Contains(strings.Join(m.Command.Command, " "), "gotest stall"):
			l.iso.hold(ctx, p, m.Command, l)
			return
		case m.Command != nil && strings.Contains(strings.Join(m.Command.Command, " "), "gotest answer"):
			l.iso.answer(ctx, p, m.Command, l)
			return
		case m.Command != nil:
			c := m.Command
			l.mu.Lock()
			l.commands = append(l.commands, c)
			first := l.held == nil
			l.mu.Unlock()
			var err error
			switch word := strings.Join(c.Command, " "); {
			case strings.Contains(word, "gotest echo"):
				err = c.Reply(ctx, -61, fmt.Sprintf("echo tid %d", c.Transaction), reversed(c.Input))
			case strings.Contains(word, "gotest tell"):
				err = c.Reply(ctx, -7, fmt.Sprintf("tell tid %d", c.Transaction), append(slices.Clone(c.Input), c.Input...))
			case strings.Contains(word, "gotest replace") && first:
				l.mu.Lock()
				l.held = c
				l.mu.Unlock()
				p.Close()
				expired := c.Reply(ctx, 0, "too late", nil)
				l.mu.Lock()
				l.expired = expired
				l.mu.Unlock()
				return
			case strings.Contains(word, "gotest replace"):
				err = c.Reply(ctx, -17, fmt.Sprintf("replaced tid %d", c.Transaction), c.Input)
			default:
				err = c.Reply(ctx, -22, "unknown test command", nil)
			}
			if err != nil {
				l.fail("reply to %v: %v", c.Command, err)
			}
		}
	}
}

// parkedContext is the wait-entry barrier of one MgrPeer.Next or
// MgrListener.Accept call: Parked is closed when the call takes ctx.Done()
// for the second time. Both calls take it once before their lock and again
// in the select that blocks on the peer's (listener's) change signal and
// ctx (cephmsgr/mgr_listener.go), so for a call that dequeues nothing the
// second Done is that select: the call is parked from then on and returns
// only through it. A Next or Accept that took Done differently would leave
// Parked open, and the bounded wait for it fails; it cannot report a call
// as parked earlier than its select. Each parkedContext serves one call.
type parkedContext struct {
	context.Context
	Parked chan struct{}

	mu    sync.Mutex
	calls int
	at    time.Time
}

func newParkedContext(ctx context.Context) *parkedContext {
	return &parkedContext{Context: ctx, Parked: make(chan struct{})}
}

func (c *parkedContext) Done() <-chan struct{} {
	c.mu.Lock()
	c.calls++
	if c.calls == 2 {
		c.at = time.Now()
		close(c.Parked)
	}
	c.mu.Unlock()
	return c.Context.Done()
}

// ParkedAt is when the call reached its select; zero if it never did.
func (c *parkedContext) ParkedAt() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

// mgrWaitReceipt is one Next call on a client peer: when it parked (the
// barrier) and when it returned, the error it returned (nil: it returned a
// message, which no peer of this step may send) and the session's terminal
// cause then (nil: live). Returned is recorded by the calling goroutine
// after Next returns, so it is bounded after the event that released the
// call, never ordered before that event's own return.
type mgrWaitReceipt struct {
	Parked, Returned time.Time
	Err, PeerErr     error
}

func waitReleased(ctx *parkedContext, p *cephmsgr.MgrPeer) mgrWaitReceipt {
	_, err := p.Next(ctx)
	return mgrWaitReceipt{Returned: time.Now(), Parked: ctx.ParkedAt(), Err: err, PeerErr: p.Err()}
}

// mgrIsolation is the consumer side of the isolation step: one native
// client's command is held unanswered while a per-call wait on its session
// is canceled and a second wait is left for Close; the other native
// client's command is answered once the first one is held.
type mgrIsolation struct {
	stalled  chan struct{} // closed when the held command arrived
	answered chan struct{} // closed after the answer was written and replyWait set
	armed    chan struct{} // closed once heldWait is set, before its call

	mu  sync.Mutex
	rec mgrIsolationRecord
}

// mgrIsolationRecord is what the isolation step's consumer recorded.
// heldWait and replyWait are the barriers of the two waits left for Close.
type mgrIsolationRecord struct {
	held, reply         *cephmsgr.MgrPeerCommand
	heldPeer, replyPeer cephmsgr.MgrPeerIdentity
	replyErr, expired   error
	canceledAt          time.Time
	canceled, heldClose mgrWaitReceipt
	replyClose          mgrWaitReceipt
	heldWait, replyWait *parkedContext
	heldDone, replyDone bool
}

func newMgrIsolation() *mgrIsolation {
	return &mgrIsolation{stalled: make(chan struct{}), answered: make(chan struct{}), armed: make(chan struct{})}
}

// hold keeps c unanswered. Its first wait is a per-call context canceled
// once that call has parked and the other peer's answer was written; its
// second wait ends only with the session (the concurrent listener Close).
// The reply to c after that is recorded.
func (s *mgrIsolation) hold(ctx context.Context, p *cephmsgr.MgrPeer, c *cephmsgr.MgrPeerCommand, l *mgrClientLog) {
	s.mu.Lock()
	if s.rec.held != nil {
		s.mu.Unlock()
		l.fail("a second stalled command, tid %d", c.Transaction)
		return
	}
	s.rec.held, s.rec.heldPeer = c, p.Identity()
	s.mu.Unlock()
	close(s.stalled)
	call, cancel := context.WithCancel(ctx)
	first := newParkedContext(call)
	go func() {
		for _, barrier := range []<-chan struct{}{s.answered, first.Parked} {
			select {
			case <-barrier:
			case <-ctx.Done():
			}
		}
		s.mu.Lock()
		s.rec.canceledAt = time.Now()
		s.mu.Unlock()
		cancel()
	}()
	canceled := waitReleased(first, p)
	cancel()
	second := newParkedContext(ctx)
	s.mu.Lock()
	s.rec.heldWait = second
	s.mu.Unlock()
	close(s.armed)
	closed := waitReleased(second, p)
	expired := c.Reply(ctx, 0, "too late", nil)
	s.mu.Lock()
	s.rec.canceled, s.rec.heldClose, s.rec.expired, s.rec.heldDone = canceled, closed, expired, true
	s.mu.Unlock()
}

// answer replies to c once the other command is held, with a status naming
// the TID and the global ID this session authenticated, then waits on the
// session until Close.
func (s *mgrIsolation) answer(ctx context.Context, p *cephmsgr.MgrPeer, c *cephmsgr.MgrPeerCommand, l *mgrClientLog) {
	select {
	case <-s.stalled:
	case <-ctx.Done():
		return
	case <-time.After(30 * time.Second):
		l.fail("the stalled command never arrived before tid %d", c.Transaction)
		return
	}
	id := p.Identity()
	err := c.Reply(ctx, -95, fmt.Sprintf("answer tid %d gid %d", c.Transaction, id.GlobalID), reversed(c.Input))
	wait := newParkedContext(ctx)
	s.mu.Lock()
	if s.rec.reply != nil {
		s.mu.Unlock()
		l.fail("a second answered command, tid %d", c.Transaction)
		return
	}
	s.rec.reply, s.rec.replyPeer, s.rec.replyErr, s.rec.replyWait = c, id, err, wait
	s.mu.Unlock()
	close(s.answered)
	closed := waitReleased(wait, p)
	s.mu.Lock()
	s.rec.replyClose, s.rec.replyDone = closed, true
	s.mu.Unlock()
}

// listenerWorkers counts goroutines running the listener's own workers
// (its accept loop and handshakes, including each published session's
// monitor, which runs that session's Wait). Application calls into the
// listener (Accept, Next, Reply) are not counted.
func listenerWorkers() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "cephmsgr.(*MgrListener).serve") || strings.Contains(g, "cephmsgr.(*MgrListener).handshake") {
			count++
		}
	}
	return count
}

// listenerSockets counts this process's TCP sockets whose local port is
// port, matched by inode between /proc/self/fd and /proc/net/tcp{,6}
// (Linux). A socket the process closed has no descriptor here, also while
// the kernel keeps it in TIME_WAIT.
func listenerSockets(port int) (int, error) {
	inodes := map[string]bool{}
	suffix := fmt.Sprintf(":%04X", port)
	for _, name := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		data, err := os.ReadFile(name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return 0, err
		}
		for _, line := range strings.Split(string(data), "\n")[1:] {
			fields := strings.Fields(line)
			if len(fields) > 9 && strings.HasSuffix(fields[1], suffix) && fields[9] != "0" {
				inodes[fields[9]] = true
			}
		}
	}
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, err
	}
	count := 0
	for _, fd := range fds {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if err == nil && strings.HasPrefix(target, "socket:[") && inodes[strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")] {
			count++
		}
	}
	return count, nil
}

func fsidString(f [16]byte) string {
	h := fmt.Sprintf("%x", f[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// TestCephMgrDaemonMessagesIntegration receives a stock OSD's daemon
// messages and native clients' service and command traffic on a Go MGR
// endpoint (MgrDaemon.Listen), on a disposable one-OSD fixture, as the
// MON-selected active MGR with the stock MGRs stopped (as in
// TestCephMgrAcceptorIntegration).
//
// The stock osd.0's session: its first message is an MMgrOpen naming
// daemon "0" with service_daemon false, and it sends no report until the
// test's MGR configures it (no MMgrReport or MPGStats within 3 s), so the
// reports that follow show the stock peer received MMgrConfigure. A second
// configure with stats_threshold 5 makes its next reports withdraw exactly
// the declared counters whose priority is below 5, and threshold 0 makes it
// declare those again: the stock MgrClient applied the received fields.
// Packed counters are read against that connection's declarations.
//
// Native object I/O and PG statistics: four objects are written natively
// into an owned pool; an MPGStats whose owned-pool PGs hold exactly those
// objects and bytes is awaited. Then a native sample (mgrsample, MON osd
// dump before and after, placement, the primary's own pg query and perf
// dump) is compared with the next MPGStats and MMgrReport the stock OSD
// sends: the sampled PG's ID, up and acting sets and primaries, object,
// byte and write counts and reported epoch, the message's map epoch (with
// the map unchanged across the sample) and the OSD's up_from; and the
// osd.op_w counter and the op_w_latency average count (with the counters
// unchanged across two native samples around the report).
//
// Reconnection: closing the session makes the OSD reconnect with a higher
// generation; it opens again, and its first report after configure
// declares every counter it packs, the same path set as at the end of the
// first session.
//
// Native clients (mgrpeer): a librados service daemon's MMgrOpen carries
// service_daemon, its service name and metadata; after configure its report
// carries its status; its MMgrClose is acknowledged, and its own messenger
// log (debug_ms 1) shows that acknowledgement received, while a second one
// whose close is left unanswered logs its close sent and nothing received.
// (Native MgrClient::shutdown marks itself dead before sending the close and
// drops the echo, so its shutdown time cannot show it.) A native client's
// first command carries TID 0. rados_mgr_command (MMgrCommand) and
// rados_mgr_command_target (a tell, MCommand with the cluster FSID) are
// answered in their family with negative results, a status and binary
// output, which the native client receives exactly, proving the TID too
// (a reply with another TID never completes it). A third command is held,
// its connection closed and the reply to its handle refused as expired
// (known not sent); the native client resends it with the same TID on its
// new connection, and that new handle's reply completes it.
//
// Peer isolation and shutdown (mgrpeers): the native side opens a TCP
// connection that reads the listener's msgr2 banner and sends four bytes of
// its own, so its handshake stays unfinished, and two librados handles,
// identified by their instance IDs, which must equal the global IDs their
// Go sessions authenticated. The first sends a tell that the consumer
// holds unanswered; a per-call wait on that session is canceled once the
// second handle's MMgrCommand was answered, and it returns
// context.Canceled with no message consumed and the session still live.
// The completed reply's receipt: the MMgrCommand family and binary input on
// the Go side, and on the native side the code, a status naming the Go
// TID and the global ID of the session that answered (the native client's
// own instance ID) and the reversed binary input. Every wait asserted
// here passes a wait-entry barrier (parkedContext) before the event that
// releases it is started: the per-call wait is canceled only after it
// parked, and Close starts only after waits on both client sessions and
// the accept loop's current Accept parked with no peer pending. Each call
// must then return its cause (context.Canceled; ErrClosed) within 10 s of
// that event's start; a caller's own record is not ordered against Close
// returning, since Close joins the listener's workers, not its callers.
// The two concurrent MgrListener.Close calls both return; the reply to the
// held handle afterwards is refused as ErrMgrReplyExpired (known not
// sent), and the native tell returns ETIMEDOUT at its own
// rados_mon_op_timeout with no output, never a reply (positive 110:
// RadosClient::mgr_command returns C_SaferCond::wait_for's timeout value
// unnegated, ceph/ceph/blob/v20.2.4/src/common/Cond.h). When Close has
// returned no goroutine runs the listener's accept loop, a handshake or a
// session monitor (which waits for its session's workers), the listener
// reports no handshake, peer or pending session, this process holds no
// socket on the listener's port (/proc/self/fd against /proc/net/tcp),
// and the port refuses a connection. The unfinished
// handshake ended at its banner stage with the native socket seeing EOF,
// and the native network namespace has no established connection to the
// MGR's address left.
//
// Not verified here: MMgrUpdate (only a stock MON sends it, after its own
// version changes; native vectors and synthetic tests cover it), MDS
// peers, OSD perf-metric queries and metric payloads on a live session,
// MgrDaemon.Close racing pending peer waits (the listener's Close is the
// one raced here), a stock peer that stops reading a published session,
// and the unsent or uncertain reply outcomes, including induced partial
// writes (synthetic tests).
//
// Pinned sources: ceph/ceph/blob/v20.2.4/src/mgr/MgrClient.cc,
// ceph/ceph/blob/v20.2.4/src/mgr/DaemonServer.cc,
// ceph/ceph/blob/v20.2.4/src/osd/OSD.cc (collect_pg_stats),
// ceph/ceph/blob/v20.2.4/src/messages/MPGStats.h,
// ceph/ceph/blob/v20.2.4/src/librados/RadosClient.cc (mgr_command).
//
//scenario:row mgr-messages.aes256k.ip4.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mgrdaemonmessages key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 capabilities=mgr-daemon-control
//scenario:covers Daemon-to-daemon
//scenario:covers CLIENT to MON/MGR
//fixture:isolated stops both stock MGRs, makes a test MGR active, writes into an owned pool and changes nothing else; restarts the MGRs
func TestCephMgrDaemonMessagesIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" || os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "2" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" {
		t.Skip("requires the disposable one-OSD two-MGR fixture")
	}
	requireUsableFixture(t, control)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	if _, admitted := mgrDaemonDump(t, ctx, control, "messages-admission"); !admitted {
		t.Skip("stopping the stock MGRs requires this test alone with fixture capability mgr-daemon-control")
	}
	var base osdFixtureOracle
	if raw, err := os.ReadFile(filepath.Join(control, "osd-oracle.json")); err != nil || json.Unmarshal(raw, &base) != nil {
		t.Fatal("fixture OSD oracle", err)
	}
	options := integrationOptions(t)
	admin, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ttl := runningServiceTicketTTL(t, ctx, admin)
	pools := createOwnedPools(t, ctx, base, 1)
	pool := uint64(pools.ids[1])

	name := fmt.Sprintf("gomsg%d", time.Now().UnixNano()%1_000_000)
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
		Monitors: options.Monitors, Identity: entity, Key: key, ConnectionMode: options.ConnectionMode,
		ExpectedFSID: options.ExpectedFSID, ServiceTicketTTL: ttl,
		DialContext: func(ctx context.Context, network, endpoint string) (net.Conn, error) {
			conn, err := options.DialContext(ctx, network, endpoint)
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
	localMu.Lock()
	host := local
	localMu.Unlock()
	var nonce [4]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	advertised := []cephmsgr.ManagerAddress{{Type: 2, Nonce: binary.LittleEndian.Uint32(nonce[:])&0x7fffffff | 1, Endpoint: netip.AddrPortFrom(host, uint16(bound.Addr().(*net.TCPAddr).Port))}}
	listener, err := daemon.Listen(cephmsgr.MgrListenerOptions{
		Listener: bound, Addresses: advertised, ConnectionMode: options.ConnectionMode, HandshakeTimeout: 90 * time.Second,
		Authorize: func(_ context.Context, id cephmsgr.MgrPeerIdentity) error {
			if id.Entity != "osd.0" && id.Entity != "client.test" {
				return errors.New("only osd.0 and the fixture's client.test are expected")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	const noecho = "gomgr-noecho"
	clients := &mgrClientLog{opens: map[string]cephmsgr.MgrDaemonOpen{}, statuses: map[string]map[string]string{}, closes: map[string]error{}, iso: newMgrIsolation()}
	osdPeers := make(chan *cephmsgr.MgrPeer, 8)
	serving, stopServing := context.WithCancel(ctx)
	var served sync.WaitGroup
	defer func() { stopServing(); listener.Close(); served.Wait() }()
	// The accept loop's current call (its barrier) and terminal receipt;
	// the receipt is read after served.Wait.
	var acceptMu sync.Mutex
	var acceptCall *parkedContext
	var acceptEnd mgrWaitReceipt
	served.Add(1)
	go func() {
		defer served.Done()
		for {
			call := newParkedContext(serving)
			acceptMu.Lock()
			acceptCall = call
			acceptMu.Unlock()
			p, err := listener.Accept(call)
			if err != nil {
				acceptEnd = mgrWaitReceipt{Returned: time.Now(), Parked: call.ParkedAt(), Err: err}
				return
			}
			if p.Identity().Entity == "osd.0" {
				osdPeers <- p
				continue
			}
			served.Add(1)
			go func() { defer served.Done(); clients.serveClient(serving, p, noecho) }()
		}
	}()

	// Become the active MGR. The beacon's mgr_features are the consumer's;
	// with SERVER_OCTOPUS among them a native client's mgr command goes as
	// MMgrCommand.
	const mgrFeatures = 0x3f07fffffffdffff
	beacons := startBeaconLoop(daemon, cephmsgr.MgrBeacon{Metadata: map[string]string{"gotest": "standby"}, Features: mgrFeatures})
	defer func() {
		if err := beacons.close(); err != nil {
			t.Error("beacon loop", err)
		}
	}()
	standby := nextMgrEvent(t, ctx, daemon, "standby", func(e cephmsgr.MgrMapEvent) bool { return e.Role == cephmsgr.MgrRoleStandby })
	stock := waitMgrDump(t, ctx, control, "messages-standby", func(d nativeMgrDump) bool {
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
	beacons.set(cephmsgr.MgrBeacon{Available: true, Addresses: advertised, Metadata: map[string]string{"gotest": "messages"}, Features: mgrFeatures})
	waitMgrDump(t, ctx, control, "messages-available", func(d nativeMgrDump) bool {
		return d.ActiveGID == gid && d.Available && sameNativeAddresses(d.ActiveAddrs.Addrvec, advertised)
	})

	nextOSD := func(label string) *cephmsgr.MgrPeer {
		t.Helper()
		select {
		case p := <-osdPeers:
			return p
		case <-time.After(90 * time.Second):
			t.Fatalf("%s: no stock OSD session (listener %+v)", label, listener.State())
		}
		return nil
	}
	next := func(p *cephmsgr.MgrPeer, wait time.Duration) (cephmsgr.MgrInboundMessage, error) {
		w, stop := context.WithTimeout(ctx, wait)
		defer stop()
		return p.Next(w)
	}
	// until reads p's messages until ok accepts one, keeping the latest
	// report and PG statistics seen.
	var lastReport *cephmsgr.MgrPeerReport
	until := func(p *cephmsgr.MgrPeer, label string, ok func(cephmsgr.MgrInboundMessage) bool) cephmsgr.MgrInboundMessage {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			m, err := next(p, time.Until(deadline))
			if err != nil {
				t.Fatalf("%s: %v (peer %v)", label, err, p.Err())
			}
			if m.Report != nil {
				lastReport = m.Report
			}
			if ok(m) {
				return m
			}
		}
		t.Fatal(label, "timed out")
		return cephmsgr.MgrInboundMessage{}
	}
	configure := func(p *cephmsgr.MgrPeer, threshold uint32) {
		t.Helper()
		if err := p.Configure(ctx, cephmsgr.MgrConfigure{StatsPeriod: 1, StatsThreshold: threshold}); err != nil {
			t.Fatal("configure", err)
		}
	}

	// 1. Open, no report before configure, then reports.
	first := nextOSD("first")
	open := until(first, "open", func(m cephmsgr.MgrInboundMessage) bool { return m.Open != nil })
	if open.Open.DaemonName != "0" || open.Open.ServiceDaemon || open.Open.Metadata != nil {
		t.Fatalf("stock OSD open %+v", open.Open)
	}
	for quiet := time.Now().Add(3 * time.Second); time.Now().Before(quiet); {
		m, err := next(first, time.Until(quiet))
		if err != nil {
			break
		}
		if m.Report != nil || m.PGStats != nil {
			t.Fatal("the stock OSD reported before it was configured", m.Type)
		}
	}
	configure(first, 0)
	report := until(first, "first report", func(m cephmsgr.MgrInboundMessage) bool { return m.Report != nil }).Report
	if len(report.Declare) == 0 || len(report.Declare) != len(report.Counters) || report.DaemonName != "0" {
		t.Fatalf("first report: %d declarations, %d counters, daemon %q", len(report.Declare), len(report.Counters), report.DaemonName)
	}
	priority := map[string]uint8{}
	for _, d := range report.Declare {
		priority[d.Path] = d.Priority
	}
	until(first, "first PG statistics", func(m cephmsgr.MgrInboundMessage) bool { return m.PGStats != nil })
	t.Logf("first session generation %d: open of daemon %q; first report after configure declares %d counters (packed %d bytes), then MPGStats",
		first.Identity().Generation, open.Open.DaemonName, len(report.Declare), len(report.Packed))

	// 2. The stock OSD applies the configured threshold.
	var below []string
	for path, p := range priority {
		if p < 5 {
			below = append(below, path)
		}
	}
	slices.Sort(below)
	configure(first, 5)
	withdrawn := until(first, "withdrawal", func(m cephmsgr.MgrInboundMessage) bool { return m.Report != nil && len(m.Report.Undeclare) > 0 }).Report
	gone := slices.Sorted(slices.Values(withdrawn.Undeclare))
	if len(below) == 0 || !slices.Equal(gone, below) {
		t.Fatalf("threshold 5 withdrew %d paths, %d were declared below priority 5", len(gone), len(below))
	}
	configure(first, 0)
	again := until(first, "redeclaration", func(m cephmsgr.MgrInboundMessage) bool { return m.Report != nil && len(m.Report.Declare) > 0 }).Report
	var redeclared []string
	for _, d := range again.Declare {
		redeclared = append(redeclared, d.Path)
	}
	slices.Sort(redeclared)
	if !slices.Equal(redeclared, below) {
		t.Fatalf("threshold 0 redeclared %d paths, %d were withdrawn", len(redeclared), len(below))
	}
	t.Logf("configure threshold 5: the next report withdrew all %d counters declared below priority 5; threshold 0: they were declared again", len(below))

	// 3. Native object I/O on the owned pool, then the matching statistics.
	var objects []string
	total := int64(0)
	for i := range 4 {
		object := fmt.Sprintf("mgr-report-%d", i)
		data := bytes.Repeat([]byte{byte(i + 1)}, 1000+517*i)
		poolSeed(t, ctx, 1, object, "", data, nil)
		objects = append(objects, object)
		total += int64(len(data))
	}
	ownedSums := func(s *cephmsgr.MgrPGStats) (objects, bytes int64) {
		for _, e := range s.PGs {
			if e.PG.Pool == pool {
				objects += e.Stat.Sum.NumObjects
				bytes += e.Stat.Sum.NumBytes
			}
		}
		return
	}
	stats := until(first, "owned-pool statistics", func(m cephmsgr.MgrInboundMessage) bool {
		if m.PGStats == nil {
			return false
		}
		o, b := ownedSums(m.PGStats)
		return o == 4 && b == total
	}).PGStats
	t.Logf("MPGStats epoch %d: owned pool %d holds the 4 natively written objects, %d bytes", stats.Epoch, pool, total)

	// 4. Independent native samples against the next statistics and report.
	sample := func() mgrNativeSample {
		var r struct {
			Sample mgrNativeSample `json:"sample"`
		}
		nativePoolResult(t, ctx, "mgrsample", map[string]any{"slot": 1, "object": objects[0], "counters": []string{"osd.op_w", "osd.op_w_latency"}}, &r)
		return r.Sample
	}
	counterOf := func(r *cephmsgr.MgrPeerReport, path string) (cephmsgr.MgrCounterValue, bool) {
		i := slices.IndexFunc(r.Counters, func(c cephmsgr.MgrCounterValue) bool { return c.Path == path })
		if i < 0 {
			return cephmsgr.MgrCounterValue{}, false
		}
		return r.Counters[i], true
	}
	matched := false
	for attempt := 1; attempt <= 3 && !matched; attempt++ {
		before := sample()
		if before.Before.Epoch != before.After.Epoch {
			t.Logf("attempt %d: the OSD map moved during the sample", attempt)
			continue
		}
		pgStats := until(first, "statistics after the sample", func(m cephmsgr.MgrInboundMessage) bool { return m.PGStats != nil }).PGStats
		rep := until(first, "report after the sample", func(m cephmsgr.MgrInboundMessage) bool { return m.Report != nil }).Report
		after := sample()
		var w struct {
			AvgCount uint64 `json:"avgcount"`
		}
		var opW, opWAfter uint64
		json.Unmarshal(before.Counters["osd.op_w"], &opW)
		json.Unmarshal(after.Counters["osd.op_w"], &opWAfter)
		json.Unmarshal(before.Counters["osd.op_w_latency"], &w)
		if opW != opWAfter || before.After.Epoch != after.Before.Epoch || before.PG.ReportedEpoch != after.PG.ReportedEpoch ||
			before.PG.Sum["num_write"] != after.PG.Sum["num_write"] {
			t.Logf("attempt %d: native counters or map moved across the report (op_w %d -> %d)", attempt, opW, opWAfter)
			continue
		}
		i := slices.IndexFunc(pgStats.PGs, func(e cephmsgr.MgrPGStatsEntry) bool {
			return fmt.Sprintf("%d.%x", e.PG.Pool, e.PG.Seed) == before.PG.PGID
		})
		if i < 0 {
			t.Fatalf("MPGStats lacks the sampled PG %s", before.PG.PGID)
		}
		g := pgStats.PGs[i].Stat
		if !slices.Equal(g.Up, before.PG.Up) || !slices.Equal(g.Acting, before.PG.Acting) || g.UpPrimary != before.PG.UpPrimary ||
			g.ActingPrimary != before.PG.ActingPrimary || g.Sum.NumObjects != before.PG.Sum["num_objects"] || g.Sum.NumBytes != before.PG.Sum["num_bytes"] ||
			g.Sum.NumWrite != before.PG.Sum["num_write"] || g.ReportedEpoch != before.PG.ReportedEpoch || pgStats.Epoch != before.Before.Epoch ||
			pgStats.OSD.UpFrom != before.Before.UpFrom["0"] || before.Placement.PGID != before.PG.PGID {
			t.Fatalf("MPGStats PG %s %+v (epoch %d, up_from %d) differs from the native sample %+v", before.PG.PGID, g, pgStats.Epoch, pgStats.OSD.UpFrom, before)
		}
		value, ok := counterOf(rep, "osd.op_w")
		latency, okLatency := counterOf(rep, "osd.op_w_latency")
		if !ok || !okLatency || value.Value != opW || latency.AvgCount != w.AvgCount || latency.Type&0x4 == 0 {
			t.Fatalf("report osd.op_w %+v op_w_latency %+v, native op_w %d avgcount %d", value, latency, opW, w.AvgCount)
		}
		t.Logf("native sample: PG %s up %v acting %v primary %d, %d objects %d bytes %d writes, reported epoch %d, map epoch %d, osd.0 up_from %d; "+
			"MPGStats matches; report osd.op_w %d and op_w_latency avgcount %d match the primary's perf dump",
			before.PG.PGID, g.Up, g.Acting, g.ActingPrimary, g.Sum.NumObjects, g.Sum.NumBytes, g.Sum.NumWrite, g.ReportedEpoch, pgStats.Epoch, pgStats.OSD.UpFrom, opW, w.AvgCount)
		matched = true
	}
	if !matched {
		t.Fatal("no stable native sample in three attempts")
	}
	declared := []string{}
	for _, c := range lastReport.Counters {
		declared = append(declared, c.Path)
	}

	// 5. Reconnection: a new session declares everything again.
	firstGeneration := first.Identity().Generation
	first.Close()
	second := nextOSD("reconnect")
	if second.Identity().Generation <= firstGeneration {
		t.Fatal("reconnection generation", second.Identity().Generation, firstGeneration)
	}
	until(second, "reopen", func(m cephmsgr.MgrInboundMessage) bool { return m.Open != nil })
	configure(second, 0)
	fresh := until(second, "first report of the new session", func(m cephmsgr.MgrInboundMessage) bool { return m.Report != nil }).Report
	var freshPaths []string
	for _, d := range fresh.Declare {
		freshPaths = append(freshPaths, d.Path)
	}
	slices.Sort(freshPaths)
	if len(fresh.Declare) != len(fresh.Counters) || !slices.Equal(freshPaths, declared) {
		t.Fatalf("new session declared %d of %d packed counters; %d were declared before", len(fresh.Declare), len(fresh.Counters), len(declared))
	}
	t.Logf("session generation %d after reconnection: the first report declares all %d counters again", second.Identity().Generation, len(fresh.Declare))
	served.Add(1)
	go func() {
		defer served.Done()
		for {
			if _, err := second.Next(serving); err != nil {
				return
			}
		}
	}()

	// 6. Native clients: a service daemon, an unanswered close and both
	// command families.
	input := make([]byte, 256)
	for i := range input {
		input[i] = byte(i)
	}
	metadata := map[string]string{"gotest_role": "service"}
	status := map[string]string{"gotest_status": "running"}
	var peer mgrPeerReceipt
	var r struct {
		Peer mgrPeerReceipt `json:"mgr_peer"`
	}
	nativePoolResult(t, ctx, "mgrpeer", map[string]any{
		"mgr_name": name, "daemon": "gomgr-svc", "noecho_daemon": noecho, "metadata": metadata, "status": status,
		"commands": []map[string]any{
			{"tell": false, "command": `{"prefix":"gotest echo"}`, "input": base64.StdEncoding.EncodeToString(input)},
			{"tell": true, "command": `{"prefix":"gotest tell"}`, "input": base64.StdEncoding.EncodeToString([]byte{0, 0xff})},
			{"tell": false, "command": `{"prefix":"gotest replace"}`, "input": base64.StdEncoding.EncodeToString([]byte("r"))},
		},
	}, &r)
	peer = r.Peer
	// A snapshot of the consumer's records; its workers keep running.
	clients.mu.Lock()
	seen := mgrClientLog{opens: maps.Clone(clients.opens), statuses: maps.Clone(clients.statuses), closes: maps.Clone(clients.closes),
		commands: slices.Clone(clients.commands), expired: clients.expired, held: clients.held, failures: slices.Clone(clients.failures)}
	clients.mu.Unlock()
	receipt, _ := json.Marshal(peer)
	t.Logf("native mgrpeer receipt %s", receipt)
	t.Logf("consumer saw service opens %v, statuses %v, closes %v, %d commands", slices.Sorted(maps.Keys(seen.opens)), seen.statuses, seen.closes, len(seen.commands))
	for _, r := range listener.State().Recent {
		if r.Entity != "" {
			t.Logf("listener record %d %s %s: %s", r.Sequence, r.Entity, r.Stage, r.Reason)
		}
	}
	if len(seen.failures) != 0 {
		t.Fatal("consumer failures", seen.failures)
	}
	svc, ok := seen.opens["gomgr-svc"]
	if !ok || svc.ServiceName != "ceph-msgr" || svc.Metadata["gotest_role"] != "service" {
		t.Fatalf("native service daemon open %+v", svc)
	}
	if seen.statuses["gomgr-svc"]["gotest_status"] != "running" {
		t.Fatal("native service daemon status after configure", seen.statuses)
	}
	if err, ok := seen.closes["gomgr-svc"]; !ok || err != nil || peer.Echoed.Sent != 1 || peer.Echoed.Received != 1 {
		t.Fatalf("acknowledged close: %v (seen %v); the native client's log shows %d sent, %d received", err, ok, peer.Echoed.Sent, peer.Echoed.Received)
	}
	if _, ok := seen.closes[noecho]; !ok || peer.Unanswered.Sent != 1 || peer.Unanswered.Received != 0 {
		t.Fatalf("unanswered close seen %v; the native client's log shows %d sent, %d received", ok, peer.Unanswered.Sent, peer.Unanswered.Received)
	}
	t.Logf("native service daemon: open with metadata, status after configure; its own messenger log shows its MMgrClose sent and the acknowledgement received (the unanswered one: sent, none received); shutdowns %.3f s and %.3f s of a %.0f s unregister timeout, which native MgrClient always waits out",
		peer.Echoed.Shutdown, peer.Unanswered.Shutdown, peer.Unanswered.Timeout)
	if len(peer.Commands) != 3 || len(seen.commands) != 4 || seen.held == nil {
		t.Fatalf("native replies %+v, Go commands %d", peer.Commands, len(seen.commands))
	}
	echo, tell, held, resent := seen.commands[0], seen.commands[1], seen.commands[2], seen.commands[3]
	if echo.Family != cephmsgr.MgrCommandFamily || !bytes.Equal(echo.Input, input) || echo.FSID != ([16]byte{}) ||
		peer.Commands[0].Code != -61 || peer.Commands[0].Status != fmt.Sprintf("echo tid %d", echo.Transaction) || !bytes.Equal(peer.Commands[0].Output, reversed(input)) {
		t.Fatalf("MMgrCommand %+v, native reply %+v", echo, peer.Commands[0])
	}
	if tell.Family != cephmsgr.TellCommandFamily || fsidString(tell.FSID) != daemon.Identity().FSID || !bytes.Equal(tell.Input, []byte{0, 0xff}) ||
		peer.Commands[1].Code != -7 || peer.Commands[1].Status != fmt.Sprintf("tell tid %d", tell.Transaction) || !bytes.Equal(peer.Commands[1].Output, []byte{0, 0xff, 0, 0xff}) {
		t.Fatalf("MCommand %+v, native reply %+v", tell, peer.Commands[1])
	}
	if held != seen.held || !errors.Is(seen.expired, cephmsgr.ErrMgrReplyExpired) || !errors.Is(seen.expired, cephmsgr.ErrNotSent) ||
		resent.Transaction != held.Transaction || resent.Family != held.Family || resent.Generation == held.Generation ||
		peer.Commands[2].Code != -17 || peer.Commands[2].Status != fmt.Sprintf("replaced tid %d", resent.Transaction) || string(peer.Commands[2].Output) != "r" {
		t.Fatalf("held %+v expired %v resent %+v, native reply %+v", held, seen.expired, resent, peer.Commands[2])
	}
	t.Logf("native commands: MMgrCommand tid %d -> %d %q with %d reversed bytes; MCommand tid %d (cluster FSID) -> %d %q; held tid %d on generation %d refused as %v after its connection closed, resent with the same tid on generation %d -> %d (%.3f s)",
		echo.Transaction, peer.Commands[0].Code, peer.Commands[0].Status, len(peer.Commands[0].Output), tell.Transaction, peer.Commands[1].Code, peer.Commands[1].Status,
		held.Transaction, held.Generation, seen.expired, resent.Generation, peer.Commands[2].Code, peer.Commands[2].Seconds)

	// 7. Peer isolation, a canceled wait and a concurrent Close: an
	// unfinished handshake, a native client whose tell is held and one
	// whose command is answered; then two concurrent listener Closes.
	iso := clients.iso
	port := bound.Addr().(*net.TCPAddr).Port
	type closeReceipt struct {
		ready                bool
		before, after        cephmsgr.MgrListenerState
		socketsBefore        int
		workersBefore        int
		started, returned    time.Time
		errs                 [2]error
		socketsAfter         int
		workersAfter         int
		socketsErr, redialed error
	}
	var shut closeReceipt
	closer := make(chan struct{})
	go func() {
		defer close(closer)
		// Close only once every call it must release has parked: the held
		// and answered peers' waits and the accept loop's current Accept
		// with no peer pending (a peer published meanwhile starts a new
		// Accept call, which is then awaited).
		limit, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		deadline := limit.Done()
		ready := func(barrier <-chan struct{}) bool {
			select {
			case <-barrier:
				return true
			case <-deadline:
				return false
			}
		}
		parked := ready(iso.armed) && ready(iso.answered)
		if parked {
			iso.mu.Lock()
			held, reply := iso.rec.heldWait, iso.rec.replyWait
			iso.mu.Unlock()
			parked = ready(held.Parked) && ready(reply.Parked)
		}
		for parked {
			acceptMu.Lock()
			call := acceptCall
			acceptMu.Unlock()
			if parked = ready(call.Parked); !parked {
				break
			}
			acceptMu.Lock()
			current := acceptCall == call
			acceptMu.Unlock()
			if current && listener.State().Pending == 0 {
				shut.ready = true
				break
			}
		}
		shut.workersBefore = listenerWorkers()
		shut.before = listener.State()
		shut.socketsBefore, shut.socketsErr = listenerSockets(port)
		shut.started = time.Now()
		var closing sync.WaitGroup
		for i := range shut.errs {
			closing.Add(1)
			go func() { defer closing.Done(); shut.errs[i] = listener.Close() }()
		}
		closing.Wait()
		shut.returned = time.Now()
		shut.after = listener.State()
		shut.workersAfter = listenerWorkers()
		if n, err := listenerSockets(port); err != nil {
			shut.socketsErr = errors.Join(shut.socketsErr, err)
		} else {
			shut.socketsAfter = n
		}
		if c, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", port), 2*time.Second); err == nil {
			c.Close()
		} else {
			shut.redialed = err
		}
	}()
	stallInput, answerInput := []byte{0xde, 0xad, 0, 0xbe, 0xef}, make([]byte, 300)
	for i := range answerInput {
		answerInput[i] = byte(i * 7)
	}
	var isolated struct {
		Peers struct {
			Handshake struct {
				Banner  []byte  `json:"banner"`
				EOF     bool    `json:"eof"`
				Extra   int     `json:"bytes_after_banner"`
				Seconds float64 `json:"eof_seconds"`
			} `json:"handshake"`
			Stalled, Answered struct {
				InstanceID uint64 `json:"instance_id"`
				Reply      struct {
					Code    int32   `json:"code"`
					Status  string  `json:"status"`
					Output  []byte  `json:"output"`
					Seconds float64 `json:"seconds"`
				} `json:"reply"`
				Error string `json:"error"`
			}
			Established int `json:"established_after_close"`
		} `json:"mgr_peers"`
	}
	nativePoolResult(t, ctx, "mgrpeers", map[string]any{
		"mgr_name": name, "address": advertised[0].Endpoint.String(), "stall_timeout": 30, "close_wait": 30,
		"stalled":  map[string]any{"tell": true, "command": `{"prefix":"gotest stall"}`, "input": base64.StdEncoding.EncodeToString(stallInput)},
		"answered": map[string]any{"tell": false, "command": `{"prefix":"gotest answer"}`, "input": base64.StdEncoding.EncodeToString(answerInput)},
	}, &isolated)
	select {
	case <-closer:
	case <-time.After(90 * time.Second):
		t.Fatal("the concurrent listener Closes did not return")
	}
	served.Wait()
	native := isolated.Peers
	raw, _ := json.Marshal(native)
	t.Logf("native mgrpeers receipt %s", raw)
	iso.mu.Lock()
	h := iso.rec
	iso.mu.Unlock()
	clients.mu.Lock()
	failures := slices.Clone(clients.failures)
	clients.mu.Unlock()
	if len(failures) != 0 {
		t.Fatal("consumer failures", failures)
	}
	if !shut.ready || h.held == nil || h.reply == nil || !h.heldDone || !h.replyDone {
		t.Fatalf("isolation step incomplete: close ready %v, held %v, answered %v, waits released %v %v", shut.ready, h.held != nil, h.reply != nil, h.heldDone, h.replyDone)
	}
	// Two independently identified native clients.
	if native.Stalled.InstanceID == 0 || native.Stalled.InstanceID == native.Answered.InstanceID ||
		h.heldPeer.GlobalID != native.Stalled.InstanceID || h.replyPeer.GlobalID != native.Answered.InstanceID ||
		h.heldPeer.Entity != "client.test" || h.replyPeer.Entity != "client.test" || h.heldPeer.Generation == h.replyPeer.Generation {
		t.Fatalf("held peer %s gid %d generation %d, answered peer %s gid %d generation %d; native instances %d and %d",
			h.heldPeer.Entity, h.heldPeer.GlobalID, h.heldPeer.Generation, h.replyPeer.Entity, h.replyPeer.GlobalID, h.replyPeer.Generation,
			native.Stalled.InstanceID, native.Answered.InstanceID)
	}
	// The completed reply: family, TID and binary data, as the native
	// client received them on its own session.
	answered := native.Answered.Reply
	if h.replyErr != nil || h.reply.Family != cephmsgr.MgrCommandFamily || !bytes.Equal(h.reply.Input, answerInput) ||
		answered.Code != -95 || answered.Status != fmt.Sprintf("answer tid %d gid %d", h.reply.Transaction, native.Answered.InstanceID) ||
		!bytes.Equal(answered.Output, reversed(answerInput)) {
		t.Fatalf("answered %+v (reply error %v), native reply %+v", h.reply, h.replyErr, answered)
	}
	t.Logf("completed reply receipt: MMgrCommand tid %d on generation %d from client.test gid %d -> native %d %q with %d reversed bytes (%.3f s), while tid %d (MCommand) of gid %d was held",
		h.reply.Transaction, h.replyPeer.Generation, h.replyPeer.GlobalID, answered.Code, answered.Status, len(answered.Output), answered.Seconds, h.held.Transaction, h.heldPeer.GlobalID)
	// The released waits.
	stalledReply := native.Stalled.Reply
	if h.held.Family != cephmsgr.TellCommandFamily || !bytes.Equal(h.held.Input, stallInput) || native.Stalled.Error != "" ||
		stalledReply.Code != 110 || stalledReply.Status != "" || len(stalledReply.Output) != 0 {
		t.Fatalf("held %+v, native stalled outcome %+v", h.held, native.Stalled)
	}
	// A released call's own record (Returned) is taken by its goroutine
	// after the call returns, so it is only bounded after the releasing
	// event (cancel, Close) began, never ordered against that event's end.
	const releaseBound = 10 * time.Second
	released := func(w mgrWaitReceipt, after time.Time) bool {
		return !w.Parked.IsZero() && w.Parked.Before(after) && !w.Returned.Before(after) && w.Returned.Sub(after) <= releaseBound
	}
	if !errors.Is(h.canceled.Err, context.Canceled) || h.canceled.PeerErr != nil || !released(h.canceled, h.canceledAt) {
		t.Fatalf("per-call wait %+v, canceled at %v", h.canceled, h.canceledAt)
	}
	for label, w := range map[string]mgrWaitReceipt{"held": h.heldClose, "answered": h.replyClose, "accept": acceptEnd} {
		if !errors.Is(w.Err, cephmsgr.ErrClosed) || label != "accept" && !errors.Is(w.PeerErr, cephmsgr.ErrClosed) || !released(w, shut.started) {
			t.Fatalf("%s call across Close %+v (Close started %v)", label, w, shut.started)
		}
	}
	if !errors.Is(h.expired, cephmsgr.ErrMgrReplyExpired) || !errors.Is(h.expired, cephmsgr.ErrNotSent) {
		t.Fatal("reply to the held command after Close", h.expired)
	}
	// Close joined the listener's own workers (accept loop, handshakes and
	// the session monitors that wait for each session's workers), observed
	// when Close returned: nothing of the listener remains on either side.
	if shut.errs != [2]error{} || shut.socketsErr != nil || shut.before.Handshakes < 1 || shut.before.Peers < 3 || shut.socketsBefore < 4 ||
		shut.workersBefore < 2 || shut.workersAfter != 0 ||
		!shut.after.Closed || shut.after.Handshakes != 0 || shut.after.Peers != 0 || shut.after.Pending != 0 || shut.socketsAfter != 0 || shut.redialed == nil {
		t.Fatalf("Close %v (sockets %v): before %d handshakes %d peers %d sockets %d workers; after closed %v, %d handshakes %d peers %d pending %d sockets %d workers; redial error %v",
			shut.errs, shut.socketsErr, shut.before.Handshakes, shut.before.Peers, shut.socketsBefore, shut.workersBefore, shut.after.Closed, shut.after.Handshakes,
			shut.after.Peers, shut.after.Pending, shut.socketsAfter, shut.workersAfter, shut.redialed)
	}
	stage := ""
	for _, r := range shut.after.Recent {
		if r.Entity == "" && !r.Time.Before(shut.started) && r.Stage == "banner" {
			stage = r.Reason
		}
	}
	if !bytes.HasPrefix(native.Handshake.Banner, []byte("ceph v2\n")) || len(native.Handshake.Banner) <= 10 || !native.Handshake.EOF || native.Handshake.Extra != 0 || stage == "" || native.Established != 0 {
		t.Fatalf("unfinished handshake: banner %q, EOF %v after %d more bytes, listener record %q; %d native connections remain",
			native.Handshake.Banner, native.Handshake.EOF, native.Handshake.Extra, stage, native.Established)
	}
	t.Logf("released waits receipt: per-call wait on gid %d parked %s before its cancel, returned %s after it, session live, no message; parked waits on both client sessions (%s, %s before Close) and Accept (%s before) returned %s, %s and %s after Close started with %v; held tid %d answered after Close: %v; native held command returned %d after %.3f s",
		h.heldPeer.GlobalID, h.canceledAt.Sub(h.canceled.Parked), h.canceled.Returned.Sub(h.canceledAt),
		shut.started.Sub(h.heldClose.Parked), shut.started.Sub(h.replyClose.Parked), shut.started.Sub(acceptEnd.Parked),
		h.heldClose.Returned.Sub(shut.started), h.replyClose.Returned.Sub(shut.started), acceptEnd.Returned.Sub(shut.started), h.heldClose.Err,
		h.held.Transaction, h.expired, stalledReply.Code, stalledReply.Seconds)
	t.Logf("close receipt: two concurrent Closes returned in %s; before %d handshakes, %d peers, %d sockets on port %d, %d listener workers; after none, port refuses (%v); the unfinished handshake ended at stage banner (%s) and its native socket saw EOF %.3f s after the answer; no native connection remains",
		shut.returned.Sub(shut.started), shut.before.Handshakes, shut.before.Peers, shut.socketsBefore, port, shut.workersBefore, shut.redialed, stage, native.Handshake.Seconds)

	// Restore a stock active MGR.
	stopServing()
	listener.Close()
	served.Wait()
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
	back := waitMgrDump(t, ctx, control, "messages-restored", func(d nativeMgrDump) bool {
		return d.Available && d.ActiveGID != gid && slices.Contains(stockNames, d.ActiveName)
	})
	t.Logf("restored: stock %s/%d active at native epoch %d", back.ActiveName, back.ActiveGID, back.Epoch)
}
