package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
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
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// nativeMgrAddress is one entry of a native `mgr dump` addrvec.
type nativeMgrAddress struct {
	Type  string `json:"type"`
	Addr  string `json:"addr"`
	Nonce uint32 `json:"nonce"`
}

// nativeMgrDump is the part of the native `ceph mgr dump --format json`
// output this test compares, read by the fixture's own CLI (control
// mgr-daemon-dump), never by the Go library.
type nativeMgrDump struct {
	Epoch       uint32 `json:"epoch"`
	ActiveGID   uint64 `json:"active_gid"`
	ActiveName  string `json:"active_name"`
	Available   bool   `json:"available"`
	ActiveAddrs struct {
		Addrvec []nativeMgrAddress `json:"addrvec"`
	} `json:"active_addrs"`
	Standbys []struct {
		GID  uint64 `json:"gid"`
		Name string `json:"name"`
	} `json:"standbys"`
}

func (d nativeMgrDump) standby(gid uint64, name string) bool {
	return slices.ContainsFunc(d.Standbys, func(s struct {
		GID  uint64 `json:"gid"`
		Name string `json:"name"`
	}) bool {
		return s.GID == gid && s.Name == name
	})
}

// mgrDaemonDump requests one native mgr dump. It reports false when the run
// did not declare the mgr-daemon-control capability.
func mgrDaemonDump(t *testing.T, ctx context.Context, control, label string) (nativeMgrDump, bool) {
	t.Helper()
	if err := testcluster.RequestControl(ctx, control, "mgr-daemon-dump", "mgr-daemon-dump-completed", label); err != nil {
		t.Fatal("native mgr dump", label, err)
	}
	data, err := os.ReadFile(filepath.Join(control, "mgr-daemon-dump.json"))
	var receipt struct {
		Label    string          `json:"label"`
		Admitted bool            `json:"admitted"`
		Dump     json.RawMessage `json:"dump"`
	}
	if err != nil || json.Unmarshal(data, &receipt) != nil || receipt.Label != label {
		t.Fatal("native mgr dump receipt", label, err)
	}
	var dump nativeMgrDump
	if receipt.Admitted && (json.Unmarshal(receipt.Dump, &dump) != nil || dump.Epoch == 0) {
		t.Fatal("native mgr dump content", label, string(receipt.Dump))
	}
	return dump, receipt.Admitted
}

// waitMgrDump repeats native dumps until ok holds.
func waitMgrDump(t *testing.T, ctx context.Context, control, label string, ok func(nativeMgrDump) bool) nativeMgrDump {
	t.Helper()
	for attempt := 1; ; attempt++ {
		dump, _ := mgrDaemonDump(t, ctx, control, fmt.Sprintf("%s-%d", label, attempt))
		if ok(dump) {
			return dump
		}
		select {
		case <-ctx.Done():
			t.Fatalf("native MgrMap did not reach %s: %+v", label, dump)
		case <-time.After(time.Second):
		}
	}
}

func sameNativeAddresses(native []nativeMgrAddress, advertised []cephmsgr.ManagerAddress) bool {
	if len(native) != len(advertised) {
		return false
	}
	for i, a := range advertised {
		kind := map[uint32]string{1: "v1", 2: "v2"}[a.Type]
		if native[i].Type != kind || native[i].Addr != a.Endpoint.String() || native[i].Nonce != a.Nonce {
			return false
		}
	}
	return true
}

func adminCommand(t *testing.T, ctx context.Context, admin *cephmsgr.Client, command map[string]any) []byte {
	t.Helper()
	encoded, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	result, err := admin.MonCommand(ctx, cephmsgr.Command{JSON: encoded})
	if err != nil || result.Code != 0 {
		t.Fatalf("native fixture command %v: code %d %q %v", command["prefix"], result.Code, result.Message, err)
	}
	return result.Data
}

// runningServiceTicketTTL returns the auth_service_ticket_ttl every MON of
// the map runs with, read from each MON's own configuration through a
// daemon-local `config get` tell, and fails unless they agree. The MON
// command `config get <who> <key>` is not that value: it answers from the
// central configuration database or the compiled default
// (ceph/ceph/blob/v20.2.4/src/mon/ConfigMonitor.cc, "config get"), while
// the fixture writes the lifetimes into each daemon's ceph.conf. The
// database therefore reports the one-hour default, and a daemon given it
// applies the 30 s cap of the native refresh rule instead of ttl/4.
func runningServiceTicketTTL(t *testing.T, ctx context.Context, admin *cephmsgr.Client) time.Duration {
	t.Helper()
	database := strings.Trim(strings.TrimSpace(string(adminCommand(t, ctx, admin, map[string]any{"prefix": "config get", "who": "mon", "key": "auth_service_ticket_ttl"}))), `"`)
	var ttl time.Duration
	members := admin.Snapshot().Monitor.Members
	if len(members) == 0 {
		t.Fatal("no MON members to read auth_service_ticket_ttl from")
	}
	for _, member := range members {
		result, err := admin.MonTellTo(ctx, member.Name, cephmsgr.Command{JSON: []byte(`{"prefix":"config get","var":"auth_service_ticket_ttl","format":"json"}`)})
		var value struct {
			TTL string `json:"auth_service_ticket_ttl"`
		}
		if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &value) != nil {
			t.Fatalf("MON %s running auth_service_ticket_ttl: code %d %q %v", member.Name, result.Code, result.Message, err)
		}
		seconds, err := strconv.ParseFloat(value.TTL, 64)
		if err != nil || seconds < 1 {
			t.Fatal("MON", member.Name, "running auth_service_ticket_ttl", value.TTL, err)
		}
		running := time.Duration(seconds * float64(time.Second))
		if ttl != 0 && running != ttl {
			t.Fatal("MONs run different auth_service_ticket_ttl values", ttl, running)
		}
		ttl = running
	}
	t.Logf("auth_service_ticket_ttl: %v on all %d MONs (central configuration database: %q)", ttl, len(members), database)
	return ttl
}

// beaconLoop sends the current beacon every two seconds, the native
// mgr_tick_period, until stopped. Beacon content is the test consumer's.
type beaconLoop struct {
	mu      sync.Mutex
	beacon  cephmsgr.MgrBeacon
	stop    chan struct{}
	done    chan struct{}
	failure error
	sent    int
	last    time.Time
}

// status reports how many beacons were queued and when the last one was.
func (l *beaconLoop) status() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return fmt.Sprintf("%d beacons queued, last at %s, failure %v", l.sent, l.last.Format(time.RFC3339Nano), l.failure)
}

func startBeaconLoop(daemon *cephmsgr.MgrDaemon, beacon cephmsgr.MgrBeacon) *beaconLoop {
	l := &beaconLoop{beacon: beacon, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(l.done)
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		for {
			l.mu.Lock()
			current := l.beacon
			l.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			err := daemon.SendBeacon(ctx, current)
			cancel()
			l.mu.Lock()
			if err != nil {
				l.failure = err
				l.mu.Unlock()
				return
			}
			l.sent++
			l.last = time.Now()
			l.mu.Unlock()
			select {
			case <-l.stop:
				return
			case <-tick.C:
			}
		}
	}()
	return l
}

func (l *beaconLoop) set(beacon cephmsgr.MgrBeacon) {
	l.mu.Lock()
	l.beacon = beacon
	l.mu.Unlock()
}

func (l *beaconLoop) close() error {
	select {
	case <-l.stop:
	default:
		close(l.stop)
	}
	<-l.done
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.failure
}

// nextMgrEvent waits for the first event that satisfies ok, logging the
// events it passes over.
func nextMgrEvent(t *testing.T, ctx context.Context, daemon *cephmsgr.MgrDaemon, label string, ok func(cephmsgr.MgrMapEvent) bool) cephmsgr.MgrMapEvent {
	t.Helper()
	for {
		event, err := daemon.NextMgrMap(ctx)
		if err != nil {
			t.Fatalf("MgrMap event %s: %v", label, err)
		}
		t.Logf("%s MgrMap event: epoch %d null %v role %d active %s/%d available %v addrs %v (MON session %d)", time.Now().Format("15:04:05.000"), event.Map.Epoch, event.Null, event.Role, event.Map.ActiveName, event.Map.ActiveGlobalID, event.Map.Available, event.Map.ActiveAddresses, event.MonitorGeneration)
		if ok(event) {
			return event
		}
	}
}

func rotatingIDs(keys cephmsgr.MgrRotatingKeys) []uint64 {
	ids := make([]uint64, 0, len(keys.Keys))
	for _, key := range keys.Keys {
		ids = append(ids, key.ID)
	}
	return ids
}

// connectorHello is what one connection to the advertised listener sent
// first: its remote endpoint and the HELLO that a stock connector sends
// after the banners (ProtocolV2's HELLO: its entity type and the exact
// target address it dialed), both decoded from the same frame. Err ends an
// observation that did not yield a whole HELLO; Entity is still set when
// the entity byte was read (HasEntity).
type connectorHello struct {
	Remote    string
	HasEntity bool
	Entity    uint8
	Target    msgr.Address
	Err       error
}

func (h connectorHello) String() string {
	if h.Err != nil {
		return fmt.Sprintf("%s (entity %d, read %v): %v", h.Remote, h.Entity, h.HasEntity, h.Err)
	}
	return fmt.Sprintf("%s entity %d target %v nonce %d", h.Remote, h.Entity, h.Target.Endpoint, h.Target.Nonce)
}

// readConnectorHello exchanges banners on conn and reads its HELLO within
// ten seconds. Nothing is authenticated.
func readConnectorHello(conn net.Conn) connectorHello {
	h := connectorHello{Remote: conn.RemoteAddr().String()}
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	local := msgr.Banner{Supported: msgr.Revision1 | msgr.Compression, Required: msgr.Revision1}
	if h.Err = msgr.WriteFull(conn, local.Encode()); h.Err != nil {
		return h
	}
	if _, h.Err = msgr.ReadBanner(conn, local); h.Err != nil {
		return h
	}
	frame, err := msgr.NewReader(conn, 4096).Read()
	if err != nil {
		h.Err = err
		return h
	}
	if frame.Tag != msgr.Hello || len(frame.Segments) != 1 || len(frame.Segments[0]) == 0 {
		h.Err = fmt.Errorf("%w: first frame tag %d with %d segments is not a HELLO", msgr.ErrFrame, frame.Tag, len(frame.Segments))
		return h
	}
	d := wire.NewDecoder(frame.Segments[0])
	h.Entity, h.HasEntity = d.U8(), true
	h.Target = msgr.DecodeAddress(d)
	h.Err = d.Done()
	return h
}

// observeHellos accepts connections on listener until it closes and sends
// one observation per connection on out, each read by its own goroutine;
// an observation not taken by the time done closes is dropped. Every
// connection is closed after its HELLO, since accepting a session is not
// part of the control test.
func observeHellos(listener net.Listener, out chan<- connectorHello, done <-chan struct{}) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			h := readConnectorHello(conn)
			select {
			case out <- h:
			case <-done:
			}
		}()
	}
}

// Ceph entity types (CEPH_ENTITY_TYPE_*) a stock connector can announce.
var connectorEntities = map[uint8]string{1: "MON", 2: "MDS", 4: "OSD", 8: "CLIENT", 16: "MGR"}

// awaitOSDHello takes observations until a stock OSD's HELLO with target
// want arrives, and returns it with the observations it passed. A stock MON
// runs a MgrClient too (ceph/ceph/blob/v20.2.4/src/mon/Monitor.cc) and may
// reach the newly published endpoint first: whole HELLOs of other Ceph
// entity types, and connections that closed before sending any HELLO byte,
// are passed over. These end the wait as errors naming the peer's remote
// endpoint: an OSD HELLO with another target, an OSD HELLO that does not
// decode, a malformed first frame or an unknown entity type. ctx bounds the
// wait; its end is an error listing what was passed.
func awaitOSDHello(ctx context.Context, in <-chan connectorHello, want msgr.Address) (connectorHello, []connectorHello, error) {
	var passed []connectorHello
	for {
		select {
		case <-ctx.Done():
			return connectorHello{}, passed, fmt.Errorf("no stock OSD HELLO targeting %v nonce %d before %w; passed %v", want.Endpoint, want.Nonce, ctx.Err(), passed)
		case h := <-in:
			_, known := connectorEntities[h.Entity]
			switch {
			case h.Err != nil && !h.HasEntity && (errors.Is(h.Err, io.EOF) || errors.Is(h.Err, io.ErrUnexpectedEOF)):
				passed = append(passed, h) // closed before its HELLO
			case h.Err != nil && h.HasEntity && h.Entity == 4:
				return h, passed, fmt.Errorf("stock OSD at %s sent a HELLO that does not decode: %w", h.Remote, h.Err)
			case h.Err != nil:
				return h, passed, fmt.Errorf("connector at %s: %w", h.Remote, h.Err)
			case !known:
				return h, passed, fmt.Errorf("connector at %s announced unknown entity type %d", h.Remote, h.Entity)
			case h.Entity != 4:
				passed = append(passed, h)
			case h.Target != want:
				return h, passed, fmt.Errorf("stock OSD at %s targeted %v nonce %d type %d, want %v nonce %d type %d", h.Remote, h.Target.Endpoint, h.Target.Nonce, h.Target.Type, want.Endpoint, want.Nonce, want.Type)
			default:
				return h, passed, nil
			}
		}
	}
}

// TestCephMgrDaemonControlIntegration drives the MGR daemon role's MON
// control channel against stock Tentacle daemons on a disposable fixture.
//
// It creates a scoped mgr.<id> credential (MON profile mgr) and first shows
// that a wrong key is refused explicitly. The daemon then authenticates as
// the MGR entity, and an independent native `mgr dump` lists its
// MON-assigned global ID and name as a standby after its unavailable beacon
// (empty server addrvec). A deliberately premature available beacon while a
// stock MGR is active must bring the MON's direct null MgrMap, which the
// daemon surfaces without raising its epoch, while native dumps before and
// after keep the stock active daemon and its endpoints. With both stock MGR
// containers stopped and failed out of the map, MON selects the daemon by
// global ID first (unavailable), then publishes the exact advertised addrvec
// after an available beacon, compared entry by entry with the native dump. A
// stock OSD then dials that published endpoint: the test's listener reads
// each connector's msgr2 HELLO, passes over other stock connectors (a MON's
// MgrClient can arrive first) and requires an OSD HELLO whose target
// address is the advertised entry with its type and nonce
// (awaitOSDHello). Stopping the MON that holds the session makes the
// daemon reconnect to another MON with the same global ID; the map resent on
// the new session is not delivered again, and native dumps keep the daemon
// active. Rotating MGR secret IDs are recorded across a native refresh, with
// a surviving and a new ID, and across the MON reconnect; in-session ticket
// renewals are counted. No key material is logged. Finally the stopped
// daemons restart and a stock MGR is active and available again.
//
// It does not verify accepting the OSD session (#135), service authorizers
// or MGR reports, VIP addresses, IPv6 or CRC mode, or the auth-epoch
// invalidation path natively (synthetic only). The fixture's ticket TTL is
// short (12 s service and AUTH), so renewal and rotation happen within the
// test; ServiceTicketTTL is the value every MON runs with
// (runningServiceTicketTTL).
//
// Why a refresh with a surviving and a new ID is guaranteed: the leader MON
// adds one key when the current (middle) key has expired, on its tick
// (KeyServer::_rotate_secret, AuthMonitor::tick), and adds more at once only
// when its rotation runs a whole TTL late; the next middle key expires at
// least one TTL after the previous one. The daemon requests a refresh
// ttl/4 (3 s) after the held middle key expired and again every second while
// the set is unchanged, so with the fixture's 5 s mon_tick_interval it
// observes each ordinary rotation alone. Given the configuration database's
// one-hour value instead, the native rule's 30 s cap let two or three
// rotations pass between refreshes, and a run could replace all three IDs
// every time.
//
// Pinned sources: ceph/ceph/blob/v20.2.4/src/mgr/MgrStandby.cc,
// ceph/ceph/blob/v20.2.4/src/mon/MgrMonitor.cc (prepare_beacon, null map),
// ceph/ceph/blob/v20.2.4/src/mon/MonClient.cc (_check_auth_rotating),
// ceph/ceph/blob/v20.2.4/src/auth/cephx/CephxKeyServer.cc (_rotate_secret),
// ceph/ceph/blob/v20.2.4/src/mon/AuthMonitor.cc (tick, check_rotate),
// ceph/ceph/blob/v20.2.4/src/mgr/MgrClient.cc (OSD reconnect on map change),
// ceph/ceph/blob/v20.2.4/src/msg/async/ProtocolV2.cc (HELLO target address).
//
//scenario:row mgr-daemon.aes256k.ip4.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mgrdaemoncontrol key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 capabilities=mgr-daemon-control
//scenario:covers Messenger/CephX
//scenario:covers Daemon-to-daemon
//fixture:isolated stops both stock MGRs and one MON and makes a test MGR active; restarts them with new MGR global IDs
func TestCephMgrDaemonControlIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" || os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "2" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" {
		t.Skip("requires the disposable one-OSD two-MGR fixture")
	}
	requireUsableFixture(t, control)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	if _, admitted := mgrDaemonDump(t, ctx, control, "admission"); !admitted {
		t.Skip("stopping the stock MGRs requires this test alone with fixture capability mgr-daemon-control")
	}
	admin, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ttl := runningServiceTicketTTL(t, ctx, admin)

	// Scoped credential: MON profile mgr, as a native MGR keyring has.
	name := fmt.Sprintf("gotest%d", time.Now().UnixNano()%1_000_000)
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
	base := integrationOptions(t)
	var localMu sync.Mutex
	var local netip.Addr
	options := cephmsgr.MgrDaemonOptions{
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
	}

	// A wrong key for the same principal is refused explicitly.
	wrong := make([]byte, 44)
	binary.LittleEndian.PutUint16(wrong, 2)
	binary.LittleEndian.PutUint16(wrong[10:], 32)
	if _, err := rand.Read(wrong[12:]); err != nil {
		t.Fatal(err)
	}
	wrongOptions := options
	if wrongOptions.Key, err = cephmsgr.ParseKey(base64.StdEncoding.EncodeToString(wrong)); err != nil {
		t.Fatal(err)
	}
	if d, err := cephmsgr.DialMgrDaemon(ctx, wrongOptions); err == nil {
		d.Close()
		t.Fatal("wrong MGR key admitted")
	} else if rejection := (*cephmsgr.AuthenticationError)(nil); !errors.As(err, &rejection) {
		t.Fatal("wrong MGR key was not an explicit MON refusal", err)
	} else {
		t.Logf("wrong key for %s: MON refused method %d with code %d", entity, rejection.Method, rejection.Code)
	}

	daemon, err := cephmsgr.DialMgrDaemon(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	identity := daemon.Identity()
	if identity.Entity != entity || identity.Name != name || identity.GlobalID == 0 || identity.FSID != admin.Snapshot().FSID {
		t.Fatal("daemon identity", identity, admin.Snapshot().FSID)
	}
	t.Logf("%s admitted as MGR(%d), FSID %s", entity, identity.GlobalID, identity.FSID)
	state := daemon.State()
	var ticketServices []string
	for _, ticket := range state.Tickets {
		ticketServices = append(ticketServices, ticket.Service)
	}
	if !slices.Equal(ticketServices, []string{"auth", "mon", "osd", "mds", "mgr"}) {
		t.Fatal("native MgrStandby ticket set", ticketServices)
	}
	first, err := daemon.WaitRotatingKeys(ctx, 0)
	if err != nil || len(first.Keys) != 3 {
		t.Fatal("rotating MGR secrets", first, err)
	}
	t.Logf("rotating MGR secret IDs %v (max %d), MON session %d", rotatingIDs(first), first.MaxVersion, first.MonitorGeneration)

	// Unavailable standby: empty server addrvec.
	beacons := startBeaconLoop(daemon, cephmsgr.MgrBeacon{Metadata: map[string]string{"gotest": "standby"}})
	defer func() {
		if !t.Failed() {
			return
		}
		// Diagnostics only: the beacon schedule and the MON's recent
		// cluster log about managers.
		t.Log("beacon loop:", beacons.status())
		diag, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		encoded, _ := json.Marshal(map[string]any{"prefix": "log last", "num": 200, "channel": "cluster", "format": "json"})
		result, err := admin.MonCommand(diag, cephmsgr.Command{JSON: encoded})
		var entries []struct {
			Stamp   string `json:"stamp"`
			Message string `json:"message"`
		}
		if err == nil && json.Unmarshal(result.Data, &entries) == nil {
			for _, entry := range entries {
				if strings.Contains(entry.Message, "anager") || strings.Contains(entry.Message, "mgr") {
					t.Log("cluster log:", entry.Stamp, entry.Message)
				}
			}
		} else {
			t.Log("cluster log unavailable", err)
		}
	}()
	defer func() {
		if err := beacons.close(); err != nil {
			t.Error("beacon loop", err)
		}
	}()
	standby := nextMgrEvent(t, ctx, daemon, "standby", func(e cephmsgr.MgrMapEvent) bool { return e.Role == cephmsgr.MgrRoleStandby })
	stock := waitMgrDump(t, ctx, control, "standby", func(d nativeMgrDump) bool {
		return d.Epoch >= standby.Map.Epoch && d.standby(identity.GlobalID, name)
	})
	if stock.ActiveGID == 0 || stock.ActiveGID == identity.GlobalID || !stock.Available {
		t.Fatal("expected a stock active MGR", stock)
	}
	t.Logf("native mgr dump epoch %d: %s/%d is a standby; stock %s/%d active at %v", stock.Epoch, name, identity.GlobalID, stock.ActiveName, stock.ActiveGID, stock.ActiveAddrs.Addrvec)

	// A premature available beacon from a non-active daemon.
	premature := []cephmsgr.ManagerAddress{{Type: 2, Nonce: 1, Endpoint: netip.AddrPortFrom(netip.MustParseAddr("192.0.2.77"), 6800)}}
	if err := daemon.SendBeacon(ctx, cephmsgr.MgrBeacon{Available: true, Addresses: premature}); err != nil {
		t.Fatal(err)
	}
	null := nextMgrEvent(t, ctx, daemon, "null map", func(e cephmsgr.MgrMapEvent) bool { return e.Null })
	after, _ := mgrDaemonDump(t, ctx, control, "after-null")
	if null.Role != cephmsgr.MgrRoleAbsent || after.ActiveGID != stock.ActiveGID || !after.standby(identity.GlobalID, name) || sameNativeAddresses(after.ActiveAddrs.Addrvec, premature) {
		t.Fatal("premature available beacon changed the active MGR", null, after)
	}
	if s := daemon.State(); s.NullMgrMaps != 1 || s.MgrMapEpoch == ^uint32(0) {
		t.Fatal("null map raised the delivered epoch", s.MgrMapEpoch, s.NullMgrMaps)
	}
	t.Logf("premature available beacon: MON sent the null MgrMap (epoch %d); native epoch %d still has %s/%d active", null.Map.Epoch, after.Epoch, after.ActiveName, after.ActiveGID)

	// MON-driven selection: stop and fail the stock MGRs.
	stockNames := []string{stock.ActiveName}
	for _, s := range after.Standbys {
		if s.GID != identity.GlobalID {
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
	selected := nextMgrEvent(t, ctx, daemon, "selection", func(e cephmsgr.MgrMapEvent) bool { return e.Role == cephmsgr.MgrRoleActive })
	if selected.Map.ActiveGlobalID != identity.GlobalID || selected.Map.ActiveName != name || selected.Map.Available {
		t.Fatal("selection by global ID", selected.Map)
	}
	waitMgrDump(t, ctx, control, "selected", func(d nativeMgrDump) bool {
		return d.ActiveGID == identity.GlobalID && d.ActiveName == name && !d.Available
	})

	// Availability and the exact advertised addrvec.
	listener, err := net.Listen("tcp4", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	hellos, observing := make(chan connectorHello, 16), make(chan struct{})
	defer close(observing)
	go observeHellos(listener, hellos, observing)
	localMu.Lock()
	host := local
	localMu.Unlock()
	port := listener.Addr().(*net.TCPAddr).Port
	var nonce [4]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	advertised := []cephmsgr.ManagerAddress{{Type: 2, Nonce: binary.LittleEndian.Uint32(nonce[:])&0x7fffffff | 1, Endpoint: netip.AddrPortFrom(host, uint16(port))}}
	beacons.set(cephmsgr.MgrBeacon{Available: true, Addresses: advertised, Metadata: map[string]string{"gotest": "active"}})
	available := nextMgrEvent(t, ctx, daemon, "available", func(e cephmsgr.MgrMapEvent) bool { return e.Role == cephmsgr.MgrRoleActive && e.Map.Available })
	if !slices.Equal(available.Map.ActiveAddresses, advertised) {
		t.Fatal("published addrvec differs from the advertised one", available.Map.ActiveAddresses, advertised)
	}
	published := waitMgrDump(t, ctx, control, "available", func(d nativeMgrDump) bool { return d.ActiveGID == identity.GlobalID && d.Available })
	if !sameNativeAddresses(published.ActiveAddrs.Addrvec, advertised) {
		t.Fatal("native active_addrs differ from the advertised addrvec", published.ActiveAddrs.Addrvec, advertised)
	}
	if got := daemon.State().Advertised; !slices.Equal(got, advertised) {
		t.Fatal("advertised addresses", got)
	}
	t.Logf("native mgr dump epoch %d: %s/%d active and available at %v", published.Epoch, name, identity.GlobalID, published.ActiveAddrs.Addrvec)

	// A stock OSD selects the published endpoint; other stock connectors,
	// such as a MON's MgrClient, may come first.
	want := msgr.Address{Type: advertised[0].Type, Nonce: advertised[0].Nonce, Endpoint: advertised[0].Endpoint}
	osd, passed, err := awaitOSDHello(ctx, hellos, want)
	for _, h := range passed {
		t.Logf("passed connector before the OSD: %v", h)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("stock OSD at %s dialed the published endpoint: HELLO entity type %d, target %v nonce %d", osd.Remote, osd.Entity, osd.Target.Endpoint, osd.Target.Nonce)

	// MON loss: stop the MON holding the session.
	before := daemon.State()
	var mons struct {
		Mons []struct {
			Name  string `json:"name"`
			Addrs struct {
				Addrvec []nativeMgrAddress `json:"addrvec"`
			} `json:"public_addrs"`
		} `json:"mons"`
	}
	if err := json.Unmarshal(adminCommand(t, ctx, admin, map[string]any{"prefix": "mon dump", "format": "json"}), &mons); err != nil {
		t.Fatal(err)
	}
	monName := ""
	for _, mon := range mons.Mons {
		for _, a := range mon.Addrs.Addrvec {
			if a.Type == "v2" && a.Addr == before.Monitor.Endpoint {
				monName = mon.Name
			}
		}
	}
	if monName == "" {
		t.Fatal("held MON endpoint is not in the MonMap", before.Monitor.Endpoint)
	}
	monStopped := true
	defer func() {
		if monStopped {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := testcluster.ControlDaemon(cleanup, control, "start", "mon", monName); err != nil {
				t.Error("restart MON", monName, err)
			}
		}
	}()
	if err := testcluster.ControlDaemon(ctx, control, "stop", "mon", monName); err != nil {
		t.Fatal("stop MON", monName, err)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		s := daemon.State()
		if s.Err != nil {
			t.Fatal("daemon ended on MON loss", s.Err)
		}
		if s.Monitor.Ready && s.Monitor.Generation > before.Monitor.Generation && s.Monitor.Endpoint != before.Monitor.Endpoint {
			if s.Identity.GlobalID != identity.GlobalID {
				t.Fatal("global ID changed across MON reconnect", s.Identity)
			}
			t.Logf("MON %s stopped: session %d -> %d, %s -> %s, same global ID %d", monName, before.Monitor.Generation, s.Monitor.Generation, before.Monitor.Endpoint, s.Monitor.Endpoint, s.Identity.GlobalID)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("daemon did not reconnect to another MON", s.Monitor)
		}
		time.Sleep(200 * time.Millisecond)
	}
	waitMgrDump(t, ctx, control, "after-mon-loss", func(d nativeMgrDump) bool {
		return d.ActiveGID == identity.GlobalID && d.Available && sameNativeAddresses(d.ActiveAddrs.Addrvec, advertised)
	})
	reconnected := waitDaemonState(t, ctx, daemon, func(s cephmsgr.MgrDaemonState) bool {
		return s.StaleMgrMaps > before.StaleMgrMaps || s.MgrMapEpoch > before.MgrMapEpoch
	})
	t.Logf("after reconnection: delivered epoch %d -> %d, maps not delivered again %d -> %d", before.MgrMapEpoch, reconnected.MgrMapEpoch, before.StaleMgrMaps, reconnected.StaleMgrMaps)
	if err := testcluster.ControlDaemon(ctx, control, "start", "mon", monName); err != nil {
		t.Fatal("restart MON", monName, err)
	}
	monStopped = false

	// Rotating refresh: a surviving and a new secret ID.
	keys := first
	for {
		next, err := daemon.WaitRotatingKeys(ctx, keys.Generation)
		if err != nil {
			t.Fatal("rotating refresh", err)
		}
		t.Logf("rotating MGR secret IDs %v (generation %d, MON session %d)", rotatingIDs(next), next.Generation, next.MonitorGeneration)
		survived, added := false, false
		for _, id := range rotatingIDs(next) {
			if slices.Contains(rotatingIDs(keys), id) {
				survived = true
			} else {
				added = true
			}
		}
		if survived && added && next.MonitorGeneration > first.MonitorGeneration {
			break
		}
		keys = next
	}
	// In-session renewal: the 12 s AUTH and service tickets are renewed
	// three quarters into their validity, so one arrives within seconds of
	// admission; the rotation steps above can be observed sooner.
	final := waitDaemonState(t, ctx, daemon, func(s cephmsgr.MgrDaemonState) bool { return s.Renewals > 0 })
	mgrTicket := final.Tickets[slices.IndexFunc(final.Tickets, func(t cephmsgr.MgrDaemonTicket) bool { return t.Service == "mgr" })]
	t.Logf("%d in-session ticket renewals; MGR service ticket secret ID %d, held rotating IDs %v", final.Renewals, mgrTicket.SecretID, rotatingIDs(final.RotatingKeys))

	// Restore a stock active MGR.
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
	back := waitMgrDump(t, ctx, control, "restored", func(d nativeMgrDump) bool {
		return d.Available && d.ActiveGID != identity.GlobalID && slices.Contains(stockNames, d.ActiveName)
	})
	t.Logf("restored: stock %s/%d active at native epoch %d", back.ActiveName, back.ActiveGID, back.Epoch)
}

func waitDaemonState(t *testing.T, ctx context.Context, daemon *cephmsgr.MgrDaemon, ok func(cephmsgr.MgrDaemonState) bool) cephmsgr.MgrDaemonState {
	t.Helper()
	for {
		s := daemon.State()
		if ok(s) {
			return s
		}
		select {
		case <-ctx.Done():
			t.Fatalf("daemon state did not settle: %+v", s)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
