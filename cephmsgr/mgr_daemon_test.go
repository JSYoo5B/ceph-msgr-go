package cephmsgr

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/maps"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// fakeMgrMon is a synthetic MON for the MGR daemon role. It shares the Go
// frame codec and CephX implementation, so it checks state transitions and
// message routing, not wire compatibility; the native vectors in
// internal/msgr and the native integration test cover those.
type fakeMgrMon struct {
	t     *testing.T
	fsid  [16]byte
	mu    sync.Mutex
	gids  []uint64 // GID per connection; the last repeats
	conns []*fakeMgrMonConn
	ready chan *fakeMgrMonConn
	// rings returns the rotating set for the n-th rotating request (from 0).
	rings      func(n int) []cephx.RotatingKey
	rotations  int
	renewals   int
	beacons    []maps.MgrBeacon
	hello      []uint8
	reclaims   []uint64
	authTTL    time.Duration
	authEpoch  uint32
	onRotating func(conn *fakeMgrMonConn, n int) bool // false withholds the reply
	// monMap overrides the minimum release and FSID of the MonMap sent on
	// the index-th connection (from 0); nil sends release 20 and fsid.
	monMap func(index int) (release uint8, fsid [16]byte)
}

type fakeMgrMonConn struct {
	conn net.Conn
	send func(msgr.MessageData) error
	mu   sync.Mutex
}

func (c *fakeMgrMonConn) push(m msgr.MessageData) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.send(m)
}

func newFakeMgrMon(t *testing.T) *fakeMgrMon {
	return &fakeMgrMon{t: t, fsid: [16]byte{1}, gids: []uint64{4242}, ready: make(chan *fakeMgrMonConn, 16), rings: func(n int) []cephx.RotatingKey {
		key, _ := mockCredential()
		base := time.Now().Add(time.Hour)
		var keys []cephx.RotatingKey
		for i := range 3 {
			keys = append(keys, cephx.RotatingKey{ID: uint64(5 + 3*n + i), Key: key.value, Expires: base.Add(time.Duration(i) * time.Hour)})
		}
		return keys
	}}
}

func (f *fakeMgrMon) options() MgrDaemonOptions {
	key, _ := mockCredential()
	return MgrDaemonOptions{Monitors: []string{"192.0.2.1:3300"}, Identity: "mgr.x", Key: key, ConnectTimeout: time.Second, KeepaliveInterval: 100 * time.Millisecond, KeepaliveTimeout: 30 * time.Second, DialContext: func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		client, peer := net.Pipe()
		go f.serve(peer)
		return client, nil
	}}
}

func encodeRotatingReply(t *testing.T, principal cephx.Key, keys []cephx.RotatingKey, gid uint64) msgr.MessageData {
	t.Helper()
	plain := wire.Encoder{}
	plain.U8(1)
	plain.U32(uint32(len(keys)))
	_, encoded := mockCredential()
	var max uint64
	for _, key := range keys {
		plain.U64(key.ID)
		plain.U8(1)
		plain.Raw(encoded) // the mock credential's CryptoKey encoding
		plain.U32(uint32(key.Expires.Unix()))
		plain.U32(uint32(key.Expires.Nanosecond()))
		max = key.ID
	}
	plain.U64(max)
	ciphertext, err := principal.Seal(0x20, plain.Data)
	if err != nil {
		t.Fatal(err)
	}
	result := wire.Encoder{}
	result.U16(0x0400)
	result.U32(0)
	result.Bytes(ciphertext)
	return authReplyMessage(gid, 0, result.Data)
}

func authReplyMessage(gid uint64, code int32, result []byte) msgr.MessageData {
	e := wire.Encoder{}
	e.U32(2)
	e.U32(uint32(code))
	e.U64(gid)
	e.Bytes(result)
	e.String("")
	return msgr.MessageData{Type: msgr.AuthReplyMessage, Version: 1, CompatVersion: 1, Front: e.Data}
}

func (f *fakeMgrMon) serve(conn net.Conn) {
	defer conn.Close()
	f.mu.Lock()
	index := len(f.conns)
	gid := f.gids[min(index, len(f.gids)-1)]
	authTTL, authEpoch := f.authTTL, f.authEpoch
	release, fsid := uint8(20), f.fsid
	if f.monMap != nil {
		release, fsid = f.monMap(index)
	}
	f.mu.Unlock()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	cfg := peerConfig{fsid: f.fsid, release: 20, role: 1, clientID: gid, entity: 0x10, name: "x", services: cephx.MgrDaemonServices, authTTL: authTTL, mgrTTL: authTTL,
		helloEntity: func(e uint8) { f.mu.Lock(); f.hello = append(f.hello, e); f.mu.Unlock() },
		authProof: func(previous, _ uint64, _ []byte) {
			f.mu.Lock()
			f.reclaims = append(f.reclaims, previous)
			f.mu.Unlock()
		}}
	r, w, err := mockAuthenticate(conn, cfg)
	if err != nil {
		return
	}
	var sequence uint64
	c := &fakeMgrMonConn{conn: conn}
	c.send = func(m msgr.MessageData) error {
		sequence++
		m.Sequence = sequence
		if m.Version == 0 {
			m.Version = 1
		}
		return w.Write(m.Frame())
	}
	f.mu.Lock()
	f.conns = append(f.conns, c)
	f.mu.Unlock()
	if c.push(msgr.MessageData{Type: msgr.MonMapMessage, Front: mockMonMapAuthEpoch(release, fsid, []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}}, 1, authEpoch)}) != nil {
		return
	}
	f.ready <- c
	key, encoded := mockCredential()
	for {
		frame, err := r.Read()
		if err != nil {
			return
		}
		if frame.Tag != msgr.Message {
			continue
		}
		m, err := msgr.DecodeMessage(frame)
		if err != nil {
			return
		}
		switch m.Type {
		case msgr.MgrBeaconMessage:
			beacon, err := decodeTestMgrBeacon(m)
			if err != nil {
				f.t.Error("beacon decode", err)
				return
			}
			f.mu.Lock()
			f.beacons = append(f.beacons, beacon)
			f.mu.Unlock()
		case msgr.AuthMessage:
			d := wire.NewDecoder(m.Front)
			d.Raw(18)
			if d.U32() != 2 {
				f.t.Error("MAuth protocol")
				return
			}
			payload := d.Bytes()
			d.U32()
			if d.Done() != nil || len(payload) < 2 {
				f.t.Error("MAuth shape")
				return
			}
			if payload[0] == 0x00 && payload[1] == 0x04 {
				f.mu.Lock()
				n := f.rotations
				f.rotations++
				keys, hook := f.rings(n), f.onRotating
				f.mu.Unlock()
				if hook != nil && !hook(c, n) {
					continue
				}
				if c.push(encodeRotatingReply(f.t, key.value, keys, gid)) != nil {
					return
				}
				continue
			}
			f.mu.Lock()
			f.renewals++
			ttl := f.authTTL
			f.mu.Unlock()
			tickets, err := mockTicket(key.value, encoded, cephx.ServiceAuth, ttl)
			if err != nil {
				return
			}
			extra, err := mockServiceTickets(key.value, encoded, cephx.MgrDaemonServices, ttl)
			if err != nil {
				return
			}
			result := wire.Encoder{}
			result.U16(0x100)
			result.U32(0)
			result.Raw(tickets)
			result.Bytes(nil)
			result.Bytes(extra)
			if c.push(authReplyMessage(gid, 0, result.Data)) != nil {
				return
			}
		}
	}
}

func (f *fakeMgrMon) next(t *testing.T) *fakeMgrMonConn {
	t.Helper()
	select {
	case c := <-f.ready:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("no MON session")
		return nil
	}
}

// decodeTestMgrBeacon reads an MMgrBeacon v11 front for the tests; the
// product only encodes beacons.
func decodeTestMgrBeacon(m msgr.MessageData) (maps.MgrBeacon, error) {
	var b maps.MgrBeacon
	if m.Type != msgr.MgrBeaconMessage || m.Version != 11 || m.CompatVersion != 8 || m.Priority != 196 {
		return b, msgr.ErrFrame
	}
	d := wire.NewDecoder(m.Front)
	d.Raw(18)
	b.ServerAddresses = msgr.DecodeAddresses(d)
	b.GlobalID = d.U64()
	b.Available = d.Bool()
	b.Name = d.String()
	copy(b.FSID[:], d.Raw(16))
	n := d.Count(4, 1024)
	for range n {
		_ = d.String()
	}
	n = d.Count(6, 1024)
	for range n {
		_, c := d.Struct(1)
		command := maps.MgrCommand{Command: c.String(), Help: c.String(), Module: c.String(), Permissions: c.String()}
		_ = c.String()
		command.Flags = c.U64()
		d.Fail(c.Done())
		b.Commands = append(b.Commands, command)
	}
	for _, target := range []*map[string]string{&b.Metadata, &b.Services} {
		n = d.Count(8, 1024)
		*target = make(map[string]string, n)
		for range n {
			k := d.String()
			(*target)[k] = d.String()
		}
	}
	n = d.Count(6, 1024)
	for range n {
		_, info := d.Struct(2)
		b.Modules = append(b.Modules, maps.ModuleInfo{Name: info.String(), CanRun: info.Bool(), ErrorString: info.String()})
	}
	b.Features = d.U64()
	n = d.Count(5, 1024)
	for range n {
		b.Clients = append(b.Clients, maps.MgrClient{Addresses: msgr.DecodeAddresses(d)})
	}
	if names := d.Count(4, 1024); names != n {
		return b, msgr.ErrFrame
	}
	for i := range n {
		b.Clients[i].Name = d.String()
	}
	return b, d.Done()
}

func testMgrMap(epoch uint32, active uint64, available bool, addresses []msgr.Address, standbys ...uint64) msgr.MessageData {
	e := wire.Encoder{}
	e.U32(epoch)
	msgr.EncodeAddresses(&e, addresses)
	e.U64(active)
	if available {
		e.U8(1)
	} else {
		e.U8(0)
	}
	e.String("a")
	e.U32(uint32(len(standbys)))
	for _, gid := range standbys {
		e.U64(gid)
		info := wire.Encoder{}
		info.U64(gid)
		info.String("x")
		info.U32(0)
		info.U32(0)
		info.U64(0)
		e.Struct(4, 1, info.Data)
	}
	e.U32(0)
	e.U32(0)
	e.U32(0)
	mockMgrMapTail(&e)
	out := wire.Encoder{}
	out.Struct(14, 6, e.Data)
	return msgr.MessageData{Type: msgr.MgrMapMessage, Front: out.Data}
}

func nextEvent(t *testing.T, d *MgrDaemon) MgrMapEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	event, err := d.NextMgrMap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func waitDaemon(t *testing.T, d *MgrDaemon, ok func(MgrDaemonState) bool) MgrDaemonState {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		state := d.State()
		if ok(state) {
			return state
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon state did not settle: %+v", state)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMgrDaemonRejectsNonManagerIdentityBeforeDialing(t *testing.T) {
	f := newFakeMgrMon(t)
	options := f.options()
	var dialed atomic.Bool
	options.DialContext = func(context.Context, string, string) (net.Conn, error) {
		dialed.Store(true)
		return nil, errors.New("dialed")
	}
	for _, identity := range []string{"client.x", "mgr.", "osd.1", "x"} {
		options.Identity = identity
		if d, err := DialMgrDaemon(context.Background(), options); err == nil || d != nil {
			t.Fatal("accepted identity", identity)
		}
	}
	if dialed.Load() {
		t.Fatal("dialed before refusing the identity")
	}
}

func TestMgrDaemonIdentityTicketsBeaconAndMapRoles(t *testing.T) {
	f := newFakeMgrMon(t)
	d, err := DialMgrDaemon(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c := f.next(t)
	identity := d.Identity()
	if identity != (MgrDaemonIdentity{Entity: "mgr.x", Name: "x", GlobalID: 4242, FSID: "01000000-0000-0000-0000-000000000000"}) {
		t.Fatal("identity", identity)
	}
	f.mu.Lock()
	hello := append([]uint8(nil), f.hello...)
	f.mu.Unlock()
	if !reflect.DeepEqual(hello, []uint8{0x10}) {
		t.Fatal("HELLO did not name the MGR entity type", hello)
	}
	state := d.State()
	var services []string
	for _, ticket := range state.Tickets {
		services = append(services, ticket.Service)
	}
	if !reflect.DeepEqual(services, []string{"auth", "mon", "osd", "mds", "mgr"}) {
		t.Fatal("native MgrStandby ticket set", services)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	keys, err := d.WaitRotatingKeys(ctx, 0)
	if err != nil || len(keys.Keys) != 3 || keys.Keys[0].ID != 5 || keys.Keys[2].ID != 7 || keys.MaxVersion != 7 || keys.Generation != 1 || keys.MonitorGeneration != 1 {
		t.Fatal("rotating keys", keys, err)
	}
	if _, ok := d.rotatingKey(6); !ok {
		t.Fatal("held rotating key 6 not found")
	}
	if _, ok := d.rotatingKey(8); ok {
		t.Fatal("unknown rotating ID found")
	}
	if err := d.SendBeacon(ctx, MgrBeacon{Metadata: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	advertised := []ManagerAddress{{Type: 2, Nonce: 77, Endpoint: netip.MustParseAddrPort("192.0.2.9:6800")}, {Type: 1, Nonce: 77, Endpoint: netip.MustParseAddrPort("192.0.2.9:6801")}}
	if err := d.SendBeacon(ctx, MgrBeacon{Available: true, Addresses: advertised, Commands: []MgrCommandDescriptor{{Command: "c", Module: "mgr", Permissions: "r", Flags: 8}}, Clients: []MgrBeaconClient{{Name: "z"}, {Name: "a", Addresses: advertised[:1]}}}); err != nil {
		t.Fatal(err)
	}
	var beacons []maps.MgrBeacon
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		f.mu.Lock()
		beacons = append([]maps.MgrBeacon(nil), f.beacons...)
		f.mu.Unlock()
		if len(beacons) == 2 {
			break
		}
	}
	if len(beacons) != 2 || beacons[0].GlobalID != 4242 || beacons[0].Name != "x" || beacons[0].FSID != f.fsid || beacons[0].Available || len(beacons[0].ServerAddresses) != 0 || beacons[0].Metadata["k"] != "v" {
		t.Fatal("standby beacon", beacons)
	}
	if !beacons[1].Available || !reflect.DeepEqual(beacons[1].ServerAddresses, wireAddresses(advertised)) || len(beacons[1].Commands) != 1 || beacons[1].Commands[0].Flags != 8 || beacons[1].Clients[0].Name != "a" || beacons[1].Clients[1].Name != "z" {
		t.Fatal("available beacon", beacons[1])
	}
	if got := d.State().Advertised; !reflect.DeepEqual(got, advertised) {
		t.Fatal("advertised addresses", got)
	}
	active := []msgr.Address{{Type: 2, Nonce: 77, Endpoint: netip.MustParseAddrPort("192.0.2.9:6800")}, {Type: 1, Nonce: 77, Endpoint: netip.MustParseAddrPort("192.0.2.9:6801")}}
	for _, m := range []msgr.MessageData{
		testMgrMap(2, 99, true, active[:1], 4242),
		testMgrMap(3, 4242, false, nil, 99),
		testMgrMap(2, 99, true, active[:1], 4242), // older: never a rollback
		testMgrMap(maps.NullMgrEpoch, 0, false, nil),
		testMgrMap(4, 4242, true, active),
	} {
		if err := c.push(m); err != nil {
			t.Fatal(err)
		}
	}
	if e := nextEvent(t, d); e.Role != MgrRoleStandby || e.Map.Epoch != 2 || e.Null || e.MonitorGeneration != 1 {
		t.Fatal("standby event", e)
	}
	if e := nextEvent(t, d); e.Role != MgrRoleActive || e.Map.Epoch != 3 || e.Map.Available {
		t.Fatal("selection event", e)
	}
	if e := nextEvent(t, d); !e.Null || e.Role != MgrRoleAbsent || e.Map.Epoch != maps.NullMgrEpoch {
		t.Fatal("null map event", e)
	}
	e := nextEvent(t, d)
	if e.Role != MgrRoleActive || e.Map.Epoch != 4 || !e.Map.Available || !reflect.DeepEqual(e.Map.ActiveAddresses, managerAddresses(active)) {
		t.Fatal("ordinary map after the null map", e)
	}
	state = d.State()
	if state.MgrMapEpoch != 4 || state.StaleMgrMaps != 1 || state.NullMgrMaps != 1 || state.Role != MgrRoleActive {
		t.Fatal("map counters", state.MgrMapEpoch, state.StaleMgrMaps, state.NullMgrMaps, state.Role)
	}
}

func TestMgrDaemonRenewsTicketsInSessionAndRefreshesOnInvalidation(t *testing.T) {
	f := newFakeMgrMon(t)
	f.authTTL = 2 * time.Second
	d, err := DialMgrDaemon(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c := f.next(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := d.WaitRotatingKeys(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	// RenewAfter is three quarters of the 2 s validity.
	state := waitDaemon(t, d, func(s MgrDaemonState) bool { return s.Renewals > 0 })
	if state.Monitor.Generation != 1 || state.Identity.GlobalID != 4242 {
		t.Fatal("renewal replaced the session or identity", state)
	}
	// A service-key wipe raises the MonMap auth epoch: the held set is
	// discarded and fetched again on the same session.
	if err := c.push(msgr.MessageData{Type: msgr.MonMapMessage, Front: mockMonMapAuthEpoch(20, f.fsid, []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}}, 2, 1)}); err != nil {
		t.Fatal(err)
	}
	second, err := d.WaitRotatingKeys(ctx, first.Generation)
	if err != nil || second.Invalidations != 1 || second.Keys[0].ID == first.Keys[0].ID || second.MonitorGeneration != 1 {
		t.Fatal("refresh after invalidation", first, second, err)
	}
}

// TestMgrDaemonStaleRotatingReplyCannotRepublish parks a verified rotating
// reply of the first MON session before its publication, replaces the
// session with one whose MonMap raises the auth epoch, lets the new session
// publish, then releases the old reply: the visible set must not change.
func TestMgrDaemonStaleRotatingReplyCannotRepublish(t *testing.T) {
	f := newFakeMgrMon(t)
	parked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hooks := &mgrDaemonHooks{beforeRotatingPublish: func(generation uint64) {
		if generation == 1 {
			once.Do(func() { close(parked); <-release })
		}
	}}
	ctx := context.WithValue(context.Background(), mgrDaemonHooksKey{}, hooks)
	d, err := DialMgrDaemon(ctx, f.options())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	first := f.next(t)
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("first rotating reply never arrived")
	}
	// MON loss while the old reply waits; the next MON also reports a
	// service-key wipe (a higher auth epoch), which invalidates the set.
	f.mu.Lock()
	f.authEpoch = 1
	f.mu.Unlock()
	first.conn.Close()
	f.next(t)
	wait, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	current, err := d.WaitRotatingKeys(wait, 0)
	if err != nil || current.MonitorGeneration != 2 || current.Keys[0].ID != 8 || current.Invalidations != 1 {
		t.Fatal("new session set", current, err)
	}
	close(release)
	state := waitDaemon(t, d, func(s MgrDaemonState) bool { return s.StaleRotatingReplies == 1 })
	if !reflect.DeepEqual(state.RotatingKeys, current) {
		t.Fatal("stale reply changed the visible set", state.RotatingKeys, current)
	}
}

func TestMgrDaemonReconnectReclaimsGlobalIDAndFencesAnotherID(t *testing.T) {
	f := newFakeMgrMon(t)
	f.gids = []uint64{4242, 4242, 5000}
	d, err := DialMgrDaemon(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	f.next(t).conn.Close()
	second := f.next(t)
	state := waitDaemon(t, d, func(s MgrDaemonState) bool { return s.Monitor.Ready && s.Monitor.Generation == 2 })
	f.mu.Lock()
	reclaims := append([]uint64(nil), f.reclaims...)
	f.mu.Unlock()
	if state.Identity.GlobalID != 4242 || !reflect.DeepEqual(reclaims, []uint64{0, 4242}) {
		t.Fatal("reconnection did not reclaim the global ID", state.Identity, reclaims)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.SendBeacon(ctx, MgrBeacon{}); err != nil {
		t.Fatal("beacon after reconnection", err)
	}
	second.conn.Close()
	state = waitDaemon(t, d, func(s MgrDaemonState) bool { return s.Err != nil })
	if !errors.Is(state.Err, ErrMgrDaemonIdentityChanged) {
		t.Fatal("a new global ID was not fenced", state.Err)
	}
	if _, err := d.NextMgrMap(ctx); !errors.Is(err, ErrMgrDaemonIdentityChanged) {
		t.Fatal("waits do not report the fence", err)
	}
	if err := d.SendBeacon(ctx, MgrBeacon{}); !errors.Is(err, ErrMgrDaemonIdentityChanged) {
		t.Fatal("beacon under another identity", err)
	}
}

func TestMgrDaemonMapOverflowFailsClosedAfterDraining(t *testing.T) {
	f := newFakeMgrMon(t)
	options := f.options()
	options.MaxBufferedMaps = 2
	d, err := DialMgrDaemon(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c := f.next(t)
	for epoch := uint32(2); epoch <= 4; epoch++ {
		if err := c.push(testMgrMap(epoch, 99, false, nil, 4242)); err != nil {
			break
		}
	}
	waitDaemon(t, d, func(s MgrDaemonState) bool { return s.Err != nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, epoch := range []uint32{2, 3} {
		if e, err := d.NextMgrMap(ctx); err != nil || e.Map.Epoch != epoch {
			t.Fatal("accepted map lost", epoch, e, err)
		}
	}
	if _, err := d.NextMgrMap(ctx); !errors.Is(err, ErrMgrMapOverflow) {
		t.Fatal("overflow", err)
	}
}

// TestMgrDaemonBeaconSurvivesCallerCancellation cancels each beacon's context
// as soon as SendBeacon returns, as a consumer's per-call timeout does. Every
// admitted beacon must still reach the MON.
func TestMgrDaemonBeaconSurvivesCallerCancellation(t *testing.T) {
	f := newFakeMgrMon(t)
	d, err := DialMgrDaemon(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	f.next(t)
	const beacons = 50
	for i := range beacons {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := d.SendBeacon(ctx, MgrBeacon{Metadata: map[string]string{"n": strconv.Itoa(i)}})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		f.mu.Lock()
		n := len(f.beacons)
		f.mu.Unlock()
		if n == beacons {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d admitted beacons reached the MON", n, beacons)
		}
	}
}

// TestMgrDaemonRetiresSessionWithUnansweredAuthRequest withholds the first
// rotating reply on a MON session that otherwise stays live. The request's
// deadline (KeepaliveTimeout) must retire that session, and the next session
// must refresh from an empty request queue.
func TestMgrDaemonRetiresSessionWithUnansweredAuthRequest(t *testing.T) {
	f := newFakeMgrMon(t)
	f.onRotating = func(_ *fakeMgrMonConn, n int) bool { return n > 0 }
	options := f.options()
	options.KeepaliveTimeout = 1500 * time.Millisecond
	d, err := DialMgrDaemon(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	first := f.next(t)
	// Keep the first transport live, so only the request deadline can end it.
	live := make(chan struct{})
	defer close(live)
	go func() {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-live:
				return
			case <-tick.C:
				if first.push(testMgrMap(2, 99, false, nil, 4242)) != nil {
					return
				}
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	keys, err := d.WaitRotatingKeys(ctx, 0)
	if err != nil || keys.MonitorGeneration != 2 || keys.Keys[0].ID != 8 {
		t.Fatal("refresh after the unanswered request", keys, err)
	}
	state := d.State()
	if state.Err != nil || state.Identity.GlobalID != 4242 || state.Monitor.Generation != 2 {
		t.Fatal("deadline ended the daemon or changed its identity", state)
	}
}

// TestMgrDaemonFatalMonMapCausesEndTheDaemon sends a MonMap of another
// cluster, one below Tentacle, or an undecodable MgrMap, on an established
// session and on a reconnecting session's admission. Each must end the
// daemon with that cause, without another MON session.
func TestMgrDaemonFatalMonMapCausesEndTheDaemon(t *testing.T) {
	other := [16]byte{2}
	invalidMgrMap := msgr.MessageData{Type: msgr.MgrMapMessage, Front: testMgrMap(2, 99, false, nil, 4242).Front[:20]}
	for _, test := range []struct {
		name      string
		reconnect bool
		release   uint8
		fsid      [16]byte
		message   *msgr.MessageData
		cause     func(error) bool
	}{
		{name: "established FSID", fsid: other, cause: isFSIDMismatch},
		{name: "established release", release: 19, cause: isReleaseRejection},
		{name: "established MgrMap", message: &invalidMgrMap, cause: isTerminalFrame},
		{name: "reconnect FSID", reconnect: true, fsid: other, cause: isFSIDMismatch},
		{name: "reconnect release", reconnect: true, release: 19, cause: isReleaseRejection},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFakeMgrMon(t)
			release, fsid := test.release, test.fsid
			if release == 0 {
				release = 20
			}
			if fsid == [16]byte{} {
				fsid = f.fsid
			}
			if test.reconnect {
				f.monMap = func(index int) (uint8, [16]byte) {
					if index == 0 {
						return 20, f.fsid
					}
					return release, fsid
				}
			}
			d, err := DialMgrDaemon(context.Background(), f.options())
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			c := f.next(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := d.WaitRotatingKeys(ctx, 0); err != nil {
				t.Fatal(err)
			}
			switch {
			case test.reconnect:
				c.conn.Close()
			case test.message != nil:
				if err := c.push(*test.message); err != nil {
					t.Fatal(err)
				}
			default:
				if err := c.push(msgr.MessageData{Type: msgr.MonMapMessage, Front: mockMonMapAuthEpoch(release, fsid, []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}}, 2, 0)}); err != nil {
					t.Fatal(err)
				}
			}
			state := waitDaemon(t, d, func(s MgrDaemonState) bool { return s.Err != nil })
			if !test.cause(state.Err) {
				t.Fatal("terminal cause", state.Err)
			}
			if _, err := d.NextMgrMap(ctx); !test.cause(err) {
				t.Fatal("waits do not report the cause", err)
			}
			// No retry as MON loss: no session after the failing one.
			time.Sleep(700 * time.Millisecond)
			f.mu.Lock()
			sessions := len(f.conns)
			f.mu.Unlock()
			want := 1
			if test.reconnect {
				want = 2
			}
			if sessions != want {
				t.Fatalf("%d MON sessions, want %d", sessions, want)
			}
		})
	}
}

func isFSIDMismatch(err error) bool {
	var terminal *mgrDaemonTerminal
	return errors.As(err, &terminal) && strings.Contains(err.Error(), "FSID mismatch")
}

func isReleaseRejection(err error) bool { return errors.Is(err, maps.ErrRelease) }

func isTerminalFrame(err error) bool {
	var terminal *mgrDaemonTerminal
	return errors.As(err, &terminal) && errors.Is(err, msgr.ErrFrame)
}

// TestMgrDaemonIgnoresFatalCauseOfReplacedSession checks that only the
// current MON session's cause can end the daemon.
func TestMgrDaemonIgnoresFatalCauseOfReplacedSession(t *testing.T) {
	replaced, current := &session.Session{}, &session.Session{}
	d := &MgrDaemon{changed: make(chan struct{}), wake: make(chan struct{}, 1), cancel: func() {}, sessions: map[*session.Session]struct{}{}, mon: current, monReady: true}
	d.sessionClosed(replaced, maps.ErrRelease)
	if d.failure != nil || !d.monReady {
		t.Fatal("a replaced session's cause changed the daemon", d.failure, d.monReady)
	}
	d.sessionClosed(current, maps.ErrRelease)
	if !errors.Is(d.failure, maps.ErrRelease) || d.monReady {
		t.Fatal("the current session's cause was discarded", d.failure, d.monReady)
	}
}

// TestMgrDaemonSettlesFatalCauseBeforeCloseCallback ends the established MON
// session with a MonMap of another cluster and holds that session's close
// callback, so its Done is observable while sessionClosed has not run. The
// supervisor must classify the session's own error and end the daemon with
// it instead of reconnecting as after MON loss.
func TestMgrDaemonSettlesFatalCauseBeforeCloseCallback(t *testing.T) {
	f := newFakeMgrMon(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hooks := &mgrDaemonHooks{beforeSessionClosed: func(generation uint64) {
		if generation == 1 {
			once.Do(func() { close(entered); <-release })
		}
	}}
	d, err := DialMgrDaemon(context.WithValue(context.Background(), mgrDaemonHooksKey{}, hooks), f.options())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	defer close(release) // before Close, which waits for the held callback
	first := f.next(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := d.WaitRotatingKeys(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if err := first.push(msgr.MessageData{Type: msgr.MonMapMessage, Front: mockMonMapAuthEpoch(20, [16]byte{2}, []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}}, 2, 0)}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("the close callback never ran")
	}
	result := make(chan error, 1)
	go func() {
		_, err := d.NextMgrMap(ctx)
		result <- err
	}()
	select {
	case <-f.ready:
		t.Fatal("the supervisor reconnected before the fatal cause was settled")
	case err := <-result:
		if !isFSIDMismatch(err) {
			t.Fatal("terminal cause", err)
		}
	}
	if state := d.State(); !isFSIDMismatch(state.Err) || state.Monitor.Generation != 1 {
		t.Fatal("daemon state", state)
	}
}

// TestMgrDaemonEndedSessionCannotPublish parks a verified rotating reply,
// ends its session with transport loss and keeps the reconnection dialing,
// so d.mon still names the ended session when the reply resumes. The reply
// must count as stale instead of publishing a set of the ended session.
func TestMgrDaemonEndedSessionCannotPublish(t *testing.T) {
	f := newFakeMgrMon(t)
	parked, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hooks := &mgrDaemonHooks{beforeRotatingPublish: func(generation uint64) {
		once.Do(func() { close(parked); <-resume })
	}}
	options := f.options()
	dial := options.DialContext
	var dials atomic.Int32
	redial, hold := make(chan struct{}), make(chan struct{})
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if dials.Add(1) > 1 {
			select {
			case <-redial:
			default:
				close(redial)
			}
			select {
			case <-hold:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return dial(ctx, network, endpoint)
	}
	d, err := DialMgrDaemon(context.WithValue(context.Background(), mgrDaemonHooksKey{}, hooks), options)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	defer close(hold)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-parked:
	case <-ctx.Done():
		t.Fatal("first rotating reply never arrived")
	}
	d.mu.Lock()
	s := d.mon
	d.mu.Unlock()
	s.Fail(errors.New("test transport loss"))
	select {
	case <-redial:
	case <-ctx.Done():
		t.Fatal("no reconnection")
	}
	close(resume)
	state := waitDaemon(t, d, func(s MgrDaemonState) bool { return s.StaleRotatingReplies == 1 })
	if state.RotatingKeys.Generation != 0 || state.Err != nil {
		t.Fatal("an ended session published its rotating set", state.RotatingKeys, state.Err)
	}
}
