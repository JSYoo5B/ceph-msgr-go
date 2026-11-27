package cephmsgr

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// wipeMgrDump is the part of the native `mgr dump` the wipe cell reads,
// through the fixture's own CLI (control mgr-daemon-dump).
type wipeMgrDump struct {
	Epoch       uint32 `json:"epoch"`
	ActiveGID   uint64 `json:"active_gid"`
	ActiveName  string `json:"active_name"`
	Available   bool   `json:"available"`
	ActiveAddrs struct {
		Addrvec []struct {
			Type  string `json:"type"`
			Addr  string `json:"addr"`
			Nonce uint32 `json:"nonce"`
		} `json:"addrvec"`
	} `json:"active_addrs"`
	Standbys []struct {
		GID  uint64 `json:"gid"`
		Name string `json:"name"`
	} `json:"standbys"`
}

func wipeDump(t *testing.T, ctx context.Context, control, label string) (wipeMgrDump, bool) {
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
	var dump wipeMgrDump
	if receipt.Admitted && (json.Unmarshal(receipt.Dump, &dump) != nil || dump.Epoch == 0) {
		t.Fatal("native mgr dump content", label)
	}
	return dump, receipt.Admitted
}

func wipeDumpUntil(t *testing.T, ctx context.Context, control, label string, ok func(wipeMgrDump) bool) wipeMgrDump {
	t.Helper()
	for attempt := 1; ; attempt++ {
		dump, _ := wipeDump(t, ctx, control, fmt.Sprintf("%s-%d", label, attempt))
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

func wipeCommand(t *testing.T, ctx context.Context, admin *Client, command map[string]any) []byte {
	t.Helper()
	encoded, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	result, err := admin.MonCommand(ctx, Command{JSON: encoded})
	if err != nil || result.Code != 0 {
		t.Fatalf("native fixture command %v: code %d %q %v", command["prefix"], result.Code, result.Message, err)
	}
	return result.Data
}

// wipeFingerprints commits to each held rotating key without revealing it:
// HMAC-SHA256 keyed by the secret over a per-run random probe
// (cephx.Key.Signature). The values are compared in memory and never
// logged.
func wipeFingerprints(d *MgrDaemon, probe []byte) map[uint64][]byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[uint64][]byte, len(d.ring.Keys))
	for _, key := range d.ring.Keys {
		out[key.ID] = key.Key.Signature(probe)
	}
	return out
}

func wipeIDs(keys MgrRotatingKeys) []uint64 {
	var ids []uint64
	for _, key := range keys.Keys {
		ids = append(ids, key.ID)
	}
	return ids
}

// TestCephMgrDaemonRotatingWipeIntegration is the native auth-epoch
// invalidation cell of MgrDaemon (#133): one `auth wipe-rotating-service-keys`
// on a disposable one-OSD, two-MGR fixture whose service keys are never
// restored.
//
// A scoped mgr.<id> daemon becomes the MON-selected active MGR (the stock
// MGRs are stopped and failed out), publishes a rotating set and accepts
// the stock osd.0's authenticated session on a Go listener. A CLIENT MGR
// service ticket issued by the stock MON is retained with at least nine
// seconds of validity; an authorizer built from it is admitted by the
// listener before the wipe. The wipe is then submitted once through the
// admin connection; a failed or uncertain submission is never repeated.
// The independently read `mon dump` epoch and auth_epoch must advance, and
// the daemon's MON session must reach that auth epoch.
//
// Invalidation, joined from independent observations: a second authorizer
// of the retained pre-wipe ticket, presented as soon as the daemon holds
// the raised auth epoch and while the ticket is still valid, is refused as
// an authorizer whose secret ID is no longer held or whose ticket no held
// key decrypts, and the client sees the explicit -EACCES; the daemon
// counts one more
// invalidation and publishes a nonempty set carrying that invalidation
// count and the current MON session; that set's keys share no keyed
// fingerprint with the pre-wipe keys. Secret IDs and MaxVersion may repeat
// after a wipe (KeyServer::prepare_rotating_update rebuilds the MGR ring
// from version zero), so neither is used as replacement proof; they are
// logged with the fingerprint relation only.
//
// Fresh post-wipe handshakes: the admin connection's renewed MGR ticket,
// fetched after the post-wipe set was published, is admitted; closing the
// stock OSD's pre-wipe session makes osd.0 reconnect, and its first
// admitted session is verified against the post-wipe set, which shares no
// key with the pre-wipe one. Refusals of osd.0 in between are logged. The
// surviving pre-wipe OSD session is recorded but is neither observation.
//
// Not verified: stale-reply races (a deterministic synthetic control,
// TestMgrDaemonStaleRotatingReplyCannotRepublish), stock MGR peers after a
// wipe, CRC mode, IPv6, and restoration of the wiped service keys, which
// the cluster cannot undo.
//
// Pinned sources: ceph/ceph/blob/v20.2.4/src/mon/AuthMonitor.cc (wipe
// command, auth epoch bump), ceph/ceph/blob/v20.2.4/src/auth/cephx/CephxKeyServer.cc
// (prepare_rotating_update), ceph/ceph/blob/v20.2.4/src/mon/MonClient.cc
// (a daemon's reaction to a raised auth epoch),
// ceph/ceph/blob/v20.2.4/src/auth/cephx/CephxProtocol.cc (authorizer
// verification).
//
//scenario:row mgr-daemon.aes256k.ip4.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mgrdaemonrotatingwipe key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 capabilities=mgr-daemon-control
//scenario:covers Messenger/CephX
//scenario:covers Daemon-to-daemon
//fixture:isolated wipes the rotating service keys, which nothing restores, and stops the stock MGRs; restarts the MGRs
func TestCephMgrDaemonRotatingWipeIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" || os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "2" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" {
		t.Skip("requires the disposable one-OSD two-MGR fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if _, admitted := wipeDump(t, ctx, control, "wipe-admission"); !admitted {
		t.Skip("the wipe cell requires this test alone with fixture capability mgr-daemon-control")
	}
	base := integrationOptions(t)
	admin, err := Dial(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	// The service ticket TTL every MON runs with (daemon-local config get).
	var ttl time.Duration
	for _, member := range admin.Snapshot().Monitor.Members {
		result, err := admin.MonTellTo(ctx, member.Name, Command{JSON: []byte(`{"prefix":"config get","var":"auth_service_ticket_ttl","format":"json"}`)})
		var value struct {
			TTL string `json:"auth_service_ticket_ttl"`
		}
		if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &value) != nil {
			t.Fatal("MON", member.Name, "running auth_service_ticket_ttl", err)
		}
		seconds, err := strconv.ParseFloat(value.TTL, 64)
		if err != nil || seconds < 1 || (ttl != 0 && time.Duration(seconds*float64(time.Second)) != ttl) {
			t.Fatal("MON", member.Name, "auth_service_ticket_ttl", value.TTL, ttl, err)
		}
		ttl = time.Duration(seconds * float64(time.Second))
	}
	if ttl == 0 {
		t.Fatal("no MON members")
	}

	name := fmt.Sprintf("gowipe%d", time.Now().UnixNano()%1_000_000)
	entity := "mgr." + name
	var created []struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(wipeCommand(t, ctx, admin, map[string]any{"prefix": "auth get-or-create", "entity": entity, "caps": []string{"mon", "allow profile mgr", "osd", "allow *", "mds", "allow *"}, "format": "json"}), &created); err != nil || len(created) != 1 {
		t.Fatal("scoped MGR credential", err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		wipeCommand(t, cleanup, admin, map[string]any{"prefix": "auth rm", "entity": entity})
	}()
	key, err := ParseKey(created[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	var localMu sync.Mutex
	var local netip.Addr
	daemon, err := DialMgrDaemon(ctx, MgrDaemonOptions{
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

	// The Go listener admits osd.0 and the admin connection's entity.
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
	advertised := []ManagerAddress{{Type: 2, Nonce: binary.LittleEndian.Uint32(nonce[:])&0x7fffffff | 1, Endpoint: netip.AddrPortFrom(host, uint16(bound.Addr().(*net.TCPAddr).Port))}}
	listener, err := daemon.Listen(MgrListenerOptions{
		Listener: bound, Addresses: advertised, ConnectionMode: base.ConnectionMode, HandshakeTimeout: 5 * time.Second,
		Authorize: func(_ context.Context, id MgrPeerIdentity) error {
			if id.Entity != "osd.0" && id.Entity != base.Identity {
				return errors.New("unexpected peer")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	// Become the active MGR.
	var beaconMu sync.Mutex
	beacon := MgrBeacon{Metadata: map[string]string{"gotest": "standby"}}
	stopBeacons, beaconsDone := make(chan struct{}), make(chan struct{})
	var beaconErr error
	go func() {
		defer close(beaconsDone)
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		for {
			beaconMu.Lock()
			current := beacon
			beaconMu.Unlock()
			send, stop := context.WithTimeout(context.Background(), 20*time.Second)
			err := daemon.SendBeacon(send, current)
			stop()
			if err != nil {
				beaconMu.Lock()
				beaconErr = err
				beaconMu.Unlock()
				return
			}
			select {
			case <-stopBeacons:
				return
			case <-tick.C:
			}
		}
	}()
	beaconsStopped := false
	endBeacons := func() error {
		if !beaconsStopped {
			beaconsStopped = true
			close(stopBeacons)
		}
		<-beaconsDone
		beaconMu.Lock()
		defer beaconMu.Unlock()
		return beaconErr
	}
	defer endBeacons()
	stock := wipeDumpUntil(t, ctx, control, "wipe-standby", func(d wipeMgrDump) bool {
		return d.Available && len(d.Standbys) == 2 && slices.ContainsFunc(d.Standbys, func(s struct {
			GID  uint64 `json:"gid"`
			Name string `json:"name"`
		}) bool {
			return s.GID == gid
		})
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
		wipeCommand(t, ctx, admin, map[string]any{"prefix": "mgr fail", "who": n})
	}
	wipeDumpUntil(t, ctx, control, "wipe-selected", func(d wipeMgrDump) bool { return d.ActiveGID == gid })
	beaconMu.Lock()
	beacon = MgrBeacon{Available: true, Addresses: advertised, Metadata: map[string]string{"gotest": "rotating-wipe"}}
	beaconMu.Unlock()
	wipeDumpUntil(t, ctx, control, "wipe-available", func(d wipeMgrDump) bool {
		return d.ActiveGID == gid && d.Available && len(d.ActiveAddrs.Addrvec) == 1 && d.ActiveAddrs.Addrvec[0].Nonce == advertised[0].Nonce
	})

	acceptOSD := func(label string) *MgrPeer {
		t.Helper()
		wait, stop := context.WithTimeout(ctx, 90*time.Second)
		defer stop()
		peer, err := listener.Accept(wait)
		if err != nil {
			t.Fatalf("%s: no stock OSD session: %v (listener %+v)", label, err, listener.State())
		}
		if id := peer.Identity(); id.Entity != "osd.0" || id.MgrGlobalID != gid {
			t.Fatalf("%s: identity %+v", label, id)
		}
		go func() {
			for {
				if _, err := peer.Next(context.Background()); err != nil {
					return
				}
			}
		}()
		return peer
	}
	target := msgr.Address{Type: 2, Nonce: advertised[0].Nonce, Endpoint: advertised[0].Endpoint}
	// present opens a CLIENT session to the Go listener with authorizer and
	// returns the listener's record of it: the admitted identity, or the
	// refusal of the connection from the returned local address with the
	// client's own handshake error.
	present := func(label string, authorizer *cephx.Authorizer) (MgrPeerIdentity, *MgrListenerRefusal, error) {
		t.Helper()
		before := listener.State()
		dial, stop := context.WithTimeout(ctx, 30*time.Second)
		defer stop()
		var localAddress netip.AddrPort
		options := admin.options
		// A direct dial: the listener is local, and its record names this
		// connection's own local address.
		conn, err := (&net.Dialer{}).DialContext(dial, "tcp", advertised[0].Endpoint.String())
		if err != nil {
			t.Fatal(label, "dial", err)
		}
		localAddress = conn.LocalAddr().(*net.TCPAddr).AddrPort()
		localAddress = netip.AddrPortFrom(localAddress.Addr().Unmap(), localAddress.Port())
		transport, err := session.Handshake(dial, conn, target, 16, gid, session.ServiceAuth{Authorizer: authorizer}, options.ConnectionMode.sessionMode(), options.MaxFrameSize, options.ConnectTimeout)
		if err == nil {
			transport.Conn.Close()
			for {
				state := listener.State()
				if state.Accepted > before.Accepted && state.LastAccepted.Remote == localAddress {
					// Take the published session; no OSD session is due
					// while this one is pending.
					peer, err := listener.Accept(dial)
					if err != nil || peer.Identity().Remote != localAddress {
						t.Fatal(label, "admitted session not published first", err)
					}
					peer.Close()
					return state.LastAccepted, nil, nil
				}
				select {
				case <-dial.Done():
					t.Fatal(label, "admitted session not recorded", listener.State())
				case <-time.After(50 * time.Millisecond):
				}
			}
		}
		conn.Close()
		after := before.Refused + before.Failed
		for {
			r, werr := listener.WaitRefusal(dial, after)
			if werr != nil {
				t.Fatalf("%s: handshake failed (%v) with no listener record: %v", label, err, werr)
			}
			if r.Remote == localAddress {
				return MgrPeerIdentity{}, &r, err
			}
			after = r.Sequence
		}
	}

	// 1. Before the wipe: a rotating set, the stock OSD's session and a
	// retained stock-MON-issued CLIENT MGR ticket that is admitted.
	osdBefore := acceptOSD("pre-wipe")
	t.Logf("pre-wipe: osd.0 session %d, ticket secret ID %d", osdBefore.Identity().Generation, osdBefore.Identity().SecretID)
	var retained *cephx.Client
	for deadline := time.Now().Add(time.Minute); ; {
		auth := admin.snapshotAuth()
		if ticket, ok := auth.Tickets[cephx.ServiceMgr]; ok && time.Until(ticket.Expires) >= 9*time.Second {
			retained = auth
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no MGR ticket with nine seconds of validity")
		}
		time.Sleep(100 * time.Millisecond)
	}
	retainedTicket := retained.Tickets[cephx.ServiceMgr]
	beforeUse, err := retained.Authorizer(cephx.ServiceMgr)
	if err != nil {
		t.Fatal(err)
	}
	afterUse, err := retained.Authorizer(cephx.ServiceMgr)
	if err != nil {
		t.Fatal(err)
	}
	if id, refusal, _ := present("pre-wipe retained ticket", beforeUse); refusal != nil || id.SecretID != retainedTicket.SecretID {
		t.Fatal("the retained pre-wipe ticket was not admitted before the wipe", refusal, id)
	}
	pre := daemon.State()
	probe := make([]byte, 32)
	if _, err := rand.Read(probe); err != nil {
		t.Fatal(err)
	}
	preFingerprints := wipeFingerprints(daemon, probe)
	if len(preFingerprints) == 0 || !slices.Contains(wipeIDs(pre.RotatingKeys), retainedTicket.SecretID) {
		t.Fatal("pre-wipe set", wipeIDs(pre.RotatingKeys), retainedTicket.SecretID)
	}
	t.Logf("pre-wipe: rotating IDs %v (max %d, generation %d, invalidations %d, MON session %d), MON auth epoch %d; retained CLIENT MGR ticket secret ID %d valid for %v",
		wipeIDs(pre.RotatingKeys), pre.RotatingKeys.MaxVersion, pre.RotatingKeys.Generation, pre.RotatingKeys.Invalidations, pre.RotatingKeys.MonitorGeneration, pre.Monitor.AuthEpoch, retainedTicket.SecretID, time.Until(retainedTicket.Expires).Round(time.Millisecond))

	type monDump struct {
		Epoch     uint32  `json:"epoch"`
		AuthEpoch *uint32 `json:"auth_epoch"`
	}
	readMap := func() monDump {
		t.Helper()
		var m monDump
		if err := json.Unmarshal(wipeCommand(t, ctx, admin, map[string]any{"prefix": "mon dump", "format": "json"}), &m); err != nil || m.Epoch == 0 || m.AuthEpoch == nil {
			t.Fatal("native mon dump", err)
		}
		return m
	}
	before := readMap()

	// 2. One wipe. Never repeated: an uncertain submission ends the test.
	// The MON replies after the commit; the daemon may see the raised auth
	// epoch first, and the retained ticket is presented as soon as it does.
	wipeAt := time.Now()
	type wipeResult struct {
		result Result
		err    error
	}
	submitted := make(chan wipeResult, 1)
	go func() {
		result, err := admin.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"auth wipe-rotating-service-keys"}`)})
		submitted <- wipeResult{result, err}
	}()
	var reply *wipeResult
	for daemon.State().Monitor.AuthEpoch <= pre.Monitor.AuthEpoch {
		select {
		case r := <-submitted:
			if r.err != nil || r.result.Code != 0 {
				t.Fatalf("wipe submitted once: code %d %q %v", r.result.Code, r.result.Message, r.err)
			}
			reply = &r
		case <-time.After(20 * time.Millisecond):
		}
		if time.Since(wipeAt) > time.Minute {
			t.Fatal("the daemon's MON session did not see a raised auth epoch", daemon.State().Monitor)
		}
	}
	seen := time.Since(wipeAt).Round(time.Millisecond)

	// 3. The retained pre-wipe ticket, still valid, is refused.
	if !time.Now().Before(retainedTicket.Expires) {
		t.Fatal("the retained ticket expired before it could be presented; a refusal would not be attributable to the wipe")
	}
	left := time.Until(retainedTicket.Expires).Round(time.Millisecond)
	// Only the authorizer refusals a replaced key set can cause qualify: the
	// secret ID is no longer held, or a held key of that ID does not
	// decrypt the ticket. Another authentication-stage end (a mode
	// refusal, a challenge mismatch, a timeout) is not this observation,
	// and the client must see the explicit -EACCES the listener wrote.
	var authorizer *cephx.AuthorizerError
	var rejection *AuthenticationError
	if id, refusal, clientErr := present("post-wipe retained ticket", afterUse); refusal == nil {
		t.Fatalf("pre-wipe MGR ticket (secret ID %d) admitted after the wipe: %+v", retainedTicket.SecretID, id)
	} else if !refusal.Refused || refusal.Stage != "authentication" || !errors.As(refusal.Err, &authorizer) ||
		(authorizer.Reason != cephx.RefusedUnknownSecret && authorizer.Reason != cephx.RefusedTicket) ||
		!errors.As(clientErr, &rejection) || rejection.Code != -13 {
		t.Fatalf("pre-wipe MGR ticket was not refused for its key: listener %+v, client %v", refusal, clientErr)
	} else {
		t.Logf("post-wipe: the daemon saw auth epoch %d %v after the wipe was sent; the retained ticket (secret ID %d, %v of validity left) was refused at %s: %s; the client saw method %d code %d", daemon.State().Monitor.AuthEpoch, seen, retainedTicket.SecretID, left, refusal.Stage, refusal.Reason, rejection.Method, rejection.Code)
	}
	if reply == nil {
		r := <-submitted
		if r.err != nil || r.result.Code != 0 {
			t.Fatalf("wipe submitted once: code %d %q %v", r.result.Code, r.result.Message, r.err)
		}
	}
	after := readMap()
	if after.Epoch <= before.Epoch || *after.AuthEpoch <= *before.AuthEpoch || daemon.State().Monitor.AuthEpoch != *after.AuthEpoch {
		t.Fatal("native MonMap and auth epochs", before, after, "daemon auth epoch", daemon.State().Monitor.AuthEpoch)
	}
	t.Logf("wipe committed in %v: native MonMap epoch %d -> %d, auth_epoch %d -> %d, equal to the daemon's", time.Since(wipeAt).Round(time.Millisecond), before.Epoch, after.Epoch, *before.AuthEpoch, *after.AuthEpoch)

	// 4. The invalidation and the post-wipe set.
	var post MgrRotatingKeys
	for {
		next, err := daemon.WaitRotatingKeys(ctx, pre.RotatingKeys.Generation)
		if err != nil {
			t.Fatal("post-wipe rotating set", err)
		}
		if next.Invalidations == pre.RotatingKeys.Invalidations+1 {
			post = next
			break
		}
		if next.Invalidations > pre.RotatingKeys.Invalidations+1 {
			t.Fatal("more than one invalidation for one wipe", next.Invalidations)
		}
		pre.RotatingKeys.Generation = next.Generation // an ordinary refresh before the invalidation
	}
	state := daemon.State()
	if len(post.Keys) == 0 || post.MonitorGeneration != state.Monitor.Generation || state.Monitor.AuthEpoch != *after.AuthEpoch || state.RotatingKeys.Invalidations != post.Invalidations {
		t.Fatalf("post-wipe set %+v, state %+v, native auth epoch %d", post, state.Monitor, *after.AuthEpoch)
	}
	postFingerprints := wipeFingerprints(daemon, probe)
	var repeated []uint64
	for id, fingerprint := range postFingerprints {
		for oldID, old := range preFingerprints {
			if bytes.Equal(fingerprint, old) {
				t.Fatalf("post-wipe key %d has the fingerprint of pre-wipe key %d", id, oldID)
			}
		}
		if _, ok := preFingerprints[id]; ok {
			repeated = append(repeated, id)
		}
	}
	slices.Sort(repeated)
	t.Logf("post-wipe: rotating IDs %v (max %d, generation %d, invalidations %d, MON session %d) published %v after the wipe; repeated IDs %v; no key fingerprint shared with the pre-wipe set; %d stale replies",
		wipeIDs(post), post.MaxVersion, post.Generation, post.Invalidations, post.MonitorGeneration, post.Received.Sub(wipeAt).Round(time.Millisecond), repeated, state.StaleRotatingReplies)

	// 5. Fresh post-wipe handshakes: the renewed CLIENT ticket and the stock
	// OSD's reconnection.
	var fresh *cephx.Client
	for deadline := time.Now().Add(2 * ttl); ; {
		auth := admin.snapshotAuth()
		if ticket, ok := auth.Tickets[cephx.ServiceMgr]; ok && !ticket.Expires.Before(post.Received.Add(ttl-time.Second)) {
			fresh = auth
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the admin connection holds no MGR ticket issued after the post-wipe set")
		}
		time.Sleep(100 * time.Millisecond)
	}
	freshUse, err := fresh.Authorizer(cephx.ServiceMgr)
	if err != nil {
		t.Fatal(err)
	}
	if id, refusal, _ := present("post-wipe fresh ticket", freshUse); refusal != nil {
		t.Fatalf("a MGR ticket issued after the wipe was refused: %+v", refusal)
	} else if !slices.Contains(wipeIDs(daemon.State().RotatingKeys), id.SecretID) {
		t.Fatal("fresh ticket secret ID", id.SecretID, wipeIDs(daemon.State().RotatingKeys))
	} else {
		t.Logf("post-wipe: the renewed CLIENT MGR ticket (secret ID %d) was admitted", id.SecretID)
	}
	select {
	case <-osdBefore.Done():
		t.Logf("the pre-wipe osd.0 session ended by itself: %v", osdBefore.Err())
	default:
		t.Logf("the pre-wipe osd.0 session %d survived the wipe (not an acceptance observation)", osdBefore.Identity().Generation)
	}
	refusalsBefore := listener.State()
	osdBefore.Close()
	osdAfter := acceptOSD("post-wipe")
	held := daemon.State().RotatingKeys
	if id := osdAfter.Identity(); !slices.Contains(wipeIDs(held), id.SecretID) || held.Invalidations != post.Invalidations {
		t.Fatalf("post-wipe osd.0 session secret ID %d, held %v (invalidations %d)", id.SecretID, wipeIDs(held), held.Invalidations)
	}
	osdHost := osdAfter.Identity().Remote.Addr()
	for _, r := range listener.State().Recent {
		if r.Sequence > refusalsBefore.Refused+refusalsBefore.Failed && r.Remote.Addr() == osdHost {
			t.Logf("osd.0 attempt before its post-wipe admission: stage %s refused %v: %s", r.Stage, r.Refused, r.Reason)
		}
	}
	t.Logf("post-wipe: osd.0 session %d admitted with ticket secret ID %d from the post-wipe set %v", osdAfter.Identity().Generation, osdAfter.Identity().SecretID, wipeIDs(held))

	// Restore a stock active MGR; the wiped keys stay wiped.
	osdAfter.Close()
	listener.Close()
	if err := endBeacons(); err != nil {
		t.Fatal("beacon loop", err)
	}
	daemon.Close()
	for _, n := range stockNames {
		if err := testcluster.ControlDaemon(ctx, control, "start", "mgr", n); err != nil {
			t.Fatal("restart stock MGR", n, err)
		}
	}
	restored = true
	wipeCommand(t, ctx, admin, map[string]any{"prefix": "mgr fail", "who": name})
	back := wipeDumpUntil(t, ctx, control, "wipe-restored", func(d wipeMgrDump) bool {
		return d.Available && d.ActiveGID != gid && slices.Contains(stockNames, d.ActiveName)
	})
	t.Logf("restored: stock %s/%d active at native epoch %d; %s", back.ActiveName, back.ActiveGID, back.Epoch, strings.Join(stockNames, ","))
}
