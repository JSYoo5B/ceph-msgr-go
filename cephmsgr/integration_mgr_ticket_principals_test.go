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
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

// principalTicket is one retained CLIENT MGR service ticket: an immutable
// copy of the admin connection's authentication state, from which every
// authorizer of one cohort is built. Two authorizers of one cohort carry
// the same ticket (global ID, service, secret ID and opaque blob) with
// independent nonces; each target answers with its own challenge.
type principalTicket struct {
	auth   *cephx.Client
	ticket cephx.Ticket
}

// retainPrincipalTicket waits for an admin MGR ticket sealed with a secret
// ID other than every ID in seen, with at least validity left.
func retainPrincipalTicket(t *testing.T, ctx context.Context, admin *Client, label string, seen []uint64, validity, within time.Duration) principalTicket {
	t.Helper()
	for deadline := time.Now().Add(within); ; {
		auth := admin.snapshotAuth()
		if ticket, ok := auth.Tickets[cephx.ServiceMgr]; ok && !slices.Contains(seen, ticket.SecretID) && time.Until(ticket.Expires) >= validity {
			return principalTicket{auth: auth, ticket: ticket}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: no MGR ticket with a secret ID outside %v and %v of validity within %v", label, seen, validity, within)
		}
		select {
		case <-ctx.Done():
			t.Fatal(label, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// authorizer builds a fresh authorizer of the retained ticket.
func (p principalTicket) authorizer(t *testing.T) *cephx.Authorizer {
	t.Helper()
	a, err := p.auth.Authorizer(cephx.ServiceMgr)
	if err != nil {
		t.Fatal("authorizer of the retained ticket", err)
	}
	return a
}

// corrupted builds an authorizer of the same ticket whose encrypted blob
// has one flipped bit in its first byte: the negative control of each target. The retained
// blob itself is never modified.
func (p principalTicket) corrupted(t *testing.T) *cephx.Authorizer {
	t.Helper()
	auth := copyAuth(p.auth)
	ticket := auth.Tickets[cephx.ServiceMgr]
	ticket.Blob = bytes.Clone(ticket.Blob)
	// The first cipher block: with AES-CBC it holds the encrypted magic,
	// with aes256k any change fails the HMAC.
	ticket.Blob[0] ^= 0x10
	auth.Tickets[cephx.ServiceMgr] = ticket
	a, err := auth.Authorizer(cephx.ServiceMgr)
	if err != nil {
		t.Fatal("authorizer of the corrupted ticket", err)
	}
	return a
}

// ringFingerprint commits to the daemon's rotating MGR secret of exactly
// id without revealing it: HMAC-SHA256 keyed by the secret over a per-run
// probe. Values are compared in memory and never logged.
func ringFingerprint(d *MgrDaemon, id uint64, probe []byte) ([]byte, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key, ok := d.ring.Lookup(id)
	if !ok {
		return nil, false
	}
	return key.Signature(probe), true
}

// principalListener is one Go MGR daemon with its own scoped principal,
// MON session, GID, rotating MGR set and listener.
type principalListener struct {
	name       string
	daemon     *MgrDaemon
	listener   *MgrListener
	advertised ManagerAddress
}

// principalPresent opens a CLIENT session to one Go listener with
// authorizer and returns the listener's record of it: the admitted
// identity, or the refusal of the connection from the returned local
// address with the client's own handshake error.
func principalPresent(t *testing.T, ctx context.Context, admin *Client, target principalListener, label string, authorizer *cephx.Authorizer) (MgrPeerIdentity, *MgrListenerRefusal, error) {
	t.Helper()
	listener := target.listener
	before := listener.State()
	dial, stop := context.WithTimeout(ctx, 30*time.Second)
	defer stop()
	conn, err := (&net.Dialer{}).DialContext(dial, "tcp", target.advertised.Endpoint.String())
	if err != nil {
		t.Fatal(label, "dial", err)
	}
	local := conn.LocalAddr().(*net.TCPAddr).AddrPort()
	local = netip.AddrPortFrom(local.Addr().Unmap(), local.Port())
	address := msgr.Address{Type: 2, Nonce: target.advertised.Nonce, Endpoint: target.advertised.Endpoint}
	transport, err := session.Handshake(dial, conn, address, 16, target.daemon.Identity().GlobalID, session.ServiceAuth{Authorizer: authorizer}, admin.options.ConnectionMode.sessionMode(), admin.options.MaxFrameSize, admin.options.ConnectTimeout)
	if err == nil {
		transport.Conn.Close()
		for {
			state := listener.State()
			if state.Accepted > before.Accepted && state.LastAccepted.Remote == local {
				peer, err := listener.Accept(dial)
				if err != nil || peer.Identity().Remote != local {
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
	after := before.Refused + before.Failed
	for {
		r, werr := listener.WaitRefusal(dial, after)
		if werr != nil {
			t.Fatalf("%s: handshake failed (%v) with no listener record: %v", label, err, werr)
		}
		if r.Remote == local {
			return MgrPeerIdentity{}, &r, err
		}
		after = r.Sequence
	}
}

// stockMgrAddress is the msgr2 entry of a native MgrMap address vector.
func stockMgrAddress(t *testing.T, d wipeMgrDump) msgr.Address {
	t.Helper()
	for _, a := range d.ActiveAddrs.Addrvec {
		if a.Type != "v2" {
			continue
		}
		endpoint, err := netip.ParseAddrPort(a.Addr)
		if err != nil {
			t.Fatal("native MGR address", a.Addr, err)
		}
		return msgr.Address{Type: 2, Nonce: a.Nonce, Endpoint: endpoint}
	}
	t.Fatal("native MgrMap has no msgr2 active address", d.ActiveAddrs.Addrvec)
	return msgr.Address{}
}

// stockPresent authenticates one CLIENT Messenger connection to the stock
// MGR the native dump names, pinned to its address, nonce and GID
// (SERVER_IDENT must carry both), with authorizer. Success means the MGR
// decrypted the ticket and proved it with the ticket's session key (the
// authorizer reply); no MGR message is sent.
func stockPresent(ctx context.Context, admin *Client, d wipeMgrDump, address msgr.Address, authorizer *cephx.Authorizer) error {
	open, stop := context.WithTimeout(ctx, 30*time.Second)
	defer stop()
	transport, err := admin.open(open, dialAddress(address), address, 16, d.ActiveGID, session.ServiceAuth{Authorizer: authorizer})
	if err != nil {
		return err
	}
	return transport.Conn.Close()
}

// TestCephMgrTicketPrincipalsIntegration answers #139: can independently
// authenticated MGR instances validate the same MGR service ticket? A CLIENT
// MGR service ticket is issued once by the stock MON to the admin
// connection and retained; every authorizer of one cohort is built from
// that one retained copy, so the presented ticket bytes are equal by
// construction, and each handshake answers its own target's challenge.
//
// Go cell, at the same time: two scoped mgr.<id> principals, created by the
// MON with distinct keys (compared in memory), authenticate as Go MGR
// daemons with their own GIDs and rotating MGR sets, and each listens on
// its own endpoint. They never beacon, so the stock MGRs stay active. For
// a first cohort and a cohort sealed with a newer rotating secret ID, both
// rings hold the ticket's secret ID with an equal keyed fingerprint (the
// MON hands every mgr.* principal the same MGR service secrets), both
// listeners admit the ticket with that secret ID and the admin's global
// ID, and both refuse the same ticket with one bit of its blob flipped
// (ticket does not decrypt, -EACCES to the client). Each admission happens
// within the ticket's validity.
//
// Stock cell, in sequential active intervals: the two stock mgr.* principal
// keys are compared in memory. Stock promotion loads modules before the
// promoted MGR publishes its address, which can outlast the fixture's 12 s
// service ticket TTL, so the cell overrides auth_service_ticket_ttl with
// 120 s at runtime on every MON (`tell config set`; the fixture's ceph.conf
// value outranks the MON configuration database) and waits for an admin
// ticket with at least 90 s of validity. A
// cohort presents the ticket to the active MGR, then `mgr fail` promotes
// the standby and the same ticket is presented to it. Each handshake is
// pinned to the native MgrMap's address, nonce and GID and bracketed by a
// native dump showing the same active GID, and each target refuses the
// corrupted copy. The second cohort, sealed with a newer secret ID, runs
// in the other direction. Stock MGR authorizer verification does not check
// ticket expiry (cephx_verify_authorizer), so the validity condition is
// this experiment's own: a presentation after expiry, a changed active
// GID or a bracket mismatch is reported as inconclusive and fails the
// test. The overrides are removed afterwards (`config unset`) and every
// MON must run the fixture's TTL again.
//
// Not covered: a ticket sealed with the all-ones (general) secret ID,
// rings that differ during a refresh, IPv6, arm64 and the stock MGR's own
// view (only the client's verified authorizer reply). The issue names
// ceph-testcontainers-go as the place of record; this experiment lives in
// this repository.
//
// Pinned sources: ceph/ceph/blob/v20.2.4/src/auth/cephx/CephxKeyServer.cc
// (get_rotating_encrypted selects the rotating set by entity type;
// get_service_secret and _rotate_secret), ceph/ceph/blob/v20.2.4/src/auth/cephx/CephxProtocol.cc
// (cephx_decode_ticket and cephx_verify_authorizer),
// ceph/ceph/blob/v20.2.4/src/mgr/MgrStandby.cc (promotion and beacon
// addresses), ceph/ceph/blob/v20.2.4/src/common/options/global.yaml.in
// (auth_service_ticket_ttl is a runtime option).
//
//scenario:row mgr-daemon.aes256k.ip4.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mgrticketprincipals key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 capabilities=mgr-daemon-control
//scenario:row mgr-daemon.aes.ip4.crc.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mgrticketprincipals key=aes service=aes mode=crc family=4 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 capabilities=mgr-daemon-control
//scenario:covers Messenger/CephX
//scenario:covers Daemon-to-daemon
//fixture:isolated fails over the stock MGRs and overrides auth_service_ticket_ttl on every MON at runtime; removes the overrides
func TestCephMgrTicketPrincipalsIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" || os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "2" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" {
		t.Skip("requires the disposable one-OSD two-MGR fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if _, admitted := wipeDump(t, ctx, control, "principals-admission"); !admitted {
		t.Skip("failing over the stock MGRs requires this test alone with fixture capability mgr-daemon-control")
	}
	base := integrationOptions(t)
	admin, err := Dial(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ttl := principalRunningTTL(t, ctx, admin)
	probe := make([]byte, 32)
	if _, err := rand.Read(probe); err != nil {
		t.Fatal(err)
	}

	goCell(t, ctx, admin, base, ttl, probe)
	stockCell(t, ctx, control, admin, ttl)
}

// principalRunningTTL is the auth_service_ticket_ttl every MON runs with
// (daemon-local config get).
func principalRunningTTL(t *testing.T, ctx context.Context, admin *Client) time.Duration {
	t.Helper()
	ttl := principalRunningTTLValue(ctx, admin)
	if ttl < time.Second {
		t.Fatal("the MONs run no common auth_service_ticket_ttl")
	}
	return ttl
}

func goCell(t *testing.T, ctx context.Context, admin *Client, base Options, ttl time.Duration, probe []byte) {
	suffix := time.Now().UnixNano() % 1_000_000
	var targets []principalListener
	var keys []Key
	for _, label := range []string{"a", "b"} {
		name := fmt.Sprintf("gotk%s%d", label, suffix)
		entity := "mgr." + name
		var created []struct {
			Key string `json:"key"`
		}
		if err := json.Unmarshal(wipeCommand(t, ctx, admin, map[string]any{"prefix": "auth get-or-create", "entity": entity, "caps": []string{"mon", "allow profile mgr", "osd", "allow *", "mds", "allow *"}, "format": "json"}), &created); err != nil || len(created) != 1 {
			t.Fatal("scoped MGR credential", entity, err)
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
		keys = append(keys, key)
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
			t.Fatal(entity, err)
		}
		defer daemon.Close()
		if _, err := daemon.WaitRotatingKeys(ctx, 0); err != nil {
			t.Fatal(entity, "rotating MGR secrets", err)
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
		advertised := ManagerAddress{Type: 2, Nonce: binary.LittleEndian.Uint32(nonce[:])&0x7fffffff | 1, Endpoint: netip.AddrPortFrom(host, uint16(bound.Addr().(*net.TCPAddr).Port))}
		listener, err := daemon.Listen(MgrListenerOptions{
			Listener: bound, Addresses: []ManagerAddress{advertised}, ConnectionMode: base.ConnectionMode, HandshakeTimeout: 5 * time.Second,
			Authorize: func(_ context.Context, id MgrPeerIdentity) error {
				if id.Entity != base.Identity {
					return errors.New("unexpected peer")
				}
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		targets = append(targets, principalListener{name: entity, daemon: daemon, listener: listener, advertised: advertised})
	}
	if bytes.Equal(keys[0].value.Signature(probe), keys[1].value.Signature(probe)) {
		t.Fatal("the two scoped MGR principals share one key")
	}
	a, b := targets[0], targets[1]
	if a.daemon.Identity().GlobalID == b.daemon.Identity().GlobalID {
		t.Fatal("the two Go MGR daemons share a GID", a.daemon.Identity().GlobalID)
	}
	t.Logf("go cell: principals %s (GID %d) and %s (GID %d) with distinct keys; service ticket TTL %v",
		a.name, a.daemon.Identity().GlobalID, b.name, b.daemon.Identity().GlobalID, ttl)

	var seen []uint64
	for cohort, order := range [][]principalListener{{a, b}, {b, a}} {
		label := fmt.Sprintf("go cohort %d", cohort+1)
		retained := retainPrincipalTicket(t, ctx, admin, label, seen, ttl/2, 3*ttl)
		id := retained.ticket.SecretID
		seen = append(seen, id)
		// Both rings hold the ticket's secret before it is presented.
		var fingerprints [][]byte
		waitFor := func() bool {
			fingerprints = fingerprints[:0]
			for _, target := range order {
				f, ok := ringFingerprint(target.daemon, id, probe)
				if !ok {
					return false
				}
				fingerprints = append(fingerprints, f)
			}
			return true
		}
		for !waitFor() {
			if !time.Now().Before(retained.ticket.Expires.Add(-time.Second)) {
				t.Fatalf("%s: inconclusive, the rings %v and %v did not both hold secret ID %d within the ticket's validity",
					label, wipeIDs(order[0].daemon.State().RotatingKeys), wipeIDs(order[1].daemon.State().RotatingKeys), id)
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !bytes.Equal(fingerprints[0], fingerprints[1]) {
			t.Fatalf("%s: %s and %s hold different keys under MGR secret ID %d", label, order[0].name, order[1].name, id)
		}
		first, second := retained.authorizer(t), retained.authorizer(t)
		for i, target := range order {
			authorizer := first
			if i == 1 {
				authorizer = second
			}
			admitted, refusal, clientErr := principalPresent(t, ctx, admin, target, label+" "+target.name, authorizer)
			at := time.Now()
			if refusal != nil {
				t.Fatalf("%s: %s refused the retained ticket (secret ID %d): %+v, client %v", label, target.name, id, refusal, clientErr)
			}
			if !at.Before(retained.ticket.Expires) {
				t.Fatalf("%s: inconclusive, %s admitted the ticket only after its expiry", label, target.name)
			}
			if admitted.SecretID != id || admitted.GlobalID != retained.auth.GlobalID || admitted.Entity != base.Identity || admitted.MgrGlobalID != target.daemon.Identity().GlobalID {
				t.Fatalf("%s: %s admitted %+v, want secret ID %d, global ID %d", label, target.name, admitted, id, retained.auth.GlobalID)
			}
			t.Logf("%s: %s (GID %d) admitted the retained ticket: secret ID %d, client global ID %d, %v of validity left; held IDs %v",
				label, target.name, admitted.MgrGlobalID, id, admitted.GlobalID, retained.ticket.Expires.Sub(at).Round(time.Millisecond), wipeIDs(target.daemon.State().RotatingKeys))
		}
		for _, target := range order {
			var authorizer *cephx.AuthorizerError
			var rejection *AuthenticationError
			if admitted, refusal, clientErr := principalPresent(t, ctx, admin, target, label+" corrupted "+target.name, retained.corrupted(t)); refusal == nil {
				t.Fatalf("%s: %s admitted the corrupted ticket: %+v", label, target.name, admitted)
			} else if !refusal.Refused || refusal.Stage != "authentication" || !errors.As(refusal.Err, &authorizer) || authorizer.Reason != cephx.RefusedTicket ||
				!errors.As(clientErr, &rejection) || rejection.Code != -13 {
				t.Fatalf("%s: %s did not refuse the corrupted ticket for its blob: %+v, client %v", label, target.name, refusal, clientErr)
			} else {
				t.Logf("%s: %s refused the corrupted copy at %s: %s; the client saw method %d code %d", label, target.name, refusal.Stage, refusal.Reason, rejection.Method, rejection.Code)
			}
		}
		t.Logf("%s: both Go MGR daemons hold secret ID %d with one keyed fingerprint and validated one retained ticket", label, id)
	}
	if seen[0] == seen[1] {
		t.Fatal("the go cohorts share one secret ID", seen)
	}
}

func stockCell(t *testing.T, ctx context.Context, control string, admin *Client, fixtureTTL time.Duration) {
	start := wipeDumpUntil(t, ctx, control, "principals-stock-start", func(d wipeMgrDump) bool { return d.Available && d.ActiveGID != 0 && len(d.Standbys) == 1 })
	names := []string{start.ActiveName, start.Standbys[0].Name}
	probe := make([]byte, 32)
	if _, err := rand.Read(probe); err != nil {
		t.Fatal(err)
	}
	var fingerprints [][]byte
	for _, name := range names {
		var entries []struct {
			Entity string `json:"entity"`
			Key    string `json:"key"`
		}
		if err := json.Unmarshal(wipeCommand(t, ctx, admin, map[string]any{"prefix": "auth get", "entity": "mgr." + name, "format": "json"}), &entries); err != nil || len(entries) != 1 || entries[0].Entity != "mgr."+name {
			t.Fatal("native stock MGR principal", name, err)
		}
		key, err := ParseKey(entries[0].Key)
		if err != nil {
			t.Fatal("stock MGR principal key", name, err)
		}
		fingerprints = append(fingerprints, key.value.Signature(probe))
	}
	if bytes.Equal(fingerprints[0], fingerprints[1]) {
		t.Fatal("the stock MGR principals share one key", names)
	}
	t.Logf("stock cell: principals mgr.%s and mgr.%s with distinct keys; mgr.%s/%d active at native epoch %d", names[0], names[1], start.ActiveName, start.ActiveGID, start.Epoch)

	// Raise the service ticket TTL for this cell on every MON, and remove
	// that override afterwards. The fixture writes the TTL to each MON's
	// ceph.conf, which outranks the MON configuration database, so the
	// change is a daemon-local runtime override (`tell mon.<id> config
	// set`), and `config unset` returns each MON to its file value.
	members := admin.Snapshot().Monitor.Members
	tell := func(ctx context.Context, member string, command string) {
		result, err := admin.MonTellTo(ctx, member, Command{JSON: []byte(command)})
		if err != nil || result.Code != 0 {
			t.Errorf("MON %s %s: code %d %q %v", member, command, result.Code, result.Message, err)
		}
	}
	raised := 120 * time.Second
	restored := false
	restore := func(ctx context.Context) {
		for _, member := range members {
			tell(ctx, member.Name, `{"prefix":"config unset","var":"auth_service_ticket_ttl"}`)
		}
	}
	defer func() {
		if !restored {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			restore(cleanup)
		}
	}()
	for _, member := range members {
		tell(ctx, member.Name, `{"prefix":"config set","var":"auth_service_ticket_ttl","val":["120"]}`)
	}
	if t.Failed() {
		t.FailNow()
	}
	if running := principalRunningTTLValue(ctx, admin); running != raised {
		t.Fatal("the MONs run auth_service_ticket_ttl", running, "after the override, not", raised)
	}
	t.Logf("stock cell: auth_service_ticket_ttl %v -> %v at runtime on MONs %d", fixtureTTL, raised, len(members))

	var seen []uint64
	for cohort := 1; cohort <= 2; cohort++ {
		label := fmt.Sprintf("stock cohort %d", cohort)
		before := wipeDumpUntil(t, ctx, control, fmt.Sprintf("principals-stock-%d-before", cohort), func(d wipeMgrDump) bool { return d.Available && d.ActiveGID != 0 && len(d.Standbys) == 1 })
		// A first cohort needs a ticket issued under the raised TTL; a
		// second one, the next secret ID.
		retained := retainPrincipalTicket(t, ctx, admin, label, seen, 90*time.Second, 6*time.Minute)
		id := retained.ticket.SecretID
		seen = append(seen, id)
		first, second := retained.authorizer(t), retained.authorizer(t)
		interval := func(phase string, d wipeMgrDump, authorizer *cephx.Authorizer) {
			t.Helper()
			address := stockMgrAddress(t, d)
			err := stockPresent(ctx, admin, d, address, authorizer)
			at := time.Now()
			after, _ := wipeDump(t, ctx, control, fmt.Sprintf("principals-stock-%d-%s-bracket", cohort, phase))
			if after.ActiveGID != d.ActiveGID || after.ActiveName != d.ActiveName || !slices.Equal(after.ActiveAddrs.Addrvec, d.ActiveAddrs.Addrvec) {
				t.Fatalf("%s %s: inconclusive, the active MGR changed during the presentation: %+v -> %+v", label, phase, d, after)
			}
			if !at.Before(retained.ticket.Expires) {
				t.Fatalf("%s %s: inconclusive, presented to mgr.%s after the ticket's expiry", label, phase, d.ActiveName)
			}
			if err != nil {
				t.Fatalf("%s %s: mgr.%s/%d at %v (native epoch %d) did not authenticate the retained ticket (secret ID %d): %v", label, phase, d.ActiveName, d.ActiveGID, address.Endpoint, d.Epoch, id, err)
			}
			t.Logf("%s %s: mgr.%s/%d at %v/%d (native epochs %d..%d) authenticated the retained ticket (secret ID %d, client global ID %d, %v of validity left)",
				label, phase, d.ActiveName, d.ActiveGID, address.Endpoint, address.Nonce, d.Epoch, after.Epoch, id, retained.auth.GlobalID, retained.ticket.Expires.Sub(at).Round(time.Millisecond))
			var rejection *AuthenticationError
			if err := stockPresent(ctx, admin, d, address, retained.corrupted(t)); err == nil {
				t.Fatalf("%s %s: mgr.%s authenticated the corrupted ticket", label, phase, d.ActiveName)
			} else if !errors.As(err, &rejection) {
				t.Fatalf("%s %s: mgr.%s ended the corrupted ticket's handshake without an authentication refusal: %v", label, phase, d.ActiveName, err)
			} else {
				t.Logf("%s %s: mgr.%s refused the corrupted copy: method %d code %d", label, phase, d.ActiveName, rejection.Method, rejection.Code)
			}
		}
		interval("active", before, first)
		wipeCommand(t, ctx, admin, map[string]any{"prefix": "mgr fail", "who": before.ActiveName})
		promoted := wipeDumpUntil(t, ctx, control, fmt.Sprintf("principals-stock-%d-promoted", cohort), func(d wipeMgrDump) bool {
			return d.Available && d.ActiveGID != before.ActiveGID && d.ActiveName != before.ActiveName && len(d.ActiveAddrs.Addrvec) > 0
		})
		t.Logf("%s: mgr fail %s: native epoch %d -> %d, active %s/%d -> %s/%d", label, before.ActiveName, before.Epoch, promoted.Epoch, before.ActiveName, before.ActiveGID, promoted.ActiveName, promoted.ActiveGID)
		interval("promoted", promoted, second)
		t.Logf("%s: mgr.%s and mgr.%s, distinct principals, validated one retained ticket with secret ID %d", label, before.ActiveName, promoted.ActiveName, id)
	}
	if seen[0] == seen[1] {
		t.Fatal("the stock cohorts share one secret ID", seen)
	}

	restore(ctx)
	restored = true
	for deadline := time.Now().Add(time.Minute); principalRunningTTLValue(ctx, admin) != fixtureTTL; {
		if time.Now().After(deadline) {
			t.Fatal("the MONs did not return to the fixture auth_service_ticket_ttl", fixtureTTL)
		}
		time.Sleep(time.Second)
	}
	back := wipeDumpUntil(t, ctx, control, "principals-stock-restored", func(d wipeMgrDump) bool { return d.Available && len(d.Standbys) == 1 })
	t.Logf("restored: auth_service_ticket_ttl %v on every MON; mgr.%s/%d active with mgr.%s standby at native epoch %d", fixtureTTL, back.ActiveName, back.ActiveGID, back.Standbys[0].Name, back.Epoch)
}

// principalRunningTTLValue is principalRunningTTL without failing: zero when
// the MONs disagree or a read fails.
func principalRunningTTLValue(ctx context.Context, admin *Client) time.Duration {
	var ttl time.Duration
	for _, member := range admin.Snapshot().Monitor.Members {
		result, err := admin.MonTellTo(ctx, member.Name, Command{JSON: []byte(`{"prefix":"config get","var":"auth_service_ticket_ttl","format":"json"}`)})
		var value struct {
			TTL string `json:"auth_service_ticket_ttl"`
		}
		if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &value) != nil {
			return 0
		}
		seconds, err := strconv.ParseFloat(value.TTL, 64)
		running := time.Duration(seconds * float64(time.Second))
		if err != nil || seconds <= 0 || (ttl != 0 && running != ttl) {
			return 0
		}
		ttl = running
	}
	return ttl
}
