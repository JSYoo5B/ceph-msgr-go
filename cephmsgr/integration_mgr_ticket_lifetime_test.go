package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
)

// lifetimeHolds reports whether the Go daemon's rotating MGR set holds id,
// with the held IDs for the log.
func lifetimeHolds(d *MgrDaemon, id uint64) (bool, []uint64) {
	ids := wipeIDs(d.State().RotatingKeys)
	return slices.Contains(ids, id), ids
}

// lifetimeStock presents authorizer to the stock active MGR that before
// names, bracketed by a second native dump. A changed active MGR is
// inconclusive and fails the test; otherwise the handshake's error is
// returned after the bracket's epoch.
func lifetimeStock(t *testing.T, ctx context.Context, control string, admin *Client, before wipeMgrDump, label string, authorizer *cephx.Authorizer) (uint32, error) {
	t.Helper()
	err := stockPresent(ctx, admin, before, stockMgrAddress(t, before), authorizer)
	after, _ := wipeDump(t, ctx, control, label+"-bracket")
	if after.ActiveGID != before.ActiveGID || after.ActiveName != before.ActiveName || !slices.Equal(after.ActiveAddrs.Addrvec, before.ActiveAddrs.Addrvec) {
		t.Fatalf("%s: inconclusive, the active MGR changed during the presentation: %+v -> %+v", label, before, after)
	}
	return after.Epoch, err
}

// TestCephMgrTicketLifetimeIntegration classifies which MGR service ticket
// lifetimes the stock MGR accepts, and checks the Go MGR acceptor against
// it (#133, with the secret-ID observation of #139). One CLIENT MGR service
// ticket issued by the stock MON to the admin connection is retained, and
// fresh authorizers of that one copy are presented, at three points of its
// life, to a Go MGR daemon's listener (scoped mgr.<id> principal, no
// beacon) and to the stock active MGR, pinned to the native MgrMap address,
// nonce and GID and bracketed by a second native dump:
//
//   - within the ticket's validity, both admit it (positive control);
//   - after the ticket's expiry, while the Go daemon's rotating set (its
//     view of the MON's MGR secrets) still holds the ticket's secret ID,
//     both admit it: neither the stock MGR (cephx_verify_authorizer) nor the
//     Go acceptor checks ticket expiry;
//   - once the Go set no longer holds that ID, the Go listener refuses it
//     at authentication as an unknown service secret ID (-EACCES to the
//     client), and the stock MGR refuses it once its own set is refreshed,
//     which is polled every second for a bounded interval. A ticket the
//     admin renewed under a newer secret ID is then admitted by both, so a
//     refusal is not an outage of the target.
//
// The lookup is by exact secret ID (RotatingKeyRing::get_service_secret),
// so the accepted lifetime of a ticket is the residence of its secret ID in
// the acceptor's rotating set, and the two acceptors' sets differ only by
// their refresh times. Each presentation is classified by the Go set's
// state before and after it; a change in between, or the Go set losing the
// ID before the ticket's expiry, is inconclusive and fails the test. The
// stock MGR's own set is not observable: its refusal time is recorded
// relative to the Go set's eviction.
//
// Not covered: the all-ones (general) secret ID, tickets of other services
// and stock acceptors other than the MGR, IPv6, arm64.
//
// Pinned sources: ceph/ceph/blob/v20.2.4/src/auth/cephx/CephxProtocol.cc
// (cephx_verify_authorizer), ceph/ceph/blob/v20.2.4/src/auth/RotatingKeyRing.cc
// (get_service_secret), ceph/ceph/blob/v20.2.4/src/auth/cephx/CephxKeyServer.cc
// (_rotate_secret, get_service_secret and the ticket TTL cap),
// ceph/ceph/blob/v20.2.4/src/mon/MonClient.cc (_check_auth_rotating).
//
//scenario:row mgr-daemon.aes256k.ip4.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mgrticketlifetime key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 capabilities=mgr-daemon-control
//scenario:row mgr-daemon.aes.ip4.crc.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mgrticketlifetime key=aes service=aes mode=crc family=4 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 capabilities=mgr-daemon-control
//scenario:covers Messenger/CephX
//scenario:covers Daemon-to-daemon
//fixture:isolated needs the stock active MGR unchanged for several service ticket lifetimes; creates and removes a scoped MGR principal
func TestCephMgrTicketLifetimeIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" || os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "2" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" {
		t.Skip("requires the disposable one-OSD two-MGR fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if _, admitted := wipeDump(t, ctx, control, "lifetime-admission"); !admitted {
		t.Skip("reading the native MgrMap requires this test alone with fixture capability mgr-daemon-control")
	}
	base := integrationOptions(t)
	admin, err := Dial(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ttl := principalRunningTTL(t, ctx, admin)
	target, _, stop := startPrincipalListener(t, ctx, admin, base, ttl, fmt.Sprintf("golt%d", time.Now().UnixNano()%1_000_000))
	defer stop()
	stock := wipeDumpUntil(t, ctx, control, "lifetime-stock", func(d wipeMgrDump) bool { return d.Available && d.ActiveGID != 0 && len(d.ActiveAddrs.Addrvec) > 0 })
	t.Logf("go acceptor %s (GID %d); stock mgr.%s/%d active at native epoch %d; service ticket TTL %v",
		target.name, target.daemon.Identity().GlobalID, stock.ActiveName, stock.ActiveGID, stock.Epoch, ttl)

	retained := retainPrincipalTicket(t, ctx, admin, "lifetime", nil, ttl/2, 3*ttl)
	id := retained.ticket.SecretID
	expires := retained.ticket.Expires
	step := 0
	// goPresent presents a fresh authorizer of ticket to the Go listener and
	// returns its admission or refusal with the Go set's state around it.
	goPresent := func(phase string, ticket principalTicket, wantHeld bool) (MgrPeerIdentity, *MgrListenerRefusal, error) {
		t.Helper()
		held, ids := lifetimeHolds(target.daemon, ticket.ticket.SecretID)
		if held != wantHeld {
			t.Fatalf("%s: inconclusive, the Go set %v holds secret ID %d: %v, want %v", phase, ids, ticket.ticket.SecretID, held, wantHeld)
		}
		admitted, refusal, clientErr := principalPresent(t, ctx, admin, target, phase, ticket.authorizer(t))
		if after, afterIDs := lifetimeHolds(target.daemon, ticket.ticket.SecretID); after != held {
			t.Fatalf("%s: inconclusive, the Go set changed during the presentation: %v -> %v", phase, ids, afterIDs)
		}
		return admitted, refusal, clientErr
	}
	goAdmit := func(phase string, ticket principalTicket, wantHeld bool) {
		t.Helper()
		admitted, refusal, clientErr := goPresent(phase, ticket, wantHeld)
		if refusal != nil {
			t.Fatalf("%s: %s refused secret ID %d: %+v, client %v", phase, target.name, ticket.ticket.SecretID, refusal, clientErr)
		}
		if admitted.SecretID != ticket.ticket.SecretID || admitted.GlobalID != ticket.auth.GlobalID || admitted.Entity != base.Identity {
			t.Fatalf("%s: %s admitted %+v, want secret ID %d, global ID %d", phase, target.name, admitted, ticket.ticket.SecretID, ticket.auth.GlobalID)
		}
		_, ids := lifetimeHolds(target.daemon, ticket.ticket.SecretID)
		t.Logf("%s: %s admitted secret ID %d (ticket expiry %v from now); Go set %v",
			phase, target.name, ticket.ticket.SecretID, time.Until(ticket.ticket.Expires).Round(time.Millisecond), ids)
	}
	stockAdmit := func(phase string, ticket principalTicket) {
		t.Helper()
		step++
		epoch, err := lifetimeStock(t, ctx, control, admin, stock, fmt.Sprintf("lifetime-%d", step), ticket.authorizer(t))
		if err != nil {
			_, ids := lifetimeHolds(target.daemon, ticket.ticket.SecretID)
			t.Fatalf("%s: mgr.%s/%d (native epochs %d..%d) refused secret ID %d with the Go set %v: %v", phase, stock.ActiveName, stock.ActiveGID, stock.Epoch, epoch, ticket.ticket.SecretID, ids, err)
		}
		t.Logf("%s: mgr.%s/%d (native epochs %d..%d) authenticated secret ID %d (ticket expiry %v from now)",
			phase, stock.ActiveName, stock.ActiveGID, stock.Epoch, epoch, ticket.ticket.SecretID, time.Until(ticket.ticket.Expires).Round(time.Millisecond))
	}

	// Within the ticket's validity.
	goAdmit("valid", retained, true)
	stockAdmit("valid", retained)
	if !time.Now().Before(expires) {
		t.Fatal("inconclusive, the positive control ended after the ticket's expiry")
	}

	// After the ticket's expiry, while the Go set still holds its secret ID.
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(time.Until(expires) + 500*time.Millisecond):
	}
	goAdmit("expired", retained, true)
	stockAdmit("expired", retained)
	if held, ids := lifetimeHolds(target.daemon, id); !held {
		t.Fatalf("expired: inconclusive, the Go set %v lost secret ID %d during the stock presentation", ids, id)
	}
	t.Logf("expired: both acceptors admitted secret ID %d after the ticket's expiry", id)

	// Once the Go set no longer holds the secret ID.
	var evicted time.Time
	for deadline := expires.Add(4 * ttl); ; {
		if held, _ := lifetimeHolds(target.daemon, id); !held {
			evicted = time.Now()
			break
		}
		if time.Now().After(deadline) {
			_, ids := lifetimeHolds(target.daemon, id)
			t.Fatalf("the Go set %v still held secret ID %d %v after the ticket's expiry", ids, id, 4*ttl)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	keys := target.daemon.State().RotatingKeys
	t.Logf("evicted: the Go set dropped secret ID %d %v after the ticket's expiry: %v (generation %d)",
		id, evicted.Sub(expires).Round(time.Millisecond), wipeIDs(keys), keys.Generation)
	var authorizerErr *cephx.AuthorizerError
	var rejection *AuthenticationError
	if admitted, refusal, clientErr := goPresent("evicted", retained, false); refusal == nil {
		t.Fatalf("evicted: %s admitted secret ID %d after its eviction: %+v", target.name, id, admitted)
	} else if !refusal.Refused || refusal.Stage != "authentication" || !errors.As(refusal.Err, &authorizerErr) || authorizerErr.Reason != cephx.RefusedUnknownSecret ||
		!errors.As(clientErr, &rejection) || rejection.Code != -13 {
		t.Fatalf("evicted: %s did not refuse secret ID %d as unknown: %+v, client %v", target.name, id, refusal, clientErr)
	} else {
		t.Logf("evicted: %s refused secret ID %d at %s: %s; the client saw method %d code %d", target.name, id, refusal.Stage, refusal.Reason, rejection.Method, rejection.Code)
	}
	for attempt := 1; ; attempt++ {
		step++
		epoch, err := lifetimeStock(t, ctx, control, admin, stock, fmt.Sprintf("lifetime-%d", step), retained.authorizer(t))
		if err != nil {
			if !errors.As(err, &rejection) {
				t.Fatalf("evicted: mgr.%s ended the handshake of secret ID %d without an authentication refusal: %v", stock.ActiveName, id, err)
			}
			_, ids := lifetimeHolds(target.daemon, id)
			t.Logf("evicted: mgr.%s/%d (native epochs %d..%d) refused secret ID %d at attempt %d, %v after the Go set's eviction and %v after the ticket's expiry: method %d code %d; Go set %v",
				stock.ActiveName, stock.ActiveGID, stock.Epoch, epoch, id, attempt, time.Since(evicted).Round(time.Millisecond), time.Since(expires).Round(time.Millisecond), rejection.Method, rejection.Code, ids)
			break
		}
		if time.Since(evicted) > 2*ttl {
			t.Fatalf("evicted: mgr.%s still authenticated secret ID %d %v after the Go set's eviction (%d attempts)", stock.ActiveName, id, time.Since(evicted).Round(time.Millisecond), attempt)
		}
		t.Logf("evicted: mgr.%s still authenticated secret ID %d at attempt %d, %v after the Go set's eviction", stock.ActiveName, id, attempt, time.Since(evicted).Round(time.Millisecond))
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}

	// A renewed ticket is admitted by both targets after the refusals.
	renewed := retainPrincipalTicket(t, ctx, admin, "lifetime renewed", []uint64{id}, ttl/4, 3*ttl)
	if renewed.ticket.SecretID <= id {
		t.Fatalf("renewed: secret ID %d does not follow %d", renewed.ticket.SecretID, id)
	}
	for deadline := renewed.ticket.Expires.Add(-time.Second); ; {
		if held, _ := lifetimeHolds(target.daemon, renewed.ticket.SecretID); held {
			break
		}
		if time.Now().After(deadline) {
			_, ids := lifetimeHolds(target.daemon, renewed.ticket.SecretID)
			t.Fatalf("renewed: inconclusive, the Go set %v did not hold secret ID %d within the ticket's validity", ids, renewed.ticket.SecretID)
		}
		time.Sleep(100 * time.Millisecond)
	}
	goAdmit("renewed", renewed, true)
	stockAdmit("renewed", renewed)
	if !time.Now().Before(renewed.ticket.Expires) {
		t.Fatal("renewed: inconclusive, the control ended after the renewed ticket's expiry")
	}
	t.Logf("lifetime: secret ID %d admitted within validity and after expiry while held, refused once evicted; renewed secret ID %d admitted", id, renewed.ticket.SecretID)
}
