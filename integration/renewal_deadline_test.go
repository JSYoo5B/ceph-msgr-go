package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

func TestCephFractionalTicketRenewalIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_SHORT_TICKETS") != "1" {
		t.Skip("requires the isolated fractional AUTH ticket fixture")
	}
	// Four renewal cycles of about 3/4 of 6.5 s each, plus the initial
	// handshakes and the native keyring gates.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" {
		t.Fatal("fractional ticket fixture has no control directory")
	}
	requireFreshNativeManagerKeyring(t, ctx, control, "pre-dial")
	options := integrationOptions(t)
	dial := options.DialContext
	var managerDials atomic.Int32
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err == nil {
			_, role, _ := testcluster.SplitPeer(endpoint)
			if role == "mgr.a" || role == "mgr.b" {
				managerDials.Add(1)
			}
		}
		return conn, err
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatalf("authenticate with fractional ticket validity: %q", fmt.Sprint(err))
	}
	defer c.Close()
	if err := c.WaitMgrReady(ctx); err != nil {
		// Freeze the first refusal and the identities the client saw, then
		// collect the bounded native first-failure bundle before any other
		// diagnostic. Neither step changes this failure.
		collectAuthFirstFailureBundle(t, control, freezeAuthFirstFailure(t, "short-tickets", c.Snapshot(), 0, time.Now(), err))
		// Attribute a native keyring fault before reporting a product failure.
		gate, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		requireFreshNativeManagerKeyring(t, gate, control, "post-failure")
		t.Fatalf("authenticate initial MGR: %q", fmt.Sprint(err))
	}
	initial := c.Snapshot()
	if initial.GlobalID == 0 || managerDials.Load() != 1 {
		t.Fatal("fixture did not establish one authenticated MGR connection")
	}
	verifyValidity := func(s cephmsgr.State) {
		t.Helper()
		// Both times share the decoding clock, so their difference proves
		// fractional validity without latency bias. Ceph can cap the
		// configured TTL to its rotating key's remaining lifetime.
		if ttl := 4 * s.AuthTicket.Expires.Sub(s.AuthTicket.RenewAfter); ttl <= 0 || ttl > shortTicketAuthTTL || ttl%time.Second == 0 {
			t.Fatalf("fixture did not issue a positive fractional AUTH ticket within its %v TTL: %v", shortTicketAuthTTL, ttl)
		}
		if ttl := 4 * s.MgrTicket.Expires.Sub(s.MgrTicket.RenewAfter); ttl <= shortTicketAuthTTL || ttl > 12*time.Second {
			t.Fatalf("fixture did not retain the ordinary MGR service ticket lifetime: %v", ttl)
		}
	}
	verifyValidity(initial)
	previous := initial
	for cycle := 1; cycle <= 4; cycle++ {
		deadline := previous.AuthTicket.Expires
		if previous.MgrTicket.Expires.Before(deadline) {
			deadline = previous.MgrTicket.Expires
		}
		renew, stop := context.WithDeadline(ctx, deadline)
		defer stop()
		state := waitClientState(t, c, renew, func(s cephmsgr.State) bool {
			return s.Monitor.Ready && s.AuthTicket.RenewAfter.After(previous.AuthTicket.RenewAfter) && s.MgrTicket.RenewAfter.After(previous.MgrTicket.RenewAfter)
		})
		stop()
		if !time.Now().Before(deadline) || state.GlobalID != initial.GlobalID || state.AuthRejection != nil || !state.Manager.Ready || state.Manager.GlobalID != initial.Manager.GlobalID || managerDials.Load() != 1 {
			t.Fatal("fractional renewal was late or changed the authenticated client/MGR", state, managerDials.Load())
		}
		verifyValidity(state)
		for _, call := range []struct {
			command string
			call    func(context.Context, cephmsgr.Command) (cephmsgr.Result, error)
		}{
			{`{"prefix":"status","format":"json"}`, c.MonCommand},
			{`{"prefix":"pg stat","format":"json"}`, c.MgrCommand},
		} {
			result, err := call.call(ctx, cephmsgr.Command{JSON: []byte(call.command)})
			if err != nil || result.Code != 0 || !json.Valid(result.Data) {
				t.Fatalf("command after fractional renewal: %q", fmt.Sprint(err))
			}
		}
		t.Logf("fractional renewal cycle=%d auth-validity=%v mgr-validity=%v expiry-margin=%v retained-client-id=%d held-MGR-dials=%d", cycle, 4*state.AuthTicket.Expires.Sub(state.AuthTicket.RenewAfter), 4*state.MgrTicket.Expires.Sub(state.MgrTicket.RenewAfter), time.Until(deadline), state.GlobalID, managerDials.Load())
		previous = state
	}
}

// shortTicketAuthTTL is the fixture's fractional AUTH ticket lifetime
// (integration/fixture/provider.go). The MGR service ticket keeps 12 s.
const shortTicketAuthTTL = 6500 * time.Millisecond

// requireFreshNativeManagerKeyring asks the fixture whether an owned native MGR
// lost its registered MON session: an owned MON refused the reclaim of its
// MgrMap GID, it is missing from the MgrMap, or no owned MON holds its beacon
// session. Such a MGR keeps a stale rotating keyring and refuses current Go MGR
// tickets with -13: a native fixture fault, not product evidence. A -13 of one
// of its module MonClients is not that fault and does not excuse a Go -13.
func requireFreshNativeManagerKeyring(t *testing.T, ctx context.Context, control, label string) {
	t.Helper()
	if err := testcluster.ControlDaemon(ctx, control, "verify", "mgr-keyring", label); err != nil {
		t.Fatal("fixture error: native MGR keyring gate", err)
	}
	data, err := os.ReadFile(filepath.Join(control, "manager-keyring-gate.json"))
	var receipt struct {
		Label        string `json:"label"`
		FixtureError string `json:"fixture_error"`
	}
	if err != nil || json.Unmarshal(data, &receipt) != nil || receipt.Label != label {
		t.Fatal("fixture error: native MGR keyring gate receipt", err)
	}
	if receipt.FixtureError != "" {
		t.Fatalf("%s: %s", receipt.FixtureError, data)
	}
	t.Logf("native MGR keyring gate %s: %s", label, data)
}
