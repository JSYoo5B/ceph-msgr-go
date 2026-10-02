package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephFractionalTicketRenewalIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_SHORT_TICKETS") != "1" {
		t.Skip("requires the isolated 1.5-second ticket fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	options := integrationOptions(t)
	dial := options.DialContext
	var managerDials atomic.Int32
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err == nil {
			_, port, _ := net.SplitHostPort(endpoint)
			if port == "36800" || port == "36801" {
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
		if ttl := 4 * s.AuthTicket.Expires.Sub(s.AuthTicket.RenewAfter); ttl <= 0 || ttl > 1500*time.Millisecond || ttl%time.Second == 0 {
			t.Fatalf("fixture did not issue a positive fractional AUTH ticket within its 1.5-second TTL: %v", ttl)
		}
		if ttl := 4 * s.MgrTicket.Expires.Sub(s.MgrTicket.RenewAfter); ttl <= 1500*time.Millisecond || ttl > 12*time.Second {
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
