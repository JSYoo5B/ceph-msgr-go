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

func TestCephServiceKeyEpochIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_AUTH_EPOCH") != "1" {
		t.Skip("requires an isolated service-key disposal fixture")
	}
	admin, ctx := fixtureClient(t, time.Minute)
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
	held, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatalf("authenticate held client: %q", fmt.Sprint(err))
	}
	defer held.Close()
	if err := held.WaitMgrReady(ctx); err != nil {
		t.Fatalf("establish MGR before service-key disposal: %q", fmt.Sprint(err))
	}
	cold, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatalf("authenticate cold client: %q", fmt.Sprint(err))
	}
	defer cold.Close()
	heldInitial, coldInitial := held.Snapshot(), cold.Snapshot()
	if heldInitial.GlobalID == 0 || coldInitial.GlobalID == 0 || coldInitial.Manager.Ready || managerDials.Load() != 1 {
		t.Fatal("fixture did not establish one held and one cold MGR client")
	}

	type serverMap struct {
		Epoch     uint32  `json:"epoch"`
		AuthEpoch *uint32 `json:"auth_epoch"`
	}
	queryMap := func() serverMap {
		t.Helper()
		result, err := admin.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"mon dump","format":"json"}`)})
		var m serverMap
		if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &m) != nil || m.Epoch == 0 || m.AuthEpoch == nil {
			t.Fatalf("query current Tentacle authentication epoch: %q", fmt.Sprint(err))
		}
		return m
	}
	previous := queryMap()
	for cycle := 1; cycle <= 2; cycle++ {
		// Use a newly authenticated cold client in each cycle. Its first MGR
		// handshake must use the replacement ticket, not an existing session.
		if cycle > 1 {
			cold.Close()
			cold, err = cephmsgr.Dial(ctx, integrationOptions(t))
			if err != nil {
				t.Fatalf("authenticate next cold client: %q", fmt.Sprint(err))
			}
			defer cold.Close()
		}
		oldHeld, oldCold := held.Snapshot(), cold.Snapshot()
		for _, s := range []cephmsgr.State{oldHeld, oldCold} {
			if time.Until(s.AuthTicket.RenewAfter) < time.Minute || time.Until(s.MgrTicket.RenewAfter) < time.Minute {
				t.Fatal("fixture tickets are too short to distinguish epoch notification from scheduled renewal")
			}
		}
		result, err := admin.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"auth wipe-rotating-service-keys"}`)})
		if err != nil || result.Code != 0 {
			// The mutation is submitted once. Never hide an uncertain result
			// by issuing another wipe in a new session.
			t.Fatalf("dispose rotating service keys once: %q", fmt.Sprint(err))
		}
		current := queryMap()
		if current.Epoch <= previous.Epoch || *current.AuthEpoch <= *previous.AuthEpoch {
			t.Fatal("successful service-key disposal did not advance server MonMap/authentication epochs")
		}
		renew, cancel := context.WithTimeout(ctx, 15*time.Second)
		for i, client := range []*cephmsgr.Client{held, cold} {
			before := []cephmsgr.State{oldHeld, oldCold}[i]
			state := waitClientState(t, client, renew, func(s cephmsgr.State) bool {
				return s.Monitor.Ready && s.Monitor.MapEpoch >= current.Epoch && s.AuthTicket.Expires.After(before.AuthTicket.Expires) && s.MgrTicket.Expires.After(before.MgrTicket.Expires)
			})
			if state.GlobalID != before.GlobalID || state.AuthRejection != nil || !time.Now().Before(before.AuthTicket.RenewAfter) || !time.Now().Before(before.MgrTicket.RenewAfter) {
				t.Fatal("epoch renewal lost identity, rejected the retained proof, or waited for scheduled renewal", state)
			}
		}
		cancel()
		if err := held.WaitMgrReady(ctx); err != nil || managerDials.Load() != 1 {
			t.Fatalf("service-key disposal replaced an authenticated held MGR connection: dials=%d error=%q", managerDials.Load(), fmt.Sprint(err))
		}
		state := held.Snapshot()
		if state.Manager.GlobalID != heldInitial.Manager.GlobalID || state.Manager.Name != heldInitial.Manager.Name {
			t.Fatal("service-key disposal unexpectedly changed the active manager")
		}
		if err := fixtureRecoveryRead(ctx, held, true); err != nil {
			t.Fatalf("held MGR after service-key disposal: %q", fmt.Sprint(err))
		}
		if err := cold.WaitMgrReady(ctx); err != nil {
			t.Fatalf("cold MGR with replacement service ticket: %q", fmt.Sprint(err))
		}
		if err := fixtureRecoveryRead(ctx, cold, true); err != nil {
			t.Fatalf("cold MGR command after service-key disposal: %q", fmt.Sprint(err))
		}
		t.Logf("service-key disposal cycle=%d server-map-epoch=%d auth-epoch=%d retained-client-id=%d held-MGR-dials=%d; cold MGR authenticated before scheduled renewal", cycle, current.Epoch, *current.AuthEpoch, heldInitial.GlobalID, managerDials.Load())
		previous = current
	}
}
