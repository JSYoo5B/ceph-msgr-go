package cephmsgr

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
)

func TestCephExpiredTicketRecoveryIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" {
		t.Skip("requires a disposable fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c, err := Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	setting, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"config show","who":"mon.a","key":"auth_allow_insecure_global_id_reclaim"}`)})
	if err != nil || strings.TrimSpace(string(setting.Data)) != "false" {
		t.Fatal("fixture does not enforce secure global ID reclaim", err, string(setting.Data))
	}
	initial := c.snapshotAuth()
	if err := controlFixtureMonitors(ctx, control, false); err != nil {
		t.Fatal(err)
	}
	// Expire the actual Ceph-issued authentication ticket while no MON can
	// renew it. Tentacle may still accept its cryptographic proof for global
	// ID reclaim while the corresponding rotating secret remains available.
	wait := time.Until(initial.Tickets[cephx.ServiceAuth].Expires.Add(time.Second))
	if wait <= 0 {
		t.Fatal("fixture did not issue a current ticket")
	}
	select {
	case <-time.After(wait):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := controlFixtureMonitors(ctx, control, true); err != nil {
		t.Fatal(err)
	}
	result, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status","format":"json"}`)})
	if err != nil || !json.Valid(result.Data) {
		t.Fatal("MON after authentication ticket expiration", err)
	}
	current := c.snapshotAuth()
	if current.GlobalID != initial.GlobalID || !current.Tickets[cephx.ServiceAuth].Expires.After(initial.Tickets[cephx.ServiceAuth].Expires) {
		t.Fatal("accepted reclaim did not preserve identity and renew the ticket", initial.GlobalID, current.GlobalID)
	}
	result, err = c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)})
	if err != nil || !json.Valid(result.Data) {
		t.Fatal("MGR authorizer after client identity expiration", err)
	}
	t.Logf("strict reclaim: global ID=%d retained after ticket lifetime expired and renewed", current.GlobalID)
}
