package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
)

func TestCephExpiredTicketRecoveryIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" || os.Getenv("CEPH_MSGR_TEST_EXPIRE_TICKETS") != "1" {
		t.Skip("requires an isolated ticket expiration fixture")
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
	if err != nil {
		var rejection *AuthenticationError
		if !errors.As(err, &rejection) || rejection.Code != -13 {
			t.Fatal("expired ticket did not recover or preserve an explicit rejection", err)
		}
		// Acceptance depends on whether the MON still has this ticket's
		// rotating secret. A new caller-created client authenticates with its
		// long-term key; the rejected client must not reset its identity itself.
		fresh, freshErr := Dial(ctx, integrationOptions(t))
		if freshErr != nil {
			t.Fatal("fresh authentication after expired proof rejection", freshErr)
		}
		defer fresh.Close()
		if err := fixtureRecoveryRead(ctx, fresh, false); err != nil {
			t.Fatal(err)
		}
		if fresh.snapshotAuth().GlobalID == initial.GlobalID || c.snapshotAuth().GlobalID != initial.GlobalID {
			t.Fatal("rejected identity was automatically reclaimed")
		}
		t.Logf("expired proof rejected with code=%d; explicit fresh Dial obtained a new identity", rejection.Code)
		return
	}
	if !json.Valid(result.Data) {
		t.Fatal("MON after authentication ticket expiration", err)
	}
	current := c.snapshotAuth()
	if current.GlobalID != initial.GlobalID || !current.Tickets[cephx.ServiceAuth].Expires.After(initial.Tickets[cephx.ServiceAuth].Expires) {
		t.Fatal("accepted reclaim did not preserve identity and renew the ticket", initial.GlobalID, current.GlobalID)
	}
	t.Logf("strict reclaim: global ID=%d retained after ticket lifetime expired and renewed", current.GlobalID)
}

func TestCephDiscardedTicketProofIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" || os.Getenv("CEPH_MSGR_TEST_EXPIRE_TICKETS") != "1" {
		t.Skip("requires an isolated ticket expiration fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	options := integrationOptions(t)
	dial := options.DialContext
	var blocked atomic.Bool
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if blocked.Load() {
			return nil, io.EOF
		}
		return dial(ctx, network, endpoint)
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	initial := c.snapshotAuth()
	blocked.Store(true)
	if err := controlFixtureMonitors(ctx, control, false); err != nil {
		t.Fatal(err)
	}
	// Stop this client's recovery while the proof ages. MON persistence can
	// briefly retain an old secret after restart, so let the running quorum
	// complete rotation before allowing this client to reclaim its identity.
	select {
	case <-time.After(4 * 12 * time.Second):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := controlFixtureMonitors(ctx, control, true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-time.After(2 * 12 * time.Second):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	blocked.Store(false)
	for _, call := range []func(context.Context, Command) (Result, error){c.MonCommand, c.MgrCommand} {
		_, err := call(ctx, Command{JSON: []byte(`{"prefix":"status"}`)})
		var rejection *AuthenticationError
		var unknown *OutcomeUnknownError
		if !errors.As(err, &rejection) || rejection.Code != -13 || errors.As(err, &unknown) {
			t.Fatal("discarded proof did not block command admission with the server rejection", err)
		}
	}
	fresh, err := Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal("explicit fresh authentication", err)
	}
	defer fresh.Close()
	if err := fixtureRecoveryRead(ctx, fresh, false); err != nil {
		t.Fatal(err)
	}
	if c.snapshotAuth().GlobalID != initial.GlobalID || fresh.snapshotAuth().GlobalID == initial.GlobalID {
		t.Fatal("rejected client silently replaced its identity")
	}
	t.Log("discarded proof: server rejection blocks MON/MGR admission; explicit fresh Dial succeeds")
}
