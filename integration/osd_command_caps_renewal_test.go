package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// The OSD evaluates the capabilities carried in the client's OSD service
// ticket (TestCephOSDCommandCapsChangeIntegration). The fixture's service
// tickets are short-lived (auth_service_ticket_ttl in every daemon's ceph.conf,
// 12 s in most profiles), so a widened grant reaches the same client once that
// client renews its OSD ticket on its own schedule:
// an OSD session opened after the renewal is allowed, while the session that
// authenticated before it keeps its old capabilities. The client never renews
// because of a capability change.
func TestCephOSDCommandCapsRenewalIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	const entity = "client.ceph-msgr-caps-renewal"
	mon := func(call context.Context, fields map[string]any) cephmsgr.Result {
		encoded, _ := json.Marshal(fields)
		result, err := c.MonCommand(call, cephmsgr.Command{JSON: encoded})
		if err != nil {
			t.Fatal(fields["prefix"], err)
		}
		return result
	}
	created := mon(ctx, map[string]any{"prefix": "auth get-or-create", "entity": entity, "format": "json", "caps": []string{"mon", "allow r", "osd", "allow r"}})
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		mon(cleanup, map[string]any{"prefix": "auth rm", "entity": entity})
	})
	var keys []struct{ Key string }
	if err := json.Unmarshal(created.Data, &keys); err != nil || len(keys) != 1 {
		t.Fatal("auth get-or-create output", err)
	}
	key, err := cephmsgr.ParseKey(keys[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	options := integrationOptions(t)
	options.Key, options.Identity, options.EnableOSD = key, entity, true
	if isReplicatedPGFixtureProfile() {
		options.OSDProfile = replicatedPGFixtureOSDProfile(t)
	}
	client, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	target := cephmsgr.OSDTarget{ID: base.ID, Address: base.Address}
	version := func(o *cephmsgr.OSDConnection) int32 {
		result, err := o.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)})
		var command *cephmsgr.CommandError
		var unknown *cephmsgr.OutcomeUnknownError
		if err != nil && (!errors.As(err, &command) || errors.As(err, &unknown)) {
			t.Fatal("OSD version command", err)
		}
		return result.Code
	}
	held, err := client.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if code := version(held); code != -13 {
		t.Fatal("an osd 'allow r' identity was not refused the command", code)
	}
	issued := client.Snapshot().OSDTicket
	if lifetime := time.Until(issued.Expires); lifetime <= 0 || lifetime > 125*time.Second {
		t.Fatalf("the OSD ticket lifetime %s exceeds the fixture's service ticket TTL", lifetime)
	}

	mon(ctx, map[string]any{"prefix": "auth caps", "entity": entity, "caps": []string{"mon", "allow r", "osd", "allow *"}})
	if code := version(held); code != -13 {
		t.Fatal("the held session changed before any renewal", code)
	}
	// The client renews at three quarters of the ticket's validity.
	var renewed cephmsgr.TicketState
	for deadline := issued.Expires.Add(10 * time.Second); ; {
		if renewed = client.Snapshot().OSDTicket; renewed.Expires.After(issued.Expires) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the client did not renew its OSD ticket", issued, renewed)
		}
		time.Sleep(time.Second)
	}
	heldAfter := version(held)
	held.Close()
	reopened, err := client.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	afterRenewal := version(reopened)
	t.Logf("OSD ticket renewed (expiry %s -> %s); held session code %d, new session code %d", issued.Expires.Format(time.TimeOnly), renewed.Expires.Format(time.TimeOnly), heldAfter, afterRenewal)
	if afterRenewal != 0 {
		t.Fatal("an OSD session opened with the renewed ticket did not see the widened grant", afterRenewal)
	}
	if heldAfter != -13 {
		t.Fatal("the session authenticated before the renewal changed its capabilities", heldAfter)
	}
}
