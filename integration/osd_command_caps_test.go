package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// An OSD evaluates a command against the capabilities its session
// authenticated with. After MON `auth caps` widens an identity's OSD grant,
// this records where the change becomes visible: on the already authenticated
// OSD session, on a new OSD session of the same client (which reuses its
// cached OSD service ticket), and on a newly dialed client with fresh tickets.
// The client never refreshes tickets because of a capability change.
func TestCephOSDCommandCapsChangeIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	const entity = "client.ceph-msgr-caps-change"
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
	dial := func() *cephmsgr.Client {
		options := integrationOptions(t)
		options.Key, options.Identity, options.EnableOSD = key, entity, true
		if isReplicatedPGFixtureProfile() {
			options.OSDProfile = replicatedPGFixtureOSDProfile(t)
		}
		client, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { client.Close() })
		return client
	}
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
	first := dial()
	o, err := first.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if code := version(o); code != -13 {
		t.Fatal("an osd 'allow r' identity was not refused the command", code)
	}
	mon(ctx, map[string]any{"prefix": "auth caps", "entity": entity, "caps": []string{"mon", "allow r", "osd", "allow *"}})
	time.Sleep(2 * time.Second)
	sameSession := version(o)
	o.Close()
	o, err = first.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	sameClient := version(o)
	fresh, err := dial().OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	newClient := version(fresh)
	t.Logf("after auth caps: same OSD session %d, new OSD session of the same client %d, new client %d", sameSession, sameClient, newClient)
	// Observed: capabilities travel in the OSD service ticket, so only fresh
	// tickets see the change.
	if sameSession != -13 || sameClient != -13 || newClient != 0 {
		t.Fatalf("unexpected propagation: same session %d, same client %d, new client %d", sameSession, sameClient, newClient)
	}
}
