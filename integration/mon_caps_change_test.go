package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// A MON capability change made with `auth caps` while a client holds a MON
// session. A test-only identity with `mon 'allow r'` is refused a config write
// (-13); its grant is then widened to `allow rw`. The test records whether the
// existing session, which authenticated under the old capabilities, sees the
// change, and requires a newly dialed client to be allowed. The write is a
// harmless option on the identity's own config section, removed afterwards.
func TestCephMonCommandCapsChangeIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	admin, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	// Registered first, so it closes after the cleanup below has run.
	t.Cleanup(func() { admin.Close() })
	const entity = "client.ceph-msgr-mon-caps-change"
	mon := func(call context.Context, fields map[string]any) cephmsgr.Result {
		encoded, _ := json.Marshal(fields)
		result, err := admin.MonCommand(call, cephmsgr.Command{JSON: encoded})
		if err != nil {
			t.Fatal(fields["prefix"], err)
		}
		return result
	}
	created := mon(ctx, map[string]any{"prefix": "auth get-or-create", "entity": entity, "format": "json", "caps": []string{"mon", "allow r"}})
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		mon(cleanup, map[string]any{"prefix": "config rm", "who": entity, "name": "debug_ms"})
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
		options.Key, options.Identity = key, entity
		client, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { client.Close() })
		return client
	}
	write := cephmsgr.Command{JSON: []byte(`{"prefix":"config set","who":"` + entity + `","name":"debug_ms","value":"0/0"}`)}
	attempt := func(client *cephmsgr.Client) int32 {
		result, err := client.MonCommand(ctx, write)
		var refused *cephmsgr.CommandError
		var unknown *cephmsgr.OutcomeUnknownError
		switch {
		case err == nil:
			return result.Code
		case errors.As(err, &refused) && !errors.As(err, &unknown) && refused.Code == result.Code:
			return refused.Code
		}
		t.Fatal("config write ended without a known reply", err)
		return 0
	}
	held := dial()
	if code := attempt(held); code != -13 {
		t.Fatal("config write under mon 'allow r'", code)
	}
	if read, err := held.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"fsid"}`)}); err != nil || read.Code != 0 {
		t.Fatal("read under mon 'allow r'", err)
	}
	mon(ctx, map[string]any{"prefix": "auth caps", "entity": entity, "caps": []string{"mon", "allow rw"}})
	time.Sleep(2 * time.Second)
	existing := attempt(held)
	fresh := attempt(dial())
	t.Logf("after auth caps mon 'allow rw': existing session code %d, new client code %d (session on %s)", existing, fresh, held.Snapshot().Monitor.Endpoint)
	if fresh != 0 {
		t.Fatal("a newly dialed client did not get the widened grant", fresh)
	}
	if existing != 0 && existing != -13 {
		t.Fatal("existing session answered neither the old nor the new grant", existing)
	}
}
