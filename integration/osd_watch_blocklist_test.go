package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// A watcher's exact address (from the independent native watcher list) is
// added to the OSD blocklist through MON. The OSD sends DISCONNECT at once, the
// Go handle ends without re-registering, the native list empties, and the
// watcher client's next request to that OSD gets a known -108 (ESHUTDOWN). The
// blocklist entry is removed in cleanup.
func TestCephOSDWatchBlocklistIntegration(t *testing.T) {
	modern := isReplicatedPGFixtureProfile()
	var c *cephmsgr.Client
	var base osdFixtureOracle
	if modern {
		c, _, base, _ = replicatedPGFixture(t)
	} else {
		c, _, base, _ = osdFixture(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	owned := func(call context.Context, mode string, input omapWriteInput) omapWriteReceipt {
		if modern {
			return replicatedPGOwned(t, call, base, mode, input)
		}
		return nativeOmapWrite(t, call, base, mode, input)
	}
	requestFor := func(route osdFixtureOracle, ops ...cephmsgr.OSDOperation) cephmsgr.OSDRequest {
		if modern {
			return replicatedPGRequest(route, ops...)
		}
		return osdObjectRequest(route, ops...)
	}
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		if result := owned(cleanup, "cleanup", omapWriteInput{}); !result.Cleanup.Removed {
			t.Error("owned watch object was not removed", result.Cleanup)
		}
	})
	seeded := owned(ctx, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("blocklisted watch")}})
	prepared = true
	name := seeded.Objects[0]
	var route osdFixtureOracle
	if modern {
		route = replicatedPGNative(t, ctx, name).Placement
	} else {
		route = nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
	}

	options := integrationOptions(t)
	options.EnableOSD = true
	if modern {
		options.OSDProfile = replicatedPGFixtureOSDProfile(t)
	}
	watcher, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()
	o, err := watcher.OpenOSD(ctx, cephmsgr.OSDTarget{ID: route.ID, Address: route.Address})
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	w, _, err := o.WatchObject(ctx, requestFor(route), cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
	if err != nil {
		t.Fatal("watch", err)
	}
	defer w.Close()
	listed, err := watchReconnectList(ctx, name)
	if err != nil || len(listed.Entries) != 1 || listed.Entries[0].GlobalID != watcher.Snapshot().GlobalID || listed.Entries[0].Cookie != w.Cookie() || listed.Entries[0].Address == "" {
		t.Fatal("native watcher list before the blocklist differs", listed, err)
	}
	address := listed.Entries[0].Address

	blocklist := func(call context.Context, op string) error {
		// The default expiry (one hour) applies; cleanup removes the entry.
		fields := map[string]any{"prefix": "osd blocklist", "blocklistop": op, "addr": address}
		encoded, _ := json.Marshal(fields)
		_, err := c.MonCommand(call, cephmsgr.Command{JSON: encoded})
		return err
	}
	if err := blocklist(ctx, "add"); err != nil {
		t.Fatal("blocklist the watcher address", address, err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if err := blocklist(cleanup, "rm"); err != nil {
			t.Error("remove the blocklist entry", address, err)
		}
	})
	blocked := time.Now()

	wait, stop := context.WithTimeout(ctx, 60*time.Second)
	event, err := w.Next(wait)
	stop()
	if err != nil || event.Opcode != cephmsgr.OSDWatchDisconnect || event.Cookie != w.Cookie() {
		t.Fatalf("expected a DISCONNECT for the blocklisted watch: %+v %v", event, err)
	}
	t.Logf("DISCONNECT %s after the blocklist", time.Since(blocked).Round(time.Millisecond))
	if _, err := w.Next(ctx); !errors.Is(err, cephmsgr.ErrOSDWatchDisconnected) {
		t.Fatal("the handle did not end after DISCONNECT", err)
	}
	for deadline := time.Now().Add(60 * time.Second); ; {
		listed, err = watchReconnectList(ctx, name)
		if err == nil && len(listed.Entries) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("native watcher list still holds the blocklisted watcher", listed, err)
		}
		time.Sleep(time.Second)
	}
	call, stopCall := context.WithTimeout(ctx, 10*time.Second)
	reply, err := o.Request(call, requestFor(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}))
	stopCall()
	var server *cephmsgr.OSDError
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != -108 || reply.Code != -108 {
		t.Fatal("a blocklisted client's STAT must be a known -108 (ESHUTDOWN)", reply.Code, err)
	}
}
