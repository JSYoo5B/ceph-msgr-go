package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// The OSD ends a registration with a DISCONNECT event while the watcher's
// connection is still up in two cases (PrimaryLogPG handle_watch_timeout and
// _delete_oid): the watcher stopped pinging past its timeout, or another
// client deleted the object. The handle delivers that event and then reports
// ErrOSDWatchDisconnected; nothing is re-registered automatically.
func TestCephOSDWatchDisconnectIntegration(t *testing.T) {
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
	place := func(name string) osdFixtureOracle {
		if modern {
			return replicatedPGNative(t, ctx, name).Placement
		}
		return nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
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
			t.Error("owned watch objects were not removed", result.Cleanup)
		}
	})
	seeded := owned(ctx, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("watched")}, Reserve: 1})
	prepared = true
	if len(seeded.Objects) != 2 {
		t.Fatal("owned watch objects differ", seeded.Objects)
	}
	globalID := c.Snapshot().GlobalID
	expectDisconnect := func(t *testing.T, w *cephmsgr.OSDWatch, within time.Duration) time.Duration {
		t.Helper()
		started := time.Now()
		wait, stop := context.WithTimeout(ctx, within)
		defer stop()
		event, err := w.Next(wait)
		if err != nil || event.Opcode != cephmsgr.OSDWatchDisconnect || event.Cookie != w.Cookie() || event.GlobalID != globalID {
			t.Fatalf("expected a DISCONNECT event for cookie %d: %+v %v", w.Cookie(), event, err)
		}
		if _, err := w.Next(ctx); !errors.Is(err, cephmsgr.ErrOSDWatchDisconnected) {
			t.Fatal("the handle did not end after DISCONNECT", err)
		}
		return time.Since(started)
	}

	t.Run("timeout without pings", func(t *testing.T) {
		name := seeded.Objects[0]
		route := place(name)
		o, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: route.ID, Address: route.Address})
		if err != nil {
			t.Fatal(err)
		}
		defer o.Close()
		const timeout = 5
		w, _, err := o.WatchObject(ctx, requestFor(route), cephmsgr.OSDWatchOptions{TimeoutSeconds: timeout})
		if err != nil {
			t.Fatal("watch", err)
		}
		defer w.Close()
		listed, err := watchReconnectList(ctx, name)
		if err != nil || len(listed.Entries) != 1 || listed.Entries[0].GlobalID != globalID || listed.Entries[0].Cookie != w.Cookie() {
			t.Fatal("native watcher list before the timeout differs", listed, err)
		}
		// No Ping is sent: the OSD's watch timer expires while connected.
		elapsed := expectDisconnect(t, w, 60*time.Second)
		if elapsed < (timeout-1)*time.Second {
			t.Fatalf("DISCONNECT after %s, before the %ds timeout", elapsed, timeout)
		}
		listed, err = watchReconnectList(ctx, name)
		if err != nil || len(listed.Entries) != 0 {
			t.Fatal("native watcher list still holds the timed-out registration", listed, err)
		}
		// The connection itself stays usable for other requests.
		if _, err := o.Request(ctx, requestFor(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})); err != nil {
			t.Fatal("connection after the watch timeout", err)
		}
		t.Logf("DISCONNECT %s after registration with a %ds timeout", elapsed.Round(100*time.Millisecond), timeout)
	})

	t.Run("object deleted by another client", func(t *testing.T) {
		name := seeded.Objects[1]
		route := place(name)
		o, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: route.ID, Address: route.Address})
		if err != nil {
			t.Fatal(err)
		}
		defer o.Close()
		create := requestFor(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 4, Data: []byte("gone")})
		create.ModificationTime = cephmsgr.OSDTime{Seconds: 1700003300}
		if _, err := o.Request(ctx, create); err != nil {
			t.Fatal("create", err)
		}
		w, _, err := o.WatchObject(ctx, requestFor(route), cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
		if err != nil {
			t.Fatal("watch", err)
		}
		defer w.Close()
		options := integrationOptions(t)
		options.EnableOSD = true
		if modern {
			options.OSDProfile = replicatedPGFixtureOSDProfile(t)
		}
		deleter, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer deleter.Close()
		d, err := deleter.OpenOSD(ctx, cephmsgr.OSDTarget{ID: route.ID, Address: route.Address})
		if err != nil {
			t.Fatal(err)
		}
		remove := requestFor(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete})
		remove.ModificationTime = cephmsgr.OSDTime{Seconds: 1700003301}
		if _, err := d.Request(ctx, remove); err != nil {
			t.Fatal("delete by the other client", err)
		}
		elapsed := expectDisconnect(t, w, 20*time.Second)
		if state := owned(ctx, "observe", omapWriteInput{Object: name}).State; state.Code != -2 {
			t.Fatal("native object survived the delete", state.Code)
		}
		t.Logf("DISCONNECT %s after the other client's delete", elapsed.Round(time.Millisecond))
	})
}
