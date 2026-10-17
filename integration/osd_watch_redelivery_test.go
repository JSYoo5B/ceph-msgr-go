package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// A notification sent while the watcher's connection is closed stays pending
// on the OSD (the registration is disconnected, not removed). An explicit
// RECONNECT makes the OSD redeliver it (Watch::connect sends in-progress
// notifies), and the reconnected handle's ACK completes the notify.
func TestCephOSDWatchNotifyRedeliveryIntegration(t *testing.T) {
	modern := isReplicatedPGFixtureProfile()
	var c *cephmsgr.Client
	var base osdFixtureOracle
	if modern {
		c, _, base, _ = replicatedPGFixture(t)
	} else {
		c, _, base, _ = osdFixture(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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
	seeded := owned(ctx, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("redelivery")}})
	prepared = true
	name := seeded.Objects[0]
	var route osdFixtureOracle
	if modern {
		route = replicatedPGNative(t, ctx, name).Placement
	} else {
		route = nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
	}
	target := cephmsgr.OSDTarget{ID: route.ID, Address: route.Address}
	old, err := c.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	watch, _, err := old.WatchObject(ctx, requestFor(route), cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
	if err != nil {
		t.Fatal("watch", err)
	}
	old.Close()

	options := integrationOptions(t)
	options.EnableOSD = true
	if modern {
		options.OSDProfile = replicatedPGFixtureOSDProfile(t)
	}
	notifier, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer notifier.Close()
	n, err := notifier.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	payload, ack := []byte{0, 0xff, 'r', 'e', 'd'}, []byte{0, 'a', 'c', 'k'}
	pending, _, err := n.NotifyObject(ctx, requestFor(route), payload, cephmsgr.OSDWatchOptions{TimeoutSeconds: 20})
	if err != nil {
		t.Fatal("notify while the watcher is disconnected", err)
	}
	defer pending.Close()
	// The notify cannot complete yet: its only watcher is disconnected.
	early, stop := context.WithTimeout(ctx, 2*time.Second)
	if done, err := pending.Next(early); err == nil {
		stop()
		t.Fatal("notify completed without the disconnected watcher", done)
	}
	stop()

	o, err := c.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	current, _, err := o.ReconnectWatch(ctx, watch, requestFor(route))
	if err != nil {
		t.Fatal("RECONNECT", err)
	}
	event, err := current.Next(ctx)
	if err != nil || event.Opcode != cephmsgr.OSDWatchNotification || !bytes.Equal(event.Payload, payload) || event.Cookie != watch.Cookie() {
		t.Fatal("pending notification was not redelivered after RECONNECT", event, err)
	}
	if _, err := current.Acknowledge(ctx, route.Epoch, event, ack); err != nil {
		t.Fatal("acknowledge", err)
	}
	done, err := pending.Next(ctx)
	if err != nil || done.ReturnCode != 0 || len(done.Replies) != 1 || !bytes.Equal(done.Replies[0].Data, ack) || len(done.Missed) != 0 {
		t.Fatal("notify completion after redelivery", done, err)
	}
	if _, err := current.Unwatch(ctx, route.Epoch); err != nil {
		t.Fatal("unwatch", err)
	}
	t.Logf("notify %d sent while disconnected was redelivered after RECONNECT and completed with the ACK", event.NotifyID)
}
