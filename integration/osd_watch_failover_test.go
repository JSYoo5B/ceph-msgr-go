package integration_test

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// A watch registration is part of the object's replicated state. After its
// primary is SIGKILLed, the caller explicitly RECONNECTs the registration to
// the replacement primary of a freshly queried placement: the cookie stays,
// the generation advances, the native watcher list still shows exactly one
// registration, and a notification from another client reaches the
// reconnected handle and completes with its ACK.
func TestCephOSDWatchFailoverIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("primary failover needs three BlueStore replicas")
	}
	modern := isReplicatedPGFixtureProfile()
	var c *cephmsgr.Client
	var base osdFixtureOracle
	if modern {
		c, _, base, _ = replicatedPGFixture(t)
	} else {
		c, _, base, _ = osdFixture(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
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
			t.Error("owned watch object was not removed", result.Cleanup)
		}
	})
	seeded := owned(ctx, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("watched across failover")}})
	prepared = true
	name := seeded.Objects[0]
	route := place(name)
	if len(route.Acting) != 3 {
		t.Fatalf("expected a healthy three-replica placement: %+v", route)
	}
	globalID := c.Snapshot().GlobalID
	old, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: route.ID, Address: route.Address})
	if err != nil {
		t.Fatal(err)
	}
	request := requestFor(route)
	request.ClientIncarnation = 17
	watch, initial, err := old.WatchObject(ctx, request, cephmsgr.OSDWatchOptions{TimeoutSeconds: 30, RegistrationGeneration: 3})
	if err != nil {
		t.Fatal("watch", err)
	}
	if listed, err := watchReconnectList(ctx, name); err != nil || len(listed.Entries) != 1 || listed.Entries[0].Cookie != watch.Cookie() {
		t.Fatal("native watcher list before the failover differs", listed, err)
	}

	victim := strconv.Itoa(int(route.ID))
	if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", victim); err != nil {
		t.Fatal(err)
	}
	restarted := false
	defer func() {
		if restarted {
			return
		}
		restore, stop := context.WithTimeout(context.Background(), 150*time.Second)
		defer stop()
		if err := testcluster.ControlDaemon(restore, dir, "start", "osd", victim); err != nil {
			t.Error("restart killed OSD", err)
			return
		}
		waitOSDFixtureClean(t, restore, base)
		fresh := nativeMutation(t, restore, map[string]any{"compare": false, "placement": true}).Placement
		publishReplicaOSDRouting(t, dir, adoptMutationPlacement(t, base, fresh))
	}()
	old.Close()
	var fresh osdFixtureOracle
	deadline := time.Now().Add(90 * time.Second)
	for {
		fresh = place(name)
		if fresh.ID != route.ID && fresh.ID >= 0 && fresh.Epoch > route.Epoch {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no replacement primary was chosen")
		}
		time.Sleep(time.Second)
	}
	o, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: fresh.ID, Address: fresh.Address})
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	again := requestFor(fresh)
	again.ClientIncarnation = request.ClientIncarnation
	current, reconnect, err := o.ReconnectWatch(ctx, watch, again)
	if err != nil {
		t.Fatal("RECONNECT to the replacement primary", reconnect, err)
	}
	if current.Cookie() != watch.Cookie() || reconnect.GlobalID != globalID || reconnect.Operations[0].Watch.Generation != initial.Operations[0].Watch.Generation+1 {
		t.Fatal("RECONNECT changed the cookie or did not advance the generation", reconnect)
	}
	if listed, err := watchReconnectList(ctx, name); err != nil || len(listed.Entries) != 1 || listed.Entries[0].GlobalID != globalID || listed.Entries[0].Cookie != watch.Cookie() {
		t.Fatal("native watcher list after the failover differs", listed, err)
	}

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
	n, err := notifier.OpenOSD(ctx, cephmsgr.OSDTarget{ID: fresh.ID, Address: fresh.Address})
	if err != nil {
		t.Fatal(err)
	}
	payload, ack := []byte{0, 0xff, 'f', 'o'}, []byte{1, 0, 'a', 'c', 'k'}
	pending, _, err := n.NotifyObject(ctx, requestFor(fresh), payload, cephmsgr.OSDWatchOptions{TimeoutSeconds: 15})
	if err != nil {
		t.Fatal("notify through the replacement primary", err)
	}
	defer pending.Close()
	event, err := current.Next(ctx)
	if err != nil || event.Opcode != cephmsgr.OSDWatchNotification || !bytes.Equal(event.Payload, payload) || event.Cookie != watch.Cookie() || event.Source.ID != fresh.ID {
		t.Fatal("notification after the failover", event, err)
	}
	if _, err := current.Acknowledge(ctx, fresh.Epoch, event, ack); err != nil {
		t.Fatal("acknowledge", err)
	}
	done, err := pending.Next(ctx)
	if err != nil || done.ReturnCode != 0 || len(done.Replies) != 1 || !bytes.Equal(done.Replies[0].Data, ack) || len(done.Missed) != 0 {
		t.Fatal("notify completion after the failover", done, err)
	}
	if _, err := current.Unwatch(ctx, fresh.Epoch); err != nil {
		t.Fatal("unwatch", err)
	}
	t.Logf("watch cookie %d moved osd.%d -> osd.%d (epoch %d -> %d), generation %d -> %d, notify/ACK completed", watch.Cookie(), route.ID, fresh.ID, route.Epoch, fresh.Epoch, initial.Operations[0].Watch.Generation, reconnect.Operations[0].Watch.Generation)
}
