package integration_test

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// The watched object's primary OSD is SIGKILLed. The registration is part of
// the object's replicated state, so the new primary holds it as disconnected.
// A notify sent to the new primary while the watcher is between primaries
// waits for it; the watcher's explicit RECONNECT to the new primary makes the
// OSD redeliver the notification, and the ACK completes the notify.
func TestCephOSDWatchFailoverRedeliveryIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" || isReplicatedPGFixtureProfile() {
		// The shared restoration helper is legacy-v6 only.
		t.Skip("requires three legacy-v6 BlueStore replicas")
	}
	c, _, base, original := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		if result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}); !result.Cleanup.Removed {
			t.Error("owned watch object was not removed", result.Cleanup)
		}
	})
	seeded := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("failover redelivery")}})
	prepared = true
	name := seeded.Objects[0]
	place := func() osdFixtureOracle {
		return nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
	}
	route := place()
	old, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: route.ID, Address: route.Address})
	if err != nil {
		t.Fatal(err)
	}
	watch, _, err := old.WatchObject(ctx, osdObjectRequest(route), cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
	if err != nil {
		t.Fatal("watch", err)
	}
	victim := strconv.Itoa(int(route.ID))
	if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", victim); err != nil {
		t.Fatal("kill primary", err)
	}
	killed := time.Now()
	old.Close()
	restarted := false
	defer func() {
		if !restarted {
			recoverMutationFixture(t, c, base, original, victim, true)
		}
	}()
	moved := place()
	for deadline := killed.Add(60 * time.Second); moved.ID == route.ID; moved = place() {
		if time.Now().After(deadline) {
			t.Fatal("the killed primary was not replaced")
		}
		time.Sleep(time.Second)
	}

	options := integrationOptions(t)
	options.EnableOSD = true
	notifier, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer notifier.Close()
	n, err := notifier.OpenOSD(ctx, cephmsgr.OSDTarget{ID: moved.ID, Address: moved.Address})
	if err != nil {
		t.Fatal(err)
	}
	payload, ack := []byte{0, 0xff, 'f', 'o'}, []byte{0, 'a', 'c', 'k', 'f'}
	pending, _, err := n.NotifyObject(ctx, osdObjectRequest(moved), payload, cephmsgr.OSDWatchOptions{TimeoutSeconds: 20})
	if err != nil {
		t.Fatal("notify to the new primary", err)
	}
	defer pending.Close()
	early, stop := context.WithTimeout(ctx, 2*time.Second)
	if done, err := pending.Next(early); err == nil {
		stop()
		t.Fatal("notify completed without the disconnected watcher", done)
	}
	stop()

	o, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: moved.ID, Address: moved.Address})
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	current, _, err := o.ReconnectWatch(ctx, watch, osdObjectRequest(moved))
	if err != nil {
		t.Fatal("RECONNECT to the new primary", err)
	}
	event, err := current.Next(ctx)
	if err != nil || event.Opcode != cephmsgr.OSDWatchNotification || !bytes.Equal(event.Payload, payload) || event.Cookie != watch.Cookie() {
		t.Fatal("the notification was not redelivered after RECONNECT to the new primary", event, err)
	}
	if _, err := current.Acknowledge(ctx, moved.Epoch, event, ack); err != nil {
		t.Fatal("acknowledge", err)
	}
	done, err := pending.Next(ctx)
	if err != nil || done.ReturnCode != 0 || len(done.Replies) != 1 || !bytes.Equal(done.Replies[0].Data, ack) || len(done.Missed) != 0 {
		t.Fatal("notify completion after redelivery", done, err)
	}
	if _, err := current.Unwatch(ctx, moved.Epoch); err != nil {
		t.Fatal("unwatch", err)
	}
	t.Logf("primary osd.%d -> osd.%d (epoch %d -> %d, %s after the kill); notify %d redelivered after RECONNECT and completed", route.ID, moved.ID, route.Epoch, moved.Epoch, time.Since(killed).Round(time.Second), event.NotifyID)
	o.Close()
	recoverMutationFixture(t, c, base, original, victim, true)
	restarted = true
}
