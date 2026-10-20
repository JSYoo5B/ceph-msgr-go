package integration_test

import (
	"context"
	"errors"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// A replica of the watched object's PG is SIGKILLed, which starts a new PG
// interval while the primary keeps its role. The watcher's unchanged
// connection gets no DISCONNECT; its next Ping is answered with a known -110
// (ETIMEDOUT, the OSD's request to reconnect) while the native watcher list
// still shows the registration. An explicit RECONNECT on a newer connection
// restores it and Ping succeeds again. Nothing is re-registered automatically.
func TestCephOSDWatchIntervalChangeIntegration(t *testing.T) {
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
	seeded := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("interval change")}})
	prepared = true
	name := seeded.Objects[0]
	place := func() osdFixtureOracle {
		return nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
	}
	route := place()
	if len(route.Acting) != 3 {
		t.Fatalf("expected a three-OSD acting set: %+v", route)
	}
	target := cephmsgr.OSDTarget{ID: route.ID, Address: route.Address}
	o, err := c.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	watch, _, err := o.WatchObject(ctx, osdObjectRequest(route), cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
	if err != nil {
		t.Fatal("watch", err)
	}
	if reply, err := watch.Ping(ctx, route.Epoch); err != nil || reply.Code != 0 {
		t.Fatal("ping before the interval change", reply.Code, err)
	}
	var replica int32 = -1
	for _, id := range route.Acting {
		if id != route.ID {
			replica = id
			break
		}
	}
	victim := strconv.Itoa(int(replica))
	if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", victim); err != nil {
		t.Fatal("kill replica", err)
	}
	killed := time.Now()
	restarted := false
	defer func() {
		if !restarted {
			o.Close()
			recoverMutationFixture(t, c, base, original, victim, true)
		}
	}()
	moved := place()
	for deadline := killed.Add(60 * time.Second); slices.Contains(moved.Acting, replica); moved = place() {
		if time.Now().After(deadline) {
			t.Fatal("the killed replica never left the acting set")
		}
		time.Sleep(time.Second)
	}
	if moved.ID != route.ID {
		t.Fatalf("the primary changed (osd.%d -> osd.%d); this test needs it kept", route.ID, moved.ID)
	}
	t.Logf("acting %v -> %v (epoch %d -> %d), %s after the kill", route.Acting, moved.Acting, route.Epoch, moved.Epoch, time.Since(killed).Round(time.Second))

	wait, stop := context.WithTimeout(ctx, 5*time.Second)
	event, err := watch.Next(wait)
	stop()
	switch {
	case err == nil:
		t.Logf("event after the interval change: opcode %d cookie %d", event.Opcode, event.Cookie)
	case wait.Err() != nil:
		t.Log("no event within 5 s of the interval change")
	default:
		t.Logf("the watch handle ended after the interval change: %v", err)
	}
	ping, err := watch.Ping(ctx, moved.Epoch)
	var server *cephmsgr.OSDError
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != -110 || ping.Code != -110 {
		t.Fatal("ping after the interval change must be a known -110 (ETIMEDOUT)", ping.Code, err)
	}
	// The registration is part of the object's state and stays listed.
	if listed, err := watchReconnectList(ctx, name); err != nil || len(listed.Entries) != 1 || listed.Entries[0].Cookie != watch.Cookie() {
		t.Fatal("native watcher list after the interval change differs", listed, err)
	}

	o.Close()
	n, err := c.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	current, reply, err := n.ReconnectWatch(ctx, watch, osdObjectRequest(moved))
	if current != nil {
		defer current.Close()
	}
	if err != nil || reply.Code != 0 {
		t.Fatal("RECONNECT on a newer connection after the interval change", reply.Code, err)
	}
	if listed, err := watchReconnectList(ctx, name); err != nil || len(listed.Entries) != 1 || listed.Entries[0].Cookie != watch.Cookie() {
		t.Fatal("native watcher list after RECONNECT differs", listed, err)
	}
	if reply, err := current.Ping(ctx, moved.Epoch); err != nil || reply.Code != 0 {
		t.Fatal("ping after RECONNECT", reply.Code, err)
	}
	if _, err := current.Unwatch(ctx, moved.Epoch); err != nil {
		t.Fatal("unwatch", err)
	}
	n.Close()
	recoverMutationFixture(t, c, base, original, victim, true)
	restarted = true
}
