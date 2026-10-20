package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// A watch (5-second timeout) whose connection is closed without UNWATCH stays
// registered as disconnected until the OSD's timer fires; the OSD then removes
// it with nowhere to send an event. A RECONNECT sent after that is refused with
// a known -107 (ENOTCONN) and creates no registration; the client does not fall
// back to a new WATCH.
func TestCephOSDWatchExpiredReconnectIntegration(t *testing.T) {
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
	seeded := owned(ctx, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("expired watch")}})
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
	const timeout = 5
	watch, _, err := old.WatchObject(ctx, requestFor(route), cephmsgr.OSDWatchOptions{TimeoutSeconds: timeout})
	if err != nil {
		t.Fatal("watch", err)
	}
	old.Close()
	closed := time.Now()
	if listed, err := watchReconnectList(ctx, name); err != nil || len(listed.Entries) != 1 || listed.Entries[0].Cookie != watch.Cookie() {
		t.Fatal("a closed connection's watch should stay registered until its timeout", listed, err)
	}
	for deadline := closed.Add(30 * time.Second); ; {
		listed, err := watchReconnectList(ctx, name)
		if err == nil && len(listed.Entries) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the disconnected watch was never removed", listed, err)
		}
		time.Sleep(time.Second)
	}
	expired := time.Since(closed)
	if expired < (timeout-1)*time.Second {
		t.Fatalf("the disconnected watch was removed after %s, before its %ds timeout", expired, timeout)
	}

	o, err := c.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	again, reply, err := o.ReconnectWatch(ctx, watch, requestFor(route))
	var server *cephmsgr.OSDError
	var unknown *cephmsgr.OutcomeUnknownError
	// A handle returned with an operation error only owns the receipt.
	if again != nil {
		defer again.Close()
	}
	if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != -107 || reply.Code != -107 {
		t.Fatal("RECONNECT after the timeout must be a known -107 (ENOTCONN)", reply.Code, err)
	}
	if listed, err := watchReconnectList(ctx, name); err != nil || len(listed.Entries) != 0 {
		t.Fatal("the refused RECONNECT left a registration", listed, err)
	}
	t.Logf("disconnected watch removed %s after the close (timeout %ds); late RECONNECT code %d", expired.Round(100*time.Millisecond), timeout, reply.Code)
}
