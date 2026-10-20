package integration_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// WATCH is a WR opcode and NOTIFY an RD one (include/rados.h). An identity
// whose OSD capability is only "allow r" is refused WATCH with a known EPERM
// and no registration, while its NOTIFY reaches the admin client's watch and
// completes with that watcher's ACK.
func TestCephOSDWatchPermissionsIntegration(t *testing.T) {
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
	seeded := owned(ctx, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("permissions")}})
	prepared = true
	name := seeded.Objects[0]
	var route osdFixtureOracle
	if modern {
		route = replicatedPGNative(t, ctx, name).Placement
	} else {
		route = nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
	}
	target := cephmsgr.OSDTarget{ID: route.ID, Address: route.Address}

	options := integrationOptions(t)
	encoded, err := os.ReadFile(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "readonly.key"))
	if err != nil {
		t.Fatal(err)
	}
	if options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(encoded))); err != nil {
		t.Fatal(err)
	}
	options.Identity = "client.readonly"
	options.EnableOSD = true
	if modern {
		options.OSDProfile = replicatedPGFixtureOSDProfile(t)
	}
	readonly, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer readonly.Close()
	ro, err := readonly.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()

	refused, reply, err := ro.WatchObject(ctx, requestFor(route), cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
	var server *cephmsgr.OSDError
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != -1 || reply.Code != -1 {
		t.Fatal("a read-only WATCH must be a known EPERM", reply.Code, err)
	}
	if refused != nil {
		refused.Close()
	}
	if listed, err := watchReconnectList(ctx, name); err != nil || len(listed.Entries) != 0 {
		t.Fatal("the refused WATCH left a registration", listed, err)
	}

	admin, err := c.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	watch, _, err := admin.WatchObject(ctx, requestFor(route), cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
	if err != nil {
		t.Fatal("admin watch", err)
	}
	pending, _, err := ro.NotifyObject(ctx, requestFor(route), []byte("ro"), cephmsgr.OSDWatchOptions{TimeoutSeconds: 10})
	if err != nil {
		t.Fatal("a read-only NOTIFY must be accepted", err)
	}
	defer pending.Close()
	event, err := watch.Next(ctx)
	if err != nil || event.Opcode != cephmsgr.OSDWatchNotification || string(event.Payload) != "ro" || event.NotifierGlobalID != readonly.Snapshot().GlobalID {
		t.Fatal("read-only notification", event, err)
	}
	if _, err := watch.Acknowledge(ctx, route.Epoch, event, []byte("ok")); err != nil {
		t.Fatal("acknowledge", err)
	}
	done, err := pending.Next(ctx)
	if err != nil || done.ReturnCode != 0 || len(done.Replies) != 1 || string(done.Replies[0].Data) != "ok" {
		t.Fatal("read-only notify completion", done, err)
	}
	if _, err := watch.Unwatch(ctx, route.Epoch); err != nil {
		t.Fatal("unwatch", err)
	}
}
