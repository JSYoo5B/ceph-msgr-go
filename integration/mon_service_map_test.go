package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// A native client registers a service daemon (as RGW or rbd-mirror do); the Go
// service map watch receives a map holding it, and the daemon matches the MON's
// own "service dump" for GID, metadata and (at the same epoch) start epoch.
func TestCephMonServiceMapIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, _, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	watch, err := c.WatchServiceMap(ctx, cephmsgr.ServiceMapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	daemon := "inst-" + time.Now().Format("150405")
	// The native registration lives only while the oracle holds its client, so
	// read the Go stream concurrently with that hold.
	nativeDone := make(chan poolReceipt, 1)
	go func() {
		nativeDone <- nativePool(t, ctx, "service", map[string]any{"daemon": daemon, "hold_seconds": 25})
	}()
	var seen *cephmsgr.ServiceMap
	for seen == nil {
		waitCtx, stop := context.WithTimeout(ctx, 30*time.Second)
		m, err := watch.Next(waitCtx)
		stop()
		if err != nil {
			t.Fatal("service map", err)
		}
		var names []string
		for name, svc := range m.Services {
			for dn := range svc.Daemons {
				names = append(names, name+"/"+dn)
			}
		}
		_, found := m.Services["ceph-msgr"].Daemons[daemon]
		t.Logf("service map epoch %d, daemons %v, ours present=%v", m.Epoch, names, found)
		if found {
			seen = &m
		}
	}
	native := <-nativeDone
	var dump struct {
		Epoch    uint32 `json:"epoch"`
		Services map[string]struct {
			Daemons map[string]json.RawMessage `json:"daemons"`
		} `json:"services"`
	}
	if err := json.Unmarshal(native.ServiceDump, &dump); err != nil {
		t.Fatal(err)
	}
	var nativeDaemon struct {
		GID        uint64            `json:"gid"`
		StartEpoch uint32            `json:"start_epoch"`
		Metadata   map[string]string `json:"metadata"`
	}
	raw, ok := dump.Services["ceph-msgr"].Daemons[daemon]
	if !ok || json.Unmarshal(raw, &nativeDaemon) != nil {
		t.Fatalf("native service dump lacks the registered daemon: %s", native.ServiceDump)
	}
	m := *seen
	d := m.Services["ceph-msgr"].Daemons[daemon]
	// The daemon's start epoch moves when its MGR session re-registers, so it
	// is compared only for the same map epoch as the native dump.
	if d.GID != nativeDaemon.GID || m.Epoch == dump.Epoch && d.StartEpoch != nativeDaemon.StartEpoch || d.StartEpoch > m.Epoch ||
		d.Metadata["ceph_msgr_daemon"] != daemon || d.Metadata["ceph_msgr_role"] != nativeDaemon.Metadata["ceph_msgr_role"] || len(m.Raw) == 0 {
		t.Fatalf("Go daemon differs from native dump: go=%+v native=%+v", d, nativeDaemon)
	}
	t.Logf("daemon %s gid %d at Go epoch %d, native dump epoch %d", daemon, d.GID, m.Epoch, dump.Epoch)
}
