package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

func verifyNativeHostnameConfig(t *testing.T, ctx context.Context, control, label string, want map[string]string) {
	t.Helper()
	probe, stop := context.WithTimeout(ctx, 35*time.Second)
	defer stop()
	if err := testcluster.ControlDaemon(probe, control, "verify", "hostname-config", label); err != nil {
		t.Fatal("fresh native hostname/config oracle", label, err)
	}
	data, err := os.ReadFile(filepath.Join(control, "hostname-config-oracle.json"))
	var oracle struct {
		RequestID uint64            `json:"request_id"`
		Label     string            `json:"label"`
		Values    map[string]string `json:"rows"`
	}
	if err != nil || len(data) > 4096 || json.Unmarshal(data, &oracle) != nil || oracle.RequestID == 0 || oracle.Label != label || !reflect.DeepEqual(oracle.Values, want) {
		t.Fatal("native NODE_NAME-selected config differs from received wire values", label, err)
	}
}

func TestCephHostnameConfigIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	admin, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	saved := captureConfigFixtureSettings(t, ctx, admin)
	// Empty topology buckets are server-side config-selector fixtures only.
	// No OSD daemon, OSDMap subscription or Go CRUSH placement is introduced.
	t.Cleanup(func() {
		restoreConfigFixtureSettings(t, saved)
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		for _, name := range []string{"client.test/root:host-root-a", "client.test/root:host-root-b", "client.test/host:fixture-host-a"} {
			command, _ := newCommand("config rm", map[string]any{"who": name, "name": configFixtureOption})
			if _, err := admin.MonCommand(cleanup, command); err != nil {
				t.Error("remove fixture config selector once", name, err)
			}
		}
		for _, name := range []string{"fixture-host-a", "fixture-host-b", "host-root-a", "host-root-b"} {
			command, _ := newCommand("osd crush rm", map[string]any{"name": name})
			if _, err := admin.MonCommand(cleanup, command); err != nil {
				t.Error("remove empty fixture bucket once", name, err)
			}
		}
	})
	for _, suffix := range []string{"a", "b"} {
		configMutationOnce(t, ctx, admin, map[string]any{"prefix": "osd crush add-bucket", "name": "host-root-" + suffix, "type": "root"})
		configMutationOnce(t, ctx, admin, map[string]any{"prefix": "osd crush add-bucket", "name": "fixture-host-" + suffix, "type": "host"})
		configMutationOnce(t, ctx, admin, map[string]any{"prefix": "osd crush move", "name": "fixture-host-" + suffix, "args": []string{"root=host-root-" + suffix}})
	}
	set := func(who, value string) {
		t.Helper()
		configMutationOnce(t, ctx, admin, map[string]any{"prefix": "config set", "who": who, "name": configFixtureOption, "value": value})
	}
	set("client.test", "40")
	set("client.test/root:host-root-a", "41")
	set("client.test/root:host-root-b", "42")
	// Tentacle selects the hostname bucket's ancestors, not the bucket itself.
	set("client.test/host:fixture-host-a", "61")

	type viewer struct {
		client *cephmsgr.Client
		stream *cephmsgr.ConfigStream
	}
	viewers := make(map[string]viewer)
	want := map[string]string{"fixture-host-a": "41", "fixture-host-b": "42", "": "40", " raw-hostname ": "40"}
	var subject *cephmsgr.Client
	var fault *seedPathFault
	var managerDials atomic.Uint32
	for _, hostname := range []string{"fixture-host-a", "fixture-host-b", "", " raw-hostname "} {
		options := integrationOptions(t)
		options.Hostname = hostname
		if hostname == "fixture-host-a" {
			options.Monitors = options.Monitors[:1]
			options.MaxInFlight = 1
			options.ConnectTimeout = 2 * time.Second
			options.KeepaliveInterval, options.KeepaliveTimeout = 200*time.Millisecond, 2*time.Second
			dial := options.DialContext
			options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
				_, port, _ := net.SplitHostPort(endpoint)
				if port == "36800" || port == "36801" {
					managerDials.Add(1)
				}
				return dial(ctx, network, endpoint)
			}
			fault = installSeedPathFault(&options)
		}
		c, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal("authenticate hostname viewer", hostname, err)
		}
		t.Cleanup(func() { c.Close() })
		if hostname == "fixture-host-a" {
			subject = c
			fault.cleanupAfterClientClose(t, c)
			options.Hostname = "fixture-host-b" // Caller reassignment cannot retarget this client.
		}
		stream, err := c.WatchConfig(ctx, cephmsgr.ConfigOptions{})
		if err != nil {
			t.Fatal(err)
		}
		viewers[hostname] = viewer{c, stream}
		got := waitEffectiveConfig(t, ctx, stream, map[string]string{configFixtureOption: want[hostname]})
		if got[configFixtureOption] != want[hostname] || c.Snapshot().Manager.Ready {
			t.Fatal("hostname changed wire value or opened a MGR connection", hostname)
		}
	}
	verifyNativeHostnameConfig(t, ctx, control, "initial", want)

	// Hold the sole command slot before transmission while config, log and
	// digest registration proceed independently on the same admitted session.
	held, stopHeld := context.WithCancel(ctx)
	gate := &configSlotContext{Context: held, entered: make(chan struct{}), release: make(chan struct{})}
	finished := make(chan error, 1)
	heldDone := make(chan struct{})
	go func() {
		defer close(heldDone)
		_, err := subject.MonCommand(gate, cephmsgr.Command{JSON: []byte(`{"prefix":"status"}`)})
		finished <- err
	}()
	t.Cleanup(func() {
		stopHeld()
		gate.unblock()
		joined, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		select {
		case <-heldDone:
		case <-joined.Done():
			t.Error("held hostname test command did not join")
		}
	})
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	logs, err := subject.WatchLogs(ctx, cephmsgr.LogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { logs.Close() })
	digest, err := subject.WatchDigest(ctx, cephmsgr.DigestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { digest.Close() })
	if _, err := digest.Next(ctx); err != nil {
		t.Fatal("digest did not coexist with hostname-scoped config", err)
	}
	set("client.test/root:host-root-a", "43")
	want["fixture-host-a"] = "43"
	waitEffectiveConfig(t, ctx, viewers["fixture-host-a"].stream, map[string]string{configFixtureOption: "43"})
	verifyNativeHostnameConfig(t, ctx, control, "subscriptions", want)
	stopHeld()
	gate.unblock()
	select {
	case err := <-finished:
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.Is(err, context.Canceled) || errors.As(err, &unknown) {
			t.Fatal("held unsent command lost cancellation", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	for hostname, v := range viewers {
		if err := v.stream.Close(); err != nil {
			t.Fatal(err)
		}
		v.stream, err = v.client.WatchConfig(ctx, cephmsgr.ConfigOptions{})
		if err != nil {
			t.Fatal(err)
		}
		viewers[hostname] = v
		waitEffectiveConfig(t, ctx, v.stream, map[string]string{configFixtureOption: want[hostname]})
	}
	verifyNativeHostnameConfig(t, ctx, control, "reopen", want)
	previous := subject.Snapshot()
	renewal, stopRenewal := context.WithDeadline(ctx, previous.AuthTicket.Expires)
	waitClientState(t, subject, renewal, func(s cephmsgr.State) bool {
		return s.Monitor.Ready && s.AuthTicket.Expires.After(previous.AuthTicket.Expires)
	})
	stopRenewal()
	set("client.test/root:host-root-a", "44")
	want["fixture-host-a"] = "44"
	waitEffectiveConfig(t, ctx, viewers["fixture-host-a"].stream, map[string]string{configFixtureOption: "44"})
	verifyNativeHostnameConfig(t, ctx, control, "renewal", want)
	before := subject.Snapshot()
	if err := fault.interrupt(); err != nil {
		t.Fatal(err)
	}
	recovered := waitClientState(t, subject, ctx, func(s cephmsgr.State) bool {
		return s.Monitor.Ready && s.Monitor.Endpoint != before.Monitor.Endpoint
	})
	waitEffectiveConfig(t, ctx, viewers["fixture-host-a"].stream, map[string]string{configFixtureOption: "44"})
	set("client.test/root:host-root-a", "45")
	want["fixture-host-a"] = "45"
	waitEffectiveConfig(t, ctx, viewers["fixture-host-a"].stream, map[string]string{configFixtureOption: "45"})
	verifyNativeHostnameConfig(t, ctx, control, "learned", want)
	if subject.Snapshot().GlobalID != before.GlobalID || managerDials.Load() != 0 || recovered.Manager.Ready {
		t.Fatal("hostname recovery changed identity or opened MGR", subject.Snapshot(), managerDials.Load())
	}
	for _, v := range viewers {
		if err := v.client.Close(); err != nil {
			t.Fatal(err)
		}
	}
	fault.assertHeld(t)
	t.Log("same-identity native hostname selectors, empty/raw input, subscriptions, held command slot, reopen, renewal and learned-MON recovery preserved; no MGR/OSD data connections")
}
