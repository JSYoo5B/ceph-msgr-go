package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephConfigStreamIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal("authenticate independent config administrator", err)
	}
	t.Cleanup(func() { admin.Close() })
	saved := captureConfigFixtureSettings(t, ctx, admin)
	t.Cleanup(func() { restoreConfigFixtureSettings(t, saved) })
	options := integrationOptions(t)
	options.Monitors = options.Monitors[:1]
	options.MaxInFlight = 1
	options.ConnectTimeout = 2 * time.Second
	options.KeepaliveInterval, options.KeepaliveTimeout = 200*time.Millisecond, 2*time.Second
	dial := options.DialContext
	var monDials, mgrDials atomic.Uint32
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err == nil {
			_, port, _ := net.SplitHostPort(endpoint)
			if port == "36800" || port == "36801" {
				mgrDials.Add(1)
			} else {
				monDials.Add(1)
			}
		}
		return conn, err
	}
	seedFault := installSeedPathFault(&options)
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate effective-config subject", err)
	}
	t.Cleanup(func() { c.Close() })
	seedFault.cleanupAfterClientClose(t, c)
	initial := c.Snapshot()
	checkIdentity := func(state cephmsgr.State) {
		t.Helper()
		if state.FSID == "" || state.FSID != initial.FSID || state.GlobalID == 0 || state.GlobalID != initial.GlobalID || state.AuthRejection != nil || state.Manager.Ready || mgrDials.Load() != 0 {
			t.Fatal("config/log watches changed identity or opened a MGR connection", state, mgrDials.Load())
		}
	}
	checkIdentity(initial)
	held, stopHeld := context.WithCancel(ctx)
	gate := &configSlotContext{Context: held, entered: make(chan struct{}), release: make(chan struct{})}
	finished := make(chan error, 1)
	heldDone := make(chan struct{})
	go func() {
		defer close(heldDone)
		_, err := c.MonCommand(gate, cephmsgr.Command{JSON: []byte(`{"prefix":"status","format":"json"}`)})
		finished <- err
	}()
	t.Cleanup(func() {
		stopHeld()
		gate.unblock()
		joined, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		select {
		case <-heldDone:
		case <-joined.Done():
			t.Error("held command did not join during cleanup")
		}
	})
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("read did not reserve the sole command slot", ctx.Err())
	}
	stream, err := c.WatchConfig(ctx, cephmsgr.ConfigOptions{})
	if err != nil {
		t.Fatal("register config watch while the command slot is held", err)
	}
	t.Cleanup(func() { stream.Close() })
	logs, err := c.WatchLogs(ctx, cephmsgr.LogOptions{StartVersion: 1})
	if err != nil {
		t.Fatal("register log watch alongside config", err)
	}
	t.Cleanup(func() { logs.Close() })
	baseline, err := stream.Next(ctx)
	if err != nil {
		t.Fatal("receive native full config while the command slot is held", err)
	}
	verifyNativeConfig(t, ctx, control, "client.test", "initial", baseline)
	if _, err := c.WatchConfig(ctx, cephmsgr.ConfigOptions{}); !errors.Is(err, cephmsgr.ErrConfigWatchActive) {
		t.Fatal("second config watch acquired the active slot", err)
	}
	stopHeld()
	gate.unblock()
	select {
	case err := <-finished:
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.Is(err, context.Canceled) || errors.As(err, &unknown) {
			t.Fatal("unsent held read lost its known cancellation", err)
		}
	case <-ctx.Done():
		t.Fatal("held read did not release its command slot", ctx.Err())
	}
	// Flush registration's duplicate full responses before reopening. Config
	// has no generation ID, so this proves behavior on a stable session rather
	// than claiming that every possible late prior response is distinguishable.
	if err := fixtureRecoveryRead(ctx, c, false); err != nil {
		t.Fatal(err)
	}
	drainConfigUpdates(t, ctx, stream)
	beforeReopen, beforeDials := c.Snapshot(), monDials.Load()
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	stream, err = c.WatchConfig(ctx, cephmsgr.ConfigOptions{})
	if err != nil {
		t.Fatal("reopen config watch", err)
	}
	reopened, err := stream.Next(ctx)
	if err != nil {
		t.Fatal("reopened watch did not receive an unchanged full config", err)
	}
	afterReopen := c.Snapshot()
	if beforeDials != monDials.Load() || beforeReopen.AuthTicket.Expires != afterReopen.AuthTicket.Expires || beforeReopen.Monitor.Endpoint != afterReopen.Monitor.Endpoint || !reflect.DeepEqual(configFixtureSubset(baseline), configFixtureSubset(reopened)) {
		t.Fatal("reopen did not remain on the same admitted MON session", beforeDials, monDials.Load())
	}
	verifyNativeConfig(t, ctx, control, "client.test", "reopen", reopened)
	localWait, stopLocal := context.WithCancel(ctx)
	stopLocal()
	if _, err := stream.Next(localWait); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled Next consumed or ended the watch", err)
	}
	observe := func(label string, want map[string]string) map[string]string {
		t.Helper()
		got := waitEffectiveConfig(t, ctx, stream, want)
		verifyNativeConfig(t, ctx, control, "client.test", label, got)
		checkIdentity(c.Snapshot())
		return got
	}
	set := func(who, key, value string, force bool) {
		t.Helper()
		configMutationOnce(t, ctx, admin, map[string]any{"prefix": "config set", "who": who, "name": key, "value": value, "force": force})
	}
	remove := func(who, key string) {
		t.Helper()
		configMutationOnce(t, ctx, admin, map[string]any{"prefix": "config rm", "who": who, "name": key})
	}
	// Preserve any original settings for cleanup, then isolate these scopes so
	// an existing exact override cannot hide the hierarchy exercised below.
	for _, entry := range saved {
		if entry.present {
			remove(entry.who, entry.key)
		}
	}
	if len(configFixtureSubset(baseline)) != 0 {
		observe("reset", map[string]string{})
	}
	set("global", configFixtureOption, "31", false)
	observe("global", map[string]string{configFixtureOption: "31"})
	set("client", configFixtureOption, "32", false)
	observe("client", map[string]string{configFixtureOption: "32"})
	set("client.test", configFixtureOption, "33", false)
	observe("exact", map[string]string{configFixtureOption: "33"})
	// CephX requires a nonempty MON cap for its initial ticket. This account
	// permits only fsid, with no general MON read cap. readonly's allow-r cap
	// would not prove that config/monmap subscriptions need no read cap.
	restrictedOptions := integrationOptions(t)
	credential, err := os.ReadFile(filepath.Join(control, "configwatch.key"))
	if err != nil {
		t.Fatal("read fixture-only restricted MON credential", err)
	}
	restrictedOptions.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(credential)))
	if err != nil {
		t.Fatal("parse restricted MON credential", err)
	}
	restrictedOptions.Identity = "client.configwatch"
	restricted, err := cephmsgr.Dial(ctx, restrictedOptions)
	if err != nil {
		t.Fatal("authenticate client with one exact MON command capability", err)
	}
	t.Cleanup(func() { restricted.Close() })
	viewer, err := restricted.WatchConfig(ctx, cephmsgr.ConfigOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { viewer.Close() })
	view := waitEffectiveConfig(t, ctx, viewer, map[string]string{configFixtureOption: "32"})
	verifyNativeConfig(t, ctx, control, "client.configwatch", "no-read", view)
	result, err := restricted.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"fsid","format":"json"}`)})
	var identity struct{ FSID string }
	if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &identity) != nil || identity.FSID != initial.FSID || identity.FSID != restricted.Snapshot().FSID || identity.FSID != restrictedOptions.ExpectedFSID {
		t.Fatal("restricted account did not exercise its one exact MON command", result.Code, err)
	}
	for _, command := range []string{`{"prefix":"status","format":"json"}`, `{"prefix":"config get","who":"client.configwatch","format":"json"}`} {
		result, err := restricted.MonCommand(ctx, cephmsgr.Command{JSON: []byte(command)})
		var server *cephmsgr.CommandError
		var auth *cephmsgr.AuthenticationError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &server) || errors.As(err, &auth) || errors.As(err, &unknown) || result.Code != -13 || !restricted.Snapshot().Monitor.Ready || restricted.Snapshot().AuthRejection != nil || restricted.Snapshot().Manager.Ready {
			t.Fatal("restricted account was not independently denied a general MON read command", result.Code, err)
		}
	}
	set("client", configFixtureOption, "34", false)
	view = waitEffectiveConfig(t, ctx, viewer, map[string]string{configFixtureOption: "34"})
	verifyNativeConfig(t, ctx, control, "client.configwatch", "no-read-update", view)
	viewer.Close()
	restricted.Close()
	remove("client.test", configFixtureOption)
	observe("fallback-client", map[string]string{configFixtureOption: "34"})
	remove("client", configFixtureOption)
	observe("fallback-global", map[string]string{configFixtureOption: "31"})
	remove("global", configFixtureOption)
	observe("removed", map[string]string{})
	raw := "  raw\tline\n雪 \"quote\"\x00 end  "
	set("client.test", configFixtureRaw, raw, true)
	got := observe("raw", map[string]string{configFixtureRaw: raw})
	got[configFixtureRaw] = "caller owns the returned map"
	set("client.test", configFixtureOption, "41", false)
	set("client.test", configFixtureOption, "42", false)
	// A later full map includes both the unchanged raw option and new numeric
	// value, rather than applying a delta to caller-owned storage.
	observe("replacement", map[string]string{configFixtureOption: "42", configFixtureRaw: raw})
	text := fmt.Sprintf("ceph-msgr-config-log-%d", time.Now().UnixNano())
	publishLogOnce(t, ctx, c, text)
	batch, entry := waitLogEntry(t, ctx, logs, text)
	if batch.FSID != initial.FSID || entry.NameID != "test" {
		t.Fatal("config watch disrupted cluster log delivery")
	}
	stream.Close()
	overflow, err := c.WatchConfig(ctx, cephmsgr.ConfigOptions{MaxBufferedBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { overflow.Close() })
	waitEffectiveConfig(t, ctx, overflow, map[string]string{configFixtureOption: "42", configFixtureRaw: raw})
	set("client.test", configFixtureRaw, strings.Repeat("x", 8<<10), true)
	for {
		_, err := overflow.Next(ctx)
		if errors.Is(err, cephmsgr.ErrConfigOverflow) {
			break
		}
		if err != nil {
			t.Fatal("bounded native config overflow", err)
		}
	}
	if _, err := overflow.Next(ctx); !errors.Is(err, cephmsgr.ErrConfigOverflow) {
		t.Fatal("config overflow cause was unstable", err)
	}
	overflow.Close()
	if err := fixtureRecoveryRead(ctx, c, false); err != nil {
		t.Fatal("config overflow damaged the shared MON", err)
	}
	set("client.test", configFixtureRaw, raw, true)
	stream, err = c.WatchConfig(ctx, cephmsgr.ConfigOptions{})
	if err != nil {
		t.Fatal("reuse overflowed config watch slot", err)
	}
	observe("after-overflow", map[string]string{configFixtureOption: "42", configFixtureRaw: raw})
	previous := c.Snapshot()
	for cycle := 1; cycle <= 2; cycle++ {
		drainConfigUpdates(t, ctx, stream)
		deadline := previous.AuthTicket.Expires
		if previous.MgrTicket.Expires.Before(deadline) {
			deadline = previous.MgrTicket.Expires
		}
		renewal, stop := context.WithDeadline(ctx, deadline)
		state := waitClientState(t, c, renewal, func(state cephmsgr.State) bool {
			checkIdentity(state)
			return state.Monitor.Ready && state.AuthTicket.Expires.After(previous.AuthTicket.Expires) && state.MgrTicket.Expires.After(previous.MgrTicket.Expires)
		})
		stop()
		if !time.Now().Before(deadline) {
			t.Fatal("config recovery crossed the previous ticket expiry")
		}
		observe(fmt.Sprintf("renewal-%d", cycle), map[string]string{configFixtureOption: "42", configFixtureRaw: raw})
		previous = state
	}
	_, port, err := net.SplitHostPort(previous.Monitor.Endpoint)
	if err != nil || port != "33300" {
		t.Fatal("config subject did not hold its sole MON a seed", previous.Monitor.Endpoint, err)
	}
	drainConfigUpdates(t, ctx, stream)
	if err := seedFault.interrupt(); err != nil {
		t.Fatal("interrupt only the subject's MON-a TCP path", err)
	}
	recovered := waitClientState(t, c, ctx, func(state cephmsgr.State) bool {
		checkIdentity(state)
		_, port, err := net.SplitHostPort(state.Monitor.Endpoint)
		return state.Monitor.Ready && err == nil && (port == "33301" || port == "33302")
	})
	observe("learned", map[string]string{configFixtureOption: "42", configFixtureRaw: raw})
	remove("client.test", configFixtureRaw)
	observe("final-deletion", map[string]string{configFixtureOption: "42"})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for accepted := 0; ; accepted++ {
		_, err := stream.Next(ctx)
		if errors.Is(err, cephmsgr.ErrClosed) {
			break
		}
		if err != nil || accepted >= 1 {
			t.Fatal("Close did not drain at most one accepted full config", err)
		}
	}
	stream.Close()
	for range 2 {
		if _, err := stream.Next(ctx); !errors.Is(err, cephmsgr.ErrClosed) {
			t.Fatal("Client.Close config cause was overwritten", err)
		}
		if _, err := overflow.Next(ctx); !errors.Is(err, cephmsgr.ErrConfigOverflow) {
			t.Fatal("Client.Close overwrote prior config overflow", err)
		}
	}
	if _, err := c.WatchConfig(ctx, cephmsgr.ConfigOptions{}); !errors.Is(err, cephmsgr.ErrClosed) {
		t.Fatal("closed client admitted another config watch", err)
	}
	checkIdentity(c.Snapshot())
	stats := seedFault.assertHeld(t)
	t.Logf("native full-config oracle, hierarchy/deletion/raw strings, delivery without general MON read caps, held command slot, log coexistence, same-session reopen, overflow, two renewals and client-only seed recovery %q -> %q passed; blocked seed dials=%d; MGR dials=%d", initial.Monitor.Endpoint, recovered.Monitor.Endpoint, stats.Rejected, mgrDials.Load())
}
