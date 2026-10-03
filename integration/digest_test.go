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

func digestObjects(t *testing.T, digest cephmsgr.ClusterDigest) (map[string]any, map[string]any) {
	t.Helper()
	var status, health map[string]any
	if json.Unmarshal(digest.MonStatus, &status) != nil || json.Unmarshal(digest.Health, &health) != nil || status["name"] == nil || health["status"] == nil {
		t.Fatal("digest did not retain native MON-status/health-detail JSON", string(digest.MonStatus), string(digest.Health))
	}
	return status, health
}

func waitDigest(t *testing.T, ctx context.Context, stream *cephmsgr.DigestStream, matches func(map[string]any, map[string]any) bool) cephmsgr.ClusterDigest {
	t.Helper()
	for {
		digest, err := stream.Next(ctx)
		if err != nil {
			t.Fatal("receive subscribed digest", err)
		}
		status, health := digestObjects(t, digest)
		if matches(status, health) {
			return digest
		}
	}
}

func digestMute(health map[string]any, code string) bool {
	mutes, _ := health["mutes"].([]any)
	for _, value := range mutes {
		mute, _ := value.(map[string]any)
		if mute["code"] == code {
			return true
		}
	}
	return false
}

func digestFixtureMonitorName(t *testing.T, endpoint string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(endpoint)
	name := map[string]string{"33300": "a", "33301": "b", "33302": "c"}[port]
	if err != nil || name == "" {
		t.Fatal("digest fixture MON endpoint", endpoint, err)
	}
	return name
}

func verifyNativeDigest(t *testing.T, ctx context.Context, control, label string, digest cephmsgr.ClusterDigest) {
	t.Helper()
	status, health := digestObjects(t, digest)
	name := status["name"].(string)
	if err := testcluster.ControlDaemon(ctx, control, "verify", "digest", name+" "+label); err != nil {
		t.Fatal("fresh independent native digest oracle", err)
	}
	data, err := os.ReadFile(filepath.Join(control, "digest-oracle.json"))
	var oracle struct {
		Name, Label string
		Health      map[string]any `json:"health"`
		MonStatus   map[string]any `json:"mon_status"`
	}
	if err != nil || json.Unmarshal(data, &oracle) != nil || oracle.Name != name || oracle.Label != label {
		t.Fatal("native digest metadata", err)
	}
	// Native CLI opens independent sessions, and both reads happen later than
	// the push. Remove only elapsed-time and live peer-count fields from the
	// comparison; the product preserves every original byte and field.
	for _, key := range []string{"uptime", "quorum_age", "feature_map"} {
		delete(status, key)
		delete(oracle.MonStatus, key)
	}
	if !reflect.DeepEqual(status, oracle.MonStatus) || !reflect.DeepEqual(health, oracle.Health) {
		t.Fatal("digest differs from fresh native health detail/named MON status", status, oracle.MonStatus, health, oracle.Health)
	}
	t.Logf("%s: MON %s status and full health detail match independent native clients", label, name)
}

func TestCephDigestIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.Monitors = options.Monitors[:1]
	options.MaxInFlight = 1
	options.ConnectTimeout = 2 * time.Second
	options.KeepaliveInterval, options.KeepaliveTimeout = 200*time.Millisecond, 2*time.Second
	dial := options.DialContext
	var managerDials atomic.Uint32
	options.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		_, port, _ := net.SplitHostPort(address)
		if port == "36800" || port == "36801" {
			managerDials.Add(1)
		}
		return dial(ctx, network, address)
	}
	seedFault := installSeedPathFault(&options)
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	seedFault.cleanupAfterClientClose(t, c)
	initial := c.Snapshot()
	initialName := digestFixtureMonitorName(t, initial.Monitor.Endpoint)
	held, stopHeld := context.WithCancel(ctx)
	gate := &configSlotContext{Context: held, entered: make(chan struct{}), release: make(chan struct{})}
	finished := make(chan error, 1)
	go func() {
		_, err := c.MonCommand(gate, cephmsgr.Command{JSON: []byte(`{"prefix":"status","format":"json"}`)})
		finished <- err
	}()
	t.Cleanup(func() { stopHeld(); gate.unblock() })
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("command did not occupy the sole slot", ctx.Err())
	}
	stream, err := c.WatchDigest(ctx, cephmsgr.DigestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Close() })
	baseline := waitDigest(t, ctx, stream, func(status, health map[string]any) bool { return status["name"] == initialName })
	verifyNativeDigest(t, ctx, control, "initial", baseline)
	if _, err := c.WatchDigest(ctx, cephmsgr.DigestOptions{}); !errors.Is(err, cephmsgr.ErrDigestWatchActive) {
		t.Fatal("second digest watch acquired the slot", err)
	}
	stopHeld()
	gate.unblock()
	select {
	case err := <-finished:
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.Is(err, context.Canceled) || errors.As(err, &unknown) {
			t.Fatal("unsent held command cancellation", err)
		}
	case <-ctx.Done():
		t.Fatal("held command did not finish", ctx.Err())
	}
	local, stopLocal := context.WithCancel(ctx)
	stopLocal()
	if _, err := stream.Next(local); !errors.Is(err, context.Canceled) {
		t.Fatal("local Next cancellation", err)
	}
	command := func(prefix string, args map[string]any) {
		t.Helper()
		cmd, err := cephmsgr.NewCommand(prefix, args)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.MonCommand(ctx, cmd); err != nil {
			t.Fatal(prefix, err)
		}
	}
	// Sticky arbitrary-code mutes are supported even without a raised check.
	// The sentinel belongs only to the disposable fixture, and every mutation
	// is sent once. Observation never retries a mutation after an unknown result.
	const sentinel = "CEPH_MSGR_DIGEST_FIXTURE"
	cleanupMute := false
	t.Cleanup(func() {
		if !cleanupMute {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		admin, err := cephmsgr.Dial(cleanup, integrationOptions(t))
		if err != nil {
			t.Error("cleanup digest fixture mute", err)
			return
		}
		defer admin.Close()
		cmd, _ := cephmsgr.NewCommand("health unmute", map[string]any{"code": sentinel})
		if _, err := admin.MonCommand(cleanup, cmd); err != nil {
			t.Error("cleanup digest fixture mute", err)
		}
	})
	command("health mute", map[string]any{"code": sentinel, "sticky": true})
	cleanupMute = true
	muted := waitDigest(t, ctx, stream, func(status, health map[string]any) bool { return digestMute(health, sentinel) })
	verifyNativeDigest(t, ctx, control, "muted", muted)
	cleanupMute = false // Do not replay an uncertain unmute during cleanup.
	command("health unmute", map[string]any{"code": sentinel})
	unmuted := waitDigest(t, ctx, stream, func(status, health map[string]any) bool { return !digestMute(health, sentinel) })
	verifyNativeDigest(t, ctx, control, "unmuted", unmuted)
	previous := c.Snapshot()
	beforeExpiry, stopExpiry := context.WithDeadline(ctx, previous.AuthTicket.Expires)
	renewed := waitClientState(t, c, beforeExpiry, func(state cephmsgr.State) bool {
		return state.Monitor.Ready && state.AuthTicket.Expires.After(previous.AuthTicket.Expires)
	})
	stopExpiry()
	if renewed.GlobalID != initial.GlobalID || renewed.FSID != initial.FSID {
		t.Fatal("digest ticket renewal changed identity", renewed)
	}
	// Discard values accepted before renewal; the next delivery must arrive
	// after this boundary, rather than proving recovery with an old unread pair.
	quiet, stopQuiet := context.WithTimeout(ctx, 20*time.Millisecond)
	for {
		if _, err := stream.Next(quiet); errors.Is(err, context.DeadlineExceeded) {
			break
		} else if err != nil {
			stopQuiet()
			t.Fatal("drain digest before renewed delivery", err)
		}
	}
	stopQuiet()
	renewalName := digestFixtureMonitorName(t, renewed.Monitor.Endpoint)
	verifyNativeDigest(t, ctx, control, "renewal", waitDigest(t, ctx, stream, func(status, health map[string]any) bool { return status["name"] == renewalName }))
	if err := seedFault.interrupt(); err != nil {
		t.Fatal("interrupt sole seed client path", err)
	}
	recovered := waitClientState(t, c, ctx, func(state cephmsgr.State) bool {
		return state.Monitor.Ready && state.Monitor.Endpoint != initial.Monitor.Endpoint
	})
	recoveredName := digestFixtureMonitorName(t, recovered.Monitor.Endpoint)
	learned := waitDigest(t, ctx, stream, func(status, health map[string]any) bool { return status["name"] == recoveredName })
	verifyNativeDigest(t, ctx, control, "learned-mon", learned)
	if recovered.GlobalID != initial.GlobalID || recovered.FSID != initial.FSID || managerDials.Load() != 0 || recovered.Manager.Ready {
		t.Fatal("digest recovery identity/lazy MGR", recovered, managerDials.Load())
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := c.WatchDigest(ctx, cephmsgr.DigestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	verifyNativeDigest(t, ctx, control, "reopen", waitDigest(t, ctx, reopened, func(status, health map[string]any) bool { return status["name"] == recoveredName }))
	if err := fixtureRecoveryRead(ctx, c, false); err != nil {
		t.Fatal("ordinary command after digest reopen", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := reopened.Next(ctx); errors.Is(err, cephmsgr.ErrClosed) {
			break
		} else if err != nil {
			t.Fatal("client Close digest termination", err)
		}
	}
	seedFault.assertHeld(t)
}
