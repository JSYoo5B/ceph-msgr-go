package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// MapVersion's newest committed version equals the map epoch the native
// client's MON dump commands report, for the OSD, MON and filesystem maps.
func TestCephMonMapVersionIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, _, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	for _, tc := range []struct {
		kind cephmsgr.MapKind
		key  string
	}{{cephmsgr.OSDMapKind, "osdmap"}, {cephmsgr.MonMapKind, "monmap"}, {cephmsgr.FSMapKind, "fsmap"}, {cephmsgr.MDSMapKind, "fsmap"}} {
		deadline := time.Now().Add(60 * time.Second)
		for {
			got, err := c.MapVersion(ctx, tc.kind)
			if err != nil {
				t.Fatal(tc.kind, err)
			}
			native := nativePool(t, ctx, "versions", map[string]any{}).Epochs[tc.key]
			if got.Oldest == 0 || got.Oldest > got.Newest {
				t.Fatal("implausible range", tc.kind, got)
			}
			if got.Newest == native {
				t.Logf("%s: newest %d (native %d), oldest %d", tc.kind, got.Newest, native, got.Oldest)
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: Go newest %d never matched native epoch %d", tc.kind, got.Newest, native)
			}
			time.Sleep(time.Second)
		}
	}
	// Peons answer MMonGetVersion from their own committed state; every
	// monitor converges on the native epoch.
	peons := 0
	for _, pinned := range pinnedMonitorClients(t, ctx, c) {
		if !pinned.Leader {
			peons++
		}
		native := nativePool(t, ctx, "versions", map[string]any{}).Epochs["osdmap"]
		deadline := time.Now().Add(30 * time.Second)
		for {
			got, err := pinned.MapVersion(ctx, cephmsgr.OSDMapKind)
			if err != nil {
				t.Fatal(pinned.Seed, err)
			}
			if got.Newest == native {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: newest %d never matched native %d", pinned.Seed, got.Newest, native)
			}
			time.Sleep(time.Second)
		}
	}
	if peons == 0 {
		t.Fatal("no monitor was a peon")
	}
}

// MMonGetVersion and MStatfs sent to a peon while the leader is stopped are
// answered once the remaining monitors re-elect; the stopped leader is
// restarted and the election settled on every path.
func TestCephMonMapVersionLeaderStopIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, _, _ := fixture(t)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	requireUsableFixture(t, control)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	// The ordinary helper calls retain the default drain window. This test's
	// subject clients allow one election to finish on their admitted session
	// even when short-ticket renewal replaces it meanwhile; no request retries.
	clients := pinnedMonitorClients(t, ctx, c, leaderStopMonitorOptions)
	var peon *pinnedMonitorClient
	for i := range clients {
		if !clients[i].Leader {
			peon = &clients[i]
			break
		}
	}
	quorum, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"quorum_status","format":"json"}`)})
	var status struct {
		Leader string `json:"quorum_leader_name"`
	}
	if err != nil || json.Unmarshal(quorum.Data, &status) != nil || peon == nil {
		t.Fatal("quorum status or peon", err)
	}
	subjectBefore := peon.Snapshot()
	t.Logf("leader-stop subject seed=%s endpoint=%s GID=%d setup/drain budget=%s; operation context remains 240s",
		peon.Seed, subjectBefore.Monitor.Endpoint, subjectBefore.GlobalID, leaderStopMONDrainBudget)
	monState := c.Snapshot().Monitor
	if err := testcluster.ControlDaemon(ctx, control, "stop", "mon", status.Leader); err != nil {
		t.Fatal("stop leader", err)
	}
	stopped := true
	defer func() {
		if !stopped {
			return
		}
		restore, stop := context.WithTimeout(context.Background(), 180*time.Second)
		defer stop()
		if err := testcluster.ControlDaemon(restore, control, "start", "mon", status.Leader); err != nil {
			markFixtureUnusable(t, control, "stopped leader MON did not restart")
			return
		}
		if _, err := settleNativeMonitorElection(restore, c, control, "restarted", monState, monState.Members); err != nil {
			markFixtureUnusable(t, control, "election did not settle after the leader restart")
		}
	}()
	started := time.Now()
	version, err := peon.MapVersion(ctx, cephmsgr.OSDMapKind)
	assertLeaderStopClientIdentity(t, peon.Client, subjectBefore, "map version during election")
	if err != nil || version.Newest == 0 {
		t.Fatal("map version through a peon during the election", err)
	}
	stat, err := peon.Statfs(ctx, nil)
	assertLeaderStopClientIdentity(t, peon.Client, subjectBefore, "statfs during election")
	if err != nil || stat.KB == 0 {
		t.Fatal("statfs through a peon during the election", err)
	}
	t.Logf("map version %d and statfs answered %s after the leader stopped (peon lease still valid)", version.Newest, time.Since(started).Round(time.Millisecond))
	// Past the peon's lease the requests wait for the re-election.
	time.Sleep(6 * time.Second)
	waited := time.Now()
	v, err := peon.MapVersion(ctx, cephmsgr.OSDMapKind)
	assertLeaderStopClientIdentity(t, peon.Client, subjectBefore, "map version after lease")
	if err != nil || v.Newest < version.Newest {
		t.Fatal("map version through a peon after its lease", err)
	}
	st, err := peon.Statfs(ctx, nil)
	assertLeaderStopClientIdentity(t, peon.Client, subjectBefore, "statfs after lease")
	if err != nil || st.KB == 0 {
		t.Fatal("statfs through a peon after its lease", err)
	}
	t.Logf("after the lease: answered in %s", time.Since(waited).Round(time.Millisecond))
	if err := testcluster.ControlDaemon(ctx, control, "start", "mon", status.Leader); err != nil {
		t.Fatal("restart leader", err)
	}
	stopped = false
	waitNativeMonitorElection(t, ctx, c, control, "restarted", monState, monState.Members)
	native := nativePool(t, ctx, "versions", map[string]any{}).Epochs["osdmap"]
	if again, err := peon.MapVersion(ctx, cephmsgr.OSDMapKind); err != nil || again.Newest < version.Newest || again.Newest != native {
		t.Fatal("map version after the restart", again, native, err)
	}
	assertLeaderStopClientIdentity(t, peon.Client, subjectBefore, "map version after restart")
}
