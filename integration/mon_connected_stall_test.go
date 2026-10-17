package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// The MON a client is connected to freezes (docker pause) after admission.
// A pool-op allocation, a statfs and a map-version request sent to it end as
// unknown outcomes once keepalive gives up on the session; the client moves
// to another MON from its authenticated map (the frozen one was its only
// seed) and answers fresh requests there. Nothing is replayed: while the MON
// is frozen the native pool shows no allocation, and after it resumes the
// client's next allocation follows whatever the native pool then holds (the
// frozen MON may still deliver the request it had buffered).
func TestCephMonPoolOpConnectedStallIntegration(t *testing.T) {
	requireMonFreezeOptIn(t)
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	requireUsableFixture(t, control)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	pool := uint32(pools.ids[1])
	info := func() poolReceipt { return nativePool(t, ctx, "poolinfo", map[string]any{"slot": 1}) }

	_, peon, peonSeed := quorumPeonSeed(t, ctx, c)
	options := integrationOptions(t)
	options.Monitors = []string{peonSeed}
	options.EnableOSD = true
	if isReplicatedPGFixtureProfile() {
		options.OSDProfile = replicatedPGFixtureOSDProfile(t)
	}
	options.KeepaliveInterval, options.KeepaliveTimeout = 200*time.Millisecond, 2*time.Second
	options.ConnectTimeout = 3 * time.Second
	pinned, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	first, err := pinned.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: pool, Op: cephmsgr.SelfManagedSnapshotCreate})
	if err != nil {
		t.Fatal(err)
	}
	baseline, _ := first.SnapshotID()
	frozenEndpoint := pinned.Snapshot().Monitor.Endpoint
	monState := c.Snapshot().Monitor

	if err := testcluster.ControlDaemon(ctx, control, "pause", "mon", peon); err != nil {
		t.Fatal("pause connected MON", err)
	}
	paused, recycled := true, false
	defer func() {
		if recycled {
			return
		}
		restore, stop := context.WithTimeout(context.Background(), 180*time.Second)
		defer stop()
		if paused {
			if err := testcluster.ControlDaemon(restore, control, "resume", "mon", peon); err != nil {
				markFixtureUnusable(t, control, "paused MON did not resume")
				t.Error(err)
				return
			}
		}
		if err := recycleResumedMon(restore, control, peon); err != nil {
			markFixtureUnusable(t, control, "resumed MON did not restart")
			t.Error(err)
			return
		}
		if _, err := settleNativeMonitorElection(restore, c, control, "restarted", monState, monState.Members); err != nil {
			markFixtureUnusable(t, control, "election did not settle after the MON restart")
			t.Error(err)
		}
	}()

	type outcome struct {
		label string
		err   error
	}
	results := make(chan outcome, 3)
	started := time.Now()
	go func() {
		_, err := pinned.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: pool, Op: cephmsgr.SelfManagedSnapshotCreate})
		results <- outcome{"pool op", err}
	}()
	go func() {
		_, err := pinned.Statfs(ctx, nil)
		results <- outcome{"statfs", err}
	}()
	go func() {
		_, err := pinned.MapVersion(ctx, cephmsgr.OSDMapKind)
		results <- outcome{"map version", err}
	}()
	for range 3 {
		r := <-results
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(r.err, &unknown) || ctx.Err() != nil {
			t.Fatalf("%s to a frozen MON must end as an unknown outcome: %v", r.label, r.err)
		}
		t.Logf("%s ended unknown after %s: %v", r.label, time.Since(started).Round(time.Millisecond), r.err)
	}

	// The client recovers to another MON on its own; fresh requests succeed.
	recovered := time.Now()
	version, err := pinned.MapVersion(ctx, cephmsgr.OSDMapKind)
	if err != nil || version.Newest == 0 {
		t.Fatal("map version after the connected MON froze", err)
	}
	if stat, err := pinned.Statfs(ctx, nil); err != nil || stat.KB == 0 {
		t.Fatal("statfs after the connected MON froze", err)
	}
	current := pinned.Snapshot().Monitor.Endpoint
	if current == frozenEndpoint {
		t.Fatal("the client still reports the frozen MON", current)
	}
	t.Logf("recovered %s -> %s; fresh requests answered %s later", frozenEndpoint, current, time.Since(recovered).Round(time.Millisecond))
	if seq := info().SnapSeq; seq != baseline {
		t.Fatalf("native snap_seq %d while the MON holding the request is frozen: the client replayed it (baseline %d)", seq, baseline)
	}

	if err := testcluster.ControlDaemon(ctx, control, "resume", "mon", peon); err != nil {
		t.Fatal("resume MON", err)
	}
	paused = false
	// Leave the resumed MON time to act on whatever it had buffered.
	time.Sleep(5 * time.Second)
	settled := info().SnapSeq
	if settled != baseline && settled != baseline+1 {
		t.Fatalf("native snap_seq %d after the resume: expected %d or %d", settled, baseline, baseline+1)
	}
	if err := recycleResumedMon(ctx, control, peon); err != nil {
		t.Fatal("restart the resumed MON", err)
	}
	recycled = true
	waitNativeMonitorElection(t, ctx, c, control, "restarted", monState, monState.Members)
	if seq := info().SnapSeq; seq != settled {
		t.Fatalf("native snap_seq moved from %d to %d without a request", settled, seq)
	}
	next, err := pinned.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: pool, Op: cephmsgr.SelfManagedSnapshotCreate})
	if err != nil {
		t.Fatal("allocation after the resume", err)
	}
	if id, _ := next.SnapshotID(); id != settled+1 || info().SnapSeq != id {
		t.Fatalf("allocation after the resume: id %d, native %d before", id, settled)
	}
	t.Logf("the frozen MON's buffered allocation executed after the resume: %t (snap_seq %d -> %d)", settled == baseline+1, baseline, settled)
}

// The MON a client is connected to is stopped (docker stop) while requests
// wait on it. The leader is frozen first, so after the peon's lease expires
// the peon holds a pool-op allocation, a statfs and a map-version request
// instead of answering them. Stopping that peon ends all three as unknown
// outcomes at once; after the leader resumes, the client answers fresh
// requests from the remaining quorum, and its next allocation follows the
// native snap_seq whatever happened to the held one.
func TestCephMonPoolOpConnectedStopIntegration(t *testing.T) {
	requireMonFreezeOptIn(t)
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	requireUsableFixture(t, control)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	pool := uint32(pools.ids[1])
	info := func() poolReceipt { return nativePool(t, ctx, "poolinfo", map[string]any{"slot": 1}) }
	leader, peon, peonSeed := quorumPeonSeed(t, ctx, c)
	options := integrationOptions(t)
	options.Monitors = []string{peonSeed}
	options.EnableOSD = true
	if isReplicatedPGFixtureProfile() {
		options.OSDProfile = replicatedPGFixtureOSDProfile(t)
	}
	options.KeepaliveInterval, options.KeepaliveTimeout = 200*time.Millisecond, 2*time.Second
	options.ConnectTimeout = 3 * time.Second
	pinned, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	first, err := pinned.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: pool, Op: cephmsgr.SelfManagedSnapshotCreate})
	if err != nil {
		t.Fatal(err)
	}
	baseline, _ := first.SnapshotID()
	stoppedEndpoint := pinned.Snapshot().Monitor.Endpoint
	monState := c.Snapshot().Monitor

	if err := testcluster.ControlDaemon(ctx, control, "pause", "mon", leader); err != nil {
		t.Fatal("pause leader", err)
	}
	paused, stopped, recycled := true, false, false
	defer func() {
		if recycled && !stopped {
			return
		}
		restore, stop := context.WithTimeout(context.Background(), 180*time.Second)
		defer stop()
		if paused {
			if err := testcluster.ControlDaemon(restore, control, "resume", "mon", leader); err != nil {
				markFixtureUnusable(t, control, "paused leader MON did not resume")
				t.Error(err)
				return
			}
		}
		if stopped {
			if err := testcluster.ControlDaemon(restore, control, "start", "mon", peon); err != nil {
				markFixtureUnusable(t, control, "stopped MON did not restart")
				t.Error(err)
				return
			}
		}
		if !recycled {
			if err := recycleResumedMon(restore, control, leader); err != nil {
				markFixtureUnusable(t, control, "resumed leader MON did not restart")
				t.Error(err)
				return
			}
		}
		if _, err := settleNativeMonitorElection(restore, c, control, "restarted", monState, monState.Members); err != nil {
			markFixtureUnusable(t, control, "election did not settle after the MON restart")
			t.Error(err)
		}
	}()
	// Past the peon's lease (mon_lease 5 s) it holds requests for an election.
	time.Sleep(6 * time.Second)

	type outcome struct {
		label string
		err   error
	}
	results := make(chan outcome, 3)
	go func() {
		_, err := pinned.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: pool, Op: cephmsgr.SelfManagedSnapshotCreate})
		results <- outcome{"pool op", err}
	}()
	go func() {
		_, err := pinned.Statfs(ctx, nil)
		results <- outcome{"statfs", err}
	}()
	go func() {
		_, err := pinned.MapVersion(ctx, cephmsgr.OSDMapKind)
		results <- outcome{"map version", err}
	}()
	time.Sleep(time.Second)
	select {
	case r := <-results:
		t.Fatalf("%s was answered while the leader was frozen: %v", r.label, r.err)
	default:
	}
	stopping := time.Now()
	if err := testcluster.ControlDaemon(ctx, control, "stop", "mon", peon); err != nil {
		t.Fatal("stop connected MON", err)
	}
	stopped = true
	for range 3 {
		r := <-results
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(r.err, &unknown) || ctx.Err() != nil {
			t.Fatalf("%s held by a stopped MON must end as an unknown outcome: %v", r.label, r.err)
		}
		t.Logf("%s ended unknown %s after the stop began: %v", r.label, time.Since(stopping).Round(time.Millisecond), r.err)
	}

	if err := testcluster.ControlDaemon(ctx, control, "resume", "mon", leader); err != nil {
		t.Fatal("resume leader", err)
	}
	paused = false
	recovered := time.Now()
	version, err := pinned.MapVersion(ctx, cephmsgr.OSDMapKind)
	if err != nil || version.Newest == 0 {
		t.Fatal("map version after the connected MON stopped", err)
	}
	if stat, err := pinned.Statfs(ctx, nil); err != nil || stat.KB == 0 {
		t.Fatal("statfs after the connected MON stopped", err)
	}
	current := pinned.Snapshot().Monitor.Endpoint
	if current == stoppedEndpoint {
		t.Fatal("the client still reports the stopped MON", current)
	}
	t.Logf("recovered %s -> %s; fresh requests answered %s after the leader resumed", stoppedEndpoint, current, time.Since(recovered).Round(time.Millisecond))
	held := info().SnapSeq
	if held != baseline && held != baseline+1 {
		t.Fatalf("native snap_seq %d: expected %d or %d", held, baseline, baseline+1)
	}

	if err := testcluster.ControlDaemon(ctx, control, "start", "mon", peon); err != nil {
		t.Fatal("restart MON", err)
	}
	stopped = false
	if err := recycleResumedMon(ctx, control, leader); err != nil {
		t.Fatal("restart the resumed leader", err)
	}
	recycled = true
	waitNativeMonitorElection(t, ctx, c, control, "restarted", monState, monState.Members)
	if seq := info().SnapSeq; seq != held {
		t.Fatalf("native snap_seq moved from %d to %d without a request", held, seq)
	}
	next, err := pinned.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: pool, Op: cephmsgr.SelfManagedSnapshotCreate})
	if err != nil {
		t.Fatal("allocation after the restart", err)
	}
	if id, _ := next.SnapshotID(); id != held+1 || info().SnapSeq != id {
		t.Fatalf("allocation after the restart: id %d, native %d before", id, held)
	}
	t.Logf("the held allocation executed: %t (snap_seq %d -> %d)", held == baseline+1, baseline, held)
}

// quorumPeonSeed returns the quorum leader's name and a peon's name with the
// configured seed that reaches it. It skips without a full quorum of three.
func quorumPeonSeed(t *testing.T, ctx context.Context, c *cephmsgr.Client) (leader, peon, seed string) {
	t.Helper()
	quorum, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"quorum_status","format":"json"}`)})
	var status struct {
		Leader string   `json:"quorum_leader_name"`
		Names  []string `json:"quorum_names"`
		Map    struct {
			Mons []struct {
				Name  string `json:"name"`
				Addrs struct {
					Vec []struct{ Type, Addr string } `json:"addrvec"`
				} `json:"public_addrs"`
			} `json:"mons"`
		} `json:"monmap"`
	}
	if err != nil || json.Unmarshal(quorum.Data, &status) != nil || status.Leader == "" {
		t.Fatal("quorum status", err)
	}
	if len(status.Map.Mons) < 3 || len(status.Names) != len(status.Map.Mons) {
		t.Skip("needs a full quorum of at least three monitors")
	}
	for _, m := range status.Map.Mons {
		if m.Name == status.Leader {
			continue
		}
		for _, a := range m.Addrs.Vec {
			for _, candidate := range integrationOptions(t).Monitors {
				if a.Type == "v2" && seed == "" && strings.Contains(candidate, a.Addr) {
					peon, seed = m.Name, candidate
				}
			}
		}
	}
	if seed == "" {
		t.Fatal("no peon seed address found", status)
	}
	return status.Leader, peon, seed
}

// recycleResumedMon restarts a MON that was frozen and resumed. In one of
// several runs a resumed MON rejoined the quorum but never sent the OSD that
// was subscribed through it another OSDMap (the OSD stayed at an old epoch and
// a new pool's PG stayed unknown); a restart makes its daemon clients
// reconnect so later tests see a current cluster.
func recycleResumedMon(ctx context.Context, control, name string) error {
	if err := testcluster.ControlDaemon(ctx, control, "stop", "mon", name); err != nil {
		return err
	}
	return testcluster.ControlDaemon(ctx, control, "start", "mon", name)
}

// requireMonFreezeOptIn skips the connected-MON freeze tests unless
// CEPH_MSGR_TEST_MON_FREEZE=1: after a MON freeze the fixture's OSD sometimes
// keeps an old OSDMap and breaks later tests (MON_FREEZE_FIXTURE_ISSUE.md).
func requireMonFreezeOptIn(t *testing.T) {
	t.Helper()
	if os.Getenv("CEPH_MSGR_TEST_MON_FREEZE") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MON_FREEZE=1; see integration/fixture/MON_FREEZE_FIXTURE_ISSUE.md")
	}
}
