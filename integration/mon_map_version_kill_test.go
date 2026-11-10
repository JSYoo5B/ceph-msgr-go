package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// requireMonForcedKill skips unless the fixture selected the MON forced-kill
// cells. Broad OSD selections match these test names too, but their fixture
// refuses SIGKILL of a MON and keeps the ordinary ticket validity.
func requireMonForcedKill(t *testing.T) {
	t.Helper()
	if os.Getenv("CEPH_MSGR_NATIVE_MON_FORCED_KILL") != "1" {
		t.Skip("SIGKILL of a MON requires the exclusive selector ^TestCephMonMapVersion(ConnectedKill|UnprocessedKill)Integration$")
	}
}

// requireMonKillReceipt checks the fixture's SIGKILL receipt for one MON: the
// container stopped with exit 137 and was not killed by the OOM handler.
func requireMonKillReceipt(t *testing.T, control, name string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(control, "mon-crash-state.json"))
	var crash struct {
		Daemon    string `json:"daemon"`
		ExitCode  *int   `json:"exit_code"`
		OOMKilled *bool  `json:"oom_killed"`
	}
	if err != nil || json.Unmarshal(data, &crash) != nil || crash.Daemon != "mon."+name || crash.ExitCode == nil || *crash.ExitCode != 137 || crash.OOMKilled == nil || *crash.OOMKilled {
		t.Fatalf("MON %s has no SIGKILL receipt (not an orderly shutdown or OOM): %+v %v", name, crash, err)
	}
}

// The relay withholds the exact server reply, then the connected MON is
// killed with SIGKILL instead of stopped in order. This is the forced-death
// counterpart of TestCephMonMapVersionConnectedStopIntegration: the MON has
// answered, the caller is still waiting, and the peer dies without shutdown.
func TestCephMonMapVersionConnectedKillIntegration(t *testing.T) {
	requireMonForcedKill(t)
	runMapVersionConnectedLoss(t, "kill")
}

// monElectionStatus is the part of a MON's own mon_status that tells whether
// it can answer MMonGetVersion: only a leader or peon does.
type monElectionStatus struct {
	Name          string `json:"name"`
	State         string `json:"state"`
	ElectionEpoch uint64 `json:"election_epoch"`
	Quorum        []int  `json:"quorum"`
}

// monSessionTrimGrace is how long after its last in-quorum status the cell
// may still kill the MON. The MON starts dropping client sessions two lease
// intervals (2 x mon_lease = 10 s) after it left the quorum; the difference
// covers the kill control's own latency.
const monSessionTrimGrace = 7 * time.Second

func (s monElectionStatus) outOfQuorum() bool {
	return s.State != "" && s.State != "leader" && s.State != "peon" && len(s.Quorum) == 0
}

// A request that the MON has read but not answered. The connected MON is
// first taken out of quorum by stopping the other two; pinned Monitor.cc
// handle_get_version then parks MMonGetVersion on waitfor_quorum without a
// reply. The barrier is on the wire: the relay sees the complete request,
// then a daemon-local command sent after it on the same connection is
// answered by the same MON, which is still out of quorum, and no version
// reply exists. Only then is that MON killed with SIGKILL. No reply is
// withheld by the test and no elapsed-time guess sets the fault phase.
func TestCephMonMapVersionUnprocessedKillIntegration(t *testing.T) {
	requireMonForcedKill(t)
	options := integrationOptions(t)
	if options.ConnectionMode != cephmsgr.CRCMode {
		t.Skip("the request-specific map-version relay requires the CRC fixture")
	}
	admin, _, _, _ := osdFixture(t)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	requireUsableFixture(t, control)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	_, subject, seed := quorumPeonSeed(t, ctx, admin)
	monState := admin.Snapshot().Monitor
	var others []string
	for _, member := range monState.Members {
		if member.Name != subject {
			others = append(others, member.Name)
		}
	}
	slices.Sort(others)
	if len(others) != 2 {
		t.Fatal("the cell needs exactly two other MONs to remove the quorum", monState.Members)
	}
	relay := newMapVersionRelay()
	dial := options.DialContext
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		return relay.wrap(conn, endpoint), nil
	}
	options.Monitors = []string{seed}
	options.MaxFrameSize = mapVersionRelayLimit
	// The loss must come from the killed peer, not the keepalive timeout.
	options.KeepaliveInterval, options.KeepaliveTimeout = 30*time.Second, 60*time.Second
	client, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	before := client.Snapshot()

	// down names every MON this test may have ended and has not yet tried
	// to restart. Ownership is recorded before each stop or kill: a
	// controller error cannot prove the daemon still runs, so restoration
	// stays mandatory. A name leaves the list before its start is requested,
	// so no start is repeated; the settled quorum is still required.
	var down []string
	settled := true
	defer func() {
		if settled {
			return
		}
		restore, stop := context.WithTimeout(context.Background(), 240*time.Second)
		defer stop()
		// The provider's readiness check of the bootstrap MON "a" needs a
		// quorum, so it restarts after the others.
		slices.Sort(down)
		for _, name := range slices.Backward(down) {
			if err := testcluster.ControlDaemon(restore, control, "start", "mon", name); err != nil {
				markFixtureUnusable(t, control, "map-version kill cell could not restart MON "+name)
				t.Error("restore MON", name, err)
				return
			}
		}
		if _, err := settleNativeMonitorElection(restore, admin, control, "restarted", monState, monState.Members); err != nil {
			markFixtureUnusable(t, control, "map-version kill cell full quorum did not settle")
			t.Error(err)
		}
	}()
	status := func(label string) monElectionStatus {
		t.Helper()
		result, err := client.MonTell(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"mon_status","format":"json"}`)})
		var decoded monElectionStatus
		if err != nil || json.Unmarshal(result.Data, &decoded) != nil || decoded.Name != subject {
			t.Fatal("connected MON status", label, decoded, err)
		}
		return decoded
	}
	if initial := status("before the quorum loss"); initial.State != "peon" {
		t.Fatal("subject MON is not a peon of the full quorum", initial)
	}
	settled = false
	for _, name := range others {
		down = append(down, name)
		if err := testcluster.ControlDaemon(ctx, control, "stop", "mon", name); err != nil {
			t.Fatal("stop MON", name, err)
		}
	}
	// A MON that has been out of quorum for two lease intervals (10 s) drops
	// its client sessions on every tick (pinned Monitor.cc tick, "out of
	// quorum too long"). The whole cell therefore runs inside that interval,
	// measured from the last status that still showed the MON in quorum, so
	// such a drop cannot be mistaken for the kill.
	inQuorum := time.Now()
	var electing monElectionStatus
	for {
		asked := time.Now()
		if electing = status("waiting to leave the quorum"); electing.outOfQuorum() {
			break
		}
		inQuorum = asked
		select {
		case <-ctx.Done():
			t.Fatal("connected MON stayed in quorum without its peers", electing, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}

	relay.arm()
	type outcome struct {
		version cephmsgr.MapVersionResult
		err     error
	}
	result := make(chan outcome, 1)
	callDone := make(chan struct{})
	go func() {
		defer close(callDone)
		version, err := client.MapVersion(ctx, cephmsgr.OSDMapKind)
		result <- outcome{version, err}
	}()
	defer func() {
		client.Close()
		select {
		case <-callDone:
		case <-time.After(5 * time.Second):
			t.Error("original map-version worker did not join after client Close")
		}
	}()
	var sent mapVersionRequest
	select {
	case sent = <-relay.sent:
	case got := <-result:
		t.Fatal("map-version completed before its request was observed", got)
	case <-ctx.Done():
		t.Fatal("map-version request was not transmitted", ctx.Err())
	}
	if !strings.Contains(before.Monitor.Endpoint, sent.Endpoint) || sent.TID == 0 || sent.Handle == 0 || sent.Kind != "osdmap" {
		t.Fatal("observed request is not the admitted MON's query", before.Monitor.Endpoint, sent)
	}
	// This tell follows the request on the same connection. Its answer shows
	// that the MON read past the request, and reports the MON's state then.
	parked := status("after the request")
	i := slices.IndexFunc(relay.tellExchanges(), func(tell mapVersionTell) bool {
		return tell.Connection == sent.Connection && tell.Sequence > sent.Sequence && tell.Answered
	})
	requests, replies := relay.snapshot()
	if !parked.outOfQuorum() || i < 0 || len(requests) != 1 || len(replies) != 0 || client.Snapshot().Monitor.Endpoint != before.Monitor.Endpoint {
		t.Fatalf("no unanswered-request barrier: status=%+v later-tell=%t requests=%v replies=%v", parked, i >= 0, requests, replies)
	}
	select {
	case got := <-result:
		t.Fatal("map-version ended before the connected MON was killed", got)
	case held := <-relay.held:
		t.Fatal("the out-of-quorum MON answered the request", held)
	default:
	}
	t.Logf("unanswered request barrier: mon.%s state=%s election_epoch=%d quorum=%v; peer=%s connection=%d TID=%d sequence=%d handle=%d; later tell answered on that connection; version replies=0; dialed=%v",
		subject, parked.State, parked.ElectionEpoch, parked.Quorum, sent.Endpoint, sent.Connection, sent.TID, sent.Sequence, sent.Handle, relay.endpoints())

	down = append(down, subject)
	killRequested := time.Now()
	if out := killRequested.Sub(inQuorum); out > monSessionTrimGrace {
		t.Fatalf("barrier reached %s after the MON was last seen in quorum; its own session trim could precede the kill", out)
	}
	if err := testcluster.ControlDaemon(ctx, control, "kill", "mon", subject); err != nil {
		t.Fatal("kill the request's connected MON", err)
	}
	requireMonKillReceipt(t, control, subject)
	var lost outcome
	select {
	case lost = <-result:
	case <-ctx.Done():
		t.Fatal("pending request did not end after the MON was killed", ctx.Err())
	}
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(lost.err, &unknown) || !mapVersionPeerLoss("kill", lost.err) || ctx.Err() != nil || lost.version != (cephmsgr.MapVersionResult{}) {
		t.Fatal("killed MON did not end the request as an uncertain peer loss with an empty result", lost)
	}
	var ended mapVersionRelayEnd
	for ended.Connection != sent.Connection {
		select {
		case ended = <-relay.ended:
		case <-ctx.Done():
			t.Fatal("missing actual peer loss receipt", ctx.Err())
		}
	}
	requests, replies = relay.snapshot()
	if !mapVersionPeerLoss("kill", ended.Err) || ended.At.Before(killRequested) || len(requests) != 1 || len(replies) != 0 {
		t.Fatal("request was answered or retransmitted, or its connection ended before the kill", ended.Err, ended.At.Sub(killRequested), requests, replies)
	}
	t.Logf("connected MON %s killed with the request unanswered: GID=%d connection=%d TID=%d handle=%d terminal=%v transport=%v %s after the kill request",
		subject, before.GlobalID, sent.Connection, sent.TID, sent.Handle, lost.err, ended.Err, ended.At.Sub(killRequested).Round(time.Millisecond))

	// No MON runs now. Bring back the two stopped MONs; they form a quorum
	// without the killed one, and the client reaches them through the MonMap
	// it learned before the fault. They restart in descending name order: the
	// provider's readiness check of the bootstrap MON "a" needs a quorum, so
	// it restarts once another MON runs.
	for _, name := range slices.Backward(others) {
		down = slices.DeleteFunc(down, func(owned string) bool { return owned == name })
		if err := testcluster.ControlDaemon(ctx, control, "start", "mon", name); err != nil {
			t.Fatal("restart stopped MON", name, err)
		}
	}
	fresh, err := client.MapVersion(ctx, cephmsgr.OSDMapKind)
	if err != nil || fresh.Newest == 0 || fresh.Oldest == 0 || fresh.Oldest > fresh.Newest {
		t.Fatal("fresh explicit map-version request after the kill", fresh, err)
	}
	after := client.Snapshot()
	requests, replies = relay.snapshot()
	if len(requests) != 2 || len(replies) != 1 {
		t.Fatal("lost or duplicated request/reply identity", requests, replies)
	}
	next := replies[0]
	if next.Held || next.Request != requests[1] || next.Request.Connection == sent.Connection || next.Request.Handle != sent.Handle+1 || fresh.Newest != next.Newest || fresh.Oldest != next.Oldest ||
		after.Monitor.Endpoint == before.Monitor.Endpoint || after.GlobalID != before.GlobalID || after.FSID != before.FSID || !strings.Contains(after.Monitor.Endpoint, next.Request.Endpoint) {
		t.Fatalf("fresh query identity differs: GID=%d->%d FSID-kept=%t endpoint=%s->%s sent=%+v fresh=%+v result=%+v",
			before.GlobalID, after.GlobalID, before.FSID == after.FSID, before.Monitor.Endpoint, after.Monitor.Endpoint, sent, next, fresh)
	}
	t.Logf("explicit recovery: peer=%s GID=%d connection=%d TID=%d sequence=%d handle=%d epoch=%d oldest=%d; original query not replayed",
		next.Request.Endpoint, after.GlobalID, next.Request.Connection, next.Request.TID, next.Request.Sequence, next.Request.Handle, fresh.Newest, fresh.Oldest)

	down = nil
	if err := testcluster.ControlDaemon(ctx, control, "start", "mon", subject); err != nil {
		t.Fatal("restart killed MON", err)
	}
	summary, err := settleNativeMonitorElection(ctx, admin, control, "restarted", monState, monState.Members)
	if err != nil {
		markFixtureUnusable(t, control, "map-version kill cell full quorum did not settle after restart")
		t.Fatal(err)
	}
	settled = true
	t.Log(summary)
	// Daemons re-register after the outage and can still move the OSDMap, so
	// the native comparison brackets one native read between two Go reads.
	for attempt := 1; ; attempt++ {
		first, err := client.MapVersion(ctx, cephmsgr.OSDMapKind)
		if err != nil {
			t.Fatal("map-version after the full quorum returned", err)
		}
		native := nativePool(t, ctx, "versions", map[string]any{}).Epochs["osdmap"]
		second, err := client.MapVersion(ctx, cephmsgr.OSDMapKind)
		if err != nil {
			t.Fatal("map-version after the native read", err)
		}
		if first.Newest == native && second.Newest == native && native >= fresh.Newest {
			t.Logf("restored quorum: Go newest=%d oldest=%d native=%d (bracketed, attempt %d)", second.Newest, second.Oldest, native, attempt)
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("Go and native OSDMap epochs never agreed after recovery", first, native, second, ctx.Err())
		case <-time.After(time.Second):
		}
	}
	if final := client.Snapshot(); final.GlobalID != before.GlobalID || final.FSID != before.FSID {
		t.Fatal("client identity changed across the recovery", before.GlobalID, final.GlobalID)
	}
}
