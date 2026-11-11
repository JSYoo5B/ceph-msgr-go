package integration_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// The CRC relay first observes a transmitted MMonGetVersion and its exact
// server reply, then discards only that reply. Stopping that actual connected
// MON must propagate transport EOF to the locally pending operation. This is
// server-answered/lost-reply coverage, not an unprocessed server request or a
// forced SIGKILL. No elapsed-time guess establishes the request fault phase.
func TestCephMonMapVersionConnectedStopIntegration(t *testing.T) {
	runMapVersionConnectedLoss(t, "stop")
}

// runMapVersionConnectedLoss withholds the exact server reply and then ends
// the connected MON with the given daemon control: "stop" is the provider's
// orderly Stop, "kill" is SIGKILL with a non-OOM exit 137 receipt.
func runMapVersionConnectedLoss(t *testing.T, action string) {
	options := integrationOptions(t)
	if options.ConnectionMode != cephmsgr.CRCMode {
		t.Skip("the request-specific map-version relay requires the CRC fixture")
	}
	admin, _, _, _ := osdFixture(t)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	requireUsableFixture(t, control)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	_, peon, seed := quorumPeonSeed(t, ctx, admin)
	monState := admin.Snapshot().Monitor
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
	// The fault must finish through actual EOF, before the unrelated
	// keepalive timeout. Recovery retains its ordinary caller setup bound.
	options.KeepaliveInterval, options.KeepaliveTimeout = 30*time.Second, 60*time.Second
	client, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	before := client.Snapshot()
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
	var held mapVersionReply
	select {
	case held = <-relay.held:
	case got := <-result:
		t.Fatal("map-version completed before the reply barrier", got)
	case <-ctx.Done():
		t.Fatal("map-version reply barrier", ctx.Err())
	}
	if !strings.Contains(before.Monitor.Endpoint, held.Request.Endpoint) || held.Request.TID == 0 || held.Request.Handle == 0 || !held.Held || held.Newest == 0 || held.Oldest == 0 || held.Oldest > held.Newest {
		t.Fatal("barrier does not identify the actual admitted MON request", before.Monitor.Endpoint, held)
	}
	select {
	case got := <-result:
		t.Fatal("discarded reply completed the locally pending call", got)
	default:
	}
	stopped := false
	defer func() {
		if !stopped {
			return
		}
		restore, stop := context.WithTimeout(context.Background(), 180*time.Second)
		defer stop()
		if err := testcluster.ControlDaemon(restore, control, "start", "mon", peon); err != nil {
			markFixtureUnusable(t, control, "map-version stopped MON did not restart")
			t.Error("restore stopped MON", err)
			return
		}
		if _, err := settleNativeMonitorElection(restore, admin, control, "restarted", monState, monState.Members); err != nil {
			markFixtureUnusable(t, control, "map-version full quorum did not settle after restart")
			t.Error(err)
		}
	}()
	// Set ownership before the control call: a controller error cannot prove
	// that the daemon is still running, so restoration remains mandatory.
	stopped = true
	if err := testcluster.ControlDaemon(ctx, control, action, "mon", peon); err != nil {
		t.Fatal(action+" the reply's connected MON", err)
	}
	if action == "kill" {
		requireMonKillReceipt(t, control, peon)
	}
	var lost outcome
	select {
	case lost = <-result:
	case <-ctx.Done():
		t.Fatal("pending request did not end after MON stop", ctx.Err())
	}
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(lost.err, &unknown) || !mapVersionPeerLoss(action, lost.err) || ctx.Err() != nil || lost.version != (cephmsgr.MapVersionResult{}) {
		t.Fatal("connected "+action+" did not preserve an uncertain peer loss and empty result", lost)
	}
	var ended mapVersionRelayEnd
	for ended.Connection != held.Request.Connection {
		select {
		case ended = <-relay.ended:
		case <-ctx.Done():
			t.Fatal("missing actual peer EOF receipt", ctx.Err())
		}
	}
	if !mapVersionPeerLoss(action, ended.Err) {
		t.Fatal("relay did not observe the actual peer loss", ended.Err)
	}
	requests, _ := relay.snapshot()
	if len(requests) != 1 {
		t.Fatal("the lost query was retransmitted before an explicit new call", requests)
	}
	t.Logf("connected MON %s ended by %s after server reply: peer=%s GID=%d connection=%d TID=%d sequence=%d handle=%d epoch=%d oldest=%d terminal=%v transport=%v",
		peon, action, held.Request.Endpoint, before.GlobalID, held.Request.Connection, held.Request.TID, held.Request.Sequence, held.Request.Handle, held.Newest, held.Oldest, lost.err, ended.Err)
	fresh, err := client.MapVersion(ctx, cephmsgr.OSDMapKind)
	if err != nil || fresh.Newest == 0 || fresh.Oldest == 0 || fresh.Oldest > fresh.Newest {
		t.Fatal("fresh explicit map-version request after connected stop", fresh, err)
	}
	after := client.Snapshot()
	requests, replies := relay.snapshot()
	if len(requests) != 2 || len(replies) != 2 {
		t.Fatal("lost or duplicated request/reply identity", requests, replies)
	}
	next := replies[1]
	// TIDs belong to one session and may be reused after reconnect. The
	// relay connection and monotonically increasing MapVersion handle must
	// change; require the native reply to match that connection/TID/handle.
	if next.Held || next.Request.Connection == held.Request.Connection || next.Request.Handle != held.Request.Handle+1 || fresh.Newest != next.Newest || fresh.Oldest != next.Oldest || after.Monitor.Endpoint == before.Monitor.Endpoint || after.GlobalID != before.GlobalID || after.FSID != before.FSID || !strings.Contains(after.Monitor.Endpoint, next.Request.Endpoint) {
		t.Fatalf("fresh query identity differs: GID=%d->%d FSID-kept=%t endpoint=%s->%s held=%+v fresh=%+v result=%+v",
			before.GlobalID, after.GlobalID, before.FSID == after.FSID, before.Monitor.Endpoint, after.Monitor.Endpoint, held, next, fresh)
	}
	native := nativePool(t, ctx, "versions", map[string]any{}).Epochs["osdmap"]
	if fresh.Newest != native {
		t.Fatal("recovered map-version does not match the independent native epoch", fresh, native)
	}
	t.Logf("explicit recovery: peer=%s GID=%d connection=%d TID=%d sequence=%d handle=%d epoch=%d oldest=%d native=%d; original query not replayed",
		next.Request.Endpoint, after.GlobalID, next.Request.Connection, next.Request.TID, next.Request.Sequence, next.Request.Handle, fresh.Newest, fresh.Oldest, native)
	if err := testcluster.ControlDaemon(ctx, control, "start", "mon", peon); err != nil {
		t.Fatal("restart stopped MON", err)
	}
	stopped = false
	summary, err := settleNativeMonitorElection(ctx, admin, control, "restarted", monState, monState.Members)
	if err != nil {
		markFixtureUnusable(t, control, "map-version full quorum did not settle after restart")
		t.Fatal(err)
	}
	t.Log(summary)
}

// mapVersionPeerLoss reports whether err is the transport loss that the fault
// can produce. An orderly Stop must end as EOF. A killed peer's kernel closes
// the socket with FIN or, with unread bytes, RST, so SIGKILL admits both.
func mapVersionPeerLoss(action string, err error) bool {
	return errors.Is(err, io.EOF) || action == "kill" && errors.Is(err, syscall.ECONNRESET)
}
