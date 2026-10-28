package cephmsgr

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

func goMonTellValue(t *testing.T, ctx context.Context, c *Client, target string) int64 {
	t.Helper()
	result, err := c.MonTellTo(ctx, target, monTellGetCommand())
	value, ok := parseMonTellEffective(result.Data)
	if err != nil || !ok {
		t.Fatal("read the daemon-local option through an exact-target Tell session", err, string(result.Data))
	}
	return value
}

// The daemon-local runtime option lives only in the targeted MON process. The
// test keeps that process alive, ends only the subject connection and wait, and
// reads the later value through fresh sessions. A restart would clear the
// override, so the surviving distinct override also shows the same incarnation.
func TestCephAppliedMonitorTellIsNotReplayedIntegration(t *testing.T) {
	testAppliedMonitorTell(t, false)
}

// The current-MON API shares the admitted primary connection. Losing its reply
// ends that primary session, unlike MonTellTo's operation-private connection.
func TestCephAppliedCurrentMonitorTellIsNotReplayedIntegration(t *testing.T) {
	testAppliedMonitorTell(t, true)
}

func testAppliedMonitorTell(t *testing.T, current bool) {
	t.Helper()
	observer, ctx := fixtureClient(t, 3*time.Minute)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	const target = "b"
	role := "mon." + target
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	token := fmt.Sprintf("%x", nonce)
	// Baseline effective value and runtime override presence come from fresh
	// native views before any mutation. The effective value alone cannot show
	// whether an override exists.
	initial, err := observeNativeMonTell(ctx, control, token, "initial", target)
	if err != nil {
		t.Fatal("native baseline observation", err)
	}
	t.Log(nativeMonTellSummary(initial))
	original, ok := parseMonTellEffective(initial.Views.ConfigGet.Data)
	rank, stable := nativeMonTellStable(initial.Views, target)
	if initial.Baseline == nil || !ok || !stable {
		t.Fatal("native baseline is not one restorable bracketed daemon-local state")
	}
	a, b, err := monTellOutcomeValues(original)
	if err != nil {
		t.Fatal("refusing the candidate outside its bound", err)
	}
	baseline := nativeMonTellState{value: original}
	if initial.Baseline.Present {
		baseline.override = &original
	}
	if !nativeMonTellMatches(initial.Views, target, baseline) {
		t.Fatal("native views disagree about the baseline", nativeMonTellSummary(initial))
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		receipt, err := observeNativeMonTell(cleanup, control, token, "restore", target)
		if err != nil {
			t.Error("native restoration did not return; its outcome remains uncertain and is not retried", err)
			return
		}
		if receipt.Restoration == nil || receipt.Restoration.Code != 0 {
			t.Error("native restoration failed", receipt.Restoration, nativeMonTellSummary(receipt))
			return
		}
		t.Logf("native restoration submitted once (baseline override present=%t): signed=%d data_bytes=%d status=%q", initial.Baseline.Present,
			receipt.Restoration.Code, len(receipt.Restoration.Data), receipt.Restoration.Status)
		// Only fresh read observations repeat.
		for !nativeMonTellMatches(receipt.Views, target, baseline) {
			select {
			case <-cleanup.Done():
				t.Error("fresh native views did not show the restored baseline", nativeMonTellSummary(receipt))
				return
			case <-time.After(100 * time.Millisecond):
			}
			if receipt, err = observeNativeMonTell(cleanup, control, token, "restored", target); err != nil {
				t.Error("fresh native restored observation", err)
				return
			}
		}
		t.Log("restored baseline:", nativeMonTellSummary(receipt))
	}()
	if got := goMonTellValue(t, ctx, observer, target); got != original {
		t.Fatal("Go client and native baseline disagree before the mutation", got, original)
	}

	options := integrationOptions(t)
	if current {
		options, err = currentMonTellBootstrap(options, target)
		if err != nil {
			t.Fatal(err)
		}
	}
	dial := options.DialContext
	var writes atomic.Int32
	var connectionID atomic.Uint64
	probe := &monOutcomeWrites{}
	blocked := make(chan struct{})
	var blockOnce, armOnce sync.Once
	var mu sync.Mutex
	var subject *testcluster.LostReplyConn
	fallback := 0
	if options.ConnectionMode != CRCMode {
		fallback = 4096 // Secure frames are not decoded; use the padded size.
	}
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		_, peer, _ := testcluster.SplitPeer(endpoint)
		stalled := &testcluster.LostReplyConn{Conn: conn, Armed: &atomic.Bool{}, Closed: make(chan struct{}), SignalBlocked: func() { blockOnce.Do(func() { close(blocked) }) }}
		identified := &monOutcomeConn{Conn: stalled, record: probe, connection: connectionID.Add(1), peer: peer}
		// Only the first Tell command written to the exact target MON arms its
		// connection; later fresh sessions to that MON are never withheld.
		identified.gate = &monOutcomeGate{Peer: role, Type: msgr.TellCommandMessage, FallbackBytes: fallback, Arm: func() {
			armOnce.Do(func() {
				mu.Lock()
				subject = stalled
				mu.Unlock()
				stalled.Armed.Store(true)
			})
		}}
		return &commandWriteProbe{Conn: identified, writes: &writes}, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.mu.Lock()
	primary := c.mon
	c.mu.Unlock()
	if current {
		_, peer, err := testcluster.SplitPeer(primary.RemoteAddr().String())
		if err != nil || peer != role {
			t.Fatal("current-MON bootstrap did not admit the owned target", peer, err)
		}
	}
	probe.enabled.Store(options.ConnectionMode == CRCMode)
	c.mu.Lock()
	fsid := c.fsid
	c.mu.Unlock()
	identity := monTellOutcomeIdentity{PrimaryGlobalID: c.snapshotAuth().GlobalID, Target: role, Baseline: original}
	writeIdentity := func() {
		if current {
			writeCurrentMonTellOutcomeIdentity(t, control, identity)
		} else {
			writeMonTellOutcomeIdentity(t, control, identity)
		}
	}
	commandA := monTellSetCommand(a, 40<<10)
	frontA := msgr.TellCommand(fsid, []string{string(commandA.JSON)}, nil).Front
	var originalResult Result
	finished := make(chan error, 1)
	go func() {
		var err error
		if current {
			originalResult, err = c.MonTell(ctx, commandA)
		} else {
			originalResult, err = c.MonTellTo(ctx, target, commandA)
		}
		finished <- err
	}()
	// A is established natively while its reply is still withheld.
	applied := requireNativeMonTellState(t, ctx, control, token, "applied", target, nativeMonTellState{a, &a}, &original, finished)
	t.Log(nativeMonTellSummary(applied))
	select {
	case <-blocked:
	case err := <-finished:
		t.Fatal("applied Tell mutation finished without a withheld response", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if writes.Load() != 1 {
		t.Fatal("initial Tell mutation was not written exactly once", writes.Load())
	}
	var submitted monOutcomeWrite
	if options.ConnectionMode == CRCMode {
		submitted = requireMonOutcomeWrite(t, probe, frontA, nil)
		if submitted.Type != msgr.TellCommandMessage || submitted.Peer != role {
			t.Fatal("original CRC command is not the exact-target Tell frame", submitted.Type, submitted.Peer)
		}
		identity.Submitted = &submitted
		t.Logf("original CRC Tell submission: peer=%s connection=%d primary_client_global_id=%d (Tell session global ID unobserved) tid=%d sequence=%d complete_local_write=true blocked_receive=true",
			submitted.Peer, submitted.Connection, identity.PrimaryGlobalID, submitted.Transaction, submitted.Sequence)
	}
	identity.PaddedWrites = writes.Load()
	writeIdentity()
	// An independent client explicitly establishes distinct B on the same
	// daemon. A replay of A would overwrite it.
	if _, err := observer.MonTellTo(ctx, target, monTellSetCommand(b, 0)); err != nil {
		t.Fatal("independent Tell overwrite", err)
	}
	overwritten := requireNativeMonTellState(t, ctx, control, token, "overwritten", target, nativeMonTellState{b, &b}, &a, finished)
	t.Log(nativeMonTellSummary(overwritten))
	if writes.Load() != 1 {
		t.Fatal("more than one local padded Tell write before the subject connection ended", writes.Load())
	}
	// End only the subject connection and wait. The MON process stays alive.
	mu.Lock()
	withheld := subject
	mu.Unlock()
	if withheld == nil {
		t.Fatal("the exact-target gate never recorded its withheld connection")
	}
	if current {
		c.mu.Lock()
		stillPrimary := c.mon == primary && c.monReady
		c.mu.Unlock()
		if !stillPrimary || primary.Err() != nil {
			t.Fatal("the original primary changed before the controlled current-Tell loss")
		}
		select {
		case <-primary.Done():
			t.Fatal("the original primary ended before its controlled connection loss")
		default:
		}
	}
	withheld.Close()
	if current {
		select {
		case <-primary.Done():
			t.Log("the withheld current-MON Tell connection was the admitted primary session")
		case <-ctx.Done():
			t.Fatal("subject loss did not terminate the original primary MON session", ctx.Err())
		}
	}
	select {
	case err := <-finished:
		var unknown *OutcomeUnknownError
		if !errors.As(err, &unknown) {
			t.Fatal("applied Tell mutation lost its unknown outcome when its connection ended", err)
		}
		if originalResult.Code != 0 || originalResult.Message != "" || len(originalResult.Data) != 0 {
			t.Fatal("unanswered Tell unexpectedly gained an acknowledged server result", originalResult)
		}
		identity.OutcomeUnknown = true
		writeIdentity()
		t.Log("original caller retained OutcomeUnknownError; later native observations do not acknowledge it")
	case <-ctx.Done():
		t.Fatal("ending the subject connection did not release the pending Tell", ctx.Err())
	}
	if current {
		if err := c.WaitMonReady(ctx); err != nil {
			t.Fatal("current-MON client did not recover after its unanswered Tell", err)
		}
		c.mu.Lock()
		recoveredPrimary := c.mon
		c.mu.Unlock()
		if recoveredPrimary == primary || recoveredPrimary == nil || recoveredPrimary.Err() != nil {
			t.Fatal("recovery reused the terminated primary MON session")
		}
		_, recoveryPeer, peerErr := testcluster.SplitPeer(recoveredPrimary.RemoteAddr().String())
		if peerErr != nil || recoveryPeer == "" {
			t.Fatal("recovered primary has no owned MON identity", peerErr)
		}
		t.Logf("current-MON recovery established another admitted primary: %s", recoveryPeer)
	}
	// Fresh exact-target Go sessions and a fresh native handle read B.
	if got := goMonTellValue(t, ctx, c, target); got != b || writes.Load() != 1 {
		t.Fatal("a fresh Go session on the subject client did not read B, or A was replayed", got, b, writes.Load())
	}
	if got := goMonTellValue(t, ctx, observer, target); got != b {
		t.Fatal("a fresh independent Go session did not read B", got, b)
	}
	recovered := requireNativeMonTellState(t, ctx, control, token, "recovered", target, nativeMonTellState{b, &b}, nil, nil)
	t.Log(nativeMonTellSummary(recovered))
	if after, _ := nativeMonTellStable(recovered.Views, target); after != rank {
		t.Fatal("native daemon identity changed during the cell", after, rank)
	}
	if options.ConnectionMode == CRCMode {
		if after := requireMonOutcomeWrite(t, probe, frontA, nil); after != submitted {
			t.Fatal("original Tell identity changed after the subject connection ended")
		}
		readFront := msgr.TellCommand(fsid, []string{string(monTellGetCommand().JSON)}, nil).Front
		observed := requireMonOutcomeWrite(t, probe, readFront, nil)
		if observed.Type != msgr.TellCommandMessage || observed.Connection == submitted.Connection || observed.Peer != role {
			t.Fatal("fresh read did not use another exact-target Tell connection", observed.Type, observed.Connection, observed.Peer)
		}
		identity.Recovery, identity.RecoveryPrimaryGlobalID = &observed, c.snapshotAuth().GlobalID
		t.Logf("fresh CRC Tell read: peer=%s connection=%d tid=%d sequence=%d; original local mutation writes=1", observed.Peer, observed.Connection, observed.Transaction, observed.Sequence)
	} else {
		t.Log("secure mode: native state and padded local write count checked; CRC transaction receipt is unavailable")
	}
	identity.PaddedWrites = writes.Load()
	writeIdentity()
	t.Logf("%s (rank %d) applied %d; independent overwrite %d confirmed natively on the same daemon; fresh Go and native sessions kept %d; padded local writes=%d (not a server execution count)",
		role, rank, a, b, b, writes.Load())
}
