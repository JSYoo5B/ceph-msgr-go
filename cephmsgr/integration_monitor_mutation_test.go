package cephmsgr

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
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

func TestCephAppliedMonitorMutationIsNotReplayedIntegration(t *testing.T) {
	observer, ctx := fixtureClient(t, 2*time.Minute)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	token := fmt.Sprintf("%x", nonce)
	key := "native-mon-no-replay-" + token
	command := func(prefix string) Command {
		encoded, _ := json.Marshal(map[string]string{"prefix": prefix, "key": key})
		return Command{JSON: encoded}
	}
	initial, err := observeNativeMonOutcome(ctx, control, token, "initial")
	if err != nil || initial.Result.Code != -2 {
		t.Fatal("native initial absence did not establish ownership", initial.Result.Code, err)
	}
	t.Log(monOutcomeResultSummary(initial))
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		receipt, err := observeNativeMonOutcome(cleanup, control, token, "cleanup")
		if err != nil {
			t.Error("native cleanup did not return a confirmed removal and fresh absence; outcome remains uncertain, no retry", err)
			return
		}
		if receipt.Removal == nil || receipt.Removal.Code != 0 || receipt.Result.Code != -2 {
			t.Error("native cleanup failed its independent absence check", receipt)
			return
		}
		t.Logf("native cleanup submitted once: signed removal=%d status=%q; %s", receipt.Removal.Code, receipt.Removal.Status, monOutcomeResultSummary(receipt))
	}()
	options := integrationOptions(t)
	dial := options.DialContext
	var rejectedPort atomic.Value
	rejectedPort.Store("")
	var writes atomic.Int32
	var connectionID atomic.Uint64
	probe := &monOutcomeWrites{}
	var mu sync.Mutex
	connections := make(map[string]*testcluster.LostReplyConn)
	blocked := make(chan struct{})
	var blockOnce sync.Once
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		_, role, _ := testcluster.SplitPeer(endpoint)
		if role == rejectedPort.Load().(string) {
			return nil, net.ErrClosed
		}
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		stalled := &testcluster.LostReplyConn{Conn: conn, Armed: &atomic.Bool{}, Closed: make(chan struct{}), SignalBlocked: func() { blockOnce.Do(func() { close(blocked) }) }}
		mu.Lock()
		connections[endpoint] = stalled
		mu.Unlock()
		identified := &monOutcomeConn{Conn: stalled, record: probe, connection: connectionID.Add(1), peer: role}
		return &commandWriteProbe{Conn: identified, writes: &writes}, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	probe.enabled.Store(options.ConnectionMode == CRCMode)
	originalGlobalID := c.snapshotAuth().GlobalID
	c.mu.Lock()
	old := c.mon
	endpoint := old.RemoteAddr().String()
	c.mu.Unlock()
	_, role, _ := testcluster.SplitPeer(endpoint)
	name := map[string]string{"mon.a": "a", "mon.b": "b", "mon.c": "c"}[role]
	mu.Lock()
	stalled := connections[endpoint]
	mu.Unlock()
	if name == "" || stalled == nil {
		t.Fatal("could not identify the authenticated fixture MON", endpoint)
	}
	stalled.Armed.Store(true)
	input := bytes.Repeat(append([]byte("applied MON mutation; "), 0, 0xff), 2048)
	mutation := command("config-key set")
	mutation.Input = input
	c.mu.Lock()
	fsid := c.fsid
	c.mu.Unlock()
	mutationFront := msgr.CommandMessage(false, fsid, []string{string(mutation.JSON)}, input).Front
	var originalResult Result
	finished := make(chan error, 1)
	go func() { var err error; originalResult, err = c.MonCommand(ctx, mutation); finished <- err }()
	applied := requireNativeMonValue(t, ctx, control, token, "applied", input, finished)
	t.Log(monOutcomeResultSummary(applied))
	select {
	case <-blocked:
	case err := <-finished:
		t.Fatal("applied MON mutation did not lose its reply", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var submitted monOutcomeWrite
	var identity monOutcomeIdentity
	if options.ConnectionMode == CRCMode {
		submitted = requireMonOutcomeWrite(t, probe, mutationFront, input)
		if submitted.Peer != role || originalGlobalID == 0 {
			t.Fatal("original CRC command lacks its authenticated MON binding")
		}
		identity = monOutcomeIdentity{OriginalGlobalID: originalGlobalID, Submitted: submitted}
		writeMonOutcomeIdentity(t, control, identity)
		t.Logf("original CRC MON submission: peer=%s connection=%d global_id=%d tid=%d sequence=%d complete_local_write=true blocked_receive=true", role, submitted.Connection, originalGlobalID, submitted.Transaction, submitted.Sequence)
	}
	overwrite := command("config-key set")
	overwrite.Input = append([]byte("independent overwrite survives MON replacement"), 0, 0xfe)
	if _, err := observer.MonCommand(ctx, overwrite); err != nil {
		t.Fatal("commit the independent overwrite", err)
	}
	overwritten := requireNativeMonValue(t, ctx, control, token, "overwritten", overwrite.Input, finished)
	t.Log(monOutcomeResultSummary(overwritten))
	if writes.Load() != 1 {
		t.Fatal("more than one local padded mutation write before MON replacement", writes.Load())
	}
	// Restart the real daemon, then release the intentionally withheld receive.
	// Exclude its endpoint so recovery must authenticate a different MON.
	rejectedPort.Store(role)
	if err := testcluster.RestartMonitor(ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), name); err != nil {
		t.Fatal("restart the MON that applied the unanswered mutation", err)
	}
	stalled.Close()
	select {
	case err := <-finished:
		var unknown *OutcomeUnknownError
		if !errors.As(err, &unknown) {
			t.Fatal("applied MON mutation lost its unknown outcome on connection replacement", err)
		}
		if originalResult.Code != 0 || originalResult.Message != "" || len(originalResult.Data) != 0 {
			t.Fatal("unanswered mutation unexpectedly gained an acknowledged server result", originalResult)
		}
		t.Log("original caller retained OutcomeUnknownError; later native observations do not acknowledge it")
		if options.ConnectionMode == CRCMode {
			identity.OutcomeUnknown = true
			writeMonOutcomeIdentity(t, control, identity)
		}
	case <-ctx.Done():
		t.Fatal("MON replacement did not release the pending mutation", ctx.Err())
	}
	result, err := c.MonCommand(ctx, command("config-key get"))
	if err != nil || !bytes.Equal(result.Data, overwrite.Input) || writes.Load() != 1 {
		t.Fatal("fresh MON replayed the applied mutation", err, writes.Load())
	}
	c.mu.Lock()
	current := c.mon
	c.mu.Unlock()
	if current == old || current.RemoteAddr().String() == endpoint {
		t.Fatal("recovery did not authenticate a different MON")
	}
	recovered := requireNativeMonValue(t, ctx, control, token, "recovered", overwrite.Input, nil)
	t.Log(monOutcomeResultSummary(recovered))
	if options.ConnectionMode == CRCMode {
		if after := requireMonOutcomeWrite(t, probe, mutationFront, input); after != submitted {
			t.Fatal("original mutation identity changed after recovery")
		}
		read := command("config-key get")
		readFront := msgr.CommandMessage(false, fsid, []string{string(read.JSON)}, nil).Front
		observed := requireMonOutcomeWrite(t, probe, readFront, nil)
		if observed.Connection == submitted.Connection || observed.Peer == submitted.Peer {
			t.Fatal("recovery read did not use another authenticated MON connection")
		}
		identity.Recovery, identity.RecoveryGlobalID = &observed, c.snapshotAuth().GlobalID
		writeMonOutcomeIdentity(t, control, identity)
		t.Logf("recovery CRC MON read: peer=%s connection=%d global_id=%d tid=%d sequence=%d; original local mutation writes=1", observed.Peer, observed.Connection, c.snapshotAuth().GlobalID, observed.Transaction, observed.Sequence)
	} else {
		t.Log("secure mode: native state and padded local write count checked; CRC transaction receipt is unavailable")
	}
	t.Logf("MON %s restarted after applying an unanswered mutation; padded local writes=%d; distinct native overwrite preserved (not a server execution count)", name, writes.Load())
}
