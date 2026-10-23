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

func fixtureBalancerMode(t *testing.T, c *Client, ctx context.Context) string {
	t.Helper()
	return fixtureBalancerModeWith(t, c, ctx, Command{JSON: []byte(`{"prefix":"balancer status"}`)})
}

func fixtureBalancerModeWith(t *testing.T, c *Client, ctx context.Context, command Command) string {
	t.Helper()
	result, err := c.MgrCommand(ctx, command)
	var status struct {
		Mode string `json:"mode"`
	}
	if err != nil || json.Unmarshal(result.Data, &status) != nil || status.Mode == "" {
		t.Fatal("read balancer mode", err)
	}
	return status.Mode
}

func TestCephAppliedManagerMutationIsNotReplayedIntegration(t *testing.T) {
	observer, ctx := fixtureClient(t, 3*time.Minute)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	token := fmt.Sprintf("%x", nonce)
	// Capture the baseline effective mode and explicit entry presence through
	// fresh native views before any mutation. An effective value alone cannot
	// show whether an explicit override exists.
	initial, err := observeNativeMgrOutcome(ctx, control, token, "initial")
	if err != nil {
		t.Fatal("native baseline observation", err)
	}
	t.Log(nativeMgrSummary(initial))
	original := nativeMgrMode(initial.Views)
	if _, _, stable := nativeMgrStable(initial.Views); initial.Baseline == nil || original == "" || !stable {
		t.Fatal("native baseline is not one restorable bracketed balancer-mode state")
	}
	baseline := nativeMgrState{mode: original}
	if initial.Baseline.Present {
		if initial.Baseline.Value != original {
			t.Fatal("explicit balancer-mode entry differs from the native effective mode", initial.Baseline.Value, original)
		}
		baseline.entries = explicitMgrMode(original)
	}
	if !nativeMgrMatches(initial.Views, baseline) {
		t.Fatal("native MON and MGR views disagree about the baseline")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		receipt, err := observeNativeMgrOutcome(cleanup, control, token, "restore")
		if err != nil {
			t.Error("native restoration did not return; its outcome remains uncertain and is not retried", err)
			return
		}
		if receipt.Restoration == nil || receipt.Restoration.Code != 0 {
			t.Error("native restoration failed", receipt.Restoration, nativeMgrSummary(receipt))
			return
		}
		t.Logf("native restoration submitted once (baseline entry present=%t): signed=%d data_bytes=%d status=%q", initial.Baseline.Present,
			receipt.Restoration.Code, len(receipt.Restoration.Data), receipt.Restoration.Status)
		// Only fresh read observations repeat while configuration propagates.
		for !nativeMgrMatches(receipt.Views, baseline) {
			select {
			case <-cleanup.Done():
				t.Error("fresh native views did not show the restored baseline", nativeMgrSummary(receipt))
				return
			case <-time.After(100 * time.Millisecond):
			}
			if receipt, err = observeNativeMgrOutcome(cleanup, control, token, "restored"); err != nil {
				t.Error("fresh native restored observation", err)
				return
			}
		}
		t.Log("restored baseline:", nativeMgrSummary(receipt))
	}()
	wanted := "upmap"
	if original == wanted {
		wanted = "none"
	}
	options := integrationOptions(t)
	dial := options.DialContext
	var armed, wrapped atomic.Bool
	var writes atomic.Int32
	var connectionID atomic.Uint64
	probe := &monOutcomeWrites{}
	blocked := make(chan struct{})
	var blockOnce sync.Once
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		_, role, _ := testcluster.SplitPeer(endpoint)
		if role == "mgr.a" || role == "mgr.b" {
			if wrapped.CompareAndSwap(false, true) {
				conn = &testcluster.LostReplyConn{Conn: conn, Armed: &armed, Closed: make(chan struct{}), SignalBlocked: func() { blockOnce.Do(func() { close(blocked) }) }}
			}
			conn = &monOutcomeConn{Conn: conn, record: probe, connection: connectionID.Add(1), peer: role}
			conn = &commandWriteProbe{Conn: conn, writes: &writes}
		}
		return conn, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := fixtureBalancerMode(t, c, ctx); got != original {
		t.Fatal("Go client and native baseline disagree before the mutation", got, original)
	}
	probe.enabled.Store(options.ConnectionMode == CRCMode)
	c.mu.Lock()
	name, originalGID, fsid := c.mgrMap.Name, c.mgrMap.GlobalID, c.fsid
	c.mu.Unlock()
	identity := mgrOutcomeIdentity{ClientGlobalID: c.snapshotAuth().GlobalID, OriginalManager: name, OriginalGID: originalGID}
	mutation, _ := json.Marshal(map[string]string{"prefix": "balancer mode", "mode": wanted})
	mutation = append(mutation, bytes.Repeat([]byte{' '}, 40<<10)...)
	mutationFront := msgr.CommandMessage(true, fsid, []string{string(mutation)}, nil).Front
	armed.Store(true)
	var originalResult Result
	finished := make(chan error, 1)
	go func() {
		var err error
		originalResult, err = c.MgrCommand(ctx, Command{JSON: mutation})
		finished <- err
	}()
	// Native MGR and MON views must both establish A while its reply is withheld.
	applied := requireNativeMgrState(t, ctx, control, token, "applied", nativeMgrState{wanted, explicitMgrMode(wanted)}, original,
		func(gid uint64) bool { return gid == originalGID }, finished)
	t.Log(nativeMgrSummary(applied))
	if active, _, _ := nativeMgrStable(applied.Views); active != name {
		t.Fatal("native active MGR differs from the Go client's MGR", active, name)
	}
	select {
	case <-blocked:
	case err := <-finished:
		t.Fatal("applied mutation finished without a withheld response", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if writes.Load() != 1 {
		t.Fatal("initial mutation was not written exactly once", writes.Load())
	}
	var submitted monOutcomeWrite
	if options.ConnectionMode == CRCMode {
		submitted = requireMonOutcomeWrite(t, probe, mutationFront, nil)
		if submitted.Type != msgr.MgrCommandMessage || submitted.Peer != "mgr."+name {
			t.Fatal("original CRC command lacks its active MGR binding", submitted.Type, submitted.Peer, name)
		}
		identity.Submitted = &submitted
		t.Logf("original CRC MGR submission: peer=%s connection=%d client_global_id=%d tid=%d sequence=%d complete_local_write=true blocked_receive=true",
			submitted.Peer, submitted.Connection, identity.ClientGlobalID, submitted.Transaction, submitted.Sequence)
	}
	identity.PaddedWrites = writes.Load()
	writeMgrOutcomeIdentity(t, control, identity)
	// Another client submits a distinct overwrite B, independently confirmed
	// before replacement. Any later replay of A would overwrite B.
	overwrite, _ := json.Marshal(map[string]string{"prefix": "balancer mode", "mode": original})
	if _, err := observer.MgrCommand(ctx, Command{JSON: overwrite}); err != nil {
		t.Fatal("independent mode overwrite", err)
	}
	overwritten := requireNativeMgrState(t, ctx, control, token, "overwritten", nativeMgrState{original, explicitMgrMode(original)}, wanted,
		func(gid uint64) bool { return gid == originalGID }, finished)
	t.Log(nativeMgrSummary(overwritten))
	if writes.Load() != 1 {
		t.Fatal("more than one local padded mutation write before MGR replacement", writes.Load())
	}
	encoded, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": name})
	if _, err := observer.MonCommand(ctx, Command{JSON: encoded}); err != nil {
		t.Fatal("MGR replacement with an applied mutation still awaiting its reply", err)
	}
	select {
	case err := <-finished:
		var unknown *OutcomeUnknownError
		if !errors.As(err, &unknown) || !errors.Is(err, ErrManagerChanged) {
			t.Fatal("applied mutation did not retain its unknown outcome on MGR replacement", err)
		}
		if originalResult.Code != 0 || originalResult.Message != "" || len(originalResult.Data) != 0 {
			t.Fatal("unanswered mutation unexpectedly gained an acknowledged server result", originalResult)
		}
		identity.OutcomeUnknown = true
		writeMgrOutcomeIdentity(t, control, identity)
		t.Log("original caller retained OutcomeUnknownError; later native observations do not acknowledge it")
	case <-ctx.Done():
		t.Fatal("MGR replacement did not release the pending mutation", ctx.Err())
	}
	read := Command{JSON: []byte(`{"prefix":"balancer status","format":"json"}`)}
	if got := fixtureBalancerModeWith(t, c, ctx, read); got != original || writes.Load() != 1 {
		t.Fatal("fresh MGR replayed the applied mutation", got, original, writes.Load())
	}
	c.mu.Lock()
	recoveryName, recoveryGID := c.mgrMap.Name, c.mgrMap.GlobalID
	c.mu.Unlock()
	if recoveryGID == originalGID || recoveryGID == 0 {
		t.Fatal("recovery did not use a different active MGR GID", recoveryGID, originalGID)
	}
	// Post-replacement native reads are bracketed by one unchanged active
	// name/GID that differs from the original MGR's GID.
	recovered := requireNativeMgrState(t, ctx, control, token, "recovered", nativeMgrState{original, explicitMgrMode(original)}, "",
		func(gid uint64) bool { return gid == recoveryGID }, nil)
	t.Log(nativeMgrSummary(recovered))
	if active, _, _ := nativeMgrStable(recovered.Views); active != recoveryName {
		t.Fatal("native active MGR differs from the Go client's recovery MGR", active, recoveryName)
	}
	identity.RecoveryManager, identity.RecoveryGID, identity.PaddedWrites = recoveryName, recoveryGID, writes.Load()
	if options.ConnectionMode == CRCMode {
		if after := requireMonOutcomeWrite(t, probe, mutationFront, nil); after != submitted {
			t.Fatal("original mutation identity changed after recovery")
		}
		readFront := msgr.CommandMessage(true, fsid, []string{string(read.JSON)}, nil).Front
		observed := requireMonOutcomeWrite(t, probe, readFront, nil)
		if observed.Type != msgr.MgrCommandMessage || observed.Connection == submitted.Connection || observed.Peer != "mgr."+recoveryName {
			t.Fatal("recovery read did not use the replacement MGR connection", observed.Peer, recoveryName)
		}
		identity.Recovery, identity.RecoveryGlobalID = &observed, c.snapshotAuth().GlobalID
		t.Logf("recovery CRC MGR read: peer=%s connection=%d client_global_id=%d tid=%d sequence=%d; original local mutation writes=1",
			observed.Peer, observed.Connection, identity.RecoveryGlobalID, observed.Transaction, observed.Sequence)
	} else {
		t.Log("secure mode: native state and padded local write count checked; CRC transaction receipt is unavailable")
	}
	writeMgrOutcomeIdentity(t, control, identity)
	t.Logf("MGR %s (gid %d) applied %s; independent overwrite %s confirmed natively; replacement %s (gid %d) kept %s; padded local writes=%d (not a server execution count)",
		name, originalGID, wanted, original, recoveryName, recoveryGID, original, writes.Load())
}
