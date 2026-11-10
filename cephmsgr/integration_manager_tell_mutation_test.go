package cephmsgr

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

func goMgrTellValue(t *testing.T, ctx context.Context, c *Client, target string) int64 {
	t.Helper()
	result, err := c.MgrTellTo(ctx, target, monTellGetCommand())
	value, ok := parseMonTellEffective(result.Data)
	if err != nil || result.Code != 0 || !ok {
		t.Fatal("read the daemon-local option through an exact-name MGR Tell", err, result.Code, string(result.Data))
	}
	return value
}

// waitGoManager waits, on MgrMap changes only, until the client's
// authenticated MgrMap shows exactly the wanted available active daemon.
func waitGoManager(t *testing.T, ctx context.Context, c *Client, want nativeMgrTellDaemon) {
	t.Helper()
	for {
		c.mu.Lock()
		m, changed := c.mgrMap, c.changed
		c.mu.Unlock()
		if m.Available && m.Name == want.name && m.GlobalID == want.gid {
			return
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal("the Go MgrMap never showed the natively observed active manager", m.Name, m.GlobalID, want, ctx.Err())
		}
	}
}

// One applied MgrTellTo whose reply is withheld. The daemon-local runtime
// option lives only in the named manager process, which stays the available
// active manager throughout: only the subject connection ends. A is observed
// on that daemon's own admin socket while the Go caller is still pending, a
// native overwrite B then makes any replay of A visible, and the caller keeps
// its unknown outcome. The command schema stays in test files.
func TestCephAppliedNamedManagerTellIsNotReplayedIntegration(t *testing.T) {
	options := integrationOptions(t)
	observer, ctx := fixtureClient(t, 3*time.Minute)
	if options.ConnectionMode != CRCMode {
		t.Skip("the transaction-specific Tell reply gate decodes uncompressed CRC frames; this cell establishes no secure-mode attribution")
	}
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	// Both owned managers must be registered, one active and one standby, so
	// the bystander daemon and the standby-name refusal are part of every run.
	if err := testcluster.ControlDaemon(ctx, control, "prepare", "mgr-failover", "ready"); err != nil {
		t.Fatal("native active and standby MGR preparation", err)
	}
	if _, err := observer.MgrTell(ctx, Command{JSON: []byte(`{"prefix":"version","format":"json"}`)}); err != nil {
		t.Fatal("read the current active MGR", err)
	}
	observer.mu.Lock()
	target := observer.mgrMap.Name
	observer.mu.Unlock()
	if !mgrTellTargets[target] {
		t.Fatal("the active MGR is not an owned fixture daemon", target)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	token := fmt.Sprintf("%x", nonce)
	// Baseline effective value and runtime override presence come from the
	// daemon's own admin socket before any mutation, bracketed by fresh native
	// MgrMap reads. The effective value alone cannot show whether an override
	// exists.
	initial, err := observeNativeMgrTell(ctx, control, token, "initial", target)
	if err != nil {
		t.Fatal("native baseline observation", err)
	}
	t.Log(nativeMgrTellSummary(initial))
	daemon, stable := nativeMgrTellBracket(initial.Views)
	baseline, readable := nativeMgrTellState(initial.Views)
	if initial.Baseline == nil || !stable || !readable || daemon.name != target || initial.Baseline.Present != (baseline.override != nil) {
		t.Fatal("native baseline is not one restorable bracketed daemon-local state of the active manager")
	}
	original := baseline.value
	a, b, err := monTellOutcomeValues(original)
	if err != nil {
		t.Fatal("refusing the candidate outside its bound", err)
	}
	standbys := initial.Views.ManagerBefore.Standbys
	if len(standbys) != 1 || standbys[0] == target || !mgrTellTargets[standbys[0]] {
		t.Fatal("the native MgrMap does not show exactly one owned standby", standbys)
	}
	standby := standbys[0]
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		receipt, err := observeNativeMgrTell(cleanup, control, token, "restore", target)
		if err != nil {
			t.Error("native restoration did not return; its outcome remains uncertain and is not retried", err)
			return
		}
		if receipt.Mutation == nil || receipt.Mutation.Action != "restore" || !receipt.Mutation.OK {
			t.Error("native restoration failed or is uncertain; it is not retried", nativeMgrTellSummary(receipt))
			return
		}
		t.Logf("native restoration submitted once (baseline override present=%t): %s", initial.Baseline.Present, nativeMgrTellSummary(receipt))
		// A separate fresh read, after the restoration returned, must show both
		// the original effective value and the original provenance.
		restored, err := observeNativeMgrTell(cleanup, control, token, "restored", target)
		if err != nil {
			t.Error("fresh native restored observation", err)
			return
		}
		if !nativeMgrTellMatches(restored, daemon, baseline) {
			t.Error("fresh native views did not show the restored baseline", nativeMgrTellSummary(restored))
			return
		}
		t.Log("restored baseline:", nativeMgrTellSummary(restored))
	}()
	// The other owned manager is a bystander: its own option must not move.
	bystander, err := observeNativeMgrTell(ctx, control, token, "bystander-initial", standby)
	if err != nil {
		t.Fatal("native bystander observation", err)
	}
	bystanderState, readable := nativeMgrTellState(bystander.Views)
	if active, stable := nativeMgrTellBracket(bystander.Views); !stable || active != daemon || !readable {
		t.Fatal("native bystander observation is not bracketed by the same active manager", nativeMgrTellSummary(bystander))
	}
	t.Log("bystander:", nativeMgrTellSummary(bystander))
	waitGoManager(t, ctx, observer, daemon)
	if got := goMgrTellValue(t, ctx, observer, target); got != original {
		t.Fatal("Go client and native baseline disagree before the mutation", got, original)
	}

	dial := options.DialContext
	var writes atomic.Int32
	var connectionID atomic.Uint64
	probe := &monOutcomeWrites{}
	probe.enabled.Store(true)
	gate := newTellReplyGate()
	// Every subject connection, including later dials, is observed and gated.
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		_, peer, _ := testcluster.SplitPeer(endpoint)
		id := connectionID.Add(1)
		gated := newTellReplyGateConn(conn, gate, id)
		return &commandWriteProbe{Conn: &monOutcomeConn{Conn: gated, record: probe, connection: id, peer: peer}, writes: &writes}, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	waitGoManager(t, ctx, c, daemon)
	c.mu.Lock()
	fsid := c.fsid
	c.mu.Unlock()
	role := "mgr." + target
	identity := mgrTellOutcomeIdentity{ClientGlobalID: c.snapshotAuth().GlobalID, Manager: target, ManagerGID: daemon.gid,
		Baseline: original, OverridePresent: baseline.override != nil}
	commandA := monTellSetCommand(a, 40<<10)
	frontA := msgr.TellCommand(fsid, []string{string(commandA.JSON)}, nil).Front
	gate.arm(frontA, nil)

	// Wrong-target control: the mutation addressed to the standby's exact name
	// is refused locally and writes nothing.
	if result, err := c.MgrTellTo(ctx, standby, commandA); !errors.Is(err, ErrManagerNotActive) || result.Code != 0 || len(result.Data) != 0 {
		t.Fatal("a mutation named for the standby was not refused locally", result, err)
	}
	if matching, err := probe.matching(frontA, nil); err != nil || len(matching) != 0 || writes.Load() != 0 {
		t.Fatal("the refused standby mutation was written", matching, writes.Load(), err)
	}
	// Near-miss control: a read-only Tell on the same exact-name connection
	// passes the armed gate with its own transaction. It also establishes the
	// shared MGR session before A.
	if got := goMgrTellValue(t, ctx, c, target); got != original {
		t.Fatal("the subject client read another baseline", got, original)
	}
	readFront := msgr.TellCommand(fsid, []string{string(monTellGetCommand().JSON)}, nil).Front
	nearMiss := requireMonOutcomeWrite(t, probe, readFront, nil)
	if _, _, _, bound := gate.binding(); bound || nearMiss.Type != msgr.TellCommandMessage || nearMiss.Peer != role {
		t.Fatal("the read-only Tell bound the gate or missed the named manager", bound, nearMiss.Type, nearMiss.Peer)
	}
	identity.NearMiss = &nearMiss
	t.Logf("controls before A: standby-name mutation to mgr.%s refused locally with no write; read-only Tell passed the armed gate on connection=%d tid=%d sequence=%d",
		standby, nearMiss.Connection, nearMiss.Transaction, nearMiss.Sequence)

	var originalResult Result
	finished := make(chan error, 1)
	go func() {
		var err error
		originalResult, err = c.MgrTellTo(ctx, target, commandA)
		finished <- err
	}()
	// Barrier: the daemon's reply to A's own transaction is decoded and held
	// before any byte of it reaches the Go session.
	select {
	case <-gate.held:
	case err := <-finished:
		t.Fatal("applied Tell mutation finished without a withheld reply", err)
	case <-ctx.Done():
		t.Fatal("the reply to the applied Tell mutation was never withheld", ctx.Err())
	}
	held, err := gate.heldReply()
	if err != nil {
		t.Fatal("the reply gate could not decode a subject stream", err)
	}
	submitted := requireMonOutcomeWrite(t, probe, frontA, nil)
	connection, transaction, sequence, _ := gate.binding()
	if submitted.Type != msgr.TellCommandMessage || submitted.Peer != role || submitted.Connection != connection ||
		submitted.Transaction != transaction || submitted.Sequence != sequence {
		t.Fatal("original CRC command is not the bound exact-name Tell frame", submitted, connection, transaction, sequence)
	}
	if submitted.Connection != nearMiss.Connection || submitted.Transaction == nearMiss.Transaction {
		t.Fatal("A did not use the shared exact-name session with its own transaction", submitted, nearMiss)
	}
	if held.Type != msgr.TellCommandReplyMessage || held.Connection != submitted.Connection || held.Transaction != submitted.Transaction {
		t.Fatal("the withheld frame is not the Tell reply to A's transaction", held, submitted)
	}
	if writes.Load() != 1 {
		t.Fatal("initial Tell mutation was not written exactly once", writes.Load())
	}
	identity.Submitted, identity.WithheldReply, identity.MatchingWrites, identity.PaddedWrites = &submitted, &held, 1, writes.Load()
	writeMgrTellOutcomeIdentity(t, control, identity)
	t.Logf("original CRC Tell submission: peer=%s connection=%d client_global_id=%d tid=%d sequence=%d complete_local_write=true; withheld reply type=%d tid=%d sequence=%d frame_bytes=%d",
		submitted.Peer, submitted.Connection, identity.ClientGlobalID, submitted.Transaction, submitted.Sequence, held.Type, held.Transaction, held.Sequence, held.FrameBytes)
	// A is established on the named daemon's own admin socket while its reply
	// is still withheld.
	applied := requireNativeMgrTell(t, ctx, control, token, "applied", daemon, nativeMonTellState{a, &a})
	t.Log(nativeMgrTellSummary(applied))
	// One native overwrite establishes distinct B. A replay of A would
	// overwrite it.
	overwritten := requireNativeMgrTell(t, ctx, control, token, "overwrite", daemon, nativeMonTellState{b, &b})
	if m := overwritten.Mutation; m == nil || m.Action != "overwrite" || !m.OK {
		t.Fatal("the native overwrite did not return successfully", nativeMgrTellSummary(overwritten))
	}
	t.Log(nativeMgrTellSummary(overwritten))
	select {
	case err := <-finished:
		t.Fatal("the original caller ended while its reply was withheld", err)
	default:
	}
	if writes.Load() != 1 {
		t.Fatal("more than one local padded Tell write before the subject connection ended", writes.Load())
	}
	// End only the subject connection. The manager process stays active.
	if err := gate.closeHeld(); err != nil {
		t.Fatal("close the subject connection", err)
	}
	select {
	case err := <-finished:
		var unknown *OutcomeUnknownError
		if !errors.As(err, &unknown) {
			t.Fatal("applied Tell mutation lost its unknown outcome when its connection ended", err)
		}
		if errors.Is(err, ErrManagerChanged) {
			t.Fatal("connection loss with an unchanged manager was reported as a target change", err)
		}
		if originalResult.Code != 0 || originalResult.Message != "" || len(originalResult.Data) != 0 {
			t.Fatal("unanswered Tell unexpectedly gained an acknowledged server result", originalResult)
		}
		identity.OutcomeUnknown = true
		writeMgrTellOutcomeIdentity(t, control, identity)
		t.Log("original caller retained OutcomeUnknownError; later observations do not acknowledge it:", err)
	case <-ctx.Done():
		t.Fatal("ending the subject connection did not release the pending Tell", ctx.Err())
	}
	// A new read-only invocation on the same client opens another session to
	// the same manager and reads B. Nothing resubmits A.
	if got := goMgrTellValue(t, ctx, c, target); got != b {
		t.Fatal("a fresh session on the subject client did not read B", got, b)
	}
	if got := goMgrTellValue(t, ctx, observer, target); got != b {
		t.Fatal("an independent Go session did not read B", got, b)
	}
	recovered := requireNativeMgrTell(t, ctx, control, token, "recovered", daemon, nativeMonTellState{b, &b})
	t.Log(nativeMgrTellSummary(recovered))
	c.mu.Lock()
	after := nativeMgrTellDaemon{c.mgrMap.Name, c.mgrMap.GlobalID}
	c.mu.Unlock()
	if after != daemon {
		t.Fatal("the Go MgrMap's active manager changed during the cell", after, daemon)
	}
	// Across every subject connection there is still exactly one A, with its
	// original binding, and the recovery read used another connection.
	if again := requireMonOutcomeWrite(t, probe, frontA, nil); again != submitted || writes.Load() != 1 {
		t.Fatal("original Tell identity changed or A was written again", again, submitted, writes.Load())
	}
	reads, err := probe.matching(readFront, nil)
	if err != nil || len(reads) != 2 || reads[0] != nearMiss {
		t.Fatal("expected the near-miss read and one recovery read from the subject client", reads, err)
	}
	observed := reads[1]
	if observed.Type != msgr.TellCommandMessage || observed.Peer != role || observed.Connection == submitted.Connection || !observed.Complete {
		t.Fatal("fresh read did not use another exact-name Tell connection", observed, submitted)
	}
	if now, err := gate.heldReply(); err != nil || now != held {
		t.Fatal("the gate withheld another frame or lost a subject stream", now, held, err)
	}
	identity.Recovery, identity.RecoveryGlobalID, identity.PaddedWrites = &observed, c.snapshotAuth().GlobalID, writes.Load()
	writeMgrTellOutcomeIdentity(t, control, identity)
	t.Logf("fresh CRC Tell read: peer=%s connection=%d tid=%d sequence=%d; original local mutation writes=1", observed.Peer, observed.Connection, observed.Transaction, observed.Sequence)
	// The bystander manager's own option did not move.
	final, err := observeNativeMgrTell(ctx, control, token, "bystander-final", standby)
	if err != nil {
		t.Fatal("native bystander observation", err)
	}
	finalState, readable := nativeMgrTellState(final.Views)
	if active, stable := nativeMgrTellBracket(final.Views); !stable || active != daemon || !readable || !sameMonTellState(finalState, bystanderState) {
		t.Fatal("the bystander manager's option changed or the active manager moved", nativeMgrTellSummary(bystander), nativeMgrTellSummary(final))
	}
	t.Log("bystander:", nativeMgrTellSummary(final))
	t.Logf("%s (gid %d) applied %d; native overwrite %d confirmed on the same daemon; fresh Go and native reads kept %d; bystander mgr.%s unchanged; padded local writes=%d (not a server execution count)",
		role, daemon.gid, a, b, b, standby, writes.Load())
}
