package cephmsgr

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// mgrTell submits through the API under test: MgrTellTo for an exact name, or
// MgrTell, which takes its target from the client's MgrMap, for an empty one.
func mgrTell(ctx context.Context, c *Client, name string, command Command) (Result, error) {
	if name == "" {
		return c.MgrTell(ctx, command)
	}
	return c.MgrTellTo(ctx, name, command)
}

func goMgrTellValue(t *testing.T, ctx context.Context, c *Client, name string) int64 {
	t.Helper()
	result, err := mgrTell(ctx, c, name, monTellGetCommand())
	value, ok := parseMonTellEffective(result.Data)
	if err != nil || result.Code != 0 || !ok {
		t.Fatal("read the daemon-local option through a MGR Tell", name, err, result.Code, string(result.Data))
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
	appliedManagerTellIsNotReplayed(t, false)
}

// The same cell for MgrTell, whose target is the active manager of the
// client's MgrMap rather than a name the caller supplies. The native observer
// is bound to the manager whose connection actually carried the command.
func TestCephAppliedCurrentManagerTellIsNotReplayedIntegration(t *testing.T) {
	appliedManagerTellIsNotReplayed(t, true)
}

// appliedManagerTellIsNotReplayed runs the cell in either connection mode.
//
// CRC: the reply to A's own transaction is decoded and held, so the applied
// state is read once behind that barrier and every local write is identified
// by connection, transaction and sequence.
//
// Secure: frames cannot be decoded. Every byte received on the connection that
// carried the one padded write is withheld from before that write leaves, so
// no reply to it can reach the caller; the applied state is then awaited
// natively while the caller is checked to be still pending. Local transmission
// is counted by padded writes only: there is no transaction, sequence or reply
// attribution in this mode.
func appliedManagerTellIsNotReplayed(t *testing.T, current bool) {
	options := integrationOptions(t)
	observer, ctx := fixtureClient(t, 3*time.Minute)
	decoded := options.ConnectionMode == CRCMode
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
		if receipt.Mutation != nil && receipt.Mutation.Refused {
			t.Error("native restoration was refused and not sent: the daemon incarnation that supplied the baseline is gone or unverifiable", nativeMgrTellSummary(receipt))
			return
		}
		if !receipt.Mutation.applied("restore") {
			t.Error("native restoration failed, is uncertain or reached another daemon incarnation; it is not retried", nativeMgrTellSummary(receipt))
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

	// subject is the name the caller under test supplies: none for MgrTell.
	subject, api := target, "MgrTellTo"
	if current {
		subject, api = "", "MgrTell"
	}
	dial := options.DialContext
	var writes atomic.Int32
	var connectionID atomic.Uint64
	probe := &monOutcomeWrites{}
	probe.enabled.Store(decoded)
	gate := newTellReplyGate()
	padded := newPaddedReplyGate(4096, func(peer string) bool { return strings.HasPrefix(peer, "mgr.") })
	var peersMu sync.Mutex
	peers := map[uint64]string{}
	// Every subject connection, including later dials, is observed and gated.
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		_, peer, _ := testcluster.SplitPeer(endpoint)
		id := connectionID.Add(1)
		peersMu.Lock()
		peers[id] = peer
		peersMu.Unlock()
		var gated net.Conn
		if decoded {
			gated = newTellReplyGateConn(conn, gate, id)
		} else {
			gated = padded.wrap(conn, id, peer)
		}
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
	identity := mgrTellOutcomeIdentity{API: api, FramesDecoded: decoded, ClientGlobalID: c.snapshotAuth().GlobalID, Manager: target, ManagerGID: daemon.gid,
		Baseline: original, OverridePresent: baseline.override != nil}
	writeIdentity := func() { writeMgrTellOutcomeIdentity(t, control, current, identity) }
	commandA := monTellSetCommand(a, 40<<10)
	frontA := msgr.TellCommand(fsid, []string{string(commandA.JSON)}, nil).Front
	readFront := msgr.TellCommand(fsid, []string{string(monTellGetCommand().JSON)}, nil).Front
	if decoded {
		gate.arm(frontA, nil)
	}

	if !current {
		// Wrong-target control: the mutation addressed to the standby's exact
		// name is refused locally and writes nothing.
		if result, err := c.MgrTellTo(ctx, standby, commandA); !errors.Is(err, ErrManagerNotActive) || result.Code != 0 || len(result.Data) != 0 {
			t.Fatal("a mutation named for the standby was not refused locally", result, err)
		}
		if matching, err := probe.matching(frontA, nil); err != nil || len(matching) != 0 || writes.Load() != 0 {
			t.Fatal("the refused standby mutation was written", matching, writes.Load(), err)
		}
		t.Logf("control before A: standby-name mutation to mgr.%s refused locally with no write", standby)
	}
	// Near-miss control: a read-only Tell through the same API passes the gate
	// before A. It also establishes the shared MGR session.
	if got := goMgrTellValue(t, ctx, c, subject); got != original {
		t.Fatal("the subject client read another baseline", got, original)
	}
	var nearMiss monOutcomeWrite
	if decoded {
		nearMiss = requireMonOutcomeWrite(t, probe, readFront, nil)
		if _, _, _, bound := gate.binding(); bound || nearMiss.Type != msgr.TellCommandMessage || nearMiss.Peer != role {
			t.Fatal("the read-only Tell bound the gate or missed the active manager", bound, nearMiss.Type, nearMiss.Peer)
		}
		identity.NearMiss = &nearMiss
		t.Logf("control before A: read-only Tell passed the armed gate on connection=%d tid=%d sequence=%d", nearMiss.Connection, nearMiss.Transaction, nearMiss.Sequence)
	} else if _, _, bound := padded.binding(); bound || writes.Load() != 0 {
		t.Fatal("a write before A reached the padded size, so padded writes cannot identify A", bound, writes.Load())
	}

	var originalResult Result
	finished := make(chan error, 1)
	go func() {
		var err error
		originalResult, err = mgrTell(ctx, c, subject, commandA)
		finished <- err
	}()
	var submitted monOutcomeWrite
	var held tellHeldReply
	var heldConnection uint64
	var receiver string
	if decoded {
		// Barrier: the daemon's reply to A's own transaction is decoded and
		// held before any byte of it reaches the Go session.
		select {
		case <-gate.held:
		case err := <-finished:
			t.Fatal("applied Tell mutation finished without a withheld reply", err)
		case <-ctx.Done():
			t.Fatal("the reply to the applied Tell mutation was never withheld", ctx.Err())
		}
		if held, err = gate.heldReply(); err != nil {
			t.Fatal("the reply gate could not decode a subject stream", err)
		}
		submitted = requireMonOutcomeWrite(t, probe, frontA, nil)
		connection, transaction, sequence, _ := gate.binding()
		if submitted.Type != msgr.TellCommandMessage || submitted.Connection != connection ||
			submitted.Transaction != transaction || submitted.Sequence != sequence {
			t.Fatal("original CRC command is not the bound Tell frame", submitted, connection, transaction, sequence)
		}
		if submitted.Connection != nearMiss.Connection || submitted.Transaction == nearMiss.Transaction {
			t.Fatal("A did not use the shared MGR session with its own transaction", submitted, nearMiss)
		}
		if held.Type != msgr.TellCommandReplyMessage || held.Connection != submitted.Connection || held.Transaction != submitted.Transaction {
			t.Fatal("the withheld frame is not the Tell reply to A's transaction", held, submitted)
		}
		heldConnection, receiver = submitted.Connection, submitted.Peer
		identity.Submitted, identity.WithheldReply, identity.MatchingWrites = &submitted, &held, 1
	} else {
		// The padded write is issued: from before it left, every byte received
		// on its connection is withheld.
		select {
		case <-padded.armed:
		case err := <-finished:
			t.Fatal("applied Tell mutation finished without a padded write", err)
		case <-ctx.Done():
			t.Fatal("the padded Tell mutation was never written", ctx.Err())
		}
		heldConnection, receiver, _ = padded.binding()
	}
	// The native observer is bound to the manager whose connection carried A.
	// It must be the natively observed active manager the baseline belongs to.
	received, owned := strings.CutPrefix(receiver, "mgr.")
	if !owned || !mgrTellTargets[received] || received != target {
		t.Fatalf("A was written to %q, not to the natively observed active manager mgr.%s", receiver, target)
	}
	receiving := nativeMgrTellDaemon{received, daemon.gid}
	identity.Receiver, identity.HeldConnection = receiver, heldConnection
	var applied nativeMgrTellReceipt
	if decoded {
		// A is established on the receiving daemon's own admin socket while its
		// reply is still withheld.
		applied = requireNativeMgrTell(t, ctx, control, token, "applied", receiving, nativeMonTellState{a, &a})
	} else {
		applied = awaitNativeMgrTell(t, ctx, control, token, "applied", receiving, nativeMonTellState{a, &a}, baseline, finished)
	}
	if writes.Load() != 1 {
		t.Fatal("initial Tell mutation was not written exactly once", writes.Load())
	}
	identity.PaddedWrites = writes.Load()
	writeIdentity()
	if decoded {
		t.Logf("original CRC Tell submission: api=%s peer=%s connection=%d client_global_id=%d tid=%d sequence=%d complete_local_write=true; withheld reply type=%d tid=%d sequence=%d frame_bytes=%d",
			api, submitted.Peer, submitted.Connection, identity.ClientGlobalID, submitted.Transaction, submitted.Sequence, held.Type, held.Transaction, held.Sequence, held.FrameBytes)
	} else {
		t.Logf("original secure Tell submission: api=%s peer=%s connection=%d client_global_id=%d padded_local_writes=1; every received byte withheld from before the write; no transaction or reply attribution",
			api, receiver, heldConnection, identity.ClientGlobalID)
	}
	t.Log(nativeMgrTellSummary(applied))
	// One native overwrite establishes distinct B. A replay of A would
	// overwrite it.
	overwritten := requireNativeMgrTell(t, ctx, control, token, "overwrite", receiving, nativeMonTellState{b, &b})
	if !overwritten.Mutation.applied("overwrite") {
		t.Fatal("the native overwrite was refused, did not return successfully or reached another daemon incarnation", nativeMgrTellSummary(overwritten))
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
	closeHeld := gate.closeHeld
	if !decoded {
		closeHeld = padded.closeHeld
	}
	if err := closeHeld(); err != nil {
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
		writeIdentity()
		t.Log("original caller retained OutcomeUnknownError; later observations do not acknowledge it:", err)
	case <-ctx.Done():
		t.Fatal("ending the subject connection did not release the pending Tell", ctx.Err())
	}
	// A new read-only invocation through the same API on the same client opens
	// another session to the same manager and reads B. Nothing resubmits A.
	if got := goMgrTellValue(t, ctx, c, subject); got != b {
		t.Fatal("a fresh session on the subject client did not read B", got, b)
	}
	if got := goMgrTellValue(t, ctx, observer, target); got != b {
		t.Fatal("an independent Go session did not read B", got, b)
	}
	recovered := requireNativeMgrTell(t, ctx, control, token, "recovered", receiving, nativeMonTellState{b, &b})
	t.Log(nativeMgrTellSummary(recovered))
	c.mu.Lock()
	after := nativeMgrTellDaemon{c.mgrMap.Name, c.mgrMap.GlobalID}
	c.mu.Unlock()
	if after != daemon {
		t.Fatal("the Go MgrMap's active manager changed during the cell", after, daemon)
	}
	if writes.Load() != 1 {
		t.Fatal("A was written again", writes.Load())
	}
	if decoded {
		// Across every subject connection there is still exactly one A, with
		// its original binding, and the recovery read used another connection.
		if again := requireMonOutcomeWrite(t, probe, frontA, nil); again != submitted {
			t.Fatal("original Tell identity changed or A was written again", again, submitted)
		}
		reads, err := probe.matching(readFront, nil)
		if err != nil || len(reads) != 2 || reads[0] != nearMiss {
			t.Fatal("expected the near-miss read and one recovery read from the subject client", reads, err)
		}
		observed := reads[1]
		if observed.Type != msgr.TellCommandMessage || observed.Peer != role || observed.Connection == submitted.Connection || !observed.Complete {
			t.Fatal("fresh read did not use another Tell connection to the same manager", observed, submitted)
		}
		if now, err := gate.heldReply(); err != nil || now != held {
			t.Fatal("the gate withheld another frame or lost a subject stream", now, held, err)
		}
		identity.Recovery = &observed
		t.Logf("fresh CRC Tell read: peer=%s connection=%d tid=%d sequence=%d; original local mutation writes=1", observed.Peer, observed.Connection, observed.Transaction, observed.Sequence)
	} else {
		// The recovery read needed a later connection to the same manager, and
		// the gate still holds only the connection of the one padded write.
		later := uint64(0)
		peersMu.Lock()
		for id, peer := range peers {
			if peer == receiver && id > heldConnection {
				later = max(later, id)
			}
		}
		peersMu.Unlock()
		if connection, peer, _ := padded.binding(); later == 0 || connection != heldConnection || peer != receiver {
			t.Fatal("fresh read used no later connection to the same manager, or the gate was rebound", later, connection, peer)
		}
		identity.RecoveryConnection = later
		t.Logf("fresh secure Tell read: peer=%s connection=%d; padded local writes=1; CRC transaction receipt is unavailable", receiver, later)
	}
	identity.RecoveryGlobalID, identity.PaddedWrites = c.snapshotAuth().GlobalID, writes.Load()
	writeIdentity()
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
	t.Logf("%s to %s (gid %d) applied %d; native overwrite %d confirmed on the same daemon; fresh Go and native reads kept %d; bystander mgr.%s unchanged; padded local writes=%d (not a server execution count)",
		api, role, daemon.gid, a, b, b, standby, writes.Load())
}
