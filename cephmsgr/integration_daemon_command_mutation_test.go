package cephmsgr

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// daemonCommandHandle is one explicitly targeted daemon connection of a
// client. command returns the raw result and the caller's retained submission
// transaction.
type daemonCommandHandle struct {
	command func(context.Context, Command) (Result, uint64, error)
	close   func()
}

// daemonCommandCell describes one applied command whose reply is withheld on
// an explicitly targeted daemon connection.
type daemonCommandCell struct {
	api    string
	target nativeDaemonCommandTarget
	// endpoint is the daemon's "<ip:port>"; address adds "/<nonce>" and must
	// equal the natively observed address of the target daemon.
	endpoint, address string
	// options are the subject client's options before its connections are
	// observed; the independent Go reader uses them unchanged.
	options Options
	open    func(context.Context, *Client) (daemonCommandHandle, error)
	// values returns the distinct states A (applied by Go) and B (the native
	// overwrite) for a bounded baseline.
	values func(baseline nativeMonTellState) (a, b nativeMonTellState, err error)
	// mutation is the padded command that establishes a; read is the read-only
	// command whose raw output value decodes.
	mutation func(a nativeMonTellState) Command
	read     Command
	value    func(data []byte) (int64, bool)
}

func optionDaemonCommandValues(baseline nativeMonTellState) (nativeMonTellState, nativeMonTellState, error) {
	a, b, err := monTellOutcomeValues(baseline.value)
	return nativeMonTellState{a, &a}, nativeMonTellState{b, &b}, err
}

func goDaemonCommandValue(t *testing.T, ctx context.Context, cell daemonCommandCell, handle daemonCommandHandle) int64 {
	t.Helper()
	result, _, err := handle.command(ctx, cell.read)
	value, ok := cell.value(result.Data)
	if err != nil || result.Code != 0 || !ok {
		t.Fatal("read the owned state through the command channel under test", cell.api, err, result.Code, len(result.Data))
	}
	return value
}

// appliedDaemonCommandIsNotReplayed runs one cell in either connection mode.
// A is observed on the daemon's own admin socket while the Go caller is still
// pending; a native overwrite B then makes any replay of A visible; only the
// subject connection ends and the caller keeps its unknown outcome. The daemon
// process is the same incarnation throughout. Command schemas stay in test
// files.
//
// CRC: the reply to A's own transaction is decoded and held, so the applied
// state is read once behind that barrier and every local write is identified
// by connection, transaction and sequence.
//
// Secure: frames cannot be decoded. Every byte received on the daemon
// connection that carried the one padded write is withheld from before that
// write leaves, so no reply to it can reach the caller; the applied state is
// awaited natively, bytes are then required to have arrived and been withheld
// on that connection, and the caller is checked to be still pending. Local
// transmission is counted by padded writes only: no write, transaction or
// reply frame is identified in this mode.
func appliedDaemonCommandIsNotReplayed(t *testing.T, ctx context.Context, cell daemonCommandCell) {
	decoded := cell.options.ConnectionMode == CRCMode
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	role := cell.target.daemon()
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	token := fmt.Sprintf("%x", nonce)
	// Baseline state comes from the daemon's own admin socket before any
	// mutation, bracketed by fresh native map reads of the daemon.
	initial, err := observeNativeDaemonCommand(ctx, control, token, "initial", cell.target)
	if err != nil {
		t.Fatal("native baseline observation", err)
	}
	t.Log(nativeDaemonCommandSummary(initial))
	daemon, stable := nativeDaemonCommandBracket(initial.Views)
	baseline, readable := nativeDaemonCommandState(cell.target, initial.Views)
	if !initial.Restorable || !stable || !readable || daemon.daemon != role {
		t.Fatal("native baseline is not one restorable bracketed state of the target daemon")
	}
	if initial.Baseline != nil && initial.Baseline.Present != (baseline.override != nil) || initial.Baseline == nil && cell.target.Kind != "pg" {
		t.Fatal("native baseline provenance disagrees with its recorded baseline")
	}
	// The address the Go client dials, nonce included, is the natively observed
	// address of the daemon whose admin socket the observer reads.
	if daemon.address != cell.address {
		t.Fatalf("the Go target address is not the natively observed %s address", role)
	}
	a, b, err := cell.values(baseline)
	if err != nil {
		t.Fatal("refusing the candidate outside its bound", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		receipt, err := observeNativeDaemonCommand(cleanup, control, token, "restore", cell.target)
		if err != nil {
			t.Error("native restoration did not return; its outcome remains uncertain and is not retried", err)
			return
		}
		if receipt.Mutation != nil && receipt.Mutation.Refused {
			t.Error("native restoration was refused and not sent: the daemon incarnation that supplied the baseline is gone or unverifiable", nativeDaemonCommandSummary(receipt))
			return
		}
		if !receipt.Mutation.applied("restore") {
			t.Error("native restoration failed, is uncertain or reached another daemon incarnation; it is not retried", nativeDaemonCommandSummary(receipt))
			return
		}
		t.Log("native restoration submitted once:", nativeDaemonCommandSummary(receipt))
		// A separate fresh read, after the restoration returned, must show both
		// the original value and the original provenance.
		restored, err := observeNativeDaemonCommand(cleanup, control, token, "restored", cell.target)
		if err != nil {
			t.Error("fresh native restored observation", err)
			return
		}
		if !nativeDaemonCommandMatches(restored, cell.target, daemon, baseline) {
			t.Error("fresh native views did not show the restored baseline", nativeDaemonCommandSummary(restored))
			return
		}
		t.Log("restored baseline:", nativeDaemonCommandSummary(restored))
	}()
	// An independent Go client reads the same baseline through the channel.
	reader, err := Dial(ctx, cell.options)
	if err != nil {
		t.Fatal("dial the independent reader", err)
	}
	defer reader.Close()
	readerHandle, err := cell.open(ctx, reader)
	if err != nil {
		t.Fatal("open the independent reader's daemon connection", err)
	}
	defer readerHandle.close()
	if got := goDaemonCommandValue(t, ctx, cell, readerHandle); got != baseline.value {
		t.Fatal("Go client and native baseline disagree before the mutation", got, baseline.value)
	}

	options := cell.options
	dial := options.DialContext
	var writes atomic.Int32
	var connectionID atomic.Uint64
	probe := &monOutcomeWrites{}
	probe.enabled.Store(decoded)
	gate := newTellReplyGate()
	padded := newPaddedReplyGate(4096, func(peer string) bool { return peer == role })
	var peersMu sync.Mutex
	peers := map[uint64]string{}
	// Every subject connection, including later dials, is observed and gated.
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		_, peer, _ := testcluster.SplitPeer(endpoint)
		if endpoint == cell.endpoint {
			peer = role
		}
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
	handle, err := cell.open(ctx, c)
	if err != nil {
		t.Fatal("open the subject daemon connection", err)
	}
	defer func() { handle.close() }()
	c.mu.Lock()
	fsid := c.fsid
	c.mu.Unlock()
	identity := daemonCommandOutcomeIdentity{API: cell.api, FramesDecoded: decoded, Target: cell.target, ClientGlobalID: c.snapshotAuth().GlobalID,
		Incarnation: daemon.incarnation, Address: daemon.address, Baseline: baseline.value, OverridePresent: baseline.override != nil}
	commandA := cell.mutation(a)
	frontA := msgr.TellCommand(fsid, []string{string(commandA.JSON)}, nil).Front
	readFront := msgr.TellCommand(fsid, []string{string(cell.read.JSON)}, nil).Front
	if decoded {
		gate.arm(frontA, nil)
	}
	// Near-miss control: a read-only command on the same connection passes the
	// armed gate before A.
	if got := goDaemonCommandValue(t, ctx, cell, handle); got != baseline.value {
		t.Fatal("the subject client read another baseline", got, baseline.value)
	}
	var nearMiss monOutcomeWrite
	if decoded {
		nearMiss = awaitMonOutcomeWrite(t, ctx, probe, readFront, nil)
		if _, _, _, bound := gate.binding(); bound || nearMiss.Type != msgr.TellCommandMessage || nearMiss.Peer != role || writes.Load() != 0 {
			t.Fatal("the read-only command bound the gate, missed the target daemon or reached the padded size", bound, nearMiss.Type, nearMiss.Peer, writes.Load())
		}
		identity.NearMiss = &nearMiss
		t.Logf("control before A: read-only command passed the armed gate on connection=%d tid=%d sequence=%d", nearMiss.Connection, nearMiss.Transaction, nearMiss.Sequence)
	} else if _, _, bound := padded.binding(); bound || writes.Load() != 0 {
		t.Fatal("a write before A reached the padded size, so padded writes cannot identify A", bound, writes.Load())
	}

	var originalResult Result
	var originalTransaction uint64
	finished := make(chan error, 1)
	go func() {
		var err error
		originalResult, originalTransaction, err = handle.command(ctx, commandA)
		finished <- err
	}()
	var submitted monOutcomeWrite
	var held tellHeldReply
	var heldConnection uint64
	if decoded {
		// Barrier: the daemon's reply to A's own transaction is decoded and
		// held before any byte of it reaches the Go session.
		select {
		case <-gate.held:
		case err := <-finished:
			t.Fatal("applied command finished without a withheld reply", err)
		case <-ctx.Done():
			t.Fatal("the reply to the applied command was never withheld", ctx.Err())
		}
		if held, err = gate.heldReply(); err != nil {
			t.Fatal("the reply gate could not decode a subject stream", err)
		}
		// The held reply does not order A's local write completion: its
		// record is published by the writer after the write returned.
		submitted = awaitMonOutcomeWrite(t, ctx, probe, frontA, nil)
		connection, transaction, sequence, _ := gate.binding()
		if submitted.Type != msgr.TellCommandMessage || submitted.Peer != role || submitted.Connection != connection ||
			submitted.Transaction != transaction || submitted.Sequence != sequence {
			t.Fatal("original CRC command is not the bound command frame to the target daemon", submitted, connection, transaction, sequence)
		}
		if submitted.Connection != nearMiss.Connection || submitted.Transaction == nearMiss.Transaction {
			t.Fatal("A did not use the same daemon connection with its own transaction", submitted, nearMiss)
		}
		if held.Type != msgr.TellCommandReplyMessage || held.Connection != submitted.Connection || held.Transaction != submitted.Transaction {
			t.Fatal("the withheld frame is not the command reply to A's transaction", held, submitted)
		}
		heldConnection = submitted.Connection
		identity.Submitted, identity.WithheldReply, identity.MatchingWrites = &submitted, &held, 1
	} else {
		// The padded write is issued: from before it left, every byte received
		// on its connection is withheld.
		select {
		case <-padded.armed:
		case err := <-finished:
			t.Fatal("applied command finished without a padded write", err)
		case <-ctx.Done():
			t.Fatal("the padded command was never written", ctx.Err())
		}
		var receiver string
		if heldConnection, receiver, _ = padded.binding(); receiver != role {
			t.Fatalf("A was written to %q, not to the target daemon %s", receiver, role)
		}
	}
	identity.HeldConnection = heldConnection
	var applied nativeDaemonCommandReceipt
	if decoded {
		// A is established on the target daemon's own admin socket while its
		// reply is still withheld.
		applied = requireNativeDaemonCommand(t, ctx, control, token, "applied", cell.target, daemon, a)
	} else {
		applied = awaitNativeDaemonCommand(t, ctx, control, token, "applied", cell.target, daemon, a, baseline, finished)
		// The daemon sent something after the padded write and none of it
		// reached the session. The bytes are not attributed to a reply.
		select {
		case <-padded.received:
			identity.WithheldBytes = true
		case err := <-finished:
			t.Fatal("the original caller ended before any received byte was withheld", err)
		case <-ctx.Done():
			t.Fatal("no byte was received and withheld on the padded write's connection", ctx.Err())
		}
	}
	if count := awaitPaddedWrites(t, ctx, &writes, 1); count != 1 {
		t.Fatal("initial mutation was not written exactly once", count)
	}
	identity.PaddedWrites = writes.Load()
	writeDaemonCommandOutcomeIdentity(t, control, identity)
	if decoded {
		t.Logf("original CRC command submission: api=%s peer=%s connection=%d client_global_id=%d tid=%d sequence=%d complete_local_write=true; withheld reply type=%d tid=%d sequence=%d frame_bytes=%d",
			cell.api, submitted.Peer, submitted.Connection, identity.ClientGlobalID, submitted.Transaction, submitted.Sequence, held.Type, held.Transaction, held.Sequence, held.FrameBytes)
	} else {
		t.Logf("original secure command submission: api=%s peer=%s connection=%d client_global_id=%d padded_local_writes=1; every received byte withheld from before the write, and bytes were withheld; no transaction or reply attribution",
			cell.api, role, heldConnection, identity.ClientGlobalID)
	}
	t.Log(nativeDaemonCommandSummary(applied))
	// One native overwrite establishes distinct B. A replay of A would
	// overwrite it.
	overwritten := requireNativeDaemonCommand(t, ctx, control, token, "overwrite", cell.target, daemon, b)
	if !overwritten.Mutation.applied("overwrite") {
		t.Fatal("the native overwrite was refused, did not return successfully or reached another daemon incarnation", nativeDaemonCommandSummary(overwritten))
	}
	t.Log(nativeDaemonCommandSummary(overwritten))
	select {
	case err := <-finished:
		t.Fatal("the original caller ended while its reply was withheld", err)
	default:
	}
	if writes.Load() != 1 {
		t.Fatal("more than one local padded write before the subject connection ended", writes.Load())
	}
	// End only the subject connection. The daemon process stays up.
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
			t.Fatal("applied command lost its unknown outcome when its connection ended", err)
		}
		if originalResult.Code != 0 || originalResult.Message != "" || len(originalResult.Data) != 0 {
			t.Fatal("unanswered command unexpectedly gained an acknowledged server result", originalResult)
		}
		// The caller's retained submission names the transaction that was
		// actually written. Without decoded frames it can only be required to
		// exist: it is not compared with any frame.
		if decoded && originalTransaction != submitted.Transaction || originalTransaction == 0 {
			t.Fatal("the caller's retained submission is not the written transaction", originalTransaction, submitted.Transaction)
		}
		identity.OutcomeUnknown, identity.CallerTransaction = true, originalTransaction
		writeDaemonCommandOutcomeIdentity(t, control, identity)
		t.Log("original caller retained OutcomeUnknownError; later observations do not acknowledge it:", err)
	case <-ctx.Done():
		t.Fatal("ending the subject connection did not release the pending command", ctx.Err())
	}
	// The ended connection does not recover by itself. A new read-only
	// invocation on an explicitly reopened connection of the same client reads
	// B. Nothing resubmits A.
	handle.close()
	reopened, err := cell.open(ctx, c)
	if err != nil {
		t.Fatal("explicitly reopen the subject daemon connection", err)
	}
	handle = reopened
	if got := goDaemonCommandValue(t, ctx, cell, handle); got != b.value {
		t.Fatal("a fresh connection of the subject client did not read B", got, b.value)
	}
	if got := goDaemonCommandValue(t, ctx, cell, readerHandle); got != b.value {
		t.Fatal("an independent Go client did not read B", got, b.value)
	}
	recovered := requireNativeDaemonCommand(t, ctx, control, token, "recovered", cell.target, daemon, b)
	t.Log(nativeDaemonCommandSummary(recovered))
	if writes.Load() != 1 {
		t.Fatal("A was written again", writes.Load())
	}
	if decoded {
		// Across every subject connection there is still exactly one A, with
		// its original binding, and the recovery read used another connection.
		if again := requireMonOutcomeWrite(t, probe, frontA, nil); again != submitted {
			t.Fatal("original command identity changed or A was written again", again, submitted)
		}
		// The recovery read has the near-miss's content, and its reply does
		// not order its own local write completion either: wait for the
		// second record, then require exactly two.
		reads := awaitMonOutcomeWrites(t, ctx, probe, readFront, nil, 2)
		if len(reads) != 2 || reads[0] != nearMiss {
			t.Fatal("expected the near-miss read and one recovery read from the subject client", reads)
		}
		observed := reads[1]
		if observed.Type != msgr.TellCommandMessage || observed.Peer != role || observed.Connection == submitted.Connection || !observed.Complete {
			t.Fatal("fresh read did not use another connection to the target daemon", observed, submitted)
		}
		if now, err := gate.heldReply(); err != nil || now != held {
			t.Fatal("the gate withheld another frame or lost a subject stream", now, held, err)
		}
		identity.Recovery, identity.RecoveryConnection = &observed, observed.Connection
		t.Logf("fresh CRC command read: peer=%s connection=%d tid=%d sequence=%d; original local mutation writes=1", observed.Peer, observed.Connection, observed.Transaction, observed.Sequence)
	} else {
		// The recovery read cannot have used the withheld connection, so the
		// target daemon was dialed again, and the gate still holds only the
		// connection of the one padded write. A dial is recorded when it
		// succeeds, before any handshake or command: which of the later
		// dials carried the read is not known here and not claimed.
		peersMu.Lock()
		later := laterDials(peers, role, heldConnection)
		peersMu.Unlock()
		if connection, peer, _ := padded.binding(); len(later) == 0 || connection != heldConnection || peer != role {
			t.Fatal("the target daemon was not dialed again for the fresh read, or the gate was rebound", later, connection, peer)
		}
		identity.LaterDials = later
		t.Logf("fresh secure command read: peer=%s later dials=%v; padded local writes=1; the read's connection and transaction are not identified", role, later)
	}
	identity.PaddedWrites = writes.Load()
	writeDaemonCommandOutcomeIdentity(t, control, identity)
	t.Logf("%s to %s (incarnation %d) applied state %d; native overwrite %d confirmed on the same daemon; fresh Go and native reads kept %d; padded local writes=%d (not a server execution count)",
		cell.api, role, daemon.incarnation, a.value, b.value, b.value, writes.Load())
}

// osdCommandFixture resolves, through fresh MON reads of the test client, the
// acting primary of the fixture's object PG and that OSD's v2 address. The
// caller still supplies every target explicitly; this is test-only routing.
type osdCommandFixture struct {
	options  Options
	target   OSDTarget
	endpoint string
	pg       OSDCommandPG
	pgid     string
}

func osdCommandOutcomeFixture(t *testing.T) (osdCommandFixture, context.Context) {
	t.Helper()
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	profile := os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE")
	if os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || control == "" || profile != "" && profile != "legacy-v6" {
		t.Skip("requires the opt-in disposable legacy-v6 OSD fixture")
	}
	options := integrationOptions(t)
	options.EnableOSD = true
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	var oracle struct {
		Pool int64  `json:"pool_id"`
		Seed uint32 `json:"pg_seed"`
		PGID string `json:"pgid"`
	}
	if err := readNativeReceiptFile(filepath.Join(control, "osd-oracle.json"), 1<<20, errors.New("OSD fixture oracle exceeds its bound"), &oracle); err != nil {
		t.Fatal("read the OSD fixture oracle", err)
	}
	fixture := osdCommandFixture{options: options, pgid: fmt.Sprintf("%d.%x", oracle.Pool, oracle.Seed)}
	if oracle.Pool < 0 || fixture.pgid != oracle.PGID || !daemonCommandPGPattern.MatchString(fixture.pgid) {
		t.Fatal("the OSD fixture oracle names no canonical PG", oracle.PGID)
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	mapped, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"pg map","pgid":"` + fixture.pgid + `","format":"json"}`)})
	var placement struct {
		Epoch  uint32  `json:"epoch"`
		Acting []int32 `json:"acting"`
	}
	if err != nil || json.Unmarshal(mapped.Data, &placement) != nil || placement.Epoch == 0 || len(placement.Acting) == 0 || placement.Acting[0] < 0 {
		t.Fatal("native pg map of the fixture PG", err, string(mapped.Data))
	}
	primary := placement.Acting[0]
	dump, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"osd dump","format":"json"}`)})
	var osds struct {
		OSDs []struct {
			ID     int32 `json:"osd"`
			Public struct {
				Addrvec []struct {
					Type  string `json:"type"`
					Addr  string `json:"addr"`
					Nonce uint64 `json:"nonce"`
				} `json:"addrvec"`
			} `json:"public_addrs"`
		} `json:"osds"`
	}
	if err != nil || json.Unmarshal(dump.Data, &osds) != nil {
		t.Fatal("native osd dump", err)
	}
	for _, osd := range osds.OSDs {
		for _, address := range osd.Public.Addrvec {
			if osd.ID == primary && address.Type == "v2" && fixture.endpoint == "" {
				fixture.endpoint = address.Addr
				fixture.target = OSDTarget{ID: primary, Address: "v2:" + address.Addr + "/" + strconv.FormatUint(address.Nonce, 10)}
			}
		}
	}
	if fixture.endpoint == "" {
		t.Fatal("native osd dump lacks the primary's v2 address", primary)
	}
	fixture.pg = OSDCommandPG{Pool: uint64(oracle.Pool), Seed: oracle.Seed, Primary: primary, MapEpoch: placement.Epoch}
	return fixture, ctx
}

func (f osdCommandFixture) cell(kind string) daemonCommandCell {
	cell := daemonCommandCell{target: nativeDaemonCommandTarget{Kind: kind, ID: strconv.FormatInt(int64(f.target.ID), 10)},
		endpoint: f.endpoint, address: strings.TrimPrefix(f.target.Address, "v2:"), options: f.options}
	if kind == "pg" {
		cell.target.PGID = f.pgid
	}
	cell.open = func(ctx context.Context, c *Client) (daemonCommandHandle, error) {
		o, err := c.OpenOSD(ctx, f.target)
		if err != nil {
			return daemonCommandHandle{}, err
		}
		return daemonCommandHandle{close: func() { o.Close() }, command: func(ctx context.Context, command Command) (Result, uint64, error) {
			var result OSDCommandResult
			var err error
			if kind == "pg" {
				result, err = o.PGCommand(ctx, f.pg, command)
			} else {
				result, err = o.Command(ctx, command)
			}
			return result.Result, result.Submission.Transaction, err
		}}, nil
	}
	return cell
}

// One applied OSDConnection.Command whose reply is withheld: the daemon-local
// runtime option of the explicitly targeted OSD process, the same reversible
// command and bound as the MON and MGR Tell cells.
func TestCephAppliedOSDCommandIsNotReplayedIntegration(t *testing.T) {
	fixture, ctx := osdCommandOutcomeFixture(t)
	cell := fixture.cell("osd")
	cell.api, cell.values, cell.read, cell.value = "OSDConnection.Command", optionDaemonCommandValues, monTellGetCommand(), parseMonTellEffective
	cell.mutation = func(a nativeMonTellState) Command { return monTellSetCommand(a.value, 40<<10) }
	appliedDaemonCommandIsNotReplayed(t, ctx, cell)
}

// pgScrubSessionsCommand is the one reversible PG-routed state change of the
// pinned OSD: the scrubber's debug flag that lists its session counter in the
// primary's query output.
func pgScrubSessionsCommand(pgid string, padding int) Command {
	encoded, _ := json.Marshal(map[string]string{"prefix": "scrubdebug", "pgid": pgid, "cmd": "set", "value": "sessions"})
	return Command{JSON: append(encoded, []byte(strings.Repeat(" ", padding))...)}
}

// parsePGScrubSessions decodes a raw PG query reply of the given PG: 1 when
// the active primary's scrubber section lists the counter, 0 when it does not.
func parsePGScrubSessions(pgid string, data []byte) (int64, bool) {
	var reply struct {
		Info struct {
			PGID string `json:"pgid"`
		} `json:"info"`
		Scrubber map[string]json.RawMessage `json:"scrubber"`
	}
	if json.Unmarshal(data, &reply) != nil || reply.Info.PGID != pgid || reply.Scrubber == nil {
		return 0, false
	}
	if _, listed := reply.Scrubber["test_sequence"]; listed {
		return 1, true
	}
	return 0, true
}

// One applied OSDConnection.PGCommand whose reply is withheld. PG-routed
// commands of the pinned OSD have exactly one reversible, synchronously
// applied state: the scrubber's "sessions" debug flag. A sets it, the native
// overwrite B unsets it again, so a replay of A would list the counter again.
func TestCephAppliedPGCommandIsNotReplayedIntegration(t *testing.T) {
	fixture, ctx := osdCommandOutcomeFixture(t)
	cell := fixture.cell("pg")
	cell.api = "OSDConnection.PGCommand"
	cell.values = func(baseline nativeMonTellState) (nativeMonTellState, nativeMonTellState, error) {
		if baseline.value != 0 || baseline.override != nil {
			return nativeMonTellState{}, nativeMonTellState{}, errors.New("the scrubber flag is already set")
		}
		return nativeMonTellState{value: 1}, nativeMonTellState{}, nil
	}
	cell.mutation = func(nativeMonTellState) Command { return pgScrubSessionsCommand(fixture.pgid, 40<<10) }
	cell.read = Command{JSON: []byte(`{"prefix":"query","pgid":"` + fixture.pgid + `"}`)}
	cell.value = func(data []byte) (int64, bool) { return parsePGScrubSessions(fixture.pgid, data) }
	appliedDaemonCommandIsNotReplayed(t, ctx, cell)
}

// One applied MDSConnection.Command whose reply is withheld: the daemon-local
// runtime option of the explicitly targeted active MDS process.
func TestCephAppliedMDSCommandIsNotReplayedIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if os.Getenv("CEPH_MSGR_TEST_MDS_COMMANDS") != "1" || control == "" {
		t.Skip("set CEPH_MSGR_TEST_MDS_COMMANDS=1 for the owned raw MDS command fixture")
	}
	options := integrationOptions(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	var scope struct {
		FSCID    int32  `json:"fscid"`
		MDSGID   uint64 `json:"mds_gid"`
		Identity string `json:"client_identity"`
	}
	var fresh struct {
		FSCID int32 `json:"fscid"`
		MDS   struct {
			Name     string `json:"name"`
			GID      uint64 `json:"gid"`
			Rank     int32  `json:"rank"`
			State    string `json:"state"`
			Endpoint string `json:"endpoint"`
			Nonce    uint32 `json:"nonce"`
		} `json:"mds"`
	}
	bound := errors.New("MDS fixture oracle exceeds its bound")
	if err := readNativeReceiptFile(filepath.Join(control, "mds-commands-oracle.json"), 1<<20, bound, &scope); err != nil {
		t.Fatal(err)
	}
	if err := readNativeReceiptFile(filepath.Join(control, "mds-fresh-oracle.json"), 1<<20, bound, &fresh); err != nil {
		t.Fatal(err)
	}
	if scope.FSCID != fresh.FSCID || scope.MDSGID != fresh.MDS.GID || fresh.MDS.Rank != 0 || fresh.MDS.State != "up:active" ||
		scope.Identity == "" || scope.Identity != os.Getenv("CEPH_MSGR_MDS_COMMANDS_IDENTITY") || !daemonCommandMDSPattern.MatchString(fresh.MDS.Name) {
		t.Fatal("owned MDS command and active-daemon oracle identities differ")
	}
	encoded, err := os.ReadFile(os.Getenv("CEPH_MSGR_MDS_COMMANDS_KEY_FILE"))
	if err != nil {
		t.Fatal("read dedicated command credential", err)
	}
	options.Key, err = ParseKey(strings.TrimSpace(string(encoded)))
	clear(encoded)
	if err != nil {
		t.Fatal("parse dedicated command credential", err)
	}
	options.Identity, options.EnableMDS, options.MaxMDSConnections = scope.Identity, true, 1
	address := fmt.Sprintf("%s/%d", fresh.MDS.Endpoint, fresh.MDS.Nonce)
	target := MDSTarget{FilesystemID: scope.FSCID, Rank: 0, Address: "v2:" + address}
	cell := daemonCommandCell{api: "MDSConnection.Command", target: nativeDaemonCommandTarget{Kind: "mds", ID: fresh.MDS.Name},
		endpoint: fresh.MDS.Endpoint, address: address, options: options, values: optionDaemonCommandValues,
		read: monTellGetCommand(), value: parseMonTellEffective,
		mutation: func(a nativeMonTellState) Command { return monTellSetCommand(a.value, 40<<10) }}
	cell.open = func(ctx context.Context, c *Client) (daemonCommandHandle, error) {
		m, err := c.OpenMDS(ctx, target)
		if err != nil {
			return daemonCommandHandle{}, err
		}
		return daemonCommandHandle{close: func() { m.Close() }, command: func(ctx context.Context, command Command) (Result, uint64, error) {
			result, err := m.Command(ctx, command)
			return result.Result, result.Submission.Transaction, err
		}}, nil
	}
	appliedDaemonCommandIsNotReplayed(t, ctx, cell)
}
