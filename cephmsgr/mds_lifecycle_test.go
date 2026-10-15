package cephmsgr

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

type mdsCommandOutcome struct {
	result MDSCommandResult
	err    error
}

func mdsStartCommand(m *MDSConnection, ctx context.Context) <-chan mdsCommandOutcome {
	done := make(chan mdsCommandOutcome, 1)
	go func() {
		r, e := m.Command(ctx, Command{JSON: []byte(`{"prefix":"raw test"}`), Input: []byte{0, 255}})
		done <- mdsCommandOutcome{r, e}
	}()
	return done
}
func mdsAwaitOutcome(t *testing.T, ctx context.Context, done <-chan mdsCommandOutcome) mdsCommandOutcome {
	t.Helper()
	select {
	case got := <-done:
		return got
	case <-ctx.Done():
		t.Fatal("MDS command did not complete", ctx.Err())
		return mdsCommandOutcome{}
	}
}
func mdsNextPeer(t *testing.T, ctx context.Context, peers <-chan *mdsTestPeer) *mdsTestPeer {
	t.Helper()
	select {
	case peer := <-peers:
		return peer
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return nil
	}
}
func mdsCheckSubmission(t *testing.T, m *MDSConnection, sent msgr.MessageData, result MDSCommandResult) {
	t.Helper()
	want := MDSCommandSubmission{Transaction: sent.Transaction, GlobalID: m.globalID, ConnectionGeneration: m.generation, Target: m.target}
	if result.Submission != want || sent.Transaction == 0 {
		t.Fatal("MDS local identity lost", result.Submission, want)
	}
}

func TestMDSTargetCacheLimitsAndExplicitNewHandle(t *testing.T) {
	c, peers, ctx, dials := mdsTestClient(t, SecureMode)
	if c.auth.Services() != cephx.ServiceAuth|cephx.ServiceMgr|cephx.ServiceMDS || c.options.EnableOSD || c.options.EnableMDSMaps || c.Snapshot().MDSTicket.Expires.IsZero() {
		t.Fatal("MDS opt-in copied unrelated services or lost ticket metadata")
	}
	target := MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/17"}
	for _, bad := range []MDSTarget{{-1, 0, target.Address}, {7, -1, target.Address}, {7, 0, "192.0.2.1:6805"}, {7, 0, "v1:192.0.2.1:6805/17"}, {7, 0, "host.invalid:6805/17"}, {7, 0, "0.0.0.0:6805/17"}, {7, 0, "[::]:6805/17"}, {7, 0, "[fe80::1%en0]:6805/17"}, {7, 0, "192.0.2.1:6805/4294967296"}} {
		if _, err := c.OpenMDS(ctx, bad); err == nil {
			t.Fatal("invalid explicit MDS target admitted", bad)
		}
	}
	if dials.Load() != 0 {
		t.Fatal("invalid MDS target dialed")
	}
	m, err := c.OpenMDS(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	peer := mdsNextPeer(t, ctx, peers)
	same := target
	same.Address = "192.0.2.1:6805/17"
	shared, err := c.OpenMDS(ctx, same)
	if err != nil || shared != m {
		t.Fatal("exact wire target not shared", err)
	}
	changed := target
	changed.Address = "192.0.2.1:6805/18"
	if _, err := c.OpenMDS(ctx, changed); !errors.Is(err, ErrMDSTargetChanged) {
		t.Fatal(err)
	}
	nextTarget := target
	nextTarget.FilesystemID = 8
	if _, err := c.OpenMDS(ctx, nextTarget); !errors.Is(err, ErrMDSConnectionLimit) {
		t.Fatal(err)
	}
	first := mdsStartCommand(m, ctx)
	sent := peer.next(t)
	peer.reply(t, sent, 0, "first", nil)
	got := mdsAwaitOutcome(t, ctx, first)
	if got.err != nil {
		t.Fatal(got.err)
	}
	mdsCheckSubmission(t, m, sent, got.result)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.Done():
	default:
		t.Fatal("closed MDS handle not terminal")
	}
	second, err := c.OpenMDS(ctx, nextTarget)
	if err != nil {
		t.Fatal(err)
	}
	peer2 := mdsNextPeer(t, ctx, peers)
	defer second.Close()
	pending := mdsStartCommand(second, ctx)
	sent2 := peer2.next(t)
	peer2.reply(t, sent2, 0, "second", nil)
	got2 := mdsAwaitOutcome(t, ctx, pending)
	if got2.err != nil || sent2.Transaction <= sent.Transaction || second.generation <= m.generation || got2.result.Submission.Target != nextTarget {
		t.Fatal(got2, second.generation, m.generation)
	}
	if dials.Load() != 2 {
		t.Fatal("MDS cache unexpectedly dialed", dials.Load())
	}
}

func TestMDSCommandLocalBoundsSlotsCancellationAndClose(t *testing.T) {
	c, m, peer, ctx, dials := mdsTestOpen(t)
	c.options.MaxFrameSize = 1024
	for _, command := range []Command{{JSON: []byte(`null`)}, {JSON: []byte(`[]`)}, {JSON: []byte(`{"prefix":7}`)}, {JSON: []byte(`{"prefix":" "}`)}, {JSON: []byte(`{"prefix":"x"}`), Input: make([]byte, 1024)}} {
		result, err := m.Command(ctx, command)
		if err == nil || result.Submission != (MDSCommandSubmission{}) || c.mdsNext.Load() != 0 {
			t.Fatal("invalid command reached admission", result, err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if r, e := m.Command(canceled, Command{}); !errors.Is(e, context.Canceled) || r.Submission != (MDSCommandSubmission{}) {
		t.Fatal(r, e)
	}
	c.calls = make(chan struct{}, 1)
	first := mdsStartCommand(m, ctx)
	sent := peer.next(t)
	waiting, stop := context.WithCancel(ctx)
	second := mdsStartCommand(m, waiting)
	stop()
	unsent := mdsAwaitOutcome(t, ctx, second)
	if !errors.Is(unsent.err, context.Canceled) || unsent.result.Submission != (MDSCommandSubmission{}) || c.mdsNext.Load() != sent.Transaction {
		t.Fatal("slot waiter consumed identity", unsent)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	got := mdsAwaitOutcome(t, ctx, first)
	var unknown *OutcomeUnknownError
	if !errors.As(got.err, &unknown) || !errors.Is(got.err, ErrClosed) {
		t.Fatal("Close lost sent outcome", got.err)
	}
	mdsCheckSubmission(t, m, sent, got.result)
	if r, e := m.Command(ctx, Command{Input: make([]byte, 1024)}); !errors.Is(e, ErrClosed) || r.Submission != (MDSCommandSubmission{}) {
		t.Fatal("closed MDS command processed input", r, e)
	}
	if dials.Load() != 1 {
		t.Fatal("Close caused MDS reconnect")
	}
	select {
	case unexpected := <-peer.requests:
		t.Fatal("queued command was replayed", unexpected)
	default:
	}
}

func TestMDSCommandCancellationConsumesOnlyLocalWait(t *testing.T) {
	c, m, peer, ctx, dials := mdsTestOpen(t)
	operation, cancel := context.WithCancel(ctx)
	first := mdsStartCommand(m, operation)
	sent := peer.next(t)
	cancel()
	got := mdsAwaitOutcome(t, ctx, first)
	var unknown *OutcomeUnknownError
	if !errors.As(got.err, &unknown) || !errors.Is(got.err, context.Canceled) {
		t.Fatal(got.err)
	}
	mdsCheckSubmission(t, m, sent, got.result)
	// A late result is not replay, and cannot complete a later TID.
	peer.reply(t, sent, 0, "late", []byte{1})
	next := mdsStartCommand(m, ctx)
	sent2 := peer.next(t)
	peer.reply(t, sent2, 0, "new", []byte{2})
	fresh := mdsAwaitOutcome(t, ctx, next)
	if fresh.err != nil || fresh.result.Message != "new" || sent2.Transaction <= sent.Transaction || dials.Load() != 1 || c.mdsNext.Load() != sent2.Transaction {
		t.Fatal(fresh, dials.Load())
	}
}

func TestMDSTerminalLossNeverReconnectsOrReplays(t *testing.T) {
	c, m, peer, ctx, dials := mdsTestOpen(t)
	done := mdsStartCommand(m, ctx)
	sent := peer.next(t)
	peer.conn.Close()
	got := mdsAwaitOutcome(t, ctx, done)
	var unknown *OutcomeUnknownError
	if !errors.As(got.err, &unknown) {
		t.Fatal(got.err)
	}
	mdsCheckSubmission(t, m, sent, got.result)
	select {
	case <-m.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := c.OpenMDS(ctx, m.target); err == nil {
		t.Fatal("terminal cached MDS handle reopened")
	}
	if _, err := m.Command(ctx, Command{JSON: []byte(`{"prefix":"version"}`)}); err == nil {
		t.Fatal("terminal handle accepted command")
	}
	if dials.Load() != 1 {
		t.Fatal("terminal MDS handle redialed", dials.Load())
	}
	select {
	case replay := <-peer.requests:
		t.Fatal("lost command replayed", replay)
	default:
	}
}

func TestMDSClientCloseJoinsPendingCommand(t *testing.T) {
	c, m, peer, ctx, _ := mdsTestOpen(t)
	pending := mdsStartCommand(m, ctx)
	sent := peer.next(t)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	got := mdsAwaitOutcome(t, ctx, pending)
	var unknown *OutcomeUnknownError
	if !errors.As(got.err, &unknown) || !errors.Is(got.err, ErrClosed) {
		t.Fatal(got.err)
	}
	mdsCheckSubmission(t, m, sent, got.result)
	select {
	case <-m.Done():
	default:
		t.Fatal("Client.Close returned before MDS terminal")
	}
	if _, err := c.OpenMDS(ctx, MDSTarget{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestMDSTicketRenewalAndGlobalIdentityRetirement(t *testing.T) {
	for _, changedID := range []bool{false, true} {
		t.Run(map[bool]string{false: "renewal", true: "new-global-id"}[changedID], func(t *testing.T) {
			c, m, peer, ctx, dials := mdsTestOpen(t)
			original := c.Snapshot().MDSTicket
			if original.Expires.IsZero() || original.RenewAfter.IsZero() || !c.Snapshot().OSDTicket.Expires.IsZero() {
				t.Fatal("independent MDS ticket unavailable")
			}
			// Force a local renewal due marker, then exercise the actual MON candidate
			// authentication/install path without sleeping or opening another MDS.
			c.mu.Lock()
			expired := copyAuth(c.auth)
			ticket := expired.Tickets[cephx.ServiceMDS]
			ticket.RenewAfter = time.Now().Add(-time.Second)
			expired.Tickets[cephx.ServiceMDS] = ticket
			c.auth = expired
			c.mu.Unlock()
			if needed, _, _ := c.renewalState(time.Now()); !needed {
				t.Fatal("MDS ticket omitted from renewal scheduling")
			}
			id := uint64(42)
			if changedID {
				id = 99
			}
			c.options.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				client, server := net.Pipe()
				go mdsMockMonitorID(server, id)
				return client, nil
			}
			var pending <-chan mdsCommandOutcome
			var sent msgr.MessageData
			if changedID {
				pending = mdsStartCommand(m, ctx)
				sent = peer.next(t)
			}
			if err := c.connectMonitor(ctx); err != nil {
				t.Fatal("renew MON candidate", err)
			}
			now := c.Snapshot()
			if now.GlobalID != id || now.MDSTicket.Expires.IsZero() || !now.MDSTicket.RenewAfter.After(ticket.RenewAfter) {
				t.Fatal(now)
			}
			if changedID {
				got := mdsAwaitOutcome(t, ctx, pending)
				var unknown *OutcomeUnknownError
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, ErrMDSIdentityChanged) {
					t.Fatal(got.err)
				}
				mdsCheckSubmission(t, m, sent, got.result)
				select {
				case <-m.Done():
				default:
					t.Fatal("old MDS identity not terminal")
				}
				if _, err := c.OpenMDS(ctx, m.target); !errors.Is(err, ErrMDSIdentityChanged) {
					t.Fatal(err)
				}
			} else {
				done := mdsStartCommand(m, ctx)
				request := peer.next(t)
				peer.reply(t, request, 0, "after renewal", nil)
				got := mdsAwaitOutcome(t, ctx, done)
				if got.err != nil || got.result.Submission.GlobalID != 42 {
					t.Fatal(got)
				}
			}
			if dials.Load() != 1 {
				t.Fatal("MON renewal automatically reopened MDS", dials.Load())
			}
		})
	}
}

func TestMDSMonotonicIdentityExhaustionAndOptionBounds(t *testing.T) {
	for _, limit := range []int{-1, 1025} {
		if _, err := Dial(context.Background(), Options{Monitors: []string{"192.0.2.1:3300"}, MaxMDSConnections: limit}); err == nil {
			t.Fatal(limit)
		}
	}
	c, peers, ctx, dials := mdsTestClient(t, SecureMode)
	c.mu.Lock()
	c.mdsGeneration = math.MaxUint64
	c.mu.Unlock()
	if _, err := c.OpenMDS(ctx, MDSTarget{7, 0, "192.0.2.1:6805/17"}); err == nil || dials.Load() != 0 {
		t.Fatal("generation exhausted but dialed", err)
	}
	c.mu.Lock()
	c.mdsGeneration = 0
	c.mu.Unlock()
	m, err := c.OpenMDS(ctx, MDSTarget{7, 0, "192.0.2.1:6805/17"})
	if err != nil {
		t.Fatal(err)
	}
	peer := mdsNextPeer(t, ctx, peers)
	c.mdsNext.Store(math.MaxUint64)
	result, err := m.Command(ctx, Command{JSON: []byte(`{"prefix":"version"}`)})
	var unknown *OutcomeUnknownError
	if err == nil || errors.As(err, &unknown) || result.Submission.Transaction != 0 {
		t.Fatal("TID exhaustion not a known unsent outcome", result, err)
	}
	select {
	case request := <-peer.requests:
		t.Fatal("exhausted TID sent request", request)
	default:
	}
	disabled, _, _ := monOSDMapsTestFixture(t)
	if _, err := disabled.OpenMDS(ctx, MDSTarget{}); !errors.Is(err, ErrMDSDisabled) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(disabled.Snapshot().MDSTicket, TicketState{}) {
		t.Fatal("disabled MDS exposed ticket")
	}
}

func TestMDSSetupContextAndConcurrentSlotOwnership(t *testing.T) {
	for _, closing := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel-setup", true: "client-close"}[closing], func(t *testing.T) {
			c, _, ctx, _ := mdsTestClient(t, SecureMode)
			started := make(chan struct{})
			c.options.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			setup, cancel := context.WithCancel(ctx)
			defer cancel()
			first := make(chan error, 1)
			target := MDSTarget{7, 0, "192.0.2.1:6805/0"}
			go func() { _, err := c.OpenMDS(setup, target); first <- err }()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if _, err := c.OpenMDS(ctx, MDSTarget{8, 0, target.Address}); !errors.Is(err, ErrMDSConnectionLimit) {
				t.Fatal("setup did not retain its slot", err)
			}
			waiter, stop := context.WithCancel(ctx)
			stop()
			if _, err := c.OpenMDS(waiter, target); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if closing {
				c.Close()
			} else {
				cancel()
			}
			select {
			case err := <-first:
				want := context.Canceled
				if closing {
					want = ErrClosed
				}
				if !errors.Is(err, want) {
					t.Fatal(err, want)
				}
			case <-ctx.Done():
				t.Fatal("setup not joined", ctx.Err())
			}
			c.mu.Lock()
			count := len(c.mdss)
			c.mu.Unlock()
			if count != 0 {
				t.Fatal("failed setup retained slot", count)
			}
		})
	}
	c, peers, ctx, _ := mdsTestClient(t, SecureMode)
	setup, cancel := context.WithCancel(ctx)
	m, err := c.OpenMDS(setup, MDSTarget{7, 0, "192.0.2.1:6805/0"})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	peer := mdsNextPeer(t, ctx, peers)
	done := mdsStartCommand(m, ctx)
	sent := peer.next(t)
	peer.reply(t, sent, 0, "setup context ended", nil)
	if got := mdsAwaitOutcome(t, ctx, done); got.err != nil {
		t.Fatal("setup lifetime closed established MDS", got.err)
	}
}

func TestMDSPublicFreshAdmissionRejectsRankNonceAndLossy(t *testing.T) {
	for _, kind := range []string{"rank", "nonce", "lossy", "authentication-global-id"} {
		t.Run(kind, func(t *testing.T) {
			c, _, ctx, _ := mdsTestClient(t, SecureMode)
			var attempts int
			c.options.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				attempts++
				client, server := net.Pipe()
				cfg := peerConfig{role: 2, id: 0}
				switch kind {
				case "rank":
					cfg.id = 9
				case "nonce":
					cfg.addresses = []msgr.Address{{Type: 2, Nonce: 18, Endpoint: netip.MustParseAddrPort("192.0.2.1:6805")}}
				case "lossy":
					cfg.payload = func(tag msgr.Tag, p []byte) []byte {
						if tag == msgr.ServerIdent {
							p = append([]byte(nil), p...)
							binary.LittleEndian.PutUint64(p[len(p)-16:], 1)
						}
						return p
					}
				case "authentication-global-id":
					cfg.payload = func(tag msgr.Tag, p []byte) []byte {
						if tag == msgr.AuthDone {
							p = append([]byte(nil), p...)
							binary.LittleEndian.PutUint64(p, 99)
						}
						return p
					}
				}
				go func() { defer server.Close(); _, _, _ = mdsMockAuthenticate(server, cfg) }()
				return client, nil
			}
			if m, err := c.OpenMDS(ctx, MDSTarget{7, 0, "192.0.2.1:6805/17"}); err == nil || m != nil || attempts != 1 {
				t.Fatal("fresh MDS mismatch admitted or retried", m, err, attempts)
			}
			c.mu.Lock()
			count := len(c.mdss)
			c.mu.Unlock()
			if count != 0 {
				t.Fatal("refused fresh MDS retained slot", count)
			}
		})
	}
}
