package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestOSDRequestV9WatchReconnectPreflightPreservesOwnerAndSendsV9(t *testing.T) {
	c, peers, dials := reconnectProfileTestClient(t, OSDProfileReplicatedPGRequestV9ReplyV8)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	old, oldPeer := reconnectOpen(t, c, peers, 0)
	next, nextPeer := reconnectOpen(t, c, peers, 1)
	route := backoffTransportRequest()
	route.Operations = nil
	type registered struct {
		w   *OSDWatch
		err error
	}
	initial := make(chan registered, 1)
	go func() {
		w, _, err := old.WatchObject(ctx, route, OSDWatchOptions{RegistrationGeneration: 7})
		initial <- registered{w, err}
	}()
	sent := oldPeer.next(t)
	assertReplicatedRequestProfile(t, c.options.OSDProfile, sent)
	c.mu.Lock()
	var cookie uint64
	for cookie = range old.watchState.streams {
	}
	c.mu.Unlock()
	replyWatch := func(peer *osdTestPeer, req msgr.MessageData, input OSDRequest, action uint8, generation uint32) {
		t.Helper()
		input.Operations = []OSDOperation{{Code: OSDWatchOperation, Watch: OSDWatchInput{Cookie: cookie, Action: action, Generation: generation}}}
		reply := replyV8ProfileMessage(input, [][]byte{nil}, []int32{0}, 0, osd.FlagAck|osd.FlagOnDisk, false)
		reply.Transaction = req.Transaction
		if err := osdMapsWrite(peer, reply); err != nil {
			t.Fatal(err)
		}
	}
	replyWatch(oldPeer, sent, route, OSDWatchRegister, 7)
	watch := <-initial
	if watch.err != nil {
		t.Fatal(watch.err)
	}
	nextRoute := route
	nextRoute.Placement = cloneOSDPlacement(route.Placement)
	nextRoute.Placement.Primary = next.target.ID
	if err := next.preflightWatchReconnect(nextRoute, cookie); err != nil {
		t.Fatal("v9 preflight refused", err)
	}
	for _, edit := range []func(*OSDRequest){
		func(r *OSDRequest) { r.MapEpoch = 0 },
		func(r *OSDRequest) { r.ModificationTime.Nanoseconds = 1_000_000_000 },
		func(r *OSDRequest) { r.Placement.PG.Pool++ },
	} {
		bad := nextRoute
		bad.Placement = cloneOSDPlacement(nextRoute.Placement)
		edit(&bad)
		before := c.osdNext.Load()
		w, result, err := next.ReconnectWatch(ctx, watch.w, bad)
		if w != nil || err == nil || result.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
			t.Fatal("failed preflight submitted", result, err)
		}
		c.mu.Lock()
		unchanged := watch.w.registration.owner == watch.w.OSDWatchStream && watch.w.registration.generation == 7 && watch.w.terminal == nil && (next.watchState == nil || len(next.watchState.streams) == 0)
		c.mu.Unlock()
		if !unchanged {
			t.Fatal("failed preflight consumed old authority")
		}
		select {
		case <-nextPeer.requests:
			t.Fatal("failed preflight sent reconnect")
		default:
		}
	}
	// Verify the retained owner can still send a control after all refusals.
	pinged := make(chan error, 1)
	go func() { _, err := watch.w.Ping(ctx, route.MapEpoch); pinged <- err }()
	ping := oldPeer.next(t)
	assertReplicatedRequestProfile(t, c.options.OSDProfile, ping)
	replyWatch(oldPeer, ping, route, OSDWatchPing, 7)
	if err := <-pinged; err != nil {
		t.Fatal("old owner lost Ping", err)
	}
	done := startReconnect(ctx, next, watch.w, nextRoute)
	reconnect := nextPeer.next(t)
	assertReplicatedRequestProfile(t, c.options.OSDProfile, reconnect)
	decoded, err := osd.DecodeRequestV9AdmissionTarget(reconnect, c.options.MaxFrameSize)
	if err != nil || decoded.PG.Seed != nextRoute.Placement.PG.Seed {
		t.Fatal("reconnect lost explicit PG", decoded, err)
	}
	replyWatch(nextPeer, reconnect, nextRoute, OSDWatchReconnect, 8)
	got := <-done
	if got.err != nil || got.watch == nil || got.watch.options.RegistrationGeneration != 8 || got.reply.Submission.Transaction != reconnect.Transaction || dials.Load() != 2 {
		t.Fatal("explicit reconnect failed", got, dials.Load())
	}
	if _, err := watch.w.Ping(ctx, route.MapEpoch); !errors.Is(err, ErrOSDWatchSuperseded) {
		t.Fatal("old owner still sends after transfer", err)
	}
	select {
	case <-oldPeer.requests:
		t.Fatal("reconnect replayed on old session")
	default:
	}
	select {
	case <-nextPeer.requests:
		t.Fatal("reconnect automatically replayed")
	default:
	}
}

func TestOSDRequestV9StartedMutationCancellationAndCloseNeverReplay(t *testing.T) {
	for _, closeConnection := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "close"}[closeConnection], func(t *testing.T) {
			_, o, peer, base, dials := replicatedReplyV8ProfileOpen(t, OSDProfileReplicatedPGRequestV9ReplyV8)
			ctx, cancel := context.WithCancel(base)
			defer cancel()
			input := backoffTransportRequest()
			input.Operations = []OSDOperation{{Code: OSDAppend, Length: 2, Data: []byte{0, 255}}}
			type result struct {
				reply OSDReply
				err   error
			}
			done := make(chan result, 1)
			go func() { r, err := o.Request(ctx, input); done <- result{r, err} }()
			sent := peer.next(t)
			assertReplicatedRequestProfile(t, o.client.options.OSDProfile, sent)
			if closeConnection {
				o.Close()
			} else {
				cancel()
			}
			got := <-done
			var unknown *OutcomeUnknownError
			want := error(context.Canceled)
			if closeConnection {
				want = ErrClosed
			}
			if !errors.As(got.err, &unknown) || !errors.Is(got.err, want) || got.reply.Submission.Transaction != sent.Transaction || got.reply.Submission.GlobalID != o.globalID || got.reply.Submission.ClientIncarnation != input.ClientIncarnation || got.reply.Submission.MapEpoch != input.MapEpoch || got.reply.Transaction != 0 || len(got.reply.RawFront) != 0 {
				t.Fatal("started outcome/identity lost", got)
			}
			if dials.Load() != 1 {
				t.Fatal("mutation reconnected")
			}
			select {
			case <-peer.requests:
				t.Fatal("uncertain mutation replayed")
			default:
			}
		})
	}
}

func assertReplicatedRequestProfile(t *testing.T, profile OSDProfile, m msgr.MessageData) {
	t.Helper()
	want := uint16(8)
	if profile.requestV9() {
		want = 9
	}
	if m.Type != osd.RequestMessage || m.Version != want || m.CompatVersion != 3 || m.Transaction == 0 {
		t.Fatal("profile selected another request envelope", profile, m)
	}
	if profile.requestV9() {
		if len(m.Front) < 94 || !bytes.Equal(m.Front[87:94], []byte{1, 1, 1, 0, 0, 0, 0}) {
			t.Fatal("public v9 did not emit canonical empty OTEL")
		}
		if binary.LittleEndian.Uint64(m.Front[len(m.Front)-8:])&(1<<49|1<<57|1<<58) != (1<<49 | 1<<57 | 1<<58) {
			t.Fatal("v9 fixed features lost")
		}
	}
}

func TestOSDRequestV9ProfileRequestResultsAndNoReplay(t *testing.T) {
	// Same authenticated synthetic transport and signed-result/completion cases
	// as v8; only the explicit request profile differs. Native proof is separate.
	testReplicatedReplyV8RequestResults(t, OSDProfileReplicatedPGRequestV9ReplyV8)
}

func TestOSDRequestV9ProfileListPGStrictReplyAndNoFallback(t *testing.T) {
	testReplicatedReplyV8PGListResults(t, OSDProfileReplicatedPGRequestV9ReplyV8)
}

func TestOSDRequestV9ProfileAdmissionAndStrictReplySelection(t *testing.T) {
	p := OSDProfileReplicatedPGRequestV9ReplyV8
	if p != 3 || !p.valid() || !p.replicatedPG() || !p.replyV8() || !p.requestV9() {
		t.Fatal("fixed v9 enum/capability changed")
	}
	if err := validateOSDProfile(Options{OSDProfile: p}); !errors.Is(err, ErrOSDDisabled) {
		t.Fatal(err)
	}
	if err := validateOSDProfile(Options{EnableOSD: true, OSDProfile: p}); err != nil {
		t.Fatal(err)
	}
	o := backoffUnitConnection()
	o.client.options.OSDProfile = p
	r := backoffTransportRequest()
	r.Placement.Primary = o.target.ID
	if err := o.validatePlacement(r); err != nil {
		t.Fatal(err)
	}
	r.Placement = nil
	if err := o.validatePlacement(r); err == nil {
		t.Fatal("v9 accepted missing actual PG")
	}
	o.features = 1<<49 | 1<<57
	m := nativeReplyV8Message(t, "reply-v8-native-success")
	if _, err := o.decodeReply(m); !errors.Is(err, osd.ErrFeatures) {
		t.Fatal("v9 request feature substituted reply-v8 capability", err)
	}
	o.features |= 1 << 58
	if _, err := o.decodeReply(m); err != nil {
		t.Fatal("strict v8 native reply refused", err)
	}
	m.Version = 6
	if _, err := o.decodeReply(m); !errors.Is(err, wire.ErrVersion) {
		t.Fatal("v9 profile downgraded reply", err)
	}
}

func TestOSDRequestV9BackoffPGNLSUsesEmptyOIDAndExplicitHash(t *testing.T) {
	o := backoffUnitConnection()
	o.client.options.OSDProfile = OSDProfileReplicatedPGRequestV9ReplyV8
	o.backoffState.gateInstalled = true
	point := osd.PGCursor{Pool: 7, Hash: 3, Namespace: "tenant", Snap: osd.NoSnap}
	block := osd.Backoff{PG: osd.PG{Pool: 7, Seed: 3}, Shard: -1, MapEpoch: 15, ID: 101, Op: osd.BackoffBlock, Begin: point, End: point}
	m, err := osd.EncodeBackoff(block, 4096)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := osd.DecodeBackoff(m, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.backoffState.accept(o, decoded, o.generation, o.globalID); err != nil {
		t.Fatal(err)
	}
	input := osd.PGListRequest{PG: osd.PG{Pool: 7, Seed: 3}, ClientInc: 9, MapEpoch: 11, Count: 1, Namespace: "tenant", MaxOutputBytes: osd.MinPGListBytes}
	features := uint64(1<<8 | 1<<9 | 1<<21 | 1<<30 | 1<<49 | 1<<56 | 1<<57 | 1<<58)
	m, err = osd.EncodePGListRequestV9(input, features, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.beforeWriteBackoff(m); !errors.Is(err, ErrOSDBackoff) {
		t.Fatal("PGNLS point escaped exact head-object range", err)
	}
	for _, edit := range []func(*osd.PGListRequest){func(r *osd.PGListRequest) { r.Namespace = "unrelated" }, func(r *osd.PGListRequest) { r.PG.Seed++ }} {
		r := input
		edit(&r)
		unrelated, err := osd.EncodePGListRequestV9(r, features, 4096)
		if err != nil {
			t.Fatal(err)
		}
		if err := o.beforeWriteBackoff(unrelated); err != nil {
			t.Fatal("unrelated PGNLS was blocked or failed to decode", err)
		}
	}
	old, err := osd.EncodePGListRequestV8(input, features&^(1<<49), 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.beforeWriteBackoff(old); !errors.Is(err, ErrMalformedMessage) {
		t.Fatal("v9 backoff fell back to v8 parser", err)
	}
	m.Front = bytes.Clone(m.Front)
	m.Front[93] = 1
	if err := o.beforeWriteBackoff(m); !errors.Is(err, ErrMalformedMessage) {
		t.Fatal("nonempty trace escaped installed v9 writer gate", err)
	}
}
