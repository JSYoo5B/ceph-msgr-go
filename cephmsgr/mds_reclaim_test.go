package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/mds"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func mdsReclaimReplyMessage(result int32, epoch uint32, addrs ...msgr.Address) msgr.MessageData {
	e := wire.Encoder{}
	e.U32(uint32(result))
	e.U32(epoch)
	msgr.EncodeAddresses(&e, addrs)
	return msgr.MessageData{Type: mds.ClientReclaimReplyMessage, Version: 1, CompatVersion: 1, Front: e.Data}
}

func mdsOpenUUIDSession(t *testing.T, ctx context.Context, m *MDSConnection, peer *mdsTestPeer, uuid string) *MDSSession {
	t.Helper()
	peer.sessions.Store(true)
	pending := mdsStartSession(m, ctx, MDSSessionOptions{Metadata: map[string]string{"entity_id": "test", "uuid": uuid}, RenewInterval: time.Hour})
	if request := peer.next(t); request.Type != mds.ClientSessionMessage || request.Version != 7 {
		t.Fatal("REQUEST_OPEN envelope", request.Type, request.Version)
	}
	peer.send(t, mdsSessionV7(mds.SessionOpen, 0, map[string]string{}, 0, true))
	got := <-pending
	if got.err != nil {
		t.Fatal(got.err)
	}
	renew := mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
	peer.send(t, mdsSessionReply(mds.SessionRenewCaps, renew.Sequence))
	return got.session
}

func TestMDSReclaimRoundTripAndFinish(t *testing.T) {
	_, m, peer, ctx, _ := mdsTestOpen(t)
	s := mdsOpenUUIDSession(t, ctx, m, peer, "own-uuid")
	target := msgr.Address{Type: 2, Nonce: 77, Endpoint: netip.MustParseAddrPort("192.0.2.8:0")}
	done := make(chan struct {
		r MDSReclaimReply
		e error
	}, 1)
	go func() {
		r, e := s.Reclaim(ctx, "victim-uuid")
		done <- struct {
			r MDSReclaimReply
			e error
		}{r, e}
	}()
	sent := peer.next(t)
	wantFront, err := mds.EncodeReclaim("victim-uuid", mds.ReclaimReset)
	if err != nil || sent.Type != mds.ClientReclaimMessage || !bytes.Equal(sent.Front, wantFront.Front) || sent.Version != 1 || sent.Transaction != 0 {
		t.Fatalf("reclaim wire %+v", sent)
	}
	if _, err := s.Reclaim(ctx, "second"); !errors.Is(err, ErrMDSReclaimPending) {
		t.Fatal("a second concurrent reclaim was admitted", err)
	}
	peer.send(t, mdsReclaimReplyMessage(0, 55, target))
	got := <-done
	if got.e != nil || got.r.Result != 0 || got.r.Epoch != 55 || len(got.r.Addresses) != 1 || got.r.Addresses[0].Nonce != 77 || got.r.Addresses[0].Endpoint != target.Endpoint {
		t.Fatalf("reply %+v %v", got.r, got.e)
	}
	if s.Status().State != MDSSessionOpen {
		t.Fatal("reclaim changed the session state", s.Status().State)
	}
	if err := s.FinishReclaim(ctx); err != nil {
		t.Fatal(err)
	}
	finish := peer.next(t)
	wantFinish, _ := mds.EncodeReclaim("", mds.ReclaimFinish)
	if finish.Type != mds.ClientReclaimMessage || !bytes.Equal(finish.Front, wantFinish.Front) {
		t.Fatalf("finish wire %+v", finish)
	}
}

func TestMDSReclaimRefusalsAndUnknownOutcomes(t *testing.T) {
	_, m, peer, ctx, _ := mdsTestOpen(t)
	s := mdsOpenUUIDSession(t, ctx, m, peer, "own-uuid")
	for name, uuid := range map[string]string{"empty": ""} {
		if _, err := s.Reclaim(ctx, uuid); err == nil {
			t.Fatal("invalid uuid accepted", name)
		}
	}
	if _, err := s.Reclaim(ctx, "own-uuid"); !errors.Is(err, ErrMDSReclaimSelf) {
		t.Fatal("self reclaim accepted", err)
	}
	// An unanswered reclaim whose wait is cancelled is an unknown outcome; its
	// late reply is ignored and the session survives.
	waitCtx, cancel := context.WithCancel(ctx)
	failed := make(chan error, 1)
	go func() { _, err := s.Reclaim(waitCtx, "victim"); failed <- err }()
	peer.next(t)
	cancel()
	var unknown *OutcomeUnknownError
	if err := <-failed; !errors.As(err, &unknown) {
		t.Fatal("cancelled reclaim wait is not an unknown outcome", err)
	}
	peer.send(t, mdsReclaimReplyMessage(0, 1))
	deadline := time.Now().Add(time.Second)
	for s.Status().State != MDSSessionOpen && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Status().State != MDSSessionOpen {
		t.Fatal("a late reclaim reply ended the session", s.Status().State, s.Err())
	}
	// The abandoned reclaim's reply cannot be told from a new one's, so the
	// session refuses further reclaims instead of risking a mismatched answer.
	if _, err := s.Reclaim(ctx, "victim"); !errors.Is(err, ErrMDSReclaimUncertain) {
		t.Fatal("a reclaim after an abandoned one was admitted", err)
	}
	if err := s.FinishReclaim(ctx); err != nil {
		t.Fatal("finish is independent of the abandoned reclaim", err)
	}
}

func TestMDSReclaimMalformedReplyEndsTheSession(t *testing.T) {
	_, m, peer, ctx, _ := mdsTestOpen(t)
	s := mdsOpenUUIDSession(t, ctx, m, peer, "own-uuid")
	bad := mdsReclaimReplyMessage(0, 1)
	bad.Front = append(bad.Front, 1)
	waiting := make(chan error, 1)
	go func() { _, err := s.Reclaim(ctx, "victim"); waiting <- err }()
	peer.next(t)
	peer.send(t, bad)
	var unknown *OutcomeUnknownError
	if err := <-waiting; !errors.As(err, &unknown) {
		t.Fatal("malformed reply did not end the reclaim as unknown", err)
	}
	mdsWaitState(t, s, MDSSessionLost)
}

func TestMDSReclaimRequiresAnOpenSessionWithTheFeature(t *testing.T) {
	_, m, peer, ctx, _ := mdsTestOpen(t)
	peer.sessions.Store(true)
	pending := mdsStartSession(m, ctx, MDSSessionOptions{Metadata: map[string]string{"entity_id": "test"}, RenewInterval: time.Hour})
	peer.next(t)
	// An OPEN advertising only the low features lacks RECLAIM_CLIENT.
	open := mdsSessionV7(mds.SessionOpen, 0, map[string]string{}, 0, true)
	open.Front = bytes.Replace(open.Front, []byte{8, 0, 0, 0, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0}, []byte{8, 0, 0, 0, 0xff, 0x01, 0, 0, 0, 0, 0, 0}, 1)
	peer.send(t, open)
	got := <-pending
	if got.err != nil {
		t.Fatal(got.err)
	}
	s := got.session
	if _, err := s.Reclaim(ctx, "victim"); !errors.Is(err, ErrMDSReclaimUnsupported) {
		t.Fatal("reclaim sent to an MDS without RECLAIM_CLIENT", err)
	}
	if err := s.FinishReclaim(ctx); !errors.Is(err, ErrMDSReclaimUnsupported) {
		t.Fatal("finish sent to an MDS without RECLAIM_CLIENT", err)
	}
}
