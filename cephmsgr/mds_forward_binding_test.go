package cephmsgr

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func forwardBindingClient(t *testing.T) (*Client, <-chan *mdsTestPeer, context.Context) {
	t.Helper()
	return cephfsTestClientWith(t, func(peer *mdsTestPeer, dial int) {
		if dial > 1 {
			peer.rank = 1
		}
	})
}

func openForwardBindingSession(t *testing.T, c *Client, peers <-chan *mdsTestPeer, ctx context.Context, fs int32, rank int32, nonce string) (*MDSSession, *mdsTestPeer) {
	t.Helper()
	m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: fs, Rank: rank, Address: "v2:192.0.2.1:6805/" + nonce})
	if err != nil {
		t.Fatal(err)
	}
	var peer *mdsTestPeer
	select {
	case peer = <-peers:
	case <-ctx.Done():
		t.Fatal("MDS peer setup did not complete", ctx.Err())
	}
	pending := mdsStartSession(m, ctx, MDSSessionOptions{RenewInterval: time.Hour})
	peer.next(t)
	peer.send(t, mdsSessionV7(mds.SessionOpen, 0, nil, 0, false))
	var got mdsSessionOpenResult
	select {
	case got = <-pending:
	case <-ctx.Done():
		t.Fatal("MDS session OPEN did not complete", ctx.Err())
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
	return got.session, peer
}

func forwardBindingCall(t *testing.T, s *MDSSession, peer *mdsTestPeer, ctx context.Context, releases []MDSRequestRelease) (*MDSRequestCall, msgr.MessageData, mds.ClientRequest) {
	t.Helper()
	request := MDSRequest{Op: MDSOpSetxattr, Retry: 3, Path: MDSFilePath{Ino: 1, Path: "pinned"}, Path2: MDSFilePath{Path: "user.forwarded"},
		Stamp: MDSTime{Seconds: 7, Nanoseconds: 17}, Data: []byte{0, 255, 7, 0, 'x'}, Releases: releases}
	request.Args[0] = 1
	call, err := s.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	frame := peer.next(t)
	original, err := mds.DecodeClientRequest(frame, 0)
	if err != nil || frame.Transaction != call.Transaction() || !reflect.DeepEqual(original.Releases, releases) {
		t.Fatal("original request did not carry its source-rank release evidence", original, err)
	}
	f := wire.Encoder{}
	f.U32(1) // destination rank
	f.U32(1) // forward count
	f.U8(1)
	peer.send(t, msgr.MessageData{Type: mds.ClientRequestForwardMessage, Version: 1, CompatVersion: 1, Transaction: call.Transaction(), Front: f.Data})
	var forwarded *MDSRequestForwardedError
	if _, err := call.Wait(ctx); !errors.As(err, &forwarded) {
		t.Fatal("source did not forward the call", err)
	}
	return call, frame, original
}

func TestCephFSForwardDropsSourceRankReleases(t *testing.T) {
	c, peers, ctx := forwardBindingClient(t)
	source, p0 := openForwardBindingSession(t, c, peers, ctx, 7, 0, "17")
	destination, p1 := openForwardBindingSession(t, c, peers, ctx, 7, 1, "0")
	// Cap ID/sequences and the dentry lease belong to rank 0. Native
	// send_request regenerates these from destination-rank state instead
	// (Client.cc:2306–2407,2717–2722); this transport owns no such cache.
	releases := []MDSRequestRelease{{Ino: 1, CapID: 0x1234, Caps: 0x21, Wanted: 0x11, Seq: 4, IssueSeq: 5, MSeq: 6, DentrySeq: 7, DentryName: "source-only"}}
	call, initialFrame, original := forwardBindingCall(t, source, p0, ctx, releases)
	resent, err := destination.ResubmitForwarded(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	frame := p1.next(t)
	got, err := mds.DecodeClientRequest(frame, 0)
	want := original
	want.Retry, want.Forward, want.Releases = 0, 1, nil
	if err != nil || frame.Transaction != initialFrame.Transaction || resent.Transaction() != call.Transaction() || !reflect.DeepEqual(got, want) {
		t.Fatal("forward copied source releases or changed the logical wire body, TID or stamp", got, err)
	}
	source.mu.Lock()
	retained := append([]MDSRequestRelease(nil), call.request.Releases...)
	source.mu.Unlock()
	if !reflect.DeepEqual(retained, original.Releases) || !reflect.DeepEqual(releases, original.Releases) {
		t.Fatal("forward mutated the original request snapshot or caller releases", retained, releases)
	}
	p1.send(t, cephfsReply(original.Op, call.Transaction(), 0, true, nil))
	if _, err := resent.WaitSafe(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCephFSForwardRejectsAnotherFilesystemLabelBeforeSelection(t *testing.T) {
	c, peers, ctx := forwardBindingClient(t)
	source, p0 := openForwardBindingSession(t, c, peers, ctx, 7, 0, "17")
	wrong, pw := openForwardBindingSession(t, c, peers, ctx, 8, 1, "0")
	call, initialFrame, original := forwardBindingCall(t, source, p0, ctx, nil)
	nextTID, slots := c.mdsNext.Load(), len(c.calls)
	// These are caller routing labels, not authenticated FSCIDs. A forward
	// within one filesystem must not silently switch that local binding.
	// Reserve every free slot for unrelated work: rejection must happen
	// before even waiting for slot admission, without consuming those tokens.
	reserved := cap(c.calls) - slots
	for range reserved {
		c.calls <- struct{}{}
	}
	probe, cancel := context.WithTimeout(ctx, time.Second)
	resent, err := wrong.ResubmitForwarded(probe, call)
	cancel()
	remaining := len(c.calls)
	for range reserved {
		select {
		case <-c.calls:
		default:
			t.Fatal("local label refusal consumed an unrelated slot reservation")
		}
	}
	var unknown *OutcomeUnknownError
	if resent != nil || !errors.Is(err, ErrMDSTargetChanged) || errors.As(err, &unknown) || remaining != cap(c.calls) {
		t.Fatal("wrong filesystem label was not refused as known-unsent", resent, err)
	}
	source.mu.Lock()
	selected := call.forwarded
	source.mu.Unlock()
	wrong.mu.Lock()
	tracked := len(wrong.requests)
	wrong.mu.Unlock()
	if selected || tracked != 0 || len(c.calls) != slots || c.mdsNext.Load() != nextTID {
		t.Fatal("local label refusal selected the call, registered work or consumed slot/TID state", selected, tracked, len(c.calls), c.mdsNext.Load())
	}
	// A later ordered control is a wire barrier: an escaped request would
	// arrive before it, rather than merely being absent from a queue snapshot.
	if err := wrong.FlushJournal(ctx); err != nil {
		t.Fatal(err)
	}
	mdsSessionNext(t, pw, mds.SessionRequestFlushMDLog)
	closed := make(chan error, 1)
	go func() { closed <- wrong.Close(ctx) }()
	mdsSessionNext(t, pw, mds.SessionRequestClose)
	pw.send(t, mdsSessionV7(mds.SessionClose, 0, nil, 0, false))
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("wrong-label session did not close", ctx.Err())
	}
	if err := wrong.conn.Close(); err != nil {
		t.Fatal(err)
	}
	// The rejected selection must not consume this original call's one
	// forward opportunity. Open the right label and use exactly that call.
	right, pr := openForwardBindingSession(t, c, peers, ctx, 7, 1, "0")
	resent, err = right.ResubmitForwarded(ctx, call)
	if err != nil {
		t.Fatal("known-unsent refusal consumed the correct-label resubmission", err)
	}
	frame := pr.next(t)
	got, err := mds.DecodeClientRequest(frame, 0)
	want := original
	want.Retry, want.Forward = 0, 1
	if err != nil || frame.Transaction != initialFrame.Transaction || resent.Transaction() != call.Transaction() || c.mdsNext.Load() != nextTID || !reflect.DeepEqual(got, want) {
		t.Fatal("correct-label resubmission changed the original logical request", got, err)
	}
	pr.send(t, cephfsReply(original.Op, call.Transaction(), 0, true, nil))
	if _, err := resent.WaitSafe(ctx); err != nil || len(c.calls) != slots {
		t.Fatal("correct-label completion lost its result or request slot", err, len(c.calls))
	}
}
