package cephmsgr

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// This is a synthetic peer fixture, not a native interoperability claim.
// Native kick_flushing_caps uses UPDATE (not the FLUSH alias), followed by
// FLUSHSNAP in the caller's retained flush order.
func reconnectTestCapFlushes() []MDSReconnectCapFlush {
	return []MDSReconnectCapFlush{
		{FlushTID: 19, Caps: MDSCaps{Op: MDSCapOpUpdate, Ino: 17, CapID: 25, Caps: 4096, Dirty: 4096, OldestFlushTID: 7, SnapFollows: 2, Flags: 4, Size: 123, MTime: MDSTime{Seconds: 100, Nanoseconds: 17},
			SnapTrace: []byte{1, 2}, Xattrs: []byte{3, 4}, Flock: []byte{5}, InlineData: []byte{6}, Layout: MDSFileLayout{PoolNamespace: "ns"}, FscryptAuth: []byte{7}, FscryptFile: []byte{8}}},
		{FlushTID: 21, Caps: MDSCaps{Op: MDSCapOpFlushSnap, Ino: 17, Realm: 33, Caps: 4096, Dirty: 4096, OldestFlushTID: 7, SnapFollows: 3, Size: 120, MTime: MDSTime{Seconds: 99, Nanoseconds: 18}}},
	}
}

func TestReconnectCapFlushOwnershipAndFrameBudgets(t *testing.T) {
	chosen := reconnectTestCapFlushes()
	size := mdsCapsFrameBytes(chosen[0].Caps) + mdsCapsFrameBytes(chosen[1].Caps)
	frames, err := reconnectCapFlushMessages(chosen, uint32(size))
	if err != nil || len(frames) != 2 {
		t.Fatal(frames, err)
	}
	actual := uint64(0)
	var expected []MDSCaps
	for _, frame := range frames {
		actual += uint64(41 + len(frame.Front) + len(frame.Middle) + len(frame.Data))
		caps, err := mds.DecodeCaps(frame, 0)
		if err != nil {
			t.Fatal(err)
		}
		expected = append(expected, caps)
	}
	if actual != size {
		t.Fatal("variable cap fields were omitted from the frame budget", actual, size)
	}
	if result, err := reconnectCapFlushMessages(chosen, uint32(size-1)); !errors.Is(err, ErrLimitExceeded) || result != nil {
		t.Fatal("aggregate frame budget ignored", result, err)
	}
	// The first frame itself fits, but an overlong later entry must prevent
	// the entire list from being emitted.
	chosen[1].Caps.InlineData = make([]byte, size)
	if result, err := reconnectCapFlushMessages(chosen, uint32(size)); !errors.Is(err, ErrLimitExceeded) || result != nil {
		t.Fatal("late oversized cap flush admitted", result, err)
	}
	chosen[1].Caps.InlineData = nil
	for _, blob := range [][]byte{chosen[0].Caps.SnapTrace, chosen[0].Caps.Xattrs, chosen[0].Caps.Flock, chosen[0].Caps.InlineData, chosen[0].Caps.FscryptAuth, chosen[0].Caps.FscryptFile} {
		for i := range blob {
			blob[i] ^= 255
		}
	}
	chosen[0].FlushTID, chosen[0].Caps.Size, chosen[0].Caps.Flags = 999, 999, 0
	for i, frame := range frames {
		caps, err := mds.DecodeCaps(frame, 0)
		if err != nil || !reflect.DeepEqual(caps, expected[i]) || frame.Transaction != []uint64{19, 21}[i] {
			t.Fatal("encoded flush retained caller-owned input", caps, err)
		}
	}
	// The ordinary public cap path uses the same bound before cloning xattrs.
	_, err = encodeMDSCaps(reconnectTestCapFlushes()[0].Caps, 19, uint32(mdsCapsFrameBytes(reconnectTestCapFlushes()[0].Caps)-1))
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatal("ordinary cap encoder missed the same frame bound", err)
	}
}

func TestCephFSReconnectPrevalidatesEveryCapFlush(t *testing.T) {
	c, lost, peers, ctx := lostCephFSSession(t, 1024)
	m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/0"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	cases := map[string]func(*MDSReconnectCapFlush){
		"FLUSH alias":            func(f *MDSReconnectCapFlush) { f.Caps.Op = MDSCapOpFlush },
		"server opcode":          func(f *MDSReconnectCapFlush) { f.Caps.Op = MDSCapOpGrant },
		"clean":                  func(f *MDSReconnectCapFlush) { f.Caps.Dirty = 0 },
		"inode":                  func(f *MDSReconnectCapFlush) { f.Caps.Ino = 0 },
		"zero tid":               func(f *MDSReconnectCapFlush) { f.FlushTID = 0 },
		"duplicate tid":          func(f *MDSReconnectCapFlush) { f.FlushTID = 19 },
		"no oldest":              func(f *MDSReconnectCapFlush) { f.Caps.OldestFlushTID = 0 },
		"future oldest":          func(f *MDSReconnectCapFlush) { f.Caps.OldestFlushTID = 22 },
		"old cap seq":            func(f *MDSReconnectCapFlush) { f.Caps.Seq = 1 },
		"old cap issue seq":      func(f *MDSReconnectCapFlush) { f.Caps.IssueSeq = 1 },
		"old migrate seq":        func(f *MDSReconnectCapFlush) { f.Caps.MigrateSeq = 1 },
		"oversized later xattrs": func(f *MDSReconnectCapFlush) { f.Caps.Xattrs = make([]byte, 1024) },
	}
	call, stop := context.WithCancel(ctx)
	stop()
	if s, _, err := m.ReconnectSession(call, lost, MDSReconnectState{PendingCapFlushes: reconnectTestCapFlushes()}, MDSSessionOptions{RenewInterval: time.Hour}); !errors.Is(err, context.Canceled) || s != nil {
		t.Fatal("cancelled operation entered reconnect", s, err)
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			chosen := reconnectTestCapFlushes()
			change(&chosen[1])
			s, _, err := m.ReconnectSession(ctx, lost, MDSReconnectState{PendingCapFlushes: chosen}, MDSSessionOptions{RenewInterval: time.Hour})
			if err == nil || s != nil {
				t.Fatal("invalid pending cap flush admitted", s, err)
			}
			var unknown *OutcomeUnknownError
			if errors.As(err, &unknown) {
				t.Fatal("local validation reported an uncertain remote operation", err)
			}
			c.mu.Lock()
			exists := m.cephfs != nil
			c.mu.Unlock()
			if exists || len(c.calls) != 0 {
				t.Fatal("prevalidation created a session or acquired request slots")
			}
			select {
			case frame := <-peer.requests:
				t.Fatal("earlier cap flush escaped before later validation", frame.Type)
			default:
			}
		})
	}
}

func TestCephFSReconnectCapFlushCancellationKeepsChosenFrames(t *testing.T) {
	c, lost, peers, ctx := lostCephFSSession(t, 0)
	m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/0"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	call, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan mdsSessionOpenResult, 1)
	go func() {
		s, _, err := m.ReconnectSession(call, lost, MDSReconnectState{PendingCapFlushes: reconnectTestCapFlushes()}, MDSSessionOptions{RenewInterval: time.Hour})
		done <- mdsSessionOpenResult{s, err}
	}()
	first, second, last := peer.next(t), peer.next(t), peer.next(t)
	if first.Type != mds.ClientCapsMessage || second.Type != mds.ClientCapsMessage || last.Type != mds.ClientReconnectMessage {
		t.Fatal("flushes did not precede reconnect")
	}
	stop()
	result := <-done
	var unknown *OutcomeUnknownError
	if result.session == nil || !errors.As(result.err, &unknown) || !errors.Is(result.err, context.Canceled) || result.session.Status().State != MDSSessionOpening {
		t.Fatal(result.session, result.err)
	}
	select {
	case frame := <-peer.requests:
		t.Fatal("cancelled wait caused an automatic cap resend", frame.Type)
	default:
	}
	peer.send(t, mdsSessionV7(mds.SessionOpen, 0, nil, 0, false))
	if _, err := result.session.WaitOpen(ctx); err != nil {
		t.Fatal(err)
	}
	mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
	for i, frame := range []msgr.MessageData{first, second} {
		caps, err := mds.DecodeCaps(frame, 0)
		if err != nil {
			t.Fatal(err)
		}
		op := MDSCapOpFlushAck
		if i == 1 {
			op = MDSCapOpFlushSnapAck
		}
		peer.send(t, mds.EncodeCaps(MDSCaps{Op: op, Ino: caps.Ino, CapID: caps.CapID, Dirty: caps.Dirty, SnapFollows: caps.SnapFollows}, frame.Transaction))
		event, err := result.session.Next(ctx)
		if err != nil || event.Transaction != frame.Transaction || event.Caps == nil || event.Caps.Op != op || event.Caps.Ino != caps.Ino || event.Caps.SnapFollows != caps.SnapFollows || event.Caps.Dirty != caps.Dirty {
			t.Fatal("cap ACK identity lost across reconnect", event, err)
		}
	}
}

func TestCephFSReconnectCapFlushTransportLossIsUnknown(t *testing.T) {
	c, lost, peers, ctx := lostCephFSSession(t, 0)
	m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/0"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	done := make(chan mdsSessionOpenResult, 1)
	go func() {
		s, _, err := m.ReconnectSession(ctx, lost, MDSReconnectState{PendingCapFlushes: reconnectTestCapFlushes()}, MDSSessionOptions{RenewInterval: time.Hour})
		done <- mdsSessionOpenResult{s, err}
	}()
	if first := peer.next(t); first.Type != mds.ClientCapsMessage {
		t.Fatal(first.Type)
	}
	peer.conn.Close()
	result := <-done
	var unknown *OutcomeUnknownError
	if !errors.As(result.err, &unknown) {
		t.Fatal("transport loss after dirty flush admission looked definitely unexecuted", result.err)
	}
	// The same connection/address cannot start a fresh session after this
	// unresolved outcome. The caller still owns both original flush records.
	if s, _, err := m.ReconnectSession(ctx, lost, MDSReconnectState{PendingCapFlushes: reconnectTestCapFlushes()}, MDSSessionOptions{RenewInterval: time.Hour}); err == nil || s != nil {
		t.Fatal("uncertain flush was silently replayed", s, err)
	}
}

func TestCephFSReconnectCapFlushRefusalPreservesTypedCauseAndUnknownFlush(t *testing.T) {
	for _, op := range []uint32{mds.SessionClose, mds.SessionReject} {
		t.Run(map[uint32]string{mds.SessionClose: "CLOSE", mds.SessionReject: "REJECT"}[op], func(t *testing.T) {
			c, lost, peers, ctx := lostCephFSSession(t, 0)
			m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/0"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			done := make(chan mdsSessionOpenResult, 1)
			go func() {
				s, _, err := m.ReconnectSession(ctx, lost, MDSReconnectState{PendingCapFlushes: reconnectTestCapFlushes()}, MDSSessionOptions{RenewInterval: time.Hour})
				done <- mdsSessionOpenResult{s, err}
			}()
			for range 3 {
				peer.next(t)
			}
			peer.send(t, mdsSessionV7(op, 0, map[string]string{"error_string": "caller rejected"}, 0, false))
			result := <-done
			var unknown *OutcomeUnknownError
			if result.session != nil || !errors.As(result.err, &unknown) {
				t.Fatal("reconnect refusal implied admitted cap flushes were rolled back", result.session, result.err)
			}
			if op == mds.SessionClose {
				if !errors.Is(result.err, ErrMDSSessionReconnectDenied) || !errors.Is(unknown.Cause, ErrMDSSessionClosed) {
					t.Fatal("typed CLOSE cause was lost", result.err)
				}
			} else {
				var rejected *MDSSessionRejectedError
				if !errors.As(result.err, &rejected) || rejected.Reason != "caller rejected" {
					t.Fatal("typed REJECT cause was lost", result.err)
				}
			}
		})
	}
}
