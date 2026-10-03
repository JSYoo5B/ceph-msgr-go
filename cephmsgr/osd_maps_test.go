package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// These transport/lifetime controls follow the pinned Tentacle MOSDMap
// envelope independently of the product decoder. Blob bodies deliberately
// remain opaque; actual map bytes are compared with Ceph by integration tests.
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MOSDMap.h#L81-L100
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MOSDMap.h#L101-L163
func osdMapsMessage(version uint16, fsid [16]byte, incremental, full []OSDMapBlob) msgr.MessageData {
	e := wire.Encoder{}
	e.Raw(fsid[:])
	for _, blobs := range [][]OSDMapBlob{incremental, full} {
		e.U32(uint32(len(blobs)))
		for _, blob := range blobs {
			e.U32(blob.Epoch)
			e.Bytes(blob.Data)
		}
	}
	compat := uint16(1)
	if version >= 2 {
		e.U32(3)  // server's trim lower bound
		e.U32(99) // latest available, not necessarily a delivered blob
		compat = 2
	}
	if version >= 4 {
		e.U32(0) // obsolete removed-snapshot map, always empty in Tentacle
		compat = 3
	}
	return msgr.MessageData{Type: 41, Version: version, CompatVersion: compat, Front: e.Data}
}

func osdMapsWrite(peer *osdTestPeer, message msgr.MessageData) error {
	peer.seq++
	message.Sequence = peer.seq
	return peer.writer.Write(message.Frame())
}

func osdMapsOpen(t *testing.T, limit uint32) (*Client, *OSDConnection, *osdTestPeer, context.Context) {
	t.Helper()
	c, peers, _ := osdTestClient(t)
	if limit != 0 {
		c.options.MaxBufferedOSDMapBytes = limit // Set before OSD workers start.
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case peer := <-peers:
		return c, o, peer, ctx
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return nil, nil, nil, nil
}

func osdMapsPending(t *testing.T, o *OSDConnection, peer *osdTestPeer, ctx context.Context) (<-chan error, msgr.MessageData) {
	t.Helper()
	result := make(chan error, 1)
	go func() { _, err := o.Request(ctx, osdTestRequest()); result <- err }()
	// Receiving an actual request proves both its started status and the peer
	// writer's publication, before this goroutine sends any map control frame.
	return result, peer.next(t)
}

func osdMapsAwaitResult(t *testing.T, ctx context.Context, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		t.Fatal("OSD request did not finish", ctx.Err())
	}
	return nil
}

func TestOSDMapBeforeReplyKeepsFIFOAndCallerOwnership(t *testing.T) {
	_, o, peer, ctx := osdMapsOpen(t, 0)
	result, request := osdMapsPending(t, o, peer, ctx)
	first := osdMapsMessage(1, [16]byte{1}, []OSDMapBlob{{Epoch: 5, Data: []byte{0, 255, 1}}}, []OSDMapBlob{{Epoch: 6, Data: []byte{8, 9}}})
	second := osdMapsMessage(4, [16]byte{1}, []OSDMapBlob{{Epoch: 7, Data: []byte{10}}}, []OSDMapBlob{{Epoch: 8, Data: []byte{11, 12}}})
	for _, message := range []msgr.MessageData{first, second} {
		if err := osdMapsWrite(peer, message); err != nil {
			t.Fatal(err)
		}
	}
	peer.reply(t, request)
	if err := osdMapsAwaitResult(t, ctx, result); err != nil {
		t.Fatal("map before reply terminated the object call", err)
	}
	batch, err := o.NextMap(ctx)
	if err != nil || batch.Version != 1 || batch.CompatVersion != 1 || batch.FSID != "01000000-0000-0000-0000-000000000000" || batch.OldestMap != 0 || batch.NewestMap != 0 || !bytes.Equal(batch.RawFront, first.Front) || len(batch.IncrementalMaps) != 1 || len(batch.FullMaps) != 1 || batch.IncrementalMaps[0].Epoch != 5 || batch.FullMaps[0].Epoch != 6 {
		t.Fatal("first map was lost, changed or coalesced", batch, err)
	}
	batch.IncrementalMaps[0].Data[0] = 77
	batch.RawFront[0] = 99
	if first.Front[0] != 1 {
		t.Fatal("caller mutation reached the source's envelope")
	}
	batch, err = o.NextMap(ctx)
	if err != nil || batch.Version != 4 || batch.CompatVersion != 3 || batch.OldestMap != 3 || batch.NewestMap != 99 || !bytes.Equal(batch.RawFront, second.Front) || len(batch.IncrementalMaps) != 1 || len(batch.FullMaps) != 1 || batch.IncrementalMaps[0].Epoch != 7 || batch.FullMaps[0].Epoch != 8 || !bytes.Equal(batch.IncrementalMaps[0].Data, []byte{10}) || !bytes.Equal(batch.FullMaps[0].Data, []byte{11, 12}) {
		t.Fatal("later map was coalesced or changed through an earlier returned slice", batch, err)
	}
}

func TestOSDMapCanceledNextDoesNotConsumePublication(t *testing.T) {
	_, o, peer, ctx := osdMapsOpen(t, 0)
	result, request := osdMapsPending(t, o, peer, ctx)
	message := osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 7, Data: []byte{3}}})
	if err := osdMapsWrite(peer, message); err != nil {
		t.Fatal(err)
	}
	peer.reply(t, request)
	if err := osdMapsAwaitResult(t, ctx, result); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if batch, err := o.NextMap(canceled); !errors.Is(err, context.Canceled) || len(batch.RawFront) != 0 {
		t.Fatal("canceled map wait consumed or returned publication", batch, err)
	}
	batch, err := o.NextMap(ctx)
	if err != nil || len(batch.FullMaps) != 1 || batch.FullMaps[0].Epoch != 7 {
		t.Fatal("publication did not survive canceled wait", batch, err)
	}
	result, request = osdMapsPending(t, o, peer, ctx)
	peer.reply(t, request)
	if err := osdMapsAwaitResult(t, ctx, result); err != nil {
		t.Fatal("local map cancellation damaged the object session", err)
	}
}

func TestOSDMapConcurrentWaitersReceiveEachBatchOnce(t *testing.T) {
	_, o, peer, ctx := osdMapsOpen(t, 0)
	result, request := osdMapsPending(t, o, peer, ctx)
	peer.reply(t, request)
	if err := osdMapsAwaitResult(t, ctx, result); err != nil {
		t.Fatal(err)
	}
	type received struct {
		batch OSDMapBatch
		err   error
	}
	ready, batches := make(chan struct{}, 8), make(chan received, 8)
	for range 8 {
		go func() { ready <- struct{}{}; batch, err := o.NextMap(ctx); batches <- received{batch, err} }()
	}
	for range 8 {
		<-ready
	}
	for i := uint32(1); i <= 8; i++ {
		message := osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: i, Data: []byte{byte(i)}}})
		if err := osdMapsWrite(peer, message); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[uint32]bool)
	for range 8 {
		select {
		case got := <-batches:
			if got.err != nil || len(got.batch.FullMaps) != 1 {
				t.Fatal("concurrent waiter did not get one map", got.batch, got.err)
			}
			blob := got.batch.FullMaps[0]
			if seen[blob.Epoch] || blob.Epoch == 0 || blob.Epoch > 8 || !bytes.Equal(blob.Data, []byte{byte(blob.Epoch)}) {
				t.Fatal("publication duplicated or changed between concurrent waiters", blob)
			}
			seen[blob.Epoch] = true
		case <-ctx.Done():
			t.Fatal("concurrent map waiter remained blocked", ctx.Err())
		}
	}
}

func TestOSDMapOverflowFailsStartedRequestButDrainsAcceptedFIFO(t *testing.T) {
	for _, profile := range []struct {
		name     string
		limit    uint32
		accepted uint32
	}{{"bytes", 1024, 1}, {"batch-count", 1 << 20, 64}} {
		t.Run(profile.name, func(t *testing.T) {
			_, o, peer, ctx := osdMapsOpen(t, profile.limit)
			result, _ := osdMapsPending(t, o, peer, ctx)
			for i := uint32(1); i <= profile.accepted; i++ {
				message := osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: i, Data: bytes.Repeat([]byte{byte(i)}, 32)}})
				if err := osdMapsWrite(peer, message); err != nil {
					t.Fatal("accepted map write failed", err)
				}
			}
			// The next valid map exceeds the local queue rather than wire limits.
			// Peer I/O can finish concurrently with the client's session failure.
			_ = osdMapsWrite(peer, osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 100, Data: bytes.Repeat([]byte{100}, 32)}}))
			err := osdMapsAwaitResult(t, ctx, result)
			var unknown *OutcomeUnknownError
			if !errors.As(err, &unknown) || !errors.Is(err, ErrOSDMapOverflow) {
				t.Fatal("started request lost the overflow cause or outcome uncertainty", err)
			}
			for i := uint32(1); i <= profile.accepted; i++ {
				batch, err := o.NextMap(ctx)
				if err != nil || len(batch.FullMaps) != 1 || batch.FullMaps[0].Epoch != i || !bytes.Equal(batch.FullMaps[0].Data, bytes.Repeat([]byte{byte(i)}, 32)) {
					t.Fatal("overflow discarded/coalesced accepted FIFO publication", i, batch, err)
				}
			}
			_, cause := o.NextMap(ctx)
			if !errors.Is(cause, ErrOSDMapOverflow) {
				t.Fatal("queue exhaustion did not expose overflow", cause)
			}
			o.Close()
			if _, err := o.NextMap(ctx); err != cause {
				t.Fatal("Close replaced the first terminal overflow cause", err, cause)
			}
		})
	}
}

func TestOSDMapInvalidPublicationsFailWithStableCauseAfterClose(t *testing.T) {
	for _, damage := range []string{"foreign-fsid", "truncated-envelope"} {
		for _, closer := range []string{"handle", "parent"} {
			t.Run(damage+"/"+closer, func(t *testing.T) {
				c, o, peer, ctx := osdMapsOpen(t, 0)
				result, _ := osdMapsPending(t, o, peer, ctx)
				accepted := osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 5, Data: []byte{8}}})
				if err := osdMapsWrite(peer, accepted); err != nil {
					t.Fatal(err)
				}
				invalid := osdMapsMessage(4, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 6, Data: []byte{9}}})
				if damage == "foreign-fsid" {
					invalid.Front[0] = 2
				} else {
					invalid.Front = invalid.Front[:len(invalid.Front)-1]
				}
				_ = osdMapsWrite(peer, invalid)
				err := osdMapsAwaitResult(t, ctx, result)
				var unknown *OutcomeUnknownError
				if !errors.As(err, &unknown) || !errors.Is(err, ErrMalformedMessage) {
					t.Fatal("invalid publication did not fail the started request with its raw cause", err)
				}
				if closer == "handle" {
					o.Close()
				} else {
					c.Close()
				}
				batch, err := o.NextMap(ctx)
				if err != nil || len(batch.FullMaps) != 1 || batch.FullMaps[0].Epoch != 5 {
					t.Fatal("invalid later map or shutdown discarded accepted publication", batch, err)
				}
				_, cause := o.NextMap(ctx)
				if !errors.Is(cause, ErrMalformedMessage) {
					t.Fatal("malformed map cause was replaced by shutdown", cause)
				}
				if _, err := o.NextMap(ctx); err != cause {
					t.Fatal("terminal map failure changed between calls", err, cause)
				}
			})
		}
	}
}

func TestOSDMapHandleAndParentCloseDrainAcceptedPublication(t *testing.T) {
	for _, closer := range []string{"handle", "parent"} {
		t.Run(closer, func(t *testing.T) {
			c, o, peer, ctx := osdMapsOpen(t, 0)
			result, request := osdMapsPending(t, o, peer, ctx)
			if err := osdMapsWrite(peer, osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 9, Data: []byte{8}}})); err != nil {
				t.Fatal(err)
			}
			peer.reply(t, request)
			if err := osdMapsAwaitResult(t, ctx, result); err != nil {
				t.Fatal(err)
			}
			if closer == "handle" {
				o.Close()
			} else {
				c.Close()
			}
			batch, err := o.NextMap(ctx)
			if err != nil || len(batch.FullMaps) != 1 || batch.FullMaps[0].Epoch != 9 {
				t.Fatal("shutdown discarded unread accepted map", batch, err)
			}
			for range 2 {
				if _, err := o.NextMap(ctx); !errors.Is(err, ErrClosed) {
					t.Fatal("closed connection did not expose stable terminal cause", err)
				}
			}
		})
	}
}

func TestOSDMapFailureIsReadableBeforeDelayedConnectionCleanup(t *testing.T) {
	for _, late := range []string{"none", "malformed", "overflow"} {
		t.Run(late, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			base, clientCancel := context.WithCancel(context.Background())
			conn, peer := net.Pipe()
			release := make(chan struct{})
			held := &managerCleanupConn{Conn: conn, closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
			var releaseOnce sync.Once
			finish := func() { releaseOnce.Do(func() { close(release) }) }
			c := &Client{ctx: base, cancel: clientCancel, changed: make(chan struct{}), auth: &cephx.Client{GlobalID: 42}, fsid: [16]byte{1}, options: Options{MaxFrameSize: wire.DefaultLimit, MaxBufferedOSDMapBytes: 1024}, sessions: make(map[*session.Session]struct{}), osds: make(map[int32]*OSDConnection)}
			osdBase, osdCancel := context.WithCancel(base)
			o := &OSDConnection{client: c, ctx: osdBase, cancel: osdCancel, globalID: 42, setupDone: make(chan struct{}), mapChanged: make(chan struct{})}
			close(o.setupDone)
			source := session.New(&session.Transport{Conn: held}, session.Config{}, nil, o.stopMaps)
			o.session = source
			c.osds[0], c.sessions[source] = o, struct{}{}
			// Register real cleanup ownership without protocol workers: failure
			// publishes Err and Done before Conn.Close or onClose completes.
			c.wg.Add(1)
			go func() { source.Wait(); c.wg.Done() }()
			t.Cleanup(func() { finish(); c.Close(); peer.Close(); osdCancel() })
			if err := o.handleMap(source, osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 7, Data: []byte{1}}})); err != nil {
				t.Fatal(err)
			}
			cause := &net.OpError{Op: "write", Net: "tcp", Err: io.ErrUnexpectedEOF}
			go source.Fail(cause)
			awaitCleanup(t, ctx, held.closing)
			if source.Err() != cause {
				t.Fatal("network failure was not published before cleanup blocked", source.Err())
			}
			if late != "none" {
				// A read callback can finish after a writer has already failed.
				// Keep onClose blocked and invoke the callback before Client.Close
				// disables admission, so both later error branches really execute.
				message := osdMapsMessage(4, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 8, Data: []byte{2}}})
				want := ErrOSDMapOverflow
				if late == "malformed" {
					message.Front = message.Front[:len(message.Front)-1]
					want = ErrMalformedMessage
				}
				callbackErr := o.handleMap(source, message)
				if !errors.Is(callbackErr, want) {
					t.Fatal("late callback did not encounter its expected map error", callbackErr, want)
				}
				// Match readLoop's callback-error path: Session's earlier cause
				// must win even while its connection cleanup is still blocked.
				source.Fail(callbackErr)
			}
			closed := make(chan error, 1)
			go func() { closed <- c.Close() }()
			batch, err := o.NextMap(ctx)
			if err != nil || len(batch.FullMaps) != 1 || batch.FullMaps[0].Epoch != 7 {
				t.Fatal("accepted batch was hidden by delayed cleanup", batch, err)
			}
			for range 2 {
				if _, err := o.NextMap(ctx); err != cause {
					t.Fatal("NextMap waited for cleanup or replaced the earlier network cause", err, cause)
				}
			}
			select {
			case err := <-closed:
				t.Fatal("Client.Close returned before owned connection cleanup", err)
			default:
			}
			finish()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("Client.Close did not join released OSD cleanup", ctx.Err())
			}
			if _, err := o.NextMap(ctx); err != cause {
				t.Fatal("onClose replaced the terminal cause after cleanup completed", err, cause)
			}
		})
	}
}

type osdMapsWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *osdMapsWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestOSDMapCanceledConnectionWaitsForSessionFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	base, clientCancel := context.WithCancel(context.Background())
	conn, peer := net.Pipe()
	release := make(chan struct{})
	held := &managerCleanupConn{Conn: conn, closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
	var releaseOnce sync.Once
	finish := func() { releaseOnce.Do(func() { close(release) }) }
	c := &Client{ctx: base, cancel: clientCancel, changed: make(chan struct{}), auth: &cephx.Client{GlobalID: 42}, sessions: make(map[*session.Session]struct{}), osds: make(map[int32]*OSDConnection)}
	osdBase, osdCancel := context.WithCancel(base)
	o := &OSDConnection{client: c, ctx: osdBase, cancel: osdCancel, globalID: 42, setupDone: make(chan struct{}), mapChanged: make(chan struct{})}
	close(o.setupDone)
	source := session.New(&session.Transport{Conn: held}, session.Config{}, nil, o.stopMaps)
	o.session = source
	c.osds[0], c.sessions[source] = o, struct{}{}
	c.wg.Add(1)
	go func() { source.Wait(); c.wg.Done() }()
	t.Cleanup(func() { finish(); c.Close(); peer.Close(); osdCancel() })

	// Close cancels this context before failing its active session. Cancellation
	// alone must not fix ErrClosed while a concurrent writer is still able to
	// publish the session's winning network error.
	osdCancel()
	waitCtx := &osdMapsWaitContext{Context: ctx, waiting: make(chan struct{})}
	result := make(chan error, 1)
	go func() { _, err := o.NextMap(waitCtx); result <- err }()
	// Done observation proves NextMap reached its wait after seeing the already
	// canceled connection context; it avoids a sleep or scheduling assumption.
	select {
	case <-waitCtx.waiting:
	case err := <-result:
		t.Fatal("connection cancellation completed NextMap before session arbitration", err)
	case <-ctx.Done():
		t.Fatal("NextMap did not reach the session wait", ctx.Err())
	}
	select {
	case err := <-result:
		t.Fatal("NextMap did not remain pending before session failure", err)
	default:
	}
	if err := source.Err(); err != nil {
		t.Fatal("connection context canceled the session before failure publication", err)
	}

	cause := &net.OpError{Op: "read", Net: "tcp", Err: io.ErrUnexpectedEOF}
	go source.Fail(cause)
	awaitCleanup(t, ctx, held.closing)
	if err := osdMapsAwaitResult(t, ctx, result); err != cause {
		t.Fatal("NextMap lost the session's network cause after connection cancellation", err, cause)
	}
	select {
	case <-held.cleaned:
		t.Fatal("connection cleanup completed before its release")
	default:
	}
	if _, err := o.NextMap(ctx); err != cause {
		t.Fatal("terminal cause changed before connection cleanup", err, cause)
	}
	finish()
	source.Wait()
	if _, err := o.NextMap(ctx); err != cause {
		t.Fatal("onClose changed the cause after connection cleanup", err, cause)
	}
}
