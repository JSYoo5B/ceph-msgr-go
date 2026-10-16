package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// These tests exercise actual Dial/OpenOSD/session callbacks with synthetic
// peers. They prove concurrency and control policy, not native Ceph interop.
func backoffTransportFeatures(tag msgr.Tag, p []byte, mask uint64) []byte {
	if tag != msgr.ServerIdent {
		return p
	}
	// Synthetic ServerIdent ends with id,global_seq,supported,required,
	// reconnect_seq,flags. Both client admission and server requirements are
	// explicit in this synthetic profile; live acceptance is tested separately.
	copy := bytes.Clone(p)
	at := len(copy) - 32
	binary.LittleEndian.PutUint64(copy[at:at+8], binary.LittleEndian.Uint64(copy[at:at+8])|mask)
	at += 8
	binary.LittleEndian.PutUint64(copy[at:at+8], binary.LittleEndian.Uint64(copy[at:at+8])|mask)
	return copy
}

func backoffTransportOpen(t *testing.T) (*Client, *OSDConnection, *osdTestPeer, context.Context, *atomic.Uint32) {
	t.Helper()
	options := mockOptions(t, 20, [16]byte{1})
	options.EnableOSD = true
	options.OSDProfile = OSDProfileReplicatedPG
	options.MaxOSDConnections = 1
	peers := make(chan *osdTestPeer, 1)
	ready := make(chan struct{})
	var dials atomic.Uint32
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		client, server := net.Pipe()
		if endpoint != "192.0.2.1:6804" {
			go mockDaemon(server, peerConfig{role: 1, release: 20, fsid: [16]byte{1}, services: cephx.ServiceAuth | cephx.ServiceMgr | cephx.ServiceOSD, payload: func(tag msgr.Tag, p []byte) []byte { return backoffTransportFeatures(tag, p, 1<<30) }})
			return client, nil
		}
		dials.Add(1)
		peer := &osdTestPeer{conn: server, requests: make(chan msgr.MessageData, 16), done: make(chan struct{})}
		peers <- peer
		go func() {
			defer close(peer.done)
			defer server.Close()
			r, w, err := mockAuthenticate(server, peerConfig{role: 4, payload: func(tag msgr.Tag, p []byte) []byte {
				return backoffTransportFeatures(tag, p, 1<<8|1<<9|1<<21|1<<30|1<<56|1<<57)
			}})
			if err != nil {
				return
			}
			peer.writer = w
			close(ready)
			for {
				frame, err := r.Read()
				if err != nil {
					return
				}
				if frame.Tag != msgr.Message {
					continue
				}
				m, err := msgr.DecodeMessage(frame)
				if err != nil {
					return
				}
				peer.requests <- m
			}
		}()
		return client, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return c, o, peer, ctx, &dials
}

func backoffTransportRequest() OSDRequest {
	r := osdTestRequest()
	r.ClientIncarnation = 9
	r.Object.Hash = 0x12345678
	r.Placement = &OSDReplicatedPlacement{PG: OSDPG{Pool: 1, Seed: 0}, Primary: 0}
	return r
}
func backoffTransportControl(t *testing.T, id uint64, op uint8, epoch uint32) msgr.MessageData {
	t.Helper()
	object := osd.PGCursor{Pool: 1, Object: "object", Hash: 0x12345678, Snap: osd.NoSnap}
	m, err := osd.EncodeBackoff(osd.Backoff{PG: osd.PG{Pool: 1, Seed: 0}, Shard: -1, MapEpoch: epoch, Op: op, ID: id, Begin: object, End: object}, 4096)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestOSDBackoffTransportExplicitAckGateAndUnblock(t *testing.T) {
	_, o, peer, ctx, dials := backoffTransportOpen(t)
	block := backoffTransportControl(t, 17, osd.BackoffBlock, 5)
	if err := osdMapsWrite(peer, block); err != nil {
		t.Fatal(err)
	}
	event, err := o.NextBackoff(ctx)
	if err != nil || event.ID != 17 || event.Op != OSDBackoffBlock || o.client.options.OSDProfile != OSDProfileReplicatedPG {
		t.Fatal(event, err)
	}
	select {
	case message := <-peer.requests:
		t.Fatal("BLOCK was automatically acknowledged", message.Type)
	default:
	}
	// Intervening maps must not replace the epoch copied into ACK_BLOCK.
	if err := osdMapsWrite(peer, osdMapsMessage(4, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 8, Data: []byte{1}}})); err != nil {
		t.Fatal(err)
	}
	if _, err := o.NextMap(ctx); err != nil {
		t.Fatal(err)
	}
	if err := o.AckBackoff(ctx, event); err != nil {
		t.Fatal(err)
	}
	ack := peer.next(t)
	decoded, err := osd.DecodeBackoff(ack, 4096)
	if err != nil || ack.Priority != 127 || ack.Transaction != 0 || decoded.Op != osd.BackoffAckBlock || decoded.ID != 17 || decoded.MapEpoch != 5 || decoded.PG != (osd.PG{Pool: 1, Seed: 0}) || decoded.Shard != -1 || !bytes.Equal(decoded.Begin.Raw, event.Begin.Raw) || !bytes.Equal(decoded.End.Raw, event.End.Raw) {
		t.Fatal("ACK changed original control identity", decoded, err)
	}
	if err := o.AckBackoff(ctx, event); !errors.Is(err, ErrOSDBackoffAcknowledged) {
		t.Fatal("duplicate ACK was queued", err)
	}
	request := backoffTransportRequest()
	request.Operations = []OSDOperation{{Code: OSDAppend, Length: 1, Data: []byte{255}}}
	rejected, err := o.Request(ctx, request)
	var unknown *OutcomeUnknownError
	if !errors.Is(err, ErrOSDBackoff) || errors.As(err, &unknown) || rejected.Submission.Transaction == 0 || rejected.Submission.GlobalID != 42 || rejected.Submission.ClientIncarnation != 9 || rejected.Submission.MapEpoch != 5 || rejected.Submission.Placement == nil || *rejected.Submission.Placement != *request.Placement || rejected.Transaction != 0 || len(rejected.RawFront) != 0 {
		t.Fatal("blocked object lost known unsent outcome or submission", rejected, err)
	}
	if err := osdMapsWrite(peer, backoffTransportControl(t, 17, osd.BackoffUnblock, 8)); err != nil {
		t.Fatal(err)
	}
	released, err := o.NextBackoff(ctx)
	if err != nil || released.Op != OSDBackoffUnblock || !released.Matched || released.MapEpoch != 8 {
		t.Fatal(released, err)
	}
	// UNBLOCK only releases local admission. A new caller submission is the
	// next object observed by the peer; the refused mutation is never replayed.
	type completed struct {
		r OSDReply
		e error
	}
	done := make(chan completed, 1)
	go func() { r, e := o.Request(ctx, backoffTransportRequest()); done <- completed{r, e} }()
	sent := peer.next(t)
	if sent.Type != msgr.OSDOpMessage || sent.Version != 8 || sent.Transaction <= rejected.Submission.Transaction {
		t.Fatal("fresh v8 caller request missing or old request replayed", sent)
	}
	peer.reply(t, sent)
	got := <-done
	if got.e != nil || got.r.Submission.Transaction != sent.Transaction || len(got.r.RawData) != 1 || got.r.RawData[0] != 42 {
		t.Fatal(got.r, got.e)
	}
	if dials.Load() != 1 {
		t.Fatal("backoff triggered automatic reconnect", dials.Load())
	}
	select {
	case message := <-peer.requests:
		t.Fatal("backoff generated an extra object or ACK", message.Type, message.Transaction)
	default:
	}
}

func TestOSDBackoffTransportCommandsEventsAndStartedCancellation(t *testing.T) {
	_, o, peer, ctx, _ := backoffTransportOpen(t)
	input := backoffTransportRequest()
	input.Operations = []OSDOperation{{Code: OSDAppend, Length: 1, Data: []byte{255}}}
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type completed struct {
		r OSDReply
		e error
	}
	pending := make(chan completed, 1)
	go func() { r, e := o.Request(requestCtx, input); pending <- completed{r, e} }()
	sent := peer.next(t)
	if sent.Type != msgr.OSDOpMessage || sent.Version != 8 {
		t.Fatal(sent)
	}
	route := input
	route.Operations = nil
	// Reserve synthetic local event ownership. Native watch registration is
	// covered separately; this isolates event dispatch while the gate is active.
	stream, err := o.newWatchStream(route, OSDWatchOptions{}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	block := backoffTransportControl(t, 22, osd.BackoffBlock, 5)
	block.Transaction = sent.Transaction
	if err := osdMapsWrite(peer, block); err != nil {
		t.Fatal(err)
	}
	event, err := o.NextBackoff(ctx)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-pending:
		t.Fatal("BLOCK completed an already started request", result.e)
	default:
	}
	cancel()
	got := <-pending
	var unknown *OutcomeUnknownError
	if !errors.Is(got.e, context.Canceled) || !errors.As(got.e, &unknown) || got.r.Submission.Transaction != sent.Transaction {
		t.Fatal("backoff altered a started request outcome", got.r, got.e)
	}
	if err := osdMapsWrite(peer, watchSyntheticEvent(OSDWatchNotification, stream.cookie, 99, []byte{0, 255}, nil)); err != nil {
		t.Fatal(err)
	}
	notification, err := stream.Next(ctx)
	if err != nil || notification.NotifyID != 99 || !bytes.Equal(notification.Payload, []byte{0, 255}) {
		t.Fatal("backoff blocked event reception", notification, err)
	}
	commands := make(chan error, 1)
	go func() { _, err := o.Command(ctx, Command{JSON: []byte(`{"prefix":"version"}`)}); commands <- err }()
	command := peer.next(t)
	if command.Type != msgr.TellCommandMessage {
		t.Fatal("backoff blocked or changed a command", command.Type)
	}
	if err := osdMapsWrite(peer, osdCommandReply(command.Transaction, 0, "ok", []byte("native"))); err != nil {
		t.Fatal(err)
	}
	if err := <-commands; err != nil {
		t.Fatal(err)
	}
	// The explicit ACK is still permitted with another operation's TID in the
	// original control; its own TID remains zero and cannot complete that call.
	if err := o.AckBackoff(ctx, event); err != nil {
		t.Fatal(err)
	}
	ack := peer.next(t)
	if ack.Type != osd.BackoffMessage || ack.Transaction != 0 {
		t.Fatal("backoff ACK inherited request identity", ack)
	}
}

func TestOSDBackoffTransportCloseDrainsAcceptedControls(t *testing.T) {
	_, o, peer, ctx, _ := backoffTransportOpen(t)
	for _, op := range []uint8{osd.BackoffBlock, osd.BackoffUnblock} {
		if err := osdMapsWrite(peer, backoffTransportControl(t, 31, op, 5)); err != nil {
			t.Fatal(err)
		}
	}
	// One reader handles control frames in order. A later dequeued map proves
	// both earlier backoff events were accepted before Close wins its race.
	if err := osdMapsWrite(peer, osdMapsMessage(4, [16]byte{1}, nil, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := o.NextMap(ctx); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	for _, op := range []uint8{OSDBackoffBlock, OSDBackoffUnblock} {
		event, err := o.NextBackoff(ctx)
		if err != nil || event.Op != op || event.ID != 31 {
			t.Fatal("Close discarded accepted backoff", event, err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := o.NextBackoff(ctx); !errors.Is(err, ErrClosed) {
			t.Fatal("stable terminal backoff cause lost", err)
		}
	}
}

func TestOSDBackoffDefaultProfileRejectsConsumerImmediately(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	<-peers
	if _, err := o.NextBackoff(context.Background()); !errors.Is(err, ErrUnsupportedFeatures) {
		t.Fatal("unsupported backoff consumer waited or claimed support", err)
	}
}

func TestOSDBackoffTransportUnblockBeforeAckKeepsGateAndOrdersLateAck(t *testing.T) {
	_, o, peer, ctx, _ := backoffTransportOpen(t)
	if err := osdMapsWrite(peer, backoffTransportControl(t, 61, osd.BackoffBlock, 5)); err != nil {
		t.Fatal(err)
	}
	original, err := o.NextBackoff(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := osdMapsWrite(peer, backoffTransportControl(t, 61, osd.BackoffUnblock, 9)); err != nil {
		t.Fatal(err)
	}
	released, err := o.NextBackoff(ctx)
	if err != nil || !released.Matched {
		t.Fatal(released, err)
	}
	request := backoffTransportRequest()
	request.Operations = []OSDOperation{{Code: OSDAppend, Length: 1, Data: []byte{255}}}
	rejected, err := o.Request(ctx, request)
	var unknown *OutcomeUnknownError
	if !errors.Is(err, ErrOSDBackoff) || errors.As(err, &unknown) || rejected.Submission.Transaction == 0 {
		t.Fatal("UNBLOCK discarded its unacknowledged server tombstone gate", rejected, err)
	}
	// The original BLOCK, not the newer UNBLOCK, supplies the late ACK epoch.
	ackCtx, cancel := context.WithCancel(ctx)
	if err := o.AckBackoff(ackCtx, original); err != nil {
		t.Fatal("late explicit ACK refused", err)
	}
	cancel() // A successfully queued protocol ACK cannot now be withdrawn.
	type completed struct {
		r OSDReply
		e error
	}
	done := make(chan completed, 1)
	go func() { r, e := o.Request(ctx, backoffTransportRequest()); done <- completed{r, e} }()
	ack, fresh := peer.next(t), peer.next(t)
	decoded, err := osd.DecodeBackoff(ack, 4096)
	if err != nil || decoded.Op != osd.BackoffAckBlock || decoded.ID != 61 || decoded.MapEpoch != 5 || ack.Transaction != 0 || fresh.Type != msgr.OSDOpMessage || fresh.Transaction <= rejected.Submission.Transaction || fresh.Sequence != ack.Sequence+1 {
		t.Fatal("fresh request passed or lost the retained late ACK", decoded, fresh, err)
	}
	peer.reply(t, fresh)
	got := <-done
	if got.e != nil {
		t.Fatal(got.r, got.e)
	}
}

// The two fixed public profiles select different reply envelopes while sharing
// the same v8 request identity. These replies only drive synthetic lifetime and
// correlation checks; they are not independent native operation vectors.
func backoffProfileReadReply(t *testing.T, peer *osdTestPeer, profile OSDProfile, input OSDRequest, sent msgr.MessageData) {
	t.Helper()
	m := replyV8ProfileMessage(input, [][]byte{{42}}, []int32{0}, 0, osd.FlagAck|osd.FlagOnDisk, false)
	if profile == OSDProfileReplicatedPG {
		locator := wire.Encoder{}
		locator.U64(^uint64(0))
		locator.U32(^uint32(0))
		locator.String("")
		locator.String("")
		locator.U64(^uint64(0))
		redirect := wire.Encoder{}
		redirect.Struct(6, 3, locator.Data)
		redirect.String("")
		redirect.Bytes(nil)
		empty := wire.Encoder{}
		empty.Struct(1, 1, redirect.Data)
		// v6 always includes the redirect structure; v8's empty redirect is
		// a single false byte immediately preceding the three-u64 trace.
		at := len(m.Front) - 25
		m.Front = append(append(append([]byte(nil), m.Front[:at]...), empty.Data...), m.Front[at+1:]...)
		m.Version = 6
	}
	m.Transaction = sent.Transaction
	if err := osdMapsWrite(peer, m); err != nil {
		t.Fatal(err)
	}
}

func TestOSDBackoffTransportUnrelatedObjectsProgressBeforeLateAck(t *testing.T) {
	for _, profile := range []OSDProfile{OSDProfileReplicatedPG, OSDProfileReplicatedPGReplyV8} {
		t.Run(string(rune('0'+profile)), func(t *testing.T) {
			open := backoffTransportOpen
			if profile == OSDProfileReplicatedPGReplyV8 {
				open = replyV8ProfileOpen
			}
			_, o, peer, ctx, dials := open(t)
			if err := osdMapsWrite(peer, backoffTransportControl(t, 91, osd.BackoffBlock, 5)); err != nil {
				t.Fatal(err)
			}
			original, err := o.NextBackoff(ctx)
			if err != nil {
				t.Fatal(err)
			}
			type outcome struct {
				r OSDReply
				e error
			}
			for _, released := range []bool{false, true} {
				if released {
					if err := osdMapsWrite(peer, backoffTransportControl(t, 91, osd.BackoffUnblock, 9)); err != nil {
						t.Fatal(err)
					}
					if event, err := o.NextBackoff(ctx); err != nil || !event.Matched {
						t.Fatal(event, err)
					}
				}
				blocked := backoffTransportRequest()
				blocked.Operations = []OSDOperation{{Code: OSDAppend, Length: 1, Data: []byte{255}}}
				blocked.MapEpoch = 99 // A newer request epoch cannot evade this range.
				refused, err := o.Request(ctx, blocked)
				var unknown *OutcomeUnknownError
				if !errors.Is(err, ErrOSDBackoff) || errors.As(err, &unknown) || refused.Submission.Transaction == 0 || refused.Transaction != 0 {
					t.Fatal("matching object escaped or lost known unsent identity", released, refused, err)
				}
				for _, anotherPG := range []bool{false, true} {
					input := backoffTransportRequest()
					if anotherPG {
						input.Placement.PG.Seed = 1
					} else {
						input.Object.Name = "unrelated-object"
					}
					done := make(chan outcome, 1)
					go func() { r, e := o.Request(ctx, input); done <- outcome{r, e} }()
					sent := peer.next(t)
					identity, err := osd.DecodeRequestV8AdmissionTarget(sent, 4096)
					if err != nil || sent.Type != osd.RequestMessage || sent.Transaction <= refused.Submission.Transaction || identity.Object.Object != input.Object.Name || identity.PG.Seed != input.Placement.PG.Seed {
						t.Fatal("unrelated request did not reach peer with its own identity", identity, err)
					}
					backoffProfileReadReply(t, peer, profile, input, sent)
					got := <-done
					if got.e != nil || got.r.Transaction != sent.Transaction || !bytes.Equal(got.r.RawData, []byte{42}) {
						t.Fatal("unrelated request did not finish during BLOCK/tombstone", released, anotherPG, got)
					}
				}
			}
			if err := o.AckBackoff(ctx, original); err != nil {
				t.Fatal(err)
			}
			ack := peer.next(t)
			decoded, err := osd.DecodeBackoff(ack, 4096)
			if err != nil || decoded.MapEpoch != 5 || decoded.Op != osd.BackoffAckBlock {
				t.Fatal("late ACK lost original epoch", decoded, err)
			}
			input := backoffTransportRequest()
			done := make(chan outcome, 1)
			go func() { r, e := o.Request(ctx, input); done <- outcome{r, e} }()
			fresh := peer.next(t)
			if fresh.Sequence != ack.Sequence+1 {
				t.Fatal("fresh matching request did not follow late ACK on writer", ack, fresh)
			}
			backoffProfileReadReply(t, peer, profile, input, fresh)
			if got := <-done; got.e != nil {
				t.Fatal(got)
			}
			if dials.Load() != 1 {
				t.Fatal("range backoff caused reconnect", dials.Load())
			}
			select {
			case extra := <-peer.requests:
				t.Fatal("refused mutation was replayed", extra.Transaction)
			default:
			}
		})
	}
}
