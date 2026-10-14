package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Source-derived synthetic controls exercise dispatch and uncertainty only.
// They are not native payload encoder vectors or live interoperability proof.
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MWatchNotify.h#L58
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MOSDBackoff.h#L54
func unsupportedOSDControl(typ uint16) msgr.MessageData {
	e := wire.Encoder{}
	if typ == 44 {
		e.U8(1) // payload version
		e.U8(1) // NOTIFY, not a request completion
		e.U64(17)
		e.U64(0) // obsolete version
		e.U64(19)
		e.Bytes([]byte{0, 255, 10})
		e.U32(0) // signed return code
		e.U64(23)
		return msgr.MessageData{Type: typ, Version: 3, CompatVersion: 1, Front: e.Data}
	}
	// spg_t v1 includes pg_t v1 and signed shard -1. PG seed 0 is an
	// actual PG, not a hash. Two equal hobjects identify a single object.
	pg := wire.Encoder{}
	pg.U8(1)
	pg.U64(1)
	pg.U32(0)
	pg.U32(^uint32(0))
	pg.U8(255)
	e.Struct(1, 1, pg.Data)
	e.U32(5)
	e.U8(1) // BLOCK
	e.U64(17)
	object := wire.Encoder{}
	object.String("")
	object.String("object")
	object.U64(^uint64(1)) // CEPH_NOSNAP
	object.U32(0)
	object.U8(0) // not maximum sentinel
	object.String("")
	object.U64(1)
	e.Struct(4, 3, object.Data)
	e.Struct(4, 3, object.Data)
	return msgr.MessageData{Type: typ, Version: 1, CompatVersion: 1, Front: e.Data}
}

func TestUnsupportedOSDControlsCannotCompleteOrReplaySentMutation(t *testing.T) {
	for _, typ := range []uint16{44, 61} {
		for _, collision := range []bool{false, true} {
			t.Run(fmt.Sprintf("type-%d/tid-collision-%t", typ, collision), func(t *testing.T) {
				c, peers, dials := osdTestClient(t)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
				if err != nil {
					t.Fatal(err)
				}
				peer := <-peers
				input := osdTestRequest()
				input.ClientIncarnation = 9
				input.Operations = []OSDOperation{{Code: OSDAppend, Length: 1, Data: []byte{255}}}
				type response struct {
					reply OSDReply
					err   error
				}
				finished := make(chan response, 1)
				go func() { reply, err := o.Request(ctx, input); finished <- response{reply, err} }()
				// Receiving the operation proves transmission started before
				// any unsolicited frame; no timeout guesses establish this.
				request := peer.next(t)
				if request.Type != msgr.OSDOpMessage || request.Transaction == 0 {
					t.Fatal("mutation was not sent", request)
				}
				control := unsupportedOSDControl(typ)
				if collision {
					control.Transaction = request.Transaction
				}
				// A complete peer write can race with receiver-triggered Close.
				// The resulting error is checked at the operation, not here.
				_ = osdMapsWrite(peer, control)
				var got response
				select {
				case got = <-finished:
				case <-ctx.Done():
					t.Fatal("unsupported control left mutation pending", ctx.Err())
				}
				var unknown *OutcomeUnknownError
				var serverError *OSDError
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, ErrUnsupportedFeatures) || errors.As(got.err, &serverError) {
					t.Fatal("control became a completion or lost unsupported/unknown cause", got.err)
				}
				want := OSDSubmission{Transaction: request.Transaction, GlobalID: 42, ClientIncarnation: 9, MapEpoch: 5}
				if got.reply.Submission != want || got.reply.Transaction != 0 || got.reply.GlobalID != 0 || got.reply.Flags != 0 || len(got.reply.Operations) != 0 || len(got.reply.RawFront) != 0 || len(got.reply.RawData) != 0 {
					t.Fatalf("control fabricated a reply or discarded local submission: %+v", got.reply)
				}
				if _, err := o.NextMap(ctx); !errors.Is(err, ErrUnsupportedFeatures) {
					t.Fatal("OSD session did not expose its terminal control error", err)
				}
				if err := o.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := o.NextMap(ctx); !errors.Is(err, ErrUnsupportedFeatures) {
					t.Fatal("Close overwrote the unsupported terminal cause", err)
				}
				// Close waits for session workers. The peer's completion closes
				// the other side of the I/O barrier before inspecting replays.
				select {
				case <-peer.done:
				case <-ctx.Done():
					t.Fatal("unsupported control did not clean up its session", ctx.Err())
				}
				if dials.Load() != 1 {
					t.Fatal("unsupported control caused an automatic OSD reconnect", dials.Load())
				}
				select {
				case replay := <-peer.requests:
					t.Fatal("unsupported control caused another application message", replay.Type, replay.Transaction)
				default:
				}
			})
		}
	}
}
