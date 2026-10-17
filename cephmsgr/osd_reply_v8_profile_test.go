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

// These authenticated synthetic peers exercise public transport and admission;
// they do not establish live Ceph interoperability or pool compatibility.
func replyV8ProfileOpen(t *testing.T) (*Client, *OSDConnection, *osdTestPeer, context.Context, *atomic.Uint32) {
	return replicatedReplyV8ProfileOpen(t, OSDProfileReplicatedPGReplyV8)
}

func replicatedReplyV8ProfileOpen(t *testing.T, profile OSDProfile) (*Client, *OSDConnection, *osdTestPeer, context.Context, *atomic.Uint32) {
	t.Helper()
	options := mockOptions(t, 20, [16]byte{1})
	options.EnableOSD = true
	options.OSDProfile = profile
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
				features := uint64(1<<8 | 1<<9 | 1<<21 | 1<<30 | 1<<56 | 1<<57 | 1<<58)
				if profile.requestV9() {
					features |= 1 << 49
				}
				return backoffTransportFeatures(tag, p, features)
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

// Construct fault-injection v8 replies from the documented layout. The retained
// independent native vectors are verified separately; these inputs deliberately
// exercise both valid and contradictory operation/completion metadata.
func replyV8ProfileMessage(input OSDRequest, outputs [][]byte, results []int32, aggregate int32, flags uint64, redirect bool) msgr.MessageData {
	e := wire.Encoder{}
	e.String(input.Object.Name)
	e.U8(1)
	e.U64(uint64(input.Object.Pool))
	e.U32(input.Placement.PG.Seed)
	e.U32(^uint32(0))
	e.U64(flags)
	e.U32(uint32(aggregate))
	e.U64(13)
	e.U32(8)
	e.U32(input.MapEpoch)
	e.U32(uint32(len(input.Operations)))
	var data []byte
	for i, op := range input.Operations {
		e.U16(uint16(op.Code))
		e.U32(uint32(op.Flags))
		if osd.IsWatchOperation(uint16(op.Code)) {
			osd.EncodeWatchUnion(&e, osd.Operation{Code: uint16(op.Code), Watch: watchWireInput(op.Watch)})
		} else {
			e.U64(op.Offset)
			length := op.Length
			if op.Code == OSDAssertVersion {
				length = op.Version
			}
			e.U64(length)
			e.U64(0)
			e.U32(0)
		}
		e.U32(uint32(len(outputs[i])))
		data = append(data, outputs[i]...)
	}
	e.U32(0)
	for _, code := range results {
		e.U32(uint32(code))
	}
	e.U64(91)
	e.U32(8)
	e.U64(13)
	if redirect {
		e.U8(1)
		locator := wire.Encoder{}
		locator.U64(9)
		locator.U32(^uint32(0))
		locator.String("redirect-key")
		locator.String("redirect-namespace")
		locator.U64(^uint64(0))
		body := wire.Encoder{}
		body.Struct(6, 3, locator.Data)
		body.String("redirect-object")
		body.Bytes(nil)
		e.Struct(1, 1, body.Data)
	} else {
		e.U8(0)
	}
	e.U64(11)
	e.U64(22)
	e.U64(33)
	return msgr.MessageData{Type: msgr.OSDOpReplyMessage, Version: 8, CompatVersion: 2, Front: e.Data, Data: data}
}

func TestOSDReplyV8ProfilePlacementAdmission(t *testing.T) {
	for _, profile := range []OSDProfile{OSDProfileReplicatedPG, OSDProfileReplicatedPGReplyV8} {
		if err := validateOSDProfile(Options{OSDProfile: profile}); !errors.Is(err, ErrOSDDisabled) {
			t.Fatal(profile, err)
		}
		if err := validateOSDProfile(Options{EnableOSD: true, OSDProfile: profile}); err != nil {
			t.Fatal(profile, err)
		}
		o := backoffUnitConnection()
		o.client.options.OSDProfile = profile
		request := backoffTransportRequest()
		request.Placement.Primary = o.target.ID
		if err := o.validatePlacement(request); err != nil {
			t.Fatal(profile, err)
		}
		for _, edit := range []func(*OSDRequest){func(r *OSDRequest) { r.Placement = nil }, func(r *OSDRequest) { r.Placement.Primary++ }, func(r *OSDRequest) { r.Placement.PG.Pool++ }} {
			bad := request
			bad.Placement = cloneOSDPlacement(request.Placement)
			edit(&bad)
			if err := o.validatePlacement(bad); err == nil {
				t.Fatal("replicated placement guard bypassed", profile)
			}
		}
	}
	if OSDProfileObjectHash != 0 || OSDProfileReplicatedPG != 1 || OSDProfileReplicatedPGReplyV8 != 2 {
		t.Fatal("closed fixed profiles changed")
	}
}

func TestOSDReplyV8ProfileRequestResultAndNoReplay(t *testing.T) {
	testReplicatedReplyV8RequestResults(t, OSDProfileReplicatedPGReplyV8)
}

func testReplicatedReplyV8RequestResults(t *testing.T, profile OSDProfile) {
	for _, kind := range []string{"read", "enoent", "unknown-error", "mutation-ondisk", "mutation-ack-only", "failed-guard", "failok", "redirect", "wrong-pg", "wrong-version", "wrong-compat", "disconnect"} {
		t.Run(kind, func(t *testing.T) {
			c, o, peer, ctx, dials := replicatedReplyV8ProfileOpen(t, profile)
			input := backoffTransportRequest()
			input.Object.Name = "object"
			input.Placement.PG.Seed = 7
			outputs, results, aggregate, flags := [][]byte{{42}}, []int32{0}, int32(0), uint64(osd.FlagAck|osd.FlagOnDisk)
			switch kind {
			case "enoent", "unknown-error":
				aggregate = -2
				if kind == "unknown-error" {
					aggregate = -12345
				}
				results[0] = aggregate
				outputs[0] = nil
			case "mutation-ondisk", "mutation-ack-only", "disconnect":
				input.Operations = []OSDOperation{{Code: OSDAppend, Length: 1, Data: []byte{255}}}
				outputs[0] = nil
				if kind == "mutation-ack-only" {
					flags = osd.FlagAck
				}
			case "failed-guard", "failok":
				input.Operations = []OSDOperation{{Code: OSDAssertVersion, Version: 19}, {Code: OSDAppend, Length: 1, Data: []byte{255}}}
				if kind == "failok" {
					input.Operations[0].Flags = OSDFailOK
				}
				outputs, results = [][]byte{nil, nil}, []int32{-34, 0}
			case "redirect":
				outputs[0] = nil
			}
			type response struct {
				reply OSDReply
				err   error
			}
			done := make(chan response, 1)
			go func() { r, e := o.Request(ctx, input); done <- response{r, e} }()
			sent := peer.next(t)
			assertReplicatedRequestProfile(t, profile, sent)
			// The negotiated-feature tail is explicit and includes bit58 only here.
			if tail := binary.LittleEndian.Uint64(sent.Front[len(sent.Front)-8:]); tail&(1<<58) == 0 {
				t.Fatal("v8 request feature tail omitted reply encoding", tail)
			}
			if kind == "disconnect" {
				peer.conn.Close()
			} else {
				m := replyV8ProfileMessage(input, outputs, results, aggregate, flags, kind == "redirect")
				m.Transaction = sent.Transaction
				switch kind {
				case "wrong-pg":
					binary.LittleEndian.PutUint32(m.Front[4+len(input.Object.Name)+9:], 8)
				case "wrong-version":
					m.Version = 6
				case "wrong-compat":
					m.CompatVersion = 1
				}
				_ = osdMapsWrite(peer, m)
			}
			var got response
			select {
			case got = <-done:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if got.reply.Submission.Transaction != sent.Transaction || got.reply.Submission.GlobalID != 42 || got.reply.Submission.ClientIncarnation != 9 || got.reply.Submission.Placement == nil || *got.reply.Submission.Placement != *input.Placement {
				t.Fatal("request identity/placement lost", got.reply, got.err)
			}
			invalid := kind == "mutation-ack-only" || kind == "failed-guard" || kind == "wrong-pg" || kind == "wrong-version" || kind == "wrong-compat" || kind == "disconnect"
			var unknown *OutcomeUnknownError
			var server *OSDError
			switch {
			case invalid:
				if !errors.As(got.err, &unknown) {
					t.Fatal("uncertain result lost", kind, got.err)
				}
			case aggregate < 0:
				if !errors.As(got.err, &server) || server.Code != aggregate || errors.As(got.err, &unknown) {
					t.Fatal(got.reply, got.err)
				}
			case kind == "redirect":
				if !errors.Is(got.err, ErrOSDRedirect) || errors.As(got.err, &unknown) || got.reply.Redirect.Object != "redirect-object" {
					t.Fatal(got.reply, got.err)
				}
			default:
				if got.err != nil {
					t.Fatal(kind, got.reply, got.err)
				}
			}
			if kind != "disconnect" && kind != "wrong-version" && kind != "wrong-compat" {
				if got.reply.Code != aggregate || !bytes.Equal(got.reply.RawData, bytes.Join(outputs, nil)) {
					t.Fatal("signed/raw metadata lost", got.reply)
				}
				for i, code := range results {
					if got.reply.Operations[i].Result != code {
						t.Fatal(got.reply)
					}
				}
			}
			if kind == "read" && !bytes.Equal(got.reply.Operations[0].Data, []byte{42}) {
				t.Fatal(got.reply)
			}
			o.Close()
			if dials.Load() != 1 {
				t.Fatal("reply caused automatic reroute", dials.Load())
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("reply caused replay", replay.Transaction)
			default:
			}
			_ = c
		})
	}
}

func TestOSDReplyV8ProfileListPGStrictDecoder(t *testing.T) {
	testReplicatedReplyV8PGListResults(t, OSDProfileReplicatedPGReplyV8)
}

func testReplicatedReplyV8PGListResults(t *testing.T, profile OSDProfile) {
	for _, kind := range []string{"page", "end", "v6-forbidden", "redirect"} {
		t.Run(kind, func(t *testing.T) {
			_, o, peer, ctx, dials := replicatedReplyV8ProfileOpen(t, profile)
			r := pgListRequest()
			r.PG.Primary = 0
			r.PG.Seed = 7
			type response struct {
				reply OSDPGListReply
				err   error
			}
			done := make(chan response, 1)
			go func() { reply, err := o.ListPG(ctx, r); done <- response{reply, err} }()
			sent := peer.next(t)
			assertReplicatedRequestProfile(t, profile, sent)
			input := OSDRequest{MapEpoch: r.MapEpoch, Object: OSDObject{Pool: int64(r.PG.Pool)}, Placement: &OSDPlacement{PG: OSDPG{Pool: r.PG.Pool, Seed: r.PG.Seed}}, Operations: []OSDOperation{{Code: OSDOpcode(osd.PGNLS), Offset: r.Count, Length: uint64(r.StartEpoch)}}}
			data, aggregate := pgListData(), int32(0)
			if kind == "end" {
				aggregate = 1
			}
			if kind == "redirect" {
				data = nil
			}
			m := replyV8ProfileMessage(input, [][]byte{data}, []int32{0}, aggregate, osd.FlagAck|osd.FlagOnDisk, kind == "redirect")
			if kind == "v6-forbidden" {
				m = pgListSyntheticReply(t, r, data, aggregate)
			}
			m.Transaction = sent.Transaction
			_ = osdMapsWrite(peer, m)
			got := <-done
			switch kind {
			case "v6-forbidden":
				var unknown *OutcomeUnknownError
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, wire.ErrVersion) {
					t.Fatal(got.err)
				}
			case "redirect":
				if !errors.Is(got.err, ErrOSDRedirect) || got.reply.Redirect.Object != "redirect-object" {
					t.Fatal(got.reply, got.err)
				}
			default:
				if got.err != nil || len(got.reply.Entries) != 1 || got.reply.End != (kind == "end") || !bytes.Equal(got.reply.RawData, data) {
					t.Fatal(got.reply, got.err)
				}
			}
			o.Close()
			if dials.Load() != 1 {
				t.Fatal(dials.Load())
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("listing replayed", replay.Transaction)
			default:
			}
		})
	}
}

func TestOSDReplyV8ProfileBackoffAdmissionGate(t *testing.T) {
	_, o, peer, ctx, dials := replyV8ProfileOpen(t)
	if err := osdMapsWrite(peer, backoffTransportControl(t, 71, osd.BackoffBlock, 5)); err != nil {
		t.Fatal(err)
	}
	event, err := o.NextBackoff(ctx)
	if err != nil || event.ID != 71 {
		t.Fatal(event, err)
	}
	if err := o.AckBackoff(ctx, event); err != nil {
		t.Fatal(err)
	}
	if ack := peer.next(t); ack.Type != osd.BackoffMessage {
		t.Fatal(ack)
	}
	input := backoffTransportRequest()
	input.Operations = []OSDOperation{{Code: OSDAppend, Length: 1, Data: []byte{255}}}
	blocked, err := o.Request(ctx, input)
	var unknown *OutcomeUnknownError
	if !errors.Is(err, ErrOSDBackoff) || errors.As(err, &unknown) || blocked.Submission.Transaction == 0 {
		t.Fatal(blocked, err)
	}
	if err := osdMapsWrite(peer, backoffTransportControl(t, 71, osd.BackoffUnblock, 6)); err != nil {
		t.Fatal(err)
	}
	unblock, err := o.NextBackoff(ctx)
	if err != nil || !unblock.Matched {
		t.Fatal(unblock, err)
	}
	select {
	case replay := <-peer.requests:
		t.Fatal("UNBLOCK replayed mutation", replay.Transaction)
	default:
	}
	type response struct {
		reply OSDReply
		err   error
	}
	done := make(chan response, 1)
	go func() { r, e := o.Request(ctx, input); done <- response{r, e} }()
	sent := peer.next(t)
	m := replyV8ProfileMessage(input, [][]byte{nil}, []int32{0}, 0, osd.FlagAck|osd.FlagOnDisk, false)
	m.Transaction = sent.Transaction
	_ = osdMapsWrite(peer, m)
	got := <-done
	if got.err != nil || got.reply.Flags&OSDReplyOnDisk == 0 {
		t.Fatal(got.reply, got.err)
	}
	if dials.Load() != 1 {
		t.Fatal(dials.Load())
	}
}
