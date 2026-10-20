package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func checksumCompoundInput() OSDRequest {
	r := osdValidationRequest()
	r.ClientIncarnation = 51
	r.Operations = []OSDOperation{
		{Code: OSDChecksum, Length: 8, Checksum: OSDChecksumInput{Type: OSDChecksumCRC32C, ChunkSize: 4, Seed: 0x12345678}},
		{Code: OSDAssertVersion, Version: 19},
		{Code: OSDRead, Length: 4},
	}
	return r
}

func checksumCompoundPayload(values ...uint32) []byte {
	e := wire.Encoder{}
	e.U32(uint32(len(values)))
	for _, value := range values {
		e.U32(value)
	}
	return e.Data
}

// Authenticated synthetic transcript, deliberately not a native byte oracle.
func checksumCompoundMessage(input OSDRequest, profile OSDProfile, outputs [][]byte, results []int32, aggregate int32, redirect bool) msgr.MessageData {
	copyInput := input
	if copyInput.Placement == nil {
		copyInput.Placement = &OSDPlacement{PG: OSDPG{Pool: uint64(input.Object.Pool), Seed: input.Object.Hash}}
	}
	m := replyV8ProfileMessage(copyInput, outputs, results, aggregate, osd.FlagAck|osd.FlagOnDisk, redirect)
	first := 4 + len(input.Object.Name) + 17 + 8 + 4 + 12 + 4 + 4
	for i, op := range input.Operations {
		if op.Code == OSDChecksum {
			at := first + i*38 + 6
			binary.LittleEndian.PutUint32(m.Front[at+16:], op.Checksum.ChunkSize)
			m.Front[at+20] = byte(op.Checksum.Type)
		}
	}
	if !profile.replyV8() {
		m.Version = 6
		// v6 always carries the redirect structure; v8 carries its presence
		// byte. This transcript's v6 coverage uses the empty structure.
		locator := wire.Encoder{}
		locator.U64(^uint64(0))
		locator.U32(^uint32(0))
		locator.String("")
		locator.String("")
		locator.U64(^uint64(0))
		body := wire.Encoder{}
		body.Struct(6, 3, locator.Data)
		body.String("")
		body.Bytes(nil)
		encoded := wire.Encoder{}
		encoded.Struct(1, 1, body.Data)
		trace := append([]byte(nil), m.Front[len(m.Front)-24:]...)
		m.Front = append(m.Front[:len(m.Front)-25], encoded.Data...)
		m.Front = append(m.Front, trace...)
	}
	return m
}

func TestOSDChecksumCompoundRefusesScopeBeforeSharedAdmission(t *testing.T) {
	for name, edit := range map[string]func(*OSDRequest){
		"snapshot":        func(r *OSDRequest) { r.ReadSnapshot = 3 },
		"balance":         func(r *OSDRequest) { r.BalanceReads = true },
		"localize":        func(r *OSDRequest) { r.LocalizeReads = true },
		"cache":           func(r *OSDRequest) { r.IgnoreCache = true },
		"resubmit":        func(r *OSDRequest) { r.Resubmit = &OSDSubmission{} },
		"retry":           func(r *OSDRequest) { r.RetryAttempt = 1 },
		"FAILOK-checksum": func(r *OSDRequest) { r.Operations[0].Flags = OSDFailOK },
		"FAILOK-guard":    func(r *OSDRequest) { r.Operations[1].Flags = OSDFailOK },
		"write":           func(r *OSDRequest) { r.Operations[1] = OSDOperation{Code: OSDWriteFull, Length: 1, Data: []byte{1}} },
		"CALL": func(r *OSDRequest) {
			r.Operations[1] = OSDOperation{Code: OSDCall, Class: OSDClassInput{Mode: OSDCallRead}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			o := backoffUnitConnection()
			o.client.calls = make(chan struct{}, 1)
			o.client.calls <- struct{}{}
			r := checksumCompoundInput()
			edit(&r)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := o.Request(ctx, r)
			var unknown *OutcomeUnknownError
			if err == nil || errors.As(err, &unknown) || got.Submission != (OSDSubmission{}) || o.client.osdNext.Load() != 0 || len(o.client.calls) != 1 {
				t.Fatal("invalid compound consumed shared admission/identity", got, err)
			}
		})
	}
	o := backoffUnitConnection()
	r := checksumCompoundInput()
	r.Placement = &OSDPlacement{Erasure: true}
	if err := o.validateChecksumCompound(r); !errors.Is(err, osd.ErrRequest) {
		t.Fatal("erasure compound admitted", err)
	}
	r.Operations = r.Operations[:1]
	if err := o.validateChecksumCompound(r); err != nil {
		t.Fatal("standalone preflight changed", err)
	}
}

func TestOSDChecksumCompoundPublicPrefixAndSessionAllProfiles(t *testing.T) {
	for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPG, OSDProfileReplicatedPGReplyV8, OSDProfileReplicatedPGRequestV9ReplyV8} {
		for _, kind := range []string{"terminal-guard", "EOF-prefix", "early-error"} {
			t.Run(profileNameForCacheTest(profile)+"/"+kind, func(t *testing.T) {
				c, o, peer, ctx := enforceSnapProfileOpen(t, profile)
				r := checksumCompoundInput()
				if profile.replicatedPG() {
					r.Placement = &OSDPlacement{PG: OSDPG{Pool: uint64(r.Object.Pool), Seed: 3}, Primary: o.target.ID}
				}
				type response struct {
					reply OSDReply
					err   error
				}
				call := func(input OSDRequest, output [][]byte, codes []int32, aggregate int32) (OSDReply, error) {
					done := make(chan response, 1)
					go func() { got, err := o.Request(ctx, input); done <- response{got, err} }()
					sent := peer.next(t)
					m := checksumCompoundMessage(input, profile, output, codes, aggregate, false)
					m.Transaction = sent.Transaction
					if err := osdMapsWrite(peer, m); err != nil {
						t.Fatal(err)
					}
					select {
					case got := <-done:
						if got.reply.Submission.Transaction != sent.Transaction || got.reply.Submission.MapEpoch != input.MapEpoch || got.reply.Submission.ClientIncarnation != 51 || !reflect.DeepEqual(got.reply.Submission.Placement, input.Placement) {
							t.Fatal("compound submission identity lost", got.reply)
						}
						return got.reply, got.err
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					return OSDReply{}, nil
				}
				outputs, codes, aggregate := [][]byte{checksumCompoundPayload(11, 22), nil, nil}, []int32{0, -75, 0}, int32(-75)
				if kind == "EOF-prefix" {
					r.Operations[0].Offset = 100
					outputs[0] = nil
				}
				if kind == "early-error" {
					outputs[0] = nil
					codes[1] = 0
					aggregate = -2
				}
				got, err := call(r, outputs, codes, aggregate)
				var server *OSDError
				var unknown *OutcomeUnknownError
				if !errors.As(err, &server) || server.Code != aggregate || errors.As(err, &unknown) || !bytes.Equal(got.RawData, outputs[0]) || len(got.RawFront) == 0 {
					t.Fatal("valid negative aggregate became unknown or lost raw prefix", got, err)
				}
				if kind == "terminal-guard" && !reflect.DeepEqual(got.Operations[0].Checksum.Values, []uint64{11, 22}) {
					t.Fatal("valid error hid prefix checksum values", got.Operations[0])
				}
				if kind != "terminal-guard" && len(got.Operations[0].Checksum.Values) != 0 {
					t.Fatal("empty/ambiguous checksum claimed values")
				}
				if got.Operations[2].Result != 0 || got.Operations[2].PayloadLength != 0 || got.Operations[2].Length != r.Operations[2].Length {
					t.Fatal("unexecuted suffix was reinterpreted", got.Operations[2])
				}
				fresh := r
				fresh.Operations = []OSDOperation{r.Operations[0], {Code: OSDRead, Length: 4}}
				fresh.Operations[0].Offset = 0
				after, err := call(fresh, [][]byte{checksumCompoundPayload(33, 44), {0, 255, 128, 1}}, []int32{0, 0}, 0)
				if err != nil || after.Transaction == got.Transaction || !reflect.DeepEqual(after.Operations[0].Checksum.Values, []uint64{33, 44}) || c.osdNext.Load() != 2 {
					t.Fatal("valid error poisoned session or silently replayed", after, err)
				}
				select {
				case extra := <-peer.requests:
					t.Fatal("automatic replay", extra.Transaction)
				default:
				}
			})
		}
	}
}

func TestOSDChecksumCompoundMalformedErrorFailsSession(t *testing.T) {
	for _, kind := range []string{"aggregate", "positive-aggregate-negative-op", "suffix-data", "suffix-negative", "checksum-count", "extra-data"} {
		t.Run(kind, func(t *testing.T) {
			_, o, peer, ctx, _ := replyV8ProfileOpen(t)
			r := checksumCompoundInput()
			r.Placement = &OSDPlacement{PG: OSDPG{Pool: uint64(r.Object.Pool), Seed: 3}, Primary: o.target.ID}
			type response struct {
				reply OSDReply
				err   error
			}
			done := make(chan response, 1)
			go func() { got, err := o.Request(ctx, r); done <- response{got, err} }()
			sent := peer.next(t)
			outputs, codes, aggregate := [][]byte{checksumCompoundPayload(11, 22), nil, nil}, []int32{0, -75, 0}, int32(-75)
			switch kind {
			case "aggregate":
				aggregate = -34
			case "positive-aggregate-negative-op":
				aggregate = 0
			case "suffix-data":
				outputs[2] = []byte{1}
			case "suffix-negative":
				codes[2] = -2
			case "checksum-count":
				outputs[0] = checksumCompoundPayload(11)
			}
			m := checksumCompoundMessage(r, OSDProfileReplicatedPGReplyV8, outputs, codes, aggregate, false)
			if kind == "extra-data" {
				m.Data = append(m.Data, 9)
			}
			m.Transaction = sent.Transaction
			_ = osdMapsWrite(peer, m)
			select {
			case got := <-done:
				var unknown *OutcomeUnknownError
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, msgr.ErrFrame) {
					t.Fatal("malformed compound accepted as signed server error", got.err)
				}
				if !bytes.Equal(got.reply.RawFront, m.Front) || !bytes.Equal(got.reply.RawData, m.Data) {
					t.Fatal("malformed compound lost its raw reply")
				}
				for _, op := range got.reply.Operations {
					if len(op.Checksum.Values) != 0 {
						t.Fatal("malformed compound exposed unvalidated checksum values", op)
					}
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case extra := <-peer.requests:
				t.Fatal("malformed reply triggered replay", extra.Transaction)
			default:
			}
		})
	}
}

func TestOSDChecksumCompoundSentCancellationPreservesSubmission(t *testing.T) {
	_, o, peer, parent, _ := replyV8ProfileOpen(t)
	r := checksumCompoundInput()
	r.Placement = &OSDPlacement{PG: OSDPG{Pool: uint64(r.Object.Pool), Seed: 3}, Primary: o.target.ID}
	ctx, cancel := context.WithCancel(parent)
	type response struct {
		reply OSDReply
		err   error
	}
	done := make(chan response, 1)
	go func() { got, err := o.Request(ctx, r); done <- response{got, err} }()
	sent := peer.next(t)
	cancel()
	select {
	case got := <-done:
		var unknown *OutcomeUnknownError
		if !errors.As(got.err, &unknown) || !errors.Is(got.err, context.Canceled) || got.reply.Submission.Transaction != sent.Transaction || got.reply.Submission.MapEpoch != r.MapEpoch || !reflect.DeepEqual(got.reply.Submission.Placement, r.Placement) || got.reply.Transaction != 0 {
			t.Fatal("sent cancellation lost original uncertain identity", got)
		}
	case <-parent.Done():
		t.Fatal(parent.Err())
	}
	select {
	case extra := <-peer.requests:
		t.Fatal("cancelled read automatically replayed", extra.Transaction)
	default:
	}
}

func TestOSDChecksumCompoundRawRedirectRemainsUnfollowed(t *testing.T) {
	_, o, peer, ctx, dials := replyV8ProfileOpen(t)
	r := checksumCompoundInput()
	r.Placement = &OSDPlacement{PG: OSDPG{Pool: uint64(r.Object.Pool), Seed: 3}, Primary: o.target.ID}
	type response struct {
		reply OSDReply
		err   error
	}
	done := make(chan response, 1)
	go func() { got, err := o.Request(ctx, r); done <- response{got, err} }()
	sent := peer.next(t)
	m := checksumCompoundMessage(r, OSDProfileReplicatedPGReplyV8, [][]byte{nil, nil, nil}, []int32{0, 0, 0}, 0, true)
	m.Transaction = sent.Transaction
	_ = osdMapsWrite(peer, m)
	select {
	case got := <-done:
		var unknown *OutcomeUnknownError
		if !errors.Is(got.err, ErrOSDRedirect) || errors.As(got.err, &unknown) || got.reply.Redirect.Object != "redirect-object" || got.reply.Submission.Transaction != sent.Transaction || len(got.reply.Operations[0].Checksum.Values) != 0 || dials.Load() != 1 {
			t.Fatal("redirect was reinterpreted or followed", got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case extra := <-peer.requests:
		t.Fatal("redirect automatically replayed", extra.Transaction)
	default:
	}
}
