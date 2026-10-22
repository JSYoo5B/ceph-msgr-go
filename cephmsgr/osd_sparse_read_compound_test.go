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

func sparseReadCompoundInput() OSDRequest {
	r := osdValidationRequest()
	r.ClientIncarnation = 52
	r.Operations = []OSDOperation{
		{Code: OSDSparseRead, Length: 64, SparseRead: OSDSparseReadInput{MaxOutputBytes: 4096}},
		{Code: OSDRead, Offset: 64, Length: 4},
		{Code: OSDSparseRead, Offset: 128, Length: 64, Flags: OSDOperationFlags(osd.OpFadviseSequential), SparseRead: OSDSparseReadInput{MaxOutputBytes: 4096}},
	}
	return r
}

func sparseReadCompoundPayload(offset uint64, data []byte) []byte {
	e := wire.Encoder{}
	if len(data) == 0 {
		e.U32(0)
	} else {
		e.U32(1)
		e.U64(offset)
		e.U64(uint64(len(data)))
	}
	e.U32(uint32(len(data)))
	return append(e.Data, data...)
}

// Authenticated synthetic transcript, deliberately not a native byte oracle.
// lengths overrides each reply union's extent length (the returned bytes).
func sparseReadCompoundMessage(input OSDRequest, profile OSDProfile, outputs [][]byte, lengths []uint64, results []int32, aggregate int32) msgr.MessageData {
	m := checksumCompoundMessage(input, profile, outputs, results, aggregate, false)
	first := 4 + len(input.Object.Name) + 17 + 8 + 4 + 12 + 4 + 4
	for i := range input.Operations {
		binary.LittleEndian.PutUint64(m.Front[first+i*38+6+8:], lengths[i])
	}
	return m
}

func TestOSDSparseReadCompoundRefusesScopeBeforeSharedAdmission(t *testing.T) {
	for name, edit := range map[string]func(*OSDRequest){
		"snapshot":      func(r *OSDRequest) { r.ReadSnapshot = 3 },
		"balance":       func(r *OSDRequest) { r.BalanceReads = true },
		"localize":      func(r *OSDRequest) { r.LocalizeReads = true },
		"cache":         func(r *OSDRequest) { r.IgnoreCache = true },
		"resubmit":      func(r *OSDRequest) { r.Resubmit = &OSDSubmission{} },
		"retry":         func(r *OSDRequest) { r.RetryAttempt = 1 },
		"FAILOK-sparse": func(r *OSDRequest) { r.Operations[0].Flags = OSDFailOK },
		"FAILOK-read":   func(r *OSDRequest) { r.Operations[1].Flags = OSDFailOK },
		"truncate":      func(r *OSDRequest) { r.Operations[1].TruncateSize, r.Operations[1].TruncateSequence = 4, 2 },
		"stat":          func(r *OSDRequest) { r.Operations[1] = OSDOperation{Code: OSDStat} },
		"write":         func(r *OSDRequest) { r.Operations[1] = OSDOperation{Code: OSDWriteFull, Length: 1, Data: []byte{1}} },
		"mapext": func(r *OSDRequest) {
			r.Operations[1] = OSDOperation{Code: OSDMapExt, Length: 8, MapExt: OSDMapExtInput{MaxOutputBytes: 64}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			o := backoffUnitConnection()
			o.client.calls = make(chan struct{}, 1)
			o.client.calls <- struct{}{}
			r := sparseReadCompoundInput()
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
	r := sparseReadCompoundInput()
	r.Placement = &OSDPlacement{Erasure: true}
	if err := o.validateChecksumCompound(r); !errors.Is(err, osd.ErrRequest) {
		t.Fatal("erasure compound admitted", err)
	}
	r.Operations = r.Operations[:1]
	if err := o.validateChecksumCompound(r); err != nil {
		t.Fatal("standalone preflight changed", err)
	}
}

func TestOSDSparseReadCompoundPublicPrefixAllProfiles(t *testing.T) {
	for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPG, OSDProfileReplicatedPGReplyV8, OSDProfileReplicatedPGRequestV9ReplyV8} {
		for _, kind := range []string{"success", "terminal", "early-error"} {
			t.Run(profileNameForCacheTest(profile)+"/"+kind, func(t *testing.T) {
				c, o, peer, ctx := enforceSnapProfileOpen(t, profile)
				r := sparseReadCompoundInput()
				if profile.replicatedPG() {
					r.Placement = &OSDPlacement{PG: OSDPG{Pool: uint64(r.Object.Pool), Seed: 3}, Primary: o.target.ID}
				}
				first := sparseReadCompoundPayload(0, []byte{0, 255, 'A', '\n'})
				read := []byte{1, 2, 3, 4}
				last := sparseReadCompoundPayload(132, []byte{9, 8})
				outputs := [][]byte{first, read, last}
				lengths := []uint64{4, 4, 2}
				codes, aggregate := []int32{0, 0, 0}, int32(0)
				switch kind {
				case "terminal":
					outputs[2], lengths[2], codes[2], aggregate = nil, 64, -5, -5
				case "early-error":
					outputs, lengths, aggregate = [][]byte{nil, nil, nil}, []uint64{64, 4, 64}, -2
				}
				done := make(chan struct {
					reply OSDReply
					err   error
				}, 1)
				go func() {
					got, err := o.Request(ctx, r)
					done <- struct {
						reply OSDReply
						err   error
					}{got, err}
				}()
				sent := peer.next(t)
				m := sparseReadCompoundMessage(r, profile, outputs, lengths, codes, aggregate)
				m.Transaction = sent.Transaction
				if err := osdMapsWrite(peer, m); err != nil {
					t.Fatal(err)
				}
				var got OSDReply
				var err error
				select {
				case result := <-done:
					got, err = result.reply, result.err
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if got.Submission.Transaction != sent.Transaction || got.Submission.ClientIncarnation != 52 || !reflect.DeepEqual(got.Submission.Placement, r.Placement) {
					t.Fatal("compound submission identity lost", got)
				}
				var server *OSDError
				var unknown *OutcomeUnknownError
				if errors.As(err, &unknown) || aggregate < 0 && (!errors.As(err, &server) || server.Code != aggregate) || aggregate == 0 && err != nil {
					t.Fatal("valid compound outcome misreported", got, err)
				}
				if !bytes.Equal(got.RawData, bytes.Join(outputs, nil)) {
					t.Fatal("compound raw data lost")
				}
				wantFirst := OSDSparseReadResult{Extents: []OSDExtent{{Offset: 0, Length: 4}}, Data: []byte{0, 255, 'A', '\n'}}
				switch kind {
				case "success":
					if !reflect.DeepEqual(got.Operations[0].SparseRead, wantFirst) || !bytes.Equal(got.Operations[1].Data, read) || !reflect.DeepEqual(got.Operations[2].SparseRead, OSDSparseReadResult{Extents: []OSDExtent{{Offset: 132, Length: 2}}, Data: []byte{9, 8}}) {
						t.Fatal("validated compound outputs not decoded", got.Operations)
					}
				case "terminal":
					if !reflect.DeepEqual(got.Operations[0].SparseRead, wantFirst) || !bytes.Equal(got.Operations[1].Data, read) || got.Operations[2].SparseRead.Extents != nil || got.Operations[2].Result != -5 {
						t.Fatal("completed prefix before signed error lost or failed op decoded", got.Operations)
					}
				case "early-error":
					for i, op := range got.Operations {
						if op.SparseRead.Extents != nil || op.Data != nil || op.Result != 0 || op.Length != r.Operations[i].Length {
							t.Fatal("ambiguous early error claimed executed output", op)
						}
					}
				}
				select {
				case extra := <-peer.requests:
					t.Fatal("automatic replay", extra.Transaction)
				default:
				}
				if c.osdNext.Load() != 1 {
					t.Fatal("unexpected transaction allocation")
				}
			})
		}
	}
}

func TestOSDSparseReadCompoundMalformedFailsSession(t *testing.T) {
	for _, kind := range []string{"aggregate", "positive-aggregate-negative-op", "suffix-data", "terminal-data", "budget", "length"} {
		t.Run(kind, func(t *testing.T) {
			_, o, peer, ctx, _ := replyV8ProfileOpen(t)
			r := sparseReadCompoundInput()
			r.Placement = &OSDPlacement{PG: OSDPG{Pool: uint64(r.Object.Pool), Seed: 3}, Primary: o.target.ID}
			if kind == "budget" {
				r.Operations[0].SparseRead.MaxOutputBytes = 8
			}
			done := make(chan struct {
				reply OSDReply
				err   error
			}, 1)
			go func() {
				got, err := o.Request(ctx, r)
				done <- struct {
					reply OSDReply
					err   error
				}{got, err}
			}()
			sent := peer.next(t)
			outputs := [][]byte{sparseReadCompoundPayload(0, []byte{1, 2, 3, 4}), {5, 6, 7, 8}, nil}
			lengths, codes, aggregate := []uint64{4, 4, 64}, []int32{0, 0, -5}, int32(-5)
			switch kind {
			case "aggregate":
				aggregate = -22
			case "positive-aggregate-negative-op":
				aggregate = 0
			case "suffix-data":
				outputs[2], codes[1], aggregate = []byte{1}, -5, -5
				lengths[1] = 4
			case "terminal-data":
				outputs[2] = sparseReadCompoundPayload(128, nil)
			case "length":
				lengths[0] = 64
			}
			m := sparseReadCompoundMessage(r, OSDProfileReplicatedPGReplyV8, outputs, lengths, codes, aggregate)
			m.Transaction = sent.Transaction
			_ = osdMapsWrite(peer, m)
			select {
			case got := <-done:
				var unknown *OutcomeUnknownError
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, msgr.ErrFrame) {
					t.Fatal("malformed compound accepted", got.err)
				}
				if !bytes.Equal(got.reply.RawFront, m.Front) || !bytes.Equal(got.reply.RawData, m.Data) {
					t.Fatal("malformed compound lost its raw reply")
				}
				for _, op := range got.reply.Operations {
					if op.SparseRead.Extents != nil || op.SparseRead.Data != nil {
						t.Fatal("malformed compound exposed unvalidated sparse-read output", op)
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
