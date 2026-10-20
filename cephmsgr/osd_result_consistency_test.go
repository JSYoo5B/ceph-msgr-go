package cephmsgr

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// Mutate an independently checked two-op native reply envelope to inject
// result combinations. These fault inputs test public request semantics;
// they are not native encoder proof or observed server misbehavior.
func osdGuardMutationReply(t *testing.T, aggregate, guard int32, flags OSDOperationFlags, onDisk bool) msgr.MessageData {
	t.Helper()
	m := osdValidationFixture(t, "reply-v6-success")
	completion := uint64(0)
	if onDisk {
		completion = OSDReplyOnDisk
	}
	binary.LittleEndian.PutUint64(m.Front[30:38], completion)
	binary.LittleEndian.PutUint32(m.Front[38:42], uint32(aggregate))
	binary.LittleEndian.PutUint16(m.Front[62:64], uint16(OSDAssertVersion))
	binary.LittleEndian.PutUint32(m.Front[64:68], uint32(flags))
	binary.LittleEndian.PutUint64(m.Front[76:84], 19)
	binary.LittleEndian.PutUint32(m.Front[96:100], 0)
	binary.LittleEndian.PutUint16(m.Front[100:102], uint16(OSDAppend))
	binary.LittleEndian.PutUint64(m.Front[106:114], 0)
	binary.LittleEndian.PutUint64(m.Front[114:122], 1)
	binary.LittleEndian.PutUint32(m.Front[134:138], 0)
	binary.LittleEndian.PutUint32(m.Front[142:146], uint32(guard))
	binary.LittleEndian.PutUint32(m.Front[146:150], 0)
	m.Data = nil
	return m
}

func TestOSDSuccessfulVectorRejectsFailedOrdinaryGuard(t *testing.T) {
	for _, tc := range []struct {
		name             string
		aggregate, guard int32
		flags            OSDOperationFlags
		onDisk, invalid  bool
	}{
		{name: "contradictory-success", aggregate: 0, guard: -34, onDisk: true, invalid: true},
		{name: "failok-retains-failure", aggregate: 0, guard: -34, flags: OSDFailOK, onDisk: true},
		{name: "known-refusal-unexecuted-append", aggregate: -34, guard: -34},
		{name: "positive-codes-preserved", aggregate: 1, guard: 1, onDisk: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peers, dials := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			input := osdValidationRequest()
			input.ClientIncarnation = 9
			input.Operations = []OSDOperation{
				{Code: OSDAssertVersion, Version: 19, Flags: tc.flags},
				{Code: OSDAppend, Length: 1, Data: []byte{255}},
			}
			type response struct {
				reply OSDReply
				err   error
			}
			finished := make(chan response, 1)
			go func() { reply, err := o.Request(ctx, input); finished <- response{reply, err} }()
			request := peer.next(t)
			if request.Type != msgr.OSDOpMessage || request.Transaction == 0 {
				t.Fatal("mutation was not sent", request)
			}
			m := osdGuardMutationReply(t, tc.aggregate, tc.guard, tc.flags, tc.onDisk)
			m.Transaction = request.Transaction
			_ = osdMapsWrite(peer, m)
			var got response
			select {
			case got = <-finished:
			case <-ctx.Done():
				t.Fatal("result validation left mutation pending", ctx.Err())
			}
			want := OSDSubmission{Transaction: request.Transaction, GlobalID: 42, ClientIncarnation: 9, MapEpoch: 11}
			if got.reply.Submission != want || got.reply.Transaction != request.Transaction || got.reply.Code != tc.aggregate || len(got.reply.Operations) != 2 || got.reply.Operations[0].Result != tc.guard || got.reply.Operations[1].Result != 0 || len(got.reply.RawFront) == 0 {
				t.Fatal("wire results or local submission identity lost", got.reply, got.err)
			}
			var unknown *OutcomeUnknownError
			var serverError *OSDError
			switch {
			case tc.invalid:
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, msgr.ErrFrame) || errors.As(got.err, &serverError) {
					t.Fatal("contradictory result completed a mutation", got.err)
				}
				if _, err := o.NextMap(ctx); !errors.Is(err, msgr.ErrFrame) {
					t.Fatal("contradictory reply did not fail its OSD session", err)
				}
			case tc.aggregate < 0:
				if !errors.As(got.err, &serverError) || serverError.Code != tc.aggregate || errors.As(got.err, &unknown) {
					t.Fatal("known guard refusal became an unknown outcome", got.err)
				}
			default:
				if got.err != nil {
					t.Fatal("valid FAILOK or positive result rejected", got.err)
				}
			}
			if !tc.invalid && o.session.Err() != nil {
				t.Fatal("valid result failed the session", o.session.Err())
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-peer.done:
			case <-ctx.Done():
				t.Fatal("OSD cleanup did not finish", ctx.Err())
			}
			if dials.Load() != 1 {
				t.Fatal("result validation automatically reconnected", dials.Load())
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("result validation replayed mutation", replay.Transaction)
			default:
			}
		})
	}
}
