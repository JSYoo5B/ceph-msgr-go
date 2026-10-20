package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

func publicU64XattrOperation(value uint64) OSDOperation {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, value)
	return OSDOperation{Code: OSDCompareXattr, Xattr: OSDXattrInput{Name: "attr", Comparison: OSDXattrGreater, Mode: OSDXattrU64}, Data: data}
}

func TestOSDXattrU64PublicResultsAndKnownRefusals(t *testing.T) {
	for _, tc := range []struct {
		name          string
		result        int32
		failok, write bool
	}{
		{name: "read true", result: 1}, {name: "read false", result: -125}, {name: "invalid stored numeric", result: -22},
		{name: "write true", result: 1, write: true}, {name: "write false", result: -125, write: true}, {name: "FAILOK false guard", result: -125, write: true, failok: true},
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
			cmp := publicU64XattrOperation(0xfedcba9876543210)
			if tc.failok {
				cmp.Flags = OSDFailOK
			}
			input := osdValidationRequest()
			input.Operations = []OSDOperation{cmp}
			outputs, results := [][]byte{nil}, []int32{tc.result}
			aggregate := tc.result
			flags := uint64(0)
			if tc.write {
				input.Operations = append(input.Operations, OSDOperation{Code: OSDSetXattr, Xattr: OSDXattrInput{Name: "marker"}, Data: []byte("v")})
				input.ModificationTime = OSDTime{Seconds: 1700000000}
				outputs, results = append(outputs, nil), append(results, 0)
				if tc.result >= 0 || tc.failok {
					aggregate, flags = 0, osd.FlagOnDisk
				}
			}
			type response struct {
				reply OSDReply
				err   error
			}
			done := make(chan response, 1)
			go func() { r, e := o.Request(ctx, input); done <- response{r, e} }()
			sent := peer.next(t)
			if !bytes.HasPrefix(sent.Data, append([]byte("attr"), cmp.Data...)) {
				t.Fatal("public numeric bytes converted", sent.Data)
			}
			reply := osdXattrSyntheticReply(t, input.Operations, outputs, results, aggregate, flags)
			reply.Transaction = sent.Transaction
			if err := osdMapsWrite(peer, reply); err != nil {
				t.Fatal(err)
			}
			got := <-done
			if aggregate < 0 {
				var server *OSDError
				var unknown *OutcomeUnknownError
				if !errors.As(got.err, &server) || server.Code != aggregate || errors.As(got.err, &unknown) {
					t.Fatal("known comparison error lost", got.err)
				}
			} else if got.err != nil {
				t.Fatal(got.err)
			}
			result := got.reply.Operations[0]
			if result.Result != tc.result || result.Xattr.ComparisonMode != 2 || result.Xattr.ValueLength != 8 || !bytes.Equal(got.reply.RawFront, reply.Front) || !bytes.Equal(got.reply.RawData, reply.Data) || got.reply.Transaction != sent.Transaction || got.reply.Submission.Transaction != sent.Transaction {
				t.Fatal("numeric result or identity lost", got.reply)
			}
			if o.session.Err() != nil || dials.Load() != 1 {
				t.Fatal("known numeric refusal invalidated session")
			}
			select {
			case m := <-peer.requests:
				t.Fatal("numeric operation replayed", m.Transaction)
			default:
			}
		})
	}
}
