package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

func TestOSDTruncateFieldsAndTrimmedWrites(t *testing.T) {
	data := bytes.Repeat([]byte{7}, 1000)
	write := OSDOperation{Code: OSDWrite, Offset: 17000, Length: 1000, Data: data, TruncateSize: 280000, TruncateSequence: 1}
	for _, tc := range []struct {
		name    string
		echo    OSDOperation
		invalid bool
	}{
		{name: "old write trimmed at the object size", echo: func() OSDOperation { e := write; e.Length = 856; return e }()},
		{name: "old write trimmed to nothing", echo: func() OSDOperation { e := write; e.Length = 0; return e }()},
		{name: "full write", echo: write},
		{name: "truncate fields not echoed", echo: func() OSDOperation { e := write; e.TruncateSequence = 2; return e }(), invalid: true},
		{name: "write extended", echo: func() OSDOperation { e := write; e.Length = 1001; return e }(), invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peers, _ := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			input := osdValidationRequest()
			input.Operations = []OSDOperation{write}
			input.ModificationTime = OSDTime{Seconds: 1700000000}
			type response struct {
				reply OSDReply
				err   error
			}
			done := make(chan response, 1)
			go func() { reply, err := o.Request(ctx, input); done <- response{reply, err} }()
			m := peer.next(t)
			// The extent union: offset, length, truncate_size, truncate_seq.
			if !bytes.Contains(m.Front, binary.LittleEndian.AppendUint32(binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint64(nil, 17000), 1000), 280000), 1)) {
				t.Fatal("request did not carry the truncate fields")
			}
			reply := osdXattrSyntheticReply(t, []OSDOperation{tc.echo}, [][]byte{nil}, []int32{0}, 0, uint64(osd.FlagOnDisk|osd.FlagAck))
			reply.Transaction = m.Transaction
			_ = osdMapsWrite(peer, reply)
			got := <-done
			var unknown *OutcomeUnknownError
			if tc.invalid {
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, msgr.ErrFrame) {
					t.Fatal("contradictory truncate reply completed", got.err)
				}
				return
			}
			if got.err != nil || got.reply.Operations[0].Length != tc.echo.Length || got.reply.Operations[0].TruncateSequence != 1 || got.reply.Operations[0].TruncateSize != 280000 {
				t.Fatal("trimmed write reply was not preserved", got.reply.Operations, got.err)
			}
		})
	}
}
