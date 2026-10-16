package osd

import (
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Source-derived admission regressions; no unsafe vector is sent to Ceph.
// A remote newer truncate sequence can shorten a requested 12-byte WRITE to
// six bytes, making a six-byte READ/CMPEXT a full-object read of staged state.
func TestClippedWriteRejectsShortPrefixReads(t *testing.T) {
	encoders := map[string]func([]Operation) error{
		"v6": func(ops []Operation) error {
			r := oracleRequest()
			r.Operations = ops
			_, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
			return err
		},
		"v8": func(ops []Operation) error {
			r := nativeV8Request("read")
			r.Operations = ops
			_, err := EncodeRequestV8(r, v8OracleFeatures, wire.DefaultLimit)
			return err
		},
	}
	for name, encode := range encoders {
		t.Run(name, func(t *testing.T) {
			for _, code := range []uint16{Read, CompareExtent} {
				for _, length := range []uint64{1, 6, 11} {
					read := Operation{Code: code, Length: length}
					if code == CompareExtent {
						read.Data = make([]byte, length)
					}
					for _, flags := range []uint32{0, OpFailOK} {
						for _, seq := range []uint32{0, 1} {
							write := Operation{Code: Write, Length: 12, Data: make([]byte, 12), Flags: flags}
							write.TruncateSequence = seq
							if seq != 0 {
								write.TruncateSize = 12
							}
							vectors := [][]Operation{
								{write, read}, // Existing remote sequence is unknown.
								{{Code: Truncate, Offset: 6, TruncateSize: 6, TruncateSequence: 2}, write, read},
								{write, {Code: WriteFull, Flags: OpFailOK, Length: 16, Data: make([]byte, 16)}, read},
								{write, {Code: Append, Flags: OpFailOK, Length: 16, Data: make([]byte, 16)}, read},
							}
							for _, ops := range vectors {
								if err := encode(ops); !errors.Is(err, ErrOperation) {
									t.Fatalf("possible clipped full-object read admitted: code=%#x length=%d flags=%#x seq=%d: %v", code, length, flags, seq, err)
								}
							}
							safe := read
							safe.Offset = 1
							if err := encode([]Operation{write, safe}); err != nil {
								t.Fatal("non-prefix read rejected", err)
							}
							if err := encode([]Operation{read, write}); err != nil {
								t.Fatal("read before write rejected", err)
							}
						}
					}
				}
			}
			for _, code := range []uint16{WriteFull, Append} {
				ops := []Operation{{Code: code, Length: 12, Data: make([]byte, 12)}, {Code: Read, Length: 6}}
				if err := encode(ops); err != nil {
					t.Fatal("unclipped write's shorter prefix read rejected", code, err)
				}
			}
		})
	}
}
