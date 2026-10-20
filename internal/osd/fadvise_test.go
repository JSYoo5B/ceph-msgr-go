package osd

import (
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func TestFadviseFlagsOnlyOnDataPaths(t *testing.T) {
	data := []byte("abc")
	for _, op := range []Operation{
		{Code: Read, Length: 3, Flags: OpFadviseSequential | OpFadviseWillNeed},
		{Code: Write, Length: 3, Data: data, Flags: OpFadviseDontNeed | OpFadviseNoCache},
		{Code: WriteFull, Length: 3, Data: data, Flags: OpFadviseNoCache},
		{Code: Append, Length: 3, Data: data, Flags: OpFadviseRandom},
		{Code: CompareExtent, Length: 3, Data: data, Flags: OpFadviseWillNeed},
	} {
		if _, _, err := ValidateOperations([]Operation{op}, wire.DefaultLimit); err != nil {
			t.Fatalf("%#x with hints %#x refused: %v", op.Code, op.Flags, err)
		}
	}
	for _, op := range []Operation{
		{Code: Stat, Flags: OpFadviseNoCache},
		{Code: Create, Flags: OpFadviseDontNeed},
		{Code: Delete, Flags: OpFadviseRandom},
		{Code: Read, Length: 3, Flags: 0x80},
	} {
		if _, _, err := ValidateOperations([]Operation{op}, wire.DefaultLimit); !errors.Is(err, ErrOperation) {
			t.Fatalf("%#x with flags %#x admitted: %v", op.Code, op.Flags, err)
		}
	}
}
