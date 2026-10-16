package osd

import (
	"errors"
	"math"
	"testing"
)

func TestTruncateFieldAdmission(t *testing.T) {
	for _, tc := range []struct {
		op Operation
		ok bool
	}{
		{Operation{Code: Read, Length: 4, TruncateSize: 2, TruncateSequence: 3}, true},
		{Operation{Code: Write, Offset: 1, Length: 1, Data: []byte{1}, TruncateSize: 9, TruncateSequence: 1}, true},
		{Operation{Code: Truncate, Offset: 9, TruncateSize: 9, TruncateSequence: 2}, true},
		{Operation{Code: Truncate, Offset: 8, TruncateSize: 9, TruncateSequence: 2}, false},
		{Operation{Code: Read, Length: 4, TruncateSize: 2}, false},
		{Operation{Code: Write, Length: 1, Data: []byte{1}, TruncateSize: math.MaxUint64, TruncateSequence: 1}, false},
		{Operation{Code: Zero, Length: 1, TruncateSize: 1, TruncateSequence: 1}, false},
		{Operation{Code: WriteFull, Length: 1, Data: []byte{1}, TruncateSize: 1, TruncateSequence: 1}, false},
		{Operation{Code: Stat, TruncateSequence: 1}, false},
	} {
		_, _, err := ValidateOperations([]Operation{tc.op}, 1024)
		if tc.ok != (err == nil) || !tc.ok && !errors.Is(err, ErrRequest) {
			t.Fatalf("op %#x size %d seq %d: %v", tc.op.Code, tc.op.TruncateSize, tc.op.TruncateSequence, err)
		}
	}
}
