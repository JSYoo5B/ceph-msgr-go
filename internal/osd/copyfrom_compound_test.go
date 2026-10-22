package osd

import (
	"errors"
	"fmt"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func TestCopyFromCompoundAdmission(t *testing.T) {
	setx := Operation{Code: SetXattr, Xattr: XattrInput{Name: "k"}, Data: []byte("v")}
	rmx := Operation{Code: RemoveXattr, Xattr: XattrInput{Name: "old"}}
	type shape struct {
		name  string
		ops   []Operation
		allow bool
	}
	var shapes []shape
	for _, code := range []uint16{CopyFrom, CopyFrom2} {
		copyOp := Operation{Code: code, CopyFrom: CopyFromInput{Object: "src", Locator: Locator{Pool: 1, Hash: -1}}}
		for _, test := range []shape{
			{"copy then xattrs", []Operation{copyOp, setx, rmx}, true},
			{"xattr prefix", []Operation{setx, copyOp}, false},
			{"read suffix", []Operation{copyOp, {Code: Read, Length: 1}}, false},
			{"write suffix", []Operation{copyOp, {Code: WriteFull, Length: 1, Data: []byte{1}}}, false},
			{"stat suffix", []Operation{copyOp, {Code: Stat}}, false},
			{"second copy", []Operation{copyOp, copyOp}, false},
		} {
			test.name = fmt.Sprintf("%#x/%s", code, test.name)
			shapes = append(shapes, test)
		}
	}
	shapes = append(shapes, shape{"redirect with xattr", []Operation{{Code: SetRedirect, CopyFrom: CopyFromInput{Object: "src", Locator: Locator{Pool: 1, Hash: -1}}}, setx}, false})
	// These adjacent opcodes retain standalone guards. Copy/xattr support must
	// not accidentally admit them to a vector through a shared encoder path.
	for name, op := range map[string]Operation{
		"mapext":      {Code: MapExt, Length: 1, MapExt: MapExtInput{MaxOutputBytes: 32}},
		"sparse read": {Code: SparseRead, Length: 1, SparseRead: SparseReadInput{MaxOutputBytes: 32}},
		"isdirty":     {Code: Isdirty}, "undirty": {Code: Undirty},
	} {
		shapes = append(shapes, shape{name + " then stat", []Operation{op, {Code: Stat}}, false})
		r := Request{MapEpoch: 11, Object: "destination", Locator: Locator{Pool: 7, Hash: -1}, Operations: []Operation{op}}
		for version, encode := range inactiveCopyFromEncoders() {
			if _, err := encode(r); err != nil {
				t.Fatalf("%s standalone %s control: %v", name, version, err)
			}
		}
	}
	for _, test := range shapes {
		t.Run(test.name, func(t *testing.T) {
			_, _, validation := ValidateOperations(test.ops, wire.DefaultLimit)
			for version, encode := range inactiveCopyFromEncoders() {
				t.Run(version, func(t *testing.T) {
					r := Request{MapEpoch: 11, Object: "destination", Locator: Locator{Pool: 7, Hash: -1}, Operations: test.ops}
					m, err := encode(r)
					if test.allow {
						if validation != nil || err != nil || m.Type != RequestMessage || len(m.Front) == 0 || len(m.Data) == 0 {
							t.Fatalf("admitted copy/xattr form failed: validation=%v encode=%v", validation, err)
						}
					} else if !errors.Is(validation, ErrOperation) || !errors.Is(err, ErrOperation) || m.Type != 0 || len(m.Front)+len(m.Middle)+len(m.Data) != 0 {
						t.Fatalf("refused vector produced a message: validation=%v encode=%v", validation, err)
					}
				})
			}
		})
	}
}
