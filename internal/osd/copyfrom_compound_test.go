package osd

import (
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func TestCopyFromCompoundAdmission(t *testing.T) {
	copyOp := Operation{Code: CopyFrom2, CopyFrom: CopyFromInput{Object: "src", Locator: Locator{Pool: 1, Hash: -1}}}
	setx := Operation{Code: SetXattr, Xattr: XattrInput{Name: "k"}, Data: []byte("v")}
	rmx := Operation{Code: RemoveXattr, Xattr: XattrInput{Name: "old"}}
	if _, _, err := ValidateOperations([]Operation{copyOp, setx, rmx}, wire.DefaultLimit); err != nil {
		t.Fatal("copy followed by xattr writes refused", err)
	}
	for name, ops := range map[string][]Operation{
		"xattr before the copy": {setx, copyOp},
		"copy then data write":  {copyOp, {Code: WriteFull, Length: 1, Data: []byte{1}}},
		"two copies":            {copyOp, copyOp},
		"redirect with xattr":   {{Code: SetRedirect, CopyFrom: copyOp.CopyFrom}, setx},
	} {
		if _, _, err := ValidateOperations(ops, wire.DefaultLimit); !errors.Is(err, ErrOperation) {
			t.Fatal(name, "admitted", err)
		}
	}
}
