package cephmsgr

import "github.com/jsyoo5b/ceph-msgr-go/internal/osd"

const (
	OSDGetXattr     OSDOpcode = OSDOpcode(osd.GetXattr)
	OSDGetXattrs    OSDOpcode = OSDOpcode(osd.GetXattrs)
	OSDCompareXattr OSDOpcode = OSDOpcode(osd.CompareXattr)
	OSDSetXattr     OSDOpcode = OSDOpcode(osd.SetXattr)
	OSDRemoveXattr  OSDOpcode = OSDOpcode(osd.RemoveXattr)
)

// OSDXattrComparison compares the caller's operand on the left against the
// stored value on the right. STRING mode compares arbitrary bytes, not text.
// U64 compares a raw little-endian uint64 operand against the server's decimal
// interpretation of the stored value. The transport does not parse stored bytes.
type OSDXattrComparison uint8

const (
	OSDXattrEqual        OSDXattrComparison = 1
	OSDXattrNotEqual     OSDXattrComparison = 2
	OSDXattrGreater      OSDXattrComparison = 3
	OSDXattrGreaterEqual OSDXattrComparison = 4
	OSDXattrLess         OSDXattrComparison = 5
	OSDXattrLessEqual    OSDXattrComparison = 6
)

// OSDXattrMode selects the native CMPXATTR operand format. CompareXattr
// requires an explicit mode; every other opcode requires zero.
type OSDXattrMode uint8

const (
	OSDXattrString OSDXattrMode = OSDXattrMode(osd.XattrCompareString)
	OSDXattrU64    OSDXattrMode = OSDXattrMode(osd.XattrCompareU64)
)

// OSDXattrInput is the active union only for the five xattr opcodes. Name is
// nonempty and NUL-free except for GetXattrs, which accepts no name. Data on
// OSDOperation holds the binary SET value or CMP operand; nil encodes an empty
// value and never removal. Comparison and Mode are required only by
// CompareXattr. U64 mode requires exactly eight little-endian operand bytes.
// GetXattr/GetXattrs require a positive local MaxOutputBytes budget. It is not
// transmitted and does not request server truncation or pagination.
type OSDXattrInput struct {
	Name           string
	Comparison     OSDXattrComparison
	Mode           OSDXattrMode
	MaxOutputBytes uint32
}

// OSDXattrResult preserves the packed reply union. NameLength is an echoed
// length, not a returned name. GetXattr normalizes ValueLength to its output
// length. Operation Data retains raw values or the encoded GetXattrs map;
// this layer does not normalize names or interpret that map as storage policy.
type OSDXattrResult struct {
	NameLength, ValueLength      uint32
	ComparisonOp, ComparisonMode uint8
}

func xattrWireInput(input OSDXattrInput) osd.XattrInput {
	result := osd.XattrInput{Name: input.Name, ComparisonOp: uint8(input.Comparison), ComparisonMode: uint8(input.Mode), MaxOutputBytes: input.MaxOutputBytes}
	return result
}
