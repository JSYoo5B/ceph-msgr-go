package osd

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func FuzzXattrReplyDecode(f *testing.F) {
	// Independently described scalar fields seed each current xattr union.
	// These are malformed-input/ownership tests, not native encoder proof.
	for _, seed := range []struct {
		code   uint16
		union  string
		result int32
		data   []byte
	}{
		{GetXattr, "03000000 04000000 0000 000000000000000000000000000000000000", 0, []byte{0, 255, 65, 10}},
		{GetXattrs, "00000000 00000000 0000 000000000000000000000000000000000000", 0, []byte{0, 0, 0, 0}},
		{CompareXattr, "03000000 00000000 0101 000000000000000000000000000000000000", 1, nil},
		{CompareXattr, "06000000 08000000 0302 000000000000000000000000000000000000", 1, nil},
		{SetXattr, "03000000 04000000 0000 000000000000000000000000000000000000", 0, nil},
		{RemoveXattr, "03000000 00000000 0000 000000000000000000000000000000000000", 0, nil},
	} {
		m := xattrTestReply(f, seed.code, xattrLiteral(f, seed.union), seed.result, seed.data)
		f.Add(bytes.Clone(m.Front), bytes.Clone(m.Data))
		m.Front[78] = 1 // first reserved byte must not pass as an extent.
		f.Add(m.Front, m.Data)
	}
	// Native fresh-encoder replies additionally cover mixed outputs and signed
	// refusals. They were independently decoded by native Ceph before storage.
	for _, name := range []string{"read-mixed", "cmp-false", "set-refused", "rm-refused"} {
		read := func(segment string) []byte {
			encoded, err := os.ReadFile("testdata/xattr-native-reply-v6-" + name + "." + segment + ".hex")
			if err != nil {
				f.Fatal(err)
			}
			return xattrLiteral(f, strings.TrimSpace(string(encoded)))
		}
		f.Add(read("front"), read("data"))
	}
	f.Fuzz(func(t *testing.T, front, data []byte) {
		m := msgr.MessageData{Type: ReplyMessage, Version: 6, CompatVersion: 2, Front: front, Data: data}
		r, err := DecodeReply(m, 4096)
		if err != nil && !errors.Is(err, ErrRedirect) {
			return
		}
		if len(r.Operations) < 1 || len(r.Operations) > MaxOperations || !bytes.Equal(r.RawFront, front) || !bytes.Equal(r.RawData, data) {
			t.Fatal("accepted xattr reply lost bounded raw envelope")
		}
		var combined []byte
		for _, op := range r.Operations {
			if !SupportedOperation(op.Code) || uint64(len(op.Data)) != uint64(op.PayloadLength) {
				t.Fatal("accepted reply lost opcode/output boundary")
			}
			if IsXattrOperation(op.Code) {
				if op.Offset != 0 || op.Length != 0 || op.TruncateSize != 0 || op.TruncateSequence != 0 {
					t.Fatal("xattr metadata acquired fabricated extents")
				}
				if op.Code == CompareXattr {
					if (op.Xattr.ComparisonMode != 1 && op.Xattr.ComparisonMode != 2) || op.Xattr.ComparisonOp < 1 || op.Xattr.ComparisonOp > 6 {
						t.Fatal("unsupported xattr comparison accepted")
					}
					if op.Xattr.ComparisonMode == 2 && op.Xattr.ValueLength != 8 {
						t.Fatal("numeric comparison accepted wrong operand width")
					}
				} else if op.Xattr.ComparisonMode != 0 || op.Xattr.ComparisonOp != 0 {
					t.Fatal("noncomparison xattr acquired comparison semantics")
				}
			} else if op.Xattr != (XattrResult{}) {
				t.Fatal("ordinary operation acquired xattr metadata")
			}
			combined = append(combined, op.Data...)
		}
		if !bytes.Equal(combined, data) {
			t.Fatal("accepted xattr vector left unaccounted output bytes")
		}
	})
}
