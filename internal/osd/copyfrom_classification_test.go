package osd

import (
	"errors"
	"fmt"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
	"testing"
)

// The native flag byte has six defined bits; the public API only constructs
// zero or TRUNCATE_SEQ. Exercise every byte, including mixed known/unknown bits.
func TestCopyFromFlagClassification(t *testing.T) {
	for _, code := range []uint16{CopyFrom, CopyFrom2} {
		for flag := 0; flag < 256; flag++ {
			t.Run(fmt.Sprintf("%x/%02x", code, flag), func(t *testing.T) {
				op := copyFromOperation(code)
				op.CopyFrom.Flags = uint8(flag)
				if flag&32 != 0 {
					op.CopyFrom.TruncateSequence = 2
				}
				admitted := flag == 0 || code == CopyFrom2 && flag == 32
				r := Request{MapEpoch: 11, Object: "destination", Locator: Locator{Pool: 7, Hash: -1}, Operations: []Operation{op}}
				for name, encode := range inactiveCopyFromEncoders() {
					m, err := encode(r)
					if admitted {
						if err != nil || m.Type == 0 {
							t.Fatalf("%s admitted form: %v", name, err)
						}
					} else if !errors.Is(err, ErrOperation) || m.Type != 0 || len(m.Front)+len(m.Middle)+len(m.Data) != 0 {
						t.Fatalf("%s refused flag emitted message: %v", name, err)
					}
				}
				_, _, err := ValidateOperations(r.Operations, wire.DefaultLimit)
				if admitted != (err == nil) {
					t.Fatal("validator classification", err)
				}
			})
		}
	}
}

// Numeric validity is independent of whether a snapshot/version exists on an
// OSD. These are encoder boundaries, not daemon acceptance of extreme values.
func TestCopyFromSnapshotVersionClassification(t *testing.T) {
	for _, code := range []uint16{CopyFrom, CopyFrom2} {
		for _, snap := range []uint64{0, 1, MaxSnap, NoSnap, SnapDir} {
			for _, version := range []uint64{0, 1, ^uint64(0)} {
				op := copyFromOperation(code)
				op.CopyFrom.Snap = snap
				op.CopyFrom.Version = version
				r := Request{MapEpoch: 11, Object: "destination", Locator: Locator{Pool: 7, Hash: -1}, Operations: []Operation{op}}
				for name, encode := range inactiveCopyFromEncoders() {
					m, err := encode(r)
					if snap == SnapDir {
						if !errors.Is(err, ErrRequest) || m.Type != 0 || len(m.Front)+len(m.Middle)+len(m.Data) != 0 {
							t.Fatalf("%s snapdir accepted: %v", name, err)
						}
					} else if err != nil || m.Type == 0 {
						t.Fatalf("%s numeric input %d/%d refused: %v", name, snap, version, err)
					}
				}
			}
		}
	}
}

// A source equal to the destination is sent at the head, where the OSD answers
// -EINVAL, and refused at every other snap id, where the pinned OSD would wait
// on its own blocked head. Identity follows hobject_t: a key equal to the name
// is no key, and any differing pool, namespace, name or key is another object.
func TestCopyFromSelfSourceClassification(t *testing.T) {
	dst := Request{MapEpoch: 11, Object: "destination", Locator: Locator{Pool: 7, Key: "", Namespace: "ns", Hash: -1}}
	self := func(snap uint64) CopyFromInput {
		return CopyFromInput{Object: "destination", Locator: Locator{Pool: 7, Namespace: "ns", Hash: -1}, Snap: snap}
	}
	edit := func(snap uint64, f func(*CopyFromInput)) CopyFromInput { in := self(snap); f(&in); return in }
	cases := []struct {
		name    string
		in      CopyFromInput
		dstKey  string
		refused bool
	}{
		{"head-zero", self(0), "", false},
		{"head-nosnap", self(NoSnap), "", false},
		{"snap-one", self(1), "", true},
		{"snap-max", self(MaxSnap), "", true},
		{"snap-with-version", edit(5, func(in *CopyFromInput) { in.Version = ^uint64(0) }), "", true},
		{"source-key-equals-name", edit(5, func(in *CopyFromInput) { in.Locator.Key = "destination" }), "", true},
		{"destination-key-equals-name", self(5), "destination", true},
		{"same-key", edit(5, func(in *CopyFromInput) { in.Locator.Key = "k" }), "k", true},
		{"other-key", edit(5, func(in *CopyFromInput) { in.Locator.Key = "k" }), "", false},
		{"other-pool", edit(5, func(in *CopyFromInput) { in.Locator.Pool = 8 }), "", false},
		{"other-namespace", edit(5, func(in *CopyFromInput) { in.Locator.Namespace = "" }), "", false},
		{"other-name", edit(5, func(in *CopyFromInput) { in.Object = "destination2" }), "", false},
	}
	for _, code := range []uint16{CopyFrom, CopyFrom2} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%x/%s", code, tc.name), func(t *testing.T) {
				r := dst
				r.Locator.Key = tc.dstKey
				op := copyFromOperation(code)
				op.CopyFrom = tc.in
				r.Operations = []Operation{op, {Code: SetXattr, Xattr: XattrInput{Name: "a"}, Data: []byte("b")}}
				for name, encode := range inactiveCopyFromEncoders() {
					m, err := encode(r)
					if tc.refused {
						if !errors.Is(err, ErrRequest) || m.Type != 0 || len(m.Front)+len(m.Middle)+len(m.Data) != 0 {
							t.Fatalf("%s self source at a snap emitted a message: %v", name, err)
						}
					} else if err != nil || m.Type == 0 {
						t.Fatalf("%s admitted form refused: %v", name, err)
					}
				}
				if err := ValidateCopyFromRequest(r); tc.refused != (err != nil) {
					t.Fatal("request validator classification", err)
				}
			})
		}
	}
	// SET_REDIRECT has its own server-side self check and is not affected.
	redirect := dst
	redirect.Operations = []Operation{{Code: SetRedirect, CopyFrom: self(5)}}
	if err := ValidateCopyFromRequest(redirect); err != nil {
		t.Fatal("redirect classified as a copy", err)
	}
}
