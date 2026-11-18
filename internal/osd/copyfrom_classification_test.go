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
