package msgr

import (
	"encoding/hex"
	"testing"
)

func TestGetMonMapIndependentHeader(t *testing.T) {
	// Pinned ceph_fs.h assigns ID 5. MMonGetMap inherits Message's default
	// header version 1/compatibility 0 and encodes no payload. This literal
	// Messenger header also verifies that the request is not a subscription
	// or a command carrying an application transaction.
	want, err := hex.DecodeString("0000000000000000000000000000000005007f00010000000000000000000000000000000100000000")
	if err != nil {
		t.Fatal(err)
	}
	m := GetMonMap()
	f := m.Frame()
	if len(f.Segments) != 4 || hex.EncodeToString(f.Segments[0]) != hex.EncodeToString(want) {
		t.Fatal("MON getmap wire header changed", hex.EncodeToString(f.Segments[0]))
	}
	for _, segment := range f.Segments[1:] {
		if len(segment) != 0 {
			t.Fatal("MON getmap unexpectedly encoded a payload", segment)
		}
	}
	if m.Type != 5 || m.Version != 1 || m.CompatVersion != 0 || m.Transaction != 0 {
		t.Fatal("MON getmap request identity changed", m)
	}
}
