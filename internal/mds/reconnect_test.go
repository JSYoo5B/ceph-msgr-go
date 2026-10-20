package mds

import (
	"errors"
	"reflect"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func TestReconnectRoundTripOrderingAndLayout(t *testing.T) {
	caps := []CapReconnect{
		{Ino: 0x10000000002, CapID: 9, Wanted: 0x55, Issued: 0x15, SnapRealm: 1, PathBase: 1, Path: "dir/file", SnapFollows: 3, Flock: []byte{1, 2}},
		{Ino: 1, CapID: 2, Wanted: 1, Issued: 1, SnapRealm: 1},
	}
	realms := []RealmReconnect{{Ino: 1, Seq: 4, Parent: 0}}
	m, err := EncodeReconnect(caps, realms, true)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != ClientReconnectMessage || m.Version != 5 || m.CompatVersion != 4 || len(m.Front) != 0 || len(m.Data) == 0 {
		t.Fatal("reconnect envelope", m)
	}
	// First map key is the lowest inode; its body is a v2/compat1 envelope.
	d := wire.NewDecoder(m.Data)
	if d.U32() != 2 || d.U64() != 1 || d.U8() != 2 || d.U8() != 1 {
		t.Fatal("cap map layout")
	}
	gotCaps, gotRealms, more, err := DecodeReconnect(m, 0)
	if err != nil || !more || !reflect.DeepEqual(gotRealms, realms) {
		t.Fatal(gotCaps, gotRealms, more, err)
	}
	if !reflect.DeepEqual(gotCaps, []CapReconnect{caps[1], caps[0]}) {
		t.Fatalf("caps %+v", gotCaps)
	}
	if _, err := EncodeReconnect([]CapReconnect{{Ino: 1}, {Ino: 1}}, nil, false); !errors.Is(err, ErrRequest) {
		t.Fatal("duplicate inode accepted", err)
	}
	empty, _ := EncodeReconnect(nil, nil, false)
	if c, r, more, err := DecodeReconnect(empty, 0); err != nil || c != nil || r != nil || more {
		t.Fatal("empty reconnect", c, r, more, err)
	}
	bad := m
	bad.Version = 4
	if _, _, _, err := DecodeReconnect(bad, 0); !errors.Is(err, wire.ErrVersion) {
		t.Fatal(err)
	}
	bad = m
	bad.Front = []byte{0}
	if _, _, _, err := DecodeReconnect(bad, 0); !errors.Is(err, msgr.ErrFrame) {
		t.Fatal(err)
	}
}
