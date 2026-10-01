package maps

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func monFixture(release byte) []byte {
	e := wire.Encoder{}
	e.Raw([]byte("0123456789abcdef"))
	e.U32(7)
	e.Raw(make([]byte, 16))
	features := wire.Encoder{}
	features.U64(0)
	e.Struct(1, 1, features.Data)
	e.Struct(1, 1, features.Data)
	e.U32(1)
	e.String("a")
	info := wire.Encoder{}
	info.String("a")
	msgr.EncodeAddresses(&info, []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}})
	e.Struct(6, 1, info.Data)
	e.U32(1)
	e.String("a")
	e.U8(release)
	mapBlob := wire.Encoder{}
	mapBlob.Struct(10, 6, e.Data)
	out := wire.Encoder{}
	out.Bytes(mapBlob.Data)
	return out.Data
}
func TestMonMapReleaseAndAddresses(t *testing.T) {
	m, err := DecodeMon(monFixture(20))
	if err != nil || m.Epoch != 7 || len(m.Addresses) != 1 {
		t.Fatal(m, err)
	}
	if _, err := DecodeMon(monFixture(19)); !errors.Is(err, ErrRelease) {
		t.Fatal("Squid accepted")
	}
	for n := 0; n < len(monFixture(20)); n++ {
		if _, err := DecodeMon(monFixture(20)[:n]); err == nil {
			t.Fatalf("truncation %d accepted", n)
		}
	}
}
func TestMgrUnavailableAndActive(t *testing.T) {
	for _, available := range []bool{false, true} {
		e := wire.Encoder{}
		e.U32(8)
		msgr.EncodeAddresses(&e, []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("[2001:db8::1]:6800")}})
		e.U64(99)
		if available {
			e.U8(1)
		} else {
			e.U8(0)
		}
		e.String("a")
		out := wire.Encoder{}
		out.Struct(14, 6, e.Data)
		m, err := DecodeMgr(out.Data)
		if err != nil || m.Available != available || m.GlobalID != 99 {
			t.Fatal(m, err)
		}
	}
}
func FuzzMaps(f *testing.F) {
	f.Add(monFixture(20))
	f.Fuzz(func(t *testing.T, p []byte) { DecodeMon(p); DecodeMgr(p) })
}
