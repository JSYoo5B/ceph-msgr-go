package msgr

import (
	"encoding/hex"
	"net/netip"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestIPv4AddressFixture(t *testing.T) {
	a := Address{Type: 2, Nonce: 7, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}
	e := wire.Encoder{}
	a.Encode(&e)
	const want = "0101011c00000002000000070000001000000002000ce4c00002010000000000000000"
	if hex.EncodeToString(e.Data) != want {
		t.Fatalf("address %x", e.Data)
	}
	p, _ := hex.DecodeString(want)
	d := wire.NewDecoder(p)
	if got := DecodeAddress(d); got != a || d.Done() != nil {
		t.Fatal("IPv4 fixture")
	}
}
func TestIPv6AddressFixture(t *testing.T) {
	// Linux sockaddr_in6: LE family and scope, network-order port and flowinfo.
	const fixture = "0101012800000002000000090000001c0000000a000ce41234567820010db800000000000000000000000103000000"
	p, _ := hex.DecodeString(fixture)
	d := wire.NewDecoder(p)
	a := DecodeAddress(d)
	if d.Done() != nil || a.Endpoint.String() != "[2001:db8::1]:3300" || a.ScopeID != 3 || a.FlowInfo != 0x12345678 {
		t.Fatal(a, d.Err())
	}
	e := wire.Encoder{}
	a.Encode(&e)
	if hex.EncodeToString(e.Data) != fixture {
		t.Fatal("IPv6 encoding")
	}
}

func TestMessageHeaderFixture(t *testing.T) {
	m := MessageData{Sequence: 1, Transaction: 2, Type: 50, Priority: 127, Version: 1, AckSequence: 3, Front: []byte("cmd")}
	const want = "0100000000000000020000000000000032007f00010000000000000003000000000000000100000000"
	f := m.Frame()
	if hex.EncodeToString(f.Segments[0]) != want {
		t.Fatalf("header %x", f.Segments[0])
	}
	if _, err := DecodeMessage(f); err != nil {
		t.Fatal(err)
	}
	f.Segments[0] = f.Segments[0][:40]
	if _, err := DecodeMessage(f); err == nil {
		t.Fatal("short header accepted")
	}
}
