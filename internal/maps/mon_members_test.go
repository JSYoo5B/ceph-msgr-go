package maps

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// These helpers write bytes independently of the product wire/address encoders.
// Layout: fixed Tentacle MonMap.cc encode() and mon_info_t::encode(), and
// entity_addr_t/entity_addrvec_t in src/msg/msg_types.h, all at
// 7f793731f1b39eb4f465e960113d2363c311b964. MonMap.h declares LGPL-2.1;
// no upstream code or fixtures are copied into this hand-built test data.
type memberFixtureRecord struct {
	name, infoName string
	addresses      [][]byte
}

func memberFixtureU32(p []byte, v uint32) []byte {
	return binary.LittleEndian.AppendUint32(p, v)
}
func memberFixtureBytes(p, data []byte) []byte {
	return append(memberFixtureU32(p, uint32(len(data))), data...)
}
func memberFixtureEnvelope(version, compat byte, p []byte) []byte {
	return memberFixtureBytes([]byte{version, compat}, p)
}
func memberFixtureAddress(kind, nonce uint32, endpoint string, flow, scope uint32) []byte {
	p := memberFixtureU32(nil, kind)
	p = memberFixtureU32(p, nonce)
	var sock []byte
	if endpoint != "" {
		a := netip.MustParseAddrPort(endpoint)
		if a.Addr().Is4() {
			sock = make([]byte, 16)
			binary.LittleEndian.PutUint16(sock, 2) // Linux AF_INET
			ip := a.Addr().As4()
			copy(sock[4:], ip[:])
		} else {
			sock = make([]byte, 28)
			binary.LittleEndian.PutUint16(sock, 10) // Linux AF_INET6, including mapped
			binary.BigEndian.PutUint32(sock[4:], flow)
			ip := a.Addr().As16()
			copy(sock[8:], ip[:])
			binary.LittleEndian.PutUint32(sock[24:], scope)
		}
		binary.BigEndian.PutUint16(sock[2:], a.Port())
	}
	p = memberFixtureBytes(p, sock)
	return append([]byte{1}, memberFixtureEnvelope(1, 1, p)...)
}
func memberFixture(records []memberFixtureRecord, ranks []string) []byte {
	p := append([]byte(nil), []byte("0123456789abcdef")...)
	p = memberFixtureU32(p, 73)
	p = append(p, make([]byte, 16)...)
	for i := 0; i < 2; i++ {
		p = append(p, memberFixtureEnvelope(1, 1, make([]byte, 8))...)
	}
	p = memberFixtureU32(p, uint32(len(records)))
	for _, r := range records {
		p = memberFixtureBytes(p, []byte(r.name))
		info := memberFixtureBytes(nil, []byte(r.infoName))
		info = memberFixtureU32(append(info, 2), uint32(len(r.addresses)))
		for _, a := range r.addresses {
			info = append(info, a...)
		}
		info = append(info, make([]byte, 4+4+8)...) // priority/weight, crush_loc, time_added
		p = append(p, memberFixtureEnvelope(6, 1, info)...)
	}
	p = memberFixtureU32(p, uint32(len(ranks)))
	for _, name := range ranks {
		p = memberFixtureBytes(p, []byte(name))
	}
	p = append(p, 20) // minimum monitor release
	p = memberFixtureU32(p, 1)
	p = memberFixtureU32(p, 9) // prior removed rank does not leave a current rank hole
	p = append(p, 1)
	p = memberFixtureU32(p, 0) // disallowed leaders
	p = append(p, 0)
	p = memberFixtureBytes(p, nil) // tiebreaker
	p = memberFixtureU32(p, 0)     // marked-down monitors
	p = memberFixtureU32(p, 17)    // auth epoch
	p = memberFixtureU32(p, 2)     // service cipher
	p = memberFixtureU32(p, 1)
	p = memberFixtureU32(p, 2) // allowed cipher
	p = memberFixtureU32(p, 2) // preferred cipher
	return memberFixtureBytes(nil, memberFixtureEnvelope(10, 6, p))
}

func TestMonMembersPreserveRankAndOriginalAddresses(t *testing.T) {
	v1 := msgr.Address{Type: 1, Nonce: 0x10203040, Endpoint: netip.MustParseAddrPort("192.0.2.5:6789")}
	mapped := msgr.Address{Type: 2, Nonce: 91, Endpoint: netip.MustParseAddrPort("[::ffff:192.0.2.5]:3300"), FlowInfo: 0x01020304, ScopeID: 7}
	v6 := msgr.Address{Type: 2, Nonce: 92, Endpoint: netip.MustParseAddrPort("[fe80::1234]:3301"), FlowInfo: 0x05060708, ScopeID: 11}
	other := msgr.Address{Type: 3, Nonce: 93, Endpoint: netip.MustParseAddrPort("[2001:db8::2]:6789"), FlowInfo: 9, ScopeID: 12}
	blank := msgr.Address{Type: 2, Nonce: 94}
	records := []memberFixtureRecord{
		{"A", "A", [][]byte{memberFixtureAddress(1, v1.Nonce, v1.Endpoint.String(), 0, 0)}},
		{"a", "a", [][]byte{memberFixtureAddress(2, v6.Nonce, v6.Endpoint.String(), v6.FlowInfo, v6.ScopeID)}},
		{"empty", "empty", nil},
		{"z", "z", [][]byte{
			memberFixtureAddress(1, v1.Nonce, v1.Endpoint.String(), 0, 0),
			memberFixtureAddress(2, mapped.Nonce, mapped.Endpoint.String(), mapped.FlowInfo, mapped.ScopeID),
			memberFixtureAddress(3, other.Nonce, other.Endpoint.String(), other.FlowInfo, other.ScopeID),
			memberFixtureAddress(2, blank.Nonce, "", 0, 0),
		}},
	}
	m, err := DecodeMon(memberFixture(records, []string{"z", "empty", "a", "A"}))
	want := []MonMember{
		{Name: "z", Rank: 0, Addresses: []msgr.Address{v1, mapped, other, blank}},
		{Name: "empty", Rank: 1, Addresses: []msgr.Address{}},
		{Name: "a", Rank: 2, Addresses: []msgr.Address{v6}},
		{Name: "A", Rank: 3, Addresses: []msgr.Address{v1}},
	}
	if err != nil || !reflect.DeepEqual(m.Members, want) || m.Epoch != 73 || m.AuthEpoch != 17 {
		t.Fatalf("members=%+v, err=%v; want=%+v", m, err, want)
	}
	if !reflect.DeepEqual(m.Addresses, []msgr.Address{mapped, v6}) || !m.Members[0].Addresses[1].Endpoint.Addr().Is4In6() {
		t.Fatalf("bootstrap filtering or mapped family changed: %+v", m)
	}
}

func TestMonMembersFromActualTentacleFixtures(t *testing.T) {
	for _, tc := range []struct {
		file, ip string
		names    []string
	}{
		{"monmap-v20.2.4.bin", "127.0.0.1", []string{"a"}},
		{"monmap-3-ipv6-v20.2.4.bin", "::1", []string{"a", "b", "c"}},
	} {
		t.Run(tc.file, func(t *testing.T) {
			blob, err := os.ReadFile("testdata/" + tc.file)
			if err != nil {
				t.Fatal(err)
			}
			// Expected member names/ports come from the native cluster metadata
			// documented in testdata/README.md, not from this Go decoder.
			m, err := DecodeMon(memberFixtureBytes(nil, blob))
			if err != nil || len(m.Members) != len(tc.names) {
				t.Fatalf("actual map: %+v, %v", m, err)
			}
			for i, name := range tc.names {
				want := MonMember{Name: name, Rank: uint32(i), Addresses: []msgr.Address{{Type: 2, Endpoint: netip.AddrPortFrom(netip.MustParseAddr(tc.ip), uint16(33300+i))}}}
				if !reflect.DeepEqual(m.Members[i], want) {
					t.Fatalf("member %d=%+v; want=%+v", i, m.Members[i], want)
				}
			}
		})
	}
}

func TestMonMembersRejectInvalidTopologyWithoutPartialMap(t *testing.T) {
	a := memberFixtureAddress(2, 0, "192.0.2.1:3300", 0, 0)
	good := []memberFixtureRecord{{"a", "a", [][]byte{a}}, {"b", "b", [][]byte{a}}}
	for _, tc := range []struct {
		name    string
		records []memberFixtureRecord
		ranks   []string
	}{
		{"inconsistent name", []memberFixtureRecord{{"a", "b", [][]byte{a}}}, []string{"a"}},
		{"duplicate name", []memberFixtureRecord{{"a", "a", [][]byte{a}}, {"a", "a", [][]byte{a}}}, []string{"a", "a"}},
		{"duplicate rank", good, []string{"a", "a"}},
		{"unknown rank", good, []string{"a", "c"}},
		{"missing rank", good, []string{"a"}},
		{"extra rank", good, []string{"a", "b", "c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := DecodeMon(memberFixture(tc.records, tc.ranks))
			if err == nil || !reflect.DeepEqual(m, Mon{}) {
				t.Fatalf("invalid topology returned partial map: %+v, %v", m, err)
			}
		})
	}
	front := memberFixture(good, []string{"b", "a"})
	for n := 0; n < len(front); n++ {
		m, err := DecodeMon(front[:n])
		if err == nil || !reflect.DeepEqual(m, Mon{}) {
			t.Fatalf("truncation %d returned partial map: %+v, %v", n, m, err)
		}
	}
}

func TestMonMembersCollectionLimits(t *testing.T) {
	a := memberFixtureAddress(2, 0, "192.0.2.1:3300", 0, 0)
	for _, n := range []int{1024, 1025} {
		records := make([]memberFixtureRecord, n)
		ranks := make([]string, n)
		for i := range records {
			name := fmt.Sprintf("m%04d", i)
			records[i] = memberFixtureRecord{name, name, [][]byte{a}}
			ranks[i] = name
		}
		m, err := DecodeMon(memberFixture(records, ranks))
		if n == 1024 {
			if err != nil || len(m.Members) != n || m.Members[n-1].Rank != uint32(n-1) {
				t.Fatalf("bounded members: count=%d, err=%v", len(m.Members), err)
			}
		} else if !errors.Is(err, wire.ErrLimit) || !reflect.DeepEqual(m, Mon{}) {
			t.Fatalf("excess members: %+v, %v", m, err)
		}
	}
	for _, n := range []int{64, 65} {
		addresses := make([][]byte, n)
		for i := range addresses {
			addresses[i] = a
		}
		m, err := DecodeMon(memberFixture([]memberFixtureRecord{{"a", "a", addresses}}, []string{"a"}))
		if n == 64 {
			if err != nil || len(m.Members) != 1 || len(m.Members[0].Addresses) != n {
				t.Fatalf("bounded addresses: %+v, %v", m, err)
			}
		} else if !errors.Is(err, wire.ErrLimit) || !reflect.DeepEqual(m, Mon{}) {
			t.Fatalf("excess addresses: %+v, %v", m, err)
		}
	}
}
