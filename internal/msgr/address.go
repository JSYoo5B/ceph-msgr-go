package msgr

import (
	"encoding/binary"
	"errors"
	"net/netip"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Address uses the protocol's Linux address-family numbers and byte layout,
// never the host OS's sockaddr representation.
type Address struct {
	Type, Nonce       uint32
	Endpoint          netip.AddrPort
	FlowInfo, ScopeID uint32
}

func (a Address) Encode(e *wire.Encoder) {
	p := wire.Encoder{}
	p.U32(a.Type)
	p.U32(a.Nonce)
	if !a.Endpoint.IsValid() {
		p.U32(0)
	} else {
		ip := a.Endpoint.Addr().Unmap()
		var sock []byte
		if ip.Is4() {
			sock = make([]byte, 16)
			binary.LittleEndian.PutUint16(sock, 2)
			b := ip.As4()
			copy(sock[4:], b[:])
		} else {
			sock = make([]byte, 28)
			binary.LittleEndian.PutUint16(sock, 10)
			binary.BigEndian.PutUint32(sock[4:], a.FlowInfo)
			b := ip.As16()
			copy(sock[8:], b[:])
			binary.LittleEndian.PutUint32(sock[24:], a.ScopeID)
		}
		binary.BigEndian.PutUint16(sock[2:], a.Endpoint.Port())
		p.Bytes(sock)
	}
	e.U8(1)
	e.Struct(1, 1, p.Data)
}
func DecodeAddress(d *wire.Decoder) Address {
	if d.U8() != 1 {
		d.Fail(wire.ErrVersion)
		return Address{}
	}
	return decodeAddressBody(d)
}
func decodeAddressBody(d *wire.Decoder) Address {
	_, p := d.Struct(1)
	a := Address{Type: p.U32(), Nonce: p.U32()}
	sock := p.Bytes()
	if p.Err() != nil {
		d.Fail(p.Err())
		return Address{}
	}
	if len(sock) == 0 {
		return a
	}
	if len(sock) < 4 {
		d.Fail(ErrFrame)
		return Address{}
	}
	family := binary.LittleEndian.Uint16(sock)
	port := binary.BigEndian.Uint16(sock[2:])
	var ip netip.Addr
	switch family {
	case 2:
		if len(sock) != 16 {
			d.Fail(ErrFrame)
			return Address{}
		}
		ip = netip.AddrFrom4([4]byte(sock[4:8]))
	case 10:
		if len(sock) != 28 {
			d.Fail(ErrFrame)
			return Address{}
		}
		ip = netip.AddrFrom16([16]byte(sock[8:24]))
		a.FlowInfo = binary.BigEndian.Uint32(sock[4:])
		a.ScopeID = binary.LittleEndian.Uint32(sock[24:])
	default:
		d.Fail(errors.New("ceph messenger: unsupported address family"))
		return Address{}
	}
	a.Endpoint = netip.AddrPortFrom(ip, port)
	return a
}
func EncodeAddresses(e *wire.Encoder, addrs []Address) {
	e.U8(2)
	e.U32(uint32(len(addrs)))
	for _, a := range addrs {
		a.Encode(e)
	}
}
func DecodeAddresses(d *wire.Decoder) []Address {
	marker := d.U8()
	if marker == 1 {
		a := decodeAddressBody(d)
		return []Address{a}
	}
	if marker != 2 {
		d.Fail(wire.ErrVersion)
		return nil
	}
	n := d.Count(7, 64)
	out := make([]Address, 0, n)
	for i := 0; i < n && d.Err() == nil; i++ {
		out = append(out, DecodeAddress(d))
	}
	return out
}
