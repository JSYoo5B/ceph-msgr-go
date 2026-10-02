package cephmsgr

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

type namedTestMember struct {
	name      string
	addresses []msgr.Address
}

// These bytes are laid out independently of Address.Encode and maps.DecodeMon:
// entity_addr_t marker/envelope, Linux sockaddr family, then a MonMap v10 and
// mon_info_t v6. Authentication and framing reuse the existing synthetic peer.
// Wire sources: src/msg/msg_types.h and src/mon/MonMap.{h,cc}, pinned at
// 7f793731f1b39eb4f465e960113d2363c311b964. No upstream code is copied.
func namedTestAddress(a msgr.Address) []byte {
	body := binary.LittleEndian.AppendUint32(nil, a.Type)
	body = binary.LittleEndian.AppendUint32(body, a.Nonce)
	var sock []byte
	if a.Endpoint.IsValid() {
		ip := a.Endpoint.Addr()
		if ip.Is4() {
			sock = make([]byte, 16)
			binary.LittleEndian.PutUint16(sock, 2)
			v := ip.As4()
			copy(sock[4:], v[:])
		} else {
			sock = make([]byte, 28)
			binary.LittleEndian.PutUint16(sock, 10)
			binary.BigEndian.PutUint32(sock[4:], a.FlowInfo)
			v := ip.As16()
			copy(sock[8:], v[:])
			binary.LittleEndian.PutUint32(sock[24:], a.ScopeID)
		}
		binary.BigEndian.PutUint16(sock[2:], a.Endpoint.Port())
	}
	body = binary.LittleEndian.AppendUint32(body, uint32(len(sock)))
	body = append(body, sock...)
	return append([]byte{1}, namedTestEnvelope(1, 1, body)...)
}

func namedTestEnvelope(version, compat byte, body []byte) []byte {
	p := binary.LittleEndian.AppendUint32([]byte{version, compat}, uint32(len(body)))
	return append(p, body...)
}

func namedTestString(p []byte, text string) []byte {
	p = binary.LittleEndian.AppendUint32(p, uint32(len(text)))
	return append(p, text...)
}

func namedTestAddresses(addresses []msgr.Address) []byte {
	p := binary.LittleEndian.AppendUint32([]byte{2}, uint32(len(addresses)))
	for _, a := range addresses {
		p = append(p, namedTestAddress(a)...)
	}
	return p
}

func namedTestMap(fsid [16]byte, release byte, members []namedTestMember) []byte {
	p := append([]byte(nil), fsid[:]...)
	p = binary.LittleEndian.AppendUint32(p, 1)
	p = append(p, make([]byte, 16)...)
	for range 2 {
		p = append(p, namedTestEnvelope(1, 1, make([]byte, 8))...)
	}
	p = binary.LittleEndian.AppendUint32(p, uint32(len(members)))
	for _, member := range members {
		info := namedTestString(nil, member.name)
		info = append(info, namedTestAddresses(member.addresses)...)
		info = append(info, make([]byte, 16)...) // priority/weight, crush location, time_added
		p = namedTestString(p, member.name)
		p = append(p, namedTestEnvelope(6, 1, info)...)
	}
	p = binary.LittleEndian.AppendUint32(p, uint32(len(members)))
	for _, member := range members {
		p = namedTestString(p, member.name)
	}
	p = append(p, release)
	p = binary.LittleEndian.AppendUint32(p, 0) // removed ranks
	p = append(p, 0)                           // election strategy
	p = binary.LittleEndian.AppendUint32(p, 0) // disallowed leaders
	p = append(p, 0)                           // stretch mode
	p = namedTestString(p, "")
	p = binary.LittleEndian.AppendUint32(p, 0) // marked-down MONs
	p = binary.LittleEndian.AppendUint32(p, 0) // auth epoch
	p = binary.LittleEndian.AppendUint32(p, 2) // service cipher
	p = binary.LittleEndian.AppendUint32(p, 1)
	p = binary.LittleEndian.AppendUint32(p, 2) // allowed aes256k
	p = binary.LittleEndian.AppendUint32(p, 2) // preferred cipher
	p = namedTestEnvelope(10, 6, p)
	return append(binary.LittleEndian.AppendUint32(nil, uint32(len(p))), p...)
}

type namedTestTraceConn struct {
	net.Conn
	trace  bytes.Buffer
	active bool
}

func (c *namedTestTraceConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if c.active {
		c.trace.Write(p[:n])
	}
	return n, err
}

func namedTestInitialID(p []byte) (uint64, error) {
	raw := bytes.NewReader(p)
	if _, err := msgr.ReadBanner(raw, msgr.Banner{Supported: 3, Required: 1}); err != nil {
		return 0, err
	}
	r := msgr.NewReader(raw, 0)
	if frame, err := r.Read(); err != nil || frame.Tag != msgr.Hello {
		return 0, errors.New("initial HELLO missing")
	}
	frame, err := r.Read()
	if err != nil || frame.Tag != msgr.AuthRequest || len(frame.Segments) != 1 {
		return 0, errors.New("initial AUTH_REQUEST missing")
	}
	front := frame.Segments[0]
	if len(front) < 16 || binary.LittleEndian.Uint32(front) != 2 || binary.LittleEndian.Uint32(front[4:]) != 1 || binary.LittleEndian.Uint32(front[8:]) != 2 {
		return 0, errors.New("initial method/mode mismatch")
	}
	n := int(binary.LittleEndian.Uint32(front[12:]))
	if n != len(front)-16 {
		return 0, errors.New("initial auth payload length mismatch")
	}
	body := front[16:]
	if len(body) < 9 || body[0] != 10 || binary.LittleEndian.Uint32(body[1:]) != 8 {
		return 0, errors.New("initial identity type mismatch")
	}
	n = int(binary.LittleEndian.Uint32(body[5:]))
	if n != 4 || len(body) != 9+n+8 || string(body[9:9+n]) != "test" {
		return 0, errors.New("initial identity mismatch")
	}
	global := binary.LittleEndian.Uint64(body[9+n:])
	more, err := r.Read()
	if err != nil || more.Tag != msgr.AuthRequestMore || len(more.Segments) != 1 {
		return 0, errors.New("initial CephX proof missing")
	}
	encoded := more.Segments[0]
	if len(encoded) < 4 || int(binary.LittleEndian.Uint32(encoded)) != len(encoded)-4 {
		return 0, errors.New("initial CephX proof length mismatch")
	}
	proof := encoded[4:]
	// New AUTH credentials carry a v1 ticket with secret ID zero and an empty
	// ticket blob, rather than reclaiming the primary MON's previous proof.
	if len(proof) < 36 || binary.LittleEndian.Uint16(proof) != 0x100 || proof[2] != 3 || proof[19] != 1 || binary.LittleEndian.Uint64(proof[20:]) != 0 || binary.LittleEndian.Uint32(proof[28:]) != 0 {
		return 0, errors.New("directed authentication reused a previous ticket proof")
	}
	return global, nil
}

func namedTestClientID(p []byte) (uint64, error) {
	raw := bytes.NewReader(p)
	if _, err := msgr.ReadBanner(raw, msgr.Banner{Supported: 3, Required: 1}); err != nil {
		return 0, err
	}
	r := msgr.NewReader(raw, 0)
	for range 3 {
		if _, err := r.Read(); err != nil {
			return 0, err
		}
	} // HELLO, AUTH_REQUEST, AUTH_REQUEST_MORE
	secret := make([]byte, 40)
	for i := range secret {
		secret[i] = byte(i)
	} // Existing mock peer's independent secure secret.
	r.EnableSecure(secret[:16], secret[28:40])
	var frame msgr.Frame
	for _, tag := range []msgr.Tag{msgr.AuthSignature, msgr.CompressionRequest, msgr.ClientIdent} {
		var err error
		frame, err = r.Read()
		if err != nil || frame.Tag != tag || len(frame.Segments) != 1 {
			return 0, errors.New("secure CLIENT_IDENT trace incomplete")
		}
	}
	front := frame.Segments[0]
	if len(front) < 5 || front[0] != 2 {
		return 0, errors.New("CLIENT_IDENT address vector missing")
	}
	count := binary.LittleEndian.Uint32(front[1:])
	if count > 64 {
		return 0, errors.New("CLIENT_IDENT address count invalid")
	}
	offset := 5
	skipAddress := func() bool {
		if len(front)-offset < 7 || front[offset] != 1 {
			return false
		}
		size := int(binary.LittleEndian.Uint32(front[offset+3:]))
		if size > len(front)-offset-7 {
			return false
		}
		offset += 7 + size
		return true
	}
	for range count {
		if !skipAddress() {
			return 0, errors.New("CLIENT_IDENT local address truncated")
		}
	}
	if !skipAddress() || len(front)-offset < 8 {
		return 0, errors.New("CLIENT_IDENT target/identity truncated")
	}
	return binary.LittleEndian.Uint64(front[offset:]), nil
}
