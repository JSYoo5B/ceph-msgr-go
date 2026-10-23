package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"time"
)

// Classic libpcap file reader. It is implemented with the standard library so
// that capture analysis needs no libpcap, CGO or tshark. pcapng is rejected
// explicitly; tcpdump -w writes the classic format by default.

const (
	linkEthernet = 1
	linkRaw      = 101
	linkSLL      = 113
	linkSLL2     = 276

	tcpFIN = 0x01
	tcpSYN = 0x02
	tcpRST = 0x04
	tcpACK = 0x10

	maxPcapRecord = 256 << 10
)

var errPcapng = errors.New("pcapng is not supported; capture with tcpdump -w (classic pcap) or convert with editcap -F pcap")

type pcapInfo struct {
	Snaplen   uint32 `json:"snaplen"`
	LinkType  uint32 `json:"link_type"`
	Nanos     bool   `json:"nanosecond_timestamps"`
	Records   int    `json:"records"`
	Truncated int    `json:"truncated_records"`
	Skipped   int    `json:"non_tcp_records"`
}

type packet struct {
	Index    int
	Time     time.Time
	Src, Dst netip.AddrPort
	Seq, Ack uint32
	Flags    uint8
	Payload  []byte
	// Missing counts TCP payload bytes declared by the IP header but absent
	// from the record because of snaplen truncation.
	Missing int
}

type pcapReader struct {
	r     io.Reader
	order binary.ByteOrder
	info  pcapInfo
}

func newPcapReader(r io.Reader) (*pcapReader, error) {
	var h [24]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, fmt.Errorf("pcap header: %w", err)
	}
	p := &pcapReader{r: r}
	switch magic := binary.LittleEndian.Uint32(h[:4]); magic {
	case 0xa1b2c3d4:
		p.order = binary.LittleEndian
	case 0xa1b23c4d:
		p.order, p.info.Nanos = binary.LittleEndian, true
	case 0xd4c3b2a1:
		p.order = binary.BigEndian
	case 0x4d3cb2a1:
		p.order, p.info.Nanos = binary.BigEndian, true
	case 0x0a0d0d0a:
		return nil, errPcapng
	default:
		return nil, fmt.Errorf("not a pcap file (magic %#08x)", magic)
	}
	p.info.Snaplen = p.order.Uint32(h[16:])
	p.info.LinkType = p.order.Uint32(h[20:]) & 0x0fffffff
	switch p.info.LinkType {
	case linkEthernet, linkRaw, linkSLL, linkSLL2:
	default:
		return nil, fmt.Errorf("unsupported pcap link type %d", p.info.LinkType)
	}
	return p, nil
}

// next returns the next TCP packet. Non-TCP records are counted and skipped.
func (p *pcapReader) next() (packet, error) {
	for {
		var h [16]byte
		if _, err := io.ReadFull(p.r, h[:]); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return packet{}, fmt.Errorf("record %d: truncated pcap record header (partial capture file)", p.info.Records+1)
			}
			return packet{}, err
		}
		sec, frac := p.order.Uint32(h[0:]), p.order.Uint32(h[4:])
		caplen, wirelen := p.order.Uint32(h[8:]), p.order.Uint32(h[12:])
		if caplen > maxPcapRecord || caplen > wirelen {
			return packet{}, fmt.Errorf("record %d: invalid captured length %d (wire %d)", p.info.Records+1, caplen, wirelen)
		}
		data := make([]byte, caplen)
		if _, err := io.ReadFull(p.r, data); err != nil {
			return packet{}, fmt.Errorf("record %d: truncated pcap record data (partial capture file): %w", p.info.Records+1, err)
		}
		p.info.Records++
		if caplen < wirelen {
			p.info.Truncated++
		}
		ns := int64(frac) * 1000
		if p.info.Nanos {
			ns = int64(frac)
		}
		pkt, ok := decodeLink(p.info.LinkType, data)
		if !ok {
			p.info.Skipped++
			continue
		}
		pkt.Index = p.info.Records
		pkt.Time = time.Unix(int64(sec), ns).UTC()
		return pkt, nil
	}
}

func decodeLink(link uint32, b []byte) (packet, bool) {
	var ethertype uint16
	switch link {
	case linkEthernet:
		if len(b) < 14 {
			return packet{}, false
		}
		ethertype, b = binary.BigEndian.Uint16(b[12:]), b[14:]
		for (ethertype == 0x8100 || ethertype == 0x88a8) && len(b) >= 4 {
			ethertype, b = binary.BigEndian.Uint16(b[2:]), b[4:]
		}
	case linkSLL:
		if len(b) < 16 {
			return packet{}, false
		}
		ethertype, b = binary.BigEndian.Uint16(b[14:]), b[16:]
	case linkSLL2:
		if len(b) < 20 {
			return packet{}, false
		}
		ethertype, b = binary.BigEndian.Uint16(b[0:]), b[20:]
	case linkRaw:
		if len(b) < 1 {
			return packet{}, false
		}
		ethertype = 0x0800
		if b[0]>>4 == 6 {
			ethertype = 0x86dd
		}
	}
	var src, dst netip.Addr
	var proto uint8
	var ipPayload int // declared length of the IP payload
	switch ethertype {
	case 0x0800:
		if len(b) < 20 || b[0]>>4 != 4 {
			return packet{}, false
		}
		ihl := int(b[0]&0x0f) * 4
		total := int(binary.BigEndian.Uint16(b[2:]))
		if ihl < 20 || len(b) < ihl || total < ihl {
			return packet{}, false
		}
		if binary.BigEndian.Uint16(b[6:])&0x3fff != 0 {
			return packet{}, false // fragments are not reassembled
		}
		proto = b[9]
		src, dst = netip.AddrFrom4([4]byte(b[12:16])), netip.AddrFrom4([4]byte(b[16:20]))
		ipPayload = total - ihl
		b = b[ihl:]
	case 0x86dd:
		if len(b) < 40 {
			return packet{}, false
		}
		proto = b[6]
		ipPayload = int(binary.BigEndian.Uint16(b[4:]))
		src, dst = netip.AddrFrom16([16]byte(b[8:24])), netip.AddrFrom16([16]byte(b[24:40]))
		b = b[40:]
	default:
		return packet{}, false
	}
	if proto != 6 || len(b) < 20 {
		return packet{}, false
	}
	off := int(b[12]>>4) * 4
	if off < 20 || len(b) < off || ipPayload < off {
		return packet{}, false
	}
	pkt := packet{
		Src:   netip.AddrPortFrom(src, binary.BigEndian.Uint16(b[0:])),
		Dst:   netip.AddrPortFrom(dst, binary.BigEndian.Uint16(b[2:])),
		Seq:   binary.BigEndian.Uint32(b[4:]),
		Ack:   binary.BigEndian.Uint32(b[8:]),
		Flags: b[13],
	}
	declared := ipPayload - off
	payload := b[off:]
	if len(payload) > declared {
		payload = payload[:declared] // Ethernet padding
	}
	pkt.Payload = payload
	pkt.Missing = declared - len(payload)
	return pkt, true
}
