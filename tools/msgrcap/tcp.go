package main

import (
	"bytes"
	"net/netip"
	"sort"
	"time"
)

// TCP connection tracking and per-direction stream reassembly. Packet
// boundaries are not Messenger frame boundaries: frames are decoded only from
// the reassembled contiguous byte prefix of each direction.

type tcpEvent struct {
	Time   time.Time `json:"time"`
	From   string    `json:"from"` // "client" or "server"
	Kind   string    `json:"kind"` // SYN, SYN-ACK, FIN, RST
	Packet int       `json:"packet"`
}

type segment struct {
	off    int64
	data   []byte
	time   time.Time
	packet int
}

type chunk struct {
	start, end int64
	time       time.Time
}

type direction struct {
	base       uint32
	haveBase   bool
	sawSYN     bool
	segs       []segment
	packets    int
	missing    int // snaplen-truncated payload bytes
	beforeBase int
	maxAck     int64 // highest relative ACK this side sent about the peer
	haveAck    bool
	fin        bool
	finOffset  int64
	// Captured payload bytes at or beyond the analyzer's per-direction bound.
	// They were present in the input, so they are never counted as capture
	// loss; limit records the bound that excluded them.
	limitExcluded int64
	limit         int64

	// Reassembly results.
	stream        []byte
	chunks        []chunk
	retransmitted int
	conflicts     int
	gaps          []gap
	afterGap      int64
}

type gap struct {
	Offset int64 `json:"offset"`
	Length int64 `json:"length"`
}

type tcpConn struct {
	ID             int
	Client, Server netip.AddrPort
	Midstream      bool
	SYNs           int
	First, Last    time.Time
	SYNTime        time.Time
	SYNACKTime     time.Time
	Established    bool
	Events         []tcpEvent
	c2s, s2c       direction
	ended          bool
}

type connKey struct{ a, b netip.AddrPort }

type tracker struct {
	conns       []*tcpConn
	active      map[connKey]*tcpConn
	serverPorts map[uint16]bool
	maxBytes    int64
}

func newTracker(serverPorts []uint16, maxBytes int64) *tracker {
	t := &tracker{active: make(map[connKey]*tcpConn), serverPorts: make(map[uint16]bool), maxBytes: maxBytes}
	for _, p := range serverPorts {
		t.serverPorts[p] = true
	}
	return t
}

func (t *tracker) add(p packet) {
	key := connKey{p.Src, p.Dst}
	rkey := connKey{p.Dst, p.Src}
	c := t.active[key]
	fromClient := true
	if c == nil {
		if c = t.active[rkey]; c != nil {
			fromClient = false
		}
	} else if c.Client != p.Src {
		fromClient = false
	}
	syn := p.Flags&tcpSYN != 0 && p.Flags&tcpACK == 0
	if syn && c != nil && fromClient && c.c2s.sawSYN && c.c2s.base != p.Seq+1 {
		c = nil // a new connection reused the same four-tuple
	}
	// A SYN with the same ISN is a kernel SYN retransmission of one attempt.
	if c == nil {
		c = &tcpConn{ID: len(t.conns) + 1, First: p.Time}
		switch {
		case syn:
			c.Client, c.Server = p.Src, p.Dst
		case p.Flags&tcpSYN != 0: // SYN-ACK without the SYN
			c.Client, c.Server, c.Midstream = p.Dst, p.Src, true
		case t.serverPorts[p.Dst.Port()] || !t.serverPorts[p.Src.Port()]:
			c.Client, c.Server, c.Midstream = p.Src, p.Dst, true
		default:
			c.Client, c.Server, c.Midstream = p.Dst, p.Src, true
		}
		fromClient = c.Client == p.Src
		t.conns = append(t.conns, c)
		t.active[connKey{c.Client, c.Server}] = c
		t.active[connKey{c.Server, c.Client}] = c
	}
	c.Last = p.Time
	who, d, peer := "server", &c.s2c, &c.c2s
	if fromClient {
		who, d, peer = "client", &c.c2s, &c.s2c
	}
	d.packets++
	d.missing += p.Missing
	if p.Flags&tcpSYN != 0 {
		kind := "SYN"
		if p.Flags&tcpACK != 0 {
			kind = "SYN-ACK"
			if c.SYNACKTime.IsZero() {
				c.SYNACKTime = p.Time
			}
			c.Established = true
		} else {
			c.SYNs++
			if c.SYNTime.IsZero() {
				c.SYNTime = p.Time
			}
		}
		c.Events = append(c.Events, tcpEvent{p.Time, who, kind, p.Index})
		d.base, d.haveBase, d.sawSYN = p.Seq+1, true, true
	}
	if !d.haveBase {
		d.base, d.haveBase = p.Seq, true
	}
	if p.Flags&tcpACK != 0 && peer.haveBase {
		rel := int64(int32(p.Ack - peer.base))
		if !d.haveAck || rel > d.maxAck {
			d.maxAck, d.haveAck = rel, true
		}
	}
	off := int64(int32(p.Seq - d.base))
	if p.Flags&tcpSYN != 0 {
		off = 0
	}
	if len(p.Payload) > 0 {
		if off < 0 {
			d.beforeBase++
		} else {
			data := p.Payload
			if end := off + int64(len(data)); end > t.maxBytes {
				keep := max(t.maxBytes-off, 0)
				d.limitExcluded += int64(len(data)) - keep
				d.limit = t.maxBytes
				data = data[:keep]
			}
			if len(data) > 0 {
				d.segs = append(d.segs, segment{off, append([]byte(nil), data...), p.Time, p.Index})
			}
		}
	}
	if p.Flags&tcpFIN != 0 {
		if !d.fin {
			c.Events = append(c.Events, tcpEvent{p.Time, who, "FIN", p.Index})
		}
		d.fin, d.finOffset = true, off+int64(len(p.Payload))+int64(p.Missing)
	}
	if p.Flags&tcpRST != 0 {
		c.Events = append(c.Events, tcpEvent{p.Time, who, "RST", p.Index})
		c.ended = true
	}
}

// reassemble builds the contiguous prefix of one direction and records
// retransmissions, conflicting overlaps and gaps.
func (d *direction) reassemble() {
	segs := append([]segment(nil), d.segs...)
	sort.SliceStable(segs, func(i, j int) bool { return segs[i].off < segs[j].off })
	var cursor int64
	stopped := false
	for _, s := range segs {
		end := s.off + int64(len(s.data))
		if stopped {
			if end > cursor {
				if s.off > cursor {
					d.gaps = append(d.gaps, gap{cursor, s.off - cursor})
					cursor = s.off
				}
				d.afterGap += end - cursor
				cursor = end
			}
			continue
		}
		if s.off > cursor {
			d.gaps = append(d.gaps, gap{cursor, s.off - cursor})
			stopped = true
			d.afterGap += end - s.off
			cursor = end
			continue
		}
		if s.off < cursor {
			overlapEnd := min(end, cursor)
			if bytes.Equal(d.stream[s.off:overlapEnd], s.data[:overlapEnd-s.off]) {
				d.retransmitted++
			} else {
				d.conflicts++
			}
			if end <= cursor {
				continue
			}
			s.data, s.off = s.data[cursor-s.off:], cursor
		}
		d.stream = append(d.stream, s.data...)
		d.chunks = append(d.chunks, chunk{s.off, end, s.time})
		cursor = end
	}
}

// timeAt returns the capture time of the packet that completed byte offset
// end-1 of the reassembled stream.
func (d *direction) timeAt(end int64) time.Time {
	i := sort.Search(len(d.chunks), func(i int) bool { return d.chunks[i].end >= end })
	if i < len(d.chunks) {
		return d.chunks[i].time
	}
	return time.Time{}
}

// ackedMissing reports peer-acknowledged bytes absent from the capture. They
// were delivered on the network, so their absence is a capture loss
// (drop/snaplen), not a TCP loss. Offsets at or beyond an exhausted analysis
// bound are excluded: their absence from the stream is the analyzer's limit,
// reported separately, not evidence about the capture.
func (d *direction) ackedMissing(peerAck int64, havePeerAck bool) int64 {
	if !havePeerAck {
		return 0
	}
	if d.limit > 0 && peerAck > d.limit {
		peerAck = d.limit
	}
	end := int64(len(d.stream))
	if d.fin && int64(len(d.stream)) >= d.finOffset {
		end++ // FIN consumes one sequence number
	}
	if peerAck > end {
		return peerAck - end
	}
	return 0
}
