package testcluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// OSDReplyGate is a development-only fault for one object request of one
// test client. It wraps every connection the client dials to the selected
// OSD and withholds the reply to one request, in one of two modes.
//
// Decoded mode (uncompressed CRC connections only): the request is selected
// by a marker, a byte string that only its payload contains. When a complete
// MOSDOp frame with the marker is first written after Arm, the gate binds to
// that connection and to the transaction ID in the frame, before the bytes
// leave. On the read side it decodes the stream from the banner on and
// releases complete frames only. The MOSDOpReply frame with the bound
// transaction on the bound connection is held: no byte of it reaches the
// reader, and neither does any later byte of that connection, because the
// stream is ordered. Every frame before it is delivered first. The gate also
// counts every written MOSDOp frame that carries the marker, on every wrapped
// connection, bound or not: that is the transmission count of the request.
//
// Undecoded mode (secure connections, whose frames a wrapper cannot read):
// the gate binds to the first write of at least Threshold bytes after Arm
// and, from before that write leaves, withholds every byte received on that
// connection. It identifies no frame: not the message a write carried, not a
// transaction ID, and not whether a reply arrived. The caller must show
// separately that the selected request is the only write of that size.
//
// Both modes count the writes of at least Threshold bytes on every wrapped
// connection. A decoded run therefore shows whether the request selected by
// the marker is the only such write of the same sequence.
//
// The gate observes bytes given to net.Conn.Write and returned by
// net.Conn.Read. It does not count executions by the daemon.
type OSDReplyGate struct {
	mu          sync.Mutex
	decoded     bool
	threshold   int
	marker      []byte
	armed       bool
	bound       bool
	holder      *osdReplyGateConn
	connections int
	state       OSDReplyGateState
	boundOnce   sync.Once
	boundCh     chan struct{}
	heldOnce    sync.Once
	heldCh      chan struct{}
}

// OSDHeldReply identifies the frame a decoded gate withheld.
type OSDHeldReply struct {
	Connection  int    `json:"connection"`
	Type        uint16 `json:"type"`
	Transaction uint64 `json:"transaction"`
	Sequence    uint64 `json:"sequence"`
	FrameBytes  int    `json:"frame_bytes"`
}

// OSDReplyGateState is what the gate observed. Fields a mode cannot observe
// stay nil: a nil count is "not observed", never zero.
type OSDReplyGateState struct {
	Decoded     bool `json:"decoded"`
	Threshold   int  `json:"threshold"`
	Connections int  `json:"connections"`
	// Bound, BoundConnection (1-based dial order) and BoundWriteBytes
	// describe the write the gate bound to.
	Bound           bool `json:"bound"`
	BoundConnection int  `json:"bound_connection,omitempty"`
	BoundWriteBytes int  `json:"bound_write_bytes,omitempty"`
	// LargeWrites counts writes of at least Threshold bytes on every wrapped
	// connection since the gate was created.
	LargeWrites int `json:"large_writes"`
	// Decoded mode only.
	Transaction       *uint64       `json:"transaction,omitempty"`
	Sequence          *uint64       `json:"sequence,omitempty"`
	MarkedRequests    *int          `json:"marked_requests,omitempty"`
	MarkedConnections []int         `json:"marked_connections,omitempty"`
	Held              *OSDHeldReply `json:"held,omitempty"`
	// Undecoded mode only: bytes arrived on the bound connection after the
	// binding and were withheld. They are not attributable to any message.
	WithheldBytesSeen *bool `json:"withheld_bytes_seen,omitempty"`
	// Error is the first reason a decoded stream could not be followed. The
	// gate then forwards that connection unchanged and withholds nothing.
	Error string `json:"error,omitempty"`
}

// osdReplyGateFrameLimit bounds one decoded frame of a gated stream.
const osdReplyGateFrameLimit = 4 << 20

// NewOSDReplyGate returns a gate for CRC streams (decoded) or for streams it
// cannot read. threshold must be positive.
func NewOSDReplyGate(decoded bool, threshold int) *OSDReplyGate {
	g := &OSDReplyGate{decoded: decoded, threshold: threshold, boundCh: make(chan struct{}), heldCh: make(chan struct{})}
	g.state.Decoded, g.state.Threshold = decoded, threshold
	if decoded {
		g.state.MarkedRequests = new(int)
	} else {
		g.state.WithheldBytesSeen = new(bool)
	}
	return g
}

// Wrap gates one freshly dialed connection, before any byte is exchanged.
func (g *OSDReplyGate) Wrap(conn net.Conn) net.Conn {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.connections++
	g.state.Connections = g.connections
	return &osdReplyGateConn{Conn: conn, gate: g, index: g.connections, closed: make(chan struct{})}
}

// Arm selects the request whose reply is withheld. A decoded gate needs the
// marker; an undecoded gate ignores it. Only the first matching write binds.
func (g *OSDReplyGate) Arm(marker []byte) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.threshold <= 0 || g.decoded && len(marker) < 16 {
		return errors.New("OSD reply gate needs a positive threshold and, when decoding, a marker of at least 16 bytes")
	}
	if g.armed {
		return errors.New("OSD reply gate is already armed")
	}
	g.marker, g.armed = bytes.Clone(marker), true
	return nil
}

// Bound is closed when the gate binds to a write.
func (g *OSDReplyGate) Bound() <-chan struct{} { return g.boundCh }

// Held is closed when a decoded gate holds the bound reply, or when an
// undecoded gate withholds the first bytes after its binding.
func (g *OSDReplyGate) Held() <-chan struct{} { return g.heldCh }

// State returns a copy of what the gate observed so far.
func (g *OSDReplyGate) State() OSDReplyGateState {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.state
	if s.Transaction != nil {
		s.Transaction, s.Sequence = clone(s.Transaction), clone(s.Sequence)
	}
	s.MarkedRequests, s.Held, s.WithheldBytesSeen = clone(s.MarkedRequests), clone(s.Held), clone(s.WithheldBytesSeen)
	s.MarkedConnections = append([]int(nil), s.MarkedConnections...)
	return s
}

func clone[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// CloseHeld closes only the connection whose received bytes are withheld.
func (g *OSDReplyGate) CloseHeld() error {
	g.mu.Lock()
	holder := g.holder
	g.mu.Unlock()
	if holder == nil {
		return errors.New("the OSD reply gate holds no connection")
	}
	return holder.Close()
}

func (g *OSDReplyGate) fail(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state.Error == "" {
		g.state.Error = err.Error()
	}
}

// written is called with every outgoing write before it leaves.
func (g *OSDReplyGate) written(c *osdReplyGateConn, p []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	large := g.threshold > 0 && len(p) >= g.threshold
	if large {
		g.state.LargeWrites++
	}
	if !g.decoded {
		if large && g.armed && !g.bound {
			g.bind(c, len(p))
			c.blockAll = true
		}
		return
	}
	// The session writes one complete frame per Write call.
	if !g.armed || len(p) < 32 || p[0] != byte(msgr.Message) || msgr.CRC(0, p[:28]) != binary.LittleEndian.Uint32(p[28:32]) {
		return
	}
	reader := bytes.NewReader(p)
	frame, err := msgr.NewReader(reader, osdReplyGateFrameLimit).Read()
	if err != nil || reader.Len() != 0 {
		return
	}
	message, err := msgr.DecodeMessage(frame)
	if err != nil || message.Type != msgr.OSDOpMessage ||
		!bytes.Contains(message.Front, g.marker) && !bytes.Contains(message.Middle, g.marker) && !bytes.Contains(message.Data, g.marker) {
		return
	}
	*g.state.MarkedRequests++
	g.state.MarkedConnections = append(g.state.MarkedConnections, c.index)
	if !g.bound {
		transaction, sequence := message.Transaction, message.Sequence
		g.state.Transaction, g.state.Sequence = &transaction, &sequence
		g.bind(c, len(p))
	}
}

func (g *OSDReplyGate) bind(c *osdReplyGateConn, size int) {
	g.bound, g.holder = true, c
	g.state.Bound, g.state.BoundConnection, g.state.BoundWriteBytes = true, c.index, size
	g.boundOnce.Do(func() { close(g.boundCh) })
}

func (g *OSDReplyGate) withholds(c *osdReplyGateConn, message msgr.MessageData, frameBytes int) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.bound || g.holder != c || message.Type != msgr.OSDOpReplyMessage || message.Transaction != *g.state.Transaction {
		return false
	}
	g.state.Held = &OSDHeldReply{c.index, message.Type, message.Transaction, message.Sequence, frameBytes}
	return true
}

type osdReplyGateConn struct {
	net.Conn
	gate      *OSDReplyGate
	index     int
	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error
	// blockAll is set under gate.mu before the binding write leaves and read
	// under readMu afterwards (undecoded mode).
	blockAll bool

	readMu      sync.Mutex
	bannerDone  bool
	passthrough bool
	// pending holds received bytes of a frame that is not complete yet; ready
	// holds complete frames that are released to the reader unchanged.
	pending, ready []byte
	readErr        error
}

func (c *osdReplyGateConn) Write(p []byte) (int, error) {
	// Bind before the bytes leave, so the reply cannot pass ahead of it.
	c.gate.written(c, p)
	return c.Conn.Write(p)
}

func (c *osdReplyGateConn) blocking() bool {
	c.gate.mu.Lock()
	defer c.gate.mu.Unlock()
	return c.blockAll
}

func (c *osdReplyGateConn) park() (int, error) {
	c.gate.heldOnce.Do(func() { close(c.gate.heldCh) })
	<-c.closed
	return 0, net.ErrClosed
}

func (c *osdReplyGateConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if !c.gate.decoded {
		n, err := c.Conn.Read(p)
		if n > 0 && c.blocking() {
			c.gate.mu.Lock()
			*c.gate.state.WithheldBytesSeen = true
			c.gate.mu.Unlock()
			return c.park()
		}
		return n, err
	}
	for {
		if len(c.ready) > 0 {
			n := copy(p, c.ready)
			c.ready = c.ready[n:]
			return n, nil
		}
		if !c.passthrough {
			progressed, withheld := c.parse()
			if len(c.ready) > 0 {
				// Complete frames before the withheld reply, also from the
				// same underlying read, are delivered first.
				continue
			}
			if withheld {
				return c.park()
			}
			if progressed {
				continue
			}
		}
		if c.readErr != nil {
			// The stream ended inside a frame: release the remaining bytes
			// unchanged, then the original error.
			if len(c.pending) > 0 {
				c.ready, c.pending = c.pending, nil
				continue
			}
			return 0, c.readErr
		}
		if c.passthrough {
			return c.Conn.Read(p)
		}
		buffer := make([]byte, 32<<10)
		n, err := c.Conn.Read(buffer)
		c.pending = append(c.pending, buffer[:n]...)
		if err != nil {
			c.readErr = err
		}
	}
}

// parse moves every complete leading unit of pending (the banner, then whole
// frames) to ready. It stops at an incomplete frame, or at the withheld reply,
// which stays in pending and is never released.
func (c *osdReplyGateConn) parse() (progressed, withheld bool) {
	for len(c.pending) > 0 {
		if !c.bannerDone {
			if len(c.pending) < len(msgr.BannerPrefix)+2 {
				return progressed, false
			}
			if !bytes.HasPrefix(c.pending, []byte(msgr.BannerPrefix)) {
				c.abandon(errors.New("gated stream does not start with a msgr2 banner"))
				return true, false
			}
			size := len(msgr.BannerPrefix) + 2 + int(binary.LittleEndian.Uint16(c.pending[len(msgr.BannerPrefix):]))
			if len(c.pending) < size {
				return progressed, false
			}
			c.release(size)
			c.bannerDone, progressed = true, true
			continue
		}
		input := bytes.NewReader(c.pending)
		frame, err := msgr.NewReader(input, osdReplyGateFrameLimit).Read()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return progressed, false
		}
		if err != nil {
			c.abandon(err)
			return true, false
		}
		consumed := len(c.pending) - input.Len()
		if frame.Tag == msgr.Message {
			message, err := msgr.DecodeMessage(frame)
			if err != nil {
				c.abandon(err)
				return true, false
			}
			if c.gate.withholds(c, message, consumed) {
				return progressed, true
			}
		}
		c.release(consumed)
		progressed = true
	}
	return progressed, false
}

func (c *osdReplyGateConn) release(n int) {
	c.ready = append(c.ready, c.pending[:n]...)
	c.pending = c.pending[n:]
}

// abandon records why the stream can no longer be decoded and forwards the
// remaining bytes unchanged. The recorded error fails the test relying on it.
func (c *osdReplyGateConn) abandon(err error) {
	c.gate.fail(err)
	c.ready, c.pending, c.passthrough = append(c.ready, c.pending...), nil, true
}

// Close closes the underlying connection once and gives every caller that
// result: the session also closes its connection once its parked read is
// released, and that second Close must not turn the first into an error.
func (c *osdReplyGateConn) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.Conn.Close()
		close(c.closed)
	})
	return c.closeErr
}
