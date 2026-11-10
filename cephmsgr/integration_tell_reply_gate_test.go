package cephmsgr

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// tellReplyGate withholds exactly one incoming Tell reply (type 98) on an
// uncompressed CRC connection: the reply whose transaction ID is the one that
// the selected command actually carried when it was written. The command is
// selected by the digests of its complete front and bulk data, on whichever
// subject connection it is first written. Every other frame, in both
// directions and on every connection, passes through unchanged.
//
// It is test-only instrumentation. It decodes frames given to net.Conn.Write
// and bytes returned by net.Conn.Read; it does not count server executions,
// and secure streams must never be passed to it.
type tellReplyGate struct {
	mu          sync.Mutex
	front, data [32]byte
	armed       bool
	bound       bool
	connection  uint64
	transaction uint64
	sequence    uint64
	holder      *tellReplyGateConn
	reply       tellHeldReply
	held        chan struct{}
	heldOnce    sync.Once
	err         error
}

// tellHeldReply identifies the withheld frame. No byte of it reached the
// reader of the gated connection.
type tellHeldReply struct {
	Connection  uint64 `json:"connection"`
	Type        uint16 `json:"type"`
	Transaction uint64 `json:"transaction"`
	Sequence    uint64 `json:"sequence"`
	FrameBytes  int    `json:"frame_bytes"`
}

func newTellReplyGate() *tellReplyGate { return &tellReplyGate{held: make(chan struct{})} }

// arm selects the one command whose reply is withheld. Only the first write of
// that exact command binds; a later identical write never rebinds the gate.
func (g *tellReplyGate) arm(front, data []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.front, g.data, g.armed = sha256.Sum256(front), sha256.Sum256(data), true
}

func (g *tellReplyGate) fail(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err == nil {
		g.err = err
	}
}

// binding returns the connection and transaction the gate is bound to.
func (g *tellReplyGate) binding() (connection, transaction, sequence uint64, bound bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.connection, g.transaction, g.sequence, g.bound
}

func (g *tellReplyGate) heldReply() (tellHeldReply, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reply, g.err
}

// closeHeld ends only the connection whose reply is withheld.
func (g *tellReplyGate) closeHeld() error {
	g.mu.Lock()
	holder := g.holder
	g.mu.Unlock()
	if holder == nil {
		return errors.New("the Tell reply gate holds no connection")
	}
	return holder.Close()
}

func (g *tellReplyGate) bind(c *tellReplyGateConn, message msgr.MessageData) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.armed || g.bound || message.Type != msgr.TellCommandMessage ||
		sha256.Sum256(message.Front) != g.front || sha256.Sum256(message.Data) != g.data {
		return
	}
	g.bound, g.connection, g.transaction, g.sequence, g.holder = true, c.connection, message.Transaction, message.Sequence, c
}

func (g *tellReplyGate) withholds(c *tellReplyGateConn, message msgr.MessageData, frameBytes int) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.bound || g.holder != c || message.Type != msgr.TellCommandReplyMessage || message.Transaction != g.transaction {
		return false
	}
	g.reply = tellHeldReply{c.connection, message.Type, message.Transaction, message.Sequence, frameBytes}
	return true
}

// tellReplyGateFrameLimit bounds one decoded frame of the gated stream.
const tellReplyGateFrameLimit = 4 << 20

type tellReplyGateConn struct {
	net.Conn
	gate       *tellReplyGate
	connection uint64
	closed     chan struct{}
	closeOnce  sync.Once

	readMu      sync.Mutex
	bannerDone  bool
	passthrough bool
	// pending holds received bytes of a frame that is not complete yet; ready
	// holds complete frames that are released to the reader unchanged.
	pending, ready []byte
	readErr        error
}

func newTellReplyGateConn(conn net.Conn, gate *tellReplyGate, connection uint64) *tellReplyGateConn {
	return &tellReplyGateConn{Conn: conn, gate: gate, connection: connection, closed: make(chan struct{})}
}

func (c *tellReplyGateConn) Write(p []byte) (int, error) {
	// Bind before the bytes leave, so the reply cannot arrive ahead of the
	// binding. The session writes one complete frame per Write call.
	if len(p) >= 32 && p[0] == byte(msgr.Message) && msgr.CRC(0, p[:28]) == binary.LittleEndian.Uint32(p[28:32]) {
		reader := bytes.NewReader(p)
		if frame, err := msgr.NewReader(reader, tellReplyGateFrameLimit).Read(); err == nil && reader.Len() == 0 {
			if message, err := msgr.DecodeMessage(frame); err == nil {
				c.gate.bind(c, message)
			}
		}
	}
	return c.Conn.Write(p)
}

func (c *tellReplyGateConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for {
		if len(c.ready) > 0 {
			n := copy(p, c.ready)
			c.ready = c.ready[n:]
			return n, nil
		}
		if !c.passthrough {
			progressed, withheld := c.parse()
			if withheld {
				c.gate.heldOnce.Do(func() { close(c.gate.held) })
				<-c.closed
				return 0, net.ErrClosed
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
func (c *tellReplyGateConn) parse() (progressed, withheld bool) {
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
		frame, err := msgr.NewReader(input, tellReplyGateFrameLimit).Read()
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

func (c *tellReplyGateConn) release(n int) {
	c.ready = append(c.ready, c.pending[:n]...)
	c.pending = c.pending[n:]
}

// abandon records why the stream can no longer be decoded and forwards the
// remaining bytes unchanged. The gate then withholds nothing on this
// connection, and the recorded error fails the test that relies on it.
func (c *tellReplyGateConn) abandon(err error) {
	c.gate.fail(err)
	c.ready, c.pending, c.passthrough = append(c.ready, c.pending...), nil, true
}

func (c *tellReplyGateConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// paddedReplyGate is the gate for streams that cannot be decoded (secure
// modes). It binds to the first write of at least threshold bytes to a selected
// peer and, from before that write leaves, withholds every byte received on
// that one connection, so no reply to the padded command can reach its reader.
// Later large writes, on any connection, never rebind it.
//
// It identifies no frame: it cannot show which message a write carried, that a
// reply arrived, or a transaction ID. The padded command must be the only
// write of that size to a selected peer; the test checks that separately.
type paddedReplyGate struct {
	mu         sync.Mutex
	threshold  int
	selects    func(peer string) bool
	bound      bool
	connection uint64
	peer       string
	holder     *testcluster.LostReplyConn
	// armed is closed when the gate binds.
	armed chan struct{}
	// received is closed when bytes arrived on the bound connection after the
	// binding and were withheld. They are not attributable to any message.
	received     chan struct{}
	receivedOnce sync.Once
}

func newPaddedReplyGate(threshold int, selects func(peer string) bool) *paddedReplyGate {
	return &paddedReplyGate{threshold: threshold, selects: selects, armed: make(chan struct{}), received: make(chan struct{})}
}

type paddedReplyGateConn struct {
	*testcluster.LostReplyConn
	gate       *paddedReplyGate
	connection uint64
	peer       string
}

func (g *paddedReplyGate) wrap(conn net.Conn, connection uint64, peer string) *paddedReplyGateConn {
	lost := &testcluster.LostReplyConn{Conn: conn, Armed: &atomic.Bool{}, Closed: make(chan struct{}),
		SignalBlocked: func() { g.receivedOnce.Do(func() { close(g.received) }) }}
	return &paddedReplyGateConn{LostReplyConn: lost, gate: g, connection: connection, peer: peer}
}

func (c *paddedReplyGateConn) Write(p []byte) (int, error) {
	if g := c.gate; len(p) >= g.threshold && g.selects(c.peer) {
		g.mu.Lock()
		if !g.bound {
			// Arm before the bytes leave, so a reply cannot pass first.
			g.bound, g.connection, g.peer, g.holder = true, c.connection, c.peer, c.LostReplyConn
			c.Armed.Store(true)
			close(g.armed)
		}
		g.mu.Unlock()
	}
	return c.LostReplyConn.Write(p)
}

// binding returns the connection and peer whose received bytes are withheld.
func (g *paddedReplyGate) binding() (connection uint64, peer string, bound bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.connection, g.peer, g.bound
}

// closeHeld ends only the connection whose received bytes are withheld.
func (g *paddedReplyGate) closeHeld() error {
	g.mu.Lock()
	holder := g.holder
	g.mu.Unlock()
	if holder == nil {
		return errors.New("the padded reply gate holds no connection")
	}
	return holder.Close()
}
