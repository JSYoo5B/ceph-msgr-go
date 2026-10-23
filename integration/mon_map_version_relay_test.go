package integration_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// mapVersionRelay is a test-only CRC observer. It forwards the original bytes,
// including authentication transcripts, and discards exactly one matching
// MMonGetVersionReply. It keeps reading the peer after that discard: transport
// EOF must remain observable without a local Close or a keepalive timeout.
// Secure or compressed frames are deliberately outside this relay's contract.
type mapVersionRelay struct {
	mu       sync.Mutex
	nextConn uint64
	armed    bool
	target   *mapVersionRequest
	requests []mapVersionRequest
	replies  []mapVersionReply
	held     chan mapVersionReply
	ended    chan mapVersionRelayEnd
}

type mapVersionRequest struct {
	Connection, TID, Sequence, Handle uint64
	Endpoint, Kind                    string
}

type mapVersionReply struct {
	Request mapVersionRequest
	Newest  uint64
	Oldest  uint64
	Held    bool
}

type mapVersionRelayEnd struct {
	Connection uint64
	Err        error
}

func newMapVersionRelay() *mapVersionRelay {
	return &mapVersionRelay{held: make(chan mapVersionReply, 1), ended: make(chan mapVersionRelayEnd, 16)}
}

func (r *mapVersionRelay) wrap(conn net.Conn, endpoint string) net.Conn {
	r.mu.Lock()
	r.nextConn++
	id := r.nextConn
	r.mu.Unlock()
	c := &mapVersionRelayConn{Conn: conn, relay: r, id: id, endpoint: endpoint}
	c.reader = msgr.NewReader(io.TeeReader(conn, &c.raw), mapVersionRelayLimit)
	return c
}

func (r *mapVersionRelay) arm() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.armed = true
}

func (r *mapVersionRelay) snapshot() ([]mapVersionRequest, []mapVersionReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]mapVersionRequest(nil), r.requests...), append([]mapVersionReply(nil), r.replies...)
}

// requestLocked runs only after every byte of this frame was written to the
// underlying connection. Peer receipt is established separately by the reply.
func (r *mapVersionRelay) requestLocked(c *mapVersionRelayConn, m msgr.MessageData) error {
	if m.Type != msgr.MonGetVersionMessage {
		return nil
	}
	d := wire.NewDecoderLimit(m.Front, mapVersionRelayLimit)
	handle, kind := d.U64(), d.String()
	if err := d.Done(); err != nil {
		return err
	}
	if len(r.requests) >= 16 || handle == 0 || m.Transaction == 0 {
		return errors.New("map-version relay request identity or receipt bound")
	}
	request := mapVersionRequest{c.id, m.Transaction, m.Sequence, handle, c.endpoint, kind}
	r.requests = append(r.requests, request)
	if r.armed && r.target == nil && kind == "osdmap" {
		r.target = &request
	}
	return nil
}

func (r *mapVersionRelay) reply(c *mapVersionRelayConn, m msgr.MessageData) (bool, error) {
	if m.Type != msgr.MonGetVersionReplyMessage {
		return false, nil
	}
	decoded, err := msgr.DecodeMonGetVersionReply(m, mapVersionRelayLimit)
	if err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, request := range r.requests {
		if request.Connection != c.id || request.TID != m.Transaction || request.Handle != decoded.Handle {
			continue
		}
		if len(r.replies) >= 16 {
			return false, errors.New("map-version relay reply receipt bound")
		}
		reply := mapVersionReply{Request: request, Newest: decoded.Newest, Oldest: decoded.Oldest}
		if r.target != nil && *r.target == request {
			reply.Held = true
			r.target = nil
			r.armed = false
			r.held <- reply
		}
		r.replies = append(r.replies, reply)
		return reply.Held, nil
	}
	// Do not hide or discard a reply that cannot be joined to a transmitted
	// request. It remains available to the product's correlation checks.
	return false, nil
}

const mapVersionRelayLimit = 1 << 20

type mapVersionRelayConn struct {
	net.Conn
	relay        *mapVersionRelay
	id           uint64
	endpoint     string
	readMu       sync.Mutex
	writeMu      sync.Mutex
	readBanner   bool
	writeBanner  bool
	reader       *msgr.Reader
	raw          bytes.Buffer
	readPending  []byte
	writePending []byte
	endOnce      sync.Once
}

func (c *mapVersionRelayConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	for len(c.readPending) == 0 {
		c.raw.Reset()
		if !c.readBanner {
			var banner [26]byte
			if _, err := io.ReadFull(io.TeeReader(c.Conn, &c.raw), banner[:]); err != nil {
				return 0, c.ended(err)
			}
			if _, err := msgr.ReadBanner(bytes.NewReader(banner[:]), msgr.Banner{Supported: msgr.Revision1, Required: msgr.Revision1}); err != nil {
				return 0, err
			}
			c.readBanner = true
		} else {
			frame, err := c.reader.Read()
			if err != nil {
				return 0, c.ended(err)
			}
			if frame.Tag == msgr.Message {
				message, err := msgr.DecodeMessage(frame)
				if err != nil {
					return 0, err
				}
				discard, err := c.relay.reply(c, message)
				if err != nil {
					return 0, err
				}
				if discard {
					continue
				}
			}
		}
		c.readPending = bytes.Clone(c.raw.Bytes())
	}
	n := copy(p, c.readPending)
	c.readPending = c.readPending[n:]
	return n, nil
}

func (c *mapVersionRelayConn) ended(err error) error {
	c.endOnce.Do(func() {
		select {
		case c.relay.ended <- mapVersionRelayEnd{c.id, err}:
		default:
		}
	})
	return err
}

func (c *mapVersionRelayConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	// A complete peer reply may arrive before the sending goroutine is
	// scheduled again. Publish completed writes before matching that reply.
	c.relay.mu.Lock()
	defer c.relay.mu.Unlock()
	n, err := c.Conn.Write(p)
	if n > 0 {
		if observeErr := c.observeWritten(p[:n]); observeErr != nil && err == nil {
			err = observeErr
		}
	}
	return n, err
}

func (c *mapVersionRelayConn) observeWritten(p []byte) error {
	if len(c.writePending)+len(p) > mapVersionRelayLimit+128 {
		return errors.New("map-version relay write buffer bound")
	}
	c.writePending = append(c.writePending, p...)
	if !c.writeBanner {
		if len(c.writePending) < 26 {
			return nil
		}
		if _, err := msgr.ReadBanner(bytes.NewReader(c.writePending[:26]), msgr.Banner{Supported: msgr.Revision1, Required: msgr.Revision1}); err != nil {
			return err
		}
		c.writePending = c.writePending[26:]
		c.writeBanner = true
	}
	for len(c.writePending) >= 32 {
		preamble := c.writePending[:32]
		n := int(preamble[1])
		if n < 1 || n > 4 || preamble[26] != 0 {
			return errors.New("map-version relay requires uncompressed CRC frames")
		}
		var size uint64 = 32
		for i := 0; i < n; i++ {
			size += uint64(binary.LittleEndian.Uint32(preamble[2+i*6:]))
		}
		if binary.LittleEndian.Uint32(preamble[2:]) != 0 {
			size += 4
		}
		if n > 1 {
			size += 13
		}
		if size > mapVersionRelayLimit+128 {
			return errors.New("map-version relay frame bound")
		}
		if size > uint64(len(c.writePending)) {
			return nil
		}
		frame, err := msgr.NewReader(bytes.NewReader(c.writePending[:size]), mapVersionRelayLimit).Read()
		if err != nil {
			return fmt.Errorf("map-version relay outgoing CRC frame: %w", err)
		}
		if frame.Tag == msgr.Message {
			message, err := msgr.DecodeMessage(frame)
			if err != nil {
				return err
			}
			if err := c.relay.requestLocked(c, message); err != nil {
				return err
			}
		}
		c.writePending = c.writePending[size:]
	}
	return nil
}
